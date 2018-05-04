package aecor.journal.foundationdb

import aecor.data._
import aecor.encoding.{KeyDecoder, KeyEncoder}
import aecor.journal.foundationdb.FoundationdbEventJournal.Serializer.TypeHint
import aecor.journal.foundationdb.FoundationdbEventJournal.Serializer
import aecor.runtime.EventJournal
import cats.data.NonEmptyVector
import cats.effect._
import cats.implicits._
import com.apple.foundationdb.subspace.Subspace
import com.apple.foundationdb.tuple.Tuple
import fs2._
import scala.concurrent.duration.FiniteDuration

object FoundationdbEventJournal {
  trait Serializer[A] {
    def serialize(a: A): (TypeHint, Array[Byte])
    def deserialize(typeHint: TypeHint, bytes: Array[Byte]): Either[Throwable, A]
  }
  object Serializer {
    type TypeHint = String
  }

  final case class Settings(tableName: String, pollingInterval: FiniteDuration)

  def apply[F[_]: Timer: ConcurrentEffect, K: KeyEncoder: KeyDecoder, E](
      db: FoundationDB[F],
      settings: FoundationdbEventJournal.Settings,
      tagging: Tagging[K],
      serializer: Serializer[E]): FoundationdbEventJournal[F, K, E] =
    new FoundationdbEventJournal(db, settings, tagging, serializer)

}

final class FoundationdbEventJournal[F[_], K, E](db: FoundationDB[F],
                                                 settings: FoundationdbEventJournal.Settings,
                                                 tagging: Tagging[K],
                                                 serializer: Serializer[E])(
    implicit F: ConcurrentEffect[F],
    encodeKey: KeyEncoder[K],
    decodeKey: KeyDecoder[K],
    timer: Timer[F])
    extends EventJournal[F, K, E]
    with FoundationdbEventJournalQueries[F, K, E] {

  import settings._

  val eventSubspace = new Subspace(Tuple.from(tableName, "events"))
  val tagSubspace = new Subspace(Tuple.from(tableName, "tags"))

  val dao = new DAO(tableName)

  private[foundationdb] def dropTable: F[Unit] =
    TIO.set[F](eventSubspace.pack(), Tuple.from().pack()).transact(db)

  def appendTIO(entityKey: K, offset: Long, events: NonEmptyVector[E]): TransactionIO[F, Unit] = {
    val keyString = encodeKey(entityKey)
    for {
      currentVersion <- dao.getAggregateVersion[F](keyString).asTransactionIO
      nextVersion = currentVersion + 1
      _ <- if (nextVersion == offset)
        events.traverseWithIndexM {
          case (e, idx) =>
            val (typeHint, bytes) = serializer.serialize(e)
            val tags = tagging.tag(entityKey)
            dao.append[F](keyString, nextVersion + idx, typeHint, bytes) >>
              tags.toVector.traverse { x =>
                dao.tag[F](x.value, idx, keyString, nextVersion + idx, typeHint, bytes)
              }
        } else
        TIO.raiseError[F, Unit](
          new IllegalArgumentException(
            s"Can not append event at [$offset], current version is [$currentVersion]"))
    } yield ()
  }

  override def append(entityKey: K, offset: Long, events: NonEmptyVector[E]): F[Unit] =
    appendTIO(entityKey, offset, events).transact(db)

  private val deserialize_ =
    (serializer.deserialize _).tupled

  override def foldById[S](key: K, offset: Long, zero: S)(f: (S, E) => Folded[S]): F[Folded[S]] =
    dao
      .getById[F](encodeKey(key), offset)
      .transact(db)
      .map(deserialize_)
      .evalMap(F.fromEither)
      .scan(Folded.next(zero))((acc, e) => acc.flatMap(f(_, e)))
      .takeWhile(_.isNext, takeFailure = true)
      .compile
      .last
      .map {
        case Some(x) => x
        case None    => Folded.next(zero)
      }

  override protected val sleepBeforePolling: F[Unit] =
    timer.sleep(pollingInterval)

  def currentEventsByTag(tag: EventTag, offset: Offset): Stream[F, (Offset, EntityEvent[K, E])] =
    dao
      .currentEventsByTag[F](tag.value, offset.value)
      .transact(db)
      .map {
        case (eventOffset, keyString, seqNr, typeHint, bytes) =>
          decodeKey(keyString)
            .toRight(new IllegalArgumentException(s"Can't decode key [$keyString]"))
            .flatMap { key =>
              serializer.deserialize(typeHint, bytes).map { a =>
                (Offset(Some(eventOffset)), EntityEvent(key, seqNr, a))
              }
            }

      }
      .evalMap(F.fromEither)

}
