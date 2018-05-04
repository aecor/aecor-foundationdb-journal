package aecor.journal.foundationdb

import aecor.data._
import aecor.encoding.{KeyDecoder, KeyEncoder}
import aecor.journal.foundationdb.FoundationdbEventJournal.Serializer
import aecor.journal.foundationdb.FoundationdbEventJournal.Serializer.TypeHint
import aecor.journal.foundationdb.client.Transactor
import aecor.journal.foundationdb.client.algebra.transaction.TransactionIO
import aecor.journal.foundationdb.client.interpreters.ReaderTransaction
import aecor.runtime.EventJournal
import cats.data.NonEmptyVector
import cats.effect._
import cats.implicits._
import fs2._
import client.syntax._
import scala.concurrent.duration._

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
      tx: Transactor[F],
      settings: FoundationdbEventJournal.Settings,
      tagging: Tagging[K],
      serializer: Serializer[E]): FoundationdbEventJournal[F, K, E] =
    new FoundationdbEventJournal(tx, settings, tagging, serializer)

}

final class FoundationdbEventJournal[F[_], K, E](db: Transactor[F],
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

  private val dao =
    new FoundationdbEventJournalDAO[TransactionIO[F, ?]](tableName, ReaderTransaction[F])

  private[foundationdb] def dropTable: F[Unit] =
    dao.dropTable.transact(db)

  def appendTIO(entityKey: K, offset: Long, events: NonEmptyVector[E]): TransactionIO[F, Unit] = {

    val keyString = encodeKey(entityKey)
    dao
      .getAggregateVersion(keyString)
      .flatMap { currentVersion =>
        val nextVersion = currentVersion + 1
        if (nextVersion == offset)
          Stream.eval {
            events.traverseWithIndexM {
              case (e, idx) =>
                val (typeHint, bytes) = serializer.serialize(e)
                val tags = tagging.tag(entityKey)
                dao.append(keyString, nextVersion + idx, typeHint, bytes) >>
                  tags.toVector.traverse { x =>
                    dao.tag(x.value, idx, keyString, nextVersion + idx, typeHint, bytes)
                  }
            }
          } else
          Stream.raiseError(
            new IllegalArgumentException(
              s"Can not append event at [$offset], current version is [$currentVersion]"))
      }
      .compile
      .drain

  }

  override def append(entityKey: K, offset: Long, events: NonEmptyVector[E]): F[Unit] =
    appendTIO(entityKey, offset, events).transact(db)

  private val deserialize_ =
    (serializer.deserialize _).tupled

  override def foldById[S](key: K, offset: Long, zero: S)(f: (S, E) => Folded[S]): F[Folded[S]] =
    dao
      .getById(encodeKey(key), offset)
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
      .currentEventsByTag(tag.value, offset.value)
      .transact(db)
      .evalMap {
        case (eventOffset, keyString, seqNr, typeHint, bytes) =>
          F.fromEither {
            decodeKey(keyString)
              .toRight(new IllegalArgumentException(s"Can't decode key [$keyString]"))
              .flatMap { key =>
                serializer.deserialize(typeHint, bytes).map { a =>
                  (Offset(Some(eventOffset)), EntityEvent(key, seqNr, a))
                }
              }
          }
      }

}
