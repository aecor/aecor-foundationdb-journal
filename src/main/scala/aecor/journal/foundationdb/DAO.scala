package aecor.journal.foundationdb

import aecor.journal.foundationdb.FoundationdbEventJournal.Serializer.TypeHint
import cats.effect.Sync
import cats.Monad
import cats.implicits._
import com.apple.foundationdb._
import com.apple.foundationdb.subspace.Subspace
import com.apple.foundationdb.tuple.{Tuple, Versionstamp}
import fs2._

class DAO(tableName: String) {
  val eventSubspace = new Subspace(Tuple.from(tableName, "events"))
  val tagSubspace = new Subspace(Tuple.from(tableName, "tags"))

  def getAggregateVersion[F[_]: Sync](key: String): ReadTransactionIO[F, Long] = {
    val subspace = eventSubspace.subspace(Tuple.from(key))
    RTIO
      .getRange[F](subspace.range, 1, reverse = true)
      .compile
      .last
      .map {
        case Some(kv) => subspace.unpack(kv.getKey).getLong(0)
        case None     => 0L
      }
  }

  def append[F[_]](keyString: String,
                   sequenceNr: Long,
                   typeHint: TypeHint,
                   bytes: Array[Byte]): TransactionIO[F, Unit] = {
    val key = eventSubspace
      .pack(
        Tuple
          .from(keyString, sequenceNr: Number))
    val value = Tuple.from(typeHint, bytes).pack()
    TIO.set(key, value)
  }

  def tag[F[_]: Sync](tag: String,
                      eventBatchIndex: Int,
                      entityKey: String,
                      sequenceNr: Long,
                      typeHint: TypeHint,
                      bytes: Array[Byte]): TransactionIO[F, Unit] = {
    val key =
      tagSubspace.packWithVersionstamp(Tuple.from(tag, Versionstamp.incomplete(eventBatchIndex)))
    val value = Tuple.from(entityKey, sequenceNr: Number, typeHint, bytes).pack()
    TIO.mutate[F](MutationType.SET_VERSIONSTAMPED_KEY, key, value)
  }

  def getById[F[_]: Sync](
      keyString: String,
      sequenceNr: Long): Stream[ReadTransactionIO[F, ?], (TypeHint, Array[Byte])] =
    Stream.force {
      getAggregateVersion[F](keyString).map { lastSeqNr =>
        val begin = eventSubspace.subspace(Tuple.from(keyString, sequenceNr: Number)).pack()
        val end = eventSubspace.subspace(Tuple.from(keyString, (lastSeqNr + 1L): Number)).pack()
        RTIO
          .getRange[F](begin, end)
          .map(x => Tuple.fromBytes(x.getValue))
          .map(x => (x.getString(0), x.getBytes(1)))
      }
    }

  def currentEventsByTag[F[_]: Monad](tag: String, lastProcessedOffset: Option[Versionstamp])
    : Stream[TransactionIO[F, ?], (Versionstamp, String, Long, TypeHint, Array[Byte])] = {
    val query = lastProcessedOffset match {
      case Some(versionstamp) =>
        val getKey =
          RTIO.getKey[F](
            KeySelector.firstGreaterThan(tagSubspace.pack(Tuple.from(tag, versionstamp))))
        Stream
          .eval[ReadTransactionIO[F, ?], Array[Byte]](getKey)
          .flatMap { begin =>
            RTIO.getRange(Range.startsWith(begin))
          }
      case None =>
        RTIO.getRange[F](tagSubspace.range(Tuple.from(tag)))
    }
    TIO.withSnapshotS(query).map { kv =>
      val key = Tuple.fromBytes(kv.getKey)
      val value = Tuple.fromBytes(kv.getValue)
      println(s"$key -> $value")
      (key.getVersionstamp(3),
       value.getString(0),
       value.getLong(1),
       value.getString(2),
       value.getBytes(3))
    }
  }

}
