package aecor.journal.foundationdb

import aecor.journal.foundationdb.FoundationdbEventJournal.Serializer.TypeHint
import aecor.journal.foundationdb.client.algebra.transaction.{ReadTransaction, Transaction}
import cats.FlatMap
import com.apple.foundationdb.subspace.Subspace
import com.apple.foundationdb.tuple.{Tuple, Versionstamp}
import com.apple.foundationdb.{KeySelector, KeyValue, MutationType}
import fs2._
import cats.implicits._

private[foundationdb] final class FoundationdbEventJournalDAO[F[_]: FlatMap](
    tableName: String,
    transaction: Transaction[F]) {
  val eventSubspace = new Subspace(Tuple.from(tableName, "events"))
  val tagSubspace = new Subspace(Tuple.from(tableName, "tags"))

  def dropTable: F[Unit] =
    transaction.clear(eventSubspace.range()) >> transaction.clear(tagSubspace.range())

  def getAggregateVersion(key: String): Stream[F, Long] = {
    val subspace = eventSubspace.subspace(Tuple.from(key))
    transaction
      .getRange(subspace.range, 1, reverse = true)
      .map(kv => subspace.unpack(kv.getKey).getLong(0))
      .lastOr(0L)

  }

  def append(entityKey: String,
             sequenceNr: Long,
             typeHint: TypeHint,
             bytes: Array[Byte]): F[Unit] = {
    val key = eventSubspace
      .pack(
        Tuple
          .from(entityKey, sequenceNr: Number))
    val value = Tuple.from(typeHint, bytes).pack()
    transaction.set(key, value)
  }

  def tag(tag: String,
          eventBatchIndex: Int,
          entityKey: String,
          sequenceNr: Long,
          typeHint: TypeHint,
          bytes: Array[Byte]): F[Unit] = {
    val key =
      tagSubspace.packWithVersionstamp(Tuple.from(tag, Versionstamp.incomplete(eventBatchIndex)))
    val value = Tuple.from(entityKey, sequenceNr: Number, typeHint, bytes).pack()
    transaction.mutate(MutationType.SET_VERSIONSTAMPED_KEY, key, value)
  }

  def getById(keyString: String, sequenceNr: Long): Stream[F, (TypeHint, Array[Byte])] = {
    val begin = eventSubspace.pack(Tuple.from(keyString, sequenceNr: Number))
    val end = eventSubspace.range(Tuple.from(keyString)).end
    transaction
      .getRange(begin, end)
      .map(x => Tuple.fromBytes(x.getValue))
      .map(x => (x.getString(0), x.getBytes(1)))
  }

  def currentEventsByTag(tag: String, lastProcessedOffset: Option[Versionstamp])
    : Stream[F, (Versionstamp, String, Long, TypeHint, Array[Byte])] = {
    def query(snapshot: ReadTransaction[F]): Stream[F, KeyValue] = lastProcessedOffset match {
      case Some(versionstamp) =>
        val begin = KeySelector.firstGreaterThan(tagSubspace.pack(Tuple.from(tag, versionstamp)))
        val end = KeySelector.firstGreaterThan(tagSubspace.range(Tuple.from(tag)).end)
        snapshot.getRange(begin, end)
      case None =>
        snapshot.getRange(tagSubspace.range(Tuple.from(tag)))
    }
    Stream.eval(transaction.snapshot).flatMap(query).map { kv =>
      val key = Tuple.fromBytes(kv.getKey)
      val value = Tuple.fromBytes(kv.getValue)
      (key.getVersionstamp(3),
       value.getString(0),
       value.getLong(1),
       value.getString(2),
       value.getBytes(3))
    }
  }

}
