package aecor.journal.foundationdb.client.interpreters

import aecor.journal.foundationdb.client.algebra.transaction
import aecor.journal.foundationdb.client.algebra.transaction.Transaction
import cats.data.ReaderT
import cats.effect.Sync
import cats.implicits._
import com.apple.foundationdb
import com.apple.foundationdb.{KeySelector, KeyValue, MutationType}

object ReaderTransaction {
  def apply[F[_]: Sync]: Transaction[ReaderT[F, Transaction[F], ?]] =
    new ReaderTransaction[F]
}

final class ReaderTransaction[F[_]: Sync] extends Transaction[ReaderT[F, Transaction[F], ?]] {

  private val readerTransaction = new ReaderReadTransaction[F, Transaction[F]]

  override def watch(
      key: Array[Byte]): ReaderT[F, Transaction[F], ReaderT[F, Transaction[F], Unit]] =
    ReaderT(_.watch(key).map(x => ReaderT(_ => x)))

  override def set(key: Array[Byte], value: Array[Byte]): ReaderT[F, Transaction[F], Unit] =
    ReaderT(_.set(key, value))

  override def clear(range: foundationdb.Range): ReaderT[F, Transaction[F], Unit] =
    ReaderT(_.clear(range))

  override def mutate(mutationType: MutationType,
                      key: Array[Byte],
                      value: Array[Byte]): ReaderT[F, Transaction[F], Unit] =
    ReaderT(_.mutate(mutationType, key, value))

  override def commit: ReaderT[F, Transaction[F], Unit] =
    ReaderT(_.commit)

  override def close: ReaderT[F, Transaction[F], Unit] =
    ReaderT(_.close)

  override def snapshot
    : ReaderT[F, Transaction[F], transaction.ReadTransaction[ReaderT[F, Transaction[F], ?]]] =
    ReaderT(_.snapshot.map(_.mapK(ReaderT.liftK)))

  override def getRange(range: foundationdb.Range,
                        limit: Int,
                        reverse: Boolean): fs2.Stream[ReaderT[F, Transaction[F], ?], KeyValue] =
    readerTransaction.getRange(range, limit, reverse)

  override def getKey(selector: KeySelector): ReaderT[F, Transaction[F], Array[Byte]] =
    readerTransaction.getKey(selector)

  override def getRange(
      range: foundationdb.Range): fs2.Stream[ReaderT[F, Transaction[F], ?], KeyValue] =
    readerTransaction.getRange(range)

  override def getRange(begin: KeySelector,
                        end: KeySelector): fs2.Stream[ReaderT[F, Transaction[F], ?], KeyValue] =
    readerTransaction.getRange(begin, end)

  override def getRange(begin: Array[Byte],
                        end: Array[Byte]): fs2.Stream[ReaderT[F, Transaction[F], ?], KeyValue] =
    readerTransaction.getRange(begin, end)
}
