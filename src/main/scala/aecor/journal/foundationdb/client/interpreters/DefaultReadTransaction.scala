package aecor.journal.foundationdb.client.interpreters

import aecor.journal.foundationdb.client.algebra.transaction.ReadTransaction
import aecor.journal.foundationdb.client.interpreters.ConversionUtil._
import cats.effect._
import com.apple.foundationdb.{KeySelector, KeyValue, Range, ReadTransaction => ReadTx}
import fs2._

class DefaultReadTransaction[F[_]](tx: ReadTx)(implicit F: Concurrent[F])
    extends ReadTransaction[F] {
  def getRange(range: Range, limit: Int, reverse: Boolean): Stream[F, KeyValue] =
    Stream.suspend {
      streamFromAsyncIterable(tx.getRange(range, limit, reverse))
    }

  def getKey(selector: KeySelector): F[Array[Byte]] =
    deferFuture(tx.getKey(selector))

  def getRange(range: Range): Stream[F, KeyValue] =
    Stream.suspend {
      streamFromAsyncIterable(tx.getRange(range))
    }

  def getRange(begin: KeySelector, end: KeySelector): Stream[F, KeyValue] =
    Stream.suspend {
      streamFromAsyncIterable(tx.getRange(begin, end))
    }

  def getRange(begin: Array[Byte], end: Array[Byte]): Stream[F, KeyValue] =
    Stream.suspend {
      streamFromAsyncIterable(tx.getRange(begin, end))
    }
}
