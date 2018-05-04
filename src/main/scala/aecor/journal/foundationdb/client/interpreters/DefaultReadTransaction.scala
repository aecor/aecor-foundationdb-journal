package aecor.journal.foundationdb.client.interpreters

import aecor.journal.foundationdb.client.algebra.transaction.ReadTransaction
import aecor.journal.foundationdb.client.interpreters.ConversionUtil._
import cats.effect._
import com.apple.foundationdb.{KeySelector, KeyValue, Range, ReadTransaction => ReadTx}
import fs2._

class DefaultReadTransaction[F[_]: Concurrent](tx: ReadTx) extends ReadTransaction[F] {

  final def getRange(range: Range, limit: Int, reverse: Boolean): Stream[F, KeyValue] =
    streamFromAsyncIterable(tx.getRange(range, limit, reverse))

  final def getKey(selector: KeySelector): F[Array[Byte]] =
    deferCompletableFuture(tx.getKey(selector))

  final def getRange(range: Range): Stream[F, KeyValue] =
    streamFromAsyncIterable(tx.getRange(range))

  final def getRange(begin: KeySelector, end: KeySelector): Stream[F, KeyValue] =
    streamFromAsyncIterable(tx.getRange(begin, end))

  final def getRange(begin: Array[Byte], end: Array[Byte]): Stream[F, KeyValue] =
    streamFromAsyncIterable(tx.getRange(begin, end))

}
