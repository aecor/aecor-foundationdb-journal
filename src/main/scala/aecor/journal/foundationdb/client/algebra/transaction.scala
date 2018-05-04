package aecor.journal.foundationdb.client.algebra

import cats.data.ReaderT
import cats.~>
import com.apple.foundationdb.{KeySelector, KeyValue, MutationType, Range}
import fs2.Stream

object transaction {

  trait Transaction[F[_]] extends ReadTransaction[F] {
    def watch(key: Array[Byte]): F[F[Unit]]
    def set(key: Array[Byte], value: Array[Byte]): F[Unit]
    def clear(range: Range): F[Unit]
    def mutate(mutationType: MutationType, key: Array[Byte], value: Array[Byte]): F[Unit]
    def commit: F[Unit]
    def close: F[Unit]
    def snapshot: F[ReadTransaction[F]]
  }

  type TransactionIO[F[_], A] = ReaderT[F, Transaction[F], A]
  object TransactionIO {
    def apply[F[_], A](f: Transaction[F] => F[A]): TransactionIO[F, A] =
      ReaderT(f)
  }

  trait ReadTransaction[F[_]] { outer =>
    def getRange(range: Range, limit: Int, reverse: Boolean): Stream[F, KeyValue]

    def getKey(selector: KeySelector): F[Array[Byte]]

    def getRange(range: Range): Stream[F, KeyValue]

    def getRange(begin: KeySelector, end: KeySelector): Stream[F, KeyValue]

    def getRange(begin: Array[Byte], end: Array[Byte]): Stream[F, KeyValue]

    def mapK[G[_]](f: F ~> G): ReadTransaction[G] = new ReadTransaction[G] {
      override def getRange(range: Range, limit: Int, reverse: Boolean): Stream[G, KeyValue] =
        outer.getRange(range, limit, reverse).translate(f)

      override def getKey(selector: KeySelector): G[Array[Byte]] =
        f(outer.getKey(selector))

      override def getRange(range: Range): Stream[G, KeyValue] =
        outer.getRange(range).translate(f)

      override def getRange(begin: KeySelector, end: KeySelector): Stream[G, KeyValue] =
        outer.getRange(begin, end).translate(f)

      override def getRange(begin: Array[Byte], end: Array[Byte]): Stream[G, KeyValue] =
        outer.getRange(begin, end).translate(f)
    }
  }

  type ReadTransactionIO[F[_], A] = ReaderT[F, ReadTransaction[F], A]
  object ReadTransactionIO {
    def apply[F[_], A](f: ReadTransaction[F] => F[A]): ReadTransactionIO[F, A] = ReaderT(f)
    def toTransactionIOK[F[_]]: ReadTransactionIO[F, ?] ~> TransactionIO[F, ?] =
      new (ReadTransactionIO[F, ?] ~> TransactionIO[F, ?]) {
        override def apply[A](fa: ReadTransactionIO[F, A]): TransactionIO[F, A] =
          TransactionIO(fa.run)
      }
  }
}
