package aecor.journal

import aecor.journal.foundationdb.client.Transactor
import aecor.journal.foundationdb.client.algebra.transaction.{ReadTransactionIO, TransactionIO}
import cats.FlatMap
import fs2.Stream

package object foundationdb {
  implicit final class ReadTransactionIOOps[F[_], A](val self: ReadTransactionIO[F, A])
      extends AnyVal {
    def asTransactionIO: TransactionIO[F, A] = TransactionIO(self.run)
    def transact(foundationDB: Transactor[F]): F[A] = foundationDB.transactRead(self)
    def usingSnapshot(implicit F: FlatMap[F]): TransactionIO[F, A] = TIO.usingSnapshot(self)
  }

  implicit final class TransactionIOOps[F[_], A](val self: TransactionIO[F, A]) extends AnyVal {
    def transact(foundationDB: Transactor[F]): F[A] = foundationDB.transact(self)
  }

  implicit final class ReadTransactionIOStreamOps[F[_], A](
      val self: Stream[ReadTransactionIO[F, ?], A]) {
    def transact(foundationDB: Transactor[F]): Stream[F, A] = foundationDB.transactReadStream(self)
    def usingSnapshot(implicit F: FlatMap[F]): Stream[TransactionIO[F, ?], A] =
      TIO.usingSnapshot(self)
  }

  implicit final class TransactionIOStreamOps[F[_], A](val self: Stream[TransactionIO[F, ?], A]) {
    def transact(foundationDB: Transactor[F]): Stream[F, A] = foundationDB.transactStream(self)
  }

  val TIO = TransactionIO
  val RTIO = ReadTransactionIO
}
