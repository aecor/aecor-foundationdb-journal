package aecor.journal.foundationdb.client

import aecor.journal.foundationdb.client.algebra.transaction.{
  ReadTransactionIO,
  Transaction,
  TransactionIO
}
import cats.data.Kleisli
import cats.{FlatMap, ~>}
import fs2.Stream
import cats.implicits._

object syntax {
  implicit final class ReadTransactionIOOps[F[_], A](val self: ReadTransactionIO[F, A])
      extends AnyVal {
    def transact(foundationDB: Transactor[F]): F[A] = foundationDB.transactRead(self)
  }

  implicit final class TransactionIOOps[F[_], A](val self: TransactionIO[F, A]) extends AnyVal {
    def transact(foundationDB: Transactor[F]): F[A] = foundationDB.transact(self)
  }

  implicit final class ReadTransactionIOStreamOps[F[_], A](
      val self: Stream[ReadTransactionIO[F, ?], A]) {
    def transact(foundationDB: Transactor[F]): Stream[F, A] = foundationDB.transactReadStream(self)
    def usingSnapshot(implicit F: FlatMap[F]): Stream[TransactionIO[F, ?], A] =
      Stream.force {
        Kleisli { x: Transaction[F] =>
          x.snapshot.map { tx =>
            self.translate(new (ReadTransactionIO[F, ?] ~> TransactionIO[F, ?]) {
              override def apply[X](fa: ReadTransactionIO[F, X]): TransactionIO[F, X] =
                Kleisli(_ => fa.run(tx))
            })
          }
        }
      }
  }

  implicit final class TransactionIOStreamOps[F[_], A](val self: Stream[TransactionIO[F, ?], A]) {
    def transact(foundationDB: Transactor[F]): Stream[F, A] = foundationDB.transactStream(self)
  }
}
