package aecor.journal.foundationdb.client

import aecor.journal.foundationdb.RTIO
import aecor.journal.foundationdb.client.algebra.database.Database
import aecor.journal.foundationdb.client.algebra.transaction.{
  ReadTransactionIO,
  Transaction,
  TransactionIO
}
import cats.arrow.FunctionK
import cats.effect.ConcurrentEffect
import cats.effect.ExitCase.Completed
import cats.implicits._
import cats.~>
import fs2.Stream

final class Transactor[F[_]](db: Database[F])(implicit F: ConcurrentEffect[F]) {
  def transact[A](fa: TransactionIO[F, A]): F[A] =
    prepareTransact(fa)(FunctionK.id)

  def transactRead[A](fa: ReadTransactionIO[F, A]): F[A] =
    prepareTransact(fa)(RTIO.toTransactionIOK)

  def transactReadStream[A](stream: Stream[ReadTransactionIO[F, ?], A]): Stream[F, A] =
    prepareTransactStream(stream)(RTIO.toTransactionIOK)

  def transactStream[A](stream: Stream[TransactionIO[F, ?], A]): Stream[F, A] =
    prepareTransactStream(stream)(FunctionK.id)

  private def prepareTransact[G[_], A](ga: G[A])(f: G ~> TransactionIO[F, ?]): F[A] =
    F.bracketCase(db.createTransaction)(f(ga).run) { (tx, exitcase) =>
      exitcase match {
        case Completed => tx.commit >> tx.close
        case _         => tx.close
      }
    }

  private def prepareTransactStream[G[_], A](stream: Stream[G, A])(
      f: G ~> TransactionIO[F, ?]): Stream[F, A] = {
    def translator(tx: Transaction[F]) = new (G ~> F) {
      override def apply[X](fa: G[X]): F[X] = f(fa).run(tx)
    }
    Stream.bracket(db.createTransaction)(
      tx => stream.translate(translator(tx)) ++ Stream.eval(tx.commit).drain,
      _.close
    )
  }
}
