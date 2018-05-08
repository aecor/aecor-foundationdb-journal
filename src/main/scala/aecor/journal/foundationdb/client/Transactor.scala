package aecor.journal.foundationdb.client

import java.net.InetSocketAddress
import java.util.UUID

import aecor.journal.foundationdb.client.algebra.database.Database
import aecor.journal.foundationdb.client.algebra.transaction.{
  ReadTransactionIO,
  Transaction,
  TransactionIO
}
import aecor.journal.foundationdb.client.interpreters.DefaultDatabase
import cats.arrow.FunctionK
import cats.effect.ConcurrentEffect
import cats.effect.ExitCase.Completed
import cats.implicits._
import cats.~>
import com.apple.foundationdb.FDB
import fs2.Stream

final class Transactor[F[_]](db: Database[F])(implicit F: ConcurrentEffect[F]) {
  def transact[A](fa: TransactionIO[F, A]): F[A] =
    prepareTransact(fa)(FunctionK.id)

  def transactRead[A](fa: ReadTransactionIO[F, A]): F[A] =
    prepareTransact(fa)(ReadTransactionIO.toTransactionIOK)

  def transactReadStream[A](stream: Stream[ReadTransactionIO[F, ?], A]): Stream[F, A] =
    prepareTransactStream(stream)(ReadTransactionIO.toTransactionIOK)

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
    def gf(tx: Transaction[F]) = new (G ~> F) {
      override def apply[X](fa: G[X]): F[X] = f(fa).run(tx)
    }
    Stream.bracket(db.createTransaction)(
      tx => stream.translate(gf(tx)) ++ Stream.eval(tx.commit).drain,
      _.close
    )
  }
  def close: F[Unit] = db.close
}

object Transactor {
  def create[F[_]](description: String, id: String, nodes: Set[InetSocketAddress])(
      implicit F: ConcurrentEffect[F]): F[Transactor[F]] =
    F.delay {
      val instanceId = UUID.randomUUID().toString
      val clusterFileName = s"$instanceId.fdb.cluster"
      import java.io._
      val pw = new PrintWriter(new File(clusterFileName))
      pw.write(
        s"$description:$id@${nodes.map(x => s"${x.getHostString}:${x.getPort}").mkString(",")}")
      pw.close()
      val fdb = DefaultDatabase(FDB.selectAPIVersion(510).open(clusterFileName))
      new Transactor[F](fdb)
    }
  def create[F[_]](implicit F: ConcurrentEffect[F]): F[Transactor[F]] =
    F.delay {
      val fdb = DefaultDatabase(FDB.selectAPIVersion(510).open())
      new Transactor[F](fdb)
    }
}
