package aecor.journal.foundationdb
import java.util.concurrent.{CancellationException, CompletableFuture, CompletionException}

import aecor.journal.foundationdb.CFU._
import cats.effect._
import cats.implicits._
import cats.~>
import com.apple.foundationdb.async.{AsyncIterable, AsyncIterator}
import com.apple.foundationdb.{
  KeySelector,
  KeyValue,
  MutationType,
  Range,
  Database => Db,
  ReadTransaction => ReadTx,
  Transaction => Tx
}
import fs2._

class ReadTransaction[F[_]](tx: ReadTx)(implicit F: Concurrent[F]) {
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

class Transaction[F[_]](tx: Tx)(implicit F: Concurrent[F])
    extends ReadTransaction[F](tx)
    with TransactionContext[F] {

  def watch(key: Array[Byte]): F[F[Unit]] =
    F.delay(deferFuture(tx.watch(key)).void)

  def set(key: Array[Byte], value: Array[Byte]): F[Unit] =
    F.delay(tx.set(key, value))

  def clear(range: Range): F[Unit] = F.delay(tx.clear(range))

  def mutate(mutationType: MutationType, key: Array[Byte], value: Array[Byte]): F[Unit] =
    F.delay(tx.mutate(mutationType, key, value))

  override def run[A](f: Transaction[F] => F[A]): F[A] = f(this)

  def commit: F[Unit] = deferFuture[F, Void](tx.commit()).void
  def close: F[Unit] = F.delay(tx.close())

  def snapshot: F[ReadTransaction[F]] =
    F.delay(new ReadTransaction[F](tx.snapshot()))
}

trait TransactionContext[F[_]] {
  def run[A](f: Transaction[F] => F[A]): F[A]
}

final class FoundationDB[F[_]: ConcurrentEffect](underlying: Db) extends TransactionContext[F] {
  override def run[A](f: Transaction[F] => F[A]): F[A] =
    createTransaction.flatMap { tx =>
      f(tx).flatTap(_ => tx.commit).onError { case _ => tx.close }.flatTap(_ => tx.close)
    }

  def createTransaction: F[Transaction[F]] =
    ConcurrentEffect[F].delay(new Transaction[F](underlying.createTransaction()))

  def runSR[A](stream: Stream[ReadTransactionIO[F, ?], A]): Stream[F, A] =
    Stream.force {
      createTransaction.map { tx =>
        val transactor = new (ReadTransactionIO[F, ?] ~> F) {
          override def apply[X](fa: ReadTransactionIO[F, X]): F[X] = fa.run(tx)
        }
        (stream.translate(transactor) ++ Stream.eval(tx.commit).drain).onFinalize(tx.close)
      }
    }

  def runS[A](stream: Stream[TransactionIO[F, ?], A]): Stream[F, A] =
    Stream.force {
      createTransaction.map { tx =>
        val transactor = new (TransactionIO[F, ?] ~> F) {
          override def apply[X](fa: TransactionIO[F, X]): F[X] = fa.run(tx)
        }
        (stream.translate(transactor) ++ Stream.eval(tx.commit).drain).onFinalize(tx.close)
      }
    }

}

object CFU {
  def streamFromAsyncIterable[F[_], A](iterable: AsyncIterable[A])(
      implicit F: Concurrent[F]): Stream[F, A] = Stream.suspend {
    val iterator = iterable.iterator()
    def getNext(i: AsyncIterator[A]): F[Option[(A, AsyncIterator[A])]] =
      deferFuture[F, java.lang.Boolean](i.onHasNext())
        .flatMap { b =>
          if (b) F.delay(i.next()).map(a => (a, i).some)
          else F.pure(None)
        }
    Stream.unfoldEval(iterator)(getNext)
  }
  def deferFuture[F[_], A](fa: => CompletableFuture[A])(implicit F: Concurrent[F]): F[A] =
    F.cancelable { cb =>
      fa.handle[Unit] { (result: A, err: Throwable) =>
        err match {
          case null =>
            cb(Right(result))
          case _: CancellationException =>
            ()
          case ex: CompletionException if ex.getCause ne null =>
            cb(Left(ex.getCause))
          case ex =>
            cb(Left(ex))
        }
      }
      IO(fa.cancel(true)).void
    }

  def unsafeRun[F[_], A](fa: F[A])(implicit F: Effect[F]): CompletableFuture[A] = {
    val cf = new CompletableFuture[A]
    F.runAsync(fa) {
        case Right(a) => IO(cf.complete(a)).void
        case Left(e)  => IO(cf.completeExceptionally(e)).void
      }
      .unsafeRunSync()
    cf
  }
}
