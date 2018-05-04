package aecor.journal.foundationdb.client.interpreters

import java.util.concurrent.{CancellationException, CompletableFuture, CompletionException}

import cats.effect._
import cats.implicits._
import com.apple.foundationdb.async.{AsyncIterable, AsyncIterator}
import fs2._

object ConversionUtil {
  def streamFromAsyncIterable[F[_], A](iterable: AsyncIterable[A])(
      implicit F: Concurrent[F]): Stream[F, A] = Stream.suspend {
    val iterator = iterable.iterator()
    def getNext(i: AsyncIterator[A]): F[Option[(A, AsyncIterator[A])]] =
      deferCompletableFuture[F, java.lang.Boolean](i.onHasNext())
        .flatMap { b =>
          if (b) F.delay(i.next()).map(a => (a, i).some)
          else F.pure(None)
        }
    Stream.unfoldEval(iterator)(getNext)
  }
  def deferCompletableFuture[F[_], A](fa: => CompletableFuture[A])(
      implicit F: Concurrent[F]): F[A] =
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
}
