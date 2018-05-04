package aecor.journal.foundationdb.client.algebra

import cats.data.Kleisli
import cats.effect.Sync
import cats.implicits._
import cats.{Applicative, FlatMap, Functor, MonadError, ~>}
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

  type TransactionIO[F[_], A] = Kleisli[F, Transaction[F], A]

  object TransactionIO {
    def apply[F[_], A](f: Transaction[F] => F[A]): TransactionIO[F, A] =
      Kleisli(f)

    def pure[F[_]: Applicative, A](a: A): TransactionIO[F, A] = TransactionIO(_ => a.pure[F])
    def liftF[F[_], A](fa: F[A]): TransactionIO[F, A] = TransactionIO(_ => fa)

    def watch[F[_]: Functor](key: Array[Byte]): TransactionIO[F, TransactionIO[F, Unit]] =
      TransactionIO(_.watch(key).map(TransactionIO.liftF))

    def suspend[F[_], A](a: => TransactionIO[F, A])(implicit F: Sync[F]): TransactionIO[F, A] =
      TransactionIO(tx => F.suspend(a.run(tx)))

    def fromEither[F[_], A](either: Either[Throwable, A])(
        implicit F: MonadError[F, Throwable]): TransactionIO[F, A] =
      TransactionIO(_ => F.fromEither[A](either))

    def raiseError[F[_], A](e: Throwable)(
        implicit F: MonadError[F, Throwable]): TransactionIO[F, A] =
      TransactionIO(_ => F.raiseError[A](e))

    def usingSnapshot[F[_]: FlatMap, A](
        readTransactional: ReadTransactionIO[F, A]): TransactionIO[F, A] =
      TransactionIO(_.snapshot.flatMap(readTransactional.run))

    def usingSnapshot[F[_]: FlatMap, A](
        stream: Stream[ReadTransactionIO[F, ?], A]): Stream[TransactionIO[F, ?], A] =
      Stream.force[TransactionIO[F, ?], A] {
        TransactionIO { tx =>
          tx.snapshot.map { ss =>
            stream
              .translate(new (ReadTransactionIO[F, ?] ~> TransactionIO[F, ?]) {
                override def apply[X](fa: ReadTransactionIO[F, X]): TransactionIO[F, X] =
                  TransactionIO.liftF(fa.run(ss))
              })
          }
        }
      }

    def set[F[_]](key: Array[Byte], value: Array[Byte]): TransactionIO[F, Unit] =
      Kleisli(_.set(key, value))

    def mutate[F[_]](mutationType: MutationType,
                     key: Array[Byte],
                     value: Array[Byte]): TransactionIO[F, Unit] =
      TransactionIO(_.mutate(mutationType, key, value))
  }

  trait ReadTransaction[F[_]] {
    def getRange(range: Range, limit: Int, reverse: Boolean): Stream[F, KeyValue]

    def getKey(selector: KeySelector): F[Array[Byte]]

    def getRange(range: Range): Stream[F, KeyValue]

    def getRange(begin: KeySelector, end: KeySelector): Stream[F, KeyValue]

    def getRange(begin: Array[Byte], end: Array[Byte]): Stream[F, KeyValue]
  }

  type ReadTransactionIO[F[_], A] = Kleisli[F, ReadTransaction[F], A]

  object ReadTransactionIO {
    def toTransactionIOK[F[_]]: ReadTransactionIO[F, ?] ~> TransactionIO[F, ?] =
      new (ReadTransactionIO[F, ?] ~> TransactionIO[F, ?]) {
        override def apply[A](fa: ReadTransactionIO[F, A]): TransactionIO[F, A] =
          TransactionIO(fa.run)
      }
    def fromEither[F[_], A](either: Either[Throwable, A])(
        implicit F: MonadError[F, Throwable]): ReadTransactionIO[F, A] =
      ReadTransactionIO(_ => F.fromEither[A](either))

    def lift[F[_]]: F ~> ReadTransactionIO[F, ?] =
      new (F ~> ReadTransactionIO[F, ?]) {
        override def apply[A](fa: F[A]): ReadTransactionIO[F, A] =
          Kleisli.liftF(fa)
      }
    def apply[F[_], A](f: ReadTransaction[F] => F[A]): ReadTransactionIO[F, A] =
      Kleisli(f)

    def getKey[F[_]](selector: KeySelector): ReadTransactionIO[F, Array[Byte]] =
      ReadTransactionIO(_.getKey(selector))

    def getRange[F[_]](range: Range, limit: Int, reverse: Boolean)(
        implicit F: Applicative[F]): Stream[ReadTransactionIO[F, ?], KeyValue] =
      Stream.force[ReadTransactionIO[F, ?], KeyValue] {
        ReadTransactionIO(
          rtx =>
            rtx
              .getRange(range, limit, reverse)
              .translate(lift[F])
              .pure[F])
      }

    def getRange[F[_]](begin: KeySelector, end: KeySelector)(
        implicit F: Applicative[F]): Stream[ReadTransactionIO[F, ?], KeyValue] =
      Stream.force[ReadTransactionIO[F, ?], KeyValue] {
        ReadTransactionIO(
          rtx =>
            rtx
              .getRange(begin, end)
              .translate(lift[F])
              .pure[F])
      }

    def getRange[F[_]](begin: Array[Byte], end: Array[Byte])(
        implicit F: Applicative[F]): Stream[ReadTransactionIO[F, ?], KeyValue] =
      Stream.force[ReadTransactionIO[F, ?], KeyValue] {
        ReadTransactionIO(
          rtx =>
            rtx
              .getRange(begin, end)
              .translate(lift[F])
              .pure[F])
      }

    def getRange[F[_]](range: Range)(
        implicit F: Applicative[F]): Stream[ReadTransactionIO[F, ?], KeyValue] =
      Stream.force[ReadTransactionIO[F, ?], KeyValue] {
        ReadTransactionIO(
          rtx =>
            rtx
              .getRange(range)
              .translate(lift[F])
              .pure[F])
      }
  }
}
