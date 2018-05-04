package aecor.journal.foundationdb.client.interpreters

import aecor.journal.foundationdb.client.algebra.transaction.{ReadTransaction, Transaction}
import aecor.journal.foundationdb.client.interpreters.ConversionUtil._
import cats.effect._
import cats.implicits._
import com.apple.foundationdb.{MutationType, Range, Transaction => Tx}

final class DefaultTransaction[F[_]](tx: Tx)(implicit F: Concurrent[F])
    extends DefaultReadTransaction[F](tx)
    with Transaction[F] {

  def watch(key: Array[Byte]): F[F[Unit]] =
    F.delay(deferCompletableFuture(tx.watch(key)).void)

  def set(key: Array[Byte], value: Array[Byte]): F[Unit] =
    F.delay(tx.set(key, value))

  def clear(range: Range): F[Unit] = F.delay(tx.clear(range))

  def mutate(mutationType: MutationType, key: Array[Byte], value: Array[Byte]): F[Unit] =
    F.delay(tx.mutate(mutationType, key, value))

  def commit: F[Unit] = deferCompletableFuture[F, Void](tx.commit()).void

  def close: F[Unit] = F.delay(tx.close())

  def snapshot: F[ReadTransaction[F]] =
    F.delay(new DefaultReadTransaction[F](tx.snapshot()))
}
