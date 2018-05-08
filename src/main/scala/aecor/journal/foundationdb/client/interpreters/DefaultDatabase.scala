package aecor.journal.foundationdb.client.interpreters

import aecor.journal.foundationdb.client.algebra.database.Database
import aecor.journal.foundationdb.client.algebra.transaction.Transaction
import cats.effect.ConcurrentEffect
import com.apple.foundationdb.{Database => Db}

final class DefaultDatabase[F[_]](db: Db)(implicit F: ConcurrentEffect[F]) extends Database[F] {
  override def createTransaction: F[Transaction[F]] =
    F.delay(new DefaultTransaction[F](db.createTransaction()))

  override def close: F[Unit] =
    F.delay(db.close())
}

object DefaultDatabase {
  def apply[F[_]](db: Db)(implicit F: ConcurrentEffect[F]): Database[F] =
    new DefaultDatabase[F](db)
}
