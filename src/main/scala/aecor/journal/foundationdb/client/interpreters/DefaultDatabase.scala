package aecor.journal.foundationdb.client.interpreters

import aecor.journal.foundationdb.client.algebra.database.Database
import aecor.journal.foundationdb.client.algebra.transaction.Transaction
import cats.effect.ConcurrentEffect
import com.apple.foundationdb.{Database => Db}

class DefaultDatabase[F[_]](db: Db)(implicit F: ConcurrentEffect[F]) extends Database[F] {
  def createTransaction: F[Transaction[F]] =
    F.delay(new DefaultTransaction[F](db.createTransaction()))

  override def close: F[Unit] =
    F.delay(db.close())
}
