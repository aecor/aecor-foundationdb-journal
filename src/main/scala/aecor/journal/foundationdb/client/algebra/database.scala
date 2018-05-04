package aecor.journal.foundationdb.client.algebra

import aecor.journal.foundationdb.client.algebra.transaction.Transaction

object database {
  trait Database[F[_]] {
    def createTransaction: F[Transaction[F]]
    def close: F[Unit]
  }
}
