package aecor.journal.foundationdb.client.interpreters

import aecor.journal.foundationdb.client.algebra.transaction.ReadTransaction
import cats.Applicative
import cats.data.ReaderT
import com.apple.foundationdb
import com.apple.foundationdb.{KeySelector, KeyValue}
import fs2._
import cats.implicits._

object ReaderReadTransaction {
  def apply[F[_]: Applicative, RT <: ReadTransaction[F]]: ReadTransaction[ReaderT[F, RT, ?]] =
    new ReaderReadTransaction[F, RT]
}

final class ReaderReadTransaction[F[_]: Applicative, RT <: ReadTransaction[F]]
    extends ReadTransaction[ReaderT[F, RT, ?]] {

  def liftStream[A](f: RT => Stream[F, A]): Stream[ReaderT[F, RT, ?], A] =
    Stream.eval(ReaderT(f(_: RT).pure[F])).flatMap(_.translate(ReaderT.liftK[F, RT]))

  override def getRange(range: foundationdb.Range,
                        limit: Int,
                        reverse: Boolean): fs2.Stream[ReaderT[F, RT, ?], KeyValue] =
    liftStream(_.getRange(range, limit, reverse))

  override def getKey(selector: KeySelector): ReaderT[F, RT, Array[Byte]] =
    ReaderT(_.getKey(selector))

  override def getRange(range: foundationdb.Range): fs2.Stream[ReaderT[F, RT, ?], KeyValue] =
    liftStream(_.getRange(range))

  override def getRange(begin: KeySelector,
                        end: KeySelector): fs2.Stream[ReaderT[F, RT, ?], KeyValue] =
    liftStream(_.getRange(begin, end))

  override def getRange(begin: Array[Byte],
                        end: Array[Byte]): fs2.Stream[ReaderT[F, RT, ?], KeyValue] =
    liftStream(_.getRange(begin, end))
}
