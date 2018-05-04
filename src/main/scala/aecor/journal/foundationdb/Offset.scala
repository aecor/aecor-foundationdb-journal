package aecor.journal.foundationdb

import com.apple.foundationdb.tuple.Versionstamp

final case class Offset(value: Option[Versionstamp]) extends AnyVal {
  def increment: Offset = Offset.zero
}
object Offset {
  def zero: Offset = Offset(Option.empty)
}
