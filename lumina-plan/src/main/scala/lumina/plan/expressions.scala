package lumina.plan

sealed trait Expression

object Expression:
  final case class ColumnRef(name: String) extends Expression
  final case class Literal(value: Any) extends Expression
  final case class GreaterThan(left: Expression, right: Expression) extends Expression
  final case class EqualTo(left: Expression, right: Expression) extends Expression

sealed trait Aggregation

object Aggregation:
  final case class Sum(column: Expression, alias: Option[String] = None) extends Aggregation
  final case class Count(column: Option[Expression], alias: Option[String] = None) extends Aggregation
