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

  // Java/Kotlin-friendly factories — avoid exposing Option in the external API.

  // Java/Kotlin-friendly factories — return Aggregation (not the subtype) so
  // java.util.List.of(Aggregation.sum(...)) infers List<Aggregation> in Kotlin.

  /** Sum a column, giving the result a specific alias. */
  def sum(column: Expression, alias: String): Aggregation = Sum(column, Some(alias))

  /** Sum a column with no alias (output column name defaults to the column name). */
  def sum(column: Expression): Aggregation = Sum(column, None)

  /** Count all rows in each group, giving the result a specific alias. */
  def countAll(alias: String): Aggregation = Count(None, Some(alias))

  /** Count non-null values of a column in each group, giving the result a specific alias. */
  def count(column: Expression, alias: String): Aggregation = Count(Some(column), Some(alias))
