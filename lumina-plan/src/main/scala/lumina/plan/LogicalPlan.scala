package lumina.plan

/** Base trait for all logical plan nodes. */
sealed trait LogicalPlan:
  def children: Seq[LogicalPlan]
  def outputSchema: Option[Schema]

final case class ReadCsv(path: String, schema: Option[Schema]) extends LogicalPlan:
  override val children: Seq[LogicalPlan] = Seq.empty
  override val outputSchema: Option[Schema] = schema

final case class Project(child: LogicalPlan, columns: Vector[Expression], schema: Option[Schema])
    extends LogicalPlan:
  override val children: Seq[LogicalPlan] = Seq(child)
  override val outputSchema: Option[Schema] = schema.orElse(child.outputSchema)

final case class Filter(child: LogicalPlan, condition: Expression) extends LogicalPlan:
  override val children: Seq[LogicalPlan] = Seq(child)
  override val outputSchema: Option[Schema] = child.outputSchema

final case class Aggregate(
    child: LogicalPlan,
    groupBy: Vector[Expression],
    aggregations: Vector[Aggregation],
    schema: Option[Schema]
) extends LogicalPlan:
  override val children: Seq[LogicalPlan] = Seq(child)
  override val outputSchema: Option[Schema] = schema
