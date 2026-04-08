package lumina.plan

/** Simple fluent builder used by tests and higher-level APIs to compose logical plans. */
final case class PlanBuilder(plan: LogicalPlan):
  def project(columns: Expression*): PlanBuilder =
    PlanBuilder(Project(plan, columns.toVector, schema = None))

  def filter(condition: Expression): PlanBuilder =
    PlanBuilder(Filter(plan, condition))

  def groupBy(grouping: Seq[Expression], aggregations: Seq[Aggregation], schema: Option[Schema] = None): PlanBuilder =
    PlanBuilder(Aggregate(plan, grouping.toVector, aggregations.toVector, schema))

object PlanBuilder:
  def readCsv(path: String, schema: Option[Schema] = None): PlanBuilder =
    PlanBuilder(ReadCsv(path, schema))
