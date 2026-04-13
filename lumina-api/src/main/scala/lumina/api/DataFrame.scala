package lumina.api

import lumina.plan.*

/** Minimal Pandas-inspired facade that records a logical plan for later execution. */
final class DataFrame private (val logicalPlan: LogicalPlan):

  /** Returns a new DataFrame with the additional filter predicate applied. */
  def filter(condition: Expression): DataFrame =
    DataFrame(Filter(logicalPlan, condition))

  /** Returns a new DataFrame projecting to the provided expressions. */
  def select(columns: Expression*): DataFrame =
    DataFrame(Project(logicalPlan, columns.toVector, schema = None))

  /** Groups rows by the supplied expressions and registers aggregations. */
  def groupBy(grouping: Seq[Expression], aggregations: Seq[Aggregation], schema: Option[Schema] = None): DataFrame =
    DataFrame(Aggregate(logicalPlan, grouping.toVector, aggregations.toVector, schema))

  /** Gives access to the underlying logical plan for advanced tooling. */
  def plan: LogicalPlan = logicalPlan

object DataFrame:
  def apply(plan: LogicalPlan): DataFrame = new DataFrame(plan)

object Lumina:
  import java.util.Optional

  def readCsv(path: String): DataFrame =
    readCsv(path, Option.empty)

  def readCsv(path: String, schema: Schema): DataFrame =
    readCsv(path, Some(schema))

  def readCsv(path: String, schema: Optional[Schema]): DataFrame =
    readCsv(path, Option(schema.orElse(null)))

  def readCsv(path: String, schema: Option[Schema]): DataFrame =
    DataFrame(ReadCsv(path, schema))
