package lumina.api

import lumina.plan.*
import lumina.plan.backend.{Backend, BackendResult, Row}
import scala.jdk.CollectionConverters.*

/** Minimal Pandas-inspired facade that records a logical plan for later execution. */
final class DataFrame private (val logicalPlan: LogicalPlan):

  /** Returns a new DataFrame with the additional filter predicate applied. */
  def filter(condition: Expression): DataFrame =
    DataFrame(Filter(logicalPlan, condition))

  /** Returns a new DataFrame projecting to the provided expressions (Scala API). */
  def select(columns: Expression*): DataFrame =
    DataFrame(Project(logicalPlan, columns.toVector, schema = None))

  /** Returns a new DataFrame projecting to the provided expressions (Java/Kotlin API). */
  def select(columns: java.lang.Iterable[Expression]): DataFrame =
    DataFrame(Project(logicalPlan, columns.asScala.toVector, schema = None))

  /** Groups rows by the supplied expressions and registers aggregations (Scala API). */
  def groupBy(grouping: Seq[Expression], aggregations: Seq[Aggregation], schema: Option[Schema] = None): DataFrame =
    DataFrame(Aggregate(logicalPlan, grouping.toVector, aggregations.toVector, schema))

  /** Groups rows by the supplied expressions and registers aggregations (Java/Kotlin API). */
  def groupBy(grouping: java.lang.Iterable[Expression], aggregations: java.lang.Iterable[Aggregation]): DataFrame =
    groupBy(grouping.asScala.toSeq, aggregations.asScala.toSeq)

  /** Returns a new DataFrame sorted by the supplied sort expressions (Scala API). */
  def sort(sortExprs: SortExpr*): DataFrame =
    DataFrame(Sort(logicalPlan, sortExprs.toVector))

  /** Returns a new DataFrame sorted by the supplied sort expressions (Java/Kotlin API). */
  def sort(sortExprs: java.lang.Iterable[SortExpr]): DataFrame =
    DataFrame(Sort(logicalPlan, sortExprs.asScala.toVector))

  /** Returns a new DataFrame with at most `n` rows. */
  def limit(n: Int): DataFrame =
    DataFrame(Limit(logicalPlan, n))

  /** Joins this DataFrame with another using an inner join (Scala API). */
  def join(other: DataFrame, condition: Expression): DataFrame =
    DataFrame(Join(logicalPlan, other.logicalPlan, Some(condition), JoinType.Inner))

  /** Joins this DataFrame with another using the specified join type (Scala API). */
  def join(other: DataFrame, condition: Expression, joinType: JoinType): DataFrame =
    DataFrame(Join(logicalPlan, other.logicalPlan, Some(condition), joinType))

  /** Cross-joins this DataFrame with another (no condition). */
  def crossJoin(other: DataFrame): DataFrame =
    DataFrame(Join(logicalPlan, other.logicalPlan, None, JoinType.Inner))

  /** Executes this DataFrame's plan against the given backend and returns all result rows. */
  def collect(backend: Backend): Vector[Row] =
    backend.execute(logicalPlan) match
      case BackendResult.InMemory(rows) => rows

  /** Executes this DataFrame's plan and returns all result rows as a Java List. */
  def collectAsList(backend: Backend): java.util.List[Row] =
    collect(backend).asJava

  /**
   * Returns the logical plan rendered as a human-readable tree string.
   * Useful for understanding what Lumina will execute before calling collect.
   */
  def explainString: String = lumina.plan.LogicalPlanPrinter.explain(logicalPlan)

  /**
   * Prints the logical plan tree to standard output.
   * Mirrors Spark's df.explain() for familiarity.
   */
  def explain(): Unit = print(explainString)

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

  /** Convenience sort-expression builders. */
  def asc(expr: Expression): SortExpr  = SortExpr(expr, ascending = true)
  def desc(expr: Expression): SortExpr = SortExpr(expr, ascending = false)
