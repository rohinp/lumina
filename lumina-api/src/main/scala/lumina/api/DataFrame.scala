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

  /**
   * Adds or replaces a column by evaluating `expr` against each row.
   *
   * If `name` already exists in the result the old value is overwritten;
   * otherwise a new column is appended.
   */
  def withColumn(name: String, expr: Expression): DataFrame =
    DataFrame(WithColumn(logicalPlan, name, expr))

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

  /**
   * Executes the plan and prints the first `n` rows as a formatted ASCII table.
   *
   * {{{
   * df.show(backend)
   * // +--------+-----+---------+
   * // | city   | age | revenue |
   * // +--------+-----+---------+
   * // | Paris  |  35 | 1000.0  |
   * // | Berlin |  29 | 2000.0  |
   * // +--------+-----+---------+
   * // 2 rows
   * }}}
   */
  def show(backend: Backend, n: Int = 20): Unit =
    print(showString(backend, n))

  /** Returns the formatted table string that [[show]] prints. */
  def showString(backend: Backend, n: Int = 20): String =
    val rows    = collect(backend).take(n)
    if rows.isEmpty then
      "++ (empty)\n"
    else
      val cols    = rows.head.values.keys.toVector
      val data    = rows.map(r => cols.map(c => Option(r.values(c)).map(_.toString).getOrElse("null")))
      val widths  = cols.zipWithIndex.map { (col, i) =>
        math.max(col.length, data.map(_(i).length).maxOption.getOrElse(0))
      }
      val sep  = "+" + widths.map(w => "-" * (w + 2)).mkString("+") + "+"
      val header = "| " + cols.zipWithIndex.map { (c, i) => c.padTo(widths(i), ' ') }.mkString(" | ") + " |"
      val body   = data.map { row =>
        "| " + row.zipWithIndex.map { (v, i) => v.padTo(widths(i), ' ') }.mkString(" | ") + " |"
      }
      (Seq(sep, header, sep) ++ body ++ Seq(sep, s"${rows.size} row(s)\n")).mkString("\n")

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
