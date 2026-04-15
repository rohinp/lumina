package lumina.api

import lumina.plan.*
import lumina.plan.Expression.*
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

  /**
   * Appends one or more window-function columns to each row without changing
   * the row count.  Each [[WindowExpr]] contributes exactly one output column
   * (named by its `alias` field).
   *
   * Scala API — pass expressions as varargs:
   * {{{
   *   df.window(
   *     WindowExpr.RowNumber("rn", WindowSpec(partitionBy = Vector(col("city")), orderBy = Vector(Lumina.asc(col("revenue")))))
   *   )
   * }}}
   */
  def window(windowExprs: WindowExpr*): DataFrame =
    DataFrame(Window(logicalPlan, windowExprs.toVector))

  /** Java/Kotlin API — pass expressions as an Iterable. */
  def window(windowExprs: java.lang.Iterable[WindowExpr]): DataFrame =
    DataFrame(Window(logicalPlan, windowExprs.asScala.toVector))

  /**
   * Returns rows that appear in both this DataFrame and `other`, with
   * duplicates removed.  Both DataFrames must have the same column names.
   */
  def intersect(other: DataFrame): DataFrame =
    DataFrame(Intersect(logicalPlan, other.logicalPlan))

  /**
   * Returns rows from this DataFrame that do not appear in `other`, with
   * duplicates removed.  Both DataFrames must have the same column names.
   */
  def except(other: DataFrame): DataFrame =
    DataFrame(Except(logicalPlan, other.logicalPlan))

  /**
   * Concatenates all rows from this DataFrame and `other` without removing
   * duplicates.  Both DataFrames must have the same column names.
   */
  def unionAll(other: DataFrame): DataFrame =
    DataFrame(UnionAll(logicalPlan, other.logicalPlan))

  /**
   * Returns a new DataFrame with duplicate rows removed.
   *
   * Two rows are considered duplicates when all column values are equal.
   */
  def distinct(): DataFrame =
    DataFrame(Distinct(logicalPlan))

  /**
   * Returns a randomly sampled subset of rows.
   *
   * `fraction` must be in [0.0, 1.0].  Each row is included independently with
   * probability `fraction` (Bernoulli sampling).  Provide `seed` for
   * reproducible results.
   */
  def sample(fraction: Double, seed: Option[Long] = None): DataFrame =
    DataFrame(Sample(logicalPlan, fraction, seed))

  // ---------------------------------------------------------------------------
  // Execution shortcuts
  // ---------------------------------------------------------------------------

  /**
   * Executes the plan and returns the total number of rows.
   *
   * Pushes a COUNT(*) aggregate down to the backend rather than materialising
   * all rows in the JVM.
   */
  def count(backend: Backend): Long =
    val countPlan = Aggregate(logicalPlan, Vector.empty, Vector(Aggregation.Count(None, Some("_count"))), None)
    backend.execute(countPlan) match
      case BackendResult.InMemory(rows) =>
        rows.headOption.map(_.values("_count") match
          case n: Long => n
          case n: Int  => n.toLong
          case n: java.lang.Long    => n.longValue()
          case n: java.lang.Integer => n.longValue()
          case other => other.toString.toLong
        ).getOrElse(0L)

  /**
   * Executes the plan and returns at most the first `n` rows.
   *
   * Equivalent to `limit(n).collect(backend)` but named to match Spark / Pandas
   * conventions.
   */
  def head(n: Int, backend: Backend): Vector[Row] =
    limit(n).collect(backend)

  /** Returns true when the plan produces no rows. */
  def isEmpty(backend: Backend): Boolean =
    head(1, backend).isEmpty

  /** Returns true when the plan produces at least one row. */
  def nonEmpty(backend: Backend): Boolean =
    !isEmpty(backend)

  /**
   * Drops one or more columns by name.  Columns that do not exist are silently
   * ignored, matching Spark's behaviour.
   */
  def drop(cols: String*): DataFrame =
    DataFrame(DropColumns(logicalPlan, cols.toVector))

  /** Java/Kotlin API — pass column names as an Iterable. */
  def drop(cols: java.lang.Iterable[String]): DataFrame =
    import scala.jdk.CollectionConverters.*
    DataFrame(DropColumns(logicalPlan, cols.asScala.toVector))

  /**
   * Returns a new DataFrame with the column `oldName` renamed to `newName`.
   * If `oldName` does not exist the DataFrame is returned unchanged.
   */
  def withColumnRenamed(oldName: String, newName: String): DataFrame =
    DataFrame(RenameColumn(logicalPlan, oldName, newName))

  /**
   * Drops rows that contain a null value in any of the specified columns.
   * When no columns are specified, drops rows that contain a null in *any*
   * column — but since schema is not always available this overload requires
   * explicit column names.
   *
   * {{{
   *   df.dropNa("email", "phone")   // drop rows where email OR phone is null
   * }}}
   */
  def dropNa(cols: String*): DataFrame =
    require(cols.nonEmpty, "dropNa requires at least one column name")
    val condition = cols
      .map(c => IsNotNull(ColumnRef(c)): Expression)
      .reduce(And(_, _))
    DataFrame(Filter(logicalPlan, condition))

  /**
   * Replaces null values in the specified columns with `value`.
   * The fill value is coerced to match each column's runtime type via
   * [[Coalesce]].
   *
   * {{{
   *   df.fillNa(0, "revenue", "cost")   // replace nulls with 0 in those two columns
   * }}}
   */
  def fillNa(value: Any, cols: String*): DataFrame =
    require(cols.nonEmpty, "fillNa requires at least one column name")
    cols.foldLeft(this) { (df, col) =>
      df.withColumn(col, Coalesce(Vector(ColumnRef(col), Literal(value))))
    }

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

  // ---------------------------------------------------------------------------
  // Export / write
  // ---------------------------------------------------------------------------

  /**
   * Returns the result as a CSV-formatted string.
   *
   * Column names appear as the first row when `includeHeader` is true (the
   * default).  Values are CSV-escaped: fields that contain commas, double-
   * quotes, or newlines are wrapped in double-quotes and embedded quotes are
   * doubled.
   */
  def toCsvString(backend: Backend, includeHeader: Boolean = true): String =
    val rows = collect(backend)
    if rows.isEmpty then return if includeHeader then "" else ""
    val cols   = rows.head.values.keys.toVector
    val header = if includeHeader then cols.map(csvEscape).mkString(",") + "\n" else ""
    val body   = rows.map { row =>
      cols.map(c => csvEscape(Option(row.values(c)).map(_.toString).getOrElse(""))).mkString(",")
    }.mkString("\n")
    header + body

  /**
   * Executes the plan and writes the result to `path` as a UTF-8 CSV file.
   *
   * The file is overwritten if it already exists.  See [[toCsvString]] for
   * formatting details.
   */
  def writeCsv(path: String, backend: Backend, includeHeader: Boolean = true): Unit =
    val content = toCsvString(backend, includeHeader)
    java.nio.file.Files.writeString(java.nio.file.Paths.get(path), content,
      java.nio.charset.StandardCharsets.UTF_8)

  /**
   * Returns the result as a newline-delimited JSON string (one JSON object per
   * row).  Values are encoded as JSON strings, numbers, booleans, or `null`.
   *
   * {{{
   *   {"city":"Paris","revenue":1000.0}
   *   {"city":"Berlin","revenue":2000.0}
   * }}}
   */
  def toJsonLines(backend: Backend): String =
    collect(backend).map(rowToJson).mkString("\n")

  // ---------------------------------------------------------------------------
  // Internal helpers
  // ---------------------------------------------------------------------------

  private def csvEscape(value: String): String =
    if value.exists(c => c == ',' || c == '"' || c == '\n' || c == '\r') then
      "\"" + value.replace("\"", "\"\"") + "\""
    else value

  private def rowToJson(row: Row): String =
    val fields = row.values.map { case (k, v) =>
      val key   = "\"" + k.replace("\"", "\\\"") + "\""
      val value = v match
        case null        => "null"
        case b: Boolean  => b.toString
        case n: Int      => n.toString
        case n: Long     => n.toString
        case n: Double   => n.toString
        case n: Float    => n.toString
        case n: java.lang.Number => n.toString
        case s           => "\"" + s.toString.replace("\\", "\\\\").replace("\"", "\\\"") + "\""
      s"$key:$value"
    }.mkString(",")
    s"{$fields}"

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
