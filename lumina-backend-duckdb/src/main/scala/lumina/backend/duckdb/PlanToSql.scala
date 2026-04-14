package lumina.backend.duckdb

import lumina.plan.*
import lumina.plan.Expression.*
import lumina.plan.Aggregation.*

/**
 * Translates a [[LogicalPlan]] into a DuckDB-compatible SQL string.
 *
 * Each plan node becomes a subquery (CTE-style wrapping), so the resulting
 * SQL is a single SELECT statement that DuckDB can execute directly.
 *
 * Identifiers are double-quoted to preserve casing and avoid keyword clashes.
 * String literals are single-quoted with internal single-quotes escaped.
 */
object PlanToSql:

  /** Entry point: returns the full SQL SELECT statement for the plan. */
  def toSql(plan: LogicalPlan): String = render(plan)

  private def render(plan: LogicalPlan): String = plan match
    case ReadCsv(path, _) =>
      // memory:// URIs reference tables already created in the connection.
      // Real file paths use DuckDB's read_csv_auto.
      if path.startsWith("memory://") then
        val tableName = tableNameFor(path)
        s"""SELECT * FROM "$tableName""""
      else
        s"SELECT * FROM read_csv_auto('$path')"

    case Filter(child, condition) =>
      s"SELECT * FROM (${render(child)}) AS _filtered WHERE ${exprSql(condition)}"

    case Project(child, columns, _) =>
      val cols = columns.map(exprSql).mkString(", ")
      s"SELECT $cols FROM (${render(child)}) AS _projected"

    case Aggregate(child, groupBy, aggregations, _) =>
      val groups = groupBy.map(exprSql).mkString(", ")
      val aggs   = aggregations.map(aggSql).mkString(", ")
      val selectCols = if groups.isEmpty then aggs else s"$groups, $aggs"
      val groupClause = if groupBy.isEmpty then "" else s" GROUP BY $groups"
      s"SELECT $selectCols FROM (${render(child)}) AS _aggregated$groupClause"

  // ---------------------------------------------------------------------------
  // Expression → SQL fragment
  // ---------------------------------------------------------------------------

  private[duckdb] def exprSql(expr: Expression): String = expr match
    case ColumnRef(name)    => s""""$name""""
    case Literal(v: String) => s"'${v.replace("'", "''")}'"
    case Literal(v)         => v.toString
    case GreaterThan(l, r)  => s"${exprSql(l)} > ${exprSql(r)}"
    case EqualTo(l, r)      => s"${exprSql(l)} = ${exprSql(r)}"

  // ---------------------------------------------------------------------------
  // Aggregation → SQL fragment
  // ---------------------------------------------------------------------------

  private[duckdb] def aggSql(agg: Aggregation): String = agg match
    case Sum(col, alias)         => s"SUM(${exprSql(col)})${aliasSql(alias)}"
    case Count(None, alias)      => s"COUNT(*)${aliasSql(alias)}"
    case Count(Some(col), alias) => s"COUNT(${exprSql(col)})${aliasSql(alias)}"

  private def aliasSql(alias: Option[String]): String =
    alias.map(a => s""" AS "$a"""").getOrElse("")

  // ---------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------

  /** Converts a `memory://customers` URI to the DuckDB table name `customers`. */
  private[duckdb] def tableNameFor(path: String): String =
    path.stripPrefix("memory://")
