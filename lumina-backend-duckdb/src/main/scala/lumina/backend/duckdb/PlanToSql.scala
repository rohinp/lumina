package lumina.backend.duckdb

import lumina.plan.*
import lumina.plan.Expression.*
import lumina.plan.Aggregation.*

/**
 * Translates a [[LogicalPlan]] into a DuckDB-compatible SQL string.
 *
 * Each plan node becomes a subquery (composable nested SELECT), so the result
 * is always a single self-contained SELECT statement with no CTEs.
 *
 * Identifiers are double-quoted to preserve casing and avoid keyword clashes.
 * String literals are single-quoted with embedded single-quotes escaped.
 */
object PlanToSql:

  /** Entry point: returns the full SQL SELECT statement for the plan. */
  def toSql(plan: LogicalPlan): String = render(plan)

  private def render(plan: LogicalPlan): String = plan match
    case ReadCsv(path, _) =>
      if path.startsWith("memory://") then
        s"""SELECT * FROM "${tableNameFor(path)}""""
      else
        s"SELECT * FROM read_csv_auto('$path')"

    case Filter(child, condition) =>
      s"SELECT * FROM (${render(child)}) AS _filtered WHERE ${exprSql(condition)}"

    case Project(child, columns, _) =>
      val cols = columns.map(exprSql).mkString(", ")
      s"SELECT $cols FROM (${render(child)}) AS _projected"

    case Aggregate(child, groupBy, aggregations, _) =>
      val groups      = groupBy.map(exprSql).mkString(", ")
      val aggs        = aggregations.map(aggSql).mkString(", ")
      val selectCols  = if groups.isEmpty then aggs else s"$groups, $aggs"
      val groupClause = if groupBy.isEmpty then "" else s" GROUP BY $groups"
      s"SELECT $selectCols FROM (${render(child)}) AS _aggregated$groupClause"

    case Sort(child, sortExprs) =>
      val orderParts = sortExprs.map { se =>
        val dir = if se.ascending then "ASC" else "DESC"
        s"${exprSql(se.expr)} $dir"
      }.mkString(", ")
      s"SELECT * FROM (${render(child)}) AS _sorted ORDER BY $orderParts"

    case Limit(child, count) =>
      s"SELECT * FROM (${render(child)}) AS _limited LIMIT $count"

    case Join(left, right, condition, joinType) =>
      val joinKeyword = joinType match
        case JoinType.Inner => "INNER JOIN"
        case JoinType.Left  => "LEFT JOIN"
        case JoinType.Right => "RIGHT JOIN"
        case JoinType.Full  => "FULL OUTER JOIN"
      val onClause = condition.map(c => s" ON ${exprSql(c)}").getOrElse("")
      s"SELECT * FROM (${render(left)}) AS _join_left $joinKeyword (${render(right)}) AS _join_right$onClause"

  // ---------------------------------------------------------------------------
  // Expression → SQL fragment
  // ---------------------------------------------------------------------------

  private[duckdb] def exprSql(expr: Expression): String = expr match
    case ColumnRef(name)          => s""""$name""""
    case Literal(v: String)       => s"'${v.replace("'", "''")}'"
    case Literal(v)               => v.toString
    case GreaterThan(l, r)        => s"${exprSql(l)} > ${exprSql(r)}"
    case GreaterThanOrEqual(l, r) => s"${exprSql(l)} >= ${exprSql(r)}"
    case LessThan(l, r)           => s"${exprSql(l)} < ${exprSql(r)}"
    case LessThanOrEqual(l, r)    => s"${exprSql(l)} <= ${exprSql(r)}"
    case EqualTo(l, r)            => s"${exprSql(l)} = ${exprSql(r)}"
    case NotEqualTo(l, r)         => s"${exprSql(l)} <> ${exprSql(r)}"
    case And(l, r)                => s"(${exprSql(l)} AND ${exprSql(r)})"
    case Or(l, r)                 => s"(${exprSql(l)} OR ${exprSql(r)})"
    case Not(e)                   => s"NOT (${exprSql(e)})"
    case IsNull(e)                => s"${exprSql(e)} IS NULL"
    case IsNotNull(e)             => s"${exprSql(e)} IS NOT NULL"

  // ---------------------------------------------------------------------------
  // Aggregation → SQL fragment
  // ---------------------------------------------------------------------------

  private[duckdb] def aggSql(agg: Aggregation): String = agg match
    case Sum(col, alias)         => s"SUM(${exprSql(col)})${aliasSql(alias)}"
    case Count(None, alias)      => s"COUNT(*)${aliasSql(alias)}"
    case Count(Some(col), alias) => s"COUNT(${exprSql(col)})${aliasSql(alias)}"
    case Avg(col, alias)         => s"AVG(${exprSql(col)})${aliasSql(alias)}"
    case Min(col, alias)         => s"MIN(${exprSql(col)})${aliasSql(alias)}"
    case Max(col, alias)         => s"MAX(${exprSql(col)})${aliasSql(alias)}"

  private def aliasSql(alias: Option[String]): String =
    alias.map(a => s""" AS "$a"""").getOrElse("")

  // ---------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------

  /** Converts a `memory://customers` URI to the DuckDB table name `customers`. */
  private[duckdb] def tableNameFor(path: String): String =
    path.stripPrefix("memory://")
