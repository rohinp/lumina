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

    case WithColumn(child, columnName, expr) =>
      // SELECT *, (expr) AS "name" FROM (child) AS _wc
      // The child may or may not have a column with this name already.  The
      // deduplicateNames step in collectRows gives the LAST occurrence of a
      // repeated name the clean key, so the computed column (appended last)
      // wins over any pre-existing column with the same name.
      s"""SELECT *, ${exprSql(expr)} AS "$columnName" FROM (${render(child)}) AS _wc"""

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
      // Table-qualify column references in the ON clause so DuckDB can resolve
      // them unambiguously when both sides share column names.
      val onClause = condition.map(c => s" ON ${joinExprSql(c, "_join_left", "_join_right")}").getOrElse("")
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
    case Add(l, r)                => s"(${exprSql(l)} + ${exprSql(r)})"
    case Subtract(l, r)           => s"(${exprSql(l)} - ${exprSql(r)})"
    case Multiply(l, r)           => s"(${exprSql(l)} * ${exprSql(r)})"
    case Divide(l, r)             => s"(${exprSql(l)} / ${exprSql(r)})"
    case Negate(e)                => s"-(${exprSql(e)})"
    case Alias(e, name)           => s"""${exprSql(e)} AS "$name""""

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

  /**
   * Like [[exprSql]] but qualifies ColumnRef nodes with a table alias so that
   * column names shared by both sides of a JOIN are unambiguous.
   *
   * Convention: in a binary comparison the left operand's columns are qualified
   * with `leftAlias` and the right operand's with `rightAlias`.  This matches
   * the natural join-writing convention: `left.key = right.foreign_key`.
   * Unary expressions (e.g. `age IS NULL`) use `leftAlias` by default.
   */
  private def joinExprSql(
      expr: Expression,
      leftAlias: String,
      rightAlias: String
  ): String =
    def qualCol(e: Expression, tableAlias: String): String = e match
      case ColumnRef(name) => s"""$tableAlias."$name""""
      case other           => joinExprSql(other, tableAlias, tableAlias)

    expr match
      // Binary comparisons: qualify left child with leftAlias, right with rightAlias
      case GreaterThan(l, r)        => s"${qualCol(l, leftAlias)} > ${qualCol(r, rightAlias)}"
      case GreaterThanOrEqual(l, r) => s"${qualCol(l, leftAlias)} >= ${qualCol(r, rightAlias)}"
      case LessThan(l, r)           => s"${qualCol(l, leftAlias)} < ${qualCol(r, rightAlias)}"
      case LessThanOrEqual(l, r)    => s"${qualCol(l, leftAlias)} <= ${qualCol(r, rightAlias)}"
      case EqualTo(l, r)            => s"${qualCol(l, leftAlias)} = ${qualCol(r, rightAlias)}"
      case NotEqualTo(l, r)         => s"${qualCol(l, leftAlias)} <> ${qualCol(r, rightAlias)}"
      // Logical combinators: pass both aliases through to each side
      case And(l, r) => s"(${joinExprSql(l, leftAlias, rightAlias)} AND ${joinExprSql(r, leftAlias, rightAlias)})"
      case Or(l, r)  => s"(${joinExprSql(l, leftAlias, rightAlias)} OR ${joinExprSql(r, leftAlias, rightAlias)})"
      case Not(e)    => s"NOT (${joinExprSql(e, leftAlias, rightAlias)})"
      // For anything else, fall back to normal SQL with left qualification
      case other     => exprSql(other)

  private def aliasSql(alias: Option[String]): String =
    alias.map(a => s""" AS "$a"""").getOrElse("")

  // ---------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------

  /** Converts a `memory://customers` URI to the DuckDB table name `customers`. */
  private[duckdb] def tableNameFor(path: String): String =
    path.stripPrefix("memory://")
