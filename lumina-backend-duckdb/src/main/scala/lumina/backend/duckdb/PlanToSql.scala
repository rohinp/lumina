package lumina.backend.duckdb

import lumina.plan.*
import lumina.plan.Expression.*
import lumina.plan.Aggregation.*
import lumina.plan.WindowExpr.*

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

    case Window(child, windowExprs) =>
      val winCols = windowExprs.map(windowExprSql).mkString(", ")
      s"SELECT *, $winCols FROM (${render(child)}) AS _window"

    case UnionAll(left, right) =>
      s"SELECT * FROM ((${render(left)}) UNION ALL (${render(right)})) AS _union"

    case Distinct(child) =>
      s"SELECT DISTINCT * FROM (${render(child)}) AS _distinct"

    case Intersect(left, right) =>
      s"SELECT * FROM ((${render(left)}) INTERSECT (${render(right)})) AS _intersect"

    case Except(left, right) =>
      s"SELECT * FROM ((${render(left)}) EXCEPT (${render(right)})) AS _except"

    case Sample(child, fraction, _) =>
      // DuckDB Bernoulli sampling — seed is honored by LocalBackend but not
      // propagated to DuckDB (the JDBC driver version in use does not support
      // the REPEATABLE clause for percentage-based sampling).
      val pct = fraction * 100
      s"SELECT * FROM (${render(child)}) AS _sampled USING SAMPLE $pct PERCENT (bernoulli)"

    case DropColumns(child, cols) =>
      val excludeList = cols.map(c => s""""$c"""").mkString(", ")
      s"SELECT * EXCLUDE ($excludeList) FROM (${render(child)}) AS _drop"

    case RenameColumn(child, oldName, newName) =>
      // Remove the old name from * and add it back under the new name.
      s"""SELECT * EXCLUDE ("$oldName"), "$oldName" AS "$newName" FROM (${render(child)}) AS _rename"""

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

    // String functions
    case Upper(e)                 => s"UPPER(${exprSql(e)})"
    case Lower(e)                 => s"LOWER(${exprSql(e)})"
    case Trim(e)                  => s"TRIM(${exprSql(e)})"
    case Length(e)                => s"LENGTH(${exprSql(e)})"
    case Concat(exprs)            => s"CONCAT(${exprs.map(exprSql).mkString(", ")})"
    case Substring(e, start, len) => s"SUBSTRING(${exprSql(e)}, $start, $len)"
    case Like(e, pattern)         => s"${exprSql(e)} LIKE '${pattern.replace("'", "''")}'"
    case Replace(e, s, r)        => s"REPLACE(${exprSql(e)}, '${s.replace("'", "''")}', '${r.replace("'", "''")}')"
    case RegexpExtract(e, p, g)  =>
      // DuckDB regexp_extract(string, pattern, group_index) — group 0 = full match
      s"regexp_extract(${exprSql(e)}, '${p.replace("'", "''")}', $g)"
    case RegexpReplace(e, p, r)  =>
      // DuckDB regexp_replace without flags replaces only the first match; 'g' replaces all
      s"regexp_replace(${exprSql(e)}, '${p.replace("'", "''")}', '${r.replace("'", "''")}', 'g')"
    case StartsWith(e, prefix)   =>
      // DuckDB starts_with returns NULL for NULL input; COALESCE to match LocalBackend (returns false)
      s"COALESCE(starts_with(${exprSql(e)}, '${prefix.replace("'", "''")}'), false)"
    case EndsWith(e, suffix)     =>
      // DuckDB ends_with returns NULL for NULL input; COALESCE to match LocalBackend (returns false)
      s"COALESCE(ends_with(${exprSql(e)}, '${suffix.replace("'", "''")}'), false)"
    case LPad(e, l, p)           =>
      // DuckDB lpad truncates when string is longer than length; guard with CASE WHEN
      val col = exprSql(e)
      s"CASE WHEN length($col) >= $l THEN $col ELSE lpad($col, $l, '${p.replace("'", "''")}') END"
    case RPad(e, l, p)           =>
      // DuckDB rpad truncates when string is longer than length; guard with CASE WHEN
      val col = exprSql(e)
      s"CASE WHEN length($col) >= $l THEN $col ELSE rpad($col, $l, '${p.replace("'", "''")}') END"
    case Repeat(e, n)            => s"repeat(${exprSql(e)}, $n)"
    case Reverse(e)              => s"reverse(${exprSql(e)})"
    case InitCap(e)              =>
      // DuckDB 1.2.0 lacks initcap; implement via string_split + list_transform
      val col = exprSql(e)
      s"array_to_string(list_transform(string_split(lower($col), ' '), x -> CASE WHEN x = '' THEN '' ELSE upper(left(x, 1)) || substring(x, 2) END), ' ')"

    // Null handling
    case Coalesce(exprs)          => s"COALESCE(${exprs.map(exprSql).mkString(", ")})"

    // Type casting
    case Cast(e, t) => s"CAST(${exprSql(e)} AS ${sqlType(t)})"

    // Numeric functions
    case Abs(e)         => s"ABS(${exprSql(e)})"
    case Round(e, s)    => s"ROUND(${exprSql(e)}, $s)"
    case Floor(e)       => s"FLOOR(${exprSql(e)})"
    case Ceil(e)        => s"CEIL(${exprSql(e)})"
    case Sqrt(e)        => s"SQRT(${exprSql(e)})"
    case Power(b, exp)  => s"POWER(${exprSql(b)}, ${exprSql(exp)})"
    case Log(e)         => s"LN(${exprSql(e)})"
    case Log2(e)        => s"LOG2(${exprSql(e)})"
    case Log10(e)       => s"LOG10(${exprSql(e)})"
    case Exp(e)         => s"EXP(${exprSql(e)})"
    case Sign(e)        => s"SIGN(${exprSql(e)})"
    case Mod(d, v)      => s"(${exprSql(d)} % ${exprSql(v)})"
    case Greatest(es)   => s"GREATEST(${es.map(exprSql).mkString(", ")})"
    case Least(es)      => s"LEAST(${es.map(exprSql).mkString(", ")})"

    // Set membership — empty IN list is always false; non-empty emits SQL IN (...)
    case In(_, values) if values.isEmpty => "FALSE"
    case In(e, values)                   => s"${exprSql(e)} IN (${values.map(exprSql).mkString(", ")})"

    // Conditional
    case CaseWhen(branches, otherwise) =>
      val branchSql = branches.map { case (c, v) => s"WHEN ${exprSql(c)} THEN ${exprSql(v)}" }.mkString(" ")
      val elseSql   = otherwise.map(e => s" ELSE ${exprSql(e)}").getOrElse("")
      s"CASE $branchSql$elseSql END"

    // Date/time expressions
    case Year(e)            => s"YEAR(${exprSql(e)})"
    case Month(e)           => s"MONTH(${exprSql(e)})"
    case Day(e)             => s"DAY(${exprSql(e)})"
    case Hour(e)            => s"HOUR(${exprSql(e)})"
    case Minute(e)          => s"MINUTE(${exprSql(e)})"
    case Second(e)          => s"SECOND(${exprSql(e)})"
    case DayOfWeek(e)       => s"ISODOW(${exprSql(e)})"   // 1=Mon, 7=Sun — same as Java
    case ToDate(e)          => s"CAST(${exprSql(e)} AS DATE)"
    case ToTimestamp(e)     => s"CAST(${exprSql(e)} AS TIMESTAMP)"
    // DuckDB: DATE + INTEGER adds that many days
    case DateAdd(d, n)      => s"(${exprSql(d)} + CAST(${exprSql(n)} AS INTEGER))"
    // DATEDIFF(part, start, end) returns end - start
    case DateDiff(e, s)     => s"DATEDIFF('day', ${exprSql(s)}, ${exprSql(e)})"
    case DateFormat(e, fmt) => s"STRFTIME(${exprSql(e)}, '${javaPatternToStrftime(fmt)}')"

  // ---------------------------------------------------------------------------
  // Aggregation → SQL fragment
  // ---------------------------------------------------------------------------

  private[duckdb] def aggSql(agg: Aggregation): String = agg match
    case Sum(col, alias)             => s"SUM(${exprSql(col)})${aliasSql(alias)}"
    case Count(None, alias)          => s"COUNT(*)${aliasSql(alias)}"
    case Count(Some(col), alias)     => s"COUNT(${exprSql(col)})${aliasSql(alias)}"
    case Avg(col, alias)             => s"AVG(${exprSql(col)})${aliasSql(alias)}"
    case Min(col, alias)             => s"MIN(${exprSql(col)})${aliasSql(alias)}"
    case Max(col, alias)             => s"MAX(${exprSql(col)})${aliasSql(alias)}"
    case CountDistinct(col, alias)   => s"COUNT(DISTINCT ${exprSql(col)})${aliasSql(alias)}"
    case StdDev(col, alias)          => s"STDDEV(${exprSql(col)})${aliasSql(alias)}"
    case Variance(col, alias)        => s"VARIANCE(${exprSql(col)})${aliasSql(alias)}"

  // ---------------------------------------------------------------------------
  // Window expression → SQL fragment
  // ---------------------------------------------------------------------------

  private def windowExprSql(we: WindowExpr): String =
    import WindowExpr.*
    val spec     = we.spec
    val partPart = if spec.partitionBy.isEmpty then ""
                   else s"PARTITION BY ${spec.partitionBy.map(exprSql).mkString(", ")} "
    val ordPart  = if spec.orderBy.isEmpty then ""
                   else s"ORDER BY ${spec.orderBy.map(se => s"${exprSql(se.expr)} ${if se.ascending then "ASC" else "DESC"}").mkString(", ")}"
    val over     = s"OVER ($partPart$ordPart)"
    val fn = we match
      case RowNumber(alias, _)      => s"""ROW_NUMBER() $over AS "$alias""""
      case Rank(alias, _)           => s"""RANK() $over AS "$alias""""
      case DenseRank(alias, _)      => s"""DENSE_RANK() $over AS "$alias""""
      case WindowAgg(agg, alias, _) => s"""${aggFnSql(agg)} $over AS "$alias""""
      case Lag(expr, n, alias, _)   => s"""LAG(${exprSql(expr)}, $n) $over AS "$alias""""
      case Lead(expr, n, alias, _)  => s"""LEAD(${exprSql(expr)}, $n) $over AS "$alias""""
    fn

  /** Renders an aggregation as its bare SQL function (no alias). */
  private def aggFnSql(agg: Aggregation): String = agg match
    case Sum(col, _)           => s"SUM(${exprSql(col)})"
    case Count(None, _)        => s"COUNT(*)"
    case Count(Some(col), _)   => s"COUNT(${exprSql(col)})"
    case Avg(col, _)           => s"AVG(${exprSql(col)})"
    case Min(col, _)           => s"MIN(${exprSql(col)})"
    case Max(col, _)           => s"MAX(${exprSql(col)})"
    case CountDistinct(col, _) => s"COUNT(DISTINCT ${exprSql(col)})"
    case StdDev(col, _)        => s"STDDEV(${exprSql(col)})"
    case Variance(col, _)      => s"VARIANCE(${exprSql(col)})"

  // ---------------------------------------------------------------------------

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

  private def sqlType(dt: DataType): String = dt match
    case DataType.Int32         => "INTEGER"
    case DataType.Int64         => "BIGINT"
    case DataType.Float64       => "DOUBLE"
    case DataType.BooleanType   => "BOOLEAN"
    case DataType.StringType    => "VARCHAR"
    case DataType.DateType      => "DATE"
    case DataType.TimestampType => "TIMESTAMP"
    case DataType.Unknown       => "VARCHAR"

  /**
   * Converts a Java `DateTimeFormatter` pattern to a DuckDB `STRFTIME` format.
   *
   * Handles the most common tokens:
   * {{{
   *   yyyy → %Y,  yy → %y,  MM → %m,  dd → %d
   *   HH   → %H,  mm → %M,  ss → %S
   * }}}
   */
  private def javaPatternToStrftime(fmt: String): String =
    fmt
      .replace("yyyy", "%Y")
      .replace("yy",   "%y")
      .replace("MM",   "%m")
      .replace("dd",   "%d")
      .replace("HH",   "%H")
      .replace("mm",   "%M")
      .replace("ss",   "%S")

  // ---------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------

  /** Converts a `memory://customers` URI to the DuckDB table name `customers`. */
  private[duckdb] def tableNameFor(path: String): String =
    path.stripPrefix("memory://")
