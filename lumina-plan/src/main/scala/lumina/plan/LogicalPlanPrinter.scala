package lumina.plan

import lumina.plan.Expression.*
import lumina.plan.Aggregation.*

/**
 * Renders a [[LogicalPlan]] as a human-readable tree string.
 *
 * Output format (Spark-inspired):
 * {{{
 * == Logical Plan ==
 * Limit 10
 * +- Sort [age ASC, revenue DESC]
 *    +- Aggregate [city] → [SUM(revenue) AS total_revenue]
 *       +- Filter (age > 30)
 *          +- ReadCsv [memory://customers] schema=[city:StringType, age:Int32, revenue:Float64]
 * }}}
 *
 * Use [[LogicalPlanPrinter.explain]] to produce the full string, or call
 * [[DataFrame.explain]] / [[DataFrame.explainString]] from the API layer.
 */
object LogicalPlanPrinter:

  def explain(plan: LogicalPlan): String =
    val sb = new StringBuilder
    sb.append("== Logical Plan ==\n")
    renderNode(plan, sb, prefix = "", isRoot = true)
    sb.result()

  // ---------------------------------------------------------------------------
  // Recursive tree renderer
  // ---------------------------------------------------------------------------

  private def renderNode(
      plan: LogicalPlan,
      sb:   StringBuilder,
      prefix: String,
      isRoot: Boolean
  ): Unit =
    val connector  = if isRoot then "" else "+- "
    sb.append(prefix).append(connector).append(describe(plan)).append("\n")
    val childPrefix = if isRoot then "   " else prefix + "   "
    plan.children.foreach(renderNode(_, sb, childPrefix, isRoot = false))

  // ---------------------------------------------------------------------------
  // Per-node descriptions
  // ---------------------------------------------------------------------------

  private def describe(plan: LogicalPlan): String = plan match
    case ReadCsv(path, schema) =>
      val schemaPart = schema.map { s =>
        " schema=[" + s.columns.map(c => s"${c.name}:${c.dataType}").mkString(", ") + "]"
      }.getOrElse("")
      s"ReadCsv [$path]$schemaPart"

    case Filter(_, condition) =>
      s"Filter (${exprStr(condition)})"

    case Project(_, columns, _) =>
      s"Project [${columns.map(exprStr).mkString(", ")}]"

    case Aggregate(_, groupBy, aggregations, _) =>
      val groups = groupBy.map(exprStr).mkString(", ")
      val aggs   = aggregations.map(aggStr).mkString(", ")
      s"Aggregate [$groups] → [$aggs]"

    case WithColumn(_, name, expr) =>
      s"WithColumn [$name = ${exprStr(expr)}]"

    case Window(_, windowExprs) =>
      val parts = windowExprs.map(windowExprStr).mkString(", ")
      s"Window [$parts]"

    case Sort(_, sortExprs) =>
      val parts = sortExprs.map(se => s"${exprStr(se.expr)} ${if se.ascending then "ASC" else "DESC"}")
      s"Sort [${parts.mkString(", ")}]"

    case Limit(_, count) =>
      s"Limit $count"

    case Join(_, _, condition, joinType) =>
      val condStr = condition.map(c => s" ON ${exprStr(c)}").getOrElse(" (cross)")
      s"Join ${joinType.toString.toUpperCase}$condStr"

    case UnionAll(_, _) =>
      "UnionAll"

    case Distinct(_) =>
      "Distinct"

    case Intersect(_, _) =>
      "Intersect"

    case Except(_, _) =>
      "Except"

    case Sample(_, fraction, seed) =>
      val seedStr = seed.map(s => s" seed=$s").getOrElse("")
      s"Sample [fraction=$fraction$seedStr]"

    case DropColumns(_, cols) =>
      s"DropColumns [${cols.mkString(", ")}]"

    case RenameColumn(_, from, to) =>
      s"RenameColumn [$from → $to]"

  // ---------------------------------------------------------------------------
  // Expression display helpers
  // ---------------------------------------------------------------------------

  private[plan] def exprStr(expr: Expression): String = expr match
    case ColumnRef(name)              => name
    case Literal(v: String)           => s"'$v'"
    case Literal(v)                   => v.toString
    case GreaterThan(l, r)            => s"${exprStr(l)} > ${exprStr(r)}"
    case GreaterThanOrEqual(l, r)     => s"${exprStr(l)} >= ${exprStr(r)}"
    case LessThan(l, r)               => s"${exprStr(l)} < ${exprStr(r)}"
    case LessThanOrEqual(l, r)        => s"${exprStr(l)} <= ${exprStr(r)}"
    case EqualTo(l, r)                => s"${exprStr(l)} = ${exprStr(r)}"
    case NotEqualTo(l, r)             => s"${exprStr(l)} != ${exprStr(r)}"
    case And(l, r)                    => s"(${exprStr(l)} AND ${exprStr(r)})"
    case Or(l, r)                     => s"(${exprStr(l)} OR ${exprStr(r)})"
    case Not(e)                       => s"NOT ${exprStr(e)}"
    case IsNull(e)                    => s"${exprStr(e)} IS NULL"
    case IsNotNull(e)                 => s"${exprStr(e)} IS NOT NULL"
    case Add(l, r)                    => s"(${exprStr(l)} + ${exprStr(r)})"
    case Subtract(l, r)               => s"(${exprStr(l)} - ${exprStr(r)})"
    case Multiply(l, r)               => s"(${exprStr(l)} * ${exprStr(r)})"
    case Divide(l, r)                 => s"(${exprStr(l)} / ${exprStr(r)})"
    case Negate(e)                    => s"-(${exprStr(e)})"
    case Alias(e, name)               => s"${exprStr(e)} AS $name"
    case Upper(e)                     => s"UPPER(${exprStr(e)})"
    case Lower(e)                     => s"LOWER(${exprStr(e)})"
    case Trim(e)                      => s"TRIM(${exprStr(e)})"
    case Length(e)                    => s"LENGTH(${exprStr(e)})"
    case Concat(exprs)                => s"CONCAT(${exprs.map(exprStr).mkString(", ")})"
    case Substring(e, s, l)           => s"SUBSTRING(${exprStr(e)}, $s, $l)"
    case Like(e, pattern)             => s"${exprStr(e)} LIKE '$pattern'"
    case Coalesce(exprs)              => s"COALESCE(${exprs.map(exprStr).mkString(", ")})"
    case In(e, values)                => s"${exprStr(e)} IN (${values.map(exprStr).mkString(", ")})"
    case Cast(e, t)                   => s"CAST(${exprStr(e)} AS $t)"
    case Abs(e)                       => s"ABS(${exprStr(e)})"
    case Round(e, scale)              => s"ROUND(${exprStr(e)}, $scale)"
    case Floor(e)                     => s"FLOOR(${exprStr(e)})"
    case Ceil(e)                      => s"CEIL(${exprStr(e)})"
    case CaseWhen(branches, otherwise) =>
      val branchStr   = branches.map { case (c, v) => s"WHEN ${exprStr(c)} THEN ${exprStr(v)}" }.mkString(" ")
      val elseStr     = otherwise.map(e => s" ELSE ${exprStr(e)}").getOrElse("")
      s"CASE $branchStr$elseStr END"

    // Date/time expressions
    case Year(e)                 => s"YEAR(${exprStr(e)})"
    case Month(e)                => s"MONTH(${exprStr(e)})"
    case Day(e)                  => s"DAY(${exprStr(e)})"
    case Hour(e)                 => s"HOUR(${exprStr(e)})"
    case Minute(e)               => s"MINUTE(${exprStr(e)})"
    case Second(e)               => s"SECOND(${exprStr(e)})"
    case DayOfWeek(e)            => s"DAYOFWEEK(${exprStr(e)})"
    case DateAdd(d, n)           => s"DATEADD(${exprStr(d)}, ${exprStr(n)})"
    case DateDiff(e, s)          => s"DATEDIFF(${exprStr(e)}, ${exprStr(s)})"
    case ToDate(e)               => s"TO_DATE(${exprStr(e)})"
    case ToTimestamp(e)          => s"TO_TIMESTAMP(${exprStr(e)})"
    case DateFormat(e, fmt)      => s"DATE_FORMAT(${exprStr(e)}, '$fmt')"

  // ---------------------------------------------------------------------------
  // Aggregation display helpers
  // ---------------------------------------------------------------------------

  private def windowExprStr(we: WindowExpr): String =
    import WindowExpr.*
    val spec = we.spec
    val partStr = if spec.partitionBy.isEmpty then "" else s" PARTITION BY ${spec.partitionBy.map(exprStr).mkString(", ")}"
    val ordStr  = if spec.orderBy.isEmpty then "" else s" ORDER BY ${spec.orderBy.map(se => s"${exprStr(se.expr)} ${if se.ascending then "ASC" else "DESC"}").mkString(", ")}"
    val over    = s"OVER ($partStr$ordStr)".trim
    we match
      case RowNumber(alias, _)       => s"ROW_NUMBER() $over AS $alias"
      case Rank(alias, _)            => s"RANK() $over AS $alias"
      case DenseRank(alias, _)       => s"DENSE_RANK() $over AS $alias"
      case WindowAgg(agg, alias, _)  => s"${aggStr(agg)} $over AS $alias"
      case Lag(expr, n, alias, _)    => s"LAG(${exprStr(expr)}, $n) $over AS $alias"
      case Lead(expr, n, alias, _)   => s"LEAD(${exprStr(expr)}, $n) $over AS $alias"

  private def aggStr(agg: Aggregation): String = agg match
    case Sum(col, alias)             => s"SUM(${exprStr(col)})${alias.map(a => s" AS $a").getOrElse("")}"
    case Count(None, alias)          => s"COUNT(*)${alias.map(a => s" AS $a").getOrElse("")}"
    case Count(Some(col), alias)     => s"COUNT(${exprStr(col)})${alias.map(a => s" AS $a").getOrElse("")}"
    case Avg(col, alias)             => s"AVG(${exprStr(col)})${alias.map(a => s" AS $a").getOrElse("")}"
    case Min(col, alias)             => s"MIN(${exprStr(col)})${alias.map(a => s" AS $a").getOrElse("")}"
    case Max(col, alias)             => s"MAX(${exprStr(col)})${alias.map(a => s" AS $a").getOrElse("")}"
    case CountDistinct(col, alias)   => s"COUNT(DISTINCT ${exprStr(col)})${alias.map(a => s" AS $a").getOrElse("")}"
    case StdDev(col, alias)          => s"STDDEV(${exprStr(col)})${alias.map(a => s" AS $a").getOrElse("")}"
    case Variance(col, alias)        => s"VARIANCE(${exprStr(col)})${alias.map(a => s" AS $a").getOrElse("")}"
