package lumina.plan

import lumina.plan.Expression.*
import lumina.plan.Aggregation.*

/**
 * Renders a [[LogicalPlan]] as a human-readable tree string.
 *
 * Output format (Spark-inspired):
 * {{{
 * == Logical Plan ==
 * Aggregate [city] → [SUM(revenue) AS total_revenue]
 * +- Filter (age > 30)
 *    +- ReadCsv [memory://customers] schema=[city:StringType, age:Int32, revenue:Float64]
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
    val connector = if isRoot then "" else "+- "
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

  // ---------------------------------------------------------------------------
  // Expression and aggregation display helpers
  // ---------------------------------------------------------------------------

  private def exprStr(expr: Expression): String = expr match
    case ColumnRef(name)       => name
    case Literal(v: String)    => s"'$v'"
    case Literal(v)            => v.toString
    case GreaterThan(l, r)     => s"${exprStr(l)} > ${exprStr(r)}"
    case EqualTo(l, r)         => s"${exprStr(l)} = ${exprStr(r)}"

  private def aggStr(agg: Aggregation): String = agg match
    case Sum(col, alias)         => s"SUM(${exprStr(col)})${alias.map(a => s" AS $a").getOrElse("")}"
    case Count(None, alias)      => s"COUNT(*)${alias.map(a => s" AS $a").getOrElse("")}"
    case Count(Some(col), alias) => s"COUNT(${exprStr(col)})${alias.map(a => s" AS $a").getOrElse("")}"
