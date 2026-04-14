package lumina.plan

/** Base trait for all logical plan nodes. */
sealed trait LogicalPlan:
  def children: Seq[LogicalPlan]
  def outputSchema: Option[Schema]

final case class ReadCsv(path: String, schema: Option[Schema]) extends LogicalPlan:
  override val children: Seq[LogicalPlan]  = Seq.empty
  override val outputSchema: Option[Schema] = schema

final case class Filter(child: LogicalPlan, condition: Expression) extends LogicalPlan:
  override val children: Seq[LogicalPlan]  = Seq(child)
  override val outputSchema: Option[Schema] = child.outputSchema

final case class Project(child: LogicalPlan, columns: Vector[Expression], schema: Option[Schema])
    extends LogicalPlan:
  override val children: Seq[LogicalPlan]  = Seq(child)
  override val outputSchema: Option[Schema] = schema.orElse(child.outputSchema)

final case class Aggregate(
    child: LogicalPlan,
    groupBy: Vector[Expression],
    aggregations: Vector[Aggregation],
    schema: Option[Schema]
) extends LogicalPlan:
  override val children: Seq[LogicalPlan]  = Seq(child)
  override val outputSchema: Option[Schema] = schema

/**
 * Sort rows by one or more expressions.
 *
 * @param sortExprs ordered list of (expression, ascending) pairs; first entry
 *                  is the primary sort key, subsequent entries break ties.
 */
final case class Sort(child: LogicalPlan, sortExprs: Vector[SortExpr]) extends LogicalPlan:
  override val children: Seq[LogicalPlan]  = Seq(child)
  override val outputSchema: Option[Schema] = child.outputSchema

/** A single sort key with direction. */
final case class SortExpr(expr: Expression, ascending: Boolean = true)

/**
 * Add or replace a single column in the output.
 *
 * All existing columns pass through unchanged; `expr` is evaluated per row
 * and stored under `columnName`, overwriting any existing column with that name.
 * This avoids needing full schema knowledge at plan construction time.
 */
final case class WithColumn(child: LogicalPlan, columnName: String, expr: Expression)
    extends LogicalPlan:
  override val children: Seq[LogicalPlan]  = Seq(child)
  override val outputSchema: Option[Schema] = child.outputSchema

/**
 * Return at most `count` rows from the child plan.
 *
 * When combined with Sort, produces ordered top-N queries.
 */
final case class Limit(child: LogicalPlan, count: Int) extends LogicalPlan:
  override val children: Seq[LogicalPlan]  = Seq(child)
  override val outputSchema: Option[Schema] = child.outputSchema

/** How columns are matched when joining two datasets. */
enum JoinType:
  case Inner, Left, Right, Full

/**
 * Adds one or more window-function columns to each row without changing the
 * row count.  Each [[WindowExpr]] contributes exactly one output column.
 *
 * The child plan's output schema is preserved; the window columns are appended.
 */
final case class Window(child: LogicalPlan, windowExprs: Vector[WindowExpr])
    extends LogicalPlan:
  override val children: Seq[LogicalPlan]  = Seq(child)
  override val outputSchema: Option[Schema] = child.outputSchema

/**
 * Combine two input plans on an optional join condition.
 *
 * A `None` condition performs a cross join. For `Left`, `Right`, and `Full`
 * outer joins, rows with no match on the corresponding side are filled with
 * `null` values.
 */
final case class Join(
    left: LogicalPlan,
    right: LogicalPlan,
    condition: Option[Expression],
    joinType: JoinType
) extends LogicalPlan:
  override val children: Seq[LogicalPlan]  = Seq(left, right)
  override val outputSchema: Option[Schema] = None

/**
 * Concatenates all rows from both children without removing duplicates.
 *
 * Both children must produce the same column names.  Use [[Distinct]] on top
 * to obtain a UNION (distinct) rather than UNION ALL.
 */
final case class UnionAll(left: LogicalPlan, right: LogicalPlan) extends LogicalPlan:
  override val children: Seq[LogicalPlan]  = Seq(left, right)
  override val outputSchema: Option[Schema] = left.outputSchema

/**
 * Removes duplicate rows from the child plan.
 *
 * Two rows are considered duplicates when every column value is equal
 * (using standard Scala equality, i.e. `==`).
 */
final case class Distinct(child: LogicalPlan) extends LogicalPlan:
  override val children: Seq[LogicalPlan]  = Seq(child)
  override val outputSchema: Option[Schema] = child.outputSchema
