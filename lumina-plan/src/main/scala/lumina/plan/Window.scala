package lumina.plan

/**
 * Defines how rows are partitioned and ordered for a window computation.
 *
 * @param partitionBy  expressions that divide rows into independent groups;
 *                     empty means the whole dataset is one partition
 * @param orderBy      sort order within each partition; required by ranking
 *                     and offset functions, optional for aggregate windows
 */
final case class WindowSpec(
    partitionBy: Vector[Expression] = Vector.empty,
    orderBy:     Vector[SortExpr]   = Vector.empty
)

/**
 * A window function that adds a computed column to each row without changing
 * the number of rows.  Every variant carries an output column [[alias]] and
 * the [[WindowSpec]] describing the partitioning and ordering.
 *
 * Implemented for both [[LocalBackend]] (pure Scala) and [[DuckDBBackend]]
 * (translated to SQL `OVER (...)` clauses).
 */
sealed trait WindowExpr:
  def alias: String
  def spec:  WindowSpec

object WindowExpr:

  /** Assigns a unique sequential integer to each row within its partition. */
  final case class RowNumber(alias: String, spec: WindowSpec) extends WindowExpr

  /**
   * Assigns the same rank to tied rows, leaving gaps in the ranking sequence.
   * e.g. 1, 2, 2, 4 for four rows where rows 2 and 3 tie.
   */
  final case class Rank(alias: String, spec: WindowSpec) extends WindowExpr

  /**
   * Like [[Rank]] but without gaps: tied rows share the same rank and the
   * next distinct rank is one higher.
   * e.g. 1, 2, 2, 3 for the same four rows.
   */
  final case class DenseRank(alias: String, spec: WindowSpec) extends WindowExpr

  /**
   * Applies an [[Aggregation]] over the whole partition (not a running total).
   * Every row in the partition receives the same aggregate value.
   *
   * e.g. `WindowAgg(Sum(ColumnRef("revenue")), "total_revenue", spec)` adds
   * the partition total to every row so you can compute each row's share.
   */
  final case class WindowAgg(agg: Aggregation, alias: String, spec: WindowSpec) extends WindowExpr

  /**
   * Returns the value of `expr` from `offset` rows before the current row
   * within the partition's sort order.  Returns `null` when no preceding row
   * exists at that distance.
   */
  final case class Lag(expr: Expression, offset: Int, alias: String, spec: WindowSpec)
      extends WindowExpr

  /**
   * Returns the value of `expr` from `offset` rows after the current row.
   * Returns `null` when no following row exists at that distance.
   */
  final case class Lead(expr: Expression, offset: Int, alias: String, spec: WindowSpec)
      extends WindowExpr
