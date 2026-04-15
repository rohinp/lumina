package lumina.plan.optimizer

import lumina.plan.LogicalPlan

/**
 * Applies a sequence of rewrite [[Rule]]s to a [[LogicalPlan]] tree.
 *
 * Rules are applied bottom-up (children first) so that each rule operates on
 * an already-optimized subtree.  The full rule set is applied in a single pass;
 * call [[optimize]] again to run multiple rounds (useful when rules feed into
 * each other, e.g. CombineFilters enables PredicatePushdown on the merged node).
 *
 * {{{
 *   val optimized = Optimizer.optimize(plan)   // uses default rules
 *   val custom    = Optimizer.optimize(plan, Seq(CombineFilters))
 * }}}
 */
object Optimizer:

  /**
   * The default rule set applied by [[optimize]] when no rules are supplied.
   *
   * Order matters: CombineFilters runs first so PredicatePushdown sees merged
   * filters rather than chains of single-condition Filter nodes.
   */
  val defaultRules: Seq[Rule] = Seq(
    CombineFilters,
    PredicatePushdown
  )

  /**
   * Optimise `plan` by applying `rules` bottom-up in the given order.
   * Returns the rewritten plan; the original is never mutated.
   */
  def optimize(plan: LogicalPlan, rules: Seq[Rule] = defaultRules): LogicalPlan =
    val optimizedChildren = plan.children.map(optimize(_, rules))
    val rebuilt           = rebuildWith(plan, optimizedChildren)
    rules.foldLeft(rebuilt)((p, rule) => rule(p))

  // ---------------------------------------------------------------------------
  // Internal: rebuild a plan node with new children
  // ---------------------------------------------------------------------------

  private def rebuildWith(plan: LogicalPlan, newChildren: Seq[LogicalPlan]): LogicalPlan =
    import lumina.plan.*
    (plan, newChildren) match
      case (ReadCsv(_, _),      Seq())        => plan
      case (Filter(_, cond),    Seq(c))       => Filter(c, cond)
      case (Project(_, cols, s), Seq(c))      => Project(c, cols, s)
      case (Aggregate(_, g, a, s), Seq(c))   => Aggregate(c, g, a, s)
      case (WithColumn(_, n, e), Seq(c))      => WithColumn(c, n, e)
      case (Window(_, exprs),    Seq(c))      => Window(c, exprs)
      case (Sort(_, exprs),     Seq(c))       => Sort(c, exprs)
      case (Limit(_, n),        Seq(c))       => Limit(c, n)
      case (Join(_, _, cond, jt), Seq(l, r)) => Join(l, r, cond, jt)
      case (UnionAll(_, _),          Seq(l, r)) => UnionAll(l, r)
      case (Distinct(_),             Seq(c))    => Distinct(c)
      case (Sample(_, frac, seed),   Seq(c))    => Sample(c, frac, seed)
      case (DropColumns(_, cols),    Seq(c))    => DropColumns(c, cols)
      case (RenameColumn(_, f, t),   Seq(c))    => RenameColumn(c, f, t)
      case _                                     => plan   // unknown shape, leave intact
