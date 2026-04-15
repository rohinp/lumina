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
   * Execution order:
   *  1. [[ConstantFolding]] — collapse all-literal sub-expressions so later
   *     rules see simpler conditions (e.g. `And(true, cond)` → `cond`).
   *  2. [[CombineFilters]] — merge consecutive Filter nodes into one `And`.
   *  3. [[PredicatePushdown]] — move filters closer to the data source.
   */
  val defaultRules: Seq[Rule] = Seq(
    ConstantFolding,
    CombineFilters,
    PredicatePushdown
  )

  /**
   * Optimise `plan` by applying `rules` bottom-up in the given order.
   * Returns the rewritten plan; the original is never mutated.
   *
   * A single call applies the rule set once.  Use [[optimizeUntilFixedPoint]]
   * when rules feed each other (e.g. ConstantFolding simplifies a condition
   * that CombineFilters then merges, enabling further PredicatePushdown).
   */
  def optimize(plan: LogicalPlan, rules: Seq[Rule] = defaultRules): LogicalPlan =
    val optimizedChildren = plan.children.map(optimize(_, rules))
    val rebuilt           = rebuildWith(plan, optimizedChildren)
    rules.foldLeft(rebuilt)((p, rule) => rule(p))

  /**
   * Applies `optimize` repeatedly until the plan stops changing (fixed point)
   * or `maxPasses` is reached, whichever comes first.
   *
   * This lets rules that produce inputs for each other converge fully without
   * requiring callers to know how many passes are needed.
   */
  def optimizeUntilFixedPoint(
      plan:     LogicalPlan,
      rules:    Seq[Rule] = defaultRules,
      maxPasses: Int      = 10
  ): LogicalPlan =
    var current  = plan
    var previous = plan
    var passes   = 0
    while passes < maxPasses do
      current = optimize(current, rules)
      if current == previous then passes = maxPasses  // fixed point reached
      else { previous = current; passes += 1 }
    current

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
      case (Intersect(_, _),         Seq(l, r)) => Intersect(l, r)
      case (Except(_, _),            Seq(l, r)) => Except(l, r)
      case (Sample(_, frac, seed),   Seq(c))    => Sample(c, frac, seed)
      case (DropColumns(_, cols),    Seq(c))    => DropColumns(c, cols)
      case (RenameColumn(_, f, t),   Seq(c))    => RenameColumn(c, f, t)
      case _                                     => plan   // unknown shape, leave intact
