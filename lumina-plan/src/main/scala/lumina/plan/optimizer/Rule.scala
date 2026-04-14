package lumina.plan.optimizer

import lumina.plan.LogicalPlan

/**
 * A single rewrite rule applied to a [[LogicalPlan]] tree.
 *
 * Rules are applied bottom-up by [[Optimizer]]: each rule sees an already-
 * optimized subtree before it rewrites the current node.  A rule that cannot
 * improve a node should return the node unchanged.
 */
trait Rule:
  def name: String
  def apply(plan: LogicalPlan): LogicalPlan
