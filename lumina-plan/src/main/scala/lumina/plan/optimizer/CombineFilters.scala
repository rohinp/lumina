package lumina.plan.optimizer

import lumina.plan.*
import lumina.plan.Expression.*

/**
 * Merges consecutive Filter nodes into a single Filter with an And condition.
 *
 * {{{
 *   // Before
 *   Filter(Filter(child, c2), c1)
 *
 *   // After
 *   Filter(child, And(c1, c2))
 * }}}
 *
 * This reduces the number of passes over the data in LocalBackend and produces
 * a single WHERE clause in DuckDB's SQL translation, letting DuckDB's own
 * optimiser reason across all predicates at once.
 */
object CombineFilters extends Rule:

  override val name: String = "CombineFilters"

  override def apply(plan: LogicalPlan): LogicalPlan =
    plan match
      case Filter(Filter(child, innerCond), outerCond) =>
        Filter(child, And(outerCond, innerCond))
      case other =>
        other
