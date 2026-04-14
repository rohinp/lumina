package lumina.plan.optimizer

import lumina.plan.*
import lumina.plan.Expression.*

/**
 * Pushes Filter nodes closer to the data source.
 *
 * === Filter through Project ===
 * When a Filter sits on top of a Project and all columns referenced by the
 * filter condition are present in the projected column list, the filter can
 * move below the project.  This avoids projecting rows that will be discarded.
 *
 * {{{
 *   // Before
 *   Filter(Project(child, [city, age]), age > 30)
 *
 *   // After
 *   Project(Filter(child, age > 30), [city, age])
 * }}}
 *
 * === Filter through Sort ===
 * A Filter above a Sort can always move below it — filtering first means
 * fewer rows to sort.
 *
 * {{{
 *   // Before
 *   Filter(Sort(child, exprs), cond)
 *
 *   // After
 *   Sort(Filter(child, cond), exprs)
 * }}}
 *
 * Filters above ReadCsv, Aggregate, or Join are left in place because
 * pushing through those nodes is either semantically unsafe (Aggregate)
 * or already handled by the query engine (ReadCsv in DuckDB).
 */
object PredicatePushdown extends Rule:

  override val name: String = "PredicatePushdown"

  override def apply(plan: LogicalPlan): LogicalPlan =
    plan match

      case Filter(Project(child, columns, schema), condition)
          if canPushThrough(condition, columns) =>
        Project(Filter(child, condition), columns, schema)

      case Filter(Sort(child, sortExprs), condition) =>
        Sort(Filter(child, condition), sortExprs)

      case other =>
        other

  // ---------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------

  /**
   * Returns true when every column referenced by `condition` appears in
   * the projected column list, meaning the filter can safely execute below
   * the project without seeing columns that have been dropped.
   */
  private def canPushThrough(condition: Expression, projected: Vector[Expression]): Boolean =
    val projectedNames = projected.collect { case ColumnRef(n) => n }.toSet
    referencedColumns(condition).subsetOf(projectedNames)

  private def referencedColumns(expr: Expression): Set[String] = expr match
    case ColumnRef(name)         => Set(name)
    case Literal(_)              => Set.empty
    case GreaterThan(l, r)       => referencedColumns(l) ++ referencedColumns(r)
    case GreaterThanOrEqual(l, r)=> referencedColumns(l) ++ referencedColumns(r)
    case LessThan(l, r)          => referencedColumns(l) ++ referencedColumns(r)
    case LessThanOrEqual(l, r)   => referencedColumns(l) ++ referencedColumns(r)
    case EqualTo(l, r)           => referencedColumns(l) ++ referencedColumns(r)
    case NotEqualTo(l, r)        => referencedColumns(l) ++ referencedColumns(r)
    case And(l, r)               => referencedColumns(l) ++ referencedColumns(r)
    case Or(l, r)                => referencedColumns(l) ++ referencedColumns(r)
    case Not(e)                  => referencedColumns(e)
    case IsNull(e)               => referencedColumns(e)
    case IsNotNull(e)            => referencedColumns(e)
