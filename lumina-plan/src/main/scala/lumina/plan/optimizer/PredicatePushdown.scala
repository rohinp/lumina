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
 * === Filter through WithColumn / DropColumns / Window ===
 * Filters that don't reference newly introduced or removed columns are pushed
 * below the structural node (WithColumn, DropColumns, Window).
 *
 * === Filter through RenameColumn ===
 * When a filter references only the renamed column's *new* name, the condition
 * is rewritten to use the old name and pushed below the rename.
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

      // A filter above a WithColumn can be pushed below it when the filter
      // does not reference the derived column (which doesn't exist yet below).
      case Filter(WithColumn(child, colName, expr), condition)
          if !referencedColumns(condition).contains(colName) =>
        WithColumn(Filter(child, condition), colName, expr)

      // A filter above a Window can be pushed below it when the filter does
      // not reference any window-computed alias (those columns don't exist
      // below the Window node).
      case Filter(Window(child, windowExprs), condition)
          if referencedColumns(condition).intersect(windowExprs.map(_.alias).toSet).isEmpty =>
        Window(Filter(child, condition), windowExprs)

      // A filter above a DropColumns can be pushed below it when the filter
      // does not reference any of the dropped columns.
      case Filter(DropColumns(child, cols), condition)
          if referencedColumns(condition).intersect(cols.toSet).isEmpty =>
        DropColumns(Filter(child, condition), cols)

      // A filter above a RenameColumn: push below if the filter references the
      // new name — rewrite the condition to use the old name so it can run
      // before the rename.  If the filter doesn't touch the renamed column at
      // all, push as-is.
      case Filter(RenameColumn(child, oldName, newName), condition) =>
        val refs = referencedColumns(condition)
        if !refs.contains(newName) then
          // filter doesn't touch the renamed column — push unchanged
          RenameColumn(Filter(child, condition), oldName, newName)
        else if !refs.contains(oldName) then
          // filter references new name only — rewrite to old name and push
          RenameColumn(Filter(child, renameIn(condition, newName, oldName)), oldName, newName)
        else
          // filter references both; leave in place to be safe
          Filter(RenameColumn(child, oldName, newName), condition)

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

  /**
   * Rewrites every `ColumnRef(from)` inside `expr` to `ColumnRef(to)`.
   * Used to translate a filter condition through a RenameColumn node.
   */
  private def renameIn(expr: Expression, from: String, to: String): Expression = expr match
    case ColumnRef(name) if name == from => ColumnRef(to)
    case ColumnRef(_)                    => expr
    case Literal(_)                      => expr
    case GreaterThan(l, r)               => GreaterThan(renameIn(l, from, to), renameIn(r, from, to))
    case GreaterThanOrEqual(l, r)        => GreaterThanOrEqual(renameIn(l, from, to), renameIn(r, from, to))
    case LessThan(l, r)                  => LessThan(renameIn(l, from, to), renameIn(r, from, to))
    case LessThanOrEqual(l, r)           => LessThanOrEqual(renameIn(l, from, to), renameIn(r, from, to))
    case EqualTo(l, r)                   => EqualTo(renameIn(l, from, to), renameIn(r, from, to))
    case NotEqualTo(l, r)                => NotEqualTo(renameIn(l, from, to), renameIn(r, from, to))
    case And(l, r)                       => And(renameIn(l, from, to), renameIn(r, from, to))
    case Or(l, r)                        => Or(renameIn(l, from, to), renameIn(r, from, to))
    case Not(e)                          => Not(renameIn(e, from, to))
    case IsNull(e)                       => IsNull(renameIn(e, from, to))
    case IsNotNull(e)                    => IsNotNull(renameIn(e, from, to))
    case other                           => other  // arithmetic, string functions, etc. — recurse if needed

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
    case Add(l, r)               => referencedColumns(l) ++ referencedColumns(r)
    case Subtract(l, r)          => referencedColumns(l) ++ referencedColumns(r)
    case Multiply(l, r)          => referencedColumns(l) ++ referencedColumns(r)
    case Divide(l, r)            => referencedColumns(l) ++ referencedColumns(r)
    case Negate(e)               => referencedColumns(e)
    case Alias(e, _)             => referencedColumns(e)
    case Upper(e)                => referencedColumns(e)
    case Lower(e)                => referencedColumns(e)
    case Trim(e)                 => referencedColumns(e)
    case Length(e)               => referencedColumns(e)
    case Concat(exprs)           => exprs.flatMap(referencedColumns).toSet
    case Substring(e, _, _)      => referencedColumns(e)
    case Like(e, _)              => referencedColumns(e)
    case Coalesce(exprs)         => exprs.flatMap(referencedColumns).toSet
    case In(e, values)           => referencedColumns(e) ++ values.flatMap(referencedColumns).toSet
