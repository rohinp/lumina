package lumina.plan.optimizer

import lumina.plan.*
import lumina.plan.Expression.*

/**
 * Evaluates sub-expressions whose operands are all [[Literal]]s at optimisation
 * time, eliminating the runtime overhead of re-computing them for every row.
 *
 * === Expression-level folding ===
 * {{{
 *   Add(Literal(1), Literal(2))        →  Literal(3.0)
 *   And(Literal(true), expr)           →  expr
 *   Not(Literal(false))                →  Literal(true)
 *   EqualTo(Literal("a"), Literal("b")) →  Literal(false)
 * }}}
 *
 * === Plan-level simplification ===
 * {{{
 *   Filter(child, Literal(true))   →  child          (always-true filter)
 *   Filter(child, Literal(false))  →  Filter(child, Literal(false))  (left in place;
 *                                     executor handles the empty result naturally)
 * }}}
 *
 * The rule descends into every expression embedded in a plan node so folding
 * propagates through Filter conditions, Project columns, WithColumn expressions,
 * and so on.
 */
object ConstantFolding extends Rule:

  override val name: String = "ConstantFolding"

  override def apply(plan: LogicalPlan): LogicalPlan =
    plan match

      // A filter whose condition folds to `true` is a no-op — remove it.
      case Filter(child, cond) =>
        foldExpr(cond) match
          case Literal(true)  => child
          case folded         => Filter(child, folded)

      // Fold expressions inside projection columns.
      case Project(child, cols, schema) =>
        Project(child, cols.map(foldExpr), schema)

      // Fold the WithColumn expression.
      case WithColumn(child, name, expr) =>
        WithColumn(child, name, foldExpr(expr))

      case other => other

  // ---------------------------------------------------------------------------
  // Expression folder — rewrites a tree bottom-up, collapsing all-literal nodes
  // ---------------------------------------------------------------------------

  private[optimizer] def foldExpr(expr: Expression): Expression =
    expr match

      // ── Logical ───────────────────────────────────────────────────────────
      case And(l, r) =>
        (foldExpr(l), foldExpr(r)) match
          case (Literal(true),  rr)             => rr
          case (ll,  Literal(true))             => ll
          case (Literal(false), _)              => Literal(false)
          case (_,  Literal(false))             => Literal(false)
          case (ll, rr)                         => And(ll, rr)

      case Or(l, r) =>
        (foldExpr(l), foldExpr(r)) match
          case (Literal(true),  _)              => Literal(true)
          case (_,  Literal(true))              => Literal(true)
          case (Literal(false), rr)             => rr
          case (ll,  Literal(false))            => ll
          case (ll, rr)                         => Or(ll, rr)

      case Not(e) =>
        foldExpr(e) match
          case Literal(true)   => Literal(false)
          case Literal(false)  => Literal(true)
          case folded          => Not(folded)

      // ── Comparison ────────────────────────────────────────────────────────
      case EqualTo(l, r) =>
        (foldExpr(l), foldExpr(r)) match
          case (Literal(a), Literal(b)) => Literal(a == b)
          case (ll, rr)                 => EqualTo(ll, rr)

      case NotEqualTo(l, r) =>
        (foldExpr(l), foldExpr(r)) match
          case (Literal(a), Literal(b)) => Literal(a != b)
          case (ll, rr)                 => NotEqualTo(ll, rr)

      case GreaterThan(l, r) =>
        (foldExpr(l), foldExpr(r)) match
          case (Literal(a), Literal(b)) => Literal(toDouble(a) > toDouble(b))
          case (ll, rr)                 => GreaterThan(ll, rr)

      case GreaterThanOrEqual(l, r) =>
        (foldExpr(l), foldExpr(r)) match
          case (Literal(a), Literal(b)) => Literal(toDouble(a) >= toDouble(b))
          case (ll, rr)                 => GreaterThanOrEqual(ll, rr)

      case LessThan(l, r) =>
        (foldExpr(l), foldExpr(r)) match
          case (Literal(a), Literal(b)) => Literal(toDouble(a) < toDouble(b))
          case (ll, rr)                 => LessThan(ll, rr)

      case LessThanOrEqual(l, r) =>
        (foldExpr(l), foldExpr(r)) match
          case (Literal(a), Literal(b)) => Literal(toDouble(a) <= toDouble(b))
          case (ll, rr)                 => LessThanOrEqual(ll, rr)

      // ── Arithmetic ────────────────────────────────────────────────────────
      case Add(l, r) =>
        (foldExpr(l), foldExpr(r)) match
          case (Literal(a), Literal(b)) => Literal(toDouble(a) + toDouble(b))
          case (ll, rr)                 => Add(ll, rr)

      case Subtract(l, r) =>
        (foldExpr(l), foldExpr(r)) match
          case (Literal(a), Literal(b)) => Literal(toDouble(a) - toDouble(b))
          case (ll, rr)                 => Subtract(ll, rr)

      case Multiply(l, r) =>
        (foldExpr(l), foldExpr(r)) match
          case (Literal(a), Literal(b)) => Literal(toDouble(a) * toDouble(b))
          case (ll, rr)                 => Multiply(ll, rr)

      case Divide(l, r) =>
        (foldExpr(l), foldExpr(r)) match
          case (Literal(a), Literal(b)) if toDouble(b) != 0.0 =>
            Literal(toDouble(a) / toDouble(b))
          case (ll, rr) => Divide(ll, rr)

      case Negate(e) =>
        foldExpr(e) match
          case Literal(v) => Literal(-toDouble(v))
          case folded     => Negate(folded)

      // ── Null checks ───────────────────────────────────────────────────────
      case IsNull(e) =>
        foldExpr(e) match
          case Literal(null) => Literal(true)
          case Literal(_)    => Literal(false)   // non-null literal is never null
          case folded        => IsNull(folded)

      case IsNotNull(e) =>
        foldExpr(e) match
          case Literal(null) => Literal(false)
          case Literal(_)    => Literal(true)
          case folded        => IsNotNull(folded)

      // ── Alias / leaf ──────────────────────────────────────────────────────
      case Alias(e, name) => Alias(foldExpr(e), name)
      case other          => other   // ColumnRef, Literal, string fns, etc.

  // ---------------------------------------------------------------------------
  // Helper
  // ---------------------------------------------------------------------------

  private def toDouble(v: Any): Double = v match
    case n: Int    => n.toDouble
    case n: Long   => n.toDouble
    case n: Double => n
    case n: Float  => n.toDouble
    case n: java.lang.Number => n.doubleValue()
    case other => throw IllegalArgumentException(s"Cannot convert to Double: $other")
