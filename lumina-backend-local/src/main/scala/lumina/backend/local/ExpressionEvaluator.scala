package lumina.backend.local

import lumina.plan.Expression
import lumina.plan.Expression.*
import lumina.plan.backend.Row

/**
 * Evaluates logical-plan expressions against a single Row at runtime.
 *
 * Numeric comparisons work across Int/Long/Double by promoting all numbers to
 * Double. String comparisons use natural lexicographic ordering.
 * Null values (represented as `null` in Any) propagate through expressions:
 * any comparison involving null returns false; `IsNull` / `IsNotNull` inspect
 * the raw value.
 */
object ExpressionEvaluator:

  /** Resolve an expression to a raw value given a row. */
  def evaluate(expr: Expression, row: Row): Any =
    expr match
      case ColumnRef(name)          => row.values.getOrElse(name, null)
      case Literal(value)           => value
      case GreaterThan(l, r)        => compareValues(evaluate(l, row), evaluate(r, row)) > 0
      case GreaterThanOrEqual(l, r) => compareValues(evaluate(l, row), evaluate(r, row)) >= 0
      case LessThan(l, r)           => compareValues(evaluate(l, row), evaluate(r, row)) < 0
      case LessThanOrEqual(l, r)    => compareValues(evaluate(l, row), evaluate(r, row)) <= 0
      case EqualTo(l, r)            => equalValues(evaluate(l, row), evaluate(r, row))
      case NotEqualTo(l, r)         => !equalValues(evaluate(l, row), evaluate(r, row))
      case And(l, r)                =>
        evaluatePredicate(l, row) && evaluatePredicate(r, row)
      case Or(l, r)                 =>
        evaluatePredicate(l, row) || evaluatePredicate(r, row)
      case Not(e)                   => !evaluatePredicate(e, row)
      case IsNull(e)                => evaluate(e, row) == null
      case IsNotNull(e)             => evaluate(e, row) != null

  /** Evaluate a predicate expression; throws if the result is not a Boolean. */
  def evaluatePredicate(expr: Expression, row: Row): Boolean =
    evaluate(expr, row) match
      case b: Boolean => b
      case null       => false   // null predicate → false (SQL three-value logic simplified)
      case other      => throw IllegalStateException(s"Expected Boolean predicate, got: $other")

  /**
   * Compare two values for ordering.
   * Numerics are promoted to Double; Strings use natural order.
   * Returns negative / zero / positive like `Comparator.compare`.
   */
  def compareValues(a: Any, b: Any): Int =
    if a == null || b == null then
      throw IllegalArgumentException("Cannot compare null values for ordering")
    (a, b) match
      case (sa: String, sb: String) => sa.compareTo(sb)
      case _                        => toDouble(a).compareTo(toDouble(b))

  private def equalValues(a: Any, b: Any): Boolean =
    if a == null && b == null then true
    else if a == null || b == null then false
    else (a, b) match
      case (sa: String, sb: String) => sa == sb
      case _                        =>
        // allow cross-type numeric equality (e.g. Int 30 == Double 30.0)
        try toDouble(a) == toDouble(b)
        catch case _: IllegalArgumentException => a == b

  private def toDouble(v: Any): Double =
    v match
      case n: Int     => n.toDouble
      case n: Long    => n.toDouble
      case n: Double  => n
      case n: Float   => n.toDouble
      case n: java.lang.Integer => n.toDouble
      case n: java.lang.Long    => n.toDouble
      case n: java.lang.Double  => n
      case n: java.lang.Float   => n.toDouble
      case other => throw IllegalArgumentException(s"Cannot compare non-numeric value: $other (${other.getClass})")
