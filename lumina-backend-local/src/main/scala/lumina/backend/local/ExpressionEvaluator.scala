package lumina.backend.local

import lumina.plan.Expression
import lumina.plan.Expression.*
import lumina.plan.backend.Row

/**
 * Evaluates logical-plan expressions against a single Row at runtime.
 *
 * Numeric comparisons work across Int/Long/Double by promoting all numbers to
 * Double. This keeps the evaluator simple without requiring a typed schema at
 * evaluation time.
 */
object ExpressionEvaluator:

  /** Resolve an expression to a raw value given a row. */
  def evaluate(expr: Expression, row: Row): Any =
    expr match
      case ColumnRef(name)       => row(name)
      case Literal(value)        => value
      case GreaterThan(l, r)     => compareNumeric(evaluate(l, row), evaluate(r, row)) > 0
      case EqualTo(l, r)         => evaluate(l, row) == evaluate(r, row)

  /** Evaluate a predicate expression; throws if the result is not a Boolean. */
  def evaluatePredicate(expr: Expression, row: Row): Boolean =
    evaluate(expr, row) match
      case b: Boolean => b
      case other      => throw IllegalStateException(s"Expected Boolean predicate, got: $other")

  private def compareNumeric(a: Any, b: Any): Double =
    toDouble(a) - toDouble(b)

  private def toDouble(v: Any): Double =
    v match
      case n: Int    => n.toDouble
      case n: Long   => n.toDouble
      case n: Double => n
      case n: Float  => n.toDouble
      case other     => throw IllegalArgumentException(s"Cannot compare non-numeric value: $other")
