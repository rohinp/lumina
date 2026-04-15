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
      case Add(l, r)                => toDouble(evaluate(l, row)) + toDouble(evaluate(r, row))
      case Subtract(l, r)           => toDouble(evaluate(l, row)) - toDouble(evaluate(r, row))
      case Multiply(l, r)           => toDouble(evaluate(l, row)) * toDouble(evaluate(r, row))
      case Divide(l, r)             =>
        val divisor = toDouble(evaluate(r, row))
        if divisor == 0.0 then throw ArithmeticException("Division by zero")
        toDouble(evaluate(l, row)) / divisor
      case Negate(e)                => -toDouble(evaluate(e, row))
      case Alias(e, _)              => evaluate(e, row)   // unwrap; name is used by Project

      // String functions
      case Upper(e)                 =>
        val v = evaluate(e, row)
        if v == null then null else v.toString.toUpperCase
      case Lower(e)                 =>
        val v = evaluate(e, row)
        if v == null then null else v.toString.toLowerCase
      case Trim(e)                  =>
        val v = evaluate(e, row)
        if v == null then null else v.toString.trim
      case Length(e)                =>
        val v = evaluate(e, row)
        if v == null then null else v.toString.length
      case Concat(exprs)            =>
        val parts = exprs.map(evaluate(_, row))
        if parts.contains(null) then null
        else parts.map(_.toString).mkString
      case Substring(e, start, len) =>
        val v = evaluate(e, row)
        if v == null then null
        else
          val s = v.toString
          // 1-based start index (SQL convention)
          val from  = (start - 1).max(0)
          val until = (from + len).min(s.length)
          if from >= s.length then "" else s.substring(from, until)
      case Like(e, pattern)         =>
        val v = evaluate(e, row)
        if v == null then false
        else
          // Translate SQL LIKE pattern to a Java regex
          val regex = "\\Q" + pattern
            .replace("\\E", "\\E\\\\E\\Q")
            .replace("%", "\\E.*\\Q")
            .replace("_", "\\E.\\Q") + "\\E"
          v.toString.matches(regex)

      // Null handling
      case Coalesce(exprs)          =>
        exprs.map(evaluate(_, row)).find(_ != null).orNull

      // Set membership
      case In(e, values)            =>
        val target = evaluate(e, row)
        if target == null then false
        else values.map(evaluate(_, row)).exists(equalValues(target, _))

      // Type casting
      case Cast(e, targetType)          =>
        import lumina.plan.DataType.*
        val v = evaluate(e, row)
        if v == null then null
        else targetType match
          case Int32       => toDouble(v).toInt
          case Int64       => toDouble(v).toLong
          case Float64     => toDouble(v)
          case BooleanType => v match
            case b: Boolean => b
            case s: String  => s.toBoolean
            case n          => toDouble(n) != 0.0
          case StringType  => v.toString
          case Unknown     => v

      // Numeric functions
      case Abs(e)                       =>
        val v = evaluate(e, row)
        if v == null then null else math.abs(toDouble(v))
      case Round(e, scale)              =>
        val v = evaluate(e, row)
        if v == null then null
        else
          val factor = math.pow(10, scale)
          math.round(toDouble(v) * factor).toDouble / factor
      case Floor(e)                     =>
        val v = evaluate(e, row)
        if v == null then null else math.floor(toDouble(v))
      case Ceil(e)                      =>
        val v = evaluate(e, row)
        if v == null then null else math.ceil(toDouble(v))

      // Conditional
      case CaseWhen(branches, otherwise) =>
        branches
          .find { case (cond, _) => evaluatePredicate(cond, row) }
          .map  { case (_, value) => evaluate(value, row) }
          .orElse(otherwise.map(evaluate(_, row)))
          .orNull

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

  def equalValues(a: Any, b: Any): Boolean =
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
