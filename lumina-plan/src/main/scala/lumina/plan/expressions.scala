package lumina.plan

sealed trait Expression

object Expression:
  final case class ColumnRef(name: String) extends Expression
  final case class Literal(value: Any)     extends Expression

  // Comparison
  final case class GreaterThan(left: Expression, right: Expression)        extends Expression
  final case class GreaterThanOrEqual(left: Expression, right: Expression) extends Expression
  final case class LessThan(left: Expression, right: Expression)           extends Expression
  final case class LessThanOrEqual(left: Expression, right: Expression)    extends Expression
  final case class EqualTo(left: Expression, right: Expression)            extends Expression
  final case class NotEqualTo(left: Expression, right: Expression)         extends Expression

  // Logical combinators
  final case class And(left: Expression, right: Expression) extends Expression
  final case class Or(left: Expression, right: Expression)  extends Expression
  final case class Not(expr: Expression)                    extends Expression

  // Null checks
  final case class IsNull(expr: Expression)    extends Expression
  final case class IsNotNull(expr: Expression) extends Expression

  // Arithmetic
  final case class Add(left: Expression, right: Expression)      extends Expression
  final case class Subtract(left: Expression, right: Expression) extends Expression
  final case class Multiply(left: Expression, right: Expression) extends Expression
  final case class Divide(left: Expression, right: Expression)   extends Expression
  final case class Negate(expr: Expression)                      extends Expression

  // Named derived column — wraps any expression and gives it an output name
  final case class Alias(expr: Expression, name: String) extends Expression

  // String functions
  /** Converts a string value to upper-case. */
  final case class Upper(expr: Expression) extends Expression
  /** Converts a string value to lower-case. */
  final case class Lower(expr: Expression) extends Expression
  /** Removes leading and trailing whitespace. */
  final case class Trim(expr: Expression) extends Expression
  /** Returns the number of characters in a string. */
  final case class Length(expr: Expression) extends Expression
  /** Concatenates two or more string expressions. */
  final case class Concat(exprs: Vector[Expression]) extends Expression
  /**
   * Extracts a substring.  `start` is 1-based (SQL convention); `len` is the
   * maximum number of characters to return.
   */
  final case class Substring(expr: Expression, start: Int, len: Int) extends Expression
  /**
   * SQL LIKE pattern match.  `%` matches any sequence of characters; `_`
   * matches exactly one character.  Case-sensitive on both backends.
   */
  final case class Like(expr: Expression, pattern: String) extends Expression

  // Null handling
  /**
   * Returns the first non-null value from the supplied expressions, or null if
   * all are null.  Mirrors SQL `COALESCE(e1, e2, ...)`.
   */
  final case class Coalesce(exprs: Vector[Expression]) extends Expression

  // Set membership
  /**
   * Returns true when `expr` equals any value in `values`.
   * Mirrors SQL `col IN (v1, v2, ...)`.
   */
  final case class In(expr: Expression, values: Vector[Expression]) extends Expression

  // Type casting
  /**
   * Explicitly converts `expr` to the given [[DataType]].
   *
   * In [[LocalBackend]] the cast is performed in Scala; in [[DuckDBBackend]]
   * it maps to `CAST(expr AS sqlType)`.
   */
  final case class Cast(expr: Expression, targetType: DataType) extends Expression

  // Numeric functions
  /** Returns the absolute value of a numeric expression. */
  final case class Abs(expr: Expression) extends Expression
  /** Rounds a numeric expression to `scale` decimal places. */
  final case class Round(expr: Expression, scale: Int = 0) extends Expression
  /** Returns the largest integer not greater than the expression value. */
  final case class Floor(expr: Expression) extends Expression
  /** Returns the smallest integer not less than the expression value. */
  final case class Ceil(expr: Expression) extends Expression

  // Conditional
  /**
   * Evaluates each `(condition, value)` branch in order and returns the value
   * from the first branch whose condition is true.  Returns `otherwise` if no
   * branch matches, or null when `otherwise` is absent.
   *
   * Mirrors SQL `CASE WHEN c1 THEN v1 WHEN c2 THEN v2 … ELSE default END`.
   *
   * {{{
   *   CaseWhen(
   *     branches  = Vector(GreaterThan(col("score"), Literal(90)) -> Literal("A"),
   *                        GreaterThan(col("score"), Literal(70)) -> Literal("B")),
   *     otherwise = Some(Literal("C"))
   *   )
   * }}}
   */
  final case class CaseWhen(
      branches:  Vector[(Expression, Expression)],
      otherwise: Option[Expression] = None
  ) extends Expression

sealed trait Aggregation

object Aggregation:
  final case class Sum(column: Expression,  alias: Option[String] = None) extends Aggregation
  final case class Count(column: Option[Expression], alias: Option[String] = None) extends Aggregation
  final case class Avg(column: Expression,  alias: Option[String] = None) extends Aggregation
  final case class Min(column: Expression,  alias: Option[String] = None) extends Aggregation
  final case class Max(column: Expression,  alias: Option[String] = None) extends Aggregation

  /** Counts distinct non-null values.  Maps to SQL `COUNT(DISTINCT col)`. */
  final case class CountDistinct(column: Expression, alias: Option[String] = None) extends Aggregation

  /**
   * Sample standard deviation (divides by N−1).  Returns null when the group
   * contains fewer than two non-null values, matching SQL `STDDEV(col)`.
   */
  final case class StdDev(column: Expression, alias: Option[String] = None) extends Aggregation

  /**
   * Sample variance (divides by N−1).  Returns null when the group contains
   * fewer than two non-null values, matching SQL `VARIANCE(col)`.
   */
  final case class Variance(column: Expression, alias: Option[String] = None) extends Aggregation

  // Java/Kotlin-friendly factories — return Aggregation (not the subtype) so
  // java.util.List.of(Aggregation.sum(...)) infers List<Aggregation> in Kotlin.

  def sum(column: Expression, alias: String): Aggregation          = Sum(column, Some(alias))
  def sum(column: Expression): Aggregation                         = Sum(column, None)
  def avg(column: Expression, alias: String): Aggregation          = Avg(column, Some(alias))
  def avg(column: Expression): Aggregation                         = Avg(column, None)
  def min(column: Expression, alias: String): Aggregation          = Min(column, Some(alias))
  def min(column: Expression): Aggregation                         = Min(column, None)
  def max(column: Expression, alias: String): Aggregation          = Max(column, Some(alias))
  def max(column: Expression): Aggregation                         = Max(column, None)
  def countAll(alias: String): Aggregation                         = Count(None, Some(alias))
  def count(column: Expression, alias: String): Aggregation        = Count(Some(column), Some(alias))
  def countDistinct(column: Expression, alias: String): Aggregation = CountDistinct(column, Some(alias))
  def countDistinct(column: Expression): Aggregation                = CountDistinct(column, None)
  def stddev(column: Expression, alias: String): Aggregation        = StdDev(column, Some(alias))
  def stddev(column: Expression): Aggregation                       = StdDev(column, None)
  def variance(column: Expression, alias: String): Aggregation      = Variance(column, Some(alias))
  def variance(column: Expression): Aggregation                     = Variance(column, None)
