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

  // Extended string functions
  /** Replaces every occurrence of `search` with `replacement` in a string. */
  final case class Replace(expr: Expression, search: String, replacement: String) extends Expression
  /**
   * Extracts the text of the `group`-th capturing group (1-based) from the
   * first match of `pattern` in the string.  Returns null when there is no
   * match or the value is null.
   */
  final case class RegexpExtract(expr: Expression, pattern: String, group: Int = 1) extends Expression
  /**
   * Replaces all sub-strings that match `pattern` with `replacement`.
   * The replacement string may reference captured groups via `$1`, `$2`, etc.
   */
  final case class RegexpReplace(expr: Expression, pattern: String, replacement: String) extends Expression
  /** Returns true when the string starts with `prefix`. */
  final case class StartsWith(expr: Expression, prefix: String) extends Expression
  /** Returns true when the string ends with `suffix`. */
  final case class EndsWith(expr: Expression, suffix: String) extends Expression
  /**
   * Left-pads the string to `length` characters using `pad`.
   * If the string is already at least `length` characters it is returned
   * unchanged.  `pad` defaults to a single space.
   */
  final case class LPad(expr: Expression, length: Int, pad: String = " ") extends Expression
  /**
   * Right-pads the string to `length` characters using `pad`.
   */
  final case class RPad(expr: Expression, length: Int, pad: String = " ") extends Expression
  /** Returns the string repeated `n` times. */
  final case class Repeat(expr: Expression, n: Int) extends Expression
  /** Returns the characters of the string in reverse order. */
  final case class Reverse(expr: Expression) extends Expression
  /**
   * Converts the first letter of each word to upper-case and the rest to
   * lower-case.  Mirrors SQL `INITCAP`.
   */
  final case class InitCap(expr: Expression) extends Expression

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
  // Hash functions
  /**
   * Returns the MD5 digest of the string representation of `expr` as a
   * 32-character lowercase hexadecimal string.  Returns null for null input.
   */
  final case class Md5(expr: Expression) extends Expression

  /**
   * Returns the SHA-256 digest of the string representation of `expr` as a
   * 64-character lowercase hexadecimal string.  Returns null for null input.
   */
  final case class Sha256(expr: Expression) extends Expression

  /** Returns the square root of a non-negative numeric expression. */
  final case class Sqrt(expr: Expression) extends Expression
  /** Raises `base` to the power of `exponent`. */
  final case class Power(base: Expression, exponent: Expression) extends Expression
  /** Returns the natural logarithm (base e) of a positive numeric expression. */
  final case class Log(expr: Expression) extends Expression
  /** Returns the base-2 logarithm of a positive numeric expression. */
  final case class Log2(expr: Expression) extends Expression
  /** Returns the base-10 logarithm of a positive numeric expression. */
  final case class Log10(expr: Expression) extends Expression
  /** Returns e raised to the power of the numeric expression. */
  final case class Exp(expr: Expression) extends Expression
  /** Returns the sign of the numeric expression: -1.0, 0.0, or 1.0. */
  final case class Sign(expr: Expression) extends Expression
  /** Returns the modulo (remainder) of dividing `dividend` by `divisor`. */
  final case class Mod(dividend: Expression, divisor: Expression) extends Expression
  /** Returns the greatest (maximum) value among two or more expressions. */
  final case class Greatest(exprs: Vector[Expression]) extends Expression
  /** Returns the least (minimum) value among two or more expressions. */
  final case class Least(exprs: Vector[Expression]) extends Expression

  // Date/time component extraction
  /** Extracts the 4-digit year from a date or timestamp value. */
  final case class Year(expr: Expression) extends Expression
  /** Extracts the month number (1–12) from a date or timestamp value. */
  final case class Month(expr: Expression) extends Expression
  /** Extracts the day of the month (1–31) from a date or timestamp value. */
  final case class Day(expr: Expression) extends Expression
  /** Extracts the hour (0–23) from a timestamp value. */
  final case class Hour(expr: Expression) extends Expression
  /** Extracts the minute (0–59) from a timestamp value. */
  final case class Minute(expr: Expression) extends Expression
  /** Extracts the second (0–59) from a timestamp value. */
  final case class Second(expr: Expression) extends Expression
  /**
   * Returns the ISO day-of-week: 1 = Monday, 7 = Sunday.
   * Matches Java's `DayOfWeek.getValue()` and SQL `ISODOW`.
   */
  final case class DayOfWeek(expr: Expression) extends Expression

  // Date arithmetic
  /**
   * Adds `days` days to a date or timestamp value.
   * `days` may be any numeric expression.
   */
  final case class DateAdd(date: Expression, days: Expression) extends Expression
  /**
   * Returns the number of whole days between `start` and `end` (end − start).
   * A positive result means `end` is after `start`.
   */
  final case class DateDiff(end: Expression, start: Expression) extends Expression

  // Date parsing and formatting
  /**
   * Parses a string in ISO-8601 date format ("yyyy-MM-dd") to a
   * `java.time.LocalDate` value.  Equivalent to SQL `CAST(expr AS DATE)`.
   */
  final case class ToDate(expr: Expression) extends Expression
  /**
   * Parses a string in ISO-8601 datetime format ("yyyy-MM-dd HH:mm:ss" or
   * "yyyy-MM-ddTHH:mm:ss") to a `java.time.LocalDateTime` value.
   * Equivalent to SQL `CAST(expr AS TIMESTAMP)`.
   */
  final case class ToTimestamp(expr: Expression) extends Expression
  /**
   * Formats a date or timestamp value as a string using a Java
   * `DateTimeFormatter` pattern (e.g., `"yyyy-MM-dd"`, `"dd/MM/yyyy"`).
   * In [[DuckDBBackend]] the pattern is converted to the equivalent
   * `STRFTIME` format string automatically.
   */
  final case class DateFormat(expr: Expression, format: String) extends Expression

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

  /**
   * Returns true when `expr` is between `lower` and `upper` (inclusive).
   * Equivalent to SQL `expr BETWEEN lower AND upper`.
   * Returns false when `expr` is null.
   */
  final case class Between(expr: Expression, lower: Expression, upper: Expression) extends Expression

  /**
   * Returns `thenExpr` when `condition` is true, otherwise `elseExpr`.
   * A concise two-branch conditional that compiles to `CASE WHEN … THEN … ELSE … END`.
   */
  final case class If(condition: Expression, thenExpr: Expression, elseExpr: Expression) extends Expression

  /**
   * Returns null when `expr` equals `nullValue`; otherwise returns `expr`.
   * Equivalent to SQL `NULLIF(expr, nullValue)`.
   */
  final case class NullIf(expr: Expression, nullValue: Expression) extends Expression

  /**
   * Returns `replacement` when `expr` is null; otherwise returns `expr`.
   * Equivalent to SQL `IFNULL(expr, replacement)` / `COALESCE(expr, replacement)`.
   */
  final case class IfNull(expr: Expression, replacement: Expression) extends Expression

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

  /**
   * Returns the first non-null value encountered in the group, in input order.
   * Returns null when the group is empty or all values are null.
   * Without an explicit sort the result is deterministic only when the input has
   * a stable row order (e.g. after a Sort node above a ReadCsv).
   */
  final case class First(column: Expression, alias: Option[String] = None) extends Aggregation

  /**
   * Returns the last non-null value encountered in the group, in input order.
   * Returns null when the group is empty or all values are null.
   */
  final case class Last(column: Expression, alias: Option[String] = None) extends Aggregation

  /**
   * Returns the median (middle value) of the non-null values in the group.
   * For an even number of non-null values the average of the two middle values
   * is returned (matching SQL `PERCENTILE_CONT(0.5)`).
   * Returns null when the group contains no non-null values.
   */
  final case class Median(column: Expression, alias: Option[String] = None) extends Aggregation

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
  def first(column: Expression, alias: String): Aggregation         = First(column, Some(alias))
  def first(column: Expression): Aggregation                        = First(column, None)
  def last(column: Expression, alias: String): Aggregation          = Last(column, Some(alias))
  def last(column: Expression): Aggregation                         = Last(column, None)
  def median(column: Expression, alias: String): Aggregation        = Median(column, Some(alias))
  def median(column: Expression): Aggregation                       = Median(column, None)
  def variance(column: Expression): Aggregation                     = Variance(column, None)
