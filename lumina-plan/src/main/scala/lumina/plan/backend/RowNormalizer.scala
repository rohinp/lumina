package lumina.plan.backend

/**
 * Normalises the value types in a [[Row]] to Scala primitives.
 *
 * JDBC drivers (DuckDB included) return column values as boxed Java types
 * (`java.lang.Integer`, `java.lang.Double`, `java.math.BigDecimal`, …).
 * Without normalisation, callers would need to write backend-specific casts
 * and tests would pass on LocalBackend but fail on DuckDB (or vice versa).
 *
 * [[normalize]] maps every value in a row to the canonical Scala type:
 *
 *   java.lang.Integer   → Int
 *   java.lang.Long      → Long
 *   java.lang.Double    → Double
 *   java.lang.Float     → Float
 *   java.lang.Boolean   → Boolean
 *   java.lang.Short     → Int
 *   java.lang.Byte      → Int
 *   java.math.BigDecimal → Double
 *   null                → null (preserved)
 *   everything else     → unchanged
 */
object RowNormalizer:

  def normalize(row: Row): Row =
    Row(row.values.map { case (k, v) => k -> normalizeValue(v) })

  def normalizeAll(rows: Vector[Row]): Vector[Row] =
    rows.map(normalize)

  private def normalizeValue(v: Any): Any = v match
    case n: java.lang.Integer    => n.intValue()
    case n: java.lang.Long       => n.longValue()
    case n: java.lang.Double     => n.doubleValue()
    case n: java.lang.Float      => n.floatValue()
    case n: java.lang.Boolean    => n.booleanValue()
    case n: java.lang.Short      => n.intValue()
    case n: java.lang.Byte       => n.intValue()
    case n: java.math.BigDecimal => n.doubleValue()
    case other                   => other   // String, null, already-unboxed, etc.
