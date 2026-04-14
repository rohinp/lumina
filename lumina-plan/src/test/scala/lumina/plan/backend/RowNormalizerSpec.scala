package lumina.plan.backend

import munit.FunSuite

/**
 * Tests for RowNormalizer — each test names the Java type being normalised and
 * the Scala type expected after normalisation.
 *
 * Read this file to understand what types DuckDB (or any JDBC backend) may
 * return and how they map to Scala primitives so that callers can write
 * backend-independent assertions.
 */
class RowNormalizerSpec extends FunSuite:

  private def normalize(v: Any): Any =
    RowNormalizer.normalize(Row(Map("v" -> v))).values("v")

  test("java.lang.Integer is normalised to Scala Int"):
    val result = normalize(java.lang.Integer.valueOf(42))
    assert(result.isInstanceOf[Int], s"Expected Int, got ${result.getClass}")
    assertEquals(result, 42)

  test("java.lang.Long is normalised to Scala Long"):
    val result = normalize(java.lang.Long.valueOf(100L))
    assert(result.isInstanceOf[Long], s"Expected Long, got ${result.getClass}")
    assertEquals(result, 100L)

  test("java.lang.Double is normalised to Scala Double"):
    val result = normalize(java.lang.Double.valueOf(3.14))
    assert(result.isInstanceOf[Double], s"Expected Double, got ${result.getClass}")
    assertEquals(result, 3.14)

  test("java.lang.Float is normalised to Scala Float"):
    val result = normalize(java.lang.Float.valueOf(1.5f))
    assert(result.isInstanceOf[Float], s"Expected Float, got ${result.getClass}")
    assertEquals(result.asInstanceOf[Float], 1.5f)

  test("java.lang.Boolean is normalised to Scala Boolean"):
    val result = normalize(java.lang.Boolean.TRUE)
    assert(result.isInstanceOf[Boolean], s"Expected Boolean, got ${result.getClass}")
    assertEquals(result, true)

  test("java.math.BigDecimal is normalised to Scala Double"):
    val result = normalize(java.math.BigDecimal.valueOf(99.99))
    assert(result.isInstanceOf[Double], s"Expected Double, got ${result.getClass}")
    assertEquals(result, 99.99)

  test("java.lang.Short is normalised to Scala Int"):
    val result = normalize(java.lang.Short.valueOf(7.toShort))
    assert(result.isInstanceOf[Int], s"Expected Int, got ${result.getClass}")
    assertEquals(result, 7)

  test("null is preserved as null"):
    assertEquals(normalize(null), null)

  test("String values pass through unchanged"):
    assertEquals(normalize("hello"), "hello")

  test("already-unboxed Scala Int passes through unchanged"):
    assertEquals(normalize(99), 99)

  test("normalizeAll applies normalisation to every row in a Vector"):
    val rows = Vector(
      Row(Map("a" -> java.lang.Integer.valueOf(1), "b" -> "Paris")),
      Row(Map("a" -> java.lang.Integer.valueOf(2), "b" -> "Berlin"))
    )
    val result = RowNormalizer.normalizeAll(rows)
    assert(result.forall(r => r.values("a").isInstanceOf[Int]))
