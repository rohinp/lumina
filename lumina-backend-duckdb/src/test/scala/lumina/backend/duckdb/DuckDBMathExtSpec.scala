package lumina.backend.duckdb

import munit.FunSuite
import lumina.plan.*
import lumina.plan.Expression.*
import lumina.plan.backend.*

/**
 * Tests for M18 extended math functions in DuckDBBackend: Sqrt, Power,
 * Log, Log2, Log10, Exp, Sign, Mod, Greatest, Least.
 *
 * Mirrors LocalBackendMathExtSpec to verify both backends agree.
 */
class DuckDBMathExtSpec extends FunSuite:

  private val rows = Vector(
    Row(Map("x" -> 4.0,   "y" -> 2.0,  "neg" -> -9.0)),
    Row(Map("x" -> 100.0, "y" -> 3.0,  "neg" -> 0.0)),
    Row(Map("x" -> 1.0,   "y" -> 0.5,  "neg" -> 25.0)),
    Row(Map("x" -> null,  "y" -> null,  "neg" -> null))
  )

  private val backend = DuckDBBackend(DataRegistry.of("memory://t" -> rows))
  private val src     = ReadCsv("memory://t", None)

  private def project(expr: Expression): Vector[Any] =
    val plan = Project(src, Vector(Alias(expr, "v")), None)
    backend.execute(plan) match
      case BackendResult.InMemory(rs) => rs.map(_.values("v"))

  private def approx(a: Any, expected: Double, eps: Double = 1e-9): Boolean =
    math.abs(a.asInstanceOf[Double] - expected) < eps

  // ---------------------------------------------------------------------------
  // Sqrt
  // ---------------------------------------------------------------------------

  test("Sqrt returns the square root of a non-negative value"):
    val result = project(Sqrt(ColumnRef("x")))
    assert(approx(result(0), 2.0))
    assert(approx(result(1), 10.0))
    assert(approx(result(2), 1.0))

  test("Sqrt propagates null"):
    val result = project(Sqrt(ColumnRef("x")))
    assertEquals(result(3), null)

  // ---------------------------------------------------------------------------
  // Power
  // ---------------------------------------------------------------------------

  test("Power raises the base to the given exponent"):
    val result = project(Power(ColumnRef("x"), ColumnRef("y")))
    assert(approx(result(0), 16.0))
    assert(approx(result(1), 1e6))
    assert(approx(result(2), 1.0))

  test("Power with a literal exponent works"):
    val result = project(Power(ColumnRef("x"), Literal(3.0)))
    assert(approx(result(0), 64.0))

  test("Power propagates null when base is null"):
    val result = project(Power(ColumnRef("x"), Literal(2.0)))
    assertEquals(result(3), null)

  // ---------------------------------------------------------------------------
  // Log (natural logarithm)
  // ---------------------------------------------------------------------------

  test("Log returns the natural logarithm of a positive value"):
    val result = project(Log(ColumnRef("x")))
    assert(approx(result(2), 0.0))
    assert(approx(result(1), math.log(100)))

  test("Log propagates null"):
    val result = project(Log(ColumnRef("x")))
    assertEquals(result(3), null)

  // ---------------------------------------------------------------------------
  // Log2
  // ---------------------------------------------------------------------------

  test("Log2 returns the base-2 logarithm"):
    val result = project(Log2(ColumnRef("x")))
    assert(approx(result(0), 2.0))
    assert(approx(result(2), 0.0))

  test("Log2 propagates null"):
    val result = project(Log2(ColumnRef("x")))
    assertEquals(result(3), null)

  // ---------------------------------------------------------------------------
  // Log10
  // ---------------------------------------------------------------------------

  test("Log10 returns the base-10 logarithm"):
    val result = project(Log10(ColumnRef("x")))
    assert(approx(result(1), 2.0))
    assert(approx(result(2), 0.0))

  test("Log10 propagates null"):
    val result = project(Log10(ColumnRef("x")))
    assertEquals(result(3), null)

  // ---------------------------------------------------------------------------
  // Exp
  // ---------------------------------------------------------------------------

  test("Exp of 0.0 returns 1.0"):
    val result = project(Exp(Literal(0.0)))
    assert(approx(result(0), 1.0))

  test("Exp of 1.0 returns Euler's number"):
    val result = project(Exp(Literal(1.0)))
    assert(approx(result(0), math.E, eps = 1e-7))

  test("Exp propagates null"):
    val result = project(Exp(ColumnRef("x")))
    assertEquals(result(3), null)

  // ---------------------------------------------------------------------------
  // Sign
  // ---------------------------------------------------------------------------

  test("Sign returns 1.0 for positive values"):
    val result = project(Sign(ColumnRef("x")))
    assertEquals(result(0), 1.0)
    assertEquals(result(1), 1.0)

  test("Sign returns -1.0 for negative values"):
    val result = project(Sign(ColumnRef("neg")))
    assertEquals(result(0), -1.0)

  test("Sign returns 0.0 for zero"):
    val result = project(Sign(ColumnRef("neg")))
    assertEquals(result(1), 0.0)

  test("Sign propagates null"):
    val result = project(Sign(ColumnRef("x")))
    assertEquals(result(3), null)

  // ---------------------------------------------------------------------------
  // Mod
  // ---------------------------------------------------------------------------

  test("Mod returns the remainder of integer division"):
    val result = project(Mod(Literal(10.0), Literal(3.0)))
    assert(approx(result(0), 1.0))

  test("Mod of column values works"):
    val result = project(Mod(ColumnRef("x"), ColumnRef("y")))
    assert(approx(result(0), 0.0))
    assert(approx(result(1), 1.0))

  test("Mod propagates null when dividend is null"):
    val result = project(Mod(ColumnRef("x"), Literal(3.0)))
    assertEquals(result(3), null)

  // ---------------------------------------------------------------------------
  // Greatest
  // ---------------------------------------------------------------------------

  test("Greatest returns the maximum value among the expressions"):
    val result = project(Greatest(Vector(ColumnRef("x"), ColumnRef("y"), Literal(5.0))))
    assertEquals(result(0), 5.0)
    assertEquals(result(1), 100.0)

  test("Greatest returns null when all arguments are null"):
    val result = project(Greatest(Vector(ColumnRef("x"), ColumnRef("y"))))
    assertEquals(result(3), null)

  // ---------------------------------------------------------------------------
  // Least
  // ---------------------------------------------------------------------------

  test("Least returns the minimum value among the expressions"):
    val result = project(Least(Vector(ColumnRef("x"), ColumnRef("y"), Literal(5.0))))
    assertEquals(result(0), 2.0)
    assertEquals(result(1), 3.0)

  test("Least returns null when all arguments are null"):
    val result = project(Least(Vector(ColumnRef("x"), ColumnRef("y"))))
    assertEquals(result(3), null)
