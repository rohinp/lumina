package lumina.backend.duckdb

import munit.FunSuite
import lumina.plan.*
import lumina.plan.Expression.*
import lumina.plan.backend.*

/**
 * Tests for M19 conditional expressions in DuckDBBackend: Between, If,
 * NullIf, IfNull.
 *
 * Mirrors LocalBackendConditionalExtSpec to verify both backends agree.
 */
class DuckDBConditionalExtSpec extends FunSuite:

  private val rows = Vector(
    Row(Map("score" -> 75,  "a" -> "hello", "b" -> "hello")),
    Row(Map("score" -> 50,  "a" -> "foo",   "b" -> "bar")),
    Row(Map("score" -> 100, "a" -> null,    "b" -> "x")),
    Row(Map("score" -> null, "a" -> null,   "b" -> null))
  )

  private val backend = DuckDBBackend(DataRegistry.of("memory://t" -> rows))
  private val src     = ReadCsv("memory://t", None)

  private def project(expr: Expression): Vector[Any] =
    val plan = Project(src, Vector(Alias(expr, "v")), None)
    backend.execute(plan) match
      case BackendResult.InMemory(rs) => rs.map(_.values("v"))

  // ---------------------------------------------------------------------------
  // Between
  // ---------------------------------------------------------------------------

  test("Between returns true when the value is within the inclusive range"):
    val result = project(Between(ColumnRef("score"), Literal(60), Literal(80)))
    assertEquals(result(0), true)
    assertEquals(result(1), false)
    assertEquals(result(2), false)

  test("Between returns true at the lower boundary"):
    val result = project(Between(ColumnRef("score"), Literal(75), Literal(100)))
    assertEquals(result(0), true)

  test("Between returns true at the upper boundary"):
    val result = project(Between(ColumnRef("score"), Literal(50), Literal(75)))
    assertEquals(result(0), true)

  test("Between returns false for null input"):
    val result = project(Between(ColumnRef("score"), Literal(0), Literal(100)))
    assertEquals(result(3), false)

  test("Between can be used in a Filter to keep rows in range"):
    val plan   = Filter(src, Between(ColumnRef("score"), Literal(60), Literal(90)))
    val result = backend.execute(plan) match
      case BackendResult.InMemory(rs) => rs
    assertEquals(result.size, 1)
    assertEquals(result(0).values("score").toString.toInt, 75)

  // ---------------------------------------------------------------------------
  // If
  // ---------------------------------------------------------------------------

  test("If returns the then-expression when the condition is true"):
    val result = project(If(GreaterThan(ColumnRef("score"), Literal(60)), Literal("pass"), Literal("fail")))
    assertEquals(result(0), "pass")
    assertEquals(result(1), "fail")
    assertEquals(result(2), "pass")

  test("If returns the else-expression when the condition is false"):
    val result = project(If(EqualTo(ColumnRef("a"), Literal("hello")), Literal(1), Literal(0)))
    assertEquals(result(0).toString.toInt, 1)
    assertEquals(result(1).toString.toInt, 0)

  test("If with a null condition returns the else-expression"):
    val result = project(If(GreaterThan(ColumnRef("score"), Literal(0)), Literal("y"), Literal("n")))
    assertEquals(result(3), "n")

  // ---------------------------------------------------------------------------
  // NullIf
  // ---------------------------------------------------------------------------

  test("NullIf returns null when the expression equals the sentinel value"):
    val result = project(NullIf(ColumnRef("a"), Literal("hello")))
    assertEquals(result(0), null)   // "hello" = "hello" → null
    assertEquals(result(1), "foo")  // "foo" ≠ "hello" → kept

  test("NullIf returns the original value when it does not match the sentinel"):
    val result = project(NullIf(ColumnRef("a"), Literal("missing")))
    assertEquals(result(0), "hello")

  test("NullIf returns null when the input is already null"):
    val result = project(NullIf(ColumnRef("score"), Literal(0)))
    assertEquals(result(3), null)

  // ---------------------------------------------------------------------------
  // IfNull
  // ---------------------------------------------------------------------------

  test("IfNull returns the replacement when the expression is null"):
    val result = project(IfNull(ColumnRef("a"), Literal("unknown")))
    assertEquals(result(0), "hello")
    assertEquals(result(2), "unknown")
    assertEquals(result(3), "unknown")

  test("IfNull returns the original value when it is not null"):
    val result = project(IfNull(ColumnRef("a"), Literal("default")))
    assertEquals(result(0), "hello")
    assertEquals(result(1), "foo")

  test("IfNull with a null fallback still returns null when both are null"):
    val result = project(IfNull(ColumnRef("a"), ColumnRef("b")))
    assertEquals(result(0), "hello")
    assertEquals(result(2), "x")
    assertEquals(result(3), null)
