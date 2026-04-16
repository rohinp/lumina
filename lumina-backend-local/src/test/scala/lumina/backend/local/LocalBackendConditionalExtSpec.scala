package lumina.backend.local

import munit.FunSuite
import lumina.plan.*
import lumina.plan.Expression.*
import lumina.plan.backend.*

/**
 * Tests for M19 conditional expressions in LocalBackend: Between, If,
 * NullIf, IfNull.
 *
 * Read top-to-bottom as a specification for each expression's behaviour.
 */
class LocalBackendConditionalExtSpec extends FunSuite:

  private val rows = Vector(
    Row(Map("score" -> 75,  "a" -> "hello", "b" -> "hello")),
    Row(Map("score" -> 50,  "a" -> "foo",   "b" -> "bar")),
    Row(Map("score" -> 100, "a" -> null,    "b" -> "x")),
    Row(Map("score" -> null, "a" -> null,   "b" -> null))
  )

  private val backend = LocalBackend(DataRegistry.of("memory://t" -> rows))
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
    assertEquals(result(0), true)   // 75 is in [60, 80]
    assertEquals(result(1), false)  // 50 is below 60
    assertEquals(result(2), false)  // 100 is above 80

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
    assertEquals(result(0).values("score"), 75)

  // ---------------------------------------------------------------------------
  // If
  // ---------------------------------------------------------------------------

  test("If returns the then-expression when the condition is true"):
    val result = project(If(GreaterThan(ColumnRef("score"), Literal(60)), Literal("pass"), Literal("fail")))
    assertEquals(result(0), "pass")  // 75 > 60
    assertEquals(result(1), "fail")  // 50 not > 60
    assertEquals(result(2), "pass")  // 100 > 60

  test("If returns the else-expression when the condition is false"):
    val result = project(If(EqualTo(ColumnRef("a"), Literal("hello")), Literal(1), Literal(0)))
    assertEquals(result(0), 1)
    assertEquals(result(1), 0)

  test("If with a null condition treats null as false and returns the else-expression"):
    // score is null in row 3; null predicate → false → else branch
    val result = project(If(GreaterThan(ColumnRef("score"), Literal(0)), Literal("y"), Literal("n")))
    assertEquals(result(3), "n")

  test("If can produce a null result from the then or else branch"):
    val result = project(If(EqualTo(ColumnRef("a"), Literal("hello")), ColumnRef("a"), Literal(null)))
    assertEquals(result(0), "hello")
    assertEquals(result(1), null)

  // ---------------------------------------------------------------------------
  // NullIf
  // ---------------------------------------------------------------------------

  test("NullIf returns null when the expression equals the sentinel value"):
    val result = project(NullIf(ColumnRef("score"), Literal(50)))
    assertEquals(result(0), 75)    // 75 ≠ 50 → returned as-is
    assertEquals(result(1), null)  // 50 = 50 → null

  test("NullIf returns the original value when it does not match the sentinel"):
    val result = project(NullIf(ColumnRef("a"), Literal("missing")))
    assertEquals(result(0), "hello")

  test("NullIf returns null when the input is already null"):
    // null is not equal to anything (equalValues(null, 50) = false), so null is returned as null
    val result = project(NullIf(ColumnRef("score"), Literal(0)))
    assertEquals(result(3), null)

  test("NullIf can be used to blank out a known placeholder value"):
    val result = project(NullIf(ColumnRef("a"), Literal("hello")))
    assertEquals(result(0), null)   // "hello" is the sentinel → null
    assertEquals(result(1), "foo")  // "foo" ≠ "hello" → kept

  // ---------------------------------------------------------------------------
  // IfNull
  // ---------------------------------------------------------------------------

  test("IfNull returns the replacement when the expression is null"):
    val result = project(IfNull(ColumnRef("a"), Literal("unknown")))
    assertEquals(result(0), "hello")    // non-null → kept
    assertEquals(result(2), "unknown")  // null → replacement
    assertEquals(result(3), "unknown")  // null → replacement

  test("IfNull returns the original value when it is not null"):
    val result = project(IfNull(ColumnRef("score"), Literal(-1)))
    assertEquals(result(0), 75)
    assertEquals(result(1), 50)

  test("IfNull with a null replacement still returns null when input is null"):
    val result = project(IfNull(ColumnRef("a"), ColumnRef("b")))
    assertEquals(result(0), "hello")   // a non-null
    assertEquals(result(2), "x")       // a null, b = "x"
    assertEquals(result(3), null)      // both null

  test("IfNull is equivalent to a two-argument Coalesce"):
    val ifNullResult   = project(IfNull(ColumnRef("a"), Literal("default")))
    val coalesceResult = project(Coalesce(Vector(ColumnRef("a"), Literal("default"))))
    assertEquals(ifNullResult, coalesceResult)
