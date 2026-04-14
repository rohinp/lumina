package lumina.backend.local

import munit.FunSuite
import lumina.plan.*
import lumina.plan.Expression.*
import lumina.plan.backend.*

/**
 * Tests for M10 expressions in LocalBackend: string functions (Upper, Lower,
 * Trim, Length, Concat, Substring, Like), null handling (Coalesce), and set
 * membership (In).
 *
 * Each test exercises one expression in a realistic plan node (Filter or
 * WithColumn) so the full evaluation path is covered.
 */
class LocalBackendStringExprSpec extends FunSuite:

  private val rows = Vector(
    Row(Map("name" -> "  Alice ", "city" -> "Paris",   "score" -> 90)),
    Row(Map("name" -> "bob",      "city" -> "berlin",  "score" -> 70)),
    Row(Map("name" -> null,       "city" -> "London",  "score" -> 85)),
    Row(Map("name" -> "Charlie",  "city" -> "Paris",   "score" -> 60))
  )

  private val backend = LocalBackend(DataRegistry.of("memory://t" -> rows))
  private val src     = ReadCsv("memory://t", None)

  private def run(plan: LogicalPlan): Vector[Row] =
    backend.execute(plan) match
      case BackendResult.InMemory(rs) => rs

  // ---------------------------------------------------------------------------
  // Upper / Lower
  // ---------------------------------------------------------------------------

  test("Upper converts a string column value to upper-case"):
    val plan   = Project(src, Vector(Alias(Upper(ColumnRef("city")), "upper_city")), None)
    val result = run(plan)
    assertEquals(result.map(_.values("upper_city")), Vector("PARIS", "BERLIN", "LONDON", "PARIS"))

  test("Lower converts a string column value to lower-case"):
    val plan   = Project(src, Vector(Alias(Lower(ColumnRef("name")), "lower_name")), None)
    val result = run(plan)
    assertEquals(result(0).values("lower_name"), "  alice ")
    assertEquals(result(1).values("lower_name"), "bob")

  test("Upper on a null value returns null"):
    val plan   = Project(src, Vector(Alias(Upper(ColumnRef("name")), "u")), None)
    val result = run(plan)
    assertEquals(result(2).values("u"), null) // row with null name

  // ---------------------------------------------------------------------------
  // Trim
  // ---------------------------------------------------------------------------

  test("Trim removes leading and trailing whitespace"):
    val plan   = Project(src, Vector(Alias(Trim(ColumnRef("name")), "trimmed")), None)
    val result = run(plan)
    assertEquals(result(0).values("trimmed"), "Alice")
    assertEquals(result(1).values("trimmed"), "bob")

  // ---------------------------------------------------------------------------
  // Length
  // ---------------------------------------------------------------------------

  test("Length returns the character count of a string"):
    val plan   = Project(src, Vector(Alias(Length(ColumnRef("city")), "len")), None)
    val result = run(plan)
    assertEquals(result(0).values("len"), 5)  // "Paris"
    assertEquals(result(1).values("len"), 6)  // "berlin"
    assertEquals(result(2).values("len"), 6)  // "London"

  // ---------------------------------------------------------------------------
  // Concat
  // ---------------------------------------------------------------------------

  test("Concat joins two string expressions"):
    val plan   = Project(src, Vector(
      Alias(Concat(Vector(ColumnRef("name"), Literal(": "), ColumnRef("city"))), "label")
    ), None)
    val result = run(plan)
    assertEquals(result(1).values("label"), "bob: berlin")

  test("Concat returns null when any argument is null"):
    val plan   = Project(src, Vector(
      Alias(Concat(Vector(ColumnRef("name"), Literal("!"))) , "labeled")
    ), None)
    val result = run(plan)
    assertEquals(result(2).values("labeled"), null) // null name

  // ---------------------------------------------------------------------------
  // Substring
  // ---------------------------------------------------------------------------

  test("Substring extracts characters using 1-based start index"):
    val plan   = Project(src, Vector(Alias(Substring(ColumnRef("city"), 1, 3), "sub")), None)
    val result = run(plan)
    assertEquals(result(0).values("sub"), "Par") // "Paris"[1..3]
    assertEquals(result(2).values("sub"), "Lon") // "London"[1..3]

  test("Substring with start beyond string length returns empty string"):
    val plan   = Project(src, Vector(Alias(Substring(ColumnRef("city"), 100, 3), "sub")), None)
    val result = run(plan)
    assertEquals(result(0).values("sub"), "")

  // ---------------------------------------------------------------------------
  // Like
  // ---------------------------------------------------------------------------

  test("Like with a percent wildcard filters rows matching the pattern"):
    val plan   = Filter(src, Like(ColumnRef("city"), "P%"))
    val result = run(plan)
    assertEquals(result.size, 2)
    assert(result.forall(_.values("city").asInstanceOf[String].startsWith("P")))

  test("Like with underscore wildcard matches exactly one character"):
    val plan   = Filter(src, Like(ColumnRef("city"), "P_ris"))
    val result = run(plan)
    assertEquals(result.size, 2)
    assert(result.forall(_.values("city") == "Paris"))

  test("Like returns false for a null column value"):
    val plan   = Filter(src, Like(ColumnRef("name"), "%"))
    val result = run(plan)
    // null name row should be excluded
    assert(result.forall(_.values("name") != null))

  // ---------------------------------------------------------------------------
  // Coalesce
  // ---------------------------------------------------------------------------

  test("Coalesce returns the first non-null expression value"):
    val plan   = Project(src, Vector(
      Alias(Coalesce(Vector(ColumnRef("name"), Literal("UNKNOWN"))), "safe_name")
    ), None)
    val result = run(plan)
    assertEquals(result(0).values("safe_name"), "  Alice ")
    assertEquals(result(2).values("safe_name"), "UNKNOWN") // null name → fallback

  test("Coalesce returns null when all expressions are null"):
    val plan   = Project(src, Vector(
      Alias(Coalesce(Vector(ColumnRef("name"), ColumnRef("name"))), "n")
    ), None)
    val result = run(plan)
    assertEquals(result(2).values("n"), null) // both are null

  // ---------------------------------------------------------------------------
  // In
  // ---------------------------------------------------------------------------

  test("In keeps rows where the column value matches any listed value"):
    val plan   = Filter(src, In(ColumnRef("city"), Vector(Literal("Paris"), Literal("London"))))
    val result = run(plan)
    assertEquals(result.size, 3)
    assert(result.forall(r => r.values("city") == "Paris" || r.values("city") == "London"))

  test("In returns false for a null column value"):
    val plan   = Filter(src, In(ColumnRef("name"), Vector(Literal("Alice"), Literal(null))))
    val result = run(plan)
    // null name row is excluded even if null is listed as a value
    assert(result.forall(_.values("name") != null))

  test("In with an empty values list matches no rows"):
    val plan   = Filter(src, In(ColumnRef("city"), Vector.empty))
    val result = run(plan)
    assertEquals(result.size, 0)
