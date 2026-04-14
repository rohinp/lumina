package lumina.backend.duckdb

import munit.FunSuite
import lumina.plan.*
import lumina.plan.Expression.*
import lumina.plan.backend.*

/**
 * Tests for M10 expressions in DuckDBBackend: string functions, Coalesce, and
 * In.  Mirrors LocalBackendStringExprSpec to verify both backends agree.
 */
class DuckDBStringExprSpec extends FunSuite:

  private val rows = Vector(
    Row(Map("name" -> "  Alice ", "city" -> "Paris",  "score" -> 90)),
    Row(Map("name" -> "bob",      "city" -> "berlin", "score" -> 70)),
    Row(Map("name" -> null,       "city" -> "London", "score" -> 85)),
    Row(Map("name" -> "Charlie",  "city" -> "Paris",  "score" -> 60))
  )

  private val backend = DuckDBBackend(DataRegistry.of("memory://t" -> rows))
  private val src     = ReadCsv("memory://t", None)

  private def run(plan: LogicalPlan): Vector[Row] =
    backend.execute(plan) match
      case BackendResult.InMemory(rs) => rs

  // ---------------------------------------------------------------------------
  // Upper / Lower
  // ---------------------------------------------------------------------------

  test("Upper converts a string column to upper-case"):
    val plan   = Project(src, Vector(Alias(Upper(ColumnRef("city")), "u")), None)
    val result = run(plan)
    assertEquals(result.map(_.values("u")), Vector("PARIS", "BERLIN", "LONDON", "PARIS"))

  test("Lower converts a string column to lower-case"):
    val plan   = Project(src, Vector(Alias(Lower(ColumnRef("city")), "l")), None)
    val result = run(plan)
    assert(result.forall(r => r.values("l").asInstanceOf[String] == r.values("l").asInstanceOf[String].toLowerCase))

  // ---------------------------------------------------------------------------
  // Trim
  // ---------------------------------------------------------------------------

  test("Trim removes leading and trailing whitespace"):
    val plan   = Project(src, Vector(Alias(Trim(ColumnRef("name")), "t")), None)
    // Only check the non-null rows; DuckDB returns null for the null row
    val result = run(plan).filter(_.values("t") != null)
    assert(result.forall(r => r.values("t").asInstanceOf[String] == r.values("t").asInstanceOf[String].trim))

  // ---------------------------------------------------------------------------
  // Length
  // ---------------------------------------------------------------------------

  test("Length returns the character count of a string"):
    val plan   = Project(src, Vector(Alias(Length(ColumnRef("city")), "len")), None)
    val result = run(plan)
    val lens   = result.map(_.values("len").asInstanceOf[Number].intValue())
    assertEquals(lens, Vector(5, 6, 6, 5))

  // ---------------------------------------------------------------------------
  // Concat
  // ---------------------------------------------------------------------------

  test("Concat joins two string expressions"):
    val plan   = Filter(
      Project(src, Vector(Alias(Concat(Vector(ColumnRef("city"), Literal("!"))), "tagged")), None),
      EqualTo(ColumnRef("tagged"), Literal("Paris!"))
    )
    val result = run(plan)
    assertEquals(result.size, 2)

  // ---------------------------------------------------------------------------
  // Substring
  // ---------------------------------------------------------------------------

  test("Substring extracts characters using 1-based start index"):
    val plan   = Project(src, Vector(Alias(Substring(ColumnRef("city"), 1, 3), "sub")), None)
    val result = run(plan)
    assertEquals(result(0).values("sub"), "Par")
    assertEquals(result(2).values("sub"), "Lon")

  // ---------------------------------------------------------------------------
  // Like
  // ---------------------------------------------------------------------------

  test("Like with a percent wildcard filters matching rows"):
    val plan   = Filter(src, Like(ColumnRef("city"), "P%"))
    val result = run(plan)
    assertEquals(result.size, 2)

  // ---------------------------------------------------------------------------
  // Coalesce
  // ---------------------------------------------------------------------------

  test("Coalesce returns the first non-null value"):
    val plan   = Project(src, Vector(
      Alias(Coalesce(Vector(ColumnRef("name"), Literal("UNKNOWN"))), "safe")
    ), None)
    val result = run(plan)
    assertEquals(result(2).values("safe"), "UNKNOWN")

  // ---------------------------------------------------------------------------
  // In
  // ---------------------------------------------------------------------------

  test("In keeps rows where the column matches any listed value"):
    val plan   = Filter(src, In(ColumnRef("city"), Vector(Literal("Paris"), Literal("London"))))
    val result = run(plan)
    assertEquals(result.size, 3)

  test("In with an empty values list matches no rows"):
    val plan   = Filter(src, In(ColumnRef("city"), Vector.empty))
    val result = run(plan)
    assertEquals(result.size, 0)
