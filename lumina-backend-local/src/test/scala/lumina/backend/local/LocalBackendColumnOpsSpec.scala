package lumina.backend.local

import munit.FunSuite
import lumina.plan.*
import lumina.plan.Expression.*
import lumina.plan.backend.*

/**
 * Tests for M11 column operations in LocalBackend: DropColumns, RenameColumn.
 * The DataFrame convenience methods dropNa and fillNa are tested via the API.
 *
 * Read top-to-bottom to understand the contract of each operation.
 */
class LocalBackendColumnOpsSpec extends FunSuite:

  private val rows = Vector(
    Row(Map("city" -> "Paris",  "age" -> 35, "revenue" -> 1000.0, "email" -> "a@b.com")),
    Row(Map("city" -> "Berlin", "age" -> 29, "revenue" -> 2000.0, "email" -> null)),
    Row(Map("city" -> "London", "age" -> 40, "revenue" -> null,   "email" -> "c@d.com"))
  )

  private val backend = LocalBackend(DataRegistry.of("memory://t" -> rows))
  private val src     = ReadCsv("memory://t", None)

  private def run(plan: LogicalPlan): Vector[Row] =
    backend.execute(plan) match
      case BackendResult.InMemory(rs) => rs

  // ---------------------------------------------------------------------------
  // DropColumns
  // ---------------------------------------------------------------------------

  test("DropColumns removes the specified column from every row"):
    val plan   = DropColumns(src, Vector("email"))
    val result = run(plan)
    assertEquals(result.size, 3)
    assert(result.forall(!_.values.contains("email")))
    assert(result.forall(_.values.contains("city")))

  test("DropColumns removes multiple columns at once"):
    val plan   = DropColumns(src, Vector("email", "age"))
    val result = run(plan)
    assert(result.forall(!_.values.contains("email")))
    assert(result.forall(!_.values.contains("age")))
    assert(result.forall(_.values.contains("city")))
    assert(result.forall(_.values.contains("revenue")))

  test("DropColumns silently ignores columns that do not exist"):
    val plan   = DropColumns(src, Vector("nonexistent"))
    val result = run(plan)
    assertEquals(result, rows)

  test("DropColumns preserves all remaining column values unchanged"):
    val plan   = DropColumns(src, Vector("email"))
    val result = run(plan)
    assertEquals(result(0).values("city"),    "Paris")
    assertEquals(result(0).values("revenue"), 1000.0)

  // ---------------------------------------------------------------------------
  // RenameColumn
  // ---------------------------------------------------------------------------

  test("RenameColumn renames the column in every row"):
    val plan   = RenameColumn(src, "city", "location")
    val result = run(plan)
    assert(result.forall(!_.values.contains("city")))
    assert(result.forall(_.values.contains("location")))
    assertEquals(result(0).values("location"), "Paris")

  test("RenameColumn preserves the value under the new name"):
    val plan   = RenameColumn(src, "revenue", "sales")
    val result = run(plan)
    assertEquals(result(0).values("sales"), 1000.0)
    assertEquals(result(2).values("sales"), null)

  test("RenameColumn leaves rows unchanged when the old name does not exist"):
    val plan   = RenameColumn(src, "nonexistent", "x")
    val result = run(plan)
    assertEquals(result, rows)

  test("RenameColumn leaves all other columns intact"):
    val plan   = RenameColumn(src, "city", "location")
    val result = run(plan)
    assert(result.forall(_.values.contains("age")))
    assert(result.forall(_.values.contains("revenue")))
    assert(result.forall(_.values.contains("email")))

  // ---------------------------------------------------------------------------
  // dropNa convenience method (via DataFrame API → Filter plan)
  // ---------------------------------------------------------------------------

  test("dropNa removes rows where any of the specified columns is null"):
    import lumina.api.*
    val df     = Lumina.readCsv("memory://t")
    val result = df.dropNa("email", "revenue").collect(backend)
    // row 1 (email=null) and row 2 (revenue=null) should be removed
    assertEquals(result.size, 1)
    assertEquals(result(0).values("city"), "Paris")

  test("dropNa with a single column only drops rows null in that column"):
    import lumina.api.*
    val df     = Lumina.readCsv("memory://t")
    val result = df.dropNa("email").collect(backend)
    assertEquals(result.size, 2)
    assert(result.forall(_.values("email") != null))

  // ---------------------------------------------------------------------------
  // fillNa convenience method (via DataFrame API → WithColumn + Coalesce)
  // ---------------------------------------------------------------------------

  test("fillNa replaces null values with the given fill value"):
    import lumina.api.*
    val df     = Lumina.readCsv("memory://t")
    val result = df.fillNa("unknown", "email").collect(backend)
    assert(result.forall(_.values("email") != null))
    assertEquals(result(1).values("email"), "unknown")

  test("fillNa leaves non-null values unchanged"):
    import lumina.api.*
    val df     = Lumina.readCsv("memory://t")
    val result = df.fillNa("unknown", "email").collect(backend)
    assertEquals(result(0).values("email"), "a@b.com")

  test("fillNa applied to multiple columns fills all of them"):
    import lumina.api.*
    val df     = Lumina.readCsv("memory://t")
    val result = df.fillNa(0.0, "revenue").collect(backend)
    assertEquals(result(2).values("revenue"), 0.0)
    assertEquals(result(0).values("revenue"), 1000.0)
