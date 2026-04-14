package lumina.backend.duckdb

import munit.FunSuite
import lumina.plan.*
import lumina.plan.Expression.*
import lumina.plan.backend.*

/**
 * Tests for M11 column operations in DuckDBBackend: DropColumns and
 * RenameColumn.  Mirrors LocalBackendColumnOpsSpec for cross-backend parity.
 */
class DuckDBColumnOpsSpec extends FunSuite:

  private val rows = Vector(
    Row(Map("city" -> "Paris",  "age" -> 35, "revenue" -> 1000.0, "email" -> "a@b.com")),
    Row(Map("city" -> "Berlin", "age" -> 29, "revenue" -> 2000.0, "email" -> null)),
    Row(Map("city" -> "London", "age" -> 40, "revenue" -> 3000.0, "email" -> "c@d.com"))
  )

  private val backend = DuckDBBackend(DataRegistry.of("memory://t" -> rows))
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
    assert(result.forall(_.values.contains("revenue")))

  test("DropColumns preserves all remaining column values"):
    val plan   = DropColumns(src, Vector("email"))
    val result = run(plan)
    assertEquals(result(0).values("city"), "Paris")
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

  test("RenameColumn leaves all other columns intact"):
    val plan   = RenameColumn(src, "city", "location")
    val result = run(plan)
    assert(result.forall(_.values.contains("age")))
    assert(result.forall(_.values.contains("revenue")))

  // ---------------------------------------------------------------------------
  // PlanToSql SQL shape
  // ---------------------------------------------------------------------------

  test("DropColumns generates SELECT * EXCLUDE SQL"):
    val sql = PlanToSql.toSql(DropColumns(src, Vector("email", "age")))
    assert(sql.contains("EXCLUDE"), sql)
    assert(sql.contains(""""email""""), sql)
    assert(sql.contains(""""age""""), sql)

  test("RenameColumn generates SELECT * EXCLUDE old, old AS new SQL"):
    val sql = PlanToSql.toSql(RenameColumn(src, "city", "location"))
    assert(sql.contains("EXCLUDE"), sql)
    assert(sql.contains(""""city""""), sql)
    assert(sql.contains(""""location""""), sql)
