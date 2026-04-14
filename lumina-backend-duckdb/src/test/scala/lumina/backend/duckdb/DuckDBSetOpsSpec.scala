package lumina.backend.duckdb

import munit.FunSuite
import lumina.plan.*
import lumina.plan.Expression.*
import lumina.plan.backend.*

/**
 * Tests for M9 set operations in DuckDBBackend: UnionAll and Distinct.
 *
 * Mirrors LocalBackendSetOpsSpec to confirm both backends produce identical
 * results, and verifies PlanToSql generates valid SQL for each operator.
 */
class DuckDBSetOpsSpec extends FunSuite:

  private val cities = Vector(
    Row(Map("city" -> "Paris",  "pop" -> 2000)),
    Row(Map("city" -> "Berlin", "pop" -> 3000))
  )

  private val moreCities = Vector(
    Row(Map("city" -> "Berlin", "pop" -> 3000)),
    Row(Map("city" -> "London", "pop" -> 9000))
  )

  private val registry = DataRegistry.of(
    "memory://a" -> cities,
    "memory://b" -> moreCities
  )
  private val backend  = DuckDBBackend(registry)

  private def run(plan: LogicalPlan): Vector[Row] =
    backend.execute(plan) match
      case BackendResult.InMemory(rs) => rs

  // ---------------------------------------------------------------------------
  // UnionAll
  // ---------------------------------------------------------------------------

  test("UnionAll concatenates all rows from both datasets including duplicates"):
    val plan   = UnionAll(ReadCsv("memory://a", None), ReadCsv("memory://b", None))
    val result = run(plan)
    assertEquals(result.size, 4)
    assertEquals(result.count(_.values("city") == "Berlin"), 2)

  // ---------------------------------------------------------------------------
  // Distinct
  // ---------------------------------------------------------------------------

  test("Distinct removes duplicate rows"):
    val dupes = Vector(
      Row(Map("city" -> "Paris",  "pop" -> 2000)),
      Row(Map("city" -> "Berlin", "pop" -> 3000)),
      Row(Map("city" -> "Paris",  "pop" -> 2000))
    )
    val be     = DuckDBBackend(DataRegistry.of("memory://dupes" -> dupes))
    val plan   = Distinct(ReadCsv("memory://dupes", None))
    val result = be.execute(plan) match
      case BackendResult.InMemory(rs) => rs
    assertEquals(result.size, 2)

  test("Distinct on a dataset with no duplicates returns all rows unchanged"):
    val plan   = Distinct(ReadCsv("memory://a", None))
    val result = run(plan)
    assertEquals(result.size, 2)

  // ---------------------------------------------------------------------------
  // Combined: UnionAll then Distinct
  // ---------------------------------------------------------------------------

  test("Distinct on top of UnionAll deduplicates across both inputs"):
    val plan   = Distinct(UnionAll(ReadCsv("memory://a", None), ReadCsv("memory://b", None)))
    val result = run(plan)
    assertEquals(result.size, 3)
    val citySet = result.map(_.values("city").asInstanceOf[String]).toSet
    assertEquals(citySet, Set("Paris", "Berlin", "London"))
