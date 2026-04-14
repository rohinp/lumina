package lumina.backend.local

import munit.FunSuite
import lumina.plan.*
import lumina.plan.Expression.*
import lumina.plan.backend.*

/**
 * Tests for M9 set operations in LocalBackend: UnionAll and Distinct.
 *
 * Read top-to-bottom to understand the contract of each operator before
 * looking at the implementation in LocalBackend.
 */
class LocalBackendSetOpsSpec extends FunSuite:

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
  private val backend = LocalBackend(registry)

  private def run(plan: LogicalPlan): Vector[Row] =
    backend.execute(plan) match
      case BackendResult.InMemory(rs) => rs

  // ---------------------------------------------------------------------------
  // UnionAll
  // ---------------------------------------------------------------------------

  test("UnionAll concatenates all rows from both datasets including duplicates"):
    val plan   = UnionAll(ReadCsv("memory://a", None), ReadCsv("memory://b", None))
    val result = run(plan)
    assertEquals(result.size, 4)  // 2 + 2, Berlin appears twice
    assertEquals(result.count(_.values("city") == "Berlin"), 2)

  test("UnionAll preserves left rows in order before right rows"):
    val plan   = UnionAll(ReadCsv("memory://a", None), ReadCsv("memory://b", None))
    val result = run(plan)
    assertEquals(result(0).values("city"), "Paris")
    assertEquals(result(1).values("city"), "Berlin")

  test("UnionAll of an empty dataset with a non-empty dataset returns the non-empty dataset"):
    val be     = LocalBackend(registry.register("memory://empty", Vector.empty[Row]))
    val plan   = UnionAll(ReadCsv("memory://empty", None), ReadCsv("memory://a", None))
    val result = be.execute(plan) match
      case BackendResult.InMemory(rs) => rs
    assertEquals(result, cities)

  // ---------------------------------------------------------------------------
  // Distinct
  // ---------------------------------------------------------------------------

  test("Distinct removes duplicate rows keeping the first occurrence"):
    val dupes = Vector(
      Row(Map("city" -> "Paris",  "pop" -> 2000)),
      Row(Map("city" -> "Berlin", "pop" -> 3000)),
      Row(Map("city" -> "Paris",  "pop" -> 2000))
    )
    val reg    = DataRegistry.of("memory://dupes" -> dupes)
    val be     = LocalBackend(reg)
    val plan   = Distinct(ReadCsv("memory://dupes", None))
    val result = be.execute(plan) match
      case BackendResult.InMemory(rs) => rs
    assertEquals(result.size, 2)
    assertEquals(result(0).values("city"), "Paris")
    assertEquals(result(1).values("city"), "Berlin")

  test("Distinct on a dataset with no duplicates returns all rows unchanged"):
    val plan   = Distinct(ReadCsv("memory://a", None))
    val result = run(plan)
    assertEquals(result.size, 2)

  test("Distinct on an empty dataset returns an empty result"):
    val empty  = DataRegistry.of("memory://empty2" -> Vector.empty[Row])
    val be     = LocalBackend(empty)
    val plan   = Distinct(ReadCsv("memory://empty2", None))
    val result = be.execute(plan) match
      case BackendResult.InMemory(rs) => rs
    assertEquals(result.size, 0)

  // ---------------------------------------------------------------------------
  // Combined: UnionAll then Distinct = UNION (distinct)
  // ---------------------------------------------------------------------------

  test("Distinct on top of UnionAll produces the equivalent of SQL UNION (distinct)"):
    val plan   = Distinct(UnionAll(ReadCsv("memory://a", None), ReadCsv("memory://b", None)))
    val result = run(plan)
    // Unique cities across a and b: Paris, Berlin, London
    assertEquals(result.size, 3)
    val citySet = result.map(_.values("city").asInstanceOf[String]).toSet
    assertEquals(citySet, Set("Paris", "Berlin", "London"))
