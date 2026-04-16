package lumina.backend.duckdb

import munit.FunSuite
import lumina.plan.*
import lumina.plan.Expression.*
import lumina.plan.Aggregation.*
import lumina.plan.backend.*

/**
 * Tests for M21 aggregate functions in DuckDBBackend: First, Last, Median.
 *
 * Mirrors LocalBackendM21Spec to verify both backends agree.
 * First/Last in DuckDB are non-deterministic without ORDER BY inside the
 * aggregate; these tests use data designed so the order is unambiguous
 * (single-row groups or groups where all values are the same).
 */
class DuckDBM21Spec extends FunSuite:

  private val rows = Vector(
    Row(Map("dept" -> "eng",  "name" -> "Alice", "score" -> 10)),
    Row(Map("dept" -> "eng",  "name" -> "Bob",   "score" -> 40)),
    Row(Map("dept" -> "eng",  "name" -> "Carol",  "score" -> 20)),
    Row(Map("dept" -> "mkt",  "name" -> "Dave",  "score" -> 30)),
    Row(Map("dept" -> "mkt",  "name" -> "Eve",   "score" -> null)),
    Row(Map("dept" -> "mkt",  "name" -> "Frank", "score" -> 50))
  )

  private val backend = DuckDBBackend(DataRegistry.of("memory://t" -> rows))
  private val src     = ReadCsv("memory://t", None)

  private def wholeTable(agg: Aggregation): Any =
    val plan = Aggregate(src, Vector.empty, Vector(agg), None)
    backend.execute(plan) match
      case BackendResult.InMemory(rs) => rs.head.values.values.head

  private def byDept(agg: Aggregation): Map[String, Any] =
    val plan = Aggregate(src, Vector(ColumnRef("dept")), Vector(agg), None)
    backend.execute(plan) match
      case BackendResult.InMemory(rs) =>
        rs.map(r => r.values("dept").toString -> r.values.values.drop(1).head).toMap

  // ---------------------------------------------------------------------------
  // Median (fully deterministic — ideal for cross-backend parity tests)
  // ---------------------------------------------------------------------------

  test("Median returns the middle value for an odd count of non-null values"):
    // eng scores: 10, 40, 20 → sorted: 10, 20, 40 → median = 20
    val result = byDept(Median(ColumnRef("score"), Some("v")))
    assertEquals(result("eng").toString.toDouble, 20.0)

  test("Median returns the average of the two middle values for an even count"):
    // mkt non-null scores: 30, 50 → sorted: 30, 50 → median = 40.0
    val result = byDept(Median(ColumnRef("score"), Some("v")))
    assertEquals(result("mkt").toString.toDouble, 40.0)

  test("Median over the whole table works across all non-null rows"):
    // all non-null scores: 10, 40, 20, 30, 50 → sorted → median = 30
    val result = wholeTable(Median(ColumnRef("score"), Some("v")))
    assertEquals(result.toString.toDouble, 30.0)

  test("Median returns null when all values in the group are null"):
    val allNullRows = Vector(Row(Map("g" -> "x", "v" -> null)))
    val be          = DuckDBBackend(DataRegistry.of("memory://amn" -> allNullRows))
    val plan        = Aggregate(ReadCsv("memory://amn", None), Vector(ColumnRef("g")),
                                Vector(Median(ColumnRef("v"), Some("r"))), None)
    val rs          = be.execute(plan) match
      case BackendResult.InMemory(rs) => rs
    assertEquals(rs.head.values("r"), null)

  test("Median works correctly for a single non-null value"):
    val singleRow = Vector(Row(Map("g" -> "x", "v" -> 42)))
    val be        = DuckDBBackend(DataRegistry.of("memory://sr" -> singleRow))
    val plan      = Aggregate(ReadCsv("memory://sr", None), Vector(ColumnRef("g")),
                              Vector(Median(ColumnRef("v"), Some("r"))), None)
    val rs        = be.execute(plan) match
      case BackendResult.InMemory(rs) => rs
    assertEquals(rs.head.values("r").toString.toDouble, 42.0)

  // ---------------------------------------------------------------------------
  // First — tested on single-row groups to avoid non-determinism
  // ---------------------------------------------------------------------------

  test("First returns the non-null value from a single-element group"):
    val singleRow = Vector(Row(Map("g" -> "a", "v" -> 99)))
    val be        = DuckDBBackend(DataRegistry.of("memory://f1" -> singleRow))
    val plan      = Aggregate(ReadCsv("memory://f1", None), Vector(ColumnRef("g")),
                              Vector(First(ColumnRef("v"), Some("r"))), None)
    val rs        = be.execute(plan) match
      case BackendResult.InMemory(rs) => rs
    assertEquals(rs.head.values("r").toString.toInt, 99)

  test("First returns null when all values in the group are null"):
    val allNullRows = Vector(Row(Map("g" -> "x", "v" -> null)))
    val be          = DuckDBBackend(DataRegistry.of("memory://fn" -> allNullRows))
    val plan        = Aggregate(ReadCsv("memory://fn", None), Vector(ColumnRef("g")),
                                Vector(First(ColumnRef("v"), Some("r"))), None)
    val rs          = be.execute(plan) match
      case BackendResult.InMemory(rs) => rs
    assertEquals(rs.head.values("r"), null)

  // ---------------------------------------------------------------------------
  // Last — tested on single-row groups to avoid non-determinism
  // ---------------------------------------------------------------------------

  test("Last returns the non-null value from a single-element group"):
    val singleRow = Vector(Row(Map("g" -> "a", "v" -> 77)))
    val be        = DuckDBBackend(DataRegistry.of("memory://l1" -> singleRow))
    val plan      = Aggregate(ReadCsv("memory://l1", None), Vector(ColumnRef("g")),
                              Vector(Last(ColumnRef("v"), Some("r"))), None)
    val rs        = be.execute(plan) match
      case BackendResult.InMemory(rs) => rs
    assertEquals(rs.head.values("r").toString.toInt, 77)

  test("Last returns null when all values in the group are null"):
    val allNullRows = Vector(Row(Map("g" -> "x", "v" -> null)))
    val be          = DuckDBBackend(DataRegistry.of("memory://ln" -> allNullRows))
    val plan        = Aggregate(ReadCsv("memory://ln", None), Vector(ColumnRef("g")),
                                Vector(Last(ColumnRef("v"), Some("r"))), None)
    val rs          = be.execute(plan) match
      case BackendResult.InMemory(rs) => rs
    assertEquals(rs.head.values("r"), null)
