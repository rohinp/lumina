package lumina.backend.duckdb

import munit.FunSuite
import lumina.plan.*
import lumina.plan.Expression.*
import lumina.plan.Aggregation.*
import lumina.plan.backend.{DataRegistry, Row, BackendResult}

/**
 * Integration tests for DuckDBBackend — each test names the operator or pipeline
 * being verified so that the file reads as a specification of backend behaviour.
 *
 * All tests use in-memory `memory://` datasets registered in a DataRegistry, so
 * no filesystem I/O is required. Read these tests to understand what DuckDBBackend
 * guarantees before diving into the SQL translation or JDBC plumbing.
 */
class DuckDBBackendSpec extends FunSuite:

  private val parisRows = Vector(
    Row(Map("city" -> "Paris",  "age" -> 35, "revenue" -> 1000.0)),
    Row(Map("city" -> "Paris",  "age" -> 45, "revenue" -> 3000.0)),
    Row(Map("city" -> "Berlin", "age" -> 29, "revenue" -> 2000.0))
  )

  private def backend(rows: Vector[Row] = parisRows) =
    DuckDBBackend(DataRegistry.of("memory://customers" -> rows))

  private def rows(result: BackendResult): Vector[Row] = result match
    case BackendResult.InMemory(rs) => rs

  // ---------------------------------------------------------------------------
  // ReadCsv — full table scan
  // ---------------------------------------------------------------------------

  test("executing a ReadCsv plan returns all rows from the registered dataset"):
    val result = backend().execute(ReadCsv("memory://customers", None))
    assertEquals(rows(result).size, 3)

  // ---------------------------------------------------------------------------
  // Filter
  // ---------------------------------------------------------------------------

  test("a GreaterThan filter returns only rows where the column exceeds the threshold"):
    val plan   = Filter(ReadCsv("memory://customers", None), GreaterThan(ColumnRef("age"), Literal(30)))
    val result = rows(backend().execute(plan))
    assertEquals(result.size, 2)

  test("an EqualTo filter on a string column returns only the matching rows"):
    val plan   = Filter(ReadCsv("memory://customers", None), EqualTo(ColumnRef("city"), Literal("Paris")))
    val result = rows(backend().execute(plan))
    assert(result.forall(_.values("city") == "Paris"), result.toString)

  test("a filter that matches no rows returns an empty result"):
    val plan   = Filter(ReadCsv("memory://customers", None), EqualTo(ColumnRef("city"), Literal("Tokyo")))
    val result = rows(backend().execute(plan))
    assertEquals(result.size, 0)

  // ---------------------------------------------------------------------------
  // Project
  // ---------------------------------------------------------------------------

  test("a Project reduces each row to only the selected columns"):
    val plan = Project(
      ReadCsv("memory://customers", None),
      Vector(ColumnRef("city")),
      schema = None
    )
    val result = rows(backend().execute(plan))
    assert(result.forall(r => r.values.keySet == Set("city")), result.toString)

  // ---------------------------------------------------------------------------
  // Aggregate
  // ---------------------------------------------------------------------------

  test("a groupBy with SUM produces one row per group with the correct aggregate value"):
    val plan = Aggregate(
      ReadCsv("memory://customers", None),
      groupBy      = Vector(ColumnRef("city")),
      aggregations = Vector(Sum(ColumnRef("revenue"), alias = Some("total"))),
      schema       = None
    )
    val result = rows(backend().execute(plan))
    assertEquals(result.size, 2)  // Paris + Berlin
    val paris = result.find(r => r.values("city") == "Paris").get
    assertEquals(paris.values("total").asInstanceOf[Double], 4000.0)

  test("a COUNT(*) groupBy returns the row count per group"):
    val plan = Aggregate(
      ReadCsv("memory://customers", None),
      groupBy      = Vector(ColumnRef("city")),
      aggregations = Vector(Count(None, alias = Some("n"))),
      schema       = None
    )
    val result = rows(backend().execute(plan))
    val paris = result.find(r => r.values("city") == "Paris").get
    assertEquals(paris.values("n").asInstanceOf[Long], 2L)

  // ---------------------------------------------------------------------------
  // Multi-step pipeline
  // ---------------------------------------------------------------------------

  test("a filter followed by groupBy returns the correct aggregate for the filtered subset"):
    val plan = Aggregate(
      Filter(
        ReadCsv("memory://customers", None),
        GreaterThan(ColumnRef("age"), Literal(30))
      ),
      groupBy      = Vector(ColumnRef("city")),
      aggregations = Vector(Sum(ColumnRef("revenue"), alias = Some("total"))),
      schema       = None
    )
    val result = rows(backend().execute(plan))
    // Only Paris rows with age > 30 (both Paris rows qualify; Berlin row does not)
    assertEquals(result.size, 1)
    assertEquals(result.head.values("city"), "Paris")
    assertEquals(result.head.values("total").asInstanceOf[Double], 4000.0)

  // ---------------------------------------------------------------------------
  // Edge cases
  // ---------------------------------------------------------------------------

  test("executing against an empty dataset returns no rows"):
    val emptyBackend = DuckDBBackend(DataRegistry.of("memory://customers" -> Vector.empty))
    val result = rows(emptyBackend.execute(ReadCsv("memory://customers", None)))
    assertEquals(result.size, 0)

  test("multiple datasets can be registered and queried independently"):
    val registry = DataRegistry.of(
      "memory://customers" -> parisRows,
      "memory://orders"    -> Vector(Row(Map("id" -> 1, "amount" -> 99.0)))
    )
    val b = DuckDBBackend(registry)
    assertEquals(rows(b.execute(ReadCsv("memory://customers", None))).size, 3)
    assertEquals(rows(b.execute(ReadCsv("memory://orders",    None))).size, 1)
