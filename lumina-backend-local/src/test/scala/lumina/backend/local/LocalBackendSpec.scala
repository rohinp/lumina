package lumina.backend.local

import munit.FunSuite
import lumina.plan.*
import lumina.plan.Expression.*
import lumina.plan.Aggregation.*
import lumina.plan.backend.*

/**
 * Unit tests for LocalBackend — each test describes one discrete behaviour.
 * Read this file top-to-bottom to understand how the backend interprets each
 * logical-plan node and how the operations compose.
 */
class LocalBackendSpec extends FunSuite:

  // ---------------------------------------------------------------------------
  // Fixtures
  // ---------------------------------------------------------------------------

  private val customerSchema = Schema(
    Vector(
      Column("city",    DataType.StringType),
      Column("age",     DataType.Int32),
      Column("revenue", DataType.Float64)
    )
  )

  private val customerRows = Vector(
    Row(Map("city" -> "Paris",  "age" -> 35, "revenue" -> 1000.0)),
    Row(Map("city" -> "Paris",  "age" -> 45, "revenue" -> 3000.0)),
    Row(Map("city" -> "Berlin", "age" -> 29, "revenue" -> 2000.0)),
    Row(Map("city" -> "Berlin", "age" -> 50, "revenue" -> 500.0))
  )

  private val sourcePath = "memory://customers"

  private def backendWith(rows: Vector[Row]): LocalBackend =
    LocalBackend(DataRegistry.of(sourcePath -> rows))

  private def readPlan: LogicalPlan = ReadCsv(sourcePath, Some(customerSchema))

  private def execute(backend: LocalBackend, plan: LogicalPlan): Vector[Row] =
    backend.execute(plan) match
      case BackendResult.InMemory(rows) => rows

  // ---------------------------------------------------------------------------
  // ReadCsv — data loading
  // ---------------------------------------------------------------------------

  test("reading from a registered memory source returns all rows unchanged"):
    val rows = execute(backendWith(customerRows), readPlan)
    assertEquals(rows.length, 4)
    assertEquals(rows.head("city"), "Paris")

  test("reading from an empty registered source returns an empty result"):
    val rows = execute(backendWith(Vector.empty), readPlan)
    assert(rows.isEmpty)

  test("reading from an unregistered path raises an error"):
    val backend = LocalBackend(DataRegistry.empty)
    intercept[IllegalArgumentException]:
      backend.execute(ReadCsv("memory://unknown", None))

  // ---------------------------------------------------------------------------
  // Filter — row predicate evaluation
  // ---------------------------------------------------------------------------

  test("filter keeps only rows where GreaterThan evaluates to true"):
    val plan = Filter(readPlan, GreaterThan(ColumnRef("age"), Literal(30)))
    val rows = execute(backendWith(customerRows), plan)
    assertEquals(rows.length, 3)
    assert(rows.forall(_("age").asInstanceOf[Int] > 30))

  test("filter keeps only rows where EqualTo evaluates to true"):
    val plan = Filter(readPlan, EqualTo(ColumnRef("city"), Literal("Paris")))
    val rows = execute(backendWith(customerRows), plan)
    assertEquals(rows.length, 2)
    assert(rows.forall(_("city") == "Paris"))

  test("filter with a condition that no row satisfies returns an empty result"):
    val plan = Filter(readPlan, GreaterThan(ColumnRef("age"), Literal(100)))
    val rows = execute(backendWith(customerRows), plan)
    assert(rows.isEmpty)

  test("filter with a condition that every row satisfies returns all rows"):
    val plan = Filter(readPlan, GreaterThan(ColumnRef("age"), Literal(0)))
    val rows = execute(backendWith(customerRows), plan)
    assertEquals(rows.length, 4)

  test("filter on an empty source returns an empty result"):
    val plan = Filter(readPlan, GreaterThan(ColumnRef("age"), Literal(30)))
    val rows = execute(backendWith(Vector.empty), plan)
    assert(rows.isEmpty)

  // ---------------------------------------------------------------------------
  // Project — column selection
  // ---------------------------------------------------------------------------

  test("project keeps only the requested columns in each row"):
    val plan = Project(readPlan, Vector(ColumnRef("city"), ColumnRef("revenue")), schema = None)
    val rows = execute(backendWith(customerRows), plan)
    assertEquals(rows.length, 4)
    assert(rows.forall(r => r.values.keySet == Set("city", "revenue")))

  test("project on a single column produces rows with exactly that column"):
    val plan = Project(readPlan, Vector(ColumnRef("city")), schema = None)
    val rows = execute(backendWith(customerRows), plan)
    assert(rows.forall(r => r.values.keySet == Set("city")))

  test("project preserves row order"):
    val plan = Project(readPlan, Vector(ColumnRef("city")), schema = None)
    val rows = execute(backendWith(customerRows), plan)
    assertEquals(rows.map(_("city")), Vector("Paris", "Paris", "Berlin", "Berlin"))

  // ---------------------------------------------------------------------------
  // Aggregate — groupBy + aggregation functions
  // ---------------------------------------------------------------------------

  test("groupBy with Sum accumulates numeric values per group"):
    val plan = Aggregate(
      readPlan,
      groupBy      = Vector(ColumnRef("city")),
      aggregations = Vector(Sum(ColumnRef("revenue"), alias = Some("total_revenue"))),
      schema       = None
    )
    val rows = execute(backendWith(customerRows), plan)

    val paris  = rows.find(_("city") == "Paris").getOrElse(fail("Paris group missing"))
    val berlin = rows.find(_("city") == "Berlin").getOrElse(fail("Berlin group missing"))
    assertEquals(paris("total_revenue"),  4000.0)
    assertEquals(berlin("total_revenue"), 2500.0)

  test("groupBy with Count returns the number of rows per group"):
    val plan = Aggregate(
      readPlan,
      groupBy      = Vector(ColumnRef("city")),
      aggregations = Vector(Count(None, alias = Some("row_count"))),
      schema       = None
    )
    val rows = execute(backendWith(customerRows), plan)

    val paris  = rows.find(_("city") == "Paris").getOrElse(fail("Paris group missing"))
    val berlin = rows.find(_("city") == "Berlin").getOrElse(fail("Berlin group missing"))
    assertEquals(paris("row_count"),  2L)
    assertEquals(berlin("row_count"), 2L)

  test("groupBy on an empty source returns an empty result"):
    val plan = Aggregate(
      readPlan,
      groupBy      = Vector(ColumnRef("city")),
      aggregations = Vector(Sum(ColumnRef("revenue"), alias = Some("total_revenue"))),
      schema       = None
    )
    val rows = execute(backendWith(Vector.empty), plan)
    assert(rows.isEmpty)

  test("groupBy produces one output row per distinct key value"):
    val plan = Aggregate(
      readPlan,
      groupBy      = Vector(ColumnRef("city")),
      aggregations = Vector(Count(None, alias = Some("n"))),
      schema       = None
    )
    val rows = execute(backendWith(customerRows), plan)
    assertEquals(rows.length, 2)

  // ---------------------------------------------------------------------------
  // Composed pipelines — exercises plan node chaining
  // ---------------------------------------------------------------------------

  test("filter then groupBy computes aggregates over the filtered subset only"):
    val plan = Aggregate(
      Filter(readPlan, GreaterThan(ColumnRef("age"), Literal(30))),
      groupBy      = Vector(ColumnRef("city")),
      aggregations = Vector(Sum(ColumnRef("revenue"), alias = Some("total_revenue"))),
      schema       = None
    )
    val rows = execute(backendWith(customerRows), plan)

    // Berlin row with age=29 is excluded → only Berlin age=50 remains
    val berlin = rows.find(_("city") == "Berlin").getOrElse(fail("Berlin group missing"))
    assertEquals(berlin("total_revenue"), 500.0)

  test("filter then project returns only matching rows with the selected columns"):
    val plan = Project(
      Filter(readPlan, EqualTo(ColumnRef("city"), Literal("Paris"))),
      columns = Vector(ColumnRef("city"), ColumnRef("revenue")),
      schema  = None
    )
    val rows = execute(backendWith(customerRows), plan)
    assertEquals(rows.length, 2)
    assert(rows.forall(r => r.values.keySet == Set("city", "revenue")))
    assert(rows.forall(_("city") == "Paris"))

  test("filter then project then groupBy produces the correct final aggregation"):
    val plan = Aggregate(
      Project(
        Filter(readPlan, GreaterThan(ColumnRef("age"), Literal(30))),
        columns = Vector(ColumnRef("city"), ColumnRef("revenue")),
        schema  = None
      ),
      groupBy      = Vector(ColumnRef("city")),
      aggregations = Vector(Sum(ColumnRef("revenue"), alias = Some("total"))),
      schema       = None
    )
    val rows = execute(backendWith(customerRows), plan)

    val paris = rows.find(_("city") == "Paris").getOrElse(fail("Paris group missing"))
    assertEquals(paris("total"), 4000.0)
