package lumina.config

import munit.FunSuite
import lumina.plan.*
import lumina.plan.Expression.*
import lumina.plan.Aggregation.*
import lumina.plan.backend.*
import lumina.backend.local.{DataRegistry, LocalBackend}

/**
 * Tests for LuminaSession — describes how a session selects a backend and
 * executes logical plans end-to-end.
 */
class LuminaSessionSpec extends FunSuite:

  private val customerRows = Vector(
    Row(Map("city" -> "Paris",  "age" -> 35, "revenue" -> 1000.0)),
    Row(Map("city" -> "Paris",  "age" -> 45, "revenue" -> 3000.0)),
    Row(Map("city" -> "Berlin", "age" -> 29, "revenue" -> 2000.0))
  )

  private val sourcePath = "memory://customers"

  private val schema = Schema(
    Vector(
      Column("city",    DataType.StringType),
      Column("age",     DataType.Int32),
      Column("revenue", DataType.Float64)
    )
  )

  private def sessionWithData: LuminaSession =
    val backend  = LocalBackend(DataRegistry.of(sourcePath -> customerRows))
    val registry = BackendRegistry.empty.register(backend)
    LuminaSession(registry, "local")

  // ---------------------------------------------------------------------------
  // Backend selection
  // ---------------------------------------------------------------------------

  test("local() convenience constructor selects the local backend"):
    assertEquals(LuminaSession.local().backend.name, "local")

  test("withBackend selects the named backend from the default registry"):
    assertEquals(LuminaSession.withBackend("local").backend.name, "local")

  test("selecting an unregistered backend raises an error"):
    intercept[IllegalArgumentException]:
      LuminaSession.withBackend("nonexistent")

  // ---------------------------------------------------------------------------
  // End-to-end plan execution through the session
  // ---------------------------------------------------------------------------

  test("executing a ReadCsv plan returns all rows from the registered source"):
    val plan   = ReadCsv(sourcePath, Some(schema))
    val result = sessionWithData.execute(plan)
    result match
      case BackendResult.InMemory(rows) => assertEquals(rows.length, 3)

  test("executing a filter plan through the session returns only matching rows"):
    val plan = Filter(
      ReadCsv(sourcePath, Some(schema)),
      GreaterThan(ColumnRef("age"), Literal(30))
    )
    val result = sessionWithData.execute(plan)
    result match
      case BackendResult.InMemory(rows) =>
        assertEquals(rows.length, 2)
        assert(rows.forall(_("age").asInstanceOf[Int] > 30))

  test("executing a groupBy plan through the session produces the correct aggregated totals"):
    val plan = Aggregate(
      ReadCsv(sourcePath, Some(schema)),
      groupBy      = Vector(ColumnRef("city")),
      aggregations = Vector(Sum(ColumnRef("revenue"), alias = Some("total"))),
      schema       = None
    )
    val result = sessionWithData.execute(plan)
    result match
      case BackendResult.InMemory(rows) =>
        val paris = rows.find(_("city") == "Paris").getOrElse(fail("Paris group missing"))
        assertEquals(paris("total"), 4000.0)
