package lumina.backend.duckdb

import munit.FunSuite
import lumina.plan.*
import lumina.plan.Expression.*
import lumina.plan.Aggregation.*
import lumina.plan.WindowExpr.*
import lumina.plan.backend.*

/**
 * Tests for M8 window functions translated by PlanToSql and executed by
 * DuckDBBackend.  Mirrors LocalBackendWindowSpec to ensure both backends
 * produce identical results for window operations.
 *
 * Data: five rows with city/revenue — same dataset as LocalBackendWindowSpec.
 */
class DuckDBBackendWindowSpec extends FunSuite:

  private val rows = Vector(
    Row(Map("city" -> "Paris",  "revenue" -> 1000.0)),
    Row(Map("city" -> "Paris",  "revenue" -> 2000.0)),
    Row(Map("city" -> "Berlin", "revenue" -> 1500.0)),
    Row(Map("city" -> "Berlin", "revenue" -> 3000.0)),
    Row(Map("city" -> "Paris",  "revenue" ->  500.0))
  )

  private val backend = DuckDBBackend(DataRegistry.of("memory://w" -> rows))
  private val src     = ReadCsv("memory://w", None)

  private def run(plan: LogicalPlan): Vector[Row] =
    backend.execute(plan) match
      case BackendResult.InMemory(rs) => rs

  private def valueAt(result: Vector[Row], revenue: Double, col: String): Any =
    result.find(r => r.values("revenue") == revenue).get.values(col)

  // ---------------------------------------------------------------------------
  // RowNumber
  // ---------------------------------------------------------------------------

  test("RowNumber assigns sequential integers starting at 1 within each partition"):
    val spec = WindowSpec(
      partitionBy = Vector(ColumnRef("city")),
      orderBy     = Vector(SortExpr(ColumnRef("revenue"), ascending = true))
    )
    val plan   = Window(src, Vector(RowNumber("rn", spec)))
    val result = run(plan)

    assertEquals(valueAt(result, 500.0,  "rn"), 1L)
    assertEquals(valueAt(result, 1000.0, "rn"), 2L)
    assertEquals(valueAt(result, 2000.0, "rn"), 3L)
    assertEquals(valueAt(result, 1500.0, "rn"), 1L)
    assertEquals(valueAt(result, 3000.0, "rn"), 2L)

  test("RowNumber without partition treats the whole dataset as one partition"):
    val spec   = WindowSpec(orderBy = Vector(SortExpr(ColumnRef("revenue"), ascending = true)))
    val plan   = Window(src, Vector(RowNumber("rn", spec)))
    val result = run(plan)

    assertEquals(valueAt(result, 500.0,  "rn"), 1L)
    assertEquals(valueAt(result, 1000.0, "rn"), 2L)
    assertEquals(valueAt(result, 1500.0, "rn"), 3L)
    assertEquals(valueAt(result, 2000.0, "rn"), 4L)
    assertEquals(valueAt(result, 3000.0, "rn"), 5L)

  // ---------------------------------------------------------------------------
  // Rank and DenseRank
  // ---------------------------------------------------------------------------

  test("Rank assigns the same rank to tied rows with gaps in the sequence"):
    val tiedRows = Vector(
      Row(Map("grp" -> "A", "score" -> 10)),
      Row(Map("grp" -> "A", "score" -> 20)),
      Row(Map("grp" -> "A", "score" -> 20)),
      Row(Map("grp" -> "A", "score" -> 30))
    )
    val be   = DuckDBBackend(DataRegistry.of("memory://tied" -> tiedRows))
    val spec = WindowSpec(orderBy = Vector(SortExpr(ColumnRef("score"), ascending = true)))
    val plan = Window(ReadCsv("memory://tied", None), Vector(Rank("rnk", spec)))
    val result = be.execute(plan) match
      case BackendResult.InMemory(rs) => rs

    val ranks = result.sortBy(_.values("score").asInstanceOf[Int]).map(_.values("rnk"))
    assertEquals(ranks, Vector(1L, 2L, 2L, 4L))

  test("DenseRank assigns the same rank to tied rows without gaps in the sequence"):
    val tiedRows = Vector(
      Row(Map("grp" -> "A", "score" -> 10)),
      Row(Map("grp" -> "A", "score" -> 20)),
      Row(Map("grp" -> "A", "score" -> 20)),
      Row(Map("grp" -> "A", "score" -> 30))
    )
    val be   = DuckDBBackend(DataRegistry.of("memory://tied2" -> tiedRows))
    val spec = WindowSpec(orderBy = Vector(SortExpr(ColumnRef("score"), ascending = true)))
    val plan = Window(ReadCsv("memory://tied2", None), Vector(DenseRank("dr", spec)))
    val result = be.execute(plan) match
      case BackendResult.InMemory(rs) => rs

    val ranks = result.sortBy(_.values("score").asInstanceOf[Int]).map(_.values("dr"))
    assertEquals(ranks, Vector(1L, 2L, 2L, 3L))

  // ---------------------------------------------------------------------------
  // WindowAgg
  // ---------------------------------------------------------------------------

  test("WindowAgg sum adds the partition total to every row in the partition"):
    val spec = WindowSpec(partitionBy = Vector(ColumnRef("city")))
    val plan = Window(src, Vector(WindowAgg(Sum(ColumnRef("revenue")), "city_total", spec)))
    val result = run(plan)

    val parisRows  = result.filter(_.values("city") == "Paris")
    val berlinRows = result.filter(_.values("city") == "Berlin")
    assert(parisRows.forall(_.values("city_total")  == 3500.0))
    assert(berlinRows.forall(_.values("city_total") == 4500.0))

  // ---------------------------------------------------------------------------
  // Lag and Lead
  // ---------------------------------------------------------------------------

  test("Lag returns the value from offset rows before the current row within the partition"):
    val spec = WindowSpec(
      partitionBy = Vector(ColumnRef("city")),
      orderBy     = Vector(SortExpr(ColumnRef("revenue"), ascending = true))
    )
    val plan   = Window(src, Vector(Lag(ColumnRef("revenue"), 1, "prev_revenue", spec)))
    val result = run(plan)

    assertEquals(valueAt(result, 500.0,  "prev_revenue"), null)
    assertEquals(valueAt(result, 1000.0, "prev_revenue"), 500.0)
    assertEquals(valueAt(result, 2000.0, "prev_revenue"), 1000.0)
    assertEquals(valueAt(result, 1500.0, "prev_revenue"), null)
    assertEquals(valueAt(result, 3000.0, "prev_revenue"), 1500.0)

  test("Lead returns the value from offset rows after the current row within the partition"):
    val spec = WindowSpec(
      partitionBy = Vector(ColumnRef("city")),
      orderBy     = Vector(SortExpr(ColumnRef("revenue"), ascending = true))
    )
    val plan   = Window(src, Vector(Lead(ColumnRef("revenue"), 1, "next_revenue", spec)))
    val result = run(plan)

    assertEquals(valueAt(result, 500.0,  "next_revenue"), 1000.0)
    assertEquals(valueAt(result, 1000.0, "next_revenue"), 2000.0)
    assertEquals(valueAt(result, 2000.0, "next_revenue"), null)
