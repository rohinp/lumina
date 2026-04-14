package lumina.backend.local

import munit.FunSuite
import lumina.plan.*
import lumina.plan.Expression.*
import lumina.plan.Aggregation.*
import lumina.plan.WindowExpr.*
import lumina.plan.backend.*

/**
 * Tests for M8 window functions in LocalBackend: RowNumber, Rank, DenseRank,
 * WindowAgg (whole-partition aggregates), Lag, and Lead.
 *
 * Data: five rows with city/revenue used to verify partition-aware window
 * semantics.  The rows are intentionally inserted in unsorted order to confirm
 * that LocalBackend sorts within each partition before computing rank values.
 *
 *   city=Paris,  revenue=1000   (Paris row A)
 *   city=Paris,  revenue=2000   (Paris row B)
 *   city=Berlin, revenue=1500   (Berlin row A)
 *   city=Berlin, revenue=3000   (Berlin row B)
 *   city=Paris,  revenue=500    (Paris row C)
 */
class LocalBackendWindowSpec extends FunSuite:

  private val rows = Vector(
    Row(Map("city" -> "Paris",  "revenue" -> 1000.0)),
    Row(Map("city" -> "Paris",  "revenue" -> 2000.0)),
    Row(Map("city" -> "Berlin", "revenue" -> 1500.0)),
    Row(Map("city" -> "Berlin", "revenue" -> 3000.0)),
    Row(Map("city" -> "Paris",  "revenue"  -> 500.0))
  )

  private val backend = LocalBackend(DataRegistry.of("memory://w" -> rows))
  private val src     = ReadCsv("memory://w", None)

  private def run(plan: LogicalPlan): Vector[Row] =
    backend.execute(plan) match
      case BackendResult.InMemory(rs) => rs

  // Helper to get the window column value for a specific row (matched by revenue)
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
    val plan = Window(src, Vector(RowNumber("rn", spec)))
    val result = run(plan)

    // Paris ordered by revenue asc: 500→1, 1000→2, 2000→3
    assertEquals(valueAt(result, 500.0,  "rn"), 1)
    assertEquals(valueAt(result, 1000.0, "rn"), 2)
    assertEquals(valueAt(result, 2000.0, "rn"), 3)
    // Berlin ordered by revenue asc: 1500→1, 3000→2
    assertEquals(valueAt(result, 1500.0, "rn"), 1)
    assertEquals(valueAt(result, 3000.0, "rn"), 2)

  test("RowNumber without partition treats the whole dataset as one partition"):
    val spec = WindowSpec(orderBy = Vector(SortExpr(ColumnRef("revenue"), ascending = true)))
    val plan = Window(src, Vector(RowNumber("rn", spec)))
    val result = run(plan)

    // Global order by revenue asc: 500→1, 1000→2, 1500→3, 2000→4, 3000→5
    assertEquals(valueAt(result, 500.0,  "rn"), 1)
    assertEquals(valueAt(result, 1000.0, "rn"), 2)
    assertEquals(valueAt(result, 1500.0, "rn"), 3)
    assertEquals(valueAt(result, 2000.0, "rn"), 4)
    assertEquals(valueAt(result, 3000.0, "rn"), 5)

  test("Window preserves all input rows and adds the new column"):
    val spec   = WindowSpec(orderBy = Vector(SortExpr(ColumnRef("revenue"), ascending = true)))
    val plan   = Window(src, Vector(RowNumber("rn", spec)))
    val result = run(plan)
    assertEquals(result.size, 5)
    assert(result.forall(_.values.contains("rn")))
    assert(result.forall(_.values.contains("city")))
    assert(result.forall(_.values.contains("revenue")))

  // ---------------------------------------------------------------------------
  // Rank and DenseRank
  // ---------------------------------------------------------------------------

  test("Rank assigns the same rank to tied rows with gaps in the sequence"):
    // Add a tie: two rows with the same revenue in the same partition
    val tiedRows = Vector(
      Row(Map("group" -> "A", "score" -> 10)),
      Row(Map("group" -> "A", "score" -> 20)),
      Row(Map("group" -> "A", "score" -> 20)),
      Row(Map("group" -> "A", "score" -> 30))
    )
    val reg  = DataRegistry.of("memory://tied" -> tiedRows)
    val be   = LocalBackend(reg)
    val spec = WindowSpec(orderBy = Vector(SortExpr(ColumnRef("score"), ascending = true)))
    val plan = Window(ReadCsv("memory://tied", None), Vector(Rank("rnk", spec)))
    val result = be.execute(plan) match
      case BackendResult.InMemory(rs) => rs

    val ranks = result.sortBy(_.values("score").asInstanceOf[Int]).map(_.values("rnk"))
    // scores 10, 20, 20, 30 → ranks 1, 2, 2, 4
    assertEquals(ranks, Vector(1, 2, 2, 4))

  test("DenseRank assigns the same rank to tied rows without gaps in the sequence"):
    val tiedRows = Vector(
      Row(Map("group" -> "A", "score" -> 10)),
      Row(Map("group" -> "A", "score" -> 20)),
      Row(Map("group" -> "A", "score" -> 20)),
      Row(Map("group" -> "A", "score" -> 30))
    )
    val reg  = DataRegistry.of("memory://tied2" -> tiedRows)
    val be   = LocalBackend(reg)
    val spec = WindowSpec(orderBy = Vector(SortExpr(ColumnRef("score"), ascending = true)))
    val plan = Window(ReadCsv("memory://tied2", None), Vector(DenseRank("dr", spec)))
    val result = be.execute(plan) match
      case BackendResult.InMemory(rs) => rs

    val ranks = result.sortBy(_.values("score").asInstanceOf[Int]).map(_.values("dr"))
    // scores 10, 20, 20, 30 → dense ranks 1, 2, 2, 3
    assertEquals(ranks, Vector(1, 2, 2, 3))

  // ---------------------------------------------------------------------------
  // WindowAgg (whole-partition aggregate)
  // ---------------------------------------------------------------------------

  test("WindowAgg sum adds the partition total to every row in the partition"):
    val spec = WindowSpec(partitionBy = Vector(ColumnRef("city")))
    val plan = Window(src, Vector(WindowAgg(Sum(ColumnRef("revenue")), "city_total", spec)))
    val result = run(plan)

    // Paris total = 500 + 1000 + 2000 = 3500.0; Berlin total = 1500 + 3000 = 4500.0
    val parisRows  = result.filter(_.values("city") == "Paris")
    val berlinRows = result.filter(_.values("city") == "Berlin")
    assert(parisRows.forall(_.values("city_total")  == 3500.0))
    assert(berlinRows.forall(_.values("city_total") == 4500.0))

  test("WindowAgg count adds the partition row count to every row"):
    val spec   = WindowSpec(partitionBy = Vector(ColumnRef("city")))
    val plan   = Window(src, Vector(WindowAgg(Count(None), "cnt", spec)))
    val result = run(plan)

    val parisRows  = result.filter(_.values("city") == "Paris")
    val berlinRows = result.filter(_.values("city") == "Berlin")
    assert(parisRows.forall(_.values("cnt")  == 3L))
    assert(berlinRows.forall(_.values("cnt") == 2L))

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

    // Paris sorted asc: 500, 1000, 2000
    // Lag(1): null, 500, 1000
    assertEquals(valueAt(result, 500.0,  "prev_revenue"), null)
    assertEquals(valueAt(result, 1000.0, "prev_revenue"), 500.0)
    assertEquals(valueAt(result, 2000.0, "prev_revenue"), 1000.0)
    // Berlin sorted asc: 1500, 3000
    assertEquals(valueAt(result, 1500.0, "prev_revenue"), null)
    assertEquals(valueAt(result, 3000.0, "prev_revenue"), 1500.0)

  test("Lead returns the value from offset rows after the current row within the partition"):
    val spec = WindowSpec(
      partitionBy = Vector(ColumnRef("city")),
      orderBy     = Vector(SortExpr(ColumnRef("revenue"), ascending = true))
    )
    val plan   = Window(src, Vector(Lead(ColumnRef("revenue"), 1, "next_revenue", spec)))
    val result = run(plan)

    // Paris sorted asc: 500, 1000, 2000
    // Lead(1): 1000, 2000, null
    assertEquals(valueAt(result, 500.0,  "next_revenue"), 1000.0)
    assertEquals(valueAt(result, 1000.0, "next_revenue"), 2000.0)
    assertEquals(valueAt(result, 2000.0, "next_revenue"), null)

  // ---------------------------------------------------------------------------
  // Multiple window expressions in one Window node
  // ---------------------------------------------------------------------------

  test("Window node with multiple expressions adds all columns in a single pass"):
    val spec = WindowSpec(
      partitionBy = Vector(ColumnRef("city")),
      orderBy     = Vector(SortExpr(ColumnRef("revenue"), ascending = true))
    )
    val plan = Window(src, Vector(
      RowNumber("rn", spec),
      WindowAgg(Sum(ColumnRef("revenue")), "city_total", spec)
    ))
    val result = run(plan)

    assertEquals(result.size, 5)
    assert(result.forall(_.values.contains("rn")))
    assert(result.forall(_.values.contains("city_total")))
