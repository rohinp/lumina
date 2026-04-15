package lumina.backend.duckdb

import munit.FunSuite
import lumina.plan.*
import lumina.plan.Expression.*
import lumina.plan.Aggregation.*
import lumina.plan.backend.*

/**
 * Tests for M15 features in DuckDBBackend: CountDistinct, StdDev, and Variance
 * aggregations.  DataFrame.agg() and describe() are tested against LocalBackend
 * in LocalBackendM15Spec; here we verify the SQL generation and end-to-end
 * DuckDB execution for the new aggregation functions.
 */
class DuckDBM15Spec extends FunSuite:

  private val rows = Vector(
    Row(Map("city" -> "Paris",  "score" -> 80, "grade" -> "B")),
    Row(Map("city" -> "Berlin", "score" -> 70, "grade" -> "C")),
    Row(Map("city" -> "Paris",  "score" -> 90, "grade" -> "A")),
    Row(Map("city" -> "London", "score" -> 60, "grade" -> "D")),
    Row(Map("city" -> "Paris",  "score" -> 100,"grade" -> "A"))
  )

  private val backend = DuckDBBackend(DataRegistry.of("memory://t" -> rows))
  private val src     = ReadCsv("memory://t", None)

  private def run(plan: LogicalPlan): Vector[Row] =
    backend.execute(plan) match
      case BackendResult.InMemory(rs) => rs

  // ---------------------------------------------------------------------------
  // CountDistinct
  // ---------------------------------------------------------------------------

  test("CountDistinct counts the number of distinct non-null values in a column"):
    val plan   = Aggregate(src, Vector.empty, Vector(CountDistinct(ColumnRef("city"), Some("n"))), None)
    val result = run(plan)
    assertEquals(result(0).values("n"), 3L)

  test("CountDistinct per group counts distinct values within each partition"):
    val plan   = Aggregate(src,
                   Vector(ColumnRef("city")),
                   Vector(CountDistinct(ColumnRef("grade"), Some("n_grades"))),
                   None)
    val result = run(plan)
    val parisCnt  = result.find(_.values("city") == "Paris").map(_.values("n_grades")).get
    val berlinCnt = result.find(_.values("city") == "Berlin").map(_.values("n_grades")).get
    assertEquals(parisCnt,  2L)
    assertEquals(berlinCnt, 1L)

  test("PlanToSql generates COUNT(DISTINCT ...) for CountDistinct"):
    val plan = Aggregate(src, Vector.empty, Vector(CountDistinct(ColumnRef("city"), Some("n"))), None)
    val sql  = PlanToSql.toSql(plan)
    assert(sql.contains("COUNT(DISTINCT"), s"Expected COUNT(DISTINCT in: $sql")

  // ---------------------------------------------------------------------------
  // StdDev
  // ---------------------------------------------------------------------------

  test("StdDev returns the sample standard deviation of a numeric column"):
    val plan   = Aggregate(src, Vector.empty, Vector(StdDev(ColumnRef("score"), Some("sd"))), None)
    val result = run(plan)
    val sd     = result(0).values("sd").asInstanceOf[Double]
    // scores: 80, 70, 90, 60, 100 → sample stddev ≈ 15.811
    assert(math.abs(sd - 15.811) < 0.01, s"Expected ~15.811 but got $sd")

  test("StdDev returns null for a group with a single row"):
    val single = Vector(Row(Map("v" -> 42)))
    val be     = DuckDBBackend(DataRegistry.of("memory://single" -> single))
    val plan   = Aggregate(ReadCsv("memory://single", None), Vector.empty,
                           Vector(StdDev(ColumnRef("v"), Some("sd"))), None)
    val result = be.execute(plan) match
      case BackendResult.InMemory(rs) => rs
    assertEquals(result(0).values("sd"), null)

  test("StdDev per group computes independently within each partition"):
    val plan   = Aggregate(src,
                   Vector(ColumnRef("city")),
                   Vector(StdDev(ColumnRef("score"), Some("sd"))),
                   None)
    val result = run(plan)
    val parisSd = result.find(_.values("city") == "Paris").map(_.values("sd")).get
    assert(parisSd != null, "Paris stddev should not be null (3 rows)")
    assert(math.abs(parisSd.asInstanceOf[Double] - 10.0) < 0.001,
           s"Expected ~10.0 for Paris but got $parisSd")

  test("PlanToSql generates STDDEV SQL for StdDev aggregation"):
    val plan = Aggregate(src, Vector.empty, Vector(StdDev(ColumnRef("score"), Some("sd"))), None)
    val sql  = PlanToSql.toSql(plan)
    assert(sql.contains("STDDEV"), s"Expected STDDEV in: $sql")

  // ---------------------------------------------------------------------------
  // Variance
  // ---------------------------------------------------------------------------

  test("Variance returns the sample variance of a numeric column"):
    val plan   = Aggregate(src, Vector.empty, Vector(Variance(ColumnRef("score"), Some("v"))), None)
    val result = run(plan)
    val v      = result(0).values("v").asInstanceOf[Double]
    // scores: 80, 70, 90, 60, 100 → sample variance = 250.0
    assert(math.abs(v - 250.0) < 0.01, s"Expected 250.0 but got $v")

  test("Variance returns null for a group with a single row"):
    val single = Vector(Row(Map("v" -> 7)))
    val be     = DuckDBBackend(DataRegistry.of("memory://single2" -> single))
    val plan   = Aggregate(ReadCsv("memory://single2", None), Vector.empty,
                           Vector(Variance(ColumnRef("v"), Some("var"))), None)
    val result = be.execute(plan) match
      case BackendResult.InMemory(rs) => rs
    assertEquals(result(0).values("var"), null)

  test("PlanToSql generates VARIANCE SQL for Variance aggregation"):
    val plan = Aggregate(src, Vector.empty, Vector(Variance(ColumnRef("score"), Some("v"))), None)
    val sql  = PlanToSql.toSql(plan)
    assert(sql.contains("VARIANCE"), s"Expected VARIANCE in: $sql")

  // ---------------------------------------------------------------------------
  // Whole-table aggregation via direct plan construction
  // ---------------------------------------------------------------------------

  test("whole-table COUNT aggregate without grouping returns a single row"):
    val plan   = Aggregate(src, Vector.empty, Vector(Count(None, Some("cnt"))), None)
    val result = run(plan)
    assertEquals(result.size, 1)
    assertEquals(result(0).values("cnt"), 5L)

  test("multiple aggregations in one Aggregate node all execute correctly"):
    val plan   = Aggregate(src, Vector.empty,
                   Vector(
                     Count(None,                          Some("cnt")),
                     CountDistinct(ColumnRef("city"),     Some("n_cities")),
                     StdDev(ColumnRef("score"),           Some("sd")),
                     Variance(ColumnRef("score"),         Some("var"))
                   ), None)
    val result = run(plan)
    assertEquals(result(0).values("cnt"),      5L)
    assertEquals(result(0).values("n_cities"), 3L)
    val sd  = result(0).values("sd").asInstanceOf[Double]
    val v   = result(0).values("var").asInstanceOf[Double]
    assert(math.abs(sd - 15.811) < 0.01, s"Expected ~15.811 stddev, got $sd")
    assert(math.abs(v  - 250.0)  < 0.01, s"Expected ~250.0 variance, got $v")
