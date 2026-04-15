package lumina.backend.local

import munit.FunSuite
import lumina.plan.*
import lumina.plan.Expression.*
import lumina.plan.Aggregation.*
import lumina.plan.backend.*
import lumina.api.*

/**
 * Tests for M15 features in LocalBackend: CountDistinct, StdDev, Variance
 * aggregations; DataFrame.agg() shorthand; and DataFrame.describe().
 *
 * Read top-to-bottom as a specification for each feature's behaviour.
 */
class LocalBackendM15Spec extends FunSuite:

  private val rows = Vector(
    Row(Map("city" -> "Paris",  "score" -> 80, "grade" -> "B")),
    Row(Map("city" -> "Berlin", "score" -> 70, "grade" -> "C")),
    Row(Map("city" -> "Paris",  "score" -> 90, "grade" -> "A")),
    Row(Map("city" -> "London", "score" -> 60, "grade" -> "D")),
    Row(Map("city" -> "Paris",  "score" -> 100,"grade" -> "A"))
  )

  private val backend = LocalBackend(DataRegistry.of("memory://t" -> rows))
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
    assertEquals(result(0).values("n"), 3L)  // Paris, Berlin, London

  test("CountDistinct ignores null values when counting"):
    val withNulls = rows :+ Row(Map("city" -> null, "score" -> 50, "grade" -> "F"))
    val reg       = DataRegistry.of("memory://nulls" -> withNulls)
    val be        = LocalBackend(reg)
    val plan      = Aggregate(ReadCsv("memory://nulls", None), Vector.empty,
                              Vector(CountDistinct(ColumnRef("city"), Some("n"))), None)
    val result    = be.execute(plan) match
      case BackendResult.InMemory(rs) => rs
    assertEquals(result(0).values("n"), 3L)  // null not counted

  test("CountDistinct per group counts distinct values within each partition"):
    val plan   = Aggregate(src,
                   Vector(ColumnRef("city")),
                   Vector(CountDistinct(ColumnRef("grade"), Some("n_grades"))),
                   None)
    val result = run(plan)
    val parisCnt   = result.find(_.values("city") == "Paris").map(_.values("n_grades")).get
    val berlinCnt  = result.find(_.values("city") == "Berlin").map(_.values("n_grades")).get
    // Paris has grades B, A, A — 2 distinct; Berlin has only C — 1 distinct
    assertEquals(parisCnt,  2L)
    assertEquals(berlinCnt, 1L)

  test("CountDistinct on a single distinct value returns 1"):
    val plan   = Aggregate(src,
                   Vector(ColumnRef("city")),
                   Vector(CountDistinct(ColumnRef("city"), Some("n"))),
                   None)
    val result = run(plan)
    assert(result.forall(_.values("n") == 1L), "Each city group has exactly 1 distinct city value")

  // ---------------------------------------------------------------------------
  // StdDev
  // ---------------------------------------------------------------------------

  test("StdDev returns the sample standard deviation of a numeric column"):
    val plan   = Aggregate(src, Vector.empty, Vector(StdDev(ColumnRef("score"), Some("sd"))), None)
    val result = run(plan)
    val sd     = result(0).values("sd").asInstanceOf[Double]
    // scores: 80, 70, 90, 60, 100 → mean=80, sample variance=250, sd≈15.811
    assert(math.abs(sd - 15.811) < 0.01, s"Expected ~15.811 but got $sd")

  test("StdDev returns null for a group with a single row"):
    val single = Vector(Row(Map("v" -> 42)))
    val reg    = DataRegistry.of("memory://single" -> single)
    val be     = LocalBackend(reg)
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
    // Paris: 80, 90, 100 → mean=90, sample variance=100, sd=10.0
    val parisSd = result.find(_.values("city") == "Paris").map(_.values("sd")).get
    assert(parisSd != null, "Paris stddev should not be null (3 rows)")
    assert(math.abs(parisSd.asInstanceOf[Double] - 10.0) < 0.001,
           s"Expected ~10.0 for Paris but got $parisSd")

  // ---------------------------------------------------------------------------
  // Variance
  // ---------------------------------------------------------------------------

  test("Variance returns the sample variance of a numeric column"):
    val plan   = Aggregate(src, Vector.empty, Vector(Variance(ColumnRef("score"), Some("v"))), None)
    val result = run(plan)
    val v      = result(0).values("v").asInstanceOf[Double]
    // scores: 80, 70, 90, 60, 100 → mean=80, sample variance = (0+100+100+400+400)/4 = 250
    assert(math.abs(v - 250.0) < 0.01, s"Expected 250.0 but got $v")

  test("Variance returns null for a group with a single row"):
    val single = Vector(Row(Map("v" -> 7)))
    val reg    = DataRegistry.of("memory://single2" -> single)
    val be     = LocalBackend(reg)
    val plan   = Aggregate(ReadCsv("memory://single2", None), Vector.empty,
                           Vector(Variance(ColumnRef("v"), Some("var"))), None)
    val result = be.execute(plan) match
      case BackendResult.InMemory(rs) => rs
    assertEquals(result(0).values("var"), null)

  test("StdDev squared equals Variance for the same data"):
    val plan   = Aggregate(src, Vector.empty,
                   Vector(
                     StdDev(ColumnRef("score"),   Some("sd")),
                     Variance(ColumnRef("score"), Some("var"))
                   ), None)
    val result = run(plan)
    val sd  = result(0).values("sd").asInstanceOf[Double]
    val v   = result(0).values("var").asInstanceOf[Double]
    assert(math.abs(sd * sd - v) < 0.0001, s"sd²=${ sd*sd } should equal var=$v")

  // ---------------------------------------------------------------------------
  // DataFrame.agg()
  // ---------------------------------------------------------------------------

  test("DataFrame.agg aggregates the entire table without grouping"):
    val df     = Lumina.readCsv("memory://t")
    val result = df.agg(Count(None, Some("cnt")), Sum(ColumnRef("score"), Some("total"))).collect(backend)
    assertEquals(result.size, 1)
    assertEquals(result(0).values("cnt"), 5L)
    assertEquals(result(0).values("total"), 400.0)

  test("DataFrame.agg with CountDistinct on the whole table"):
    val df     = Lumina.readCsv("memory://t")
    val result = df.agg(CountDistinct(ColumnRef("city"), Some("n_cities"))).collect(backend)
    assertEquals(result(0).values("n_cities"), 3L)

  test("DataFrame.agg with StdDev on the whole table"):
    val df     = Lumina.readCsv("memory://t")
    val result = df.agg(StdDev(ColumnRef("score"), Some("sd"))).collect(backend)
    val sd     = result(0).values("sd").asInstanceOf[Double]
    assert(math.abs(sd - 15.811) < 0.01, s"Expected ~15.811 but got $sd")

  test("DataFrame.agg chained after filter computes stats on the filtered subset"):
    val df = Lumina.readCsv("memory://t")
      .filter(EqualTo(ColumnRef("city"), Literal("Paris")))
    // Paris rows: 80, 90, 100
    val result = df.agg(Count(None, Some("cnt")), Min(ColumnRef("score"), Some("lo"))).collect(backend)
    assertEquals(result(0).values("cnt"), 3L)
    assertEquals(result(0).values("lo"), 80.0)

  // ---------------------------------------------------------------------------
  // DataFrame.describe()
  // ---------------------------------------------------------------------------

  test("describeString returns a table with count mean stddev min and max rows"):
    val df  = Lumina.readCsv("memory://t")
    val str = df.describeString(backend)
    assert(str.contains("count"),  s"Expected 'count' row in: $str")
    assert(str.contains("mean"),   s"Expected 'mean' row in: $str")
    assert(str.contains("stddev"), s"Expected 'stddev' row in: $str")
    assert(str.contains("min"),    s"Expected 'min' row in: $str")
    assert(str.contains("max"),    s"Expected 'max' row in: $str")

  test("describeString includes every column as a header"):
    val df  = Lumina.readCsv("memory://t")
    val str = df.describeString(backend)
    assert(str.contains("city"),  s"Expected 'city' column in: $str")
    assert(str.contains("score"), s"Expected 'score' column in: $str")

  test("describeString shows N/A for mean and stddev of non-numeric columns"):
    val df  = Lumina.readCsv("memory://t")
    val str = df.describeString(backend)
    // city is a string column — mean and stddev should be N/A
    assert(str.contains("N/A"), s"Expected N/A for non-numeric column in: $str")

  test("describeString on an empty dataset returns the empty marker"):
    val empty  = LocalBackend(DataRegistry.of("memory://e" -> Vector.empty[Row]))
    val df     = Lumina.readCsv("memory://e")
    val str    = df.describeString(empty)
    assert(str.contains("empty"), s"Expected empty marker in: $str")

  test("describe count row shows total row count for each column"):
    val df   = Lumina.readCsv("memory://t")
    val str  = df.describeString(backend)
    // All 5 rows are non-null → count should be '5' for every column
    assert(str.contains("| 5"), s"Expected count=5 in: $str")
