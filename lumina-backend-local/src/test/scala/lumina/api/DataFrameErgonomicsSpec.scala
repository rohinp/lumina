package lumina.api

import munit.FunSuite
import lumina.plan.*
import lumina.plan.Expression.*
import lumina.plan.backend.*
import lumina.backend.local.LocalBackend
import lumina.plan.backend.DataRegistry

/**
 * Tests for M23 DataFrame API ergonomics: where, select(String*),
 * orderBy(String, Boolean), col, transform, and Lumina.col.
 *
 * Read top-to-bottom as a specification for each shorthand method.
 */
class DataFrameErgonomicsSpec extends FunSuite:

  private val rows = Vector(
    Row(Map("city" -> "Paris",  "age" -> 35, "revenue" -> 1000.0)),
    Row(Map("city" -> "Berlin", "age" -> 29, "revenue" -> 2000.0)),
    Row(Map("city" -> "Tokyo",  "age" -> 42, "revenue" ->  500.0)),
    Row(Map("city" -> "Paris",  "age" -> 28, "revenue" -> 1500.0))
  )

  private val backend = LocalBackend(DataRegistry.of("memory://t" -> rows))
  private val df      = Lumina.readCsv("memory://t")

  private def run(d: DataFrame): Vector[Row] = d.collect(backend)

  // ---------------------------------------------------------------------------
  // where — alias for filter
  // ---------------------------------------------------------------------------

  test("where is equivalent to filter for the same condition"):
    val cond    = GreaterThan(ColumnRef("age"), Literal(30))
    val filtered = run(df.filter(cond))
    val whered   = run(df.where(cond))
    assertEquals(whered, filtered)

  test("where keeps only rows satisfying the predicate"):
    val result = run(df.where(EqualTo(ColumnRef("city"), Literal("Paris"))))
    assertEquals(result.size, 2)
    assert(result.forall(_.values("city") == "Paris"))

  test("where can be chained with other DataFrame operations"):
    val result = run(
      df.where(GreaterThan(ColumnRef("revenue"), Literal(800.0)))
        .where(LessThan(ColumnRef("age"), Literal(36)))
    )
    assertEquals(result.size, 3)  // Paris 35 (1000), Berlin 29 (2000), Paris 28 (1500)

  // ---------------------------------------------------------------------------
  // select(String*) — select columns by name
  // ---------------------------------------------------------------------------

  test("select with column name strings keeps only those columns"):
    val result = run(df.select("city", "age"))
    assert(result.forall(r => r.values.keySet == Set("city", "age")))

  test("select with a single column name returns only that column"):
    val result = run(df.select("revenue"))
    assertEquals(result.size, rows.size)
    assert(result.forall(r => r.values.keySet == Set("revenue")))

  test("select by name produces the same result as select by ColumnRef"):
    val byName = run(df.select("city", "revenue"))
    val byRef  = run(df.select(ColumnRef("city"), ColumnRef("revenue")))
    assertEquals(byName, byRef)

  test("select by name preserves the row count"):
    assertEquals(run(df.select("city")).size, rows.size)

  // ---------------------------------------------------------------------------
  // orderBy(String, Boolean) — sort by column name
  // ---------------------------------------------------------------------------

  test("orderBy with a column name sorts ascending by default"):
    val result = run(df.orderBy("age"))
    val ages   = result.map(_.values("age").asInstanceOf[Int])
    assertEquals(ages, ages.sorted)

  test("orderBy ascending = false sorts descending"):
    val result = run(df.orderBy("revenue", ascending = false))
    val revs   = result.map(_.values("revenue").asInstanceOf[Double])
    assertEquals(revs, revs.sorted.reverse)

  test("orderBy produces the same result as sort with SortExpr"):
    val byName = run(df.orderBy("age"))
    val byExpr = run(df.sort(SortExpr(ColumnRef("age"), ascending = true)))
    assertEquals(byName, byExpr)

  test("orderBy can be chained after a filter"):
    val result = run(
      df.where(EqualTo(ColumnRef("city"), Literal("Paris")))
        .orderBy("age")
    )
    val ages = result.map(_.values("age").asInstanceOf[Int])
    assertEquals(ages, Vector(28, 35))

  // ---------------------------------------------------------------------------
  // col — ColumnRef shorthand on the DataFrame instance
  // ---------------------------------------------------------------------------

  test("col returns a ColumnRef that can be used in expressions"):
    val result = run(df.filter(GreaterThan(df.col("age"), Literal(35))))
    assertEquals(result.size, 1)
    assertEquals(result(0).values("city"), "Tokyo")

  test("col on a DataFrame instance is equivalent to ColumnRef"):
    val viaCol = df.col("city")
    val viaRef = ColumnRef("city")
    assertEquals(viaCol, viaRef)

  // ---------------------------------------------------------------------------
  // Lumina.col — static ColumnRef shorthand
  // ---------------------------------------------------------------------------

  test("Lumina.col returns a ColumnRef that can be used in expressions"):
    val result = run(df.filter(LessThan(Lumina.col("age"), Literal(30))))
    assertEquals(result.size, 2)
    assert(result.forall(_.values("age").asInstanceOf[Int] < 30))

  test("Lumina.col is equivalent to ColumnRef"):
    assertEquals(Lumina.col("revenue"), ColumnRef("revenue"))

  // ---------------------------------------------------------------------------
  // transform — composable pipeline
  // ---------------------------------------------------------------------------

  test("transform applies the function and returns the result"):
    def addBonus(d: DataFrame): DataFrame =
      d.withColumn("bonus", Multiply(ColumnRef("revenue"), Literal(0.1)))
    val result = run(df.transform(addBonus))
    assert(result.forall(_.values.contains("bonus")))
    assert(math.abs(result(0).values("bonus").asInstanceOf[Double] - 100.0) < 1e-9)

  test("transform can be chained to compose multiple steps"):
    def onlyParis(d: DataFrame): DataFrame  = d.where(EqualTo(ColumnRef("city"), Literal("Paris")))
    def youngFirst(d: DataFrame): DataFrame = d.orderBy("age")
    val result = run(df.transform(onlyParis).transform(youngFirst))
    assertEquals(result.size, 2)
    assertEquals(result(0).values("age"), 28)
    assertEquals(result(1).values("age"), 35)

  test("transform is equivalent to calling the function directly"):
    def topRevenue(d: DataFrame): DataFrame =
      d.where(GreaterThan(ColumnRef("revenue"), Literal(1000.0)))
    assertEquals(run(df.transform(topRevenue)), run(topRevenue(df)))
