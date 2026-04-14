package lumina.backend.duckdb

import munit.FunSuite
import lumina.plan.*
import lumina.plan.Expression.*
import lumina.plan.backend.{DataRegistry, Row, BackendResult}

/**
 * Tests for arithmetic expressions and WithColumn executed via DuckDB.
 *
 * These tests verify that PlanToSql emits correct SQL for arithmetic and
 * that DuckDB evaluates it correctly, after RowNormalizer has unified types.
 */
class DuckDBArithmeticSpec extends FunSuite:

  private val rows = Vector(
    Row(Map("city" -> "Paris",  "price" -> 100.0, "tax" -> 20.0, "qty" -> 3)),
    Row(Map("city" -> "Berlin", "price" -> 200.0, "tax" -> 40.0, "qty" -> 1))
  )

  private val backend = DuckDBBackend(DataRegistry.of("memory://t" -> rows))

  private def run(plan: LogicalPlan): Vector[Row] =
    backend.execute(plan) match
      case BackendResult.InMemory(rs) => rs

  // ---------------------------------------------------------------------------
  // Arithmetic
  // ---------------------------------------------------------------------------

  test("Add via DuckDB computes the sum of two column values"):
    val plan  = WithColumn(ReadCsv("memory://t", None), "total", Add(ColumnRef("price"), ColumnRef("tax")))
    val paris = run(plan).find(_.values("city") == "Paris").get
    assertEquals(paris.values("total").asInstanceOf[Double], 120.0)

  test("Subtract via DuckDB computes the difference of two column values"):
    val plan   = WithColumn(ReadCsv("memory://t", None), "net", Subtract(ColumnRef("price"), ColumnRef("tax")))
    val berlin = run(plan).find(_.values("city") == "Berlin").get
    assertEquals(berlin.values("net").asInstanceOf[Double], 160.0)

  test("Multiply via DuckDB computes the product of two column values"):
    val plan  = WithColumn(ReadCsv("memory://t", None), "revenue", Multiply(ColumnRef("price"), ColumnRef("qty")))
    val paris = run(plan).find(_.values("city") == "Paris").get
    assertEquals(paris.values("revenue").asInstanceOf[Double], 300.0)

  test("nested arithmetic via DuckDB composes correctly"):
    val expr = Multiply(Add(ColumnRef("price"), ColumnRef("tax")), ColumnRef("qty"))
    val plan = WithColumn(ReadCsv("memory://t", None), "gross", expr)
    val paris = run(plan).find(_.values("city") == "Paris").get
    assertEquals(paris.values("gross").asInstanceOf[Double], 360.0)

  // ---------------------------------------------------------------------------
  // WithColumn semantics
  // ---------------------------------------------------------------------------

  test("WithColumn via DuckDB preserves all existing columns alongside the new one"):
    val plan   = WithColumn(ReadCsv("memory://t", None), "total", Add(ColumnRef("price"), ColumnRef("tax")))
    val result = run(plan)
    assert(result.forall(_.values.contains("city")),  "city column missing")
    assert(result.forall(_.values.contains("total")), "total column missing")

  test("WithColumn via DuckDB replaces an existing column when the name matches"):
    val plan  = WithColumn(ReadCsv("memory://t", None), "price", Multiply(ColumnRef("price"), Literal(2)))
    val paris = run(plan).find(_.values("city") == "Paris").get
    assertEquals(paris.values("price").asInstanceOf[Double], 200.0)

  test("WithColumn result can be filtered on the derived column via DuckDB"):
    val plan = Filter(
      WithColumn(ReadCsv("memory://t", None), "total", Add(ColumnRef("price"), ColumnRef("tax"))),
      GreaterThan(ColumnRef("total"), Literal(150.0))
    )
    val result = run(plan)
    assertEquals(result.size, 1)
    assertEquals(result.head.values("city"), "Berlin")

  // ---------------------------------------------------------------------------
  // Duplicate column names in JOIN results
  // ---------------------------------------------------------------------------

  test("a self-join with duplicate column names returns deduplicated column names"):
    // Join customers with itself — both sides have 'city', 'price', 'tax', 'qty'.
    // Last-wins deduplication: first occurrence gets _1 suffix, last keeps clean name.
    val plan = Join(
      ReadCsv("memory://t", None),
      ReadCsv("memory://t", None),
      condition = Some(EqualTo(ColumnRef("city"), ColumnRef("city"))),
      joinType  = JoinType.Inner
    )
    val result = run(plan)
    assert(result.nonEmpty)
    // The first occurrence of 'city' is renamed city_1; the second keeps 'city'
    assert(result.head.values.contains("city_1"), s"Expected city_1, keys: ${result.head.values.keySet}")
    assert(result.head.values.contains("city"),   s"Expected city, keys: ${result.head.values.keySet}")
