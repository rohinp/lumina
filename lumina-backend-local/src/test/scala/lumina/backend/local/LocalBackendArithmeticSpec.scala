package lumina.backend.local

import munit.FunSuite
import lumina.plan.*
import lumina.plan.Expression.*
import lumina.plan.backend.*

/**
 * Tests for arithmetic expressions and WithColumn in LocalBackend.
 *
 * Read top-to-bottom to understand how derived columns are computed and
 * how arithmetic expressions compose with existing operators.
 */
class LocalBackendArithmeticSpec extends FunSuite:

  private val rows = Vector(
    Row(Map("city" -> "Paris",  "price" -> 100.0, "tax" -> 20.0, "qty" -> 3)),
    Row(Map("city" -> "Berlin", "price" -> 200.0, "tax" -> 40.0, "qty" -> 1))
  )

  private val backend = LocalBackend(DataRegistry.of("memory://t" -> rows))

  private def run(plan: LogicalPlan): Vector[Row] =
    backend.execute(plan) match
      case BackendResult.InMemory(rs) => rs

  // ---------------------------------------------------------------------------
  // Arithmetic expressions in Project / WithColumn
  // ---------------------------------------------------------------------------

  test("Add expression computes the sum of two column values"):
    val plan = WithColumn(ReadCsv("memory://t", None), "total", Add(ColumnRef("price"), ColumnRef("tax")))
    val paris = run(plan).find(_.values("city") == "Paris").get
    assertEquals(paris.values("total").asInstanceOf[Double], 120.0)

  test("Subtract expression computes the difference of two column values"):
    val plan = WithColumn(ReadCsv("memory://t", None), "net", Subtract(ColumnRef("price"), ColumnRef("tax")))
    val berlin = run(plan).find(_.values("city") == "Berlin").get
    assertEquals(berlin.values("net").asInstanceOf[Double], 160.0)

  test("Multiply expression computes the product of a column and a literal"):
    val plan = WithColumn(ReadCsv("memory://t", None), "revenue", Multiply(ColumnRef("price"), ColumnRef("qty")))
    val paris = run(plan).find(_.values("city") == "Paris").get
    assertEquals(paris.values("revenue").asInstanceOf[Double], 300.0)

  test("Divide expression computes the quotient of two column values"):
    val plan = WithColumn(ReadCsv("memory://t", None), "unit_tax", Divide(ColumnRef("tax"), ColumnRef("qty")))
    val paris = run(plan).find(_.values("city") == "Paris").get
    // tax=20, qty=3 → 20/3 ≈ 6.666
    assert(math.abs(paris.values("unit_tax").asInstanceOf[Double] - 20.0/3) < 0.001)

  test("Negate expression negates a column value"):
    val plan = WithColumn(ReadCsv("memory://t", None), "neg_price", Negate(ColumnRef("price")))
    val paris = run(plan).find(_.values("city") == "Paris").get
    assertEquals(paris.values("neg_price").asInstanceOf[Double], -100.0)

  test("nested arithmetic composes correctly"):
    // (price + tax) * qty
    val expr = Multiply(Add(ColumnRef("price"), ColumnRef("tax")), ColumnRef("qty"))
    val plan = WithColumn(ReadCsv("memory://t", None), "gross", expr)
    val paris = run(plan).find(_.values("city") == "Paris").get
    assertEquals(paris.values("gross").asInstanceOf[Double], 360.0)  // (100+20)*3

  // ---------------------------------------------------------------------------
  // WithColumn semantics
  // ---------------------------------------------------------------------------

  test("WithColumn preserves all existing columns alongside the new one"):
    val plan   = WithColumn(ReadCsv("memory://t", None), "total", Add(ColumnRef("price"), ColumnRef("tax")))
    val result = run(plan)
    assert(result.forall(r => r.values.contains("city")),  "city column missing")
    assert(result.forall(r => r.values.contains("price")), "price column missing")
    assert(result.forall(r => r.values.contains("total")), "total column missing")

  test("WithColumn overwrites an existing column when the name matches"):
    // Double the price in place
    val plan   = WithColumn(ReadCsv("memory://t", None), "price", Multiply(ColumnRef("price"), Literal(2)))
    val paris  = run(plan).find(_.values("city") == "Paris").get
    assertEquals(paris.values("price").asInstanceOf[Double], 200.0)

  test("WithColumn can be chained to add multiple derived columns"):
    val plan = WithColumn(
      WithColumn(ReadCsv("memory://t", None), "total", Add(ColumnRef("price"), ColumnRef("tax"))),
      "discounted", Multiply(ColumnRef("total"), Literal(0.9))
    )
    val paris = run(plan).find(_.values("city") == "Paris").get
    assertEquals(paris.values("total").asInstanceOf[Double],      120.0)
    assertEquals(paris.values("discounted").asInstanceOf[Double], 108.0)

  test("WithColumn result can be filtered on the new column"):
    val plan = Filter(
      WithColumn(ReadCsv("memory://t", None), "total", Add(ColumnRef("price"), ColumnRef("tax"))),
      GreaterThan(ColumnRef("total"), Literal(150.0))
    )
    val result = run(plan)
    assertEquals(result.size, 1)
    assertEquals(result.head.values("city"), "Berlin")  // 200+40 = 240 > 150

  // ---------------------------------------------------------------------------
  // Alias expression in Project
  // ---------------------------------------------------------------------------

  test("Alias expression in a Project gives a computed column its output name"):
    val plan = Project(
      ReadCsv("memory://t", None),
      Vector(ColumnRef("city"), Alias(Add(ColumnRef("price"), ColumnRef("tax")), "total")),
      schema = None
    )
    val result = run(plan)
    assert(result.forall(_.values.contains("total")), "total column missing")
    assert(result.forall(_.values.contains("city")),  "city column missing")
    assert(!result.head.values.contains("price"),     "price should not be in output")
