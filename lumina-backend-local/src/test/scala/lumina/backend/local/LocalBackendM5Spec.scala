package lumina.backend.local

import munit.FunSuite
import lumina.plan.*
import lumina.plan.Expression.*
import lumina.plan.Aggregation.*
import lumina.plan.backend.*

/**
 * Tests for M5 operators added to LocalBackend: new expressions (logical,
 * comparison, null-check), Sort, Limit, Join, and Avg/Min/Max aggregations.
 *
 * Read top-to-bottom to understand each operator's contract before looking
 * at the implementation in LocalBackend or the SQL equivalent in PlanToSql.
 */
class LocalBackendM5Spec extends FunSuite:

  private val rows = Vector(
    Row(Map("city" -> "Paris",  "age" -> 35, "revenue" -> 1000.0, "email" -> "a@b.com")),
    Row(Map("city" -> "Paris",  "age" -> 45, "revenue" -> 3000.0, "email" -> null)),
    Row(Map("city" -> "Berlin", "age" -> 29, "revenue" -> 2000.0, "email" -> "c@d.com")),
    Row(Map("city" -> "Berlin", "age" -> 22, "revenue" ->  500.0, "email" -> null))
  )

  private val backend = LocalBackend(DataRegistry.of("memory://t" -> rows))

  private def run(plan: LogicalPlan): Vector[Row] =
    backend.execute(plan) match
      case BackendResult.InMemory(rs) => rs

  // ---------------------------------------------------------------------------
  // New comparison expressions
  // ---------------------------------------------------------------------------

  test("LessThan filter keeps only rows where the column is below the threshold"):
    val plan = Filter(ReadCsv("memory://t", None), LessThan(ColumnRef("age"), Literal(30)))
    assertEquals(run(plan).size, 2)

  test("LessThanOrEqual filter includes rows where the column equals the threshold"):
    val plan = Filter(ReadCsv("memory://t", None), LessThanOrEqual(ColumnRef("age"), Literal(29)))
    assertEquals(run(plan).size, 2)

  test("GreaterThanOrEqual filter includes rows where the column equals the threshold"):
    val plan = Filter(ReadCsv("memory://t", None), GreaterThanOrEqual(ColumnRef("age"), Literal(35)))
    assertEquals(run(plan).size, 2)

  test("NotEqualTo filter excludes rows where the column matches the value"):
    val plan = Filter(ReadCsv("memory://t", None), NotEqualTo(ColumnRef("city"), Literal("Paris")))
    assertEquals(run(plan).size, 2)
    assert(run(plan).forall(_.values("city") == "Berlin"))

  // ---------------------------------------------------------------------------
  // Logical combinators
  // ---------------------------------------------------------------------------

  test("And filter keeps only rows where both conditions are true"):
    val plan = Filter(
      ReadCsv("memory://t", None),
      And(EqualTo(ColumnRef("city"), Literal("Paris")), GreaterThan(ColumnRef("age"), Literal(40)))
    )
    assertEquals(run(plan).size, 1)

  test("Or filter keeps rows where at least one condition is true"):
    val plan = Filter(
      ReadCsv("memory://t", None),
      Or(EqualTo(ColumnRef("city"), Literal("Berlin")), GreaterThan(ColumnRef("age"), Literal(40)))
    )
    assertEquals(run(plan).size, 3)

  test("Not filter inverts the predicate"):
    val plan = Filter(ReadCsv("memory://t", None), Not(EqualTo(ColumnRef("city"), Literal("Paris"))))
    assertEquals(run(plan).size, 2)
    assert(run(plan).forall(_.values("city") == "Berlin"))

  // ---------------------------------------------------------------------------
  // Null-check expressions
  // ---------------------------------------------------------------------------

  test("IsNull filter keeps only rows where the column value is null"):
    val plan = Filter(ReadCsv("memory://t", None), IsNull(ColumnRef("email")))
    assertEquals(run(plan).size, 2)

  test("IsNotNull filter keeps only rows where the column has a non-null value"):
    val plan = Filter(ReadCsv("memory://t", None), IsNotNull(ColumnRef("email")))
    assertEquals(run(plan).size, 2)
    assert(run(plan).forall(_.values("email") != null))

  // ---------------------------------------------------------------------------
  // Sort
  // ---------------------------------------------------------------------------

  test("Sort ascending orders rows from lowest to highest value"):
    val plan = Sort(ReadCsv("memory://t", None), Vector(SortExpr(ColumnRef("age"), ascending = true)))
    val ages = run(plan).map(_.values("age").asInstanceOf[Int])
    assertEquals(ages, Vector(22, 29, 35, 45))

  test("Sort descending orders rows from highest to lowest value"):
    val plan = Sort(ReadCsv("memory://t", None), Vector(SortExpr(ColumnRef("age"), ascending = false)))
    val ages = run(plan).map(_.values("age").asInstanceOf[Int])
    assertEquals(ages, Vector(45, 35, 29, 22))

  test("Sort by multiple keys uses the second key to break ties"):
    val plan = Sort(
      ReadCsv("memory://t", None),
      Vector(
        SortExpr(ColumnRef("city"),    ascending = true),
        SortExpr(ColumnRef("revenue"), ascending = false)
      )
    )
    val result = run(plan)
    // Berlin comes first (alphabetical), then Paris; within each city, higher revenue first
    assertEquals(result.head.values("city"),    "Berlin")
    assertEquals(result.head.values("revenue"), 2000.0)
    assertEquals(result(2).values("city"),      "Paris")
    assertEquals(result(2).values("revenue"),   3000.0)

  // ---------------------------------------------------------------------------
  // Limit
  // ---------------------------------------------------------------------------

  test("Limit returns at most n rows"):
    val plan = Limit(ReadCsv("memory://t", None), 2)
    assertEquals(run(plan).size, 2)

  test("Limit larger than the dataset returns all rows"):
    val plan = Limit(ReadCsv("memory://t", None), 100)
    assertEquals(run(plan).size, 4)

  test("Limit combined with Sort returns the top-n ordered rows"):
    val plan = Limit(
      Sort(ReadCsv("memory://t", None), Vector(SortExpr(ColumnRef("revenue"), ascending = false))),
      2
    )
    val result = run(plan)
    assertEquals(result.size, 2)
    assertEquals(result.head.values("revenue"), 3000.0)

  // ---------------------------------------------------------------------------
  // Avg / Min / Max aggregations
  // ---------------------------------------------------------------------------

  test("Avg aggregation returns the mean value per group"):
    val plan = Aggregate(
      ReadCsv("memory://t", None),
      groupBy      = Vector(ColumnRef("city")),
      aggregations = Vector(Avg(ColumnRef("age"), alias = Some("avg_age"))),
      schema       = None
    )
    val result = run(plan)
    val paris  = result.find(_.values("city") == "Paris").get
    assertEquals(paris.values("avg_age").asInstanceOf[Double], 40.0)

  test("Min aggregation returns the minimum value per group"):
    val plan = Aggregate(
      ReadCsv("memory://t", None),
      groupBy      = Vector(ColumnRef("city")),
      aggregations = Vector(Min(ColumnRef("age"), alias = Some("min_age"))),
      schema       = None
    )
    val paris = run(plan).find(_.values("city") == "Paris").get
    // age is stored as Int in the row; Min returns the raw value unchanged
    assertEquals(paris.values("min_age").asInstanceOf[Int], 35)

  test("Max aggregation returns the maximum value per group"):
    val plan = Aggregate(
      ReadCsv("memory://t", None),
      groupBy      = Vector(ColumnRef("city")),
      aggregations = Vector(Max(ColumnRef("revenue"), alias = Some("max_rev"))),
      schema       = None
    )
    val paris = run(plan).find(_.values("city") == "Paris").get
    assertEquals(paris.values("max_rev").asInstanceOf[Double], 3000.0)

  // ---------------------------------------------------------------------------
  // Join
  // ---------------------------------------------------------------------------

  private val ordersRows = Vector(
    Row(Map("order_city" -> "Paris",  "amount" -> 100.0)),
    Row(Map("order_city" -> "Paris",  "amount" -> 200.0)),
    Row(Map("order_city" -> "London", "amount" ->  50.0))
  )

  private val joinBackend = LocalBackend(
    DataRegistry.of(
      "memory://t"      -> rows,
      "memory://orders" -> ordersRows
    )
  )

  private def runJoin(plan: LogicalPlan): Vector[Row] =
    joinBackend.execute(plan) match
      case BackendResult.InMemory(rs) => rs

  test("inner join returns only rows where the condition matches on both sides"):
    val plan = Join(
      ReadCsv("memory://t",      None),
      ReadCsv("memory://orders", None),
      condition = Some(EqualTo(ColumnRef("city"), ColumnRef("order_city"))),
      joinType  = JoinType.Inner
    )
    val result = runJoin(plan)
    // 2 Paris customers × 2 Paris orders = 4 rows
    assertEquals(result.size, 4)
    assert(result.forall(r => r.values("city") == "Paris"))

  test("left join includes all left rows even when no right row matches"):
    val plan = Join(
      ReadCsv("memory://t",      None),
      ReadCsv("memory://orders", None),
      condition = Some(EqualTo(ColumnRef("city"), ColumnRef("order_city"))),
      joinType  = JoinType.Left
    )
    val result = runJoin(plan)
    // 2 Paris × 2 orders = 4, plus 2 Berlin with no match = 6 total
    assertEquals(result.size, 6)

  test("cross join produces the cartesian product of both datasets"):
    val plan = Join(
      ReadCsv("memory://t",      None),
      ReadCsv("memory://orders", None),
      condition = None,
      joinType  = JoinType.Inner
    )
    val result = runJoin(plan)
    assertEquals(result.size, rows.size * ordersRows.size)
