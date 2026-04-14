package lumina.backend.duckdb

import munit.FunSuite
import lumina.plan.*
import lumina.plan.Expression.*
import lumina.plan.Aggregation.*
import lumina.plan.backend.{DataRegistry, Row, BackendResult}

/**
 * Tests for M5 operators executed via DuckDB: new expressions, Sort, Limit,
 * Join, and Avg/Min/Max aggregations.
 *
 * Each test is named as a specification sentence.  Read top-to-bottom to
 * understand how PlanToSql translates each operator to SQL and what DuckDB
 * returns before looking at the SQL strings in PlanToSqlSpec.
 */
class DuckDBBackendM5Spec extends FunSuite:

  private val customerRows = Vector(
    Row(Map("city" -> "Paris",  "age" -> 35, "revenue" -> 1000.0)),
    Row(Map("city" -> "Paris",  "age" -> 45, "revenue" -> 3000.0)),
    Row(Map("city" -> "Berlin", "age" -> 29, "revenue" -> 2000.0)),
    Row(Map("city" -> "Berlin", "age" -> 22, "revenue" ->  500.0))
  )

  private val orderRows = Vector(
    Row(Map("order_city" -> "Paris",  "amount" -> 100.0)),
    Row(Map("order_city" -> "Paris",  "amount" -> 200.0)),
    Row(Map("order_city" -> "London", "amount" ->  50.0))
  )

  private val backend = DuckDBBackend(DataRegistry.of(
    "memory://customers" -> customerRows,
    "memory://orders"    -> orderRows
  ))

  private def rows(plan: LogicalPlan): Vector[Row] =
    backend.execute(plan) match
      case BackendResult.InMemory(rs) => rs

  // ---------------------------------------------------------------------------
  // New comparison expressions
  // ---------------------------------------------------------------------------

  test("LessThan filter executed via DuckDB keeps rows below the threshold"):
    val plan = Filter(ReadCsv("memory://customers", None), LessThan(ColumnRef("age"), Literal(30)))
    assertEquals(rows(plan).size, 2)

  test("And filter executed via DuckDB requires both conditions to hold"):
    val plan = Filter(
      ReadCsv("memory://customers", None),
      And(EqualTo(ColumnRef("city"), Literal("Paris")), GreaterThan(ColumnRef("age"), Literal(40)))
    )
    assertEquals(rows(plan).size, 1)

  test("Or filter executed via DuckDB includes rows matching either condition"):
    val plan = Filter(
      ReadCsv("memory://customers", None),
      Or(EqualTo(ColumnRef("city"), Literal("Berlin")), GreaterThan(ColumnRef("age"), Literal(40)))
    )
    assertEquals(rows(plan).size, 3)

  test("Not filter executed via DuckDB inverts the predicate"):
    val plan = Filter(ReadCsv("memory://customers", None), Not(EqualTo(ColumnRef("city"), Literal("Paris"))))
    assertEquals(rows(plan).size, 2)

  // ---------------------------------------------------------------------------
  // Sort
  // ---------------------------------------------------------------------------

  test("Sort ascending executed via DuckDB orders rows from lowest to highest"):
    val plan = Sort(ReadCsv("memory://customers", None), Vector(SortExpr(ColumnRef("age"), ascending = true)))
    val ages = rows(plan).map(r => r.values("age").asInstanceOf[Int])
    assertEquals(ages, Vector(22, 29, 35, 45))

  test("Sort descending executed via DuckDB orders rows from highest to lowest"):
    val plan = Sort(ReadCsv("memory://customers", None), Vector(SortExpr(ColumnRef("age"), ascending = false)))
    val ages = rows(plan).map(r => r.values("age").asInstanceOf[Int])
    assertEquals(ages, Vector(45, 35, 29, 22))

  // ---------------------------------------------------------------------------
  // Limit
  // ---------------------------------------------------------------------------

  test("Limit executed via DuckDB returns at most n rows"):
    val plan = Limit(ReadCsv("memory://customers", None), 2)
    assertEquals(rows(plan).size, 2)

  test("Limit combined with Sort via DuckDB returns the top-n ordered rows"):
    val plan = Limit(
      Sort(ReadCsv("memory://customers", None), Vector(SortExpr(ColumnRef("revenue"), ascending = false))),
      1
    )
    val result = rows(plan)
    assertEquals(result.size, 1)
    assertEquals(result.head.values("revenue").asInstanceOf[Double], 3000.0)

  // ---------------------------------------------------------------------------
  // Avg / Min / Max aggregations
  // ---------------------------------------------------------------------------

  test("Avg aggregation executed via DuckDB returns the mean value per group"):
    val plan = Aggregate(
      ReadCsv("memory://customers", None),
      groupBy      = Vector(ColumnRef("city")),
      aggregations = Vector(Avg(ColumnRef("age"), alias = Some("avg_age"))),
      schema       = None
    )
    val paris = rows(plan).find(_.values("city") == "Paris").get
    assertEquals(paris.values("avg_age").asInstanceOf[Double], 40.0)

  test("Min aggregation executed via DuckDB returns the minimum value per group"):
    val plan = Aggregate(
      ReadCsv("memory://customers", None),
      groupBy      = Vector(ColumnRef("city")),
      aggregations = Vector(Min(ColumnRef("age"), alias = Some("min_age"))),
      schema       = None
    )
    val berlin = rows(plan).find(_.values("city") == "Berlin").get
    // RowNormalizer converts JDBC Integer to Scala Int
    assertEquals(berlin.values("min_age").asInstanceOf[Int], 22)

  test("Max aggregation executed via DuckDB returns the maximum value per group"):
    val plan = Aggregate(
      ReadCsv("memory://customers", None),
      groupBy      = Vector(ColumnRef("city")),
      aggregations = Vector(Max(ColumnRef("revenue"), alias = Some("max_rev"))),
      schema       = None
    )
    val paris = rows(plan).find(_.values("city") == "Paris").get
    assertEquals(paris.values("max_rev").asInstanceOf[Double], 3000.0)

  // ---------------------------------------------------------------------------
  // Join
  // ---------------------------------------------------------------------------

  test("inner join via DuckDB returns rows where the join condition holds on both sides"):
    val plan = Join(
      ReadCsv("memory://customers", None),
      ReadCsv("memory://orders",    None),
      condition = Some(EqualTo(ColumnRef("city"), ColumnRef("order_city"))),
      joinType  = JoinType.Inner
    )
    // 2 Paris customers × 2 Paris orders = 4 rows
    assertEquals(rows(plan).size, 4)

  test("left join via DuckDB includes all left rows regardless of whether a right row matches"):
    val plan = Join(
      ReadCsv("memory://customers", None),
      ReadCsv("memory://orders",    None),
      condition = Some(EqualTo(ColumnRef("city"), ColumnRef("order_city"))),
      joinType  = JoinType.Left
    )
    // 2 Paris × 2 orders = 4 matched, plus 2 Berlin with no match = 6 total
    assertEquals(rows(plan).size, 6)
