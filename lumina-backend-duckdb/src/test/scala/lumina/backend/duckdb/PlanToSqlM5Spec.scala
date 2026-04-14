package lumina.backend.duckdb

import munit.FunSuite
import lumina.plan.*
import lumina.plan.Expression.*
import lumina.plan.Aggregation.*

/**
 * Unit tests for PlanToSql covering M5 additions: new expressions, Sort,
 * Limit, Join, and Avg/Min/Max aggregations.
 *
 * Each test verifies the exact SQL fragment emitted, making this file a
 * specification of the SQL translation layer for the new operators.
 */
class PlanToSqlM5Spec extends FunSuite:

  private def sql(plan: LogicalPlan): String = PlanToSql.toSql(plan)
  private def expr(e: Expression): String    = PlanToSql.exprSql(e)

  // ---------------------------------------------------------------------------
  // New comparison expressions
  // ---------------------------------------------------------------------------

  test("GreaterThanOrEqual renders as col >= value"):
    assertEquals(expr(GreaterThanOrEqual(ColumnRef("age"), Literal(18))), """"age" >= 18""")

  test("LessThan renders as col < value"):
    assertEquals(expr(LessThan(ColumnRef("score"), Literal(50))), """"score" < 50""")

  test("LessThanOrEqual renders as col <= value"):
    assertEquals(expr(LessThanOrEqual(ColumnRef("score"), Literal(100))), """"score" <= 100""")

  test("NotEqualTo renders as col <> value"):
    assertEquals(expr(NotEqualTo(ColumnRef("city"), Literal("Berlin"))), """"city" <> 'Berlin'""")

  test("And renders as (left AND right)"):
    val e = And(GreaterThan(ColumnRef("age"), Literal(18)), LessThan(ColumnRef("age"), Literal(65)))
    assertEquals(expr(e), """("age" > 18 AND "age" < 65)""")

  test("Or renders as (left OR right)"):
    val e = Or(EqualTo(ColumnRef("city"), Literal("Paris")), EqualTo(ColumnRef("city"), Literal("Lyon")))
    assertEquals(expr(e), """("city" = 'Paris' OR "city" = 'Lyon')""")

  test("Not renders as NOT (expr)"):
    assertEquals(expr(Not(EqualTo(ColumnRef("active"), Literal(true)))), """NOT ("active" = true)""")

  test("IsNull renders as col IS NULL"):
    assertEquals(expr(IsNull(ColumnRef("email"))), """"email" IS NULL""")

  test("IsNotNull renders as col IS NOT NULL"):
    assertEquals(expr(IsNotNull(ColumnRef("email"))), """"email" IS NOT NULL""")

  // ---------------------------------------------------------------------------
  // Sort
  // ---------------------------------------------------------------------------

  test("Sort ascending emits ORDER BY col ASC"):
    val plan = Sort(ReadCsv("memory://t", None), Vector(SortExpr(ColumnRef("age"), ascending = true)))
    assert(sql(plan).contains("""ORDER BY "age" ASC"""), sql(plan))

  test("Sort descending emits ORDER BY col DESC"):
    val plan = Sort(ReadCsv("memory://t", None), Vector(SortExpr(ColumnRef("revenue"), ascending = false)))
    assert(sql(plan).contains("""ORDER BY "revenue" DESC"""), sql(plan))

  test("Sort by multiple columns emits all keys in order"):
    val plan = Sort(
      ReadCsv("memory://t", None),
      Vector(
        SortExpr(ColumnRef("city"), ascending = true),
        SortExpr(ColumnRef("age"),  ascending = false)
      )
    )
    assert(sql(plan).contains("""ORDER BY "city" ASC, "age" DESC"""), sql(plan))

  // ---------------------------------------------------------------------------
  // Limit
  // ---------------------------------------------------------------------------

  test("Limit emits LIMIT n in the SQL"):
    val plan = Limit(ReadCsv("memory://t", None), 10)
    assert(sql(plan).contains("LIMIT 10"), sql(plan))

  // ---------------------------------------------------------------------------
  // Avg / Min / Max aggregations
  // ---------------------------------------------------------------------------

  test("Avg aggregation renders as AVG(col) AS alias"):
    val plan = Aggregate(
      ReadCsv("memory://t", None),
      groupBy      = Vector(ColumnRef("city")),
      aggregations = Vector(Avg(ColumnRef("score"), alias = Some("avg_score"))),
      schema       = None
    )
    assert(sql(plan).contains("""AVG("score") AS "avg_score""""), sql(plan))

  test("Min aggregation renders as MIN(col) AS alias"):
    val plan = Aggregate(
      ReadCsv("memory://t", None),
      groupBy      = Vector(ColumnRef("city")),
      aggregations = Vector(Min(ColumnRef("age"), alias = Some("min_age"))),
      schema       = None
    )
    assert(sql(plan).contains("""MIN("age") AS "min_age""""), sql(plan))

  test("Max aggregation renders as MAX(col) AS alias"):
    val plan = Aggregate(
      ReadCsv("memory://t", None),
      groupBy      = Vector(ColumnRef("city")),
      aggregations = Vector(Max(ColumnRef("revenue"), alias = Some("max_rev"))),
      schema       = None
    )
    assert(sql(plan).contains("""MAX("revenue") AS "max_rev""""), sql(plan))

  // ---------------------------------------------------------------------------
  // Join
  // ---------------------------------------------------------------------------

  test("inner join emits INNER JOIN with an ON clause"):
    val plan = Join(
      ReadCsv("memory://a", None),
      ReadCsv("memory://b", None),
      condition = Some(EqualTo(ColumnRef("id"), ColumnRef("ref_id"))),
      joinType  = JoinType.Inner
    )
    assert(sql(plan).contains("INNER JOIN"), sql(plan))
    assert(sql(plan).contains("ON"), sql(plan))

  test("left join emits LEFT JOIN"):
    val plan = Join(
      ReadCsv("memory://a", None),
      ReadCsv("memory://b", None),
      condition = Some(EqualTo(ColumnRef("id"), ColumnRef("ref_id"))),
      joinType  = JoinType.Left
    )
    assert(sql(plan).contains("LEFT JOIN"), sql(plan))

  test("right join emits RIGHT JOIN"):
    val plan = Join(
      ReadCsv("memory://a", None),
      ReadCsv("memory://b", None),
      condition = Some(EqualTo(ColumnRef("id"), ColumnRef("ref_id"))),
      joinType  = JoinType.Right
    )
    assert(sql(plan).contains("RIGHT JOIN"), sql(plan))

  test("full outer join emits FULL OUTER JOIN"):
    val plan = Join(
      ReadCsv("memory://a", None),
      ReadCsv("memory://b", None),
      condition = Some(EqualTo(ColumnRef("id"), ColumnRef("ref_id"))),
      joinType  = JoinType.Full
    )
    assert(sql(plan).contains("FULL OUTER JOIN"), sql(plan))

  test("cross join emits INNER JOIN with no ON clause"):
    val plan = Join(
      ReadCsv("memory://a", None),
      ReadCsv("memory://b", None),
      condition = None,
      joinType  = JoinType.Inner
    )
    val s = sql(plan)
    assert(s.contains("INNER JOIN"), s)
    assert(!s.contains("ON"), s)
