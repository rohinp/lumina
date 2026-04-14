package lumina.backend.duckdb

import munit.FunSuite
import lumina.plan.*
import lumina.plan.Expression.*
import lumina.plan.Aggregation.*

/**
 * Unit tests for PlanToSql — each test names exactly what SQL fragment a given
 * plan node or expression must produce, so this file doubles as a specification
 * for the SQL translation layer.
 *
 * Read top-to-bottom to understand how each plan node maps to SQL before looking
 * at how DuckDBBackend executes those queries.
 */
class PlanToSqlSpec extends FunSuite:

  // ---------------------------------------------------------------------------
  // ReadCsv
  // ---------------------------------------------------------------------------

  test("a memory:// ReadCsv selects all columns from the corresponding table name"):
    val sql = PlanToSql.toSql(ReadCsv("memory://customers", None))
    assertEquals(sql, """SELECT * FROM "customers"""")

  test("a file ReadCsv uses DuckDB read_csv_auto with the given path"):
    val sql = PlanToSql.toSql(ReadCsv("data/sales.csv", None))
    assertEquals(sql, "SELECT * FROM read_csv_auto('data/sales.csv')")

  // ---------------------------------------------------------------------------
  // Filter
  // ---------------------------------------------------------------------------

  test("a Filter wraps its child in a subquery and adds a WHERE clause"):
    val plan = Filter(ReadCsv("memory://t", None), GreaterThan(ColumnRef("age"), Literal(30)))
    val sql  = PlanToSql.toSql(plan)
    assert(sql.contains("WHERE"), sql)
    assert(sql.contains("""SELECT * FROM ("""), sql)

  test("a GreaterThan filter condition renders as col > value"):
    val sql = PlanToSql.toSql(
      Filter(ReadCsv("memory://t", None), GreaterThan(ColumnRef("age"), Literal(30)))
    )
    assert(sql.contains(""""age" > 30"""), sql)

  test("an EqualTo filter on a string literal single-quotes the value"):
    val sql = PlanToSql.toSql(
      Filter(ReadCsv("memory://t", None), EqualTo(ColumnRef("city"), Literal("Paris")))
    )
    assert(sql.contains(""""city" = 'Paris'"""), sql)

  test("string literals with embedded single-quotes are escaped"):
    val expr = exprSqlFor(Literal("O'Brien"))
    assertEquals(expr, "'O''Brien'")

  // ---------------------------------------------------------------------------
  // Project
  // ---------------------------------------------------------------------------

  test("a Project lists the selected column names in the SELECT clause"):
    val plan = Project(
      ReadCsv("memory://t", None),
      Vector(ColumnRef("city"), ColumnRef("revenue")),
      schema = None
    )
    val sql = PlanToSql.toSql(plan)
    assert(sql.startsWith("""SELECT "city", "revenue" FROM"""), sql)

  // ---------------------------------------------------------------------------
  // Aggregate
  // ---------------------------------------------------------------------------

  test("an Aggregate with groupBy emits GROUP BY and SUM"):
    val plan = Aggregate(
      ReadCsv("memory://t", None),
      groupBy      = Vector(ColumnRef("city")),
      aggregations = Vector(Sum(ColumnRef("revenue"), alias = Some("total"))),
      schema       = None
    )
    val sql = PlanToSql.toSql(plan)
    assert(sql.contains("GROUP BY"), sql)
    assert(sql.contains("""SUM("revenue") AS "total""""), sql)

  test("a COUNT(*) aggregation renders as COUNT(*)"):
    val plan = Aggregate(
      ReadCsv("memory://t", None),
      groupBy      = Vector(ColumnRef("city")),
      aggregations = Vector(Count(None, alias = Some("n"))),
      schema       = None
    )
    val sql = PlanToSql.toSql(plan)
    assert(sql.contains("""COUNT(*) AS "n""""), sql)

  test("an Aggregate without groupBy omits the GROUP BY clause"):
    val plan = Aggregate(
      ReadCsv("memory://t", None),
      groupBy      = Vector.empty,
      aggregations = Vector(Count(None, alias = Some("total"))),
      schema       = None
    )
    val sql = PlanToSql.toSql(plan)
    assert(!sql.contains("GROUP BY"), sql)

  // ---------------------------------------------------------------------------
  // UnionAll and Distinct
  // ---------------------------------------------------------------------------

  test("UnionAll wraps both children with UNION ALL and a surrounding SELECT"):
    val plan = UnionAll(ReadCsv("memory://a", None), ReadCsv("memory://b", None))
    val sql  = PlanToSql.toSql(plan)
    assert(sql.contains("UNION ALL"), sql)
    assert(sql.contains("_union"), sql)

  test("Distinct uses SELECT DISTINCT * from a subquery"):
    val plan = Distinct(ReadCsv("memory://t", None))
    val sql  = PlanToSql.toSql(plan)
    assert(sql.startsWith("SELECT DISTINCT *"), sql)
    assert(sql.contains("_distinct"), sql)

  // ---------------------------------------------------------------------------
  // Helper — access package-private method for expression tests
  // ---------------------------------------------------------------------------

  private def exprSqlFor(expr: Expression): String = PlanToSql.exprSql(expr)
