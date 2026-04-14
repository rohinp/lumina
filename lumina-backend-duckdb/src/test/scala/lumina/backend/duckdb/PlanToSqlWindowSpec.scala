package lumina.backend.duckdb

import munit.FunSuite
import lumina.plan.*
import lumina.plan.Expression.*
import lumina.plan.Aggregation.*
import lumina.plan.WindowExpr.*

/**
 * Tests for PlanToSql's translation of Window plan nodes to SQL OVER clauses.
 *
 * Verifies the exact SQL string produced rather than execution semantics — that
 * is covered by DuckDBBackendWindowSpec.  Read top-to-bottom to understand the
 * SQL template each window function variant produces.
 */
class PlanToSqlWindowSpec extends FunSuite:

  private val src = ReadCsv("memory://t", None)

  // ---------------------------------------------------------------------------
  // Ranking functions
  // ---------------------------------------------------------------------------

  test("RowNumber with partition and order produces ROW_NUMBER() OVER (PARTITION BY ... ORDER BY ...)"):
    val spec = WindowSpec(
      partitionBy = Vector(ColumnRef("city")),
      orderBy     = Vector(SortExpr(ColumnRef("revenue"), ascending = true))
    )
    val sql = PlanToSql.toSql(Window(src, Vector(RowNumber("rn", spec))))
    assert(sql.contains("""ROW_NUMBER() OVER (PARTITION BY "city" ORDER BY "revenue" ASC) AS "rn""""),
      s"actual: $sql")

  test("Rank without partition produces RANK() OVER (ORDER BY ...)"):
    val spec = WindowSpec(orderBy = Vector(SortExpr(ColumnRef("score"), ascending = false)))
    val sql  = PlanToSql.toSql(Window(src, Vector(Rank("rnk", spec))))
    assert(sql.contains("""RANK() OVER (ORDER BY "score" DESC) AS "rnk""""), s"actual: $sql")

  test("DenseRank produces DENSE_RANK() OVER (...)"):
    val spec = WindowSpec(orderBy = Vector(SortExpr(ColumnRef("score"), ascending = true)))
    val sql  = PlanToSql.toSql(Window(src, Vector(DenseRank("dr", spec))))
    assert(sql.contains("""DENSE_RANK() OVER (ORDER BY "score" ASC) AS "dr""""), s"actual: $sql")

  // ---------------------------------------------------------------------------
  // WindowAgg
  // ---------------------------------------------------------------------------

  test("WindowAgg sum produces SUM(...) OVER (PARTITION BY ...)"):
    val spec = WindowSpec(partitionBy = Vector(ColumnRef("city")))
    val sql  = PlanToSql.toSql(Window(src, Vector(WindowAgg(Sum(ColumnRef("revenue")), "total", spec))))
    assert(sql.contains("""SUM("revenue") OVER (PARTITION BY "city" ) AS "total""""), s"actual: $sql")

  test("WindowAgg count star produces COUNT(*) OVER (...)"):
    val spec = WindowSpec(partitionBy = Vector(ColumnRef("dept")))
    val sql  = PlanToSql.toSql(Window(src, Vector(WindowAgg(Count(None), "cnt", spec))))
    assert(sql.contains("""COUNT(*) OVER (PARTITION BY "dept" ) AS "cnt""""), s"actual: $sql")

  // ---------------------------------------------------------------------------
  // Lag and Lead
  // ---------------------------------------------------------------------------

  test("Lag produces LAG(..., offset) OVER (...) AS alias"):
    val spec = WindowSpec(
      partitionBy = Vector(ColumnRef("city")),
      orderBy     = Vector(SortExpr(ColumnRef("revenue"), ascending = true))
    )
    val sql = PlanToSql.toSql(Window(src, Vector(Lag(ColumnRef("revenue"), 1, "prev", spec))))
    assert(sql.contains("""LAG("revenue", 1) OVER (PARTITION BY "city" ORDER BY "revenue" ASC) AS "prev""""),
      s"actual: $sql")

  test("Lead produces LEAD(..., offset) OVER (...) AS alias"):
    val spec = WindowSpec(
      partitionBy = Vector(ColumnRef("city")),
      orderBy     = Vector(SortExpr(ColumnRef("revenue"), ascending = true))
    )
    val sql = PlanToSql.toSql(Window(src, Vector(Lead(ColumnRef("revenue"), 2, "next2", spec))))
    assert(sql.contains("""LEAD("revenue", 2) OVER (PARTITION BY "city" ORDER BY "revenue" ASC) AS "next2""""),
      s"actual: $sql")

  // ---------------------------------------------------------------------------
  // Multiple window expressions
  // ---------------------------------------------------------------------------

  test("Multiple window expressions in one Window node are all included in the SELECT list"):
    val spec = WindowSpec(partitionBy = Vector(ColumnRef("city")))
    val sql  = PlanToSql.toSql(Window(src, Vector(
      RowNumber("rn", spec.copy(orderBy = Vector(SortExpr(ColumnRef("revenue"), true)))),
      WindowAgg(Sum(ColumnRef("revenue")), "total", spec)
    )))
    assert(sql.contains("""ROW_NUMBER()"""), s"actual: $sql")
    assert(sql.contains("""SUM("revenue")"""),  s"actual: $sql")

  // ---------------------------------------------------------------------------
  // Overall SQL shape
  // ---------------------------------------------------------------------------

  test("Window node produces SELECT *, <window cols> FROM (child) AS _window"):
    val spec = WindowSpec(orderBy = Vector(SortExpr(ColumnRef("x"), ascending = true)))
    val sql  = PlanToSql.toSql(Window(src, Vector(RowNumber("rn", spec))))
    assert(sql.startsWith("SELECT *, "), s"actual: $sql")
    assert(sql.contains("FROM (") && sql.contains(") AS _window"), s"actual: $sql")
