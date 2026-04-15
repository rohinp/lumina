package lumina.backend.duckdb

import munit.FunSuite
import lumina.plan.*
import lumina.plan.Expression.*
import lumina.plan.backend.*

/**
 * Tests for M12 features in DuckDBBackend: CaseWhen expression, Sample plan
 * node, and the DataFrame convenience methods count / head / isEmpty.
 *
 * Mirrors LocalBackendM12Spec for cross-backend parity.
 */
class DuckDBM12Spec extends FunSuite:

  private val rows = Vector(
    Row(Map("city" -> "Paris",  "score" -> 95)),
    Row(Map("city" -> "Berlin", "score" -> 75)),
    Row(Map("city" -> "London", "score" -> 60)),
    Row(Map("city" -> "Paris",  "score" -> 45)),
    Row(Map("city" -> "Tokyo",  "score" -> 85))
  )

  private val backend = DuckDBBackend(DataRegistry.of("memory://t" -> rows))
  private val src     = ReadCsv("memory://t", None)

  private def run(plan: LogicalPlan): Vector[Row] =
    backend.execute(plan) match
      case BackendResult.InMemory(rs) => rs

  // ---------------------------------------------------------------------------
  // CaseWhen
  // ---------------------------------------------------------------------------

  test("CaseWhen returns the value from the first matching branch"):
    val grade = CaseWhen(
      branches  = Vector(
        GreaterThanOrEqual(ColumnRef("score"), Literal(90)) -> Literal("A"),
        GreaterThanOrEqual(ColumnRef("score"), Literal(70)) -> Literal("B"),
        GreaterThanOrEqual(ColumnRef("score"), Literal(50)) -> Literal("C")
      ),
      otherwise = Some(Literal("F"))
    )
    val plan   = Project(src, Vector(Alias(grade, "grade")), None)
    val result = run(plan)
    val grades = result.map(_.values("grade"))
    assert(grades.contains("A"))
    assert(grades.contains("B"))
    assert(grades.contains("F"))

  test("CaseWhen returns null when no branch matches and otherwise is absent"):
    val expr   = CaseWhen(
      branches  = Vector(GreaterThan(ColumnRef("score"), Literal(1000)) -> Literal("x")),
      otherwise = None
    )
    val plan   = Project(src, Vector(Alias(expr, "v")), None)
    val result = run(plan)
    assert(result.forall(_.values("v") == null))

  test("CaseWhen SQL shape includes CASE WHEN ... THEN ... ELSE ... END"):
    val expr = CaseWhen(
      branches  = Vector(GreaterThan(ColumnRef("score"), Literal(80)) -> Literal("high")),
      otherwise = Some(Literal("low"))
    )
    val sql = PlanToSql.toSql(Project(src, Vector(Alias(expr, "bucket")), None))
    assert(sql.contains("CASE WHEN"), sql)
    assert(sql.contains("THEN"), sql)
    assert(sql.contains("ELSE"), sql)
    assert(sql.contains("END"), sql)

  // ---------------------------------------------------------------------------
  // Sample
  // ---------------------------------------------------------------------------

  test("Sample with fraction=1.0 returns all rows"):
    val plan   = Sample(src, 1.0, None)
    val result = run(plan)
    assertEquals(result.size, 5)

  test("Sample with fraction=0.0 returns no rows"):
    val plan   = Sample(src, 0.0, None)
    val result = run(plan)
    assertEquals(result.size, 0)

  test("Sample SQL contains USING SAMPLE and PERCENT"):
    val sql = PlanToSql.toSql(Sample(src, 0.5, None))
    assert(sql.contains("USING SAMPLE"), sql)
    assert(sql.contains("PERCENT"), sql)
    assert(sql.contains("bernoulli"), sql)

  // ---------------------------------------------------------------------------
  // count / head convenience via direct plan
  // ---------------------------------------------------------------------------

  test("COUNT(*) aggregate over all rows returns 5"):
    val plan   = Aggregate(src, Vector.empty, Vector(Aggregation.Count(None, Some("n"))), None)
    val result = run(plan)
    assertEquals(result.head.values("n").asInstanceOf[Number].longValue(), 5L)

  test("Limit(2) returns at most 2 rows"):
    val result = run(Limit(src, 2))
    assertEquals(result.size, 2)

  test("Filter that matches nothing produces an empty result"):
    val result = run(Filter(src, GreaterThan(ColumnRef("score"), Literal(1000))))
    assertEquals(result.size, 0)
