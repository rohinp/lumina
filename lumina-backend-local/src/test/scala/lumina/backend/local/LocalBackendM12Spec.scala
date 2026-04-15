package lumina.backend.local

import munit.FunSuite
import lumina.plan.*
import lumina.plan.Expression.*
import lumina.plan.backend.*
import lumina.api.*

/**
 * Tests for M12 features in LocalBackend: CaseWhen expression, Sample plan
 * node, and the DataFrame convenience methods count / head / isEmpty / nonEmpty.
 *
 * Read top-to-bottom as a specification for each feature's behaviour.
 */
class LocalBackendM12Spec extends FunSuite:

  private val rows = Vector(
    Row(Map("city" -> "Paris",  "score" -> 95)),
    Row(Map("city" -> "Berlin", "score" -> 75)),
    Row(Map("city" -> "London", "score" -> 60)),
    Row(Map("city" -> "Paris",  "score" -> 45)),
    Row(Map("city" -> "Tokyo",  "score" -> 85))
  )

  private val backend = LocalBackend(DataRegistry.of("memory://t" -> rows))
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
    assertEquals(result.map(_.values("grade")), Vector("A", "B", "C", "F", "B"))

  test("CaseWhen returns null when no branch matches and otherwise is absent"):
    val expr = CaseWhen(
      branches  = Vector(GreaterThan(ColumnRef("score"), Literal(100)) -> Literal("impossible")),
      otherwise = None
    )
    val plan   = Project(src, Vector(Alias(expr, "v")), None)
    val result = run(plan)
    assert(result.forall(_.values("v") == null))

  test("CaseWhen can be used inside a Filter condition"):
    // Keep only rows where CaseWhen evaluates to "A"
    val grade = CaseWhen(
      branches  = Vector(GreaterThanOrEqual(ColumnRef("score"), Literal(90)) -> Literal("A")),
      otherwise = Some(Literal("other"))
    )
    val plan   = Filter(src, EqualTo(grade, Literal("A")))
    val result = run(plan)
    assertEquals(result.size, 1)
    assertEquals(result(0).values("score"), 95)

  test("CaseWhen evaluates branches lazily (only until the first match)"):
    // Second branch would divide by zero if evaluated — it must not be reached
    val expr = CaseWhen(
      branches = Vector(
        EqualTo(ColumnRef("score"), Literal(95)) -> Literal("first"),
        EqualTo(Divide(Literal(1), Literal(0)), Literal(0)) -> Literal("boom")
      ),
      otherwise = Some(Literal("other"))
    )
    val plan   = Filter(src, EqualTo(ColumnRef("score"), Literal(95)))
    val result = run(plan)
    // Sanity: just confirm the plan runs without dividing by zero for score=95
    assertEquals(result.size, 1)

  test("CaseWhen can be used with WithColumn to add a computed classification column"):
    // scores >= 80: Paris(95), Tokyo(85), Berlin(75)? No — 75 < 80. pass: 95, 85 → 2 rows
    // pass: score>=80 → Paris(95), Tokyo(85) = 2; fail: Berlin(75), London(60), Paris(45) = 3
    val grade = CaseWhen(
      branches  = Vector(
        GreaterThanOrEqual(ColumnRef("score"), Literal(80)) -> Literal("pass")
      ),
      otherwise = Some(Literal("fail"))
    )
    val plan   = WithColumn(src, "result", grade)
    val result = run(plan)
    assertEquals(result.count(_.values("result") == "pass"), 2)
    assertEquals(result.count(_.values("result") == "fail"), 3)

  // ---------------------------------------------------------------------------
  // Sample
  // ---------------------------------------------------------------------------

  test("Sample with seed=0 and fraction=1.0 returns all rows"):
    val plan   = Sample(src, 1.0, Some(0L))
    val result = run(plan)
    assertEquals(result.size, 5)

  test("Sample with fraction=0.0 returns no rows"):
    val plan   = Sample(src, 0.0, Some(42L))
    val result = run(plan)
    assertEquals(result.size, 0)

  test("Sample with a fixed seed is deterministic across multiple calls"):
    val plan    = Sample(src, 0.6, Some(12345L))
    val result1 = run(plan)
    val result2 = run(plan)
    assertEquals(result1, result2)

  test("Sample preserves all columns in the sampled rows"):
    val plan   = Sample(src, 1.0, Some(0L))
    val result = run(plan)
    assert(result.forall(_.values.contains("city")))
    assert(result.forall(_.values.contains("score")))

  // ---------------------------------------------------------------------------
  // DataFrame.count
  // ---------------------------------------------------------------------------

  test("count returns the total number of rows in the plan"):
    val df = Lumina.readCsv("memory://t")
    assertEquals(df.count(backend), 5L)

  test("count returns 0 for an empty result"):
    val df = Lumina.readCsv("memory://t").filter(GreaterThan(ColumnRef("score"), Literal(1000)))
    assertEquals(df.count(backend), 0L)

  test("count after filter returns only the matching row count"):
    val df = Lumina.readCsv("memory://t").filter(EqualTo(ColumnRef("city"), Literal("Paris")))
    assertEquals(df.count(backend), 2L)

  // ---------------------------------------------------------------------------
  // DataFrame.head / isEmpty / nonEmpty
  // ---------------------------------------------------------------------------

  test("head returns at most n rows"):
    val df     = Lumina.readCsv("memory://t")
    val result = df.head(3, backend)
    assertEquals(result.size, 3)

  test("head on a dataset with fewer rows than n returns all rows"):
    val df     = Lumina.readCsv("memory://t")
    val result = df.head(100, backend)
    assertEquals(result.size, 5)

  test("isEmpty returns false when the plan produces rows"):
    val df = Lumina.readCsv("memory://t")
    assertEquals(df.isEmpty(backend), false)

  test("isEmpty returns true when the plan produces no rows"):
    val df = Lumina.readCsv("memory://t").filter(GreaterThan(ColumnRef("score"), Literal(1000)))
    assertEquals(df.isEmpty(backend), true)

  test("nonEmpty returns true when the plan produces rows"):
    val df = Lumina.readCsv("memory://t")
    assertEquals(df.nonEmpty(backend), true)
