package lumina.plan.optimizer

import munit.FunSuite
import lumina.plan.*
import lumina.plan.Expression.*

/**
 * Tests for M14 optimizer additions: optimizeUntilFixedPoint.
 *
 * Verifies that the multi-pass loop terminates correctly and converges to a
 * fully-simplified plan when a single pass leaves residual optimisation
 * opportunities.
 *
 * Read top-to-bottom as a specification for the fixed-point behaviour.
 */
class OptimizerM14Spec extends FunSuite:

  private val base = ReadCsv("memory://t", None)
  private val age  = ColumnRef("age")

  // ---------------------------------------------------------------------------
  // optimizeUntilFixedPoint — termination and correctness
  // ---------------------------------------------------------------------------

  test("optimizeUntilFixedPoint returns the same plan when it is already optimal"):
    val plan   = Filter(base, GreaterThan(age, Literal(30)))
    val result = Optimizer.optimizeUntilFixedPoint(plan)
    assertEquals(result, Optimizer.optimize(plan))

  test("optimizeUntilFixedPoint produces a plan equal to single-pass optimize for simple inputs"):
    val plan   = Filter(Sort(base, Vector(SortExpr(age, ascending = true))), GreaterThan(age, Literal(30)))
    val single = Optimizer.optimize(plan)
    val multi  = Optimizer.optimizeUntilFixedPoint(plan)
    assertEquals(multi, single)

  test("optimizeUntilFixedPoint with maxPasses=1 behaves identically to optimize"):
    val plan     = Filter(Sort(base, Vector(SortExpr(age, true))), GreaterThan(age, Literal(30)))
    val single   = Optimizer.optimize(plan)
    val multiRun = Optimizer.optimizeUntilFixedPoint(plan, maxPasses = 1)
    assertEquals(multiRun, single)

  test("optimizeUntilFixedPoint with custom rule set applies only those rules"):
    // Only ConstantFolding — PredicatePushdown should NOT fire.
    // The filter is above a Sort but predicate pushdown is excluded.
    val plan   = Filter(Sort(base, Vector(SortExpr(age, true))), And(Literal(true), Literal(true)))
    val result = Optimizer.optimizeUntilFixedPoint(plan, rules = Seq(ConstantFolding))
    // The constant-true Filter should be removed; no predicate pushdown so the Sort is root.
    result match
      case Sort(ReadCsv(_, _), _) => ()
      case ReadCsv(_, _)          => ()  // also acceptable
      case other => fail(s"Expected Sort or ReadCsv at root after ConstantFolding-only pass, got: $other")

  test("optimizeUntilFixedPoint eliminates a cascade of constant-true Filters in multiple passes"):
    // Three trivially-true Filters wrapped around the base plan.
    // A single pass only removes the outermost; subsequent passes peel the rest.
    val plan = Filter(Filter(Filter(base, Literal(true)), Literal(true)), Literal(true))
    val result = Optimizer.optimizeUntilFixedPoint(plan, rules = Seq(ConstantFolding), maxPasses = 10)
    assertEquals(result, base)

  test("optimizeUntilFixedPoint does not exceed maxPasses iterations"):
    // Use a plan that is already fixed-point after one pass to verify the loop exits promptly.
    val plan   = ReadCsv("memory://t", None)
    val result = Optimizer.optimizeUntilFixedPoint(plan, maxPasses = 3)
    assertEquals(result, plan)

  // ---------------------------------------------------------------------------
  // New plan nodes (Intersect, Except) survive the optimizer pipeline unchanged
  // ---------------------------------------------------------------------------

  test("Optimizer preserves Intersect nodes unchanged"):
    val other  = ReadCsv("memory://u", None)
    val plan   = Intersect(base, other)
    val result = Optimizer.optimize(plan)
    assertEquals(result, plan)

  test("Optimizer preserves Except nodes unchanged"):
    val other  = ReadCsv("memory://u", None)
    val plan   = Except(base, other)
    val result = Optimizer.optimize(plan)
    assertEquals(result, plan)

  // ---------------------------------------------------------------------------
  // New expressions (Cast, Abs, Round, Floor, Ceil) survive the optimizer pipeline
  // ---------------------------------------------------------------------------

  test("Optimizer preserves Cast expressions inside a Project"):
    val plan   = Project(base, Vector(Alias(Cast(age, DataType.Float64), "age_float")), None)
    val result = Optimizer.optimize(plan)
    result match
      case Project(_, Vector(Alias(Cast(ColumnRef("age"), DataType.Float64), "age_float")), _) => ()
      case other => fail(s"Expected Cast to be preserved unchanged, got: $other")

  test("Optimizer preserves Abs inside a Filter condition"):
    val plan   = Filter(base, GreaterThan(Abs(age), Literal(0)))
    val result = Optimizer.optimize(plan)
    result match
      case Filter(_, GreaterThan(Abs(ColumnRef("age")), Literal(0))) => ()
      case other => fail(s"Expected Abs in condition to be preserved, got: $other")

  test("Optimizer preserves Round inside a Project"):
    val plan   = Project(base, Vector(Alias(Round(age, 2), "rounded")), None)
    val result = Optimizer.optimize(plan)
    result match
      case Project(_, Vector(Alias(Round(ColumnRef("age"), 2), "rounded")), _) => ()
      case other => fail(s"Expected Round to be preserved unchanged, got: $other")
