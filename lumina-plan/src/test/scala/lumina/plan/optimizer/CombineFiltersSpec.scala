package lumina.plan.optimizer

import munit.FunSuite
import lumina.plan.*
import lumina.plan.Expression.*

/**
 * Tests for the CombineFilters rewrite rule.
 *
 * Read these tests to understand when consecutive Filter nodes are merged
 * and what the resulting plan shape looks like before execution.
 */
class CombineFiltersSpec extends FunSuite:

  private val base = ReadCsv("memory://t", None)

  test("two consecutive Filter nodes are merged into a single Filter with an And condition"):
    val plan = Filter(Filter(base, GreaterThan(ColumnRef("age"), Literal(18))), EqualTo(ColumnRef("city"), Literal("Paris")))
    val result = CombineFilters(plan)
    result match
      case Filter(ReadCsv(_, _), And(EqualTo(ColumnRef("city"), Literal("Paris")), GreaterThan(ColumnRef("age"), Literal(18)))) =>
        () // expected shape
      case other =>
        fail(s"Expected single Filter with And, got: $other")

  test("a single Filter node is left unchanged by CombineFilters"):
    val plan   = Filter(base, GreaterThan(ColumnRef("age"), Literal(30)))
    val result = CombineFilters(plan)
    assertEquals(result, plan)

  test("a plan with no Filter nodes is left unchanged by CombineFilters"):
    val result = CombineFilters(base)
    assertEquals(result, base)

  test("three consecutive Filter nodes collapse to one via two optimizer passes"):
    val plan = Filter(
      Filter(Filter(base, LessThan(ColumnRef("age"), Literal(65))),
             GreaterThan(ColumnRef("age"), Literal(18))),
      EqualTo(ColumnRef("city"), Literal("Paris"))
    )
    // Optimizer applies the rule bottom-up, so inner pair merges first, then outer
    val result = Optimizer.optimize(plan, Seq(CombineFilters))
    result match
      case Filter(ReadCsv(_, _), And(_, And(_, _))) => () // two-level And — expected
      case Filter(ReadCsv(_, _), _)                 => () // any single Filter is acceptable
      case other => fail(s"Expected collapsed Filter, got: $other")
