package lumina.plan.optimizer

import munit.FunSuite
import lumina.plan.*
import lumina.plan.Expression.*
import lumina.plan.WindowExpr

/**
 * Tests for the PredicatePushdown rewrite rule.
 *
 * Read these tests to understand when a Filter moves below a Project or Sort,
 * and when it stays in place because pushing is unsafe or not beneficial.
 */
class PredicatePushdownSpec extends FunSuite:

  private val base = ReadCsv("memory://t", None)

  // ---------------------------------------------------------------------------
  // Filter through Project
  // ---------------------------------------------------------------------------

  test("a Filter referencing only projected columns is pushed below the Project"):
    val plan = Filter(
      Project(base, Vector(ColumnRef("city"), ColumnRef("age")), schema = None),
      GreaterThan(ColumnRef("age"), Literal(30))
    )
    val result = PredicatePushdown(plan)
    result match
      case Project(Filter(ReadCsv(_, _), GreaterThan(ColumnRef("age"), _)), _, _) =>
        () // filter is now below project
      case other =>
        fail(s"Expected filter pushed below project, got: $other")

  test("a Filter referencing a column not in the Project stays above the Project"):
    val plan = Filter(
      Project(base, Vector(ColumnRef("city")), schema = None),
      GreaterThan(ColumnRef("age"), Literal(30))  // age not in [city]
    )
    val result = PredicatePushdown(plan)
    // must be unchanged — pushing would reference a dropped column
    result match
      case Filter(Project(_, _, _), _) => () // filter stays on top
      case other                        => fail(s"Expected Filter above Project, got: $other")

  test("a Filter with an And condition where all referenced columns are projected is pushed down"):
    val plan = Filter(
      Project(base, Vector(ColumnRef("city"), ColumnRef("age")), schema = None),
      And(EqualTo(ColumnRef("city"), Literal("Paris")), GreaterThan(ColumnRef("age"), Literal(18)))
    )
    val result = PredicatePushdown(plan)
    result match
      case Project(Filter(_, _), _, _) => ()
      case other                        => fail(s"Expected filter pushed below project, got: $other")

  // ---------------------------------------------------------------------------
  // Filter through Sort
  // ---------------------------------------------------------------------------

  test("a Filter above a Sort is pushed below the Sort to reduce rows before sorting"):
    val plan = Filter(
      Sort(base, Vector(SortExpr(ColumnRef("age"), ascending = true))),
      GreaterThan(ColumnRef("age"), Literal(30))
    )
    val result = PredicatePushdown(plan)
    result match
      case Sort(Filter(ReadCsv(_, _), _), _) => () // filter is now below sort
      case other                               => fail(s"Expected filter pushed below sort, got: $other")

  // ---------------------------------------------------------------------------
  // Filter stays above Aggregate and ReadCsv
  // ---------------------------------------------------------------------------

  test("a Filter above an Aggregate is not pushed through the Aggregate"):
    val plan = Filter(
      Aggregate(base, Vector(ColumnRef("city")), Vector(Aggregation.Sum(ColumnRef("revenue"))), None),
      GreaterThan(ColumnRef("city"), Literal("A"))
    )
    val result = PredicatePushdown(plan)
    result match
      case Filter(Aggregate(_, _, _, _), _) => () // left in place
      case other                             => fail(s"Expected Filter above Aggregate, got: $other")

  test("a Filter directly above a ReadCsv is left in place"):
    val plan   = Filter(base, GreaterThan(ColumnRef("age"), Literal(30)))
    val result = PredicatePushdown(plan)
    assertEquals(result, plan)

  // ---------------------------------------------------------------------------
  // Filter through Window
  // ---------------------------------------------------------------------------

  test("a Filter on a non-window column is pushed below the Window node"):
    val spec = WindowSpec(orderBy = Vector(SortExpr(ColumnRef("revenue"), ascending = true)))
    val plan = Filter(
      Window(base, Vector(WindowExpr.RowNumber("rn", spec))),
      GreaterThan(ColumnRef("revenue"), Literal(1000))  // revenue is not a window alias
    )
    val result = PredicatePushdown(plan)
    result match
      case Window(Filter(ReadCsv(_, _), _), _) => () // filter pushed below Window
      case other => fail(s"Expected filter pushed below Window, got: $other")

  test("a Filter referencing a window alias stays above the Window node"):
    val spec = WindowSpec(orderBy = Vector(SortExpr(ColumnRef("revenue"), ascending = true)))
    val plan = Filter(
      Window(base, Vector(WindowExpr.RowNumber("rn", spec))),
      GreaterThan(ColumnRef("rn"), Literal(2))  // rn is a window alias
    )
    val result = PredicatePushdown(plan)
    result match
      case Filter(Window(_, _), _) => () // filter stays above Window
      case other => fail(s"Expected Filter above Window, got: $other")

  // ---------------------------------------------------------------------------
  // Full optimizer pipeline
  // ---------------------------------------------------------------------------

  test("the default Optimizer moves all filters below a Project in a single bottom-up pass"):
    // Two stacked filters above a project.
    // Bottom-up: inner Filter(Project, age>18) → Project(Filter(ReadCsv, age>18))
    // Then outer Filter(Project(Filter(ReadCsv)), city='Paris') → Project(Filter(Filter(ReadCsv), city='Paris'))
    // Both filters end up inside the Project — the top-level node is a Project.
    val plan = Filter(
      Filter(
        Project(base, Vector(ColumnRef("city"), ColumnRef("age")), schema = None),
        GreaterThan(ColumnRef("age"), Literal(18))
      ),
      EqualTo(ColumnRef("city"), Literal("Paris"))
    )
    val result = Optimizer.optimize(plan)
    result match
      case Project(Filter(_, _), _, _) => ()  // both conditions pushed inside Project
      case other                        => fail(s"Unexpected shape after optimization: $other")
