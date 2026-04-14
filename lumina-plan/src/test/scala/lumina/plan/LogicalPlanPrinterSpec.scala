package lumina.plan

import munit.FunSuite
import lumina.plan.Expression.*
import lumina.plan.Aggregation.*

/**
 * Tests for LogicalPlanPrinter — each test reads like a specification of what
 * the explain output must contain for a given plan shape.
 *
 * Read top-to-bottom to understand how each plan node is rendered and how
 * the tree structure is presented to the developer.
 */
class LogicalPlanPrinterSpec extends FunSuite:

  private val schema = Schema(
    Vector(
      Column("city",    DataType.StringType),
      Column("age",     DataType.Int32),
      Column("revenue", DataType.Float64)
    )
  )

  // ---------------------------------------------------------------------------
  // Header
  // ---------------------------------------------------------------------------

  test("explain output always begins with the Logical Plan header"):
    val plan   = ReadCsv("data.csv", None)
    val output = LogicalPlanPrinter.explain(plan)
    assert(output.startsWith("== Logical Plan =="), s"Unexpected header: $output")

  // ---------------------------------------------------------------------------
  // ReadCsv
  // ---------------------------------------------------------------------------

  test("ReadCsv node shows the source path"):
    val output = LogicalPlanPrinter.explain(ReadCsv("memory://customers", None))
    assert(output.contains("ReadCsv [memory://customers]"), output)

  test("ReadCsv node shows schema column names and types when schema is present"):
    val output = LogicalPlanPrinter.explain(ReadCsv("data.csv", Some(schema)))
    assert(output.contains("city:StringType"), output)
    assert(output.contains("age:Int32"),       output)
    assert(output.contains("revenue:Float64"), output)

  test("ReadCsv node omits the schema section when no schema is provided"):
    val output = LogicalPlanPrinter.explain(ReadCsv("data.csv", None))
    assert(!output.contains("schema="), output)

  // ---------------------------------------------------------------------------
  // Filter
  // ---------------------------------------------------------------------------

  test("Filter node shows the condition expression in human-readable form"):
    val plan   = Filter(ReadCsv("data.csv", None), GreaterThan(ColumnRef("age"), Literal(30)))
    val output = LogicalPlanPrinter.explain(plan)
    assert(output.contains("Filter (age > 30)"), output)

  test("Filter with EqualTo shows the equality condition"):
    val plan   = Filter(ReadCsv("data.csv", None), EqualTo(ColumnRef("city"), Literal("Paris")))
    val output = LogicalPlanPrinter.explain(plan)
    assert(output.contains("Filter (city = 'Paris')"), output)

  // ---------------------------------------------------------------------------
  // Project
  // ---------------------------------------------------------------------------

  test("Project node lists the selected column names"):
    val plan = Project(
      ReadCsv("data.csv", None),
      Vector(ColumnRef("city"), ColumnRef("revenue")),
      schema = None
    )
    val output = LogicalPlanPrinter.explain(plan)
    assert(output.contains("Project [city, revenue]"), output)

  // ---------------------------------------------------------------------------
  // Aggregate
  // ---------------------------------------------------------------------------

  test("Aggregate node shows the groupBy columns and aggregation functions"):
    val plan = Aggregate(
      ReadCsv("data.csv", None),
      groupBy      = Vector(ColumnRef("city")),
      aggregations = Vector(Sum(ColumnRef("revenue"), alias = Some("total_revenue"))),
      schema       = None
    )
    val output = LogicalPlanPrinter.explain(plan)
    assert(output.contains("Aggregate [city]"),                     output)
    assert(output.contains("SUM(revenue) AS total_revenue"),        output)

  test("Aggregate with Count shows the COUNT(*) aggregation"):
    val plan = Aggregate(
      ReadCsv("data.csv", None),
      groupBy      = Vector(ColumnRef("city")),
      aggregations = Vector(Count(None, alias = Some("n"))),
      schema       = None
    )
    val output = LogicalPlanPrinter.explain(plan)
    assert(output.contains("COUNT(*) AS n"), output)

  // ---------------------------------------------------------------------------
  // Tree structure — nested pipelines
  // ---------------------------------------------------------------------------

  test("a three-node pipeline renders each node on its own line in outermost-first order"):
    val plan = Aggregate(
      Filter(
        ReadCsv("memory://customers", Some(schema)),
        GreaterThan(ColumnRef("age"), Literal(30))
      ),
      groupBy      = Vector(ColumnRef("city")),
      aggregations = Vector(Sum(ColumnRef("revenue"), alias = Some("total"))),
      schema       = None
    )
    val output = LogicalPlanPrinter.explain(plan)
    val lines  = output.linesIterator.toVector

    // Each node must appear, and parent must appear before child
    val aggregateLine = lines.indexWhere(_.contains("Aggregate"))
    val filterLine    = lines.indexWhere(_.contains("Filter"))
    val readLine      = lines.indexWhere(_.contains("ReadCsv"))

    assert(aggregateLine >= 0, s"Aggregate not found in:\n$output")
    assert(filterLine    >= 0, s"Filter not found in:\n$output")
    assert(readLine      >= 0, s"ReadCsv not found in:\n$output")
    assert(aggregateLine < filterLine, "Aggregate must appear before Filter")
    assert(filterLine    < readLine,   "Filter must appear before ReadCsv")

  // ---------------------------------------------------------------------------
  // Sort, Limit, Join
  // ---------------------------------------------------------------------------

  test("Sort node shows the sort expressions and directions"):
    val plan = Sort(
      ReadCsv("data.csv", None),
      Vector(SortExpr(ColumnRef("age"), ascending = true), SortExpr(ColumnRef("revenue"), ascending = false))
    )
    val output = LogicalPlanPrinter.explain(plan)
    assert(output.contains("Sort [age ASC, revenue DESC]"), output)

  test("Limit node shows the row count"):
    val plan   = Limit(ReadCsv("data.csv", None), 10)
    val output = LogicalPlanPrinter.explain(plan)
    assert(output.contains("Limit 10"), output)

  test("Join node shows the join type and condition"):
    val plan = Join(
      ReadCsv("a.csv", None),
      ReadCsv("b.csv", None),
      condition = Some(EqualTo(ColumnRef("id"), ColumnRef("ref_id"))),
      joinType  = JoinType.Inner
    )
    val output = LogicalPlanPrinter.explain(plan)
    assert(output.contains("Join INNER"), output)
    assert(output.contains("id = ref_id"), output)

  // ---------------------------------------------------------------------------
  // Tree structure — nested pipelines
  // ---------------------------------------------------------------------------

  test("child nodes are indented relative to their parent"):
    val plan = Filter(
      ReadCsv("data.csv", None),
      GreaterThan(ColumnRef("age"), Literal(30))
    )
    val output = LogicalPlanPrinter.explain(plan)
    val filterLine = output.linesIterator.find(_.contains("Filter")).get
    val readLine   = output.linesIterator.find(_.contains("ReadCsv")).get
    // ReadCsv (child) must have more leading whitespace than Filter (parent)
    assert(readLine.takeWhile(_ == ' ').length > filterLine.takeWhile(_ == ' ').length,
      s"ReadCsv should be indented more than Filter:\n$output")
