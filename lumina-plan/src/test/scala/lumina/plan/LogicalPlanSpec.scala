package lumina.plan

import munit.FunSuite
import Expression.*
import Aggregation.*

class LogicalPlanSpec extends FunSuite:
  private val baseSchema = Schema(
    Vector(
      Column("city", DataType.StringType),
      Column("age", DataType.Int32),
      Column("revenue", DataType.Float64)
    )
  )

  test("readCsv -> filter -> groupBy plan shape"):
    val builder = PlanBuilder
      .readCsv("data/customers.csv", Some(baseSchema))
      .filter(GreaterThan(ColumnRef("age"), Literal(21)))
      .groupBy(
        grouping = Seq(ColumnRef("city")),
        aggregations = Seq(Sum(ColumnRef("revenue"), alias = Some("total_revenue")))
      )

    val aggregate = builder.plan match
      case agg: Aggregate => agg
      case other          => fail(s"Expected Aggregate plan, found ${other.getClass.getSimpleName}")

    assertEquals(aggregate.groupBy, Vector(ColumnRef("city")))
    assertEquals(aggregate.aggregations, Vector(Sum(ColumnRef("revenue"), Some("total_revenue"))))

    val filter = aggregate.child match
      case f: Filter => f
      case other     => fail(s"Expected Filter child, found ${other.getClass.getSimpleName}")

    assertEquals(filter.condition, GreaterThan(ColumnRef("age"), Literal(21)))

    val read = filter.child match
      case r: ReadCsv => r
      case other      => fail(s"Expected ReadCsv child, found ${other.getClass.getSimpleName}")

    assertEquals(read.path, "data/customers.csv")
    assertEquals(read.schema, Some(baseSchema))

  test("project inherits schema when unspecified"):
    val plan = PlanBuilder
      .readCsv("data/customers.csv", Some(baseSchema))
      .project(ColumnRef("city"), ColumnRef("revenue"))
      .plan

    val project = plan match
      case p: Project => p
      case other      => fail(s"Expected Project plan, found ${other.getClass.getSimpleName}")

    assertEquals(project.outputSchema, Some(baseSchema))
