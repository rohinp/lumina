package lumina.plan

import munit.FunSuite

import scala.reflect.ClassTag

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

  test("Scenario: city revenue rolled up after age filter"):
    val plan = PlanBuilder
      .readCsv("data/customers.csv", Some(baseSchema))
      .filter(GreaterThan(ColumnRef("age"), Literal(21)))
      .groupBy(
        grouping = Seq(ColumnRef("city")),
        aggregations = Seq(Sum(ColumnRef("revenue"), alias = Some("total_revenue")))
      )
      .plan

    val aggregate = assertNode[Aggregate](plan, "final node should be Aggregate")
    assertEquals(aggregate.groupBy, Vector(ColumnRef("city")))
    assertEquals(aggregate.aggregations, Vector(Sum(ColumnRef("revenue"), Some("total_revenue"))))

    val filter = assertNode[Filter](aggregate.child, "Aggregate child should be Filter")
    assertEquals(filter.condition, GreaterThan(ColumnRef("age"), Literal(21)))

    val read = assertNode[ReadCsv](filter.child, "Filter child should be ReadCsv")
    assertEquals(read.path, "data/customers.csv")
    assertEquals(read.schema, Some(baseSchema))

  test("Project inherits schema when no override supplied"):
    val project = PlanBuilder
      .readCsv("data/customers.csv", Some(baseSchema))
      .project(ColumnRef("city"), ColumnRef("revenue"))
      .plan
      .asInstanceOf[Project]

    assertEquals(project.outputSchema, Some(baseSchema))

  test("Project can override schema for downstream consumers"):
    val trimmed = Schema(Vector(Column("city", DataType.StringType)))
    val project = PlanBuilder
      .readCsv("data/customers.csv", Some(baseSchema))
      .project(ColumnRef("city"), ColumnRef("revenue"))
      .plan
      .asInstanceOf[Project]
      .copy(schema = Some(trimmed))

    assertEquals(project.outputSchema, Some(trimmed))

  test("Filter always forwards the child schema verbatim"):
    val filter = PlanBuilder
      .readCsv("data/customers.csv", Some(baseSchema))
      .filter(EqualTo(ColumnRef("city"), Literal("Paris")))
      .plan
      .asInstanceOf[Filter]

    assertEquals(filter.outputSchema, Some(baseSchema))

  test("Aggregate opts into provided schema and leaves it empty otherwise"):
    val aggregateWithSchema = Aggregate(
      child = ReadCsv("data/customers.csv", Some(baseSchema)),
      groupBy = Vector(ColumnRef("city")),
      aggregations = Vector(Count(column = None, alias = Some("count"))),
      schema = Some(
        Schema(
          Vector(
            Column("city", DataType.StringType, nullable = false),
            Column("count", DataType.Int64, nullable = false)
          )
        )
      )
    )
    assert(aggregateWithSchema.outputSchema.exists(_.columnNames == Vector("city", "count")))

    val aggregateWithoutSchema = aggregateWithSchema.copy(schema = None)
    assertEquals(aggregateWithoutSchema.outputSchema, None)

  private def assertNode[T <: LogicalPlan: ClassTag](plan: LogicalPlan, reason: String): T =
    plan match
      case node: T => node
      case other   => fail(s"$reason, but found ${other.getClass.getSimpleName}")
