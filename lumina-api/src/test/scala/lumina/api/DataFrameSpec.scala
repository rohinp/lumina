package lumina.api

import munit.FunSuite

import lumina.plan.*
import lumina.plan.Expression.*
import lumina.plan.Aggregation.*

class DataFrameSpec extends FunSuite:

  private val schema = Schema(
    Vector(
      Column("city", DataType.StringType),
      Column("age", DataType.Int32),
      Column("revenue", DataType.Float64)
    )
  )

  test("readCsv -> filter -> select records the logical plan in declared order"):
    val df = Lumina
      .readCsv("data/customers.csv", schema)
      .filter(GreaterThan(ColumnRef("age"), Literal(30)))
      .select(ColumnRef("city"), ColumnRef("revenue"))

    val project = df.plan match
      case node: Project => node
      case other         => fail(s"Expected Project at the top, found ${other.getClass.getSimpleName}")

    val filter = project.children.head match
      case node: Filter => node
      case other        => fail(s"Expected Filter under Project, found ${other.getClass.getSimpleName}")

    val read = filter.children.head match
      case node: ReadCsv => node
      case other         => fail(s"Expected ReadCsv under Filter, found ${other.getClass.getSimpleName}")

    assertEquals(read.path, "data/customers.csv")
    assertEquals(read.outputSchema, Some(schema))

  test("groupBy helper documents the aggregate contract"):
    val df = Lumina
      .readCsv("data/customers.csv")
      .groupBy(
        grouping = Seq(ColumnRef("city")),
        aggregations = Seq(Sum(ColumnRef("revenue"), alias = Some("total_revenue")))
      )

    val aggregate = df.plan match
      case node: Aggregate => node
      case other           => fail(s"Expected Aggregate, found ${other.getClass.getSimpleName}")

    assertEquals(aggregate.groupBy, Vector(ColumnRef("city")))
    assertEquals(aggregate.aggregations.head, Sum(ColumnRef("revenue"), Some("total_revenue")))
