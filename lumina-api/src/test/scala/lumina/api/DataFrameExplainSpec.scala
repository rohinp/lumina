package lumina.api

import munit.FunSuite
import lumina.plan.*
import lumina.plan.Expression.*
import lumina.plan.Aggregation.*

/**
 * Tests for DataFrame.explainString and DataFrame.explain().
 *
 * Read these tests to understand what the explain output looks like from the
 * user-facing API and what information is available before execution.
 */
class DataFrameExplainSpec extends FunSuite:

  private val schema = Schema(
    Vector(
      Column("city",    DataType.StringType),
      Column("age",     DataType.Int32),
      Column("revenue", DataType.Float64)
    )
  )

  test("explainString on a readCsv DataFrame includes the source path"):
    val df = Lumina.readCsv("data/customers.csv", schema)
    assert(df.explainString.contains("data/customers.csv"), df.explainString)

  test("explainString on a readCsv DataFrame includes the schema column names"):
    val df = Lumina.readCsv("data/customers.csv", schema)
    assert(df.explainString.contains("city"),    df.explainString)
    assert(df.explainString.contains("revenue"), df.explainString)

  test("explainString on a filter DataFrame shows the filter condition"):
    val df = Lumina
      .readCsv("data/customers.csv")
      .filter(GreaterThan(ColumnRef("age"), Literal(30)))
    assert(df.explainString.contains("Filter"),   df.explainString)
    assert(df.explainString.contains("age > 30"), df.explainString)

  test("explainString on a full pipeline shows all three node types"):
    val df = Lumina
      .readCsv("data/customers.csv", schema)
      .filter(GreaterThan(ColumnRef("age"), Literal(30)))
      .groupBy(
        grouping     = Seq(ColumnRef("city")),
        aggregations = Seq(Sum(ColumnRef("revenue"), alias = Some("total")))
      )
    val out = df.explainString
    assert(out.contains("Aggregate"), out)
    assert(out.contains("Filter"),    out)
    assert(out.contains("ReadCsv"),   out)

  test("explainString always starts with the Logical Plan header"):
    val df = Lumina.readCsv("any.csv")
    assert(df.explainString.startsWith("== Logical Plan =="), df.explainString)

  test("explain() prints to stdout without throwing"):
    // We cannot easily capture stdout in MUnit, but we verify no exception is thrown
    // and that explainString (which explain() delegates to) is non-empty.
    val df = Lumina.readCsv("any.csv").filter(EqualTo(ColumnRef("city"), Literal("Paris")))
    assert(df.explainString.nonEmpty)
    df.explain() // must not throw
