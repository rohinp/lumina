package lumina.api

import munit.FunSuite
import lumina.plan.*
import lumina.plan.Expression.*
import lumina.plan.Aggregation.*
import lumina.plan.backend.{Backend, BackendCapabilities, BackendResult, Row}

/**
 * Tests for DataFrame.collect and the Java-friendly API surface.
 * Read these tests to understand how a DataFrame is executed against a backend
 * and what each Java-friendly overload produces.
 */
class DataFrameCollectSpec extends FunSuite:

  // ---------------------------------------------------------------------------
  // Stub backend — returns a fixed dataset so these tests are independent of
  // LocalBackend and focus purely on the DataFrame API contract
  // ---------------------------------------------------------------------------

  private val fixedRows = Vector(
    Row(Map("city" -> "Paris",  "age" -> 35, "revenue" -> 1000.0)),
    Row(Map("city" -> "Berlin", "age" -> 29, "revenue" -> 2000.0))
  )

  private val stubBackend: Backend = new Backend:
    override val name         = "stub"
    override val capabilities = BackendCapabilities(false, false, false)
    override def execute(plan: lumina.plan.LogicalPlan) = BackendResult.InMemory(fixedRows)

  // ---------------------------------------------------------------------------
  // collect — plan execution
  // ---------------------------------------------------------------------------

  test("collect returns the rows produced by the backend for the dataframe's plan"):
    val rows = Lumina.readCsv("any.csv").collect(stubBackend)
    assertEquals(rows, fixedRows)

  test("collect returns an empty Vector when the backend produces no rows"):
    val emptyBackend: Backend = new Backend:
      override val name         = "empty"
      override val capabilities = BackendCapabilities(false, false, false)
      override def execute(plan: lumina.plan.LogicalPlan) = BackendResult.InMemory(Vector.empty)

    val rows = Lumina.readCsv("any.csv").collect(emptyBackend)
    assert(rows.isEmpty)

  // ---------------------------------------------------------------------------
  // collectAsList — Java-friendly execution
  // ---------------------------------------------------------------------------

  test("collectAsList returns a java.util.List containing the same rows as collect"):
    val df       = Lumina.readCsv("any.csv")
    val scalaRes = df.collect(stubBackend)
    val javaRes  = df.collectAsList(stubBackend)
    assertEquals(javaRes.size, scalaRes.size)
    assertEquals(javaRes.get(0), scalaRes.head)

  // ---------------------------------------------------------------------------
  // Java-friendly groupBy overload
  // ---------------------------------------------------------------------------

  test("groupBy with java.util.List produces the same plan node as the Scala Seq overload"):
    import scala.jdk.CollectionConverters.*

    val scalaDF = Lumina.readCsv("any.csv")
      .groupBy(
        grouping     = Seq(ColumnRef("city")),
        aggregations = Seq(Sum(ColumnRef("revenue"), alias = Some("total")))
      )

    val javaDF = Lumina.readCsv("any.csv")
      .groupBy(
        java.util.List.of(ColumnRef("city")),
        java.util.List.of(Sum(ColumnRef("revenue"), alias = Some("total")))
      )

    assertEquals(scalaDF.plan, javaDF.plan)

  // ---------------------------------------------------------------------------
  // Java-friendly select overload
  // ---------------------------------------------------------------------------

  test("select with java.util.List produces the same plan node as the Scala varargs overload"):
    val scalaDF = Lumina.readCsv("any.csv").select(ColumnRef("city"), ColumnRef("age"))
    val javaDF  = Lumina.readCsv("any.csv")
      .select(java.util.List.of(ColumnRef("city"), ColumnRef("age")))

    assertEquals(scalaDF.plan, javaDF.plan)
