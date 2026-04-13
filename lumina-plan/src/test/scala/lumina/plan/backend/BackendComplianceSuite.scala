package lumina.plan.backend

import munit.FunSuite
import lumina.plan.*
import lumina.plan.Expression.*
import lumina.plan.Aggregation.*

/** Base suite backend implementations can extend to verify minimum behavior. */
abstract class BackendComplianceSuite extends FunSuite:

  protected def backend: Backend

  private val schema = Schema(
    Vector(
      Column("city", DataType.StringType),
      Column("age", DataType.Int32),
      Column("revenue", DataType.Float64)
    )
  )

  private val samplePath = "memory://customers"

  test("filter then groupBy pipeline returns aggregated rows"):
    val plan = PlanBuilder
      .readCsv(samplePath, Some(schema))
      .filter(GreaterThan(ColumnRef("age"), Literal(30)))
      .groupBy(
        grouping = Seq(ColumnRef("city")),
        aggregations = Seq(Sum(ColumnRef("revenue"), alias = Some("total_revenue")))
      )
      .plan

    backend.execute(plan) match
      case BackendResult.InMemory(rows) =>
        val paris = rows.find(_.values("city") == "Paris").getOrElse(fail("Paris row missing"))
        assertEquals(paris.values("total_revenue"), 4000.0)

object BackendComplianceSuite:

  /** Simple backend used to prove the suite itself is sound. */
  class InMemoryBackend extends Backend:
    override val name: String = "in-memory-test"
    override val capabilities: BackendCapabilities = BackendCapabilities(
      supportsDistributedExecution = false,
      supportsVectorizedExecution = true,
      supportsUserDefinedFunctions = false
    )

    private val data = Vector(
      Row(Map("city" -> "Paris", "age" -> 35, "revenue" -> 1000.0)),
      Row(Map("city" -> "Paris", "age" -> 45, "revenue" -> 3000.0)),
      Row(Map("city" -> "Berlin", "age" -> 29, "revenue" -> 2000.0))
    )

    override def execute(plan: LogicalPlan): BackendResult =
      plan match
        case _: Aggregate =>
          val parisRevenue = data.filter(_("city") == "Paris").map(_("revenue").asInstanceOf[Double]).sum
          BackendResult.InMemory(Vector(Row(Map("city" -> "Paris", "total_revenue" -> parisRevenue))))
        case _ =>
          BackendResult.InMemory(data)

class BackendComplianceSuiteSpec extends BackendComplianceSuite:
  override protected val backend: Backend = new BackendComplianceSuite.InMemoryBackend
