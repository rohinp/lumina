package lumina.plan.backend

import lumina.plan.LogicalPlan

/** Abstraction every execution backend must implement. */
trait Backend:
  def name: String
  def capabilities: BackendCapabilities
  def execute(plan: LogicalPlan): BackendResult

final case class BackendCapabilities(
    supportsDistributedExecution: Boolean,
    supportsVectorizedExecution: Boolean,
    supportsUserDefinedFunctions: Boolean
)

sealed trait BackendResult
object BackendResult:
  final case class InMemory(rows: Vector[Row]) extends BackendResult

final case class Row(values: Map[String, Any]):
  def apply(column: String): Any = values(column)
