package lumina.backend.spark

import lumina.plan.LogicalPlan
import lumina.plan.backend.*

/**
 * Stub for the Spark/Flink distributed backend (Milestone 3+).
 *
 * Will translate the LogicalPlan DAG into Spark Dataset operations once the
 * LocalBackend and PolarsBackend baselines are stable.
 * All operations throw UnsupportedOperationException until implemented.
 */
final class SparkBackend extends Backend:

  override val name: String = "spark"

  override val capabilities: BackendCapabilities = BackendCapabilities(
    supportsDistributedExecution = true,
    supportsVectorizedExecution  = true,
    supportsUserDefinedFunctions = true
  )

  override def execute(plan: LogicalPlan): BackendResult =
    throw UnsupportedOperationException(
      "SparkBackend is not yet implemented. Use LocalBackend for in-process execution."
    )
