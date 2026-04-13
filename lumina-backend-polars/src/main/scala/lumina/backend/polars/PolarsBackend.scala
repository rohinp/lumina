package lumina.backend.polars

import lumina.plan.LogicalPlan
import lumina.plan.backend.*

/**
 * Stub for the Polars/DuckDB backend (Milestone 3).
 *
 * Will integrate via JNI/FFI once the LocalBackend baseline is stable.
 * All operations throw UnsupportedOperationException until implemented.
 */
final class PolarsBackend extends Backend:

  override val name: String = "polars"

  override val capabilities: BackendCapabilities = BackendCapabilities(
    supportsDistributedExecution = false,
    supportsVectorizedExecution  = true,
    supportsUserDefinedFunctions = false
  )

  override def execute(plan: LogicalPlan): BackendResult =
    throw UnsupportedOperationException(
      "PolarsBackend is not yet implemented. Use LocalBackend for in-process execution."
    )
