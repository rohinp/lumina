package lumina.config

import lumina.plan.LogicalPlan
import lumina.plan.backend.{Backend, BackendResult}

/**
 * Entry point for executing logical plans against a chosen backend.
 *
 * A session binds a BackendRegistry and a selected backend name.  All plan
 * executions within the session use the same backend.
 *
 * {{{
 *   val session = LuminaSession.local()
 *   val result  = session.execute(myPlan)
 * }}}
 */
final class LuminaSession(registry: BackendRegistry, backendName: String):

  val backend: Backend = registry.getOrFail(backendName)

  def execute(plan: LogicalPlan): BackendResult = backend.execute(plan)

object LuminaSession:

  /** Create a session backed by the LocalBackend with an empty data registry. */
  def local(): LuminaSession =
    new LuminaSession(BackendRegistry.default(), "local")

  /** Create a session backed by DuckDB with the provided DataRegistry. */
  def duckdb(registry: lumina.plan.backend.DataRegistry): LuminaSession =
    import lumina.backend.duckdb.DuckDBBackend
    val reg = BackendRegistry.empty.register(DuckDBBackend(registry))
    new LuminaSession(reg, "duckdb")

  /** Create a session using a specific backend name from the default registry. */
  def withBackend(name: String): LuminaSession =
    new LuminaSession(BackendRegistry.default(), name)

  /** Create a session from a custom registry and backend name. */
  def apply(registry: BackendRegistry, backendName: String): LuminaSession =
    new LuminaSession(registry, backendName)
