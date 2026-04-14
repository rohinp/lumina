package lumina.backend.duckdb

import lumina.plan.backend.{BackendComplianceSuite, DataRegistry, Row}

/**
 * Verifies DuckDBBackend passes the shared compliance suite.
 *
 * The compliance suite fires the canonical "filter → groupBy" pipeline against
 * the backend and checks row-level correctness.  Passing here means DuckDBBackend
 * produces the same semantics as LocalBackend and is ready to be registered in
 * BackendRegistry.default().
 */
class DuckDBBackendComplianceSpec extends BackendComplianceSuite:

  override protected val backend: DuckDBBackend =
    DuckDBBackend(
      DataRegistry.of(
        "memory://customers" -> Vector(
          Row(Map("city" -> "Paris",  "age" -> 35, "revenue" -> 1000.0)),
          Row(Map("city" -> "Paris",  "age" -> 45, "revenue" -> 3000.0)),
          Row(Map("city" -> "Berlin", "age" -> 29, "revenue" -> 2000.0))
        )
      )
    )
