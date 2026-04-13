package lumina.backend.local

import lumina.plan.backend.{BackendComplianceSuite, Row}

/**
 * Verifies LocalBackend passes the shared compliance suite.
 *
 * The compliance suite fires the canonical "filter → groupBy" pipeline against
 * the backend.  Passing here means LocalBackend produces the same row-level
 * semantics as every other Lumina backend, and is ready to be selected via
 * the backend registry.
 */
class LocalBackendComplianceSpec extends BackendComplianceSuite:

  override protected val backend: LocalBackend =
    LocalBackend(
      DataRegistry.of(
        "memory://customers" -> Vector(
          Row(Map("city" -> "Paris",  "age" -> 35, "revenue" -> 1000.0)),
          Row(Map("city" -> "Paris",  "age" -> 45, "revenue" -> 3000.0)),
          Row(Map("city" -> "Berlin", "age" -> 29, "revenue" -> 2000.0))
        )
      )
    )
