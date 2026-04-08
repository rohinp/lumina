# Lumina - Working Plan

## Guiding Principles
- Test-first, incremental delivery; validate every public API slice with unit tests before implementation.
- JVM-wide ergonomics: API signatures must remain friendly to Scala, Java, and Kotlin (no Scala-only constructs in user surface).
- Clear layering: Pandas-like API -> logical plan graph -> pluggable backend contracts.
- Logical-plan expressions stay runtime-typed to maximize JVM portability; typed helpers can exist at the Scala facade layer but must not leak into shared modules unless we adopt a schema-aware system that all JVM languages can consume.

## Architecture Stages
1. **API Facade (Milestone 1)**
   - Define minimal DataFrame surface: `readCsv`, `select`, `filter`, `groupBy`, `collect`.
   - Keep public types neutral (interfaces, simple builders) and wrap Scala-specific logic internally.
   - Tests: ensure fluent calls produce expected logical plans.

2. **Logical Plan Core (Milestone 1)**
   - Model immutable nodes (`ReadCsv`, `Filter`, `Project`, `Aggregate`).
   - Carry schema metadata for compile-time safety and later validation.
   - Snapshot tests assert DAG structure; serialization format (JSON/proto-friendly) sketched early.

3. **Backend Contract (Milestone 2)**
   - Introduce `Backend` trait/interface ingesting logical plans and emitting iterators/row sets.
   - Define capability flags (supports UDFs, window ops, distributed execution) so planning can adapt.
   - Implement LocalBackend prototype using pure Scala collections; behavioral tests compare outputs.

4. **Backend Registry & Config (Milestone 2)**
   - Provide configuration entry point to pick backend per session/job.
   - Stub `PolarsBackend` and `SparkBackend` that throw `UnsupportedOperationException` until ready, ensuring selection path is wired.

5. **Multi-language Support (Milestone 3)**
   - Create Java/Kotlin facade modules (`LuminaJava`, `LuminaKotlin`).
   - Add small integration tests/projects that compile pipelines to guard API compatibility.

6. **Performance & Advanced Features (Later)**
   - Lazy evaluation controls (`cache`, `explain`).
   - Extended logical nodes for joins, windows, and user-defined transforms once first backend is stable.

## Module Layout
- `lumina-plan`: Owns logical nodes, schema metadata, and serialization; zero backend or API dependencies.
- `lumina-api`: Public DataFrame DSL plus Scala/Java/Kotlin entry points; depends only on `lumina-plan`.
- `lumina-backend-local`: First executable backend interpreting plans via Scala collections.
- `lumina-backend-polars`: JNI/FFI integration for Polars/DuckDB (stub until implementation); shares the backend contract.
- `lumina-backend-spark`: Distributed planner/executor targeting Spark or Flink; initially a stub.
- `lumina-config`: Backend registry and session configuration that wires API calls to a concrete backend module.
- `integration-tests`: Holds cross-language smoke tests and end-to-end flows that exercise API + config + chosen backend together.

## Testing Strategy
- API behavior tests (ScalaTest/MUnit) focus on logical-plan outputs before execution.
- Backend compliance suite uses shared fixtures to validate row-level semantics across all backends.
- Cross-language smoke builds to ensure signatures remain callable from Java/Kotlin.

## Immediate Next Steps
1. Finalize exact minimal API signatures and logical node data model.
2. Write failing tests that assert plan shapes for `readCsv -> filter -> groupBy` pipelines.
3. Implement logical node structures and DataFrame wrappers to satisfy those tests.
