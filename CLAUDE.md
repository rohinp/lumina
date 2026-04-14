# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
# Compile
sbt compile

# Run all tests
sbt test

# Run tests for a specific module
sbt lumina-plan/test
sbt lumina-api/test
sbt "integration-tests/test"

# Format code
scalafmt
```

## Architecture

Lumina is a unified data engineering framework for the JVM providing a Pandas-like, lazy-evaluation API with pluggable execution backends. The central design goal: write data logic once and execute it on any backend (local in-memory, Polars/DuckDB, Spark).

### Three-Layer Design

**Layer 1 — Public API (`lumina-api`)**
`DataFrame.scala` exposes the user-facing fluent DSL (`readCsv`, `filter`, `select`, `groupBy`, `plan`). The `Lumina` companion object is the entry point. This layer depends only on `lumina-plan` and is the primary surface for Java/Kotlin consumers.

**Layer 2 — Logical Plan Core (`lumina-plan`)**
The heart of the project. Contains:
- `LogicalPlan.scala` — sealed AST nodes: `ReadCsv`, `Filter`, `Project`, `Aggregate`
- `expressions.scala` — `Expression` hierarchy (`ColumnRef`, `Literal`, `GreaterThan`, `EqualTo`) and `Aggregation` types (`Sum`, `Count`). Intentionally **unparameterized** (not `Expression[T]`) for JVM interoperability — Java/Kotlin would suffer from Scala variance rules.
- `Schema.scala` — `Column`, `DataType` (enum: `Int32`, `Int64`, `Float64`, `BooleanType`, `StringType`, `Unknown`), `Schema`
- `PlanBuilder.scala` — fluent builder for constructing plans in tests
- `backend/Backend.scala` — `Backend` trait, `BackendCapabilities`, `BackendResult`

**Layer 3 — Pluggable Backends**
- `lumina-backend-local` — pure Scala, in-memory executor; walks `LogicalPlan` AST directly
- `lumina-backend-duckdb` — JDBC-based executor; translates plan to SQL via `PlanToSql` then runs against DuckDB in-process; `DuckDBBackend` is stateless (new JDBC connection per `execute` call)
- `lumina-backend-polars` — stub (UnsupportedOperationException)
- `lumina-backend-spark` — stub (UnsupportedOperationException)
- `lumina-config` — `BackendRegistry` (name→Backend map), `LuminaSession` (binds registry + backend name)

### Module Dependency Graph

```
lumina-api → lumina-plan ← lumina-backend-{local,duckdb,polars,spark}
lumina-config → lumina-plan + backends
integration-tests → lumina-api + lumina-config
```

### Data Flow

```
Lumina.readCsv(...)     → ReadCsv node
  .filter(expr)         → Filter node wrapping ReadCsv
  .groupBy(...).sum(...) → Aggregate node
  .plan                 → LogicalPlan DAG

Backend.execute(plan) → BackendResult.InMemory(Vector[Row])
```

### Testing Strategy

Tests serve as the primary developer documentation for this project. Test names must be full English sentences that describe the exact behavior being verified, structured so a developer can read a test file top-to-bottom and understand how the component works.

**Layers:**
- **Unit tests** (`lumina-plan`, `lumina-api`, `lumina-backend-local`): one test per operation verifying its isolated behavior, followed by combined pipeline tests, then edge cases (empty data, no matches, all rows match)
- **BackendComplianceSuite** (`lumina-plan`): abstract MUnit suite every backend must extend to prove minimum behavioral correctness; ships with a stub backend to validate the suite itself
- **Integration tests** (`integration-tests`): dynamically compile Java and Kotlin source strings at test time to prove API surface remains JVM-friendly

**Naming convention:** prefer `"filter keeps only rows where the condition is true"` over `"testFilter"`. Avoid terse names.

### Milestone Status

| Milestone | Status | Notes |
|-----------|--------|-------|
| **M1: API Facade + Logical Plan Core** | Complete | Plan nodes, schema/types, expression AST, DataFrame DSL, plan-shape tests |
| **M2: Backend Contract + Registry** | Complete | `LocalBackend` (pure-Scala executor), `DataRegistry`, `CsvLoader`, `BackendRegistry`, `LuminaSession`; `PolarsBackend`/`SparkBackend` are UnsupportedOperation stubs |
| **M3: Multi-language Support** | Complete | `LuminaJava` facade, `Row.of(key,val,...)` varargs factory, `Aggregation.sum/count` Java factories, `Iterable`-based overloads on `DataFrame`; integration tests compile and execute full pipelines from Java and Kotlin |
| **M4: Explain + DuckDB Backend** | Complete | `LogicalPlanPrinter` / `DataFrame.explain()`, `lumina-backend-duckdb` with `PlanToSql` SQL translator, `DuckDBBackend` (stateless JDBC), `BackendComplianceSuite` passed, wired into `BackendRegistry.default()` |
| **M5: Performance & Advanced Features** | Not started | Joins, windows, UDFs, predicate pushdown, columnar execution |

**Key DuckDB implementation notes:**
- `PlanToSql` translates each `LogicalPlan` node to a nested SQL subquery (no CTEs, pure composable SELECTs)
- `DuckDBBackend` opens a fresh `jdbc:duckdb:` connection per `execute()` call — no shared state
- `memory://name` URIs → DuckDB tables created from `DataRegistry` rows via `CREATE TABLE` + `INSERT`
- `BackendComplianceSuite` is the shared correctness contract; both `LocalBackend` and `DuckDBBackend` must pass it
