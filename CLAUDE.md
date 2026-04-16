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
| **M5: Extended Operators** | Complete | `Sort`, `Limit`, `Join` (Inner/Left/Right/Full) plan nodes; `LessThan`, `GreaterThanOrEqual`, `And`, `Or`, `Not`, `IsNull`, `IsNotNull`, `NotEqualTo` expressions; `Avg`/`Min`/`Max` aggregations; all implemented in `LocalBackend` + `DuckDBBackend` with full test coverage |
| **M6: Optimizer + Type Normalisation** | Complete | Rule-based `Optimizer` (bottom-up, composable); `CombineFilters` merges consecutive Filters into `And`; `PredicatePushdown` moves Filters below Project and Sort; `RowNormalizer` maps JDBC boxed types to Scala primitives so both backends return identical value types; optimizer wired into `LocalBackend` and `DuckDBBackend` |
| **M7: DuckDB Hardening** | Complete | Arithmetic expressions (`Add`, `Subtract`, `Multiply`, `Divide`, `Negate`); `Alias` expression; `WithColumn` plan node (add/replace column, uses `SELECT *, expr AS name` + last-wins dedup); `DataFrame.withColumn()` / `show()` / `showString()`; JOIN ON clause table-qualified to resolve ambiguous column refs; `collectRows` deduplication uses column index (not name) — last occurrence of a repeated name wins |
| **M8: Window Functions** | Complete | `WindowSpec`, `WindowExpr` hierarchy (`RowNumber`, `Rank`, `DenseRank`, `WindowAgg`, `Lag`, `Lead`); `Window` plan node; `DataFrame.window()`; `LocalBackend` pure-Scala partition/sort/compute implementation; `PlanToSql` SQL `OVER (...)` translation for DuckDB; full test coverage in both backends |
| **M9: Set Operations + Optimizer Improvements** | Complete | `UnionAll` and `Distinct` plan nodes; `DataFrame.unionAll()` / `DataFrame.distinct()`; both backends implemented; `PredicatePushdown` extended through `Window` (pushes filters on non-window columns below the Window node); `referencedColumns` exhaustive (covers arithmetic and `Alias`) |
| **M10: Extended Expression Library** | Complete | String functions: `Upper`, `Lower`, `Trim`, `Length`, `Concat`, `Substring`, `Like`; null handling: `Coalesce`; set membership: `In`; all expressions evaluated in `LocalBackend`, translated to SQL in `DuckDBBackend`; `referencedColumns` extended; full test coverage in both backends |
| **M11: Column Operations** | Complete | `DropColumns` and `RenameColumn` plan nodes; `DataFrame.drop()` / `withColumnRenamed()`; `DataFrame.dropNa(cols*)` (compiles to Filter+IsNotNull) and `DataFrame.fillNa(value, cols*)` (compiles to WithColumn+Coalesce); `PredicatePushdown` through `DropColumns` and `RenameColumn` (rewrites condition column refs when pushing through rename) |
| **M12: CaseWhen, Sample, DataFrame Shortcuts** | Complete | `CaseWhen(branches, otherwise)` expression (CASE WHEN … THEN … ELSE … END); `Sample(fraction, seed)` plan node (Bernoulli — seed honored by LocalBackend, ignored by DuckDB 1.2.0); `DataFrame.count()` (pushes COUNT(*) to backend), `head(n)`, `isEmpty()`, `nonEmpty()`; `PredicatePushdown` through `Sample` |
| **M13: Constant Folding + Export** | Complete | `ConstantFolding` optimizer rule (runs before CombineFilters+PredicatePushdown): collapses all-literal arithmetic/comparison/logical/null-check sub-expressions and removes trivially-true Filters; `DataFrame.toCsvString()`, `writeCsv()`, `toJsonLines()` — CSV-escaped and JSON-encoded output without schema knowledge |
| **M14: Multi-pass Optimizer + Intersect/Except + Cast + Numeric Functions** | Complete | `Optimizer.optimizeUntilFixedPoint(plan, rules, maxPasses)` — reruns the rule pipeline until the plan stabilises; `Intersect` and `Except` plan nodes (`DataFrame.intersect()` / `except()`); type casting: `Cast(expr, DataType)`; numeric functions: `Abs`, `Round(expr, scale)`, `Floor`, `Ceil`; all features implemented in `LocalBackend` + `DuckDBBackend` with full test coverage |
| **M15: Statistical Aggregations + DataFrame.agg() + describe()** | Complete | `CountDistinct`, `StdDev`, `Variance` aggregations (sample statistics, null for N≤1); `DataFrame.agg(aggregations*)` shorthand for whole-table aggregation without grouping; `DataFrame.describe()` / `describeString()` prints count/mean/stddev/min/max per column (N/A for non-numeric); Java/Kotlin factories for new aggregation types |
| **M16: Date/Time Functions** | Complete | New `DataType.DateType` / `TimestampType`; expressions: `ToDate`, `ToTimestamp`, `Year`, `Month`, `Day`, `Hour`, `Minute`, `Second`, `DayOfWeek` (ISO 1=Mon), `DateAdd(date, n)`, `DateDiff(end, start)`, `DateFormat(expr, javaPattern)`; `RowNormalizer` converts JDBC `java.sql.Date`/`Timestamp` → `java.time.LocalDate`/`LocalDateTime`; `DuckDBBackend.sqlTypeFor` now creates DATE/TIMESTAMP columns for `java.time` values; `javaPatternToStrftime` converts Java format patterns for DuckDB `STRFTIME` |
| **M17: Extended String Functions** | Complete | `Replace`, `RegexpExtract`, `RegexpReplace`, `StartsWith`, `EndsWith`, `LPad`, `RPad`, `Repeat`, `Reverse`, `InitCap`; evaluated in `LocalBackend`, translated to SQL in `DuckDBBackend`; DuckDB quirks handled: `regexp_replace` uses `'g'` flag for all-match semantics, `starts_with`/`ends_with` wrapped in `COALESCE(..., false)` for null parity, `lpad`/`rpad` guarded with `CASE WHEN length >= n` to avoid truncation, `initcap` emulated via `list_transform(string_split(...))` (DuckDB 1.2.0 lacks the built-in) |
| **M18: Extended Math Functions** | Complete | `Sqrt`, `Power`, `Log` (natural), `Log2`, `Log10`, `Exp`, `Sign`, `Mod`, `Greatest`, `Least`; evaluated in `LocalBackend` (Scala `math.*`), translated to SQL in `DuckDBBackend`; `Greatest`/`Least` skip nulls in LocalBackend (returns max/min of non-null args) — SQL `GREATEST`/`LEAST` propagate null per standard semantics |
| **M19: Conditional Expressions** | Complete | `Between(expr, lo, hi)`, `If(cond, then, else)`, `NullIf(expr, sentinel)`, `IfNull(expr, replacement)`; LocalBackend comparison operators now null-guard (return false for null operands instead of throwing); DuckDB `BETWEEN` wrapped in `COALESCE(..., false)` for null parity |

**Key DuckDB implementation notes:**
- `PlanToSql` translates each `LogicalPlan` node to a nested SQL subquery (no CTEs, pure composable SELECTs)
- `DuckDBBackend` opens a fresh `jdbc:duckdb:` connection per `execute()` call — no shared state
- `memory://name` URIs → DuckDB tables created from `DataRegistry` rows via `CREATE TABLE` + `INSERT`
- `BackendComplianceSuite` is the shared correctness contract; both `LocalBackend` and `DuckDBBackend` must pass it
