## Project: Lumina

The Unified Data Engineering Framework for the JVM

---

## 1. Vision

Lumina is a type-safe, lazy-execution data manipulation library written in Scala but designed for the entire JVM ecosystem. It provides a familiar, Pandas-like API that abstracts away the underlying execution engine.

The goal is to write logic once and choose the most efficient backend — from a single CPU to a massive Spark cluster — accessible seamlessly from Scala, Java, and Kotlin.

---

## 2. Core Problem Statement

The JVM data landscape is currently split between "heavy" distributed tools and "fragmented" local libraries:

- **Small Data**: No high-performance, lazy-first equivalent to Pandas or Polars on the JVM.
- **Big Data**: Apache Spark has high overhead for local development and testing.
- **The Ecosystem Gap**: Libraries are often built for one language (Scala-only or Java-only), making cross-team collaboration difficult.

---

## 3. The "Pluggable Backend" Architecture

Lumina separates the **Interface** (user code) from the **Implementation** (execution). The same pipeline runs unchanged on any backend:

1. **Local** — pure JVM, in-memory Scala collections. Works today.
2. **DuckDB** — translates the logical plan to SQL and executes it via DuckDB's embedded JDBC driver. Works today.
3. **Distributed** — Spark or Flink for petabyte-scale data. *(planned)*

---

## 4. Current Status

Milestones M1–M15 are complete. The framework is fully operational across both execution backends.

**What works today:**
- Full `DataFrame` DSL: `filter`, `select`, `groupBy`, `sort`, `limit`, `join` (inner/left/right/full), `window`, `unionAll`, `distinct`, `intersect`, `except`, `withColumn`, `drop`, `withColumnRenamed`, `sample`
- Null-safe operations: `dropNa`, `fillNa`, `IsNull`, `IsNotNull`, `Coalesce`
- Aggregations: `Sum`, `Count`, `Avg`, `Min`, `Max`, `CountDistinct`, `StdDev`, `Variance`
- Expressions: arithmetic, string functions (`Upper`, `Lower`, `Trim`, `Length`, `Concat`, `Substring`, `Like`), `CaseWhen`, `Cast`, `Abs`, `Round`, `Floor`, `Ceil`, `In`
- Window functions: `RowNumber`, `Rank`, `DenseRank`, `WindowAgg`, `Lag`, `Lead`
- Execution shortcuts: `count`, `head`, `isEmpty`, `nonEmpty`, `agg`
- Export: `toCsvString`, `writeCsv`, `toJsonLines`, `show`, `describe`
- Rule-based optimizer with `ConstantFolding`, `CombineFilters`, `PredicatePushdown`, and multi-pass fixed-point iteration
- Both `LocalBackend` (pure Scala) and `DuckDBBackend` (JDBC) implement all operators
- Full Java and Kotlin API surface with integration tests

For the complete milestone history and implementation notes, see [`CLAUDE.md`](CLAUDE.md).

---

## 5. Working Examples

### Scala

```scala
import lumina.api.Lumina
import lumina.plan.Expression.*
import lumina.plan.Aggregation.*
import lumina.plan.backend.DataRegistry
import lumina.backend.local.LocalBackend
import lumina.plan.backend.Row

val rows = Vector(
  Row.of("city", "Paris",  "age", 35, "revenue", 1000.0),
  Row.of("city", "Paris",  "age", 45, "revenue", 3000.0),
  Row.of("city", "Berlin", "age", 29, "revenue", 2000.0)
)

val backend = LocalBackend(DataRegistry.of("memory://customers" -> rows))

val result = Lumina
  .readCsv("memory://customers")
  .filter(GreaterThan(ColumnRef("age"), Literal(30)))
  .groupBy(
    grouping     = Seq(ColumnRef("city")),
    aggregations = Seq(Sum(ColumnRef("revenue"), alias = Some("total_revenue")))
  )
  .collect(backend)

// result: Vector(Row(Map(city -> Paris, total_revenue -> 4000.0)))
```

### Explain (any backend)

```scala
val df = Lumina
  .readCsv("memory://customers")
  .filter(GreaterThan(ColumnRef("age"), Literal(30)))
  .groupBy(Seq(ColumnRef("city")), Seq(Sum(ColumnRef("revenue"), alias = Some("total"))))

df.explain()
// == Logical Plan ==
// Aggregate [city] → [SUM(revenue) AS total]
// +- Filter (age > 30)
//    +- ReadCsv [memory://customers]
```

### DuckDB Backend

```scala
import lumina.backend.duckdb.DuckDBBackend

val backend = DuckDBBackend(DataRegistry.of("memory://customers" -> rows))

val result = Lumina
  .readCsv("memory://customers")
  .filter(GreaterThan(ColumnRef("age"), Literal(30)))
  .groupBy(
    grouping     = Seq(ColumnRef("city")),
    aggregations = Seq(Sum(ColumnRef("revenue"), alias = Some("total")))
  )
  .collect(backend)
// result: Vector(Row(Map(city -> Paris, total -> 4000.0)))
```

### Java

```java
import lumina.api.LuminaJava;
import lumina.plan.Expression.ColumnRef;
import lumina.plan.Expression.Literal;
import lumina.plan.Expression.GreaterThan;
import lumina.plan.Aggregation;
import lumina.plan.backend.Row;
import lumina.backend.local.LocalBackend;
import lumina.backend.local.DataRegistry;
import java.util.ArrayList;

var rows = new ArrayList<Row>();
rows.add(Row.of("city", "Paris",  "age", 35, "revenue", 1000.0));
rows.add(Row.of("city", "Paris",  "age", 45, "revenue", 3000.0));
rows.add(Row.of("city", "Berlin", "age", 29, "revenue", 2000.0));

var backend = new LocalBackend(DataRegistry.empty().register("memory://customers", rows));

var result = LuminaJava.readCsv("memory://customers")
  .filter(new GreaterThan(new ColumnRef("age"), new Literal(30)))
  .groupBy(
    java.util.List.of(new ColumnRef("city")),
    java.util.List.of(Aggregation.sum(new ColumnRef("revenue"), "total_revenue"))
  )
  .collectAsList(backend);
```

### Kotlin

```kotlin
import lumina.api.LuminaJava
import lumina.plan.Expression
import lumina.plan.Expression.ColumnRef
import lumina.plan.Expression.Literal
import lumina.plan.Expression.GreaterThan
import lumina.plan.Aggregation
import lumina.plan.backend.Row
import lumina.backend.local.LocalBackend
import lumina.backend.local.DataRegistry

val rows = java.util.ArrayList<Row>()
rows.add(Row.of("city", "Paris",  "age", 35, "revenue", 1000.0))
rows.add(Row.of("city", "Paris",  "age", 45, "revenue", 3000.0))
rows.add(Row.of("city", "Berlin", "age", 29, "revenue", 2000.0))

val backend = LocalBackend(DataRegistry.empty().register("memory://customers", rows))

val groupingCols = java.util.ArrayList<Expression>()
groupingCols.add(ColumnRef("city"))

val aggCols = java.util.ArrayList<Aggregation>()
aggCols.add(Aggregation.sum(ColumnRef("revenue"), "total_revenue"))

val result = LuminaJava.readCsv("memory://customers")
  .filter(GreaterThan(ColumnRef("age"), Literal(30)))
  .groupBy(groupingCols, aggCols)
  .collectAsList(backend)
```

---

## 6. Module Layout

| Module | Status | Responsibility |
|--------|--------|---------------|
| `lumina-plan` | Implemented | Logical plan AST, schema, expressions, backend interface |
| `lumina-api` | Implemented | `DataFrame` DSL, `Lumina`/`LuminaJava` entry points, Java/Kotlin bridges |
| `lumina-backend-local` | Implemented | Pure-Scala in-memory executor; interprets the full plan AST |
| `lumina-backend-duckdb` | Implemented | Translates plan to SQL via `PlanToSql`; runs against DuckDB in-process JDBC |
| `lumina-backend-polars` | Stub | Reserved for a future columnar backend; not on the roadmap |
| `lumina-backend-spark` | Stub | Future Spark translation layer for distributed execution |
| `lumina-config` | Implemented | `BackendRegistry` and `LuminaSession` for wiring API to backends |
| `integration-tests` | Implemented | Compiles and executes Java and Kotlin pipelines end-to-end |

---

## 7. Design Trade-offs: Expression Typing

The core `Expression` AST is intentionally **unparameterized** (not `Expression[T]`). While type parameters could catch some mismatches, they introduce three problems that matter for Lumina:

1. **Heterogeneous Logical Plans** — Real queries mix column types, so every node would devolve to `Expression[Any]` or require complicated existential types.
2. **JVM Interoperability** — Java and Kotlin callers would face verbose wildcards and Scala variance rules when composing expressions.
3. **Builder Complexity** — The DSL and backend contracts would inherit type-level machinery, making serialization and plan shipping harder.

The shared AST stays runtime-typed while leaving room for typed Scala-layer helpers (e.g., `ColumnRef.int("age")`) that don't leak into cross-language modules.

---

## 8. Technical Comparison

| Feature | Pandas (Python) | Spark (Scala) | Lumina |
|---------|-----------------|---------------|--------|
| Execution model | Eager | Lazy, distributed | Lazy, pluggable |
| JVM language support | N/A | Primarily Scala | Scala, Java, Kotlin |
| Small-data backend | N/A (Python only) | Overkill — JVM + driver overhead | `LocalBackend` (pure Scala collections) |
| Analytical backend | N/A | N/A | `DuckDBBackend` (in-process SQL via JDBC) |
| Distributed backend | N/A | Native | Planned (Spark translation layer) |
| Query optimiser | None | Catalyst | Rule-based (`CombineFilters`, `PredicatePushdown`) |
| Cross-backend type safety | N/A | N/A | `RowNormalizer` — identical value types from every backend |
| Type safety | None | Strong | Strong |
| External dependencies | numpy, pandas | Spark cluster | Zero for LocalBackend; `duckdb_jdbc` jar for DuckDB |
| Setup for local dev | pip install | Spark cluster or local mode | `sbt test` — no cluster, no native libs |
