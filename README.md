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
2. **Turbo** — Polars or DuckDB via JNI for multi-core execution. *(planned)*
3. **Distributed** — Spark or Flink for petabyte-scale data. *(planned)*

---

## 4. Current Status

| Milestone | Status | What shipped |
|-----------|--------|-------------|
| M1 — API Facade + Logical Plan Core | ✅ Complete | `DataFrame` DSL, plan AST nodes, schema/type system, expression AST, plan-shape tests |
| M2 — Backend Contract + Registry | ✅ Complete | `LocalBackend` (pure-Scala executor), `DataRegistry`, `CsvLoader`, `BackendRegistry`, `LuminaSession`, `PolarsBackend`/`SparkBackend` stubs |
| M3 — Multi-language Support | ✅ Complete | `LuminaJava` facade, `Row.of(...)` varargs factory, `Aggregation` Java factories, `Iterable`-based overloads; integration tests compile **and execute** pipelines from Java and Kotlin |
| M4 — Performance & Advanced Features | 🔜 Planned | Lazy eval controls (`cache`, `explain`), joins, window functions, UDFs |

---

## 5. Working Examples

### Scala

```scala
import lumina.api.Lumina
import lumina.plan.Expression.*
import lumina.plan.Aggregation.*
import lumina.backend.local.{LocalBackend, DataRegistry}
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
| `lumina-backend-polars` | Stub | Polars/DuckDB JNI integration for multi-core execution |
| `lumina-backend-spark` | Stub | Spark/Flink planner and distributed executor |
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
| Execution | Eager | Lazy (Distributed) | Lazy (Pluggable) |
| JVM Language Support | N/A | Primarily Scala | Scala, Java, Kotlin |
| Local Performance | Good | Poor (High Overhead) | Good (LocalBackend); Excellent planned (Polars/JNI) |
| Type Safety | None | Strong | Strong |
| Small-data overhead | Low | Very High | Low |
