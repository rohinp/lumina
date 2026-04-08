## Project : Lumina

The Unified Data Engineering Framework for the JVM

## 1. Vision
Lumina is a type-safe, lazy-execution data manipulation library written in Scala but designed for the entire JVM ecosystem. It provides a familiar, Pandas-like API that abstracts away the underlying execution engine.
The goal is to write logic once and choose the most efficient backend—from a single CPU to a massive Spark cluster—accessible seamlessly from Scala, Java, and Kotlin.
------------------------------
## 2. Core Problem Statement
The JVM data landscape is currently split between "heavy" distributed tools and "fragmented" local libraries:

* Small Data: No high-performance, lazy-first equivalent to Pandas or Polars on the JVM.
* Big Data: Apache Spark has high overhead for local development and testing.
* The Ecosystem Gap: Libraries are often built for one language (e.g., Scala-only or Java-only), making cross-team collaboration difficult.

------------------------------
## 3. The "Pluggable Backend" Architecture
Lumina separates the Interface (User Code) from the Implementation (Execution).
## The Three Execution Modes

   1. Local (Standalone): A pure JVM implementation for small, in-memory datasets.
   2. Turbo (Concurrent): Uses Polars (via JNI) or DuckDB to utilize all local CPU cores.
   3. Distributed (Cluster): Translates the logic into Spark or Flink jobs for petabyte-scale data.

------------------------------
## 4. Universal JVM Interoperability
While written in Scala for its powerful type system and lazy constructs, Lumina is built to be a first-class citizen for all JVM languages.

* Java-Friendly API: Exposes clean, idiomatic Java interfaces that hide Scala’s internal complexity (like implicits or macros).
* Kotlin Support: Fully compatible with Kotlin's syntax; users can leverage Lumina's data frames within Kotlin projects effortlessly.
* Standard Collections: Includes automatic converters for java.util.List and other standard JVM structures to ensure zero friction during data input/output.

------------------------------
## 5. Key Features

* Pandas-Inspired DSL: Familiar syntax for filtering, joining, and aggregating.
* Lazy Evaluation: Operations are recorded as a "Logical Plan" and only executed when an action (like .show() or .save()) is called.
* Compile-Time Type Safety: Catches schema errors before execution, even when called from Java or Kotlin.
* One Codebase, Any Scale:

// One API, switch backends with one config changevar df = Lumina.readCsv("data.csv", Backends.POLARS);
var result = df.filter(row -> row.getInt("age") > 21)
               .groupBy("city")
               .sum("revenue");


## Design Trade-offs: Expression Typing

We intentionally keep the core `Expression` AST unparameterized instead of using a generic form like `Expression[T]`. While type parameters could catch some mismatches (e.g., preventing `GreaterThan[String]` from comparing mixed column types), they introduce three issues that matter for Lumina:

1. **Heterogeneous Logical Plans** – Real queries juggle many column types at once, so every logical node would either devolve to `Expression[Any]` or adopt complicated existential types, negating the safety win.
2. **JVM Interoperability** – Java and Kotlin consumers would have to deal with verbose wildcards and Scala-specific variance rules when composing expressions, making the API feel foreign.
3. **Builder Complexity** – The fluent DSL and backend contracts would inherit type-level machinery, slowing development and making it harder to serialize/ship logical plans between processes.

Our chosen approach keeps the shared AST runtime-typed while leaving room for Scala-idiomatic helpers later (e.g., `ColumnRef.int("age")` returning a typed wrapper). This balance lets every JVM language interoperate today, yet still allows typed facades to add compile-time hints without leaking Scala mechanics into cross-language modules.


## 6. Module Layout

1. `lumina-plan`: Core logical plan graph, schema metadata, and serialization helpers used by every other module.
2. `lumina-api`: Public JVM-friendly DataFrame surface plus Scala/Java/Kotlin facades; depends only on `lumina-plan`.
3. `lumina-backend-local`: Pure Scala executor that interprets logical plans against in-memory collections; forms the initial compliance target.
4. `lumina-backend-polars`: JNI/FFI integration with Polars or DuckDB for multi-core execution (stubbed until implementation lands).
5. `lumina-backend-spark`: Adapter layer that lowers logical plans onto Spark/Flink for distributed execution (stubbed initially).
6. `lumina-config`: Backend registry and selection utilities that wire the API to a specific backend at runtime.
7. `integration-tests`: Cross-language smoke tests and end-to-end scenarios that validate the API plus chosen backend in concert.

------------------------------
## 7. Technical Comparison

| Feature | Pandas (Python) | Spark (Scala) | Lumina (Proposed) |
|---|---|---|---|
| Execution | Eager | Lazy (Distributed) | Lazy (Pluggable) |
| JVM Language Support | N/A | Primarily Scala | Scala, Java, Kotlin |
| Local Performance | Good | Poor (High Overhead) | Excellent (via Polars/JNI) |
| Type Safety | None | Strong | Strong |
