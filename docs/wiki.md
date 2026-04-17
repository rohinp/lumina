# Lumina Developer Wiki

A complete reference for building data pipelines with Lumina — a JVM-native, Pandas-inspired DataFrame library with pluggable execution backends (Local, DuckDB, Spark).

---

## Table of Contents

1. [What is Lumina?](#1-what-is-lumina)
2. [Core Concepts](#2-core-concepts)
3. [Quick Start](#3-quick-start)
4. [Reading Data](#4-reading-data)
5. [Filtering Rows](#5-filtering-rows)
6. [Selecting Columns](#6-selecting-columns)
7. [Adding and Replacing Columns](#7-adding-and-replacing-columns)
8. [Sorting](#8-sorting)
9. [Grouping and Aggregation](#9-grouping-and-aggregation)
10. [Joins](#10-joins)
11. [Window Functions](#11-window-functions)
12. [Set Operations](#12-set-operations)
13. [Null Handling](#13-null-handling)
14. [Expression Reference](#14-expression-reference)
15. [Aggregation Reference](#15-aggregation-reference)
16. [Collecting Results and Output](#16-collecting-results-and-output)
17. [Composing Pipelines with transform](#17-composing-pipelines-with-transform)
18. [Inspecting Plans with explain](#18-inspecting-plans-with-explain)
19. [Backends](#19-backends)
20. [Java and Kotlin Usage](#20-java-and-kotlin-usage)

---

## 1. What is Lumina?

Lumina is a **lazy, plan-based DataFrame library for the JVM**. You write data transformations using a fluent API; Lumina records them as a logical plan and executes the plan on a pluggable backend only when you call `collect`, `show`, `count`, or a similar action.

```
Your code → Logical Plan → Backend → Result rows
```

**Key properties:**
- **Lazy evaluation** — nothing runs until you ask for data.
- **Pluggable backends** — swap `LocalBackend` (pure Scala, zero deps) for `DuckDBBackend` (in-process SQL engine) or future Spark/Polars backends without changing your pipeline code.
- **JVM-first** — first-class Scala API with Java and Kotlin-idiomatic overloads on every method.
- **Write-once** — the same `DataFrame` object runs identically on every backend.

---

## 2. Core Concepts

### Logical Plan

Every DataFrame method (`filter`, `select`, `groupBy`, …) returns a **new** DataFrame wrapping a new plan node. No data moves at this point.

```
Lumina.readCsv("orders.csv")        →  ReadCsv node
  .filter(age > 30)                 →  Filter node
  .select("name", "city")           →  Project node
  .groupBy(...)                     →  Aggregate node
  .plan                             →  the full DAG, inspectable
```

### Backend

A `Backend` is the component that walks the plan and produces rows. Pass one to `collect`:

```scala
val backend = LocalBackend(DataRegistry.of("memory://orders" -> rows))
val result  = df.collect(backend)   // execution happens here
```

### Row

`Row` is a `Map[String, Any]` wrapper. Values are untyped at the library boundary; the backends normalise primitive JDBC types to Scala primitives (`Int`, `Long`, `Double`, `Boolean`, `String`, `java.time.LocalDate`, `java.time.LocalDateTime`).

### Expression

All column references, literals, and function calls are `Expression` values. They are **unparameterized** (`Expression`, not `Expression[T]`) so that Java and Kotlin callers never encounter Scala variance rules.

---

## 3. Quick Start

### Scala

```scala
import lumina.api.*
import lumina.plan.Expression.*
import lumina.plan.Aggregation.*
import lumina.backend.local.LocalBackend
import lumina.plan.backend.{DataRegistry, Row}

// 1. Build an in-memory data source
val rows = Vector(
  Row(Map("city" -> "Paris",  "age" -> 35, "revenue" -> 1000.0)),
  Row(Map("city" -> "Berlin", "age" -> 29, "revenue" -> 2000.0)),
  Row(Map("city" -> "Tokyo",  "age" -> 42, "revenue" ->  500.0)),
)
val backend = LocalBackend(DataRegistry.of("memory://sales" -> rows))

// 2. Build a lazy pipeline
val df = Lumina.readCsv("memory://sales")
  .where(GreaterThan(Lumina.col("revenue"), Literal(800.0)))
  .orderBy("revenue", ascending = false)
  .select("city", "revenue")

// 3. Execute
df.show(backend)
// +---------+---------+
// | city    | revenue |
// +---------+---------+
// | Berlin  | 2000.0  |
// | Paris   | 1000.0  |
// +---------+---------+
```

### Java

```java
import lumina.api.*;
import lumina.plan.Expression.*;
import lumina.plan.Aggregation;
import lumina.plan.backend.*;
import lumina.backend.local.LocalBackend;
import java.util.List;
import java.util.Map;

var rows = List.of(
    Row.of("city", "Paris",  "age", 35, "revenue", 1000.0),
    Row.of("city", "Berlin", "age", 29, "revenue", 2000.0),
    Row.of("city", "Tokyo",  "age", 42, "revenue",  500.0)
);
var backend = new LocalBackend(DataRegistry.empty().register("memory://sales", rows));

var result = LuminaJava.readCsv("memory://sales")
    .filter(new GreaterThan(new ColumnRef("revenue"), new Literal(800.0)))
    .orderBy("revenue", false)
    .select("city", "revenue")
    .collectAsList(backend);
```

### Pandas comparison

```python
# Pandas
import pandas as pd

df = pd.DataFrame({
    "city":    ["Paris", "Berlin", "Tokyo"],
    "age":     [35, 29, 42],
    "revenue": [1000.0, 2000.0, 500.0]
})
result = (df[df["revenue"] > 800]
            .sort_values("revenue", ascending=False)
            [["city", "revenue"]])
```

---

## 4. Reading Data

### From a CSV file

| Pandas | Lumina (Scala) |
|--------|----------------|
| `pd.read_csv("orders.csv")` | `Lumina.readCsv("orders.csv")` |

```scala
val df = Lumina.readCsv("orders.csv")
```

```java
// Java
var df = LuminaJava.readCsv("orders.csv");
```

### From in-memory rows (tests / embedding)

```scala
val rows = Vector(
  Row(Map("id" -> 1, "name" -> "Alice")),
  Row(Map("id" -> 2, "name" -> "Bob")),
)
val backend = LocalBackend(DataRegistry.of("memory://users" -> rows))
val df      = Lumina.readCsv("memory://users")
```

`DataRegistry.of(...)` accepts any number of `(uri -> rows)` pairs. The `memory://` scheme is resolved entirely in memory — no file I/O.

```java
// Java — DataRegistry from multiple sources
var registry = DataRegistry.empty()
    .register("memory://users",  userRows)
    .register("memory://orders", orderRows);
var backend = new LocalBackend(registry);
```

### With DuckDB backend

```scala
import lumina.backend.duckdb.DuckDBBackend

val backend = DuckDBBackend(DataRegistry.of("memory://sales" -> rows))
val result  = df.collect(backend)
```

The `DuckDBBackend` opens a **fresh in-process JDBC connection per `execute()` call** — it is stateless and thread-safe. When using `memory://` URIs it creates temporary DuckDB tables, runs the translated SQL, and closes the connection.

---

## 5. Filtering Rows

`filter` (and its alias `where`) push a `Filter` node onto the plan. The condition is an `Expression` tree that both backends evaluate independently — `LocalBackend` evaluates it row-by-row in Scala; `DuckDBBackend` translates it to a SQL `WHERE` clause.

### Comparison operators

| Operation | Expression | Pandas equivalent |
|-----------|-----------|-------------------|
| `col > value` | `GreaterThan(col("x"), Literal(v))` | `df[df["x"] > v]` |
| `col >= value` | `GreaterThanOrEqual(col("x"), Literal(v))` | `df[df["x"] >= v]` |
| `col < value` | `LessThan(col("x"), Literal(v))` | `df[df["x"] < v]` |
| `col <= value` | `LessThanOrEqual(col("x"), Literal(v))` | `df[df["x"] <= v]` |
| `col == value` | `EqualTo(col("x"), Literal(v))` | `df[df["x"] == v]` |
| `col != value` | `NotEqualTo(col("x"), Literal(v))` | `df[df["x"] != v]` |

```scala
// Scala — single condition
val adults = df.filter(GreaterThanOrEqual(Lumina.col("age"), Literal(18)))

// Alias: where is identical to filter
val adults = df.where(GreaterThanOrEqual(Lumina.col("age"), Literal(18)))
```

```java
// Java
var adults = df.filter(new GreaterThanOrEqual(new ColumnRef("age"), new Literal(18)));
```

```python
# Pandas
adults = df[df["age"] >= 18]
```

### Logical combinators

```scala
// AND — both conditions must be true
df.filter(And(
  GreaterThan(Lumina.col("age"), Literal(18)),
  EqualTo(Lumina.col("city"), Literal("Paris"))
))

// OR
df.filter(Or(
  EqualTo(Lumina.col("city"), Literal("Paris")),
  EqualTo(Lumina.col("city"), Literal("Berlin"))
))

// NOT
df.filter(Not(EqualTo(Lumina.col("status"), Literal("inactive"))))
```

```python
# Pandas equivalents
df[(df["age"] > 18) & (df["city"] == "Paris")]
df[(df["city"] == "Paris") | (df["city"] == "Berlin")]
df[df["status"] != "inactive"]
```

### Chaining filters

```scala
// Equivalent to AND — plan optimizer combines consecutive Filters automatically
df.where(GreaterThan(Lumina.col("revenue"), Literal(1000.0)))
  .where(EqualTo(Lumina.col("city"), Literal("Paris")))
```

### Null checks

```scala
df.filter(IsNull(Lumina.col("email")))     // rows where email IS NULL
df.filter(IsNotNull(Lumina.col("email")))  // rows where email IS NOT NULL
```

```python
# Pandas
df[df["email"].isna()]
df[df["email"].notna()]
```

### Set membership (IN)

```scala
df.filter(In(Lumina.col("city"), Vector(Literal("Paris"), Literal("Berlin"))))
```

```python
# Pandas
df[df["city"].isin(["Paris", "Berlin"])]
```

### Range (BETWEEN)

```scala
// Inclusive on both ends; returns false for null values
df.filter(Between(Lumina.col("age"), Literal(18), Literal(65)))
```

```python
df[df["age"].between(18, 65)]
```

---

## 6. Selecting Columns

`select` pushes a `Project` node onto the plan. Lumina offers three overloads.

### By column name (strings)

```scala
df.select("city", "revenue")          // keeps only these two columns
df.select("city")                     // single column
```

```java
// Java — using the string varargs overload
df.select("city", "revenue");
```

```python
# Pandas
df[["city", "revenue"]]
df[["city"]]
```

### By Expression (full control)

```scala
import lumina.plan.Expression.*

df.select(
  ColumnRef("city"),
  Alias(Multiply(ColumnRef("revenue"), Literal(1.1)), "revenue_with_tax")
)
```

```python
# Pandas
df.assign(revenue_with_tax=df["revenue"] * 1.1)[["city", "revenue_with_tax"]]
```

### col() shorthand

`Lumina.col(name)` and `df.col(name)` both return a `ColumnRef` — use whichever reads more naturally:

```scala
df.filter(GreaterThan(df.col("age"), Literal(30)))
df.filter(GreaterThan(Lumina.col("age"), Literal(30)))
```

---

## 7. Adding and Replacing Columns

### withColumn

Appends a new column or overwrites an existing one. If the column name already exists, the old value is replaced.

```scala
// Compute a new derived column
df.withColumn("tax", Multiply(Lumina.col("revenue"), Literal(0.2)))

// Overwrite an existing column (capitalise city names)
df.withColumn("city", Upper(Lumina.col("city")))
```

```java
// Java
df.withColumn("tax", new Multiply(new ColumnRef("revenue"), new Literal(0.2)));
```

```python
# Pandas
df["tax"] = df["revenue"] * 0.2
df["city"] = df["city"].str.upper()
```

### withColumnRenamed

```scala
df.withColumnRenamed("old_name", "new_name")
```

```python
df.rename(columns={"old_name": "new_name"})
```

### drop

```scala
df.drop("temp_col", "internal_id")
```

```python
df.drop(columns=["temp_col", "internal_id"])
```

### dropNa / fillNa

```scala
// Remove rows where any of the listed columns is null
df.dropNa("email", "phone")

// Replace nulls with a default value
df.fillNa(0, "revenue", "cost")
df.fillNa("unknown", "city")
```

```python
# Pandas
df.dropna(subset=["email", "phone"])
df[["revenue", "cost"]].fillna(0)
df["city"].fillna("unknown")
```

---

## 8. Sorting

### By column name

```scala
df.orderBy("age")                         // ascending (default)
df.orderBy("revenue", ascending = false)  // descending
```

```java
df.orderBy("age");                   // ascending
df.orderBy("revenue", false);        // descending
```

```python
df.sort_values("age")
df.sort_values("revenue", ascending=False)
```

### By expression (multi-column sort)

```scala
df.sort(
  Lumina.asc(Lumina.col("city")),
  Lumina.desc(Lumina.col("revenue"))
)
```

```java
var sortExprs = List.of(Lumina.asc(new ColumnRef("city")), Lumina.desc(new ColumnRef("revenue")));
df.sort(sortExprs);
```

```python
df.sort_values(["city", "revenue"], ascending=[True, False])
```

### Limit

```scala
df.orderBy("revenue", ascending = false).limit(10)   // top 10 by revenue
```

```python
df.sort_values("revenue", ascending=False).head(10)
```

---

## 9. Grouping and Aggregation

### Basic groupBy

```scala
import lumina.plan.Aggregation.*

df.groupBy(
  grouping      = Seq(ColumnRef("city")),
  aggregations  = Seq(
    Sum(ColumnRef("revenue"), Some("total_revenue")),
    Count(None, Some("order_count"))
  )
)
```

```java
// Java — use factory methods to avoid variance issues with List<Aggregation>
var result = df.groupBy(
    List.of(new ColumnRef("city")),
    List.of(
        Aggregation.sum(new ColumnRef("revenue"), "total_revenue"),
        Aggregation.countAll("order_count")
    )
);
```

```python
# Pandas
df.groupby("city").agg(
    total_revenue=("revenue", "sum"),
    order_count=("revenue", "count")
).reset_index()
```

### Whole-table aggregation with agg

```scala
// No groupBy — aggregate the entire table
df.agg(
  Count(None, Some("total_rows")),
  Sum(ColumnRef("revenue"), Some("total_revenue")),
  Avg(ColumnRef("age"), Some("avg_age"))
)
```

```java
var stats = df.agg(List.of(
    Aggregation.countAll("total_rows"),
    Aggregation.sum(new ColumnRef("revenue"), "total_revenue"),
    Aggregation.avg(new ColumnRef("age"), "avg_age")
));
```

```python
df.agg(
    total_rows=("revenue", "count"),
    total_revenue=("revenue", "sum"),
    avg_age=("age", "mean")
)
```

### Available aggregations

| Aggregation | Description |
|-------------|-------------|
| `Sum(col, alias)` | Sum of non-null values |
| `Count(None, alias)` | Count all rows (`COUNT(*)`) |
| `Count(Some(col), alias)` | Count non-null values in column |
| `Avg(col, alias)` | Arithmetic mean |
| `Min(col, alias)` | Minimum value |
| `Max(col, alias)` | Maximum value |
| `CountDistinct(col, alias)` | Count of distinct non-null values |
| `StdDev(col, alias)` | Sample standard deviation (N−1) |
| `Variance(col, alias)` | Sample variance (N−1) |
| `Median(col, alias)` | Median; average of two middle values for even N |
| `First(col, alias)` | First non-null value in input order |
| `Last(col, alias)` | Last non-null value in input order |

### describe — summary statistics

```scala
df.describe(backend)
// +---------+-------+--------+--------+
// | summary | age   | revenue| city   |
// +---------+-------+--------+--------+
// | count   | 3     | 3      | 3      |
// | mean    | 35.33 | 1166.7 | N/A    |
// | stddev  | 6.506 | 776.0  | N/A    |
// | min     | 29.0  | 500.0  | Berlin |
// | max     | 42.0  | 2000.0 | Tokyo  |
// +---------+-------+--------+--------+
```

```python
df.describe(include="all")
```

---

## 10. Joins

All joins use an explicit `Expression` as the ON condition. Use **distinct column names** on each side when both tables share a column name (e.g. name the right-side key `r_id` instead of `id`) to avoid row-merge collisions in `LocalBackend`.

### Inner join

```scala
val orders    = Lumina.readCsv("memory://orders")
val customers = Lumina.readCsv("memory://customers")

orders.join(
  customers,
  condition = EqualTo(ColumnRef("order_cid"), ColumnRef("customer_id"))
)
```

```java
orders.join(
    customers,
    new EqualTo(new ColumnRef("order_cid"), new ColumnRef("customer_id"))
);
```

```python
orders.merge(customers, left_on="order_cid", right_on="customer_id", how="inner")
```

### Left / Right / Full outer join

```scala
orders.join(customers, condition, JoinType.Left)
orders.join(customers, condition, JoinType.Right)
orders.join(customers, condition, JoinType.Full)
```

```python
orders.merge(customers, ..., how="left")
orders.merge(customers, ..., how="right")
orders.merge(customers, ..., how="outer")
```

### Semi-join — filter by existence

Keeps left rows that have **at least one** matching right row. Output contains only left columns.

```scala
// Which orders come from active customers?
orders.semiJoin(activeCustomers, EqualTo(ColumnRef("order_cid"), ColumnRef("cid")))
```

```python
# Pandas equivalent
orders[orders["order_cid"].isin(active_customers["cid"])]
```

### Anti-join — filter by absence

Keeps left rows that have **no** matching right row.

```scala
// Which orders have no corresponding customer?
orders.antiJoin(customers, EqualTo(ColumnRef("order_cid"), ColumnRef("cid")))
```

```python
mask = ~orders["order_cid"].isin(customers["cid"])
orders[mask]
```

### Cross join

```scala
tableA.crossJoin(tableB)   // every row in A × every row in B
```

---

## 11. Window Functions

Window functions compute a value for each row based on a sliding "window" of related rows — without collapsing the row count the way `groupBy` does.

### Window spec

```scala
import lumina.plan.*
import lumina.plan.Expression.*

val spec = WindowSpec(
  partitionBy = Vector(ColumnRef("city")),           // reset the frame per city
  orderBy     = Vector(SortExpr(ColumnRef("revenue"), ascending = false))
)
```

### Row number, rank, dense rank

```scala
df.window(
  WindowExpr.RowNumber("rn",         spec),   // 1, 2, 3, … per partition
  WindowExpr.Rank("rank",            spec),   // 1, 1, 3, … (gaps for ties)
  WindowExpr.DenseRank("dense_rank", spec)    // 1, 1, 2, … (no gaps)
)
```

```python
import pandas as pd

df["rn"]         = df.groupby("city")["revenue"].rank(method="first",  ascending=False).astype(int)
df["rank"]       = df.groupby("city")["revenue"].rank(method="min",    ascending=False).astype(int)
df["dense_rank"] = df.groupby("city")["revenue"].rank(method="dense",  ascending=False).astype(int)
```

### Window aggregation (running totals, moving averages)

```scala
df.window(
  WindowExpr.WindowAgg(
    agg   = Aggregation.Sum(ColumnRef("revenue")),
    alias = "running_revenue",
    spec  = spec
  )
)
```

```python
df["running_revenue"] = df.groupby("city")["revenue"].cumsum()
```

### Lag / Lead

```scala
df.window(
  WindowExpr.Lag( ColumnRef("revenue"), n = 1, alias = "prev_revenue", spec = spec),
  WindowExpr.Lead(ColumnRef("revenue"), n = 1, alias = "next_revenue", spec = spec)
)
```

```python
df["prev_revenue"] = df.groupby("city")["revenue"].shift(1)
df["next_revenue"] = df.groupby("city")["revenue"].shift(-1)
```

---

## 12. Set Operations

Both DataFrames must have identical column names.

### Union (with duplicates)

```scala
df1.unionAll(df2)
```

```python
pd.concat([df1, df2])
```

### Distinct

```scala
df.distinct()         // remove duplicate rows
df1.unionAll(df2).distinct()   // union + dedup = UNION in SQL
```

```python
df.drop_duplicates()
pd.concat([df1, df2]).drop_duplicates()
```

### Intersect

```scala
df1.intersect(df2)    // rows that appear in BOTH
```

```python
pd.merge(df1, df2, how="inner")
```

### Except

```scala
df1.except(df2)       // rows in df1 that do NOT appear in df2
```

```python
df1[~df1.apply(tuple, axis=1).isin(df2.apply(tuple, axis=1))]
```

### Sample

```scala
df.sample(fraction = 0.1)             // ~10% of rows, random
df.sample(fraction = 0.1, seed = Some(42L))   // reproducible (LocalBackend only)
```

```python
df.sample(frac=0.1, random_state=42)
```

---

## 13. Null Handling

### Coalesce — first non-null

```scala
// Use 0.0 when revenue is null
df.withColumn("revenue", Coalesce(Vector(ColumnRef("revenue"), Literal(0.0))))
```

```python
df["revenue"].fillna(0.0)
```

### IfNull — two-argument shorthand

```scala
df.withColumn("city", IfNull(ColumnRef("city"), Literal("Unknown")))
```

```python
df["city"].fillna("Unknown")
```

### NullIf — convert sentinel to null

```scala
// Treat empty string as null
df.withColumn("email", NullIf(ColumnRef("email"), Literal("")))
```

```python
df["email"].replace("", None)
```

### dropNa / fillNa (DataFrame shortcuts)

```scala
df.dropNa("email", "phone")        // drop rows where email OR phone is null
df.fillNa(0, "revenue", "cost")    // fill both columns
df.fillNa("n/a", "city", "country")
```

---

## 14. Expression Reference

Import with `import lumina.plan.Expression.*` (Scala) or use the fully-qualified class name in Java.

### Comparison

| Expression | SQL equivalent | Example |
|------------|----------------|---------|
| `GreaterThan(l, r)` | `l > r` | `GreaterThan(col("age"), Literal(30))` |
| `GreaterThanOrEqual(l, r)` | `l >= r` | |
| `LessThan(l, r)` | `l < r` | |
| `LessThanOrEqual(l, r)` | `l <= r` | |
| `EqualTo(l, r)` | `l = r` | |
| `NotEqualTo(l, r)` | `l <> r` | |
| `Between(e, lo, hi)` | `e BETWEEN lo AND hi` | `Between(col("age"), Literal(18), Literal(65))` |

### Logical

| Expression | SQL equivalent |
|------------|----------------|
| `And(l, r)` | `l AND r` |
| `Or(l, r)` | `l OR r` |
| `Not(e)` | `NOT e` |
| `IsNull(e)` | `e IS NULL` |
| `IsNotNull(e)` | `e IS NOT NULL` |
| `In(e, values)` | `e IN (v1, v2, …)` |

### Arithmetic

| Expression | SQL equivalent |
|------------|----------------|
| `Add(l, r)` | `l + r` |
| `Subtract(l, r)` | `l - r` |
| `Multiply(l, r)` | `l * r` |
| `Divide(l, r)` | `l / r` |
| `Negate(e)` | `-e` |
| `Mod(dividend, divisor)` | `dividend % divisor` |

### String functions

| Expression | Description | SQL equivalent |
|------------|-------------|----------------|
| `Upper(e)` | Uppercase | `UPPER(e)` |
| `Lower(e)` | Lowercase | `LOWER(e)` |
| `Trim(e)` | Strip whitespace | `TRIM(e)` |
| `Length(e)` | Character count | `LENGTH(e)` |
| `Concat(exprs)` | Concatenate strings | `CONCAT(e1, e2, …)` |
| `Substring(e, start, len)` | Extract substring (1-based start) | `SUBSTRING(e, start, len)` |
| `Like(e, pattern)` | SQL LIKE (`%`, `_`) | `e LIKE pattern` |
| `Replace(e, search, repl)` | Replace all occurrences | `REPLACE(e, s, r)` |
| `RegexpExtract(e, pattern, group)` | First regex match group | `regexp_extract(e, pattern, group)` |
| `RegexpReplace(e, pattern, repl)` | Replace all regex matches | `regexp_replace(e, pattern, repl, 'g')` |
| `StartsWith(e, prefix)` | String starts with prefix | `starts_with(e, prefix)` |
| `EndsWith(e, suffix)` | String ends with suffix | `ends_with(e, suffix)` |
| `LPad(e, length, pad)` | Left-pad to length | `LPAD(e, length, pad)` |
| `RPad(e, length, pad)` | Right-pad to length | `RPAD(e, length, pad)` |
| `Repeat(e, n)` | Repeat string n times | `repeat(e, n)` |
| `Reverse(e)` | Reverse characters | `reverse(e)` |
| `InitCap(e)` | Title-case each word | `INITCAP(e)` |

### Numeric functions

| Expression | Description |
|------------|-------------|
| `Abs(e)` | Absolute value |
| `Round(e, scale)` | Round to `scale` decimal places |
| `Floor(e)` | Largest integer ≤ e |
| `Ceil(e)` | Smallest integer ≥ e |
| `Sqrt(e)` | Square root |
| `Power(base, exp)` | base ^ exponent |
| `Log(e)` | Natural logarithm |
| `Log2(e)` | Base-2 logarithm |
| `Log10(e)` | Base-10 logarithm |
| `Exp(e)` | e^value |
| `Sign(e)` | −1, 0, or 1 |
| `Greatest(exprs)` | Maximum of two or more (skips nulls in LocalBackend) |
| `Least(exprs)` | Minimum of two or more (skips nulls in LocalBackend) |

### Hash functions

| Expression | Output | Note |
|------------|--------|------|
| `Md5(e)` | 32-char lowercase hex | Null input → null output |
| `Sha256(e)` | 64-char lowercase hex | Null input → null output |

Both backends produce byte-identical digests over the UTF-8 string representation of the value.

### Date / time functions

| Expression | Description |
|------------|-------------|
| `ToDate(e)` | Parse ISO-8601 string to `LocalDate` |
| `ToTimestamp(e)` | Parse ISO-8601 string to `LocalDateTime` |
| `Year(e)` | 4-digit year |
| `Month(e)` | Month 1–12 |
| `Day(e)` | Day of month 1–31 |
| `Hour(e)` | Hour 0–23 |
| `Minute(e)` | Minute 0–59 |
| `Second(e)` | Second 0–59 |
| `DayOfWeek(e)` | ISO day: 1=Mon, 7=Sun |
| `DateAdd(date, days)` | Add N days to a date |
| `DateDiff(end, start)` | Days between start and end (end − start) |
| `DateFormat(e, pattern)` | Format date using Java `DateTimeFormatter` pattern |

```scala
// Parse a date and extract components
df.withColumn("date",  ToDate(ColumnRef("date_str")))
  .withColumn("year",  Year(ColumnRef("date")))
  .withColumn("month", Month(ColumnRef("date")))
  .withColumn("label", DateFormat(ColumnRef("date"), "dd/MM/yyyy"))
```

### Conditional expressions

| Expression | Description |
|------------|-------------|
| `CaseWhen(branches, otherwise)` | Multi-branch CASE WHEN |
| `If(cond, thenExpr, elseExpr)` | Two-branch conditional |
| `NullIf(expr, nullValue)` | Return null when expr equals nullValue |
| `IfNull(expr, replacement)` | Return replacement when expr is null |
| `Coalesce(exprs)` | First non-null from a list |

```scala
// Grade mapping
df.withColumn("grade",
  CaseWhen(
    branches = Vector(
      GreaterThanOrEqual(ColumnRef("score"), Literal(90)) -> Literal("A"),
      GreaterThanOrEqual(ColumnRef("score"), Literal(70)) -> Literal("B"),
      GreaterThanOrEqual(ColumnRef("score"), Literal(50)) -> Literal("C")
    ),
    otherwise = Some(Literal("F"))
  )
)

// Simple if
df.withColumn("result", If(GreaterThan(ColumnRef("score"), Literal(50)), Literal("pass"), Literal("fail")))
```

```python
# Pandas
import numpy as np

df["grade"] = np.select(
    [df["score"] >= 90, df["score"] >= 70, df["score"] >= 50],
    ["A", "B", "C"],
    default="F"
)
df["result"] = np.where(df["score"] > 50, "pass", "fail")
```

### Type casting

```scala
import lumina.plan.DataType

df.withColumn("age_long", Cast(ColumnRef("age"), DataType.Int64))
df.withColumn("price",    Cast(ColumnRef("price_str"), DataType.Float64))
```

```python
df["age_long"] = df["age"].astype("int64")
df["price"]    = df["price_str"].astype(float)
```

Available `DataType` values: `Int32`, `Int64`, `Float64`, `BooleanType`, `StringType`, `DateType`, `TimestampType`.

### Alias — named output column

```scala
df.select(
  ColumnRef("city"),
  Alias(Multiply(ColumnRef("revenue"), Literal(1.1)), "revenue_plus_10pct")
)
```

---

## 15. Aggregation Reference

All aggregations accept an `alias: Option[String]` that names the output column. Without an alias the column name is implementation-defined.

```scala
Sum(ColumnRef("revenue"), Some("total"))   // named output
Sum(ColumnRef("revenue"))                  // unnamed
```

**Java factory methods** (return `Aggregation` so `List.of(...)` infers `List<Aggregation>`):

```java
Aggregation.sum(new ColumnRef("revenue"), "total")
Aggregation.avg(new ColumnRef("score"),   "avg_score")
Aggregation.min(new ColumnRef("age"),     "min_age")
Aggregation.max(new ColumnRef("age"),     "max_age")
Aggregation.countAll("row_count")
Aggregation.count(new ColumnRef("email"), "emails")
Aggregation.countDistinct(new ColumnRef("city"), "unique_cities")
Aggregation.stddev(new ColumnRef("price"), "price_stddev")
Aggregation.variance(new ColumnRef("price"))
Aggregation.first(new ColumnRef("name"),  "first_name")
Aggregation.last(new ColumnRef("name"),   "last_name")
Aggregation.median(new ColumnRef("score"), "median_score")
```

---

## 16. Collecting Results and Output

### collect — get all rows

```scala
val rows: Vector[Row] = df.collect(backend)
rows.foreach(r => println(r.values("city")))
```

```java
List<Row> rows = df.collectAsList(backend);
```

### show — print ASCII table

```scala
df.show(backend)        // up to 20 rows
df.show(backend, n = 5) // first 5 rows
```

```
+--------+-----+---------+
| city   | age | revenue |
+--------+-----+---------+
| Paris  |  35 | 1000.0  |
| Berlin |  29 | 2000.0  |
+--------+-----+---------+
2 row(s)
```

### head / count / isEmpty / nonEmpty

```scala
df.count(backend)                // Long — total row count (pushes COUNT(*) to backend)
df.head(5, backend)              // Vector[Row] — first 5 rows
df.isEmpty(backend)              // Boolean
df.nonEmpty(backend)             // Boolean
```

```python
len(df)
df.head(5)
df.empty
not df.empty
```

### toCsvString / writeCsv

```scala
val csv = df.toCsvString(backend)
df.writeCsv("output.csv", backend)
df.writeCsv("output.csv", backend, includeHeader = false)
```

### toJsonLines

```scala
val jsonl = df.toJsonLines(backend)
// {"city":"Paris","revenue":1000.0}
// {"city":"Berlin","revenue":2000.0}
```

---

## 17. Composing Pipelines with transform

`transform` applies a `DataFrame => DataFrame` function to the current DataFrame. This lets you name, reuse, and chain transformation steps cleanly.

```scala
def onlyActive(df: DataFrame): DataFrame =
  df.filter(EqualTo(Lumina.col("status"), Literal("active")))

def withTax(df: DataFrame): DataFrame =
  df.withColumn("tax", Multiply(Lumina.col("revenue"), Literal(0.2)))

def topN(n: Int)(df: DataFrame): DataFrame =
  df.orderBy("revenue", ascending = false).limit(n)

// Chain transformations
val result = df
  .transform(onlyActive)
  .transform(withTax)
  .transform(topN(10))
  .collect(backend)
```

```python
# Pandas equivalent using pipe()
def only_active(df): return df[df["status"] == "active"]
def with_tax(df):    df["tax"] = df["revenue"] * 0.2; return df
def top_n(n):        return lambda df: df.sort_values("revenue", ascending=False).head(n)

result = df.pipe(only_active).pipe(with_tax).pipe(top_n(10))
```

---

## 18. Inspecting Plans with explain

`explain()` prints the logical plan as a tree — useful for understanding what will execute before triggering it.

```scala
val df = Lumina.readCsv("memory://sales")
  .where(GreaterThan(Lumina.col("revenue"), Literal(800.0)))
  .select("city", "revenue")
  .orderBy("revenue", ascending = false)

df.explain()
// == Logical Plan ==
// Sort [revenue DESC]
//    +- Project [city, revenue]
//       +- Filter (revenue > 800.0)
//          +- ReadCsv [memory://sales]
```

`explainString` returns the same string without printing:

```scala
val planText = df.explainString
```

---

## 19. Backends

### LocalBackend

Pure Scala in-memory executor. Zero external dependencies. Walks the `LogicalPlan` AST row by row. Best for: unit tests, small datasets, environments where you cannot add JVM dependencies.

```scala
import lumina.backend.local.LocalBackend
import lumina.plan.backend.{DataRegistry, Row}

val rows    = Vector(Row(Map("x" -> 1, "y" -> 2)))
val backend = LocalBackend(DataRegistry.of("memory://t" -> rows))
val result  = df.collect(backend)
```

### DuckDBBackend

Translates the `LogicalPlan` to nested SQL subqueries and runs them via JDBC against an in-process DuckDB engine. Best for: analytical queries on larger datasets, production use, anything where SQL-native optimisation matters.

```scala
import lumina.backend.duckdb.DuckDBBackend
import lumina.plan.backend.DataRegistry

val backend = DuckDBBackend(DataRegistry.of("memory://t" -> rows))
val result  = df.collect(backend)
```

**Key DuckDB notes:**
- A fresh JDBC connection is opened per `execute()` call — no shared state.
- `memory://` URIs are materialised as temporary DuckDB tables within that connection.
- The optimizer runs before translation: consecutive `Filter` nodes are combined into a single `WHERE` clause; filters are pushed below `Project` and `Sort` nodes automatically.
- Both backends return **identical value types** thanks to `RowNormalizer`: JDBC boxed integers become Scala `Int`, `java.sql.Date` becomes `java.time.LocalDate`, etc.

### BackendRegistry / LuminaSession

For applications that want to select a backend by name at startup:

```scala
import lumina.config.{BackendRegistry, LuminaSession}

val session = LuminaSession(BackendRegistry.default(), backendName = "duckdb")
val result  = df.collect(session.backend)
```

`BackendRegistry.default()` pre-registers `"local"` and `"duckdb"`.

### Swapping backends without changing pipelines

```scala
def pipeline(df: DataFrame): DataFrame =
  df.where(GreaterThan(Lumina.col("age"), Literal(30)))
    .groupBy(Seq(ColumnRef("city")), Seq(Sum(ColumnRef("revenue"), Some("total"))))

// Same pipeline, two backends
val localResult  = pipeline(df).collect(localBackend)
val duckdbResult = pipeline(df).collect(duckdbBackend)
// localResult == duckdbResult (backends are behaviourally equivalent)
```

---

## 20. Java and Kotlin Usage

### Entry points

| Scala | Java / Kotlin |
|-------|---------------|
| `Lumina.readCsv(path)` | `LuminaJava.readCsv(path)` |
| `Lumina.col(name)` | `new ColumnRef(name)` |
| `Lumina.asc(expr)` | `Lumina.asc(expr)` *(same)* |

### Row construction

```java
// Java
Row row = Row.of("city", "Paris", "age", 35, "revenue", 1000.0);
```

```kotlin
// Kotlin
val row = Row.of("city", "Paris", "age", 35, "revenue", 1000.0)
```

### DataRegistry

```java
var registry = DataRegistry.empty()
    .register("memory://users",  userRows)
    .register("memory://orders", orderRows);
var backend = new LocalBackend(registry);
```

### Full Java pipeline

```java
import lumina.api.*;
import lumina.plan.Expression.*;
import lumina.plan.Aggregation;
import lumina.plan.backend.*;
import lumina.backend.local.LocalBackend;
import java.util.List;

var rows = List.of(
    Row.of("city", "Paris",  "age", 35, "revenue", 1000.0),
    Row.of("city", "Berlin", "age", 29, "revenue", 2000.0),
    Row.of("city", "Paris",  "age", 28, "revenue", 1500.0)
);
var backend = new LocalBackend(DataRegistry.empty().register("memory://sales", rows));

var result = LuminaJava.readCsv("memory://sales")
    .filter(new GreaterThan(new ColumnRef("age"), new Literal(25)))
    .groupBy(
        List.of(new ColumnRef("city")),
        List.of(
            Aggregation.sum(new ColumnRef("revenue"), "total"),
            Aggregation.countAll("orders")
        )
    )
    .orderBy("total", false)
    .collectAsList(backend);

for (var row : result) {
    System.out.println(row.values().get("city") + " → " + row.values().get("total"));
}
// Paris  → 2500.0
// Berlin → 2000.0
```

### Full Kotlin pipeline

```kotlin
import lumina.api.*
import lumina.plan.Expression.*
import lumina.plan.Aggregation
import lumina.plan.backend.*
import lumina.backend.local.LocalBackend

val rows = listOf(
    Row.of("city", "Paris",  "age", 35, "revenue", 1000.0),
    Row.of("city", "Berlin", "age", 29, "revenue", 2000.0),
    Row.of("city", "Paris",  "age", 28, "revenue", 1500.0)
)
val backend = LocalBackend(DataRegistry.empty().register("memory://sales", rows))

val result = LuminaJava.readCsv("memory://sales")
    .filter(GreaterThan(ColumnRef("age"), Literal(25)))
    .groupBy(
        listOf(ColumnRef("city")),
        listOf(
            Aggregation.sum(ColumnRef("revenue"), "total"),
            Aggregation.countAll("orders")
        )
    )
    .orderBy("total", false)
    .collectAsList(backend)

result.forEach { row ->
    println("${row.values["city"]} → ${row.values["total"]}")
}
```

### Full Scala pipeline (comparable)

```scala
import lumina.api.*
import lumina.plan.Expression.*
import lumina.plan.Aggregation.*
import lumina.backend.local.LocalBackend
import lumina.plan.backend.{DataRegistry, Row}

val rows = Vector(
  Row(Map("city" -> "Paris",  "age" -> 35, "revenue" -> 1000.0)),
  Row(Map("city" -> "Berlin", "age" -> 29, "revenue" -> 2000.0)),
  Row(Map("city" -> "Paris",  "age" -> 28, "revenue" -> 1500.0)),
)
val backend = LocalBackend(DataRegistry.of("memory://sales" -> rows))

val result = Lumina.readCsv("memory://sales")
  .filter(GreaterThan(Lumina.col("age"), Literal(25)))
  .groupBy(
    grouping     = Seq(ColumnRef("city")),
    aggregations = Seq(Sum(ColumnRef("revenue"), Some("total")), Count(None, Some("orders")))
  )
  .orderBy("total", ascending = false)
  .collect(backend)

result.foreach(row => println(s"${row.values("city")} → ${row.values("total")}"))
// Paris  → 2500.0
// Berlin → 2000.0
```
