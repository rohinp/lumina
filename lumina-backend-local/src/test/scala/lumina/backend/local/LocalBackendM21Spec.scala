package lumina.backend.local

import munit.FunSuite
import lumina.plan.*
import lumina.plan.Expression.*
import lumina.plan.Aggregation.*
import lumina.plan.backend.*

/**
 * Tests for M21 aggregate functions in LocalBackend: First, Last, Median.
 *
 * Read top-to-bottom as a specification for each function's behaviour.
 * First/Last operate in input order; Median uses sorted-value semantics.
 */
class LocalBackendM21Spec extends FunSuite:

  // Scores: 10, 40, 20, 30, null — two groups by "dept"
  private val rows = Vector(
    Row(Map("dept" -> "eng",  "name" -> "Alice", "score" -> 10)),
    Row(Map("dept" -> "eng",  "name" -> "Bob",   "score" -> 40)),
    Row(Map("dept" -> "eng",  "name" -> "Carol",  "score" -> 20)),
    Row(Map("dept" -> "mkt",  "name" -> "Dave",  "score" -> 30)),
    Row(Map("dept" -> "mkt",  "name" -> "Eve",   "score" -> null)),
    Row(Map("dept" -> "mkt",  "name" -> "Frank", "score" -> 50))
  )

  private val backend = LocalBackend(DataRegistry.of("memory://t" -> rows))
  private val src     = ReadCsv("memory://t", None)

  private def groupByDept(agg: Aggregation): Map[String, Any] =
    val plan = Aggregate(src, Vector(ColumnRef("dept")),
                         Vector(agg), None)
    backend.execute(plan) match
      case BackendResult.InMemory(rs) =>
        rs.map(r => r.values("dept").toString -> r.values.values.drop(1).head).toMap

  private def wholeTable(agg: Aggregation): Any =
    val plan = Aggregate(src, Vector.empty, Vector(agg), None)
    backend.execute(plan) match
      case BackendResult.InMemory(rs) => rs.head.values.values.head

  // ---------------------------------------------------------------------------
  // First
  // ---------------------------------------------------------------------------

  test("First returns the first non-null value in the group in input order"):
    val result = groupByDept(First(ColumnRef("score"), Some("v")))
    assertEquals(result("eng"), 10)   // first eng row has score 10
    assertEquals(result("mkt"), 30)   // first mkt row has score 30 (Dave)

  test("First skips leading null values and returns the first non-null"):
    val nullFirstRows = Vector(
      Row(Map("g" -> "a", "v" -> null)),
      Row(Map("g" -> "a", "v" -> 7)),
      Row(Map("g" -> "a", "v" -> 3))
    )
    val be   = LocalBackend(DataRegistry.of("memory://nf" -> nullFirstRows))
    val plan = Aggregate(ReadCsv("memory://nf", None), Vector(ColumnRef("g")),
                         Vector(First(ColumnRef("v"), Some("r"))), None)
    val rs   = be.execute(plan) match
      case BackendResult.InMemory(rs) => rs
    assertEquals(rs.head.values("r"), 7)

  test("First returns null when all values in the group are null"):
    val allNullRows = Vector(Row(Map("g" -> "x", "v" -> null)))
    val be          = LocalBackend(DataRegistry.of("memory://an" -> allNullRows))
    val plan        = Aggregate(ReadCsv("memory://an", None), Vector(ColumnRef("g")),
                                Vector(First(ColumnRef("v"), Some("r"))), None)
    val rs          = be.execute(plan) match
      case BackendResult.InMemory(rs) => rs
    assertEquals(rs.head.values("r"), null)

  test("First over the whole table returns the first non-null row value"):
    val result = wholeTable(First(ColumnRef("score"), Some("v")))
    assertEquals(result, 10)

  // ---------------------------------------------------------------------------
  // Last
  // ---------------------------------------------------------------------------

  test("Last returns the last non-null value in the group in input order"):
    val result = groupByDept(Last(ColumnRef("score"), Some("v")))
    assertEquals(result("eng"), 20)   // last eng row has score 20 (Carol)
    assertEquals(result("mkt"), 50)   // last non-null mkt score is 50 (Frank)

  test("Last skips trailing null values and returns the last non-null"):
    val nullLastRows = Vector(
      Row(Map("g" -> "a", "v" -> 5)),
      Row(Map("g" -> "a", "v" -> 9)),
      Row(Map("g" -> "a", "v" -> null))
    )
    val be   = LocalBackend(DataRegistry.of("memory://nl" -> nullLastRows))
    val plan = Aggregate(ReadCsv("memory://nl", None), Vector(ColumnRef("g")),
                         Vector(Last(ColumnRef("v"), Some("r"))), None)
    val rs   = be.execute(plan) match
      case BackendResult.InMemory(rs) => rs
    assertEquals(rs.head.values("r"), 9)

  test("Last returns null when all values in the group are null"):
    val allNullRows = Vector(Row(Map("g" -> "x", "v" -> null)))
    val be          = LocalBackend(DataRegistry.of("memory://aln" -> allNullRows))
    val plan        = Aggregate(ReadCsv("memory://aln", None), Vector(ColumnRef("g")),
                                Vector(Last(ColumnRef("v"), Some("r"))), None)
    val rs          = be.execute(plan) match
      case BackendResult.InMemory(rs) => rs
    assertEquals(rs.head.values("r"), null)

  test("Last over the whole table returns the last non-null value"):
    val result = wholeTable(Last(ColumnRef("score"), Some("v")))
    assertEquals(result, 50)

  // ---------------------------------------------------------------------------
  // Median
  // ---------------------------------------------------------------------------

  test("Median returns the middle value for an odd count of non-null values"):
    // eng scores: 10, 40, 20 → sorted: 10, 20, 40 → median = 20
    val result = groupByDept(Median(ColumnRef("score"), Some("v")))
    assertEquals(result("eng"), 20.0)

  test("Median returns the average of the two middle values for an even count"):
    // mkt non-null scores: 30, 50 → sorted: 30, 50 → median = (30+50)/2 = 40.0
    val result = groupByDept(Median(ColumnRef("score"), Some("v")))
    assertEquals(result("mkt"), 40.0)

  test("Median over the whole table works across all non-null rows"):
    // all scores: 10, 40, 20, 30, 50 (null excluded) → sorted: 10, 20, 30, 40, 50 → median = 30
    val result = wholeTable(Median(ColumnRef("score"), Some("v")))
    assertEquals(result, 30.0)

  test("Median returns null when all values in the group are null"):
    val allNullRows = Vector(Row(Map("g" -> "x", "v" -> null)))
    val be          = LocalBackend(DataRegistry.of("memory://amn" -> allNullRows))
    val plan        = Aggregate(ReadCsv("memory://amn", None), Vector(ColumnRef("g")),
                                Vector(Median(ColumnRef("v"), Some("r"))), None)
    val rs          = be.execute(plan) match
      case BackendResult.InMemory(rs) => rs
    assertEquals(rs.head.values("r"), null)

  test("Median works correctly for a single non-null value"):
    val singleRow = Vector(Row(Map("g" -> "x", "v" -> 42)))
    val be        = LocalBackend(DataRegistry.of("memory://sr" -> singleRow))
    val plan      = Aggregate(ReadCsv("memory://sr", None), Vector(ColumnRef("g")),
                              Vector(Median(ColumnRef("v"), Some("r"))), None)
    val rs        = be.execute(plan) match
      case BackendResult.InMemory(rs) => rs
    assertEquals(rs.head.values("r"), 42.0)
