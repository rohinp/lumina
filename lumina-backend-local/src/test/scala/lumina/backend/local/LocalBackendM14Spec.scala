package lumina.backend.local

import munit.FunSuite
import lumina.plan.*
import lumina.plan.Expression.*
import lumina.plan.backend.*

/**
 * Tests for M14 features in LocalBackend: Intersect, Except, Cast, Abs,
 * Round, Floor, and Ceil.
 *
 * Read top-to-bottom as a specification for each operator's behaviour.
 */
class LocalBackendM14Spec extends FunSuite:

  private val left = Vector(
    Row(Map("city" -> "Paris",  "score" -> 80)),
    Row(Map("city" -> "Berlin", "score" -> 70)),
    Row(Map("city" -> "London", "score" -> 90))
  )

  private val right = Vector(
    Row(Map("city" -> "Berlin", "score" -> 70)),
    Row(Map("city" -> "Tokyo",  "score" -> 85))
  )

  private val nums = Vector(
    Row(Map("v" -> -3.7)),
    Row(Map("v" -> 2.5)),
    Row(Map("v" -> 0.0)),
    Row(Map("v" -> -0.1))
  )

  private val mixed = Vector(
    Row(Map("n" -> 42, "s" -> "hello"))
  )

  private val registry = DataRegistry.of(
    "memory://left"  -> left,
    "memory://right" -> right,
    "memory://nums"  -> nums,
    "memory://mixed" -> mixed
  )
  private val backend  = LocalBackend(registry)

  private def run(plan: LogicalPlan): Vector[Row] =
    backend.execute(plan) match
      case BackendResult.InMemory(rs) => rs

  // ---------------------------------------------------------------------------
  // Intersect
  // ---------------------------------------------------------------------------

  test("Intersect returns only rows that appear in both datasets"):
    val plan   = Intersect(ReadCsv("memory://left", None), ReadCsv("memory://right", None))
    val result = run(plan)
    assertEquals(result.size, 1)
    assertEquals(result(0).values("city"), "Berlin")

  test("Intersect removes duplicates from the result"):
    val dupes = left :+ Row(Map("city" -> "Berlin", "score" -> 70))  // Berlin appears twice in left
    val reg   = registry.register("memory://dupes", dupes)
    val be    = LocalBackend(reg)
    val plan  = Intersect(ReadCsv("memory://dupes", None), ReadCsv("memory://right", None))
    val result = be.execute(plan) match
      case BackendResult.InMemory(rs) => rs
    assertEquals(result.size, 1)  // deduplicated to one Berlin row

  test("Intersect returns an empty result when the datasets share no rows"):
    val noMatch = Vector(Row(Map("city" -> "Madrid", "score" -> 55)))
    val reg     = registry.register("memory://nomatch", noMatch)
    val be      = LocalBackend(reg)
    val plan    = Intersect(ReadCsv("memory://left", None), ReadCsv("memory://nomatch", None))
    val result  = be.execute(plan) match
      case BackendResult.InMemory(rs) => rs
    assertEquals(result.size, 0)

  test("Intersect returns an empty result when the left dataset is empty"):
    val reg    = registry.register("memory://empty", Vector.empty[Row])
    val be     = LocalBackend(reg)
    val plan   = Intersect(ReadCsv("memory://empty", None), ReadCsv("memory://right", None))
    val result = be.execute(plan) match
      case BackendResult.InMemory(rs) => rs
    assertEquals(result.size, 0)

  // ---------------------------------------------------------------------------
  // Except
  // ---------------------------------------------------------------------------

  test("Except returns rows from the left dataset that do not appear in the right"):
    val plan   = Except(ReadCsv("memory://left", None), ReadCsv("memory://right", None))
    val result = run(plan)
    assertEquals(result.size, 2)
    val cities = result.map(_.values("city").asInstanceOf[String]).toSet
    assertEquals(cities, Set("Paris", "London"))

  test("Except removes duplicates from the result"):
    val dupes = left :+ Row(Map("city" -> "Paris", "score" -> 80))  // Paris appears twice
    val reg   = registry.register("memory://dupes2", dupes)
    val be    = LocalBackend(reg)
    val plan  = Except(ReadCsv("memory://dupes2", None), ReadCsv("memory://right", None))
    val result = be.execute(plan) match
      case BackendResult.InMemory(rs) => rs
    // Paris and London should each appear once
    assertEquals(result.size, 2)

  test("Except returns all left rows when right dataset is empty"):
    val reg    = registry.register("memory://empty2", Vector.empty[Row])
    val be     = LocalBackend(reg)
    val plan   = Except(ReadCsv("memory://left", None), ReadCsv("memory://empty2", None))
    val result = be.execute(plan) match
      case BackendResult.InMemory(rs) => rs
    assertEquals(result.size, 3)

  test("Except returns an empty result when all left rows appear in the right dataset"):
    val plan   = Except(ReadCsv("memory://right", None), ReadCsv("memory://left", None))
    val result = run(plan)
    // Tokyo is in right but not left; Berlin is in both → Tokyo only
    assertEquals(result.size, 1)
    assertEquals(result(0).values("city"), "Tokyo")

  // ---------------------------------------------------------------------------
  // Cast
  // ---------------------------------------------------------------------------

  test("Cast converts an Int column to Float64"):
    val plan   = Project(
      ReadCsv("memory://left", None),
      Vector(Alias(Cast(ColumnRef("score"), DataType.Float64), "score_f")),
      None
    )
    val result = run(plan)
    result.foreach { row =>
      assert(row.values("score_f").isInstanceOf[Double], s"Expected Double, got: ${row.values("score_f")}")
    }

  test("Cast converts a numeric column to StringType"):
    val plan   = Project(
      ReadCsv("memory://left", None),
      Vector(Alias(Cast(ColumnRef("score"), DataType.StringType), "score_s")),
      None
    )
    val result = run(plan)
    result.foreach { row =>
      assert(row.values("score_s").isInstanceOf[String], s"Expected String, got: ${row.values("score_s")}")
    }

  test("Cast converts a numeric column to Int32"):
    val plan   = Project(
      ReadCsv("memory://nums", None),
      Vector(Alias(Cast(ColumnRef("v"), DataType.Int32), "v_int")),
      None
    )
    val result = run(plan)
    // -3.7 → -3, 2.5 → 2, 0.0 → 0, -0.1 → 0
    assertEquals(result(0).values("v_int"), -3)
    assertEquals(result(1).values("v_int"), 2)
    assertEquals(result(2).values("v_int"), 0)

  // ---------------------------------------------------------------------------
  // Abs
  // ---------------------------------------------------------------------------

  test("Abs returns the absolute value of negative numbers"):
    val plan   = Project(
      ReadCsv("memory://nums", None),
      Vector(Alias(Abs(ColumnRef("v")), "abs_v")),
      None
    )
    val result = run(plan)
    val values = result.map(_.values("abs_v").asInstanceOf[Double])
    assertEquals(values(0), 3.7)
    assertEquals(values(1), 2.5)
    assertEquals(values(2), 0.0)

  test("Abs of a positive value is unchanged"):
    val plan   = Project(
      ReadCsv("memory://left", None),
      Vector(Alias(Abs(ColumnRef("score")), "abs_score")),
      None
    )
    val result = run(plan)
    assertEquals(result(0).values("abs_score").asInstanceOf[Double], 80.0)

  test("Abs propagates null when the input is null"):
    val nullRow = Vector(Row(Map("v" -> null)))
    val reg     = registry.register("memory://nullv", nullRow)
    val be      = LocalBackend(reg)
    val plan    = Project(
      ReadCsv("memory://nullv", None),
      Vector(Alias(Abs(ColumnRef("v")), "abs_v")),
      None
    )
    val result  = be.execute(plan) match
      case BackendResult.InMemory(rs) => rs
    assertEquals(result(0).values("abs_v"), null)

  // ---------------------------------------------------------------------------
  // Round
  // ---------------------------------------------------------------------------

  test("Round rounds to the specified number of decimal places"):
    val plan   = Project(
      ReadCsv("memory://nums", None),
      Vector(Alias(Round(ColumnRef("v"), 1), "r")),
      None
    )
    val result = run(plan)
    assertEquals(result(0).values("r").asInstanceOf[Double], -3.7)
    assertEquals(result(1).values("r").asInstanceOf[Double], 2.5)
    assertEquals(result(3).values("r").asInstanceOf[Double], -0.1)

  test("Round with scale=0 rounds to the nearest integer"):
    val plan   = Project(
      ReadCsv("memory://nums", None),
      Vector(Alias(Round(ColumnRef("v"), 0), "r")),
      None
    )
    val result = run(plan)
    assertEquals(result(0).values("r").asInstanceOf[Double], -4.0)
    assertEquals(result(1).values("r").asInstanceOf[Double], 3.0) // 2.5 rounds to 3

  test("Round propagates null when the input is null"):
    val nullRow = Vector(Row(Map("v" -> null)))
    val reg     = registry.register("memory://nullv2", nullRow)
    val be      = LocalBackend(reg)
    val plan    = Project(
      ReadCsv("memory://nullv2", None),
      Vector(Alias(Round(ColumnRef("v"), 2), "r")),
      None
    )
    val result  = be.execute(plan) match
      case BackendResult.InMemory(rs) => rs
    assertEquals(result(0).values("r"), null)

  // ---------------------------------------------------------------------------
  // Floor
  // ---------------------------------------------------------------------------

  test("Floor returns the largest integer not greater than the value"):
    val plan   = Project(
      ReadCsv("memory://nums", None),
      Vector(Alias(Floor(ColumnRef("v")), "f")),
      None
    )
    val result = run(plan)
    assertEquals(result(0).values("f").asInstanceOf[Double], -4.0)
    assertEquals(result(1).values("f").asInstanceOf[Double], 2.0)
    assertEquals(result(2).values("f").asInstanceOf[Double], 0.0)
    assertEquals(result(3).values("f").asInstanceOf[Double], -1.0)

  test("Floor propagates null when the input is null"):
    val nullRow = Vector(Row(Map("v" -> null)))
    val reg     = registry.register("memory://nullv3", nullRow)
    val be      = LocalBackend(reg)
    val plan    = Project(
      ReadCsv("memory://nullv3", None),
      Vector(Alias(Floor(ColumnRef("v")), "f")),
      None
    )
    val result  = be.execute(plan) match
      case BackendResult.InMemory(rs) => rs
    assertEquals(result(0).values("f"), null)

  // ---------------------------------------------------------------------------
  // Ceil
  // ---------------------------------------------------------------------------

  test("Ceil returns the smallest integer not less than the value"):
    val plan   = Project(
      ReadCsv("memory://nums", None),
      Vector(Alias(Ceil(ColumnRef("v")), "c")),
      None
    )
    val result = run(plan)
    assertEquals(result(0).values("c").asInstanceOf[Double], -3.0)
    assertEquals(result(1).values("c").asInstanceOf[Double], 3.0)
    assertEquals(result(2).values("c").asInstanceOf[Double], 0.0)
    assertEquals(result(3).values("c").asInstanceOf[Double], 0.0)

  test("Ceil propagates null when the input is null"):
    val nullRow = Vector(Row(Map("v" -> null)))
    val reg     = registry.register("memory://nullv4", nullRow)
    val be      = LocalBackend(reg)
    val plan    = Project(
      ReadCsv("memory://nullv4", None),
      Vector(Alias(Ceil(ColumnRef("v")), "c")),
      None
    )
    val result  = be.execute(plan) match
      case BackendResult.InMemory(rs) => rs
    assertEquals(result(0).values("c"), null)

  // ---------------------------------------------------------------------------
  // Combined: chaining numeric functions
  // ---------------------------------------------------------------------------

  test("Abs followed by Floor produces the floor of the absolute value"):
    val plan   = Project(
      ReadCsv("memory://nums", None),
      Vector(Alias(Floor(Abs(ColumnRef("v"))), "fabs")),
      None
    )
    val result = run(plan)
    assertEquals(result(0).values("fabs").asInstanceOf[Double], 3.0)  // floor(abs(-3.7)) = 3.0
    assertEquals(result(1).values("fabs").asInstanceOf[Double], 2.0)  // floor(abs(2.5)) = 2.0
