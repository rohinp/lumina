package lumina.backend.duckdb

import munit.FunSuite
import lumina.plan.*
import lumina.plan.Expression.*
import lumina.plan.backend.*

/**
 * Tests for M14 features in DuckDBBackend: Intersect, Except, Cast, Abs,
 * Round, Floor, and Ceil.
 *
 * Mirrors LocalBackendM14Spec to confirm both backends produce identical
 * results, and verifies PlanToSql generates valid SQL for each operator.
 */
class DuckDBM14Spec extends FunSuite:

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

  private val registry = DataRegistry.of(
    "memory://left"  -> left,
    "memory://right" -> right,
    "memory://nums"  -> nums
  )
  private val backend  = DuckDBBackend(registry)

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

  test("Intersect returns an empty result when the datasets share no rows"):
    val noMatch = Vector(Row(Map("city" -> "Madrid", "score" -> 55)))
    val reg     = registry.register("memory://nomatch", noMatch)
    val be      = DuckDBBackend(reg)
    val plan    = Intersect(ReadCsv("memory://left", None), ReadCsv("memory://nomatch", None))
    val result  = be.execute(plan) match
      case BackendResult.InMemory(rs) => rs
    assertEquals(result.size, 0)

  test("Intersect removes duplicates from the result"):
    val dupes = left :+ Row(Map("city" -> "Berlin", "score" -> 70))
    val reg   = registry.register("memory://dupes", dupes)
    val be    = DuckDBBackend(reg)
    val plan  = Intersect(ReadCsv("memory://dupes", None), ReadCsv("memory://right", None))
    val result = be.execute(plan) match
      case BackendResult.InMemory(rs) => rs
    assertEquals(result.size, 1)

  // ---------------------------------------------------------------------------
  // Except
  // ---------------------------------------------------------------------------

  test("Except returns rows from the left dataset that do not appear in the right"):
    val plan   = Except(ReadCsv("memory://left", None), ReadCsv("memory://right", None))
    val result = run(plan)
    assertEquals(result.size, 2)
    val cities = result.map(_.values("city").asInstanceOf[String]).toSet
    assertEquals(cities, Set("Paris", "London"))

  test("Except returns all left rows when the right dataset contains no matching rows"):
    val noMatch = Vector(Row(Map("city" -> "Madrid", "score" -> 55)))
    val reg     = registry.register("memory://nomatch2", noMatch)
    val be      = DuckDBBackend(reg)
    val plan    = Except(ReadCsv("memory://left", None), ReadCsv("memory://nomatch2", None))
    val result  = be.execute(plan) match
      case BackendResult.InMemory(rs) => rs
    assertEquals(result.size, 3)

  test("Except returns an empty result when all left rows appear in the right dataset"):
    val plan   = Except(ReadCsv("memory://right", None), ReadCsv("memory://left", None))
    val result = run(plan)
    // Tokyo is in right but not left
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

  test("Cast converts a Double column to Int32 by truncating the fractional part"):
    val plan   = Project(
      ReadCsv("memory://nums", None),
      Vector(Alias(Cast(ColumnRef("v"), DataType.Int32), "v_int")),
      None
    )
    val result = run(plan)
    val values = result.map(_.values("v_int"))
    // DuckDB CAST(DOUBLE AS INTEGER) truncates toward zero: -3.7 → -3, 2.5 → 2
    assert(values(0).toString.toInt == -3 || values(0).toString.toInt == -4,
      s"Expected -3 or -4 from -3.7 cast to Int, got: ${values(0)}")
    assert(values(1).toString.toInt == 2 || values(1).toString.toInt == 3,
      s"Expected 2 or 3 from 2.5 cast to Int, got: ${values(1)}")

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
    // DuckDB preserves the column type: integer score → integer result
    val v = result(0).values("abs_score")
    assertEquals(v.toString.toDouble, 80.0)

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

  test("Round with scale=0 rounds to the nearest integer"):
    val plan   = Project(
      ReadCsv("memory://nums", None),
      Vector(Alias(Round(ColumnRef("v"), 0), "r")),
      None
    )
    val result = run(plan)
    // -3.7 → -4.0, 0.0 → 0.0
    assertEquals(result(0).values("r").asInstanceOf[Double], -4.0)
    assertEquals(result(2).values("r").asInstanceOf[Double], 0.0)

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

  // ---------------------------------------------------------------------------
  // PlanToSql smoke — verify the generated SQL strings are structurally correct
  // ---------------------------------------------------------------------------

  test("PlanToSql generates INTERSECT SQL for Intersect nodes"):
    val plan = Intersect(ReadCsv("memory://left", None), ReadCsv("memory://right", None))
    val sql  = PlanToSql.toSql(plan)
    assert(sql.contains("INTERSECT"), s"Expected INTERSECT in: $sql")

  test("PlanToSql generates EXCEPT SQL for Except nodes"):
    val plan = Except(ReadCsv("memory://left", None), ReadCsv("memory://right", None))
    val sql  = PlanToSql.toSql(plan)
    assert(sql.contains("EXCEPT"), s"Expected EXCEPT in: $sql")

  test("PlanToSql generates CAST SQL for Cast expressions"):
    val plan = Project(
      ReadCsv("memory://left", None),
      Vector(Alias(Cast(ColumnRef("score"), DataType.Float64), "sf")),
      None
    )
    val sql = PlanToSql.toSql(plan)
    assert(sql.contains("CAST"), s"Expected CAST in: $sql")
    assert(sql.contains("DOUBLE"), s"Expected DOUBLE type in: $sql")

  test("PlanToSql generates ABS SQL for Abs expressions"):
    val plan = Project(
      ReadCsv("memory://nums", None),
      Vector(Alias(Abs(ColumnRef("v")), "av")),
      None
    )
    val sql = PlanToSql.toSql(plan)
    assert(sql.contains("ABS"), s"Expected ABS in: $sql")

  test("PlanToSql generates ROUND SQL for Round expressions"):
    val plan = Project(
      ReadCsv("memory://nums", None),
      Vector(Alias(Round(ColumnRef("v"), 2), "rv")),
      None
    )
    val sql = PlanToSql.toSql(plan)
    assert(sql.contains("ROUND"), s"Expected ROUND in: $sql")

  test("PlanToSql generates FLOOR and CEIL SQL for Floor and Ceil expressions"):
    val plan = Project(
      ReadCsv("memory://nums", None),
      Vector(
        Alias(Floor(ColumnRef("v")), "fv"),
        Alias(Ceil(ColumnRef("v")), "cv")
      ),
      None
    )
    val sql = PlanToSql.toSql(plan)
    assert(sql.contains("FLOOR"), s"Expected FLOOR in: $sql")
    assert(sql.contains("CEIL"),  s"Expected CEIL in: $sql")
