package lumina.backend.duckdb

import munit.FunSuite
import lumina.plan.*
import lumina.plan.Expression.*
import lumina.plan.backend.*
import java.time.LocalDate

/**
 * Tests for M16 date/time functions in DuckDBBackend: ToDate, component
 * extraction, DateAdd, DateDiff, and DateFormat.
 *
 * Also verifies PlanToSql generates syntactically correct SQL for each
 * date expression.  RowNormalizer date handling is covered implicitly
 * (DuckDB returns java.sql.Date; we assert LocalDate equality).
 */
class DuckDBDateSpec extends FunSuite:

  private val rows = Vector(
    Row(Map("id" -> 1, "event_date" -> "2024-03-15")),
    Row(Map("id" -> 2, "event_date" -> "2023-12-01")),
    Row(Map("id" -> 3, "event_date" -> "2024-01-31"))
  )

  private val backend = DuckDBBackend(DataRegistry.of("memory://t" -> rows))
  private val src     = ReadCsv("memory://t", None)

  private def run(plan: LogicalPlan): Vector[Row] =
    backend.execute(plan) match
      case BackendResult.InMemory(rs) => rs

  // ---------------------------------------------------------------------------
  // ToDate (CAST AS DATE) — RowNormalizer converts java.sql.Date → LocalDate
  // ---------------------------------------------------------------------------

  test("ToDate parses an ISO date string and returns a LocalDate value"):
    val plan   = Project(src, Vector(Alias(ToDate(ColumnRef("event_date")), "d")), None)
    val result = run(plan)
    assertEquals(result(0).values("d"), LocalDate.of(2024, 3, 15))
    assertEquals(result(1).values("d"), LocalDate.of(2023, 12, 1))

  test("PlanToSql generates CAST(... AS DATE) for ToDate"):
    val sql = PlanToSql.toSql(Project(src, Vector(Alias(ToDate(ColumnRef("event_date")), "d")), None))
    assert(sql.contains("CAST"), s"Expected CAST in: $sql")
    assert(sql.contains("DATE"), s"Expected DATE in: $sql")

  // ---------------------------------------------------------------------------
  // Year / Month / Day component extraction
  // ---------------------------------------------------------------------------

  test("Year extracts the 4-digit year from a date column"):
    val plan   = Project(src,
      Vector(Alias(Year(ToDate(ColumnRef("event_date"))), "yr")), None)
    val result = run(plan)
    assertEquals(result(0).values("yr").toString.toInt, 2024)
    assertEquals(result(1).values("yr").toString.toInt, 2023)

  test("Month extracts the month number from a date column"):
    val plan   = Project(src,
      Vector(Alias(Month(ToDate(ColumnRef("event_date"))), "mo")), None)
    val result = run(plan)
    assertEquals(result(0).values("mo").toString.toInt, 3)   // March
    assertEquals(result(1).values("mo").toString.toInt, 12)  // December

  test("Day extracts the day of the month from a date column"):
    val plan   = Project(src,
      Vector(Alias(Day(ToDate(ColumnRef("event_date"))), "dy")), None)
    val result = run(plan)
    assertEquals(result(0).values("dy").toString.toInt, 15)
    assertEquals(result(2).values("dy").toString.toInt, 31)

  test("PlanToSql generates YEAR MONTH DAY SQL for component extractions"):
    val plan = Project(src,
      Vector(
        Alias(Year(ToDate(ColumnRef("event_date"))),  "yr"),
        Alias(Month(ToDate(ColumnRef("event_date"))), "mo"),
        Alias(Day(ToDate(ColumnRef("event_date"))),   "dy")
      ), None)
    val sql = PlanToSql.toSql(plan)
    assert(sql.contains("YEAR"),  s"Expected YEAR in: $sql")
    assert(sql.contains("MONTH"), s"Expected MONTH in: $sql")
    assert(sql.contains("DAY"),   s"Expected DAY in: $sql")

  // ---------------------------------------------------------------------------
  // DayOfWeek — ISODOW: 1=Mon, 7=Sun
  // ---------------------------------------------------------------------------

  test("DayOfWeek returns the ISO day number for a known date"):
    // 2024-03-15 is Friday (5)
    val plan   = Project(src,
      Vector(Alias(DayOfWeek(ToDate(ColumnRef("event_date"))), "dow")), None)
    val result = run(plan)
    assertEquals(result(0).values("dow").toString.toInt, 5)

  test("PlanToSql generates ISODOW for DayOfWeek"):
    val sql = PlanToSql.toSql(
      Project(src, Vector(Alias(DayOfWeek(ToDate(ColumnRef("event_date"))), "dow")), None))
    assert(sql.contains("ISODOW"), s"Expected ISODOW in: $sql")

  // ---------------------------------------------------------------------------
  // DateAdd
  // ---------------------------------------------------------------------------

  test("DateAdd adds the specified number of days to a date"):
    val plan   = Project(src,
      Vector(Alias(DateAdd(ToDate(ColumnRef("event_date")), Literal(10)), "later")), None)
    val result = run(plan)
    assertEquals(result(0).values("later"), LocalDate.of(2024, 3, 25))

  test("DateAdd with a negative offset subtracts days"):
    val plan   = Project(src,
      Vector(Alias(DateAdd(ToDate(ColumnRef("event_date")), Literal(-5)), "earlier")), None)
    val result = run(plan)
    assertEquals(result(0).values("earlier"), LocalDate.of(2024, 3, 10))

  test("PlanToSql generates integer addition for DateAdd"):
    val sql = PlanToSql.toSql(
      Project(src, Vector(Alias(DateAdd(ToDate(ColumnRef("event_date")), Literal(7)), "d")), None))
    assert(sql.contains("CAST"), s"Expected CAST in DateAdd SQL: $sql")

  // ---------------------------------------------------------------------------
  // DateDiff
  // ---------------------------------------------------------------------------

  test("DateDiff returns the number of days between two date columns"):
    val twoDateRows = Vector(
      Row(Map("start" -> "2024-01-01", "end" -> "2024-01-11"))
    )
    val be   = DuckDBBackend(DataRegistry.of("memory://dd" -> twoDateRows))
    val plan = Project(ReadCsv("memory://dd", None),
      Vector(Alias(
        DateDiff(ToDate(ColumnRef("end")), ToDate(ColumnRef("start"))), "diff"
      )), None)
    val result = be.execute(plan) match
      case BackendResult.InMemory(rs) => rs
    assertEquals(result(0).values("diff").toString.toLong, 10L)

  test("PlanToSql generates DATEDIFF SQL for DateDiff"):
    val sql = PlanToSql.toSql(
      Project(src,
        Vector(Alias(DateDiff(ToDate(ColumnRef("event_date")), ToDate(ColumnRef("event_date"))), "d")),
        None))
    assert(sql.contains("DATEDIFF"), s"Expected DATEDIFF in: $sql")

  // ---------------------------------------------------------------------------
  // DateFormat
  // ---------------------------------------------------------------------------

  test("DateFormat formats a date using a Java pattern"):
    val dateRows = Vector(Row(Map("d" -> LocalDate.of(2024, 7, 4))))
    val be       = DuckDBBackend(DataRegistry.of("memory://fmt" -> dateRows))
    val plan     = Project(ReadCsv("memory://fmt", None),
      Vector(Alias(DateFormat(ColumnRef("d"), "dd/MM/yyyy"), "formatted")), None)
    val result   = be.execute(plan) match
      case BackendResult.InMemory(rs) => rs
    assertEquals(result(0).values("formatted"), "04/07/2024")

  test("PlanToSql converts Java date pattern to STRFTIME format"):
    val sql = PlanToSql.toSql(
      Project(src, Vector(Alias(DateFormat(ToDate(ColumnRef("event_date")), "yyyy-MM-dd"), "f")), None))
    assert(sql.contains("STRFTIME"), s"Expected STRFTIME in: $sql")
    assert(sql.contains("%Y-%m-%d"), s"Expected converted pattern in: $sql")

  // ---------------------------------------------------------------------------
  // Filter on date expressions
  // ---------------------------------------------------------------------------

  test("Filter using Year returns only rows from the specified year"):
    val plan   = Filter(src, EqualTo(Year(ToDate(ColumnRef("event_date"))), Literal(2024)))
    val result = run(plan)
    assertEquals(result.size, 2)

  test("Filter using Month returns only rows from the specified month"):
    val plan   = Filter(src, EqualTo(Month(ToDate(ColumnRef("event_date"))), Literal(3)))
    val result = run(plan)
    assertEquals(result.size, 1)
