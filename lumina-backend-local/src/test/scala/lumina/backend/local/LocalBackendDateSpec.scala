package lumina.backend.local

import munit.FunSuite
import lumina.plan.*
import lumina.plan.Expression.*
import lumina.plan.backend.*
import java.time.{LocalDate, LocalDateTime}

/**
 * Tests for M16 date/time functions in LocalBackend: ToDate, ToTimestamp,
 * component extraction (Year, Month, Day, Hour, Minute, Second, DayOfWeek),
 * arithmetic (DateAdd, DateDiff), and DateFormat.
 *
 * Read top-to-bottom as a specification for each operation's behaviour.
 */
class LocalBackendDateSpec extends FunSuite:

  private val rows = Vector(
    Row(Map("id" -> 1, "event_date" -> "2024-03-15", "event_ts" -> "2024-03-15 14:30:45")),
    Row(Map("id" -> 2, "event_date" -> "2023-12-01", "event_ts" -> "2023-12-01 09:00:00")),
    Row(Map("id" -> 3, "event_date" -> "2024-01-31", "event_ts" -> "2024-01-31 23:59:59"))
  )

  private val backend = LocalBackend(DataRegistry.of("memory://t" -> rows))
  private val src     = ReadCsv("memory://t", None)

  private def run(plan: LogicalPlan): Vector[Row] =
    backend.execute(plan) match
      case BackendResult.InMemory(rs) => rs

  // ---------------------------------------------------------------------------
  // ToDate
  // ---------------------------------------------------------------------------

  test("ToDate parses an ISO date string to a LocalDate value"):
    val plan   = Project(src, Vector(Alias(ToDate(ColumnRef("event_date")), "d")), None)
    val result = run(plan)
    assertEquals(result(0).values("d"), LocalDate.of(2024, 3, 15))
    assertEquals(result(1).values("d"), LocalDate.of(2023, 12, 1))

  test("ToDate is idempotent when the value is already a LocalDate"):
    val withDate = Vector(Row(Map("d" -> LocalDate.of(2024, 6, 1))))
    val be       = LocalBackend(DataRegistry.of("memory://d" -> withDate))
    val plan     = Project(ReadCsv("memory://d", None), Vector(Alias(ToDate(ColumnRef("d")), "out")), None)
    val result   = be.execute(plan) match
      case BackendResult.InMemory(rs) => rs
    assertEquals(result(0).values("out"), LocalDate.of(2024, 6, 1))

  test("ToDate propagates null"):
    val nullRow = Vector(Row(Map("d" -> null)))
    val be      = LocalBackend(DataRegistry.of("memory://n" -> nullRow))
    val plan    = Project(ReadCsv("memory://n", None), Vector(Alias(ToDate(ColumnRef("d")), "out")), None)
    val result  = be.execute(plan) match
      case BackendResult.InMemory(rs) => rs
    assertEquals(result(0).values("out"), null)

  // ---------------------------------------------------------------------------
  // ToTimestamp
  // ---------------------------------------------------------------------------

  test("ToTimestamp parses an ISO datetime string to a LocalDateTime value"):
    val plan   = Project(src, Vector(Alias(ToTimestamp(ColumnRef("event_ts")), "ts")), None)
    val result = run(plan)
    assertEquals(result(0).values("ts"), LocalDateTime.of(2024, 3, 15, 14, 30, 45))

  test("ToTimestamp converts a LocalDate to midnight LocalDateTime"):
    val plan   = Project(src,
      Vector(Alias(ToTimestamp(ToDate(ColumnRef("event_date"))), "ts")), None)
    val result = run(plan)
    assertEquals(result(0).values("ts"), LocalDateTime.of(2024, 3, 15, 0, 0, 0))

  // ---------------------------------------------------------------------------
  // Year / Month / Day
  // ---------------------------------------------------------------------------

  test("Year extracts the 4-digit year from a date string"):
    val plan   = Project(src,
      Vector(Alias(Year(ToDate(ColumnRef("event_date"))), "yr")), None)
    val result = run(plan)
    assertEquals(result(0).values("yr"), 2024)
    assertEquals(result(1).values("yr"), 2023)

  test("Month extracts the month number (1–12) from a date"):
    val plan   = Project(src,
      Vector(Alias(Month(ToDate(ColumnRef("event_date"))), "mo")), None)
    val result = run(plan)
    assertEquals(result(0).values("mo"), 3)   // March
    assertEquals(result(1).values("mo"), 12)  // December

  test("Day extracts the day of the month from a date"):
    val plan   = Project(src,
      Vector(Alias(Day(ToDate(ColumnRef("event_date"))), "dy")), None)
    val result = run(plan)
    assertEquals(result(0).values("dy"), 15)
    assertEquals(result(2).values("dy"), 31)

  test("Year Month Day work directly on LocalDate values stored in rows"):
    val dateRows = Vector(Row(Map("d" -> LocalDate.of(2025, 7, 4))))
    val be       = LocalBackend(DataRegistry.of("memory://dr" -> dateRows))
    val src2     = ReadCsv("memory://dr", None)
    val plan     = Project(src2,
      Vector(
        Alias(Year(ColumnRef("d")),  "yr"),
        Alias(Month(ColumnRef("d")), "mo"),
        Alias(Day(ColumnRef("d")),   "dy")
      ), None)
    val result = be.execute(plan) match
      case BackendResult.InMemory(rs) => rs
    assertEquals(result(0).values("yr"), 2025)
    assertEquals(result(0).values("mo"), 7)
    assertEquals(result(0).values("dy"), 4)

  // ---------------------------------------------------------------------------
  // Hour / Minute / Second
  // ---------------------------------------------------------------------------

  test("Hour extracts the hour from a timestamp"):
    val plan   = Project(src,
      Vector(Alias(Hour(ToTimestamp(ColumnRef("event_ts"))), "hr")), None)
    val result = run(plan)
    assertEquals(result(0).values("hr"), 14)
    assertEquals(result(1).values("hr"), 9)

  test("Minute extracts the minute from a timestamp"):
    val plan   = Project(src,
      Vector(Alias(Minute(ToTimestamp(ColumnRef("event_ts"))), "mn")), None)
    val result = run(plan)
    assertEquals(result(0).values("mn"), 30)

  test("Second extracts the second from a timestamp"):
    val plan   = Project(src,
      Vector(Alias(Second(ToTimestamp(ColumnRef("event_ts"))), "sc")), None)
    val result = run(plan)
    assertEquals(result(0).values("sc"), 45)
    assertEquals(result(2).values("sc"), 59)

  // ---------------------------------------------------------------------------
  // DayOfWeek
  // ---------------------------------------------------------------------------

  test("DayOfWeek returns the ISO day number where 1=Monday and 7=Sunday"):
    // 2024-03-15 is a Friday (5)
    val plan   = Project(src,
      Vector(Alias(DayOfWeek(ToDate(ColumnRef("event_date"))), "dow")), None)
    val result = run(plan)
    assertEquals(result(0).values("dow"), 5)  // Friday

  test("DayOfWeek of a known Sunday returns 7"):
    val sunRow = Vector(Row(Map("d" -> LocalDate.of(2024, 3, 17))))  // Sunday
    val be     = LocalBackend(DataRegistry.of("memory://sun" -> sunRow))
    val plan   = Project(ReadCsv("memory://sun", None),
      Vector(Alias(DayOfWeek(ColumnRef("d")), "dow")), None)
    val result = be.execute(plan) match
      case BackendResult.InMemory(rs) => rs
    assertEquals(result(0).values("dow"), 7)

  test("DayOfWeek of a known Monday returns 1"):
    val monRow = Vector(Row(Map("d" -> LocalDate.of(2024, 3, 18))))  // Monday
    val be     = LocalBackend(DataRegistry.of("memory://mon" -> monRow))
    val plan   = Project(ReadCsv("memory://mon", None),
      Vector(Alias(DayOfWeek(ColumnRef("d")), "dow")), None)
    val result = be.execute(plan) match
      case BackendResult.InMemory(rs) => rs
    assertEquals(result(0).values("dow"), 1)

  // ---------------------------------------------------------------------------
  // DateAdd
  // ---------------------------------------------------------------------------

  test("DateAdd adds a positive number of days to a date"):
    val plan   = Project(src,
      Vector(Alias(DateAdd(ToDate(ColumnRef("event_date")), Literal(10)), "later")), None)
    val result = run(plan)
    assertEquals(result(0).values("later"), LocalDate.of(2024, 3, 25))

  test("DateAdd with 0 days returns the same date"):
    val plan   = Project(src,
      Vector(Alias(DateAdd(ToDate(ColumnRef("event_date")), Literal(0)), "same")), None)
    val result = run(plan)
    assertEquals(result(0).values("same"), LocalDate.of(2024, 3, 15))

  test("DateAdd with a negative number of days subtracts days"):
    val plan   = Project(src,
      Vector(Alias(DateAdd(ToDate(ColumnRef("event_date")), Literal(-5)), "earlier")), None)
    val result = run(plan)
    assertEquals(result(0).values("earlier"), LocalDate.of(2024, 3, 10))

  test("DateAdd wraps correctly across month boundaries"):
    val plan   = Project(src,
      Vector(Alias(DateAdd(ToDate(ColumnRef("event_date")), Literal(17)), "next")), None)
    // 2024-03-15 + 17 = 2024-04-01
    val result = run(plan)
    assertEquals(result(0).values("next"), LocalDate.of(2024, 4, 1))

  // ---------------------------------------------------------------------------
  // DateDiff
  // ---------------------------------------------------------------------------

  test("DateDiff returns the number of days between two dates (end - start)"):
    val dateRows = Vector(
      Row(Map("start" -> LocalDate.of(2024, 1, 1), "end" -> LocalDate.of(2024, 1, 11)))
    )
    val be   = LocalBackend(DataRegistry.of("memory://dd" -> dateRows))
    val plan = Project(ReadCsv("memory://dd", None),
      Vector(Alias(DateDiff(ColumnRef("end"), ColumnRef("start")), "diff")), None)
    val result = be.execute(plan) match
      case BackendResult.InMemory(rs) => rs
    assertEquals(result(0).values("diff"), 10L)

  test("DateDiff returns a negative value when end is before start"):
    val dateRows = Vector(
      Row(Map("start" -> LocalDate.of(2024, 3, 15), "end" -> LocalDate.of(2024, 3, 10)))
    )
    val be   = LocalBackend(DataRegistry.of("memory://dd2" -> dateRows))
    val plan = Project(ReadCsv("memory://dd2", None),
      Vector(Alias(DateDiff(ColumnRef("end"), ColumnRef("start")), "diff")), None)
    val result = be.execute(plan) match
      case BackendResult.InMemory(rs) => rs
    assertEquals(result(0).values("diff"), -5L)

  test("DateDiff returns 0 when both dates are equal"):
    val dateRows = Vector(
      Row(Map("d" -> LocalDate.of(2024, 6, 1)))
    )
    val be   = LocalBackend(DataRegistry.of("memory://dd3" -> dateRows))
    val plan = Project(ReadCsv("memory://dd3", None),
      Vector(Alias(DateDiff(ColumnRef("d"), ColumnRef("d")), "diff")), None)
    val result = be.execute(plan) match
      case BackendResult.InMemory(rs) => rs
    assertEquals(result(0).values("diff"), 0L)

  // ---------------------------------------------------------------------------
  // DateFormat
  // ---------------------------------------------------------------------------

  test("DateFormat formats a LocalDate using a Java DateTimeFormatter pattern"):
    val dateRows = Vector(Row(Map("d" -> LocalDate.of(2024, 7, 4))))
    val be       = LocalBackend(DataRegistry.of("memory://fmt" -> dateRows))
    val plan     = Project(ReadCsv("memory://fmt", None),
      Vector(Alias(DateFormat(ColumnRef("d"), "dd/MM/yyyy"), "formatted")), None)
    val result   = be.execute(plan) match
      case BackendResult.InMemory(rs) => rs
    assertEquals(result(0).values("formatted"), "04/07/2024")

  test("DateFormat with ISO pattern produces the standard date string"):
    val dateRows = Vector(Row(Map("d" -> LocalDate.of(2024, 1, 31))))
    val be       = LocalBackend(DataRegistry.of("memory://fmt2" -> dateRows))
    val plan     = Project(ReadCsv("memory://fmt2", None),
      Vector(Alias(DateFormat(ColumnRef("d"), "yyyy-MM-dd"), "formatted")), None)
    val result   = be.execute(plan) match
      case BackendResult.InMemory(rs) => rs
    assertEquals(result(0).values("formatted"), "2024-01-31")

  // ---------------------------------------------------------------------------
  // Filter on date expressions
  // ---------------------------------------------------------------------------

  test("Filter using Year extracts only rows from a given year"):
    val plan   = Filter(src, EqualTo(Year(ToDate(ColumnRef("event_date"))), Literal(2024)))
    val result = run(plan)
    assertEquals(result.size, 2)  // rows 1 and 3 are 2024

  test("Filter using Month extracts only rows from a specific month"):
    val plan   = Filter(src, EqualTo(Month(ToDate(ColumnRef("event_date"))), Literal(3)))
    val result = run(plan)
    assertEquals(result.size, 1)  // only 2024-03-15

  // ---------------------------------------------------------------------------
  // GroupBy on date component
  // ---------------------------------------------------------------------------

  test("GroupBy Year aggregates rows by calendar year"):
    val plan   = Aggregate(src,
      Vector(Year(ToDate(ColumnRef("event_date")))),
      Vector(Aggregation.Count(None, Some("cnt"))),
      None)
    val result = run(plan)
    val totals = result.map(_.values("cnt").asInstanceOf[Long]).toSet
    assertEquals(result.size, 2)   // two distinct years
    assertEquals(totals.sum, 3L)   // 3 total rows
