package lumina.backend.local

import munit.FunSuite
import lumina.plan.backend.*
import lumina.api.*

/**
 * Tests for M13 export methods: toCsvString, writeCsv, and toJsonLines.
 *
 * Each test describes one aspect of the formatted output so the spec is
 * readable top-to-bottom without running the code.
 */
class DataFrameExportSpec extends FunSuite:

  private val rows = Vector(
    Row(Map("city" -> "Paris",  "revenue" -> 1000.0)),
    Row(Map("city" -> "Berlin", "revenue" -> 2000.0))
  )

  private val backend = LocalBackend(DataRegistry.of("memory://t" -> rows))
  private val df      = Lumina.readCsv("memory://t")

  // ---------------------------------------------------------------------------
  // toCsvString
  // ---------------------------------------------------------------------------

  test("toCsvString produces a header row followed by one row per result"):
    val csv = df.toCsvString(backend)
    val lines = csv.split("\n")
    assertEquals(lines.length, 3) // header + 2 data rows

  test("toCsvString header contains all column names"):
    val header = df.toCsvString(backend).split("\n").head
    assert(header.contains("city"),    header)
    assert(header.contains("revenue"), header)

  test("toCsvString data rows contain the correct values"):
    val lines = df.toCsvString(backend).split("\n")
    assert(lines(1).contains("Paris"),  lines(1))
    assert(lines(2).contains("Berlin"), lines(2))

  test("toCsvString without header omits the column name row"):
    val csv   = df.toCsvString(backend, includeHeader = false)
    val lines = csv.split("\n")
    assertEquals(lines.length, 2)
    assert(!lines.head.contains("city"))

  test("toCsvString escapes values that contain commas"):
    val specialRows = Vector(Row(Map("desc" -> "hello, world", "n" -> 1)))
    val be  = LocalBackend(DataRegistry.of("memory://s" -> specialRows))
    val csv = Lumina.readCsv("memory://s").toCsvString(be)
    assert(csv.contains("\"hello, world\""), csv)

  test("toCsvString escapes values that contain double-quotes"):
    val specialRows = Vector(Row(Map("desc" -> "say \"hi\"", "n" -> 1)))
    val be  = LocalBackend(DataRegistry.of("memory://q" -> specialRows))
    val csv = Lumina.readCsv("memory://q").toCsvString(be)
    assert(csv.contains("\"say \"\"hi\"\"\""), csv)

  test("toCsvString on an empty result returns an empty string"):
    val emptyRows = Vector.empty[Row]
    val be  = LocalBackend(DataRegistry.of("memory://e" -> emptyRows))
    val csv = Lumina.readCsv("memory://e").toCsvString(be)
    assertEquals(csv, "")

  // ---------------------------------------------------------------------------
  // writeCsv
  // ---------------------------------------------------------------------------

  test("writeCsv writes a file that toCsvString would produce"):
    val tmp = java.nio.file.Files.createTempFile("lumina-test-", ".csv")
    try
      df.writeCsv(tmp.toString, backend)
      val written  = java.nio.file.Files.readString(tmp)
      val expected = df.toCsvString(backend)
      assertEquals(written, expected)
    finally
      java.nio.file.Files.deleteIfExists(tmp)

  // ---------------------------------------------------------------------------
  // toJsonLines
  // ---------------------------------------------------------------------------

  test("toJsonLines produces one JSON object per row separated by newlines"):
    val json  = df.toJsonLines(backend)
    val lines = json.split("\n")
    assertEquals(lines.length, 2)

  test("toJsonLines output is valid JSON objects with field names and values"):
    val json = df.toJsonLines(backend)
    assert(json.contains("\"city\""),    json)
    assert(json.contains("\"Paris\""),   json)
    assert(json.contains("\"Berlin\""),  json)
    assert(json.contains("\"revenue\""), json)

  test("toJsonLines encodes null values as JSON null"):
    val nullRows = Vector(Row(Map("name" -> null, "score" -> 42)))
    val be   = LocalBackend(DataRegistry.of("memory://n" -> nullRows))
    val json = Lumina.readCsv("memory://n").toJsonLines(be)
    assert(json.contains("null"), json)

  test("toJsonLines encodes boolean values without quotes"):
    val boolRows = Vector(Row(Map("flag" -> true, "other" -> false)))
    val be   = LocalBackend(DataRegistry.of("memory://b" -> boolRows))
    val json = Lumina.readCsv("memory://b").toJsonLines(be)
    assert(json.contains("true"),  json)
    assert(json.contains("false"), json)

  test("toJsonLines on an empty result returns an empty string"):
    val emptyRows = Vector.empty[Row]
    val be  = LocalBackend(DataRegistry.of("memory://ej" -> emptyRows))
    val out = Lumina.readCsv("memory://ej").toJsonLines(be)
    assertEquals(out, "")
