package lumina.backend.local

import munit.FunSuite
import lumina.plan.*
import lumina.plan.Expression.*
import lumina.plan.backend.*

/**
 * Tests for M17 extended string functions in LocalBackend: Replace,
 * RegexpExtract, RegexpReplace, StartsWith, EndsWith, LPad, RPad,
 * Repeat, Reverse, InitCap.
 *
 * Read top-to-bottom as a specification for each function's behaviour.
 */
class LocalBackendStringExtSpec extends FunSuite:

  private val rows = Vector(
    Row(Map("name" -> "alice smith",  "code" -> "US-001", "tag" -> "hello world")),
    Row(Map("name" -> "Bob Jones",    "code" -> "GB-042", "tag" -> "foo bar baz")),
    Row(Map("name" -> "  carol  ",    "code" -> "AU-007", "tag" -> "test123")),
    Row(Map("name" -> null,           "code" -> "XX-000", "tag" -> null))
  )

  private val backend = LocalBackend(DataRegistry.of("memory://t" -> rows))
  private val src     = ReadCsv("memory://t", None)

  private def project(expr: Expression): Vector[Any] =
    val plan = Project(src, Vector(Alias(expr, "v")), None)
    backend.execute(plan) match
      case BackendResult.InMemory(rs) => rs.map(_.values("v"))

  // ---------------------------------------------------------------------------
  // Replace
  // ---------------------------------------------------------------------------

  test("Replace substitutes every occurrence of the search string"):
    val result = project(Replace(ColumnRef("code"), "-", "_"))
    assertEquals(result(0), "US_001")
    assertEquals(result(1), "GB_042")

  test("Replace with no match returns the original string"):
    val result = project(Replace(ColumnRef("code"), "ZZ", "xx"))
    assertEquals(result(0), "US-001")

  test("Replace propagates null"):
    val result = project(Replace(ColumnRef("name"), "x", "y"))
    assertEquals(result(3), null)

  // ---------------------------------------------------------------------------
  // RegexpExtract
  // ---------------------------------------------------------------------------

  test("RegexpExtract returns the first capturing group that matches"):
    // Extract the country prefix before the dash
    val result = project(RegexpExtract(ColumnRef("code"), "([A-Z]+)-\\d+", 1))
    assertEquals(result(0), "US")
    assertEquals(result(1), "GB")
    assertEquals(result(2), "AU")

  test("RegexpExtract returns null when no match is found"):
    val result = project(RegexpExtract(ColumnRef("tag"), "^(\\d+)$", 1))
    assertEquals(result(0), null)  // "hello world" has no all-digit match

  test("RegexpExtract with group=0 returns the whole match"):
    val result = project(RegexpExtract(ColumnRef("code"), "[A-Z]+-\\d+", 0))
    assertEquals(result(0), "US-001")

  test("RegexpExtract propagates null"):
    val result = project(RegexpExtract(ColumnRef("name"), "\\w+", 0))
    assertEquals(result(3), null)

  // ---------------------------------------------------------------------------
  // RegexpReplace
  // ---------------------------------------------------------------------------

  test("RegexpReplace replaces all matches of the pattern"):
    val result = project(RegexpReplace(ColumnRef("code"), "[A-Z]", "X"))
    assertEquals(result(0), "XX-001")
    assertEquals(result(1), "XX-042")

  test("RegexpReplace with no match returns the original string"):
    val result = project(RegexpReplace(ColumnRef("tag"), "\\d{10}", ""))
    assertEquals(result(0), "hello world")

  test("RegexpReplace propagates null"):
    val result = project(RegexpReplace(ColumnRef("name"), "\\w+", "X"))
    assertEquals(result(3), null)

  // ---------------------------------------------------------------------------
  // StartsWith / EndsWith
  // ---------------------------------------------------------------------------

  test("StartsWith returns true when the string begins with the prefix"):
    val result = project(StartsWith(ColumnRef("code"), "US"))
    assertEquals(result(0), true)
    assertEquals(result(1), false)

  test("StartsWith returns false for null input"):
    val result = project(StartsWith(ColumnRef("name"), "a"))
    assertEquals(result(3), false)

  test("EndsWith returns true when the string ends with the suffix"):
    val result = project(EndsWith(ColumnRef("code"), "001"))
    assertEquals(result(0), true)
    assertEquals(result(1), false)

  test("EndsWith returns false for null input"):
    val result = project(EndsWith(ColumnRef("name"), "h"))
    assertEquals(result(3), false)

  test("StartsWith and EndsWith can be combined in a Filter condition"):
    val plan   = Filter(src,
      And(StartsWith(ColumnRef("code"), "GB"), EndsWith(ColumnRef("code"), "042")))
    val result = backend.execute(plan) match
      case BackendResult.InMemory(rs) => rs
    assertEquals(result.size, 1)
    assertEquals(result(0).values("name"), "Bob Jones")

  // ---------------------------------------------------------------------------
  // LPad / RPad
  // ---------------------------------------------------------------------------

  test("LPad pads a string on the left to the given length"):
    val result = project(LPad(ColumnRef("code"), 8, "0"))
    assertEquals(result(0), "00US-001")
    assertEquals(result(1), "00GB-042")

  test("LPad returns the original string when it is already long enough"):
    val result = project(LPad(ColumnRef("code"), 6, "0"))
    assertEquals(result(0), "US-001")  // already 6 chars

  test("LPad with multi-char pad fills correctly"):
    val result = project(LPad(ColumnRef("code"), 9, "AB"))
    assertEquals(result(0).asInstanceOf[String].length, 9)
    assert(result(0).asInstanceOf[String].endsWith("US-001"))

  test("LPad propagates null"):
    val result = project(LPad(ColumnRef("name"), 10, " "))
    assertEquals(result(3), null)

  test("RPad pads a string on the right to the given length"):
    val result = project(RPad(ColumnRef("code"), 8, "*"))
    assertEquals(result(0), "US-001**")

  test("RPad returns the original string when it is already long enough"):
    val result = project(RPad(ColumnRef("code"), 3, "*"))
    assertEquals(result(0), "US-001")  // 6 > 3 → returned as-is

  test("RPad propagates null"):
    val result = project(RPad(ColumnRef("name"), 10, " "))
    assertEquals(result(3), null)

  // ---------------------------------------------------------------------------
  // Repeat
  // ---------------------------------------------------------------------------

  test("Repeat returns the string repeated n times"):
    val result = project(Repeat(Literal("ab"), 3))
    assertEquals(result(0), "ababab")

  test("Repeat with n=1 returns the original string"):
    val result = project(Repeat(ColumnRef("code"), 1))
    assertEquals(result(0), "US-001")

  test("Repeat with n=0 returns an empty string"):
    val result = project(Repeat(ColumnRef("code"), 0))
    assertEquals(result(0), "")

  test("Repeat propagates null"):
    val result = project(Repeat(ColumnRef("name"), 2))
    assertEquals(result(3), null)

  // ---------------------------------------------------------------------------
  // Reverse
  // ---------------------------------------------------------------------------

  test("Reverse returns the characters of the string in reverse order"):
    val result = project(Reverse(ColumnRef("code")))
    assertEquals(result(0), "100-SU")
    assertEquals(result(1), "240-BG")

  test("Reverse of an empty string returns an empty string"):
    val emptyRow = Vector(Row(Map("s" -> "")))
    val be       = LocalBackend(DataRegistry.of("memory://e" -> emptyRow))
    val plan     = Project(ReadCsv("memory://e", None), Vector(Alias(Reverse(ColumnRef("s")), "r")), None)
    val result   = be.execute(plan) match
      case BackendResult.InMemory(rs) => rs
    assertEquals(result(0).values("r"), "")

  test("Reverse propagates null"):
    val result = project(Reverse(ColumnRef("name")))
    assertEquals(result(3), null)

  // ---------------------------------------------------------------------------
  // InitCap
  // ---------------------------------------------------------------------------

  test("InitCap capitalises the first letter of each word"):
    val result = project(InitCap(ColumnRef("name")))
    assertEquals(result(0), "Alice Smith")
    assertEquals(result(1), "Bob Jones")

  test("InitCap lowercases letters after the first in each word"):
    val mixedRow = Vector(Row(Map("s" -> "hELLO wORLD")))
    val be       = LocalBackend(DataRegistry.of("memory://m" -> mixedRow))
    val plan     = Project(ReadCsv("memory://m", None), Vector(Alias(InitCap(ColumnRef("s")), "v")), None)
    val result   = be.execute(plan) match
      case BackendResult.InMemory(rs) => rs
    assertEquals(result(0).values("v"), "Hello World")

  test("InitCap propagates null"):
    val result = project(InitCap(ColumnRef("name")))
    assertEquals(result(3), null)
