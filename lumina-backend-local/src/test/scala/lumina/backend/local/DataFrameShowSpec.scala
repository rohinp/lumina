package lumina.backend.local

import munit.FunSuite
import lumina.plan.*
import lumina.plan.backend.{DataRegistry, Row}
import lumina.api.{DataFrame, Lumina}

/**
 * Tests for DataFrame.show() and DataFrame.showString() executed via LocalBackend.
 *
 * show() delegates to showString(), so all assertions target showString()
 * to avoid capturing stdout.  Tests verify the table structure rather than
 * exact pixel-perfect formatting, since column widths vary with data.
 *
 * Lives in lumina-backend-local because a concrete backend is needed to
 * execute the plan before formatting.
 */
class DataFrameShowSpec extends FunSuite:

  private val sampleRows = Vector(
    Row(Map("city" -> "Paris",  "age" -> 35, "revenue" -> 1000.0)),
    Row(Map("city" -> "Berlin", "age" -> 29, "revenue" -> 2000.0))
  )

  private val registry    = DataRegistry.of("memory://customers" -> sampleRows)
  private val backend     = LocalBackend(registry)
  private val df          = DataFrame(ReadCsv("memory://customers", None))

  test("showString includes all column names in the header row"):
    val out = df.showString(backend)
    assert(out.contains("city"),    out)
    assert(out.contains("age"),     out)
    assert(out.contains("revenue"), out)

  test("showString includes all row values"):
    val out = df.showString(backend)
    assert(out.contains("Paris"),  out)
    assert(out.contains("Berlin"), out)
    assert(out.contains("35"),     out)

  test("showString includes a row count footer"):
    val out = df.showString(backend)
    assert(out.contains("2 row"), out)

  test("showString on an empty result prints the empty marker"):
    val emptyBackend = LocalBackend(DataRegistry.of("memory://customers" -> Vector.empty))
    val out = df.showString(emptyBackend)
    assert(out.contains("empty"), out)

  test("showString respects the n limit and shows only the first n rows"):
    val out = df.showString(backend, n = 1)
    assert(out.contains("1 row"), out)
    val cityCount = Seq("Paris", "Berlin").count(out.contains)
    assertEquals(cityCount, 1)

  test("showString uses separator lines to delimit the header from the data"):
    val out = df.showString(backend)
    val sepLines = out.linesIterator.filter(_.startsWith("+")).toVector
    assert(sepLines.size >= 2, s"Expected at least 2 separator lines:\n$out")

  test("show() prints without throwing"):
    df.show(backend)
