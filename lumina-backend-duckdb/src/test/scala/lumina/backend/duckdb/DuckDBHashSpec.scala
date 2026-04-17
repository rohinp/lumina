package lumina.backend.duckdb

import munit.FunSuite
import lumina.plan.*
import lumina.plan.Expression.*
import lumina.plan.backend.*

/**
 * Tests for M22 hash functions in DuckDBBackend: Md5, Sha256.
 *
 * Mirrors LocalBackendHashSpec to verify both backends agree on digest values.
 */
class DuckDBHashSpec extends FunSuite:

  private val rows = Vector(
    Row(Map("word" -> "hello",  "num" -> 42)),
    Row(Map("word" -> "alice",  "num" -> 0)),
    Row(Map("word" -> "",       "num" -> -1)),
    Row(Map("word" -> null,     "num" -> null))
  )

  private val backend = DuckDBBackend(DataRegistry.of("memory://t" -> rows))
  private val src     = ReadCsv("memory://t", None)

  private def project(expr: Expression): Vector[Any] =
    val plan = Project(src, Vector(Alias(expr, "v")), None)
    backend.execute(plan) match
      case BackendResult.InMemory(rs) => rs.map(_.values("v"))

  // ---------------------------------------------------------------------------
  // Md5
  // ---------------------------------------------------------------------------

  test("Md5 returns the correct 32-char lowercase hex digest"):
    val result = project(Md5(ColumnRef("word")))
    assertEquals(result(0).toString, "5d41402abc4b2a76b9719d911017c592")
    assertEquals(result(1).toString, "6384e2b2184bcbf58eccf10ca7a6563c")

  test("Md5 of an empty string returns the well-known empty-string digest"):
    val result = project(Md5(ColumnRef("word")))
    assertEquals(result(2).toString, "d41d8cd98f00b204e9800998ecf8427e")

  test("Md5 propagates null"):
    val result = project(Md5(ColumnRef("word")))
    assertEquals(result(3), null)

  test("Md5 can be applied to a Literal string"):
    val result = project(Md5(Literal("hello")))
    assertEquals(result(0).toString, "5d41402abc4b2a76b9719d911017c592")

  // ---------------------------------------------------------------------------
  // Sha256
  // ---------------------------------------------------------------------------

  test("Sha256 returns the correct 64-char lowercase hex digest"):
    val result = project(Sha256(ColumnRef("word")))
    assertEquals(result(0).toString, "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824")
    assertEquals(result(1).toString, "2bd806c97f0e00af1a1fc3328fa763a9269723c8db8fac4f93af71db186d6e90")

  test("Sha256 of an empty string returns the well-known empty-string digest"):
    val result = project(Sha256(ColumnRef("word")))
    assertEquals(result(2).toString, "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855")

  test("Sha256 propagates null"):
    val result = project(Sha256(ColumnRef("word")))
    assertEquals(result(3), null)

  test("Sha256 can be applied to a Literal string"):
    val result = project(Sha256(Literal("hello")))
    assertEquals(result(0).toString, "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824")
