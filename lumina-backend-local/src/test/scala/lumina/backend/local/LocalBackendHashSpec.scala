package lumina.backend.local

import munit.FunSuite
import lumina.plan.*
import lumina.plan.Expression.*
import lumina.plan.backend.*

/**
 * Tests for M22 hash functions in LocalBackend: Md5, Sha256.
 *
 * Expected digests were computed with Python hashlib over UTF-8 encoded input,
 * which is the same algorithm used by java.security.MessageDigest.
 *
 * Read top-to-bottom as a specification for each function's behaviour.
 */
class LocalBackendHashSpec extends FunSuite:

  private val rows = Vector(
    Row(Map("word" -> "hello",  "num" -> 42)),
    Row(Map("word" -> "alice",  "num" -> 0)),
    Row(Map("word" -> "",       "num" -> -1)),
    Row(Map("word" -> null,     "num" -> null))
  )

  private val backend = LocalBackend(DataRegistry.of("memory://t" -> rows))
  private val src     = ReadCsv("memory://t", None)

  private def project(expr: Expression): Vector[Any] =
    val plan = Project(src, Vector(Alias(expr, "v")), None)
    backend.execute(plan) match
      case BackendResult.InMemory(rs) => rs.map(_.values("v"))

  // ---------------------------------------------------------------------------
  // Md5
  // ---------------------------------------------------------------------------

  test("Md5 returns the correct 32-char lowercase hex digest for a known string"):
    val result = project(Md5(ColumnRef("word")))
    assertEquals(result(0), "5d41402abc4b2a76b9719d911017c592")  // md5("hello")
    assertEquals(result(1), "6384e2b2184bcbf58eccf10ca7a6563c")  // md5("alice")

  test("Md5 of an empty string returns the well-known empty-string digest"):
    val result = project(Md5(ColumnRef("word")))
    assertEquals(result(2), "d41d8cd98f00b204e9800998ecf8427e")

  test("Md5 result is always exactly 32 characters"):
    val result = project(Md5(ColumnRef("word")))
    result.take(3).foreach(v => assertEquals(v.asInstanceOf[String].length, 32))

  test("Md5 propagates null"):
    val result = project(Md5(ColumnRef("word")))
    assertEquals(result(3), null)

  test("Md5 works on a non-string column by converting via toString"):
    // 42 → "42" → md5
    import java.security.MessageDigest
    val expected = MessageDigest.getInstance("MD5")
      .digest("42".getBytes(java.nio.charset.StandardCharsets.UTF_8))
      .map(b => f"${b & 0xff}%02x").mkString
    val result = project(Md5(ColumnRef("num")))
    assertEquals(result(0), expected)

  test("Md5 can be applied to a Literal string"):
    val result = project(Md5(Literal("hello")))
    assertEquals(result(0), "5d41402abc4b2a76b9719d911017c592")

  // ---------------------------------------------------------------------------
  // Sha256
  // ---------------------------------------------------------------------------

  test("Sha256 returns the correct 64-char lowercase hex digest for a known string"):
    val result = project(Sha256(ColumnRef("word")))
    assertEquals(result(0), "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824")
    assertEquals(result(1), "2bd806c97f0e00af1a1fc3328fa763a9269723c8db8fac4f93af71db186d6e90")

  test("Sha256 of an empty string returns the well-known empty-string digest"):
    val result = project(Sha256(ColumnRef("word")))
    assertEquals(result(2), "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855")

  test("Sha256 result is always exactly 64 characters"):
    val result = project(Sha256(ColumnRef("word")))
    result.take(3).foreach(v => assertEquals(v.asInstanceOf[String].length, 64))

  test("Sha256 propagates null"):
    val result = project(Sha256(ColumnRef("word")))
    assertEquals(result(3), null)

  test("Sha256 can be applied to a Literal string"):
    val result = project(Sha256(Literal("hello")))
    assertEquals(result(0), "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824")

  // ---------------------------------------------------------------------------
  // Md5 and Sha256 produce distinct results
  // ---------------------------------------------------------------------------

  test("Md5 and Sha256 of the same input produce different digests"):
    val md5    = project(Md5(ColumnRef("word")))(0).asInstanceOf[String]
    val sha256 = project(Sha256(ColumnRef("word")))(0).asInstanceOf[String]
    assertNotEquals(md5, sha256)
