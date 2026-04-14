package lumina.integration

import munit.FunSuite
import java.io.File
import java.nio.file.{Files, Path}
import java.util.Locale
import javax.tools.ToolProvider

import scala.jdk.CollectionConverters.*

import org.jetbrains.kotlin.cli.jvm.K2JVMCompiler

/**
 * Cross-language integration tests — prove that the full Lumina pipeline
 * (readCsv → filter → groupBy → collectAsList) can be compiled and executed
 * from both Java and Kotlin without any Scala-specific imports.
 *
 * Each test:
 *  1. Writes a source snippet to a temp directory
 *  2. Compiles it against the Lumina classpath
 *  3. Loads and invokes the compiled class via reflection to verify it runs
 *
 * Read the source snippets in this file to understand the intended Java/Kotlin
 * developer experience for Lumina.
 */
class CrossLanguageSmokeSpec extends FunSuite:

  // ---------------------------------------------------------------------------
  // Classpath — includes all Lumina modules so snippets can use the full stack
  // ---------------------------------------------------------------------------

  private val repoRoot = Path.of("").toAbsolutePath.normalize()

  private val moduleClassDirs = Seq(
    "lumina-api/target/scala-3.8.3/classes",
    "lumina-plan/target/scala-3.8.3/classes",
    "lumina-backend-local/target/scala-3.8.3/classes",
    "lumina-config/target/scala-3.8.3/classes"
  ).map(repoRoot.resolve).filter(p => Files.exists(p)).map(_.toString)

  // SBT loads test dependencies via a URL classloader, not java.class.path.
  // Collect all URLs from the context classloader so dynamically-compiled
  // snippets have access to all test-scope jars (including kotlin-stdlib).
  private val urlClassLoaderEntries: Seq[String] =
    Thread.currentThread.getContextClassLoader match
      case ucl: java.net.URLClassLoader => ucl.getURLs.map(_.getPath).toSeq
      case _                            => Seq.empty

  private val classpath =
    (sys.props("java.class.path").split(File.pathSeparator).toSeq
      ++ urlClassLoaderEntries
      ++ moduleClassDirs)
      .distinct
      .mkString(File.pathSeparator)

  // ---------------------------------------------------------------------------
  // Java — compilation
  // ---------------------------------------------------------------------------

  test("Java snippet that builds and executes a full filter → groupBy pipeline compiles"):
    val workdir = Files.createTempDirectory("lumina-java-compile")
    val source  = workdir.resolve("LuminaJavaPipeline.java")
    Files.writeString(source, javaFullPipelineSource("LuminaJavaPipeline"))
    assert(compileJava(source, workdir), "Java full-pipeline snippet failed to compile")

  // ---------------------------------------------------------------------------
  // Java — execution
  // ---------------------------------------------------------------------------

  test("Java snippet executes a filter → groupBy → collectAsList pipeline and returns correct rows"):
    val workdir = Files.createTempDirectory("lumina-java-exec")
    val source  = workdir.resolve("LuminaJavaExec.java")
    Files.writeString(source, javaFullPipelineSource("LuminaJavaExec"))

    assert(compileJava(source, workdir), "Java execution snippet failed to compile")

    val classLoader = new java.net.URLClassLoader(
      Array(workdir.toUri.toURL),
      Thread.currentThread.getContextClassLoader
    )
    val clazz  = classLoader.loadClass("LuminaJavaExec")
    val method = clazz.getMethod("run")
    val result = method.invoke(null).asInstanceOf[java.util.List[?]]

    assertEquals(result.size, 1, "expected exactly one group (Paris only, age > 30 filter)")

    val row         = result.get(0)
    val applyMethod = row.getClass.getMethod("apply", classOf[String])
    assertEquals(applyMethod.invoke(row, "city"):         Any, "Paris": Any)
    assertEquals(applyMethod.invoke(row, "total_revenue"):Any, 4000.0:  Any)

  // ---------------------------------------------------------------------------
  // Kotlin — compilation
  // ---------------------------------------------------------------------------

  test("Kotlin snippet that builds and executes a full filter → groupBy pipeline compiles"):
    val workdir  = Files.createTempDirectory("lumina-kotlin-compile")
    val classdir = workdir.resolve("classes")
    Files.createDirectories(classdir)
    val source = workdir.resolve("LuminaKotlinPipeline.kt")
    Files.writeString(source, kotlinFullPipelineSource)
    val exitCode = compileKotlin(source, classdir)
    assertEquals(exitCode, 0, "Kotlin full-pipeline snippet failed to compile")

  // ---------------------------------------------------------------------------
  // Kotlin — execution
  // ---------------------------------------------------------------------------

  test("Kotlin snippet executes a filter → groupBy → collectAsList pipeline and returns correct rows"):
    val workdir  = Files.createTempDirectory("lumina-kotlin-exec")
    val classdir = workdir.resolve("classes")
    Files.createDirectories(classdir)
    val source = workdir.resolve("LuminaKotlinPipeline.kt")
    Files.writeString(source, kotlinFullPipelineSource)

    val exitCode = compileKotlin(source, classdir)
    assertEquals(exitCode, 0, "Kotlin execution snippet failed to compile")

    val classLoader = new java.net.URLClassLoader(
      Array(classdir.toUri.toURL),
      Thread.currentThread.getContextClassLoader
    )
    val clazz  = classLoader.loadClass("LuminaKotlinPipelineKt")
    val method = clazz.getMethod("runPipeline")
    val result = method.invoke(null).asInstanceOf[java.util.List[?]]

    assertEquals(result.size, 1, "expected exactly one group (Paris only, age > 30 filter)")

    val row         = result.get(0)
    val applyMethod = row.getClass.getMethod("apply", classOf[String])
    assertEquals(applyMethod.invoke(row, "city"):          Any, "Paris": Any)
    assertEquals(applyMethod.invoke(row, "total_revenue"): Any, 4000.0:  Any)

  // ---------------------------------------------------------------------------
  // Source snippets
  // ---------------------------------------------------------------------------

  /**
   * Canonical Java usage of the Lumina API.
   *
   * Demonstrates:
   *   - Row.of(key, value, ...) for building sample data
   *   - DataRegistry / LocalBackend for in-memory execution
   *   - LuminaJava.readCsv → filter → groupBy (with java.util.List) → collectAsList
   *   - Aggregation.sum(column, alias) — no Option required
   *
   * This snippet is the reference for Java developers onboarding to Lumina.
   */
  private def javaFullPipelineSource(className: String): String =
    s"""|import lumina.api.LuminaJava;
        |import lumina.plan.Expression.ColumnRef;
        |import lumina.plan.Expression.Literal;
        |import lumina.plan.Expression.GreaterThan;
        |import lumina.plan.Aggregation;
        |import lumina.plan.backend.Row;
        |import lumina.plan.backend.DataRegistry;
        |import lumina.backend.local.LocalBackend;
        |import java.util.List;
        |
        |public class $className {
        |
        |  /** Registers sample data, runs the pipeline, and returns the result rows. */
        |  public static List<Row> run() {
        |    var rows = List.of(
        |      Row.of("city", "Paris",  "age", 35, "revenue", 1000.0),
        |      Row.of("city", "Paris",  "age", 45, "revenue", 3000.0),
        |      Row.of("city", "Berlin", "age", 29, "revenue", 2000.0)
        |    );
        |
        |    var registry = DataRegistry.empty().register("memory://customers", rows);
        |    var backend  = new LocalBackend(registry);
        |
        |    return LuminaJava.readCsv("memory://customers")
        |      .filter(new GreaterThan(new ColumnRef("age"), new Literal(30)))
        |      .groupBy(
        |        List.of(new ColumnRef("city")),
        |        List.of(Aggregation.sum(new ColumnRef("revenue"), "total_revenue"))
        |      )
        |      .collectAsList(backend);
        |  }
        |}
        |""".stripMargin

  /**
   * Canonical Kotlin usage of the Lumina API.
   *
   * Demonstrates:
   *   - Row.of(key, value, ...) for building sample data
   *   - DataRegistry / LocalBackend for in-memory execution
   *   - LuminaJava.readCsv → filter → groupBy (with java.util.List) → collectAsList
   *   - Aggregation.sum(column, alias) — no Option required
   *
   * This snippet is the reference for Kotlin developers onboarding to Lumina.
   */
  private val kotlinFullPipelineSource: String =
    """|import lumina.api.LuminaJava
       |import lumina.plan.Expression
       |import lumina.plan.Expression.ColumnRef
       |import lumina.plan.Expression.Literal
       |import lumina.plan.Expression.GreaterThan
       |import lumina.plan.Aggregation
       |import lumina.plan.backend.Row
       |import lumina.plan.backend.DataRegistry
       |import lumina.backend.local.LocalBackend
       |
       |/**
       | * Kotlin reference pipeline for Lumina.
       | *
       | * Uses ArrayList (unambiguous Java type in Kotlin without stdlib) for all
       | * collection construction. Returns Any to avoid Kotlin's java.util.List
       | * platform type ambiguity — callers can cast the result as needed.
       | */
       |fun runPipeline(): Any {
       |  val rows = java.util.ArrayList<Row>()
       |  rows.add(Row.of("city", "Paris",  "age", 35, "revenue", 1000.0))
       |  rows.add(Row.of("city", "Paris",  "age", 45, "revenue", 3000.0))
       |  rows.add(Row.of("city", "Berlin", "age", 29, "revenue", 2000.0))
       |
       |  val registry = DataRegistry.empty().register("memory://customers", rows)
       |  val backend  = LocalBackend(registry)
       |
       |  val groupingCols = java.util.ArrayList<Expression>()
       |  groupingCols.add(ColumnRef("city"))
       |
       |  val aggCols = java.util.ArrayList<Aggregation>()
       |  aggCols.add(Aggregation.sum(ColumnRef("revenue"), "total_revenue"))
       |
       |  return LuminaJava.readCsv("memory://customers")
       |    .filter(GreaterThan(ColumnRef("age"), Literal(30)))
       |    .groupBy(groupingCols, aggCols)
       |    .collectAsList(backend)
       |}
       |""".stripMargin

  // ---------------------------------------------------------------------------
  // Compiler helpers
  // ---------------------------------------------------------------------------

  private def compileJava(source: Path, outputDir: Path): Boolean =
    val compiler = ToolProvider.getSystemJavaCompiler
    assert(compiler != null, "JDK compiler is required — run with a JDK, not just a JRE")
    val fileManager      = compiler.getStandardFileManager(null, Locale.getDefault, null)
    val compilationUnits = fileManager.getJavaFileObjects(source.toFile)
    val options          = List("-classpath", classpath, "-d", outputDir.toString).asJava
    compiler.getTask(null, fileManager, null, options, null, compilationUnits).call()

  private def compileKotlin(source: Path, outputDir: Path): Int =
    // Use -no-stdlib because the kotlin-stdlib jar is already included in
    // `classpath` (pulled from the URLClassLoader entries above). Without
    // -no-stdlib, K2JVMCompiler tries to locate kotlin-stdlib.jar on the
    // filesystem via a "kotlin-home" directory, which does not exist when
    // running via kotlin-compiler-embeddable from Maven/Ivy.
    K2JVMCompiler().exec(
      System.err,
      "-classpath", classpath,
      "-d",         outputDir.toString,
      "-no-stdlib",
      "-no-reflect",
      source.toString
    ).getCode
