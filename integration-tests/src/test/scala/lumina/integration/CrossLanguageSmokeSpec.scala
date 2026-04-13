package lumina.integration

import munit.FunSuite
import java.io.File
import java.nio.file.{Files, Path}
import java.util.Locale
import javax.tools.ToolProvider

import scala.jdk.CollectionConverters.*

import org.jetbrains.kotlin.cli.jvm.K2JVMCompiler

class CrossLanguageSmokeSpec extends FunSuite:

  private val repoRoot = Path.of("").toAbsolutePath.normalize()
  private val extraEntries = Seq(
    repoRoot.resolve("lumina-api/target/scala-3.8.3/classes"),
    repoRoot.resolve("lumina-plan/target/scala-3.8.3/classes")
  ).filter(Files.exists(_)).map(_.toString)
  private val classpath = (sys.props("java.class.path").split(File.pathSeparator).toSeq ++ extraEntries).distinct.mkString(File.pathSeparator)

  test("Java snippet compiles against the Lumina API"):
    val workdir = Files.createTempDirectory("lumina-java-smoke")
    val source = workdir.resolve("LuminaJavaSmokeTest.java")
    Files.writeString(
      source,
      """|import lumina.api.Lumina;
         |import lumina.plan.Expression.ColumnRef;
         |import lumina.plan.Expression.Literal;
         |import lumina.plan.Expression.GreaterThan;
         |
         |public class LuminaJavaSmokeTest {
         |  public static void entrypoint() {
         |    var df = Lumina.readCsv("data.csv");
         |    var filtered = df.filter(new GreaterThan(new ColumnRef("age"), new Literal(30)));
         |    filtered.plan();
         |  }
         |}
         |""".stripMargin
    )

    val compiler = ToolProvider.getSystemJavaCompiler
    assert(compiler != null, "JDK compiler is required for the smoke test")
    val fileManager = compiler.getStandardFileManager(null, Locale.getDefault, null)
    val compilationUnits = fileManager.getJavaFileObjects(source.toFile)
    val options = List("-classpath", classpath, "-d", workdir.toString).asJava
    val task = compiler.getTask(null, fileManager, null, options, null, compilationUnits)
    assert(task.call(), "Java smoke snippet failed to compile")

  test("Kotlin snippet compiles against the Lumina API"):
    val workdir = Files.createTempDirectory("lumina-kotlin-smoke")
    val source = workdir.resolve("LuminaSmoke.kt")
    Files.writeString(
      source,
      """|import lumina.api.Lumina
         |import lumina.plan.Expression
         |import lumina.plan.Expression.ColumnRef
         |import lumina.plan.Expression.Literal
         |import lumina.plan.Expression.GreaterThan
         |
         |fun runSmoke() {
         |  val df = Lumina.readCsv("data.csv")
         |  val filtered = df.filter(GreaterThan(ColumnRef("age"), Literal(30)))
         |  filtered.plan()
         |}
         |""".stripMargin
    )

    val compiler = K2JVMCompiler()
    val exitCode = compiler.exec(
      System.err,
      "-classpath",
      classpath,
      "-d",
      workdir.resolve("classes").toString,
      "-no-stdlib",
      "-no-reflect",
      source.toString
    )
    assertEquals(exitCode.getCode, 0, "Kotlin smoke snippet failed to compile")
