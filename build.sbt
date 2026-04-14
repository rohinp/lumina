import sbt.*

val scala3Version = "3.8.3"
val projectVersion = "0.1.0-SNAPSHOT"
val kotlinVersion = "1.9.24"
val duckdbVersion = "1.2.0"

lazy val commonSettings: Seq[Def.Setting[?]] = Seq(
  scalaVersion := scala3Version,
  version := projectVersion
)

val testLibraries: Seq[ModuleID] = Seq(
  "org.scalameta" %% "munit" % "1.2.4" % Test
)

def luminaModule(
    moduleName: String,
    base: Project,
    extraSettings: Seq[Def.Setting[?]] = Seq.empty
): Project =
  base
    .settings(commonSettings*)
    .settings(
      name := moduleName,
      libraryDependencies ++= testLibraries
    )
    .settings(extraSettings*)

lazy val luminaPlan = luminaModule("lumina-plan", project.in(file("lumina-plan")))

lazy val luminaApi =
  luminaModule(
    "lumina-api",
    project.in(file("lumina-api")).dependsOn(luminaPlan)
  )

lazy val luminaBackendLocal =
  luminaModule(
    "lumina-backend-local",
    // "test->test" makes lumina-plan's test classes (e.g. BackendComplianceSuite)
    // visible in this module's test scope.
    project.in(file("lumina-backend-local")).dependsOn(luminaPlan % "compile->compile;test->test")
  )

lazy val luminaBackendDuckdb =
  luminaModule(
    "lumina-backend-duckdb",
    project.in(file("lumina-backend-duckdb")).dependsOn(luminaPlan % "compile->compile;test->test"),
    Seq(libraryDependencies += "org.duckdb" % "duckdb_jdbc" % duckdbVersion)
  )

lazy val luminaBackendPolars =
  luminaModule(
    "lumina-backend-polars",
    project.in(file("lumina-backend-polars")).dependsOn(luminaPlan)
  )

lazy val luminaBackendSpark =
  luminaModule(
    "lumina-backend-spark",
    project.in(file("lumina-backend-spark")).dependsOn(luminaPlan)
  )

lazy val luminaConfig =
  luminaModule(
    "lumina-config",
    project
      .in(file("lumina-config"))
      .dependsOn(
        luminaPlan,
        luminaBackendLocal,
        luminaBackendDuckdb,
        luminaBackendPolars,
        luminaBackendSpark
      )
  )

lazy val luminaIntegrationTests =
  luminaModule(
    "lumina-integration-tests",
    project.in(file("integration-tests")).dependsOn(luminaApi, luminaConfig),
    Seq(
      libraryDependencies ++= Seq(
        "org.jetbrains.kotlin" % "kotlin-compiler-embeddable" % kotlinVersion % Test,
        "org.jetbrains.kotlin" % "kotlin-stdlib"              % kotlinVersion % Test
      ),
      // Flat classloader strategy ensures dynamically compiled and loaded Java/Kotlin
      // classes (via URLClassLoader) can see all project dependencies at runtime.
      Test / classLoaderLayeringStrategy := ClassLoaderLayeringStrategy.Flat
    )
  )

lazy val root = project
  .in(file("."))
  .aggregate(
    luminaPlan,
    luminaApi,
    luminaBackendLocal,
    luminaBackendDuckdb,
    luminaBackendPolars,
    luminaBackendSpark,
    luminaConfig,
    luminaIntegrationTests
  )
  .settings(commonSettings*)
  .settings(
    name := "lumina",
    publish / skip := true,
    libraryDependencies ++= testLibraries
  )
