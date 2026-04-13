package lumina.plan.backend

import lumina.plan.LogicalPlan

/** Abstraction every execution backend must implement. */
trait Backend:
  def name: String
  def capabilities: BackendCapabilities
  def execute(plan: LogicalPlan): BackendResult

final case class BackendCapabilities(
    supportsDistributedExecution: Boolean,
    supportsVectorizedExecution: Boolean,
    supportsUserDefinedFunctions: Boolean
)

sealed trait BackendResult
object BackendResult:
  final case class InMemory(rows: Vector[Row]) extends BackendResult

final case class Row(values: Map[String, Any]):
  def apply(column: String): Any = values(column)

object Row:
  /**
   * Java/Kotlin-friendly factory: accepts alternating key-value pairs.
   *
   *   // Java
   *   Row.of("city", "Paris", "age", 35, "revenue", 1000.0)
   *
   *   // Kotlin
   *   Row.of("city", "Paris", "age", 35, "revenue", 1000.0)
   *
   * @varargs generates a Java Object... overload so callers don't need to wrap
   * arguments in a Scala Seq.
   */
  @scala.annotation.varargs
  def of(keyValuePairs: Any*): Row =
    require(keyValuePairs.size % 2 == 0, "Row.of requires an even number of arguments (key, value, ...)")
    val pairs = keyValuePairs.grouped(2).map { case Seq(k: String, v) => k -> v }.toMap
    Row(pairs)

  /** Java/Kotlin-friendly factory: accepts a java.util.Map. */
  def fromJava(values: java.util.Map[String, Any]): Row =
    import scala.jdk.CollectionConverters.*
    Row(values.asScala.toMap)
