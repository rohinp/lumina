package lumina.api

import lumina.plan.*
import lumina.plan.backend.{Backend, BackendResult, Row}
import scala.jdk.CollectionConverters.*

/** Minimal Pandas-inspired facade that records a logical plan for later execution. */
final class DataFrame private (val logicalPlan: LogicalPlan):

  /** Returns a new DataFrame with the additional filter predicate applied. */
  def filter(condition: Expression): DataFrame =
    DataFrame(Filter(logicalPlan, condition))

  /** Returns a new DataFrame projecting to the provided expressions (Scala API). */
  def select(columns: Expression*): DataFrame =
    DataFrame(Project(logicalPlan, columns.toVector, schema = None))

  /** Returns a new DataFrame projecting to the provided expressions (Java/Kotlin API). */
  def select(columns: java.lang.Iterable[Expression]): DataFrame =
    DataFrame(Project(logicalPlan, columns.asScala.toVector, schema = None))

  /** Groups rows by the supplied expressions and registers aggregations (Scala API). */
  def groupBy(grouping: Seq[Expression], aggregations: Seq[Aggregation], schema: Option[Schema] = None): DataFrame =
    DataFrame(Aggregate(logicalPlan, grouping.toVector, aggregations.toVector, schema))

  /** Groups rows by the supplied expressions and registers aggregations (Java/Kotlin API). */
  def groupBy(grouping: java.lang.Iterable[Expression], aggregations: java.lang.Iterable[Aggregation]): DataFrame =
    groupBy(grouping.asScala.toSeq, aggregations.asScala.toSeq)

  /** Executes this DataFrame's plan against the given backend and returns all result rows. */
  def collect(backend: Backend): Vector[Row] =
    backend.execute(logicalPlan) match
      case BackendResult.InMemory(rows) => rows

  /** Executes this DataFrame's plan and returns all result rows as a Java List. */
  def collectAsList(backend: Backend): java.util.List[Row] =
    collect(backend).asJava

  /** Gives access to the underlying logical plan for advanced tooling. */
  def plan: LogicalPlan = logicalPlan

object DataFrame:
  def apply(plan: LogicalPlan): DataFrame = new DataFrame(plan)

object Lumina:
  import java.util.Optional

  def readCsv(path: String): DataFrame =
    readCsv(path, Option.empty)

  def readCsv(path: String, schema: Schema): DataFrame =
    readCsv(path, Some(schema))

  def readCsv(path: String, schema: Optional[Schema]): DataFrame =
    readCsv(path, Option(schema.orElse(null)))

  def readCsv(path: String, schema: Option[Schema]): DataFrame =
    DataFrame(ReadCsv(path, schema))
