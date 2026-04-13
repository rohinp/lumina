package lumina.backend.local

import lumina.plan.*
import lumina.plan.Expression.*
import lumina.plan.Aggregation.*
import lumina.plan.backend.*

/**
 * Pure-Scala, in-memory execution backend.
 *
 * Walks the LogicalPlan AST recursively, materialising each node as a
 * Vector[Row].  No I/O threads, no external dependencies.
 *
 * @param registry  Named datasets resolved when a ReadCsv path starts with
 *                  "memory://".  For real CSV paths the CsvLoader is used.
 */
final class LocalBackend(registry: DataRegistry = DataRegistry.empty) extends Backend:

  override val name: String = "local"

  override val capabilities: BackendCapabilities = BackendCapabilities(
    supportsDistributedExecution = false,
    supportsVectorizedExecution  = false,
    supportsUserDefinedFunctions = false
  )

  override def execute(plan: LogicalPlan): BackendResult =
    BackendResult.InMemory(run(plan))

  // ---------------------------------------------------------------------------
  // Plan interpreter
  // ---------------------------------------------------------------------------

  private def run(plan: LogicalPlan): Vector[Row] =
    plan match
      case ReadCsv(path, _)                          => load(path)
      case Filter(child, condition)                  => filter(run(child), condition)
      case Project(child, columns, _)                => project(run(child), columns)
      case Aggregate(child, groupBy, aggregations, _) =>
        aggregate(run(child), groupBy, aggregations)

  // ---------------------------------------------------------------------------
  // Operators
  // ---------------------------------------------------------------------------

  private def load(path: String): Vector[Row] =
    if path.startsWith("memory://") then
      registry.get(path).getOrElse(
        throw IllegalArgumentException(s"No dataset registered for path: $path")
      )
    else
      CsvLoader.load(path)

  private def filter(rows: Vector[Row], condition: Expression): Vector[Row] =
    rows.filter(ExpressionEvaluator.evaluatePredicate(condition, _))

  private def project(rows: Vector[Row], columns: Vector[Expression]): Vector[Row] =
    rows.map { row =>
      val projected = columns.map {
        case ColumnRef(name) => name -> row(name)
        case expr            =>
          throw UnsupportedOperationException(s"Project only supports ColumnRef, got: $expr")
      }
      Row(projected.toMap)
    }

  private def aggregate(
      rows: Vector[Row],
      groupBy: Vector[Expression],
      aggregations: Vector[Aggregation]
  ): Vector[Row] =
    // Group rows by the evaluated grouping key tuple
    val grouped: Map[Vector[Any], Vector[Row]] =
      rows.groupBy { row =>
        groupBy.map(ExpressionEvaluator.evaluate(_, row))
      }

    grouped.map { case (keyValues, groupRows) =>
      // Rebuild grouping columns
      val groupCols: Map[String, Any] =
        groupBy.zip(keyValues).collect {
          case (ColumnRef(name), value) => name -> value
        }.toMap

      // Apply each aggregation
      val aggCols: Map[String, Any] =
        aggregations.map {
          case Sum(colExpr, alias) =>
            val total = groupRows.map(r => toDouble(ExpressionEvaluator.evaluate(colExpr, r))).sum
            alias.getOrElse(nameOf(colExpr)) -> total

          case Count(colExprOpt, alias) =>
            val count = colExprOpt match
              case None       => groupRows.size.toLong
              case Some(expr) => groupRows.count(r => ExpressionEvaluator.evaluate(expr, r) != null).toLong
            alias.getOrElse("count") -> count
        }.toMap

      Row(groupCols ++ aggCols)
    }.toVector

  // ---------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------

  private def toDouble(v: Any): Double =
    v match
      case n: Int    => n.toDouble
      case n: Long   => n.toDouble
      case n: Double => n
      case n: Float  => n.toDouble
      case other     => throw IllegalArgumentException(s"Cannot sum non-numeric value: $other")

  private def nameOf(expr: Expression): String =
    expr match
      case ColumnRef(name) => name
      case _               => "value"
