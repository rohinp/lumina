package lumina.backend.local

import lumina.plan.*
import lumina.plan.Expression.*
import lumina.plan.Aggregation.*
import lumina.plan.backend.*
import lumina.plan.optimizer.Optimizer

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
    BackendResult.InMemory(run(Optimizer.optimize(plan)))

  // ---------------------------------------------------------------------------
  // Plan interpreter
  // ---------------------------------------------------------------------------

  private def run(plan: LogicalPlan): Vector[Row] =
    plan match
      case ReadCsv(path, _)                             => load(path)
      case Filter(child, condition)                     => filter(run(child), condition)
      case Project(child, columns, _)                   => project(run(child), columns)
      case Aggregate(child, groupBy, aggregations, _)   =>
        aggregate(run(child), groupBy, aggregations)
      case Sort(child, sortExprs)                       => sort(run(child), sortExprs)
      case Limit(child, count)                          => run(child).take(count)
      case Join(left, right, condition, joinType)       =>
        join(run(left), run(right), condition, joinType)

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
        case ColumnRef(name) => name -> row.values.getOrElse(name, null)
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
    val grouped: Map[Vector[Any], Vector[Row]] =
      rows.groupBy { row =>
        groupBy.map(ExpressionEvaluator.evaluate(_, row))
      }

    grouped.map { case (keyValues, groupRows) =>
      val groupCols: Map[String, Any] =
        groupBy.zip(keyValues).collect {
          case (ColumnRef(name), value) => name -> value
        }.toMap

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

          case Avg(colExpr, alias) =>
            val vals = groupRows.map(r => toDouble(ExpressionEvaluator.evaluate(colExpr, r)))
            val avg  = if vals.isEmpty then 0.0 else vals.sum / vals.size
            alias.getOrElse(nameOf(colExpr)) -> avg

          case Min(colExpr, alias) =>
            val vals = groupRows.map(r => ExpressionEvaluator.evaluate(colExpr, r))
            val min  = vals.minBy(v => toDouble(v))
            alias.getOrElse(nameOf(colExpr)) -> min

          case Max(colExpr, alias) =>
            val vals = groupRows.map(r => ExpressionEvaluator.evaluate(colExpr, r))
            val max  = vals.maxBy(v => toDouble(v))
            alias.getOrElse(nameOf(colExpr)) -> max
        }.toMap

      Row(groupCols ++ aggCols)
    }.toVector

  private def sort(rows: Vector[Row], sortExprs: Vector[SortExpr]): Vector[Row] =
    rows.sortWith { (a, b) =>
      sortExprs.iterator.map { se =>
        val av = ExpressionEvaluator.evaluate(se.expr, a)
        val bv = ExpressionEvaluator.evaluate(se.expr, b)
        val cmp =
          (av, bv) match
            case (sa: String, sb: String) => sa.compareTo(sb)
            case _ => toDouble(av).compareTo(toDouble(bv))
        if se.ascending then cmp else -cmp
      }.find(_ != 0).getOrElse(0) < 0
    }

  private def join(
      leftRows:  Vector[Row],
      rightRows: Vector[Row],
      condition: Option[Expression],
      joinType:  JoinType
  ): Vector[Row] =
    joinType match
      case JoinType.Inner => innerJoin(leftRows, rightRows, condition)
      case JoinType.Left  => leftOuterJoin(leftRows, rightRows, condition)
      case JoinType.Right => leftOuterJoin(rightRows, leftRows, condition)
      case JoinType.Full  => fullOuterJoin(leftRows, rightRows, condition)

  private def mergeRows(left: Row, right: Row): Row =
    Row(left.values ++ right.values)

  private def nullRow(sample: Row): Row =
    Row(sample.values.map { case (k, _) => k -> null })

  private def matchesCond(left: Row, right: Row, cond: Option[Expression]): Boolean =
    cond match
      case None    => true
      case Some(c) => ExpressionEvaluator.evaluatePredicate(c, mergeRows(left, right))

  private def innerJoin(
      leftRows: Vector[Row], rightRows: Vector[Row], cond: Option[Expression]
  ): Vector[Row] =
    for
      l <- leftRows
      r <- rightRows
      if matchesCond(l, r, cond)
    yield mergeRows(l, r)

  private def leftOuterJoin(
      leftRows: Vector[Row], rightRows: Vector[Row], cond: Option[Expression]
  ): Vector[Row] =
    leftRows.flatMap { l =>
      val matches = rightRows.filter(r => matchesCond(l, r, cond))
      if matches.isEmpty then
        Vector(mergeRows(l, rightRows.headOption.map(nullRow).getOrElse(Row(Map.empty))))
      else
        matches.map(r => mergeRows(l, r))
    }

  private def fullOuterJoin(
      leftRows: Vector[Row], rightRows: Vector[Row], cond: Option[Expression]
  ): Vector[Row] =
    val leftMatched  = scala.collection.mutable.Set.empty[Int]
    val rightMatched = scala.collection.mutable.Set.empty[Int]

    val inner = for
      (l, li) <- leftRows.zipWithIndex
      (r, ri) <- rightRows.zipWithIndex
      if matchesCond(l, r, cond)
    yield
      leftMatched  += li
      rightMatched += ri
      mergeRows(l, r)

    val leftOnly  = leftRows.zipWithIndex.collect {
      case (l, li) if !leftMatched.contains(li) =>
        mergeRows(l, rightRows.headOption.map(nullRow).getOrElse(Row(Map.empty)))
    }
    val rightOnly = rightRows.zipWithIndex.collect {
      case (r, ri) if !rightMatched.contains(ri) =>
        mergeRows(leftRows.headOption.map(nullRow).getOrElse(Row(Map.empty)), r)
    }

    inner ++ leftOnly ++ rightOnly

  // ---------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------

  private def toDouble(v: Any): Double =
    v match
      case n: Int                  => n.toDouble
      case n: Long                 => n.toDouble
      case n: Double               => n
      case n: Float                => n.toDouble
      case n: java.lang.Integer    => n.toDouble
      case n: java.lang.Long       => n.toDouble
      case n: java.lang.Double     => n
      case n: java.lang.Float      => n.toDouble
      case other =>
        throw IllegalArgumentException(s"Cannot apply numeric operation to: $other (${if other == null then "null" else other.getClass.getSimpleName})")

  private def nameOf(expr: Expression): String =
    expr match
      case ColumnRef(name) => name
      case _               => "value"
