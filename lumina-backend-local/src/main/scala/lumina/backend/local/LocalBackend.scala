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
      case WithColumn(child, name, expr)                =>
        run(child).map { row =>
          Row(row.values + (name -> ExpressionEvaluator.evaluate(expr, row)))
        }
      case Sort(child, sortExprs)                       => sort(run(child), sortExprs)
      case Limit(child, count)                          => run(child).take(count)
      case Join(left, right, condition, joinType)       =>
        join(run(left), run(right), condition, joinType)
      case Window(child, windowExprs)                  =>
        windowExprs.foldLeft(run(child))(applyWindowExpr)
      case UnionAll(left, right)                       =>
        run(left) ++ run(right)
      case Distinct(child)                             =>
        // Deduplicate by the full row values map; preserves first occurrence.
        run(child).foldLeft((Vector.empty[Row], Set.empty[Map[String, Any]])) {
          case ((acc, seen), row) =>
            if seen.contains(row.values) then (acc, seen)
            else (acc :+ row, seen + row.values)
        }._1
      case Intersect(left, right)                      =>
        val rightSet = run(right).map(_.values).toSet
        run(left)
          .filter(r => rightSet.contains(r.values))
          .foldLeft((Vector.empty[Row], Set.empty[Map[String, Any]])) {
            case ((acc, seen), row) =>
              if seen.contains(row.values) then (acc, seen)
              else (acc :+ row, seen + row.values)
          }._1
      case Except(left, right)                         =>
        val rightSet = run(right).map(_.values).toSet
        run(left)
          .filterNot(r => rightSet.contains(r.values))
          .foldLeft((Vector.empty[Row], Set.empty[Map[String, Any]])) {
            case ((acc, seen), row) =>
              if seen.contains(row.values) then (acc, seen)
              else (acc :+ row, seen + row.values)
          }._1
      case Sample(child, fraction, seed)               =>
        val rng  = seed.map(new scala.util.Random(_)).getOrElse(new scala.util.Random())
        run(child).filter(_ => rng.nextDouble() < fraction)
      case DropColumns(child, cols)                    =>
        val toDrop = cols.toSet
        run(child).map(row => Row(row.values.filterNot { case (k, _) => toDrop.contains(k) }))
      case RenameColumn(child, oldName, newName)       =>
        run(child).map { row =>
          row.values.get(oldName) match
            case None    => row
            case Some(v) => Row((row.values - oldName) + (newName -> v))
        }

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
        case ColumnRef(name)  => name -> row.values.getOrElse(name, null)
        case Alias(expr, name) => name -> ExpressionEvaluator.evaluate(expr, row)
        case expr             => nameOf(expr) -> ExpressionEvaluator.evaluate(expr, row)
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

          case CountDistinct(colExpr, alias) =>
            val distinct = groupRows
              .map(r => ExpressionEvaluator.evaluate(colExpr, r))
              .filter(_ != null)
              .toSet
            alias.getOrElse(nameOf(colExpr)) -> distinct.size.toLong

          case StdDev(colExpr, alias) =>
            val vals = groupRows
              .map(r => ExpressionEvaluator.evaluate(colExpr, r))
              .filter(_ != null)
              .map(toDouble)
            val result: Any =
              if vals.size <= 1 then null
              else
                val mean     = vals.sum / vals.size
                val variance = vals.map(v => (v - mean) * (v - mean)).sum / (vals.size - 1)
                math.sqrt(variance)
            alias.getOrElse(nameOf(colExpr)) -> result

          case Variance(colExpr, alias) =>
            val vals = groupRows
              .map(r => ExpressionEvaluator.evaluate(colExpr, r))
              .filter(_ != null)
              .map(toDouble)
            val result: Any =
              if vals.size <= 1 then null
              else
                val mean = vals.sum / vals.size
                vals.map(v => (v - mean) * (v - mean)).sum / (vals.size - 1)
            alias.getOrElse(nameOf(colExpr)) -> result
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
      case JoinType.Semi  => leftRows.filter(l => rightRows.exists(r => matchesCond(l, r, condition)))
      case JoinType.Anti  => leftRows.filter(l => !rightRows.exists(r => matchesCond(l, r, condition)))

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
  // Window functions
  // ---------------------------------------------------------------------------

  /**
   * Applies a single WindowExpr to all rows, adding the computed column.
   * Rows are partitioned by spec.partitionBy, sorted within each partition by
   * spec.orderBy, and the window value is computed per row.  Input order is
   * preserved: each row's position in the output matches its position in input.
   */
  private def applyWindowExpr(rows: Vector[Row], we: WindowExpr): Vector[Row] =
    import WindowExpr.*
    val spec = we.spec

    // Tag each row with its original index so we can reassemble in input order
    val indexed = rows.zipWithIndex

    // Group into partitions; empty partitionBy = one partition containing all rows
    val partitions = indexed.groupBy { case (row, _) =>
      spec.partitionBy.map(ExpressionEvaluator.evaluate(_, row))
    }

    val results = new Array[Row](rows.size)

    partitions.foreach { case (_, partRows) =>
      // Sort within partition by the window's ORDER BY (stable sort preserves ties)
      val sorted =
        if spec.orderBy.isEmpty then partRows
        else partRows.sortWith { case ((a, _), (b, _)) =>
          spec.orderBy.iterator.map { se =>
            val av = ExpressionEvaluator.evaluate(se.expr, a)
            val bv = ExpressionEvaluator.evaluate(se.expr, b)
            val cmp = (av, bv) match
              case (sa: String, sb: String) => sa.compareTo(sb)
              case _                        => toDouble(av).compareTo(toDouble(bv))
            if se.ascending then cmp else -cmp
          }.find(_ != 0).getOrElse(0) < 0
        }

      // Compute the window value for each row's sorted position
      val values: IndexedSeq[Any] = we match
        case RowNumber(_, _) =>
          (1 to sorted.size).toVector

        case Rank(_, _) =>
          windowRank(sorted.map(_._1), spec.orderBy, dense = false)

        case DenseRank(_, _) =>
          windowRank(sorted.map(_._1), spec.orderBy, dense = true)

        case WindowAgg(agg, _, _) =>
          val v = computeWindowAgg(sorted.map(_._1), agg)
          Vector.fill(sorted.size)(v)

        case Lag(expr, offset, _, _) =>
          sorted.indices.map { i =>
            if i - offset < 0 then null
            else ExpressionEvaluator.evaluate(expr, sorted(i - offset)._1)
          }

        case Lead(expr, offset, _, _) =>
          sorted.indices.map { i =>
            if i + offset >= sorted.size then null
            else ExpressionEvaluator.evaluate(expr, sorted(i + offset)._1)
          }

      // Write each computed value back to the slot of the row's original index
      sorted.zip(values).foreach { case ((row, origIdx), v) =>
        results(origIdx) = Row(row.values + (we.alias -> v))
      }
    }

    results.toVector

  /**
   * Computes RANK or DENSE_RANK values for rows that are already sorted within
   * their partition.  Ties (identical ORDER BY keys) receive the same rank.
   */
  private def windowRank(
      sortedRows: Vector[Row],
      orderBy: Vector[SortExpr],
      dense: Boolean
  ): Vector[Int] =
    if sortedRows.isEmpty then return Vector.empty
    val result  = new Array[Int](sortedRows.size)
    var i       = 0
    var rank    = 1
    while i < sortedRows.size do
      // Find the end of the current tie group
      val keys = orderBy.map(se => ExpressionEvaluator.evaluate(se.expr, sortedRows(i)))
      var j = i
      while j < sortedRows.size && orderBy.indices.forall { k =>
        ExpressionEvaluator.evaluate(orderBy(k).expr, sortedRows(j)) == keys(k)
      } do
        result(j) = rank
        j += 1
      rank    = if dense then rank + 1 else rank + (j - i)
      i       = j
    result.toVector

  /** Applies an aggregate function over the entire partition (whole-partition window). */
  private def computeWindowAgg(rows: Vector[Row], agg: Aggregation): Any =
    agg match
      case Sum(col, _)         =>
        rows.map(r => toDouble(ExpressionEvaluator.evaluate(col, r))).sum
      case Count(None, _)      =>
        rows.size.toLong
      case Count(Some(col), _) =>
        rows.count(r => ExpressionEvaluator.evaluate(col, r) != null).toLong
      case Avg(col, _)         =>
        val vals = rows.map(r => toDouble(ExpressionEvaluator.evaluate(col, r)))
        if vals.isEmpty then 0.0 else vals.sum / vals.size
      case Min(col, _)         =>
        rows.map(r => ExpressionEvaluator.evaluate(col, r)).minBy(v => toDouble(v))
      case Max(col, _)         =>
        rows.map(r => ExpressionEvaluator.evaluate(col, r)).maxBy(v => toDouble(v))

      case CountDistinct(col, _) =>
        rows.map(r => ExpressionEvaluator.evaluate(col, r)).filter(_ != null).toSet.size.toLong

      case StdDev(col, _) =>
        val vals = rows.map(r => ExpressionEvaluator.evaluate(col, r)).filter(_ != null).map(toDouble)
        if vals.size <= 1 then null
        else
          val mean     = vals.sum / vals.size
          val variance = vals.map(v => (v - mean) * (v - mean)).sum / (vals.size - 1)
          math.sqrt(variance)

      case Variance(col, _) =>
        val vals = rows.map(r => ExpressionEvaluator.evaluate(col, r)).filter(_ != null).map(toDouble)
        if vals.size <= 1 then null
        else
          val mean = vals.sum / vals.size
          vals.map(v => (v - mean) * (v - mean)).sum / (vals.size - 1)

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
