package lumina.backend.duckdb

import lumina.plan.*
import lumina.plan.backend.{Backend, BackendResult, DataRegistry, Row}

import lumina.plan.backend.{BackendCapabilities, RowNormalizer}
import lumina.plan.optimizer.Optimizer
import java.sql.{Connection, DriverManager, ResultSet}

/**
 * Lumina backend that executes logical plans via DuckDB's in-process JDBC driver.
 *
 * Each call to [[execute]] opens a fresh in-memory DuckDB connection, materialises
 * any `memory://` tables from the [[DataRegistry]], translates the plan to SQL via
 * [[PlanToSql]], executes the query, and closes the connection.
 *
 * This keeps execution stateless — no connection pooling, no shared state between
 * calls — which makes the backend safe to use from multiple threads and easy to test.
 *
 * {{{
 * val backend = DuckDBBackend(DataRegistry.of(
 *   "memory://sales" -> Vector(Row(Map("region" -> "EU", "amount" -> 500.0)))
 * ))
 * val rows = backend.execute(myPlan)
 * }}}
 */
final class DuckDBBackend(registry: DataRegistry) extends Backend:

  override val name: String = "duckdb"

  override val capabilities: BackendCapabilities = BackendCapabilities(
    supportsDistributedExecution = false,
    supportsVectorizedExecution  = true,
    supportsUserDefinedFunctions = false
  )

  override def execute(plan: LogicalPlan): BackendResult =
    val optimized = Optimizer.optimize(plan)
    val conn = DriverManager.getConnection("jdbc:duckdb:")
    try
      createTables(conn)
      val sql  = PlanToSql.toSql(optimized)
      val rs   = conn.createStatement().executeQuery(sql)
      BackendResult.InMemory(RowNormalizer.normalizeAll(collectRows(rs)))
    finally
      conn.close()

  // ---------------------------------------------------------------------------
  // Table creation from DataRegistry
  // ---------------------------------------------------------------------------

  private def createTables(conn: Connection): Unit =
    registry.allEntries.foreach { (path, rows) =>
      val tableName = PlanToSql.tableNameFor(path)
      if rows.nonEmpty then
        createTableFromRows(conn, tableName, rows)
      else
        conn.createStatement().execute(s"""CREATE TABLE "$tableName" (_empty VARCHAR)""")
    }

  private def createTableFromRows(conn: Connection, tableName: String, rows: Vector[Row]): Unit =
    val sample  = rows.head.values
    // Each column definition: "columnName" TYPE
    val colDefs = sample.map { case (name, value) => s"\"$name\" ${sqlTypeFor(value)}" }.mkString(", ")
    conn.createStatement().execute(s"""CREATE TABLE "$tableName" ($colDefs)""")

    val cols         = sample.keys.toSeq
    val placeholders = cols.map(_ => "?").mkString(", ")
    val colNames     = cols.map(c => s"\"$c\"").mkString(", ")
    val insertSql    = s"""INSERT INTO "$tableName" ($colNames) VALUES ($placeholders)"""
    val pstmt        = conn.prepareStatement(insertSql)

    rows.foreach { row =>
      cols.zipWithIndex.foreach { (col, idx) =>
        row.values.get(col).foreach(v => pstmt.setObject(idx + 1, v))
      }
      pstmt.addBatch()
    }
    pstmt.executeBatch()
    pstmt.close()

  private def sqlTypeFor(value: Any): String = value match
    case _: Int     => "INTEGER"
    case _: Long    => "BIGINT"
    case _: Double  => "DOUBLE"
    case _: Float   => "FLOAT"
    case _: Boolean => "BOOLEAN"
    case _          => "VARCHAR"

  // ---------------------------------------------------------------------------
  // ResultSet → Row conversion
  // ---------------------------------------------------------------------------

  private def collectRows(rs: ResultSet): Vector[Row] =
    val meta     = rs.getMetaData
    val colCount = meta.getColumnCount
    val colNames = (1 to colCount).map(meta.getColumnName)
    val buf      = Vector.newBuilder[Row]
    while rs.next() do
      val values = colNames.map(name => name -> rs.getObject(name)).toMap
      buf += Row(values)
    buf.result()

object DuckDBBackend:
  def apply(registry: DataRegistry): DuckDBBackend = new DuckDBBackend(registry)
  def empty: DuckDBBackend = new DuckDBBackend(DataRegistry.empty)
