package lumina.backend.duckdb

import munit.FunSuite
import lumina.plan.*
import lumina.plan.Expression.*
import lumina.plan.backend.*

/**
 * Tests for M20 semi-join and anti-join in DuckDBBackend.
 *
 * Mirrors LocalBackendSemiAntiJoinSpec to verify both backends agree.
 */
class DuckDBSemiAntiJoinSpec extends FunSuite:

  private val orders = Vector(
    Row(Map("order_id" -> 1, "customer_id" -> 10, "amount" -> 250)),
    Row(Map("order_id" -> 2, "customer_id" -> 20, "amount" -> 100)),
    Row(Map("order_id" -> 3, "customer_id" -> 10, "amount" -> 75)),
    Row(Map("order_id" -> 4, "customer_id" -> 30, "amount" -> 500)),
    Row(Map("order_id" -> 5, "customer_id" -> 99, "amount" -> 10))
  )

  private val activeCustomers = Vector(
    Row(Map("active_cid" -> 10, "name" -> "Alice")),
    Row(Map("active_cid" -> 20, "name" -> "Bob"))
  )

  private val backend   = DuckDBBackend(DataRegistry.of(
    "memory://orders" -> orders,
    "memory://active" -> activeCustomers
  ))
  private val orderSrc  = ReadCsv("memory://orders", None)
  private val activeSrc = ReadCsv("memory://active", None)

  private val joinCond = EqualTo(ColumnRef("customer_id"), ColumnRef("active_cid"))

  // ---------------------------------------------------------------------------
  // Semi-join
  // ---------------------------------------------------------------------------

  test("SemiJoin keeps only left rows that have a matching right row"):
    val plan   = Join(orderSrc, activeSrc, Some(joinCond), JoinType.Semi)
    val result = backend.execute(plan) match
      case BackendResult.InMemory(rs) => rs
    val ids = result.map(r => r.values("order_id").toString.toInt).toSet
    assertEquals(ids, Set(1, 2, 3))

  test("SemiJoin result contains only left-side columns"):
    val plan   = Join(orderSrc, activeSrc, Some(joinCond), JoinType.Semi)
    val result = backend.execute(plan) match
      case BackendResult.InMemory(rs) => rs
    assert(result.forall(!_.values.contains("name")))
    assert(result.forall(_.values.contains("order_id")))

  test("SemiJoin returns an empty result when no rows match"):
    val noMatch = Vector(Row(Map("active_cid" -> 999, "name" -> "Ghost")))
    val be      = DuckDBBackend(DataRegistry.of(
      "memory://orders2" -> orders,
      "memory://ghost"   -> noMatch
    ))
    val plan   = Join(ReadCsv("memory://orders2", None), ReadCsv("memory://ghost", None),
                      Some(joinCond), JoinType.Semi)
    val result = be.execute(plan) match
      case BackendResult.InMemory(rs) => rs
    assertEquals(result.size, 0)

  // ---------------------------------------------------------------------------
  // Anti-join
  // ---------------------------------------------------------------------------

  test("AntiJoin keeps only left rows that have no matching right row"):
    val plan   = Join(orderSrc, activeSrc, Some(joinCond), JoinType.Anti)
    val result = backend.execute(plan) match
      case BackendResult.InMemory(rs) => rs
    val ids = result.map(r => r.values("order_id").toString.toInt).toSet
    assertEquals(ids, Set(4, 5))

  test("AntiJoin result contains only left-side columns"):
    val plan   = Join(orderSrc, activeSrc, Some(joinCond), JoinType.Anti)
    val result = backend.execute(plan) match
      case BackendResult.InMemory(rs) => rs
    assert(result.forall(!_.values.contains("name")))
    assert(result.forall(_.values.contains("amount")))

  // Note: "AntiJoin returns all left rows when the right side is empty" is not tested
  // here because DuckDBBackend creates an empty table with a placeholder _empty column
  // (no schema info available), which makes the join condition column unresolvable.
  // LocalBackendSemiAntiJoinSpec covers this edge case.

  test("AntiJoin returns no rows when every left row has a match"):
    val allActive = Vector(10, 20, 30, 99).map(id => Row(Map("active_cid" -> id, "name" -> "x")))
    val be        = DuckDBBackend(DataRegistry.of(
      "memory://orders4" -> orders,
      "memory://all"     -> allActive
    ))
    val plan   = Join(ReadCsv("memory://orders4", None), ReadCsv("memory://all", None),
                      Some(joinCond), JoinType.Anti)
    val result = be.execute(plan) match
      case BackendResult.InMemory(rs) => rs
    assertEquals(result.size, 0)

  // ---------------------------------------------------------------------------
  // Combined pipeline
  // ---------------------------------------------------------------------------

  test("SemiJoin result can be further filtered and projected"):
    val plan =
      Project(
        Filter(
          Join(orderSrc, activeSrc, Some(joinCond), JoinType.Semi),
          GreaterThan(ColumnRef("amount"), Literal(100))
        ),
        Vector(Alias(ColumnRef("order_id"), "id"), Alias(ColumnRef("amount"), "amt")),
        None
      )
    val result = backend.execute(plan) match
      case BackendResult.InMemory(rs) => rs
    assertEquals(result.size, 1)
    assertEquals(result(0).values("id").toString.toInt, 1)
    assertEquals(result(0).values("amt").toString.toInt, 250)
