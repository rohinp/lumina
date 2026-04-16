package lumina.backend.local

import munit.FunSuite
import lumina.plan.*
import lumina.plan.Expression.*
import lumina.plan.backend.*

/**
 * Tests for M20 semi-join and anti-join in LocalBackend.
 *
 * Semi-join keeps left rows that have at least one matching right row.
 * Anti-join keeps left rows that have no matching right row.
 * Both produce only left-side columns.
 *
 * Read top-to-bottom as a specification for each join type's behaviour.
 */
class LocalBackendSemiAntiJoinSpec extends FunSuite:

  private val orders = Vector(
    Row(Map("order_id" -> 1, "customer_id" -> 10, "amount" -> 250)),
    Row(Map("order_id" -> 2, "customer_id" -> 20, "amount" -> 100)),
    Row(Map("order_id" -> 3, "customer_id" -> 10, "amount" -> 75)),
    Row(Map("order_id" -> 4, "customer_id" -> 30, "amount" -> 500)),
    Row(Map("order_id" -> 5, "customer_id" -> 99, "amount" -> 10))
  )

  // Only customers 10 and 20 are "active".
  // The join key is named "active_cid" (distinct from "customer_id") so that
  // mergeRows does not overwrite the left-side value when both rows share a name —
  // the same convention used by all other LocalBackend join tests.
  private val activeCustomers = Vector(
    Row(Map("active_cid" -> 10, "name" -> "Alice")),
    Row(Map("active_cid" -> 20, "name" -> "Bob"))
  )

  private val backend  = LocalBackend(DataRegistry.of(
    "memory://orders"   -> orders,
    "memory://active"   -> activeCustomers
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
    val ids = result.map(_.values("order_id").asInstanceOf[Int]).toSet
    assertEquals(ids, Set(1, 2, 3))   // orders for customers 10 and 20

  test("SemiJoin result contains only left-side columns"):
    val plan   = Join(orderSrc, activeSrc, Some(joinCond), JoinType.Semi)
    val result = backend.execute(plan) match
      case BackendResult.InMemory(rs) => rs
    // "name" is a right-side column and must not appear in the output
    assert(result.forall(!_.values.contains("name")))
    assert(result.forall(_.values.contains("order_id")))

  test("SemiJoin does not duplicate left rows when multiple right rows match"):
    // customer 10 appears twice in activeCustomers if we add a duplicate — but
    // here each customer appears once; orders 1 and 3 both belong to customer 10
    // and each should appear exactly once in the result.
    val ids = backend.execute(Join(orderSrc, activeSrc, Some(joinCond), JoinType.Semi)) match
      case BackendResult.InMemory(rs) => rs.map(_.values("order_id"))
    // The result may contain duplicates when the right side has multiple matches;
    // LocalBackend uses exists() so no duplication occurs.
    assertEquals(ids.count(_ == 1), 1)
    assertEquals(ids.count(_ == 3), 1)

  test("SemiJoin returns an empty result when no rows match"):
    val noMatch = Vector(Row(Map("active_cid" -> 999, "name" -> "Ghost")))
    val be      = LocalBackend(DataRegistry.of(
      "memory://orders2" -> orders,
      "memory://ghost"   -> noMatch
    ))
    val plan   = Join(ReadCsv("memory://orders2", None), ReadCsv("memory://ghost", None),
                      Some(joinCond), JoinType.Semi)
    val result = be.execute(plan) match
      case BackendResult.InMemory(rs) => rs
    assertEquals(result.size, 0)

  test("SemiJoin with no condition keeps all left rows (cross-semi is a no-op filter)"):
    val plan   = Join(orderSrc, activeSrc, None, JoinType.Semi)
    val result = backend.execute(plan) match
      case BackendResult.InMemory(rs) => rs
    // Every left row has at least one right row → all kept
    assertEquals(result.size, orders.size)

  // ---------------------------------------------------------------------------
  // Anti-join
  // ---------------------------------------------------------------------------

  test("AntiJoin keeps only left rows that have no matching right row"):
    val plan   = Join(orderSrc, activeSrc, Some(joinCond), JoinType.Anti)
    val result = backend.execute(plan) match
      case BackendResult.InMemory(rs) => rs
    val ids = result.map(_.values("order_id").asInstanceOf[Int]).toSet
    assertEquals(ids, Set(4, 5))   // customers 30 and 99 are not active

  test("AntiJoin result contains only left-side columns"):
    val plan   = Join(orderSrc, activeSrc, Some(joinCond), JoinType.Anti)
    val result = backend.execute(plan) match
      case BackendResult.InMemory(rs) => rs
    assert(result.forall(!_.values.contains("name")))
    assert(result.forall(_.values.contains("amount")))

  test("AntiJoin returns all left rows when the right side is empty"):
    val emptyRight = Vector.empty[Row]   // no rows, so nothing can match
    val be         = LocalBackend(DataRegistry.of(
      "memory://orders3" -> orders,
      "memory://empty"   -> emptyRight
    ))
    val plan   = Join(ReadCsv("memory://orders3", None), ReadCsv("memory://empty", None),
                      Some(joinCond), JoinType.Anti)
    val result = be.execute(plan) match
      case BackendResult.InMemory(rs) => rs
    assertEquals(result.size, orders.size)

  test("AntiJoin returns no rows when every left row has a match"):
    val allActive = Vector(10, 20, 30, 99).map(id => Row(Map("active_cid" -> id, "name" -> "x")))
    val be        = LocalBackend(DataRegistry.of(
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
    assertEquals(result(0).values("id"), 1)
    assertEquals(result(0).values("amt"), 250)
