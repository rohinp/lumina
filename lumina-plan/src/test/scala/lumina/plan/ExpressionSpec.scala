package lumina.plan

import munit.FunSuite
import lumina.plan.Expression.*

/**
 * Tests for the Expression AST — each test verifies that an expression can be
 * constructed and that the display helpers render it in human-readable form.
 *
 * These tests also serve as a reference for what expressions exist and how they
 * compose, before looking at how backends evaluate them.
 */
class ExpressionSpec extends FunSuite:

  private def str(expr: Expression): String = LogicalPlanPrinter.exprStr(expr)

  // ---------------------------------------------------------------------------
  // Comparison operators
  // ---------------------------------------------------------------------------

  test("GreaterThan renders as left > right"):
    assertEquals(str(GreaterThan(ColumnRef("age"), Literal(30))), "age > 30")

  test("GreaterThanOrEqual renders as left >= right"):
    assertEquals(str(GreaterThanOrEqual(ColumnRef("age"), Literal(18))), "age >= 18")

  test("LessThan renders as left < right"):
    assertEquals(str(LessThan(ColumnRef("score"), Literal(50))), "score < 50")

  test("LessThanOrEqual renders as left <= right"):
    assertEquals(str(LessThanOrEqual(ColumnRef("score"), Literal(100))), "score <= 100")

  test("EqualTo renders as left = right"):
    assertEquals(str(EqualTo(ColumnRef("city"), Literal("Paris"))), "city = 'Paris'")

  test("NotEqualTo renders as left != right"):
    assertEquals(str(NotEqualTo(ColumnRef("city"), Literal("Berlin"))), "city != 'Berlin'")

  // ---------------------------------------------------------------------------
  // Logical combinators
  // ---------------------------------------------------------------------------

  test("And renders as (left AND right)"):
    val expr = And(GreaterThan(ColumnRef("age"), Literal(18)), LessThan(ColumnRef("age"), Literal(65)))
    assertEquals(str(expr), "(age > 18 AND age < 65)")

  test("Or renders as (left OR right)"):
    val expr = Or(EqualTo(ColumnRef("city"), Literal("Paris")), EqualTo(ColumnRef("city"), Literal("Lyon")))
    assertEquals(str(expr), "(city = 'Paris' OR city = 'Lyon')")

  test("Not renders as NOT expr"):
    assertEquals(str(Not(EqualTo(ColumnRef("city"), Literal("Berlin")))), "NOT city = 'Berlin'")

  // ---------------------------------------------------------------------------
  // Null checks
  // ---------------------------------------------------------------------------

  test("IsNull renders as expr IS NULL"):
    assertEquals(str(IsNull(ColumnRef("email"))), "email IS NULL")

  test("IsNotNull renders as expr IS NOT NULL"):
    assertEquals(str(IsNotNull(ColumnRef("email"))), "email IS NOT NULL")

  // ---------------------------------------------------------------------------
  // Composition
  // ---------------------------------------------------------------------------

  test("complex nested expressions render correctly"):
    val expr = And(
      GreaterThan(ColumnRef("age"), Literal(18)),
      Or(EqualTo(ColumnRef("city"), Literal("Paris")), IsNotNull(ColumnRef("email")))
    )
    assertEquals(str(expr), "(age > 18 AND (city = 'Paris' OR email IS NOT NULL))")
