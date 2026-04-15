package lumina.plan.optimizer

import munit.FunSuite
import lumina.plan.*
import lumina.plan.Expression.*

/**
 * Tests for the ConstantFolding optimizer rule.
 *
 * Read top-to-bottom as a specification.  Each section covers one category of
 * folding so it is easy to see exactly which constant sub-expressions collapse
 * and which are left unchanged.
 */
class ConstantFoldingSpec extends FunSuite:

  private val base = ReadCsv("memory://t", None)
  private val age  = ColumnRef("age")

  // Convenience: apply the rule to an expression directly
  private def fold(expr: Expression): Expression =
    ConstantFolding.foldExpr(expr)

  // ---------------------------------------------------------------------------
  // Logical operators
  // ---------------------------------------------------------------------------

  test("And(true, expr) folds to expr"):
    assertEquals(fold(And(Literal(true), age)), age)

  test("And(expr, true) folds to expr"):
    assertEquals(fold(And(age, Literal(true))), age)

  test("And(false, expr) folds to false"):
    assertEquals(fold(And(Literal(false), age)), Literal(false))

  test("And(expr, false) folds to false"):
    assertEquals(fold(And(age, Literal(false))), Literal(false))

  test("Or(true, expr) folds to true"):
    assertEquals(fold(Or(Literal(true), age)), Literal(true))

  test("Or(false, expr) folds to expr"):
    assertEquals(fold(Or(Literal(false), age)), age)

  test("Or(expr, false) folds to expr"):
    assertEquals(fold(Or(age, Literal(false))), age)

  test("Not(true) folds to false"):
    assertEquals(fold(Not(Literal(true))), Literal(false))

  test("Not(false) folds to true"):
    assertEquals(fold(Not(Literal(false))), Literal(true))

  test("Not(ColumnRef) is left unchanged"):
    assertEquals(fold(Not(age)), Not(age))

  // ---------------------------------------------------------------------------
  // Comparison operators on literals
  // ---------------------------------------------------------------------------

  test("EqualTo of equal literals folds to true"):
    assertEquals(fold(EqualTo(Literal(5), Literal(5))), Literal(true))

  test("EqualTo of unequal literals folds to false"):
    assertEquals(fold(EqualTo(Literal("a"), Literal("b"))), Literal(false))

  test("NotEqualTo of equal literals folds to false"):
    assertEquals(fold(NotEqualTo(Literal(3), Literal(3))), Literal(false))

  test("GreaterThan of literals folds to its boolean result"):
    assertEquals(fold(GreaterThan(Literal(10), Literal(5))), Literal(true))
    assertEquals(fold(GreaterThan(Literal(5), Literal(10))), Literal(false))

  test("LessThan of literals folds to its boolean result"):
    assertEquals(fold(LessThan(Literal(2), Literal(3))), Literal(true))

  test("GreaterThanOrEqual of equal literals folds to true"):
    assertEquals(fold(GreaterThanOrEqual(Literal(5), Literal(5))), Literal(true))

  test("comparison involving a ColumnRef is left unchanged"):
    val expr = GreaterThan(age, Literal(30))
    assertEquals(fold(expr), expr)

  // ---------------------------------------------------------------------------
  // Arithmetic operators on literals
  // ---------------------------------------------------------------------------

  test("Add of two numeric literals folds to their sum"):
    assertEquals(fold(Add(Literal(3), Literal(4))), Literal(7.0))

  test("Subtract of two numeric literals folds to their difference"):
    assertEquals(fold(Subtract(Literal(10), Literal(3))), Literal(7.0))

  test("Multiply of two numeric literals folds to their product"):
    assertEquals(fold(Multiply(Literal(3), Literal(4))), Literal(12.0))

  test("Divide of two numeric literals folds to their quotient"):
    assertEquals(fold(Divide(Literal(10.0), Literal(4.0))), Literal(2.5))

  test("Divide by literal zero is left unchanged to avoid a compile-time error"):
    val expr = Divide(Literal(5), Literal(0))
    assertEquals(fold(expr), expr)

  test("Negate of a numeric literal folds to the negated value"):
    assertEquals(fold(Negate(Literal(7))), Literal(-7.0))

  test("arithmetic expression with a ColumnRef is left unchanged"):
    val expr = Add(age, Literal(1))
    assertEquals(fold(expr), expr)

  // ---------------------------------------------------------------------------
  // Null checks
  // ---------------------------------------------------------------------------

  test("IsNull of a null Literal folds to true"):
    assertEquals(fold(IsNull(Literal(null))), Literal(true))

  test("IsNull of a non-null Literal folds to false"):
    assertEquals(fold(IsNull(Literal(42))), Literal(false))

  test("IsNotNull of a null Literal folds to false"):
    assertEquals(fold(IsNotNull(Literal(null))), Literal(false))

  test("IsNotNull of a non-null Literal folds to true"):
    assertEquals(fold(IsNotNull(Literal("hello"))), Literal(true))

  // ---------------------------------------------------------------------------
  // Plan-level simplification
  // ---------------------------------------------------------------------------

  test("Filter with a Literal(true) condition is removed from the plan"):
    val plan   = Filter(base, Literal(true))
    val result = ConstantFolding(plan)
    assertEquals(result, base)

  test("Filter with a Literal(false) condition is left in place"):
    val plan   = Filter(base, Literal(false))
    val result = ConstantFolding(plan)
    assertEquals(result, plan)

  test("Filter whose condition folds to true is removed"):
    // And(true, ColumnRef) folds to ColumnRef first; then Filter stays.
    // And(true, true) → true → Filter removed.
    val plan   = Filter(base, And(Literal(true), Literal(true)))
    val result = ConstantFolding(plan)
    assertEquals(result, base)

  // ---------------------------------------------------------------------------
  // Integration: Optimizer.optimize uses ConstantFolding as first rule
  // ---------------------------------------------------------------------------

  test("Optimizer removes a trivially-true Filter in the default pipeline"):
    val plan   = Filter(base, And(Literal(true), Literal(true)))
    val result = Optimizer.optimize(plan)
    assertEquals(result, base)

  test("Optimizer folds constants inside a Project column list"):
    val plan   = Project(base, Vector(Alias(Add(Literal(1), Literal(2)), "three")), None)
    val result = Optimizer.optimize(plan)
    result match
      case Project(_, Vector(Alias(Literal(3.0), "three")), _) => ()
      case other => fail(s"Expected folded literal, got: $other")
