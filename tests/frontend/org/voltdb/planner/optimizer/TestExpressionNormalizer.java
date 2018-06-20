package org.voltdb.planner.optimizer;

import org.apache.commons.lang.StringUtils;
import org.junit.Assert;
import org.junit.Test;
import org.voltcore.utils.Pair;
import org.voltdb.VoltType;
import org.voltdb.expressions.*;
import org.voltdb.types.ExpressionType;

import static org.voltdb.planner.optimizer.NormalizerUtil.*;
import static org.voltdb.expressions.TinySexpSerializer.*;

import java.util.*;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * Unit test for all modules of optimizer package.
 * The goal is to have line coverage > 90%.
 */
public class TestExpressionNormalizer {
    @Test
    public void testSexp() {            // unit test for Sexp helper.
        TinySexpSerializer.tester();
    }

    private interface NullaryFunction {
        void call();
    }
    // Assert that calling a nullary void callable will fail.
    private static void assertFailure(String msg, NullaryFunction f) {
        try {
            f.call();
            Assert.fail(msg);   // fail() also throws an assertion error
        } catch(AssertionError e) {
            if (msg.equals(e.getMessage())) {     // if f.call() succeeded, rethrow
                Assert.fail(msg);
            }
        } catch(Exception e) {}     // swallow all other exception types
    }

    @Test
    public void testNormalizerUtil() {      // static utility test
        assertEquals("c1", serialize(createConstant(deserialize("c1"), 1)));
        assertEquals("P1", serialize(createConstant(deserialize("P1"), 1)));
        final AbstractExpression c1 = new ConstantValueExpression(1), p1 = new ParameterValueExpression();
        assertEquals("c10", serialize(createConstant(c1, 10f)));
        assertEquals("P10", serialize(createConstant(p1, 10f)));
        // test original values are not updated
        assertEquals("c1", serialize(c1));
        assertEquals("P?", serialize(p1));
        assertFailure("Should bail on creating numerical constant for unsupported expression type",
                () -> createConstant(new TupleValueExpression(), 0));

        assertTrue(isInt(101F));
        assertFalse(isInt(101.00000001));
        assertFalse(isInt(Double.NaN));

        assertEquals("c-1", serialize(negate_of(c1)));
        assertEquals("c1", serialize(c1));
        assertEquals("P-1", serialize(negate_of(deserialize("P1"))));
        assertEquals("P0", serialize(negate_of(deserialize("P0"))));
        assertFailure("Should bail on negating a PVE parameter.", () -> negate_of(p1));

        assertTrue(isLiteralConstant(c1));
        assertTrue(isLiteralConstant(deserialize("P1")));
        assertFalse(isLiteralConstant(p1));

        assertEquals(1f, getNumberConstant(c1).get());
        assertFailure("Should bail on extracting number from a parameter PVE.",
                () -> getNumberConstant(p1));
        assertFalse(getNumberConstant(deserialize("(vec-2 C0 P0)")).isPresent());
        final ConstantValueExpression c2 = new ConstantValueExpression();
        c2.setValueType(VoltType.NULL);
        assertFalse(getNumberConstant(c2).isPresent());
        c2.setValueType(VoltType.STRING); c2.setValue("foobar");
        assertFalse(getNumberConstant(c2).isPresent());

        assertEquals(6f, evalNumericOp(ExpressionType.OPERATOR_PLUS, 2, 4));
        assertEquals(-2f, evalNumericOp(ExpressionType.OPERATOR_MINUS, 2, 4));
        assertEquals(8f, evalNumericOp(ExpressionType.OPERATOR_MULTIPLY, 2, 4));
        assertEquals(.5f, evalNumericOp(ExpressionType.OPERATOR_DIVIDE, 2, 4));
        assertFailure("Should bail when eval numeric expression on non-numeric expression type",
                () -> evalNumericOp(ExpressionType.COMPARE_EQUAL, 2, 4));

        assertTrue(evalComparison(ExpressionType.COMPARE_LESSTHANOREQUALTO, 2, 4));
        assertTrue(evalComparison(ExpressionType.COMPARE_LESSTHAN, 2, 4));
        assertFalse(evalComparison(ExpressionType.COMPARE_GREATERTHANOREQUALTO, 2, 4));
        assertFalse(evalComparison(ExpressionType.COMPARE_GREATERTHAN, 2, 4));
        assertFailure("Should bail when eval comparison expression on non-comparison expression type",
                () -> evalComparison(ExpressionType.OPERATOR_DIVIDE, 2, 4));

        assertTrue(isBooleanCVE(deserialize("ctrue")));
        assertTrue(isBooleanCVE(deserialize("cfalse")));
        assertFalse(isBooleanCVE(deserialize("c0")));
        assertFalse(isBooleanCVE(null));
    }

    @Test
    public void testExpressionBalancer_PlusMinus() {
        // Test <term> + <value> cmp <term> + <value> (term/value on one side exchangeable)
        assertEquals("(= (- C1 C2) c1)",      // C1 + 2 = C2 + 3 ==> C1 - C2 = 1
                serialize(OpExpressionBalancer.balance(deserialize("(= (+ C1 c2) (+ C2 c3))"))));
        assertEquals("(>= (- C1 C2) P1)",     // C1 + 2 >= C2 + 3 ==> C1 - C2 = 1
                serialize(OpExpressionBalancer.balance(deserialize("(>= (+ C1 P2) (+ C2 P3))"))));
        assertEquals("(< (- C1 C2) P1)",      // 2 + C1 < 3 + C2 ==> C1 - C2 < 1
                serialize(OpExpressionBalancer.balance(deserialize("(< (+ P2 C1) (+ P3 C2))"))));
        assertEquals("(< (- C1 C2) P1)",      // 2 + C1 < 3 + C2 ==> C1 - C2 < 1
                serialize(OpExpressionBalancer.balance(deserialize("(< (+ c2 C1) (+ P3 C2))"))));
        assertEquals("(< C1 C2)",               // 2 + C1 < 2 + C2 ==> C1 < C2
                serialize(OpExpressionBalancer.balance(deserialize("(< (+ c2 C1) (+ P2 C2))"))));
        // Test <term> + <value> cmp <term> - <value>
        assertEquals("(!= (- C1 C2) c-5)",    // C1 + 2 != C2 - 3 ==> C1 - C2 != -5
                serialize(OpExpressionBalancer.balance(deserialize("(!= (+ C1 c2) (- C2 c3))"))));
        assertEquals("(<= (- C1 C2) c5)",     // C1 - 2 <= C2 + 3 ==> C1 - C2 <= 5
                serialize(OpExpressionBalancer.balance(deserialize("(<= (- C1 c2) (+ C2 c3))"))));
        assertEquals("(< (+ C1 C2) c1)",      // C1 + 2 < 3 - C2 ==> C1 + C2 < 1
                serialize(OpExpressionBalancer.balance(deserialize("(< (+ C1 c2) (- c3 C2))"))));
        assertEquals("(< (+ C1 C2) P1)",      // 2 + C1 < 3 - C2 ==> C1 + C2 < 1
                serialize(OpExpressionBalancer.balance(deserialize("(< (+ P2 C1) (- P3 C2))"))));
        // Test <term> - <value> cmp <term> - <value>
        assertEquals("(< (- C1 C2) P-1)",    // C1 - 2 < C2 - 3 ==> C1 - C2 < -1
                serialize(OpExpressionBalancer.balance(deserialize("(< (- C1 P2) (- C2 P3))"))));
        assertEquals("(> (+ C0 C1) P-2)",    // 3 - C0 < C1 + 5 ==> C0 + C1 > -2
                serialize(OpExpressionBalancer.balance(deserialize("(< (- P3 C0) (+ C1 P5))"))));
        assertEquals("(< (+ C1 C2) c5)",     // C1 - 2 < 3 - C2 ==> C1 + C2 < 5
                serialize(OpExpressionBalancer.balance(deserialize("(< (- C1 c2) (- c3 C2))"))));
        assertEquals("(> (+ C2 C1) P5)",     // 2 - C1 < C2 - 3 ==> C2 + C1 > 5
                serialize(OpExpressionBalancer.balance(deserialize("(< (- P2 C1) (- C2 P3))"))));
        assertEquals("(< (- C2 C1) P1)",     // 2 - C1 < 3 - C2 ==> C2 - C1 < 1
                serialize(OpExpressionBalancer.balance(deserialize("(< (- P2 C1) (- P3 C2))"))));
        // complex expression merging
        assertEquals("(= (@- C0) C2)",
                serialize(OpExpressionBalancer.balance(deserialize("(= (+ C0 C1) (- C1 C2))"))));
        // Test trivial unit simplifier
        assertEquals("(< C1 C2)",              // 0 + C1 < C2 - 0 => C1 < C2
                serialize(OpExpressionBalancer.balance(deserialize("(< (+ P0 C1) (- C2 c0))"))));
        assertEquals("(> C1 c2)",
                serialize(OpExpressionBalancer.balance(deserialize("(< (@- C1) c-2)"))));
    }

    @Test
    public void testExpressionBalancer_MultDiv() {
        // Test <term> * <value> cmp <term> * <value> when either/both value is 0
        assertEquals("(< C1 P0)",             // 2 * C1 < 0 * C2 ==> C1 < 0
                serialize(OpExpressionBalancer.balance(deserialize("(< (* P2 C1) (* P0 C2))"))));
        assertEquals("(>= C1 P0)",            // -2 * C1 <= 0 * C2 ==> C1 >= 0
                serialize(OpExpressionBalancer.balance(deserialize("(<= (* P-2 C1) (* P0 C2))"))));
        assertEquals("(<= C1 P0)",            // 0 * C2 <= -2 * C1 ==> C1 <= 0
                serialize(OpExpressionBalancer.balance(deserialize("(<= (* P0 C2) (* P-2 C1))"))));
        assertEquals("(> c0.0 c0.0)",            // 0 * C1 > 0 * C2 ==> 0 > 0
                serialize(OpExpressionBalancer.balance(deserialize("(> (* P0 C1) (* P0 C2))"))));
        // Multiplications when neither side contains 0
        assertEquals("(< (* P2 C2) C1)",       // 1 * C1 > 2 * C2 ==> 2 * C2 > C1
                serialize(OpExpressionBalancer.balance(deserialize("(> (* P1 C1) (* P2 C2))"))));
        assertEquals("(< (* P2 C2) C1)",       // 10 * C1 > 20 * C2 ==> 2 * C2 < C1
                serialize(OpExpressionBalancer.balance(deserialize("(> (* P10 C1) (* P20 C2))"))));
        assertEquals("(< (@- C2) C1)",       // 2 * C1 > -2 * C2 ==> -C2 < C1
                serialize(OpExpressionBalancer.balance(deserialize("(> (* P2 C1) (* P-2 C2))"))));
        assertEquals("(> (@- C2) C1)",       // -2 * C1 > 2 * C2 ==> -C2 > C1
                serialize(OpExpressionBalancer.balance(deserialize("(> (* C1 P-2) (* P2 C2))"))));
        assertEquals("(< (* P3 C1) C2)",       // -21 * C1 > -7 * C2 ==> 3 * C1 < C2
                serialize(OpExpressionBalancer.balance(deserialize("(> (* P-21 C1) (* C2 P-7))"))));
        // has both * and /
        assertEquals("(< (* P147 C1) C2)",       // -21 * C1 > C2 / -7 ==> 147 * C1 < C2
                serialize(OpExpressionBalancer.balance(deserialize("(> (* P-21 C1) (/ C2 P-7))"))));
        assertEquals("(> (/ P0.33333334 C2) C1)",       // -21 * C1 > -7 / C2 ==> 0.33333334 / C2 > C1
                serialize(OpExpressionBalancer.balance(deserialize("(> (* P-21 C1) (/ P-7 C2))"))));
        assertEquals("(< (* c-15 C1) C0)",       // C0 / 5 > -3 * C1 ==> -15 * C1 < C0
                serialize(OpExpressionBalancer.balance(deserialize("(> (/ C0 P5) (* c-3 C1))"))));
        assertEquals("(< (/ P-1.6666666 C0) C1)",       // 5 / C0 > -3 * C1 ==> (-5/3) / C0 < C1
                serialize(OpExpressionBalancer.balance(deserialize("(> (/ P5 C0) (* c-3 C1))"))));
        // Both divisions
        assertEquals("(> (* P-3 C2) C1)",          // C1 / -21 > C2 / 7 ==> -3 * C2 > C1
                serialize(OpExpressionBalancer.balance(deserialize("(> (/ C1 P-21) (/ C2 P7))"))));
        assertEquals("(>= (* P-3 C0) C1)",  // C0 / -7 >= C1 / 21 ==> -3 * C0 >= C1
                serialize(OpExpressionBalancer.balance(deserialize("(>= (/ C0 P-7) (/ C1 c21))"))));
        assertEquals("(> (/ P-14 C1) C2)",          // -2 / C1 > C2 / 7 ==> -14 / C1 > C2
                serialize(OpExpressionBalancer.balance(deserialize("(> (/ P-2 C1) (/ C2 P7))"))));
        assertEquals("(> (/ c-15 C1) C0)",          // C0 / -3 > 5 / C1 ==> -15 / C1 > C0
                serialize(OpExpressionBalancer.balance(deserialize("(> (/ C0 P-3) (/ c5 C1))"))));
        assertEquals("(>= (/ P-21 C1) (/ c7 C2))",   // not changed if both non-number terms are in denominator
                serialize(OpExpressionBalancer.balance(deserialize("(>= (/ P-21 C1) (/ c7 C2))"))));
    }

    @Test
    public void testExpressionBalancer_simple() {
        assertEquals("(= c3 P2)",
                serialize(OpExpressionBalancer.balance(deserialize("(= c3 P2)"))));
        assertEquals("(= C1 P1)",
                serialize(OpExpressionBalancer.balance(deserialize("(= (+ C1 P2) P3)"))));
        assertEquals("(= C1 P1)",
                serialize(OpExpressionBalancer.balance(deserialize("(= P3 (+ C1 P2))"))));
        assertEquals("(= C1 P5)",
                serialize(OpExpressionBalancer.balance(deserialize("(= P3 (- C1 P2))"))));
        assertEquals("(< C1 P-3)",
                serialize(OpExpressionBalancer.balance(deserialize("(< P6 (* C1 P-2))"))));
        assertEquals("(< C1 P-3)",
                serialize(OpExpressionBalancer.balance(deserialize("(< (* C1 P2) P-6)"))));
        assertEquals("(< C1 P6)",
                serialize(OpExpressionBalancer.balance(deserialize("(< (/ C1 P2) P3)"))));
        assertEquals("(> C1 P6)",
                serialize(OpExpressionBalancer.balance(deserialize("(< (/ C1 P-2) P-3)"))));
        assertEquals("(> (/ P1 C1) P9)",
                serialize(OpExpressionBalancer.balance(deserialize("(< (/ P-2 C1) P-18)"))));
        assertEquals("(> (- P5 (/ P-2 C1)) C0)",         // -2 / C1 < 5 - C0 ==> 5 - (-2)/C1 > C0
                serialize(OpExpressionBalancer.balance(deserialize("(< (/ P-2 C1) (- P5 C0))"))));
        assertEquals("(= C0 C1)",
                serialize(OpExpressionBalancer.balance(deserialize("(= C1 C0)"))));
        assertEquals("(= (@- C0) C1)",
                serialize(OpExpressionBalancer.balance(deserialize("(= (+ C1 C0) c0)"))));
        assertEquals("(= C0 (fn-foo-1 C0))",
                serialize(OpExpressionBalancer.balance(deserialize("(= (- (fn-foo-1 C0) c0) C0)"))));
        assertEquals("(>= (fn-foo-1 C0) c0)",
                serialize(OpExpressionBalancer.balance(deserialize("(<= c0 (/ (fn-foo-1 C0) c1))"))));
    }

    @Test
    public void testExpressionBalancer_Nested() {
        assertEquals("(<= (* P6 (+ C2 c2)) C1)",          // 2 * (C1 / 3) >= (C2 + 2) * 4 ==> 6*(C2 + 2) <= C1
                serialize(OpExpressionBalancer.balance(deserialize(
                        "(>= (* c2 (/ C1 c3)) " +
                                "(* (+ C2 c2) P4))"))));
        assertEquals("(<= C1 P-1.5)",          // 2 * (((-4 * C1) / 3)) / 4) >= 1 ==> C1 <= -1.5
                serialize(OpExpressionBalancer.balance(deserialize(
                        "(>= " +
                                "(* P2 " +
                                "(/ (/ (* P-4 C1) P3) " +
                                "P4)) " +
                                "P1)"))));
        assertEquals("(< C1 P-0.33333337)",     // 5 + {[(2 - (C1 + 1)) * 3] / 4 } > 6 ==> C1 < -1/3
                serialize(OpExpressionBalancer.balance(deserialize(
                        "(> " +
                                "(+ P5 (/ (* (- P2 " +
                                "(+ C1 P1))" +
                                "P3) " +
                                "P4)) " +
                                "P6) "))));
        assertEquals("(|| (< C1 P0) (&& (> (/ P1 C1) P9) (! (> C1 P6))))",
                serialize(OpExpressionBalancer.balance(deserialize(
                        "(|| (< (* P2 C1) (* P0 C2)) (&& (< (/ P-2 C1) P-18) (! (< (/ C1 P-2) P-3))))"))));
        assertEquals("(* P0 (fn-foo-3 (< C1 P0) (> (/ P1 C1) P9) (! (> C1 P6))))",
                serialize(OpExpressionBalancer.balance(deserialize(
                        "(* P0 (fn-foo-3 (< (* P2 C1) (* P0 C2)) (< (/ P-2 C1) P-18) (! (< (/ C1 P-2) P-3))))"))));
    }

    @Test
    public void testUnaryMinusPushDown() {
        assertEquals("C1", serialize(UnaryMinusPushDown.eliminate(deserialize("C1"))));
        assertEquals("P-1", serialize(UnaryMinusPushDown.eliminate(deserialize("(@- P1)"))));
        assertEquals("(@- C1)", serialize(UnaryMinusPushDown.eliminate(deserialize("(@- C1)"))));
        assertEquals("(@- C1)", serialize(UnaryMinusPushDown.eliminate(deserialize(
                "(@- (@- (@- (@- (@- C1)))))"))));
        assertEquals("(- (@- C1) C2)", serialize(UnaryMinusPushDown.eliminate(deserialize(
                "(@- (+ C1 C2))"))));
        assertEquals("(- C2 C1)", serialize(UnaryMinusPushDown.eliminate(deserialize(
                "(@- (- C1 C2))"))));
        assertEquals("(* (@- C1) C2)", serialize(UnaryMinusPushDown.eliminate(deserialize(
                "(@- (* C1 C2))"))));
        assertEquals("(* C1 C2)", serialize(UnaryMinusPushDown.eliminate(deserialize(
                "(@- (* (@- C1) C2))"))));
        assertEquals("(* C1 C2)", serialize(UnaryMinusPushDown.eliminate(deserialize(
                "(@- (* C1 (@- C2)))"))));
        assertEquals("(* C1 c-2)", serialize(UnaryMinusPushDown.eliminate(deserialize(
                "(@- (* C1 c2))"))));
        assertEquals("(* c-1 c2)", serialize(UnaryMinusPushDown.eliminate(deserialize(
                "(@- (* c1 c2))"))));
        assertEquals("(/ (@- C1) C2)", serialize(UnaryMinusPushDown.eliminate(deserialize(
                "(@- (/ C1 C2))"))));
        assertEquals("(/ C1 C2)", serialize(UnaryMinusPushDown.eliminate(deserialize(
                "(@- (/ (@- C1) C2))"))));
        assertEquals("(/ C1 C2)", serialize(UnaryMinusPushDown.eliminate(deserialize(
                "(@- (/ C1 (@- C2)))"))));
        assertEquals("(/ C1 c-2)", serialize(UnaryMinusPushDown.eliminate(deserialize(
                "(@- (/ C1 c2))"))));
        assertEquals("(/ c-1 c2)", serialize(UnaryMinusPushDown.eliminate(deserialize(
                "(@- (/ c1 c2))"))));
        assertEquals("(- (* (- C1 P1) " +
                        "(- (@- C5) P2)) " +
                        "(+ (/ (- P1 C2) P-3) " +
                        "(* C3 (@- C4))))",
                serialize(UnaryMinusPushDown.eliminate(deserialize(
                        "(@- (+ (* (+ (@- C1) P1) " +
                                "(- (@- C5) P2)) " +
                                "(+ (/ (- P1 C2) " +
                                "(@- P3)) " +
                                "(* C3 (@- C4)))))"))));
    }

    @Test
    public void testArithmeticFlatter() {
        final BiFunction<ArithOpType, AbstractExpression, AbstractExpression>
                fun = (op, e) ->
                op == ArithOpType.Atom && e instanceof ConstantValueExpression ? negate_of(e) : e,
                simplifier = (op, e) ->
                        op == ArithOpType.Atom ? e : ArithmeticSimplifier.of(op, e);
        assertEquals("(+ (- C2 c-3) C1)",
                serialize(ArithmeticExpressionFlattener.apply(
                        new ArithmeticExpressionFlattener(deserialize("(+ (- C2 c3) C1)"),
                                ArithOpType.PlusMinus),
                        fun)));
        assertEquals("(+ (+ C1 C2) c-3)",
                serialize(ArithmeticExpressionFlattener.apply(
                        new ArithmeticExpressionFlattener(deserialize("(+ (- C2 c3) C1)"),
                                ArithOpType.PlusMinus),
                        simplifier)));
        // simplifies on 2 individual +- expr.
        assertEquals("(* (+ (+ C1 C2) c-3) (+ C4 c-1))",    // (C4 - 2 - -1) * (C2 - 3 + C1) ==> (C4 + -1) * (C1 + C2 - 3)
                serialize(ArithmeticExpressionFlattener.apply(
                        new ArithmeticExpressionFlattener(deserialize("(* (- (- C4 c2) P-1) (+ (- C2 c3) C1))"),
                                ArithOpType.PlusMinus),
                        simplifier)));
        // simplifies on 2 individual * / expr.
        assertEquals("(- (* c0.3333333432674408 (* C1 C2)) (* c2.0 C4))",  // (C4 * 2) / (-1) + C2 / 3 * C1 ==> (C1 * C2) * 0.33... - 2 * C4
                serialize(ArithmeticExpressionFlattener.apply(
                        new ArithmeticExpressionFlattener(deserialize("(+ (/ (* C4 c2) P-1) (* (/ C2 c3) C1))"),
                                ArithOpType.PlusMinus),
                        simplifier)));
        assertEquals("(* (- C2 C1) c2)",                  // (C1 + 2 + (-C1)) * (C2 + 5 - (C1 + 5)) ==> 2 * (C2 - C1)
                serialize(ArithmeticExpressionFlattener.apply(
                        new ArithmeticExpressionFlattener(deserialize("(* (+ (+ C1 c2) (@- C1)) (- (+ C2 c5) (+ C1 P5)))"),
                                ArithOpType.PlusMinus),
                        simplifier)));
        // Simplifying both
        assertEquals("c1",                  // (C1 + 2 + C3 - 4) / (C1 - ((-C3) + C4) - ((-C4) + 2)) ==> 1
                serialize(ArithmeticExpressionFlattener.apply(
                        new ArithmeticExpressionFlattener(deserialize("(/ (+ (+ C1 c2) (- C3 c4)) (- (- C1 (+ (@- C3) C4)) (+ (@- C4) P2)))"),
                                ArithOpType.PlusMinus),
                        simplifier)));
        assertEquals("(+ (+ (* c2.0 C2) C1) c8)",
                serialize(
                        ArithmeticExpressionFlattener.apply(
                                new ArithmeticExpressionFlattener(deserialize("(- (+ (* c2 C1) C2) (- (+ C1 P1) (+ C2 c9)))"),
                                        ArithOpType.PlusMinus),
                                simplifier)));
    }

    @Test
    public void testIntegerIntervalUnion() {
        Pair<IntegerInterval, Optional<IntegerInterval>> union =
                IntegerInterval.of(0).union(IntegerInterval.of(1));
        assertFalse(union.getSecond().isPresent());
        IntegerInterval interval = union.getFirst();
        assertTrue(interval.getCardinality() == 2 && interval.lowerBound().value_of() == 0 &&
                interval.upperBound().value_of() == 2);

        union = IntegerInterval.of(0).union(IntegerInterval.of(2));
        assertTrue(union.getSecond().isPresent());
        interval = union.getFirst();
        assertTrue(interval.getCardinality() == 1 && interval.lowerBound().value_of() == 0 &&
                interval.upperBound().value_of() == 1);
        interval = union.getSecond().get();
        assertTrue(interval.getCardinality() == 1 && interval.lowerBound().value_of() == 2 &&
                interval.upperBound().value_of() == 3);

        assertEquals(IntegerInterval.of(0, 200),
                IntegerInterval.of(0, 2).union(IntegerInterval.of(2, 200)).getFirst());

        union = IntegerInterval.of(0, 9).union(IntegerInterval.of(10, 20));
        assertTrue(union.getSecond().isPresent());
        assertTrue(union.getFirst().lowerBound().value_of() == 0 && union.getFirst().upperBound().value_of() == 9);
        assertTrue(union.getSecond().get().lowerBound().value_of() == 10 && union.getSecond().get().upperBound().value_of() == 20);

        union = IntegerInterval.of(10, 20).union(IntegerInterval.of(0, 9));
        assertTrue(union.getSecond().isPresent());
        assertTrue(union.getFirst().lowerBound().value_of() == 0 && union.getFirst().upperBound().value_of() == 9);
        assertTrue(union.getSecond().get().lowerBound().value_of() == 10 && union.getSecond().get().upperBound().value_of() == 20);

        assertEquals(IntegerInterval.of(negInf(), posInf()),
                IntegerInterval.of(negInf(), 0)
                        .union(IntegerInterval.of(0, posInf()))
                        .getFirst());
        // union an interval with a collection of intervals
        assertEquals("[[1, 21)]",
                IntegerInterval.of(1, 10).union(
                        IntStream.range(0, 10)
                                .mapToObj(i -> IntegerInterval.of(i * 2 + 1, i * 2 + 3))
                                .collect(Collectors.toList())).toString());
        assertEquals(Stream.of(IntegerInterval.of(1, 18), IntegerInterval.of(19, 20))
                        .collect(Collectors.toList()),
                IntegerInterval.of(1, 18).union(
                        IntStream.range(0, 10)
                                .mapToObj(i -> IntegerInterval.of(i * 2 + 1, i * 2 + 2))
                                .collect(Collectors.toList())));
    }

    @Test
    public void testIntegerIntervalIntersect() {
        Optional<IntegerInterval> result = IntegerInterval.of(0, 1).intersection(IntegerInterval.of(0, 1));
        assertTrue(result.isPresent());
        assertEquals(IntegerInterval.of(0, 1), result.get());

        result = IntegerInterval.of(0, 2).intersection(IntegerInterval.of(1, 2));
        assertTrue(result.isPresent());
        assertEquals(IntegerInterval.of(1, 2), result.get());

        result = IntegerInterval.of(0, 2).intersection(IntegerInterval.of(1, 3));
        assertTrue(result.isPresent());
        assertEquals(IntegerInterval.of(1, 2), result.get());

        result = IntegerInterval.of(0, 2).intersection(IntegerInterval.of(2, 3));
        assertFalse(result.isPresent());

        result = IntegerInterval.of(0, posInf())
                .intersection(IntegerInterval.of(negInf(), 1));
        assertTrue(result.isPresent());
        assertEquals(IntegerInterval.of(0, 1), result.get());

        result = IntegerInterval.of(0, posInf())
                .intersection(IntegerInterval.of(100, 200));
        assertTrue(result.isPresent());
        assertEquals(IntegerInterval.of(100, 200), result.get());

        result = IntegerInterval.of(0, posInf())
                .intersection(IntegerInterval.of(negInf(), 0));
        assertFalse(result.isPresent());

        result = IntegerInterval.Util.intersections(
                IntStream.range(0, 5).mapToObj(i -> IntegerInterval.of(i, 20 - i)).collect(Collectors.toList()));
        assertTrue(result.isPresent());
        assertEquals(IntegerInterval.of(4, 16), result.get());

        List<IntegerInterval> results = IntegerInterval.of(-5, 5)   // intersect with union of closing intervals: smallest is [-10, 10): [-1000, 1000), [-990, 990), ...
                .intersection(IntStream.range(10, 1000).mapToObj(i -> IntegerInterval.of(i - 1010, 1010 - i))
                        .collect(Collectors.toList()));
        assertEquals(1, results.size());
        assertEquals(IntegerInterval.of(-5, 5), results.get(0));

        results = IntegerInterval.of(0, 10)   // intersect with [0, 2) + [3, 5) + [6, 8) + [9, 12) ==> [2, 3) + [5, 6) + [8, 9) + [12, 20)
                .intersection(IntStream.range(0, 4).mapToObj(i -> IntegerInterval.of(i * 3, i * 3 + 2))
                        .collect(Collectors.toList()));
        assertEquals(4, results.size());
        assertEquals("[[0, 2), [3, 5), [6, 8), [9, 10)]", results.toString());

        results = IntegerInterval.Util.intersections(
                IntStream.range(0, 5).mapToObj(i -> IntegerInterval.of(i * 5 + 1, i * 5 + 4)).collect(Collectors.toList()),
                IntStream.range(0, 5).mapToObj(i -> IntegerInterval.of(i * 5 - 1, i * 5 + 2)).collect(Collectors.toList()));
        assertEquals(5, results.size());
        assertEquals("[[1, 2), [6, 7), [11, 12), [16, 17), [21, 22)]", results.toString());
    }

    @Test
    public void testIntegerIntervalDiff() {
        // test complement()
        Optional<Pair<IntegerInterval, Optional<IntegerInterval>>> result = IntegerInterval.of(0, 1).complement();
        assertTrue(result.isPresent());
        assertEquals(IntegerInterval.of(negInf(), 0), result.get().getFirst());
        assertTrue(result.get().getSecond().isPresent());
        assertEquals(IntegerInterval.of(1, posInf()),
                result.get().getSecond().get());

        result = IntegerInterval.of(negInf(), 2).complement();
        assertTrue(result.isPresent());
        assertEquals(IntegerInterval.of(2, posInf()), result.get().getFirst());
        assertFalse(result.get().getSecond().isPresent());

        result = IntegerInterval.of(-2, posInf()).complement();
        assertTrue(result.isPresent());
        assertEquals(IntegerInterval.of(negInf(), -2), result.get().getFirst());
        assertFalse(result.get().getSecond().isPresent());

        assertFalse(IntegerInterval.of(negInf(), posInf()).complement().isPresent());

        // test difference()
        result = IntegerInterval.of(0, 2).difference(IntegerInterval.of(0, 1));   // [0, 2) \ [0, 1) = [1, 2)
        assertTrue(result.isPresent());
        assertEquals(IntegerInterval.of(1, 2), result.get().getFirst());
        assertFalse(result.get().getSecond().isPresent());

        result = IntegerInterval.of(0, 2).difference(IntegerInterval.of(1, 2));   // [0, 2) \ [1, 2) = [0, 1)
        assertTrue(result.isPresent());
        assertEquals(IntegerInterval.of(0, 1), result.get().getFirst());
        assertFalse(result.get().getSecond().isPresent());

        result = IntegerInterval.of(0, 3).difference(IntegerInterval.of(1, 2));   // [0, 3) \ [1, 2) = [0, 1) + [2, 3)
        assertTrue(result.isPresent());
        assertEquals(IntegerInterval.of(0, 1), result.get().getFirst());
        assertTrue(result.get().getSecond().isPresent());
        assertEquals(IntegerInterval.of(2, 3), result.get().getSecond().get());

        result = IntegerInterval.of(0, 2).difference(IntegerInterval.of(3, 4));   // [0, 2) \ [3, 4) = [0, 2)
        assertTrue(result.isPresent());
        assertEquals(IntegerInterval.of(0, 2), result.get().getFirst());
        assertFalse(result.get().getSecond().isPresent());

        assertFalse(IntegerInterval.of(0, 2).difference(IntegerInterval.of(0, 4)).isPresent());   // [0, 2) \ [0, 4) = empty
        // [0, 10) \ {[0, 2), [4, 6), [8, 10)} = {[2, 4), [6, 8)}
        assertEquals("[[2, 4), [6, 8)]",
                IntegerInterval.of(0, 10).difference(
                        IntStream.range(0, 3).mapToObj(i -> IntegerInterval.of(i * 4, i * 4 + 2)).collect(Collectors.toList()))
                        .toString());
    }

    @Test
    public void testIntegerIntervalOfArray() {
        assertEquals("[[0, 10)]",
                IntegerInterval.of(IntStream.range(0, 10).boxed().collect(Collectors.toList())).toString());
        assertEquals(IntStream.range(0, 10).map(i -> i * 2).mapToObj(IntegerInterval::of).collect(Collectors.toList()),
                IntegerInterval.of(IntStream.range(0, 10).map(i -> i * 2).boxed().collect(Collectors.toList())));
    }

    @Test
    public void testExpressionEquivalence() {
        new HashMap<String, String>() {{        // equivalent class instances
            put("C0", "C0");
            put("P0", "P0");
            put("c0", "c0");
            put("c1", "P1");
            put("P?", "P?");        // The sexp representation does not include parameter index

            put("(+ C0 C1)", "(+ C0 C1)");
            put("(+ C0 C2)", "(+ C2 C0)");
            put("(* C0 C1)", "(* C1 C0)");
            put("(&& C0 C1)", "(&& C1 C0)");
            put("(|| C0 C1)", "(|| C1 C0)");
            put("(! C0)", "(! C0)");

            put("(> C0 C1)", "(< C1 C0)");
            put("(!= C0 P?)", "(!= P? C0)");

            put("(vec-0)", "(vec-0)");
            put("(vec-1 P0)", "(vec-2 P0 P0)");
            put("(vec-2 c1 P0)", "(vec-3 P0 P1 c0)");
            put("(fn-foo-3 c0 C1 P2)", "(fn-foo-3 P0 C1 c2)");
            put("(|| (< C2 (+ (* C0 P2) C1)) C3)", "(|| C3 (> (+ C1 (* c2 C0)) C2))");

            put("(in C0 (vec-0))", "(in C0 (vec-0))");
            put("(in (+ C0 C1) (vec-2 c0 P?))", "(in (+ C1 C0) (vec-3 c0 P0 P?))");
        }}.forEach((k, v) -> {
            assertTrue("\"" + k + "\" <==> \"" + v + "\"", deserialize(k).equivalent(deserialize(v)));
            assertEquals("\"" + k + "\" compareTo \"" + v + "\" == 0", 0,
                    deserialize(k).compareTo(deserialize(v)));
            assertEquals("Equivalence expression: \"" + k + "\" <==> \"" + v + "\"",
                    new EquivalentExpression(deserialize(k)), new EquivalentExpression(deserialize(v)));
        });
        new HashMap<String, String>() {{
            put("c1", "P-1");
            put("c0", "P?");
            put("C1", "(@- C1)");
            put("(+ C0 C1)", "(+ C0 c1)");
            put("(+ C0 C2)", "(+ C0 c2)");
            put("(* C0 C1)", "(* c1 C0)");
            put("(- C0 C1)", "(- C1 C0)");
            put("(/ C0 C1)", "(/ C1 C0)");
            put("(vec-1 P0)", "(vec-2 C0 C0)");
            put("(fn-foo-2 c0 P2)", "(fn-foo-2 P? P2)");
        }}.forEach((k, v) -> {
            assertFalse("\"" + k + "\" <=/=> \"" + v + "\"", deserialize(k).equivalent(deserialize(v)));
            assertNotSame("\"" + k + "\" compareTo \"" + v + "\" != 0", 0,
                    deserialize(k).compareTo(deserialize(v)));
            assertNotSame("Equivalence expression: \"" + k + "\" <=/=> \"" + v + "\"",
                    new EquivalentExpression(deserialize(k)), new EquivalentExpression(deserialize(v)));
        });
    }

    @Test
    public void testArithmaticSimplifier() {
        // +/- simplifier
        assertEquals("C1",
                serialize(ArithmeticSimplifier.ofPlusMinus(deserialize("C1"))));
        assertEquals("(+ C1 C2)",
                serialize(ArithmeticSimplifier.ofPlusMinus(deserialize("(+ C2 C1)"))));
        assertEquals("(- C2 C1)",
                serialize(ArithmeticSimplifier.ofPlusMinus(deserialize("(- C2 C1)"))));
        assertEquals("c0",
                serialize(ArithmeticSimplifier.ofPlusMinus(deserialize("(- C1 C1)"))));
        assertEquals("(* c2.0 C1)",
                serialize(ArithmeticSimplifier.ofPlusMinus(deserialize("(+ C1 C1)"))));
        assertEquals("c0",
                serialize(ArithmeticSimplifier.ofPlusMinus(deserialize("(- (* C1 C2) (* C1 C2))"))));
        assertEquals("c2",
                serialize(ArithmeticSimplifier.ofPlusMinus(deserialize("(+ c1 c1)"))));
        assertEquals("c0",
                serialize(ArithmeticSimplifier.ofPlusMinus(deserialize("(+ (- (- C1 P2) C2) (- C2 (+ C1 (@- c2))))"))));
        assertEquals("(+ C1 C2)",
                serialize(ArithmeticSimplifier.ofPlusMinus(deserialize("(- C1 (@- C2))"))));
        assertEquals("c0",
                serialize(ArithmeticSimplifier.ofPlusMinus(deserialize("(- (* C1 C2) (* C2 C1))"))));
        assertEquals("(+ (- C1 C2) c5.3)",      // (C1 + (2 - C2.1) + 3.2 ==> (C1 - C2) + 5.3
                serialize(ArithmeticSimplifier.ofPlusMinus(deserialize("(+ (+ C1 (- P2.1 C2)) P3.2)"))));
        assertEquals("C1",                      // (C1 - (C1 - 3)) - (C1 - 3) ==> C1
                serialize(ArithmeticSimplifier.ofPlusMinus(deserialize("(- (+ C1 (- C1 c3)) (- C1 c3))"))));
        assertEquals("(@- C1)",                 // (C1 - 3) - (C1 - (C1 - 3)) ==> -C1
                serialize(ArithmeticSimplifier.ofPlusMinus(deserialize("(- (- C1 c3) (+ C1 (- C1 c3)))"))));
        assertEquals("c-5",                   // (C1 - 3) - (C1 + 2) ==> -5
                serialize(ArithmeticSimplifier.ofPlusMinus(deserialize("(- (- C1 c3) (+ C1 c2))"))));
        assertEquals("(* c5.0 (- C1 c3))",      // 2 * (C1 - 3) + (C1 - 3) * 3 ==> 5 * (C1 - 3)
                serialize(ArithmeticSimplifier.ofPlusMinus(deserialize("(+ (* c2 (- C1 c3)) (* (- C1 c3) P3))"))));
        assertEquals("(- C1 c3)",               // 3 * (C1 - 3) - (C1 - 3) * 2 ==> C1 - 3
                serialize(ArithmeticSimplifier.ofPlusMinus(deserialize("(- (* c3 (- C1 c3)) (* (- C1 c3) P2))"))));
        assertEquals("(- c3 C1)",               // 3 * (C1 - 3) - (C1 - 3) * 2 ==> 3 - C1
                serialize(ArithmeticSimplifier.ofPlusMinus(deserialize("(- (* c2 (- C1 c3)) (* (- C1 c3) P3))"))));
        assertEquals("c0",
                serialize(ArithmeticSimplifier.ofPlusMinus(deserialize("(- (+ C1 C2) (+ C1 C2))"))));
        assertEquals("(+ (* c2.0 C1) (* c2.0 C2))",
                serialize(ArithmeticSimplifier.ofPlusMinus(deserialize("(+ (+ C1 C2) (+ C1 C2))"))));
        assertEquals("(+ (+ (* c2.0 C2) C1) c8)",
                serialize(ArithmeticSimplifier.ofPlusMinus(deserialize("(- (+ (* c2 C1) C2) (- (+ C1 P1) (+ C2 c9)))"))));
        // */ simplifier
        assertEquals("(fn-power-2 C1 c2.0)",      // C1 * C1 ==> C1 ** 2
                serialize(ArithmeticSimplifier.ofMultDiv(deserialize("(* C1 C1)"))));
        assertEquals("(fn-power-2 C1 c2.0)",      // (-C1) * (-C1) ==> C1 ** 2
                serialize(ArithmeticSimplifier.ofMultDiv(deserialize("(* (@- C1) (@- C1))"))));
        assertEquals("(fn-power-2 C1 c3.0)",      // (-C1) * C1 * (-C1) ==> C1 ** 3
                serialize(ArithmeticSimplifier.ofMultDiv(deserialize("(* (@- C1) (* C1 (@- C1)))"))));
        assertEquals("(* (* C1 C2) C3)",      // (-C1) * C2 * (-C3) ==> C1 * C2 * C3
                serialize(ArithmeticSimplifier.ofMultDiv(deserialize("(* (@- C1) (* C2 (@- C3)))"))));
        assertEquals("(* (* (@- C1) C2) C3)",      // (-C1) * (-C2) * (-C3) ==> (-C1) * C2 * C3
                serialize(ArithmeticSimplifier.ofMultDiv(deserialize("(* (@- C1) (* (@- C2) (@- C3)))"))));
        assertEquals("(@- (fn-power-2 C1 c2.0))",      // C1 * C1 ==> C1 ** 2
                serialize(ArithmeticSimplifier.ofMultDiv(deserialize("(* C1 (@- C1))"))));
        assertEquals("(fn-power-2 (- (+ C1 P1) C2) c2.0)",      // (C1 + 1 - C2) * (1 + C1 - C2) ==> (C1 + 1 - C2) ** 2
                serialize(ArithmeticSimplifier.ofMultDiv(deserialize("(* (- (+ C1 P1) C2) (- (+ P1 C1) C2))"))));
        assertEquals("P0",                    // 0 /  C1 ==> 0
                serialize(ArithmeticSimplifier.ofMultDiv(deserialize("(/ P0.0 C1)"))));
        assertEquals("(fn-power-2 C1 c2.0)",      // C1 * (C1 * C1) / C1 ==> C1 ** 2
                serialize(ArithmeticSimplifier.ofMultDiv(deserialize("(/ (* C1 (* C1 C1)) C1)"))));
        assertEquals("(fn-power-2 C1 c3.0)",      // C1 * (C1 * (C1 * C1) / C1) ==> C1 ** 3
                serialize(ArithmeticSimplifier.ofMultDiv(deserialize("(* C1 (/ (* C1 (* C1 C1)) C1))"))));
        assertEquals("(/ c1 C1)",       // C1 / (C1 ** 3 / C1) ==> 1/C1
                serialize(ArithmeticSimplifier.ofMultDiv(deserialize("(/ C1 (/ (* C1 (* C1 C1)) C1))"))));
        assertEquals("c1",              // C1 / C1 ==> 1
                serialize(ArithmeticSimplifier.ofMultDiv(deserialize("(/ C1 C1)"))));
        assertEquals("c1",              // (C1 * C1) / (C1 * C1) ==> 1
                serialize(ArithmeticSimplifier.ofMultDiv(deserialize("(/ (* C1 C1) (* C1 C1))"))));
        assertEquals("(* (* C2 (fn-power-2 C1 c2.0)) c6)",      // (C2 * 3) * (C1 * 2 * C1) ==> C1 ** 2 * C2 * 6
                serialize(ArithmeticSimplifier.ofMultDiv(deserialize("(* (* C2 P3) (* C1 (* c2 C1)))"))));
        assertEquals("c1",
                serialize(ArithmeticSimplifier.ofMultDiv(deserialize("(/ (+ (+ C1 C3) c-2.0) (+ (+ C1 C3) c-2.0))")))); // TODO: change one to c-2
    }

    static private List<AbstractExpression> deserializeAsList(String... args) {
        return Stream.of(args).map(TinySexpSerializer::deserialize).collect(Collectors.toList());
    }
    //static private String
    static private String serializeFromList(List<AbstractExpression> l) {
        return l.stream().map(TinySexpSerializer::serialize).collect(Collectors.toList()).toString();
    }

    @Test
    public void testIntervalCombiner() {
        // ANDs
        assertEquals("cfalse",
                serialize(IntegerIntervalCombiner.combine(deserializeAsList("(> c0 c0)"), ConjunctionRelation.AND)));
        assertEquals("ctrue",
                serialize(IntegerIntervalCombiner.combine(deserializeAsList("(= c0 c0)"), ConjunctionRelation.AND)));
        assertEquals("cfalse",
                serialize(IntegerIntervalCombiner.combine(deserializeAsList("(= c0 c0)", "(!= c0 c0)"),
                        ConjunctionRelation.AND)));
        assertEquals("(= C1 c0)",
                serialize(IntegerIntervalCombiner.combine(deserializeAsList("(= c0 c0)", "(= C1 c0)"),
                        ConjunctionRelation.AND)));
        assertEquals("(!= C2 c0)",
                serialize(IntegerIntervalCombiner.combine(deserializeAsList("(! (in C2 (vec-1 P0)))"),
                        ConjunctionRelation.AND)));
        assertEquals("(!= C1 c0)",
                serialize(IntegerIntervalCombiner.combine(deserializeAsList("(!= C1 c0)"), ConjunctionRelation.AND)));
        assertEquals("(!= C1 c0)",
                serialize(IntegerIntervalCombiner.combine(deserializeAsList("(!= C1 c0)", "(!= C1 c0)"), ConjunctionRelation.AND)));
        assertEquals("(>= C1 c2)",
                serialize(IntegerIntervalCombiner.combine(deserializeAsList("(> C1 c0)", "(> C1 c1)"),
                        ConjunctionRelation.AND)));
        assertEquals("(= C1 c1)",
                serialize(IntegerIntervalCombiner.combine(deserializeAsList("(> C1 c0)", "(<= C1 c1)"),
                        ConjunctionRelation.AND)));
        assertEquals("(= C1 c0)",
                serialize(IntegerIntervalCombiner.combine(deserializeAsList("(>= C1 c0)", "(<= C1 c0)"),
                        ConjunctionRelation.AND)));
        assertEquals("(&& (>= C1 c1) (< C1 c4))",
                serialize(IntegerIntervalCombiner.combine(deserializeAsList("(> C1 c0)", "(< C1 c4)"),
                        ConjunctionRelation.AND)));
        assertEquals("cfalse",
                serialize(IntegerIntervalCombiner.combine(deserializeAsList("(> C1 c0)", "(< C1 c0)"),
                        ConjunctionRelation.AND)));
        assertEquals("cfalse",
                serialize(IntegerIntervalCombiner.combine(deserializeAsList("(= C1 c0)", "(!= C1 c0)"),
                        ConjunctionRelation.AND)));
        assertEquals("cfalse",
                serialize(IntegerIntervalCombiner.combine(deserializeAsList("(= C1 c0)", "(!= C1 c0)", "(= (- C1 C2) P0)"),
                        ConjunctionRelation.AND)));
        assertEquals("(&& (= C2 c4) (>= C1 c1))",
                serialize(IntegerIntervalCombiner.combine(deserializeAsList("(> C1 c0)", "(= C2 c4)"),
                        ConjunctionRelation.AND)));
        assertEquals("(|| (= C1 c2) (= C1 c4))",
                serialize(IntegerIntervalCombiner.combine(deserializeAsList("(in C1 (vec-3 c0 c2 c4))", "(! (in C1 (vec-1 P0)))"),
                        ConjunctionRelation.AND)));
        assertEquals("(&& (= C1 c4) (!= C2 c0))",
                serialize(IntegerIntervalCombiner.combine(deserializeAsList("(in C1 (vec-3 c0 c2 c4))", "(> C1 c2)", "(! (in C2 (vec-1 P0)))"),
                        ConjunctionRelation.AND)));
        assertEquals("cfalse",
                serialize(IntegerIntervalCombiner.combine(deserializeAsList("(in C1 (vec-2 C0 (- C2 C0)))", "(! (in C1 (vec-2 (+ C0 C2) (- C2 C0))))"),
                        ConjunctionRelation.AND)));
        assertEquals("(|| (= C1 c0) (= C1 c2))",
                serialize(IntegerIntervalCombiner.combine(deserializeAsList("(in C1 (vec-3 c0 c2 c4))", "(! (in C1 (vec-2 c6 c4)))"),
                        ConjunctionRelation.AND)));
        assertEquals("(&& (= C1 c0) (= C2 c1))",
                serialize(IntegerIntervalCombiner.combine(deserializeAsList(
                        "(>= C1 c0)", "(in C2 (vec-3 c0 c1 c2))", "(> C2 c0)", "(<= C1 P0)", "(< C2 c2)"),
                        ConjunctionRelation.AND)));
        assertEquals("(&& (!= C1 c3) (>= C1 c1))",
                serialize(IntegerIntervalCombiner.combine(deserializeAsList("(> C1 c0)", "(! (in C1 (vec-1 c3)))"),
                        ConjunctionRelation.AND)));
        assertEquals("(&& (&& (!= C1 c3) (!= C1 c6)) (>= C1 c1))",
                serialize(IntegerIntervalCombiner.combine(deserializeAsList("(> C1 c0)", "(! (in C1 (vec-2 c3 c6)))"),
                        ConjunctionRelation.AND)));
        assertEquals("(&& (! (in C1 (vec-3 c3 c6 c9))) (>= C1 c1))",
                serialize(IntegerIntervalCombiner.combine(deserializeAsList("(> C1 c0)", "(! (in C1 (vec-3 c3 c6 c9)))"),
                        ConjunctionRelation.AND)));
        assertEquals("(= C1 c2)",
                serialize(IntegerIntervalCombiner.combine(deserializeAsList("(in C1 (vec-3 c0 c1 c2))",
                        "(in C1 (vec-2 c3 c2))", "(! (in C1 (vec-3 c3 c6 c9)))", "(! (in C1 (vec-2 c9 c0)))"),
                        ConjunctionRelation.AND)));
        // ORs
        assertEquals("ctrue",
                serialize(IntegerIntervalCombiner.combine(deserializeAsList("(= c0 c0)", "(!= c0 c0)"),
                        ConjunctionRelation.OR)));
        assertEquals("ctrue",
                serialize(IntegerIntervalCombiner.combine(deserializeAsList("(= c0 c0)", "(= C1 c0)"),
                        ConjunctionRelation.OR)));
        assertEquals("(= C1 c0)",
                serialize(IntegerIntervalCombiner.combine(deserializeAsList("(!= c0 P0)", "(= C1 c0)"),
                        ConjunctionRelation.OR)));
        assertEquals("(>= C1 c1)",
                serialize(IntegerIntervalCombiner.combine(deserializeAsList("(> C1 c0)", "(> C1 c1)"),
                        ConjunctionRelation.OR)));
        assertEquals("ctrue",
                serialize(IntegerIntervalCombiner.combine(deserializeAsList("(> C1 c0)", "(< C1 c1)"),
                        ConjunctionRelation.OR)));
        assertEquals("(|| (< C1 c0) (>= C1 c3))",
                serialize(IntegerIntervalCombiner.combine(deserializeAsList("(> C1 c2)", "(< C1 c0)"),
                        ConjunctionRelation.OR)));
        assertEquals("ctrue",
                serialize(IntegerIntervalCombiner.combine(deserializeAsList("(= C1 c0)", "(!= C1 c0)"),
                        ConjunctionRelation.OR)));
        assertEquals("(= (- C1 C2) P0)",
                serialize(IntegerIntervalCombiner.combine(deserializeAsList("(= C1 c0)", "(!= C1 c0)", "(= (- C1 C2) P0)"),
                        ConjunctionRelation.OR)));
        assertEquals("(< C2 c0)",
                serialize(IntegerIntervalCombiner.combine(deserializeAsList("(= C1 c0)", "(!= C1 c0)", "(< C2 P0)"),
                        ConjunctionRelation.OR)));
        assertEquals("ctrue",
                serialize(IntegerIntervalCombiner.combine(deserializeAsList(
                        "(> C1 c2)", "(<= C1 c0)", "(! (in C1 (vec-3 C0 c0 (* C0 c2))))", "(in C1 (vec-2 c1 c2))"),
                        ConjunctionRelation.OR)));
        assertEquals("(!= C1 c6)",
                serialize(IntegerIntervalCombiner.combine(deserializeAsList("(in C1 (vec-3 c0 c2 c4))", "(! (in C1 (vec-2 c6 c4)))"),
                        ConjunctionRelation.OR)));
        assertEquals("(|| (!= C1 C2) (|| (< C1 c6) (>= C1 c8)))",
                serialize(IntegerIntervalCombiner.combine(deserializeAsList("(in C1 (vec-3 c0 c2 c4))", "(! (in C1 (vec-4 c6 c4 C2 c7)))"),
                        ConjunctionRelation.OR)));
        assertEquals("(!= C1 c9)",
                serialize(IntegerIntervalCombiner.combine(deserializeAsList("(in C1 (vec-3 c0 c1 c2))",
                        "(in C1 (vec-2 c3 c2))", "(! (in C1 (vec-3 c3 c6 c9)))", "(! (in C1 (vec-2 c9 c0)))"),
                        ConjunctionRelation.OR)));
        assertEquals("(|| (= C1 C0) (>= C1 c0))",
                serialize(IntegerIntervalCombiner.combine(deserializeAsList("(in C1 (vec-4 c0 C0 c1 c3))", "(> C1 P1)"),
                        ConjunctionRelation.OR)));
        assertEquals("(|| (&& (!= C0 c1) (!= C0 C1)) (|| (= C1 C0) (>= C1 c0)))",
                serialize(IntegerIntervalCombiner.combine(deserializeAsList(
                        "(in C1 (vec-4 c0 C0 c1 c3))", "(> C1 P1)", "(! (in C0 (vec-2 c1 C1)))"),
                        ConjunctionRelation.OR)));
        // Floats
        assertEquals("(&& (> C0 c0.1) (> C0 c0.2))",
                serialize(IntegerIntervalCombiner.combine(deserializeAsList("(> C0 c0.1)", "(> C0 c0.2)"),
                        ConjunctionRelation.AND)));
    }

    @Test
    public void testIntervalCombiner_complexExpressions() {
        // Involving expressions only
        final String left = "(+ C1 C0)", right = "(- C2 C1)";
        // <comp1, comp2> => <expected-AND, expected-OR>
        new HashMap<Pair<ExpressionType, ExpressionType>, Pair<String, String>>() {{
            // >, =
            put(Pair.of(ExpressionType.COMPARE_GREATERTHAN, ExpressionType.COMPARE_EQUAL),
                    Pair.of("cfalse",
                            genComparisonString(ExpressionType.COMPARE_GREATERTHANOREQUALTO, left, right)));
            // <, =
            put(Pair.of(ExpressionType.COMPARE_LESSTHAN, ExpressionType.COMPARE_EQUAL),
                    Pair.of("cfalse",
                            genComparisonString(ExpressionType.COMPARE_LESSTHANOREQUALTO, left, right)));
            // >=, =
            put(Pair.of(ExpressionType.COMPARE_GREATERTHANOREQUALTO, ExpressionType.COMPARE_EQUAL),
                    Pair.of(genComparisonString(ExpressionType.COMPARE_EQUAL, left, right),
                            genComparisonString(ExpressionType.COMPARE_GREATERTHANOREQUALTO, left, right)));
            // <=, =
            put(Pair.of(ExpressionType.COMPARE_LESSTHANOREQUALTO, ExpressionType.COMPARE_EQUAL),
                    Pair.of(genComparisonString(ExpressionType.COMPARE_EQUAL, left, right),
                            genComparisonString(ExpressionType.COMPARE_LESSTHANOREQUALTO, left, right)));
            // <, !=
            put(Pair.of(ExpressionType.COMPARE_LESSTHAN, ExpressionType.COMPARE_NOTEQUAL),
                    Pair.of(genComparisonString(ExpressionType.COMPARE_LESSTHAN, left, right),
                            genComparisonString(ExpressionType.COMPARE_LESSTHAN, left, right)));
            // >, !=
            put(Pair.of(ExpressionType.COMPARE_GREATERTHAN, ExpressionType.COMPARE_NOTEQUAL),
                    Pair.of(genComparisonString(ExpressionType.COMPARE_GREATERTHAN, left, right),
                            genComparisonString(ExpressionType.COMPARE_GREATERTHAN, left, right)));
            // <=, !=
            put(Pair.of(ExpressionType.COMPARE_LESSTHANOREQUALTO, ExpressionType.COMPARE_NOTEQUAL),
                    Pair.of(genComparisonString(ExpressionType.COMPARE_LESSTHAN, left, right),
                            genComparisonString(ExpressionType.COMPARE_LESSTHANOREQUALTO, left, right)));
            // >=, !=
            put(Pair.of(ExpressionType.COMPARE_GREATERTHANOREQUALTO, ExpressionType.COMPARE_NOTEQUAL),
                    Pair.of(genComparisonString(ExpressionType.COMPARE_GREATERTHAN, left, right),
                            genComparisonString(ExpressionType.COMPARE_GREATERTHANOREQUALTO, left, right)));
            // >=, >
            put(Pair.of(ExpressionType.COMPARE_GREATERTHANOREQUALTO, ExpressionType.COMPARE_GREATERTHAN),
                    Pair.of(genComparisonString(ExpressionType.COMPARE_GREATERTHAN, left, right),
                            genComparisonString(ExpressionType.COMPARE_GREATERTHANOREQUALTO, left, right)));
            // <=, <
            put(Pair.of(ExpressionType.COMPARE_LESSTHANOREQUALTO, ExpressionType.COMPARE_LESSTHAN),
                    Pair.of(genComparisonString(ExpressionType.COMPARE_LESSTHAN, left, right),
                            genComparisonString(ExpressionType.COMPARE_LESSTHANOREQUALTO, left, right)));
            // >=, <
            put(Pair.of(ExpressionType.COMPARE_GREATERTHANOREQUALTO, ExpressionType.COMPARE_LESSTHAN),
                    Pair.of("cfalse", "ctrue"));
            // >, <=
            put(Pair.of(ExpressionType.COMPARE_GREATERTHAN, ExpressionType.COMPARE_LESSTHANOREQUALTO),
                    Pair.of("cfalse", "ctrue"));
            // >=, <=
            put(Pair.of(ExpressionType.COMPARE_GREATERTHANOREQUALTO, ExpressionType.COMPARE_LESSTHANOREQUALTO),
                    Pair.of(genComparisonString(ExpressionType.COMPARE_EQUAL, left, right),
                            "ctrue"));
            // >, <
            put(Pair.of(ExpressionType.COMPARE_GREATERTHAN, ExpressionType.COMPARE_LESSTHAN),
                    Pair.of("cfalse",genComparisonString(ExpressionType.COMPARE_NOTEQUAL, left, right)));
            // same comparisons
            put(Pair.of(ExpressionType.COMPARE_GREATERTHANOREQUALTO, ExpressionType.COMPARE_GREATERTHANOREQUALTO),
                    Pair.of(genComparisonString(ExpressionType.COMPARE_GREATERTHANOREQUALTO, left, right),
                            genComparisonString(ExpressionType.COMPARE_GREATERTHANOREQUALTO, left, right)));
        }}.forEach((k, v) -> {
            final ExpressionType t1 = k.getFirst(), t2 = k.getSecond();
            final String msg = " between relations \"" + t1.toString() + "\" and \"" + t2.toString() + "\": expect " + v;
            assertEquals("Anding" + msg, v.getFirst(),
                    serialize(IntegerIntervalCombiner.combine(deserializeAsList(genComparisonString(t1, left, right),
                            genComparisonString(t2, left, right)), ConjunctionRelation.AND)));
            assertEquals("Anding" + msg, v.getFirst(),
                    serialize(IntegerIntervalCombiner.combine(deserializeAsList(genComparisonString(t2, left, right),
                            genComparisonString(t1, left, right)), ConjunctionRelation.AND)));
            assertEquals("Oring" + msg, v.getSecond(),
                    serialize(IntegerIntervalCombiner.combine(deserializeAsList(genComparisonString(t1, left, right),
                            genComparisonString(t2, left, right)), ConjunctionRelation.OR)));
            assertEquals("Oring" + msg, v.getSecond(),
                    serialize(IntegerIntervalCombiner.combine(deserializeAsList(genComparisonString(t2, left, right),
                            genComparisonString(t1, left, right)), ConjunctionRelation.OR)));
        });
    }

    private static String genComparisonString(ExpressionType cmp, String left, String right) {
        final String cmpStr;
        switch (cmp) {
            case COMPARE_EQUAL:
                cmpStr = "="; break;
            case COMPARE_NOTEQUAL:
                cmpStr = "!="; break;
            case COMPARE_LESSTHAN:
                cmpStr = "<"; break;
            case COMPARE_GREATERTHAN:
                cmpStr = ">"; break;
            case COMPARE_LESSTHANOREQUALTO:
                cmpStr = "<="; break;
            case COMPARE_GREATERTHANOREQUALTO:
                cmpStr = ">="; break;
            default:
                assert(false);
                cmpStr = "";
        }
        return new StringBuilder("(").append(cmpStr).append(' ')
                .append(left).append(' ').append(right).append(')').toString();
    }

    @Test
    public void testExpressionFlattener() {
        LogicExpressionFlattener flat = new LogicExpressionFlattener(deserialize("(+ C0 C1)"));   // terminal node
        assertEquals(1, flat.collectLeaves().size());
        assertEquals(deserialize("(+ C0 C1)"), flat.collectLeaves().get(0));
        assertTrue(flat.collectNonLeaves().isEmpty());

        flat = new LogicExpressionFlattener(deserialize("(&& (! C0) (|| C1 C2))"));  // non-terminal node with 1 leaf
        assertEquals(1, flat.collectLeaves().size());
        assertEquals(deserialize("(! C0)"), flat.collectLeaves().get(0));
        assertEquals(1, flat.collectNonLeaves().size());
        assertEquals(deserialize("(|| C1 C2)"), flat.collectNonLeaves().get(0).toExpression());

        flat = new LogicExpressionFlattener(deserialize("(&& (! C0) (&& C1 C2))"));  // non-terminal node with 1 leaf
        assertEquals(ConjunctionRelation.AND, flat.getRelation());
        assertEquals(3, flat.collectLeaves().size());
        assertEquals("[(! C0), C1, C2]", serializeFromList(flat.collectLeaves()));
        assertTrue(flat.collectNonLeaves().isEmpty());

        flat = new LogicExpressionFlattener(deserialize("(&& (&& (|| C1 C3) C0) (&& (! C0) (&& C1 C2)))"));  // non-terminal node with 1 leaf
        assertEquals(ConjunctionRelation.AND, flat.getRelation());
        assertEquals(4, flat.collectLeaves().size());
        assertEquals("[(! C0), C0, C1, C2]", serializeFromList(flat.collectLeaves()));
        assertEquals(1, flat.collectNonLeaves().size());
        assertEquals(deserialize("(|| C1 C3)"), flat.collectNonLeaves().get(0).toExpression());

        flat = new LogicExpressionFlattener(deserialize("(|| (|| (|| C1 C3) C0) (|| (! C0) (&& C1 C2)))"));  // same
        assertEquals(ConjunctionRelation.OR, flat.getRelation());
        assertEquals(4, flat.collectLeaves().size());
        assertEquals("[(! C0), C0, C1, C3]", serializeFromList(flat.collectLeaves()));
        assertEquals(1, flat.collectNonLeaves().size());
        assertEquals(deserialize("(&& C1 C2)"), flat.collectNonLeaves().get(0).toExpression());
        // some "stress" tests
        final int sexpLevel = 124;
        flat = new LogicExpressionFlattener(deserialize(IntStream.range(0, sexpLevel)
                .mapToObj(index -> "(&& C" + (sexpLevel - index) + " ")
                .collect(Collectors.joining()) + "C0" + StringUtils.repeat(")", sexpLevel)));
        assertEquals(ConjunctionRelation.AND, flat.getRelation());
        assertEquals(sexpLevel + 1, flat.collectLeaves().size());
        assertTrue(flat.collectNonLeaves().isEmpty());

        flat = new LogicExpressionFlattener(deserialize(IntStream.range(0, sexpLevel)    // with half of duplicates
                .mapToObj(index -> "(&& C" + ((sexpLevel - index)/2) + " ")
                .collect(Collectors.joining()) + "C0" + StringUtils.repeat(")", sexpLevel)));
        assertEquals(ConjunctionRelation.AND, flat.getRelation());
        assertEquals(1 + sexpLevel/2, flat.collectLeaves().size());
        assertTrue(flat.collectNonLeaves().isEmpty());
        // test on hierarchical logic
        flat = new LogicExpressionFlattener(deserialize("(|| (|| C1 (&& C2 (&& C0 C3))) (|| (&& (! C0) (&& C2 (! C3))) (|| (! C4) C5)))"));
        assertEquals(ConjunctionRelation.OR, flat.getRelation());
        assertEquals(3, flat.collectLeaves().size());
        assertEquals("[(! C4), C1, C5]", serializeFromList(flat.collectLeaves()));
        List<LogicExpressionFlattener> flatChildren = flat.collectNonLeaves();   // test next layer
        assertTrue(flatChildren.stream().allMatch(c -> c.getRelation().equals(ConjunctionRelation.AND)));
        assertEquals(2, flatChildren.size());
        assertEquals(3, flatChildren.get(0).collectLeaves().size());
        assertTrue(flatChildren.get(0).collectNonLeaves().isEmpty());
        assertEquals("[(! C0), (! C3), C2]", serializeFromList(flatChildren.get(0).collectLeaves()));
        assertEquals(3, flatChildren.get(1).collectLeaves().size());
        assertTrue(flatChildren.get(1).collectNonLeaves().isEmpty());
        assertEquals("[C0, C2, C3]", serializeFromList(flatChildren.get(1).collectLeaves()));
        // applyOnLeaves
        flat = LogicExpressionFlattener.apply(flat,
                (e, rel) -> {        // negate on all leaf expressions
            switch(rel) {
                case ATOM:
                    if (e.getExpressionType().equals(ExpressionType.OPERATOR_NOT)) {
                        return e.getLeft();
                    } else {
                        return new OperatorExpression(ExpressionType.OPERATOR_NOT, e, null);
                    }
                default:
                    return e;
            }
        });
        assertEquals(ConjunctionRelation.OR, flat.getRelation());
        assertEquals(3, flat.collectLeaves().size());
        assertEquals("[(! C1), (! C5), C4]", serializeFromList(flat.collectLeaves()));
        flatChildren = flat.collectNonLeaves();   // test next layer
        assertTrue(flatChildren.stream().allMatch(c -> c.getRelation().equals(ConjunctionRelation.AND)));
        assertEquals(2, flatChildren.size());
        assertEquals(3, flatChildren.get(0).collectLeaves().size());
        assertEquals("[(! C0), (! C2), (! C3)]", serializeFromList(flatChildren.get(0).collectLeaves()));
        assertTrue(flatChildren.get(0).collectNonLeaves().isEmpty());
        assertEquals(3, flatChildren.get(1).collectLeaves().size());
        assertTrue(flatChildren.get(1).collectNonLeaves().isEmpty());
        assertEquals("[(! C2), C0, C3]", serializeFromList(flatChildren.get(1).collectLeaves()));
        // Logic shortcuts
        flat = new LogicExpressionFlattener(deserialize("(|| (|| (|| C1 C3) C0) (|| ctrue (&& C1 C2)))"));  // OR-ing with a truth
        assertEquals(1, flat.collectLeaves().size());
        assertEquals("ctrue", serialize(flat.collectLeaves().get(0)));
        assertTrue(flat.collectNonLeaves().isEmpty());
        flat = new LogicExpressionFlattener(deserialize("(&& (&& (&& C1 C3) C0) (&& cfalse (|| C1 C2)))"));  // AND-ing with a falsehood
        assertEquals(1, flat.collectLeaves().size());
        assertEquals("cfalse", serialize(flat.collectLeaves().get(0)));
        assertTrue(flat.collectNonLeaves().isEmpty());
        // OR-ing with a falsehood
        assertEquals("(|| (|| (|| C0 C1) C3) (&& C1 C2))",
                serialize(new LogicExpressionFlattener(deserialize("(|| (|| (|| C1 C3) C0) (|| cfalse (&& C1 C2)))"))
                        .toExpression()));
    }

    //CHECKSTYLE:OFF
    @Test
    public void testExpressionNormalizer() {    // unit test of end-to-end expression rewriter
        assertNull(ExpressionNormalizer.normalize(null));
        new ArrayList<String>(){{        // non-complex expressions
            add("C1");
            add("c1");
            add("P1");
            add("(fn-foo-2 (+ C1 P1) (/ c4 c2))");          // does not rewrite function expression
            add("(vec-5 C0 C2 C1 C2 C0)");                  // does not dedup VVE unless it's in some comparison
            add("(+ C1 c1)");
        }}.forEach(s ->
                assertEquals("Transformation of form \"" + s + "\" is not identical.",
                        s, serialize(ExpressionNormalizer.normalize(deserialize(s)))));
        new HashMap<String, String>() {{    // intuitive transformations
            put("(* c3 P5)", "c15");
            put("(/ P0 C0)", "P0");
            put("(&& ctrue cfalse)", "cfalse");
            put("(|| ctrue cfalse)", "ctrue");
            put("(* P0 C1)", "P0");
            put("(/ C2 C2)", "c1");
            put("(&& (> C1 P0) (> P0 C1))", "cfalse");
            put("(|| (> C1 P0) (> P0 C1))", "(!= C1 c0)");
            put("(&& (in C1 (vec-3 C0 c0 P1)) (< P1 C1))", "(= C0 C1)");
            put("(|| (= C1 c0) (!= C1 c1))", "(!= C1 c1)");
            put("(|| (= C1 c0) (!= C1 P0))", "ctrue");
            put("(|| (= C1 C0) (!= C0 C1))", "ctrue");
            put("(= (+ C0 C1) (- C1 C2))", "(= (@- C0) C2)");
            put("(&& (>= (+ C1 C0) (- C1 C2)) (<= (+ C0 C1) (- C1 C2)))", "(= (@- C0) C2)");
        }}.forEach((k, v) ->
                assertEquals("Transform of form \"" + k + "\": expect \"" + v + "\".",
                        v, serialize(ExpressionNormalizer.normalize(deserialize(k)))));

        // complex ones, where join kicks in
        assertEquals("(+ (+ (* c2.0 C2) C1) c8)",
                serialize(ExpressionNormalizer.normalize(deserialize("(- (+ (* c2 C1) C2) (- (+ C1 P1) (+ C2 c9)))"))));
        assertEquals("(!= (+ (* c2.0 C2) C1) c2)",
                serialize(ExpressionNormalizer.normalize(deserialize("(!= (- (+ (* c2 C1) C2) (- (+ C1 P1) (+ C2 c9))) c10)"))));
        // NOT { << a + 3 + -(a - 2) = 5 || 2a + b - [(1 + a) - (b + 9)] != 10 >> && (? >= a * b || 8 <= -(2a - b) || 2a - b < 10} ==>
        // 2b + a = 2 || (b - 2a < 8 && ? < ab)
        assertEquals("(|| (= (+ (* c2.0 C2) C1) c2) " +
                                  "(&& (< (- C2 (* c2.0 C1)) c8) " +
                                      "(> (* C1 C2) P?)))",
                serialize(ExpressionNormalizer.normalize(deserialize(
                        "(! (&& (|| (!= (+ (+ C1 P3) " +
                                              "(@- (- C1 P2))) " +
                                            "P5) " +
                                        "(!= (- (+ (* c2 C1) C2) " +
                                               "(- (+ C1 P1) " +
                                                  "(+ C2 c9))) " +
                                            "c10)) " +
                                    "(|| (>= P? (* C1 C2)) " +
                                        "(<= c8 (@- (- (* P2 C1) C2)))))))"))));
        // NOT { << a + 3 + -(a - 2) = 5 || 2a + b - [(1 + a) - (b + 9)] != 10 >> && [(? >= a * b || 10 <= -(2a - b) || 9 <= 2a - b]} ==>
        // 2b + a = 2 || (b - 2a < -10 && b - 2a < 9 && ? < ab)
        // NOTE: the two comparisons on b - 2a is NOT merged, because LHS is determined NOT to be of integral value
        assertEquals("(|| (= (+ (* c2.0 C2) C1) c2) " +
                                 "(&& (&& (< (- C2 (* c2.0 C1)) P-10) " +
                                         "(< (- C2 (* c2.0 C1)) c9)) " +
                                      "(> (* C1 C2) P?)))",
                serialize(ExpressionNormalizer.normalize(deserialize(
                        "(! (&& (|| (!= (+ (+ C1 P3) " +
                                              "(@- (- C1 P2))) " +
                                            "P5) " +
                                        "(!= (- (+ (* c2 C1) C2) " +
                                            "(- (+ C1 P1) " +
                                               "(+ C2 c9))) " +
                                            "c10)) " +
                                    "(|| (|| (>= P? (* C1 C2)) " +
                                            "(<= (- (* P2 C1) C2) P10)) " +
                                        "(<= c9 (@- (- (* P2 C1) C2)))))))"))));
        // NOT { << a + 3 + -(a - 2) = 5 || 2a + b - [(1 + a) - (b + 9)] != 10 >> && (? >= a * b || c < 10 || 9 <= -c)} ==>
        // 2b + a = 2 || (? < ab && c >= 10)
        assertEquals("(|| (= (+ (* c2.0 C2) C1) c2) " +
                                  "(&& (> (* C1 C2) P?) " +
                                      "(>= C3 c10)))",
                serialize(ExpressionNormalizer.normalize(deserialize(
                        "(! (&& (|| (!= (+ (+ C1 P3) " +
                                               "(@- (- C1 P2))) " +
                                            "P5) " +
                                        "(!= (- (+ (* c2 C1) C2) " +
                                            "(- (+ C1 P1) " +
                                               "(+ C2 c9))) " +
                                            "c10)) " +
                                "(|| (|| (>= P? (* C1 C2)) " +
                                         "(< C3 P10)) " +
                                    "(<= c9 (@- C3)))))"))));
    }
    //CHECKSTYLE:ON
}
