/* This file is part of VoltDB.
 * Copyright (C) 2008-2018 VoltDB Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.voltdb.planner.optimizer;

import com.google_voltpatches.common.util.concurrent.AtomicDouble;
import org.voltcore.utils.Pair;
import org.voltdb.expressions.AbstractExpression;
import org.voltdb.expressions.ConstantValueExpression;
import org.voltdb.expressions.FunctionExpression;
import org.voltdb.expressions.OperatorExpression;
import org.voltdb.types.ExpressionType;
import static org.voltdb.planner.optimizer.NormalizerUtil.*;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * Simplifies an arithmetic expression.
 *
 * An arithmetic expression is an expression tree whose non-leaf nodes are
 * the four arithmetic operations: PLUS, MINUS, MULTIPLICATION and DIVISION, and leaf nodes are
 * expressions of all other types.
 *
 * It simplifies an expression in layered fashion: each layer contains either a sequence of PLUS/MINUS or
 * MULTIPLICATION/DIVISION. Client usually uses it by calling
 * <code> ArithmeticSimplifier.of(ArithmeticExpressionFlattener.OpType.PlusMinus, expr); </code>, or
 * <code> ArithmeticSimplifier.ofPlusMinus(expr); </code>.
 *
 * It works by combining number constants and common expressions. For example,  all number terms are
 * combined into at most one term (zero term when original expression does not contain any number constants, or
 * the combination of number constants can be ignored). Common non-number expressions are either canceled out
 * (by MINUS/DIVISION), or combined together into either multiplication of that term with an integer constant (for
 * PLUS/MINUS), or a POWER(expr, exponent) (for MULTIPLICATION/DIVISION).
 *
 * The only observable difference this transformation introduce is expression cancellation when it could be
 * zero, in which case the un-transformed query results in a RUNTIME ERROR and the transformed term (most likely)
 * silently skip calculation of division-by-zero.
 */
final class ArithmeticSimplifier {
    /**
     * A map of accepted operators to their negated operators, e.g. {+ => -; - => +} or {* => /; / => *}
     */
    private final Map<ExpressionType, ExpressionType> m_negatedOperators;
    /**
     * m_negator - The operator that causes its RHS operators to be negated, e.g. "-" in +/- or "/" in * /
     * m_freeop - operator that freely binds to any operand without changing its value. "+" in +/- or "*" in * /
     */
    private final ExpressionType m_negator, m_freeop;
    /**
     * Unit of operation under interest, s.t. anything op unit == anything, and anything negator anything == unit.
     * E.g. 0 for +/-, 1 for * /.
     */
    private final int m_unit;
    private final TermSimplifier m_termSimplifier;
    private ArithmeticSimplifier(AbstractExpression e, Map<ExpressionType, ExpressionType> negated,
                                 ExpressionType negator, int unit) {
        m_negatedOperators = negated;
        m_negator = negator;
        m_freeop = m_negatedOperators.get(m_negator);
        m_unit = unit;
        m_termSimplifier = new TermSimplifier(e);
    }

    /**
     * API to use this module. Undestructive.
     * @param op Operation type to simplify on
     * @param e source expression.
     * @return transformed/simplified expression.
     */
    static AbstractExpression of(ArithOpType op, AbstractExpression e) {
        switch (op) {
            case PlusMinus:
                return ofPlusMinus(e);
            case MultDiv:
                return ofMultDiv(e);
            case Atom:
            default:
                return e;
        }
    }

    /**
     * API to use this module. Simplifies an expression on +/- operations. Undestructive.
     * @param e source expression
     * @return transformed/simplified expression
     */
    static AbstractExpression ofPlusMinus(AbstractExpression e) {
        return new ArithmeticSimplifier(e,
                new HashMap<ExpressionType, ExpressionType>() {{
                    put(ExpressionType.OPERATOR_PLUS, ExpressionType.OPERATOR_MINUS);
                    put(ExpressionType.OPERATOR_MINUS, ExpressionType.OPERATOR_PLUS);
                }},
                ExpressionType.OPERATOR_MINUS, 0)
                .get();
    }
    /**
     * API to use this module. Simplifies an expression on * / operations. Undestructive.
     * @param e source expression
     * @return transformed/simplified expression
     */
    static AbstractExpression ofMultDiv(AbstractExpression e) {
        return new ArithmeticSimplifier(e,
                new HashMap<ExpressionType, ExpressionType>(){{
                    put(ExpressionType.OPERATOR_MULTIPLY, ExpressionType.OPERATOR_DIVIDE);
                    put(ExpressionType.OPERATOR_DIVIDE, ExpressionType.OPERATOR_MULTIPLY);
                }},
                ExpressionType.OPERATOR_DIVIDE, 1)
                .get();
    }
    // generate simplified expression tree as leftest tree, e.g. (op5 (op3 (op2 (op1 v11 v12) v22) v32) v42)
    private AbstractExpression get() {
        return m_termSimplifier.get();
    }

    /**
     *  A term is either "+/-" operator (or "* /" operator), or an operand that does not include "+/-" (or "* /"), as
     *  a leaf node of an Arithmeic expression tree.
     *  It is needed for uniform storage/access of flattened expression tree
     */
    private static final class Term {
        private final ExpressionType m_operator;
        private final AbstractExpression m_operand;
        public Term(AbstractExpression e) {
            m_operand = e;
            m_operator = null;
        }
        public Term(ExpressionType t) {
            m_operator = t;
            m_operand = null;
        }
        public ExpressionType getOperator() {
            return m_operator;
        }
        public AbstractExpression getOperand() {
            return m_operand;
        }
    }

    /**
     * Simplifies an arithmetic expression (e.g. "a + 3 - b", "2 * a + 5 + b - 1"). It can only looks at +/- and
     * combines constant terms (CVE/PVE) together, e.g. "a + 1 + b - 1 - a" => "b"; or only look at * // and ???
     * "(a + 1 + b - 1) * c" => "(a + b) * c"
     */
    private final class TermSimplifier {
        private final AbstractExpression m_result;

        TermSimplifier(AbstractExpression e) {
            m_result = UnaryMinusPushDown.eliminate(simplify(new ArithExpression(e).flatten()));
        }

        /**
         * Simplified a chained expression, by combining "simplifiable" terms. "Simplifiable" means that either
         * the terms are constant value, or they are identical.
         * Note that (operator operand) pairs are interchangeable.
         * @param src a list of operator operand alternating terms
         * @return either an AbstractExpression when that is the single Term included, or an OperatorExpression.
         */
        private AbstractExpression simplify(List<Term> src) {
            assert(src.size() % 2 == 1);
            if(src.size() <= 1) {   // If there is only one term found, then return it as is.
                return src.get(0).getOperand();
            } else { // Otherwise, first transform into <operator, operand> pairs
                AtomicDouble value = new AtomicDouble(m_unit);
                // operator that can be freely applied to an operand based on the context: + for PlusMinus or * for MultDiv
                final AtomicInteger negatedExpressions = new AtomicInteger(0);  //
                // <op, expr> list
                List<Pair<ExpressionType, AbstractExpression>> pairs =
                        Stream.concat(
                                // 1st operand is transformed to is + operand (or * operand)
                                Stream.of(Pair.of(m_freeop, new EquivalentExpression(src.get(0).getOperand()))),
                                // For each ensuing (operator, operand) pairs,
                                IntStream.range(0, src.size() / 2).map(i -> i * 2 + 1).boxed()
                                        .flatMap(index -> {
                                            assert(src.get(index).getOperator() != null && src.get(index + 1).getOperand() != null);
                                            final ExpressionType operator = src.get(index).getOperator();
                                            final AbstractExpression operand = src.get(index + 1).getOperand();
                                            if (isLiteralConstant(operand)) {       // do constant-folding for number literals,
                                                final Optional<Float> other = getNumberConstant(operand);
                                                if (other.isPresent()) {            // ignoring parameters in PVE (or VVE)
                                                    value.set(evalNumericOp(operator, (float) value.get(), other.get()));
                                                    return Stream.empty();
                                                }
                                            }
                                            return Stream.of(Pair.of(src.get(index).getOperator(), new EquivalentExpression(src.get(index + 1).getOperand())));
                                        })).collect(Collectors.toList())    // Then, try to cancel out terms
                                .stream()
                                .map(kv -> {           // For each (kv: expression => multiplier) pair,
                                    // e.g. (+, term), (*, term) => (term, 1); (/, term) (-, term) => (term, -1);
                                    if (m_negator == ExpressionType.OPERATOR_MINUS &&      // If we're operating in PlusMinus mode, and
                                            kv.getSecond().get().getExpressionType() == ExpressionType.OPERATOR_MULTIPLY &&    // current term is a multiplication
                                            (isLiteralConstant(kv.getSecond().get().getLeft()) ||           // between a number constant and some other expression, e.g.
                                                    isLiteralConstant(kv.getSecond().get().getRight()))) {      // + 5 * <term> - 2 * <term>, then transform them into new pairs:
                                        final boolean isCoefficientOnLeft =
                                                isLiteralConstant(kv.getSecond().get().getLeft());
                                        final EquivalentExpression term = new EquivalentExpression(isCoefficientOnLeft ?
                                                kv.getSecond().get().getRight() : kv.getSecond().get().getLeft());
                                        float coef = getNumberConstant(isCoefficientOnLeft ?
                                                kv.getSecond().get().getLeft() : kv.getSecond().get().getRight()).get();
                                        if (kv.getFirst() == ExpressionType.OPERATOR_MINUS) {  // + 5 * <term> ==> (term, 5); - 2 * <term> ==> (term, -2).
                                            coef = -coef;
                                        }
                                        return Pair.of(term, coef);
                                    } else if (kv.getSecond().get().getExpressionType() == ExpressionType.OPERATOR_UNARY_MINUS) {  // when the term has minus in it,
                                        if (m_negator == ExpressionType.OPERATOR_MINUS) {      // combine with the +/- operators properly in PlusMinus mode,
                                            return Pair.of(new EquivalentExpression(kv.getSecond().get().getLeft()),
                                                    -1f * (kv.getFirst() == ExpressionType.OPERATOR_PLUS ? 1 : -1));
                                        } else {              // In the MultDiv mode, treat -<term> as -1 * <term> and combine all such terms to determine
                                            negatedExpressions.incrementAndGet();   // its final positivity.
                                            return Pair.of(new EquivalentExpression(kv.getSecond().get().getLeft()), 1f);
                                        }
                                    } else {
                                        return Pair.of(kv.getSecond(), kv.getFirst() == m_freeop ? 1f : -1f);
                                    }
                                })  // Now sum up all common terms seen so far, by adding each term's coefficients, thereby combining them
                                .collect(Collectors.groupingBy(Pair::getFirst, Collectors.summingDouble(Pair::getSecond)))
                                .entrySet().stream()
                                .flatMap(kv -> {
                                    final AbstractExpression expr = kv.getKey().get();
                                    final double multiplier = kv.getValue();
                                    if (almostEquals(multiplier, 0f)) {     // take care of numerical rounding error introduced in the combination
                                        return Stream.empty();                  // When they cancel out, make them disappear;
                                    } else if (almostEquals(multiplier, 1f)) {  // when the term appears only once, ignore the coefficient and properly handle operator,
                                        return Stream.of(Pair.of(m_freeop, expr));
                                    } else if (almostEquals(multiplier, -1f)) {
                                        return Stream.of(Pair.of(m_negator, expr));
                                    } else if (m_negator == ExpressionType.OPERATOR_MINUS) {    // When combining in PlusMinus mode, change coefficients into coef * <term>;
                                        return Stream.of(Pair.of(multiplier > 0 ? m_freeop : m_negator,
                                                (AbstractExpression) new OperatorExpression(ExpressionType.OPERATOR_MULTIPLY,
                                                        new ConstantValueExpression(Math.abs(multiplier)), expr)));
                                    } else {     // in MultDiv mode, use POWER() with positive raised power to handle coefficients,
                                        assert(m_negator == ExpressionType.OPERATOR_DIVIDE);
                                        FunctionExpression power = new FunctionExpression();
                                        power.setAttributes("POWER", null, 14 /* From src/ee/functionexpression.h */);
                                        power.setArgs(new ArrayList<AbstractExpression>(){{
                                            add(expr);
                                            add(new ConstantValueExpression(Math.abs(multiplier)));
                                        }});    // and use term "1 / POWER(<term>, abs(coef))" to handle negative coefficients.
                                        return Stream.of(Pair.of(multiplier > 0 ? m_freeop : m_negator, (AbstractExpression) power));
                                    }
                                }).sorted(Comparator.comparing(Pair::getSecond))   // Then first sort on operands in their natural order, combine into
                                .collect(Collectors.toList());                     // (operator, operand) pairs, and then
                pairs.sort(Comparator.comparingInt(p -> p.getFirst().getValue())); // major sort on operator's integer value, so that 1st term is likely (+, <term>) or (*, <term>),
                if (pairs.size() == 1 && isLiteralConstant(pairs.get(0).getSecond())) { // and therefore the final expression can simply start with <term> (instead of -<term> or 1/<term>).
                    return createConstant(pairs.get(0).getSecond(),
                            evalNumericOp(pairs.get(0).getFirst(),
                                    (float) value.get(),
                                    getNumberConstant(pairs.get(0).getSecond()).get()));   // Special case when only numerical constant remains
                }
                // In MultDiv mode, if some term is found as -<term>, count all such occurrences
                if (negatedExpressions.get() > 0 && m_negator == ExpressionType.OPERATOR_DIVIDE) { // set 1st term to be negative
                    if (negatedExpressions.get() % 2 == 1) {    // If sum is odd, then negate 1st term in expression
                        final Pair<ExpressionType, AbstractExpression> firstTerms = pairs.get(0);
                        if (isLiteralConstant(firstTerms.getSecond())) {   // When 1st term is a number literal, then simply negate that number;
                            pairs.set(0, Pair.of(firstTerms.getFirst(),
                                    negate_of(firstTerms.getSecond())));
                        } else {        // Otherwise negate the expression by changing <term> into -<term>
                            pairs.set(0, Pair.of(firstTerms.getFirst(),
                                    new OperatorExpression(ExpressionType.OPERATOR_UNARY_MINUS,
                                            firstTerms.getSecond(), null)));
                        }
                    }
                }
                if (value.get() != m_unit) {   // Now deal with folded constant: append numeric term to last when needed (i.e. not +0 or *1)
                    pairs.add(Pair.of(m_negatedOperators.get(m_negator),
                            createConstant(new ConstantValueExpression(), (float) value.get())));
                }
                if (pairs.isEmpty()) {         // In case of nothing remains, returns unit as constant literal
                    return new ConstantValueExpression(m_unit);
                } else if (pairs.get(0).getFirst() == m_negator) {    // If the expression doesn't contain any + or * in expression, then rewrite 1st term to rid of its negator:
                    if (pairs.get(0).getFirst() == ExpressionType.OPERATOR_MINUS) { // (-, term) => unary_minus term
                        pairs.set(0, Pair.of(m_freeop, new OperatorExpression(ExpressionType.OPERATOR_UNARY_MINUS, pairs.get(0).getSecond(), null)));
                    } else {    // (/, term) => 1 / term
                        pairs.add(0, Pair.of(m_freeop, new ConstantValueExpression(m_unit)));
                    }
                } else if (pairs.get(0).getFirst() == ExpressionType.OPERATOR_MULTIPLY &&   // Special case when there is only one non-number term present:
                        isLiteralConstant(pairs.get(0).getSecond())) {                   // like number * <term> or number / <term>.
                    assert(pairs.size() == 2);
                    if (almostEquals(getNumberConstant(pairs.get(0).getSecond()).get(), 0f)) {  // If the folded number literal is 0, then return it
                        return createConstant(pairs.get(0).getSecond(), 0f);
                    }
                }
                // For general case, assemble simplified expression together into rightest expression tree (i.e. left node of each node is always leaf).
                return pairs.stream().skip(1)           // Use 1st (operator, operand) pair as init for folding
                        .map(pair -> (AbstractExpression) (new OperatorExpression(pair.getFirst(), pair.getSecond(), null)))
                        .reduce(pairs.get(0).getSecond(), (acc, rhs) -> new OperatorExpression(rhs.getExpressionType(), acc, rhs.getLeft()));
            }
        }

        /**
         * Get an ordered operation change from left to right, e.g. original expression "a + 3 - b + 20 + b - 5"
         * is simplified into "a + 18".
         * @return simplified expression
         */
        public AbstractExpression get() {
            return m_result;
        }

        /**
         * A binary tree representation of supported arithmetic: each non-leaf node is one of the two supported operations,
         * and each leaf node is an calculation that is NOT a supported operation.
         * For example, for +/- tree, each leaf node is either a non-arithmetic expression, or a * / arithmetic operation.
         *
         * A leaf node has m_expr not-null and m_left / m_right being null, and m_type set to INVALID (to prevent from further
         * evaluation); a non-leaf node has m_expr being null and both m_left and m_right not-null.
         */
        private final class ArithExpression {
            private final ArithExpression m_left, m_right;
            private final ExpressionType m_type;
            private final AbstractExpression m_expr;

            ArithExpression(AbstractExpression e) {
                if (e == null) {
                    m_expr = null;
                    m_left = m_right = null;
                    m_type = ExpressionType.INVALID;
                } else {
                    final ExpressionType type = e.getExpressionType();
                    if (!m_negatedOperators.containsKey(type)) {
                        m_expr = e;
                        m_left = m_right = null;
                        m_type = ExpressionType.INVALID;
                    } else {
                        m_expr = null;
                        m_type = type;
                        m_left = new ArithExpression(e.getLeft());
                        m_right = new ArithExpression(e.getRight());
                    }
                }
            }

            /**
             * Flatten a binary tree into a one-level multi-way tree so that calculation can be carried out from left to right.
             * An expression of expr1 + expr2 - (expr3 - expr4) + expr5 is converted into a 5-branch tree with leaves:
             * expr1; expr2; -expr3; expr4; expr5.
             * @return Flattened representation
             */
            List<Term> flatten() {
                if (getLeaf() != null) {
                    return new ArrayList<Term>(){{ add(new Term(getLeaf())); }};
                } else {
                    return new ArrayList<Term>() {{
                        addAll(getLeft().flatten());
                        add(new Term(getType()));
                        final List<Term> right = getRight().flatten();
                        addAll(getType() == m_negator ? negated(right) : right);
                    }};
                }
            }
            ArithExpression getLeft() {
                return m_left;
            }

            ArithExpression getRight() {
                return m_right;
            }

            ExpressionType getType() {
                return m_type;
            }

            AbstractExpression getLeaf() {
                return m_expr;
            }
        }
    }

    /**
     * Negate a collection of Term. Negate in a broad sense: change all operators to their reverse. Undestructive.
     * @param src A collection of terms to negate.
     * @return negated terms
     */
    private List<Term> negated(List<Term> src) {
        return src.stream().map(t -> {
            if (t.getOperand() != null) {
                return t;
            } else {
                return new Term(m_negatedOperators.get(t.getOperator()));
            }
        }).collect(Collectors.toList());
    }
}
