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

import com.google_voltpatches.common.base.Predicate;
import org.voltdb.expressions.*;
import org.voltdb.planner.PlanningErrorException;
import org.voltdb.types.ExpressionType;
import static org.voltdb.planner.optimizer.NormalizerUtil.ArithOpType;

import java.util.function.Function;
import java.util.stream.Collectors;

import static org.voltdb.planner.optimizer.NormalizerUtil.*;

/**
 * Balance an OperatorExpression so that its RHS is as simple as possible.
 * e.g. "a - b > 0" is preferred over "a > b" or "0 < a - b".
 * All static methods, unless otherwise stated, are nondestructive.
 */
final class OpExpressionBalancer {
    private final AbstractExpression m_expr;
    private OpExpressionBalancer(AbstractExpression e) {
        if (e instanceof InComparisonExpression) {      // do nothing for "expr in [...]" expression
            m_expr = e;
        }else if (e instanceof ComparisonExpression) {
            m_expr = balance((ComparisonExpression) e);
        } else {                                        // For non-comparisons, balance all sub-parts
            m_expr = e.clone();
            if (m_expr.getLeft() != null) {
                m_expr.setLeft(balance(m_expr.getLeft()));
            }
            if (m_expr.getRight() != null) {
                m_expr.setRight(balance(m_expr.getRight()));
            }
            if (m_expr.getArgs() != null) {
                m_expr.setArgs(m_expr.getArgs().stream()
                        .map(OpExpressionBalancer::balance)
                        .collect(Collectors.toList()));
            }
        }
    }
    private AbstractExpression get() {
        return m_expr;
    }

    /**
     * API to call to balance an expression.
     * @param e source expression
     * @return transformed/balanced expression
     */
    static AbstractExpression balance(AbstractExpression e) {
        return new OpExpressionBalancer(e).get();
    }

    /**
     * Get rid of "unit" in an operation, e.g. x + 0, 0 + x, x - 0, x * 1, 1 * x, x / 1 ==> x; 0 - x => -x, etc.
     * @param e expression that might contain "unit" in calculation
     * @return cleaned expression
     */
    private static AbstractExpression ridOfUnitOperator(AbstractExpression e) {
        final OpExpressionBreaker breaker = new OpExpressionBreaker(e);
        final ExpressionType op = e.getExpressionType();
        if (!breaker.isBreakable()) {       // an expression can possibly contain a "unit" if it is "breakable",
            return e;                       // i.e. has 2 sides and one of them is a number literal.
        } else if (breaker.getNumber() == 0) {
            if (op == ExpressionType.OPERATOR_PLUS || (op == ExpressionType.OPERATOR_MINUS && breaker.isNumberOnRight())) {
                return breaker.getExpression();     // x + 0, 0 + x or x - 0 ==> x
            } else if (op == ExpressionType.OPERATOR_MINUS) {       // 0 - x ==> -x (push minus down to lowest level)
                return new UnaryMinusPushDown(breaker.getExpression()).get();
            } else {
                return e;
            }
        } else if (breaker.getNumber() == 1 &&      // x * 1, 1 * x, x / 1 ==> x
                (op == ExpressionType.OPERATOR_MULTIPLY || (op == ExpressionType.OPERATOR_DIVIDE && breaker.isNumberOnRight()))) {
            return breaker.getExpression();
        } else if (breaker.getNumber() == -1 &&     // x * (-1), (-1) * x, x / (-1) ==> -x (push minus down to lowest level)
                (op == ExpressionType.OPERATOR_MULTIPLY || op == ExpressionType.OPERATOR_DIVIDE && breaker.isNumberOnRight())) {
            return new UnaryMinusPushDown(breaker.getExpression()).get();
        } else {
            return e;
        }
    }

    private static ComparisonExpression balance(ComparisonExpression e) {
        if (!e.needsRightExpression()) {
            return e;
        } else {
            // SetReorder takes care of left/right ordering
            final ExpressionType cmp = e.getExpressionType(), negatedCmp = ComparisonExpression.reverses.get(cmp);
            final AbstractExpression left = ridOfUnitOperator(e.getLeft()), right = ridOfUnitOperator(e.getRight());
            final boolean isLeftLiteralConstant = isLiteralConstant(left), isRightLiteralConstant = isLiteralConstant(right);
            if(left instanceof FunctionExpression || right instanceof FunctionExpression) {
                if (isLiteralConstant(left) || isLiteralConstant(right)) {      // place constant on right
                    return isLiteralConstant(left) ?
                            new ComparisonExpression(negatedCmp, right, left) :
                            new ComparisonExpression(cmp, left, right);
                } else if (left.compareTo(right) <= 0) {        // if neither side is a number, place less side on left
                    return new ComparisonExpression(cmp, left, right);
                } else {
                    return new ComparisonExpression(negatedCmp, right, left);
                }
            } else if (isLeftLiteralConstant || isRightLiteralConstant) {
                if (isLeftLiteralConstant) {        // if both sides are number, leave as is; otherwise recursively call this method with reversed comparison (e.g. a > b ==> b < a)
                    return isRightLiteralConstant ? e : balance(e.reverseOperator());   // as the expression side might contain a number that could combine with the number side.
                } else if (! (right instanceof VectorValueExpression) &&    // Skipping the case like "expr in (expr1, expr2, ...)",
                        getNumberConstant(right).get() == 0 &&      // balance the special comparison: x + y cmp 0 or x - y cmp 0:
                        ArithOpType.get(left.getExpressionType()) == ArithOpType.PlusMinus) {
                    // x + y cmp 0 ==> x cmp -y; x - y cmp 0 ==> x cmp y
                    return balance(new ComparisonExpression(cmp, left.getLeft(),
                            left.getExpressionType() == ExpressionType.OPERATOR_PLUS ?
                                    new OperatorExpression(ExpressionType.OPERATOR_UNARY_MINUS, left.getRight(), null) :
                                    left.getRight()));
                } else if (left.getExpressionType() == ExpressionType.OPERATOR_UNARY_MINUS) {
                    // The caller for OpExpressionBalancer should ensure that unary minus is pushed down. Prefer simpler "expr" over "-expr",
                    // by transforming -expr cmp number ==> expr (reverse cmp) -number. No need to recurse because of that guarantee.
                    return new ComparisonExpression(negatedCmp, left.getLeft(), negate_of(right));
                } else {        // Now that we know RHS is a number constant, try to merge any number constant from LHS to RHS.
                    final OpExpressionBreaker expr = new OpExpressionBreaker(left); // check if LHS contains a number constant in the calculation,
                    if (expr.isBreakable()) {                                       // merge if it is.
                        final float lhsValue = expr.getNumber(), rhsValue = getNumberConstant(right).get();
                        switch (left.getExpressionType()) {
                            case OPERATOR_PLUS:     // for + or *, doesn't matter if number is on RHS
                                return balance(new ComparisonExpression(e.getExpressionType(), expr.getExpression(),
                                        createConstant(right, rhsValue - lhsValue)));
                            case OPERATOR_MULTIPLY:
                                if (lhsValue == 0) {        // expr * 0 cmp number ==> 0 cmp number ==> number (reverse cmp) 0
                                    return new ComparisonExpression(cmp, createConstant(expr.getNumberTerm(), 0), right)
                                            .reverseOperator();
                                } else {                    // take care of sign when doing division on both sides
                                    return balance(new ComparisonExpression(lhsValue > 0 ? cmp : negatedCmp,    // expr * number1 cmp number2 ==> expr ?cmp? number2/number1
                                            expr.getExpression(), createConstant(right, rhsValue / lhsValue)));
                                }
                            case OPERATOR_MINUS:
                                if (expr.isNumberOnRight()) {       // expr - number1 cmp number2 ==> expr cmp number1 + number2
                                    return balance(new ComparisonExpression(cmp,
                                            expr.getExpression(), createConstant(right, lhsValue + rhsValue)));
                                } else {    // number1 - expr cmp number2 ==> number1 - number2 cmp expr ==> expr (reverse cmp) number1 - number2
                                    return balance(new ComparisonExpression(cmp,
                                            createConstant(right, lhsValue - rhsValue), expr.getExpression())
                                            .reverseOperator());
                                }
                            case OPERATOR_DIVIDE:
                                if (expr.isNumberOnRight()) {       // expr / number1 cmp number 2 ==> expr ?cmp? number1 * number2
                                    return balance(new ComparisonExpression(lhsValue > 0 ? cmp : negatedCmp,
                                            expr.getExpression(), createConstant(right, lhsValue * rhsValue)));
                                } else {        // -3 / x < 5 ==> 1 / x > -5/3, no further simplification possible
                                    return new ComparisonExpression(lhsValue > 0 ? cmp : negatedCmp,
                                            new OperatorExpression(left.getExpressionType(),
                                                    createConstant(expr.getNumberTerm(), 1), expr.getExpression()),
                                            createConstant(right, rhsValue / lhsValue));
                                }
                            default:
                                throw new PlanningErrorException("Unexpected expression inside comparison: " +
                                        left.getExpressionType().toString());
                        }
                    }
                }
            } else {        // neither side is a literal number constant:
                final OpExpressionBreaker lhsBreaker = new OpExpressionBreaker(left),
                        rhsBreaker = new OpExpressionBreaker(right);
                if (lhsBreaker.isBreakable() && rhsBreaker.isBreakable()) {
                    // When both sides of the comparison are calculations with a number constant, further simplify it.
                    // e.g. a + 3 > b + 5 or a * 3 > b - 5
                    return mergeSimplify(arrangeTerms(new ComparisonExpression(cmp, left, right), lhsBreaker, rhsBreaker));
                }
            }
            return mergeSimplify(rearrange(new ComparisonExpression(cmp, left, right)));
        }
    }

    /**
     * Arrange a comparison if neither side is a literal constant, so that left is ordered (in expression sense) before right.
     * @param e source comparison expression
     * @return possibly reversed comparison expression
     */
    private static ComparisonExpression rearrange(ComparisonExpression e) {
        final AbstractExpression left = e.getLeft(), right = e.getRight();
        return isLiteralConstant(left) || isLiteralConstant(right) || left.compareTo(right) <= 0 ?
                e : e.reverseOperator();
    }

    /**
     * If both sides are non-numerical-literal arithmetic expression (i.e. involves +-* /), then
     * move right term to left side and let ArithmeticSimplifier simplify further, then put it back to balanced comparison
     * @param e comparison expression to merge-simplify
     * @return merge-simplifed expression
     */
    private static ComparisonExpression mergeSimplify(ComparisonExpression e) {
        final AbstractExpression left = e.getLeft(), right = e.getRight();
        final Predicate<AbstractExpression> isArithmetic = expr ->
                expr instanceof OperatorExpression && expr.getExpressionType() != ExpressionType.OPERATOR_UNARY_MINUS;
        if (isLiteralConstant(left) || isLiteralConstant(right) ||      // do nothing if either side is a number, or isn't arithmetic
                (! isArithmetic.apply(left) || ! isArithmetic.apply(right))) {
            return e;
        } else  {
            final AbstractExpression l = ArithmeticSimplifier.ofPlusMinus(      // move right to left and combine/simplify
                    new OperatorExpression(ExpressionType.OPERATOR_MINUS, left, right));
            final ExpressionType cmp = e.getExpressionType();
        switch (l.getExpressionType()) {            // put back to balanced form
                case OPERATOR_MINUS:
                    return new ComparisonExpression(cmp, l.getLeft(), l.getRight());
                case OPERATOR_PLUS:
                    final AbstractExpression fl, fr;
                    if (l.getLeft().compareTo(l.getRight()) <= 0) {
                        fl = l.getLeft(); fr = l.getRight();
                    } else {
                        fl = l.getRight(); fr = l.getLeft();
                    }
                    return new ComparisonExpression(cmp,
                            new OperatorExpression(ExpressionType.OPERATOR_UNARY_MINUS, fl, null),
                            fr);
                default:
                    return new ComparisonExpression(cmp, l, new ConstantValueExpression(0));
            }
        }
    }

    /**
     * Check if two sides of a comparison can further be simplified, and do the simplification if it is. The comparison
     * can be further simplified, if calculation from both sides are +/- or *, /.
     *
     * \pre both expressions are breakable, i.e. are calculation with a number constant involved.
     * @param e original comparison expression
     * @param lhs representation of LHS of comparison
     * @param rhs representation of RHS of comparison
     * @return simplified/assembled expression
     */
    private static ComparisonExpression arrangeTerms(ComparisonExpression e, OpExpressionBreaker lhs, OpExpressionBreaker rhs) {
        assert (lhs.isBreakable() && rhs.isBreakable());
        final ArithOpType lhsType = ArithOpType.get(e.getLeft().getExpressionType()),
                rhsType = ArithOpType.get(e.getRight().getExpressionType());
        if (lhsType == rhsType) { // Apply rules for further simplification
            return lhsType == ArithOpType.PlusMinus ? arrangePlusMinusTerms(e, lhs, rhs) :
            arrangeMultiplyDivisionTerms(e, lhs, rhs);
        } else {
            return rearrange(e);
        }
    }

    /**
     * Combine number constants that are available on both side of a comparison to transform it into a simpler one.
     * \pre both sides are "breakable", or contains number constant in calculation. It is recursive because the simplified
     * result could be e.g. "expr1 - expr2 cmp 0", which can then be further simplified into "expr1 cmp expr2".
     * @param e original comparison expression
     * @param lhs representation of LHS
     * @param rhs representation of RHS
     * @return transformed/simplified comparison.
     */
    private static ComparisonExpression arrangePlusMinusTerms(ComparisonExpression e, OpExpressionBreaker lhs, OpExpressionBreaker rhs) {
        assert(lhs.isBreakable() && rhs.isBreakable());
        final ExpressionType lhsType = e.getLeft().getExpressionType(), rhsType = e.getRight().getExpressionType(),
                cmp = e.getExpressionType();
        // store combined constants
        final AbstractExpression rightResult = rhs.getNumberTerm(), leftResult = lhs.getNumberTerm();
        final float l = lhs.getNumber(), r = rhs.getNumber();
        final boolean lhsNumOnRight = lhs.isNumberOnRight(), rhsNumOnRight = rhs.isNumberOnRight();
        if (lhsType == ExpressionType.OPERATOR_PLUS && rhsType == lhsType) {  // x + 3 > y + 5 ==> x - y > 2
            return balance(new ComparisonExpression(cmp,
                    new OperatorExpression(ExpressionType.OPERATOR_MINUS, lhs.getExpression(), rhs.getExpression()),
                    createConstant(rightResult, r - l)));
        } else if (lhsType != rhsType) {
            if (lhsType == ExpressionType.OPERATOR_PLUS && rhsNumOnRight) {    // x + 3 > y - 5 ==> x - y > -8
                return balance(new ComparisonExpression(cmp,
                        new OperatorExpression(ExpressionType.OPERATOR_MINUS, lhs.getExpression(), rhs.getExpression()),
                        createConstant(rightResult, -r-l)));
            } else if (lhsType == ExpressionType.OPERATOR_PLUS) {              // x + 3 > 5 - y ==> x + y > 2
                return balance(new ComparisonExpression(cmp,
                        new OperatorExpression(ExpressionType.OPERATOR_PLUS, lhs.getExpression(), rhs.getExpression()),
                        createConstant(rightResult, r - l)));
            } else if (lhsType == ExpressionType.OPERATOR_MINUS && lhsNumOnRight) {    // x - 3 > y + 5 ==> x - y > 8
                return balance(new ComparisonExpression(cmp,
                        new OperatorExpression(ExpressionType.OPERATOR_MINUS, lhs.getExpression(), rhs.getExpression()),
                        createConstant(rightResult, l + r)));
            } else {                                                                // 3 - x > y + 5 ==> -2 > x + y ==> x + y < -2
                return balance(new ComparisonExpression(cmp,
                        createConstant(leftResult, l - r),
                        new OperatorExpression(ExpressionType.OPERATOR_PLUS, lhs.getExpression(), rhs.getExpression()))
                        .reverseOperator());
            }
        } else {    // both -
            if (lhsNumOnRight && rhsNumOnRight) {                                   // x - 3 > y - 5 ==> x - y > -2
                return balance(new ComparisonExpression(cmp,
                        new OperatorExpression(ExpressionType.OPERATOR_MINUS, lhs.getExpression(), rhs.getExpression()),
                        createConstant(rightResult, l - r)));
            } else if (lhsNumOnRight == rhsNumOnRight) {                            // 3 - x > 5 - y ==> y - x > 2
                return balance(new ComparisonExpression(cmp,
                        new OperatorExpression(ExpressionType.OPERATOR_MINUS, rhs.getExpression(), lhs.getExpression()),
                        createConstant(rightResult, r - l)));
            } else if (lhsNumOnRight) {                                             // x - 3 > 5 - y ==> x + y > 8
                return balance(new ComparisonExpression(cmp,
                        new OperatorExpression(ExpressionType.OPERATOR_PLUS, lhs.getExpression(), rhs.getExpression()),
                        createConstant(rightResult, l + r)));
            } else {                                                                // 3 - x > y - 5 ==> 8 > y + x ==> y + x < 8
                return balance(new ComparisonExpression(cmp, createConstant(leftResult, l + r),
                        new OperatorExpression(ExpressionType.OPERATOR_PLUS, rhs.getExpression(), lhs.getExpression()))
                        .reverseOperator());
            }
        }
    }

    private static ComparisonExpression arrangeMultiplyDivisionTerms(
            ComparisonExpression e, OpExpressionBreaker lhs, OpExpressionBreaker rhs) {
        assert(lhs.isBreakable() && rhs.isBreakable());
        final ExpressionType lhsType = e.getLeft().getExpressionType(),
                rhsType = e.getRight().getExpressionType(),
                comparisonType = e.getExpressionType();
        final Function<Float, ExpressionType> calcCmp = val ->
                val >= 0 ? comparisonType : ComparisonExpression.reverses.get(comparisonType);
        // store combined constants
        final AbstractExpression rightResult = rhs.isNumberOnRight() ? e.getRight().getRight() : e.getRight().getLeft(),
                leftResult = lhs.isNumberOnRight() ? e.getLeft().getRight() : e.getLeft().getLeft();
        final float l = lhs.getNumber(), r = rhs.getNumber();
        final boolean r_divisible = (int)(l/r) * r == l, l_divisible = (int)(r/l) * l == r;
        if (lhsType == rhsType) {
            if (lhsType == ExpressionType.OPERATOR_MULTIPLY) {
                // First check if any constant multiplier is 0
                if (lhs.getNumber() == 0 || rhs.getNumber() == 0) {
                    if (lhs.getNumber() == rhs.getNumber()) {   // both are 0
                        return new ComparisonExpression(comparisonType, new ConstantValueExpression(0.f),
                                new ConstantValueExpression(0.f));
                    } else if (lhs.getNumber() == 0) {          // x * 0 > y * 5
                        return balance(new ComparisonExpression(calcCmp.apply(rhs.getNumber()),
                                createConstant(leftResult, 0), rhs.getExpression()).reverseOperator());
                    } else {                                    // x * 5 > y * 0
                        return balance(new ComparisonExpression(calcCmp.apply(lhs.getNumber()),
                                lhs.getExpression(), createConstant(rightResult, 0)));
                    }
                } else {            // neither side has 0: make it into form of <number> * <term> cmp <term>
                    // check if either l/r or r/l might be an integer
                    if (l_divisible || !r_divisible) {      // either r is a multiple of l, or neither r is of l, nor l of r.
                        return balance(new ComparisonExpression(calcCmp.apply(l), lhs.getExpression(),
                                new OperatorExpression(lhsType, createConstant(rightResult, r / l), rhs.getExpression()))
                                .reverseOperator());
                    } else {        // l is a multiple of r
                        return balance(new ComparisonExpression(calcCmp.apply(r),
                                new OperatorExpression(lhsType, createConstant(leftResult, l/r), lhs.getExpression()),
                                rhs.getExpression()));
                    }
                }
            } else {        // both are division
                if (lhs.isNumberOnRight() && rhs.isNumberOnRight()) {   // e.g. x/3 > y/5 ==> 5/3 * x > y
                    if (l_divisible || !r_divisible) {                  // e.g. x / 3 > y / 6
                        return balance(new ComparisonExpression(calcCmp.apply(r),
                                new OperatorExpression(ExpressionType.OPERATOR_MULTIPLY, // prefer multiplication over division
                                        createConstant(leftResult, r/l), lhs.getExpression()), rhs.getExpression()));
                    } else {
                        return balance(new ComparisonExpression(calcCmp.apply(l), lhs.getExpression(),
                                new OperatorExpression(ExpressionType.OPERATOR_MULTIPLY,
                                        createConstant(rightResult, l/r), rhs.getExpression()))
                                .reverseOperator());
                    }
                } else if (lhs.isNumberOnRight()) {         // e.g. x/3 > 5/y ==> x > 15/y
                    return balance(new ComparisonExpression(calcCmp.apply(l), lhs.getExpression(),
                            new OperatorExpression(rhsType, createConstant(rightResult, l * r), rhs.getExpression())));
                } else if (rhs.isNumberOnRight()) {         // e.g. 3/x > y/5 ==> 3*5/x > y ==> y < 15/x
                    return balance(new ComparisonExpression(calcCmp.apply(r),
                            new OperatorExpression(lhsType, createConstant(leftResult, l * r), lhs.getExpression()),
                            rhs.getExpression()).reverseOperator());
                } else {                                    // e.g. 3/x > 5/y ==> leave as it is
                    return e;
                }
            }
        } else {        // one is *, one is /
            final boolean isDivisionNumberOnRight = lhsType == ExpressionType.OPERATOR_MULTIPLY ?
                    rhs.isNumberOnRight() : lhs.isNumberOnRight();
            if (isDivisionNumberOnRight) {
                if (lhsType == ExpressionType.OPERATOR_MULTIPLY) {     // 3 * x > y / 5 ==> 15 * x > y
                    return balance(new ComparisonExpression(calcCmp.apply(r),
                            new OperatorExpression(ExpressionType.OPERATOR_MULTIPLY,
                                    createConstant(leftResult, l * r), lhs.getExpression()), rhs.getExpression()));
                } else {            // y / 5 > 3 * x ==> y > (3 * 5) * x ==> 15 * x < y
                    return balance(new ComparisonExpression(calcCmp.apply(l), lhs.getExpression(),
                            new OperatorExpression(rhsType, createConstant(rightResult, l * r), rhs.getExpression()))
                            .reverseOperator());
                }
            } else if (lhsType == ExpressionType.OPERATOR_MULTIPLY) {  // 3 * x > 5 / y ==> x > (5/3) / y
                return balance(new ComparisonExpression(calcCmp.apply(l), lhs.getExpression(),
                        new OperatorExpression(ExpressionType.OPERATOR_DIVIDE, createConstant(rightResult, r / l),
                                rhs.getExpression())));
            } else {                                                        // 5/y > 3*x ==> (5/3)/y > x ==> x < (5/3)/y
                return balance(new ComparisonExpression(calcCmp.apply(r),
                        new OperatorExpression(lhsType, createConstant(leftResult, l/r), lhs.getExpression()),
                        rhs.getExpression()).reverseOperator());
            }
        }
    }

    /**
     * Checks if the expression is a binary OperatorExpression, containing a literal number value.
     * An expression is breakable, if either side of the expression is a literal number. e.g. "a > 1", "1 = 2", "a - 0" are breakable,
     * while "a = b", "a && b" are not.
     */
    private static final class OpExpressionBreaker {
        final private float m_number;
        final private AbstractExpression m_expr, m_numberTerm;
        final private boolean m_isBreakable, m_numberOnRight;
        OpExpressionBreaker(AbstractExpression e) {
            m_numberOnRight = e.needsRightExpression() && isNumber(e.getRight());
            if (e.needsRightExpression() && (isNumber(e.getLeft()) || m_numberOnRight)) {
                m_isBreakable = true;
                if (!m_numberOnRight) {
                    m_numberTerm = e.getLeft();
                    m_expr = e.getRight();
                } else {
                    m_numberTerm = e.getRight();
                    m_expr = e.getLeft();
                }
                m_number = getNumberConstant(m_numberTerm).get();
            } else {
                m_number = 0f;
                m_isBreakable = false;
                m_expr = m_numberTerm = null;
            }
        }
        private static boolean isNumber(AbstractExpression e) {
            return isLiteralConstant(e) && ! (e instanceof VectorValueExpression);
        }
        float getNumber() {
            return m_number;
        }
        /**
         * Getters for the side of expression that is not a number.
         */
        AbstractExpression getExpression() {
            return m_isBreakable ? m_expr : null;
        }

        /**
         * Getter for the side of expression that is a number
         * @return
         */
        AbstractExpression getNumberTerm() {
            return m_numberTerm;
        }
        boolean isNumberOnRight() {
            return m_numberOnRight;
        }
        boolean isBreakable() {
            return m_isBreakable;
        }
    }
}
