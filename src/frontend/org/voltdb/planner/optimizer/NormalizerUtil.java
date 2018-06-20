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

import org.voltdb.expressions.AbstractExpression;
import org.voltdb.expressions.ConstantValueExpression;
import org.voltdb.expressions.ParameterValueExpression;
import org.voltdb.expressions.VectorValueExpression;
import org.voltdb.planner.PlanningErrorException;
import org.voltdb.types.ExpressionType;

import java.util.*;

final class NormalizerUtil {
    private NormalizerUtil(){}

    private static final Number<Integer>
            s_negInf = new Number<>(Number.Classify.NEG_INF),
            s_posInf = new Number<>(Number.Classify.POS_INF);

    /**
     * Static factory for creating -Inf value of Number<Integer> object.
     * @return -Inf value for Number<Integer> type.
     */
    static Number<Integer> negInf() {
        return s_negInf;
    }
    static Number<Integer> posInf() {
        return s_posInf;
    }


    /**
     * Conjunction relation. ATOM means a non-conjunction expression.
     */
    enum ConjunctionRelation {
        ATOM, AND, OR;

        /**
         * Convert an expression into a conjunction relation.
         * @param e expression
         * @return conjunction relation thereof.
         */
        static ConjunctionRelation get(AbstractExpression e) {
            assert(e != null);
            switch (e.getExpressionType()) {
                case CONJUNCTION_AND:
                    return AND;
                case CONJUNCTION_OR:
                    return OR;
                default:
                    return ATOM;
            }
        }
        /**
         * Convert a conjunction relation to an ExpressionType. Fails on a non-conjunction relation.
         * @param rel conjunction relation.
         * @return converted expression type.
         */
        static ExpressionType conjOf(ConjunctionRelation rel) {
            switch (rel) {
                case OR:
                    return ExpressionType.CONJUNCTION_OR;
                case AND:
                    return ExpressionType.CONJUNCTION_AND;
                case ATOM:
                default:
                    assert(false);
                    return ExpressionType.INVALID;
            }
        }
    }

    /**
     * Representation of arithmetic "types". +/- are of the same type (PlusMinus) and *,/ are of the same type (MultDiv);
     * all other expression types are of atomic type in calculation.
     */
    enum ArithOpType {
        PlusMinus, MultDiv, Atom;       // Atom, or leaf node. This type does not check validity.
        static ArithOpType get(ExpressionType op) {
            switch (op) {
                case OPERATOR_PLUS:
                case OPERATOR_MINUS:
                    return PlusMinus;
                case OPERATOR_MULTIPLY:
                case OPERATOR_DIVIDE:
                    return MultDiv;
                default:
                    return Atom;
            }
        }
    }

    /**
     * Create a brand new CVE or PVE with given numerical value and its numerical type (int or float)
     * and given expression type.
     * @param e source expression to be referenced, must be either CVE or PVE (can be a parameter).
     * @param val value to set for the new constant
     * @return brand new constant expression
     */
    static AbstractExpression createConstant(AbstractExpression e, float val) {
        assert(e instanceof ConstantValueExpression || e instanceof ParameterValueExpression);
        if (e instanceof ConstantValueExpression) {
            return isInt(val) ? new ConstantValueExpression((int) val) : new ConstantValueExpression(val);
        } else {
            ParameterValueExpression pve = (ParameterValueExpression) e.clone();
            pve.setOriginalValue((ConstantValueExpression) createConstant(new ConstantValueExpression(), val));
            return pve;
        }
    }

    /**
     * Check if a float value can be represented as an integer
     * @param v float value
     * @return whether it can be represented as an integer
     */
    static boolean isInt(double v) {
        return Math.floor(v) == Math.ceil(v);
    }

    /**
     * Check if a CVE/PVE is an integer value
     * @param e expression
     * @return if it is a CVE/PVE with integer value
     */
    static boolean isInt(AbstractExpression e) {
        return e != null && isLiteralConstant(e) &&
                ! (e instanceof VectorValueExpression) &&
                isInt(getNumberConstant(e).get());
    }

    /**
     * Negate a numeric value of CVE/PVE, by creating a negated copy.
     * pre: expression must be either CVE or PVE.
     * @param e source expression
     * @return negated value
     */
    static AbstractExpression negate_of(AbstractExpression e) {
        if (e instanceof ConstantValueExpression) {
            return createConstant(e, -(Float.valueOf(((ConstantValueExpression) e).getValue())));
        } else {
            assert(((ParameterValueExpression)e).getOriginalValue() != null);   // a PVE to be negated cannot be a parameter
            return createConstant(e,
                    -(Float.valueOf(((ParameterValueExpression) e)
                            .getOriginalValue()
                            .getValue())));
        }
    }

    /**
     * Checks if it's either CVE or PVE with literal value (i.e. not a parameter), or VVE
     * @param e expression to check
     * @return whether it is either a VVE or a literal constant value
     */
    static boolean isLiteralConstant(AbstractExpression e) {
        return e instanceof VectorValueExpression ||    // although VVE can be parameterized, it should always be on RHS.
                e instanceof ConstantValueExpression ||
                e instanceof ParameterValueExpression && ((ParameterValueExpression) e).getOriginalValue() != null;
    }

    /**
     * Try to extract numerical constant from a CVE/PVE
     * @param e expression to extract from
     * @return numerical value if it can be extracted, empty otherwise (parameterized PVE or non CVE/PVE).
     */
    static Optional<Float> getNumberConstant(AbstractExpression e) {
        assert(isLiteralConstant(e));
        if ((e instanceof ConstantValueExpression || e instanceof ParameterValueExpression) &&
                e.getValueType().isNumber()) {
            ConstantValueExpression cve = e instanceof ConstantValueExpression ? (ConstantValueExpression) e :
                    ((ParameterValueExpression) e).getOriginalValue();
            if (cve.getValue() != null) {
                return Optional.of(Float.valueOf(cve.getValue()));
            }
        }
        return Optional.empty();
    }

    /**
     * Evaluate binary arithmetic operation.
     * @param operator
     * @param lhs
     * @param rhs
     * @return
     */
    static float evalNumericOp(ExpressionType operator, float lhs, float rhs) {
        // TODO: divide-by-zero
        switch (operator) {
            case OPERATOR_PLUS:
                return lhs + rhs;
            case OPERATOR_MINUS:
                return lhs - rhs;
            case OPERATOR_MULTIPLY:
                return lhs * rhs;
            case OPERATOR_DIVIDE:
                return lhs / rhs;
            default:
                throw new PlanningErrorException("Unsupported arithmetic operation: " + operator.toString());
        }
    }

    /**
     * Compares double numbers with tolerance of +- 10**-10.
     * @param lhs LHS arg
     * @param rhs RHS arg
     * @return whether they are almost equal within threshold.
     */
    static boolean almostEquals(double lhs, double rhs) {
        return Math.abs(lhs - rhs) < 1e-10;
    }

    static boolean evalComparison(ExpressionType cmp, float lhs, float rhs) {
        switch (cmp) {
            case COMPARE_EQUAL:
                return lhs == rhs;
            case COMPARE_NOTEQUAL:
                return lhs != rhs;
            case COMPARE_LESSTHAN:
                return lhs < rhs;
            case COMPARE_LESSTHANOREQUALTO:
                return lhs <= rhs;
            case COMPARE_GREATERTHAN:
                return lhs > rhs;
            case COMPARE_GREATERTHANOREQUALTO:
                return lhs >= rhs;
            default:
                throw new PlanningErrorException("Unsupported arithmetic comparison: " + cmp.toString());
        }
    }

    static boolean isBooleanCVE(AbstractExpression e) {
        return ConstantValueExpression.isBooleanTrue(e) || ConstantValueExpression.isBooleanFalse(e);
    }

    /**
     * Intersection between two sets of expressions, using equality.
     * @param lhs expression set #1
     * @param rhs expression set #2
     * @return common expressions present in both sets
     */
    static List<AbstractExpression> intersection(List<AbstractExpression> lhs, List<AbstractExpression> rhs) {
        Set<AbstractExpression> l = new HashSet<>(lhs);
        l.retainAll(rhs);
        return new ArrayList<>(l);
    }

    /**
     * Set difference between two lists of expressions, using equality.
     * @param lhs expression set #1 to be subtracted from
     * @param rhs expression set #2 to extract
     * @return resulting expression set
     */
    static List<AbstractExpression> minus(List<AbstractExpression> lhs, List<AbstractExpression> rhs) {
        Set<AbstractExpression> result = new HashSet<>(lhs);
        result.removeAll(rhs);
        return new ArrayList<>(result);
    }

    /**
     * Utilities to handle/merge comparison relationship when two comparisons have same LHS/RHS but
     * different comparison types.
     */
    static final class ComparisonTypeMerger {
        private ComparisonTypeMerger(){}
        /**
         * two comparison types are compliment of each other, i.e. no pair of values exists to satisfy both,
         * and no pair of values exists to fail both.
         * Precondition: left.compareTo(right) < 0
         * @param left comparison type of the lesser of the two
         * @param right comparison type of the greater of the two
         * @return whether the two comparison types compliment with each other.
         */
        private static boolean areComplements(ExpressionType left, ExpressionType right) {
            assert(left.compareTo(right) < 0);
            if (left.equals(ExpressionType.COMPARE_EQUAL) &&
                    right.equals(ExpressionType.COMPARE_NOTEQUAL)) {
                return true;
            } else if (left.equals(ExpressionType.COMPARE_LESSTHAN) &&
                    right.equals(ExpressionType.COMPARE_GREATERTHANOREQUALTO)) {
                return true;
            } else {
                return left.equals(ExpressionType.COMPARE_GREATERTHAN) &&
                        right.equals(ExpressionType.COMPARE_LESSTHANOREQUALTO);
            }
        }

        /**
         * Two comparison types contradict with each other if no pairs of values exists to satisify both.
         * This is weaker condition than compliments, e.g. (=, >).
         * Requires same precondition.
         * @param left comparison type of the lesser of the two
         * @param right comparison type of the greater of the two
         * @return whether the two comparison types contradict with each other.
         */
        static boolean areContradicts(ExpressionType left, ExpressionType right) {
            assert(left.compareTo(right) < 0);
            if (areComplements(left, right)) {
                return true;
            }
            switch(left) {
                case COMPARE_EQUAL:
                    switch (right) {
                        // case COMPARE_NOTEQUAL is covered by areCompliments.test()
                        case COMPARE_LESSTHAN:
                        case COMPARE_GREATERTHAN:
                            return true;
                        default:
                            return false;
                    }
                case COMPARE_LESSTHAN:
                    switch (right) {
                        case COMPARE_GREATERTHAN:
                            // case COMPARE_GREATERTHANOREQUALTO is covered by areCompliments)
                            return true;
                        default:
                            return false;
                    }
                    // case COMPARE_GREATERTHAN is covered by above cases and areCompliments)
                default:
                    return false;
            }
        }

        /**
         * Test whether two comparisons cover the entire space of A x B
         * requires the same precondition
         * @param left comparison type of the lesser of the two
         * @param right comparison type of the greater of the two
         * @return whether the two comparison types cover all possible relations
         */
        static boolean coverAll(ExpressionType left, ExpressionType right) {
            assert(left.compareTo(right) < 0);
            if (areComplements(left, right)) {
                return  true;
            }
            return left.equals(ExpressionType.COMPARE_LESSTHANOREQUALTO) &&
                    right.equals(ExpressionType.COMPARE_GREATERTHANOREQUALTO);
        }
        /**
         * Rules to resolves two comparison types for logical relations
         * Requires same precondition.
         * @param left comparison type of the lesser of the two
         * @param right comparison type of the greater of the two
         * @param isAnd resolve using conjunction (i.e. AND) or disjunction (i.e. OR) between comparisons
         * @return merged comparison type
         */
        static ExpressionType resolve(ExpressionType left, ExpressionType right, boolean isAnd) {
            return isAnd ? resolveAnd(left, right) : resolveOr(left, right);
        }
        private static ExpressionType resolveAnd(ExpressionType left, ExpressionType right) {
            assert(left.compareTo(right) < 0);
            switch (left) {
                case COMPARE_EQUAL:
                    switch (right) {
                        case COMPARE_GREATERTHANOREQUALTO:
                        case COMPARE_LESSTHANOREQUALTO:
                            return left;
                        default:
                            assert(false);
                            return left;
                    }
                case COMPARE_NOTEQUAL:
                    switch (right) {
                        case COMPARE_LESSTHAN:
                        case COMPARE_GREATERTHAN:
                            return right;
                        case COMPARE_GREATERTHANOREQUALTO:
                            return ExpressionType.COMPARE_GREATERTHAN;
                        case COMPARE_LESSTHANOREQUALTO:
                            return ExpressionType.COMPARE_LESSTHAN;
                        default:
                            assert(false);
                            return left;
                    }
                case COMPARE_LESSTHAN:
                    switch (right) {
                        case COMPARE_LESSTHANOREQUALTO:
                            return left;
                        default:
                            assert(false);
                            return left;
                    }
                case COMPARE_GREATERTHAN:
                    switch (right) {
                        case COMPARE_GREATERTHANOREQUALTO:
                            return left;
                        default:
                            assert(false);
                            return left;
                    }
                case COMPARE_LESSTHANOREQUALTO:
                    switch (right) {
                        case COMPARE_GREATERTHANOREQUALTO:
                            return ExpressionType.COMPARE_EQUAL;
                        default:
                            assert(false);
                            return left;
                    }
                default:
                    assert(false);
                    return left;
            }
        }
        private static ExpressionType resolveOr(ExpressionType left, ExpressionType right) {
            assert(left.compareTo(right) < 0);
            switch (left) {
                case COMPARE_EQUAL:
                    switch (right) {
                        case COMPARE_LESSTHAN:
                            return ExpressionType.COMPARE_LESSTHANOREQUALTO;
                        case COMPARE_GREATERTHAN:
                            return ExpressionType.COMPARE_GREATERTHANOREQUALTO;
                        case COMPARE_GREATERTHANOREQUALTO:
                        case COMPARE_LESSTHANOREQUALTO:
                            return right;
                        default:
                            assert (false);
                            return left;
                    }
                case COMPARE_NOTEQUAL:
                    switch (right) {
                        case COMPARE_LESSTHAN:
                        case COMPARE_GREATERTHAN:
                        case COMPARE_GREATERTHANOREQUALTO:
                        case COMPARE_LESSTHANOREQUALTO:
                            return right;
                        default:
                            assert (false);
                            return left;
                    }
                case COMPARE_LESSTHAN:
                    switch (right) {
                        case COMPARE_GREATERTHAN:
                            return ExpressionType.COMPARE_NOTEQUAL;
                        case COMPARE_LESSTHANOREQUALTO:
                            return right;
                        default:
                            assert (false);
                            return left;
                    }
                case COMPARE_GREATERTHAN:
                    switch (right) {
                        case COMPARE_GREATERTHANOREQUALTO:
                            return right;
                        default:
                            assert (false);
                            return left;
                    }
                default:
                    assert (false);
                    return left;
            }
        }
    }

}
