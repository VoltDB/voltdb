/* This file is part of VoltDB.
 * Copyright (C) 2008-2013 VoltDB Inc.
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

package org.voltdb.expressions;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Stack;

import org.voltdb.VoltType;
import org.voltdb.types.ExpressionType;
import org.voltdb.types.TimestampType;
import org.voltdb.utils.Encoder;

/**
 *
 */
public abstract class ExpressionUtil {

    public static void finalizeValueTypes(AbstractExpression exp)
    {
        exp.normalizeOperandTypes_recurse();
        exp.finalizeValueTypes();
    }

    /**
     *
     * @param exps
     */
    public static AbstractExpression combine(Collection<AbstractExpression> exps) {
        if (exps.isEmpty()) {
            return null;
        }
        Stack<AbstractExpression> stack = new Stack<AbstractExpression>();
        stack.addAll(exps);

        // TODO: This code probably doesn't need to go through all this trouble to create AND trees
        // like "((D and C) and B) and A)" from the list "[A, B, C, D]".
        // There is an easier algorithm that does not require stacking intermediate results.
        // Even better, it would be easier here to generate "(D and (C and (B and A)))"
        // which would also short-circuit slightly faster in the executor.
        // NOTE: Any change to the structure of the trees produced by this algorithm should be
        // reflected in the algorithm used to reverse the process in uncombine(AbstractExpression expr).

        AbstractExpression ret = null;
        while (stack.size() > 1) {
            AbstractExpression child_exp = stack.pop();
            //
            // If our return node is null, then we need to make a new one
            //
            if (ret == null) {
                ret = new ConjunctionExpression(ExpressionType.CONJUNCTION_AND);
                ret.setLeft(child_exp);
            //
            // Check whether we can add it to the right side
            //
            } else if (ret.getRight() == null) {
                ret.setRight(child_exp);
                stack.push(ret);
                ret = null;
            }
        }
        if (ret == null) {
            ret = stack.pop();
        } else {
            ret.setRight(stack.pop());
        }
        return ret;
    }

    /**
     * Undo the effects of the combine(List<AbstractExpression> exps) method to reconstruct the list
     * of expressions in its original order, basically right-to-left in the given expression tree.
     * NOTE: This implementation is tuned to the odd shape of the trees produced by combine,
     * namely leaf-nodes-on-the-right "(((D and C) and B) and A)" from "[A,B,C,D]".
     * Any change there should have a corresponding change here.
     * @param expr
     * @return
     */
    public static List<AbstractExpression> uncombine(AbstractExpression expr)
    {
        if (expr == null) {
            return new ArrayList<AbstractExpression>();
        }
        if (expr instanceof ConjunctionExpression) {
            ConjunctionExpression conj = (ConjunctionExpression)expr;
            if (conj.getExpressionType() == ExpressionType.CONJUNCTION_AND) {
                // Calculate the list for the tree or leaf on the left.
                List<AbstractExpression> branch = uncombine(conj.getLeft());
                // Insert the leaf on the right at the head of that list
                branch.add(0, conj.getRight());
                return branch;
            }
            // Any other kind of conjunction must have been a leaf. Fall through.
        }
        // At the left-most leaf, start a new list.
        List<AbstractExpression> leaf = new ArrayList<AbstractExpression>();
        leaf.add(expr);
        return leaf;
    }

    /**
     * Convert one or more predicates, potentially in an arbitrarily nested conjunction tree
     * into a flattened collection. Similar to uncombine but for arbitrary tree shapes and with no
     * guarantee of the result collection type or of any ordering within the collection.
     * In fact, it currently fills an ArrayDeque via a left=to-right breadth first traversal,
     * but for no particular reason, so that's all subject to change.
     * @param expr
     * @return a Collection containing expr or if expr is a conjunction, its top-level non-conjunction
     * child expressions.
     */
    public static Collection<AbstractExpression> uncombineAny(AbstractExpression expr)
    {
        ArrayDeque<AbstractExpression> out = new ArrayDeque<AbstractExpression>();
        if (expr != null) {
            ArrayDeque<AbstractExpression> in = new ArrayDeque<AbstractExpression>();
            // this chunk of code breaks the code into a list of expression that
            // all have to be true for the where clause to be true
            in.add(expr);
            AbstractExpression inExpr = null;
            while ((inExpr = in.poll()) != null) {
                if (inExpr.getExpressionType() == ExpressionType.CONJUNCTION_AND) {
                    in.add(inExpr.getLeft());
                    in.add(inExpr.getRight());
                }
                else {
                    out.add(inExpr);
                }
            }
        }
        return out;
    }

    /**
     *
     * @param left
     * @param right
     * @return Both expressions passed in combined by an And conjunction.
     */
    public static AbstractExpression combine(AbstractExpression left, AbstractExpression right) {
        return new ConjunctionExpression(ExpressionType.CONJUNCTION_AND, left, right);
    }

    /**
     * Recursively walk an expression and return a list of all the tuple
     * value expressions it contains.
     */
    public static List<TupleValueExpression>
    getTupleValueExpressions(AbstractExpression input)
    {
        ArrayList<TupleValueExpression> tves =
            new ArrayList<TupleValueExpression>();
        // recursive stopping steps
        if (input == null)
        {
            return tves;
        }
        if (input instanceof TupleValueExpression)
        {
            tves.add((TupleValueExpression) input);
            return tves;
        }

        // recursive calls
        tves.addAll(getTupleValueExpressions(input.m_left));
        tves.addAll(getTupleValueExpressions(input.m_right));
        if (input.m_args != null) {
            for (AbstractExpression argument : input.m_args) {
                tves.addAll(getTupleValueExpressions(argument));
            }
        }
        return tves;
    }

    static void checkConstantValueTypeSafety(ConstantValueExpression expr) {
        if (expr.getValueType().isInteger()) {
            Long.parseLong(expr.getValue());
        }
        if ((expr.getValueType() == VoltType.DECIMAL) ||
            (expr.getValueType() == VoltType.DECIMAL)) {
            Double.parseDouble(expr.getValue());
        }
    }

    static void castIntegerValueDownSafely(ConstantValueExpression expr, VoltType integerType) {
        if (expr.m_isNull) {
            expr.setValueType(integerType);
            expr.setValueSize(integerType.getLengthInBytesForFixedTypes());
            return;
        }

        long value = Long.parseLong(expr.getValue());

        // Note that while Long.MIN_VALUE is used to represent NULL in VoltDB, we have decided that
        // pass in the literal for Long.MIN_VALUE makes very little sense when you have the option
        // to use the literal NULL. Thus the NULL values for each of the 4 integer types are considered
        // an underflow exception for the type.

        if (integerType == VoltType.BIGINT || integerType == VoltType.TIMESTAMP) {
            if (value == VoltType.NULL_BIGINT)
                throw new NumberFormatException("Constant value underflows BIGINT type.");
        }
        if (integerType == VoltType.INTEGER) {
            if ((value > Integer.MAX_VALUE) || (value <= VoltType.NULL_INTEGER))
                throw new NumberFormatException("Constant value overflows/underflows INTEGER type.");
        }
        if (integerType == VoltType.SMALLINT) {
            if ((value > Short.MAX_VALUE) || (value <= VoltType.NULL_SMALLINT))
                throw new NumberFormatException("Constant value overflows/underflows SMALLINT type.");
        }
        if (integerType == VoltType.TINYINT) {
            if ((value > Byte.MAX_VALUE) || (value <= VoltType.NULL_TINYINT))
                throw new NumberFormatException("Constant value overflows/underflows TINYINT type.");
        }
        expr.setValueType(integerType);
        expr.setValueSize(integerType.getLengthInBytesForFixedTypes());
    }

    static void setOutputTypeForInsertExpressionRecursively(
            AbstractExpression input, VoltType parentType, int parentSize, Map<Integer, VoltType> paramTypeOverrideMap) {
        // stopping condiditon
        if (input == null) return;

        // make sure parameters jive with their parent types
        if (input.getExpressionType() == ExpressionType.VALUE_PARAMETER) {
            ParameterValueExpression pve = (ParameterValueExpression) input;
            switch (parentType) {
                case BIGINT:
                case INTEGER:
                case SMALLINT:
                case TINYINT:
                    paramTypeOverrideMap.put(pve.m_paramIndex, VoltType.BIGINT);
                    input.setValueType(VoltType.BIGINT);
                    input.setValueSize(VoltType.BIGINT.getLengthInBytesForFixedTypes());
                    break;
                case DECIMAL:
                    paramTypeOverrideMap.put(pve.m_paramIndex, VoltType.DECIMAL);
                    input.setValueType(VoltType.DECIMAL);
                    input.setValueSize(VoltType.DECIMAL.getLengthInBytesForFixedTypes());
                    break;
                case FLOAT:
                    paramTypeOverrideMap.put(pve.m_paramIndex, VoltType.FLOAT);
                    input.setValueType(VoltType.FLOAT);
                    input.setValueSize(VoltType.FLOAT.getLengthInBytesForFixedTypes());
                    break;
            }
        }

        // this is probably unnecessary
        if (input.getExpressionType() == ExpressionType.VALUE_CONSTANT) {
            checkConstantValueTypeSafety((ConstantValueExpression) input);
        }

        // recursive step
        setOutputTypeForInsertExpressionRecursively(input.getLeft(), parentType, parentSize, paramTypeOverrideMap);
        setOutputTypeForInsertExpressionRecursively(input.getRight(), parentType, parentSize, paramTypeOverrideMap);
    }

    public static void setOutputTypeForInsertExpression(
            AbstractExpression input,
            VoltType neededType,
            int neededSize,
            Map<Integer, VoltType> paramTypeOverrideMap)
            throws Exception
        {

        if (input.getExpressionType() == ExpressionType.VALUE_PARAMETER) {
            ParameterValueExpression pve = (ParameterValueExpression) input;
            paramTypeOverrideMap.put(pve.m_paramIndex, neededType);
            input.setValueType(neededType);
            input.setValueSize(neededSize);
        }
        else if (input.getExpressionType() == ExpressionType.VALUE_CONSTANT) {
            ConstantValueExpression cve = (ConstantValueExpression) input;

            if (cve.m_isNull) {
                cve.setValueType(neededType);
                cve.setValueSize(neededSize);
                return;
            }

            // handle the simple case where the constant is the type we want
            if (cve.getValueType() == neededType) {

                // only worry about strings/varbinary being too long
                if ((cve.getValueType() == VoltType.STRING) || (cve.getValueType() == VoltType.VARBINARY)) {
                    if (cve.getValue().length() > neededSize)
                        throw new StringIndexOutOfBoundsException("Constant VARCHAR value too long for column.");
                }
                cve.setValueSize(neededSize);
                checkConstantValueTypeSafety(cve);
                return;
            }

            // handle downcasting integers
            if (neededType.isInteger()) {
                if (cve.getValueType().isInteger()) {
                    castIntegerValueDownSafely(cve, neededType);
                    checkConstantValueTypeSafety(cve);
                    return;
                }
            }

            // handle the types that can be converted to float
            if (neededType == VoltType.FLOAT) {
                if (cve.getValueType().isExactNumeric()) {
                    cve.setValueType(neededType);
                    cve.setValueSize(neededSize);
                    checkConstantValueTypeSafety(cve);
                    return;
                }

            }

            // handle the types that can be converted to decimal
            else if (neededType == VoltType.DECIMAL) {
                if ((cve.getValueType().isExactNumeric()) || (cve.getValueType() == VoltType.FLOAT)) {
                    cve.setValueType(neededType);
                    cve.setValueSize(neededSize);
                    checkConstantValueTypeSafety(cve);
                    return;
                }
            }

            else if (neededType == VoltType.VARBINARY) {
                if ((cve.getValueType() == VoltType.STRING) && (Encoder.isHexEncodedString(cve.getValue()))) {
                    cve.setValueType(neededType);
                    cve.setValueSize(neededSize);
                    checkConstantValueTypeSafety(cve);
                    return;
                }
            }

            else if (neededType == VoltType.TIMESTAMP) {
                if (cve.getValueType() == VoltType.STRING) {
                    try {
                        TimestampType ts = new TimestampType(cve.m_value);
                        cve.m_value = String.valueOf(ts.getTime());
                        cve.setValueType(neededType);
                        cve.setValueSize(neededSize);
                        return;
                    }
                    // It couldn't be converted to timestamp, fall through and throw Exception.
                    catch (IllegalArgumentException e) {}
                }
            }

            throw new Exception(
                    String.format("Constant value cannot be converted to %s column type.",
                                  neededType.name()));
        }
        else {
            input.setValueType(neededType);
            input.setValueSize(neededSize);
            setOutputTypeForInsertExpressionRecursively(input.getLeft(), neededType, neededSize, paramTypeOverrideMap);
            setOutputTypeForInsertExpressionRecursively(input.getRight(), neededType, neededSize, paramTypeOverrideMap);
        }
    }

    /**
     * Method to simplify an expression by eliminating identical subexpressions (same id)
     * If the expression is a logical conjunction of the form e1 AND e2 AND e3 AND e4,
     * and subexpression e1 is identical to the subexpression e2 the simplified expression is
     * e1 AND e3 AND e4.
     *
     * @param expr to simplify
     * @return simplified expression.
     */
    public static AbstractExpression eliminateDuplicates(Collection<AbstractExpression> exprList) {
        // Eliminate duplicates by building the map of expression's ids, values.
        Map<String, AbstractExpression> subExprMap = new HashMap<String, AbstractExpression>();
        for (AbstractExpression subExpr : exprList) {
            subExprMap.put(subExpr.m_id, subExpr);
        }
        // Now reconstruct the expression
        ArrayList<AbstractExpression> newList = new ArrayList<AbstractExpression>();
        newList.addAll(subExprMap.values());
        return ExpressionUtil.combine(newList);
    }

    /**
     *  A condition is null-rejected for a given table in the following cases:
     *      If it is of the form A IS NOT NULL, where A is an attribute of any of the inner tables
     *      If it is a predicate containing a reference to an inner table that evaluates to UNKNOWN
     *          when one of its arguments is NULL
     *      If it is a conjunction containing a null-rejected condition as a conjunct
     *      If it is a disjunction of null-rejected conditions
     *
     * @param expr
     * @param tableName
     * @return
     */
    public static boolean isNullRejectingExpression(AbstractExpression expr, String tableName) {
        ExpressionType exprType = expr.getExpressionType();
        if (exprType == ExpressionType.CONJUNCTION_AND) {
            assert(expr.m_left != null && expr.m_right != null);
            return isNullRejectingExpression(expr.m_left, tableName) || isNullRejectingExpression(expr.m_right, tableName);
        } else if (exprType == ExpressionType.CONJUNCTION_OR) {
            assert(expr.m_left != null && expr.m_right != null);
            return isNullRejectingExpression(expr.m_left, tableName) && isNullRejectingExpression(expr.m_right, tableName);
        } else if (exprType == ExpressionType.OPERATOR_NOT) {
            assert(expr.m_left != null);
            // "NOT ( P and Q )" is as null-rejecting as "NOT P or NOT Q"
            // "NOT ( P or Q )" is as null-rejecting as "NOT P and NOT Q"
            // Handling AND and OR expressions requires a "negated" flag to the recursion that tweaks
            // (switches?) the handling of ANDs and ORs to enforce the above equivalences.
            if (expr.m_left.getExpressionType() == ExpressionType.OPERATOR_IS_NULL) {
                return containsMatchingTVE(expr, tableName);
            } else if (expr.m_left.getExpressionType() == ExpressionType.CONJUNCTION_AND ||
                    expr.m_left.getExpressionType() == ExpressionType.CONJUNCTION_OR) {
                assert(expr.m_left.m_left != null && expr.m_left.m_right != null);
                // Need to test for an existing child NOT and skip it.
                // e.g. NOT (P AND NOT Q) --> (NOT P) OR NOT NOT Q --> (NOT P) OR Q
                AbstractExpression tempLeft = null;
                if (expr.m_left.m_left.getExpressionType() != ExpressionType.OPERATOR_NOT) {
                    tempLeft = new OperatorExpression(ExpressionType.OPERATOR_NOT, expr.m_left.m_left, null);
                } else {
                    assert(expr.m_left.m_left.m_left != null);
                    tempLeft = expr.m_left.m_left.m_left;
                }
                AbstractExpression tempRight = null;
                if (expr.m_left.m_right.getExpressionType() != ExpressionType.OPERATOR_NOT) {
                    tempRight = new OperatorExpression(ExpressionType.OPERATOR_NOT, expr.m_left.m_right, null);
                } else {
                    assert(expr.m_left.m_right.m_left != null);
                    tempRight = expr.m_left.m_right.m_left;
                }
                ExpressionType type = (expr.m_left.getExpressionType() == ExpressionType.CONJUNCTION_AND) ?
                        ExpressionType.CONJUNCTION_OR : ExpressionType.CONJUNCTION_AND;
                AbstractExpression tempExpr = new OperatorExpression(type, tempLeft, tempRight);
                return isNullRejectingExpression(tempExpr, tableName);
            } else if (expr.m_left.getExpressionType() == ExpressionType.OPERATOR_NOT) {
                // It's probably safe to assume that HSQL will have stripped out other double negatives,
                // (like "NOT T.c IS NOT NULL"). Yet, we could also handle them here
                assert(expr.m_left.m_left != null);
                return isNullRejectingExpression(expr.m_left.m_left, tableName);
            } else {
                return isNullRejectingExpression(expr.m_left, tableName);
            }
        } else if (exprType == ExpressionType.OPERATOR_IS_NULL) {
            // IS NOT NULL is NULL rejecting -- IS NULL is not
            return false;
        } else {
            // @TODO ENG_3038 Is it safe to assume for the rest of the expressions that if
            // it contains a TVE with the matching table name then it is NULL rejection expression?
            // Presently, yes, logical expressions are not expected to appear inside other
            // generalized expressions, so since the handling of other kinds of expressions
            // is pretty much "containsMatchingTVE", this fallback should be safe.
            // The only planned developments that might contradict this restriction (AFAIK --paul)
            // would be support for standard pseudo-functions that take logical condition arguments.
            // These should probably be supported as special non-functions/operations for a number
            // of reasons and may need special casing here.
            return containsMatchingTVE(expr, tableName);
        }
    }

    private static boolean containsMatchingTVE(AbstractExpression expr, String tableName) {
        assert(expr != null);
        List<TupleValueExpression> tves = getTupleValueExpressions(expr);
        for (TupleValueExpression tve : tves) {
            if (tve.m_tableName == tableName) {
                return true;
            }
        }
        return false;
    }
}
