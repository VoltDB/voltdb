/* This file is part of VoltDB.
 * Copyright (C) 2008-2011 VoltDB Inc.
 *
 * VoltDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * VoltDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.voltdb.expressions;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Stack;

import org.voltdb.VoltType;
import org.voltdb.types.ExpressionType;
import org.voltdb.utils.Encoder;
import org.voltdb.utils.NotImplementedException;
import org.voltdb.utils.Pair;
import org.voltdb.utils.VoltTypeUtil;

/**
 *
 */
public abstract class ExpressionUtil {

    /**
     * Convenience method for determining whether an Expression object should have a child
     * Expression on its RIGHT side. The follow types of Expressions do not need a right child:
     *      OPERATOR_NOT
     *      COMPARISON_IN
     *      AggregageExpression
     *
     * @param exp
     * @return Does this expression need a right expression to be valid?
     */
    public static Boolean needsRightExpression(AbstractExpression exp) {
        assert(exp != null);
        return (exp.getExpressionType() != ExpressionType.OPERATOR_NOT &&
                exp.getExpressionType() != ExpressionType.COMPARE_IN &&
                !(exp instanceof AggregateExpression));
    }

    /**
     * Given an expression, find and convert its NUMERIC literals to
     * acceptable VoltTypes. Inserts may have expressions that are stand-alone
     * constant value expressions. In this case, allow the caller to pass
     * a column type. Otherwise, base the NUMERIC to VoltType conversion
     * on the other operand's type.
     */
    public static void assignLiteralConstantTypesRecursively(AbstractExpression exp) {
        assignLiteralConstantTypesRecursively(exp, VoltType.INVALID);
    }

    public static void assignLiteralConstantTypesRecursively(
            AbstractExpression exp, VoltType columnType)
    {
        if (exp == null)
            return;

        if ((exp instanceof ConstantValueExpression) &&
            (exp.m_valueType == VoltType.NUMERIC))
        {
            assert(columnType != VoltType.INVALID);

            if ((columnType == VoltType.FLOAT) || (columnType == VoltType.DECIMAL)) {
                exp.m_valueType = columnType;
                exp.m_valueSize = columnType.getLengthInBytesForFixedTypes();
                return;
            }
            else if (columnType.isInteger()) {
                ConstantValueExpression cve = (ConstantValueExpression) exp;
                Long.parseLong(cve.getValue());
                exp.m_valueType = columnType;
                exp.m_valueSize = columnType.getLengthInBytesForFixedTypes();
            }
            else {
                throw new NumberFormatException("NUMERIC constant value type must match a FLOAT or DECIMAL column");
            }
        }
        assignLiteralConstantTypes_recurse(exp);
    }

    /**
     * Constant literals have a place-holder type of NUMERIC. These types
     * need to be converted to DECIMAL or DOUBLE in Volt based on the other
     * operand's type.
     */
    private static void assignLiteralConstantTypes_recurse(AbstractExpression exp)
    {
        // Depth first search for NUMERIC children.
        if (exp == null) {
            return;
        }

        assignLiteralConstantTypes_recurse(exp.m_left);
        assignLiteralConstantTypes_recurse(exp.m_right);

        if (exp.m_left != null && exp.m_left.m_valueType == VoltType.NUMERIC) {
            assignLiteralConstantType(exp.m_left, exp.m_right);
        }
        if (exp.m_right != null && exp.m_right.m_valueType == VoltType.NUMERIC) {
            assignLiteralConstantType(exp.m_right, exp.m_left);
        }
    }

    /**
     * Helper function to patch up NUMERIC typed constants.
     */
    private static void assignLiteralConstantType(AbstractExpression literal,
                                                  AbstractExpression other)
    {
        if (other.m_valueType != VoltType.DECIMAL) {
            literal.m_valueType = VoltType.FLOAT;
            literal.m_valueSize = VoltType.FLOAT.getLengthInBytesForFixedTypes();
        }
        else {
            literal.m_valueType = VoltType.DECIMAL;
            literal.m_valueSize = VoltType.DECIMAL.getLengthInBytesForFixedTypes();
        }
    }

    public static void assignOutputValueTypesRecursively(AbstractExpression exp) {
        if (exp == null)
            return;
        if (exp.m_left != null)
            assignOutputValueTypesRecursively(exp.m_left);
        if (exp.m_right != null)
            assignOutputValueTypesRecursively(exp.m_right);

        Pair<VoltType,Integer> exprData = calculateOutputValueTypes(exp);
        exp.m_valueType = exprData.getFirst();
        exp.m_valueSize = exprData.getSecond();
    }

    /**
     * For a given Expression node, figure out what its output values are
     * This method only looks at a single node and will not recursively walk through the tree
     *
     * @param exp
     */
    public static Pair<VoltType,Integer> calculateOutputValueTypes(AbstractExpression exp) {
        VoltType retType = VoltType.INVALID;
        int retSize = 0;
        //
        // First get the value types for the left and right children
        //
        ExpressionType exp_type = exp.getExpressionType();
        AbstractExpression left_exp = exp.getLeft();
        AbstractExpression right_exp = exp.getRight();

        // -------------------------------
        // CONSTANT/NULL/PARAMETER/TUPLE VALUES
        // If our current expression is a Value node, then the QueryPlanner should have
        // already figured out our types and there is nothing we need to do here
        // -------------------------------
        if (exp instanceof ConstantValueExpression ||
            exp instanceof NullValueExpression ||
            exp instanceof ParameterValueExpression ||
            exp instanceof TupleValueExpression) {
            //
            // Nothing to do...
            // We have to return what we already have to keep the QueryPlanner happy
            //
            retType = exp.getValueType();
            retSize = exp.getValueSize();
        // -------------------------------
        // CONJUNCTION & COMPARISON
        // If it is an Comparison or Conjunction node, then the output is always
        // going to be either true or false
        // -------------------------------
        } else if (exp instanceof ComparisonExpression ||
                   exp instanceof ConjunctionExpression) {
            //
            // Make sure that they have the same number of output values
            // NOTE: We do not need to do this check for COMPARE_IN
            //
            if (exp_type != ExpressionType.COMPARE_IN) {
                //
                // IMPORTANT:
                // We are not handling the case where one of types is NULL. That is because we
                // are only dealing with what the *output* type should be, not what the actual
                // value is at execution time. There will need to be special handling code
                // over on the ExecutionEngine to handle special cases for conjunctions with NULLs
                // Therefore, it is safe to assume that the output is always going to be an
                // integer (for booleans)
                //
                retType = VoltType.BIGINT;
                retSize = retType.getLengthInBytesForFixedTypes();
            //
            // Everything else...
            //
            } else {
                //
                // TODO: Need to figure out how COMPARE_IN is going to work
                //
                throw new NotImplementedException("The '" + exp_type + "' Expression is not yet supported");
            }
        // -------------------------------
        // AGGREGATES
        // -------------------------------
        } else if (exp instanceof AggregateExpression) {
            switch (exp_type) {
                case AGGREGATE_COUNT:
                case AGGREGATE_COUNT_STAR:
                    //
                    // Always an integer
                    //
                    retType = VoltType.BIGINT;
                    retSize = retType.getLengthInBytesForFixedTypes();
                    break;
                case AGGREGATE_AVG:
                case AGGREGATE_MAX:
                case AGGREGATE_MIN:
                    //
                    // It's always whatever the base type is
                    //
                    retType = left_exp.getValueType();
                    retSize = left_exp.getValueSize();
                    break;
                case AGGREGATE_SUM:
                    if (left_exp.getValueType() == VoltType.TINYINT ||
                            left_exp.getValueType() == VoltType.SMALLINT ||
                            left_exp.getValueType() == VoltType.INTEGER) {
                        retType = VoltType.BIGINT;
                        retSize = retType.getLengthInBytesForFixedTypes();
                    } else {
                        retType = left_exp.getValueType();
                        retSize = left_exp.getValueSize();
                    }
                    break;
                default:
                    throw new RuntimeException("ERROR: Invalid Expression type '" + exp_type + "' for Expression '" + exp + "'");
            } // SWITCH
        // -------------------------------
        // EVERYTHING ELSE
        // We need to look at our children and iterate through their
        // output value types. We will match up the left and right output types
        // at each position and call the method to figure out the cast type
        // -------------------------------
        } else {
            VoltType left_type = left_exp.getValueType();
            VoltType right_type = right_exp.getValueType();
            VoltType cast_type = VoltType.INVALID;
            //
            // If there doesn't need to be a a right expression, then the type will always be a integer (for booleans)
            //
            if (!ExpressionUtil.needsRightExpression(exp)) {
                //
                // Make sure that they can cast the left-side expression with integer
                // This is just a simple check to make sure that it is a numeric value
                //
                try {
                    // NOTE: in some brave new Decimal world, this check will be
                    // unnecessary.  This code path is currently extremely unlikely
                    // anyway since we don't support any of the ways to get here
                    cast_type = VoltType.DECIMAL;
                    if (left_type != VoltType.DECIMAL)
                    {
                        VoltTypeUtil.determineImplicitCasting(left_type, VoltType.BIGINT);
                        cast_type = VoltType.BIGINT;
                    }
                } catch (Exception ex) {
                    throw new RuntimeException("ERROR: Invalid type '" + left_type + "' used in a '" + exp_type + "' Expression");
                }
            //
            // Otherwise, use VoltTypeUtil to figure out what to case the value to
            //
            } else {
                cast_type = VoltTypeUtil.determineImplicitCasting(left_type, right_type);
            }
            if (cast_type == VoltType.INVALID) {
                throw new RuntimeException("ERROR: Invalid output value type for Expression '" + exp + "'");
            }
            retType = cast_type;
            // this may not always be safe
            retSize = cast_type.getLengthInBytesForFixedTypes();
        }
        return new Pair<VoltType, Integer>(retType, retSize);
    }

    /**
     * Returns true if the provided Expression object is a ConstantValueExpression and its
     * value will evaluate to true.
     * @param exp the Expression to check whether it is a
     * @return true if the exp is a ConstantValueExpression that evaluates to true
     */
    public static Boolean isTrueBooleanExpression(AbstractExpression exp) {
        return (exp instanceof ConstantValueExpression &&
                Integer.valueOf(((ConstantValueExpression)exp).getValue()) == 1);
    }

    /**
     *
     * @param exp
     * @throws Exception
     */
    public static AbstractExpression clone(AbstractExpression exp) throws Exception {
        AbstractExpression ret = (AbstractExpression)exp.clone();
        if (exp.getLeft() != null) {
            ret.setLeft(ExpressionUtil.clone(exp.getLeft()));
        }
        if (exp.getRight() != null) {
            ret.setRight(ExpressionUtil.clone(exp.getRight()));
        }
        return ret;
    }


    /**
     *
     * @param exps
     */
    public static AbstractExpression combine(List<AbstractExpression> exps) {
        Stack<AbstractExpression> stack = new Stack<AbstractExpression>();
        stack.addAll(exps);

        if (stack.isEmpty()) {
            return null;
        } else if (stack.size() == 1) {
            return stack.pop();
        }
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
     *
     * @param left
     * @param right
     * @return Both expressions passed in combined by an And conjunction.
     */
    public static AbstractExpression combine(AbstractExpression left, AbstractExpression right) {
        AbstractExpression ret = null;
        ret = new ConjunctionExpression(ExpressionType.CONJUNCTION_AND);
        ret.setLeft(left);
        ret.setRight(right);
        return ret;
    }

    public static AbstractExpression getOtherTableExpression(AbstractExpression expr, String tableName) {
        assert(expr != null);
        AbstractExpression retval = expr.getLeft();

        AbstractExpression left = expr.getLeft();
        if (left instanceof TupleValueExpression) {
            TupleValueExpression lv = (TupleValueExpression) left;
            if (lv.getTableName().equals(tableName))
                retval = null;
        }

        if (retval == null) {
            retval = expr.getRight();
            AbstractExpression right = expr.getRight();
            if (right instanceof TupleValueExpression) {
                TupleValueExpression rv = (TupleValueExpression) right;
                if (rv.getTableName().equals(tableName))
                    retval = null;
            }
        }

        return retval;
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
            if (neededType == VoltType.DECIMAL) {
                if ((cve.getValueType().isExactNumeric()) || (cve.getValueType() == VoltType.FLOAT)) {
                    cve.setValueType(neededType);
                    cve.setValueSize(neededSize);
                    checkConstantValueTypeSafety(cve);
                    return;
                }
            }

            if (neededType == VoltType.VARBINARY) {
                if ((cve.getValueType() == VoltType.STRING) && (Encoder.isHexEncodedString(cve.getValue()))) {
                    cve.setValueType(neededType);
                    cve.setValueSize(neededSize);
                    checkConstantValueTypeSafety(cve);
                    return;
                }
            }

            if (neededType == VoltType.TIMESTAMP) {
                if (cve.getValueType() == VoltType.STRING) {
                    try {
                        java.sql.Timestamp sqlTS = java.sql.Timestamp.valueOf(cve.m_value);
                        long timeInMicroSecs = sqlTS.getTime() * 1000;
                        timeInMicroSecs += sqlTS.getNanos() / 1000;
                        cve.m_value = String.valueOf(timeInMicroSecs);
                        cve.setValueType(neededType);
                        cve.setValueSize(neededSize);
                        checkConstantValueTypeSafety(cve);
                    }
                    // ignore errors if it's not the right format
                    catch (IllegalArgumentException e) {}
                    return;
                }
            }

            throw new Exception("Constant value cannot be converted to column type.");
        }
        else {
            input.setValueType(neededType);
            input.setValueSize(neededSize);
            setOutputTypeForInsertExpressionRecursively(input.getLeft(), neededType, neededSize, paramTypeOverrideMap);
            setOutputTypeForInsertExpressionRecursively(input.getRight(), neededType, neededSize, paramTypeOverrideMap);
        }
    }
}
