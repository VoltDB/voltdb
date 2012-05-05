/* This file is part of VoltDB.
 * Copyright (C) 2008-2012 VoltDB Inc.
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
import org.voltdb.types.TimestampType;
import org.voltdb.utils.Encoder;
import org.voltdb.utils.NotImplementedException;
import org.voltdb.utils.VoltTypeUtil;

/**
 *
 */
public abstract class ExpressionUtil {

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
        normalizeOperandConstantTypes_recurse(exp);
    }

    /**
     * Constant literals have a place-holder type of NUMERIC. These types
     * need to be converted to DECIMAL or DOUBLE in Volt based on the other
     * operand's type.
     */
    private static void normalizeOperandConstantTypes_recurse(AbstractExpression exp)
    {
        // TODO: This method wants to be a non-final AbstractExpression method when it grows up.
        // Depth first search for NUMERIC children.

        if (exp.m_left != null) {
            normalizeOperandConstantTypes_recurse(exp.m_left);
        }
        if (exp.m_right != null) {
            normalizeOperandConstantTypes_recurse(exp.m_right);
        }
        if (exp.m_args != null) {
            for (AbstractExpression argument : exp.m_args) {
                normalizeOperandConstantTypes_recurse(argument);
                if (argument.m_valueType == VoltType.NUMERIC) {
                    //XXX: We need the preferred numeric type to normalize a function argument.
                    // For now, assume that it is identical to the function's return type, which may have been recently normalized, itself.
                    // If there are exceptions for specific functions,
                    // that may need to be modeled as an optional attribute of SupportedFunction.
                    assignOperandConstantType(argument, exp.m_valueType);
                }
            }
        }

        // XXX: There's no check here that the Numeric operands are actually constants.
        // Can a sub-expression of type Numeric arise in any other case?
        // Would that case always be amenable to having its valueType/valueSize redefined here?
        if (exp.m_left != null && exp.m_right != null) {
            if (exp.m_left.m_valueType == VoltType.NUMERIC) {
                assignOperandConstantType(exp.m_left, exp.m_right.m_valueType);
            }
            if (exp.m_right.m_valueType == VoltType.NUMERIC) {
                assignOperandConstantType(exp.m_right, exp.m_left.m_valueType);
            }
        }
    }

    /**
     * Helper function to patch up NUMERIC typed constants.
     */
    private static void assignOperandConstantType(AbstractExpression literal, VoltType valueType)
    {
        if (valueType != VoltType.DECIMAL) {
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
        // TODO: This method wants to be a non-final AbstractExpression method when it grows up.
        // -------------------------------
        // CONSTANT/NULL/PARAMETER/TUPLE VALUES
        // If our current expression is a Value node, then the QueryPlanner should have
        // already figured out our types and there is nothing we need to do here
        // -------------------------------
        if (exp instanceof AbstractValueExpression) {
            //
            // Nothing to do...
            //
            return;
        }

        if (exp.m_left != null)
            assignOutputValueTypesRecursively(exp.m_left);
        if (exp.m_right != null)
            assignOutputValueTypesRecursively(exp.m_right);
        if (exp.m_args != null) {
            for (AbstractExpression argument : exp.m_args) {
                assignOutputValueTypesRecursively(argument);
            }
        }

        VoltType retType = VoltType.INVALID;
        int retSize = 0;
        //
        // First get the value types for the left and right children
        //
        ExpressionType exp_type = exp.getExpressionType();
        AbstractExpression left_exp = exp.getLeft();
        AbstractExpression right_exp = exp.getRight();


        // -------------------------------
        // CONJUNCTION & COMPARISON
        // If it is an Comparison or Conjunction node, then the output is always
        // going to be either true or false
        // -------------------------------
        if (exp instanceof ComparisonExpression ||
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
                // TODO: Need to figure out how COMPARE_IN is going to work
                throw new NotImplementedException("The '" + exp_type + "' Expression is not yet supported");
            }
        // -------------------------------
        // UNARY BOOLEAN OPERATORS
        // -------------------------------
        } else if (exp instanceof OperatorExpression &&
                (exp_type == ExpressionType.OPERATOR_IS_NULL ||
                 exp_type == ExpressionType.OPERATOR_NOT)) {
            retType = VoltType.BIGINT;
            retSize = retType.getLengthInBytesForFixedTypes();
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
        } else if (exp instanceof FunctionExpression) {
            if (exp.getValueType() != null) {
                // Nothing special required for this case.
                return;
            }
            // Avoid non-castable types runtime exception by propagating the type of the first argument,
            // assumed to be driving the unspecified (parameterized) function return type.
            retType = exp.m_args.get(0).getValueType();
            retSize = retType.getMaxLengthInBytes();
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
            if (exp.needsRightExpression()) {
                //
                // Use VoltTypeUtil to figure out what to cast the value to
                //
                cast_type = VoltTypeUtil.determineImplicitCasting(left_type, right_type);
            } else {
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
            }
            if (cast_type == VoltType.INVALID) {
                throw new RuntimeException("ERROR: Invalid output value type for Expression '" + exp + "'");
            }
            retType = cast_type;
            // this may not always be safe
            retSize = cast_type.getLengthInBytesForFixedTypes();
        }

        exp.m_valueType = retType;
        exp.m_valueSize = retSize;
    }

    /**
     *
     * @param exps
     */
    public static AbstractExpression combine(List<AbstractExpression> exps) {
        if (exps.isEmpty()) {
            return null;
        }
        Stack<AbstractExpression> stack = new Stack<AbstractExpression>();
        stack.addAll(exps);

        // TODO: This code probably doesn't need to go through all this trouble to create balanced AND trees
        // like "(A and B) and (C and D)".
        // Simpler skewed AND trees like "A and (B and (C and D))" are likely as good if not better and can be
        // constructed serially with much less effort.

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
        return new ConjunctionExpression(ExpressionType.CONJUNCTION_AND, left, right);
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
                        TimestampType ts = new TimestampType(cve.m_value);
                        cve.m_value = String.valueOf(ts.getTime());
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
