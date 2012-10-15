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
            if (input.m_right != null) {
                // recursive call
                tves.addAll(getTupleValueExpressions(input.m_right));
            }
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
