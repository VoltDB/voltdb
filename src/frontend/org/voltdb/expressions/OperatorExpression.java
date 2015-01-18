/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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

import org.voltdb.VoltType;
import org.voltdb.types.ExpressionType;
import org.voltdb.utils.VoltTypeUtil;

/**
 * An instance of OperatorExpression is one of the following:
 *   - + (add)
 *   - - (subtract)
 *   - * (multiply)
 *   - / (divide)
 *   - % (modulus)
 *   - || (concat)
 *   - is null
 *   - not
 *   - cast(... as type)
 *   - case when
 *   - alternative (unsupported?)
 */
public class OperatorExpression extends AbstractExpression {
    public OperatorExpression(ExpressionType type) {
        super(type);
    }

    public OperatorExpression(ExpressionType type, AbstractExpression left, AbstractExpression right) {
        super(type, left, right);
    }

    public OperatorExpression() {
        //
        // This is needed for serialization
        //
        super();
    }

    @Override
    public boolean needsRightExpression() {
        ExpressionType type = getExpressionType();
        //XXX Not sure how unary minus (and unary plus?) are handled (possibly via an implicit zero left argument?)
        switch(type) {
        case OPERATOR_NOT:
        case OPERATOR_IS_NULL:
        case OPERATOR_CAST:
            return false;
        default: return true;
        }
    }

    @Override
    public void refineOperandType(VoltType columnType) {
        if ((m_valueType != null) && (m_valueType != VoltType.NUMERIC)) {
            return;
        }
        ExpressionType type = getExpressionType();
        if (type == ExpressionType.OPERATOR_IS_NULL || type == ExpressionType.OPERATOR_NOT) {
            m_valueType = VoltType.BIGINT;
            m_valueSize = m_valueType.getLengthInBytesForFixedTypes();
            return;
        }
        m_left.refineOperandType(columnType);
        //XXX Not sure how unary minus (and unary plus?) are handled (possibly via an implicit zero left argument?)
        m_right.refineOperandType(columnType);
        VoltType cast_type = VoltTypeUtil.determineImplicitCasting(m_left.getValueType(), m_right.getValueType());
        if (cast_type == VoltType.INVALID) {
            throw new RuntimeException("ERROR: Invalid output value type for Expression '" + this + "'");
        }
        m_valueType = cast_type;
        m_valueSize = cast_type.getLengthInBytesForFixedTypes();
    }

    @Override
    public void refineValueType(VoltType neededType, int neededSize)
    {
        if (! needsRightExpression()) {
            return;
        }
        // The intent here is to allow operands to have the maximum flexibility given the
        // desired result type. The interesting cases are basically integer, decimal, and
        // float. If any of the lhs, rhs, or target result type are float, then any ambiguity
        // in the remaining arguments (such as parameters) should be resolved in favor of
        // float. Otherwise, if any are decimal, then decimal should be favored. Otherwise,
        // the broadest integer type is preferable, even if the target is of a more limited
        // integer type -- math has a way of scaling values up AND down.
        VoltType operandType = neededType;
        if (operandType.isInteger()) {
            operandType = VoltType.BIGINT;
        }
        VoltType leftType = m_left.getValueType();
        VoltType rightType = m_right.getValueType();
        if (leftType == VoltType.FLOAT || rightType == VoltType.FLOAT) {
            operandType = VoltType.FLOAT;
        }
        else if (operandType != VoltType.FLOAT) {
            if (leftType == VoltType.DECIMAL || rightType == VoltType.DECIMAL) {
                operandType = VoltType.DECIMAL;
            }
        }
        m_left.refineOperandType(operandType);
        m_right.refineOperandType(operandType);
        //XXX Not sure how unary minus (and unary plus?) are handled (possibly via an implicit zero left argument?)
        VoltType cast_type = VoltTypeUtil.determineImplicitCasting(m_left.getValueType(), m_right.getValueType());
        if (cast_type == VoltType.INVALID) {
            throw new RuntimeException("ERROR: Invalid output value type for Expression '" + this + "'");
        }
        m_valueType = cast_type;
        m_valueSize = cast_type.getLengthInBytesForFixedTypes();
    }

    @Override
    public void finalizeValueTypes()
    {
        finalizeChildValueTypes();
        ExpressionType type = getExpressionType();
        if (m_right == null) {
            if (type == ExpressionType.OPERATOR_IS_NULL || type == ExpressionType.OPERATOR_NOT) {
                m_valueType = VoltType.BIGINT;
                m_valueSize = m_valueType.getLengthInBytesForFixedTypes();
            }
            return;
        }
        if (type == ExpressionType.OPERATOR_CASE_WHEN || type == ExpressionType.OPERATOR_ALTERNATIVE) {
            assert(m_valueType != null);
            m_valueSize = m_valueType.getMaxLengthInBytes();
            return;
        }
        VoltType left_type = m_left.getValueType();
        //XXX Not sure how unary minus (and unary plus?) are handled (possibly via an implicit zero left argument?)
        VoltType right_type = m_right.getValueType();
        VoltType cast_type = VoltTypeUtil.determineImplicitCasting(left_type, right_type);
        if (cast_type == VoltType.INVALID) {
            throw new RuntimeException("ERROR: Invalid output value type for Expression '" + this + "'");
        }
        m_valueType = cast_type;
        // this may not always be safe
        m_valueSize = cast_type.getLengthInBytesForFixedTypes();
    }

    @Override
    public String explain(String impliedTableName) {
        ExpressionType type = getExpressionType();
        if (type == ExpressionType.OPERATOR_IS_NULL) {
            return "(" + m_left.explain(impliedTableName) + " IS NULL)";
        }
        if (type == ExpressionType.OPERATOR_NOT) {
            return "(NOT " + m_left.explain(impliedTableName) + ")";
        }
        if (type == ExpressionType.OPERATOR_CAST) {
            return "(CAST " + m_left.explain(impliedTableName) + " AS " + m_valueType.toSQLString() + ")";
        }
        if (type == ExpressionType.OPERATOR_CASE_WHEN) {
            return "CASE WHEN " + m_left.explain(impliedTableName) + " THEN " +
                    m_right.m_left.explain(impliedTableName) + " ELSE " +
                    m_right.m_right.explain(impliedTableName) + " END";
        }
        return "(" + m_left.explain(impliedTableName) +
            " " + type.symbol() + " " +
            m_right.explain(impliedTableName) + ")";
    }

}
