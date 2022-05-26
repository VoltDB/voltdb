/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
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

import com.google_voltpatches.common.base.Preconditions;
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
 *   - - (unary minus)
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
        // This is needed for the "reflective" way to construct an abstract expression.
        // See AbstractParsedStmt#parseOperationExpression()
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
        case OPERATOR_EXISTS:
        case OPERATOR_UNARY_MINUS:
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
            m_valueType = VoltType.BOOLEAN;
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
    public void refineValueType(VoltType neededType, int neededSize) {
        if (! needsRightExpression()) {
            return;
        }

        if (getExpressionType() == ExpressionType.OPERATOR_CASE_WHEN) {
            assert(m_right.getExpressionType() == ExpressionType.OPERATOR_ALTERNATIVE);
            m_right.refineValueType(neededType, neededSize);
            m_valueType = m_right.getValueType();
            m_valueSize = m_right.getValueSize();
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
        if (operandType.isBackendIntegerType()) {
            operandType = VoltType.BIGINT;
        }
        VoltType leftType = m_left.getValueType();
        VoltType rightType = m_right.getValueType();
        if (leftType == VoltType.FLOAT || rightType == VoltType.FLOAT) {
            operandType = VoltType.FLOAT;
        } else if (operandType != VoltType.FLOAT) {
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
    public void finalizeValueTypes() {
        finalizeChildValueTypes();
        ExpressionType type = getExpressionType();
        if (m_right == null) {
            if (type == ExpressionType.OPERATOR_IS_NULL || type == ExpressionType.OPERATOR_NOT ||
                    type == ExpressionType.OPERATOR_EXISTS) {
                m_valueType = VoltType.BOOLEAN;
                m_valueSize = m_valueType.getLengthInBytesForFixedTypes();
            }

            if (type == ExpressionType.OPERATOR_UNARY_MINUS) {
                m_valueType = m_left.getValueType();
                m_valueSize = m_valueType.getLengthInBytesForFixedTypes();
            }
            return;
        } else if (type == ExpressionType.OPERATOR_CASE_WHEN || type == ExpressionType.OPERATOR_ALTERNATIVE) {
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

    @Override       // NOTE: this method does similar job of what Calcite.unparse does: to canonicalize a query, but the name is misleading. Refer to CatalogSchemaTools.toSchema().
    public String explain(String impliedTableName) {
        final String explainLeftTableName = m_left.explain(impliedTableName);
        switch(getExpressionType()) {
            case OPERATOR_IS_NULL:
                return String.format("(%s IS NULL)", explainLeftTableName);
            case OPERATOR_NOT:
                return String.format("(NOT %s)", explainLeftTableName);
            case OPERATOR_CAST:
                return String.format("(CAST (%s AS %s))", explainLeftTableName, m_valueType.toSQLString());
            case OPERATOR_EXISTS:
                return String.format("(EXISTS %s)", explainLeftTableName);
            case OPERATOR_CASE_WHEN:
                return String.format("(CASE WHEN %s THEN %s ELSE %s END)",
                        explainLeftTableName, m_right.m_left.explain(impliedTableName),
                        m_right.m_right.explain(impliedTableName));
            case OPERATOR_UNARY_MINUS:
                return String.format("(-%s)", explainLeftTableName);
            default:
                return String.format("(%s %s %s)",
                        explainLeftTableName,
                        getExpressionType().symbol(),
                        m_right.explain(impliedTableName));
        }
    }

    @Override
    public boolean isValueTypeIndexable(StringBuffer msg) {
        ExpressionType type = getExpressionType();
        switch(type) {
            case OPERATOR_NOT:
            case OPERATOR_IS_NULL:
            case OPERATOR_EXISTS:
                msg.append("operator '").append(getExpressionType().symbol()).append("'");
                return false;
            default:
                return true;
        }
    }

    /**
     * Casting between numeric and string type are not considered safe, since they use
     * different comparison semantics
     */
    public boolean isSafeCast() {
        Preconditions.checkState(getExpressionType() == ExpressionType.OPERATOR_CAST);
        final VoltType dst_type = getValueType(),
              src_type = getLeft().getValueType();
        return ! (dst_type.isNumber() && src_type.isVariableLength()) &&
            ! (dst_type.isVariableLength() && src_type.isNumber());
    }
}

