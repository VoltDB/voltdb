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

import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;
import org.voltdb.VoltType;
import org.voltdb.catalog.Database;
import org.voltdb.types.ExpressionType;
import org.voltdb.utils.VoltTypeUtil;

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
    protected void loadFromJSONObject(JSONObject obj, Database db) throws JSONException {}

    @Override
    public boolean needsRightExpression() {
        ExpressionType type = getExpressionType();
        //XXX Not sure how unary minus (and unary plus?) are handled (possibly via an implicit zero left argument?)
        return type != ExpressionType.OPERATOR_NOT && type != ExpressionType.OPERATOR_IS_NULL;
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
    public void refineValueType(VoltType columnType) {
        if ((m_valueType != null) && (m_valueType != VoltType.NUMERIC)) {
            return;
        }
        ExpressionType type = getExpressionType();
        if (type == ExpressionType.OPERATOR_IS_NULL || type == ExpressionType.OPERATOR_NOT) {
            m_valueType = VoltType.BIGINT;
            m_valueSize = m_valueType.getLengthInBytesForFixedTypes();
            return;
        }
        m_left.refineValueType(columnType);
        //XXX Not sure how unary minus (and unary plus?) are handled (possibly via an implicit zero left argument?)
        m_right.refineValueType(columnType);
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
        if (type == ExpressionType.OPERATOR_IS_NULL || type == ExpressionType.OPERATOR_NOT) {
            m_valueType = VoltType.BIGINT;
            m_valueSize = m_valueType.getLengthInBytesForFixedTypes();
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

}
