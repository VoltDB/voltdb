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
import org.json_voltpatches.JSONStringer;
import org.voltdb.VoltType;
import org.voltdb.catalog.Database;
import org.voltdb.types.ExpressionType;

/**
 *
 */
public class ParameterValueExpression extends AbstractValueExpression {

    public enum Members {
        PARAM_IDX;
    }

    protected int m_paramIndex = -1;

    public ParameterValueExpression() {
        super(ExpressionType.VALUE_PARAMETER);
    }

    @Override
    public Object clone() throws CloneNotSupportedException {
        ParameterValueExpression clone = (ParameterValueExpression)super.clone();
        clone.m_paramIndex = m_paramIndex;
        return clone;
    }

    /**
     * @return the param
     */
    public Integer getParameterIndex() {
        return m_paramIndex;
    }

    /**
     * @param paramIndex The index of the parameter to set
     */
    public void setParameterIndex(Integer paramIndex) {
        m_paramIndex = paramIndex;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof ParameterValueExpression == false) {
            return false;
        }
        ParameterValueExpression expr = (ParameterValueExpression) obj;

        return m_paramIndex == expr.m_paramIndex;
    }

    @Override
    public int hashCode() {
        // based on implementation of equals
        return m_paramIndex;
    }

    @Override
    public void toJSONString(JSONStringer stringer) throws JSONException {
        super.toJSONString(stringer);
        stringer.key(Members.PARAM_IDX.name()).value(m_paramIndex);
    }

    @Override
    public void loadFromJSONObject(JSONObject obj, Database db) throws JSONException {
        if (!obj.isNull(Members.PARAM_IDX.name())) {
            m_paramIndex = obj.getInt(Members.PARAM_IDX.name());
        }
    }

    @Override
    public void refineValueType(VoltType columnType) {
        if (m_valueType != null && m_valueType != VoltType.NUMERIC) {
            return;
        }
        if ((columnType == VoltType.FLOAT) || (columnType == VoltType.DECIMAL) || columnType.isInteger()) {
            m_valueType = columnType;
            m_valueSize = columnType.getLengthInBytesForFixedTypes();
            return;
        }
    }

    @Override
    public void refineOperandType(VoltType columnType) {
        if (m_valueType != null && m_valueType != VoltType.NUMERIC) {
            return;
        }
        if ((columnType == VoltType.FLOAT) || (columnType == VoltType.DECIMAL) || columnType.isInteger()) {
            m_valueType = columnType;
            m_valueSize = columnType.getLengthInBytesForFixedTypes();
        }
    }

    @Override
    public void finalizeValueTypes() {
        if (m_valueType != VoltType.NUMERIC) {
            return;
        }
        m_valueType = VoltType.FLOAT;
        m_valueSize = m_valueType.getLengthInBytesForFixedTypes();
    }

}
