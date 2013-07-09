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

import java.util.ArrayList;
import java.util.List;

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
    // Only parameters injected by the plan cache "parameterizer" have an associated constant
    // representing the original statement's constant value that the parameter replaces.
    // The constant value does not need to participate in parameter value identity (equality/hashing)
    // or serialization (export to EE).
    private ConstantValueExpression m_originalValue = null;

    public ParameterValueExpression() {
        super(ExpressionType.VALUE_PARAMETER);
    }

    @Override
    public Object clone() {
        ParameterValueExpression clone = (ParameterValueExpression)super.clone();
        clone.m_paramIndex = m_paramIndex;
        clone.m_originalValue = m_originalValue;
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
        if (columnType == null) {
            return;
        }
        if ((columnType == VoltType.FLOAT) || (columnType == VoltType.DECIMAL) || columnType.isInteger()) {
            m_valueType = columnType;
            m_valueSize = columnType.getLengthInBytesForFixedTypes();
        } else if (m_valueType == null) {
            m_valueType = columnType;
            m_valueSize = columnType.getMaxLengthInBytes();
        }
    }

    @Override
    public void finalizeValueTypes() {
        // At this late stage, it's better to force a specific type than to leave one that will only
        // cause problems in the ProcedureRunner (chokes on null or NUMERIC) or executor (chokes on NUMERIC).
        if (m_valueType != null && m_valueType != VoltType.NUMERIC) {
            return;
        }
        m_valueType = VoltType.FLOAT;
        m_valueSize = m_valueType.getLengthInBytesForFixedTypes();
    }

    public void setOriginalValue(ConstantValueExpression cve) {
        m_originalValue = cve;
    }

    public ConstantValueExpression getOriginalValue() {
        return m_originalValue;
    }

    // Return this parameter in a list of bound parameters if the expr argument is in fact
    // its original value constant "binding". This ensures that the index plan that depends
    // on the parameter binding to a critical constant value does not get misapplied to a later
    // query in which that constant differs.
    @Override
    public List<AbstractExpression> bindingToIndexedExpression(AbstractExpression expr) {
        if (m_originalValue == null || ! m_originalValue.equals(expr)) {
            return null;
        }
        // This parameter's value was matched, so return this as one bound parameter.
        List<AbstractExpression> result = new ArrayList<AbstractExpression>();
        result.add(this);
        return result;
    }

    @Override
    public String explain(String unused) {
        return "?" + m_paramIndex;
    }

}
