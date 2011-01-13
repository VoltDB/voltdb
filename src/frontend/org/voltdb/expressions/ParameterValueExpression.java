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

import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;
import org.json_voltpatches.JSONStringer;
import org.voltdb.catalog.Database;
import org.voltdb.types.*;

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
    public ParameterValueExpression(AbstractExpression left, AbstractExpression right) {
        super(ExpressionType.VALUE_PARAMETER, right, left);
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
    public Integer getParameterId() {
        return m_paramIndex;
    }

    /**
     * @param paramIndex The index of the parameter to set
     */
    public void setParameterId(Integer paramIndex) {
        m_paramIndex = paramIndex;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof ParameterValueExpression == false) return false;
        ParameterValueExpression expr = (ParameterValueExpression) obj;

        if (expr.m_paramIndex != m_paramIndex)
            return false;

        // if all seems well, defer to the superclass, which checks kids
        return super.equals(obj);
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
}
