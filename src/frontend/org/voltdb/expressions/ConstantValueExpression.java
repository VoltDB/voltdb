/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB L.L.C.
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

import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONStringer;
import org.voltdb.VoltType;
import org.voltdb.catalog.Database;
import org.voltdb.types.*;

/**
 *
 */
public class ConstantValueExpression extends AbstractValueExpression {

    public enum Members {
        VALUE;
    }

    protected String m_value = null;

    public ConstantValueExpression() {
        super(ExpressionType.VALUE_CONSTANT);
    }
    public ConstantValueExpression(AbstractExpression left, AbstractExpression right) {
        super(ExpressionType.VALUE_CONSTANT, null, null);
        assert(left == null);
        assert(right == null);
    }

    @Override
    public Object clone() throws CloneNotSupportedException {
        ConstantValueExpression clone = (ConstantValueExpression)super.clone();
        clone.m_value = m_value;
        return clone;
    }

    @Override
    public void validate() throws Exception {
        super.validate();

        // Make sure our value is not null
        if (m_value == null) {
            throw new Exception("ERROR: The constant value for '" + this + "' is NULL");

        // Make sure the value type is something we support here
        } else if (m_valueType == VoltType.NULL ||
                   m_valueType == VoltType.VOLTTABLE) {
            throw new Exception("ERROR: Invalid constant value type '" + m_valueType + "' for '" + this + "'");
        }
    }

    /**
     * @return the value
     */
    public String getValue() {
        return m_value;
    }

    /**
     * @param value the value to set
     */
    public void setValue(String value) {
        m_value = value;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof ConstantValueExpression == false) return false;
        ConstantValueExpression expr = (ConstantValueExpression) obj;

        if ((expr.m_value == null) != (m_value == null))
            return false;

        if (expr.m_value != null)
            if (expr.m_value.equals(m_value) == false)
                return false;

        // if all seems well, defer to the superclass, which checks kids
        return super.equals(obj);
    }

    @Override
    public void toJSONString(JSONStringer stringer) throws JSONException {
        super.toJSONString(stringer);
        stringer.key(Members.VALUE.name());
        switch (m_valueType) {
        case INVALID:
            throw new JSONException("ConstantValueExpression.toJSONString(): value_type should never be VoltType.INVALID");
        case NULL:
            throw new JSONException("ConstantValueExpression.toJSONString(): And they should be never be this either! VoltType.NULL");
        case TINYINT:
            stringer.value(Long.valueOf(m_value));
            break;
        case SMALLINT:
            stringer.value(Long.valueOf(m_value));
            break;
        case INTEGER:
            stringer.value(Long.valueOf(m_value));
            break;
        case BIGINT:
            stringer.value(Long.valueOf(m_value));
            break;
        case FLOAT:
            stringer.value(Double.valueOf(m_value));
            break;
        case STRING:
            stringer.value(m_value);
            break;
        case TIMESTAMP:
            stringer.value(Long.valueOf(m_value));
            break;
        case DECIMAL:
            stringer.value(m_value);
            break;
        default:
            throw new JSONException("ConstantValueExpression.toJSONString(): Unrecognized value_type " + m_valueType);
        }
    }

    @Override
    public void loadFromJSONObject(JSONObject obj, Database db) throws JSONException {
        if (!obj.isNull(Members.VALUE.name())) {
            m_value = obj.getString(Members.VALUE.name());
        }
    }
}
