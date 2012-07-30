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

public class FunctionExpression extends AbstractExpression {
    public enum Members {
        NAME,
        ALIAS,
        FUNCTION_ID,
        PARAMETER_ARG,
    }

    private String m_name;
    private String m_alias;
    private int m_functionId;
    private int m_parameterArg;

    public int getParameterArg() {
        return m_parameterArg;
    }

    public FunctionExpression() {
        //
        // This is needed for serialization
        //
        super();
        setExpressionType(ExpressionType.FUNCTION);
    }

    public void setAttributes(String name, String volt_alias, int id, int parameterArg) {
        m_name = name;
        m_alias = volt_alias;
        m_functionId = id;
        m_parameterArg = parameterArg;
    }

    @Override
    public void validate() throws Exception {
        super.validate();
        //
        // Validate that there are no children other than the argument list (mandatory even if empty)
        //
        if (m_left != null) {
            throw new Exception("ERROR: The left child expression '" + m_left + "' for '" + this + "' is not NULL");
        }

        if (m_right != null) {
            throw new Exception("ERROR: The right child expression '" + m_right + "' for '" + this + "' is not NULL");
        }

        if (m_args == null) {
            throw new Exception("ERROR: The function argument list for '" + this + "' is NULL");
        }

        if (m_name == null) {
            throw new Exception("ERROR: The function name for '" + this + "' is NULL");
        }
        if (m_parameterArg < -1 || ((m_args != null) && m_parameterArg >= m_args.size())) {
            throw new Exception("ERROR: The function parameter argument index '" + m_parameterArg + "' for '" + this + "' is out of bounds");
        }

    }

    @Override
    public Object clone() throws CloneNotSupportedException {
        FunctionExpression clone = (FunctionExpression)super.clone();
        clone.m_name = m_name;
        clone.m_alias = m_alias;
        clone.m_functionId = m_functionId;
        clone.m_parameterArg = m_parameterArg;
        return clone;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof FunctionExpression == false) return false;
        FunctionExpression expr = (FunctionExpression) obj;

        if (expr.m_name == null)
            return false;
        if (expr.m_name.equals(m_name) == false)
            return false;

        if (expr.m_alias == null) {
            if (m_alias != null) {
                return false;
            }
        } else {
            if (expr.m_alias.equals(m_alias) == false) {
                return false;
            }
        }
        if (expr.m_functionId != m_functionId)
            return false;
        if (expr.m_parameterArg != m_parameterArg)
            return false;

        // if all seems well, defer to the superclass, which checks kids
        return super.equals(obj);
    }

    @Override
    public int hashCode() {
        // based on implementation of equals
        int result = 0;
        result += m_name.hashCode();
        if (m_alias != null) {
            result += m_alias.hashCode();
        }
        result += m_functionId;
        // defer to the superclass, which factors in other attributes
        return result += super.hashCode();
    }

    @Override
    public void toJSONString(JSONStringer stringer) throws JSONException {
        super.toJSONString(stringer);
        stringer.key(Members.NAME.name()).value(m_name);
        if (m_alias != null) {
            stringer.key(Members.ALIAS.name()).value(m_alias);
        }
        stringer.key(Members.FUNCTION_ID.name()).value(m_functionId);
        stringer.key(Members.PARAMETER_ARG.name()).value(m_parameterArg);
    }

    @Override
    protected void loadFromJSONObject(JSONObject obj, Database db) throws JSONException {
        m_name = obj.getString(Members.NAME.name());
        m_alias = obj.getString(Members.ALIAS.name());
        m_functionId = obj.getInt(Members.FUNCTION_ID.name());
        m_parameterArg = obj.getInt(Members.PARAMETER_ARG.name());
    }

    @Override
    public void refineOperandType(VoltType columnType) {
        if (m_valueType != null && m_valueType != VoltType.NUMERIC) {
            return;
        }
        if (m_parameterArg == -1) {
            return;
        }
        AbstractExpression arg = m_args.get(m_parameterArg);
        VoltType valueType = arg.getValueType();
        if (valueType != null && valueType != VoltType.NUMERIC) {
            return;
        }
        arg.refineOperandType(columnType);
        m_valueType = arg.getValueType();
        m_valueSize = m_valueType.getLengthInBytesForFixedTypes();
    }

    @Override
    public void refineValueType(VoltType columnType) {
        if (m_valueType != null && m_valueType != VoltType.NUMERIC) {
            return;
        }
        if (m_parameterArg == -1) {
            return;
        }
        AbstractExpression arg = m_args.get(m_parameterArg);
        VoltType valueType = arg.getValueType();
        if (valueType != null && valueType != VoltType.NUMERIC) {
            return;
        }
        arg.refineValueType(columnType);
        m_valueType = arg.getValueType();
        m_valueSize = m_valueType.getLengthInBytesForFixedTypes();
    }

    @Override
    public void finalizeValueTypes()
    {
        finalizeChildValueTypes();
        if (m_parameterArg != -1) {
            AbstractExpression arg = m_args.get(m_parameterArg);
            m_valueType = arg.getValueType();
            m_valueSize = m_valueType.getMaxLengthInBytes();
        }
    }

}
