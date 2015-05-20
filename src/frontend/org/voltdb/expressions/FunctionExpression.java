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

import org.hsqldb_voltpatches.FunctionForVoltDB;
import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;
import org.json_voltpatches.JSONStringer;
import org.voltdb.VoltType;
import org.voltdb.catalog.Table;
import org.voltdb.types.ExpressionType;

public class FunctionExpression extends AbstractExpression {
    public enum Members {
        NAME,
        ALIAS,
        FUNCTION_ID,
        PARAMETER_ARG,
    }

    private final static int NOT_PARAMETERIZED = -1;

    private String m_name;
    private String m_alias;
    private int m_functionId;
    private int m_parameterArg = NOT_PARAMETERIZED;

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

    public void setAttributes(String name, String volt_alias, int id) {
        m_name = name;
        m_alias = volt_alias;
        m_functionId = id;
    }

    public int getFunctionId() {
        return m_functionId;
    }

    public void setParameterArg(int parameterArg) {
        m_parameterArg = parameterArg;
    }

    /** Negotiate the type(s) of the parameterized function's result and its parameter argument.
     * This avoids a fatal "non-castable type" runtime exception.
     */
    public void negotiateInitialValueTypes() {
        // Either of the function result type or parameter type could be null or a specific supported value type, or a generic
        // NUMERIC. Replace any "generic" type (null or NUMERIC) with the more specific type without over-specifying
        // -- the BEST type might only become clear later when the context/caller of this function is parsed, so don't
        // risk guessing wrong here just for the sake of specificity.
        // There will be a "finalize" pass over the completed expression tree to finish specifying any remaining "generics".

        // DO use the type chosen by HSQL for the parameterized function as a specific type hint
        // for numeric constant arguments that could either go decimal or float.
        AbstractExpression param_arg = m_args.get(m_parameterArg);
        VoltType param_type = param_arg.getValueType();
        VoltType value_type = getValueType();
        // The heuristic for which type to change is that any type (parameter type or return type) specified so far,
        // including NUMERIC is better than nothing. And that anything else is better than NUMERIC.
        if (value_type != param_type) {
            if (value_type == null) {
                value_type = param_type;
            } else if (value_type == VoltType.NUMERIC) {
                if (param_type != null) {
                    value_type = param_type;
                }
                // Pushing a type DOWN to the argument is a lot like work, and not worth it just to
                // propagate down a known NUMERIC return type,
                // since it will just have to be re-specialized when a more specific type is inferred from
                // the context or finalized when the expression is complete.
            } else if ((param_type == null) || (param_type == VoltType.NUMERIC)) {
                // The only purpose of refining the parameter argument's type is to force a more specific
                // refinement than NUMERIC as implied by HSQL, in case that might be more specific than
                // what can be be inferred later from the function call context.
                param_arg.refineValueType(value_type, value_type.getMaxLengthInBytes());
            }
        }
        if (value_type != null) {
            setValueType(value_type);
            setValueSize(value_type.getMaxLengthInBytes());
        }
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
        if (m_parameterArg != NOT_PARAMETERIZED) {
            if (m_parameterArg < 0 || ((m_args != null) && m_parameterArg >= m_args.size())) {
                throw new Exception("ERROR: The function parameter argument index '" + m_parameterArg + "' for '" + this + "' is out of bounds");
            }
        }

    }

    @Override
    public Object clone() {
        FunctionExpression clone = (FunctionExpression)super.clone();
        clone.m_name = m_name;
        clone.m_alias = m_alias;
        clone.m_functionId = m_functionId;
        clone.m_parameterArg = m_parameterArg;
        return clone;
    }

    @Override
    public boolean hasEqualAttributes(AbstractExpression obj) {
        if (obj instanceof FunctionExpression == false) {
            return false;
        }
        FunctionExpression expr = (FunctionExpression) obj;

        assert(m_name != null);
        if (m_name == null) {
            // This is most unpossible. BUT...
            // better to fail the equality test than to embarrass ourselves in production
            // (when asserts are turned off) with an NPE on the next line.
            return false;
        }
        if (m_name.equals(expr.m_name) == false) {
            return false;
        }
        if (m_alias == null) {
            if (expr.m_alias != null) {
                return false;
            }
        } else {
            if (m_alias.equals(expr.m_alias) == false) {
                return false;
            }
        }
        if (m_functionId != expr.m_functionId) {
            return false;
        }
        if (m_parameterArg != expr.m_parameterArg) {
            return false;
        }
        return true;
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
    protected void loadFromJSONObject(JSONObject obj) throws JSONException
    {
        m_name = obj.getString(Members.NAME.name());
        m_alias = obj.getString(Members.ALIAS.name());
        m_functionId = obj.getInt(Members.FUNCTION_ID.name());
        m_parameterArg = obj.getInt(Members.PARAMETER_ARG.name());
    }

    @Override
    public void refineOperandType(VoltType columnType) {
        if (m_parameterArg == NOT_PARAMETERIZED) {
            // Non-parameterized functions should have a fixed SPECIFIC type.
            // Further refinement should be useless/un-possible.
            return;
        }
        // A parameterized function may be able to usefully refine its parameter argument's type
        // and have that change propagate up to its return type.
        if (m_valueType != null && m_valueType != VoltType.NUMERIC) {
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
    public void refineValueType(VoltType neededType, int neededSize) {
        if (m_parameterArg == NOT_PARAMETERIZED) {
            // Non-parameterized functions should have a fixed SPECIFIC type.
            // Further refinement should be useless/un-possible.
            return;
        }
        // A parameterized function may be able to usefully refine its parameter argument's type
        // and have that change propagate up to its return type.
        if (m_valueType != null && m_valueType != VoltType.NUMERIC) {
            return;
        }
        AbstractExpression arg = m_args.get(m_parameterArg);
        VoltType valueType = arg.getValueType();
        if (valueType != null && valueType != VoltType.NUMERIC) {
            return;
        }
        // No assumption is made that functions that are parameterized by
        // variably-sized types are size-preserving, so allow any size
        arg.refineValueType(neededType, neededType.getMaxLengthInBytes());
        m_valueType = arg.getValueType();
        m_valueSize = m_valueType.getMaxLengthInBytes();
    }

    @Override
    public void finalizeValueTypes()
    {
        finalizeChildValueTypes();
        if (m_parameterArg == NOT_PARAMETERIZED) {
            // Non-parameterized functions should have a fixed SPECIFIC type.
            // Further refinement should be useless/un-possible.
            return;
        }
        // A parameterized function should reflect the final value of its parameter argument's type.
        AbstractExpression arg = m_args.get(m_parameterArg);
        m_valueType = arg.getValueType();
        m_valueSize = m_valueType.getMaxLengthInBytes();
    }


    @Override
    public void resolveForTable(Table table) {
        resolveChildrenForTable(table);
        if (m_parameterArg == NOT_PARAMETERIZED) {
            // Non-parameterized functions should have a fixed SPECIFIC type.
            // Further refinement should be useless/un-possible.
            return;
        }
        // resolving a child column has type implications for parameterized functions
        negotiateInitialValueTypes();
    }

    @Override
    public String explain(String impliedTableName) {
        String result = m_name;
        String connector = "(";
        // This is temporary and will be replaced once the Unit Attribute is in the XML
        if (FunctionForVoltDB.isUnitFunction(m_functionId)) {
            result += connector + m_alias.substring(m_name.length()+1);
            connector = ", ";
        }
        if (m_args != null) {
            for (AbstractExpression arg : m_args) {
                result += connector + arg.explain(impliedTableName);
                connector = ", ";
            }
            result += ")";
        }
        return result;
    }

}
