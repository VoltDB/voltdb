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

import org.hsqldb_voltpatches.FunctionForVoltDB;
import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;
import org.json_voltpatches.JSONStringer;
import org.voltdb.VoltType;
import org.voltdb.catalog.Table;
import org.voltdb.exceptions.ValidationError;
import org.voltdb.types.ExpressionType;

public class FunctionExpression extends AbstractExpression {
    private enum Members {
        NAME,
        IMPLIED_ARGUMENT, // implied argument is only one at the begining of function expression
        FUNCTION_ID,
        RESULT_TYPE_PARAM_IDX,
        OPTIONAL_ARGUMENT, // Optional argument is only one at the end of the function expression
    }

    private final static int NOT_PARAMETERIZED = -1;

    /// The name of the actual generic SQL function being invoked,
    /// normally this is just the upper-case formatted version of the function
    /// name that was used in the SQL statement.
    /// It can be something different when the statement used a function name
    /// that is just an alias or a specialization of another function.
    /// For example:
    /// Name used in SQL |  m_name
    /// ABS              |   ABS
    /// NOW              |   CURRENT_TIMESTAMP
    /// DAY              |   EXTRACT
    /// WEEK             |   EXTRACT
    /// EXTRACT          |   EXTRACT
    /// LTRIM            |   TRIM
    /// TRIM             |   TRIM
    private String m_name;

    /// An optional implied keyword argument, always upper case,
    /// normally null -- except for functions that had an initial keyword
    /// argument which got optimized out of its argument list prior to export
    /// from the HSQL front-end.
    /// For example:
    /// SQL invocation  |  m_impliedArgument
    /// ABS             |  null
    /// NOW             |  null
    /// DAY             |  DAY -- because of aliasing to EXTRACT(DAY...
    /// WEEK            |  WEEK -- because of aliasing to EXTRACT(WEEK...
    /// EXTRACT(DAY...  |  DAY
    /// EXTRACT(week... |  WEEK
    /// LTRIM           |  LEADING
    /// TRIM(LEADING... |  LEADING
    private String m_impliedArgument;

    /// Optional arguments string representation like "START", "END" in time_window
    /// We only support 1 optional argument at the end
    private String m_optionalArgument;

    /// The unique function (implementation) identifier for a named SQL
    /// function, assigned from constants defined in various HSQL frontend
    /// modules like Tokens.java, FunctionSQL.java, FunctionForVoltDB.java,
    /// etc. AND identically defined in functionsexpression.h.
    /// Aliases for the same function (implementation) will share an ID.
    /// For example, NOW and CURRENT_TIMESTAMP share one.
    /// Functions whose behavior is parameterized by a keyword argument will
    /// have a different function for each possible value of that argument
    /// For example, EXTRACT(DAY... and EXTRACT(WEEK... will have different IDs,
    /// which they share, respectively, with their aliases DAY(... and WEEK(...
    private int m_functionId;

    /// Defaults to the out-of-range value NOT_PARAMETERIZED for normal functions
    /// that have a fixed return type. For functions with a return type that depends
    /// on the type of one of its parameters, this is the index of that parameter.
    /// For example, 1 for the MOD function, 0 for ABS, etc.
    private int m_resultTypeParameterIndex = NOT_PARAMETERIZED;

    /// This is used for both initial construction
    /// and JSON deserialization.
    public FunctionExpression() {
        super();
        setExpressionType(ExpressionType.FUNCTION);
    }

    public FunctionExpression(String name, String impliedArgs, String optionalArgs, int id) {
        this();
        setAttributes(name, impliedArgs, optionalArgs, id);
    }

    public void setAttributes(String name, String impliedArgument, String optionalArgument, int id) {
        assert(name != null);
        m_name = name;
        m_impliedArgument = impliedArgument;
        m_optionalArgument = optionalArgument;
        m_functionId = id;
    }

    public boolean hasFunctionId(int functionId) { return m_functionId == functionId; }

    public void setResultTypeParameterIndex(int resultTypeParameterIndex) {
        m_resultTypeParameterIndex = resultTypeParameterIndex;
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
        AbstractExpression typing_arg = m_args.get(m_resultTypeParameterIndex);
        VoltType param_type = typing_arg.getValueType();
        VoltType value_type = getValueType();
        // The heuristic for which type to change is that any type (parameter type or return type) specified so far,
        // including NUMERIC is better than nothing. And that anything else is better than NUMERIC.
        if (value_type != param_type) {
            if (value_type == null) {
                value_type = param_type;
            }
            else if (value_type == VoltType.NUMERIC) {
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
                // what can be inferred later from the function call context.
                typing_arg.refineValueType(value_type, value_type.getMaxLengthInBytes());
            }
        }
        if (value_type != null) {
            setValueType(value_type);
            if (value_type != VoltType.INVALID && value_type != VoltType.NUMERIC) {
                int size = value_type.getMaxLengthInBytes();
                setValueSize(size);
            }
        }
    }


    @Override
    public void validate() {
        super.validate();
        //
        // Validate that there are no children other than the argument list (mandatory even if empty)
        //
        if (m_left != null) {
            throw new ValidationError("The left child expression '%s' for '%s' is not NULL",
                    m_left, toString());
        } else if (m_right != null) {
            throw new ValidationError("The right child expression '%s' for '%s' is not NULL",
                    m_right, toString());
        } else if (m_args == null) {
            throw new ValidationError("The function argument list for '%s' is NULL",
                    toString());
        } else if (m_name == null) {
            throw new ValidationError("The function name for '%s' is NULL", toString());
        } else if (m_resultTypeParameterIndex != NOT_PARAMETERIZED &&
                (m_resultTypeParameterIndex < 0 || m_resultTypeParameterIndex >= m_args.size())) {
                throw new ValidationError("The function parameter argument index '%d' for '%s' is out of bounds",
                        m_resultTypeParameterIndex, toString());
        }
    }

    @Override
    public boolean hasEqualAttributes(AbstractExpression obj) {
        if (! (obj instanceof FunctionExpression)) {
            return false;
        }
        FunctionExpression expr = (FunctionExpression) obj;

        // Function id determines all other attributes
        return m_functionId == expr.m_functionId;
    }

    @Override
    public int hashCode() {
        // based on implementation of equals
        int result = m_functionId;
        // defer to the superclass, which factors in arguments and other attributes
        return result += super.hashCode();
    }

    @Override
    public void toJSONString(JSONStringer stringer) throws JSONException {
        super.toJSONString(stringer);
        assert(m_name != null);
        stringer.keySymbolValuePair(Members.NAME.name(), m_name);
        stringer.keySymbolValuePair(Members.FUNCTION_ID.name(), m_functionId);
        if (m_impliedArgument != null) {
            stringer.keySymbolValuePair(Members.IMPLIED_ARGUMENT.name(), m_impliedArgument);
        }
        if (m_optionalArgument != null) {
            stringer.keySymbolValuePair(Members.OPTIONAL_ARGUMENT.name(), m_optionalArgument);
        }
        if (m_resultTypeParameterIndex != NOT_PARAMETERIZED) {
            stringer.keySymbolValuePair(Members.RESULT_TYPE_PARAM_IDX.name(), m_resultTypeParameterIndex);
        }
    }

    @Override
    protected void loadFromJSONObject(JSONObject obj) throws JSONException
    {
        m_name = obj.getString(Members.NAME.name());
        assert(m_name != null);
        m_functionId = obj.getInt(Members.FUNCTION_ID.name());
        m_impliedArgument = null;
        if (obj.has(Members.IMPLIED_ARGUMENT.name())) {
            m_impliedArgument = obj.getString(Members.IMPLIED_ARGUMENT.name());
        }
        if (obj.has(Members.OPTIONAL_ARGUMENT.name())) {
            m_optionalArgument = obj.getString(Members.OPTIONAL_ARGUMENT.name());
        }
        if (obj.has(Members.RESULT_TYPE_PARAM_IDX.name())) {
            m_resultTypeParameterIndex = obj.getInt(Members.RESULT_TYPE_PARAM_IDX.name());
        } else {
            m_resultTypeParameterIndex = NOT_PARAMETERIZED;
        }
    }

    @Override
    public void refineOperandType(VoltType columnType) {
        if (m_resultTypeParameterIndex == NOT_PARAMETERIZED) {
            // Non-parameterized functions should have a fixed SPECIFIC type.
            // Further refinement should be useless/un-possible.
            return;
        }
        // A parameterized function may be able to usefully refine its parameter argument's type
        // and have that change propagate up to its return type.
        if (m_valueType != null && m_valueType != VoltType.NUMERIC) {
            return;
        }
        AbstractExpression arg = m_args.get(m_resultTypeParameterIndex);
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
        if (m_resultTypeParameterIndex == NOT_PARAMETERIZED) {
            // Non-parameterized functions should have a fixed SPECIFIC type.
            // Further refinement should be useless/un-possible.
            return;
        }
        // A parameterized function may be able to usefully refine its parameter argument's type
        // and have that change propagate up to its return type.
        if (m_valueType != null && m_valueType != VoltType.NUMERIC) {
            return;
        }
        AbstractExpression arg = m_args.get(m_resultTypeParameterIndex);
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
        if (m_resultTypeParameterIndex == NOT_PARAMETERIZED) {
            // Non-parameterized functions should have a fixed SPECIFIC type.
            // Further refinement should be useless/un-possible.
            return;
        }
        // A parameterized function should reflect the final value of its parameter argument's type.
        AbstractExpression arg = m_args.get(m_resultTypeParameterIndex);
        m_valueType = arg.getValueType();
        m_valueSize = m_valueType.getMaxLengthInBytes();
    }


    @Override
    public void resolveForTable(Table table) {
        resolveChildrenForTable(table);
        if (m_resultTypeParameterIndex == NOT_PARAMETERIZED) {
            // Non-parameterized functions should have a fixed SPECIFIC type.
            // Further refinement should be useless/un-possible.
            return;
        }
        // resolving a child column has type implications for parameterized functions
        negotiateInitialValueTypes();
    }

    @Override
    public String explain(String impliedTableName) {
        assert(m_name != null);
        String result = m_name;

        if ( ! m_args.isEmpty()) {
            String connector = "(";

            // The purpose of m_impliedArgument is to allow functions with
            // different leading keyword arguments to be implemented as different
            // functions in VoltDB but to be "unified" back to their
            // original/generic form when explained to the user or
            // re-generated as SQL syntax for round trips back to the parser.
            // For example, SQL function invocations like
            //   "trim(leading 'X' from field)" and
            //   "trim(trailing 'X' from field)"
            // get invoked as separate volt functions.
            // They are modeled internally as something more like
            // "trim_leading('X', field)" and "trim_trailing('X', field)".
            // Note: it's actually m_functionId, not m_impliedParameter that
            // drives that distinction.

            // We slightly extended the supported SQL grammar to allow consistent
            // use of comma separators in the place of keyword separators,
            // even in non-traditional cases like
            //   "trim(leading, 'X', field)"
            // as a normalized equivalent of
            //   "trim(leading 'X' from field)".

            // SQL functions that use this mechanism include variants of
            // "extract", "since_epoch", "to_timestamp", "trim" and "truncate"
            // and their various aliases.
            // It is assumed that there is at least 1 explicit argument following
            // the implied argument (so m_args will not be empty for these cases).
            if (m_impliedArgument != null) {
                result += connector + m_impliedArgument;
                connector = ", ";
            }

            // Append each normal argument.
            for (AbstractExpression arg : m_args) {
                result += connector + arg.explain(impliedTableName);
                connector = ", ";
            }
            // Optional argument is only 1 at the end if need list modify this
            // we can have a single optional argument
            if (m_optionalArgument != null) {
                result += connector + m_optionalArgument;
            }
            result += ")";
        } else {
            // The two functions MIN_VALID_TIMESTAMP and MAX_VALID_TIMESTAMP
            // are nullary.  Others may be in the future.
            result += "()";
        }
        return result;
    }

    @Override
    public boolean isValueTypeIndexable(StringBuffer msg) {
        StringBuffer dummyMsg = new StringBuffer();
        if (!super.isValueTypeIndexable(dummyMsg)) {
            msg.append("a " + m_valueType.getName() + " valued function '"+ m_name.toUpperCase() + "'");
            return false;
        }
        return true;
    }

    @Override
    public void findUnsafeOperatorsForDDL(UnsafeOperatorsForDDL ops) {
        ops.add(explain("Be Explicit"));
    }

    /**
     * @return True iff this function is user defined.
     */
    public boolean isUserDefined() {
        return FunctionForVoltDB.isUserDefinedFunctionId(m_functionId);
    }

    /**
     * Return the name of this function.
     *
     * @return The name of the function in lower case.
     */
    public String getFunctionName() {
        return m_name.toLowerCase();
    }
}
