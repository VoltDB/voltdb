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

import java.util.ArrayList;
import java.util.List;

import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;
import org.json_voltpatches.JSONStringer;
import org.voltdb.VoltType;
import org.voltdb.types.ExpressionType;

/**
 *
 */
public class ParameterValueExpression extends AbstractValueExpression {

    public enum Members {
        PARAM_IDX;
    }

    int m_paramIndex = -1;
    private boolean m_paramIsVector = false;
    // Only parameters injected by the plan cache "parameterizer" have an associated constant
    // representing the original statement's constant value that the parameter replaces.
    // The constant value does not need to participate in parameter value identity (equality/hashing)
    // or serialization (export to EE).
    private ConstantValueExpression m_originalValue = null;
    // In case of subqueries, TVE from a parent query that is part of a correlated expression
    // is substituted with the PVE within the child. The m_correlatedExpr points back to the
    // original TVE for 'explain' purposes
    private AbstractExpression m_correlatedExpr;

    public ParameterValueExpression() {
        super(ExpressionType.VALUE_PARAMETER);
        m_correlatedExpr = null;
    }

    public ParameterValueExpression(int nextParamIndex, AbstractExpression expr) {
        super(ExpressionType.VALUE_PARAMETER);
        m_paramIndex = nextParamIndex;
        setValueType(expr.getValueType());
        setValueSize(expr.getValueSize());
        setInBytes(expr.getInBytes());
        m_correlatedExpr = expr;
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
        if (! (obj instanceof ParameterValueExpression)) {
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
        stringer.keySymbolValuePair(Members.PARAM_IDX.name(), m_paramIndex);
    }

    @Override
    public void loadFromJSONObject(JSONObject obj) throws JSONException {
        assert ! obj.isNull(Members.PARAM_IDX.name());
        m_paramIndex = obj.getInt(Members.PARAM_IDX.name());
    }

    @Override
    public void refineValueType(VoltType neededType, int neededSize) {
        if (m_originalValue != null) {
            // Do not push down a target type that contradicts the original constant value
            // of a generated parameter.
            m_originalValue.refineValueType(neededType, neededSize);
            VoltType fallbackType = m_originalValue.getValueType();
            if (fallbackType != neededType) {
                setValueType(fallbackType);
                setValueSize(fallbackType.getLengthInBytesForFixedTypes());
                return;
            }
        }
        // Otherwise, target knows best?
        setValueType(neededType);
        setValueSize(neededSize);
    }

    @Override
    public void refineOperandType(VoltType columnType) {
        if (columnType == null) {
        } else if (columnType == VoltType.FLOAT || columnType == VoltType.DECIMAL || columnType.isBackendIntegerType()) {
            m_valueType = columnType;
            m_valueSize = columnType.getLengthInBytesForFixedTypes();
        } else if (m_valueType == null) {
            m_valueType = columnType;
            m_valueSize = columnType.getMaxLengthInBytes();
        }
    }

    @Override
    public void finalizeValueTypes() {
        // The setting of m_valueType on each ParameterValueExpression is especially significant
        // because it drives the early argument type validation in voltQueueSQL or in
        // ProcedureRunner's parameter-set processing for single-statement procedures.
        // At this late stage of statement initialization,
        // it's considered better to force a specific type than to leave one that
        // will only cause problems in the ProcedureRunner (has been known to choke on null or NUMERIC)
        // or executor (has been known to choke at least on NUMERIC).
        // In many scenarios, the required type of the parameter has already been determined from its
        // expression context. The HSQL parser takes a first (flawed?) pass at this and it gets later
        // refined as VoltDB statement/expression initialization proceeds.
        // Yet gaps (cases of null and NUMERIC m_valueType) remain, requiring this finalizaton step
        // to fill them in.
        // Algorithm: If the parameter is from a parameterized constant, and the constant was integral,
        // take this as a sign that the parameter's value (and future values when the parameterized
        // plan can get re-used) should be integral.
        // Otherwise fall back to FLOAT. The rationale for that choice is that it seems to
        // work out for the cases that have historically fallen through the cracks.
        //
        // TODO: consider leaving room for type ambiguity.
        // It is possible that some of the problem being "solved" here is actually better solved by
        // de-sensitizing the ProcedureRunner and/or EE.
        // Rationale: It seems plausible for a plan to not do very much with a parameter
        // that would indicate/restrict the type of the parameter.
        // Such a plan might just work regardless of the type of the actual parameter value
        // -- as long as the actual parameter had any valid concrete type.
        // Similarly, a plan that applied some generic arithmetic to a parameter in a way that did not
        // indicate or constrain the result type might just work regardless of the type of the actual
        // parameter value -- EXCEPT for the requirement that it be some concrete NUMERIC sub-type.
        // There are dozens of lines of NValue code in the EE that guard against abuses like applying
        // arithmetic to strings or string operations to numeric types, but it seems better to catch
        // the more obvious cases that involve constants at planning time -- as the HSQL parser tends
        // to do -- and to catch the more obvious cases that involve parameters at statement invocation
        // time in voltQueueSQL and ProcedureRunner.
        // There are also dozens of lines of NValue code in the EE intending to provide flexibility
        // in choice of exact numeric types to arithmetic operators, etc., so there is some reason to
        // have confidence that at least SOME plans are capable of correct execution when invoked with
        // variously typed parameters, especially if restricted to various numeric types.
        // It is POSSIBLE that the EE's over-sensitivity to ambiguous types is
        // isolated to something obscure like expression deserialization.
        // That is, it may be gagging on input that it could otherwise easily stomach.
        // ON THE OTHER HAND, it's not guaranteed that the planner itself is quite ready to
        // operate and generate optimal plans when parameter types may be left ambiguous here.
        // Worst case, is it possible that the determination of parameter types here is influencing
        // the choice of the best applicable plan, so that plan choice would have to be
        // conditional on the concrete type of the parameters passed in?
        // There IS (or was?) at least one case of type-checking in the code that considers
        // indexed access paths, but it might not be critical (i.e. might be obsolete).
        if (m_valueType != null && m_valueType != VoltType.NUMERIC) {
            return;
        }
        // BigInt or Float, Decimal is not selected here because of its range is smaller
        VoltType fallbackType = VoltType.FLOAT;
        if (m_originalValue != null) {
            m_originalValue.refineOperandType(VoltType.BIGINT);
            fallbackType = m_originalValue.getValueType(); // Typically BIGINT or FLOAT.
        }
        m_valueType = fallbackType;
        m_valueSize = m_valueType.getLengthInBytesForFixedTypes();
    }

    public void setOriginalValue(ConstantValueExpression cve) {
        m_originalValue = cve;
        // only called in AbstractParsedStmt.parseValueExpression()
        // when needConstant and needParamter are both true
        // the size of m_originalValue is MAX_LENGTH for STRING
        // or VARCHAR, and fixed length for other types
        setValueType(m_originalValue.getValueType());
        setValueSize(m_originalValue.getValueSize());
    }

    public ConstantValueExpression getOriginalValue() {
        return m_originalValue;
    }

    public AbstractExpression getCorrelatedExpression() {
        return m_correlatedExpr;
    }

    public void setCorrelatedExpression(AbstractExpression correlatedExpr) {
        m_correlatedExpr = correlatedExpr;
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
        if (m_correlatedExpr == null) {
            return "?" + m_paramIndex;
        } else {
            return m_correlatedExpr.explain(unused);
        }
    }

    // Mark a parameter as vector-valued, so that it can properly drive argument type checking for
    // cases like "col in ?", especially for single-statement procedures. This setting is irreversible.
    public void setParamIsVector() {
        m_paramIsVector = true;
    }

    public boolean getParamIsVector() {
        return m_paramIsVector;
    }

}
