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

import org.json_voltpatches.JSONArray;
import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;
import org.json_voltpatches.JSONStringer;
import org.voltdb.VoltType;
import org.voltdb.catalog.Database;
import org.voltdb.planner.ParameterizationInfo;
import org.voltdb.plannodes.AbstractPlanNode;

/**
 * Base class to represent sub-select, row, and scalar expressions
 * The value type is always a BIGINT because the expression's eval method on the EE side
 * returns the subquery id as a result.
 * This id can be used by others expressions to access the actual subquery results
*/
public abstract class AbstractSubqueryExpression extends AbstractExpression {

    public static final String SUBQUERY_TAG = "Subquery_";

    public enum Members {
        SUBQUERY_ID,
        PARAM_IDX;
    }
    // subquery unique id
    protected int m_subqueryId;
    // subquery root plan node
    protected AbstractPlanNode m_subqueryNode = null;
    // List of correlated parameter indexes that originate at the immediate parent's level
    // and need to be set by this SubqueryExpression on the EE side prior to the evaluation
    private List<Integer> m_parameterIdxList = new ArrayList<>();

    protected AbstractSubqueryExpression() {
        m_valueType = VoltType.BIGINT;
        m_valueSize = m_valueType.getLengthInBytesForFixedTypes();
    }

    public int getSubqueryId() {
        return m_subqueryId;
    }

    public int getSubqueryNodeId() {
        // -1 is used to indicate there is no subquery node
        return (m_subqueryNode != null) ? m_subqueryNode.getPlanNodeId() : -1;
    }

    public AbstractPlanNode getSubqueryNode() {
        return m_subqueryNode;
    }

    public void setSubqueryNode(AbstractPlanNode subqueryNode) {
        assert(subqueryNode != null);
        m_subqueryNode = subqueryNode;
    }

    public List<Integer> getParameterIdxList() {
        return m_parameterIdxList;
    }

    // Create a matching PVE for this expression to be used on the EE side
    // to get the original expression value
    protected void addCorrelationParameterValueExpression(AbstractExpression expr, List<AbstractExpression> pves) {
        int paramIdx = ParameterizationInfo.getNextParamIndex();
        m_parameterIdxList.add(paramIdx);
        ParameterValueExpression pve = new ParameterValueExpression(paramIdx, expr);
        pves.add(pve);
    }

    protected void addArgumentParameter(Integer paramIdx, AbstractExpression expr) {
        m_args.add(expr);
        m_parameterIdxList.add(paramIdx);
    }

    public  int overrideSubqueryNodeIds(int newId) {
        assert(m_subqueryNode != null);
        return m_subqueryNode.resetPlanNodeIds(newId);
    }

    @Override
    public AbstractSubqueryExpression clone() {
        AbstractSubqueryExpression clone = (AbstractSubqueryExpression) super.clone();
        clone.m_parameterIdxList = new ArrayList<>(m_parameterIdxList);
        return clone;
    }

    @Override
    public void validate() {
        super.validate();

        if (m_subqueryNode != null) {
            m_subqueryNode.validate();
        }
    }

    @Override
    public boolean equals(Object obj) {
        if (super.equals(obj) && obj instanceof AbstractSubqueryExpression) {
            AbstractSubqueryExpression other = (AbstractSubqueryExpression) obj;
            // Expressions are equal if they have the same subquery id (refer to the same subquery)
            return m_subqueryId == other.m_subqueryId;
        }
        return false;
    }

    @Override
    public int hashCode() {
        // defer to the superclass, which factors in other attributes
        int result = super.hashCode();
        result += m_subqueryId;
        return result;
    }

    @Override
    public void toJSONString(JSONStringer stringer) throws JSONException {
        super.toJSONString(stringer);
        stringer.keySymbolValuePair(Members.SUBQUERY_ID.name(), m_subqueryId);
        // Output the correlated parameter ids that originates at this subquery immediate
        // parent and need to be set before the evaluation
        if (!m_parameterIdxList.isEmpty()) {
            stringer.key(Members.PARAM_IDX.name()).array();
            for (Integer idx : m_parameterIdxList) {
                stringer.value(idx);
            }
            stringer.endArray();
        }
    }

    @Override
    protected void loadFromJSONObject(JSONObject obj) throws JSONException {
        m_subqueryId = obj.getInt(Members.SUBQUERY_ID.name());
        if (obj.has(AbstractExpression.Members.VALUE_TYPE)) {
            m_valueType = VoltType.get((byte) obj.getInt(AbstractExpression.Members.VALUE_TYPE));
            m_valueSize = m_valueType.getLengthInBytesForFixedTypes();
        }
        if (obj.has(Members.PARAM_IDX.name())) {
            JSONArray paramIdxArray = obj.getJSONArray(Members.PARAM_IDX.name());
            int paramSize = paramIdxArray.length();
            assert(m_args != null);
            for (int i = 0; i < paramSize; ++i) {
                m_parameterIdxList.add(paramIdxArray.getInt(i));
            }
        }
    }

    @Override
    public void finalizeValueTypes() {
        // Nothing to do there
    }

    public void resolveColumnIndexes() {
        if (m_subqueryNode != null) {
            m_subqueryNode.resolveColumnIndexes();
        }
    }

    public void generateOutputSchema(Database db) {
        if (m_subqueryNode != null) {
            m_subqueryNode.generateOutputSchema(db);
        }
    }

    @Override
    public String getContentDeterminismMessage() {
        return null;
    }
}
