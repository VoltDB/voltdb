/* This file is part of VoltDB.
 * Copyright (C) 2008-2014 VoltDB Inc.
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
import java.util.Map;

import org.json_voltpatches.JSONArray;
import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;
import org.json_voltpatches.JSONStringer;
import org.voltdb.VoltType;
import org.voltdb.planner.AbstractParsedStmt;
import org.voltdb.planner.parseinfo.StmtSubqueryScan;
import org.voltdb.plannodes.AbstractPlanNode;
import org.voltdb.types.ExpressionType;

/**
*
*/
public class SubqueryExpression extends AbstractExpression {

    public enum Members {
        SUBQUERY_ID,
        SUBQUERY_ROOT_NODE_ID,
        PARAM_IDX;
    }

    private StmtSubqueryScan m_subquery;
    private int m_subqueryId;
    private int m_subqueryNodeId = -1;
    private AbstractPlanNode m_subqueryNode = null;
    // TODO ENG_451 - better comment
    // List of parameter indexes that this subquery depends on
    private List<Integer> m_parameterIdxList = new ArrayList<Integer>();

    /**
     * Create a new SubqueryExpression
     * @param subquey The parsed statement
     */
    public SubqueryExpression(StmtSubqueryScan subquery) {
        super(ExpressionType.SUBQUERY);
        assert(subquery != null);
        m_subquery = subquery;
        assert(m_subquery.getSubquery() != null);
        m_subqueryId = m_subquery.getSubquery().m_stmtId;
        if (m_subquery.getBestCostPlan() != null && m_subquery.getBestCostPlan().rootPlanGraph != null) {
            m_subqueryNode = m_subquery.getBestCostPlan().rootPlanGraph;
            m_subqueryNodeId = m_subqueryNode.getPlanNodeId();
        }
        m_valueType = VoltType.BIGINT;
        m_valueSize = m_valueType.getLengthInBytesForFixedTypes();
        m_args = new ArrayList<AbstractExpression>();
        moveUpTVE();
    }

    /**
     * Create a new empty SubqueryExpression for loading from JSON
     * @param subquey The parsed statement
     */
    public SubqueryExpression() {
        super(ExpressionType.SUBQUERY);
    }

    public StmtSubqueryScan getTable() {
        return m_subquery;
    }

    public AbstractParsedStmt getSubquery() {
        return m_subquery.getSubquery();
    }

    public int getSubqueryId() {
        return m_subqueryId;
    }

    public int getSubqueryNodeId() {
        return m_subqueryNodeId;
    }

    public AbstractPlanNode getSubqueryNode() {
        return m_subqueryNode;
    }

    public void setSubqueryNode(AbstractPlanNode subqueryNode) {
        assert(subqueryNode != null);
        m_subqueryNode = subqueryNode;
        m_subqueryNodeId = m_subqueryNode.getPlanNodeId();
    }

    public List<Integer> getParameterIdxList() {
        return m_parameterIdxList;
    }

    @Override
    public Object clone() {
        SubqueryExpression clone = new SubqueryExpression(m_subquery);
        // The parameter TVE map must be cloned explicitly because the original TVEs
        // from the statement are already replaced with the corresponding PVEs
        clone.m_args = new ArrayList<AbstractExpression>();
        for (AbstractExpression arg: m_args) {
            clone.m_args.add((AbstractExpression)arg.clone());
        }
        clone.m_parameterIdxList = new ArrayList<Integer>();
        clone.m_parameterIdxList.addAll(m_parameterIdxList);
        return clone;
    }

    @Override
    public void validate() throws Exception {
        super.validate();

        if ((m_right != null) || (m_left != null))
            throw new Exception("ERROR: A subquery expression has child expressions for '" + this + "'");

    }


    @Override
    public boolean equals(Object obj) {
        if (obj == null || obj instanceof SubqueryExpression == false) {
            return false;
        }
        SubqueryExpression other = (SubqueryExpression) obj;
        // Expressions are equal if they have the same subquery id (refer to the same subquery)
        return m_subqueryId == other.m_subqueryId;
    }

    @Override
    public int hashCode() {
        int result = m_subquery.hashCode();
        // defer to the superclass, which factors in other attributes
        return result += super.hashCode();
    }

    @Override
    public void toJSONString(JSONStringer stringer) throws JSONException {
        super.toJSONString(stringer);
        stringer.key(Members.SUBQUERY_ID.name()).value(m_subqueryId);
        stringer.key(Members.SUBQUERY_ROOT_NODE_ID.name()).value(m_subqueryNodeId);
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
        m_subqueryNodeId = obj.getInt(Members.SUBQUERY_ROOT_NODE_ID.name());
        if (obj.has(Members.PARAM_IDX.name())) {
            JSONArray paramIdxArray = obj.getJSONArray(Members.PARAM_IDX.name());
            int paramSize = paramIdxArray.length();
            assert(m_args != null && paramSize == m_args.size());
            for (int i = 0; i < paramSize; ++i) {
                m_parameterIdxList.add(paramIdxArray.getInt(i));
            }
        }
    }

    @Override
    public String explain(String impliedTableName) {
        if (m_subqueryNode != null) {
            // Explain the subquery
            StringBuilder sb = new StringBuilder();
            m_subqueryNode.explainPlan_recurse(sb, "");
            return "(Subquery_" + m_subqueryId + " " + sb.toString() + "Subquery_"+
            m_subqueryId + ")";
        } else {
            return "(Subquery: null)";
        }
    }

    @Override
    public void finalizeValueTypes() {
        // Nothing to do there
    }

    private void moveUpTVE() {
        // Get TVE
        /** Traverse down the expression tree identifying all the TVEs which reference the
         * columns from the parent statement (getOrigStmtId() != this.subqueryId) and replace them with
         * the corresponding ParameterValueExpression. Keep the mapping between the original TVE
         * and new PVE which will be required by the back-end executor*/
        AbstractParsedStmt subqueryStmt = m_subquery.getSubquery();
        AbstractParsedStmt parentStmt = m_subquery.getSubquery().m_parentStmt;
        // we must have a parent -it's a subquery statement
        assert(parentStmt != null);
        for (Map.Entry<Integer, AbstractExpression> entry : subqueryStmt.m_parameterTveMap.entrySet()) {
            Integer paramIdx = entry.getKey();
            AbstractExpression expr = entry.getValue();
            if (expr instanceof AggregateExpression) {
                // Aggregate expression is always from THIS statement
                m_args.add(expr);
                m_parameterIdxList.add(paramIdx);
            } else if (expr instanceof TupleValueExpression) {
                TupleValueExpression tve = (TupleValueExpression) expr;
                if(tve.getOrigStmtId() == parentStmt.m_stmtId) {
                    // TVE originates from the statement where this SubqueryExpression belongs to
                    m_args.add(expr);
                    m_parameterIdxList.add(paramIdx);
                } else {
                    // TVE originates from the parent. Move it up
                    parentStmt.m_parameterTveMap.put(paramIdx, expr);
                }
            } else {
                // so far it should be either AggregateExpression or TupleValueExpression types
                assert(false);
            }
        }
        subqueryStmt.m_parameterTveMap.clear();
    }

}
