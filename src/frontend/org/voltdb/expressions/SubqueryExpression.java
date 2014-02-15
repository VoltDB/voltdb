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

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;
import org.json_voltpatches.JSONStringer;
import org.voltdb.VoltType;
import org.voltdb.planner.AbstractParsedStmt;
import org.voltdb.planner.parseinfo.TempTable;
import org.voltdb.plannodes.AbstractPlanNode;
import org.voltdb.types.ExpressionType;

/**
*
*/
public class SubqueryExpression extends AbstractExpression {

    public enum Members {
        SUBQUERY_ID,
        SUBQUERY_ROOT_NODE_ID;
    }

    private TempTable m_subquery;
    private int m_subqueryId;
    private int m_subqueryNodeId = -1;
    private AbstractPlanNode m_subqueryNode = null;
    // Parent TVE that are referenced from the child subquery mapped to the corresponding parameter
    // from that statement
    private Map<Integer, TupleValueExpression> m_parameterTveMap = new HashMap<Integer, TupleValueExpression>();

    /**
     * Create a new SubqueryExpression
     * @param subquey The parsed statement
     */
    public SubqueryExpression(TempTable subquery) {
        super(ExpressionType.SUBQUERY);
        assert(subquery != null);
        m_subquery = subquery;
        assert(m_subquery.getSubQuery() != null);
        m_subqueryId = m_subquery.getSubQuery().stmtId;
        if (m_subquery.getBetsCostPlan() != null && m_subquery.getBetsCostPlan().rootPlanGraph != null) {
            m_subqueryNode = m_subquery.getBetsCostPlan().rootPlanGraph;
            m_subqueryNodeId = m_subqueryNode.getPlanNodeId();
        }
        AbstractExpression expr = m_subquery.getSubQuery().joinTree.getAllFilters();
        replaceParentTveWithPve(expr);
        m_valueType = VoltType.BIGINT;
        m_valueSize = m_valueType.getLengthInBytesForFixedTypes();
    }

    /**
     * Create a new empty SubqueryExpression for loading from JSON
     * @param subquey The parsed statement
     */
    public SubqueryExpression() {
        super(ExpressionType.SUBQUERY);
    }

    public TempTable getTable() {
        return m_subquery;
    }

    public AbstractParsedStmt getSubquery() {
        return m_subquery.getSubQuery();
    }

    public Collection<TupleValueExpression> getParameterTves() {
        return m_parameterTveMap.values();
    }

    public Map<Integer, TupleValueExpression> getParameterTveMap() {
        return m_parameterTveMap;
    }

    public void setParameterTveMap(Map<Integer, TupleValueExpression> parameterTveMap) {
        m_parameterTveMap = parameterTveMap;
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


    @Override
    public Object clone() {
        SubqueryExpression clone = new SubqueryExpression(m_subquery);
        // The parent TVE map must be cloned explicitly because the original TVEs
        // from the statement are already replaced with the corresponding PVEs
        clone.m_parameterTveMap = new HashMap<Integer, TupleValueExpression>();
        for (Map.Entry<Integer, TupleValueExpression> tveEntry: m_parameterTveMap.entrySet()) {
            clone.m_parameterTveMap.put(tveEntry.getKey(), (TupleValueExpression)tveEntry.getValue().clone());
        }
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
    }

    @Override
    protected void loadFromJSONObject(JSONObject obj) throws JSONException {
        m_subqueryId = obj.getInt(Members.SUBQUERY_ID.name());
        m_subqueryNodeId = obj.getInt(Members.SUBQUERY_ROOT_NODE_ID.name());
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

    /** Traverse down the expression tree identifying all the TVEs which reference the
     * columns from the parent statement (getOrigStmtId() != this.subqueryId) and replace them with
     * the corresponding ParameterValueExpression. Keep the mapping between the original TVE
     * and new PVE which will be required by the back-end executor
     *
     * @param expr
     * @return modified expression
     */
    private AbstractExpression replaceParentTveWithPve(AbstractExpression expr) {
        if (expr == null) {
            return null;
        } else if (expr instanceof TupleValueExpression) {
            if (((TupleValueExpression)expr).getOrigStmtId() != m_subqueryId) {
                int paramIdx = AbstractParsedStmt.NEXT_PARAMETER_ID++;
                ParameterValueExpression pve = new ParameterValueExpression();
                pve.setParameterIndex(paramIdx);
                pve.setValueSize(expr.getValueSize());
                pve.setValueType(expr.getValueType());
                m_parameterTveMap.put(paramIdx, (TupleValueExpression)expr);
                return pve;
            }
        } else {
            expr.setLeft(replaceParentTveWithPve(expr.getLeft()));
            expr.setRight(replaceParentTveWithPve(expr.getRight()));
        }
        return expr;
    }
}
