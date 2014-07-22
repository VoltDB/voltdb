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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
        PARAM_IDX,
        OTHER_PARAM_IDX;
    }

    public static final String SUBQUERY_TAG = "Subquery_";

    private StmtSubqueryScan m_subquery;
    private int m_subqueryId;
    private int m_subqueryNodeId = -1;
    private AbstractPlanNode m_subqueryNode = null;
    // List of correlated parameter indexes that originate at the immediate parent's level
    // and need to be set by this SubqueryExpression on the EE side prior to the evaluation
    private List<Integer> m_parameterIdxList = new ArrayList<Integer>();
    // List of all correlated parameter indexes this subquery and its descendants depend on
    // They may originate at different levels in the subquery hierarchy.
    private List<Integer> m_allParameterIdxList = new ArrayList<Integer>();

    /**
     * Create a new SubqueryExpression
     * @param subquey The parsed statement
     */
    public SubqueryExpression(StmtSubqueryScan subquery) {
        this(ExpressionType.SUBQUERY, subquery);
    }

    public SubqueryExpression(ExpressionType subqueryType, StmtSubqueryScan subquery) {
        super(subqueryType);
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
        if (m_subquery != null && m_subquery.getBestCostPlan() != null) {
            m_subquery.getBestCostPlan().rootPlanGraph = m_subqueryNode;
        }
        resetSubqueryNodeId();
    }

    public void resetSubqueryNodeId() {
        m_subqueryNodeId = m_subqueryNode.getPlanNodeId();
    }

    public List<Integer> getParameterIdxList() {
        return m_parameterIdxList;
    }

    @Override
    public Object clone() {
        SubqueryExpression clone = new SubqueryExpression(m_subquery);
        clone.setExpressionType(m_type);
        // The parameter TVE map must be cloned explicitly because the original TVEs
        // from the statement are already replaced with the corresponding PVEs
        clone.m_args = new ArrayList<AbstractExpression>();
        for (AbstractExpression arg: m_args) {
            clone.m_args.add((AbstractExpression)arg.clone());
        }
        clone.m_parameterIdxList = new ArrayList<Integer>();
        clone.m_parameterIdxList.addAll(m_parameterIdxList);
        clone.m_allParameterIdxList.addAll(m_allParameterIdxList);
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
        // Output the correlated parameter ids that originates at this subquery immediate
        // parent and need to be set before the evaluation
        if (!m_parameterIdxList.isEmpty()) {
            stringer.key(Members.PARAM_IDX.name()).array();
            for (Integer idx : m_parameterIdxList) {
                stringer.value(idx);
            }
            stringer.endArray();
        }
        // Output the correlated parameter ids that this subquery or its descendants
        // depends upon but originate at the grandparent level and do not need to be set
        // by this subquery
        if (!m_allParameterIdxList.isEmpty()) {
            // Calculate the difference between two sets of parameters
            Set<Integer> allParams = new HashSet<Integer>();
            allParams.addAll(m_allParameterIdxList);
            allParams.removeAll(m_parameterIdxList);
            if (!allParams.isEmpty()) {
                stringer.key(Members.OTHER_PARAM_IDX.name()).array();
                for (Integer idx : allParams) {
                    stringer.value(idx);
                }
                stringer.endArray();
            }
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
        if (obj.has(Members.OTHER_PARAM_IDX.name())) {
            JSONArray allParamIdxArray = obj.getJSONArray(Members.OTHER_PARAM_IDX.name());
            int paramSize = allParamIdxArray.length();
            for (int i = 0; i < paramSize; ++i) {
                m_allParameterIdxList.add(allParamIdxArray.getInt(i));
            }
        }
    }

    @Override
    public String explain(String impliedTableName) {
        if (m_subqueryNode != null) {
            // Surround the explained subquery by the 'Subquery_#' tags.The explained subquery
            // will be extracted into a separated line from the final explain string
            StringBuilder sb = new StringBuilder();
            m_subqueryNode.explainPlan_recurse(sb, "");
            return "(" + SUBQUERY_TAG + m_subqueryId + " " + sb.toString() + SUBQUERY_TAG +
            m_subqueryId + ")";
        } else {
            return "(Subquery: null)";
        }
    }

    @Override
    public void finalizeValueTypes() {
        // Nothing to do there
    }

    /**
     * Traverse down the expression tree identifying all the TVEs which reference the
     * columns from the parent statement (getOrigStmtId() != parentStmt.subqueryId) and replace them with
     * the corresponding ParameterValueExpression. Keep the mapping between the original TVE
     * and new PVE which will be required by the back-end executor.
     * If a TVE references the grandparent, move it up to be resolved at a higher level.
     */
    public void moveUpTVE() {
        AbstractParsedStmt subqueryStmt = m_subquery.getSubquery();
        AbstractParsedStmt parentStmt = m_subquery.getSubquery().m_parentStmt;
        // we must have a parent -it's a subquery statement
        assert(parentStmt != null);
        // Preserve indexes of all parameters this subquery depends on.
        // It includes parameters from the child subqueries.
        m_allParameterIdxList.addAll(subqueryStmt.m_parameterTveMap.keySet());
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
