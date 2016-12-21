/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
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
import org.voltdb.planner.CompiledPlan;
import org.voltdb.planner.parseinfo.StmtSubqueryScan;
import org.voltdb.plannodes.AbstractPlanNode;
import org.voltdb.types.ExpressionType;

/**
 * Expression to represent a select sub query (SELECT ...).
*
*/
public class SelectSubqueryExpression extends AbstractSubqueryExpression {

    public enum Members {
        OTHER_PARAM_IDX;
    }

    // subquery
    private StmtSubqueryScan m_subquery;
    // List of all correlated parameter indexes this subquery and its descendants depend on
    // They may originate at different levels in the subquery hierarchy.
    private List<Integer> m_allParameterIdxList = new ArrayList<Integer>();

    // SelectSubqueryExpression can be changed to a ScalarSubqueryExpression in certain contexts
    // By default, AbstractSubqueryExpression use the BigInt as the return type because of possible
    // optimization all the other IN/EXISTS into EXISTS (SELECT 1 FROM...).
    // However, scalar subquery is used quite different and temporary hides under this class.
    // Eventually, scalar subquery is changed to a expression with one child as SelectSubqueryExpression.
    private VoltType m_scalarExprType = null;

    /**
     * Create a new SubqueryExpression. The type can be either:
     *    SCALAR_SUBQUERY   - SELECT A, (SELECT C...) FROM .... - single row one column
     *    SELECT_SUBQUERY   - WHERE (...) IN (SELECT C1, C2 ...) - multiple rows
     * @param subqueryType
     * @param subquery The parsed statement
     */
    public SelectSubqueryExpression(ExpressionType type, StmtSubqueryScan subquery) {
        super();
        m_type = type;
        assert(subquery != null);
        m_subquery = subquery;
        assert(m_subquery.getSubqueryStmt() != null);
        m_subqueryId = m_subquery.getSubqueryStmt().m_stmtId;
        if (m_subquery.getBestCostPlan() != null && m_subquery.getBestCostPlan().rootPlanGraph != null) {
            m_subqueryNode = m_subquery.getBestCostPlan().rootPlanGraph;
            m_subqueryNodeId = m_subqueryNode.getPlanNodeId();
        }
        m_args = new ArrayList<AbstractExpression>();
        resolveCorrelations();

        m_scalarExprType = m_valueType;
        if (m_subquery.getOutputSchema().size() == 1) {
            // potential scalar sub-query
            m_scalarExprType = m_subquery.getOutputSchema().get(0).getType();
        }
    }

    /**
     * Create a new empty SubqueryExpression for loading from JSON
     * @param subquey The parsed statement
     */
    public SelectSubqueryExpression() {
        super();
        setExpressionType(ExpressionType.SELECT_SUBQUERY);
    }

    public StmtSubqueryScan getSubqueryScan() {
        return m_subquery;
    }

    public AbstractParsedStmt getSubqueryStmt() {
        return (m_subquery == null) ? null : m_subquery.getSubqueryStmt();
    }

    @Override
    public int getSubqueryId() {
        return m_subqueryId;
    }

    @Override
    public int getSubqueryNodeId() {
        return m_subqueryNodeId;
    }

    @Override
    public AbstractPlanNode getSubqueryNode() {
        return m_subqueryNode;
    }

    /**
     * This function should only be called when this expression should be changed to ScalarSubqueryExpression.
     */
    public void changeToScalarExprType() {
        m_valueType = m_scalarExprType;
        m_valueSize = m_valueType.getMaxLengthInBytes();
    }

    /**
     * From JSON
     */
    @Override
    public void setSubqueryNode(AbstractPlanNode subqueryNode) {
        assert(subqueryNode != null);
        m_subqueryNode = subqueryNode;
        if (m_subquery != null && m_subquery.getBestCostPlan() != null) {
            m_subquery.getBestCostPlan().rootPlanGraph = m_subqueryNode;
        }
        resetSubqueryNodeId();
    }

    @Override
    public  int overrideSubqueryNodeIds(int newId) {
        assert(m_subquery != null);
        CompiledPlan subqueryPlan = m_subquery.getBestCostPlan();
        newId = subqueryPlan.resetPlanNodeIds(newId);
        resetSubqueryNodeId();
        return newId;
    }

    @Override
    public SelectSubqueryExpression clone() {
        SelectSubqueryExpression clone = (SelectSubqueryExpression) super.clone();
        if (!m_allParameterIdxList.isEmpty()) {
            clone.m_allParameterIdxList = new ArrayList<Integer>();
            for (Integer paramIdx : m_allParameterIdxList) {
                clone.m_allParameterIdxList.add(new Integer(paramIdx.intValue()));
            }
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
    public void toJSONString(JSONStringer stringer) throws JSONException {
        super.toJSONString(stringer);
        // Output the correlated parameter ids that this subquery or its descendants
        // depends upon but originate at the grandparent level and do not need to be set
        // by this subquery
        if (!m_allParameterIdxList.isEmpty()) {
            // Calculate the difference between two sets of parameters
            Set<Integer> allParams = new HashSet<Integer>();
            allParams.addAll(m_allParameterIdxList);
            allParams.removeAll(getParameterIdxList());
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
        super.loadFromJSONObject(obj);
        if (obj.has(Members.OTHER_PARAM_IDX.name())) {
            JSONArray otherParamIdxArray = obj.getJSONArray(Members.OTHER_PARAM_IDX.name());
            int paramSize = otherParamIdxArray.length();
            for (int i = 0; i < paramSize; ++i) {
                m_allParameterIdxList.add(otherParamIdxArray.getInt(i));
            }
            m_allParameterIdxList.addAll(getParameterIdxList());
        }
    }

    @Override
    public String explain(String impliedTableName) {
        if (m_subqueryNode != null) {
            // Surround the explained subquery by the 'Subquery_#' tags.The explained subquery
            // will be extracted into a separated line from the final explain string
            StringBuilder sb = new StringBuilder();
            m_subqueryNode.explainPlan_recurse(sb, "");
            String result = "(" + SUBQUERY_TAG + m_subqueryId + " " + sb.toString()
                    + SUBQUERY_TAG + m_subqueryId + "";
            if (m_args != null && ! m_args.isEmpty()) {
                String connector = "\n on arguments (";
                for (AbstractExpression arg : m_args) {
                    result += connector + arg.explain(impliedTableName);
                    connector = ", ";
                }
                result += ")\n";
            }
            result +=")";
            return result;
        } else {
            return "(Subquery: null)";
        }
    }

    /**
     * Resolve the subquery's correlated TVEs (and, in one special case, aggregates)
     * that became ParameterValueExpressions in the subquery statement (or its children).
     * If they reference a column from the parent statement (getOrigStmtId() == parentStmt.m_stmtId)
     * that PVE will have to be initialized by this subquery expression in the back-end executor.
     * Otherwise, the TVE references a grandparent statement with its own subquery expression,
     * so just add it to the parent statement's set of correlated TVEs needing to be resolved later
     * at a higher level.
     */
    public void resolveCorrelations() {
        AbstractParsedStmt subqueryStmt = m_subquery.getSubqueryStmt();
        AbstractParsedStmt parentStmt = subqueryStmt.m_parentStmt;

        // we must have a parent - it's a subquery statement
        assert(parentStmt != null);
        // Preserve indexes of all parameters this subquery depends on.
        // It might include parameters from its nested child subqueries that
        // the subquery statement could not resolve itself and had to "move up".
        m_allParameterIdxList.addAll(subqueryStmt.m_parameterTveMap.keySet());
        for (Map.Entry<Integer, AbstractExpression> entry : subqueryStmt.m_parameterTveMap.entrySet()) {
            Integer paramIdx = entry.getKey();
            AbstractExpression expr = entry.getValue();
            if (expr instanceof TupleValueExpression) {
                TupleValueExpression tve = (TupleValueExpression) expr;
                if (tve.getOrigStmtId() == parentStmt.m_stmtId) {
                    // TVE originates from the statement that this SubqueryExpression belongs to
                    addArgumentParameter(paramIdx, expr);
                }
                else {
                    // TVE originates from a statement above this parent. Move it up.
                    parentStmt.m_parameterTveMap.put(paramIdx, expr);
                }
            }
            else if (expr instanceof AggregateExpression) {
                // An aggregate expression is always from THIS parent statement.
                addArgumentParameter(paramIdx, expr);
            }
            else {
                // so far it should be either AggregateExpression or TupleValueExpression types
                assert(false);
            }
        }
        subqueryStmt.m_parameterTveMap.clear();
    }

    public String calculateContentDeterminismMessage() {
        return m_subquery.calculateContentDeterminismMessage();
    }
}
