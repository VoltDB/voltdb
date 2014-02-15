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

package org.voltdb.plannodes;

import java.util.Map;

import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;
import org.json_voltpatches.JSONStringer;
import org.voltdb.catalog.Cluster;
import org.voltdb.catalog.Database;
import org.voltdb.compiler.DatabaseEstimates;
import org.voltdb.compiler.ScalarValueHints;
import org.voltdb.expressions.TupleValueExpression;
import org.voltdb.types.PlanNodeType;

public class NestLoopInPlanNode extends AbstractJoinPlanNode {

    public enum Members {
        SUBQUERY_ID,
        SUBQUERY_ROOT_NODE_ID,
        SUBQUERY_PARAMS,
        PARAM_IDX,
        PARAM_TVE;
    }

    private int m_subqueryId = -1;
    // The inner subquey plan node deserves a special treatment because it's not a 'true'
    // inner child scan (has its own executor stack) and also not a 'true' inline node
    // (may have its own children)
    private AbstractPlanNode m_subqueryNode;
    // The subquery node id is required to load the node from the JSON
    private int m_subqueryNodeId = -1;
    // correlated parameters
    Map<Integer, TupleValueExpression> m_parameterTveMap = null;

    public NestLoopInPlanNode() {
        super();
    }

    public NestLoopInPlanNode(int subqueryId, AbstractPlanNode outerNode, AbstractPlanNode subqueryNode,
            Map<Integer, TupleValueExpression> params) {
        super();
        m_subqueryId = subqueryId;
        m_subqueryNodeId = outerNode.getPlanNodeId();
        addAndLinkChild(outerNode);
        m_subqueryNode = subqueryNode;
        m_subqueryNode.disconnectParents();
        m_parameterTveMap = params;
    }

    public int getSubqueryId() {
        return m_subqueryId;
    }

    public int getSubqueryNodeId() {
        return (m_subqueryNode == null) ? m_subqueryNodeId : m_subqueryNode.getPlanNodeId();
    }

    public AbstractPlanNode getSubqueryNode() {
        return m_subqueryNode;
    }

    public void setSubqueryNode(AbstractPlanNode subqueryNode) {
        m_subqueryNode = subqueryNode;
        m_subqueryNode.disconnectParents();
        m_subqueryNodeId = m_subqueryNode.getPlanNodeId();
    }

    public Map<Integer, TupleValueExpression> getParameterTveMap() {
        return m_parameterTveMap;
    }

    public void setParameterTveMap(Map<Integer, TupleValueExpression> parameterTveMap) {
        m_parameterTveMap = parameterTveMap;
    }

    @Override
    public void toJSONString(JSONStringer stringer) throws JSONException
    {
        super.toJSONString(stringer);
        stringer.key(Members.SUBQUERY_ID.name()).value(m_subqueryId);
        stringer.key(Members.SUBQUERY_ROOT_NODE_ID.name()).value(m_subqueryNode.m_id);
    }

    @Override
    public void loadFromJSONObject(JSONObject obj, Database db) throws JSONException {
        super.loadFromJSONObject(obj, db);

        m_subqueryId = obj.getInt(Members.SUBQUERY_ID.name());
        m_subqueryNodeId = obj.getInt(Members.SUBQUERY_ROOT_NODE_ID.name());
    }

    @Override
    public PlanNodeType getPlanNodeType() {
        return PlanNodeType.NESTLOOPIN;
    }

    @Override
    public void computeCostEstimates(long childOutputTupleCountEstimate,
                                     Cluster cluster,
                                     Database db,
                                     DatabaseEstimates estimates,
                                     ScalarValueHints[] paramHints)
    {
        // Add subquery estimates to the totals
        assert(m_subqueryNode != null);
        m_estimatedOutputTupleCount =
                childOutputTupleCountEstimate + m_subqueryNode.m_estimatedOutputTupleCount;
        m_estimatedProcessedTupleCount =
                childOutputTupleCountEstimate + m_subqueryNode.m_estimatedProcessedTupleCount;
    }

    @Override
    public void generateOutputSchema(Database db)
    {
        assert(m_children.size() == 1);
        getChild(0).generateOutputSchema(db);
        m_subqueryNode.generateOutputSchema(db);

        // The output schema is the outer child output schema
        m_outputSchema = m_children.get(0).getOutputSchema().copyAndReplaceWithTVE();
        m_hasSignificantOutputSchema = true;
    }

    @Override
    public void resolveColumnIndexes()
    {
        assert(m_children.size() == 1);
        getChild(0).resolveColumnIndexes();
        m_subqueryNode.resolveColumnIndexes();
    }

    @Override
    protected String explainPlanForNode(String indent) {
        String extraindent = " ";
        // Explain the subquery
        StringBuilder sb = new StringBuilder();
        m_subqueryNode.explainPlan_recurse(sb, "");
        // Explain itself
        return "NEST LOOP IN JOIN\n" +
            indent + extraindent + "(Subquery_" + m_subqueryId + " " + sb.toString() +
            "Subquery_" + m_subqueryId + ")";
    }

}
