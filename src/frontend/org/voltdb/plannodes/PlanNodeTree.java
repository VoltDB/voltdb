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

package org.voltdb.plannodes;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.json_voltpatches.JSONArray;
import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;
import org.json_voltpatches.JSONString;
import org.json_voltpatches.JSONStringer;
import org.voltdb.catalog.Database;
import org.voltdb.expressions.AbstractExpression;
import org.voltdb.expressions.AbstractSubqueryExpression;
import org.voltdb.types.PlanNodeType;

/**
 *
 */
public class PlanNodeTree implements JSONString {
    private static final String PLAN_NODES_MEMBER_NAME = "PLAN_NODES";
    private static final String PLAN_NODES_LISTS_MEMBER_NAME = "PLAN_NODES_LISTS";
    private static final String STATEMENT_ID_MEMBER_NAME = "STATEMENT_ID";

    // Subquery ID / subquery plan node list map. The top level statement always has id = 0
    protected final Map<Integer, List<AbstractPlanNode>> m_planNodesListMap = new HashMap<Integer, List<AbstractPlanNode>>();

    public PlanNodeTree() {
    }

    public PlanNodeTree(AbstractPlanNode root_node) {
        try {
            List<AbstractPlanNode> nodeList = new ArrayList<AbstractPlanNode>();
            m_planNodesListMap.put(0, nodeList);
            constructTree(nodeList, root_node);
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    public AbstractPlanNode getRootPlanNode() {
        assert(!m_planNodesListMap.isEmpty() && !m_planNodesListMap.get(0).isEmpty());
        return m_planNodesListMap.get(0).get(0);
    }

    private boolean constructTree(List<AbstractPlanNode> planNodes, AbstractPlanNode node) throws Exception {
        planNodes.add(node);
        extractSubqueries(node);
        for (int i = 0; i < node.getChildCount(); i++) {
            AbstractPlanNode child = node.getChild(i);
            if (!constructTree(planNodes, child)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public String toJSONString() {
        JSONStringer stringer = new JSONStringer();
        try {
            stringer.object();
            toJSONString(stringer);
            stringer.endObject();
        }
        catch (JSONException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
        return stringer.toString();
    }

    public void toJSONString(JSONStringer stringer) throws JSONException {
        if (m_planNodesListMap.size() == 1) {
            stringer.key(PLAN_NODES_MEMBER_NAME).array();
            for (AbstractPlanNode node : m_planNodesListMap.get(0)) {
                assert (node instanceof JSONString);
                stringer.value(node);
            }
            stringer.endArray(); //end entries
        } else {
            stringer.key(PLAN_NODES_LISTS_MEMBER_NAME).array();
            for (Map.Entry<Integer, List<AbstractPlanNode>> planNodes : m_planNodesListMap.entrySet()) {
                stringer.object().key(STATEMENT_ID_MEMBER_NAME).
                    value(planNodes.getKey());
                stringer.key(PLAN_NODES_MEMBER_NAME).array();
                for (AbstractPlanNode node : planNodes.getValue()) {
                    assert (node instanceof JSONString);
                    stringer.value(node);
                }
                stringer.endArray().endObject(); //end entries
            }
            stringer.endArray(); //end entries
        }
    }

    public List<AbstractPlanNode> getNodeList() {
        return m_planNodesListMap.get(0);
    }

    /**
     *  Load json plan. The plan must have either "PLAN_NODE" array in case of a statement without
     *  subqueries or "PLAN_NODES_LISTS" array of "PLAN_NODE" arrays for each sub statement.
     * @param jobj
     * @param db
     * @throws JSONException
     */
    public void loadFromJSONPlan(JSONObject jobj, Database db)  throws JSONException {
        if (jobj.has(PLAN_NODES_LISTS_MEMBER_NAME)) {
            JSONArray jplanNodesArray = jobj.getJSONArray(PLAN_NODES_LISTS_MEMBER_NAME);
            for (int i = 0; i < jplanNodesArray.length(); ++i) {
                JSONObject jplanNodesObj = jplanNodesArray.getJSONObject(i);
                JSONArray jplanNodes = jplanNodesObj.getJSONArray(PLAN_NODES_MEMBER_NAME);
                int stmtId = jplanNodesObj.getInt(STATEMENT_ID_MEMBER_NAME);
                loadPlanNodesFromJSONArrays(stmtId, jplanNodes, db);
            }
        }
        else {
            // There is only one statement in the plan. Its id is set to 0 by default
            int stmtId = 0;
            JSONArray jplanNodes = jobj.getJSONArray(PLAN_NODES_MEMBER_NAME);
            loadPlanNodesFromJSONArrays(stmtId, jplanNodes, db);
        }

        // Connect the parent and child statements
        for (List<AbstractPlanNode> nextPlanNodes : m_planNodesListMap.values()) {
            for (AbstractPlanNode node : nextPlanNodes) {
                findPlanNodeWithPredicate(node);
            }
        }
    }

    /**
     * Scan node, join node can have predicate, so does the Aggregate node (Having clause).
     */
    private void findPlanNodeWithPredicate(AbstractPlanNode node) {
        NodeSchema outputSchema = node.getOutputSchema();
        if (outputSchema != null) {
            for (SchemaColumn col : outputSchema.getColumns()) {
                connectPredicateStmt(col.getExpression());
            }
        }
        if (node instanceof AbstractScanPlanNode) {
            AbstractScanPlanNode scanNode = (AbstractScanPlanNode)node;
            connectPredicateStmt(scanNode.getPredicate());
        } else if (node instanceof AbstractJoinPlanNode) {
            AbstractJoinPlanNode joinNode = (AbstractJoinPlanNode)node;
            connectPredicateStmt(joinNode.getPreJoinPredicate());
            connectPredicateStmt(joinNode.getJoinPredicate());
            connectPredicateStmt(joinNode.getWherePredicate());
        } else if (node instanceof AggregatePlanNode) {
            AggregatePlanNode aggNode = (AggregatePlanNode)node;
            connectPredicateStmt(aggNode.getPostPredicate());
        }

        for (AbstractPlanNode inlineNode: node.getInlinePlanNodes().values()) {
            findPlanNodeWithPredicate(inlineNode);
        }
    }

    private void connectPredicateStmt(AbstractExpression predicate) {
        if (predicate == null) {
            return;
        }
        List<AbstractExpression> subquerysExprs = predicate.findAllSubquerySubexpressions();
        for (AbstractExpression expr : subquerysExprs) {
            assert(expr instanceof AbstractSubqueryExpression);
            AbstractSubqueryExpression subqueryExpr = (AbstractSubqueryExpression) expr;
            int subqueryId = subqueryExpr.getSubqueryId();
            int subqueryNodeId = subqueryExpr.getSubqueryNodeId();
            List<AbstractPlanNode> subqueryNodes = m_planNodesListMap.get(subqueryId);

            if (subqueryNodes == null) {
                return;
            }

            AbstractPlanNode subqueryNode = getNodeofId(subqueryNodeId, subqueryNodes);
            assert(subqueryNode != null);
            subqueryExpr.setSubqueryNode(subqueryNode);
        }
    }

    /**
     *  Load plan nodes from the "PLAN_NODE" array. It is assumed that
     *  these nodes are from the main statement and not from the substatement
     * @param jArray - PLAN_NODES
     * @param db
     * @throws JSONException
     */
    public void loadPlanNodesFromJSONArrays(JSONArray jArray, Database db) {
        loadPlanNodesFromJSONArrays(0, jArray, db);
    }

    /**
     *  Load plan nodes from the "PLAN_NODE" array. All the nodes are from
     *  a substatement with the id = stmtId
     * @param stmtId
     * @param jArray - PLAN_NODES
     * @param db
     * @throws JSONException
     */
    private void loadPlanNodesFromJSONArrays(int stmtId, JSONArray jArray, Database db) {
        List<AbstractPlanNode> planNodes = new ArrayList<AbstractPlanNode>();
        int size = jArray.length();

        try {
            for (int i = 0; i < size; i++) {
                JSONObject jobj = jArray.getJSONObject(i);
                String nodeTypeStr = jobj.getString("PLAN_NODE_TYPE");
                PlanNodeType nodeType = PlanNodeType.get(nodeTypeStr);
                AbstractPlanNode apn = nodeType.getPlanNodeClass().newInstance();
                apn.loadFromJSONObject(jobj, db);
                planNodes.add(apn);
            }

            //link children and parents
            for (int i = 0; i < size; i++) {
                JSONObject jobj = jArray.getJSONObject(i);
                if (jobj.has("CHILDREN_IDS")) {
                    AbstractPlanNode parent = planNodes.get(i);
                    JSONArray children = jobj.getJSONArray("CHILDREN_IDS");
                    for (int j = 0; j < children.length(); j++) {
                        AbstractPlanNode child = getNodeofId(children.getInt(j), planNodes);
                        parent.addAndLinkChild(child);
                    }
                }
            }
            m_planNodesListMap.put(stmtId,  planNodes);
        }
        catch (JSONException | InstantiationException | IllegalAccessException e) {
            System.err.println(e);
            e.printStackTrace();
        }
    }

    private AbstractPlanNode getNodeofId (int id, List<AbstractPlanNode> planNodes) {
        int size = planNodes.size();
        for (int i = 0; i < size; i++) {
            if (planNodes.get(i).getPlanNodeId() == id) {
                return planNodes.get(i);
            }
        }
        return null;
    }

    /**
     * Traverse down the plan extracting all the subquery plans. The potential places where
     * the subqueries could be found are:
     *  - NestLoopInPlan
     *  - AbstractScanPlanNode predicate
     *  - AbstractJoinPlanNode predicates
     *  - IndexScan search keys and predicates
     *  - IndexJoin inline inner scan
     *  - Aggregate post-predicate(HAVING clause)
     *  - Projection, output schema (scalar subquery)
     * @param node
     * @throws Exception
     */
    private void extractSubqueries(AbstractPlanNode node)  throws Exception {
        assert(node != null);
        Collection<AbstractExpression> subexprs = node.findAllSubquerySubexpressions();
        for (AbstractExpression nextexpr : subexprs) {
            assert(nextexpr instanceof AbstractSubqueryExpression);
            AbstractSubqueryExpression subqueryExpr = (AbstractSubqueryExpression) nextexpr;
            int stmtId = subqueryExpr.getSubqueryId();
            List<AbstractPlanNode> planNodes = new ArrayList<AbstractPlanNode>();
            assert(!m_planNodesListMap.containsKey(stmtId));
            m_planNodesListMap.put(stmtId, planNodes);
            constructTree(planNodes, subqueryExpr.getSubqueryNode());
        }
    }

}
