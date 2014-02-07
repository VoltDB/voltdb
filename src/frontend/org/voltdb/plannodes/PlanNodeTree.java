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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.json_voltpatches.JSONArray;
import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;
import org.json_voltpatches.JSONString;
import org.json_voltpatches.JSONStringer;
import org.voltcore.utils.Pair;
import org.voltdb.VoltType;
import org.voltdb.catalog.Database;
import org.voltdb.expressions.AbstractExpression;
import org.voltdb.expressions.SubqueryExpression;
import org.voltdb.types.ExpressionType;
import org.voltdb.types.PlanNodeType;

/**
 *
 */
public class PlanNodeTree implements JSONString {

    public enum Members {
        PLAN_NODES,
        PARAMETERS,
        SUBQUERIES_PLAN_NODES,
        SUBQUERY_ID;
    }

    protected final List<AbstractPlanNode> m_planNodes;
    protected final Map<Integer, AbstractPlanNode> m_idToNodeMap = new HashMap<Integer, AbstractPlanNode>();
    protected final List< Pair< Integer, VoltType > > m_parameters = new ArrayList< Pair< Integer, VoltType > >();
    // Subquery ID / subquery plan node list map
    protected final Map<Integer, List<AbstractPlanNode>> m_subqueryPlanList = new HashMap<Integer, List<AbstractPlanNode>>();
    // Subquery ID / subquery root node plan
    protected final Map<Integer, AbstractPlanNode> m_subqueryMap = new HashMap<Integer, AbstractPlanNode>();

    public PlanNodeTree() {
        m_planNodes = new ArrayList<AbstractPlanNode>();
    }

    public PlanNodeTree(AbstractPlanNode root_node) {
        this();
        try {
            constructTree(m_planNodes, root_node);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public Integer getRootPlanNodeId() {
        return m_planNodes.get(0).getPlanNodeId();
    }

    public AbstractPlanNode getRootPlanNode() {
        return m_planNodes.get(0);
    }

    public Boolean constructTree(List<AbstractPlanNode> planNodes, AbstractPlanNode node) throws Exception {
        planNodes.add(node);
        m_idToNodeMap.put(node.getPlanNodeId(), node);
        extractSubqueries(node);
        for (int i = 0; i < node.getChildCount(); i++) {
            AbstractPlanNode child = node.getChild(i);
            if (!constructTree(planNodes, child)) {
                return false;
            }
        }
        return true;
    }

    public List< Pair< Integer, VoltType > > getParameters() {
        return m_parameters;
    }

    public void setParameters(List< Pair< Integer, VoltType > > parameters) {
        m_parameters.clear();
        m_parameters.addAll(parameters);
    }

    @Override
    public String toJSONString() {
        JSONStringer stringer = new JSONStringer();
        try {
            stringer.object();
            toJSONString(stringer);
            stringer.endObject();
        } catch (JSONException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
        return stringer.toString();
    }

    public void toJSONString(JSONStringer stringer) throws JSONException {
        stringer.key(Members.PLAN_NODES.name()).array();
        for (AbstractPlanNode node : m_planNodes) {
            assert (node instanceof JSONString);
            stringer.value(node);
        }
        stringer.endArray(); //end entries

        stringer.key(Members.PARAMETERS.name()).array();
        for (Pair< Integer, VoltType > parameter : m_parameters) {
            stringer.array().value(parameter.getFirst()).value(parameter.getSecond().name()).endArray();
        }
        stringer.endArray();

        stringer.key(Members.SUBQUERIES_PLAN_NODES.name()).array();
        for (Map.Entry<Integer, List<AbstractPlanNode>> entry : m_subqueryPlanList.entrySet()) {
            stringer.object();
            stringer.key(Members.SUBQUERY_ID.name()).value(entry.getKey());
            stringer.key(Members.PLAN_NODES.name()).array();
            for (AbstractPlanNode node : entry.getValue()) {
                assert (node instanceof JSONString);
                stringer.value(node);
            }
            stringer.endArray().endObject(); //end list and entry
        }
        stringer.endArray(); // end map
    }

    public List<AbstractPlanNode> getNodeList() {
        return m_planNodes;
    }

    /**
     *  Load json plan. The plan must have "PLAN_NODE" array and optional SUBQUERIES_PLAN_NODES
     *  array containing subquery(es) plan nodes if any
     * @param jobj
     * @param db
     * @throws JSONException
     */
    public void loadFromJSONPlan( JSONObject jobj, Database db )  throws JSONException {
        JSONArray jArray = jobj.getJSONArray(PlanNodeTree.Members.PLAN_NODES.name());
        JSONArray jsubArray = null;
        if (jobj.has(PlanNodeTree.Members.SUBQUERIES_PLAN_NODES.name())) {
            jsubArray = jobj.getJSONArray(PlanNodeTree.Members.SUBQUERIES_PLAN_NODES.name());
        }
        loadFromJSONArrays(jArray, jsubArray, db);
    }

    /**
     *  Load plan nodes from the "PLAN_NODE" array
     * @param jobj
     * @param db
     * @throws JSONException
     */
    public void loadFromJSONArray( JSONArray jArray, Database db )  throws JSONException {
        loadFromJSONArrays(jArray, null, db);
    }

    private void loadFromJSONArrays( JSONArray jArray, JSONArray jSubArray, Database db )  {
        // Load the parent query first
        loadFromJSONArray(jArray, db, m_planNodes);

        if (jSubArray == null) {
            return;
        }
        // Load subqueries
        int size = jSubArray.length();

        try {
            for( int i = 0; i < size; i++ ) {
                JSONObject jobj;
                jobj = jSubArray.getJSONObject(i);
                int subqueryId = jobj.getInt("SUBQUERY_ID");
                JSONArray jnodeArray = jobj.getJSONArray("PLAN_NODES");
                List<AbstractPlanNode> planNodes = new ArrayList<AbstractPlanNode>();
                loadFromJSONArray(jnodeArray, db, planNodes);
                m_subqueryPlanList.put(subqueryId, planNodes);
            }
        }
        catch (JSONException e) {
            e.printStackTrace();
        }
        // Connect the parent and child statements
        connectParentChildStmt(m_planNodes);
        for (Map.Entry<Integer, List<AbstractPlanNode>> stmtEntry : m_subqueryPlanList.entrySet()) {
            List<AbstractPlanNode> nodeList = stmtEntry.getValue();
            connectParentChildStmt(nodeList);
        }
    }

    private void loadFromJSONArray( JSONArray jArray, Database db, List<AbstractPlanNode> planNodes)  {
        int size = jArray.length();

        try {
            for( int i = 0; i < size; i++ ) {
                JSONObject jobj;
                jobj = jArray.getJSONObject(i);
                String nodeTypeStr = jobj.getString("PLAN_NODE_TYPE");
                PlanNodeType nodeType = PlanNodeType.get( nodeTypeStr );
                AbstractPlanNode apn = null;
                try {
                    apn = nodeType.getPlanNodeClass().newInstance();
                } catch (InstantiationException e) {
                    System.err.println( e.getMessage() );
                    e.printStackTrace();
                    return;
                } catch (IllegalAccessException e) {
                    System.err.println( e.getMessage() );
                    e.printStackTrace();
                    return;
                }
                apn.loadFromJSONObject(jobj, db);
                planNodes.add(apn);
            }
            //link children and parents
            for( int i = 0; i < size; i++ ) {
                JSONObject jobj;
                jobj = jArray.getJSONObject(i);
                JSONArray children = jobj.getJSONArray("CHILDREN_IDS");
                for( int j = 0; j < children.length(); j++ ) {
                    planNodes.get(i).addAndLinkChild( getNodeofId(children.getInt(j), planNodes) );
                }
            }
        }
        catch (JSONException e) {
            e.printStackTrace();
        }
    }

    private void connectParentChildStmt(List<AbstractPlanNode> planNodes) {
        for(AbstractPlanNode node : planNodes) {
            if (node instanceof NestLoopInPlanNode) {
                NestLoopInPlanNode nlInj = (NestLoopInPlanNode)node;
                int subqueryId = nlInj.getSubqueryId();
                int subqueryNodeId = nlInj.getSubqueryNodeId();
                List<AbstractPlanNode> subqueryNodes = m_subqueryPlanList.get(subqueryId);
                AbstractPlanNode subqueryNode = this.getNodeofId(subqueryNodeId, subqueryNodes);
                assert(subqueryNode != null);
                nlInj.setSubqueryNode(subqueryNode);
            } else if (node instanceof AbstractScanPlanNode) {
                AbstractScanPlanNode scanNode = (AbstractScanPlanNode)node;
                connectPredicateStmt(scanNode.getPredicate());
            } else if (node instanceof AbstractJoinPlanNode) {
                AbstractJoinPlanNode joinNode = (AbstractJoinPlanNode) node;
                connectPredicateStmt(joinNode.getPreJoinPredicate());
                connectPredicateStmt(joinNode.getJoinPredicate());
                connectPredicateStmt(joinNode.getWherePredicate());
            }
        }
    }

    private void connectPredicateStmt(AbstractExpression predicate) {
        if (predicate == null) {
            return;
        }
        List<AbstractExpression> existsExprs = predicate.findAllSubexpressionsOfType(
                ExpressionType.OPERATOR_EXISTS);
        for (AbstractExpression expr : existsExprs) {
            assert(expr.getLeft() != null && expr.getLeft() instanceof SubqueryExpression);
            SubqueryExpression subqueryExpr = (SubqueryExpression) expr.getLeft();
            int subqueryId = subqueryExpr.getSubqueryId();
            int subqueryNodeId = subqueryExpr.getSubqueryNodeId();
            List<AbstractPlanNode> subqueryNodes = m_subqueryPlanList.get(subqueryId);
            AbstractPlanNode subqueryNode = getNodeofId(subqueryNodeId, subqueryNodes);
            assert(subqueryNode != null);
            subqueryExpr.setSubqueryNode(subqueryNode);
        }
    }

    public AbstractPlanNode getNodeofId (int ID, List<AbstractPlanNode> planNodes) {
        int size = planNodes.size();
        for( int i = 0; i < size; i++ ) {
            if( planNodes.get(i).getPlanNodeId() == ID ) {
                return planNodes.get(i);
            }
        }
        return null;
    }

    /**
     * Traverse down the plan extracting all the subquery plans. The potential places where
     * the suqueries could be found are:
     *  - NestLoopInPlan
     *  - AbstractScanPlanNode predicate
     *  - AbstractJoinPlanNode predicates
     * @param node
     * @throws Exception
     */
    private void extractSubqueries(AbstractPlanNode node)  throws Exception {
        if (node instanceof NestLoopInPlanNode) {
            NestLoopInPlanNode nlinj = (NestLoopInPlanNode) node;
            m_subqueryMap.put(nlinj.getSubqueryId(), nlinj.getSubqueryNode());
            List<AbstractPlanNode> planNodes = new ArrayList<AbstractPlanNode>();
            m_subqueryPlanList.put(nlinj.getSubqueryId(), planNodes);
            constructTree(planNodes, nlinj.getSubqueryNode());
        } else if (node instanceof AbstractScanPlanNode) {
            AbstractScanPlanNode scanNode = (AbstractScanPlanNode) node;
            extractSubqueriesFromExpression(scanNode.getPredicate());
        } else if (node instanceof AbstractJoinPlanNode) {
            AbstractJoinPlanNode joinNode = (AbstractJoinPlanNode) node;
            extractSubqueriesFromExpression(joinNode.getPreJoinPredicate());
            extractSubqueriesFromExpression(joinNode.getJoinPredicate());
            extractSubqueriesFromExpression(joinNode.getWherePredicate());
            AbstractPlanNode inlineScan = joinNode.getInlinePlanNode(PlanNodeType.INDEXSCAN);
            if (inlineScan != null) {
                assert(inlineScan instanceof AbstractScanPlanNode);
                extractSubqueriesFromExpression(((AbstractScanPlanNode)inlineScan).getPredicate());
            }
        }
    }
    private void extractSubqueriesFromExpression(AbstractExpression expr)  throws Exception {
        if (expr == null) {
            return;
        }
        List<AbstractExpression> subexprs = expr.findAllSubexpressionsOfType(ExpressionType.SUBQUERY);
        for(AbstractExpression subexpr : subexprs) {
            assert(subexpr instanceof SubqueryExpression);
            m_subqueryMap.put(((SubqueryExpression) subexpr).getSubqueryId(),
                    ((SubqueryExpression) subexpr).getSubqueryNode());
            List<AbstractPlanNode> planNodes = new ArrayList<AbstractPlanNode>();
            m_subqueryPlanList.put(((SubqueryExpression) subexpr).getSubqueryId(), planNodes);
            constructTree(planNodes, ((SubqueryExpression) subexpr).getSubqueryNode());
        }
    }
}
