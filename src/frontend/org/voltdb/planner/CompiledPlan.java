/* This file is part of VoltDB.
 * Copyright (C) 2008-2012 VoltDB Inc.
 *
 * VoltDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * VoltDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.voltdb.planner;

import org.voltdb.ParameterSet;
import org.voltdb.VoltDB;
import org.voltdb.VoltType;
import org.voltdb.expressions.AbstractExpression;
import org.voltdb.plannodes.AbstractPlanNode;
import org.voltdb.plannodes.NodeSchema;
import org.voltdb.plannodes.PlanNodeList;
import org.voltdb.types.PlanNodeType;

/**
 * A triple-tuple to hold a complete plan graph along with its
 * input (parameters) and output (result columns). This class just
 * exists to make it a convenient return value for methods that
 * output plans.
 *
 */
public class CompiledPlan {

    /** A complete plan graph for SP plans and the top part of MP plans */
    public AbstractPlanNode rootPlanGraph;

    /** A "collector" fragment for two-part MP plans */
    public AbstractPlanNode subPlanGraph;

    /**
     * The SQL text of the statement
     */
    public String sql = null;

    /**
     * Cost of the plan as estimated (not necessarily well)
     * by the planner
     */
    public double cost = 0.0;

    /**
     * The textual explanation of aggregation, join order and
     * access path selection.
     */
    public String explainedPlan = null;

    /** A list of parameter names, indexes and types */
    public VoltType[] parameters = null;

    /** Parameter values, if the planner pulled constants out of the plan */
    public ParameterSet extractedParamValues = new ParameterSet();

    /** A list of output column ids, indexes and types */
    public NodeSchema columns = new NodeSchema();

    /**
     * If true, divide the number of tuples changed
     * by the number of partitions, as the number will
     * be the sum of tuples changed on all replicas.
     */
    public boolean replicatedTableDML = false;

    /** Does the statment write? */
    public boolean readOnly = false;

    /**
     * The tree representing the full where clause of the SQL
     * statement that generated this plan. This is not used for
     * execution, but is of interest to the database designer.
     * Ultimately, this will end up serialized in the catalog.
     * Note: this is not serialized when the parent CompiledPlan
     * instance is serialized (only used for ad hoc sql).
     */
    public AbstractExpression fullWhereClause = null;

    /**
     * The plangraph representing the full generated plan with
     * the lowest cost for this sql statement. This is not used for
     * execution, but is of interest to the database designer.
     * Ultimately, this will end up serialized in the catalog.
     * Note: this is not serialized when the parent CompiledPlan
     * instance is serialized (only used for ad hoc sql).
     */
    public AbstractPlanNode fullWinnerPlan = null;

    /**
     * Whether the plan's statement mandates a result with deterministic content;
     */
    private boolean m_statementIsContentDeterministic = false;

    /**
     * Whether the plan's statement mandates a result with deterministic content and order;
     */
    private boolean m_statementIsOrderDeterministic = false;

    /** Which extracted param is the partitioning object (assuming parameterized plans) */
    public int partitioningKeyIndex = -1;

    private Object m_partitioningKey;

    void resetPlanNodeIds() {
        int nextId = resetPlanNodeIds(rootPlanGraph, 1);
        if (subPlanGraph != null) {
            resetPlanNodeIds(subPlanGraph, nextId);
        }
    }

    private int resetPlanNodeIds(AbstractPlanNode node, int nextId) {
        node.overrideId(nextId++);
        for (AbstractPlanNode inNode : node.getInlinePlanNodes().values()) {
            inNode.overrideId(0);
        }

        for (int i = 0; i < node.getChildCount(); i++) {
            AbstractPlanNode child = node.getChild(i);
            assert(child != null);
            nextId = resetPlanNodeIds(child, nextId);
        }

        return nextId;
    }

    /**
     * Mark the level of result determinism imposed by the statement,
     * which can save us from a difficult determination based on the plan graph.
     */
    public void statementGuaranteesDeterminism(boolean content, boolean order) {
        if (order) {
            // Can't be order-deterministic without also being content-deterministic.
            assert (content);
            m_statementIsContentDeterministic = true;
            m_statementIsOrderDeterministic = true;
        } else {
            assert (m_statementIsOrderDeterministic == false);
            if (content) {
                m_statementIsContentDeterministic = true;
            }
        }
    }

    /**
     * Accessor for flag marking the plan as guaranteeing an identical result/effect
     * when "replayed" against the same database state, such as during replication or CL recovery.
     * @return the corresponding value from the first fragment
     */
    public boolean isOrderDeterministic() {
        if (m_statementIsOrderDeterministic) {
            return true;
        }
        return rootPlanGraph.isOrderDeterministic();
    }

    /**
     * Accessor for flag marking the plan as guaranteeing an identical result/effect
     * when "replayed" against the same database state, such as during replication or CL recovery.
     * @return the corresponding value from the first fragment
     */
    public boolean isContentDeterministic() {
        if (m_statementIsContentDeterministic) {
            return true;
        }
        return rootPlanGraph.isContentDeterministic();
    }

    /**
     * Accessor for description of plan non-determinism.
     * @return the corresponding value from the first fragment
     */
    public String nondeterminismDetail() {
        return rootPlanGraph.nondeterminismDetail();
    }

    public int countSeqScans() {
        int total = rootPlanGraph.findAllNodesOfType(PlanNodeType.SEQSCAN).size();
        if (subPlanGraph != null) {
            total += subPlanGraph.findAllNodesOfType(PlanNodeType.SEQSCAN).size();
        }
        return total;
    }

    public void setPartitioningKey(Object object) {
        m_partitioningKey = object;
    }

    public Object getPartitioningKey() {
        return m_partitioningKey;
    }

    public static byte[] bytesForPlan(AbstractPlanNode planGraph) {
        if (planGraph == null) {
            return null;
        }

        PlanNodeList planList = new PlanNodeList(planGraph);
        return planList.toJSONString().getBytes(VoltDB.UTF8ENCODING);
    }
}
