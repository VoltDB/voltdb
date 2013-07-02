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

package org.voltdb.planner;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

import org.voltdb.ParameterSet;
import org.voltdb.VoltType;
import org.voltdb.common.Constants;
import org.voltdb.expressions.AbstractExpression;
import org.voltdb.expressions.ParameterValueExpression;
import org.voltdb.plannodes.AbstractPlanNode;
import org.voltdb.plannodes.IndexCountPlanNode;
import org.voltdb.plannodes.IndexScanPlanNode;
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

    /** Parameter types in parameter index order */
    public VoltType[] parameters = null;

    /** Parameter values, if the planner pulled constants out of the plan */
    public ParameterSet extractedParamValues = ParameterSet.emptyParameterSet();

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
        return planList.toJSONString().getBytes(Constants.UTF8ENCODING);
    }

    // A reusable step extracted from boundParamIndexes so it can be applied to two different
    // sources of bindings, IndexScans and IndexCounts.
    private static void setParamIndexes(BitSet ints, List<AbstractExpression> params) {
        for(AbstractExpression ae : params) {
            assert(ae instanceof ParameterValueExpression);
            ParameterValueExpression pve = (ParameterValueExpression) ae;
            int param = pve.getParameterIndex();
            ints.set(param);
        }
    }

    // An obvious but apparently missing BitSet utility function
    // to convert the set bits to their integer indexes.
    private static int[] bitSetToIntVector(BitSet ints) {
        int intCount = ints.cardinality();
        if (intCount == 0) {
            return null;
        }
        int[] result = new int[intCount];
        int nextBit = ints.nextSetBit(0);
        for (int ii = 0; ii < intCount; ii++) {
            assert(nextBit != -1);
            result[ii] = nextBit;
            nextBit = ints.nextSetBit(nextBit+1);
        }
        assert(nextBit == -1);
        return result;
    }

    /// Extract a sorted de-duped vector of all the bound parameter indexes in a plan. Or null if none.
    public int[] boundParamIndexes() {
        if (parameters.length == 0) {
            return null;
        }

        BitSet ints = new BitSet();
        ArrayList<AbstractPlanNode> ixscans = rootPlanGraph.findAllNodesOfType(PlanNodeType.INDEXSCAN);
        if (subPlanGraph != null) {
            ixscans.addAll(subPlanGraph.findAllNodesOfType(PlanNodeType.INDEXSCAN));
        }
        for (AbstractPlanNode apn : ixscans) {
            assert(apn instanceof IndexScanPlanNode);
            IndexScanPlanNode ixs = (IndexScanPlanNode) apn;
            setParamIndexes(ints, ixs.getBindings());
        }

        ArrayList<AbstractPlanNode> ixcounts = rootPlanGraph.findAllNodesOfType(PlanNodeType.INDEXCOUNT);
        if (subPlanGraph != null) {
            ixcounts.addAll(subPlanGraph.findAllNodesOfType(PlanNodeType.INDEXCOUNT));
        }
        for (AbstractPlanNode apn : ixcounts) {
            assert(apn instanceof IndexCountPlanNode);
            IndexCountPlanNode ixc = (IndexCountPlanNode) apn;
            setParamIndexes(ints, ixc.getBindings());
        }
        return bitSetToIntVector(ints);
    }

}
