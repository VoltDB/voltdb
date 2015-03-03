/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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

    public final static int MAX_PARAM_COUNT = 1025; // keep synched with value in EE VoltDBEngine.h

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

    /** Parameters and their types in parameter index order */
    public ParameterValueExpression[] parameters = null;
    private VoltType[] m_parameterTypes = null;

    /** Parameter values, if the planner pulled constants out of the plan */
    private ParameterSet m_extractedParamValues = ParameterSet.emptyParameterSet();

    /** Compiler generated parameters for cacheble AdHoc queries */
    private int m_generatedParameterCount = 0;

    /**
     * If true, divide the number of tuples changed
     * by the number of partitions, as the number will
     * be the sum of tuples changed on all replicas.
     */
    public boolean replicatedTableDML = false;

    /** Does the statement write? */
    private boolean m_readOnly = false;

    /**
     * Whether the plan's statement mandates a result with nondeterministic content;
     */
    private boolean m_statementHasLimitOrOffset = false;

    /**
     * Whether the plan's statement mandates a result with deterministic content and order;
     */
    private boolean m_statementIsOrderDeterministic = false;

    /** Which extracted param is the partitioning object (assuming parameterized plans) */
    public int partitioningKeyIndex = -1;

    private Object m_partitioningValue;

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
    public void statementGuaranteesDeterminism(boolean hasLimitOrOffset, boolean order) {
        m_statementHasLimitOrOffset = hasLimitOrOffset;
        m_statementIsOrderDeterministic = order;
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
     * Accessor for flag marking the original statement as guaranteeing an identical result/effect
     * when "replayed" against the same database state, such as during replication or CL recovery.
     */
    public boolean hasDeterministicStatement()
    {
        return m_statementIsOrderDeterministic;
    }

    /**
     * Accessor for flag marking the plan as guaranteeing an identical result/effect
     * when "replayed" against the same database state, such as during replication or CL recovery.
     * @return the corresponding value from the first fragment
     */
    public boolean hasLimitOrOffset() {
        return m_statementHasLimitOrOffset;
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
        // add full index scans
        ArrayList<AbstractPlanNode> indexScanNodes = rootPlanGraph.findAllNodesOfType(PlanNodeType.INDEXSCAN);
        if (subPlanGraph != null) {
            indexScanNodes.addAll(subPlanGraph.findAllNodesOfType(PlanNodeType.INDEXSCAN));
        }
        for (AbstractPlanNode node : indexScanNodes) {
            if (((IndexScanPlanNode)node).getSearchKeyExpressions().isEmpty()) {
                total++;
            }
        }
        return total;
    }

    public void setPartitioningValue(Object object) {
        m_partitioningValue = object;
    }

    public Object getPartitioningValue() {
        return m_partitioningValue;
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

    // This is assumed to be called only after parameters has been fully initialized.
    public VoltType[] parameterTypes() {
        if (m_parameterTypes == null) {
            m_parameterTypes = new VoltType[parameters.length];
            int ii = 0;
            for (ParameterValueExpression param : parameters) {
                m_parameterTypes[ii++] = param.getValueType();
            }
        }
        return m_parameterTypes;
    }

    public boolean extractParamValues(ParameterizationInfo paramzInfo) throws Exception {
        VoltType[] paramTypes = parameterTypes();
        if (paramTypes.length > MAX_PARAM_COUNT) {
            return false;
        }
        if (paramzInfo.paramLiteralValues != null) {
            m_generatedParameterCount = paramzInfo.paramLiteralValues.length;
        }

        m_extractedParamValues = paramzInfo.extractedParamValues(paramTypes);
        return true;
    }

    public ParameterSet extractedParamValues() {
        return m_extractedParamValues;
    }

    public int getQuestionMarkParameterCount() {
        if (parameters == null) {
            return 0;
        }
        return parameters.length - m_generatedParameterCount;
    }

    public boolean getReadOnly() {
        return m_readOnly;
    }

    public void setReadOnly(boolean newValue) {
        m_readOnly = newValue;
    }
}
