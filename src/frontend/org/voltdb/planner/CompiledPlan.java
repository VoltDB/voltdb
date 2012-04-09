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

import java.util.ArrayList;
import java.util.List;

import org.voltdb.expressions.AbstractExpression;
import org.voltdb.plannodes.AbstractPlanNode;
import org.voltdb.plannodes.NodeSchema;
import org.voltdb.types.PlanNodeType;

/**
 * A triple-tuple to hold a complete plan graph along with its
 * input (parameters) and output (result columns). This class just
 * exists to make it a convenient return value for methods that
 * output plans.
 *
 */
public class CompiledPlan {

    public static class Fragment implements Comparable<Fragment> {
        /** A complete plan graph */
        public AbstractPlanNode planGraph;

        /** IDs of dependencies that must be met to begin */
        public boolean hasDependencies = false;

        /** Does this fragment get sent to all partitions */
        public boolean multiPartition = false;

        @Override
        public int compareTo(Fragment o) {
            Integer me = multiPartition == false ? 0 : 1;
            Integer them = o.multiPartition == false ? 0 : 1;
            return me.compareTo(them);
        }
    }

    /**
     * The set of plan fragments that make up this plan
     * The first in the list must be the root.
     */
    public List<Fragment> fragments = new ArrayList<Fragment>();

    /**
     * The SQL text of the statement
     */
    public String sql = null;

    /**
     * Cost of the plan as extimated (not necessarily well)
     * by the planner
     */
    public double cost = 0.0;

    /**
     * The textual explanation of aggregation, join order and
     * access path selection.
     */
    public String explainedPlan = null;

    /** A list of parameter names, indexes and types */
    public List<ParameterInfo> parameters = new ArrayList<ParameterInfo>();

    /** A list of output column ids, indexes and types */
    public NodeSchema columns = new NodeSchema();

    /**
     * If true, divide the number of tuples changed
     * by the number of partitions, as the number will
     * be the sum of tuples changed on all replicas.
     */
    public boolean replicatedTableDML = false;

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

    void resetPlanNodeIds() {
        int nextId = 1;
        for (Fragment f : fragments)
            nextId = resetPlanNodeIds(f.planGraph, nextId);
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
        AbstractPlanNode apn = fragments.get(0).planGraph;
        return apn.isOrderDeterministic();
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
        AbstractPlanNode apn = fragments.get(0).planGraph;
        return apn.isContentDeterministic();
    }

    /**
     * Accessor for description of plan non-determinism.
     * @return the corresponding value from the first fragment
     */
    public String nondeterminismDetail() {
        return fragments.get(0).planGraph.nondeterminismDetail();
    }

    public int countSeqScans() {
        int total = 0;
        for (Fragment fragment : fragments) {
            AbstractPlanNode apn = fragment.planGraph;
            total += apn.findAllNodesOfType(PlanNodeType.SEQSCAN).size();
        }
        return total;
    }

}
