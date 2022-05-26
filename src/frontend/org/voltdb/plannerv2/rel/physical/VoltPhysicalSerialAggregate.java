/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
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

package org.voltdb.plannerv2.rel.physical;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.ImmutableBitSet;
import org.voltdb.plannerv2.rel.util.PlanCostUtil;
import org.voltdb.plannodes.AggregatePlanNode;

import java.util.List;

public class VoltPhysicalSerialAggregate extends VoltPhysicalAggregate {

    // Inline Rels
    final private RexNode m_offset;
    final private RexNode m_limit;

    /**
     * Constructor
     */
    public VoltPhysicalSerialAggregate(
            RelOptCluster cluster, RelTraitSet traitSet, RelNode child, boolean indicator, ImmutableBitSet groupSet,
            List<ImmutableBitSet> groupSets, List<AggregateCall> aggCalls, RexNode postPredicate,
            boolean isCoordinatorAggr, RexNode offset, RexNode limit) {
        super(cluster, traitSet, child, indicator, groupSet, groupSets, aggCalls, postPredicate,
                isCoordinatorAggr);
        m_offset = offset;
        m_limit = limit;
    }

    public VoltPhysicalSerialAggregate(
            RelOptCluster cluster, RelTraitSet traitSet, RelNode child, boolean indicator, ImmutableBitSet groupSet,
            List<ImmutableBitSet> groupSets, List<AggregateCall> aggCalls, RexNode postPredicate,
            boolean isCoordinatorAggr) {
        super(cluster, traitSet, child, indicator, groupSet, groupSets, aggCalls, postPredicate,
                isCoordinatorAggr);
        m_offset = null;
        m_limit = null;
    }

    @Override
    public RelOptCost computeSelfCost(
            RelOptPlanner planner, RelMetadataQuery mq) {
        double rowCount =
                PlanCostUtil.discountSerialAggregateRowCount(getInput().estimateRowCount(mq), getGroupCount());
        return planner.getCostFactory().makeCost(rowCount, rowCount, 0);
    }

    @Override
    public VoltPhysicalSerialAggregate copy(
            RelTraitSet traitSet, RelNode input, boolean indicator, ImmutableBitSet groupSet,
            List<ImmutableBitSet> groupSets, List<AggregateCall> aggCalls) {
        return new VoltPhysicalSerialAggregate(
                getCluster(), traitSet, input, indicator, groupSet, groupSets, aggCalls, getPostPredicate(),
                getIsCoordinatorAggr(), m_offset, m_limit);
    }

    @Override
    public VoltPhysicalSerialAggregate copy(
            RelOptCluster cluster, RelTraitSet traitSet, RelNode input, boolean indicator, ImmutableBitSet groupSet,
            List<ImmutableBitSet> groupSets, List<AggregateCall> aggCalls, RexNode havingExpression,
            boolean isCoordinatorAggr) {
        return new VoltPhysicalSerialAggregate(
                cluster, traitSet, input, indicator, groupSet, groupSets, aggCalls, havingExpression,
                isCoordinatorAggr, m_offset, m_limit);
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        super.explainTerms(pw);
        pw.item("type", "serial");
        return pw;
    }

    public RexNode getOffset() {
        return m_offset;
    }

    public RexNode getLimit() {
        return m_limit;
    }

    @Override
    protected String computeDigest() {
        String digest = super.computeDigest();
        if (m_limit != null) {
            digest += "_limit_" + m_limit.toString();
        }
        if (m_offset != null) {
            digest += "_offset_" + m_offset.toString();
        }
        return digest;
    }

    @Override
    protected AggregatePlanNode getAggregatePlanNode() {
        return new AggregatePlanNode();
    }
}
