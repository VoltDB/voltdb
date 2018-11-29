/* This file is part of VoltDB.
 * Copyright (C) 2008-2018 VoltDB Inc.
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

package org.voltdb.calciteadapter.rel.physical;

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
import org.voltdb.calciteadapter.rel.util.PlanCostUtil;

import java.util.List;

public class VoltDBPSerialAggregate extends AbstractVoltDBPAggregate {

    // Inline Rels
    final private RexNode m_offset;
    final private RexNode m_limit;

    /**
     * Constructor
     */
    public VoltDBPSerialAggregate(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            RelNode child,
            boolean indicator,
            ImmutableBitSet groupSet,
            List<ImmutableBitSet> groupSets,
            List<AggregateCall> aggCalls,
            RexNode postPredicate,
            int splitCount,
            boolean isCoordinatorAggr,
            RexNode offset,
            RexNode limit
    ) {
        super(cluster,
                traitSet,
                child,
                indicator,
                groupSet,
                groupSets,
                aggCalls,
                postPredicate,
                splitCount,
                isCoordinatorAggr);
        m_offset = offset;
        m_limit = limit;
    }

    public VoltDBPSerialAggregate(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            RelNode child,
            boolean indicator,
            ImmutableBitSet groupSet,
            List<ImmutableBitSet> groupSets,
            List<AggregateCall> aggCalls,
            RexNode postPredicate,
            int splitCount,
            boolean isCoordinatorAggr) {
        this(cluster,
                traitSet,
                child,
                indicator,
                groupSet,
                groupSets,
                aggCalls,
                postPredicate,
                splitCount,
                isCoordinatorAggr,
                null,
                null);
    }

    public VoltDBPSerialAggregate(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            RelNode child,
            boolean indicator,
            ImmutableBitSet groupSet,
            List<ImmutableBitSet> groupSets,
            List<AggregateCall> aggCalls,
            RexNode postPredicate,
            boolean isCoordinatorAggr) {
        this(cluster,
                traitSet,
                child,
                indicator,
                groupSet,
                groupSets,
                aggCalls,
                postPredicate,
                1,
                isCoordinatorAggr);
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner,
                                      RelMetadataQuery mq) {
        double rowCount = getInput().estimateRowCount(mq);

        rowCount = PlanCostUtil.discountRowCountSerialAggregate(rowCount, getGroupCount());
        return planner.getCostFactory().makeCost(rowCount, 0, 0);
    }

    @Override
    public VoltDBPSerialAggregate copy(RelTraitSet traitSet,
                                       RelNode input,
                                       boolean indicator,
                                       ImmutableBitSet groupSet,
                                       List<ImmutableBitSet> groupSets,
                                       List<AggregateCall> aggCalls) {
        return new VoltDBPSerialAggregate(
                getCluster(),
                traitSet,
                input,
                indicator,
                groupSet,
                groupSets,
                aggCalls,
                getPostPredicate(),
                getSplitCount(),
                getIsCoordinatorAggr(),
                m_offset,
                m_limit);
    }

    @Override
    public VoltDBPSerialAggregate copy(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            RelNode input,
            boolean indicator,
            ImmutableBitSet groupSet,
            List<ImmutableBitSet> groupSets,
            List<AggregateCall> aggCalls,
            RexNode postPredicate,
            int splitCount,
            boolean isCoordinatorAggr) {
        return new VoltDBPSerialAggregate(
                cluster,
                traitSet,
                input,
                indicator,
                groupSet,
                groupSets,
                aggCalls,
                postPredicate,
                splitCount,
                isCoordinatorAggr,
                m_offset,
                m_limit);
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        super.explainTerms(pw);
        pw.item("type", "serial");
        return pw;
    }

    private boolean hasLimitOffset() {
        return (m_limit != null || m_offset != null);
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
}
