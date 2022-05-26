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

package org.voltdb.plannerv2.rel.logical;

import java.util.List;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.util.ImmutableBitSet;

import com.google.common.base.Preconditions;

/**
 * Sub-class of {@link Aggregate} targeted at the VoltDB logical calling convention.
 *
 * @see org.apache.calcite.rel.logical.LogicalAggregate
 * @author Michael Alexeev
 * @since 9.0
 */
public class VoltLogicalAggregate extends Aggregate implements VoltLogicalRel {

    /**
     * Creates a VoltLogicalAggregate.
     *
     * @param cluster   Cluster that this relational expression belongs to
     * @param traitSet  Trait set
     * @param child     Input relational expression
     * @param groupSet  Bit set of grouping fields
     * @param groupSets Grouping sets, or null to use just {@code groupSet}
     * @param aggCalls  Array of aggregates to compute, not null
     */
    public VoltLogicalAggregate(
            RelOptCluster cluster, RelTraitSet traitSet, RelNode child, ImmutableBitSet groupSet,
            List<ImmutableBitSet> groupSets, List<AggregateCall> aggCalls) {
        super(cluster, traitSet, child, false /*indicator*/, groupSet, groupSets, aggCalls);
        Preconditions.checkArgument(getConvention() == VoltLogicalRel.CONVENTION);
    }

    @Override public VoltLogicalAggregate copy(
            RelTraitSet traitSet, RelNode input, boolean indicator, ImmutableBitSet groupSet,
            List<ImmutableBitSet> groupSets, List<AggregateCall> aggCalls) {
        return new VoltLogicalAggregate(getCluster(), traitSet, input, groupSet, groupSets, aggCalls);
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        final RelOptCost cost = super.computeSelfCost(planner, mq);
        return planner.getCostFactory().makeCost(cost.getRows(),
                cost.getRows(),     // NOTE: CPU cost comes into effect in physical planning stage.
                cost.getIo());
    }

}
