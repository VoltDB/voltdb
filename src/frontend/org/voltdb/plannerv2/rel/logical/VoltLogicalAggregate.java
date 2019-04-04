/* This file is part of VoltDB.
 * Copyright (C) 2008-2019 VoltDB Inc.
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

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelDistributionTraitDef;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
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
        super(cluster,
                updateTraitSet(aggCalls, traitSet, child),
                child, false /*indicator*/, groupSet, groupSets, aggCalls);
        Preconditions.checkArgument(getConvention() == VoltLogicalRel.CONVENTION);
    }

    /**
     * Update trait set, by inspecting all the aggregation function arguments when the child is HASH_DISTRIBUTED:
     * change it to SINGLETON distribution only when all aggregation function arguments are partition columns.
     * @param aggs aggregation functions
     * @param traitSet source trait set
     * @param child child node whose distribution we inspect, and act upon
     * @return converted distribution.
     */
    private static RelTraitSet updateTraitSet(List<AggregateCall> aggs, RelTraitSet traitSet, RelNode child) {
        if (! aggs.isEmpty()) {       // not a "true" aggregate
            final RelDistribution childDist = child.getTraitSet().getTrait(RelDistributionTraitDef.INSTANCE);
            if (childDist != null) {
                final Set<Integer> partCols = new HashSet<>(childDist.getKeys());
                if (aggs.stream().flatMap(call -> {
                    assert call.getArgList().size() <= 1;
                    if (call.getArgList().isEmpty()) {  // COUNT removes argument
                        return Stream.empty();
                    } else {
                        return Stream.of(call.getArgList().get(0));
                    }
                }).allMatch(partCols::contains)) {
                    return traitSet.replace(RelDistributions.SINGLETON.with(null, true));
                }
            }
        }
        return traitSet;
    }

    @Override public VoltLogicalAggregate copy(
            RelTraitSet traitSet, RelNode input, boolean indicator, ImmutableBitSet groupSet,
            List<ImmutableBitSet> groupSets, List<AggregateCall> aggCalls) {
        return new VoltLogicalAggregate(getCluster(), traitSet, input, groupSet, groupSets, aggCalls);
    }
}
