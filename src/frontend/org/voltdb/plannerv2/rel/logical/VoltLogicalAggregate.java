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

package org.voltdb.plannerv2.rel.logical;

import com.google.common.base.Preconditions;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.util.ImmutableBitSet;

import java.util.List;

/**
 * Logical Aggregate with <code>VOLTDB_LOGICAL</code> convention trait.
 *
 * @author Michael Alexeev
 * @since 8.4
 */
public class VoltLogicalAggregate extends Aggregate implements VoltLogicalRel {

    /**
     * Constructor
     */
    private VoltLogicalAggregate(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            RelNode child,
            boolean indicator,
            ImmutableBitSet groupSet,
            List<ImmutableBitSet> groupSets,
            List<AggregateCall> aggCalls) {
        super(cluster, traitSet, child, indicator, groupSet, groupSets, aggCalls);
        Preconditions.checkArgument(getConvention() == VoltLogicalRel.CONVENTION);
    }

    @Override
    public VoltLogicalAggregate copy(RelTraitSet traitSet, RelNode input,
                                 boolean indicator, ImmutableBitSet groupSet,
                                 List<ImmutableBitSet> groupSets, List<AggregateCall> aggCalls) {
        return VoltLogicalAggregate.create(
                getCluster(),
                traitSet,
                input,
                indicator,
                groupSet,
                groupSets,
                aggCalls);
    }

    public static VoltLogicalAggregate create(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            RelNode child,
            boolean indicator,
            ImmutableBitSet groupSet,
            List<ImmutableBitSet> groupSets,
            List<AggregateCall> aggCalls) {
        return new VoltLogicalAggregate(
                cluster,
                traitSet,
                child,
                indicator,
                groupSet,
                groupSets,
                aggCalls);
    }
}
