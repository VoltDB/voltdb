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

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rel.core.Exchange;
import org.apache.calcite.rel.metadata.RelMetadataQuery;

import com.google.common.base.Preconditions;

/**
 * Sub-class of {@link Calc} targeted at the VoltDB logical calling convention.
 *
 * @see org.apache.calcite.rel.logical.LogicalCalc
 * @author Michael Alexeev
 * @since 9.0
 */
public class VoltLogicalExchange extends Exchange implements VoltLogicalRel {

    /**
     * Create a VoltLogicalCalc.
     *
     * @param cluster Cluster
     * @param traitSet Traits
     * @param input Input relation
     * @param distribution
     */
    public VoltLogicalExchange(RelOptCluster cluster, RelTraitSet traitSet,
            RelNode input, RelDistribution distribution) {
        super(cluster, traitSet, input, distribution);
        Preconditions.checkArgument(getConvention() == VoltLogicalRel.CONVENTION);
    }

    @Override public Exchange copy(RelTraitSet traitSet, RelNode newInput,
            RelDistribution newDistributionm) {
        return new VoltLogicalExchange(getCluster(), traitSet, newInput, newDistributionm);
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        final RelOptCost cost = super.computeSelfCost(planner, mq);
        return planner.getCostFactory().makeCost(cost.getRows(),
                cost.getRows(),     // NOTE: CPU cost comes into effect in physical planning stage.
                cost.getIo());
    }

}
