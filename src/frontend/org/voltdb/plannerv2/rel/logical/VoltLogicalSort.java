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
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;

import com.google.common.base.Preconditions;

/**
 * Sub-class of {@link org.apache.calcite.rel.core.Sort}
 * targeted at the VoltDB logical calling convention.
 *
 * @author Michael Alexeev
 * @since 9.0
 */
public class VoltLogicalSort extends Sort implements VoltLogicalRel {

    /**
     * Creates a VoltLogicalSort.
     *
     * @param cluster   Cluster this relational expression belongs to
     * @param traitSet  Traits
     * @param input     Input relational expression
     * @param collation Array of sort specifications
     */
    public VoltLogicalSort(RelOptCluster cluster, RelTraitSet traitSet, RelNode input, RelCollation collation) {
        super(cluster, traitSet, input, collation, null, null);
        Preconditions.checkArgument(getConvention() == VoltLogicalRel.CONVENTION);
    }

    @Override public VoltLogicalSort copy(
            RelTraitSet traitSet, RelNode input, RelCollation collation, RexNode offset, RexNode fetch) {
        return new VoltLogicalSort(getCluster(), traitSet, input, collation);
    }

    @Override
    public double estimateRowCount(RelMetadataQuery mq) {
        return getInput(0).estimateRowCount(mq);
     }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        final RelOptCost cost = super.computeSelfCost(planner, mq);
        return planner.getCostFactory().makeCost(cost.getRows(),
                cost.getRows(),     // NOTE: CPU cost comes into effect in physical planning stage.
                cost.getIo());
    }

}
