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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Values;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexLiteral;

import java.util.List;

/**
 * Sub-class of {@link org.apache.calcite.rel.core.Values}
 * targeted at the VoltDB logical calling {@link #CONVENTION}.
 *
 * @author Chao Zhou
 * @since 9.0
 */
public class VoltLogicalValues extends Values implements VoltLogicalRel {
    public VoltLogicalValues(
            RelOptCluster cluster, RelTraitSet traitSet, RelDataType rowType,
            ImmutableList<ImmutableList<RexLiteral>> tuples) {
        // VoltLogicalValues is always SP
        super(cluster, rowType, tuples, traitSet.replace(RelDistributions.SINGLETON.with(null, true)));
        Preconditions.checkArgument(getConvention() == VoltLogicalRel.CONVENTION);
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new VoltLogicalValues(getCluster(), traitSet, rowType, tuples);
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        final RelOptCost cost = super.computeSelfCost(planner, mq);
        return planner.getCostFactory().makeCost(cost.getRows(),
                cost.getRows(),     // NOTE: CPU cost comes into effect in physical planning stage.
                cost.getIo());
    }

    @Override
    public double estimateRowCount(RelMetadataQuery mq) {
        return 1.;
    }
}
