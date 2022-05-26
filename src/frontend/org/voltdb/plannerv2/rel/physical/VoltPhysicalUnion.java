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

import java.util.List;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Union;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.voltdb.plannerv2.rel.util.PlanCostUtil;
import org.voltdb.plannerv2.rel.util.PlanNodeUtil;
import org.voltdb.plannodes.AbstractPlanNode;

import com.google.common.base.Preconditions;

/**
 * Sub-class of {@link org.apache.calcite.rel.core.Union} targeted at the VoltDB physical calling convention.
 *
 * @author Mike Alexeev
 * @since 9.0
 */
public class VoltPhysicalUnion extends Union implements VoltPhysicalRel {

    /**
     * Creates a VoltPhysicalUnion.
     *
     * @param cluster          Cluster
     * @param traitSet         Trait set
     * @param inputs           inputs
     * @param all              SetOps ALL qualifier
     */
    public VoltPhysicalUnion(
            RelOptCluster cluster, RelTraitSet traitSet, List<RelNode> inputs, boolean all) {
        super(cluster, traitSet, inputs, all);
        Preconditions.checkArgument(getConvention() == VoltPhysicalRel.CONVENTION);
    }

    @Override public VoltPhysicalUnion copy(RelTraitSet traitSet, List<RelNode> inputs, boolean all) {
        return new VoltPhysicalUnion(getCluster(), traitSet, inputs, all);
    }

    @Override
    public double estimateRowCount(RelMetadataQuery mq) {
        // For unions, row count and cpu is a simple sum of children totals
        return PlanCostUtil.computeSetOpCost(getInputs(), mq);
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        double rowCount = estimateRowCount(mq);
        double cpu = rowCount;
        return planner.getCostFactory().makeCost(rowCount, cpu, 0);
    }

    @Override
    public AbstractPlanNode toPlanNode() {
        return PlanNodeUtil.setOpToPlanNode(this);
    }

}
