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
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelDistributionTraitDef;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Exchange;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.voltdb.plannerv2.converter.RexConverter;
import org.voltdb.plannodes.AbstractPlanNode;
import org.voltdb.plannodes.ReceivePlanNode;
import org.voltdb.plannodes.SendPlanNode;

import com.google_voltpatches.common.base.Preconditions;

public class VoltPhysicalExchange extends Exchange implements VoltPhysicalRel {

    public VoltPhysicalExchange(
            RelOptCluster cluster, RelTraitSet traitSet, RelNode input, RelDistribution newDistribution) {
        super(cluster, traitSet, input, newDistribution);
        Preconditions.checkArgument(! RelDistributions.ANY.getType().equals(
                traitSet.getTrait(RelDistributionTraitDef.INSTANCE).getType()));
    }

    @Override
    public VoltPhysicalExchange copy(
            RelTraitSet traitSet, RelNode newInput, RelDistribution newDistribution) {
        return new VoltPhysicalExchange(getCluster(), traitSet, newInput, newDistribution);
    }

    @Override
    public double estimateRowCount(RelMetadataQuery mq) {
        return getInput().estimateRowCount(mq);
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        // row count and cpu cost are the same
        double rowCount = estimateRowCount(mq);
        double cpu = rowCount;
        double io = rowCount;
        return planner.getCostFactory().makeCost(rowCount, cpu, io);
    }

    @Override
    public AbstractPlanNode toPlanNode() {
        final AbstractPlanNode child = inputRelNodeToPlanNode(this, 0);
        final SendPlanNode spn = new SendPlanNode();
        spn.addAndLinkChild(child);
        final ReceivePlanNode rpn = new ReceivePlanNode();
        rpn.setOutputSchema(RexConverter.convertToVoltDBNodeSchema(getRowType(), 0));
        rpn.setHaveSignificantOutputSchema(true);
        rpn.addAndLinkChild(spn);
        return rpn;
    }

}
