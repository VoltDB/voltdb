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

import java.util.ArrayList;
import java.util.List;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelDistributionTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.voltdb.plannerv2.VoltTable;
import org.voltdb.plannerv2.rel.AbstractVoltTableScan;

/**
 * Relational expression representing a scan of a {@link VoltTable}, in the logical phase.
 *
 * @author Mike Alexeev
 * @since 9.0
 */
public class VoltLogicalTableScan extends AbstractVoltTableScan implements VoltLogicalRel {

    /**
     * Create a {@link VoltLogicalTableScan}.
     *
     * @param cluster    Cluster that this relational expression belongs to
     * @param traitSet   Trait set
     * @param table      The corresponding relational dataset in a {@link RelOptSchema}.
     * @param voltTable  VoltDB translatable table
     */
    public VoltLogicalTableScan(
            RelOptCluster cluster, RelTraitSet traitSet, RelOptTable table, VoltTable voltTable) {
        super(cluster, convert(table, voltTable, traitSet), table, voltTable);
    }

    private static RelTraitSet convert(RelOptTable table, VoltTable voltTable, RelTraitSet traitSet) {
        if (traitSet.getTrait(RelDistributionTraitDef.INSTANCE) == null) {
            return traitSet;
        } else {
            final Integer partitionColumn = voltTable.getPartitionColumn();
            final List<Integer> keys = new ArrayList<>();
            if (partitionColumn != null) {
                keys.add(partitionColumn);
            }
            return traitSet.replace(traitSet.getTrait(RelDistributionTraitDef.INSTANCE)
                    .with(table.getDistribution().getType(), keys, null,
                            // For replicated table, partitionColumn == null => SP
                            partitionColumn == null));
        }
    }

    @Override public RelNode copy(RelTraitSet traits, List<RelNode> inputs) {
        return new VoltLogicalTableScan(getCluster(), traits, getTable(), getVoltTable());
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
        return getTable().getRowCount();
    }

}
