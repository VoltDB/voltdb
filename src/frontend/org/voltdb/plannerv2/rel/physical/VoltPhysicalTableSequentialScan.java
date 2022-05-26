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
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.rex.RexProgramBuilder;
import org.voltdb.plannerv2.VoltTable;
import org.voltdb.plannerv2.rel.AbstractVoltTableScan;
import org.voltdb.plannodes.AbstractPlanNode;
import org.voltdb.plannodes.SeqScanPlanNode;

import com.google.common.base.Preconditions;

/**
 * The relational expression that represent a sequential VoltDB physical table scan.
 *
 * @author Michael Alexeev
 * @since 9.0
 */
public class VoltPhysicalTableSequentialScan extends VoltPhysicalTableScan {

    public VoltPhysicalTableSequentialScan(
            RelOptCluster cluster, RelTraitSet traitSet, RelOptTable table, VoltTable voltTable,
            RexProgram program, RexNode offset, RexNode limit, RelNode aggregate,
            RelDataType preAggregateRowType, RexProgram preAggregateProgram) {
        super(cluster, traitSet, table, voltTable, program, offset, limit, aggregate, preAggregateRowType,
                preAggregateProgram);
    }

    public VoltPhysicalTableSequentialScan(
            RelOptCluster cluster, RelTraitSet traitSet, RelOptTable table, VoltTable voltTable) {
        this(cluster, traitSet, table, voltTable,
                RexProgram.createIdentity(voltTable.getRowType(cluster.getTypeFactory())), null,
                null, null, null, null);
        Preconditions.checkArgument(getConvention() == VoltPhysicalRel.CONVENTION);
    }

    /**
     * Returns the cost of this plan (not including children).
     * We will consider the row count as estimateRowCount,
     * and the CPU cost as estimateRowCount+1.
     * The IO cost is always 0 cause we are in-memory.
     *
     * @param planner Planner for cost calculation
     * @param mq      Metadata query
     * @return Cost of this plan (not including children)
     */
    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        double dRows = estimateRowCount(mq);
        double dCpu = dRows;
        double dIo = 0;
        return planner.getCostFactory().makeCost(dRows, dCpu, dIo);
    }

    @Override
    public AbstractVoltTableScan copyWithLimitOffset(RelTraitSet traitSet, RexNode offset, RexNode limit) {
        return new VoltPhysicalTableSequentialScan(
                getCluster(), traitSet, getTable(), getVoltTable(), getProgram(), offset, limit,
                getAggregateRelNode(), getPreAggregateRowType(), getPreAggregateProgram());
    }

    @Override
    public AbstractVoltTableScan copyWithProgram(RelTraitSet traitSet, RexProgram newProgram, RexBuilder programRexBuilder) {
        // Merge two programs program / m_program into a new merged program
        return new VoltPhysicalTableSequentialScan(
                getCluster(), traitSet, getTable(), getVoltTable(),
                RexProgramBuilder.mergePrograms(newProgram, m_program, programRexBuilder),
                getOffsetRexNode(), getLimitRexNode(), getAggregateRelNode(), getPreAggregateRowType(),
                getPreAggregateProgram());
    }

    @Override
    public AbstractVoltTableScan copyWithAggregate(RelTraitSet traitSet, RelNode aggregate) {
        // Need to create a Program for the inline aggregate because it will define
        // the output row type for the scan
        // Preserve the original program and row type
        return new VoltPhysicalTableSequentialScan(
                getCluster(), traitSet, getTable(), getVoltTable(), RexProgram.createIdentity(aggregate.getRowType()),
                getOffsetRexNode(), getLimitRexNode(), aggregate, getRowType(), getProgram());
    }

    @Override
    public AbstractPlanNode toPlanNode() {
        final SeqScanPlanNode sspn = new SeqScanPlanNode();
        final List<String> qualName = table.getQualifiedName();
        // index_0: schema name, Index_1: table name
        sspn.setTargetTableAlias(qualName.get(1));
        sspn.setTargetTableName(m_voltTable.getCatalogTable().getTypeName());

        addProjection(sspn);
        addPredicate(sspn);
        if (m_aggregate == null) {
            // If there is an aggregate, the Limit / Offset will be inlined with aggregate node
            addLimitOffset(sspn);
        }
        addAggregate(sspn);

        return sspn;
    }
}
