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

package org.voltdb.calciteadapter.rel.physical;

import java.util.List;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.rex.RexProgramBuilder;
import org.voltdb.calciteadapter.rel.VoltDBTable;
import org.voltdb.plannodes.AbstractPlanNode;
import org.voltdb.plannodes.SeqScanPlanNode;

public class VoltDBTableSeqScan extends AbstractVoltDBPhysicalTableScan {

    public VoltDBTableSeqScan(RelOptCluster cluster,
            RelTraitSet traitSet,
            RelOptTable table,
            VoltDBTable voltDBTable) {
          this(cluster,
                  traitSet,
                  table,
                  voltDBTable,
                  RexProgram.createIdentity(voltDBTable.getRowType(cluster.getTypeFactory())), null, null);
          assert traitSet.contains(VoltDBPhysicalRel.VOLTDB_PHYSICAL);
    }

    public VoltDBTableSeqScan(RelOptCluster cluster,
            RelTraitSet traitSet,
            RelOptTable table,
            VoltDBTable voltDBTable,
            RexProgram program,
            RexNode offset,
            RexNode limit) {
          super(cluster, traitSet, table, voltDBTable, program, offset, limit);
    }

    @Override public RelOptCost computeSelfCost(RelOptPlanner planner,
            RelMetadataQuery mq) {
        double dRows = estimateRowCount(mq);
        double dCpu = dRows + 1; // ensure non-zero cost
        double dIo = 0;
        return planner.getCostFactory().makeCost(dRows, dCpu, dIo);
    }

    @Override
    public double estimateRowCount(RelMetadataQuery mq) {
        double rowCount = AbstractVoltDBPhysicalTableScan.MAX_TABLE_ROW_COUNT;
        rowCount = estimateRowCountWithPredicate(rowCount);
        // SeqScanPlanNode does not pay attention to limit
//        rowCount = estimateRowCountWithLimit(rowCount);
        return rowCount;
    }

    @Override
    public AbstractPlanNode toPlanNode() {
        SeqScanPlanNode sspn = new SeqScanPlanNode();
        List<String> qualName = table.getQualifiedName();
        sspn.setTargetTableAlias(qualName.get(0));
        sspn.setTargetTableName(m_voltDBTable.getCatTable().getTypeName());

        addProjection(sspn);
        addPredicate(sspn);
        addLimitOffset(sspn);

        return sspn;
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, RexNode offset, RexNode limit) {
        // Do we need a deep copy including the inputs?
        VoltDBTableSeqScan newScan = new VoltDBTableSeqScan(
                getCluster(),
                traitSet,
                getTable(),
                m_voltDBTable,
                m_program,
                offset,
                limit);
        return newScan;
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, RexProgram newProgram, RexBuilder programRexBuilder) {
        // Merge two programs program / m_program into a new merged program
        RexProgram mergedProgram = RexProgramBuilder.mergePrograms(
                newProgram,
                m_program,
                programRexBuilder);

        VoltDBTableSeqScan newScan = new VoltDBTableSeqScan(
                getCluster(),
                traitSet,
                getTable(),
                getVoltDBTable(),
                mergedProgram,
                getOffsetRexNode(),
                getLimitRexNode());
        return newScan;
    }

}
