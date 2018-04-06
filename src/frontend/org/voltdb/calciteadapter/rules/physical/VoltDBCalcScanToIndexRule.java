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

package org.voltdb.calciteadapter.rules.physical;

import java.util.List;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.rex.RexProgramBuilder;
import org.voltdb.calciteadapter.rel.VoltDBTable;
import org.voltdb.calciteadapter.rel.physical.VoltDBCalc;
import org.voltdb.calciteadapter.rel.physical.VoltDBTableIndexScan;
import org.voltdb.calciteadapter.rel.physical.VoltDBTableSeqScan;
import org.voltdb.calciteadapter.util.IndexUtil;
import org.voltdb.calciteadapter.util.VoltDBRexUtil;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.Index;
import org.voltdb.catalog.Table;
import org.voltdb.planner.AccessPath;
import org.voltdb.types.SortDirectionType;
import org.voltdb.utils.CatalogUtil;

public class VoltDBCalcScanToIndexRule extends RelOptRule {

    public static final VoltDBCalcScanToIndexRule INSTANCE = new VoltDBCalcScanToIndexRule();

    private VoltDBCalcScanToIndexRule() {
        super(operand(VoltDBCalc.class, operand(VoltDBTableSeqScan.class, none())));
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        VoltDBTableSeqScan scan = call.rel(1);
        VoltDBTable table = scan.getVoltDBTable();
        assert(table != null);
        boolean matches = !table.getCatTable().getIndexes().isEmpty();
        return matches;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        VoltDBCalc calc = call.rel(0);
        VoltDBTableSeqScan scan = call.rel(1);

        RexProgram calcProgram = calc.getProgram();

        RexProgram scanProgram = scan.getProgram();
        assert(scanProgram != null);

        // Merge two programs
        RexBuilder rexBuilder = calc.getCluster().getRexBuilder();
        RexProgram mergedProgram = RexProgramBuilder.mergePrograms(
                calcProgram,
                scanProgram,
                rexBuilder);

        Table catTableable = scan.getVoltDBTable().getCatTable();
        List<Column> columns = CatalogUtil.getSortedCatalogItems(catTableable.getColumns(), "index");

        RexNode filterCondition = calc.getProgram().getCondition();

        for (Index index : catTableable.getIndexes()) {
            AccessPath accessPath = IndexUtil.getCalciteRelevantAccessPathForIndex(
                    catTableable, columns, filterCondition, mergedProgram, index, SortDirectionType.INVALID);

            if (accessPath != null) {
                // if accessPath.other is not null, need to create a new Filter
                // @TODO Adjust Calc program Condition based on the access path "other" filters
                RelCollation indexCollation = VoltDBRexUtil.createIndexCollation(
                        index,
                        catTableable,
                        rexBuilder,
                        mergedProgram);
                VoltDBTableIndexScan indexScan = VoltDBTableIndexScan.create(
                        scan.getCluster(),
                        // Need to add an index collation trait
                        scan.getTraitSet().plus(indexCollation),
                        scan.getTable(),
                        scan.getVoltDBTable(),
                        mergedProgram,
                        index,
                        accessPath,
                        scan.getLimitRexNode(),
                        scan.getOffsetRexNode());
                call.transformTo(indexScan);
            }
        }
    }

}
