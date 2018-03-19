/* This file is part of VoltDB.
 * Copyright (C) 2008-2017 VoltDB Inc.
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

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexProgram;
import org.voltdb.calciteadapter.VoltDBTable;
import org.voltdb.calciteadapter.rel.physical.VoltDBSort;
import org.voltdb.calciteadapter.rel.physical.VoltDBTableIndexScan;
import org.voltdb.calciteadapter.rel.physical.VoltDBTableSeqScan;
import org.voltdb.calciteadapter.voltdb.RexUtil;
import org.voltdb.catalog.Index;
import org.voltdb.catalog.Table;
import org.voltdb.planner.AccessPath;
import org.voltdb.types.IndexLookupType;
import org.voltdb.types.SortDirectionType;

public class VoltDBSortScanToIndexRule extends RelOptRule {

    public static final VoltDBSortScanToIndexRule INSTANCE = new VoltDBSortScanToIndexRule();

    private VoltDBSortScanToIndexRule() {
        super(operand(VoltDBSort.class,
                operand(VoltDBTableSeqScan.class, none())));
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        VoltDBSort sort = call.rel(0);
        VoltDBTableSeqScan scan = call.rel(1);
        VoltDBTable table = scan.getVoltDBTable();
        assert(table != null);
        boolean matches = !table.getCatTable().getIndexes().isEmpty() &&
                !sort.getCollation().getFieldCollations().isEmpty() &&
                sort.fetch == null &&
                sort.offset == null;
        return matches;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        VoltDBSort sort = call.rel(0);
        RelCollation sortCollation = sort.getCollation();

        VoltDBTableSeqScan scan = call.rel(1);
        RexBuilder builder = scan.getCluster().getRexBuilder();
        RexProgram scanProgram = scan.getProgram();
        assert(scanProgram != null);
        Table catTable = scan.getVoltDBTable().getCatTable();

        for (Index index : catTable.getIndexes()) {
            RelCollation indexCollation =
                    RexUtil.createIndexCollation(index, catTable, builder, scanProgram);
            org.voltcore.utils.Pair<SortDirectionType, Boolean> collationsCompatibility =
                    RexUtil.areCollationsCompartible(sortCollation, indexCollation);
            //@TODO Cutting corner here. Should probably use something similar to
            // the SubPlanAssembler.WindowFunctionScoreboard
            if (collationsCompatibility.getSecond()) {
                AccessPath accessPath = new AccessPath(
                        index,
                        // With no index expression, the lookup type will be ignored and
                        // the sort direction will determine the scan direction;
                        IndexLookupType.EQ,
                        collationsCompatibility.getFirst(),
                        true);
                VoltDBTableIndexScan indexScan = new VoltDBTableIndexScan(
                            scan.getCluster(),
                            // Have to preserve the sort collation in addition to the index own one
                            // to make sure that the index node has the same collation
                            // as the sort node it replaces
                            scan.getTraitSet().plus(sortCollation),
                            scan.getTable(),
                            scan.getVoltDBTable(),
                            scan.getProgram(),
                            index,
                            accessPath,
                            scan.getLimitRexNode(),
                            scan.getOffsetRexNode());
                call.transformTo(indexScan);
            }
        }
    }

}