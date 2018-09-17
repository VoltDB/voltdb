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

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexProgram;
import org.voltdb.calciteadapter.rel.VoltDBTable;
import org.voltdb.calciteadapter.rel.physical.VoltDBPCalc;
import org.voltdb.calciteadapter.rel.physical.VoltDBPSort;
import org.voltdb.calciteadapter.rel.physical.VoltDBPTableIndexScan;
import org.voltdb.calciteadapter.rel.physical.VoltDBPTableSeqScan;
import org.voltdb.calciteadapter.util.VoltDBRelUtil;
import org.voltdb.calciteadapter.util.VoltDBRexUtil;
import org.voltdb.catalog.Index;
import org.voltdb.catalog.Table;
import org.voltdb.planner.AccessPath;
import org.voltdb.types.IndexLookupType;
import org.voltdb.types.SortDirectionType;

public class VoltDBPSortScanToIndexRule extends RelOptRule {

    // TODO: why not split INSTANCE_1 and INSTANCE_2 to two rules?
    public static final VoltDBPSortScanToIndexRule INSTANCE_1 =
            new VoltDBPSortScanToIndexRule(operand(VoltDBPSort.class,
                operand(VoltDBPTableSeqScan.class, none())),
                    "SortScanToIndexRule_1");

    public static final VoltDBPSortScanToIndexRule INSTANCE_2 =
            new VoltDBPSortScanToIndexRule(operand(VoltDBPSort.class,
                operand(VoltDBPCalc.class,
                        operand(VoltDBPTableSeqScan.class, none()))),
                    "SortScanToIndexRule_2");

    private VoltDBPSortScanToIndexRule(RelOptRuleOperand operand, String desc) {
        super(operand, desc);
    }

    // match only when we have a index on the table
    @Override
    public boolean matches(RelOptRuleCall call) {
        VoltDBPTableSeqScan scan = (call.rels.length == 2) ?
                call.rel(1) : call.rel(2);
        VoltDBTable table = scan.getVoltDBTable();
        assert(table != null);
        return !table.getCatTable().getIndexes().isEmpty();
    }


    @Override
    public void onMatch(RelOptRuleCall call) {
        VoltDBPSort sort = call.rel(0);
        RelCollation origSortCollation = sort.getCollation();
        assert(!RelCollations.EMPTY.equals(origSortCollation) &&
                sort.fetch == null &&
                sort.offset == null);

        RelCollation scanSortCollation = null;
        VoltDBPCalc calc = null;
        VoltDBPTableSeqScan scan = null;
        if (call.rels.length == 2) {
            scan = call.rel(1);
            scanSortCollation = origSortCollation;
        } else {
            calc = call.rel(1);
            scanSortCollation = VoltDBRelUtil.sortCollationCalcTranspose(origSortCollation, calc);
            if (RelCollations.EMPTY.equals(scanSortCollation)) {
                return;
            }
            scan = call.rel(2);
        }
        Table catTable = scan.getVoltDBTable().getCatTable();

        RexBuilder builder = scan.getCluster().getRexBuilder();
        RexProgram program = scan.getProgram();
        assert(program != null);

        for (Index index : catTable.getIndexes()) {
            if (!index.getPredicatejson().isEmpty()) {
                // this is apartial index and it can not be considered here
                continue;
            }
            RelCollation indexCollation =
                    VoltDBRexUtil.createIndexCollation(index, catTable, builder, program);
            SortDirectionType sortDirection =
                    VoltDBRexUtil.areCollationsCompartible(scanSortCollation, indexCollation);
            //@TODO Cutting corner here. Should probably use something similar to
            // the SubPlanAssembler.WindowFunctionScoreboard
            if (SortDirectionType.INVALID != sortDirection) {
                AccessPath accessPath = new AccessPath(
                        index,
                        // With no index expression, the lookup type will be ignored and
                        // the sort direction will determine the scan direction;
                        IndexLookupType.EQ,
                        sortDirection,
                        true);
                VoltDBPTableIndexScan indexScan = new VoltDBPTableIndexScan(
                        scan.getCluster(),
                        // Need to add sort collation trait
                        scan.getTraitSet().replace(scanSortCollation),
                        scan.getTable(),
                        scan.getVoltDBTable(),
                        scan.getProgram(),
                        index,
                        accessPath,
                        scan.getLimitRexNode(),
                        scan.getOffsetRexNode(),
                        scan.getAggregateRelNode(),
                        scan.getPreAggregateRowType(),
                        scan.getPreAggregateProgram(),
                        scan.getSplitCount());

                RelNode result = null;
                if (calc == null) {
                    result = indexScan;
                } else {
                    // The new Calc collation must match the original Sort collation
                    result = calc.copy(
                            calc.getTraitSet().replace(origSortCollation),
                            indexScan,
                            calc.getProgram(),
                            calc.getSplitCount());
                }
                call.transformTo(result);
            }
        }
    }

}
