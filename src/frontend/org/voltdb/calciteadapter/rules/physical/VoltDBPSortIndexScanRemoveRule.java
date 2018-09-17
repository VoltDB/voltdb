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
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexProgram;
import org.voltdb.calciteadapter.rel.physical.VoltDBPCalc;
import org.voltdb.calciteadapter.rel.physical.VoltDBPSort;
import org.voltdb.calciteadapter.rel.physical.VoltDBPTableIndexScan;
import org.voltdb.calciteadapter.util.VoltDBRelUtil;
import org.voltdb.calciteadapter.util.VoltDBRexUtil;
import org.voltdb.planner.AccessPath;
import org.voltdb.types.SortDirectionType;

public class VoltDBPSortIndexScanRemoveRule extends RelOptRule {

    // TODO: why not split INSTANCE_1 and INSTANCE_2 to two rules?
    public static final VoltDBPSortIndexScanRemoveRule INSTANCE_1 =
            new VoltDBPSortIndexScanRemoveRule(
                    operand(VoltDBPSort.class,
                            operand(VoltDBPTableIndexScan.class, none())),
                    "VoltDBPSortIndexScanRemoveRule_1");

    public static final VoltDBPSortIndexScanRemoveRule INSTANCE_2 =
            new VoltDBPSortIndexScanRemoveRule(
                    operand(VoltDBPSort.class,
                            operand(VoltDBPCalc.class,
                                    operand(VoltDBPTableIndexScan.class, none()))),
                    "VoltDBPSortIndexScanRemoveRule_2");

    private VoltDBPSortIndexScanRemoveRule(RelOptRuleOperand operand, String desc) {
        super(operand, desc);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        VoltDBPSort sort = call.rel(0);
        RelCollation origSortCollation = sort.getCollation();

        RelCollation scanSortCollation = null;
        VoltDBPCalc calc = null;
        VoltDBPTableIndexScan scan = null;
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

        RexProgram program =  scan.getProgram();
        assert(program != null);

        RelCollation indexCollation = scan.getTraitSet().getTrait(RelCollationTraitDef.INSTANCE);
        SortDirectionType sortDirection =
                 VoltDBRexUtil.areCollationsCompartible(scanSortCollation, indexCollation);

        if (SortDirectionType.INVALID != sortDirection) {
            // Update scan's sort direction
            AccessPath accessPath = scan.getAccessPath();

            if (accessPath != null) {
                accessPath.setSortDirection(sortDirection);

                VoltDBPTableIndexScan newScan = new VoltDBPTableIndexScan(
                    scan.getCluster(),
                    // IndexScan already have a collation trait, so replace it
                    scan.getTraitSet().replace(scanSortCollation),
                    scan.getTable(),
                    scan.getVoltDBTable(),
                    scan.getProgram(),
                    scan.getIndex(),
                    accessPath,
                    scan.getOffsetRexNode(),
                    scan.getLimitRexNode(),
                    scan.getAggregateRelNode(),
                    scan.getPreAggregateRowType(),
                    scan.getPreAggregateProgram(),
                    scan.getSplitCount());

                RelNode result = null;
                if (calc == null) {
                    result = newScan;
                } else {
                    // The new Calc collation must match the original Sort collation
                    result = calc.copy(
                            calc.getTraitSet().replace(origSortCollation),
                            newScan,
                            calc.getProgram(),
                            calc.getSplitCount());
                }

                call.transformTo(result);
            }
        }
    }

}
