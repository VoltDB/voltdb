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

package org.voltdb.plannerv2.rules.physical;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexProgram;
import org.voltdb.planner.AccessPath;
import org.voltdb.plannerv2.rel.physical.VoltPhysicalCalc;
import org.voltdb.plannerv2.rel.physical.VoltPhysicalSort;
import org.voltdb.plannerv2.rel.physical.VoltPhysicalTableIndexScan;
import org.voltdb.plannerv2.utils.VoltRelUtil;
import org.voltdb.plannerv2.utils.VoltRexUtil;
import org.voltdb.types.SortDirectionType;

public class VoltPSortIndexScanRemoveRule extends RelOptRule {

    public static final VoltPSortIndexScanRemoveRule INSTANCE_SORT_INDEXSCAN =
            new VoltPSortIndexScanRemoveRule(operand(VoltPhysicalSort.class,
                    operand(VoltPhysicalTableIndexScan.class, none())),
                    "VoltDBPSortIndexScanRemoveRule_SortIscan");

    public static final VoltPSortIndexScanRemoveRule INSTANCE_SORT_CALC_INDEXSCAN =
            new VoltPSortIndexScanRemoveRule(operand(VoltPhysicalSort.class,
                    operand(VoltPhysicalCalc.class,
                            operand(VoltPhysicalTableIndexScan.class, none()))),
                    "VoltDBPSortIndexScanRemoveRule_SortCalcIscan");

    private VoltPSortIndexScanRemoveRule(RelOptRuleOperand operand, String desc) {
        super(operand, desc);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        final VoltPhysicalSort sort = call.rel(0);
        final RelCollation origSortCollation = sort.getCollation();

        final RelCollation scanSortCollation;
        final VoltPhysicalCalc calc;
        final VoltPhysicalTableIndexScan scan;
        final RexProgram calcScanProgram;
        if (call.rels.length == 2) {
            calc = null;
            scan = call.rel(1);
            scanSortCollation = origSortCollation;
            calcScanProgram = scan.getProgram();
        } else {
            calc = call.rel(1);
            scanSortCollation = VoltRelUtil.sortCollationCalcTranspose(origSortCollation, calc);
            if (RelCollations.EMPTY.equals(scanSortCollation)) {
                return;
            }
            scan = call.rel(2);
            calcScanProgram = VoltRexUtil.mergeProgram(scan.getProgram(),
                    calc.getProgram(), scan.getCluster().getRexBuilder());
        }

        final RelCollation indexCollation = scan.getIndexCollation();
        final SortDirectionType sortDirection =
                VoltRexUtil.areCollationsCompatible(scanSortCollation, indexCollation, calcScanProgram);

        if (SortDirectionType.INVALID != sortDirection) {
            // Update scan's sort direction
            final AccessPath accessPath = scan.getAccessPath();
            if (accessPath != null) {
                accessPath.setSortDirection(sortDirection);

                final RelNode newScan = new VoltPhysicalTableIndexScan(
                        scan.getCluster(),
                        // Need to preserve the sort's collation
                        scan.getTraitSet().replace(scanSortCollation),
                        scan.getTable(), scan.getVoltTable(), scan.getProgram(), scan.getIndex(), accessPath,
                        scan.getOffsetRexNode(), scan.getLimitRexNode(), scan.getAggregateRelNode(),
                        scan.getPreAggregateRowType(), scan.getPreAggregateProgram(),
                        indexCollation, scan.isInlinedInnerScan());
                final RelNode result;
                if (calc == null) {
                    result = newScan;
                } else { // The new Calc collation must match the original Sort collation
                    result = calc.copy(calc.getTraitSet().replace(origSortCollation), newScan,
                            calc.getProgram());
                }
                call.transformTo(result);
            }
        }
    }

}
