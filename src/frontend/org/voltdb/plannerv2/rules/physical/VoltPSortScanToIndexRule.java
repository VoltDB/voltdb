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

import java.util.HashMap;
import java.util.Map;

import com.google_voltpatches.common.base.Preconditions;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexProgram;
import org.json_voltpatches.JSONException;
import org.voltdb.catalog.Index;
import org.voltdb.catalog.Table;
import org.voltdb.planner.AccessPath;
import org.voltdb.plannerv2.VoltTable;
import org.voltdb.plannerv2.rel.physical.*;
import org.voltdb.plannerv2.utils.VoltRexUtil;
import org.voltdb.plannerv2.utils.VoltRelUtil;
import org.voltdb.types.IndexLookupType;
import org.voltdb.types.SortDirectionType;

public class VoltPSortScanToIndexRule extends RelOptRule {

    public static final VoltPSortScanToIndexRule INSTANCE_SORT_SCAN =
            new VoltPSortScanToIndexRule(operand(VoltPhysicalSort.class,
                operand(VoltPhysicalTableSequentialScan.class, none())), "SortScanToIndexRule");

    public static final VoltPSortScanToIndexRule INSTANCE_SORT_CALC_SCAN =
            new VoltPSortScanToIndexRule(operand(VoltPhysicalSort.class,
                    operand(VoltPhysicalCalc.class, operand(VoltPhysicalTableSequentialScan.class, none()))),
                    "SortCalcScanToIndexRule");

    private VoltPSortScanToIndexRule(RelOptRuleOperand operand, String desc) {
        super(operand, desc);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        final VoltPhysicalTableScan scan = call.rels.length == 2 ? call.rel(1) : call.rel(2);
        final VoltTable table = scan.getVoltTable();
        Preconditions.checkNotNull(table);
        return !table.getCatalogTable().getIndexes().isEmpty();
    }


    @Override
    public void onMatch(RelOptRuleCall call) {
        final VoltPhysicalSort sort = call.rel(0);
        final RelCollation origSortCollation = sort.getCollation();
        Preconditions.checkState(! RelCollations.EMPTY.equals(origSortCollation));
        Preconditions.checkState(sort.fetch == null && sort.offset == null);

        final RelCollation scanSortCollation;
        final VoltPhysicalCalc calc;
        final VoltPhysicalTableScan scan;
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
        final Table catTable = scan.getVoltTable().getCatalogTable();

        final RexBuilder builder = scan.getCluster().getRexBuilder();
        final RexProgram program = scan.getProgram();
        Preconditions.checkNotNull(program);
        RelNode equivRel = null;
        final Map<RelNode, RelNode> equivMap = new HashMap<>();

        for (Index index : catTable.getIndexes()) {
            if (index.getPredicatejson().isEmpty()) { // TODO: a partial index is not considered
                RelCollation indexCollation = null;
                try {
                    indexCollation = VoltRexUtil.createIndexCollation(index, catTable, builder, program);
                } catch (JSONException ignored) { }
                Preconditions.checkNotNull(indexCollation);
                final SortDirectionType sortDirection = VoltRexUtil.areCollationsCompatible(
                        scanSortCollation, indexCollation, calcScanProgram);
                //@TODO Cutting corner here. Should probably use something similar to
                // {@code org.voltdb.planner.WindowFunctionScoreboard}
                if (SortDirectionType.INVALID != sortDirection) {
                    final AccessPath accessPath = new AccessPath(
                            index,
                            // With no index expression, the lookup type will be ignored and
                            // the sort direction will determine the scan direction;
                            IndexLookupType.EQ, sortDirection, true);
                    final RelNode indexScan = new VoltPhysicalTableIndexScan(
                            scan.getCluster(),
                            // Need to preserve sort collation trait
                            scan.getTraitSet().replace(scanSortCollation),
                            scan.getTable(), scan.getVoltTable(), scan.getProgram(),
                            index, accessPath, scan.getLimitRexNode(), scan.getOffsetRexNode(),
                            scan.getAggregateRelNode(), scan.getPreAggregateRowType(), scan.getPreAggregateProgram(),
                            indexCollation, false);

                    final RelNode result;
                    if (calc == null) {
                        result = indexScan;
                    } else { // The new Calc collation must match the original Sort collation
                        result = calc.copy(calc.getTraitSet().replace(origSortCollation),
                                indexScan, calc.getProgram());
                    }
                    if (equivRel == null) {
                        equivRel = result;
                    } else {
                        equivMap.put(result, sort);
                    }
                }
            }
        }
        if (equivRel != null) {
            call.transformTo(equivRel, equivMap);
        }
    }
}
