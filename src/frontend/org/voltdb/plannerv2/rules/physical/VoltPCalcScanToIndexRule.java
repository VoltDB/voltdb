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
import java.util.Optional;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelDistributionTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.rex.RexProgramBuilder;
import org.apache.calcite.util.Pair;
import org.json_voltpatches.JSONException;
import org.voltdb.catalog.Index;
import org.voltdb.catalog.Table;
import org.voltdb.planner.AccessPath;
import org.voltdb.plannerv2.VoltTable;
import org.voltdb.plannerv2.rel.AbstractVoltTableScan;
import org.voltdb.plannerv2.rel.physical.VoltPhysicalCalc;
import org.voltdb.plannerv2.rel.physical.VoltPhysicalTableIndexScan;
import org.voltdb.plannerv2.rel.physical.VoltPhysicalTableScan;
import org.voltdb.plannerv2.rel.physical.VoltPhysicalTableSequentialScan;
import org.voltdb.plannerv2.utils.IndexUtil;
import org.voltdb.plannerv2.utils.VoltRexUtil;
import org.voltdb.types.SortDirectionType;

import com.google_voltpatches.common.base.Preconditions;

public class VoltPCalcScanToIndexRule extends RelOptRule {

    public static final VoltPCalcScanToIndexRule INSTANCE = new VoltPCalcScanToIndexRule();

    private VoltPCalcScanToIndexRule() {
        super(operand(VoltPhysicalCalc.class, operand(VoltPhysicalTableSequentialScan.class, none())));
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        final AbstractVoltTableScan scan = call.rel(1);
        final VoltTable table = scan.getVoltTable();
        Preconditions.checkNotNull(table);
        return ! table.getCatalogTable().getIndexes().isEmpty();
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        final VoltPhysicalCalc calc = call.rel(0);
        final VoltPhysicalTableScan scan = call.rel(1);

        final RexProgram calcProgram = calc.getProgram();

        final RexProgram scanProgram = scan.getProgram();
        Preconditions.checkNotNull(scanProgram);

        // Merge two programs
        final RexBuilder rexBuilder = calc.getCluster().getRexBuilder();
        final RexProgram mergedProgram = RexProgramBuilder.mergePrograms(calcProgram, scanProgram, rexBuilder);

        final Table catTable = scan.getVoltTable().getCatalogTable();

        final RexLocalRef filterCondition = calc.getProgram().getCondition();
        final Map<RelNode, RelNode> equiv = new HashMap<>();

        for (Index index : catTable.getIndexes()) {
            final Optional<Pair<AccessPath, RexNode>> accsessPathDataOpt =
                    IndexUtil.getCalciteRelevantAccessPathForIndex(
                            rexBuilder, catTable, filterCondition, mergedProgram, index,
                            SortDirectionType.INVALID, -1, false, true);

            accsessPathDataOpt.ifPresent(accessPathData -> {
                // if accessPath.other is not null, need to create a new Filter
                // @TODO Adjust Calc program Condition based on the access path "other" filters
                RelCollation indexCollation;
                try {
                    indexCollation = VoltRexUtil.createIndexCollation(index, catTable, rexBuilder, mergedProgram);
                } catch (JSONException e) {
                    indexCollation = null;
                }
                // Make sure we preserve Calc's distribution including partitioning value
                // within a new index scan
                RelTraitSet scanTraits = scan.getTraitSet().replace(
                        calc.getTraitSet().getTrait(RelDistributionTraitDef.INSTANCE));
                Preconditions.checkNotNull(indexCollation);
                final RelNode nextIndexScan = new VoltPhysicalTableIndexScan(
                        scan.getCluster(), scanTraits, scan.getTable(), scan.getVoltTable(),
                        mergedProgram, index, accessPathData.left, scan.getLimitRexNode(), scan.getOffsetRexNode(),
                        scan.getAggregateRelNode(), scan.getPreAggregateRowType(), scan.getPreAggregateProgram(),
                        indexCollation, false);
                equiv.put(nextIndexScan, calc);
            });
        }
        equiv.keySet().stream().findFirst().ifPresent(
                indexScan -> {
                    equiv.remove(indexScan);
                    call.transformTo(indexScan, equiv);
                });
    }
}
