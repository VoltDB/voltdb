/* This file is part of VoltDB.
 * Copyright (C) 2008-2019 VoltDB Inc.
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

package org.voltdb.plannerv2.rules.logical;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelDistributionTraitDef;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rex.RexNode;
import org.voltcore.utils.Pair;

/**
 * Rule that fallback the processing of a multi-partition query without joins to
 * the legacy planner.
 *
 * @author Chao Zhou
 * @since 9.0
 */
public class MPQueryFallBackRule extends RelOptRule {

    public static final MPQueryFallBackRule INSTANCE = new MPQueryFallBackRule();

    private MPQueryFallBackRule() {
        super(operand(SingleRel.class, RelDistributions.ANY,
                some(operand(RelNode.class, any()))));
    }

    @Override public void onMatch(RelOptRuleCall call) {
        // NOTE:
        // This rule depends on the VoltLogicalCalc and the VoltLogicalTableScan nodes at the leaf level to
        // determine whether this query is SP or not first, then propagate this information upwards to the root.
        // Therefore, it can only be run using a Hep planner following the bottom-up order.
        // It will not work properly with Hep planners following other orders or Volcano planners.

        if (call.rel(0) instanceof Calc && call.rel(1) instanceof TableScan) {
            // If it is a VoltLogicalCalc / VoltLogicalTableScan pattern, check the filter and see if it can be run SP.
            // VoltLogicalTableScan gives the partitioning scheme information,
            // and the VoltLogicalCalc above it gives the information about the filters.
            final Calc calc = call.rel(0);
            final TableScan tableScan = call.rel(1);
            final Pair<Boolean, RexNode> r = RelDistributionUtils.isCalcScanSP(tableScan, calc);
            final RelDistribution tableDist = tableScan.getTable().getDistribution();
            call.transformTo(calc.copy(calc.getTraitSet().replace(tableDist.with(
                    tableDist.getType(),
                    RelDistributionUtils.adjustProjection(calc.getProgram(), tableDist.getKeys()),
                    r.getSecond(), r.getFirst())),
                    calc.getInputs()));
        } else {
            // Otherwise, propagate the DistributionTrait bottom up.
            final RelDistribution childDist =
                    call.rel(1).getTraitSet().getTrait(RelDistributionTraitDef.INSTANCE);
            if (childDist != RelDistributions.ANY) {
                final SingleRel node = call.rel(0);
                final RelDistribution dist;
                if (node instanceof Calc) {
                    dist = childDist.with(childDist.getType(),
                            RelDistributionUtils.adjustProjection(((Calc) node).getProgram(), childDist.getKeys()),
                            childDist.getPartitionEqualValue(),
                            childDist.getIsSP());
                } else {
                    dist = childDist;
                }
                call.transformTo(node.copy(node.getTraitSet().replace(dist), node.getInputs()));
            }
        }
    }
}

