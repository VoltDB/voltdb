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
import org.voltdb.plannerv2.rel.logical.VoltLogicalCalc;
import org.voltdb.plannerv2.rel.logical.VoltLogicalTableScan;

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

        if (call.rel(0) instanceof VoltLogicalCalc && call.rel(1) instanceof VoltLogicalTableScan) {
            // If it is a VoltLogicalCalc / VoltLogicalTableScan pattern, check the filter and see if it can be run SP.
            // VoltLogicalTableScan gives the partitioning scheme information,
            // and the VoltLogicalCalc above it gives the information about the filters.
            VoltLogicalCalc calc = call.rel(0);
            VoltLogicalTableScan tableScan = call.rel(1);
            // TODO: the exception thrown here could be too early, e.g. the result is used as a child of a join,
            // which will change MP decision to SP at that point.
            RelDistributionUtils.checkedFallBack(! RelDistributionUtils.isCalcScanSP(tableScan, calc));
        } else {
            // Otherwise, propagate the DistributionTrait bottom up.
            RelNode child = call.rel(1);
            RelDistribution childDist = child.getTraitSet().getTrait(RelDistributionTraitDef.INSTANCE);
            if (childDist != RelDistributions.ANY) {
                SingleRel node = call.rel(0);
                call.transformTo(node.copy(node.getTraitSet().replace(childDist), node.getInputs()));
            }
        }
    }

}

