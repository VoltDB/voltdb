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

package org.voltdb.plannerv2.rules.inlining;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.voltdb.plannerv2.rel.physical.VoltPhysicalCalc;
import org.voltdb.plannerv2.rel.physical.VoltPhysicalTableScan;

/**
 * This simply merges/inline Calc(project and/or Condition) into a Scan.
 * It does not attempt to convert a Sequential scan into a Index scan.
 * This should be done by the VoltDBCalcToIndexRule
 *
 * @author mikealexeev
 */
public class VoltPhysicalCalcScanMergeRule extends RelOptRule {

    public static final VoltPhysicalCalcScanMergeRule INSTANCE = new VoltPhysicalCalcScanMergeRule();

    private VoltPhysicalCalcScanMergeRule() {
        super(operand(VoltPhysicalCalc.class,
                operand(VoltPhysicalTableScan.class, none())));
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        VoltPhysicalCalc calc = call.rel(0);
        VoltPhysicalTableScan scan = call.rel(1);

        // Merge trait sets to make sure we are not loosing any required traits if any
        RelTraitSet mergedTraitSet = scan.getTraitSet().merge(calc.getTraitSet());
        RelNode newScan = scan.copyWithProgram(mergedTraitSet, calc.getProgram(), calc.getCluster().getRexBuilder());
        call.transformTo(newScan);
    }
}
