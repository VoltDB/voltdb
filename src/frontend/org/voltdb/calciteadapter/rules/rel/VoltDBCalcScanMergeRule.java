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

package org.voltdb.calciteadapter.rules.rel;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Calc;
import org.voltdb.calciteadapter.rel.AbstractVoltDBTableScan;

public class VoltDBCalcScanMergeRule extends RelOptRule {

    public static final VoltDBCalcScanMergeRule INSTANCE = new VoltDBCalcScanMergeRule();

    private VoltDBCalcScanMergeRule() {
        super(operand(Calc.class, operand(AbstractVoltDBTableScan.class, none())));
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        Calc calc = call.rel(0);
        AbstractVoltDBTableScan scan = call.rel(1);
        RelNode newScan = AbstractVoltDBTableScan.copy(scan, calc.getProgram(), calc.getCluster().getRexBuilder());
        call.getPlanner().setImportance(calc, 0.);
        call.transformTo(newScan);
    }
}