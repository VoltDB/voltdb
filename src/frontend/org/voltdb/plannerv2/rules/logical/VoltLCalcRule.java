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

package org.voltdb.plannerv2.rules.logical;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalCalc;
import org.voltdb.plannerv2.rel.logical.VoltLogicalCalc;
import org.voltdb.plannerv2.rel.logical.VoltLogicalRel;

public class VoltLCalcRule extends RelOptRule {
    public static final RelOptRule INSTANCE = new VoltLCalcRule();

    private VoltLCalcRule() {
        super(operand(LogicalCalc.class, Convention.NONE, any()));
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        final LogicalCalc calc = call.rel(0);
        final RelNode input = calc.getInput();
        call.transformTo(new VoltLogicalCalc(
                calc.getCluster(), calc.getTraitSet().replace(VoltLogicalRel.CONVENTION),
                convert(input, input.getTraitSet().replace(VoltLogicalRel.CONVENTION)),
                calc.getProgram()));
    }
}
