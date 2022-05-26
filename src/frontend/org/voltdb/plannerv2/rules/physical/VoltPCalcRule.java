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
import org.apache.calcite.rel.RelNode;
import org.voltdb.plannerv2.rel.logical.VoltLogicalCalc;
import org.voltdb.plannerv2.rel.logical.VoltLogicalRel;
import org.voltdb.plannerv2.rel.physical.VoltPhysicalCalc;
import org.voltdb.plannerv2.rel.physical.VoltPhysicalRel;

/**
 * VoltDB physical rule that transform {@link VoltLogicalCalc} to {@link VoltPhysicalCalc}.
 *
 * @author Michael Alexeev
 * @since 9.0
 */
public class VoltPCalcRule extends RelOptRule {

    public static final VoltPCalcRule INSTANCE = new VoltPCalcRule();

    VoltPCalcRule() {
        super(operand(VoltLogicalCalc.class, VoltLogicalRel.CONVENTION, any()));
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        final VoltLogicalCalc calc = call.rel(0);
        final RelNode input = calc.getInput();
        final RelNode convertedInput = convert(input,
                input.getTraitSet().replace(VoltPhysicalRel.CONVENTION).simplify());
        call.transformTo(new VoltPhysicalCalc(
                calc.getCluster(), calc.getTraitSet().replace(VoltPhysicalRel.CONVENTION),
                convertedInput, calc.getProgram()));
    }
}
