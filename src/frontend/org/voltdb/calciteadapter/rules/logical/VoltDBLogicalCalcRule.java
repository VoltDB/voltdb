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

package org.voltdb.calciteadapter.rules.logical;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalCalc;
import org.voltdb.calciteadapter.rel.logical.VoltDBLogicalCalc;
import org.voltdb.calciteadapter.rel.logical.VoltDBLogicalRel;

public class VoltDBLogicalCalcRule extends RelOptRule {

        public static final VoltDBLogicalCalcRule INSTANCE = new VoltDBLogicalCalcRule();

        VoltDBLogicalCalcRule() {
            super(operand(LogicalCalc.class, Convention.NONE, any()));
        }

        @Override
        public void onMatch(RelOptRuleCall call) {
            LogicalCalc calc = (LogicalCalc) call.rel(0);
            RelNode input = calc.getInput();
            RelTraitSet convertedTraits = calc.getTraitSet().replace(VoltDBLogicalRel.VOLTDB_LOGICAL);
            RelNode convertedInput = convert(input, input.getTraitSet().replace(VoltDBLogicalRel.VOLTDB_LOGICAL));
            call.transformTo(new VoltDBLogicalCalc(
                    calc.getCluster(),
                    convertedTraits,
                    convertedInput,
                    calc.getProgram()));
        }
}
