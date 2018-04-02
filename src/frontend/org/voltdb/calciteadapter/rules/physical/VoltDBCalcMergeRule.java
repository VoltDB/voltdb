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

package org.voltdb.calciteadapter.rules.physical;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.rex.RexProgramBuilder;
import org.voltdb.calciteadapter.rel.physical.VoltDBCalc;

/**
 * This simply merges two VoltDBCalc nodes into a single VoltDBCalc. Adopted
 * from the CalcMergeRUle
 *
 */
public class VoltDBCalcMergeRule extends RelOptRule {

    public static final VoltDBCalcMergeRule INSTANCE = new VoltDBCalcMergeRule();

    private VoltDBCalcMergeRule() {
        super(operand(VoltDBCalc.class, operand(VoltDBCalc.class, any())));
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        final VoltDBCalc topCalc = call.rel(0);
        final VoltDBCalc bottomCalc = call.rel(1);

        // Don't merge a calc which contains windowed aggregates onto a
        // calc. That would effectively be pushing a windowed aggregate down
        // through a filter.
        RexProgram topProgram = topCalc.getProgram();
        if (RexOver.containsOver(topProgram)) {
            return;
        }

        // Merge the programs together.

        RexProgram mergedProgram = RexProgramBuilder.mergePrograms(
                topCalc.getProgram(), bottomCalc.getProgram(),
                topCalc.getCluster().getRexBuilder());
        assert mergedProgram.getOutputRowType() == topProgram
                .getOutputRowType();
        final Calc newCalc = topCalc.copy(topCalc.getTraitSet(),
                bottomCalc.getInput(), mergedProgram);

        if (newCalc.getDigest().equals(bottomCalc.getDigest())) {
            // newCalc is equivalent to bottomCalc, which means that topCalc
            // must be trivial. Take it out of the game.
            call.getPlanner().setImportance(topCalc, 0.0);
        }

        call.transformTo(newCalc);
    }
}
