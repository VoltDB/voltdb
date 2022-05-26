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

import java.util.function.Predicate;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.rex.RexProgramBuilder;
import org.voltdb.plannerv2.rel.logical.VoltLogicalCalc;
import org.voltdb.plannerv2.rel.logical.VoltLogicalJoin;
import org.voltdb.plannerv2.rel.logical.VoltLogicalRel;

import com.google.common.collect.ImmutableList;

/**
 * Volt logical rule to merge Calc (with WHERE condition)/ Join into
 * a Calc / Join (with WHERE condition)
 *
 */
public class VoltLCalcJoinMergeRule extends RelOptRule {

    /**
     * Predicate to match Calc with non-null condition
    */
    private static final Predicate<Calc> HAS_CONDITION_PREDICATE = calc -> calc.getProgram().getCondition() != null;

    /** Instance of the rule that merges a LogicacCalc with a condition with a LogicalJoin
     * The Calc's condition becomes Join's WHERE condition
     */
    public static final VoltLCalcJoinMergeRule INSTANCE = new VoltLCalcJoinMergeRule();

    private VoltLCalcJoinMergeRule() {
        super(operandJ(VoltLogicalCalc.class, null,
                HAS_CONDITION_PREDICATE,
                operand(VoltLogicalJoin.class, VoltLogicalRel.CONVENTION, any())),
                "VoltLCalcJoinMergeRule");
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        VoltLogicalCalc calc = call.rel(0);
        VoltLogicalJoin join = call.rel(1);

        // Get calc's condition
        RexProgram calcProgram = calc.getProgram();
        RexNode calcCondition = calcProgram.expandLocalRef(calcProgram.getCondition());
        // Build a new calc's program without a condition
        RexProgramBuilder programBuilder = RexProgramBuilder.forProgram(calcProgram, calc.getCluster().getRexBuilder(), true);
        programBuilder.clearCondition();
        RexProgram newProgram = programBuilder.getProgram();
        //New Join with WHERE condition
        VoltLogicalJoin newJoin = new VoltLogicalJoin(
                join.getCluster(),
                join.getTraitSet(),
                join.getLeft(),
                join.getRight(),
                join.getCondition(),
                join.getVariablesSet(),
                join.getJoinType(),
                join.isSemiJoinDone(),
                ImmutableList.copyOf(join.getSystemFieldList()),
                calcCondition);

        // Transform
        call.transformTo(new VoltLogicalCalc(
                calc.getCluster(),
                calc.getTraitSet(),
                newJoin,
                newProgram));
    }
}
