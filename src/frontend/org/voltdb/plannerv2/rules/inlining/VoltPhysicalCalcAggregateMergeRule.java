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
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.rex.RexProgramBuilder;
import org.voltdb.plannerv2.rel.physical.VoltPhysicalAggregate;
import org.voltdb.plannerv2.rel.physical.VoltPhysicalCalc;

/**
 * This simply merges/inlines Calc's condition into an Aggregate node.
 * In the VoltDB land this condition represents aggregate's HAVING clause
 *
 * @author mikealexeev
 */
public class VoltPhysicalCalcAggregateMergeRule extends RelOptRule {

    public static final VoltPhysicalCalcAggregateMergeRule INSTANCE = new VoltPhysicalCalcAggregateMergeRule();

    private VoltPhysicalCalcAggregateMergeRule() {
        super(operand(VoltPhysicalCalc.class,
                operand(VoltPhysicalAggregate.class, any())));
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        VoltPhysicalCalc calc = call.rel(0);
        // We only can merge a Calc's condition
        return calc.getProgram().getCondition() != null;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        VoltPhysicalCalc calc = call.rel(0);
        VoltPhysicalAggregate aggregate = call.rel(1);

        RexProgram program = calc.getProgram();
        RexLocalRef conditionRefExpr = program.getCondition();
        RexNode conditionExpr = program.expandLocalRef(conditionRefExpr);

        VoltPhysicalAggregate newAggregate = aggregate.copy(
                aggregate.getCluster(),
                aggregate.getTraitSet(),
                aggregate.getInput(),
                aggregate.indicator,
                aggregate.getGroupSet(),
                aggregate.getGroupSets(),
                aggregate.getAggCallList(),
                conditionExpr,
                aggregate.getIsCoordinatorAggr());

        RexProgramBuilder programBuilder = RexProgramBuilder
                .forProgram(program, calc.getCluster().getRexBuilder(), false);
        // remove condition from the Calc. It is moving to the new VoltDBCalc
        programBuilder.clearCondition();
        RexProgram newProgram = programBuilder.getProgram();

        VoltPhysicalCalc newCalc = new VoltPhysicalCalc(
                calc.getCluster(),
                calc.getTraitSet(),
                newAggregate,
                newProgram);

        call.transformTo(newCalc);
    }
}
