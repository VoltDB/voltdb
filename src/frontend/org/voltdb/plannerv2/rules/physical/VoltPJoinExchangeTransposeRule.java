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

package org.voltdb.plannerv2.rules.physical;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperandChildPolicy;
import org.apache.calcite.plan.RelOptRuleOperandChildren;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.RelNode;

import com.google.common.collect.ImmutableList;
import org.voltdb.plannerv2.rel.physical.VoltPhysicalExchange;
import org.voltdb.plannerv2.rel.physical.VoltPhysicalJoin;
import org.voltdb.plannerv2.rel.physical.VoltPhysicalSingletonExchange;

/**
 * Transform Join / Exchange rels into Singleton Exchange / Join
 */
public class VoltPJoinExchangeTransposeRule extends RelOptRule {

    public static final RelOptRule INSTANCE = new VoltPJoinExchangeTransposeRule();

    private VoltPJoinExchangeTransposeRule() {
        super(operand(VoltPhysicalJoin.class, RelDistributions.ANY,
                new RelOptRuleOperandChildren(RelOptRuleOperandChildPolicy.SOME,
                        ImmutableList.of(
                                operand(VoltPhysicalSingletonExchange.class, any()),
                                operand(VoltPhysicalSingletonExchange.class, any())))));
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        VoltPhysicalJoin joinRel = call.rel(0);
        final VoltPhysicalExchange outerSingletonExchangeRel = call.rel(1);
        final VoltPhysicalExchange innerSingletonExchangeRel = call.rel(2);

        RelTraitSet joinTraits = joinRel.getTraitSet();

        final RelNode newJoinRel = joinRel.copy(
                joinTraits, joinRel.getCondition(), outerSingletonExchangeRel.getInput(),
                innerSingletonExchangeRel.getInput(), joinRel.getJoinType(), joinRel.isSemiJoinDone());
        call.transformTo(new VoltPhysicalSingletonExchange(
                outerSingletonExchangeRel.getCluster(),
                outerSingletonExchangeRel.getTraitSet(),
                newJoinRel));
    }
}
