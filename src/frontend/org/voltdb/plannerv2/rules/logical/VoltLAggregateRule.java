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
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.voltdb.plannerv2.rel.logical.VoltLogicalAggregate;
import org.voltdb.plannerv2.rel.logical.VoltLogicalRel;

/**
 * VoltDB logical rule that transform {@link LogicalAggregate} to {@link VoltLogicalAggregate}.
 *
 * @author Michael Alexeev
 * @since 9.0
 */
public class VoltLAggregateRule extends RelOptRule {

    public static final VoltLAggregateRule INSTANCE = new VoltLAggregateRule();

    VoltLAggregateRule() {
        super(operand(LogicalAggregate.class, Convention.NONE, any()));
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        LogicalAggregate aggr = call.rel(0);
        RelNode input = aggr.getInput();
        RelTraitSet convertedTraits = aggr.getTraitSet().replace(VoltLogicalRel.CONVENTION);
        RelNode convertedInput = convert(input, input.getTraitSet().replace(VoltLogicalRel.CONVENTION));
        call.transformTo(new VoltLogicalAggregate(aggr.getCluster(), convertedTraits,
                convertedInput, aggr.getGroupSet(), aggr.getGroupSets(), aggr.getAggCallList()));
    }
}
