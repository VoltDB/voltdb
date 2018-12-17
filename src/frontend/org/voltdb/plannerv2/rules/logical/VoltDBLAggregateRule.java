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

package org.voltdb.plannerv2.rules.logical;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.voltdb.plannerv2.rel.logical.VoltDBLAggregate;
import org.voltdb.plannerv2.rel.logical.VoltDBLRel;

/**
 * VoltDB logical rule that transform {@link LogicalAggregate} to {@link VoltDBLAggregate}.
 *
 * @author Michael Alexeev
 * @since 8.4
 */
public class VoltDBLAggregateRule extends RelOptRule {

    public static final VoltDBLAggregateRule INSTANCE = new VoltDBLAggregateRule();

    VoltDBLAggregateRule() {
        super(operand(LogicalAggregate.class, Convention.NONE, any()));
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        LogicalAggregate aggr = call.rel(0);
        RelNode input = aggr.getInput();
        RelTraitSet convertedTraits = aggr.getTraitSet().replace(VoltDBLRel.VOLTDB_LOGICAL);
        RelNode convertedInput = convert(input, input.getTraitSet().replace(VoltDBLRel.VOLTDB_LOGICAL));
        call.transformTo(VoltDBLAggregate.create(
                aggr.getCluster(),
                convertedTraits,
                convertedInput,
                aggr.indicator,
                aggr.getGroupSet(),
                aggr.getGroupSets(),
                aggr.getAggCallList()));
    }
}
