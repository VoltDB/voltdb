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
import org.apache.calcite.plan.RelOptRuleOperand;
import org.voltdb.plannerv2.rel.physical.VoltPhysicalLimit;
import org.voltdb.plannerv2.rel.physical.VoltPhysicalSerialAggregate;

/**
 * The rule that Transform  VoltDBPLimit / VoltDBPSerialAggregate to VoltDBPSerialAggregate with Limit.
 *
 * @author mikealexeev
 */
public class VoltPhysicalLimitSerialAggregateMergeRule extends RelOptRule {

    public static final VoltPhysicalLimitSerialAggregateMergeRule INSTANCE =
            new VoltPhysicalLimitSerialAggregateMergeRule(
                    operand(VoltPhysicalLimit.class,
                            operand(VoltPhysicalSerialAggregate.class, any()))
            );

    /**
     * Transform  VoltDBPLimit / VoltDBPSerialAggregate to VoltDBPSerialAggregate with Limit
     */
    private VoltPhysicalLimitSerialAggregateMergeRule(RelOptRuleOperand operand) {
        super(operand);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        VoltPhysicalLimit limitOffset = call.rel(0);
        VoltPhysicalSerialAggregate aggregate = call.rel(1);

        VoltPhysicalSerialAggregate newAggregate = new VoltPhysicalSerialAggregate(
                aggregate.getCluster(),
                aggregate.getTraitSet(),
                aggregate.getInput(),
                aggregate.indicator,
                aggregate.getGroupSet(),
                aggregate.getGroupSets(),
                aggregate.getAggCallList(),
                aggregate.getPostPredicate(),
                aggregate.getIsCoordinatorAggr(),
                limitOffset.getOffset(),
                limitOffset.getLimit());

        call.transformTo(newAggregate);
    }
}
