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
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.voltdb.plannerv2.rel.logical.VoltLogicalAggregate;
import org.voltdb.plannerv2.rel.logical.VoltLogicalRel;
import org.voltdb.plannerv2.rel.physical.VoltPhysicalHashAggregate;
import org.voltdb.plannerv2.rel.physical.VoltPhysicalRel;
import org.voltdb.plannerv2.rel.physical.VoltPhysicalSerialAggregate;


/**
 * VoltDB physical rule that transform {@link VoltLogicalAggregate} to {@link VoltPhysicalHashAggregate}
 *
 * @author Michael Alexeev
 * @since 9.0
 */
public class VoltPAggregateRule extends RelOptRule {

    public static final VoltPAggregateRule INSTANCE = new VoltPAggregateRule();

    private VoltPAggregateRule() {
        super(operand(VoltLogicalAggregate.class, VoltLogicalRel.CONVENTION, any()));
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        final VoltLogicalAggregate aggregate = call.rel(0);
        RelTraitSet convertedAggrTraits = aggregate.getTraitSet().replace(VoltPhysicalRel.CONVENTION).simplify();

        final RelNode input = aggregate.getInput();
        RelTraitSet convertedInputTraits = input.getTraitSet().replace(VoltPhysicalRel.CONVENTION).simplify();
        final RelNode convertedInput = convert(input, convertedInputTraits);

        if (aggregate.getGroupSet().isEmpty()) {
            // GROUP BY is empty - Serial Aggregate
            call.transformTo(new VoltPhysicalSerialAggregate(
                    aggregate.getCluster(), convertedAggrTraits, convertedInput, aggregate.indicator,
                    aggregate.getGroupSet(), aggregate.getGroupSets(), aggregate.getAggCallList(), null,
                    false));
        } else {
            // Transform to a physical Hash Aggregate.
            call.transformTo(new VoltPhysicalHashAggregate(
                    aggregate.getCluster(), convertedAggrTraits, convertedInput, aggregate.indicator,
                    aggregate.getGroupSet(), aggregate.getGroupSets(), aggregate.getAggCallList(), null,
                    false));
        }
    }

}
