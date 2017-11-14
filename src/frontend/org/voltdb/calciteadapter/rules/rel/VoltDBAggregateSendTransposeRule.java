/* This file is part of VoltDB.
 * Copyright (C) 2008-2017 VoltDB Inc.
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

package org.voltdb.calciteadapter.rules.rel;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.voltdb.calciteadapter.VoltDBConvention;
import org.voltdb.calciteadapter.rel.VoltDBAggregate;
import org.voltdb.calciteadapter.rel.VoltDBSend;

/**
 * Transform Aggregate(fragment)/VoltDBSend expression to an equivalent one
 * Aggregate(coordinator)/VoltDBSend/Aggregate(fragment)
 *
 *
 */
public class VoltDBAggregateSendTransposeRule extends RelOptRule {

    public static final VoltDBAggregateSendTransposeRule INSTANCE = new VoltDBAggregateSendTransposeRule();

    private VoltDBAggregateSendTransposeRule() {
        // Match LogicalAggregate or VoltDBAggregate
        super(operand(Aggregate.class, operand(VoltDBSend.class, none())));
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        Aggregate aggregate = call.rel(0);
        // Only match lower fragment aggregator
        if (aggregate instanceof VoltDBAggregate &&
                ((VoltDBAggregate)aggregate).isCoordinatorPredicate()) {
            // This is already a coordinator aggregate
            return false;
        }
        // Only match aggregator that does not have VoltDBConvention yet
        if (aggregate.getConvention() instanceof VoltDBConvention) {
            return false;
        }
        return true;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        Aggregate aggregate = call.rel(0);
        VoltDBSend send = call.rel(1);

        RelNode sendInput = send.getInput();
        RelNode fragmentAggregate = VoltDBAggregate.createFrom(
                aggregate,
                sendInput,
                false);
        RelNode newSend = VoltDBSend.create(
                send.getCluster(),
                send.getTraitSet(),
                fragmentAggregate,
                send.getPartitioning(),
                send.getLevel() + 1);
        if (needCoordinatorAggregate(aggregate)) {
            newSend = VoltDBAggregate.createFrom(
                aggregate,
                newSend,
                true);
        }
        call.transformTo(newSend);
    }

    private boolean needCoordinatorAggregate(Aggregate aggregate) {
        return true;
    }
}