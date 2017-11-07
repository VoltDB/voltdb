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
import org.voltdb.calciteadapter.rel.LogicalAggregateMerge;
import org.voltdb.calciteadapter.rel.LogicalSend;

public class VoltDBAggregateSendTransposeRule extends RelOptRule {

    public static final VoltDBAggregateSendTransposeRule INSTANCE = new VoltDBAggregateSendTransposeRule();

    private VoltDBAggregateSendTransposeRule() {
        // Match LogicalAggregate or LogicalAggregateMerge
        super(operand(Aggregate.class, operand(LogicalSend.class, none())));
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        Aggregate aggregate = call.rel(0);
        if (aggregate instanceof LogicalAggregateMerge &&
                ((LogicalAggregateMerge)aggregate).isCoordinatorPredicate()) {
            // This is already a coordinator aggregate
            return false;
        }
        return true;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        Aggregate aggregate = call.rel(0);
        LogicalSend send = call.rel(1);

        RelNode sendInput = send.getInput();
        RelNode fragmentAggregate = LogicalAggregateMerge.createFrom(
                aggregate,
                sendInput,
                false);
        RelNode newSend = send.copy(fragmentAggregate, send.getLevel());
        RelNode coordinatorAggregate = LogicalAggregateMerge.createFrom(
                aggregate,
                newSend,
                true);
        call.transformTo(coordinatorAggregate);
    }

}