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
import org.apache.calcite.rel.core.Filter;
import org.voltdb.calciteadapter.rel.LogicalSend;

public class VoltDBFilterSendTransposeRule extends RelOptRule {

    public static final VoltDBFilterSendTransposeRule INSTANCE = new VoltDBFilterSendTransposeRule();

    private VoltDBFilterSendTransposeRule() {
        super(operand(Filter.class, operand(LogicalSend.class, none())));
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        Filter filter = call.rel(0);
        LogicalSend send = call.rel(1);

        // If filter has an equality expression on partition column send needs to be removed
        RelNode sendInput = send.getInput();
        RelNode newFilterRel = filter.copy(filter.getTraitSet(), sendInput, filter.getCondition());
        // Generate RowType
        newFilterRel.getRowType();
        RelNode newSend = send.copy(newFilterRel, send.getLevel() + 1);

        call.transformTo(newSend);
    }

}