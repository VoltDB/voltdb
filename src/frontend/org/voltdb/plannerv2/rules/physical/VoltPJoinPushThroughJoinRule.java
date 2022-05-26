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

import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.rules.JoinPushThroughJoinRule;
import org.voltdb.plannerv2.rel.VoltPRelBuilder;
import org.voltdb.plannerv2.rel.physical.VoltPhysicalNestLoopJoin;

/**
 * Volt extension of the JoinPushThroughJoinRule
 * {@link org.apache.calcite.rel.rules.JoinPushThroughJoinRule}
 * that operates on VoltPhysicalNestLoopJoin relation
 */
public class VoltPJoinPushThroughJoinRule extends JoinPushThroughJoinRule {

    /** Instance of the rule that works on physical NJ joins only, and pushes to the
     * right. */
    public static final VoltPJoinPushThroughJoinRule RIGHT_JOIN_JOIN = new VoltPJoinPushThroughJoinRule(
            operand(VoltPhysicalNestLoopJoin.class,
                    operand(VoltPhysicalNestLoopJoin.class, any()),
                    operand(RelNode.class, any())),
            "VoltPJoinPushThroughJoinRule:right_join_join", true);

    /** Instance of the rule that works on physical NJ joins only, and pushes to the
     * left. */
    public static final VoltPJoinPushThroughJoinRule LEFT_JOIN_JOIN = new VoltPJoinPushThroughJoinRule(
            operand(VoltPhysicalNestLoopJoin.class,
                    operand(VoltPhysicalNestLoopJoin.class, any()),
                    operand(RelNode.class, any())),
            "VoltPJoinPushThroughJoinRule:left", false);

    private VoltPJoinPushThroughJoinRule(RelOptRuleOperand operand, String description, boolean right) {
        super(description, right, VoltPhysicalNestLoopJoin.class, VoltPRelBuilder.PHYSICAL_BUILDER);
    }
}
