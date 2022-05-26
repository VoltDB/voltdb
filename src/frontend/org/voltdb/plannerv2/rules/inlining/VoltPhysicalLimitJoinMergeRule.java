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
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Calc;
import org.voltdb.plannerv2.rel.physical.VoltPhysicalCalc;
import org.voltdb.plannerv2.rel.physical.VoltPhysicalJoin;
import org.voltdb.plannerv2.rel.physical.VoltPhysicalLimit;

public class VoltPhysicalLimitJoinMergeRule extends RelOptRule {

    public static final RelOptRule INSTANCE_LIMIT_JOIN =
            new VoltPhysicalLimitJoinMergeRule(operand(VoltPhysicalLimit.class,
                    operand(VoltPhysicalJoin.class, any())), "VoltPhysicalLimitJoinMergeRule:LIMIT_JOIN");
    public static final RelOptRule INSTANCE_LIMIT_CALC_JOIN =
            new VoltPhysicalLimitJoinMergeRule(operand(VoltPhysicalLimit.class,
                    operand(VoltPhysicalCalc.class, operand(VoltPhysicalJoin.class, any()))), "VoltPhysicalLimitJoinMergeRule:LIMIT_CALC_JOIN");

    /**
     * Transform  VoltDBPLimit / AbstractVoltDBPJoin into AbstractVoltDBPJoin with Limit
     * Transform  VoltDBPLimit / Calc / AbstractVoltDBPJoin to Calc / AbstractVoltDBPJoin with Limit
     */
    private VoltPhysicalLimitJoinMergeRule(RelOptRuleOperand operand, String desc) {
        super(operand, desc);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        final VoltPhysicalLimit limitOffset = call.rel(0);
        final VoltPhysicalJoin join = call.rel(call.rels.length - 1);
        RelNode newRel = join.copyWithLimitOffset(
                join.getTraitSet(),
                limitOffset.getOffset(),
                limitOffset.getLimit());
        if (call.rels.length == 3) {
            final Calc calc = call.rel(1);
            newRel = calc.copy(calc.getTraitSet(), newRel, calc.getProgram());
        }
        call.transformTo(newRel);
    }

}
