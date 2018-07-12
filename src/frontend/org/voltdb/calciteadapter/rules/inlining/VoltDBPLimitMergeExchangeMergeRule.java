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

package org.voltdb.calciteadapter.rules.inlining;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.rel.RelNode;
import org.voltdb.calciteadapter.rel.physical.VoltDBPCalc;
import org.voltdb.calciteadapter.rel.physical.VoltDBPLimit;
import org.voltdb.calciteadapter.rel.physical.VoltDBPMergeExchange;

public class VoltDBPLimitMergeExchangeMergeRule extends RelOptRule {

    public static final VoltDBPLimitMergeExchangeMergeRule INSTANCE_1 =
            new VoltDBPLimitMergeExchangeMergeRule(
                    operand(VoltDBPLimit.class,
                            operand(VoltDBPMergeExchange.class, any()))
                    );

    public static final VoltDBPLimitMergeExchangeMergeRule INSTANCE_2 =
            new VoltDBPLimitMergeExchangeMergeRule(
                    operand(VoltDBPLimit.class,
                            operand(VoltDBPCalc.class,
                                    operand(VoltDBPMergeExchange.class, any())))
                    );

    /**
     * Transform  VoltDBPLimit / VoltDBPMergeExchange to VoltDBPMergeExchange with Limit
     * Transform  VoltDBPLimit / VoltDBCalc / VoltDBPMergeExchange to
     *            VoltDBCalc / VoltDBPMergeExchange with Limit
     */
    private VoltDBPLimitMergeExchangeMergeRule(RelOptRuleOperand operand) {
        super(operand);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        VoltDBPLimit limitOffset = call.rel(0);
        int exchangeIdx = call.rels.length - 1;
        VoltDBPMergeExchange mergeExchange = call.rel(exchangeIdx);

        VoltDBPMergeExchange newExchange = (VoltDBPMergeExchange) mergeExchange.copy(
                mergeExchange.getTraitSet(),
                mergeExchange.getInput(),
                mergeExchange.getDistribution());
        newExchange.setOffset(limitOffset.getOffset());
        newExchange.setLimit(limitOffset.getLimit());

        RelNode newRel;
        if (call.rels.length == 3) {
            VoltDBPCalc calc = call.rel(1);
            newRel = calc.copy(calc.getTraitSet(), newExchange, calc.getProgram());
        } else {
            newRel = newExchange;
        }
        call.transformTo(newRel);
    }

}
