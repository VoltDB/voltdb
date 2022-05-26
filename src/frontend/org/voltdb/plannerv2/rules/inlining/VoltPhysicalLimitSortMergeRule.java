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
import org.voltdb.plannerv2.rel.physical.VoltPhysicalCalc;
import org.voltdb.plannerv2.rel.physical.VoltPhysicalLimit;
import org.voltdb.plannerv2.rel.physical.VoltPhysicalSort;

/**
 * The rule that Transform  VoltDBPLimit / VoltDBPSort to VoltDBPSort with Limit.
 *
 * @author mikealexeev
 */
public class VoltPhysicalLimitSortMergeRule extends RelOptRule {

    public static final VoltPhysicalLimitSortMergeRule INSTANCE_1 =
            new VoltPhysicalLimitSortMergeRule(
                    operand(VoltPhysicalLimit.class,
                            operand(VoltPhysicalSort.class, any()))
                    );

    public static final VoltPhysicalLimitSortMergeRule INSTANCE_2 =
            new VoltPhysicalLimitSortMergeRule(
                    operand(VoltPhysicalLimit.class,
                            operand(VoltPhysicalCalc.class,
                                    operand(VoltPhysicalSort.class, any())))
                    );

    /**
     * Transform  VoltDBPLimit / VoltDBPSort to VoltDBPSort with Limit
     * Transform  VoltDBPLimit / VoltDBCalc / VoltDBPSort to
     *            VoltDBCalc / VoltDBPSort with Limit
     */
    private VoltPhysicalLimitSortMergeRule(RelOptRuleOperand operand) {
        super(operand);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        VoltPhysicalLimit limitOffset = call.rel(0);
        int sortIdx = call.rels.length - 1;
        VoltPhysicalSort sort = call.rel(sortIdx);

        RelNode newSort = sort.copy(
                sort.getTraitSet(),
                sort.getInput(),
                sort.getCollation(),
                limitOffset.getOffset(),
                limitOffset.getLimit());
        RelNode newRel;
        if (call.rels.length == 3) {
            VoltPhysicalCalc calc = call.rel(1);
            newRel = calc.copy(calc.getTraitSet(), newSort, calc.getProgram());
        } else {
            newRel = newSort;
        }

        call.transformTo(newRel);
    }
}
