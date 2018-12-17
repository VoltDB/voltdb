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

package org.voltdb.plannerv2.rules.physical;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.voltdb.plannerv2.rel.logical.VoltDBLLimit;
import org.voltdb.plannerv2.rel.logical.VoltDBLRel;
import org.voltdb.plannerv2.rel.physical.VoltDBPLimit;
import org.voltdb.plannerv2.rel.physical.VoltDBPRel;
import org.voltdb.plannerv2.utils.VoltDBRelUtil;

/**
 * VoltDB physical rule that transform {@link VoltDBLLimit} to {@link VoltDBPLimit}.
 *
 * @author Michael Alexeev
 * @since 8.4
 */
public class VoltDBPLimitRule extends RelOptRule {

    public static final VoltDBPLimitRule INSTANCE = new VoltDBPLimitRule();

    VoltDBPLimitRule() {
        super(operand(VoltDBLLimit.class,
                VoltDBLRel.VOLTDB_LOGICAL, any()));
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        VoltDBLLimit limitOffset = call.rel(0);
        RelNode input = limitOffset.getInput();
        RelTraitSet convertedTraits = limitOffset.getTraitSet()
                .replace(VoltDBPRel.VOLTDB_PHYSICAL).simplify();
        RelNode convertedInput = convert(input,
                input.getTraitSet().replace(VoltDBPRel.VOLTDB_PHYSICAL).simplify());
        int splitCount = VoltDBRelUtil.decideSplitCount(convertedInput);

        call.transformTo(new VoltDBPLimit(
                limitOffset.getCluster(),
                convertedTraits,
                convertedInput,
                limitOffset.getOffset(),
                limitOffset.getLimit(),
                splitCount));
    }
}
