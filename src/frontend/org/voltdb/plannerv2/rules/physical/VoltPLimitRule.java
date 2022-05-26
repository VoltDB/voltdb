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
import org.voltdb.plannerv2.rel.logical.VoltLogicalLimit;
import org.voltdb.plannerv2.rel.logical.VoltLogicalRel;
import org.voltdb.plannerv2.rel.physical.VoltPhysicalLimit;
import org.voltdb.plannerv2.rel.physical.VoltPhysicalRel;

/**
 * VoltDB physical rule that transform {@link VoltLogicalLimit} to {@link VoltPhysicalLimit}.
 *
 * @author Michael Alexeev
 * @since 9.0
 */
public class VoltPLimitRule extends RelOptRule {

    public static final VoltPLimitRule INSTANCE = new VoltPLimitRule();

    VoltPLimitRule() {
        super(operand(VoltLogicalLimit.class,
                VoltLogicalRel.CONVENTION, any()));
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        VoltLogicalLimit limitOffset = call.rel(0);
        RelNode input = limitOffset.getInput();
        RelTraitSet convertedTraits = limitOffset.getTraitSet()
                .replace(VoltPhysicalRel.CONVENTION).simplify();
        RelNode convertedInput = convert(input,
                input.getTraitSet().replace(VoltPhysicalRel.CONVENTION).simplify());

        call.transformTo(new VoltPhysicalLimit(
                limitOffset.getCluster(),
                convertedTraits,
                convertedInput,
                limitOffset.getOffset(),
                limitOffset.getLimit(),
                false));
    }
}
