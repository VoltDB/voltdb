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

package org.voltdb.plannerv2.rules.logical;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.voltdb.plannerv2.rel.logical.VoltLogicalJoin;
import org.voltdb.plannerv2.rel.logical.VoltLogicalRel;

/**
 * VoltDB logical rule that transform {@link LogicalJoin} to {@link VoltLogicalJoin}.
 *
 * @author Chao Zhou
 * @since 9.0
 */
public class VoltLJoinRule extends RelOptRule {
    public static final VoltLJoinRule INSTANCE = new VoltLJoinRule();

    VoltLJoinRule() {
        super(operand(LogicalJoin.class, Convention.NONE, any()));
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        LogicalJoin join = call.rel(0);
        RelNode left = join.getLeft();
        RelNode right = join.getRight();
        RelTraitSet convertedTraits = join.getTraitSet().replace(VoltLogicalRel.CONVENTION);
        RelNode convertedLeft = convert(left, left.getTraitSet().replace(VoltLogicalRel.CONVENTION));
        RelNode convertedRight = convert(right, right.getTraitSet().replace(VoltLogicalRel.CONVENTION));
        ImmutableList<RelDataTypeField> systemFieldList = ImmutableList.copyOf(join.getSystemFieldList());

        call.transformTo(new VoltLogicalJoin(join.getCluster(), convertedTraits, convertedLeft, convertedRight,
                join.getCondition(), join.getVariablesSet(), join.getJoinType(),
                join.isSemiJoinDone(), systemFieldList));
    }
}
