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

import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.voltdb.plannerv2.rel.logical.VoltLogicalJoin;
import org.voltdb.plannerv2.rel.logical.VoltLogicalRel;
import org.voltdb.plannerv2.rel.physical.VoltPhysicalJoin;
import org.voltdb.plannerv2.rel.physical.VoltPhysicalNestLoopJoin;
import org.voltdb.plannerv2.rel.physical.VoltPhysicalRel;

/**
 * VoltDB physical rule that transform {@link VoltLogicalJoin} to {@link VoltPhysicalJoin}.
 *
 * @author Chao Zhou
 * @since 9.0
 */
public class VoltPJoinRule extends RelOptRule {

    public static final VoltPJoinRule INSTANCE = new VoltPJoinRule();

    VoltPJoinRule() {
        super(operand(VoltLogicalJoin.class, VoltLogicalRel.CONVENTION, any()));
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        final VoltLogicalJoin join = call.rel(0);
        final RelNode left = join.getLeft();
        final RelNode right = join.getRight();
        final RelTraitSet convertedTraits = join.getTraitSet().replace(VoltPhysicalRel.CONVENTION);
        final RelNode convertedLeft = convert(left, left.getTraitSet().replace(VoltPhysicalRel.CONVENTION));
        final RelNode convertedRight = convert(right, right.getTraitSet().replace(VoltPhysicalRel.CONVENTION));
        final ImmutableList<RelDataTypeField> systemFieldList = ImmutableList.copyOf(join.getSystemFieldList());

        // TODO: we may need to add splitCount attribute when we calculate/reduce the cost of a distribution node for MP query.
        call.transformTo(new VoltPhysicalNestLoopJoin(join.getCluster(), convertedTraits, convertedLeft, convertedRight,
                join.getCondition(), join.getVariablesSet(), join.getJoinType(),
                join.isSemiJoinDone(), systemFieldList, join.getWhereCondition()));
    }
}
