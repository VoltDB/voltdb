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

import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.voltdb.plannerv2.rel.logical.VoltDBLJoin;
import org.voltdb.plannerv2.rel.logical.VoltLogicalRel;
import org.voltdb.plannerv2.rel.physical.VoltDBPJoin;
import org.voltdb.plannerv2.rel.physical.VoltDBPRel;

/**
 * VoltDB physical rule that transform {@link VoltDBLJoin} to {@link VoltDBPJoin}.
 *
 * @author Chao Zhou
 * @since 8.4
 */
public class VoltPJoinRule extends RelOptRule {

    public static final VoltPJoinRule INSTANCE = new VoltPJoinRule();

    VoltPJoinRule() {
        super(operand(VoltDBLJoin.class, VoltLogicalRel.VOLTDB_LOGICAL, any()));
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        VoltDBLJoin join = call.rel(0);
        RelNode left = join.getLeft();
        RelNode right = join.getRight();
        RelTraitSet convertedTraits = join.getTraitSet().replace(VoltDBPRel.VOLTDB_PHYSICAL);
        RelNode convertedLeft = convert(left, left.getTraitSet().replace(VoltDBPRel.VOLTDB_PHYSICAL));
        RelNode convertedRight = convert(right, right.getTraitSet().replace(VoltDBPRel.VOLTDB_PHYSICAL));
        ImmutableList<RelDataTypeField> systemFieldList = ImmutableList.copyOf(join.getSystemFieldList());

        // TODO: How many number of concurrent processes that a Join will be executed in ?
        // I use 1 for now.
        call.transformTo(new VoltDBPJoin(join.getCluster(), convertedTraits, convertedLeft, convertedRight,
                join.getCondition(), join.getVariablesSet(), join.getJoinType(),
                join.isSemiJoinDone(), systemFieldList, Constants.JOIN_SPLIT_COUNT));
    }
}
