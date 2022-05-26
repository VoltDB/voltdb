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

import java.util.List;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.voltdb.plannerv2.rel.logical.VoltLogicalExchange;
import org.voltdb.plannerv2.rel.logical.VoltLogicalJoin;

import com.google.common.collect.Lists;

/**
 * Rules that fallback a query with Join operator if it is multi-partitioned.
 * Note that VoltLogicalJoin does not sub-class LogicalJoin
 *
 * @author Chao Zhou
 * @since 9.0
 */
public class MPJoinQueryFallBackRule extends RelOptRule {

    public static final MPJoinQueryFallBackRule INSTANCE =
            new MPJoinQueryFallBackRule(
                    operand(VoltLogicalJoin.class, RelDistributions.ANY,
                            some(operand(RelNode.class, any()),
                                    operand(RelNode.class, any()))),
                    "MPJoinQueryFallBackRule");

    private MPJoinQueryFallBackRule(RelOptRuleOperand operand, String desc) {
        super(operand, desc);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        final VoltLogicalJoin join = call.rel(0);
        final RelNode outer = call.rel(1), inner = call.rel(2);
        final RelDistributionUtils.JoinState joinState = RelDistributionUtils.isJoinSP(join, outer, inner);
        // The query is SP, and the distributions of any partitioned tables had been set.
        final RelDistribution outerDist = RelDistributionUtils.getDistribution(outer),
                innerDist = RelDistributionUtils.getDistribution(inner);
        final boolean isOuterPartitioned = outerDist.getType() == RelDistribution.Type.HASH_DISTRIBUTED;
        final boolean isInnerPartitioned = innerDist.getType() == RelDistribution.Type.HASH_DISTRIBUTED;
        final RelDistribution intermediate =
                isOuterPartitioned || isInnerPartitioned ? RelDistributions.hash(joinState.getPartCols()) : innerDist;
        final RelDistribution newDist;
        List<RelNode> newInputs;
        // If we have a LEFT / RIGHT / FULL join and its outer node is replicated while the other one is
        // partitioned we need to collect all the rows from the partitioned table prior joining them
        // with the replicated ones to guarantee the join correctness.
        // That means that we need to insert an Exchange node above the partitioned child and
        // make the join distribution itself to be a SINGLETON
        if ((JoinRelType.LEFT == join.getJoinType() || JoinRelType.FULL == join.getJoinType())
                && !isOuterPartitioned && isInnerPartitioned) {
            newDist = RelDistributions.SINGLETON.with(innerDist.getPartitionEqualValue(), false);
            VoltLogicalExchange innerExchange = new VoltLogicalExchange(inner.getCluster(),
                    inner.getTraitSet(), inner, innerDist);
            newInputs = Lists.newArrayList(outer, innerExchange);
        } else if((JoinRelType.RIGHT == join.getJoinType() || JoinRelType.FULL == join.getJoinType())
                && isOuterPartitioned && !isInnerPartitioned) {
            newDist = RelDistributions.SINGLETON.with(outerDist.getPartitionEqualValue(), false);
            VoltLogicalExchange outerExchange = new VoltLogicalExchange(outer.getCluster(),
                    outer.getTraitSet(), outer, outerDist);
            newInputs = Lists.newArrayList(outerExchange, inner);
        } else {
            newDist = intermediate
                    .with(joinState.getLiteral(), joinState.isSP());
            newInputs = join.getInputs();
        }
        call.transformTo(join.copy(join.getTraitSet().replace(newDist), newInputs));
    }
}
