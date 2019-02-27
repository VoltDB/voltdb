/* This file is part of VoltDB.
 * Copyright (C) 2008-2019 VoltDB Inc.
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

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelDistributionTraitDef;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.RelNode;
import org.voltdb.plannerv2.rel.logical.VoltLogicalJoin;

/**
 * Rules that fallback a query with Join operator if it is multi-partitioned.
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
        RelDistributionUtils.checkedFallBack(! joinState.isSP());
        // The query is SP, and the distributions of any partitioned tables had been set.
        final RelDistribution outerDist = RelDistributionUtils.getDistribution(outer),
                innerDist = RelDistributionUtils.getDistribution(inner),
                newDist = (outerDist == RelDistributions.SINGLETON || innerDist == RelDistributions.SINGLETON ?
                        RelDistributions.SINGLETON : innerDist)
                        .withPartitionEqualValue(joinState.getLiteral());
        call.transformTo(join.copy(join.getTraitSet().replace(newDist), join.getInputs()));
    }
}
