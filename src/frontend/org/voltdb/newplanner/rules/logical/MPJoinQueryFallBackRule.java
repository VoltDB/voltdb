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

package org.voltdb.newplanner.rules.logical;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelDistributionTraitDef;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.RelNode;
import org.voltdb.calciteadapter.rel.logical.VoltDBLJoin;
import org.voltdb.calciteadapter.rel.logical.VoltDBLTableScan;

/**
 * Rules that fallback a query with Join operator if it is multi-partitioned.
 *
 * @author Chao Zhou
 * @since 8.4
 */
public class MPJoinQueryFallBackRule extends RelOptRule {
    public static final MPJoinQueryFallBackRule INSTANCE =
            new MPJoinQueryFallBackRule(
                    operand(VoltDBLJoin.class,
                            operand(RelNode.class, any()),
                            operand(RelNode.class, any())), "MPJoinQueryFallBackRule");

    private MPJoinQueryFallBackRule(RelOptRuleOperand operand, String desc) {
        super(operand, desc);
    }

    private RelDistribution getDistribution(RelNode node) {
        if (node instanceof VoltDBLTableScan) {
            return node.getTable().getDistribution();
        }
        return node.getTraitSet().getTrait(RelDistributionTraitDef.INSTANCE);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        VoltDBLJoin join = call.rel(0);
        RelDistribution leftDist = getDistribution(call.rel(1));
        RelDistribution rightDist = getDistribution(call.rel(2));

        if ((call.rel(1) instanceof VoltDBLTableScan && leftDist != RelDistributions.SINGLETON) ||
                (call.rel(2) instanceof VoltDBLTableScan && rightDist != RelDistributions.SINGLETON)) {
            // partitioned table without filter, throw
            throw new UnsupportedOperationException("MP query not supported in Calcite planner.");
        }

        if (leftDist != RelDistributions.SINGLETON && rightDist != RelDistributions.SINGLETON) {
            throw new UnsupportedOperationException("MP query not supported in Calcite planner.");
        }

        if (leftDist == RelDistributions.SINGLETON && rightDist == RelDistributions.SINGLETON) {
            call.transformTo(join.copy(join.getTraitSet().replace(RelDistributions.SINGLETON), join.getInputs()));
        } else if (leftDist != RelDistributions.SINGLETON) {
            call.transformTo(join.copy(join.getTraitSet().replace(leftDist), join.getInputs()));
        } else {
            call.transformTo(join.copy(join.getTraitSet().replace(rightDist), join.getInputs()));
        }
    }
}
