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

import java.util.List;
import java.util.stream.Collectors;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelDistribution.Type;
import org.apache.calcite.rel.RelDistributionTraitDef;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.SetOp;
import org.voltdb.plannerv2.guards.PlannerFallbackException;

/**
 * Rules that fallback a query with SetOp operator if it is multi-partitioned.
 *
 * @author Mike Alexeev
 * @since 9.0
 */
public class MPSetOpsQueryFallBackRule extends RelOptRule {

    public static final MPSetOpsQueryFallBackRule INSTANCE =
            new MPSetOpsQueryFallBackRule(
                    operand(SetOp.class, RelDistributions.ANY,
                            some(operand(RelNode.class, any()),
                                    operand(RelNode.class, any()))), "MPSetOpsQueryFallBackRule");

    private MPSetOpsQueryFallBackRule(RelOptRuleOperand operand, String desc) {
        super(operand, desc);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        SetOp setOp = call.rel(0);
        List<RelDistribution> distributions =
                setOp.getInputs()
                     .stream()
                     .map(node -> node.getTraitSet().getTrait(RelDistributionTraitDef.INSTANCE))
                     .collect(Collectors.toList());

        // @TODO For now allow no more than one HASH distribution. Everything else - reject
        List<RelDistribution> hashDistributions =
                distributions.stream()
                             .filter(distribution -> Type.HASH_DISTRIBUTED == distribution.getType())
                             .collect(Collectors.toList());

        if (hashDistributions.size() > 1) {
            throw new PlannerFallbackException("MP query not supported in Calcite planner.");
        }

        RelDistribution newDistribution = (hashDistributions.isEmpty()) ?
                RelDistributions.SINGLETON : hashDistributions.get(0);
        call.transformTo(setOp.copy(setOp.getTraitSet().replace(newDistribution), setOp.getInputs()));

    }
}
