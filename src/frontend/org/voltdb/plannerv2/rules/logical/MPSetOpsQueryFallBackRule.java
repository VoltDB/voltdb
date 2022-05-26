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
import java.util.stream.Collectors;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelDistributionTraitDef;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.SetOp;
import org.voltdb.plannerv2.rel.logical.VoltLogicalExchange;
import org.voltdb.plannerv2.rules.logical.RelDistributionUtils.JoinState;

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
        // Set Op is SP if all of its children are SP and their partitioning values are
        // either NULL (a child is a replicated scan) or equal each other
        // (implies that there is a "WHERE partitionColumn = LITERAL_VALUE" for each child)
        JoinState setOpState = RelDistributionUtils.isSetOpSP(setOp.getInputs());
        // At the moment Volt only supports SetOP involving either a single partitioned table
        // without a equality filter on its partitioning column or
        // multiple partitioned ones with matching partitioning filters.
        // In both cases, the SetOp itself should be part of a coordinator's fragment meaning
        // his distribution is a SINGLETON.
        // In case of a single partitioned table without a filter we need to and an Exchange node
        // above this table.
        // Perhaps there is room for improvements like pushing down Exchange node
        // if partitioning column is part of SELECT columns for each statement
        RelDistribution newDistribution = RelDistributions.SINGLETON.with(
                setOpState.getLiteral(), setOpState.isSP());
        boolean isMP = !setOpState.isSP() && setOpState.getLiteral() == null;
        List<RelNode> inputs = setOp.getInputs()
                .stream()
                .map(node -> {
                    RelDistribution dist = node.getTraitSet().getTrait(RelDistributionTraitDef.INSTANCE);
                    boolean needExchange = isMP && RelDistribution.Type.SINGLETON != dist.getType();
                    if (needExchange) {
                        VoltLogicalExchange exchange = new VoltLogicalExchange(node.getCluster(),
                                    node.getTraitSet(), node, dist);
                        return exchange;
                    } else {
                        return node;
                    }
                })
                .collect(Collectors.toList());
        SetOp newSetOp = setOp.copy(setOp.getTraitSet().replace(newDistribution), inputs);
        call.transformTo(newSetOp);
    }
}
