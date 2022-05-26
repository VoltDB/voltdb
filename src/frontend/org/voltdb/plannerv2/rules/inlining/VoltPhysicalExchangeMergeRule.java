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

package org.voltdb.plannerv2.rules.inlining;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelDistributionTraitDef;
import org.voltdb.plannerv2.rel.physical.VoltPhysicalLimit;
import org.voltdb.plannerv2.rel.physical.VoltPhysicalMergeExchange;
import org.voltdb.plannerv2.rel.physical.VoltPhysicalSort;

/**
 * The rule that Merges/inline Limit and Sort into a MergeExchange.
 *
 * @author mikealexeev
 */
public class VoltPhysicalExchangeMergeRule extends RelOptRule {
    private enum MergeType {
        LIMIT_EXCHANGE,
        SORT_EXCHANGE
    }

    public static final VoltPhysicalExchangeMergeRule INSTANCE_LIMIT_MERGE_EXCHANGE =
            new VoltPhysicalExchangeMergeRule(operand(VoltPhysicalLimit.class,
                    operand(VoltPhysicalMergeExchange.class, none())), MergeType.LIMIT_EXCHANGE);

    public static final VoltPhysicalExchangeMergeRule INSTANCE_SORT_MERGE_EXCHANGE =
            new VoltPhysicalExchangeMergeRule(operand(VoltPhysicalSort.class,
                    operand(VoltPhysicalMergeExchange.class, none())), MergeType.SORT_EXCHANGE);

    /**
     * Transform  VoltPhysicalLimit / VoltPhysicalMergeExchange to VoltPhysicalMergeExchange with inline Limit
     * Transform  VoltPhysicalSort / VoltPhysicalMergeExchange to VoltPhysicalMergeExchange with inline Sort
     */
    private VoltPhysicalExchangeMergeRule(RelOptRuleOperand operand, MergeType mergeType) {
        super(operand, mergeType.toString());
        m_mergeType = mergeType;
    }

    private final MergeType m_mergeType;

    @Override
    public void onMatch(RelOptRuleCall call) {
        switch (m_mergeType) {
        case LIMIT_EXCHANGE:
            mergeLimit(call);
            break;
        case SORT_EXCHANGE:
            mergeSort(call);
            break;
        }
    }

    private void mergeLimit(RelOptRuleCall call) {
        VoltPhysicalLimit limit = (VoltPhysicalLimit) call.rels[0];
        VoltPhysicalMergeExchange exchange = (VoltPhysicalMergeExchange) call.rels[1];
        // We have to preserve top node (limit) traits including its distribution
        // for the Planner to properly add a final SendPlanNode to a converted Volt plan
        // see PlannerTool.calciteToVoltDBPlan
        // Thus, we have to replace exchange's distribution with the limit's one
        // even though it doesn't look right.
        RelDistribution limitDistribution = limit.getTraitSet().getTrait(RelDistributionTraitDef.INSTANCE);
        RelTraitSet newExchangeTraits = exchange.getTraitSet().replace(limitDistribution);
        VoltPhysicalMergeExchange newExchange = exchange.copy(
                newExchangeTraits,
                exchange.getInput(),
                limitDistribution,
                exchange.getChildExps(),
                limit.getOffset(),
                limit.getLimit());
        call.transformTo(newExchange);
    }

    private void mergeSort(RelOptRuleCall call) {
        VoltPhysicalSort sort = (VoltPhysicalSort) call.rels[0];
        VoltPhysicalMergeExchange exchange = (VoltPhysicalMergeExchange) call.rels[1];
        // Replace exchange's distribution and collation traits with the sort's ones.
        // See comment to the mergeLimit method.
        RelCollation sortCollation = sort.getCollation();
        RelDistribution sortDistribution = sort.getTraitSet().getTrait(RelDistributionTraitDef.INSTANCE);
        RelTraitSet newExchangeTraits = exchange.getTraitSet().replace(sortDistribution).replace(sortCollation);
        // Sort may already have Limit / Offset inlined
        VoltPhysicalMergeExchange newExchange = exchange.copy(
                newExchangeTraits,
                exchange.getInput(),
                sortDistribution,
                sort.getChildExps(),
                sort.offset,
                sort.fetch);
        call.transformTo(newExchange);
    }

}
