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

import java.util.function.Predicate;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelDistributionTraitDef;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Exchange;
import org.voltdb.plannerv2.rel.physical.VoltPhysicalExchange;
import org.voltdb.plannerv2.rel.physical.VoltPhysicalLimit;
import org.voltdb.plannerv2.rel.physical.VoltPhysicalMergeExchange;
import org.voltdb.plannerv2.rel.physical.VoltPhysicalSort;
import org.voltdb.plannerv2.utils.VoltRelUtil;

/**
 * Rules that to push Limit, Sort and Aggregation nodes down through an Exchange node
 *
 * @author Michael Alexeev
 * @since 9.0
 */
public class VoltPExchangeTransposeRule extends RelOptRule {

    private enum ExchangeType {
        LIMIT_EXCHANGE,
        SORT_EXCHANGE,
        AGGREGATE_EXCHANGE,
        CALC_AGGREGATE_EXCHANGE,
        LIMIT_SORT_EXCHANGE
    }

    /**
     * Predicate to stop LIMIT_EXCHANGE to match on LIMIT / EXCHANGE / LIMIT
    */
    private static final Predicate<VoltPhysicalLimit> LIMIT_STOP_PREDICATE = rel -> !rel.isPushedDown();
    /*
     * Rule to transform Limit(pusheddown = false) / Exchange / SingleRel to
     *                   Limit(pusheddown = true) / Exchange / Limit(pusheddown = false) / SingleRel
     */
    public static final VoltPExchangeTransposeRule INSTANCE_LIMIT_EXCHANGE =
            new VoltPExchangeTransposeRule(
                    operandJ(VoltPhysicalLimit.class, null, LIMIT_STOP_PREDICATE,
                            operand(VoltPhysicalExchange.class,
                                    operand(RelNode.class, any()))),
                    ExchangeType.LIMIT_EXCHANGE);

    /**
     * Predicate to stop SORT_EXCHANGE to match on SORT / EXCHANGE / SORT
    */
    private static final Predicate<VoltPhysicalSort> SORT_STOP_PREDICATE = rel -> !rel.isPushedDown();
    /*
     * Rule to transform Sort(pusheddown = false) / Exchange / SingleRel to
     *                   Sort(pusheddown = true) / MergeExchange / Sort(pusheddown = false) / SingleRel
     */
    public static final VoltPExchangeTransposeRule INSTANCE_SORT_EXCHANGE =
            new VoltPExchangeTransposeRule(
                    operandJ(VoltPhysicalSort.class, null, SORT_STOP_PREDICATE,
                            operand(VoltPhysicalExchange.class,
                                    operand(RelNode.class, any()))),
                    ExchangeType.SORT_EXCHANGE);

    /**
     * Predicate to select coordinator's SORT that was already pushed down
    */
    private static final Predicate<VoltPhysicalSort> SORT_PUSHED_PREDICATE = rel -> rel.isPushedDown();
    /*
     * Rule to transform Limit(pusheddown = false) / Sort(pusheddown = true) / MergeExchange / SingleRel to
     *                   Limit(pusheddown = true) / Sort(pusheddown = true) / MergeExchange / Limit(pusheddown = false) / Sort(pusheddown = false) /SingleRel
     *
     * This rule must fire after the SORT_EXCHANGE to preserve the right Limit / Sort order
     * at the partition's fragment
     */
    public static final VoltPExchangeTransposeRule INSTANCE_LIMIT_SORT_EXCHANGE =
            new VoltPExchangeTransposeRule(
                    operandJ(VoltPhysicalLimit.class, null, LIMIT_STOP_PREDICATE,
                            operandJ(VoltPhysicalSort.class, null, SORT_PUSHED_PREDICATE,
                                    operand(VoltPhysicalMergeExchange.class,
                                            operand(RelNode.class, any())))),
                    ExchangeType.LIMIT_SORT_EXCHANGE);


    private final ExchangeType mExchangeType;

    private VoltPExchangeTransposeRule(RelOptRuleOperand operand, ExchangeType exchangeType) {
        super(operand, exchangeType.toString());
        mExchangeType = exchangeType;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        switch (mExchangeType) {
            case LIMIT_EXCHANGE:
                transposeLimitExchange(call);
                break;
            case SORT_EXCHANGE:
                transposeSortExchange(call);
                break;
            case LIMIT_SORT_EXCHANGE:
                transposeLimitSortExchange(call);
                break;
            case AGGREGATE_EXCHANGE:
            case CALC_AGGREGATE_EXCHANGE:
                transposeAggregateExchange(call);
                break;
        }
    }

    private void transposeLimitExchange(RelOptRuleCall call) {
        VoltPhysicalLimit coordinatorLimit = (VoltPhysicalLimit) call.rels[0];
        Exchange exchange = (Exchange) call.rels[1];
        RelNode child = call.rels[2];

        VoltRelUtil.buildFragmentLimit(coordinatorLimit, exchange.getDistribution(), child)
            .ifPresent(fragmentLimit -> {
                // Build chain
                Exchange newExchange = exchange.copy(exchange.getTraitSet(), fragmentLimit, exchange.getDistribution());
                VoltPhysicalLimit newCoordinatorLimit = coordinatorLimit.copy(
                        coordinatorLimit.getTraitSet(),
                        newExchange,
                        coordinatorLimit.getOffset(),
                        coordinatorLimit.getLimit(), true);
                call.transformTo(newCoordinatorLimit);
            });
    }

    private void transposeSortExchange(RelOptRuleCall call) {
        VoltPhysicalSort sort = (VoltPhysicalSort) call.rels[0];
        Exchange exchange = (Exchange) call.rels[1];
        RelNode child = call.rels[2];

        // Fragment Sort
        VoltPhysicalSort fragmentSort = new VoltPhysicalSort(
                sort.getCluster(),
                sort.getTraitSet().replace(exchange.getDistribution()),
                child,
                sort.getCollation(),
                false);
        // New exchange. Since fragment's results will be sorted we can use MergeExchange
        // to eliminate redundant coordinator's sort
        Exchange newExchange = new VoltPhysicalMergeExchange(exchange.getCluster(), exchange.getTraitSet(), fragmentSort, exchange.getDistribution());
        // New Coordinator's Sort
        // If the sort node was created by the Calcite (from an existing collation trait)
        // its distribution needs to be reset
        RelDistribution coordinatorDist = RelDistributions.SINGLETON.with(
                sort.getTraitSet().getTrait(RelDistributionTraitDef.INSTANCE).getPartitionEqualValue(),
                false);
        VoltPhysicalSort coordinatorSort = new VoltPhysicalSort(
                sort.getCluster(),
                sort.getTraitSet().replace(coordinatorDist),
                newExchange,
                sort.getCollation(),
                true);
        call.transformTo(coordinatorSort);
    }

    private void transposeLimitSortExchange(RelOptRuleCall call) {
        VoltPhysicalLimit coordinatorLimit = (VoltPhysicalLimit) call.rels[0];
        VoltPhysicalSort coordinatorSort = (VoltPhysicalSort) call.rels[1];
        Exchange exchange = (Exchange) call.rels[2];
        RelNode child = call.rels[3];

        VoltRelUtil.buildFragmentLimit(coordinatorLimit, exchange.getDistribution(), child)
            .ifPresent(fragmentLimit -> {
                // Build chain
                Exchange newExchange = exchange.copy(exchange.getTraitSet(), fragmentLimit, exchange.getDistribution());
                VoltPhysicalSort newCoordinatorSort = coordinatorSort.copy(
                    coordinatorSort.getTraitSet(),
                    newExchange,
                    coordinatorSort.getCollation(),
                    coordinatorSort.offset,
                    coordinatorSort.fetch);
                VoltPhysicalLimit newCoordinatorLimit = coordinatorLimit.copy(
                        coordinatorLimit.getTraitSet(),
                        newCoordinatorSort,
                        coordinatorLimit.getOffset(),
                        coordinatorLimit.getLimit(), true);
                call.transformTo(newCoordinatorLimit);
            });
    }

    private void transposeAggregateExchange(RelOptRuleCall call) {
        // How to deal with Calc / Aggr / Exchange vs Aggr / Exchange
        // If we have aggr with HAVING we should move both Calc and Aggr to fragment
        // but rule Aggr / Exchange may move just the Aggr (this rule is required
        // for aggr without HAVING or GROUP BY)
        // Moving just Aggr without the Calc may be OK but  sub-optimal and should have a higher
        // cost than the plan with both Calc / Aggr pushed down
    }
}
