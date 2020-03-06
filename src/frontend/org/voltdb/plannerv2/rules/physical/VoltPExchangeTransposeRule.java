/* This file is part of VoltDB.
 * Copyright (C) 2008-2020 VoltDB Inc.
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

import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelDistributionTraitDef;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Exchange;
import org.apache.calcite.rex.RexProgramBuilder;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.voltdb.plannerv2.rel.physical.VoltPhysicalAggregate;
import org.voltdb.plannerv2.rel.physical.VoltPhysicalCalc;
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

    /**
     * Predicate to stop AGGREGATE_EXCHANGE to match on Aggregate / EXCHANGE / Aggregate
    */
    private static final Predicate<VoltPhysicalAggregate> AGGREGATE_STOP_PREDICATE = rel -> !rel.isPushedDown();
    /*
     * Rule to transform Calc / Aggr(pusheddown = false) / Exchange / SingleRel to
     *                   Calc / Aggr(pusheddown = true) / Exchange / Calc / Aggr(pusheddown = false) / SingleRel
     */
    public static final VoltPExchangeTransposeRule INSTANCE_CALC_AGGREGATE_EXCHANGE =
            new VoltPExchangeTransposeRule(
                    operand(VoltPhysicalCalc.class,
                            operandJ(VoltPhysicalAggregate.class, null, AGGREGATE_STOP_PREDICATE,
                                    operand(VoltPhysicalExchange.class,
                                            operand(RelNode.class, any())))),
                    ExchangeType.CALC_AGGREGATE_EXCHANGE);
    /*
     * Rule to transform Aggr(pusheddown = false) / Exchange / SingleRel to
     *                   Aggr(pusheddown = true) / Exchange / Aggr(pusheddown = false) / SingleRel
     */
    public static final VoltPExchangeTransposeRule INSTANCE_AGGREGATE_EXCHANGE =
            new VoltPExchangeTransposeRule(
                    operandJ(VoltPhysicalAggregate.class, null, AGGREGATE_STOP_PREDICATE,
                            operand(VoltPhysicalExchange.class,
                                    operand(RelNode.class, any()))),
                    ExchangeType.AGGREGATE_EXCHANGE);


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
        final VoltPhysicalCalc aggrCalc;
        final VoltPhysicalAggregate aggregate;
        final Exchange exchange;
        final RelNode child;
        if (mExchangeType == ExchangeType.CALC_AGGREGATE_EXCHANGE) {
            aggrCalc = (VoltPhysicalCalc) call.rels[0];
            aggregate = (VoltPhysicalAggregate) call.rels[1];
            exchange = (Exchange) call.rels[2];
            child = call.rels[3];
        } else {
            aggrCalc = null;
            aggregate = (VoltPhysicalAggregate) call.rels[0];
            exchange = (Exchange) call.rels[1];
            child = call.rels[2];
        }

        // Fragment Aggregate is a copy of the current aggregate
        VoltPhysicalAggregate fragmentAggregate = aggregate.copy(
                aggregate.getCluster(),
                aggregate.getTraitSet().replace(exchange.getDistribution()),
                child,
                aggregate.indicator,
                aggregate.getGroupSet(),
                aggregate.getGroupSets(),
                aggregate.getAggCallList(),
                aggregate.getPostPredicate(),
                aggregate.isPushedDown());

        final RelNode exhangeInput;
        if (mExchangeType == ExchangeType.CALC_AGGREGATE_EXCHANGE) {
            // Can't just copy calc. Only its filter. Project should remain
            RexProgramBuilder builder = RexProgramBuilder.forProgram(
                    aggrCalc.getProgram(),
                    aggrCalc.getCluster().getRexBuilder(),
                    true);
            builder.clearProjects();
            aggregate.getRowType().getFieldList().forEach(field -> {
                builder.addProject(field.getIndex(), field.getName());
            });
            exhangeInput = aggrCalc.copy(
                    aggrCalc.getTraitSet().replace(exchange.getDistribution()),
                    fragmentAggregate,
                    builder.getProgram());
        } else {
            exhangeInput = fragmentAggregate;
        }
        // New exchange.
        Exchange newExchange = new VoltPhysicalExchange(exchange.getCluster(),
                exchange.getTraitSet(),
                exhangeInput,
                exchange.getDistribution());

        // Coordinator fragment. Replace all occurrences of COUNT with SUM
        List<AggregateCall> aggCalls = aggregate.getAggCallList().stream()
                .map(oldCall -> {
                    if (oldCall.getAggregation().kind == SqlKind.COUNT) {
                        final AggregateCall sumCall =
                                AggregateCall.create(SqlStdOperatorTable.SUM0FROMCOUNT,
                                    oldCall.isDistinct(),
                                    oldCall.isApproximate(),
                                    oldCall.getArgList(),
                                    oldCall.filterArg,
                                    aggregate.getGroupCount(),
                                    aggregate,
                                    oldCall.getType(),
                                    null);
                        return sumCall;
                    } else {
                        return oldCall;
                    }
                })
                .collect(Collectors.toList());

        // Can be smarter here and remove aggregates related to HAVING conditions if any
        // since we eliminate Coordinator's HAVING condition altogether.
        VoltPhysicalAggregate coordinatorAggregate = aggregate.copy(
                aggregate.getCluster(),
                aggregate.getTraitSet(),
                newExchange,
                aggregate.indicator,
                aggregate.getGroupSet(),
                aggregate.getGroupSets(),
                aggCalls,
                aggregate.getPostPredicate(),
                true);
        final RelNode finalResult;
        if (mExchangeType == ExchangeType.CALC_AGGREGATE_EXCHANGE) {
            // HAVING condition can be dropped here because fragment's calc already has them
            RexProgramBuilder builder = RexProgramBuilder.forProgram(
                    aggrCalc.getProgram(),
                    aggrCalc.getCluster().getRexBuilder(),
                    true);
            builder.clearCondition();
            finalResult = aggrCalc.copy(
                    aggrCalc.getTraitSet(),
                    coordinatorAggregate,
                    builder.getProgram());
        } else {
            finalResult = coordinatorAggregate;
        }
        call.transformTo(finalResult);
    }
}
