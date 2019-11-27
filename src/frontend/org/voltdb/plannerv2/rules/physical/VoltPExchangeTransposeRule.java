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

package org.voltdb.plannerv2.rules.physical;

import java.util.function.Predicate;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Exchange;
import org.voltdb.plannerv2.rel.physical.VoltPhysicalExchange;
import org.voltdb.plannerv2.rel.physical.VoltPhysicalLimit;

/**
 * Rules that to push Limit, Sort and Aggregation nodes down through an Exchange node
 *
 * @author Michael Alexeev
 * @since 9.0
 */
public class VoltPExchangeTransposeRule extends RelOptRule {

    private enum ExchangeType {
//        SORT_EXCHANGE,
        LIMIT_EXCHANGE
//        LIMIT_SORT_EXCHANGE
    }

//    /**
//     * Predicate to stop SORT_EXCHANGE to match on SORT / EXCHANGE / SORT
//    */
//    private static final Predicate<RelNode> SORT_STOP_PREDICATE = rel -> !(rel instanceof Sort);
//    public static final VoltLExchangeTransposeRule INSTANCE_SORT_EXCHANGE =
//            new VoltLExchangeTransposeRule(
//                    operand(VoltLogicalSort.class,
//                            operand(VoltLogicalExchange.class,
//                                    operandJ(RelNode.class, null, SORT_STOP_PREDICATE, any()))),
//                    ExchangeType.SORT_EXCHANGE);

    /**
     * Predicate to stop LIMIT_EXCHANGE to match on LIMIT / EXCHANGE / LIMIT
    */
    private static final Predicate<VoltPhysicalLimit> LIMIT_STOP_PREDICATE = rel -> !rel.isPushedDown();
    public static final VoltPExchangeTransposeRule INSTANCE_LIMIT_EXCHANGE =
            new VoltPExchangeTransposeRule(
                    operandJ(VoltPhysicalLimit.class, null, LIMIT_STOP_PREDICATE,
                            operand(VoltPhysicalExchange.class,
                                    operand(RelNode.class, any()))),
                    ExchangeType.LIMIT_EXCHANGE);

//    public static final VoltPExchangeTransposeRule INSTANCE_LIMIT_SORT_EXCHANGE =
//            new VoltPExchangeTransposeRule(
//                    operand(VoltLogicalLimit.class,
//                            operand(VoltLogicalSort.class,
//                                    operand(VoltLogicalExchange.class,
//                                            operand(VoltLogicalSort.class, any())))),
//                    ExchangeType.LIMIT_SORT_EXCHANGE);

    private final ExchangeType mExchangeType;

    private VoltPExchangeTransposeRule(RelOptRuleOperand operand, ExchangeType exchangeType) {
        super(operand, exchangeType.toString());
        mExchangeType = exchangeType;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        switch (mExchangeType) {
//            case SORT_EXCHANGE:
//                transposeSortExchange(call);
//                break;
            case LIMIT_EXCHANGE:
                transposeLimitExchange(call);
                break;
//            case LIMIT_SORT_EXCHANGE:
//                transposeLimitSortExchange(call);
//                break;
        }
    }

//    private void transposeSortExchange(RelOptRuleCall call) {
//        Sort sort = (Sort) call.rels[0];
//        Exchange exchange = (Exchange) call.rels[1];
//        RelNode child = call.rels[2];
//        Sort fragmentSort = sort.copy(sort.getTraitSet().replace(exchange.getDistribution()), child, sort.getCollation());
//        Exchange newExchange = exchange.copy(exchange.getTraitSet(), fragmentSort, exchange.getDistribution());
//        Sort coordinatorSort = sort.copy(sort.getTraitSet(), newExchange, sort.getCollation());
//        call.transformTo(coordinatorSort);
//    }

    private void transposeLimitExchange(RelOptRuleCall call) {
        VoltPhysicalLimit limit = (VoltPhysicalLimit) call.rels[0];
        Exchange exchange = (Exchange) call.rels[1];
        RelNode child = call.rels[2];

        // Check when LIMIT / OFFSET
        VoltPhysicalLimit fragmentLimit = new VoltPhysicalLimit(
                limit.getCluster(), limit.getTraitSet().replace(exchange.getDistribution()), child,
                limit.getOffset(), limit.getLimit(), false);
                Exchange newExchange = exchange.copy(exchange.getTraitSet(), fragmentLimit, exchange.getDistribution());
                VoltPhysicalLimit coordinatorLimit = new VoltPhysicalLimit(
                                limit.getCluster(), limit.getTraitSet(), newExchange,
                                limit.getOffset(), limit.getLimit(), true);
        call.transformTo(coordinatorLimit);
    }

//    private void transposeLimitSortExchange(RelOptRuleCall call) {
//        VoltLogicalLimit limit = (VoltLogicalLimit) call.rels[0];
//        Sort coordinatorSort = (Sort) call.rels[1];
//        Exchange exchange = (Exchange) call.rels[2];
//        Sort fragmentSort = (Sort) call.rels[3];
//
//        // Check when LIMIT / OFFSET
//        VoltLogicalLimit fragmentLimit = new VoltLogicalLimit(
//                limit.getCluster(), limit.getTraitSet().replace(exchange.getDistribution()), fragmentSort,
//                limit.getOffset(), limit.getLimit());
//        Exchange newExchange = exchange.copy(exchange.getTraitSet(), fragmentLimit, exchange.getDistribution());
//        Sort newCoordinatorSort = coordinatorSort.copy(coordinatorSort.getTraitSet(), newExchange, coordinatorSort.getCollation());
//        VoltLogicalLimit coordinatorLimit = new VoltLogicalLimit(
//                limit.getCluster(), limit.getTraitSet(), newCoordinatorSort,
//                limit.getOffset(), limit.getLimit());
//        call.transformTo(coordinatorLimit);
//    }

}
