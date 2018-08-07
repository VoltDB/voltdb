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

package org.voltdb.calciteadapter.rules.physical;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelOptRuleOperandChildPolicy;
import org.apache.calcite.plan.RelOptRuleOperandChildren;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.RelNode;
import org.voltdb.calciteadapter.rel.physical.AbstractVoltDBPAggregate;
import org.voltdb.calciteadapter.rel.physical.AbstractVoltDBPExchange;
import org.voltdb.calciteadapter.rel.physical.VoltDBPMergeExchange;
import org.voltdb.calciteadapter.rel.physical.VoltDBPSingletonExchange;
import org.voltdb.calciteadapter.rel.physical.VoltDBPUnionExchange;

import com.google.common.collect.ImmutableList;

/**
 * Push Aggregate down trough an Exchange.
 * Transform Aggregate / SingletonExchange rel to SingletonExchange / Aggregate
 * Transform Aggregate / Merge(Union)Exchange rel to
 *                 SingletonExchange / Coordinator Aggregate / Merge(Union)Exchange / Fragment Aggregate
 */
public class VoltDBPAggregateExchangeTransposeRule extends RelOptRule {

    public static final VoltDBPAggregateExchangeTransposeRule INSTANCE_1 =
            new VoltDBPAggregateExchangeTransposeRule(
                    operand(AbstractVoltDBPAggregate.class,
                            RelDistributions.ANY,
                            new RelOptRuleOperandChildren(
                                    RelOptRuleOperandChildPolicy.ANY,
                                    ImmutableList.of(
                                            operand(VoltDBPSingletonExchange.class, any())))),
                    true);

    public static final VoltDBPAggregateExchangeTransposeRule INSTANCE_2 =
            new VoltDBPAggregateExchangeTransposeRule(
                    operand(AbstractVoltDBPAggregate.class,
                            RelDistributions.ANY,
                            new RelOptRuleOperandChildren(
                                    RelOptRuleOperandChildPolicy.ANY,
                                    ImmutableList.of(
                                            operand(VoltDBPUnionExchange.class, any()),
                                            operand(VoltDBPMergeExchange.class, any())))),
                    false);

    private final boolean m_isSingleton;

    private VoltDBPAggregateExchangeTransposeRule(RelOptRuleOperand operand, boolean isSingleton) {
        super(operand);
        m_isSingleton = isSingleton;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        AbstractVoltDBPAggregate aggr = call.rel(0);
        AbstractVoltDBPExchange exchange = call.rel(1);

        RelNode result;
        if (m_isSingleton) {
            assert (exchange instanceof VoltDBPSingletonExchange);
            result = transformSingletonExchange(aggr, (VoltDBPSingletonExchange) exchange);
            call.transformTo(result);
        } else {
            result = null;
        }
    }

    private RelNode transformSingletonExchange(AbstractVoltDBPAggregate aggr, VoltDBPSingletonExchange exchange) {
        // Simply push the aggregate below the exchange
        AbstractVoltDBPAggregate newAggr = aggr.copy(
                aggr.getCluster(),
                aggr.getTraitSet().plus(exchange.getChildDistribution()),
                exchange.getInput(),
                aggr.indicator,
                aggr.getGroupSet(),
                aggr.getGroupSets(),
                aggr.getAggCallList(),
                aggr.getPostPredicate());

        AbstractVoltDBPExchange newExchange = exchange.copy(
                exchange.getTraitSet(),
                newAggr,
                exchange.getChildDistribution(),
                exchange.getLevel() + 1);
        return newExchange;
    }
}
