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

import java.math.BigDecimal;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperandChildPolicy;
import org.apache.calcite.plan.RelOptRuleOperandChildren;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.voltdb.calciteadapter.rel.physical.AbstractVoltDBPExchange;
import org.voltdb.calciteadapter.rel.physical.VoltDBPLimit;
import org.voltdb.calciteadapter.rel.physical.VoltDBPSingeltonExchange;

import com.google.common.collect.ImmutableList;

/**
 * Transform Limit / Exchange rels into
 *  a) Singleton Exchange / Coordinator Limit / Exchange / Fragment Limit
 *      if the original Exchange relation is a Union or Merge Exchanges
 *  b) Singleton Exchange / Limit if the original Exchange relation is a Singleton
 */
public class VoltDBPLimitExchangeTransposeRule extends RelOptRule {

    public static final VoltDBPLimitExchangeTransposeRule INSTANCE= new VoltDBPLimitExchangeTransposeRule();

    private VoltDBPLimitExchangeTransposeRule() {
        super(
                operand(
                        VoltDBPLimit.class,
                        RelDistributions.ANY,
                        new RelOptRuleOperandChildren(
                                RelOptRuleOperandChildPolicy.ANY,
                                ImmutableList.of(
                                        operand(AbstractVoltDBPExchange.class, any()))))
                );
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        VoltDBPLimit limitRel = call.rel(0);
        assert(limitRel != null);
        AbstractVoltDBPExchange exchangeRel = call.rel(1);

        RelNode result;
        if (exchangeRel instanceof VoltDBPSingeltonExchange) {
            result = transposeSingletonExchange((VoltDBPSingeltonExchange) exchangeRel, limitRel);
        } else {
            result = transposeDistributedExchange(exchangeRel, limitRel);
        }

        call.transformTo(result);
//        // Remove the original rel from the search space
        call.getPlanner().setImportance(limitRel, 0);

    }

    private RelNode transposeSingletonExchange(VoltDBPSingeltonExchange exchangeRel, VoltDBPLimit limitRel) {
        // Simply push the limit through the exchange
        VoltDBPLimit newLimitRel = limitRel.copy(
                // Update Limit distribution's trait
                limitRel.getTraitSet().plus(exchangeRel.getChildDistribution()),
                exchangeRel.getInput(),
                limitRel.getOffset(),
                limitRel.getLimit(),
                exchangeRel.getChildSplitCount());

        AbstractVoltDBPExchange newExchange = exchangeRel.copy(
                exchangeRel.getTraitSet(),
                newLimitRel,
                exchangeRel.getChildDistribution(),
                exchangeRel.getLevel() + 1);
        return newExchange;
    }

    private RelNode transposeDistributedExchange(AbstractVoltDBPExchange exchangeRel, VoltDBPLimit limitRel) {
        // We can not push just an OFFSET through a distributed exchange
        AbstractVoltDBPExchange newExchangeRel;
        if (limitRel.getLimit() == null) {
            newExchangeRel = exchangeRel;
        } else {
            // The fragment limit always has 0 offset and its limit =
            // sum of the coordinator's limit and offset
            int fragmentLimit = RexLiteral.intValue(limitRel.getLimit());
            if (limitRel.getOffset() != null) {
                fragmentLimit += RexLiteral.intValue(limitRel.getOffset());
            }
            RexBuilder rexBuilder = limitRel.getCluster().getRexBuilder();
            RexNode fragmentLimitRex = rexBuilder.makeBigintLiteral(new BigDecimal(fragmentLimit));
            RelNode fragmentLimitRel = limitRel.copy(
                    // Update Limit distribution's trait
                    limitRel.getTraitSet().plus(exchangeRel.getChildDistribution()),
                    exchangeRel.getInput(),
                    null,
                    fragmentLimitRex,
                    exchangeRel.getChildSplitCount());
            newExchangeRel = exchangeRel.copy(
                    exchangeRel.getTraitSet(),
                    fragmentLimitRel,
                    exchangeRel.getChildDistribution(),
                    exchangeRel.getLevel() + 1);
        }

        // Coordinator's limit
        VoltDBPLimit coordinatorLimitRel = limitRel.copy(
            limitRel.getTraitSet().replace(RelDistributions.SINGLETON),
            newExchangeRel,
            limitRel.getOffset(),
            limitRel.getLimit(),
            limitRel.getSplitCount());

        // Add a SingletonExchange on top of it to be propagated all the way to the root
        // The relations that will be transposed with the Singleton Exchange represent
        // the coordinator's nodes in the final VoltDB plan 
        // and can not / should not be pushed beyond the VoltDBPMergeExchange exchange
        RelNode result = new VoltDBPSingeltonExchange(
                    exchangeRel.getCluster(),
                    exchangeRel.getTraitSet(),
                    coordinatorLimitRel,
                    newExchangeRel.getLevel() + 1);

        return result;
    }

}
