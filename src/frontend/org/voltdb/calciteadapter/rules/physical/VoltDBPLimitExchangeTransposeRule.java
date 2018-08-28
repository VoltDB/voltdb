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
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelOptRuleOperandChildPolicy;
import org.apache.calcite.plan.RelOptRuleOperandChildren;
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.voltdb.calciteadapter.rel.physical.AbstractVoltDBPExchange;
import org.voltdb.calciteadapter.rel.physical.VoltDBPLimit;
import org.voltdb.calciteadapter.rel.physical.VoltDBPSingletonExchange;

import com.google.common.collect.ImmutableList;

/**
 * INSTANCE_DISTRIBUTION_ANY Transform Limit with RelDistribution.ANY / Exchange rels into
 *  a) Singleton Exchange / Coordinator Limit / Exchange / Fragment Limit
 *      if the original Exchange relation is a Union or Merge Exchanges
 *  b) Singleton Exchange / Limit if the original Exchange relation is a Singleton
 *
 * INSTANCE_DISTRIBUTION_SINGLE Transform Limit with RelDistribution.SINGLETON / Exchange rels into
 * Coordinator Limit / Exchange / Fragment Limit
 * Since the original Limit already has a RelDistribution.SINGLETON trait, there must be Singleton Exchange
 * above it. This limit rel is part of a coordinator fragment.
 */
public class VoltDBPLimitExchangeTransposeRule extends RelOptRule {

    public static final VoltDBPLimitExchangeTransposeRule INSTANCE =
            new VoltDBPLimitExchangeTransposeRule(operand(
                        VoltDBPLimit.class,
                        RelDistributions.ANY,
                        new RelOptRuleOperandChildren(
                                RelOptRuleOperandChildPolicy.ANY,
                                ImmutableList.of(
                                        operand(AbstractVoltDBPExchange.class, any())))),
                    "VoltDBPLimitExchangeTransposeRule_ANY"
                );

    private VoltDBPLimitExchangeTransposeRule(RelOptRuleOperand operand, String description) {
        super(operand, description);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        VoltDBPLimit limitRel = call.rel(0);
        assert(limitRel != null);
        AbstractVoltDBPExchange exchangeRel = call.rel(1);

        RelNode result;
        if (exchangeRel instanceof VoltDBPSingletonExchange) {
            result = transposeSingletonExchange((VoltDBPSingletonExchange) exchangeRel, limitRel);
        } else {
            result = transposeDistributedExchange(exchangeRel, limitRel);
        }

        call.transformTo(result);
    }

    private RelNode transposeSingletonExchange(VoltDBPSingletonExchange exchangeRel, VoltDBPLimit limitRel) {
        // Simply push the limit through the exchange
        RelTraitSet exchangeTraits = exchangeRel.getTraitSet();
        RelTrait collationTrait = exchangeTraits.getTrait(RelCollationTraitDef.INSTANCE);
        RelDistribution distributionTrait = exchangeRel.getDistribution();
        // Update Limit distribution's and collation's traits
        RelTraitSet newLimitTraits = limitRel.getTraitSet()
                .replace(collationTrait);
        // Do not change distribution trait if this is a top exchange.
        // The trait will be updated when a limit relation will be transposed with a bottom(fragment) exchange
        if (!exchangeRel.isTopExchange()) {
            newLimitTraits = newLimitTraits.replace(distributionTrait);
        }
        VoltDBPLimit newLimitRel = limitRel.copy(
                newLimitTraits,
                exchangeRel.getInput(),
                limitRel.getOffset(),
                limitRel.getLimit(),
                exchangeRel.getSplitCount());

        AbstractVoltDBPExchange newExchange = exchangeRel.copy(
                exchangeTraits,
                newLimitRel,
                distributionTrait,
                exchangeRel.isTopExchange());
        return newExchange;
    }

    private RelNode transposeDistributedExchange(AbstractVoltDBPExchange exchangeRel, VoltDBPLimit origLimitRel) {
        RelTraitSet exchangeTraits = exchangeRel.getTraitSet();
        RelTrait collationTrait = exchangeTraits.getTrait(RelCollationTraitDef.INSTANCE);
        RelDistribution distributionTrait = exchangeRel.getDistribution();

        // We can not push just an OFFSET through a distributed exchange
        AbstractVoltDBPExchange newExchangeRel;
        if (origLimitRel.getLimit() == null) {
            newExchangeRel = exchangeRel;
        } else {
            // The fragment limit always has 0 offset and its limit =
            // sum of the coordinator's limit and offset
            int fragmentLimit = RexLiteral.intValue(origLimitRel.getLimit());
            if (origLimitRel.getOffset() != null) {
                fragmentLimit += RexLiteral.intValue(origLimitRel.getOffset());
            }
            RelTraitSet fragmentLimitTraits = origLimitRel.getTraitSet()
                    .replace(collationTrait)
                    .replace(distributionTrait);

            RexBuilder rexBuilder = origLimitRel.getCluster().getRexBuilder();
            RexNode fragmentLimitRex = rexBuilder.makeBigintLiteral(new BigDecimal(fragmentLimit));
            RelNode fragmentLimitRel = origLimitRel.copy(
                    fragmentLimitTraits,
                    exchangeRel.getInput(),
                    null,
                    fragmentLimitRex,
                    exchangeRel.getSplitCount());
            newExchangeRel = exchangeRel.copy(
                    exchangeTraits,
                    fragmentLimitRel,
                    exchangeRel.getDistribution(),
                    exchangeRel.isTopExchange());
        }

        // Coordinator's limit
        RelTraitSet coordinatorLimitTraits = origLimitRel.getTraitSet()
                                                .replace(collationTrait)
                                                .replace(RelDistributions.SINGLETON);

        VoltDBPLimit coordinatorLimitRel = origLimitRel.copy(
                coordinatorLimitTraits,
                newExchangeRel,
                origLimitRel.getOffset(),
                origLimitRel.getLimit(),
                origLimitRel.getSplitCount());

        return coordinatorLimitRel;
    }

}
