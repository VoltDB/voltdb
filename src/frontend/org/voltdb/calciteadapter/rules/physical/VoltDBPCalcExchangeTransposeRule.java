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
import org.apache.calcite.plan.RelOptRuleOperandChildPolicy;
import org.apache.calcite.plan.RelOptRuleOperandChildren;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.core.Calc;
import org.voltdb.calciteadapter.rel.physical.AbstractVoltDBPExchange;
import org.voltdb.calciteadapter.rel.physical.VoltDBPCalc;
import org.voltdb.calciteadapter.rel.physical.VoltDBPMergeExchange;
import org.voltdb.calciteadapter.util.VoltDBRexUtil;

import com.google.common.collect.ImmutableList;

/**
 * Transform Calc / Exchange rel to Exchange / Calc
 */
public class VoltDBPCalcExchangeTransposeRule extends RelOptRule {

    public static final VoltDBPCalcExchangeTransposeRule INSTANCE= new VoltDBPCalcExchangeTransposeRule();

    private VoltDBPCalcExchangeTransposeRule() {
        super(operand(VoltDBPCalc.class,
                RelDistributions.ANY,
                new RelOptRuleOperandChildren(
                        RelOptRuleOperandChildPolicy.ANY,
                        ImmutableList.of(
                                operand(AbstractVoltDBPExchange.class, any())))));
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        VoltDBPCalc calc = call.rel(0);
        AbstractVoltDBPExchange exchange = call.rel(1);

        // Adjust exchage's RelCollation trait
        RelTraitSet exchangeTraits = exchange.getTraitSet();
        RelTraitSet calcTraits = calc.getTraitSet();
        RelCollation collationTrait = exchangeTraits.getTrait(RelCollationTraitDef.INSTANCE);

        // Update Calc's collation trait
        RelCollation adjustedCollation = VoltDBRexUtil.adjustCollationForProgram(
                calc.getCluster().getRexBuilder(),
                calc.getProgram(), collationTrait);

        // @TODO Collations that use expressions can't be converted.
        // Need to update VoltDBRexUtil.adjustCollationForProgram to handle expressions
//        if (adjustedCollation.getFieldCollations().size() != collationTrait.getFieldCollations().size()) {
//            return;
//        }

        exchangeTraits = exchangeTraits.replace(adjustedCollation);
        calcTraits = calcTraits.replace(adjustedCollation);

        // Update Calc's distribution trait
        // Do not change distribution trait if this is a top exchange.
        // The trait will be updated when a limit relation will be transposed with a bottom(fragment) exchange
        if (!exchange.isTopExchange()) {
            calcTraits = calcTraits.replace(exchange.getChildDistribution());
        }

        Calc newCalc = calc.copy(
                calcTraits,
                exchange.getInput(),
                calc.getProgram(),
                exchange.getSplitCount());

        AbstractVoltDBPExchange newExchange;
        if (exchange instanceof VoltDBPMergeExchange) {
            // @TODO WIP VoltDBPMergeExchange's collationFieldExprs has to be adjusted properly
            // similar to the collation itself
            newExchange = new VoltDBPMergeExchange(
                    exchange.getCluster(),
                    exchangeTraits,
                    newCalc,
                    exchange.getChildDistribution(),
                    exchange.getSplitCount(),
                    exchange.isTopExchange(),
                    calc.getProgram().getExprList());
        } else {
            newExchange = exchange.copy(
                exchangeTraits,
                newCalc,
                exchange.getChildDistribution(),
                exchange.isTopExchange());
        }

        call.transformTo(newExchange);
    }
}
