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
import org.apache.calcite.rel.core.Calc;
import org.voltdb.calciteadapter.rel.physical.AbstractVoltDBPExchange;
import org.voltdb.calciteadapter.rel.physical.VoltDBPCalc;

/**
 * Transform Calc / Exchange rel to Exchange / Calc
 */
public class VoltDBPCalcExchangeTransposeRule extends RelOptRule {

    public static final VoltDBPCalcExchangeTransposeRule INSTANCE= new VoltDBPCalcExchangeTransposeRule();

    private VoltDBPCalcExchangeTransposeRule() {
        super(operand(VoltDBPCalc.class,
                operand(AbstractVoltDBPExchange.class, any())));
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        VoltDBPCalc calc = call.rel(0);
        AbstractVoltDBPExchange exchange = call.rel(1);

        Calc newCalc = calc.copy(
                // Update Calc distribution's trait
                calc.getTraitSet().plus(exchange.getChildDistribution()),
                exchange.getInput(),
                calc.getProgram(),
                exchange.getChildSplitCount());
        AbstractVoltDBPExchange newExchange = exchange.copy(
                exchange.getTraitSet(),
                newCalc,
                exchange.getChildDistribution(),
                exchange.getLevel() + 1);

        call.transformTo(newExchange);
        // Remove the original rel from the search space
        call.getPlanner().setImportance(calc, 0);

    }
}
