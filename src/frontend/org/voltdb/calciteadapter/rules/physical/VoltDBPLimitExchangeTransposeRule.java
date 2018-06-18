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
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.voltdb.calciteadapter.rel.physical.AbstractVoltDBPExchange;
import org.voltdb.calciteadapter.rel.physical.VoltDBPLimit;

import com.google.common.base.Predicate;

/**
 * Transform Limit / Exchange rels into Limit / Exchange / Limit
 * It is only possible if the original Limit rel has a LIMIT set -
 * can not push down an OFFSET only
 */
public class VoltDBPLimitExchangeTransposeRule extends RelOptRule {

    public static final VoltDBPLimitExchangeTransposeRule INSTANCE= new VoltDBPLimitExchangeTransposeRule();

    // Predicate to eliminate this rule from firing on already pushed down LIMIT rel
    // LIMIT/EXCHANGE -> LIMIT/EXCHANGE/LIMIT -> LIMIT/EXCHANGE/LIMIT/LIMIT -> ....
    private static Predicate<RelNode> checkPredicate() {
        return new Predicate<RelNode> () {
            @Override
            public boolean apply(RelNode exchangeRel) {
                return !(exchangeRel instanceof VoltDBPLimit);
            }
        };
    }

    private VoltDBPLimitExchangeTransposeRule() {
        super(operand(VoltDBPLimit.class,
                operand(AbstractVoltDBPExchange.class,
                        unordered(operand(RelNode.class, null, checkPredicate(), any())))));
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        boolean matches = super.matches(call);
        if (matches) {
            VoltDBPLimit limit = call.rel(0);
            // Must have limit. Can not push down the limit rel if it only has an offset
            matches = limit.getLimit() != null;
        }
        return matches;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        VoltDBPLimit limitRel = call.rel(0);
        assert(limitRel != null);
        AbstractVoltDBPExchange exchangeRel = call.rel(1);

        // The fragment limit always has 0 offset and its limit =
        // sum of the coordinator's limit and offset
        int fragmentLimit = RexLiteral.intValue(limitRel.getLimit());
        if (limitRel.getOffset() != null) {
            fragmentLimit += RexLiteral.intValue(limitRel.getOffset());
        }
        RexBuilder rexBuilder = limitRel.getCluster().getRexBuilder();
        RexNode fragmentLimitRex = rexBuilder.makeBigintLiteral(new BigDecimal(fragmentLimit));
        VoltDBPLimit fragmentLimitRel = limitRel.copy(
                limitRel.getTraitSet(),
                exchangeRel.getInput(),
                null,
                fragmentLimitRex,
                exchangeRel.getChildSplitCount());

        AbstractVoltDBPExchange newExchange = exchangeRel.copy(
                exchangeRel.getTraitSet(),
                fragmentLimitRel,
                exchangeRel.getDistribution(),
                exchangeRel.getLevel() + 1);

        // Coordinator's limit
        VoltDBPLimit coordinatorLimitRel = limitRel.copy(
                limitRel.getTraitSet(),
                newExchange,
                limitRel.getOffset(),
                limitRel.getLimit(),
                limitRel.getSplitCount());

        call.transformTo(coordinatorLimitRel);
//        // Remove the original rel from the search space
        call.getPlanner().setImportance(limitRel, 0);

    }
}
