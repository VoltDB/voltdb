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

import java.util.Map;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperandChildPolicy;
import org.apache.calcite.plan.RelOptRuleOperandChildren;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.RelNode;
import org.voltdb.calciteadapter.rel.physical.AbstractVoltDBPExchange;
import org.voltdb.calciteadapter.rel.physical.VoltDBPMergeExchange;
import org.voltdb.calciteadapter.rel.physical.VoltDBPSingletonExchange;
import org.voltdb.calciteadapter.rel.physical.VoltDBPSort;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

/**
 * Transform Sort / Exchange rels into
 *  a) Singleton Merge Exchange / Sort if the original Exchange relation is a Union or Merge Exchanges
 *  b) Singleton Exchange / Sort if the original Exchange relation is a Singleton
 */
public class VoltDBPSortExchangeTransposeRule extends RelOptRule {

    public static final VoltDBPSortExchangeTransposeRule INSTANCE= new VoltDBPSortExchangeTransposeRule();

    private VoltDBPSortExchangeTransposeRule() {
        super(
                operand(
                        VoltDBPSort.class,
                        RelDistributions.ANY,
                        new RelOptRuleOperandChildren(
                                RelOptRuleOperandChildPolicy.ANY,
                                ImmutableList.of(
                                        operand(AbstractVoltDBPExchange.class, any()))))
                );
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        VoltDBPSort sortRel = call.rel(0);
        assert(sortRel != null);
        AbstractVoltDBPExchange exchangeRel = call.rel(1);

        VoltDBPSort newSortRel = sortRel.copy(
                // Update Limit distribution's trait
                sortRel.getTraitSet().plus(exchangeRel.getChildDistribution()),
                exchangeRel.getInput(),
                sortRel.getCollation(),
                sortRel.offset,
                sortRel.fetch,
                exchangeRel.getChildSplitCount());

        RelNode result = transposeExchange(exchangeRel, newSortRel);

        // Not only is newExchane equivalent to sort;
        // newSort is equivalent to exchangeRel's input
        Map<RelNode, RelNode> equiv;
        if (newSortRel.getCluster().getPlanner().getRelTraitDefs()
                .contains(RelCollationTraitDef.INSTANCE)) {
            equiv = ImmutableMap.of((RelNode) newSortRel, exchangeRel.getInput());
        } else {
            equiv = ImmutableMap.of();
        }

        call.transformTo(result, equiv);
        // Ideally, this rule should work without the next line but...
        // If we don't set the impotence of the original Sort expression to 0
        // the compilation of the following simple SQL
        // select si, i from P1 order by si
        // would go into an infinite loop. This rule would keep firing matching the same VoltDBPSort
        // expression somehow keep adding the new Exchange into its child result set.
        // I suspect it has something to do with the Collation Trait that Calcite treats specially.
        // If I add an additional VoltDBPSort on top of an Exchange node, the query would be compiled.
        //
        // Remove the original rel from the search space
        call.getPlanner().setImportance(sortRel, 0);

    }

    private RelNode transposeExchange(AbstractVoltDBPExchange exchangeRel, VoltDBPSort sortRel) {
        // The new exchange that will be sitting above the Sort relation must have sort's collation trait
        // since the top relation is required to have collation matching the sort's one
        RelTraitSet newExchangeTraits = exchangeRel.getTraitSet().replace(sortRel.getCollation());
        AbstractVoltDBPExchange newExchange = null;
        if (exchangeRel instanceof VoltDBPSingletonExchange) {
            newExchange = exchangeRel.copy(
                    newExchangeTraits,
                    sortRel,
                    exchangeRel.getChildDistribution(),
                    exchangeRel.getLevel() + 1);
            return newExchange;
        } else {
            VoltDBPMergeExchange mergeExchange = new VoltDBPMergeExchange(
                    exchangeRel.getCluster(),
                    newExchangeTraits,
                    sortRel,
                    exchangeRel.getChildDistribution(),
                    exchangeRel.getChildSplitCount(),
                    exchangeRel.getLevel() + 1,
                    sortRel.getChildExps());
            // Add a SingletonExchange on top of it to be propagated all the way to the root
            // The relations that will be transposed with the Singleton Exchange represent
            // the coordinator's nodes in the final VoltDB plan
            // and can not / should not be pushed beyond the VoltDBPMergeExchange exchange
            newExchange = new VoltDBPSingletonExchange(
                        exchangeRel.getCluster(),
                        newExchangeTraits,
                        mergeExchange,
                        mergeExchange.getLevel() + 1);
            return newExchange;
        }
    }

}
