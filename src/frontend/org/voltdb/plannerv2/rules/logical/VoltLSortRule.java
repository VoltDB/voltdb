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

package org.voltdb.plannerv2.rules.logical;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalSort;
import org.voltdb.plannerv2.rel.logical.VoltLogicalLimit;
import org.voltdb.plannerv2.rel.logical.VoltLogicalRel;
import org.voltdb.plannerv2.rel.logical.VoltLogicalSort;

/**
 * VoltDB logical rule that transform {@link LogicalSort} to
 * {@link VoltLogicalSort} or {@link VoltLogicalLimit}.
 *
 * @author Michael Alexeev
 * @since 9.0
 */
public class VoltLSortRule extends RelOptRule {

    public static final VoltLSortRule INSTANCE = new VoltLSortRule();

    // declare want kind of operand we should match
    VoltLSortRule() {
        super(operand(LogicalSort.class, Convention.NONE, any()));
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        LogicalSort sort = call.rel(0);
        RelNode input = sort.getInput();
        RelTraitSet convertedTraits = sort.getTraitSet().replace(VoltLogicalRel.CONVENTION);
        RelNode convertedInput = convert(input, input.getTraitSet().replace(VoltLogicalRel.CONVENTION));
        RelNode logicalRel = null;
        RelCollation sortCollation = sort.getCollation();
        if (!sortCollation.getFieldCollations().isEmpty()) {
            logicalRel = new VoltLogicalSort(
                    sort.getCluster(),
                    convertedTraits,
                    convertedInput,
                    sort.getCollation());
        }
        // Q: why split it to two phases? Why instead of single VoltDBLSort we need VoltDBLSort and VoltDBLLimit
        // Mike:
        // We split it into two res mainly for a couple of reasons:
        // 1.During physical phase the VoltDBLSort is transformed into a collation trait that Volcano Planner would propagate
        // as appropriate -see the VoltDBPSortRule implementation. It won't be possible if the sort would have limit / offset
        // as well
        // 2.The rules for pushing down Limit and Sort RelNodes differ. For example, Limit / Exchange is transformed
        // into Coordinator Limit / Exchange / Fragment Limit while Sort / Exchange potentially can be transformed to
        // Coordinator Sort / MergeExchange / fragment Sort.
        // 3.The Limit node can be inlined with Scan or Serial Aggregate nodes
        if (sort.offset != null || sort.fetch != null) {
            RelNode limitInput = (logicalRel != null) ? logicalRel : convertedInput;
            logicalRel = new VoltLogicalLimit(
                    sort.getCluster(),
                    convertedTraits,
                    limitInput,
                    sort.offset,
                    sort.fetch);
        }
        assert (logicalRel != null);
        call.transformTo(logicalRel);
    }
}
