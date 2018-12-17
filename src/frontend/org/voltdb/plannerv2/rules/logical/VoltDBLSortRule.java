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

package org.voltdb.plannerv2.rules.logical;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalSort;
import org.voltdb.plannerv2.rel.logical.VoltDBLLimit;
import org.voltdb.plannerv2.rel.logical.VoltDBLRel;
import org.voltdb.plannerv2.rel.logical.VoltDBLSort;

/**
 * VoltDB logical rule that transform {@link LogicalSort} to {@link VoltDBLSort} or {@link VoltDBLLimit}.
 *
 * @author Michael Alexeev
 * @since 8.4
 */
public class VoltDBLSortRule extends RelOptRule {

    public static final VoltDBLSortRule INSTANCE = new VoltDBLSortRule();

    // declare want kind of operand we should match
    VoltDBLSortRule() {
        super(operand(LogicalSort.class, Convention.NONE, any()));
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        LogicalSort sort = call.rel(0);
        RelNode input = sort.getInput();
        RelTraitSet convertedTraits = sort.getTraitSet().replace(VoltDBLRel.VOLTDB_LOGICAL);
        RelNode convertedInput = convert(input, input.getTraitSet().replace(VoltDBLRel.VOLTDB_LOGICAL));
        RelNode logicalRel = null;
        RelCollation sortCollation = sort.getCollation();
        if (!sortCollation.getFieldCollations().isEmpty()) {
            logicalRel = new VoltDBLSort(
                    sort.getCluster(),
                    convertedTraits,
                    convertedInput,
                    sort.getCollation());
        }
        // Q: why split it to two phases? why instead of single VoltDBLSort we need VoltDBLSort and VoltDBLLimit
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
            logicalRel = new VoltDBLLimit(
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
