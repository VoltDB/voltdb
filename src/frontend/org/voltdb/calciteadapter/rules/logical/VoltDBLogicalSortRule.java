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

package org.voltdb.calciteadapter.rules.logical;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalSort;
import org.voltdb.calciteadapter.rel.logical.VoltDBLogicalLimit;
import org.voltdb.calciteadapter.rel.logical.VoltDBLogicalRel;
import org.voltdb.calciteadapter.rel.logical.VoltDBLogicalSort;

public class VoltDBLogicalSortRule extends RelOptRule {

        public static final VoltDBLogicalSortRule INSTANCE = new VoltDBLogicalSortRule();

        VoltDBLogicalSortRule() {
            super(operand(LogicalSort.class, Convention.NONE, any()));
        }

        @Override
        public void onMatch(RelOptRuleCall call) {
            LogicalSort sort = (LogicalSort) call.rel(0);
            RelNode input = sort.getInput();
            RelTraitSet convertedTraits = sort.getTraitSet().replace(VoltDBLogicalRel.VOLTDB_LOGICAL);
            RelNode convertedInput = convert(input, input.getTraitSet().replace(VoltDBLogicalRel.VOLTDB_LOGICAL));
            RelNode logicalRel = null;
            RelCollation sortCollation = sort.getCollation();
            if (!sortCollation.getFieldCollations().isEmpty()) {
                logicalRel = new VoltDBLogicalSort(
                        sort.getCluster(),
                        convertedTraits,
                        convertedInput,
                        sort.getCollation());
            }
            if (sort.offset != null || sort.fetch != null) {
                RelNode limitInput = (logicalRel != null) ? logicalRel : convertedInput;
                logicalRel = new VoltDBLogicalLimit(
                        sort.getCluster(),
                        convertedTraits,
                        limitInput,
                        sort.offset,
                        sort.fetch);
            }
            assert(logicalRel != null);
            call.transformTo(logicalRel);
        }
}
