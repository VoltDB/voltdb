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
import org.apache.calcite.rel.RelNode;

import org.voltdb.calciteadapter.rel.physical.VoltDBLimit;
import org.voltdb.calciteadapter.rel.physical.VoltDBSort;

public class VoltDBLimitSortMergeRule extends RelOptRule {

    public static final VoltDBLimitSortMergeRule INSTANCE = new VoltDBLimitSortMergeRule();

    private VoltDBLimitSortMergeRule() {
        super(operand(VoltDBLimit.class,
                operand(VoltDBSort.class, any())));
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        VoltDBLimit limitOffset = call.rel(0);
        VoltDBSort sort = call.rel(1);

        RelNode newSort = sort.copy(
                sort.getTraitSet(),
                sort.getInput(),
                sort.getCollation(),
                limitOffset.getOffset(),
                limitOffset.getLimit());
        call.transformTo(newSort);
    }

}
