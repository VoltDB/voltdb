/* This file is part of VoltDB.
 * Copyright (C) 2008-2017 VoltDB Inc.
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

package org.voltdb.calciteadapter.rules.rel;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rex.RexNode;
import org.voltdb.calciteadapter.rel.VoltDBTableScan;

public class VoltDBSortScanMergeRule extends RelOptRule {

    public static final VoltDBSortScanMergeRule INSTANCE = new VoltDBSortScanMergeRule();

    private VoltDBSortScanMergeRule() {
        super(operand(Sort.class, operand(VoltDBTableScan.class, none())));
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        Sort sort = call.rel(0);
        return (sort.offset != null || sort.fetch != null);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        Sort sort = call.rel(0);
        VoltDBTableScan scan = call.rel(1);

        RexNode offset = sort.offset;
        RexNode fetch = sort.fetch;

        // @TODO need to add collations
        VoltDBTableScan newScan = (VoltDBTableScan) scan.copyWithLimitOffset(fetch, offset);
        call.transformTo(newScan);
    }
}