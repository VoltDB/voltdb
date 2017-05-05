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

import java.util.List;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rex.RexNode;
import org.voltdb.calciteadapter.rel.VoltDBTableScan;

public class VoltDBSortScanMergeRule extends RelOptRule {

    public static final VoltDBSortScanMergeRule INSTANCE = new VoltDBSortScanMergeRule();

    private VoltDBSortScanMergeRule() {
        super(operand(LogicalSort.class, operand(VoltDBTableScan.class, none())));
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        LogicalSort sort = call.rel(0);
        return (sort.offset != null || sort.fetch != null);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        LogicalSort sort = call.rel(0);
        VoltDBTableScan scan = call.rel(1);

        RexNode offset = sort.offset;
        RexNode fetch = sort.fetch;

        // @TODO copy doesn't work????
        VoltDBTableScan newScan = (VoltDBTableScan) scan.copy(fetch, offset);
        List<RelFieldCollation> collations = sort.collation.getFieldCollations();
        if (collations == null || collations.isEmpty()) {
            // Has only LIMIT/OFFSET
            newScan.setLimit(fetch);
            newScan.setOffset(offset);
            call.transformTo(newScan);
        } else {
            // sort.copy
            // @TODO Deal with ORDER BY
        }

//        call.transformTo(scan.copy(calc.getProgram()));
    }
}