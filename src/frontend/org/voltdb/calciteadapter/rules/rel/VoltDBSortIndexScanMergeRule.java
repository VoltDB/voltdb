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
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rex.RexNode;
import org.voltcore.utils.Pair;
import org.voltdb.calciteadapter.rel.VoltDBTableIndexScan;
import org.voltdb.calciteadapter.voltdb.RexCollationUtil;
import org.voltdb.types.SortDirectionType;

public class VoltDBSortIndexScanMergeRule extends RelOptRule {

    public static final VoltDBSortIndexScanMergeRule INSTANCE = new VoltDBSortIndexScanMergeRule();

    private VoltDBSortIndexScanMergeRule() {
        super(operand(Sort.class, operand(VoltDBTableIndexScan.class, none())));
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        Sort sort = call.rel(0);
        VoltDBTableIndexScan scan = call.rel(1);
        RelCollation sortCollation = sort.getCollation();
        RelCollation scanCollation = scan.getCollation();
        if (sortCollation == RelCollations.EMPTY) {
            // Inline LIMIT/OFFSET if present
            if (sort.offset != null || sort.fetch != null) {
                RelNode newScan = scan.copyWithLimitOffset(sort.fetch, sort.offset);
                call.transformTo(newScan);
            }
            return;
        } else if (scanCollation == RelCollations.EMPTY) {
            // Index is not scannable. Sort is required
            return;
        }
        Pair<SortDirectionType, Boolean> collationInfo =
                RexCollationUtil.areCollationsCompartible(sortCollation, scanCollation);

        if (collationInfo.getFirst() != SortDirectionType.INVALID) {
            // Push down the sort data (limit and offset). The sort collation is redundant
            RexNode offset = sort.offset;
            RexNode fetch = sort.fetch;
            RelNode newScan = scan.copyWithLimitOffset(fetch, offset);
            assert(newScan instanceof VoltDBTableIndexScan);
            VoltDBTableIndexScan newIndexScan = (VoltDBTableIndexScan)newScan;
            if (collationInfo.getSecond() == true) {
                newIndexScan.setCollation(RexCollationUtil.reverseCollation(scanCollation));
            }
            newIndexScan.setSortDirection(collationInfo.getFirst());
            call.transformTo(newScan);
        }
    }

}