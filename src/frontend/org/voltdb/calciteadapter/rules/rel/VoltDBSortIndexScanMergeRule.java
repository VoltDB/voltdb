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
import org.voltdb.calciteadapter.rel.VoltDBTableIndexScan;

public class VoltDBSortIndexScanMergeRule extends RelOptRule {

    public static final VoltDBSortIndexScanMergeRule INSTANCE = new VoltDBSortIndexScanMergeRule();

    private VoltDBSortIndexScanMergeRule() {
        super(operand(Sort.class, operand(VoltDBTableIndexScan.class, none())));
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        Sort sort = call.rel(0);
        VoltDBTableIndexScan scan = call.rel(1);

        RelCollation sortCollation = sort.getCollation();
        RelCollation scanCollation = scan.getCollation();
        boolean matches = areCollationsCompartible(scanCollation, sortCollation);
        return matches;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        Sort sort = call.rel(0);
        VoltDBTableIndexScan scan = call.rel(1);

        // Push down the sort data (limit and offset). The sort collation is redundant
        RexNode offset = sort.offset;
        RexNode fetch = sort.fetch;
        RelNode newScan = scan.copyWithLimitOffset(fetch, offset);
        call.transformTo(newScan);
    }

    private boolean areCollationsCompartible(RelCollation scanCollation, RelCollation sortCollation) {
      return sortCollation == RelCollations.EMPTY ||
              (scanCollation.getFieldCollations().size() == sortCollation.getFieldCollations().size()
              && sortCollation.satisfies(scanCollation));
  }

}