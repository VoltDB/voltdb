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
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rex.RexProgram;
import org.voltdb.calciteadapter.rel.physical.VoltDBSort;
import org.voltdb.calciteadapter.rel.physical.VoltDBTableIndexScan;
import org.voltdb.calciteadapter.util.VoltDBRexUtil;
import org.voltdb.planner.AccessPath;
import org.voltdb.types.SortDirectionType;

public class VoltDBSortIndexScanRemoveRule extends RelOptRule {

    public static final VoltDBSortIndexScanRemoveRule INSTANCE = new VoltDBSortIndexScanRemoveRule();

    private VoltDBSortIndexScanRemoveRule() {
        super(operand(VoltDBSort.class,
                operand(VoltDBTableIndexScan.class, none())));
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        VoltDBSort sort = call.rel(0);
        RelCollation sortCollation = sort.getCollation();

        VoltDBTableIndexScan scan = call.rel(1);
        RexProgram program =  scan.getProgram();
        assert(program != null);

        RelCollation indexCollation = scan.getTraitSet().getTrait(RelCollationTraitDef.INSTANCE);
        SortDirectionType sortDirection =
                 VoltDBRexUtil.areCollationsCompartible(sortCollation, indexCollation);

        if (SortDirectionType.INVALID != sortDirection) {
            // Update scan's sort direction
            AccessPath accessPath = scan.getAccessPath();
            accessPath.setSortDirection(sortDirection);

            VoltDBTableIndexScan newScan = VoltDBTableIndexScan.create(
                    scan.getCluster(),
                    // IndexScan already have a collation trait, so replace it
                    scan.getTraitSet().replace(sortCollation),
                    scan.getTable(),
                    scan.getVoltDBTable(),
                    scan.getProgram(),
                    scan.getIndex(),
                    accessPath,
                    scan.getOffsetRexNode(),
                    scan.getLimitRexNode()
                    );
            call.transformTo(newScan);
        }
    }

}
