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

package org.voltdb.calciteadapter.rules.physical;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.rex.RexProgramBuilder;
import org.voltdb.calciteadapter.rel.physical.AbstractVoltDBPhysicalTableScan;
import org.voltdb.calciteadapter.rel.physical.VoltDBFilter;

public class VoltDBFilterScanMergeRule extends RelOptRule {

    public static final VoltDBFilterScanMergeRule INSTANCE = new VoltDBFilterScanMergeRule();

    private VoltDBFilterScanMergeRule() {
        super(operand(VoltDBFilter.class,
                operand(AbstractVoltDBPhysicalTableScan.class, none())));
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        VoltDBFilter filter= call.rel(0);
        AbstractVoltDBPhysicalTableScan scan = call.rel(1);

        // Create a program containing the filter.
        final RexBuilder rexBuilder = filter.getCluster().getRexBuilder();
        final RexProgramBuilder progBuilder =
            new RexProgramBuilder( scan.getRowType(), rexBuilder);
        progBuilder.addIdentity();
        progBuilder.addCondition(filter.getCondition());
        RexProgram topProgram = progBuilder.getProgram();
        RexProgram bottomProgram = scan.getProgram();

        // Merge the programs together.
        RexProgram mergedProgram =
            RexProgramBuilder.mergePrograms(
                topProgram,
                bottomProgram,
                rexBuilder);

        RelNode newScan = scan.copy(mergedProgram, rexBuilder);
        call.transformTo(newScan);
    }

}