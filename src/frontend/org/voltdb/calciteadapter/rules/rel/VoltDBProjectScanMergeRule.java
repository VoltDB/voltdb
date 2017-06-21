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
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgramBuilder;
import org.apache.calcite.util.Pair;
import org.voltdb.calciteadapter.rel.AbstractVoltDBTableScan;
import org.voltdb.calciteadapter.rel.VoltDBProject;

public class VoltDBProjectScanMergeRule extends RelOptRule {

    public static final VoltDBProjectScanMergeRule INSTANCE = new VoltDBProjectScanMergeRule();

    private VoltDBProjectScanMergeRule() {
        super(operand(VoltDBProject.class, operand(AbstractVoltDBTableScan.class, none())));
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        VoltDBProject proj= call.rel(0);
        AbstractVoltDBTableScan scan = call.rel(1);

        RexBuilder rexBuilder = proj.getCluster().getRexBuilder();
        RexProgramBuilder rpb = new RexProgramBuilder(scan.getRowType(), rexBuilder);

        for (Pair<RexNode, String> item : proj.getNamedProjects()) {
            rpb.addProject(item.left, item.right);
        }
        call.transformTo(AbstractVoltDBTableScan.copy(scan, rpb.getProgram(), rexBuilder));
    }

}