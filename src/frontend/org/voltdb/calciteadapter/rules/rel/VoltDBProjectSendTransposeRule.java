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
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.voltdb.calciteadapter.rel.LogicalSend;

public class VoltDBProjectSendTransposeRule extends RelOptRule {

    public static final VoltDBProjectSendTransposeRule INSTANCE = new VoltDBProjectSendTransposeRule();

    private VoltDBProjectSendTransposeRule() {
        super(operand(Project.class, operand(LogicalSend.class, none())));
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        Project project = call.rel(0);
        LogicalSend send = call.rel(1);

        RelNode sendInput = send.getInput();
        Project newProjectRel = project.copy(project.getTraitSet(), sendInput, project.getProjects(), project.getRowType());
        LogicalSend newSend = (LogicalSend) send.copy(newProjectRel, send.getLevel() + 1);
        call.transformTo(newSend);
    }

}