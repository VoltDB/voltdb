/* This file is part of VoltDB.
 * Copyright (C) 2008-2014 VoltDB Inc.
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

package org.voltdb.planner;

import java.util.List;

import org.voltdb.catalog.Database;
import org.voltdb.plannodes.AbstractPlanNode;
import org.voltdb.plannodes.ReceivePlanNode;
import org.voltdb.plannodes.SendPlanNode;
import org.voltdb.types.PlanNodeType;

/**
 * Wrapper class for one static method that accepts a CompiledPlan
 * instance with one PlanFragment and it splits it up into a set of
 * PlanFragments. It splits plans anywhere it sees a Recv/Send-PlanNode
 * pair, and makes the RecvNode's fragment depend on the SendNode's
 * fragment.
 *
 */
public class Fragmentizer {

    /**
     * Static method that is the main entry point for chopping up a plan.
     * Note this will modify the plan in place as well as returning it.
     *
     * @param plan The plan to chop up.
     * @return The chopped up plan.
     */
    static void fragmentize(CompiledPlan plan, Database db) {
        List<AbstractPlanNode> receives = plan.rootPlanGraph.findAllNodesOfType(PlanNodeType.RECEIVE);

        if (receives.isEmpty()) return;

        assert (receives.size() == 1);

        ReceivePlanNode recvNode = (ReceivePlanNode) receives.get(0);
        assert(recvNode.getChildCount() == 1);
        AbstractPlanNode childNode = recvNode.getChild(0);
        assert(childNode instanceof SendPlanNode);
        SendPlanNode sendNode = (SendPlanNode) childNode;

        // disconnect the send and receive nodes
        sendNode.clearParents();
        recvNode.cacheDeterminism();
        recvNode.clearChildren();

        plan.subPlanGraph = sendNode;

        return;
    }
}
