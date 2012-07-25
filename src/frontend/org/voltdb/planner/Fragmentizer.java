/* This file is part of VoltDB.
 * Copyright (C) 2008-2012 VoltDB Inc.
 *
 * VoltDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * VoltDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.voltdb.planner;

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
    static CompiledPlan fragmentize(CompiledPlan plan, Database db) {
        assert(plan != null);
        assert(plan.rootPlanGraph != null);
        assert(plan.subPlanGraph == null);

        // chop up the plan and set all the proper dependencies recursively
        recursiveFindFragment(plan, plan.rootPlanGraph);

        return plan;
    }

    /**
     * Chop up the plan and set all the proper dependencies recursively
     *
     * @param currentFragment The fragment currently being examined recurively. This
     * will likely contain the root of plan-subgraph currently being walked.
     * @param currentNode The node in the plan graph currently being visited. This is
     * the main unit of recursion here.
     * @param fragments The list of fragments (initially one) that will be appended to
     * as the method splits up the plan.
     */
    static void recursiveFindFragment(
            CompiledPlan plan,
            AbstractPlanNode currentNode) {

        // the place to split is the send-recv node pairing
        if (currentNode instanceof ReceivePlanNode) {
            ReceivePlanNode recvNode = (ReceivePlanNode) currentNode;
            assert(recvNode.getChildCount() == 1);
            AbstractPlanNode childNode = recvNode.getChild(0);
            assert(childNode instanceof SendPlanNode);
            SendPlanNode sendNode = (SendPlanNode) childNode;

            // disconnect the send and receive nodes
            sendNode.clearParents();
            recvNode.cacheDeterminism();
            recvNode.clearChildren();

            plan.subPlanGraph = sendNode;
            assert(plan.subPlanGraph.findAllNodesOfType(PlanNodeType.RECEIVE).size() == 0);

            // stop here if we found a recv node
            return;
        }

        // if not a recv node, just do a boring recursive call
        // stopping condition is when there are no children
        for (int i = 0; i < currentNode.getChildCount(); i++) {
            AbstractPlanNode childNode = currentNode.getChild(i);
            recursiveFindFragment(plan, childNode);
        }
    }
}
