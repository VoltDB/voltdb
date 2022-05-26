/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
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
package org.voltdb.planner.microoptimizations;

import org.voltdb.planner.AbstractParsedStmt;
import org.voltdb.planner.ScanPlanNodeWhichCanHaveInlineInsert;
import org.voltdb.plannodes.AbstractPlanNode;
import org.voltdb.plannodes.InsertPlanNode;

public class MakeInsertNodesInlineIfPossible extends MicroOptimization {

    @Override
    protected AbstractPlanNode recursivelyApply(AbstractPlanNode plan, AbstractParsedStmt parsedStmt) {
        return recursivelyApply(plan, -1);
    }

    /**
     * This helper function is called when we recurse down the childIdx-th
     * child of a parent node.
     * @param plan
     * @param parentIdx
     * @return
     */
    private AbstractPlanNode recursivelyApply(AbstractPlanNode plan, int childIdx) {
        // If this is an insert plan node, then try to
        // inline it.  There will only ever by one insert
        // node, so if we can't inline it we just return the
        // given plan.
        if (plan instanceof InsertPlanNode) {
            InsertPlanNode insertNode = (InsertPlanNode)plan;
            assert(insertNode.getChildCount() == 1);
            AbstractPlanNode abstractChild = insertNode.getChild(0);
            ScanPlanNodeWhichCanHaveInlineInsert targetNode
                  = (abstractChild instanceof ScanPlanNodeWhichCanHaveInlineInsert)
                        ? ((ScanPlanNodeWhichCanHaveInlineInsert)abstractChild)
                        : null;
            // If we have a sequential/index scan node without an inline aggregate
            // node then we can inline the insert node.
            if ( targetNode != null
                 && ! insertNode.isUpsert()
                 && ! targetNode.hasInlineAggregateNode()
                 // If INSERT INTO and SELECT FROM have the same target table name,
                 // then it could be a recursive insert into select.
                 // Currently, our scan executor implementations cannot handle it well. (ENG-13036)
                 && ! targetNode.getTargetTableName().equalsIgnoreCase(insertNode.getTargetTableName()) ) {
                AbstractPlanNode parent = (insertNode.getParentCount() > 0) ? insertNode.getParent(0) : null;
                AbstractPlanNode abstractTargetNode = targetNode.getAbstractNode();
                abstractTargetNode.addInlinePlanNode(insertNode);
                // Don't call removeFromGraph.  That
                // screws up the order of the children.
                insertNode.clearChildren();
                insertNode.clearParents();
                // Remvoe all the abstractTarget node's parents.
                // It used to be the insertNode, which is now
                // dead to us.
                abstractTargetNode.clearParents();
                if (parent != null) {
                    parent.setAndLinkChild(childIdx, abstractTargetNode);
                }
                plan = abstractTargetNode;
            }
            return plan;
        }
        for (int idx = 0; idx < plan.getChildCount(); idx += 1) {
            AbstractPlanNode child = plan.getChild(idx);
            recursivelyApply(child, idx);
        }
        return plan;
    }

    @Override
    MicroOptimizationRunner.Phases getPhase() {
        return MicroOptimizationRunner.Phases.AFTER_COMPLETE_PLAN_ASSEMBLY;
    }
}
