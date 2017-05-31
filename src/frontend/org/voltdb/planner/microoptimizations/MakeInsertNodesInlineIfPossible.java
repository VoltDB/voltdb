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
package org.voltdb.planner.microoptimizations;

import org.voltdb.planner.ScanPlanNodeWhichCanHaveInlineInsert;
import org.voltdb.plannodes.AbstractPlanNode;
import org.voltdb.plannodes.InsertPlanNode;

public class MakeInsertNodesInlineIfPossible extends MicroOptimization {

    @Override
    protected AbstractPlanNode recursivelyApply(AbstractPlanNode plan) {
        AbstractPlanNode answer = null;
        for (AbstractPlanNode node = plan, child = null;
                node != null;
                node = child) {
            child = (node.getChildCount() > 0) ? node.getChild(0) : null;
            /*
             * Look for an insert node whose (first) child is
             * a ScanPlanNodeWhichCanHaveInlineInsert.
             */
            if (node instanceof InsertPlanNode) {
                InsertPlanNode insertNode = (InsertPlanNode)node;
                ScanPlanNodeWhichCanHaveInlineInsert targetNode
                  = (child instanceof ScanPlanNodeWhichCanHaveInlineInsert)
                        ? ((ScanPlanNodeWhichCanHaveInlineInsert)child)
                        : null;
                // If we have a sequential scan node without an inline aggregate
                // node, which is also not an then we can inline the insert node.
                if (child != null
                        && ( targetNode != null )
                        && ( ! insertNode.isUpsert())
                        && ( ! targetNode.hasInlineAggregateNode())) {
                    AbstractPlanNode parent = (insertNode.getParentCount() > 0) ? insertNode.getParent(0) : null;
                    targetNode.addInlinePlanNode(insertNode);
                    if (parent != null) {
                        parent.clearChildren();
                        targetNode.getAbstractNode().clearParents();
                        parent.addAndLinkChild(targetNode.getAbstractNode());
                    } else {
                        answer = targetNode.getAbstractNode();
                    }
                }
            }
        }
        if (answer != null) {
            return answer;
        }
        return plan;
    }

    @Override
    MicroOptimizationRunner.Phases getPhase() {
        return MicroOptimizationRunner.Phases.AFTER_BEST_SELECTION;
    }
}
