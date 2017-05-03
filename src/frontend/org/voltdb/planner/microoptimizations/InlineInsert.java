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

import org.voltdb.planner.ParsedInsertStmt;
import org.voltdb.plannodes.AbstractJoinPlanNode;
import org.voltdb.plannodes.AbstractPlanNode;
import org.voltdb.plannodes.AbstractScanPlanNode;
import org.voltdb.plannodes.AggregatePlanNode;
import org.voltdb.plannodes.InsertPlanNode;
import org.voltdb.plannodes.ProjectionPlanNode;
import org.voltdb.plannodes.SeqScanPlanNode;

public class InlineInsert extends MicroOptimization {

    @Override
    protected AbstractPlanNode recursivelyApply(AbstractPlanNode planNode) {
        AbstractPlanNode workingNode = planNode;
        //
        // Look for an insert node, but stop if we see a
        // scan or join node.  We are going to refuse to
        //
        // nodes as well.
        //
        boolean sawAgg = false;
        boolean sawProjection = false;
        while((! (workingNode instanceof InsertPlanNode))
                  && ( ! (workingNode instanceof AbstractScanPlanNode))
                  && ( ! (workingNode instanceof AbstractJoinPlanNode))
                  && (workingNode.getChildCount() > 0)) {
            if ((workingNode instanceof AggregatePlanNode)
                    || (workingNode instanceof ProjectionPlanNode)) {
                // We can't have an inline insert node with an
                // aggregate node. or a non-line projection node.
                return planNode;
            }
            workingNode = workingNode.getChild(0);
        }
        //
        // If we found an insert node, and it's just before a sequential
        // scan node, then yank it out of the tree and put the insert
        // node into the sequential scan node.
        if (workingNode instanceof InsertPlanNode) {
            AbstractPlanNode child = (workingNode.getChildCount() > 0) ? workingNode.getChild(0) : null;
            if ((child != null)
                    && (child instanceof SeqScanPlanNode)) {
                SeqScanPlanNode seqScanNode = (SeqScanPlanNode)child;
                // We can't have an inline insert node with an
                // inline aggregate node right yet.  It may be
                // possible, but not now.
                if (AggregatePlanNode.getInlineAggregationNode(seqScanNode) == null) {
                    // Yank the working node and then
                    // push it into the sequential scan plan node.
                    workingNode.removeFromGraph();
                    seqScanNode.addInlinePlanNode(workingNode);
                    if (workingNode == planNode) {
                        planNode = seqScanNode;
                    }
                }
            }
        }
        return planNode;
    }
}
