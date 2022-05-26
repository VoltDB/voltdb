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

import java.util.LinkedList;
import java.util.Queue;

import org.voltdb.planner.AbstractParsedStmt;
import org.voltdb.plannodes.AbstractPlanNode;
import org.voltdb.plannodes.AggregatePlanNode;
import org.voltdb.plannodes.IndexScanPlanNode;
import org.voltdb.plannodes.LimitPlanNode;
import org.voltdb.plannodes.WindowFunctionPlanNode;
import org.voltdb.types.PlanNodeType;

public class InlineAggregation extends MicroOptimization {

    @Override
    protected AbstractPlanNode recursivelyApply(AbstractPlanNode planNode, AbstractParsedStmt parsedStmt)
    {
        assert(planNode != null);

        // breadth first:
        //     find AggregatePlanNode with exactly one child
        //     where that child is an AbstractScanPlanNode.
        //     Inline any qualifying AggregatePlanNode to its AbstractScanPlanNode.

        Queue<AbstractPlanNode> children = new LinkedList<AbstractPlanNode>();
        children.add(planNode);

        while(!children.isEmpty()) {
            AbstractPlanNode plan = children.remove();
            AbstractPlanNode newPlan = inlineAggregationApply(plan);
            if (newPlan != plan) {
                if (plan == planNode) {
                    planNode = newPlan;
                } else {
                    planNode.replaceChild(plan, newPlan);
                }
            }

            for (int i = 0; i < newPlan.getChildCount(); i++) {
                children.add(newPlan.getChild(i));
            }
        }

        return planNode;
    }

    AbstractPlanNode inlineAggregationApply(AbstractPlanNode plan) {
        // check for an aggregation of the right form
        if ( ! (plan instanceof AggregatePlanNode) ) {
            return plan;
        }
        assert(plan.getChildCount() == 1);
        AggregatePlanNode aggplan = (AggregatePlanNode)plan;

        // Assuming all AggregatePlanNode has not been inlined before this microoptimization
        AbstractPlanNode child = aggplan.getChild(0);

        // EE Currently support: seqscan + indexscan
        if (child.getPlanNodeType() != PlanNodeType.SEQSCAN &&
            child.getPlanNodeType() != PlanNodeType.INDEXSCAN &&
            child.getPlanNodeType() != PlanNodeType.NESTLOOP &&
            child.getPlanNodeType() != PlanNodeType.NESTLOOPINDEX) {
            return plan;
        }

        if (child.getPlanNodeType() == PlanNodeType.INDEXSCAN) {
            // Currently do not conflict with the optimized MIN/MAX
            // because of the big amount of tests changed.

            IndexScanPlanNode isp = (IndexScanPlanNode)child;
            LimitPlanNode limit = (LimitPlanNode)isp.getInlinePlanNode(PlanNodeType.LIMIT);
            if (limit != null && (aggplan.isTableMin() || aggplan.isTableMax())) {
                // Optimized MIN/MAX
                if (limit.getLimit() == 1 && limit.getOffset() == 0) {
                    return plan;
                }
            }
        }

        // Inline aggregate node
        AbstractPlanNode parent = null;
        if (aggplan.getParentCount() == 1) {
            parent = aggplan.getParent(0);
        }
        child.addInlinePlanNode(aggplan);
        child.clearParents();
        if (parent != null) {
            parent.replaceChild(aggplan, child);
        }
        return child;
    }

}
