/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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
import java.util.List;
import java.util.Queue;

import org.voltdb.plannodes.AbstractPlanNode;
import org.voltdb.plannodes.AbstractScanPlanNode;
import org.voltdb.plannodes.AggregatePlanNode;
import org.voltdb.plannodes.OrderByPlanNode;
import org.voltdb.plannodes.ReceivePlanNode;
import org.voltdb.types.PlanNodeType;

public class ReplaceOrderBy extends MicroOptimization {

    @Override
    protected AbstractPlanNode recursivelyApply(AbstractPlanNode planNode)
    {
        assert(planNode != null);

        Queue<AbstractPlanNode> children = new LinkedList<AbstractPlanNode>();
        children.add(planNode);

        while(!children.isEmpty()) {
            AbstractPlanNode plan = children.remove();
            if (PlanNodeType.RECEIVE == plan.getPlanNodeType()) {
                // continue. We are after the coordinator ORDER BY node.
                return planNode;
            } else if (PlanNodeType.ORDERBY == plan.getPlanNodeType()) {
                assert(plan instanceof OrderByPlanNode);
                AbstractPlanNode newPlan = applyOptimization((OrderByPlanNode)plan);
                if (newPlan != plan) {
                    // Only one coordinator ORDER BY node is possible
                    if (plan == planNode) {
                        return newPlan;
                    } else {
                        return planNode;
                    }
                }
            }

            for (int i = 0; i < plan.getChildCount(); i++) {
                children.add(plan.getChild(i));
            }
        }
        return planNode;
    }

    /** For MP queries, the coordinator's OrderBy node can be replaced with
     * a specialized Receive node that merges individual partitions results
     * into a final result set. The preconditions for that are:
     *  - partition result set is sorted in the order matching the ORDER BY order
     *  - no aggregation at the coordinator node
     *  If optimization is possible the returned plan needs to be reconnected with the parent
     *
     * @param orderbyNode - ORDER BY node to optimize
     * @return optimized plan
     */
    AbstractPlanNode applyOptimization(OrderByPlanNode orderbyNode) {
        // For MP queries, the coordinator's OrderBy node can be replaced with
        // a specialized Receive node that merges individual partitions results
        // into a final result set. The preconditions for that are:
        //  - partition result set is sorted in the order matching the ORDER BY order
        //  - no aggregation at the coordinator node
        List<AbstractPlanNode> receives = orderbyNode.findAllNodesOfType(PlanNodeType.RECEIVE);
        if (receives.isEmpty()) {
            return orderbyNode;
        }
        assert(receives.size() == 1);

        ReceivePlanNode receive = (ReceivePlanNode)receives.get(0);
        // Make sure that this receive node belongs to the same coordinator as
        // the ORDER BY does. It could belong to a distributed subquery instead.
        // Walk up the tree starting at the receive node until we hit either a scan node
        // (distributed subquery) or the original order by node (distributed order by)
        AbstractPlanNode nextParent = receive.getParent(0);
        while (orderbyNode != nextParent) {
            if (nextParent instanceof AbstractScanPlanNode) {
                // it's a subquery
                return orderbyNode;
            }
            nextParent = nextParent.getParent(0);
        }
        assert(receive.getChildCount() == 1);
        assert(receive.getChild(0).getChildCount() == 1);
        AbstractPlanNode partitionRoot = receive.getChild(0).getChild(0);
        if (partitionRoot.isOutputOrdered()) {
            // Partition results are ordered
            List<AbstractPlanNode> aggs = orderbyNode.findAllNodesOfClass(AggregatePlanNode.class);
            for(AbstractPlanNode agg : aggs) {
                if (((AggregatePlanNode)agg).m_isCoordinatingAggregator) {
                    // Top level aggregation. Not supported at the moment
                    return orderbyNode;
                }
            }
        } else {
            // Partition results are not ordered
            return orderbyNode;
        }

        // Get the new root
        assert(orderbyNode.getChildCount() == 1);
        AbstractPlanNode newRoot = orderbyNode.getChild(0);
        // Get the ORDER BY parent if such exists
        assert (orderbyNode.getParentCount() <= 1);
        AbstractPlanNode parent = (orderbyNode.getParentCount() == 1) ? orderbyNode.getParent(0) : null;
        // Disconnect the order by node
        orderbyNode.disconnectParents();
        orderbyNode.disconnectChildren();
        // Convert the receive node to the merge receive and inline the order by and limit nodes
        receive.setMergeReceive(true);
        receive.addInlinePlanNode(orderbyNode);
        // Inline limit
        AbstractPlanNode limit = orderbyNode.getInlinePlanNode(PlanNodeType.LIMIT);
        if (limit != null) {
            receive.addInlinePlanNode(limit);
            orderbyNode.removeInlinePlanNode(PlanNodeType.LIMIT);
        }
        // Connect parent
        if (parent != null) {
            parent.addAndLinkChild(receive);
        }
        // return the new root
        return newRoot;
    }

}
