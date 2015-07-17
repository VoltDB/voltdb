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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

import org.voltdb.expressions.AbstractExpression;
import org.voltdb.plannodes.AbstractPlanNode;
import org.voltdb.plannodes.AbstractScanPlanNode;
import org.voltdb.plannodes.AggregatePlanNode;
import org.voltdb.plannodes.HashAggregatePlanNode;
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
        // Make sure that this receive node belongs to the same coordinator fragment as
        // the ORDER BY does. Alternatively, it could belong to a distributed subquery.
        // Walk up the tree starting at the receive node until we hit either a scan node
        // (distributed subquery) or the original order by node (distributed order by)
        // Collect all nodes that are currently in between ORDER BY and RECEIVE nodes
        // If the optimization is possible, they will be converted to inline nodes of
        // the MERGE RECEIVE node. The expected node types are:
        // LIMIT, AGGREGATE/PARTIALAGGREGATE/HASHAGGREGATE
        // The HASHAGGREGATE must be convertible to AGGREGATE or PARTIALAGGREGATE for optimization
        // to be applicable. The Projection node sitting on top of the Aggregation is not supported yet.
        Map<PlanNodeType, AbstractPlanNode> inlineCandidates = new HashMap<PlanNodeType, AbstractPlanNode>();
        AbstractPlanNode inlineCandidate = receive.getParent(0);
        inlineCandidates.put(orderbyNode.getPlanNodeType(), orderbyNode);
        while (orderbyNode != inlineCandidate) {
            if (inlineCandidate instanceof AbstractScanPlanNode) {
                // it's a subquery
                return orderbyNode;
            }
            PlanNodeType nodeType = inlineCandidate.getPlanNodeType();
            if (nodeType == PlanNodeType.HASHAGGREGATE) {
                AbstractPlanNode newAggr = convertToSerialAggregation(inlineCandidate, orderbyNode);
                if (newAggr.getPlanNodeType() == PlanNodeType.HASHAGGREGATE) {
                    return orderbyNode;
                }
                inlineCandidates.put(newAggr.getPlanNodeType(), newAggr);
                assert(inlineCandidate.getParentCount() ==1);
                inlineCandidate = inlineCandidate.getParent(0);
            } else if ((nodeType == PlanNodeType.AGGREGATE || nodeType == PlanNodeType.PARTIALAGGREGATE
                    || nodeType == PlanNodeType.LIMIT) && !inlineCandidates.containsKey(nodeType)) {
                inlineCandidates.put(nodeType, inlineCandidate);
                assert(inlineCandidate.getParentCount() ==1);
                inlineCandidate = inlineCandidate.getParent(0);
            } else {
                // Don't know how to handle this node or there is already node
                // of this type. Aborting the optimization
                return orderbyNode;
            }
        }

        assert(receive.getChildCount() == 1);
        assert(receive.getChild(0).getChildCount() == 1);
        AbstractPlanNode partitionRoot = receive.getChild(0).getChild(0);
        if (!partitionRoot.isOutputOrdered()) {
            // Partition results are not ordered
            return orderbyNode;
        }

        // Short circuit the current ORDER BY parent (if such exists) and the RECIEVE node
        // that becomes MERGE RECEIVE. All in-between nodes will be inlined
        assert (orderbyNode.getParentCount() <= 1);
        AbstractPlanNode rootNode = (orderbyNode.getParentCount() == 1) ? orderbyNode.getParent(0) : null;
        receive.clearParents();
        if (rootNode == null) {
            rootNode = receive;
        } else {
            rootNode.clearChildren();
            rootNode.addAndLinkChild(receive);
        }

        // Add inline ORDER BY node
        AbstractPlanNode orderByNode = inlineCandidates.get(PlanNodeType.ORDERBY);
        assert(orderByNode != null);
        receive.addInlinePlanNode(orderByNode);

        // LIMIT can be already inline with ORDER BY node
        AbstractPlanNode limitNode = orderByNode.getInlinePlanNode(PlanNodeType.LIMIT);
        if (limitNode != null) {
            orderByNode.removeInlinePlanNode(PlanNodeType.LIMIT);
        } else {
            limitNode  = inlineCandidates.get(PlanNodeType.LIMIT);
        }

        // Add inline aggregate
        AbstractPlanNode aggrNode = inlineCandidates.get(PlanNodeType.AGGREGATE);
        if (aggrNode == null) {
            aggrNode = inlineCandidates.get(PlanNodeType.PARTIALAGGREGATE);
        }
        if (aggrNode != null) {
            if (limitNode != null) {
                aggrNode.addInlinePlanNode(limitNode);
            }
            receive.addInlinePlanNode(aggrNode);
        }
        // Add LIMIT if it is exist and wasn't inline with aggregate node
        if (limitNode != null && aggrNode == null) {
            receive.addInlinePlanNode(limitNode);
        }

        // Convert the receive node to the merge receive
        receive.setMergeReceive(true);

        // return the new root
        return rootNode;
    }

    //@TODO need to be rewritten similar to the SubPlanAssembler.determineIndexOrdering
    // or PlanAssembler.calculateGroupbyColumnsCovered
    AbstractPlanNode convertToSerialAggregation(AbstractPlanNode aggregateNode, OrderByPlanNode orderbyNode) {
        assert(aggregateNode instanceof HashAggregatePlanNode);
        HashAggregatePlanNode hashAggr = (HashAggregatePlanNode) aggregateNode;
        Set<AbstractExpression> groupbys = new HashSet<AbstractExpression>(hashAggr.getGroupByExpressions());
        Set<AbstractExpression> orderbys = new HashSet<AbstractExpression>(orderbyNode.getSortExpressions());
        boolean result = groupbys.removeAll(orderbys);
        if (!result) {
            // GROUP BY and ORDER BY don't have common expressions - HASH aggregation
            return aggregateNode;
        } else if (groupbys.isEmpty()) {
            // All GROUP BY expressions are also ORDER BY - Serial aggregation
            return AggregatePlanNode.convertToSerialAggregatePlanNode(hashAggr);
        } else {
            // Partial aggregation
            List<Integer> coveredGroupByColumns = new ArrayList<Integer>();
            int idx = 0;
            for (AbstractExpression groupByExpr : hashAggr.getGroupByExpressions()) {
                if (!groupbys.contains(groupByExpr)) {
                    // This groupByExpr is covered by the ORDER BY
                    coveredGroupByColumns.add(idx);
                }
                ++idx;
            }
            return AggregatePlanNode.convertToPartialAggregatePlanNode(hashAggr, coveredGroupByColumns);
        }
    }
}
