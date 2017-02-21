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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Set;

import org.voltdb.expressions.AbstractExpression;
import org.voltdb.plannodes.IndexSortablePlanNode;
import org.voltdb.plannodes.AbstractPlanNode;
import org.voltdb.plannodes.AbstractScanPlanNode;
import org.voltdb.plannodes.AggregatePlanNode;
import org.voltdb.plannodes.HashAggregatePlanNode;
import org.voltdb.plannodes.MergeReceivePlanNode;
import org.voltdb.plannodes.OrderByPlanNode;
import org.voltdb.plannodes.ReceivePlanNode;
import org.voltdb.plannodes.SendPlanNode;
import org.voltdb.plannodes.WindowFunctionPlanNode;
import org.voltdb.types.PlanNodeType;

public class InlineOrderByIntoMergeReceive extends MicroOptimization {

    @Override
    protected AbstractPlanNode recursivelyApply(AbstractPlanNode planNode)
    {
        assert(planNode != null);

        // This optimization was interfering with some UPSERT ... FROM
        // queries because once the optimization is applied to a subquery,
        // it becomes difficult to correct the subquery to work in a
        // multi-partition DML statement context. That's no longer
        // simply a matter of removing the Send/Receive pair without
        // side effects.
        if (m_parsedStmt.topmostParentStatementIsDML()) {
            return planNode; // Do not apply the optimization.
        }

        Queue<AbstractPlanNode> children = new LinkedList<>();
        children.add(planNode);

        while(!children.isEmpty()) {
            AbstractPlanNode plan = children.remove();
            PlanNodeType nodeType = plan.getPlanNodeType();
            if (PlanNodeType.RECEIVE == nodeType) {
                // continue. We are after the coordinator ORDER BY or WINDOWFUNCTION node.
                return planNode;
            }
            if (PlanNodeType.ORDERBY == nodeType) {
                assert(plan instanceof OrderByPlanNode);
                AbstractPlanNode newPlan = applyOptimization((OrderByPlanNode)plan);
                // (*) If we have changed plan to newPlan, then the
                //     new nodes are inside the tree unless plan is the top.
                //     So, return the original argument, planNode, unless
                //     we actually changed the top plan node.  Then return
                //     the new plan node.
                if (newPlan != plan) {
                    // Only one coordinator ORDER BY node is possible
                    if (plan == planNode) {
                        return newPlan;
                    } else {
                        return planNode; // Do not apply the optimization.
                    }
                }
            } else if (PlanNodeType.WINDOWFUNCTION == nodeType) {
                assert(plan instanceof WindowFunctionPlanNode);
                AbstractPlanNode newPlan = applyOptimization((WindowFunctionPlanNode)plan);
                // See above for why this is the way it is.
                if (newPlan != plan) {
                    return newPlan;
                } else {
                    return planNode;
                }
            }

            for (int i = 0; i < plan.getChildCount(); i++) {
                children.add(plan.getChild(i));
            }
        }
        return planNode; // Do not apply the optimization.
    }

    /**
     * Convert ReceivePlanNodes into MergeReceivePlanNodes when the
     * RECEIVE node's nearest parent is a window function.  We won't
     * have any inline limits or aggregates here, so this is somewhat
     * simpler than the order by case.
     *
     * @param plan
     * @return
     */
    private AbstractPlanNode applyOptimization(WindowFunctionPlanNode plan) {
        assert(plan.getChildCount() == 1);
        assert(plan.getChild(0) != null);
        AbstractPlanNode child = plan.getChild(0);
        assert(child != null);
        // SP Plans which have an index which can provide
        // the window function ordering don't create
        // an order by node.
        if ( ! ( child instanceof OrderByPlanNode ) ) {
            return plan;
        }
        OrderByPlanNode onode = (OrderByPlanNode)child;
        child = onode.getChild(0);
        // The order by node needs a RECEIVE node child
        // for this optimization to work.
        if ( ! ( child instanceof ReceivePlanNode)) {
            return plan;
        }
        ReceivePlanNode receiveNode = (ReceivePlanNode)child;
        assert(receiveNode.getChildCount() == 1);
        child = receiveNode.getChild(0);
        // The Receive node needs a send node child.
        assert( child instanceof SendPlanNode );
        SendPlanNode sendNode = (SendPlanNode)child;
        child = sendNode.getChild(0);
        // If this window function does not use the
        // index then this optimization is not possible.
        // We've recorded a number of the window function
        // in the root of the subplan, which will be
        // the first child of the send node.
        //
        // Right now the only window function has number
        // 0, and we don't record that in the
        // WINDOWFUNCTION plan node.  If there were
        // more than one window function we would need
        // to record a number in the plan node and
        // then check that child.getWindowFunctionUsesIndex()
        // returns the number in the plan node.
        if ( ! ( child instanceof IndexSortablePlanNode)) {
            return plan;
        }
        IndexSortablePlanNode indexed = (IndexSortablePlanNode)child;
        if (indexed.indexUse().getWindowFunctionUsesIndex() != 0) {
            return plan;
        }
        // Remove the Receive node and the Order by node
        // and replace them with a MergeReceive node.  Leave
        // the order by node inline in the MergeReceive node,
        // since we need it to calculate the merge.
        plan.clearChildren();
        receiveNode.removeFromGraph();
        MergeReceivePlanNode mrnode = new MergeReceivePlanNode();
        mrnode.addInlinePlanNode(onode);
        mrnode.addAndLinkChild(sendNode);
        plan.addAndLinkChild(mrnode);
        return plan;
    }

    /**
     * For MP queries, the coordinator's OrderBy node can be replaced with
     * a specialized Receive node that merges individual partitions results
     * into a final result set if the partitions result set is sorted
     * in the order matching the ORDER BY order
     *
     * @param orderbyNode - ORDER BY node to optimize
     * @return optimized plan
     */
    AbstractPlanNode applyOptimization(OrderByPlanNode orderbyNode) {
        // Find all child RECEIVE nodes. We are not interested in the MERGERECEIVE nodes there
        // because they could only come from subqueries.
        List<AbstractPlanNode> receives = orderbyNode.findAllNodesOfType(PlanNodeType.RECEIVE);
        if (receives.isEmpty()) {
            return orderbyNode;
        }
        assert(receives.size() == 1);

        ReceivePlanNode receive = (ReceivePlanNode)receives.get(0);
        // Make sure that this receive node belongs to the same coordinator fragment that
        // the ORDER BY node does. Alternatively, it could belong to a distributed subquery.
        // Walk up the tree starting at the receive node until we hit either a scan node
        // (distributed subquery) or the original order by node (distributed order by)
        // Collect all nodes that are currently in between ORDER BY and RECEIVE nodes
        // If the optimization is possible, they will be converted to inline nodes of
        // the MERGE RECEIVE node. The expected node types are:
        //      LIMIT, AGGREGATE/PARTIALAGGREGATE/HASHAGGREGATE
        // The HASHAGGREGATE must be convertible to AGGREGATE or PARTIALAGGREGATE for optimization
        // to be applicable.

        // LIMIT can be already inline with ORDER BY node
        AbstractPlanNode limitNode = orderbyNode.getInlinePlanNode(PlanNodeType.LIMIT);
        AbstractPlanNode aggregateNode = null;
        AbstractPlanNode inlineCandidate = receive.getParent(0);
        while (orderbyNode != inlineCandidate) {
            if (inlineCandidate instanceof AbstractScanPlanNode) {
                // it's a subquery
                return orderbyNode;
            }
            PlanNodeType nodeType = inlineCandidate.getPlanNodeType();
            if (nodeType == PlanNodeType.LIMIT && limitNode == null) {
                limitNode = inlineCandidate;
            } else if ((nodeType == PlanNodeType.AGGREGATE || nodeType == PlanNodeType.PARTIALAGGREGATE) &&
                    aggregateNode == null) {
                aggregateNode = inlineCandidate;
            } else if (nodeType == PlanNodeType.HASHAGGREGATE && aggregateNode == null) {
                aggregateNode = convertToSerialAggregation(inlineCandidate, orderbyNode);
                if (PlanNodeType.HASHAGGREGATE == aggregateNode.getPlanNodeType()) {
                    return orderbyNode;
                }
            } else {
                // Don't know how to handle this node or there is already a node of this type
                return orderbyNode;
            }
            // move up one node
            assert(inlineCandidate.getParentCount() == 1);
            inlineCandidate = inlineCandidate.getParent(0);
        }

        assert(receive.getChildCount() == 1);
        AbstractPlanNode partitionRoot = receive.getChild(0);
        if (!partitionRoot.isOutputOrdered(orderbyNode.getSortExpressions(), orderbyNode.getSortDirections())) {
            // Partition results are not ordered
            return orderbyNode;
        }

        // At this point we confirmed that the optimization is applicable.
        // Short circuit the current ORDER BY parent (if such exists) and
        // the new MERGERECIEVE node.. All in-between nodes will be inlined
        assert (orderbyNode.getParentCount() <= 1);
        AbstractPlanNode rootNode = (orderbyNode.getParentCount() == 1) ? orderbyNode.getParent(0) : null;
        MergeReceivePlanNode mergeReceive = new MergeReceivePlanNode();
        assert(receive.getChildCount() == 1);
        mergeReceive.addAndLinkChild(receive.getChild(0));
        receive.removeFromGraph();
        if (rootNode == null) {
            rootNode = mergeReceive;
        } else {
            rootNode.clearChildren();
            rootNode.addAndLinkChild(mergeReceive);
        }

        // Add inline ORDER BY node and remove inline LIMIT node if any
        mergeReceive.addInlinePlanNode(orderbyNode);
        if (limitNode != null) {
            orderbyNode.removeInlinePlanNode(PlanNodeType.LIMIT);
        }

        // Add inline aggregate
        if (aggregateNode != null) {
            if (limitNode != null) {
                // Inline LIMIT with aggregate
                aggregateNode.addInlinePlanNode(limitNode);
            }
            mergeReceive.addInlinePlanNode(aggregateNode);
        }
        // Add LIMIT if it is exist and wasn't inline with aggregate node
        if (limitNode != null && aggregateNode == null) {
            mergeReceive.addInlinePlanNode(limitNode);
        }

        // return the new root
        return rootNode;
    }

    /**
     * The Hash aggregate can be converted to a Serial or Partial aggregate if
     *   - all GROUP BY and ORDER BY expressions bind to each other - Serial Aggregate
     *   - a subset of the GROUP BY expressions covers all of the ORDER BY  - Partial
     *   - anything else - remains a Hash Aggregate
     * @param aggregateNode
     * @param orderbyNode
     * @return new aggregate node if the conversion is possible or the original hash aggregate otherwise
     */
    AbstractPlanNode convertToSerialAggregation(AbstractPlanNode aggregateNode, OrderByPlanNode orderbyNode) {
        assert(aggregateNode instanceof HashAggregatePlanNode);
        HashAggregatePlanNode hashAggr = (HashAggregatePlanNode) aggregateNode;
        List<AbstractExpression> groupbys = new ArrayList<>(hashAggr.getGroupByExpressions());
        List<AbstractExpression> orderbys = new ArrayList<>(orderbyNode.getSortExpressions());
        Set<Integer> coveredGroupByColumns = new HashSet<>();

        Iterator<AbstractExpression> orderbyIt = orderbys.iterator();
        while (orderbyIt.hasNext()) {
            AbstractExpression orderby = orderbyIt.next();
            int idx = 0;
            for (AbstractExpression groupby : groupbys) {
                if (!coveredGroupByColumns.contains(idx)) {
                    if (orderby.equals(groupby)) {
                        orderbyIt.remove();
                        coveredGroupByColumns.add(idx);
                        break;
                    }
                }
                ++idx;
            }
        }
        if (orderbys.isEmpty() && groupbys.size() == coveredGroupByColumns.size()) {
            // All GROUP BY expressions are also ORDER BY - Serial aggregation
            return AggregatePlanNode.convertToSerialAggregatePlanNode(hashAggr);
        }
        if (orderbys.isEmpty() && !coveredGroupByColumns.isEmpty() ) {
            // Partial aggregation
            List<Integer> coveredGroupByColumnList = new ArrayList<>();
            coveredGroupByColumnList.addAll(coveredGroupByColumns);
            return AggregatePlanNode.convertToPartialAggregatePlanNode(hashAggr, coveredGroupByColumnList);
        }
        return aggregateNode;
    }

}
