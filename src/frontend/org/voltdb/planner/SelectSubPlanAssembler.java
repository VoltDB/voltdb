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

package org.voltdb.planner;

import java.util.*;

import org.voltdb.exceptions.PlanningErrorException;
import org.voltdb.expressions.AbstractExpression;
import org.voltdb.expressions.ExpressionUtil;
import org.voltdb.expressions.TupleValueExpression;
import org.voltdb.planner.parseinfo.BranchNode;
import org.voltdb.planner.parseinfo.JoinNode;
import org.voltdb.planner.parseinfo.StmtSubqueryScan;
import org.voltdb.planner.parseinfo.StmtTableScan;
import org.voltdb.planner.parseinfo.SubqueryLeafNode;
import org.voltdb.plannodes.IndexSortablePlanNode;
import org.voltdb.plannodes.AbstractJoinPlanNode;
import org.voltdb.plannodes.AbstractPlanNode;
import org.voltdb.plannodes.AbstractReceivePlanNode;
import org.voltdb.plannodes.IndexScanPlanNode;
import org.voltdb.plannodes.IndexUseForOrderBy;
import org.voltdb.plannodes.MaterializedScanPlanNode;
import org.voltdb.plannodes.NestLoopIndexPlanNode;
import org.voltdb.plannodes.NestLoopPlanNode;
import org.voltdb.types.ExpressionType;
import org.voltdb.types.JoinType;
import org.voltdb.utils.PermutationGenerator;

/**
 * For a select, delete or update plan, this class builds the part of the plan
 * which collects tuples from relations. Given the tables and the predicate
 * (and sometimes the output columns), this will build a plan that will output
 * matching tuples to a temp table. A delete, update or send plan node can then
 * be glued on top of it. In selects, aggregation and other projections are also
 * done on top of the result from this class.
 *
 */
public class SelectSubPlanAssembler extends SubPlanAssembler {

    /** The list of generated plans. This allows their generation in batches.*/
    Deque<AbstractPlanNode> m_plans = new ArrayDeque<>();

    /** The list of all possible join orders, assembled by queueAllJoinOrders */
    private Deque<JoinNode> m_joinOrders = new ArrayDeque<>();

    private static final Runtime RUN_TIME = Runtime.getRuntime();
    // Number of times generateSubPlanForJoinNode() gets called recursively that we collect an estimate of heap size,
    // and early exit if too large heap size had been used.
    private static final int PLAN_ESTIMATE_PERIOD = 300;
    // Stop generating any further possible plans, if we have reached xx% of available JVM heap memory
    private static final short MAX_HEAP_MEMORY_USAGE_PCT = 80;
    private static final long MAX_ALLOWED_PLAN_MEMORY = RUN_TIME.maxMemory() * MAX_HEAP_MEMORY_USAGE_PCT / 100;

    /**
     * Stop further planning, if we have used more heap memory than we could hopefully exhaustively plan it out,
     * at the time this method is called.
     *
     * @return whether we should stop further planning. By the time it returns true, GC had already kicked in a few
     * rounds.
     */
    private static boolean shouldStopPlanning() {
        return RUN_TIME.totalMemory() - RUN_TIME.freeMemory() >= MAX_ALLOWED_PLAN_MEMORY;
    }
    /**
     *
     * @param selectStmt The parsed and dissected statement object describing the sql to execute.
     * @param partitioning in/out param first element is partition key value, forcing a single-partition statement if non-null,
     * second may be an inferred partition key if no explicit single-partitioning was specified
     */
    SelectSubPlanAssembler(ParsedSelectStmt selectStmt, StatementPartitioning partitioning) {
        super(selectStmt, partitioning);
        if (selectStmt.hasJoinOrder()) {
            // If a join order was provided or large number of tables join
            m_joinOrders.addAll(selectStmt.getJoinOrder());
        } else {
            assert(m_parsedStmt.m_noTableSelectionList.size() == 0);
            m_joinOrders = queueJoinOrders(m_parsedStmt.m_joinTree, true);
        }
    }

    /**
     * Compute every permutation of the list of involved tables and put them in a deque.
     * TODO(XIN): takes at least 3.3% cpu of planner. Optimize it when possible.
     */
    public static Deque<JoinNode> queueJoinOrders(JoinNode joinNode, boolean findAll) {
        assert(joinNode != null);

        // Clone the original
        JoinNode clonedTree = (JoinNode) joinNode.clone();
        // Split join tree into a set of subtrees. The join type for all nodes in a subtree is the same
        List<JoinNode> subTrees = clonedTree.extractSubTrees();
        assert(!subTrees.isEmpty());
        // Generate possible join orders for each sub-tree separately
        List<List<JoinNode>> joinOrderList = generateJoinOrders(subTrees);
        // Reassemble the all possible combinations of the sub-tree and queue them
        ArrayDeque<JoinNode> joinOrders = new ArrayDeque<>();
        queueSubJoinOrders(joinOrderList, 0, new ArrayList<>(), joinOrders, findAll);
        return joinOrders;
    }

    private static void queueSubJoinOrders(List<List<JoinNode>> joinOrderList, int joinOrderListIdx,
            List<JoinNode> currentJoinOrder, Deque<JoinNode> joinOrders, boolean findAll) {
        if (!findAll && joinOrders.size() > 0) {
            // At least find one valid join order
        } else if (joinOrderListIdx == joinOrderList.size()) {
            // End of recursion
            assert(!currentJoinOrder.isEmpty());
            JoinNode joinTree = JoinNode.reconstructJoinTreeFromSubTrees(currentJoinOrder);
            joinOrders.add(joinTree);
        } else {
            // Recursive step
            List<JoinNode> nextTrees = joinOrderList.get(joinOrderListIdx);
            for (JoinNode headTree : nextTrees) {
                ArrayList<JoinNode> updatedJoinOrder = new ArrayList<>();
                // Order is important: The top sub-trees must be first
                for (JoinNode node : currentJoinOrder) {
                    updatedJoinOrder.add((JoinNode) node.clone());
                }
                updatedJoinOrder.add((JoinNode) headTree.clone());
                queueSubJoinOrders(joinOrderList, joinOrderListIdx + 1, updatedJoinOrder, joinOrders, findAll);
            }
        }
    }

    /**
     * For each join tree from the input list generate a set of the join trees by permuting the leafs
     * (table nodes) of the tree without breaking the joins semantic.
     *
     * @param subTrees the list of join trees.
     * @return The list containing the list of trees of all possible permutations of the input trees
     */
    private static List<List<JoinNode>> generateJoinOrders(List<JoinNode> subTrees) {
        List<List<JoinNode>> permutations = new ArrayList<>();
        for (JoinNode subTree : subTrees) {
            permutations.add(generateJoinOrdersForTree(subTree));
        }
        return permutations;
    }

    private static List<JoinNode> generateJoinOrdersForTree(JoinNode subTree) {
        if (subTree instanceof BranchNode) {
            BranchNode branchSubTree = (BranchNode) subTree;
            JoinType joinType = branchSubTree.getJoinType();
            if (joinType == JoinType.INNER) {
                return generateInnerJoinOrdersForTree(subTree);
            } else if (joinType == JoinType.LEFT) {
                return generateOuterJoinOrdersForTree(subTree);
            } else if (joinType == JoinType.FULL) {
                return generateFullJoinOrdersForTree(subTree);
            } else {
                // Shouldn't get there
                throw new PlanningErrorException("Internal error: unsupported join type " + joinType.toString());
            }
        } else {
            // Single tables and subqueries
            return generateInnerJoinOrdersForTree(subTree);
        }
    }

    /**
     * Helper method to generate join orders for a join tree containing only INNER joins that
     * can be obtained by the permutation of the original tables.
     *
     * @param subTree join tree
     * @return list of valid join orders
     */
    private static List<JoinNode> generateInnerJoinOrdersForTree(JoinNode subTree) {
        // Get a list of the leaf nodes(tables) to permute them
        List<JoinNode> tableNodes = subTree.generateLeafNodesJoinOrder();
        List<List<JoinNode>> joinOrders = PermutationGenerator.generatePurmutations(tableNodes);
        List<JoinNode> newTrees = new ArrayList<>();
        for (List<JoinNode> joinOrder: joinOrders) {
            newTrees.add(JoinNode.reconstructJoinTreeFromTableNodes(joinOrder, JoinType.INNER));
        }
        //Collect all the join/where conditions to reassign them later
        AbstractExpression combinedWhereExpr = subTree.getAllFilters();
        List<JoinNode> treePermutations = new ArrayList<>();
        for (JoinNode newTree : newTrees) {
            if (combinedWhereExpr != null) {
                newTree.setWhereExpression(combinedWhereExpr.clone());
            }
            // The new tree root node id must match the original one to be able to reconnect the
            // subtrees
            newTree.setId(subTree.getId());
            treePermutations.add(newTree);
        }
        return treePermutations;
    }

    /**
     * Helper method to generate join orders for an OUTER join tree.
     * At the moment, permutations for LEFT Joins are not supported yet
     *
     * @param subTree join tree
     * @return list of valid join orders
     */
    private static List<JoinNode> generateOuterJoinOrdersForTree(JoinNode subTree) {
        List<JoinNode> treePermutations = new ArrayList<>();
        treePermutations.add(subTree);
        return treePermutations;
    }

    /**
     * Helper method to generate join orders for a join tree containing only FULL joins.
     * The only allowed permutation is a join order that has original left and right nodes
     * swapped.
     *
     * @param subTree  join tree
     * @return list of valid join orders
     */
    private static List<JoinNode> generateFullJoinOrdersForTree(JoinNode subTree) {
        assert(subTree != null);
        List<JoinNode> joinOrders = new ArrayList<>();
        if (!(subTree instanceof BranchNode)) {
            // End of recursion
            joinOrders.add(subTree);
            return joinOrders;
        }
        BranchNode branchNode = (BranchNode) subTree;
        // Descend to the left branch
        assert(branchNode.getLeftNode() != null);
        List<JoinNode> leftJoinOrders = generateFullJoinOrdersForTree(branchNode.getLeftNode());
        assert(!leftJoinOrders.isEmpty());
        // Descend to the right branch
        assert(branchNode.getRightNode() != null);
        List<JoinNode> rightJoinOrders = generateFullJoinOrdersForTree(branchNode.getRightNode());
        assert(!rightJoinOrders.isEmpty());
        // Create permutation pairing left and right nodes and the revere variant
        for (JoinNode leftNode : leftJoinOrders) {
            for (JoinNode rightNode : rightJoinOrders) {
                JoinNode resultOne = new BranchNode(branchNode.getId(), branchNode.getJoinType(),
                        (JoinNode) leftNode.clone(), (JoinNode) rightNode.clone());
                JoinNode resultTwo = new BranchNode(branchNode.getId(), branchNode.getJoinType(),
                        (JoinNode) rightNode.clone(), (JoinNode) leftNode.clone());
                if (branchNode.getJoinExpression() != null) {
                    resultOne.setJoinExpression(branchNode.getJoinExpression().clone());
                    resultTwo.setJoinExpression(branchNode.getJoinExpression().clone());
                }
                if (branchNode.getWhereExpression() != null) {
                    resultOne.setWhereExpression(branchNode.getWhereExpression().clone());
                    resultTwo.setWhereExpression(branchNode.getWhereExpression().clone());
                }
                joinOrders.add(resultOne);
                joinOrders.add(resultTwo);
            }
        }
        return joinOrders;
    }

    /**
     * Pull a join order out of the join orders deque, compute all possible plans
     * for that join order, then append them to the computed plans deque.
     */
    @Override
    protected AbstractPlanNode nextPlan() {

        // repeat (usually run once) until plans are created
        // or no more plans can be created
        while (m_plans.size() == 0) {
            // get the join order for us to make plans out of
            JoinNode joinTree = m_joinOrders.poll();

            // no more join orders => no more plans to generate
            if (joinTree == null) {
                return null;
            }
            joinTree.analyzeJoinExpressions(m_parsedStmt);
            if (! m_parsedStmt.m_noTableSelectionList.isEmpty()) {
                // a query that is a little too quirky or complicated.
                throw new PlanningErrorException("Join with filters that do not depend on joined tables is not supported in VoltDB");
            }

            if ( ! m_partitioning.wasSpecifiedAsSingle()) {
                // Now that analyzeJoinExpressions has done its job of properly categorizing
                // and placing the various filters that the HSQL parser tends to leave in the strangest
                // configuration, this is the first opportunity to analyze WHERE and JOIN filters'
                // effects on statement partitioning.
                // But this causes the analysis to be run based on a particular join order.
                // Which join orders does this analysis actually need to be run on?
                // Can it be run on the first join order and be assumed to hold for all join orders?
                // If there is a join order that fails to generate a single viable plan, is its
                // determination of partitioning (or partitioning failure) always valid for other
                // join orders, or should the analysis be repeated on a viable join order
                // in that case?
                // For now, analyze each join order independently and when an invalid partitioning is
                // detected, skip the plan generation for that particular ordering.
                // If this causes all plans to be skipped, commonly the case, the PlanAssembler
                // should propagate an error message identifying partitioning as the problem.
                m_partitioning.analyzeForMultiPartitionAccess(
                        m_parsedStmt.allScans(), joinTree.getAllEquivalenceFilters());
                if (! m_partitioning.isJoinValid()) {
                    // The case of more than one independent partitioned table
                    // would result in an illegal plan with more than two fragments.
                    // Don't throw a planning error here, in case the problem is just with this
                    // particular join order, but do leave a hint to the PlanAssembler in case
                    // the failure is unanimous -- a common case.
                    m_recentErrorMsg = m_partitioning.getJoinInvalidReason();
                    // This join order, at least, is not worth trying to plan.
                    continue;
                }
            }
            generateMorePlansForJoinTree(joinTree);
        }
        return m_plans.poll();
    }

    /**
     * Given a specific join order, compute all possible sub-plan-graphs for that
     * join order and add them to the deque of plans. If this doesn't add plans,
     * it doesn't mean no more plans can be generated. It's possible that the
     * particular join order it got had no reasonable plans.
     *
     * @param joinTree An array of tables in the join order.
     */
    private void generateMorePlansForJoinTree(JoinNode joinTree) {
        assert(joinTree != null);
        // generate the access paths for all nodes
        generateAccessPaths(joinTree);

        List<JoinNode> nodes = joinTree.generateAllNodesJoinOrder();
        generateSubPlanForJoinNodeRecursively(joinTree, 0, nodes);
    }

    /**
     * Generate all possible access paths for all nodes in the tree.
     * The join and filter expressions are kept at the parent node
     * 1- The OUTER-only join conditions - Testing the outer-only conditions COULD be considered as an
     *   optimal first step to processing each outer tuple - PreJoin predicate for NLJ or NLIJ
     * 2 -The INNER-only and INNER_OUTER join conditions are used for finding a matching inner tuple(s) for a
     *   given outer tuple. Index and end-Index expressions for NLIJ and join predicate for NLJ.
     * 3 -The OUTER-only filter conditions. - Can be pushed down to pre-qualify the outer tuples before they enter
     *   the join - Where condition for the left child
     * 4. The INNER-only and INNER_OUTER where conditions are used for filtering joined tuples. -
     *   Post join predicate for NLIJ and NLJ .  Possible optimization -
     *   if INNER-only condition is NULL-rejecting (inner_tuple is NOT NULL or inner_tuple > 0)
     *   it can be pushed down as a filter expression to the inner child
     *
     * @param joinNode A root to the join tree to generate access paths to all nodes in that tree.
     */
    private void generateAccessPaths(JoinNode joinNode) {
        assert(joinNode != null);
        if (joinNode instanceof BranchNode) {
            generateOuterAccessPaths((BranchNode)joinNode);
            generateInnerAccessPaths((BranchNode)joinNode);
            // An empty access path for the root
            joinNode.m_accessPaths.add(new AccessPath());
            return;
        }
        // This is a select from a single table
        joinNode.m_accessPaths.addAll(getRelevantAccessPathsForTable(joinNode.getTableScan(),
                joinNode.m_joinInnerList, joinNode.m_whereInnerList, null));
    }

    /**
     * Generate all possible access paths for an outer node in a join.
     * The outer table and/or join can have the naive access path and possible index path(s)
     * Optimizations - outer-table-only where expressions can be pushed down to the child node
     * to pre-qualify the outer tuples before they enter the join.
     * For inner joins outer-table-only join expressions can be pushed down as well
     *
     * @param parentNode A parent node to the node to generate paths to.
     */
    private void generateOuterAccessPaths(BranchNode parentNode) {
        JoinNode outerChildNode = parentNode.getLeftNode();
        assert(outerChildNode != null);
        JoinType joinType = parentNode.getJoinType();
        // For LEFT and FULL join types, the outer join expressions are kept as a pre-join predicate
        // at the join node to pre-qualify the outer rows
        List<AbstractExpression> joinOuterList =  joinType == JoinType.INNER ?
                parentNode.m_joinOuterList : null;
        if (outerChildNode instanceof BranchNode) {
            generateOuterAccessPaths((BranchNode)outerChildNode);
            generateInnerAccessPaths((BranchNode)outerChildNode);
            // The join node can have only sequential scan access
            outerChildNode.m_accessPaths.add(getRelevantNaivePath(joinOuterList,
                    parentNode.m_whereOuterList));
            assert(outerChildNode.m_accessPaths.size() > 0);
            return;
        }
        // WHERE Outer expressions must stay at the join node for the FULL joins
        // They will be added later as part of the WHERE predicate of the join node
        List<AbstractExpression> parentWhereList = null;
        if (joinType != JoinType.FULL) {
            parentWhereList = parentNode.m_whereOuterList;
        }
        outerChildNode.m_accessPaths.addAll(
                getRelevantAccessPathsForTable(outerChildNode.getTableScan(),
                        joinOuterList, parentWhereList, null));
    }

    /**
     * Generate all possible access paths for an inner node in a join.
     * The set of potential index expressions depends whether the inner node can be inlined
     * with the NLIJ or not. In the former case, inner and inner-outer join expressions can
     * be considered for the index access. In the latter, only inner join expressions qualifies.
     *
     * @param parentNode A parent node to the node to generate paths to.
     */
    private void generateInnerAccessPaths(BranchNode parentNode) {
        JoinNode innerChildNode = parentNode.getRightNode();
        assert(innerChildNode != null);
        // In case of inner join WHERE and JOIN expressions can be merged
        if (parentNode.getJoinType() == JoinType.INNER) {
            parentNode.m_joinInnerOuterList.addAll(parentNode.m_whereInnerOuterList);
            parentNode.m_whereInnerOuterList.clear();
            parentNode.m_joinInnerList.addAll(parentNode.m_whereInnerList);
            parentNode.m_whereInnerList.clear();
        }
        if (innerChildNode instanceof BranchNode) {
            generateOuterAccessPaths((BranchNode)innerChildNode);
            generateInnerAccessPaths((BranchNode)innerChildNode);
            // The inner node is a join node itself. Only naive access path is possible
            innerChildNode.m_accessPaths.add(
                    getRelevantNaivePath(parentNode.m_joinInnerOuterList, parentNode.m_joinInnerList));
            return;
        }

        // The inner table can have multiple index access paths based on
        // inner and inner-outer join expressions plus the naive one.
        List<AbstractExpression> filterExprs = null;
        List<AbstractExpression> postExprs = null;
        // For the FULL join type, the inner join expressions must stay at the join node and
        // not go down to the inner node as filters (as predicates for SeqScan nodes and/or
        // index expressions for Index Scan). The latter case (IndexScan) won't work for NLJ because
        // the inner join expression will effectively filter out inner tuple prior to the NLJ.
        if (parentNode.getJoinType() != JoinType.FULL) {
            filterExprs = parentNode.m_joinInnerList;
        } else {
            postExprs = parentNode.m_joinInnerList;
        }
        StmtTableScan innerTable = innerChildNode.getTableScan();
        assert(innerTable != null);
        innerChildNode.m_accessPaths.addAll(
                getRelevantAccessPathsForTable(innerTable,
                        parentNode.m_joinInnerOuterList, filterExprs, postExprs));

        // If there are inner expressions AND inner-outer expressions, it could be that there
        // are indexed access paths that use elements of both in the indexing expressions,
        // especially in the case of a compound index.
        // These access paths can not be considered for use with an NLJ because they rely on
        // inner-outer expressions.
        // If there is a possibility that NLIJ will not be an option due to the
        // "special case" processing that puts a send/receive plan between the join node
        // and its inner child node, other access paths need to be considered that use the
        // same indexes as those identified so far but in a simpler, less effective way
        // that does not rely on inner-outer expressions.
        // The following simplistic method of finding these access paths is to force
        // inner-outer expressions to be handled as NLJ-compatible post-filters and repeat
        // the search for access paths.
        // This will typically generate some duplicate access paths, including the naive
        // sequential scan path and any indexed paths that happened to use only the inner
        // expressions.
        // For now, we deal with this redundancy by dropping (and re-generating) all
        // access paths EXCPT those that reference the inner-outer expressions.
        // TODO: implementing access path hash and equality and possibly using a "Set"
        // would allow deduping as new access paths are added OR
        // the simplified access path search process could be based on
        // the existing indexed access paths -- for each access path that "hasInnerOuterIndexExpression"
        // try to generate and add a simpler access path using the same index,
        // this time with the inner-outer expressions used only as non-indexable post-filters.

        // Don't bother generating these redundant or inferior access paths unless there is
        // an inner-outer expression and a chance that NLIJ will be taken out of the running.
        boolean mayNeedInnerSendReceive = ! m_partitioning.wasSpecifiedAsSingle() &&
                m_partitioning.getCountOfPartitionedTables() > 0 &&
                parentNode.getJoinType() != JoinType.INNER &&
                ! innerTable.getIsReplicated();
// too expensive/complicated to test here? (parentNode.m_leftNode has a replicated result?) &&

        if (mayNeedInnerSendReceive && ! parentNode.m_joinInnerOuterList.isEmpty()) {
            List<AccessPath> innerOuterAccessPaths = new ArrayList<>();
            for (AccessPath innerAccessPath : innerChildNode.m_accessPaths) {
                if ((innerAccessPath.index != null) &&
                    hasInnerOuterIndexExpression(innerChildNode.getTableAlias(),
                                                 innerAccessPath.indexExprs,
                                                 innerAccessPath.initialExpr,
                                                 innerAccessPath.endExprs)) {
                    innerOuterAccessPaths.add(innerAccessPath);
                }
            }
            if (parentNode.getJoinType() != JoinType.FULL) {
                filterExprs = parentNode.m_joinInnerList;
                postExprs = parentNode.m_joinInnerOuterList;
            } else {
                // For FULL join type the inner join expressions must be part of the post predicate
                // in order to stay at the join node and not be pushed down to the inner node
                filterExprs = null;
                postExprs = new ArrayList<>(parentNode.m_joinInnerList);
                postExprs.addAll(parentNode.m_joinInnerOuterList);
            }
            Collection<AccessPath>
            nljAccessPaths = getRelevantAccessPathsForTable(
                    innerTable, null, filterExprs, postExprs);

            innerChildNode.m_accessPaths.clear();
            innerChildNode.m_accessPaths.addAll(nljAccessPaths);
            innerChildNode.m_accessPaths.addAll(innerOuterAccessPaths);
        }

        assert(innerChildNode.m_accessPaths.size() > 0);
    }

    /**
     * Generate all possible plans for the tree, or to the extent that further planning would drain JVM heap memory
     * (at threshold of MAX_HEAP_MEMORY_USAGE_PCT% of available JVM heap memory)
     *
     * @param rootNode The root node for the whole join tree.
     * @param nodes The node list to iterate over.
     */
    private void generateSubPlanForJoinNodeRecursively(JoinNode rootNode, int nextNode, List<JoinNode> nodes) {
        assert(nodes.size() > nextNode);
        JoinNode joinNode = nodes.get(nextNode);
        if (nodes.size() == nextNode + 1) {
            for (AccessPath path : joinNode.m_accessPaths) {
                joinNode.m_currentAccessPath = path;
                AbstractPlanNode plan = getSelectSubPlanForJoinNode(rootNode);
                if (plan == null) {
                    continue;
                }
                m_plans.add(plan);
            }
        } else if (! m_plans.isEmpty() && m_plans.size() % PLAN_ESTIMATE_PERIOD == 0 && shouldStopPlanning()) {
            // If we have drained heap memory, don't recurse further on.
        } else {
            for (AccessPath path : joinNode.m_accessPaths) {
                joinNode.m_currentAccessPath = path;
                generateSubPlanForJoinNodeRecursively(rootNode, nextNode + 1, nodes);
            }
        }
    }

    /**
     * Given a specific join node and access path set for inner and outer tables, construct the plan
     * that gives the right tuples.
     *
     * @param joinNode The join node to build the plan for.
     * @return A completed plan-sub-graph that should match the correct tuples from the
     * correct tables.
     */
    private AbstractPlanNode getSelectSubPlanForJoinNode(JoinNode joinNode) {
        assert(joinNode != null);
        if (joinNode instanceof BranchNode) {
            BranchNode branchJoinNode = (BranchNode)joinNode;
            // Outer node
            AbstractPlanNode outerScanPlan =
                    getSelectSubPlanForJoinNode(branchJoinNode.getLeftNode());
            if (outerScanPlan == null) {
                return null;
            }
            // Inner Node.
            AbstractPlanNode innerScanPlan =
                    getSelectSubPlanForJoinNode((branchJoinNode).getRightNode());
            if (innerScanPlan == null) {
                return null;
            }
            // Join Node
            IndexSortablePlanNode answer = getSelectSubPlanForJoin(branchJoinNode, outerScanPlan, innerScanPlan);
            // Propagate information used for order by clauses in window functions
            // and the statement level order by clause.  This is only if the
            // branch node is an inner join.
            if ((answer != null)
                    && (branchJoinNode.getJoinType() == JoinType.INNER)
                    && outerScanPlan instanceof IndexSortablePlanNode) {
                IndexUseForOrderBy indexUseForJoin = answer.indexUse();
                IndexUseForOrderBy indexUseFromScan = ((IndexSortablePlanNode)outerScanPlan).indexUse();
                indexUseForJoin.setWindowFunctionUsesIndex(indexUseFromScan.getWindowFunctionUsesIndex());
                indexUseForJoin.setWindowFunctionIsCompatibleWithOrderBy(indexUseFromScan.isWindowFunctionCompatibleWithOrderBy());
                indexUseForJoin.setFinalExpressionOrderFromIndexScan(indexUseFromScan.getFinalExpressionOrderFromIndexScan());
                indexUseForJoin.setSortOrderFromIndexScan(indexUseFromScan.getSortOrderFromIndexScan());
            }
            if (answer == null) {
                return null;
            } else {
                return answer.planNode();
            }
        }
        // End of recursion
        AbstractPlanNode scanNode = getAccessPlanForTable(joinNode);
        // Connect the sub-query tree if any
        if (joinNode instanceof SubqueryLeafNode) {
            StmtSubqueryScan tableScan = ((SubqueryLeafNode)joinNode).getSubqueryScan();
            CompiledPlan subQueryPlan = tableScan.getBestCostPlan();
            assert(subQueryPlan != null);
            assert(subQueryPlan.rootPlanGraph != null);
            // The sub-query best cost plan needs to be un-linked from the previous parent plan
            // it's the same child plan that gets re-attached to many parents one at a time
            subQueryPlan.rootPlanGraph.disconnectParents();
            scanNode.addAndLinkChild(subQueryPlan.rootPlanGraph);
        }
        return scanNode;
    }


   /**
     * Given a join node and plan-sub-graph for outer and inner sub-nodes,
     * construct the plan-sub-graph for that node.
     *
     * @param joinNode A parent join node.
     * @param outerPlan The outer node plan-sub-graph.
     * @param innerPlan The inner node plan-sub-graph.
     * @return A completed plan-sub-graph
     * or null if a valid plan can not be produced for given access paths.
     */
    private IndexSortablePlanNode getSelectSubPlanForJoin(
            BranchNode joinNode, AbstractPlanNode outerPlan, AbstractPlanNode innerPlan) {
        // Filter (post-join) expressions
        List<AbstractExpression> whereClauses  = new ArrayList<>();
        whereClauses.addAll(joinNode.m_whereInnerList);
        whereClauses.addAll(joinNode.m_whereInnerOuterList);
        if (joinNode.getJoinType() == JoinType.FULL) {
            // For all other join types, the whereOuterList expressions were pushed down to the outer node
            whereClauses.addAll(joinNode.m_whereOuterList);
        }

        // The usual approach of calculating a local (partial) join result on each partition,
        // then sending and merging them with other partial results on the coordinator does not
        // ensure correct answers for some queries like:
        //     SELECT * FROM replicated LEFT JOIN partitioned ON ...
        // They require a "global view" of the partitioned working set in order to
        // properly identify which replicated rows need to be null-padded,
        // and to ensure that the same replicated row is not null-padded redundantly on multiple partitions.
        // Many queries using this pattern impractically require redistribution and caching of a considerable
        // subset of a partitioned table in preparation for a "coordinated" join.
        // Yet, there may be useful cases with sufficient constant-based partitioned table filtering
        // in the "ON clause" to keep the distributed working set size under control, like
        //     SELECT * FROM replicated R LEFT JOIN partitioned P
        //     ON R.key == P.non_partition_key AND P.non_partition_key BETWEEN ? and ?;
        //
        // Such queries need to be prohibited by the planner if it can not guarantee the
        // correct results that require a "send before join" plan.
        // This could be the case if the replicated-to-partition join in these examples
        // were subject to another join with a partitioned table, like
        //     SELECT * FROM replicated R LEFT JOIN partitioned P1 ON ...
        //                                LEFT JOIN also_partitioned P2 ON ...
        //
        assert(joinNode.getRightNode() != null);
        JoinNode innerJoinNode = joinNode.getRightNode();
        AccessPath innerAccessPath = innerJoinNode.m_currentAccessPath;
        // We may need to add a send/receive pair to the inner plan for the special case.
        // This trick only works once per plan, BUT once the partitioned data has been
        // received on the coordinator, it can be treated as replicated data in later
        // joins, which MAY help with later outer joins with replicated data.

        boolean needInnerSendReceive = m_partitioning.requiresTwoFragments() &&
                                       ! innerPlan.hasReplicatedResult() &&
                                       outerPlan.hasReplicatedResult() &&
                                       joinNode.getJoinType() != JoinType.INNER;

        // When the inner plan is an IndexScan, there MAY be a choice of whether to join using a
        // NestLoopJoin (NLJ) or a NestLoopIndexJoin (NLIJ). The NLJ will have an advantage over the
        // NLIJ in the cases where it applies, since it does a single access or iteration over the index
        // and caches the result, where the NLIJ does an index access or iteration for each outer row.
        // The NestLoopJoin applies when the inner IndexScan is driven only by parameter and constant
        // expressions determined at the start of the query. That requires that none of the IndexScan's
        // various expressions that drive the index access may reference columns from the outer row
        // -- they can only reference columns of the index's base table (the indexed expressions)
        // as well as constants and parameters. The IndexScan's "otherExprs" expressions that only
        // drive post-filtering are not an issue since the NestLoopJoin does feature per-outer-tuple
        // post-filtering on each pass over the cached index scan result.

        // The special case of an OUTER JOIN of replicated outer row data with a partitioned inner
        // table requires that the partitioned data be sent to the coordinator prior to the join.
        // This limits the join option to NLJ. The index scan must make a single index access on
        // each partition and cache the result at the coordinator for post-filtering.
        // This requires that the index access be based on parameters or constants only
        // -- the replicated outer row data will only be available later at the coordinator,
        // so it can not drive the per-partition index scan.

        // If the NLJ option is precluded for the usual reason (outer-row-based indexing) AND
        // the NLIJ is precluded by the special case (OUTER JOIN of replicated outer rows and
        // partitioned inner rows) this method returns null, effectively rejecting this indexed
        // access path for the inner node. Other access paths or join orders may prove more successful.

        boolean canHaveNLJ = true;
        boolean canHaveNLIJ = true;
        if (innerPlan instanceof IndexScanPlanNode) {
            if (hasInnerOuterIndexExpression(joinNode.getRightNode().getTableAlias(),
                    innerAccessPath.indexExprs, innerAccessPath.initialExpr, innerAccessPath.endExprs)) {
                canHaveNLJ = false;
            }
        } else {
            canHaveNLIJ = false;
        }
        if (needInnerSendReceive) {
            canHaveNLIJ = false;
        }

        // Prohibit FULL join plans with distributed outer and replicated inner branches -
        // The join must happen on either a coordinator node or both tables must be joined on
        // partition columns
        if (joinNode.getJoinType() == JoinType.FULL &&
                m_partitioning.requiresTwoFragments() &&
                !outerPlan.hasReplicatedResult() &&
                innerPlan.hasReplicatedResult()) {
            canHaveNLIJ = false;
            canHaveNLJ = false;
        }

        AbstractJoinPlanNode ajNode;
        if (canHaveNLJ) {
            NestLoopPlanNode nljNode = new NestLoopPlanNode();
            // get all the clauses that join the applicable two tables
            // Copy innerAccessPath.joinExprs to leave it unchanged,
            // avoiding accumulation of redundant expressions when
            // joinClauses gets built up for various alternative plans.
            List<AbstractExpression> joinClauses = new ArrayList<>(innerAccessPath.joinExprs);
            if ((innerPlan instanceof IndexScanPlanNode) ||
                (innerPlan instanceof NestLoopIndexPlanNode
                    && innerPlan.getChild(0) instanceof MaterializedScanPlanNode)) {
                // InnerPlan is an IndexScan OR an NLIJ of a MaterializedScan
                // (IN LIST) and an IndexScan. In this case, the inner and
                // inner-outer non-index join expressions (if any) are in the
                // indexScan's otherExpr. The former should stay as IndexScanPlan
                // predicates but the latter need to be pulled up into NLJ
                // predicates because the IndexScan is executed once, not once
                // per outer tuple.
                ArrayList<AbstractExpression> otherExprs = new ArrayList<>();
                // PLEASE do not update the "innerAccessPath.otherExprs", it may be reused
                // for other path evaluation on the other outer side join.
                List<AbstractExpression> innerExpr = filterSingleTVEExpressions(innerAccessPath.otherExprs, otherExprs);
                joinClauses.addAll(otherExprs);
                IndexScanPlanNode scanNode = null;
                if (innerPlan instanceof IndexScanPlanNode) {
                    scanNode = (IndexScanPlanNode)innerPlan;
                } else {
                    assert(innerPlan instanceof NestLoopIndexPlanNode);
                    scanNode = ((NestLoopIndexPlanNode) innerPlan).getInlineIndexScan();
                }
                scanNode.setPredicate(innerExpr);
            } else if (innerJoinNode instanceof BranchNode && joinNode.getJoinType() != JoinType.INNER) {
                // If the innerJoinNode is a LEAF node OR if the join type is an INNER join,
                // the conditions that apply to the inner side
                // have been applied as predicates to the inner scan node already.

                // otherExpr of innerAccessPath comes from its parentNode's joinInnerList.
                // For Outer join (LEFT or FULL), it could mean a join predicate on the table of
                // the inner node ONLY, that can not be pushed down.
                joinClauses.addAll(innerAccessPath.otherExprs);
            }
            nljNode.setJoinPredicate(ExpressionUtil.combinePredicates(ExpressionType.CONJUNCTION_AND, joinClauses));

            // combine the tails plan graph with the new head node
            nljNode.addAndLinkChild(outerPlan);

            // If successful in the special case, the NLJ plan must be modified to cause the
            // partitioned inner data to be sent to the coordinator prior to the join.
            // This is done by adding send and receive plan nodes between the NLJ and its
            // right child node.
            if (needInnerSendReceive) {
                // This trick only works once per plan.
                if (outerPlan.hasAnyNodeOfClass(AbstractReceivePlanNode.class) ||
                        innerPlan.hasAnyNodeOfClass(AbstractReceivePlanNode.class)) {
                    return null;
                }
                innerPlan = addSendReceivePair(innerPlan);
            }

            nljNode.addAndLinkChild(innerPlan);
            ajNode = nljNode;
        } else if (canHaveNLIJ) {
            NestLoopIndexPlanNode nlijNode = new NestLoopIndexPlanNode();

            IndexScanPlanNode innerNode = (IndexScanPlanNode) innerPlan;
            // Set IndexScan predicate. The INNER join expressions for a FULL join come from
            // the innerAccessPath.joinExprs and need to be combined with the other join expressions
            innerNode.setPredicate(innerAccessPath.joinExprs, innerAccessPath.otherExprs);

            nlijNode.addInlinePlanNode(innerPlan);

            // combine the tails plan graph with the new head node
            nlijNode.addAndLinkChild(outerPlan);

            ajNode = nlijNode;
        } else {
            m_recentErrorMsg =
                "Unsupported special case of complex OUTER JOIN between replicated outer table and partitioned inner table.";
            return null;
        }
        ajNode.setJoinType(joinNode.getJoinType());
        ajNode.setPreJoinPredicate(ExpressionUtil.combinePredicates(ExpressionType.CONJUNCTION_AND, joinNode.m_joinOuterList));
        ajNode.setWherePredicate(ExpressionUtil.combinePredicates(ExpressionType.CONJUNCTION_AND, whereClauses));
        ajNode.resolveSortDirection();
        return ajNode;
    }

    /**
     * A method to filter out single-TVE expressions.
     *
     * @param expr List of single-TVE expressions.
     * @param otherExprs List of multi-TVE expressions.
     * @return List of single-TVE expressions from the input collection.
     */
    private static List<AbstractExpression> filterSingleTVEExpressions(List<AbstractExpression> exprs,
            List<AbstractExpression> otherExprs) {
        List<AbstractExpression> singleTVEExprs = new ArrayList<>();
        for (AbstractExpression expr : exprs) {
            List<TupleValueExpression> tves = ExpressionUtil.getTupleValueExpressions(expr);
            if (tves.size() == 1) {
                singleTVEExprs.add(expr);
            } else {
                otherExprs.add(expr);
            }
        }
        return singleTVEExprs;
    }

    /**
     * For a join node, determines whether any of the inner-outer expressions were used
     * for an inner index access -- this requires joining with a NestLoopIndexJoin.
     * These are detected as TVE references in the various clauses that drive the indexing
     * -- as opposed to TVE references in post-filters that pose no problem with either
     * NLIJ or the more efficient (one-pass through the index) NestLoopJoin.
     *
     * @param innerTable - the Table of all inner TVEs that are exempt from the check.
     * @param indexExprs - a list of expressions used in the indexing
     * @param initialExpr - a list of expressions used in the indexing
     * @param endExprs - a list of expressions used in the indexing
     * @return true if at least one of the expression lists references a TVE.
     */
    private static boolean hasInnerOuterIndexExpression(
            String innerTableAlias, Collection<AbstractExpression> indexExprs,
            Collection<AbstractExpression> initialExpr, Collection<AbstractExpression> endExprs) {
        Set<AbstractExpression> indexedExprs = new HashSet<>();
        indexedExprs.addAll(indexExprs);
        indexedExprs.addAll(initialExpr);
        indexedExprs.addAll(endExprs);
        // Find an outer TVE by ignoring any TVEs based on the inner table.
        for (AbstractExpression indexed : indexedExprs) {
            if (! indexed.findAllTupleValueSubexpressions().stream()
                    .allMatch(e -> TupleValueExpression.isOperandDependentOnTable(e, innerTableAlias))) {
                return true;
            }
        }
        return false;
    }

}
