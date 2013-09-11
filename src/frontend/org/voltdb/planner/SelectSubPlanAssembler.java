/* This file is part of VoltDB.
 * Copyright (C) 2008-2013 VoltDB Inc.
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

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.voltdb.catalog.Database;
import org.voltdb.catalog.Table;
import org.voltdb.expressions.AbstractExpression;
import org.voltdb.expressions.ExpressionUtil;
import org.voltdb.expressions.TupleValueExpression;
import org.voltdb.plannodes.AbstractJoinPlanNode;
import org.voltdb.plannodes.AbstractPlanNode;
import org.voltdb.plannodes.IndexScanPlanNode;
import org.voltdb.plannodes.NestLoopIndexPlanNode;
import org.voltdb.plannodes.NestLoopPlanNode;
import org.voltdb.types.JoinType;
import org.voltdb.types.PlanNodeType;
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
    ArrayDeque<AbstractPlanNode> m_plans = new ArrayDeque<AbstractPlanNode>();

    /** The list of all possible join orders, assembled by queueAllJoinOrders */
    ArrayDeque<JoinNode> m_joinOrders = new ArrayDeque<JoinNode>();

    /**
     *
     * @param db The catalog's Database object.
     * @param parsedStmt The parsed and dissected statement object describing the sql to execute.
     * @param m_partitioning in/out param first element is partition key value, forcing a single-partition statement if non-null,
     * second may be an inferred partition key if no explicit single-partitioning was specified
     */
    SelectSubPlanAssembler(Database db, AbstractParsedStmt parsedStmt, PartitioningForStatement partitioning)
    {
        super(db, parsedStmt, partitioning);
        //If a join order was provided
        if (parsedStmt.joinOrder != null) {
            //Extract the table names from the , separated list
            ArrayList<String> tableNames = new ArrayList<String>();
            //Don't allow dups for now since self joins aren't supported
            HashSet<String> dupCheck = new HashSet<String>();
            for (String table : parsedStmt.joinOrder.split(",")) {
                tableNames.add(table.trim());
                if (!dupCheck.add(table.trim())) {
                    StringBuilder sb = new StringBuilder();
                    sb.append("The specified join order \"");
                    sb.append(parsedStmt.joinOrder).append("\" contains duplicate tables. ");
                    throw new RuntimeException(sb.toString());
                }
            }

            if (parsedStmt.tableList.size() != tableNames.size()) {
                StringBuilder sb = new StringBuilder();
                sb.append("The specified join order \"");
                sb.append(parsedStmt.joinOrder).append("\" does not contain the correct number of tables\n");
                sb.append("Expected ").append(parsedStmt.tableList.size());
                sb.append(" but found ").append(tableNames.size()).append(" tables");
                throw new RuntimeException(sb.toString());
            }

            Table tables[] = new Table[tableNames.size()];
            int zz = 0;
            ArrayList<Table> tableList = new ArrayList<Table>(parsedStmt.tableList);
            for (int qq = tableNames.size() - 1; qq >= 0; qq--) {
                String name = tableNames.get(qq);
                boolean foundMatch = false;
                for (int ii = 0; ii < tableList.size(); ii++) {
                    if (tableList.get(ii).getTypeName().equalsIgnoreCase(name)) {
                        tables[zz++] = tableList.remove(ii);
                        foundMatch = true;
                        break;
                    }
                }
                if (!foundMatch) {
                    StringBuilder sb = new StringBuilder();
                    sb.append("The specified join order \"");
                    sb.append(parsedStmt.joinOrder).append("\" contains ").append(name);
                    sb.append(" which doesn't exist in the FROM clause");
                    throw new RuntimeException(sb.toString());
                }
            }
            if (zz != tableNames.size()) {
                StringBuilder sb = new StringBuilder();
                sb.append("The specified join order \"");
                sb.append(parsedStmt.joinOrder).append("\" doesn't contain enough tables ");
                throw new RuntimeException(sb.toString());
            }
            if ( ! isValidJoinOrder(tableNames)) {
                throw new RuntimeException("The specified join order is invalid for the given query");
            }
            //m_parsedStmt.joinTree.m_joinOrder = tables;
            m_joinOrders.add(m_parsedStmt.joinTree);
        } else {
            queueAllJoinOrders();
        }
    }

    /**
     * Validate the specified join order against the join tree.
     * In general, outer joins are not associative and commutative. Not all orders are valid.
     * In case of a valid join order, the initial join tree is rebuilt to match the specified order
     * @param tables list of tables to join
     * @return true if the join order is valid
     */
    private boolean isValidJoinOrder(List<String> tableNames)
    {
        assert(m_parsedStmt.joinTree != null);

        // Split the original tree into the sub-trees having the same join type for all nodes
        List<JoinNode> subTrees = m_parsedStmt.joinTree.extractSubTrees();

        // For a sub-tree with inner joins only any join order is valid. The only requirement is that
        // each and every table from that sub-tree constitute an uninterrupted sequence in the specified join order
        // The outer joins are associative but changing the join order precedence
        // includes moving ON clauses to preserve the initial SQL semantics. For example,
        // T1 right join T2 on T1.C1 = T2.C1 left join T3 on T2.C2=T3.C2 can be rewritten as
        // T1 right join (T2 left join T3 on T2.C2=T3.C2) on T1.C1 = T2.C1
        // At the moment, such transformations are not supported. The specified joined order must
        // match the SQL order
        int tableNameIdx = 0;
        List<JoinNode> finalSubTrees = new ArrayList<JoinNode>();
        // we need to process the sub-trees last one first because the top sub-tree is the first one on the list
        for (int i = subTrees.size() - 1; i >= 0; --i) {
            JoinNode subTree = subTrees.get(i);
            // Get all tables for the subTree
            List<JoinNode> subTableNodes = subTree.generateLeafNodesJoinOrder();

            if (subTree.m_joinType == JoinType.INNER) {
                // Collect all the "real" tables from the sub-tree skipping the nodes representing
                // the sub-trees with the different join type (id < 0)
                Map<String, JoinNode> nodeNameMap = new HashMap<String, JoinNode>();
                for (JoinNode tableNode : subTableNodes) {
                    assert(tableNode.m_table != null);
                    if (tableNode.m_id >= 0) {
                        nodeNameMap.put(tableNode.m_table.getTypeName(), tableNode);
                    }
                }

                // rearrange the sub tree to match the order
                List<JoinNode> joinOrderSubNodes = new ArrayList<JoinNode>();
                for (int j = 0; j < subTableNodes.size(); ++j) {
                    if (subTableNodes.get(j).m_id >= 0) {
                        assert(tableNameIdx < tableNames.size());
                        String tableName = tableNames.get(tableNameIdx);
                        if (!nodeNameMap.containsKey(tableName)) {
                            return false;
                        }
                        joinOrderSubNodes.add(nodeNameMap.get(tableName));
                        ++tableNameIdx;
                    } else {
                        // It's dummy node
                        joinOrderSubNodes.add(subTableNodes.get(j));
                    }
                }
                JoinNode joinOrderSubTree = JoinNode.reconstructJoinTreeFromTableNodes(joinOrderSubNodes);
                //Collect all the join/where conditions to reassign them later
                AbstractExpression combinedWhereExpr = subTree.getAllInnerJoinFilters();
                if (combinedWhereExpr != null) {
                    joinOrderSubTree.m_whereExpr = (AbstractExpression)combinedWhereExpr.clone();
                }
                // The new tree root node id must match the original one to be able to reconnect the
                // subtrees
                joinOrderSubTree.m_id = subTree.m_id;
                finalSubTrees.add(0, joinOrderSubTree);
            } else {
                for (JoinNode tableNode : subTableNodes) {
                    assert(tableNode.m_table != null && tableNameIdx < tableNames.size());
                    if (tableNode.m_id >= 0) {
                        if (!tableNames.get(tableNameIdx++).equals(tableNode.m_table.getTypeName())) {
                            return false;
                        }
                    }
                }
                // add the sub-tree as is
                finalSubTrees.add(0, subTree);
            }
        }
        // if we got there the join order is OK. Rebuild the whole tree
        m_parsedStmt.joinTree = JoinNode.reconstructJoinTreeFromSubTrees(finalSubTrees);
        return true;
    }

    /**
     * Compute every permutation of the list of involved tables and put them in a deque.
     * TODO(XIN): takes at least 3.3% cpu of planner. Optimize it when possible.
     */
    private void queueAllJoinOrders() {
        // these just shouldn't happen right?
        assert(m_parsedStmt.noTableSelectionList.size() == 0);
        assert(m_parsedStmt.joinTree != null);

        // Simplify the outer join if possible
        JoinNode simplifiedJoinTree = simplifyOuterJoin(m_parsedStmt.joinTree);

        // The execution engine expects to see the outer table on the left side only
        // which means that RIGHT joins need to be converted to the LEFT ones
        simplifiedJoinTree.toLeftJoin();
        // Clone the original
        JoinNode clonedTree = (JoinNode) simplifiedJoinTree.clone();
        // Split join tree into a set of subtrees. The join type for all nodes in a subtree is the same
        List<JoinNode> subTrees = clonedTree.extractSubTrees();
        assert(!subTrees.isEmpty());
        // Generate possible join orders for each sub-tree separately
        ArrayList<ArrayList<JoinNode>> joinOrderList = generateJoinOrders(subTrees);
        // Reassemble the all possible combinations of the sub-tree and queue them
        queueSubJoinOrders(joinOrderList, 0, new ArrayList<JoinNode>());
}

    private void queueSubJoinOrders(List<ArrayList<JoinNode>> joinOrderList, int joinOrderListIdx, ArrayList<JoinNode> currentJoinOrder) {
        if (joinOrderListIdx == joinOrderList.size()) {
            // End of recursion
            assert(!currentJoinOrder.isEmpty());
            JoinNode joinTree = JoinNode.reconstructJoinTreeFromSubTrees(currentJoinOrder);
            m_joinOrders.add(joinTree);
            return;
        }
        // Recursive step
        ArrayList<JoinNode> nextTrees = joinOrderList.get(joinOrderListIdx);
        for (JoinNode headTree: nextTrees) {
            ArrayList<JoinNode> updatedJoinOrder = new ArrayList<JoinNode>();
            // Order is important: The top sub-trees must be first
            for (JoinNode node : currentJoinOrder) {
                updatedJoinOrder.add((JoinNode)node.clone());
            }
            updatedJoinOrder.add((JoinNode)headTree.clone());
            queueSubJoinOrders(joinOrderList, joinOrderListIdx + 1, updatedJoinOrder);
        }
    }

    /**
     * For each join tree from the input list generate a set of the join trees by permuting the leafs
     * (table nodes) of the tree without breaking the joins semantic.
     *
     * @param subTrees the list of join trees.
     * @return The list containing the list of trees of all possible permutations of the input trees
     */
    ArrayList<ArrayList<JoinNode>> generateJoinOrders(List<JoinNode> subTrees) {
        ArrayList<ArrayList<JoinNode>> permutations = new ArrayList<ArrayList<JoinNode>>();
        for (JoinNode subTree : subTrees) {
            ArrayList<JoinNode> treePermutations = new ArrayList<JoinNode>();
            if (subTree.m_joinType != JoinType.INNER) {
                // Permutations for Outer Join are not supported yet
                treePermutations.add(subTree);
            } else {
                // if all joins are inner then join orders can be obtained by the permutation of
                // the original tables. Get a list of the leaf nodes(tables) to permute them
                List<JoinNode> tableNodes = subTree.generateLeafNodesJoinOrder();
                List<List<JoinNode>> joinOrders = PermutationGenerator.generatePurmutations(tableNodes);
                List<JoinNode> newTrees = new ArrayList<JoinNode>();
                for (List<JoinNode> joinOrder: joinOrders) {
                    newTrees.add(JoinNode.reconstructJoinTreeFromTableNodes(joinOrder));
                }
                //Collect all the join/where conditions to reassign them later
                AbstractExpression combinedWhereExpr = subTree.getAllInnerJoinFilters();
                for (JoinNode newTree : newTrees) {
                    if (combinedWhereExpr != null) {
                        newTree.m_whereExpr = (AbstractExpression)combinedWhereExpr.clone();
                    }
                    // The new tree root node id must match the original one to be able to reconnect the
                    // subtrees
                    newTree.m_id = subTree.m_id;
                    treePermutations.add(newTree);
                }
            }
            permutations.add(treePermutations);
        }
        return permutations;
    }

    /**
     * Outer join simplification using null rejection.
     * http://citeseerx.ist.psu.edu/viewdoc/summary?doi=10.1.1.43.2531
     * Outerjoin Simplification and Reordering for Query Optimization
     * by Cesar A. Galindo-Legaria , Arnon Rosenthal
     * Algorithm:
     * Traverse the join tree top-down:
     *  For each join node n1 do:
     *    For each expression expr (join and where) at the node n1
     *      For each join node n2 descended from n1 do:
     *          If expr rejects nulls introduced by n2 inner table,
     *          then convert n2 to an inner join. If n2 is a full join then need repeat this step
     *          for n2 inner and outer tables
     */
    private JoinNode simplifyOuterJoin(JoinNode joinTree) {
        assert(joinTree != null);
        List<AbstractExpression> exprs = new ArrayList<AbstractExpression>();
        // For the top level node only WHERE expressions need to be evaluated for NULL-rejection
        if (joinTree.m_leftNode != null && joinTree.m_leftNode.m_whereExpr != null) {
            exprs.add(joinTree.m_leftNode.m_whereExpr);
        }
        if (joinTree.m_rightNode != null && joinTree.m_rightNode.m_whereExpr != null) {
            exprs.add(joinTree.m_rightNode.m_whereExpr);
        }
        simplifyOuterJoinRecursively(joinTree, exprs);
        return joinTree;
    }

    private void simplifyOuterJoinRecursively(JoinNode joinNode, List<AbstractExpression> exprs) {
        assert (joinNode != null);
        if (joinNode.m_table != null) {
            // End of the recursion. Nothing to simplify
            return;
        }
        assert(joinNode.m_leftNode != null);
        assert(joinNode.m_rightNode != null);
        JoinNode leftNode = joinNode.m_leftNode;
        JoinNode rightNode = joinNode.m_rightNode;
        JoinNode innerNode = null;
        if (joinNode.m_joinType == JoinType.LEFT) {
            innerNode = rightNode;
        } else if (joinNode.m_joinType == JoinType.RIGHT) {
            innerNode = leftNode;
        } else if (joinNode.m_joinType == JoinType.FULL) {
            // Full joins are not supported
            assert(false);
        }
        if (innerNode != null) {
            for (AbstractExpression expr : exprs) {
                if (innerNode.m_table != null) {
                    if (ExpressionUtil.isNullRejectingExpression(expr, innerNode.m_table.getTypeName())) {
                        // We are done at this level
                        joinNode.m_joinType = JoinType.INNER;
                        break;
                    }
                } else {
                    // This is a join node itself. Get all the tables underneath this node and
                    // see if the expression is NULL-rejecting for any of them
                    List<Table> tables = innerNode.generateTableJoinOrder();
                    boolean rejectNull = false;
                    for (Table table : tables) {
                        if (ExpressionUtil.isNullRejectingExpression(expr, table.getTypeName())) {
                            // We are done at this level
                            joinNode.m_joinType = JoinType.INNER;
                            rejectNull = true;
                            break;
                        }
                    }
                    if (rejectNull) {
                        break;
                    }
                }
            }
        }

        // Now add this node expression to the list and descend
        if (leftNode.m_joinExpr != null) {
            exprs.add(leftNode.m_joinExpr);
        }
        if (leftNode.m_whereExpr != null) {
            exprs.add(leftNode.m_whereExpr);
        }
        if (rightNode.m_joinExpr != null) {
            exprs.add(rightNode.m_joinExpr);
        }
        if (rightNode.m_whereExpr != null) {
            exprs.add(rightNode.m_whereExpr);
        }
        simplifyOuterJoinRecursively(joinNode.m_leftNode, exprs);
        simplifyOuterJoinRecursively(joinNode.m_rightNode, exprs);
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
            if (joinTree == null)
                return null;

            // Analyze join and filter conditions
            m_parsedStmt.analyzeJoinExpressions(joinTree);
            // a query that is a little too quirky or complicated.
            assert(m_parsedStmt.noTableSelectionList.size() == 0);

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
                HashMap<AbstractExpression, Set<AbstractExpression>>
                    valueEquivalence = joinTree.getAllEquivalenceFilters();
                int countOfIndependentlyPartitionedTables =
                        m_partitioning.analyzeForMultiPartitionAccess(m_parsedStmt.tableList,
                                                                      valueEquivalence);
                if (countOfIndependentlyPartitionedTables > 1) {
                    // The case of more than one independent partitioned table
                    // would result in an illegal plan with more than two fragments.
                    // Don't throw a planning error here, in case the problem is just with this
                    // particular join order, but do leave a hint to the PlanAssembler in case
                    // the failure is unanimous -- a common case.
                    m_recentErrorMsg =
                        "Join of multiple partitioned tables has insufficient join criteria.";
                    //System.out.println("DEBUG: bad partitioning for: " + joinTree);
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
     * @param joinOrder An array of tables in the join order.
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
        if (joinNode.m_table != null) {
            // This is a select from a single table
            joinNode.m_accessPaths.addAll(getRelevantAccessPathsForTable(joinNode.m_table,
                    joinNode.m_joinInnerList,
                    joinNode.m_whereInnerList,
                    null));
        } else {
            assert (joinNode.m_leftNode != null && joinNode.m_rightNode != null);
            generateOuterAccessPaths(joinNode);
            generateInnerAccessPaths(joinNode);
            // An empty access path for the root
            joinNode.m_accessPaths.add(new AccessPath());
        }
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
    private void generateOuterAccessPaths(JoinNode parentNode) {
        assert(parentNode.m_leftNode != null);
        JoinNode outerChildNode = parentNode.m_leftNode;
        List<AbstractExpression> joinOuterList =  (parentNode.m_joinType == JoinType.INNER) ?
                parentNode.m_joinOuterList : null;
        if (outerChildNode.m_table == null) {
            assert (outerChildNode.m_leftNode != null && outerChildNode.m_rightNode != null);
            generateOuterAccessPaths(outerChildNode);
            generateInnerAccessPaths(outerChildNode);
            // The join node can have only sequential scan access
            outerChildNode.m_accessPaths.add(getRelevantNaivePath(joinOuterList, parentNode.m_whereOuterList));
        } else {
            assert (outerChildNode.m_table != null);
            outerChildNode.m_accessPaths.addAll(getRelevantAccessPathsForTable(outerChildNode.m_table,
                    joinOuterList,
                    parentNode.m_whereOuterList,
                    null));
        }
        assert(outerChildNode.m_accessPaths.size() > 0);
    }

    /**
     * Generate all possible access paths for an inner node in a join.
     * The set of potential index expressions depends whether the inner node can be inlined
     * with the NLIJ or not. In the former case, inner and inner-outer join expressions can
     * be considered for the index access. In the latter, only inner join expressions qualifies.
     *
     * @param parentNode A parent node to the node to generate paths to.
     */
    private void generateInnerAccessPaths(JoinNode parentNode) {
        assert(parentNode.m_rightNode != null);
        JoinNode innerChildNode = parentNode.m_rightNode;
        // In case of inner join WHERE and JOIN expressions can be merged
        if (parentNode.m_joinType == JoinType.INNER) {
            parentNode.m_joinInnerOuterList.addAll(parentNode.m_whereInnerOuterList);
            parentNode.m_whereInnerOuterList.clear();
            parentNode.m_joinInnerList.addAll(parentNode.m_whereInnerList);
            parentNode.m_whereInnerList.clear();
        }
        if (innerChildNode.m_table == null) {
            assert (innerChildNode.m_leftNode != null && innerChildNode.m_rightNode != null);
            generateOuterAccessPaths(innerChildNode);
            generateInnerAccessPaths(innerChildNode);
            // The inner node is a join node itself. Only naive access path is possible
            innerChildNode.m_accessPaths.add(getRelevantNaivePath(parentNode.m_joinInnerOuterList, parentNode.m_joinInnerList));
            return;
        }

        // The inner table can have multiple index access paths based on
        // inner and inner-outer join expressions plus the naive one.
        innerChildNode.m_accessPaths.addAll(getRelevantAccessPathsForTable(innerChildNode.m_table,
                parentNode.m_joinInnerOuterList,
                parentNode.m_joinInnerList,
                null));

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
        boolean mayNeedInnerSendReceive = ( ! m_partitioning.wasSpecifiedAsSingle()) &&
                (m_partitioning.getCountOfPartitionedTables() > 0) &&
                (parentNode.m_joinType != JoinType.INNER) &&
                ! innerChildNode.m_table.getIsreplicated();
// too expensive/complicated to test here? (parentNode.m_leftNode has a replicated result?) &&


        if (mayNeedInnerSendReceive && ! parentNode.m_joinInnerOuterList.isEmpty()) {
            List<AccessPath> innerOuterAccessPaths = new ArrayList<AccessPath>();
            for (AccessPath innerAccessPath : innerChildNode.m_accessPaths) {
                if ((innerAccessPath.index != null) &&
                    hasInnerOuterIndexExpression(innerChildNode.m_table,
                                                 innerAccessPath.indexExprs,
                                                 innerAccessPath.initialExpr,
                                                 innerAccessPath.endExprs)) {
                    innerOuterAccessPaths.add(innerAccessPath);
                }
            }
            Collection<AccessPath> nljAccessPaths =
                    getRelevantAccessPathsForTable(innerChildNode.m_table,
                                                   null,
                                                   parentNode.m_joinInnerList,
                                                   parentNode.m_joinInnerOuterList);
            innerChildNode.m_accessPaths.clear();
            innerChildNode.m_accessPaths.addAll(nljAccessPaths);
            innerChildNode.m_accessPaths.addAll(innerOuterAccessPaths);
        }

        assert(innerChildNode.m_accessPaths.size() > 0);
    }

    /**
     * generate all possible plans for the tree.
     *
     * @param rootNode The root node for the whole join tree.
     * @param nodes The node list to iterate over.
     */
    private void generateSubPlanForJoinNodeRecursively(JoinNode rootNode,
                                                       int nextNode, List<JoinNode> nodes)
    {
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
            return;
        }

        for (AccessPath path : joinNode.m_accessPaths) {
            joinNode.m_currentAccessPath = path;
            generateSubPlanForJoinNodeRecursively(rootNode, nextNode+1, nodes);
        }
    }

    /**
     * Given a specific join node and access path set for inner and outer tables, construct the plan
     * that gives the right tuples.
     *
     * @param joinNode The join node to build the plan for.
     * @param isInnerTable True if the join node is the inner node in the join
     * @return A completed plan-sub-graph that should match the correct tuples from the
     * correct tables.
     */
    private AbstractPlanNode getSelectSubPlanForJoinNode(JoinNode joinNode) {
        assert(joinNode != null);
        if (joinNode.m_table == null) {
            assert(joinNode.m_leftNode != null && joinNode.m_rightNode != null);
            // Outer node
            AbstractPlanNode outerScanPlan = getSelectSubPlanForJoinNode(joinNode.m_leftNode);
            if (outerScanPlan == null) {
                return null;
            }

            // Inner Node.
            AbstractPlanNode innerScanPlan = getSelectSubPlanForJoinNode(joinNode.m_rightNode);
            if (innerScanPlan == null) {
                return null;
            }

            // Join Node
            return getSelectSubPlanForJoin(joinNode, outerScanPlan, innerScanPlan);
        }
        // End of recursion
        AbstractPlanNode scanNode = getAccessPlanForTable(joinNode.m_table, joinNode.m_currentAccessPath);
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
    private AbstractPlanNode getSelectSubPlanForJoin(JoinNode joinNode,
                                                     AbstractPlanNode outerPlan,
                                                     AbstractPlanNode innerPlan)
    {
        // Filter (post-join) expressions
        ArrayList<AbstractExpression> whereClauses  = new ArrayList<AbstractExpression>();
        whereClauses.addAll(joinNode.m_whereInnerList);
        whereClauses.addAll(joinNode.m_whereInnerOuterList);

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

        assert(joinNode.m_rightNode != null);
        AccessPath innerAccessPath = joinNode.m_rightNode.m_currentAccessPath;
        // We may need to add a send/receive pair to the inner plan for the special case.
        // This trick only works once per plan, BUT once the partitioned data has been
        // received on the coordinator, it can be treated as replicated data in later
        // joins, which MAY help with later outer joins with replicated data.
        boolean needInnerSendReceive = ( ! m_partitioning.wasSpecifiedAsSingle()) &&
                                       (m_partitioning.getCountOfPartitionedTables() > 0) &&
                                       (joinNode.m_joinType != JoinType.INNER) &&
                                       ( ! hasReplicatedResult(innerPlan)) &&
                                       hasReplicatedResult(outerPlan);

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
            if (hasInnerOuterIndexExpression(joinNode.m_rightNode.m_table,
                                             innerAccessPath.indexExprs,
                                             innerAccessPath.initialExpr,
                                             innerAccessPath.endExprs)) {
                canHaveNLJ = false;
            }
        }
        else {
            canHaveNLIJ = false;
        }
        if (needInnerSendReceive) {
            canHaveNLIJ = false;
        }

        AbstractJoinPlanNode ajNode = null;
        if (canHaveNLJ) {
            NestLoopPlanNode nljNode = new NestLoopPlanNode();
            // get all the clauses that join the applicable two tables
            ArrayList<AbstractExpression> joinClauses = innerAccessPath.joinExprs;
            if (innerPlan instanceof IndexScanPlanNode) {
                // InnerPlan is an IndexScan. In this case the inner and inner-outer
                // non-index join expressions (if any) are in the otherExpr. The former should stay as
                // an IndexScanPlan predicate and the latter stay at the NLJ node as a join predicate
                List<AbstractExpression> innerExpr = filterSingleTVEExpressions(innerAccessPath.otherExprs);
                joinClauses.addAll(innerAccessPath.otherExprs);
                AbstractExpression indexScanPredicate = ExpressionUtil.combine(innerExpr);
                ((IndexScanPlanNode)innerPlan).setPredicate(indexScanPredicate);
            }
            nljNode.setJoinPredicate(ExpressionUtil.combine(joinClauses));

            // combine the tails plan graph with the new head node
            nljNode.addAndLinkChild(outerPlan);

            // If successful in the special case, the NLJ plan must be modified to cause the
            // partitioned inner data to be sent to the coordinator prior to the join.
            // This is done by adding send and receive plan nodes between the NLJ and its
            // right child node.
            if (needInnerSendReceive) {
                // This trick only works once per plan.
                if (outerPlan.hasAnyNodeOfType(PlanNodeType.RECEIVE) || innerPlan.hasAnyNodeOfType(PlanNodeType.RECEIVE)) {
                    return null;
                }
                innerPlan = addSendReceivePair(innerPlan);
            }

            nljNode.addAndLinkChild(innerPlan);
            ajNode = nljNode;
        }
        else if (canHaveNLIJ) {
            NestLoopIndexPlanNode nlijNode = new NestLoopIndexPlanNode();

            IndexScanPlanNode innerNode = (IndexScanPlanNode) innerPlan;
            // Set IndexScan predicate
            innerNode.setPredicate(ExpressionUtil.combine(innerAccessPath.otherExprs));

            nlijNode.addInlinePlanNode(innerPlan);

            // combine the tails plan graph with the new head node
            nlijNode.addAndLinkChild(outerPlan);

            ajNode = nlijNode;
        }
        else {
            m_recentErrorMsg =
                "Unsupported special case of complex OUTER JOIN between replicated outer table and partitioned inner table.";
            return null;
        }
        ajNode.setJoinType(joinNode.m_joinType);
        ajNode.setPreJoinPredicate(ExpressionUtil.combine(joinNode.m_joinOuterList));
        ajNode.setWherePredicate(ExpressionUtil.combine(whereClauses));
        ajNode.resolveSortDirection();
        return ajNode;
    }

    private boolean hasReplicatedResult(AbstractPlanNode plan)
    {
        HashSet<String> tablesRead = new HashSet<String>();
        plan.getTablesReadByFragment(tablesRead);
        for (String tableName : tablesRead) {
            Table table = m_parsedStmt.getTableFromDB(tableName);
            if ( ! table.getIsreplicated()) {
                return false;
            }

        }
        return true;
    }

    /**
     * A method to filter out single TVE expressions.
     *
     * @param expr List of expressions.
     * @return List of single TVE expressions from the input collection.
     *         They are also removed from the input.
     */
    private List<AbstractExpression> filterSingleTVEExpressions(List<AbstractExpression> exprs) {
        List<AbstractExpression> singleTVEExprs = new ArrayList<AbstractExpression>();
        for (AbstractExpression expr : exprs) {
            List<TupleValueExpression> tves = ExpressionUtil.getTupleValueExpressions(expr);
            if (tves.size() == 1) {
                singleTVEExprs.add(expr);
            }
        }
        exprs.removeAll(singleTVEExprs);
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
    private boolean hasInnerOuterIndexExpression(Table innerTable,
                                                 Collection<AbstractExpression> indexExprs,
                                                 Collection<AbstractExpression> initialExpr,
                                                 Collection<AbstractExpression> endExprs)
    {
        HashSet<AbstractExpression> indexedExprs = new HashSet<AbstractExpression>();
        indexedExprs.addAll(indexExprs);
        indexedExprs.addAll(initialExpr);
        indexedExprs.addAll(endExprs);
        // Find an outer TVE by ignoring any TVEs based on the inner table.
        for (AbstractExpression indexed : indexedExprs) {
            Collection<AbstractExpression> indexedTVEs = indexed.findBaseTVEs();
            for (AbstractExpression indexedTVExpr : indexedTVEs) {
                if ( ! TupleValueExpression.isOperandDependentOnTable(indexedTVExpr, innerTable)) {
                    return true;
                }
            }
        }
        return false;
    }

}
