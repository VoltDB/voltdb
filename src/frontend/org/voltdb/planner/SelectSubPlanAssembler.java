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
import org.voltdb.planner.JoinNode;
import org.voltdb.plannodes.AbstractJoinPlanNode;
import org.voltdb.plannodes.AbstractPlanNode;
import org.voltdb.plannodes.IndexScanPlanNode;
import org.voltdb.plannodes.NestLoopIndexPlanNode;
import org.voltdb.plannodes.NestLoopPlanNode;
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
        List<JoinNode> subTrees = new ArrayList<JoinNode>();
        extractSubTrees(m_parsedStmt.joinTree, subTrees);

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
                JoinNode joinOrderSubTree = reconstructJoinTreeFromTableNodes(joinOrderSubNodes);
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
        m_parsedStmt.joinTree = reconstructJoinTreeFromSubTrees(finalSubTrees);
        return true;
    }

    /**
     * Compute every permutation of the list of involved tables and put them in a deque.
     */
    private void queueAllJoinOrders() {
        // these just shouldn't happen right?
        assert(m_parsedStmt.multiTableSelectionList.size() == 0);
        assert(m_parsedStmt.noTableSelectionList.size() == 0);

        queueSubJoinOrders();
    }

    /**
     * Add all valid join orders (permutations) for the input join tree.
     *
     */
    private void queueSubJoinOrders() {
        assert(m_parsedStmt.joinTree != null);

        // Simplify the outer join if possible
        JoinNode simplifiedJoinTree = simplifyOuterJoin(m_parsedStmt.joinTree);

        // The execution engine expects to see the outer table on the left side only
        // which means that RIGHT joins need to be converted to the LEFT ones
        simplifiedJoinTree.toLeftJoin();
        // Clone the original
        JoinNode clonedTree = (JoinNode) simplifiedJoinTree.clone();
        // Split join tree into a set of subtrees. The join type for all nodes in a subtree is the same
        List<JoinNode> subTrees = new ArrayList<JoinNode>();
        extractSubTrees(clonedTree, subTrees);
        assert(!subTrees.isEmpty());
        // Generate possible join orders for each sub-tree separately
        ArrayList<ArrayList<JoinNode>> joinOrderList = generateJoinOrders(subTrees);
        // Reassemble the all possible combinations of the sub-tree and queue them
        queueSubJoinOrders(joinOrderList, 0, new ArrayList<JoinNode>());
}

    /**
     * Split a join tree into one or more sub-trees. Each sub-tree has the same join type
     * for all join nodes.
     * @param root - The root of the join tree
     * @param subTrees - the list of sub-trees from the input tree
     */
    private void extractSubTrees(JoinNode root, List<JoinNode> subTrees) {
        // Extract the first sub-tree starting at the root
        List<JoinNode> leafNodes = new ArrayList<JoinNode>();
        extractSubTree(root, leafNodes);
        subTrees.add(root);

        // Continue with the leafs
        for (JoinNode leaf : leafNodes) {
            extractSubTrees(leaf, subTrees);
        }
    }

    /**
     * Starting from the root recurse to its children stopping at the first join node
     * of the different type and discontinue the tree at this point by replacing the join node with
     * the temporary node which id matches the join node id. This join node is the root of the next
     * sub-tree.
     * @param root - The root of the join tree
     * @param leafNodes - the list of the root nodes of the next sub-trees
     */
    private void extractSubTree(JoinNode root, List<JoinNode> leafNodes) {
        if (root.m_table != null) {
            return;
        }
        JoinNode[] children = {root.m_leftNode, root.m_rightNode};
        for (JoinNode child : children) {

            // Leaf nodes don't have a significant join type,
            // test for them first and never attempt to start a new tree at a leaf.
            if (child.m_table != null) {
                continue;
            }

            if (child.m_joinType == root.m_joinType) {
                // The join type for this node is the same as the root's one
                // Keep walking down the tree
                extractSubTree(child, leafNodes);
            } else {
                // The join type for this join differs from the root's one
                // Terminate the sub-tree
                leafNodes.add(child);
                // Replace the join node with the temporary node having the id negated
                // This will help to distinguish it from a real node and to reassemble the tree at the later stage
                JoinNode tempNode = new JoinNode(
                        -child.m_id, child.m_joinType, new Table(), child.m_joinExpr, child.m_whereExpr);
                if (child == root.m_leftNode) {
                    root.m_leftNode = tempNode;
                } else {
                    root.m_rightNode = tempNode;
                }
            }
        }
    }

    private void queueSubJoinOrders(List<ArrayList<JoinNode>> joinOrderList, int joinOrderListIdx, ArrayList<JoinNode> currentJoinOrder) {
        if (joinOrderListIdx == joinOrderList.size()) {
            // End of recursion
            assert(!currentJoinOrder.isEmpty());
            JoinNode joinTree = reconstructJoinTreeFromSubTrees(currentJoinOrder);
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
                    newTrees.add(reconstructJoinTreeFromTableNodes(joinOrder));
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
     * Reconstruct a join tree from the list of tables always appending the next node to the right.
     *
     * @param tableNodes the list of tables to build the tree from.
     * @return The reconstructed tree
     */
    private JoinNode reconstructJoinTreeFromTableNodes(List<JoinNode> tableNodes) {
        JoinNode root = null;
        for (JoinNode leafNode : tableNodes) {
            assert(leafNode.m_table != null);
            JoinNode node = new JoinNode(leafNode.m_id, leafNode.m_joinType, leafNode.m_table, null, null);
            if (root == null) {
                root = node;
            } else {
                // We only care about the root node id to be able to reconnect the sub-trees
                // The intermediate node id can be anything. For the final root node its id
                // will be set later to the original tree's root id
                root = new JoinNode(-node.m_id, JoinType.INNER, root, node);
            }
        }
        return root;
    }

    /**
     * Reconstruct a join tree from the list of sub-trees by replacing the nodes with the negative ids with
     * the root of the next sub-tree.
     *
     * @param subTrees the list of sub trees.
     * @return The reconstructed tree
     */
    JoinNode reconstructJoinTreeFromSubTrees(List<JoinNode> subTrees) {
        // Reconstruct the tree. The first element is the first sub-tree and so on
        JoinNode joinNode = subTrees.get(0);
        for (int i = 1; i < subTrees.size(); ++i) {
            JoinNode nextNode = subTrees.get(i);
            boolean replaced = replaceChild(joinNode, nextNode);
            // There must be a node in the current tree to be replaced
            assert(replaced == true);
        }
        return joinNode;
    }

    private boolean replaceChild(JoinNode root, JoinNode node) {
        // can't replace self
        assert (root != null && Math.abs(root.m_id) != Math.abs(node.m_id));
        if (root.m_table != null) {
            return false;
        } else if (Math.abs(root.m_leftNode.m_id) == Math.abs(node.m_id)) {
            root.m_leftNode  = node;
            return true;
        } else if (Math.abs(root.m_rightNode.m_id) == Math.abs(node.m_id)) {
            root.m_rightNode  = node;
            return true;
        } else if (replaceChild(root.m_leftNode, node) == true) {
            return true;
        } else if (replaceChild(root.m_rightNode, node) == true) {
            return true;
        }

        return false;
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

            // generate more plans
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
        generateSubPlanForJoinNodeRecursively(joinTree, nodes);
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
        // If the join is INNER or the inner table is replicated or the send/receive pair can be deferred,
        // the join node can be NLIJ, otherwise it will be NLJ even for an index access path.
        if (parentNode.m_joinType == JoinType.INNER || innerChildNode.m_table.getIsreplicated() ||
                canDeferSendReceivePairForNode()) {
            // This case can support either NLIJ -- assuming joinNode.m_joinInnerOuterList
            // is non-empty AND at least ONE of its clauses can be leveraged in the IndexScan
            // -- or NLJ, otherwise.
            innerChildNode.m_accessPaths.addAll(getRelevantAccessPathsForTable(innerChildNode.m_table,
                    parentNode.m_joinInnerOuterList,
                    parentNode.m_joinInnerList,
                    null));
        } else {
            // Only NLJ is supported in this case.
            // If the join is NLJ, the inner node won't be inlined
            // which means that it can't use inner-outer join expressions
            // -- they must be set aside to be processed within the NLJ.
            innerChildNode.m_accessPaths.addAll(getRelevantAccessPathsForTable(innerChildNode.m_table,
                    null,
                    parentNode.m_joinInnerList,
                    parentNode.m_joinInnerOuterList));
        }

        assert(innerChildNode.m_accessPaths.size() > 0);
    }

    /**
     * generate all possible plans for the tree.
     *
     * @param rootNode The root node for the whole join tree.
     * @param nodes The node list to iterate over.
     */
    private void generateSubPlanForJoinNodeRecursively(JoinNode rootNode, List<JoinNode> nodes) {
        assert(nodes.size() > 0);
        JoinNode joinNode = nodes.get(0);
        if (nodes.size() == 1) {
            for (AccessPath path : joinNode.m_accessPaths) {
                joinNode.m_currentAccessPath = path;
                AbstractPlanNode plan = getSelectSubPlanForJoinNode(rootNode, false);
                if (plan == null) {
                    continue;
                }
                /*
                 * If the access plan for the table in the join order was for a
                 * distributed table scan there will be a send/receive pair at the top.
                 */
                if (m_partitioning.getCountOfPartitionedTables() > 1 && m_partitioning.requiresTwoFragments()) {
                    plan = addSendReceivePair(plan);
                }

                m_plans.add(plan);
            }
        } else {
            for (AccessPath path : joinNode.m_accessPaths) {
                joinNode.m_currentAccessPath = path;
                generateSubPlanForJoinNodeRecursively(rootNode, nodes.subList(1, nodes.size()));
            }
        }
    }

    /**
     * Given a specific join node and access path set for inner and outer tables, construct the plan
     * that gives the right tuples. If
     *
     * @param joinNode The join node to build the plan for.
     * @param isInnerTable True if the join node is the inner node in the join
     * @return A completed plan-sub-graph that should match the correct tuples from the
     * correct tables.
     */
    private AbstractPlanNode getSelectSubPlanForJoinNode(JoinNode joinNode, boolean isInnerNode) {
        assert(joinNode != null);
        if (joinNode.m_table != null) {
            // End of recursion
            AbstractPlanNode scanNode = getAccessPlanForTable(joinNode.m_table, joinNode.m_currentAccessPath);
            // Add the send/receive pair to the outer table only if required.
            // In case of the inner table, the position of the send/receive pair depends on the join type and
            // will be be added later (if required) during the join node construction.
            // For the inner join, it will be placed above the join node to allow for
            // the NLIJ/inline IndexScan plan. For the outer join, the pair must be added
            // immediately above the table node.
            if (!isInnerNode && !joinNode.m_table.getIsreplicated() && !canDeferSendReceivePairForNode()) {
                scanNode = addSendReceivePair(scanNode);
            }
            return scanNode;
        } else {
            assert(joinNode.m_leftNode != null && joinNode.m_rightNode != null);
            // Outer node
            AbstractPlanNode outerScanPlan = getSelectSubPlanForJoinNode(joinNode.m_leftNode, false);
            if (outerScanPlan == null) {
                return null;
            }

            // Inner Node.
            AbstractPlanNode innerScanPlan = getSelectSubPlanForJoinNode(joinNode.m_rightNode, true);
            if (innerScanPlan == null) {
                return null;
            }

            // Join Node
            return getSelectSubPlanForOuterAccessPathStep(joinNode, outerScanPlan, innerScanPlan);
        }
    }


   /**
     * Given a join node and plan-sub-graph for outer and inner sub-nodes,
     * construct the plan-sub-graph for that node.
     *
     * @param joinNode A parent join node.
     * @param outerPlan The outer node plan-sub-graph.
     * @param innerPlan The inner node plan-sub-graph.
     * @return A completed plan-sub-graph or null if a valid plan can not be produced for given access paths.
     */
    private AbstractPlanNode getSelectSubPlanForOuterAccessPathStep(JoinNode joinNode, AbstractPlanNode outerPlan, AbstractPlanNode innerPlan) {
        // Filter (post-join) expressions
        ArrayList<AbstractExpression> whereClauses  = new ArrayList<AbstractExpression>();
        whereClauses.addAll(joinNode.m_whereInnerList);
        whereClauses.addAll(joinNode.m_whereInnerOuterList);

        AccessPath innerAccessPath = joinNode.m_rightNode.m_currentAccessPath;

        AbstractJoinPlanNode ajNode = null;
        AbstractPlanNode retval = null;
        // We may need to add a send/receive pair to the inner plan. The outer plan should already have it
        // if it's required.
        assert(joinNode.m_rightNode != null);
        boolean needInnerSendReceive = joinNode.m_rightNode.m_table != null &&
                (!joinNode.m_rightNode.m_table.getIsreplicated() && !canDeferSendReceivePairForNode());

        // In case of innerPlan being an IndexScan the NLIJ will have an advantage
        // over the NLJ/IndexScan only if there is at least one inner-outer join expression
        // that is used for the index access. If this is the case then this expression
        // will be missing from the otherExprs list but is in the original joinNode.m_joinInnerOuterList
        // If not, NLJ/IndexScan is a better choice.
        // An additional requirement for the outer joins is that the inner node should not require
        // the send/receive pair. Otherwise, the outer table will be joined with the individual
        // partitions instead of the whole table leading to the erroneous rows in the result set.
        boolean canHaveNLIJ = innerPlan instanceof IndexScanPlanNode &&
                hasInnerOuterIndexExpression(joinNode.m_joinInnerOuterList, innerAccessPath.otherExprs) &&
                (joinNode.m_joinType == JoinType.INNER || !needInnerSendReceive);
        if (canHaveNLIJ) {
            NestLoopIndexPlanNode nlijNode = new NestLoopIndexPlanNode();

            nlijNode.setJoinType(joinNode.m_joinType);

            @SuppressWarnings("unused")
            IndexScanPlanNode innerNode = (IndexScanPlanNode) innerPlan;
            // Set IndexScan predicate
            innerNode.setPredicate(ExpressionUtil.combine(innerAccessPath.otherExprs));

            nlijNode.addInlinePlanNode(innerPlan);

            // combine the tails plan graph with the new head node
            nlijNode.addAndLinkChild(outerPlan);
            // now generate the output schema for this join
            nlijNode.generateOutputSchema(m_db);

            ajNode = nlijNode;
            retval = (needInnerSendReceive) ? addSendReceivePair(ajNode) : ajNode;
        } else {
            // get all the clauses that join the applicable two tables
            ArrayList<AbstractExpression> joinClauses = innerAccessPath.joinExprs;
            if (innerPlan instanceof IndexScanPlanNode) {
                // InnerPlan is an IndexScan. If there is a need for the intermediate send/receive pair
                // the index can not be based on the inner-outer join expression because the outer table
                // won't be 'visible' for the IndexScan node
                if (needInnerSendReceive && hasTableTVE(joinNode.m_leftNode, innerAccessPath)) {
                    return null;
                }
                // The inner and inner-outer non-index join expressions (if any) are in the otherExpr container.
                // The former should stay as an IndexScanPlan predicate and the latter stay at the NLJ node
                // as a join predicate
                List<AbstractExpression> innerExpr = filterSingleTVEExpressions(innerAccessPath.otherExprs);
                joinClauses.addAll(innerAccessPath.otherExprs);
                AbstractExpression indexScanPredicate = ExpressionUtil.combine(innerExpr);
                ((IndexScanPlanNode)innerPlan).setPredicate(indexScanPredicate);
            }
            NestLoopPlanNode nljNode = new NestLoopPlanNode();
            nljNode.setJoinPredicate(ExpressionUtil.combine(joinClauses));
            nljNode.setJoinType(joinNode.m_joinType);

            // combine the tails plan graph with the new head node
            nljNode.addAndLinkChild(outerPlan);

            // Add send/receive pair above the inner plan
            if (needInnerSendReceive) {
                innerPlan = addSendReceivePair(innerPlan);
            }

            nljNode.addAndLinkChild(innerPlan);
            // now generate the output schema for this join
            nljNode.generateOutputSchema(m_db);

            ajNode = nljNode;
            retval = ajNode;
        }

        ajNode.setPreJoinPredicate(ExpressionUtil.combine(joinNode.m_joinOuterList));

        ajNode.setWherePredicate(ExpressionUtil.combine(whereClauses));

        return retval;
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
     * For a join node determines whether any of the inner-outer expressions were used
     * for an index access.
     *
     * @param originalInnerOuterExprs The initial list of inner-outer join expressions.
     * @param nonIndexInnerOuterList The list of inner-outer join expressions which are not
     *        used for an index access
     * @return true if at least one of the original expressions is used for index access.
     */
    private boolean hasInnerOuterIndexExpression(List<AbstractExpression> originalInnerOuterExprs,
            List<AbstractExpression> nonIndexInnerOuterList) {
        HashSet<AbstractExpression> nonIndexInnerOuterSet = new HashSet<AbstractExpression>();
        nonIndexInnerOuterSet.addAll(nonIndexInnerOuterList);
        for (AbstractExpression originalInnerOuterExpr : originalInnerOuterExprs) {
            if (nonIndexInnerOuterSet.contains(originalInnerOuterExpr)) {
                nonIndexInnerOuterSet.remove(originalInnerOuterExpr);
            } else {
                return true;
            }
        }
        return false;
    }

    /**
     * For a join node determines whether any of the end or index expressions for a given access path
     * has a TVE based on a child table of the input node.
     *
     * @param joinNode JoinNode.
     * @param accessPath Access path
     * @return true if at least one of the tables is involved in the index expressions.
     */
    boolean hasTableTVE(JoinNode joinNode, AccessPath accessPath) {
        assert (joinNode != null);
        // Get the list of tables for a given node
        List<Table> tables = joinNode.generateTableJoinOrder();
        Set<String> tableNames = new HashSet<String>();
        for (Table table : tables) {
            tableNames.add(table.getTypeName());
        }
        // Collect all TVEs
        List<TupleValueExpression> tves= new ArrayList<TupleValueExpression>();
        for (AbstractExpression expr : accessPath.indexExprs) {
            tves.addAll(ExpressionUtil.getTupleValueExpressions(expr));
        }
        for (AbstractExpression expr : accessPath.endExprs) {
            tves.addAll(ExpressionUtil.getTupleValueExpressions(expr));
        }
        Set<String> tveTableNames = new HashSet<String>();
        for (TupleValueExpression tve : tves) {
            tveTableNames.add(tve.getTableName());
        }
        tveTableNames.retainAll(tableNames);
        return !tveTableNames.isEmpty();
    }
}
