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
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

import org.voltdb.catalog.Database;
import org.voltdb.catalog.Table;
import org.voltdb.expressions.AbstractExpression;
import org.voltdb.expressions.ExpressionUtil;
import org.voltdb.planner.JoinTree.JoinNode;
import org.voltdb.plannodes.AbstractJoinPlanNode;
import org.voltdb.plannodes.AbstractPlanNode;
import org.voltdb.plannodes.IndexScanPlanNode;
import org.voltdb.plannodes.NestLoopIndexPlanNode;
import org.voltdb.plannodes.NestLoopPlanNode;
import org.voltdb.types.JoinType;

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
    ArrayDeque<JoinTree> m_joinOrders = new ArrayDeque<JoinTree>();

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
            m_parsedStmt.joinTree.m_joinOrder = tables;
            m_joinOrders.add(m_parsedStmt.joinTree);
        } else {
            queueAllJoinOrders();
        }
    }

    /**
     * Validate the specified join order against the join tree.
     * In general, outer joins are not associative and commutative. Not all orders are valid.
     * @param tables list of tables to join
     * @return true if the join order is valid
     */
    private boolean isValidJoinOrder(List<String> tableNames)
    {
        if ( ! m_parsedStmt.joinTree.m_hasOuterJoin) {
            // The inner join is commutative. Any order is valid.
            return true;
        }

        // In general, the outer joins are associative but changing the join order precedence
        // includes moving ON clauses to preserve the initial SQL semantics. For example,
        // T1 right join T2 on T1.C1 = T2.C1 left join T3 on T2.C2=T3.C2 can be rewritten as
        // T1 right join (T2 left join T3 on T2.C2=T3.C2) on T1.C1 = T2.C1
        // At the moment, such transformations are not supported. The specified joined order must
        // match the SQL order
        Table[] joinOrder = m_parsedStmt.joinTree.generateJoinOrder().toArray(new Table[0]);
        assert(joinOrder.length == tableNames.size());
        int i = 0;
        for (Table table : joinOrder) {
            if (!table.getTypeName().equalsIgnoreCase(tableNames.get(i))) {
                return false;
            }
        }
        // The outer join matched the specified join order.
        return true;
    }

    /**
     * Compute every permutation of the list of involved tables and put them in a deque.
     */
    private void queueAllJoinOrders() {
        // these just shouldn't happen right?
        assert(m_parsedStmt.multiTableSelectionList.size() == 0);
        assert(m_parsedStmt.noTableSelectionList.size() == 0);

        if (m_parsedStmt.joinTree.m_hasOuterJoin) {
            queueOuterSubJoinOrders();
        } else {
            queueInnerSubJoinOrders();
        }
    }

    /**
     * Add all valid join orders (permutations) for the input join tree.
     *
     */
    private void queueOuterSubJoinOrders() {
        assert(m_parsedStmt.joinTree != null);
        // TODO ENG_3038 >2 table outer join
        // All join tree modifications must be perform on a clone of the original tree

        // Simplify the outer join if possible
        JoinTree simplifiedJoinTree = simplifyOuterJoin(m_parsedStmt.joinTree);
        // It is possible that simplified tree has inner joins only
        if (simplifiedJoinTree.m_hasOuterJoin == false) {
            queueInnerSubJoinOrders();
            return;
        }

        // The execution engine expects to see the outer table on the left side only
        // which means that RIGHT joins need to be converted to the LEFT ones
        simplifiedJoinTree.m_root.toLeftJoin();
        m_joinOrders.add(simplifiedJoinTree);
    }

    /**
     * Add all join orders (permutations) for the input table list.
     */
    private void queueInnerSubJoinOrders() {
        // if all joins are inner then all join orders obtained by the permutation of
        // the original tables are valid. Create arrays of the tables to permute them
        Table[] inputTables = new Table[m_parsedStmt.tableList.size()];
        Table[] outputTables = new Table[m_parsedStmt.tableList.size()];

        // fill the input table with tables from the parsed statement structure
        for (int i = 0; i < inputTables.length; i++)
            inputTables[i] = m_parsedStmt.tableList.get(i);

        // use recursion to solve...
        queueInnerSubJoinOrdersRecursively(inputTables, outputTables, 0);

    }

    /**
     * Recursively add all join orders (permutations) for the input table list.
     *
     * @param inputTables An array of tables to order.
     * @param outputTables A scratch space for recursion for an array of tables. Making this a parameter
     * might make the procedure a slight bit faster than if it was a return value.
     * @param place The index of the table to permute (all tables before index=place are fixed).
     */
    private void queueInnerSubJoinOrdersRecursively(Table[] inputTables, Table[] outputTables, int place) {
        // recursive stopping condition:
        //
        // stop when there is only one place and one table to permute
        if (place == inputTables.length) {
            // The inner join doesn't need a tree at all, only the the flat list of joined table.
            // The join and where conditions are always the same regardless of the table order need to be
            // analyzed only once.
            JoinTree joinNode = new JoinTree();
            joinNode.m_joinOrder = outputTables.clone();
            m_joinOrders.add(joinNode);
            return;
        }

        // recursive step:
        //
        // pick all possible options for the current
        for (int i = 0; i < outputTables.length; i++) {
            // choose a candidate table for this place
            outputTables[place] = inputTables[i];

            // don't select tables that have been chosen before
            boolean duplicate = false;
            for (int j = 0; j < place; j++) {
                if (outputTables[j].getTypeName().equalsIgnoreCase(outputTables[place].getTypeName())) {
                    duplicate = true;
                    break;
                }
            }
            if (duplicate)
                continue;

            // recursively call this function to permute the remaining places
            queueInnerSubJoinOrdersRecursively(inputTables, outputTables, place + 1);
        }
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
    private JoinTree simplifyOuterJoin(JoinTree joinTree) {
        assert(joinTree.m_root != null);
        List<AbstractExpression> exprs = new ArrayList<AbstractExpression>();
        // For the top level node only WHERE expressions need to be evaluated for NULL-rejection
        if (joinTree.m_root.m_leftNode != null && joinTree.m_root.m_leftNode.m_whereExpr != null) {
            exprs.add(joinTree.m_root.m_leftNode.m_whereExpr);
        }
        if (joinTree.m_root.m_rightNode != null && joinTree.m_root.m_rightNode.m_whereExpr != null) {
            exprs.add(joinTree.m_root.m_rightNode.m_whereExpr);
        }
        simplifyOuterJoinRecursively(joinTree.m_root, exprs);
        joinTree.m_hasOuterJoin = joinTree.m_root.hasOuterJoin();
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
        assert(leftNode.m_joinType == JoinType.INNER || rightNode.m_joinType == JoinType.INNER);
        JoinNode innerNode = null;
        if (rightNode.m_joinType == JoinType.LEFT || leftNode.m_joinType == JoinType.RIGHT) {
            innerNode = rightNode;
        } else if (rightNode.m_joinType == JoinType.RIGHT || leftNode.m_joinType == JoinType.LEFT) {
            innerNode = leftNode;
        } else if (!(rightNode.m_joinType == JoinType.INNER && leftNode.m_joinType == JoinType.INNER)) {
            // Full joins are not supported
            assert(false);
        }
        if (innerNode != null) {
            for (AbstractExpression expr : exprs) {
                if (innerNode.m_table != null) {
                    if (ExpressionUtil.isNullRejectingExpression(expr, innerNode.m_table.getTypeName())) {
                        // We are done at this level
                        leftNode.m_joinType = JoinType.INNER;
                        rightNode.m_joinType = JoinType.INNER;
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
                            leftNode.m_joinType = JoinType.INNER;
                            rightNode.m_joinType = JoinType.INNER;
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
            JoinTree joinTree = m_joinOrders.poll();

            // no more join orders => no more plans to generate
            if (joinTree == null)
                return null;

            // Analyze join and filter conditions
            m_parsedStmt.analyzeTreeExpressions(joinTree);

            // generate more plans
            generateMorePlansForJoinOrder(joinTree);
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
    private void generateMorePlansForJoinOrder(JoinTree joinTree) {
        if (m_parsedStmt.joinTree.m_hasOuterJoin == false) {
            generateMorePlansForInnerJoinOrder(joinTree);
        } else {
            generateMorePlansForOuterJoinOrder(joinTree);
        }
    }

    /**
     * Specialization for the outer join.
     *
     * @param joinTree A join tree.
     */
    private void generateMorePlansForOuterJoinOrder(JoinTree joinTree) {
        JoinNode joinNode = joinTree.m_root;
        assert(joinNode != null);

        // generate the access paths for all nodes
        generateAccessPaths(null, joinTree.m_root);

        List<JoinNode> nodes = joinNode.generateJoinOrder();
        generateSubPlanForJoinNodeRecursively(joinNode, nodes);
    }

    /**
     * generate all possible access paths for all nodes in the tree.
     *
     * @param parentNode A parent node to the node to generate paths to.
     * @param childNode A node to generate paths to.
     */
    private void generateAccessPaths(JoinNode parentNode, JoinNode childNode) {
        assert(childNode != null);
        if (childNode.m_leftNode != null) {
            generateAccessPaths(childNode, childNode.m_leftNode);
        }
        if (childNode.m_rightNode != null) {
            generateAccessPaths(childNode, childNode.m_rightNode);
        }
        // The join and filter expressions are kept at the parent node
        // 1- The OUTER-only join conditions - Testing the outer-only conditions COULD be considered as an
        // optimal first step to processing each outer tuple - PreJoin predicate for NLJ or NLIJ
        // 2 -The INNER-only and INNER_OUTER join conditions are used for finding a matching inner tuple(s) for a
        // given outer tuple. Index and end-Index expressions for NLIJ and join predicate for NLJ.
        // 3 -The OUTER-only filter conditions. - Can be pushed down to pre-qualify the outer tuples before they enter
        // the join - Where condition for the left child
        // 4. The INNER-only and INNER_OUTER where conditions are used for filtering joined tuples. -
        // Post join predicate for NLIJ and NLJ
        // Possible optimization - if INNER-only condition is NULL-rejecting (inner_tuple is NOT NULL or
        // inner_tuple > 0) it can be pushed down as a filter expression to the inner child
        if (parentNode != null) {
            if (parentNode.m_leftNode == childNode) {
                // This is the outer table which can have the naive access path and possible index path(s)
                // Optimizations - outer-table-only where expressions can be pushed down to the child node
                // to pre-qualify the outer tuples before they enter the join.
                if (childNode.m_table != null) {
                    childNode.m_accessPaths.addAll(getRelevantAccessPathsForTable(childNode.m_table,
                                                                                  null,
                                                                                  parentNode.m_whereOuterList,
                                                                                  null));
                } else {
                    childNode.m_accessPaths.add(getRelevantNaivePathForTable(null, parentNode.m_whereOuterList));
                }
            } else {
                assert(parentNode.m_rightNode == childNode);
                // This is the inner node
                childNode.m_accessPaths.addAll(getRelevantAccessPathsForInnerNode(parentNode, childNode));
            }
        } else {
            childNode.m_accessPaths.add(getRelevantNaivePathForTable(null, null));
        }
        assert(childNode.m_accessPaths.size() > 0);
    }

    /**
     * Generate all possible access paths for an inner node in an outer join.
     * The set of potential index expressions depends whether the inner node can be inlined
     * with the NLIJ or not. In the former case, inner and inner-outer join expressions can
     * be considered for the index access. In the latter, only inner join expressions qualifies.
     *
     * @param joinNode the join node
     * @param innerNode the inner node
     * @return List of valid access paths
     */
    protected List<AccessPath> getRelevantAccessPathsForInnerNode(JoinNode joinNode, JoinNode innerNode) {
        if (innerNode.m_table == null) {
            // The inner node is a join node itself. Only naive access path is possible
            ArrayList<AccessPath> accessPaths = new ArrayList<AccessPath>();
            accessPaths.add(getRelevantNaivePathForTable(joinNode.m_joinInnerOuterList, joinNode.m_joinInnerList));
            return accessPaths;
        }

        // The inner table can have multiple index access paths based on
        // inner and inner-outer join expressions plus the naive one.

        // If the inner table is partitioned and the outer node is replicated,
        // the join node will be NLJ and not NLIJ, even for an index access path.
        if (joinNode.m_isReplicated || canDeferSendReceivePairForNode()) {
            // This case can support either NLIJ -- assuming joinNode.m_joinInnerOuterList
            // is non-empty AND at least ONE of its clauses can be leveraged in the IndexScan
            // -- or NLJ, otherwise.
            return getRelevantAccessPathsForTable(innerNode.m_table,
                                                  joinNode.m_joinInnerOuterList,
                                                  joinNode.m_joinInnerList,
                                                  null);
        }

        // Only NLJ is supported in this case.
        // If the join is NLJ, the inner node won't be inlined
        // which means that it can't use inner-outer join expressions
        // -- they must be set aside to be processed within the NLJ.
        return getRelevantAccessPathsForTable(innerNode.m_table,
                                              null,
                                              joinNode.m_joinInnerList,
                                              joinNode.m_joinInnerOuterList);
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
                AbstractPlanNode plan = getSelectSubPlanForJoinNode(rootNode);
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
     * Specialization for all inner join.
     *
     * @param joinOrder An array of tables in the join order.
     */
    private void generateMorePlansForInnerJoinOrder(JoinTree joinTree) {
        assert(joinTree.m_joinOrder != null);
        assert(m_plans.size() == 0);

        // compute the reasonable access paths for all tables
        //HashMap<Table, ArrayList<Index[]>> accessPathOptions = generateAccessPathsForEachTable(joinOrder);
        // compute all combinations of access paths for this particular join order
        ArrayList<AccessPath[]> listOfAccessPathCombos = generateAllAccessPathCombinationsForJoinOrder(joinTree.m_joinOrder);

        // for each access path
        for (AccessPath[] accessPath : listOfAccessPathCombos) {
            // get a plan
            AbstractPlanNode scanPlan = getSelectSubPlanForAccessPath(joinTree.m_joinOrder, accessPath);
            m_plans.add(scanPlan);
        }
    }

    /**
     * Given a specific join order and access path set for that join order, construct the plan
     * that gives the right tuples. This method is the meat of sub-plan-graph generation, but all
     * of the smarts are probably done by now, so this is just boring actual construction.
     *
     * @param joinOrder An array of tables in a specific join order.
     * @param accessPath An array of access paths that match with the input tables.
     * @return A completed plan-sub-graph that should match the correct tuples from the
     * correct tables.
     */
    private AbstractPlanNode getSelectSubPlanForAccessPath(Table[] joinOrder, AccessPath[] accessPath) {

        // do the actual work
        AbstractPlanNode retv = getSelectSubPlanForAccessPathsIterative(joinOrder, accessPath);
        // If there is a multi-partition statement on one or more partitioned Tables
        // and the pre-join Send/Receive nodes were suppressed,
        // they need to come into play "post-join".
        if (m_partitioning.getCountOfPartitionedTables() > 1 && m_partitioning.requiresTwoFragments()) {
            retv = addSendReceivePair(retv);
        }
        return retv;
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
        if (joinNode.m_table != null) {
            // End of recursion
            Table joinOrder[] = new Table[1];
            AccessPath accessPath[] = new AccessPath[1];
            joinOrder[0] = joinNode.m_table;
            accessPath[0] = joinNode.m_currentAccessPath;
            return getSelectSubPlanForAccessPathsIterative(joinOrder, accessPath);
        } else {
            assert(joinNode.m_leftNode != null && joinNode.m_rightNode != null);
            // Outer node
            AbstractPlanNode outerScanPlan = getSelectSubPlanForJoinNode(joinNode.m_leftNode);

            // Inner Node
            AbstractPlanNode innerScanPlan = getSelectSubPlanForJoinNode(joinNode.m_rightNode);

            // Join Node
            return getSelectSubPlanForOuterAccessPathStep(joinNode, outerScanPlan, innerScanPlan);
        }
    }


   /**
     * Given a specific join order and access path set for that join order, construct the plan
     * that gives the right tuples. This method is the meat of sub-plan-graph generation, but all
     * of the smarts are probably done by now, so this is just boring actual construction.
     * In case of all participant tables are joined on respective partition keys generation of
     * Send/Received node pair is suppressed.
     *
     * @param joinOrder An array of tables in a specific join order.
     * @param accessPath An array of access paths that match with the input tables.
     * @return A completed plan-sub-graph that should match the correct tuples from the
     * correct tables.
     */
    protected AbstractPlanNode getSelectSubPlanForAccessPathsIterative(Table[] joinOrder, AccessPath[] accessPath) {
        AbstractPlanNode resultPlan = null;
        for (int at = joinOrder.length-1; at >= 0; --at) {
            AbstractPlanNode scanPlan = getAccessPlanForTable(joinOrder[at], accessPath[at]);
            if (resultPlan == null) {
                resultPlan = scanPlan;
            } else {
                /*
                 * The optimizations (nestloop, nestloopindex) that follow don't care
                 * about the send/receive pair. Send in the IndexScanPlanNode or
                 * ScanPlanNode for them to work on.
                 */
                resultPlan = getSelectSubPlanForAccessPathStep(accessPath[at], resultPlan, scanPlan);
            }
            /*
             * If the access plan for the table in the join order was for a
             * distributed table scan there will be a send/receive pair at the top.
             */
            if (joinOrder[at].getIsreplicated() || canDeferSendReceivePairForNode()) {
                continue;
            }
            resultPlan = addSendReceivePair(resultPlan);
        }
        return resultPlan;
    }

    private AbstractPlanNode getSelectSubPlanForAccessPathStep(AccessPath accessPath, AbstractPlanNode subPlan, AbstractPlanNode nljAccessPlan) {
        AbstractJoinPlanNode retval = null;
        if (nljAccessPlan instanceof IndexScanPlanNode) {
            NestLoopIndexPlanNode nlijNode = new NestLoopIndexPlanNode();

            nlijNode.setJoinType(JoinType.INNER);

            @SuppressWarnings("unused")
            IndexScanPlanNode innerNode = (IndexScanPlanNode) nljAccessPlan;

            nlijNode.addInlinePlanNode(nljAccessPlan);

            // combine the tails plan graph with the new head node
            nlijNode.addAndLinkChild(subPlan);
            // now generate the output schema for this join
            nlijNode.generateOutputSchema(m_db);

            retval = nlijNode;
        }
        else {
            // get all the clauses that join the applicable two tables
            ArrayList<AbstractExpression> joinClauses = accessPath.joinExprs;
            NestLoopPlanNode nljNode = new NestLoopPlanNode();
            if ((joinClauses != null) && (joinClauses.size() > 0))
                nljNode.setJoinPredicate(ExpressionUtil.combine(joinClauses));
            nljNode.setJoinType(JoinType.INNER);

            // combine the tails plan graph with the new head node
            nljNode.addAndLinkChild(nljAccessPlan);

            nljNode.addAndLinkChild(subPlan);

            // now generate the output schema for this join
            nljNode.generateOutputSchema(m_db);

            retval = nljNode;
        }
        return retval;
    }

    // @TODO ENG_3038 just for now. Can be merged with the above version for inner joins
    // if the order of inner/outer tables for NLJ can be reversed
    private AbstractPlanNode getSelectSubPlanForOuterAccessPathStep(JoinNode joinNode, AbstractPlanNode outerPlan, AbstractPlanNode innerPlan) {
        // Filter (post-join) expressions
        ArrayList<AbstractExpression> whereClauses  = new ArrayList<AbstractExpression>();
        whereClauses.addAll(joinNode.m_whereInnerList);
        whereClauses.addAll(joinNode.m_whereInnerOuterList);

        AccessPath innerAccessPath = joinNode.m_rightNode.m_currentAccessPath;

        AbstractJoinPlanNode retval = null;
        // In case of innerPlan being an IndexScan the NLIJ will have an advantage
        // over the NLJ/IndexScan only if there is at least one inner-outer join expression
        // that is used for the index access. If this is the case then this expression
        // will be missing from the otherExprs list but is in the original joinNode.m_joinInnerOuterList
        //
        //If not, NLJ/IndexScan is a better choice
        if (innerPlan instanceof IndexScanPlanNode &&
                hasInnerOuterIndexExpression(joinNode.m_joinInnerOuterList, innerAccessPath.otherExprs)) {
            NestLoopIndexPlanNode nlijNode = new NestLoopIndexPlanNode();

            nlijNode.setJoinType(joinNode.m_rightNode.m_joinType);

            @SuppressWarnings("unused")
            IndexScanPlanNode innerNode = (IndexScanPlanNode) innerPlan;

            nlijNode.addInlinePlanNode(innerPlan);

            // combine the tails plan graph with the new head node
            nlijNode.addAndLinkChild(outerPlan);
            // now generate the output schema for this join
            nlijNode.generateOutputSchema(m_db);

            retval = nlijNode;
        }
        else {
            // get all the clauses that join the applicable two tables
            ArrayList<AbstractExpression> joinClauses = innerAccessPath.joinExprs;
            NestLoopPlanNode nljNode = new NestLoopPlanNode();
            if ((joinClauses != null) && ! joinClauses.isEmpty()) {
                nljNode.setJoinPredicate(ExpressionUtil.combine(joinClauses));
            }
            nljNode.setJoinType(joinNode.m_rightNode.m_joinType);

            // combine the tails plan graph with the new head node
            nljNode.addAndLinkChild(outerPlan);

            nljNode.addAndLinkChild(innerPlan);
            // now generate the output schema for this join
            nljNode.generateOutputSchema(m_db);

            retval = nljNode;
        }

        if ((joinNode.m_joinOuterList != null) && ! joinNode.m_joinOuterList.isEmpty()) {
            retval.setPreJoinPredicate(ExpressionUtil.combine(joinNode.m_joinOuterList));
        }

        if ((whereClauses != null) && ! whereClauses.isEmpty()) {
            retval.setWherePredicate(ExpressionUtil.combine(whereClauses));
        }
        return retval;
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
        HashSet<AbstractExpression> otherSet = new HashSet<AbstractExpression>();
        otherSet.addAll(nonIndexInnerOuterList);
        return !otherSet.containsAll(originalInnerOuterExprs);
    }

    /**
     * For each table in the list, compute the set of all valid access paths that will get
     * tuples that match the right predicate (assuming there is a predicate).
     *
     * @param tables The array of tables we are computing paths for.
     * @return A map that contains a list of access paths for each table in the input array.
     * An access path is an array of indexes (possibly empty).
     */
    private HashMap<Table, ArrayList<AccessPath>> generateAccessPathsForEachTable(Table[] tables) {
        // this means just use full scans for all access paths (for now).
        // an access path is a list of indexes (possibly empty)
        HashMap<Table, ArrayList<AccessPath>> retval = new HashMap<Table, ArrayList<AccessPath>>();

        // for each table, just add the empty access path (the full table scan)
        for (int i = 0; i < tables.length; i++) {
            Table currentTable = tables[i];
            Table nextTables[] = new Table[tables.length - (i + 1)];
            System.arraycopy(tables, i + 1, nextTables, 0, tables.length - (i + 1));
            ArrayList<AccessPath> paths = getRelevantAccessPathsForTable(currentTable, nextTables);
            retval.put(tables[i], paths);
        }

        return retval;
    }

    /**
     * Given a join order, compute a list of all combinations of access paths. This will return a list
     * of sets of specific ways to access each table in a join order. It is called recursively.
     *
     * @param joinOrder The list of tables in this sub-select in a particular order.
     * @return A list of lists of lists (ugh). For a given table, an access path is a list of indexes
     * which might be empty. Given a join order, a complete access path for that join order is an
     * array (one slot per table) of access paths. The list of all possible complete access paths is
     * returned.
     */
    private ArrayList<AccessPath[]> generateAllAccessPathCombinationsForJoinOrder(Table[] joinOrder){

        HashMap<Table, ArrayList<AccessPath>> accessPathOptions = generateAccessPathsForEachTable(joinOrder);

        // An access path for a table is a an Index[]
        // A complete access path for a join order is an Index[][]
        // All possible complete access paths is an ArrayList<Index[][]>
        ArrayList<AccessPath[]> retval = new ArrayList<AccessPath[]>();

        // recursive stopping condition:
        //
        // if this is a single-table select, then this will be pretty easy
        if (joinOrder.length == 1) {
            // walk through all the access paths for this single table and put them
            // in the list of all possible access paths
            for (AccessPath path : accessPathOptions.get(joinOrder[0])) {
                AccessPath[] paths = new AccessPath[1];
                paths[0] = path;
                retval.add(paths);
            }
            return retval;
        }

        // recursive step:
        //
        // if we get here, assume join order is multi-table

        // make a copy of the tail (list - head) of the join order array
        Table[] subJoinOrder = Arrays.copyOfRange(joinOrder, 1, joinOrder.length);

        // recursively get all possible access path combinations for the tail of the join order
        ArrayList<AccessPath[]> subList = generateAllAccessPathCombinationsForJoinOrder(subJoinOrder);

        // get all possible access paths for the head, and glue them onto the options for the tail
        for (AccessPath path : accessPathOptions.get(joinOrder[0])) {
            // take the selected path for the head and cross-product with all tail options
            for (AccessPath[] choice : subList) {
                AccessPath[] paths = new AccessPath[joinOrder.length];
                paths[0] = path;
                assert(choice.length == subJoinOrder.length);
                for (int i = 0; i < choice.length; i++)
                    paths[i + 1] = choice[i];
                retval.add(paths);
            }
        }

        return retval;
    }

}
