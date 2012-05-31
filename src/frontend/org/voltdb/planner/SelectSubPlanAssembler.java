/* This file is part of VoltDB.
 * Copyright (C) 2008-2012 VoltDB Inc.
 *
 * VoltDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * VoltDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.voltdb.planner;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;

import org.voltdb.catalog.Database;
import org.voltdb.catalog.Table;
import org.voltdb.expressions.AbstractExpression;
import org.voltdb.expressions.ExpressionUtil;
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
    ArrayDeque<Table[]> m_joinOrders = new ArrayDeque<Table[]>();

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
                    sb.append("Self-joins are not supported yet.");
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
            m_joinOrders.add(tables);
        } else {
            queueAllJoinOrders();
        }
    }

    /**
     * Compute every permutation of the list of involved tables and put them in a deque.
     */
    private void queueAllJoinOrders() {
        // these just shouldn't happen right?
        assert(m_parsedStmt.multiTableSelectionList.size() == 0);
        assert(m_parsedStmt.noTableSelectionList.size() == 0);

        // create arrays of the tables to permute them
        Table[] inputTables = new Table[m_parsedStmt.tableList.size()];
        Table[] outputTables = new Table[m_parsedStmt.tableList.size()];

        // fill the input table with tables from the parsed statement structure
        for (int i = 0; i < inputTables.length; i++)
            inputTables[i] = m_parsedStmt.tableList.get(i);

        // use recursion to solve...
        queueSubJoinOrders(inputTables, outputTables, 0);
    }

    /**
     * Recursively add all join orders (permutations) for the input table list.
     *
     * @param inputTables An array of tables to order.
     * @param outputTables A scratch space for recursion for an array of tables. Making this a parameter
     * might make the procedure a slight bit faster than if it was a return value.
     * @param place The index of the table to permute (all tables before index=place are fixed).
     */
    private void queueSubJoinOrders(Table[] inputTables, Table[] outputTables, int place) {

        // recursive stopping condition:
        //
        // stop when there is only one place and one table to permute
        if (place == inputTables.length) {
            m_joinOrders.add(outputTables.clone());
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
            queueSubJoinOrders(inputTables, outputTables, place + 1);
        }
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
            Table[] joinOrder = m_joinOrders.poll();

            // no more join orders => no more plans to generate
            if (joinOrder == null)
                return null;

            // generate more plans
            generateMorePlansForJoinOrder(joinOrder);
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
    private void generateMorePlansForJoinOrder(Table[] joinOrder) {
        assert(joinOrder != null);
        assert(m_plans.size() == 0);

        // compute the reasonable access paths for all tables
        //HashMap<Table, ArrayList<Index[]>> accessPathOptions = generateAccessPathsForEachTable(joinOrder);
        // compute all combinations of access paths for this particular join order
        ArrayList<AccessPath[]> listOfAccessPathCombos = generateAllAccessPathCombinationsForJoinOrder(joinOrder);

        // Identify whether all the partitioned tables in the join are filtered/joined on a common partition key
        // Currently, if a partition key was provided to the planner,
        // a single-partition scan is required and all partitioned tables accessed are
        // assumed to follow this partition-key-equality pattern -- the query will only operate on the rows within a single partition,
        // so caveat emptor if the query designating a single-partition does not fit the pattern.
        // If no partition key was provided to the planner, multi-partition execution is initially assumed.
        // That leaves several interesting cases.
        // 1) In the best case, analysis of the plan can detect the partition-key-equality-join pattern and also detect a
        // constant equality filter on any of the equivalent keys. In this case, the constant can be field-promoted
        // to a partitioning value and the statement run single-partition on whichever partition the value hashes to.
        // 2) In the next best case, a partition-key-equality pattern is detected but a constant equality filter does
        // not exist or can not be resolved to a (hashable) constant value. The result is a two-fragment query where
        // the top fragment coordinates the results of the distributed partition-based scan/joins in the bottom fragment.
        // 3) In the worst case, there is no partition-key-equality -- or an incomplete one that does not cover all partitioned tables.
        // Such a query would require a 3-fragment plan, with a top fragment joining the results of scans received from two lower fragments
        // -- 3-fragment plans are not supported, so such plans can be rejected at this stage with no loss of functionality.
        // 4) There is an additional edge case that would theoretically not have to exceed the 2-fragment limit.
        // This would involve an arbitrary join with two partitioned tables (and, as implied in the above cases,
        // any number of replicated tables) with no constraints on the join criteria BUT a requirement that the last
        // partitioned table to be scanned (the first as listed in the reversed "joinOrder/accessPath" vector) have a
        // constant equality filter as in case 1 that can be used as a partition key. In that case, the partition designated
        // by that key could be selected as the coordinator to execute a top fragment join of the results of the bottom
        // fragment's multi-partition scan.  The problem with this case is that (AFAIK --paul) there is no way to
        // separately indicate to the initiator that a plan (fragment) must be run on a particular partition (like an SP query)
        // but that the query has also has a distributed lower fragment (like an MP query).
        // Supporting cases 1 and 2 but rejecting cases 3 and 4 simplifies the required analysis and supports
        // inferred SP ad hocs based on a constant equality filter and a partition-key-equality pattern (case 1)
        // in the context of and without regressing the fix for ENG-496 (case 2) which was the original motivation for
        // AccessPath.isPartitionKeyEquality.

        boolean suppressSendReceivePair = true;
        int partitionedTableCount = m_partitioning.getCountOfPartitionedTables();
        if ((m_partitioning.wasSpecifiedAsSingle() == false) &&  partitionedTableCount > 0) {
            // It's possible that a leftover inferred value from the previous join order may not hold for the current one?
            m_partitioning.setEffectiveValue(null);
            // It's usually better to send and receive pre-join tuples than post-join tuples.
            suppressSendReceivePair = false; // tentative/default value.
            // This analysis operates independently of indexes, so only needs to operate on the naive (first) accessPath.
            AccessPath.tagForMultiPartitionAccess(joinOrder, listOfAccessPathCombos.get(0), m_partitioning);
            int multiPartitionScanCount = m_partitioning.getCountOfIndependentlyPartitionedTables();
            if (multiPartitionScanCount > 1) {
                // The case of more than one independent partitioned table would result in an illegal plan with more than two fragments.
                return;
            }
            if (m_partitioning.effectivePartitioningValue() == null) {
                // For (multiPartitionScanCount == 1), "case 2" where the last-listed (first-scanned) partitioned table
                // has no constant filter, it accounts for the 1 independent partitioned scan.
                // In this case, whether to suppress the usual Receive/Send nodes below the joins depends on whether
                // there are other partitioned tables being scanned but not counted because of partition key equality.
                // If not, (for joins solely against replicated tables) it's probably better to try to inject the Receive/Send nodes
                // below the join.
                // If so, the Receive/Send nodes must be suppressed until after the join.

                // For (multiPartitionScanCount == 0), the failure to produce a partitionKey
                // when that would involve expression evaluation has forced a degenerate case
                // that case can be handled the same way.
                if (partitionedTableCount > 1) {
                    suppressSendReceivePair = true;
                }
            } else {
                if (multiPartitionScanCount == 1) {
                    // "case 4" IF the last-listed (first-scanned) partitioned table has a constant filter,
                    // then some other independent partitioned scan is getting counted as the 1.
                    // We WISH we could support this case, but we don't.
                    // When we do, we'll have to figure out how to deal with the possible existence of both
                    // localized and non-localized joins.
                    return;
                } else {
                    suppressSendReceivePair = true; // No Send/Receive needed in a single-partition statement.
                }
            }
            // Anything else can be handled in one or two plan fragments, injecting Receive/Send nodes below any joins.
        }
        // for each access path
        for (AccessPath[] accessPath : listOfAccessPathCombos) {
            // get a plan
            AbstractPlanNode scanPlan = getSelectSubPlanForAccessPath(joinOrder, accessPath, suppressSendReceivePair);
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
     * @param suppressSendReceivePair A flag preventing the usual injection of Receive and Send nodes above scans of non-replicated tables.
     * @return A completed plan-sub-graph that should match the correct tuples from the
     * correct tables.
     */
    private AbstractPlanNode getSelectSubPlanForAccessPath(Table[] joinOrder, AccessPath[] accessPath, boolean suppressSendReceivePair) {

        // do the actual work
        AbstractPlanNode retv = getSelectSubPlanForAccessPathsIterative(joinOrder, accessPath, suppressSendReceivePair);
        // If there is a multi-partition statement on one or more partitioned Tables and the Send/Receive nodes were suppressed,
        // they need to come into play "post-join".
        if (suppressSendReceivePair &&
                (m_partitioning.getCountOfPartitionedTables() > 0) &&
                m_partitioning.effectivePartitioningValue() == null) {
            retv = addSendReceivePair(retv);
        }
        return retv;
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
     * @param supressSendReceivePair indicator whether to suppress intermediate Send/Receive pairs or not
     * @return A completed plan-sub-graph that should match the correct tuples from the
     * correct tables.
     */
    protected AbstractPlanNode getSelectSubPlanForAccessPathsIterative(Table[] joinOrder, AccessPath[] accessPath,
            boolean suppressSendReceivePair) {
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
            if (suppressSendReceivePair || joinOrder[at].getIsreplicated()) {
                continue;
            }
            resultPlan = addSendReceivePair(resultPlan);
        }
        return resultPlan;
    }

    private AbstractPlanNode getSelectSubPlanForAccessPathStep(AccessPath accessPath, AbstractPlanNode subPlan, AbstractPlanNode nljAccessPlan) {

        // get all the clauses that join the applicable two tables
        ArrayList<AbstractExpression> joinClauses = accessPath.joinExprs;

        AbstractPlanNode retval = null;
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
            NestLoopPlanNode nljNode = new NestLoopPlanNode();
            if ((joinClauses != null) && (joinClauses.size() > 0))
                nljNode.setPredicate(ExpressionUtil.combine(joinClauses));
            nljNode.setJoinType(JoinType.LEFT);

            // combine the tails plan graph with the new head node
            nljNode.addAndLinkChild(nljAccessPlan);

            nljNode.addAndLinkChild(subPlan);
            // now generate the output schema for this join
            nljNode.generateOutputSchema(m_db);

            retval = nljNode;
        }

        return retval;
    }

    private AbstractPlanNode getAccessPlanForTable(Table table, AccessPath accessPath, boolean suppressSendReceivePair) {
        AbstractPlanNode result = getAccessPlanForTable(table, accessPath);
        if (suppressSendReceivePair || table.getIsreplicated())
            return result;
        return addSendReceivePair(result);
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
