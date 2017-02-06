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

package org.voltdb.planner;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableSet;
import java.util.Set;

import org.json_voltpatches.JSONException;
import org.voltdb.VoltType;
import org.voltdb.catalog.CatalogMap;
import org.voltdb.catalog.Cluster;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.ColumnRef;
import org.voltdb.catalog.Constraint;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Index;
import org.voltdb.catalog.Table;
import org.voltdb.expressions.AbstractExpression;
import org.voltdb.expressions.AggregateExpression;
import org.voltdb.expressions.ConstantValueExpression;
import org.voltdb.expressions.ExpressionUtil;
import org.voltdb.expressions.OperatorExpression;
import org.voltdb.expressions.ParameterValueExpression;
import org.voltdb.expressions.SelectSubqueryExpression;
import org.voltdb.expressions.TupleAddressExpression;
import org.voltdb.expressions.TupleValueExpression;
import org.voltdb.expressions.WindowFunctionExpression;
import org.voltdb.planner.microoptimizations.MicroOptimizationRunner;
import org.voltdb.planner.parseinfo.BranchNode;
import org.voltdb.planner.parseinfo.JoinNode;
import org.voltdb.planner.parseinfo.StmtSubqueryScan;
import org.voltdb.planner.parseinfo.StmtTableScan;
import org.voltdb.plannodes.AbstractJoinPlanNode;
import org.voltdb.plannodes.AbstractPlanNode;
import org.voltdb.plannodes.AbstractReceivePlanNode;
import org.voltdb.plannodes.AbstractScanPlanNode;
import org.voltdb.plannodes.AggregatePlanNode;
import org.voltdb.plannodes.DeletePlanNode;
import org.voltdb.plannodes.HashAggregatePlanNode;
import org.voltdb.plannodes.IndexScanPlanNode;
import org.voltdb.plannodes.InsertPlanNode;
import org.voltdb.plannodes.LimitPlanNode;
import org.voltdb.plannodes.MaterializePlanNode;
import org.voltdb.plannodes.MergeReceivePlanNode;
import org.voltdb.plannodes.NestLoopPlanNode;
import org.voltdb.plannodes.NodeSchema;
import org.voltdb.plannodes.OrderByPlanNode;
import org.voltdb.plannodes.PartialAggregatePlanNode;
import org.voltdb.plannodes.WindowFunctionPlanNode;
import org.voltdb.plannodes.ProjectionPlanNode;
import org.voltdb.plannodes.ReceivePlanNode;
import org.voltdb.plannodes.SchemaColumn;
import org.voltdb.plannodes.SendPlanNode;
import org.voltdb.plannodes.SeqScanPlanNode;
import org.voltdb.plannodes.UnionPlanNode;
import org.voltdb.plannodes.UpdatePlanNode;
import org.voltdb.types.ConstraintType;
import org.voltdb.types.ExpressionType;
import org.voltdb.types.IndexType;
import org.voltdb.types.JoinType;
import org.voltdb.types.PlanNodeType;
import org.voltdb.types.SortDirectionType;
import org.voltdb.utils.CatalogUtil;

/**
 * The query planner accepts catalog data, SQL statements from the catalog, then
 * outputs a set of complete and correct query plans. It will output MANY plans
 * and some of them will be stupid. The best plan will be selected by computing
 * resource usage statistics for the plans, then using those statistics to
 * compute the cost of a specific plan. The plan with the lowest cost wins.
 *
 */
public class PlanAssembler {
    // The convenience struct to accumulate results after parsing multiple statements
    private static class ParsedResultAccumulator {
        public final boolean m_orderIsDeterministic;
        public final boolean m_hasLimitOrOffset;
        public final String m_isContentDeterministic;

        public ParsedResultAccumulator(boolean orderIsDeterministic,
                                       boolean hasLimitOrOffset,
                                       String isContentDeterministic)
        {
            m_orderIsDeterministic = orderIsDeterministic;
            m_hasLimitOrOffset  = hasLimitOrOffset;
            m_isContentDeterministic = isContentDeterministic;
        }
    }

    /** convenience pointer to the cluster object in the catalog */
    private final Cluster m_catalogCluster;
    /** convenience pointer to the database object in the catalog */
    private final Database m_catalogDb;

    /** parsed statement for an insert */
    private ParsedInsertStmt m_parsedInsert = null;
    /** parsed statement for an update */
    private ParsedUpdateStmt m_parsedUpdate = null;
    /** parsed statement for an delete */
    private ParsedDeleteStmt m_parsedDelete = null;
    /** parsed statement for an select */
    private ParsedSelectStmt m_parsedSelect = null;
    /** parsed statement for an union */
    private ParsedUnionStmt m_parsedUnion = null;

    /** plan selector */
    private final PlanSelector m_planSelector;

    /** Describes the specified and inferred partition context. */
    private StatementPartitioning m_partitioning;

    /** Error message */
    private String m_recentErrorMsg;

    /**
     * Used to generate the table-touching parts of a plan. All join-order and
     * access path selection stuff is done by the SelectSubPlanAssember.
     */
    private SubPlanAssembler m_subAssembler = null;

    /**
     * Flag when the only expected plan for a statement has already been generated.
     */
    private boolean m_bestAndOnlyPlanWasGenerated = false;

    /**
     *
     * @param catalogCluster
     *            Catalog info about the physical layout of the cluster.
     * @param catalogDb
     *            Catalog info about schema, metadata and procedures.
     * @param partitioning
     *            Describes the specified and inferred partition context.
     */
    PlanAssembler(Cluster catalogCluster, Database catalogDb, StatementPartitioning partitioning, PlanSelector planSelector) {
        m_catalogCluster = catalogCluster;
        m_catalogDb = catalogDb;
        m_partitioning = partitioning;
        m_planSelector = planSelector;
    }

    String getSQLText() {
        if (m_parsedDelete != null) {
            return m_parsedDelete.m_sql;
        }

        if (m_parsedInsert != null) {
            return m_parsedInsert.m_sql;
        }

        if (m_parsedUpdate != null) {
            return m_parsedUpdate.m_sql;
        }

        if (m_parsedSelect != null) {
            return m_parsedSelect.m_sql;
        }

        assert(false);
        return null;
    }

    /**
     * Return true if tableList includes at least one matview.
     */
    private boolean tableListIncludesReadOnlyView(List<Table> tableList) {
        NavigableSet<String> exportTables = CatalogUtil.getExportTableNames(m_catalogDb);
        for (Table table : tableList) {
            if (table.getMaterializer() != null && !exportTables.contains(table.getMaterializer().getTypeName())) {
                return true;
            }
        }
        return false;
    }

    /**
     * Return true if tableList includes at least one export table.
     */
    private boolean tableListIncludesExportOnly(List<Table> tableList) {
        // list of all export tables (assume uppercase)
        NavigableSet<String> exportTables = CatalogUtil.getExportTableNames(m_catalogDb);

        // this loop is O(number-of-joins * number-of-export-tables)
        // which seems acceptable if not great. Probably faster than
        // re-hashing the export only tables for faster lookup.
        for (Table table : tableList) {
            if (exportTables.contains(table.getTypeName())) {
                return true;
            }
        }

        return false;
    }

    private boolean isPartitionColumnInGroupbyList(ArrayList<ParsedColInfo> groupbyColumns) {
        assert(m_parsedSelect != null);

        if (groupbyColumns == null) {
            return false;
        }

        for (ParsedColInfo groupbyCol : groupbyColumns) {
            StmtTableScan scanTable = m_parsedSelect.getStmtTableScanByAlias(groupbyCol.tableAlias);
            // table alias may be from AbstractParsedStmt.TEMP_TABLE_NAME.
            if (scanTable != null && scanTable.getPartitioningColumns() != null) {
                for (SchemaColumn pcol : scanTable.getPartitioningColumns()) {
                    if  (pcol != null && pcol.getColumnName().equals(groupbyCol.columnName) ) {
                        return true;
                    }
                }
            }
        }

        return false;
    }

    private boolean canPushDownDistinctAggregation(AggregateExpression aggExpr) {
        assert(m_parsedSelect != null);
        assert(aggExpr != null);
        assert(aggExpr.isDistinct());

        if ( aggExpr.getExpressionType() == ExpressionType.AGGREGATE_COUNT_STAR ) {
            return true;
        }

        AbstractExpression aggArg = aggExpr.getLeft();
        // constant
        if (aggArg instanceof ConstantValueExpression ||
                aggArg instanceof ParameterValueExpression) {
            return true;
        }

        if ( ! (aggArg instanceof TupleValueExpression)) {
            return false;
        }

        TupleValueExpression tve = (TupleValueExpression) aggArg;
        String tableAlias = tve.getTableAlias();
        StmtTableScan scanTable = m_parsedSelect.getStmtTableScanByAlias(tableAlias);
        // table alias may be from AbstractParsedStmt.TEMP_TABLE_NAME.
        if (scanTable == null || scanTable.getPartitioningColumns() == null) {
            return false;
        }

        for (SchemaColumn pcol : scanTable.getPartitioningColumns()) {
            if (pcol != null &&
                    pcol.getColumnName().equals(tve.getColumnName()) ) {
                return true;
            }
        }

        return false;
    }

    /**
     * Clear any old state and get ready to plan a new plan. The next call to
     * getNextPlan() will return the first candidate plan for these parameters.
     *
     */
    private void setupForNewPlans(AbstractParsedStmt parsedStmt) {
        m_bestAndOnlyPlanWasGenerated = false;
        m_partitioning.analyzeTablePartitioning(parsedStmt.allScans());

        if (parsedStmt instanceof ParsedUnionStmt) {
            m_parsedUnion = (ParsedUnionStmt) parsedStmt;
            return;
        }

        if (parsedStmt instanceof ParsedSelectStmt) {
            if (tableListIncludesExportOnly(parsedStmt.m_tableList)) {
                throw new PlanningErrorException(
                        "Illegal to read a stream.");
            }

            m_parsedSelect = (ParsedSelectStmt) parsedStmt;
            // Simplify the outer join if possible
            if (m_parsedSelect.m_joinTree instanceof BranchNode) {
                if (! m_parsedSelect.hasJoinOrder()) {
                    simplifyOuterJoin((BranchNode)m_parsedSelect.m_joinTree);
                }

                // Convert RIGHT joins to the LEFT ones
                ((BranchNode)m_parsedSelect.m_joinTree).toLeftJoin();
            }
            m_subAssembler = new SelectSubPlanAssembler(m_catalogDb, m_parsedSelect, m_partitioning);

            // Process the GROUP BY information, decide whether it is group by the partition column
            if (isPartitionColumnInGroupbyList(m_parsedSelect.m_groupByColumns)) {
                m_parsedSelect.setHasPartitionColumnInGroupby();
            }
            if (isPartitionColumnInWindowedAggregatePartitionByList()) {
                m_parsedSelect.setHasPartitionColumnInWindowedAggregate();
            }

            // FIXME: is the following scheme/comment obsolete?
            // FIXME: turn it on when we are able to push down DISTINCT
//            if (isPartitionColumnInGroupbyList(m_parsedSelect.m_distinctGroupByColumns)) {
//                m_parsedSelect.setHasPartitionColumnInDistinctGroupby();
//            }

            return;
        }

        // @TODO
        // Need to use StmtTableScan instead
        // check that no modification happens to views
        if (tableListIncludesReadOnlyView(parsedStmt.m_tableList)) {
            throw new PlanningErrorException("Illegal to modify a materialized view.");
        }

        m_partitioning.setIsDML();

        // Check that only multi-partition writes are made to replicated tables.
        // figure out which table we're updating/deleting
        assert (parsedStmt.m_tableList.size() == 1);
        Table targetTable = parsedStmt.m_tableList.get(0);
        if (targetTable.getIsreplicated()) {
            if (m_partitioning.wasSpecifiedAsSingle()
                    && !m_partitioning.isReplicatedDmlToRunOnAllPartitions()) {
                String msg = "Trying to write to replicated table '" + targetTable.getTypeName()
                        + "' in a single-partition procedure.";
                throw new PlanningErrorException(msg);
            }
        }
        else if (m_partitioning.wasSpecifiedAsSingle() == false) {
            m_partitioning.setPartitioningColumnForDML(targetTable.getPartitioncolumn());
        }

        if (parsedStmt instanceof ParsedInsertStmt) {
            m_parsedInsert = (ParsedInsertStmt) parsedStmt;
            // The currently handled inserts are too simple to even require a subplan assembler. So, done.
            return;
        }

        if (parsedStmt instanceof ParsedUpdateStmt) {
            if (tableListIncludesExportOnly(parsedStmt.m_tableList)) {
                throw new PlanningErrorException("Illegal to update a stream.");
            }
            m_parsedUpdate = (ParsedUpdateStmt) parsedStmt;
        }
        else if (parsedStmt instanceof ParsedDeleteStmt) {
            if (tableListIncludesExportOnly(parsedStmt.m_tableList)) {
                throw new PlanningErrorException("Illegal to delete from a stream.");
            }
            m_parsedDelete = (ParsedDeleteStmt) parsedStmt;
        }
        else {
            throw new RuntimeException("Unknown subclass of AbstractParsedStmt.");
        }

        if ( ! m_partitioning.wasSpecifiedAsSingle()) {
            //TODO: When updates and deletes can contain joins, this step may have to be
            // deferred so that the valueEquivalence set can be analyzed per join order.
            // This appears to be an unfortunate side effect of how the HSQL interface
            // misleadingly organizes the placement of join/where filters on the statement tree.
            // This throws off the accounting of equivalence join filters until they can be
            // normalized in analyzeJoinFilters, but that normalization process happens on a
            // per-join-order basis, and so, so must this analysis.
            HashMap<AbstractExpression, Set<AbstractExpression>>
                valueEquivalence = parsedStmt.analyzeValueEquivalence();
            Collection<StmtTableScan> scans = parsedStmt.allScans();
            m_partitioning.analyzeForMultiPartitionAccess(scans, valueEquivalence);
        }
        m_subAssembler = new WriterSubPlanAssembler(m_catalogDb, parsedStmt, m_partitioning);
    }

    private boolean isPartitionColumnInWindowedAggregatePartitionByList() {
        assert (m_parsedSelect != null);
        return (m_parsedSelect.isPartitionColumnInWindowedAggregatePartitionByList());
    }

    private static void failIfNonDeterministicDml(AbstractParsedStmt parsedStmt, CompiledPlan plan) {
        // If we have content non-determinism on DML, then fail planning.
        // This can happen if:
        //   INSERT INTO ... SELECT ... where the select statement has a limit on unordered data.
        //   UPSERT INTO ... SELECT has the same issue, but no limit is required because
        //                      order may determine which rows are updated and which are inserted
        //   DELETE ... ORDER BY <n> LIMIT <n> also has this issue
        // Update doesn't have this issue yet (but having ORDER BY and LIMIT there doesn't seem out
        // of the question).
        // When subqueries in WHERE clauses of DML are allowed, we will need to make sure the
        // subqueries are content-deterministic too.

        if (plan == null || plan.isReadOnly()) {
            return;
        }

        boolean contentDeterministic = plan.isContentDeterministic();
        if (parsedStmt instanceof ParsedInsertStmt && !(plan.isOrderDeterministic() && contentDeterministic)) {
            ParsedInsertStmt parsedInsert = (ParsedInsertStmt)parsedStmt;
            boolean targetHasLimitRowsTrigger = parsedInsert.targetTableHasLimitRowsTrigger();
            String contentDeterministicMsg = "";
            if (!contentDeterministic) {
                contentDeterministicMsg = "  " + plan.nondeterminismDetail();
            }

            if (parsedStmt.m_isUpsert) {
                throw new PlanningErrorException(
                        "UPSERT statement manipulates data in a non-deterministic way.  "
                        + "Adding an ORDER BY clause to UPSERT INTO ... SELECT may address this issue."
                        + contentDeterministicMsg);
            }

            if (targetHasLimitRowsTrigger) {
                throw new PlanningErrorException(
                        "Order of rows produced by SELECT statement in INSERT INTO ... SELECT is "
                        + "non-deterministic.  Since the table being inserted into has a row limit "
                        + "trigger, the SELECT output must be ordered.  Add an ORDER BY clause "
                        + "to address this issue."
                        + contentDeterministicMsg
                        );
            }

            if (plan.hasLimitOrOffset()) {
                throw new PlanningErrorException(
                        "INSERT statement manipulates data in a content non-deterministic way.  "
                        + "Adding an ORDER BY clause to INSERT INTO ... SELECT may address this issue."
                        + contentDeterministicMsg);
            }

            if (!contentDeterministic) {
                throw new PlanningErrorException("INSERT statement manipulates data in a non-deterministic way."
                                                 + contentDeterministicMsg);
            }
        }

        if (parsedStmt instanceof ParsedDeleteStmt
                && !((ParsedDeleteStmt)parsedStmt).sideEffectsAreDeterministic()) {
                throw new PlanningErrorException(
                        "DELETE statement manipulates data in a non-deterministic way.  This may happen "
                                + "when the DELETE has an ORDER BY clause with a LIMIT, but the order is not "
                                + "well-defined.");
        }
    }

    /**
     * Generate the best cost plan for the current SQL statement context.
     *
     * @param parsedStmt Current SQL statement to generate plan for
     * @return The best cost plan or null.
     */
    static String IN_EXISTS_SCALAR_ERROR_MESSAGE = "Subquery expressions are only supported for "
            + "single partition procedures and AdHoc queries referencing only replicated tables.";

    CompiledPlan getBestCostPlan(AbstractParsedStmt parsedStmt) {
        // parse any subqueries that the statement contains
        List<StmtSubqueryScan> subqueryNodes = parsedStmt.getSubqueryScans();
        ParsedResultAccumulator fromSubqueryResult = null;
        if (! subqueryNodes.isEmpty()) {
            fromSubqueryResult = getBestCostPlanForFromSubQueries(subqueryNodes);
            if (fromSubqueryResult == null) {
                // There was at least one sub-query and we should have a compiled plan for it
                return null;
            }
        }

        // Get the best plans for the expression subqueries ( IN/EXISTS (SELECT...) )
        Set<AbstractExpression> subqueryExprs = parsedStmt.findSubquerySubexpressions();
        if ( ! subqueryExprs.isEmpty() ) {

            // guards against IN/EXISTS/Scalar subqueries
            if ( ! m_partitioning.wasSpecifiedAsSingle() ) {
                // Don't allow partitioned tables in subqueries.
                // This restriction stems from the lack of confidence that the
                // planner can reliably identify all cases of adequate and
                // inadequate partition key join criteria across different
                // levels of correlated subqueries.
                for (AbstractExpression e : subqueryExprs) {
                    assert(e instanceof SelectSubqueryExpression);
                    SelectSubqueryExpression subExpr = (SelectSubqueryExpression)e;
                    if (! subExpr.getSubqueryScan().getIsReplicated()) {
                        m_recentErrorMsg = IN_EXISTS_SCALAR_ERROR_MESSAGE;
                        return null;
                    }
                }
            }

            if (!getBestCostPlanForExpressionSubQueries(subqueryExprs)) {
                // There was at least one sub-query and we should have a compiled plan for it
                return null;
            }
        }

        // set up the plan assembler for this statement
        setupForNewPlans(parsedStmt);

        // get ready to find the plan with minimal cost
        CompiledPlan rawplan = null;

        // loop over all possible plans
        while (true) {
            rawplan = getNextPlan();

            // stop this while loop when no more plans are generated
            if (rawplan == null) {
                break;
            }

            // Update the best cost plan so far
            m_planSelector.considerCandidatePlan(rawplan, parsedStmt);
        }

        CompiledPlan retval = m_planSelector.m_bestPlan;
        if (retval == null) {
            return null;
        }

        if (fromSubqueryResult != null) {
            // Calculate the combined state of determinism for the parent and child statements
            boolean orderIsDeterministic = retval.isOrderDeterministic();
            String contentDeterminismDetail = fromSubqueryResult.m_isContentDeterministic;
            if (orderIsDeterministic && ! fromSubqueryResult.m_orderIsDeterministic) {
                //TODO: this reliance on the vague isOrderDeterministicInSpiteOfUnorderedSubqueries test
                // is subject to false negatives for determinism. It misses the subtlety of parent
                // queries that surgically add orderings for specific "key" columns of a subquery result
                // or a subquery-based join for an effectively deterministic result.
                // The first step towards repairing this would involve detecting deterministic and
                // non-deterministic subquery results IN CONTEXT where they are scanned in the parent
                // query, so that the parent query can ensure that ALL the columns from a
                // non-deterministic subquery are later sorted.
                // The next step would be to extend the model for "subquery scans"
                // to identify dependencies / uniqueness constraints in subquery results
                // that can be exploited to impose determinism with fewer parent order by columns
                // -- like just the keys.
                orderIsDeterministic = parsedStmt.isOrderDeterministicInSpiteOfUnorderedSubqueries();
            }
            boolean hasLimitOrOffset =
                    fromSubqueryResult.m_hasLimitOrOffset || retval.hasLimitOrOffset();
            retval.statementGuaranteesDeterminism(hasLimitOrOffset, orderIsDeterministic, contentDeterminismDetail);
            // Need to re-attach the sub-queries plans to the best parent plan. The same best plan for each
            // sub-query is reused with all parent candidate plans and needs to be reconnected with
            // the final best parent plan
            retval.rootPlanGraph = connectChildrenBestPlans(retval.rootPlanGraph);
        }

        /*
         * Find out if the query is inherently content deterministic and
         * remember it.
         */
        String contentDeterminismMessage = parsedStmt.getContentDeterminismMessage();
        if (contentDeterminismMessage != null) {
            retval.setNondeterminismDetail(contentDeterminismMessage);
        }
        failIfNonDeterministicDml(parsedStmt, retval);

        if (m_partitioning != null) {
            retval.setStatementPartitioning(m_partitioning);
        }

        return retval;
    }

    /**
     * Output the best cost plan.
     *
     */
    void finalizeBestCostPlan() {
        m_planSelector.finalizeOutput();
    }

    /**
     * Generate best cost plans for a list of FROM sub-queries.
     * @param subqueryNodes - list of FROM sub-queries.
     * @return ParsedResultAccumulator
     */
    private ParsedResultAccumulator getBestCostPlanForFromSubQueries(List<StmtSubqueryScan> subqueryNodes) {
        int nextPlanId = m_planSelector.m_planId;
        boolean orderIsDeterministic = true;
        boolean hasSignificantOffsetOrLimit = false;
        String isContentDeterministic = null;
        for (StmtSubqueryScan subqueryScan : subqueryNodes) {
            nextPlanId = planForParsedSubquery(subqueryScan, nextPlanId);
            CompiledPlan subqueryBestPlan = subqueryScan.getBestCostPlan();
            if (subqueryBestPlan == null) {
                throw new PlanningErrorException(m_recentErrorMsg);
            }
            orderIsDeterministic &= subqueryBestPlan.isOrderDeterministic();
            if (isContentDeterministic != null && !subqueryBestPlan.isContentDeterministic()) {
                isContentDeterministic = subqueryBestPlan.nondeterminismDetail();
            }
            // Offsets or limits in subqueries are only significant (only effect content determinism)
            // when they apply to un-ordered subquery contents.
            hasSignificantOffsetOrLimit |=
                    (( ! subqueryBestPlan.isOrderDeterministic() ) && subqueryBestPlan.hasLimitOrOffset());
        }

        // need to reset plan id for the entire SQL
        m_planSelector.m_planId = nextPlanId;

        return new ParsedResultAccumulator(orderIsDeterministic,
                                           hasSignificantOffsetOrLimit,
                                           isContentDeterministic);
    }


    /**
     * Generate best cost plans for each Subquery expression from the list
     * @param subqueryExprs - list of subquery expressions
     * @return true if a best plan was generated for each subquery, false otherwise
     */
    private boolean getBestCostPlanForExpressionSubQueries(Set<AbstractExpression> subqueryExprs) {
        int nextPlanId = m_planSelector.m_planId;

        for (AbstractExpression expr : subqueryExprs) {
            assert(expr instanceof SelectSubqueryExpression);
            if (!(expr instanceof SelectSubqueryExpression)) {
                continue; // DEAD CODE?
            }

            SelectSubqueryExpression subqueryExpr = (SelectSubqueryExpression) expr;
            StmtSubqueryScan subqueryScan = subqueryExpr.getSubqueryScan();
            nextPlanId = planForParsedSubquery(subqueryScan, nextPlanId);
            CompiledPlan bestPlan = subqueryScan.getBestCostPlan();
            if (bestPlan == null) {
                return false;
            }

            subqueryExpr.setSubqueryNode(bestPlan.rootPlanGraph);
            // The subquery plan must not contain Receive/Send nodes because it will be executed
            // multiple times during the parent statement execution.
            if (bestPlan.rootPlanGraph.hasAnyNodeOfType(PlanNodeType.SEND)) {
                // fail the whole plan
                m_recentErrorMsg = IN_EXISTS_SCALAR_ERROR_MESSAGE;
                return false;
            }
        }
        // need to reset plan id for the entire SQL
        m_planSelector.m_planId = nextPlanId;

        return true;
    }


    /**
     * Generate a unique and correct plan for the current SQL statement context.
     * This method gets called repeatedly until it returns null, meaning there
     * are no more plans.
     *
     * @return A not-previously returned query plan or null if no more
     *         computable plans.
     */
    private CompiledPlan getNextPlan() {
        CompiledPlan retval;
        AbstractParsedStmt nextStmt = null;
        if (m_parsedUnion != null) {
            nextStmt = m_parsedUnion;
            retval = getNextUnionPlan();
        }
        else if (m_parsedSelect != null) {
            nextStmt = m_parsedSelect;
            retval = getNextSelectPlan();
        }
        else if (m_parsedInsert != null) {
            nextStmt = m_parsedInsert;
            retval = getNextInsertPlan();
        }
        else if (m_parsedDelete != null) {
            nextStmt = m_parsedDelete;
            retval = getNextDeletePlan();
            // note that for replicated tables, multi-fragment plans
            // need to divide the result by the number of partitions
        }
        else if (m_parsedUpdate != null) {
            nextStmt = m_parsedUpdate;
            retval = getNextUpdatePlan();
        }
        else {
            throw new RuntimeException(
                    "setupForNewPlans encountered unsupported statement type.");
        }

        if (retval == null || retval.rootPlanGraph == null) {
            return null;
        }

        assert (nextStmt != null);
        retval.parameters = nextStmt.getParameters();
        return retval;
    }

    /**
     * This is a UNION specific method. Generate a unique and correct plan
     * for the current SQL UNION statement by building the best plans for each individual statements
     * within the UNION.
     *
     * @return A union plan or null.
     */
    private CompiledPlan getNextUnionPlan() {
        String isContentDeterministic = null;
        // Since only the one "best" plan is considered,
        // this method should be called only once.
        if (m_bestAndOnlyPlanWasGenerated) {
            return null;
        }

        m_bestAndOnlyPlanWasGenerated = true;
        // Simply return an union plan node with a corresponding union type set
        AbstractPlanNode subUnionRoot = new UnionPlanNode(m_parsedUnion.m_unionType);
        m_recentErrorMsg = null;

        ArrayList<CompiledPlan> childrenPlans = new ArrayList<>();
        StatementPartitioning commonPartitioning = null;

        // Build best plans for the children first
        int planId = 0;
        for (AbstractParsedStmt parsedChildStmt : m_parsedUnion.m_children) {
            StatementPartitioning partitioning = (StatementPartitioning)m_partitioning.clone();
            PlanSelector planSelector = (PlanSelector) m_planSelector.clone();
            planSelector.m_planId = planId;
            PlanAssembler assembler = new PlanAssembler(
                    m_catalogCluster, m_catalogDb, partitioning, planSelector);
            CompiledPlan bestChildPlan = assembler.getBestCostPlan(parsedChildStmt);
            partitioning = assembler.m_partitioning;

            // make sure we got a winner
            if (bestChildPlan == null) {
                m_recentErrorMsg = assembler.getErrorMessage();
                if (m_recentErrorMsg == null) {
                    m_recentErrorMsg = "Unable to plan for statement. Error unknown.";
                }
                return null;
            }

            childrenPlans.add(bestChildPlan);
            // Remember the content non-determinism message for the
            // first non-deterministic children we find.
            if (isContentDeterministic != null) {
                isContentDeterministic = bestChildPlan.nondeterminismDetail();
            }

            // Make sure that next child's plans won't override current ones.
            planId = planSelector.m_planId;

            // Decide whether child statements' partitioning is compatible.
            if (commonPartitioning == null) {
                commonPartitioning = partitioning;
                continue;
            }

            AbstractExpression statementPartitionExpression = partitioning.singlePartitioningExpression();
            if (commonPartitioning.requiresTwoFragments()) {
                if (partitioning.requiresTwoFragments() || statementPartitionExpression != null) {
                    // If two child statements need to use a second fragment,
                    // it can't currently be a two-fragment plan.
                    // The coordinator expects a single-table result from each partition.
                    // Also, currently the coordinator of a two-fragment plan is not allowed to
                    // target a particular partition, so neither can the union of the coordinator
                    // and a statement that wants to run single-partition.
                    throw new PlanningErrorException(
                            "Statements are too complex in set operation using multiple partitioned tables.");
                }
                // the new statement is apparently a replicated read and has no effect on partitioning
                continue;
            }

            AbstractExpression commonPartitionExpression = commonPartitioning.singlePartitioningExpression();
            if (commonPartitionExpression == null) {
                // the prior statement(s) were apparently replicated reads
                // and have no effect on partitioning
                commonPartitioning = partitioning;
                continue;
            }

            if (partitioning.requiresTwoFragments()) {
                // Again, currently the coordinator of a two-fragment plan is not allowed to
                // target a particular partition, so neither can the union of the coordinator
                // and a statement that wants to run single-partition.
                throw new PlanningErrorException(
                        "Statements are too complex in set operation using multiple partitioned tables.");
            }

            if (statementPartitionExpression == null) {
                // the new statement is apparently a replicated read and has no effect on partitioning
                continue;
            }

            if ( ! commonPartitionExpression.equals(statementPartitionExpression)) {
                throw new PlanningErrorException(
                        "Statements use conflicting partitioned table filters in set operation or sub-query.");
            }
        }

        if (commonPartitioning != null) {
            m_partitioning = commonPartitioning;
        }

        // need to reset plan id for the entire UNION
        m_planSelector.m_planId = planId;

        // Add and link children plans
        for (CompiledPlan selectPlan : childrenPlans) {
            subUnionRoot.addAndLinkChild(selectPlan.rootPlanGraph);
        }

        // order by
        if (m_parsedUnion.hasOrderByColumns()) {
            subUnionRoot = handleOrderBy(m_parsedUnion, subUnionRoot);
        }

        // limit/offset
        if (m_parsedUnion.hasLimitOrOffset()) {
            subUnionRoot = handleUnionLimitOperator(subUnionRoot);
        }

        CompiledPlan retval = new CompiledPlan();
        retval.rootPlanGraph = subUnionRoot;
        retval.setReadOnly(true);
        retval.sql = m_planSelector.m_sql;
        boolean orderIsDeterministic = m_parsedUnion.isOrderDeterministic();
        boolean hasLimitOrOffset = m_parsedUnion.hasLimitOrOffset();
        retval.statementGuaranteesDeterminism(hasLimitOrOffset, orderIsDeterministic, isContentDeterministic);

        // compute the cost - total of all children
        retval.cost = 0.0;
        for (CompiledPlan bestChildPlan : childrenPlans) {
            retval.cost += bestChildPlan.cost;
        }
        return retval;
    }

    private int planForParsedSubquery(StmtSubqueryScan subqueryScan, int planId) {
        AbstractParsedStmt subQuery = subqueryScan.getSubqueryStmt();
        assert(subQuery != null);
        PlanSelector planSelector = (PlanSelector) m_planSelector.clone();
        planSelector.m_planId = planId;
        StatementPartitioning currentPartitioning = (StatementPartitioning)m_partitioning.clone();
        PlanAssembler assembler = new PlanAssembler(
                m_catalogCluster, m_catalogDb, currentPartitioning, planSelector);
        CompiledPlan compiledPlan = assembler.getBestCostPlan(subQuery);
        // make sure we got a winner
        if (compiledPlan == null) {
            String tbAlias = subqueryScan.getTableAlias();
            m_recentErrorMsg = "Subquery statement for table " + tbAlias
                    + " has error: " + assembler.getErrorMessage();
            return planSelector.m_planId;
        }

        subqueryScan.setSubqueriesPartitioning(currentPartitioning);

        // Remove the coordinator send/receive pair.
        // It will be added later for the whole plan.
        //TODO: It may make more sense to plan ahead and not generate the send/receive pair
        // at all for subquery contexts where it is not needed.
        if (subqueryScan.canRunInOneFragment()) {
            // The MergeReceivePlanNode always has an inline ORDER BY node and may have
            // LIMIT/OFFSET and aggregation node(s). Removing the MergeReceivePlanNode will
            // also remove its inline node(s) which may produce an invalid access plan.
            // For example,
            // SELECT TC1 FROM (SELECT C1 AS TC1 FROM P ORDER BY C1) PT LIMIT 4;
            // where P is partitioned and C1 is a non-partitioned index column.
            // Removing the subquery MergeReceivePlnaNode and its ORDER BY node results
            // in the invalid access plan - the subquery result order is significant in this case
            // The concern with generally keeping the (Merge)Receive node in the subquery is
            // that it would needlessly generate more-than-2-fragment plans in cases
            // where 2 fragments could have done the job.
            if ( ! compiledPlan.rootPlanGraph.hasAnyNodeOfClass(MergeReceivePlanNode.class)) {
                compiledPlan.rootPlanGraph = removeCoordinatorSendReceivePair(compiledPlan.rootPlanGraph);
            }
        }
        subqueryScan.setBestCostPlan(compiledPlan);
        return planSelector.m_planId;
    }

    /**
     * Remove the coordinator send/receive pair if any from the graph.
     *
     * @param root the complete plan node.
     * @return the plan without the send/receive pair.
     */
    static public AbstractPlanNode removeCoordinatorSendReceivePair(AbstractPlanNode root) {
        assert(root != null);
        return removeCoordinatorSendReceivePairRecursive(root, root);
    }

    static private AbstractPlanNode removeCoordinatorSendReceivePairRecursive(
            AbstractPlanNode root,
            AbstractPlanNode current) {
        if (current instanceof AbstractReceivePlanNode) {
            assert(current.getChildCount() == 1);

            AbstractPlanNode child = current.getChild(0);
            assert(child instanceof SendPlanNode);

            assert(child.getChildCount() == 1);
            child = child.getChild(0);
            child.clearParents();
            if (current == root) {
                return child;
            }
            assert(current.getParentCount() == 1);
            AbstractPlanNode parent = current.getParent(0);
            parent.unlinkChild(current);
            parent.addAndLinkChild(child);
            return root;
        }

        if (current.getChildCount() == 1) {
            // This is still a coordinator node
            return removeCoordinatorSendReceivePairRecursive(root,
                    current.getChild(0));
        }

        // We have hit a multi-child plan node -- a nestloop join or a union.
        // Can we really assume that there is no send/receive below this point?
        // TODO: It seems to me (--paul) that for a replicated-to-partitioned
        // left outer join, we should be following the second (partitioned)
        // child node of a nestloop join.
        // I'm not sure what the correct behavior is for a union.
        return root;
    }

    /**
     * For each Subquery node in the plan tree attach the subquery plan to the parent node.
     * @param initial plan
     * @return A complete plan tree for the entire SQl.
     */
    private AbstractPlanNode connectChildrenBestPlans(AbstractPlanNode parentPlan) {
        if (parentPlan instanceof AbstractScanPlanNode) {
            AbstractScanPlanNode scanNode = (AbstractScanPlanNode) parentPlan;
            StmtTableScan tableScan = scanNode.getTableScan();
            if (tableScan instanceof StmtSubqueryScan) {
                CompiledPlan bestCostPlan = ((StmtSubqueryScan)tableScan).getBestCostPlan();
                assert (bestCostPlan != null);
                AbstractPlanNode subQueryRoot = bestCostPlan.rootPlanGraph;
                subQueryRoot.disconnectParents();
                scanNode.clearChildren();
                scanNode.addAndLinkChild(subQueryRoot);
            }
        }
        else {
            for (int i = 0; i < parentPlan.getChildCount(); ++i) {
                connectChildrenBestPlans(parentPlan.getChild(i));
            }
        }
        return parentPlan;
    }

    private CompiledPlan getNextSelectPlan() {
        assert (m_subAssembler != null);

        // A matview reaggregation template plan may have been initialized
        // with a post-predicate expression moved from the statement's
        // join tree prior to any subquery planning.
        // Since normally subquery planning is driven from the join tree,
        // any subqueries that are moved out of the join tree would need
        // to be planned separately.
        // This planning would need to be done prior to calling
        // m_subAssembler.nextPlan()
        // because it can have query partitioning implications.
        // Under the current query limitations, the partitioning implications
        // are very simple -- subqueries are not allowed in multipartition
        // queries against partitioned data, so detection of a subquery in
        // the same query as a matview reaggregation can just return an error,
        // without any need for subquery planning here.
        HashAggregatePlanNode reAggNode = null;
        HashAggregatePlanNode mvReAggTemplate = m_parsedSelect.m_mvFixInfo.getReAggregationPlanNode();
        if (mvReAggTemplate != null) {
            reAggNode = new HashAggregatePlanNode(mvReAggTemplate);
            AbstractExpression postPredicate = reAggNode.getPostPredicate();
            if (postPredicate != null && postPredicate.hasSubquerySubexpression()) {
                // For now, this is just a special case violation of the limitation on
                // use of subquery expressions in MP queries on partitioned data.
                // That special case was going undetected when we didn't flag it here.
                m_recentErrorMsg = IN_EXISTS_SCALAR_ERROR_MESSAGE;
                return null;
            }

            // // Something more along these lines would have to be enabled
            // // to allow expression subqueries to be used in multi-partition
            // // matview queries.
            // if (!getBestCostPlanForExpressionSubQueries(subqueryExprs)) {
            //     // There was at least one sub-query and we should have a compiled plan for it
            //    return null;
            // }
        }

        AbstractPlanNode subSelectRoot = m_subAssembler.nextPlan();

        if (subSelectRoot == null) {
            m_recentErrorMsg = m_subAssembler.m_recentErrorMsg;
            return null;
        }

        AbstractPlanNode root = subSelectRoot;

        boolean mvFixNeedsProjection = false;
        /*
         * If the access plan for the table in the join order was for a
         * distributed table scan there must be a send/receive pair at the top
         * EXCEPT for the special outer join case in which a replicated table
         * was on the OUTER side of an outer join across from the (joined) scan
         * of the partitioned table(s) (all of them) in the query. In that case,
         * the one required send/receive pair is already in the plan below the
         * inner side of a NestLoop join.
         */
        if (m_partitioning.requiresTwoFragments()) {
            boolean mvFixInfoCoordinatorNeeded = true;
            boolean mvFixInfoEdgeCaseOuterJoin = false;

            ArrayList<AbstractPlanNode> receivers = root.findAllNodesOfClass(AbstractReceivePlanNode.class);
            if (receivers.size() == 1) {
                // The subplan SHOULD be good to go, but just make sure that it doesn't
                // scan a partitioned table except under the ReceivePlanNode that was just found.

                // Edge cases: left outer join with replicated table.
                if (m_parsedSelect.m_mvFixInfo.needed()) {
                    mvFixInfoCoordinatorNeeded = false;
                    AbstractPlanNode receiveNode = receivers.get(0);
                    if (receiveNode.getParent(0) instanceof NestLoopPlanNode) {
                        if (subSelectRoot.hasInlinedIndexScanOfTable(m_parsedSelect.m_mvFixInfo.getMVTableName())) {
                            return getNextSelectPlan();
                        }

                        List<AbstractPlanNode> nljs = receiveNode.findAllNodesOfType(PlanNodeType.NESTLOOP);
                        List<AbstractPlanNode> nlijs = receiveNode.findAllNodesOfType(PlanNodeType.NESTLOOPINDEX);

                        // outer join edge case does not have any join plan node under receive node.
                        // This is like a single table case.
                        if (nljs.size() + nlijs.size() == 0) {
                            mvFixInfoEdgeCaseOuterJoin = true;
                        }
                        root = handleMVBasedMultiPartQuery(reAggNode, root, mvFixInfoEdgeCaseOuterJoin);
                    }
                }
            }
            else {
                if (receivers.size() > 0) {
                    throw new PlanningErrorException(
                            "This special case join between an outer replicated table and " +
                            "an inner partitioned table is too complex and is not supported.");
                }
                root = SubPlanAssembler.addSendReceivePair(root);
                // Root is a receive node here.
                assert(root instanceof ReceivePlanNode);

                if (m_parsedSelect.mayNeedAvgPushdown()) {
                    m_parsedSelect.switchOptimalSuiteForAvgPushdown();
                }
                if (m_parsedSelect.m_tableList.size() > 1 && m_parsedSelect.m_mvFixInfo.needed()
                        && subSelectRoot.hasInlinedIndexScanOfTable(m_parsedSelect.m_mvFixInfo.getMVTableName())) {
                    // MV partitioned joined query needs reAggregation work on coordinator.
                    // Index scan on MV table can not be supported.
                    // So, in-lined index scan of Nested loop index join can not be possible.
                    return getNextSelectPlan();
                }
            }

            root = handleAggregationOperators(root);

            // Process the re-aggregate plan node and insert it into the plan.
            if (m_parsedSelect.m_mvFixInfo.needed() && mvFixInfoCoordinatorNeeded) {
                AbstractPlanNode tmpRoot = root;
                root = handleMVBasedMultiPartQuery(reAggNode, root, mvFixInfoEdgeCaseOuterJoin);
                if (root != tmpRoot) {
                    mvFixNeedsProjection = true;
                }
            }
        }
        else {
            /*
             * There is no receive node and root is a single partition plan.
             */

            // If there is no receive plan node and no distributed plan has been generated,
            // the fix set for MV is not needed.
            m_parsedSelect.m_mvFixInfo.setNeeded(false);
            root = handleAggregationOperators(root);
        }

        // If we have a windowed expression in the display list we want to
        // add a PartitionByPlanNode here.
        if (m_parsedSelect.hasWindowFunctionExpression()) {
            root = handleWindowedOperators(root);
        }

        if (m_parsedSelect.hasOrderByColumns()) {
            root = handleOrderBy(m_parsedSelect, root);
            if (m_parsedSelect.isComplexOrderBy() && root instanceof OrderByPlanNode) {
                AbstractPlanNode child = root.getChild(0);
                AbstractPlanNode grandChild = child.getChild(0);

                // swap the ORDER BY and complex aggregate Projection node
                if (child instanceof ProjectionPlanNode) {
                    root.unlinkChild(child);
                    child.unlinkChild(grandChild);

                    child.addAndLinkChild(root);
                    root.addAndLinkChild(grandChild);

                    // update the new root
                    root = child;
                }
                else if (m_parsedSelect.hasDistinctWithGroupBy() &&
                        child.getPlanNodeType() == PlanNodeType.HASHAGGREGATE &&
                        grandChild.getPlanNodeType() == PlanNodeType.PROJECTION) {

                    AbstractPlanNode grandGrandChild = grandChild.getChild(0);
                    child.clearParents();
                    root.clearChildren();
                    grandGrandChild.clearParents();
                    grandChild.clearChildren();

                    grandChild.addAndLinkChild(root);
                    root.addAndLinkChild(grandGrandChild);

                    root = child;
                }
            }
        }

        // Add a project node if we need one.  Some types of nodes can have their
        // own inline projection nodes, while others need an out-of-line projection
        // node.
        if (mvFixNeedsProjection || needProjectionNode(root)) {
            root = addProjection(root);
        }

        if (m_parsedSelect.hasLimitOrOffset()) {
            root = handleSelectLimitOperator(root);
        }

        CompiledPlan plan = new CompiledPlan();
        plan.rootPlanGraph = root;
        plan.setReadOnly(true);
        boolean orderIsDeterministic = m_parsedSelect.isOrderDeterministic();
        boolean hasLimitOrOffset = m_parsedSelect.hasLimitOrOffset();
        String contentDeterminismMessage = m_parsedSelect.getContentDeterminismMessage();
        plan.statementGuaranteesDeterminism(hasLimitOrOffset, orderIsDeterministic, contentDeterminismMessage);

        // Apply the micro-optimization:
        // LIMIT push down, Table count / Counting Index, Optimized Min/Max
        MicroOptimizationRunner.applyAll(plan, m_parsedSelect);

        return plan;
    }

    /**
     * Return true if the plan referenced by root node needs a
     * projection node appended to the top.
     *
     * This method does a lot of "if this node is an
     * instance of this class.... else if this node is an
     * instance of this other class..."   Perhaps it could be replaced
     * by a virtual method on AbstractPlanNode?
     *
     * @param root   The root node of a plan
     * @return true if a project node is required
     */
    private boolean needProjectionNode (AbstractPlanNode root) {
        if (!root.planNodeClassNeedsProjectionNode()) {
            return false;
        }
        // If there is a complexGroupby at his point, it means that
        // display columns contain all the order by columns and
        // does not require another projection node on top of sort node.

        // If there is a complex aggregation case, the projection plan node is already added
        // right above the group by plan node. In future, we may inline that projection node.
        if (m_parsedSelect.hasComplexGroupby() || m_parsedSelect.hasComplexAgg()) {
            return false;
        }

        if (root instanceof AbstractReceivePlanNode &&
                m_parsedSelect.hasPartitionColumnInGroupby()) {
            // Top aggregate has been removed, its schema is exactly the same to
            // its local aggregate node.
            return false;
        }

        return true;
    }

    // ENG-4909 Bug: currently disable NESTLOOPINDEX plan for IN
    private static boolean disableNestedLoopIndexJoinForInComparison (AbstractPlanNode root, AbstractParsedStmt parsedStmt) {
        if (root.getPlanNodeType() == PlanNodeType.NESTLOOPINDEX) {
            assert(parsedStmt != null);
            return true;
        }

        return false;
    }

    /** Returns true if this DELETE can be executed in the EE as a truncate operation */
    static private boolean deleteIsTruncate(ParsedDeleteStmt stmt, AbstractPlanNode plan) {
        if (!(plan instanceof SeqScanPlanNode)) {
            return false;
        }

        // Assume all index scans have filters in this context, so only consider seq scans.
        SeqScanPlanNode seqScanNode = (SeqScanPlanNode)plan;
        if (seqScanNode.getPredicate() != null) {
            return false;
        }

        if (stmt.hasLimitOrOffset()) {
            return false;
        }

        return true;
    }

    private CompiledPlan getNextDeletePlan() {
        assert (m_subAssembler != null);

        // figure out which table we're deleting from
        assert (m_parsedDelete.m_tableList.size() == 1);
        Table targetTable = m_parsedDelete.m_tableList.get(0);

        AbstractPlanNode subSelectRoot = m_subAssembler.nextPlan();
        if (subSelectRoot == null) {
            return null;
        }

        // ENG-4909 Bug: currently disable NESTLOOPINDEX plan for IN
        if (disableNestedLoopIndexJoinForInComparison(subSelectRoot, m_parsedDelete)) {
            // Recursion here, now that subAssembler.nextPlan() has been called,
            // simply jumps ahead to the next plan (if any).
            return getNextDeletePlan();
        }

        boolean isSinglePartitionPlan = m_partitioning.wasSpecifiedAsSingle() || m_partitioning.isInferredSingle();

        // generate the delete node with the right target table
        DeletePlanNode deleteNode = new DeletePlanNode();
        deleteNode.setTargetTableName(targetTable.getTypeName());


        assert(subSelectRoot instanceof AbstractScanPlanNode);

        // If the scan matches all rows, we can throw away the scan
        // nodes and use a truncate delete node.
        if (deleteIsTruncate(m_parsedDelete, subSelectRoot)) {
            deleteNode.setTruncate(true);
        }
        else {

            // User may have specified an ORDER BY ... LIMIT clause
            if (m_parsedDelete.orderByColumns().size() > 0
                    && !isSinglePartitionPlan
                    && !targetTable.getIsreplicated()) {
                throw new PlanningErrorException(
                        "DELETE statements affecting partitioned tables must "
                        + "be able to execute on one partition "
                        + "when ORDER BY and LIMIT or OFFSET clauses "
                        + "are present.");
            }

            boolean needsOrderByNode = isOrderByNodeRequired(m_parsedDelete, subSelectRoot);

            AbstractExpression addressExpr = new TupleAddressExpression();
            NodeSchema proj_schema = new NodeSchema();
            // This planner-created column is magic.
            proj_schema.addColumn(
                    AbstractParsedStmt.TEMP_TABLE_NAME,
                    AbstractParsedStmt.TEMP_TABLE_NAME,
                    "tuple_address", "tuple_address",
                    addressExpr);
            if (needsOrderByNode) {
                // Projection will need to pass the sort keys to the order by node
                for (ParsedColInfo col : m_parsedDelete.orderByColumns()) {
                    proj_schema.addColumn(col.asSchemaColumn());
                }
            }
            ProjectionPlanNode projectionNode =
                    new ProjectionPlanNode(proj_schema);
            subSelectRoot.addInlinePlanNode(projectionNode);

            AbstractPlanNode root = subSelectRoot;
            if (needsOrderByNode) {
                OrderByPlanNode ob = buildOrderByPlanNode(m_parsedDelete.orderByColumns());
                ob.addAndLinkChild(root);
                root = ob;
            }

            if (m_parsedDelete.hasLimitOrOffset()) {
                assert(m_parsedDelete.orderByColumns().size() > 0);
                root.addInlinePlanNode(m_parsedDelete.limitPlanNode());
            }

            deleteNode.addAndLinkChild(root);
        }

        CompiledPlan plan = new CompiledPlan();
        if (isSinglePartitionPlan) {
            plan.rootPlanGraph = deleteNode;
        }
        else {
            // Send the local result counts to the coordinator.
            AbstractPlanNode recvNode = SubPlanAssembler.addSendReceivePair(deleteNode);
            // add a sum or a limit and send on top of the union
            plan.rootPlanGraph = addSumOrLimitAndSendToDMLNode(recvNode, targetTable.getIsreplicated());
        }

        // check non-determinism status
        plan.setReadOnly(false);

        // treat this as deterministic for reporting purposes:
        // delete statements produce just one row that is the
        // number of rows affected
        boolean orderIsDeterministic = true;

        boolean hasLimitOrOffset = m_parsedDelete.hasLimitOrOffset();

        // The delete statement cannot be inherently content non-deterministic.
        // So, the last parameter is always null.
        plan.statementGuaranteesDeterminism(hasLimitOrOffset, orderIsDeterministic, null);

        return plan;
    }

    private CompiledPlan getNextUpdatePlan() {
        assert (m_subAssembler != null);

        AbstractPlanNode subSelectRoot = m_subAssembler.nextPlan();
        if (subSelectRoot == null) {
            return null;
        }

        if (disableNestedLoopIndexJoinForInComparison(subSelectRoot, m_parsedUpdate)) {
            // Recursion here, now that subAssembler.nextPlan() has been called,
            // simply jumps ahead to the next plan (if any).
            return getNextUpdatePlan();
        }

        UpdatePlanNode updateNode = new UpdatePlanNode();
        //FIXME: does this assert need to be relaxed in the face of non-from-clause subquery support?
        // It was not in Mike A's original branch.
        assert (m_parsedUpdate.m_tableList.size() == 1);
        Table targetTable = m_parsedUpdate.m_tableList.get(0);
        updateNode.setTargetTableName(targetTable.getTypeName());
        // set this to false until proven otherwise
        updateNode.setUpdateIndexes(false);

        TupleAddressExpression tae = new TupleAddressExpression();
        NodeSchema proj_schema = new NodeSchema();
        // This planner-generated column is magic.
        proj_schema.addColumn(
                AbstractParsedStmt.TEMP_TABLE_NAME,
                AbstractParsedStmt.TEMP_TABLE_NAME,
                "tuple_address", "tuple_address",
                tae);

        // get the set of columns affected by indexes
        Set<String> affectedColumns = getIndexedColumnSetForTable(targetTable);

        // add the output columns we need to the projection
        //
        // Right now, the EE is going to use the original column names
        // and compare these to the persistent table column names in the
        // update executor in order to figure out which table columns get
        // updated.  We'll associate the actual values with VOLT_TEMP_TABLE
        // to avoid any false schema/column matches with the actual table.
        for (Entry<Column, AbstractExpression> colEntry :
            m_parsedUpdate.columns.entrySet()) {
            Column col = colEntry.getKey();
            String colName = col.getTypeName();
            AbstractExpression expr = colEntry.getValue();
            expr.setInBytes(colEntry.getKey().getInbytes());

            proj_schema.addColumn(
                    AbstractParsedStmt.TEMP_TABLE_NAME,
                    AbstractParsedStmt.TEMP_TABLE_NAME,
                    colName, colName,
                    expr);

            // check if this column is an indexed column
            if (affectedColumns.contains(colName)) {
                updateNode.setUpdateIndexes(true);
            }
        }
        ProjectionPlanNode projectionNode =
                new ProjectionPlanNode(proj_schema);


        // add the projection inline (TODO: this will break if more than one
        // layer is below this)
        //
        // When we inline this projection into the scan, we're going
        // to overwrite any original projection that we might have inlined
        // in order to simply cull the columns from the persistent table.
        assert(subSelectRoot instanceof AbstractScanPlanNode);
        subSelectRoot.addInlinePlanNode(projectionNode);

        // connect the nodes to build the graph
        updateNode.addAndLinkChild(subSelectRoot);

        AbstractPlanNode planRoot = null;
        if (m_partitioning.wasSpecifiedAsSingle() || m_partitioning.isInferredSingle()) {
            planRoot = updateNode;
        }
        else {
            // Send the local result counts to the coordinator.
            AbstractPlanNode recvNode = SubPlanAssembler.addSendReceivePair(updateNode);
            // add a sum or a limit and send on top of the union
            planRoot = addSumOrLimitAndSendToDMLNode(recvNode, targetTable.getIsreplicated());
        }

        CompiledPlan retval = new CompiledPlan();
        retval.rootPlanGraph = planRoot;
        retval.setReadOnly (false);

        if (targetTable.getIsreplicated()) {
            retval.replicatedTableDML = true;
        }

        //FIXME: This assumption was only safe when we didn't support updates
        // w/ possibly non-deterministic subqueries.
        // Is there some way to integrate a "subquery determinism" check here?
        // because we didn't support updates with limits, either.
        // Since the update cannot be inherently non-deterministic, there is
        // no message, and the last parameter is null.
        retval.statementGuaranteesDeterminism(false, true, null);

        return retval;
    }

    static private AbstractExpression castExprIfNeeded(
            AbstractExpression expr, Column column) {
        if (expr.getValueType().getValue() != column.getType() ||
                expr.getValueSize() != column.getSize()) {
            expr = new OperatorExpression(ExpressionType.OPERATOR_CAST, expr, null);
            expr.setValueType(VoltType.get((byte) column.getType()));
            // We don't really support parameterized casting, such as specifically to "VARCHAR(3)"
            // vs. just VARCHAR, but set the size parameter anyway in this case to make sure that
            // the tuple that gets the result of the cast can be properly formatted as inline.
            // A too-wide value survives the cast (to generic VARCHAR of any length) but the
            // attempt to cache the result in the inline temp tuple storage will throw an early
            // runtime error on be  half of the target table column.
            // The important thing here is to leave the formatting hint in the output schema that
            // drives the temp tuple layout.
            expr.setValueSize(column.getSize());
        }

        return expr;
    }

    /**
     * Get the next (only) plan for a SQL insertion. Inserts are pretty simple
     * and this will only generate a single plan.
     *
     * @return The next plan for a given insert statement.
     */
    private CompiledPlan getNextInsertPlan() {
        // there's really only one way to do an insert, so just
        // do it the right way once, then return null after that
        if (m_bestAndOnlyPlanWasGenerated)
            return null;
        m_bestAndOnlyPlanWasGenerated = true;

        // The child of the insert node produces rows containing values
        // from one of
        //   - A VALUES clause.  In this case the child node is a MaterializeNode
        //   - a SELECT statement as in "INSERT INTO ... SELECT ...".  In this case
        //       the child node is the root of an arbitrary subplan.

        // figure out which table we're inserting into
        assert (m_parsedInsert.m_tableList.size() == 1);
        Table targetTable = m_parsedInsert.m_tableList.get(0);
        StmtSubqueryScan subquery = m_parsedInsert.getSubqueryScan();
        CompiledPlan retval = null;
        String isContentDeterministic = null;
        if (subquery != null) {
            isContentDeterministic = subquery.calculateContentDeterminismMessage();
            if (subquery.getBestCostPlan() == null) {
                // Seems like this should really be caught earlier
                // in getBestCostPlan, above.
                throw new PlanningErrorException("INSERT INTO ... SELECT subquery could not be planned: "
                        + m_recentErrorMsg);

            }

            boolean targetIsExportTable = tableListIncludesExportOnly(m_parsedInsert.m_tableList);
            InsertSubPlanAssembler subPlanAssembler =
                    new InsertSubPlanAssembler(m_catalogDb, m_parsedInsert, m_partitioning,
                            targetIsExportTable);
            AbstractPlanNode subplan = subPlanAssembler.nextPlan();
            if (subplan == null) {
                throw new PlanningErrorException(subPlanAssembler.m_recentErrorMsg);
            }

            assert(m_partitioning.isJoinValid());

            //  Use the subquery's plan as the basis for the insert plan.
            retval = subquery.getBestCostPlan();
        }
        else {
            retval = new CompiledPlan();
        }
        retval.setReadOnly(false);

        // Iterate over each column in the table we're inserting into:
        //   - Make sure we're supplying values for columns that require it.
        //     For a normal INSERT, these are the usual non-nullable values that
        //     don't have a default value.
        //     For an UPSERT, the (only) required values are the primary key
        //     components. Other required values can be supplied from the
        //     existing row in "UPDATE mode". If some other value is required
        //     for an INSERT, UPSERT's "INSERT mode" will throw a runtime
        //     constraint violation as the INSERT operation tries to set the
        //     non-nullable column to null.
        //   - Set partitioning expressions for VALUES (...) case.
        //     TODO: it would be good someday to do the same kind of processing
        //      for the INSERT ... SELECT ... case, by analyzing the subquery.
        if (m_parsedInsert.m_isUpsert) {
            boolean hasPrimaryKey = false;
            for (Constraint constraint : targetTable.getConstraints()) {
                if (constraint.getType() != ConstraintType.PRIMARY_KEY.getValue()) {
                    continue;
                }

                hasPrimaryKey = true;
                boolean targetsPrimaryKey = false;
                for (ColumnRef colRef : constraint.getIndex().getColumns()) {
                    int primary = colRef.getColumn().getIndex();
                    for (Column targetCol : m_parsedInsert.m_columns.keySet()) {
                        if (targetCol.getIndex() == primary) {
                            targetsPrimaryKey = true;
                            break;
                        }
                    }
                    if (! targetsPrimaryKey) {
                        throw new PlanningErrorException("UPSERT on table \"" +
                                targetTable.getTypeName() +
                                "\" must specify a value for primary key \"" +
                                colRef.getColumn().getTypeName() + "\".");
                    }
                }
            }
            if (! hasPrimaryKey) {
                throw new PlanningErrorException("UPSERT is not allowed on table \"" +
                        targetTable.getTypeName() + "\" that has no primary key.");
            }
        }
        CatalogMap<Column> targetTableColumns = targetTable.getColumns();
        for (Column col : targetTableColumns) {
            boolean needsValue = (!m_parsedInsert.m_isUpsert) &&
                    (col.getNullable() == false) && (col.getDefaulttype() == 0);
            if (needsValue && !m_parsedInsert.m_columns.containsKey(col)) {
                // This check could be done during parsing?
                throw new PlanningErrorException("Column " + col.getName()
                        + " has no default and is not nullable.");
            }

            // hint that this statement can be executed SP.
            if (col.equals(m_partitioning.getPartitionColForDML()) && subquery == null) {
                // When AdHoc insert-into-select is supported, we'll need to be able to infer
                // partitioning of the sub-select
                AbstractExpression expr = m_parsedInsert.getExpressionForPartitioning(col);
                String fullColumnName = targetTable.getTypeName() + "." + col.getTypeName();
                m_partitioning.addPartitioningExpression(fullColumnName, expr, expr.getValueType());
            }
        }

        NodeSchema matSchema = null;
        if (subquery == null) {
            matSchema = new NodeSchema();
        }

        int[] fieldMap = new int[m_parsedInsert.m_columns.size()];
        int i = 0;

        // The insert statement's set of columns are contained in a LinkedHashMap,
        // meaning that we'll iterate over the columns here in the order that the user
        // specified them in the original SQL.  (If the statement didn't specify any
        // columns, then all the columns will be in the map in schema order.)
        //   - Build the field map, used by insert executor to build tuple to execute
        //   - For VALUES(...) insert statements, build the materialize node's schema
        for (Map.Entry<Column, AbstractExpression> e : m_parsedInsert.m_columns.entrySet()) {
            Column col = e.getKey();
            fieldMap[i] = col.getIndex();

            if (matSchema != null) {
                AbstractExpression valExpr = e.getValue();
                valExpr.setInBytes(col.getInbytes());

                // Patch over any mismatched expressions with an explicit cast.
                // Most impossible-to-cast type combinations should have already been caught by the
                // parser, but there are also runtime checks in the casting code
                // -- such as for out of range values.
                valExpr = castExprIfNeeded(valExpr, col);

                matSchema.addColumn(
                        AbstractParsedStmt.TEMP_TABLE_NAME,
                        AbstractParsedStmt.TEMP_TABLE_NAME,
                        col.getTypeName(), col.getTypeName(),
                        valExpr);
            }

            i++;
        }

        // the root of the insert plan is always an InsertPlanNode
        InsertPlanNode insertNode = new InsertPlanNode();
        insertNode.setTargetTableName(targetTable.getTypeName());
        if (subquery != null) {
            insertNode.setSourceIsPartitioned(! subquery.getIsReplicated());
        }

        // The field map tells the insert node
        // where to put values produced by child into the row to be inserted.
        insertNode.setFieldMap(fieldMap);

        if (matSchema != null) {
            MaterializePlanNode matNode =
                    new MaterializePlanNode(matSchema);
            // connect the insert and the materialize nodes together
            insertNode.addAndLinkChild(matNode);

            retval.statementGuaranteesDeterminism(false, true, isContentDeterministic);
        }
        else {
            insertNode.addAndLinkChild(retval.rootPlanGraph);
        }

        if (m_partitioning.wasSpecifiedAsSingle() || m_partitioning.isInferredSingle()) {
            insertNode.setMultiPartition(false);
            retval.rootPlanGraph = insertNode;
            return retval;
        }

        insertNode.setMultiPartition(true);
        AbstractPlanNode recvNode = SubPlanAssembler.addSendReceivePair(insertNode);

        // add a count or a limit and send on top of the union
        retval.rootPlanGraph = addSumOrLimitAndSendToDMLNode(recvNode, targetTable.getIsreplicated());
        return retval;
    }

    /**
     * Adds a sum or limit node followed by a send node to the given DML node. If the DML target
     * is a replicated table, it will add a limit node, otherwise it adds a sum node.
     *
     * @param dmlRoot
     * @param isReplicated Whether or not the target table is a replicated table.
     * @return
     */
    private static AbstractPlanNode addSumOrLimitAndSendToDMLNode(AbstractPlanNode dmlRoot, boolean isReplicated)
    {
        AbstractPlanNode sumOrLimitNode;
        if (isReplicated) {
            // Replicated table DML result doesn't need to be summed. All partitions should
            // modify the same number of tuples in replicated table, so just pick the result from
            // any partition.
            LimitPlanNode limitNode = new LimitPlanNode();
            sumOrLimitNode = limitNode;
            limitNode.setLimit(1);
        }
        else {
            // create the nodes being pushed on top of dmlRoot.
            AggregatePlanNode countNode = new AggregatePlanNode();
            sumOrLimitNode = countNode;

            // configure the count aggregate (sum) node to produce a single
            // output column containing the result of the sum.
            // Create a TVE that should match the tuple count input column
            // This TVE is magic.
            // really really need to make this less hard-wired
            TupleValueExpression count_tve = new TupleValueExpression(
                    AbstractParsedStmt.TEMP_TABLE_NAME,
                    AbstractParsedStmt.TEMP_TABLE_NAME,
                    "modified_tuples",
                    "modified_tuples",
                    0);
            count_tve.setValueType(VoltType.BIGINT);
            count_tve.setValueSize(VoltType.BIGINT.getLengthInBytesForFixedTypes());
            countNode.addAggregate(ExpressionType.AGGREGATE_SUM, false, 0, count_tve);

            // The output column. Not really based on a TVE (it is really the
            // count expression represented by the count configured above). But
            // this is sufficient for now.  This looks identical to the above
            // TVE but it's logically different so we'll create a fresh one.
            TupleValueExpression tve = new TupleValueExpression(
                    AbstractParsedStmt.TEMP_TABLE_NAME,
                    AbstractParsedStmt.TEMP_TABLE_NAME,
                    "modified_tuples",
                    "modified_tuples",
                    0);
            tve.setValueType(VoltType.BIGINT);
            tve.setValueSize(VoltType.BIGINT.getLengthInBytesForFixedTypes());
            NodeSchema count_schema = new NodeSchema();
            count_schema.addColumn(
                    AbstractParsedStmt.TEMP_TABLE_NAME,
                    AbstractParsedStmt.TEMP_TABLE_NAME,
                    "modified_tuples",
                    "modified_tuples",
                    tve);
            countNode.setOutputSchema(count_schema);
        }

        // connect the nodes to build the graph
        sumOrLimitNode.addAndLinkChild(dmlRoot);
        SendPlanNode sendNode = new SendPlanNode();
        sendNode.addAndLinkChild(sumOrLimitNode);

        return sendNode;
    }

    /**
     * Given a relatively complete plan-sub-graph, apply a trivial projection
     * (filter) to it. If the root node can embed the projection do so. If not,
     * add a new projection node.
     *
     * @param rootNode
     *            The root of the plan-sub-graph to add the projection to.
     * @return The new root of the plan-sub-graph (might be the same as the
     *         input).
     */
    private AbstractPlanNode addProjection(AbstractPlanNode rootNode) {
        assert (m_parsedSelect != null);
        assert (m_parsedSelect.m_displayColumns != null);

        // Build the output schema for the projection based on the display columns
        NodeSchema proj_schema = m_parsedSelect.getFinalProjectionSchema();
        for (SchemaColumn col : proj_schema.getColumns()) {
            // Adjust the differentiator fields of TVEs, since they need to
            // reflect the inlined projection node in scan nodes.
            AbstractExpression colExpr = col.getExpression();
            Collection<TupleValueExpression> allTves =
                    ExpressionUtil.getTupleValueExpressions(colExpr);
            for (TupleValueExpression tve : allTves) {
                if ( ! tve.needsDifferentiation()) {
                    // PartitionByPlanNode and a following OrderByPlanNode
                    // can have an internally generated RANK column.
                    // These do not need to have their differentiator updated,
                    // since it's only used for disambiguation in some
                    // combinations of "SELECT *" and subqueries.
                    // In fact attempting to adjust this special column will
                    // cause failed assertions.  The tve for this expression
                    // will be marked as not needing differentiation,
                    // so we just ignore it here.
                    continue;
                }
                rootNode.adjustDifferentiatorField(tve);
            }
        }

        ProjectionPlanNode projectionNode = new ProjectionPlanNode();
        projectionNode.setOutputSchemaWithoutClone(proj_schema);

        // If the projection can be done inline. then add the
        // projection node inline.  Even if the rootNode is a
        // scan, if we have a windowed expression we need to
        // add it out of line.
        if (rootNode instanceof AbstractScanPlanNode) {
            rootNode.addInlinePlanNode(projectionNode);
            return rootNode;
        }

        projectionNode.addAndLinkChild(rootNode);
        return projectionNode;
    }

    /** Given a list of ORDER BY columns, construct and return an OrderByPlanNode. */
    private static OrderByPlanNode buildOrderByPlanNode(List<ParsedColInfo> cols) {
        OrderByPlanNode n = new OrderByPlanNode();

        for (ParsedColInfo col : cols) {
            n.addSort(col.expression,
                    col.ascending ? SortDirectionType.ASC
                                  : SortDirectionType.DESC);
        }

        return n;
    }

    /**
     * Determine if an OrderByPlanNode is needed.  This may return false if the
     * statement has no ORDER BY clause, or if the subtree is already producing
     * rows in the correct order.
     * @param parsedStmt    The statement whose plan may need an OrderByPlanNode
     * @param root          The subtree which may need its output tuples ordered
     * @return true if the plan needs an OrderByPlanNode, false otherwise
     */
    private static boolean isOrderByNodeRequired(AbstractParsedStmt parsedStmt, AbstractPlanNode root) {
        // Only sort when the statement has an ORDER BY.
        if ( ! parsedStmt.hasOrderByColumns()) {
            return false;
        }

        SortDirectionType sortDirection = SortDirectionType.INVALID;
        // Skip the explicit ORDER BY plan step if an IndexScan is already providing the equivalent ordering.
        // Note that even tree index scans that produce values in their own "key order" only report
        // their sort direction != SortDirectionType.INVALID
        // when they enforce an ordering equivalent to the one requested in the ORDER BY clause.
        // Even an intervening non-hash aggregate will not interfere in this optimization.
        AbstractPlanNode nonAggPlan = root;

        // EE keeps the insertion ORDER so that ORDER BY could apply before DISTINCT.
        // However, this probably is not optimal if there are low cardinality results.
        // Again, we have to replace the TVEs for ORDER BY clause for these cases in planning.

        if (nonAggPlan.getPlanNodeType() == PlanNodeType.AGGREGATE) {
            nonAggPlan = nonAggPlan.getChild(0);
        }
        if (nonAggPlan instanceof IndexScanPlanNode) {
            sortDirection = ((IndexScanPlanNode)nonAggPlan).getSortDirection();
        }
        // Optimization for NestLoopIndex on IN list, possibly other cases of ordered join results.
        // Skip the explicit ORDER BY plan step if NestLoopIndex is providing the equivalent ordering
        else if (nonAggPlan instanceof AbstractJoinPlanNode) {
            sortDirection = ((AbstractJoinPlanNode)nonAggPlan).getSortDirection();
        }

        if (sortDirection != SortDirectionType.INVALID) {
            return false;
        }

        return true;
    }

    /**
     * Create an order by node as required by the statement and make it a parent of root.
     * @param parsedStmt  Parsed statement, for context
     * @param root        The root of the plan needing ordering
     * @return new orderByNode (the new root) or the original root if no orderByNode was required.
     */
    private static AbstractPlanNode handleOrderBy(AbstractParsedStmt parsedStmt, AbstractPlanNode root) {
        assert (parsedStmt instanceof ParsedSelectStmt || parsedStmt instanceof ParsedUnionStmt ||
                parsedStmt instanceof ParsedDeleteStmt);

        if (! isOrderByNodeRequired(parsedStmt, root)) {
            return root;
        }

        OrderByPlanNode orderByNode = buildOrderByPlanNode(parsedStmt.orderByColumns());
        orderByNode.addAndLinkChild(root);
        return orderByNode;
    }

    /**
     * Add a limit, pushed-down if possible, and return the new root.
     * @param root top of the original plan
     * @return new plan's root node
     */
    private AbstractPlanNode handleSelectLimitOperator(AbstractPlanNode root)
    {
        // The coordinator's top limit graph fragment for a MP plan.
        // If planning "order by ... limit", getNextSelectPlan()
        // will have already added an order by to the coordinator frag.
        // This is the only limit node in a SP plan
        LimitPlanNode topLimit = m_parsedSelect.getLimitNodeTop();
        assert(topLimit != null);

        /*
         * TODO: allow push down limit with distinct (select distinct C from T limit 5)
         * , DISTINCT in aggregates and DISTINCT PUSH DOWN with partition column included.
         */
        AbstractPlanNode sendNode = null;
        // Whether or not we can push the limit node down
        boolean canPushDown = ! m_parsedSelect.hasDistinctWithGroupBy();
        if (canPushDown) {
            sendNode = checkLimitPushDownViability(root);
            if (sendNode == null) {
                canPushDown = false;
            }
            else {
                canPushDown = m_parsedSelect.getCanPushdownLimit();
            }
        }

        if (m_parsedSelect.m_mvFixInfo.needed()) {
            // Do not push down limit for mv based distributed query.
            canPushDown = false;
        }

        /*
         * Push down the limit plan node when possible even if offset is set. If
         * the plan is for a partitioned table, do the push down. Otherwise,
         * there is no need to do the push down work, the limit plan node will
         * be run in the partition.
         */
        if (canPushDown) {
            /*
             * For partitioned table, the pushed-down limit plan node has a limit based
             * on the combined limit and offset, which may require an expression if either of these
             * was not a hard-coded constant and didn't get parameterized.
             * The top level limit plan node remains the same, with the original limit and offset values.
             */
            LimitPlanNode distLimit = m_parsedSelect.getLimitNodeDist();

            // Disconnect the distributed parts of the plan below the SEND node
            AbstractPlanNode distributedPlan = sendNode.getChild(0);
            distributedPlan.clearParents();
            sendNode.clearChildren();

            // If the distributed limit must be performed on ordered input,
            // ensure the order of the data on each partition.
            if (m_parsedSelect.hasOrderByColumns()) {
                distributedPlan = handleOrderBy(m_parsedSelect, distributedPlan);
            }

            if (isInlineLimitPlanNodePossible(distributedPlan)) {
                // Inline the distributed limit.
                distributedPlan.addInlinePlanNode(distLimit);
                sendNode.addAndLinkChild(distributedPlan);
            }
            else {
                distLimit.addAndLinkChild(distributedPlan);
                // Add the distributed work back to the plan
                sendNode.addAndLinkChild(distLimit);
            }
        }
        // In future, inline LIMIT for join, Receive
        // Then we do not need to distinguish the order by node.
        return inlineLimitOperator(root, topLimit);
    }

    /**
     * Add a limit, and return the new root.
     * @param root top of the original plan
     * @return new plan's root node
     */
    private AbstractPlanNode handleUnionLimitOperator(AbstractPlanNode root) {
        // The coordinator's top limit graph fragment for a MP plan.
        // If planning "order by ... limit", getNextUnionPlan()
        // will have already added an order by to the coordinator frag.
        // This is the only limit node in a SP plan
        LimitPlanNode topLimit = m_parsedUnion.getLimitNodeTop();
        assert(topLimit != null);
        return inlineLimitOperator(root, topLimit);
    }

    /**
     * Inline Limit plan node if possible
     * @param root
     * @param topLimit
     * @return
     */
    private AbstractPlanNode inlineLimitOperator(AbstractPlanNode root,
            LimitPlanNode topLimit) {
        if (isInlineLimitPlanNodePossible(root)) {
            root.addInlinePlanNode(topLimit);
        }
        else if (root instanceof ProjectionPlanNode &&
                isInlineLimitPlanNodePossible(root.getChild(0)) ) {
            // In future, inlined this projection node for OrderBy and Aggregate
            // Then we could delete this ELSE IF block.
            root.getChild(0).addInlinePlanNode(topLimit);
        }
        else {
            topLimit.addAndLinkChild(root);
            root = topLimit;
        }
        return root;
    }

    /**
     * Inline limit plan node can be applied with ORDER BY node
     * and serial aggregation node
     * @param pn
     * @return
     */
    static private boolean isInlineLimitPlanNodePossible(AbstractPlanNode pn) {
        if (pn instanceof OrderByPlanNode ||
            pn.getPlanNodeType() == PlanNodeType.AGGREGATE) {
            return true;
        }
        return false;
    }


    private AbstractPlanNode handleMVBasedMultiPartQuery(
            HashAggregatePlanNode reAggNode,
            AbstractPlanNode root,
            boolean edgeCaseOuterJoin) {
        MaterializedViewFixInfo mvFixInfo = m_parsedSelect.m_mvFixInfo;

        AbstractPlanNode receiveNode = root;
        AbstractPlanNode reAggParent = null;
        // Find receive plan node and insert the constructed
        // re-aggregation plan node.
        if (root instanceof AbstractReceivePlanNode) {
            root = reAggNode;
        }
        else {
            List<AbstractPlanNode> recList = root.findAllNodesOfClass(AbstractReceivePlanNode.class);
            assert(recList.size() == 1);
            receiveNode = recList.get(0);

            reAggParent = receiveNode.getParent(0);
            boolean result = reAggParent.replaceChild(receiveNode, reAggNode);
            assert(result);
        }
        reAggNode.addAndLinkChild(receiveNode);
        reAggNode.m_isCoordinatingAggregator = true;

        assert(receiveNode instanceof ReceivePlanNode);
        AbstractPlanNode sendNode = receiveNode.getChild(0);
        assert(sendNode instanceof SendPlanNode);
        AbstractPlanNode sendNodeChild = sendNode.getChild(0);

        HashAggregatePlanNode reAggNodeForReplace = null;
        if (m_parsedSelect.m_tableList.size() > 1 && !edgeCaseOuterJoin) {
            reAggNodeForReplace = reAggNode;
        }
        boolean find = mvFixInfo.processScanNodeWithReAggNode(sendNode, reAggNodeForReplace);
        assert(find);

        // If it is a normal joined query, replace the node under the
        // receive node with materialized view scan node.
        if (m_parsedSelect.m_tableList.size() > 1 && !edgeCaseOuterJoin) {
            AbstractPlanNode joinNode = sendNodeChild;
            // No agg, limit pushed down at this point.
            assert(joinNode instanceof AbstractJoinPlanNode);

            // Fix the node after Re-aggregation node.
            joinNode.clearParents();

            assert(mvFixInfo.m_scanNode != null);
            mvFixInfo.m_scanNode.clearParents();

            // replace joinNode with MV scan node on each partition.
            sendNode.clearChildren();
            sendNode.addAndLinkChild(mvFixInfo.m_scanNode);

            // If reAggNode has parent node before we put it under join node,
            // its parent will be the parent of the new join node. Update the root node.
            if (reAggParent != null) {
                reAggParent.replaceChild(reAggNode, joinNode);
                root = reAggParent;
            }
            else {
                root = joinNode;
            }
        }

        return root;
    }

    private static class IndexGroupByInfo {
        boolean m_multiPartition = false;

        List<Integer> m_coveredGroupByColumns;
        boolean m_canBeFullySerialized = false;

        AbstractPlanNode m_indexAccess = null;

        boolean isChangedToSerialAggregate() {
            return m_canBeFullySerialized && m_indexAccess != null;
        }

        boolean isChangedToPartialAggregate() {
            return !m_canBeFullySerialized && m_indexAccess != null;
        }

        boolean needHashAggregator(AbstractPlanNode root, ParsedSelectStmt parsedSelect) {
            // A hash is required to build up per-group aggregates in parallel vs.
            // when there is only one aggregation over the entire table OR when the
            // per-group aggregates are being built serially from the ordered output
            // of an index scan.
            // Currently, an index scan only claims to have a sort direction when its output
            // matches the order demanded by the ORDER BY clause.
            if (! parsedSelect.isGrouped()) {
                return false;
            }

            if (isChangedToSerialAggregate() && ! m_multiPartition) {
                return false;
            }

            boolean predeterminedOrdering = false;
            if (root instanceof IndexScanPlanNode) {
                if (((IndexScanPlanNode)root).getSortDirection() !=
                        SortDirectionType.INVALID) {
                    predeterminedOrdering = true;
                }
            }
            else if (root instanceof AbstractJoinPlanNode) {
                if (((AbstractJoinPlanNode)root).getSortDirection() !=
                        SortDirectionType.INVALID) {
                    predeterminedOrdering = true;
                }
            }
            if (predeterminedOrdering) {
                // The ordering predetermined by indexed access is known
                // to cover (at least) the ORDER BY columns.
                // Yet, any additional non-ORDER-BY columns in the GROUP BY
                // clause will need partial aggregation.
                if (parsedSelect.groupByIsAnOrderByPermutation()) {
                    return false;
                }
            }

            return true;
        }

    }

    private static AbstractPlanNode findSeqScanCandidateForGroupBy(
            AbstractPlanNode candidate) {
        if (candidate.getPlanNodeType() == PlanNodeType.SEQSCAN &&
                ! candidate.isSubQuery()) {
            // scan on sub-query does not support index, early exit here
            // In future, support sub-query edge cases.
            return candidate;
        }

        // For join node, find outer sequential scan plan node
        if (candidate.getPlanNodeType() == PlanNodeType.NESTLOOP) {
            assert(candidate.getChildCount() == 2);
            return findSeqScanCandidateForGroupBy(candidate.getChild(0));
        }

        if (candidate.getPlanNodeType() == PlanNodeType.NESTLOOPINDEX) {
            return findSeqScanCandidateForGroupBy(candidate.getChild(0));
        }

        return null;
    }

    /**
     * For a seqscan feeding a GROUP BY, consider substituting an IndexScan
     * that pre-sorts by the GROUP BY keys.
     * If a candidate is already an indexscan,
     * simply calculate GROUP BY column coverage
     *
     * @param candidate
     * @param gbInfo
     * @return true when planner can switch to index scan
     *         from a sequential scan, and when the index scan
     *         has no parent plan node or the candidate is already
     *         an indexscan and covers all or some GROUP BY columns
     */
    private boolean switchToIndexScanForGroupBy(AbstractPlanNode candidate,
            IndexGroupByInfo gbInfo) {
        if (! m_parsedSelect.isGrouped()) {
            return false;
        }

        if (candidate instanceof IndexScanPlanNode) {
            calculateIndexGroupByInfo((IndexScanPlanNode) candidate, gbInfo);
            if (gbInfo.m_coveredGroupByColumns != null &&
                    !gbInfo.m_coveredGroupByColumns.isEmpty()) {
                // The candidate index does cover all or some
                // of the GROUP BY columns and can be serialized
                gbInfo.m_indexAccess = candidate;
                return true;
            }
            return false;
        }

        AbstractPlanNode sourceSeqScan = findSeqScanCandidateForGroupBy(candidate);
        if (sourceSeqScan == null) {
            return false;
        }
        assert(sourceSeqScan instanceof SeqScanPlanNode);

        AbstractPlanNode parent = null;
        if (sourceSeqScan.getParentCount() > 0) {
            parent = sourceSeqScan.getParent(0);
        }
        AbstractPlanNode indexAccess = indexAccessForGroupByExprs(
                (SeqScanPlanNode)sourceSeqScan, gbInfo);

        if (indexAccess.getPlanNodeType() != PlanNodeType.INDEXSCAN) {
            // does not find proper index to replace sequential scan
            return false;
        }

        gbInfo.m_indexAccess = indexAccess;
        if (parent != null) {
            // have a parent and would like to replace
            // the sequential scan with an index scan
            indexAccess.clearParents();
            // For two children join node, index 0 is its outer side
            parent.replaceChild(0, indexAccess);

            return false;
        }

        // parent is null and switched to index scan from sequential scan
        return true;
    }

    /**
     * Create nodes for windowed operations.
     *
     * @param root
     * @return
     */
    private AbstractPlanNode handleWindowedOperators(AbstractPlanNode root) {
        // Get the windowed expression.  We need to set its output
        // schema from the display list.
        WindowFunctionExpression winExpr = m_parsedSelect.getWindowFunctionExpressions().get(0);
        assert(winExpr != null);

        // This will set the output schema to contain the
        // windowed schema column only.  In generateOutputSchema
        // we will add the input columns.
        WindowFunctionPlanNode pnode = new WindowFunctionPlanNode();
        pnode.setWindowFunctionExpression(winExpr);
        OrderByPlanNode onode = new OrderByPlanNode();
        // We need to extract more information from the windowed expression.
        // to construct the output schema.
        List<AbstractExpression> partitionByExpressions = winExpr.getPartitionByExpressions();
        // If the order by expression list contains a partition by expression then
        // we won't have to sort by it twice.  We sort by the partition by expressions
        // first, and we don't care what order we sort by them.  So, find the
        // sort direction in the order by list and use that in the partition by
        // list, and then mark that it was deleted in the order by
        // list.
        //
        // We choose to make this dontsort rather than dosort because the
        // Java default value for boolean is false, and we want to sort by
        // default.
        boolean dontsort[] = new boolean[winExpr.getOrderbySize()];
        List<AbstractExpression> orderByExpressions = winExpr.getOrderByExpressions();
        List<SortDirectionType>  orderByDirections  = winExpr.getOrderByDirections();
        for (int idx = 0; idx < winExpr.getPartitionbySize(); ++idx) {
            SortDirectionType pdir = SortDirectionType.ASC;
            AbstractExpression partitionByExpression = partitionByExpressions.get(idx);
            int sidx = winExpr.getSortIndexOfOrderByExpression(partitionByExpression);
            if (0 <= sidx) {
                pdir = orderByDirections.get(sidx);
                dontsort[sidx] = true;
            }
            onode.addSort(partitionByExpression, pdir);
        }
        for (int idx = 0; idx < winExpr.getOrderbySize(); ++idx) {
            if (!dontsort[idx]) {
                AbstractExpression orderByExpr = orderByExpressions.get(idx);
                SortDirectionType  orderByDir  = orderByDirections.get(idx);
                onode.addSort(orderByExpr, orderByDir);
            }
        }
        onode.addAndLinkChild(root);
        pnode.addAndLinkChild(onode);
        return pnode;
    }

    private AbstractPlanNode handleAggregationOperators(AbstractPlanNode root) {
        /* Check if any aggregate expressions are present */

        /*
         * "Select A from T group by A" is grouped but has no aggregate operator
         * expressions. Catch that case by checking the grouped flag
         */
        if (m_parsedSelect.hasAggregateOrGroupby()) {
            AggregatePlanNode aggNode = null;
            AggregatePlanNode topAggNode = null; // i.e., on the coordinator
            IndexGroupByInfo gbInfo = new IndexGroupByInfo();

            if (root instanceof AbstractReceivePlanNode) {
                // do not apply index scan for serial/partial aggregation
                // for distinct that does not group by partition column
                if ( ! m_parsedSelect.hasAggregateDistinct() ||
                        m_parsedSelect.hasPartitionColumnInGroupby()) {
                    AbstractPlanNode candidate = root.getChild(0).getChild(0);
                    gbInfo.m_multiPartition = true;
                    switchToIndexScanForGroupBy(candidate, gbInfo);
                }
            }
            else if (switchToIndexScanForGroupBy(root, gbInfo)) {
                root = gbInfo.m_indexAccess;
            }
            boolean needHashAgg = gbInfo.needHashAggregator(root, m_parsedSelect);

            // Construct the aggregate nodes
            if (needHashAgg) {
                if ( m_parsedSelect.m_mvFixInfo.needed() ) {
                    // TODO: may optimize this edge case in future
                    aggNode = new HashAggregatePlanNode();
                }
                else {
                    if (gbInfo.isChangedToSerialAggregate()) {
                        assert(root instanceof ReceivePlanNode);
                        aggNode = new AggregatePlanNode();
                    }
                    else if (gbInfo.isChangedToPartialAggregate()) {
                        aggNode = new PartialAggregatePlanNode(gbInfo.m_coveredGroupByColumns);
                    }
                    else {
                        aggNode = new HashAggregatePlanNode();
                    }

                    topAggNode = new HashAggregatePlanNode();
                }
            }
            else {
                aggNode = new AggregatePlanNode();

                if ( ! m_parsedSelect.m_mvFixInfo.needed()) {
                    topAggNode = new AggregatePlanNode();
                }
            }

            int outputColumnIndex = 0;
            NodeSchema agg_schema = new NodeSchema();
            NodeSchema top_agg_schema = new NodeSchema();

            for (ParsedColInfo col : m_parsedSelect.m_aggResultColumns) {
                AbstractExpression rootExpr = col.expression;
                AbstractExpression agg_input_expr = null;
                SchemaColumn schema_col = null;
                SchemaColumn top_schema_col = null;
                if (rootExpr instanceof AggregateExpression) {
                    ExpressionType agg_expression_type = rootExpr.getExpressionType();
                    agg_input_expr = rootExpr.getLeft();

                    // A bit of a hack: ProjectionNodes after the
                    // aggregate node need the output columns here to
                    // contain TupleValueExpressions (effectively on a temp table).
                    // So we construct one based on the output of the
                    // aggregate expression, the column alias provided by HSQL,
                    // and the offset into the output table schema for the
                    // aggregate node that we're computing.
                    // Oh, oh, it's magic, you know..
                    TupleValueExpression tve = new TupleValueExpression(
                            AbstractParsedStmt.TEMP_TABLE_NAME,
                            AbstractParsedStmt.TEMP_TABLE_NAME,
                            "", col.alias,
                            rootExpr, outputColumnIndex);

                    boolean is_distinct = ((AggregateExpression)rootExpr).isDistinct();
                    aggNode.addAggregate(agg_expression_type, is_distinct, outputColumnIndex, agg_input_expr);
                    schema_col = new SchemaColumn(
                            AbstractParsedStmt.TEMP_TABLE_NAME,
                            AbstractParsedStmt.TEMP_TABLE_NAME,
                            "", col.alias,
                            tve);
                    top_schema_col = new SchemaColumn(
                            AbstractParsedStmt.TEMP_TABLE_NAME,
                            AbstractParsedStmt.TEMP_TABLE_NAME,
                            "", col.alias,
                            tve);

                    /*
                     * Special case count(*), count(), sum(), min() and max() to
                     * push them down to each partition. It will do the
                     * push-down if the select columns only contains the listed
                     * aggregate operators and other group-by columns. If the
                     * select columns includes any other aggregates, it will not
                     * do the push-down. - nshi
                     */
                    if (topAggNode != null) {
                        ExpressionType top_expression_type = agg_expression_type;
                        /*
                         * For count(*), count() and sum(), the pushed-down
                         * aggregate node doesn't change. An extra sum()
                         * aggregate node is added to the coordinator to sum up
                         * the numbers from all the partitions. The input schema
                         * and the output schema of the sum() aggregate node is
                         * the same as the output schema of the push-down
                         * aggregate node.
                         *
                         * If DISTINCT is specified, don't do push-down for
                         * count() and sum() when not group by partition column.
                         * An exception is the aggregation arguments are the
                         * partition column (ENG-4980).
                         */
                        if (agg_expression_type == ExpressionType.AGGREGATE_COUNT_STAR ||
                            agg_expression_type == ExpressionType.AGGREGATE_COUNT ||
                            agg_expression_type == ExpressionType.AGGREGATE_SUM) {
                            if (is_distinct &&
                                    ! (m_parsedSelect.hasPartitionColumnInGroupby() ||
                                            canPushDownDistinctAggregation((AggregateExpression)rootExpr) ) ) {
                                topAggNode = null;
                            }
                            else {
                                // for aggregate distinct when group by
                                // partition column, the top aggregate node
                                // will be dropped later, thus there is no
                                // effect to assign the top_expression_type.
                                top_expression_type = ExpressionType.AGGREGATE_SUM;
                            }
                        }
                        /*
                         * For min() and max(), the pushed-down aggregate node
                         * doesn't change. An extra aggregate node of the same
                         * type is added to the coordinator. The input schema
                         * and the output schema of the top aggregate node is
                         * the same as the output schema of the pushed-down
                         * aggregate node.
                         *
                         * APPROX_COUNT_DISTINCT can be similarly pushed down, but
                         * must be split into two different functions, which is
                         * done later, from pushDownAggregate().
                         */
                        else if (agg_expression_type != ExpressionType.AGGREGATE_MIN &&
                                 agg_expression_type != ExpressionType.AGGREGATE_MAX &&
                                 agg_expression_type != ExpressionType.AGGREGATE_APPROX_COUNT_DISTINCT) {
                            /*
                             * Unsupported aggregate for push-down (AVG for example).
                             */
                            topAggNode = null;
                        }

                        if (topAggNode != null) {
                            /*
                             * Input column of the top aggregate node is the
                             * output column of the push-down aggregate node
                             */
                            boolean topDistinctFalse = false;
                            topAggNode.addAggregate(top_expression_type,
                                    topDistinctFalse, outputColumnIndex, tve);
                        }
                    }// end if we have a top agg node
                }
                else {
                    // All complex aggregations have been simplified,
                    // cases like "MAX(counter)+1" or "MAX(col)/MIN(col)"
                    // has already been broken down.
                    assert( ! rootExpr.hasAnySubexpressionOfClass(AggregateExpression.class));

                    /*
                     * These columns are the pass through columns that are not being
                     * aggregated on. These are the ones from the SELECT list. They
                     * MUST already exist in the child node's output. Find them and
                     * add them to the aggregate's output.
                     */
                    schema_col = new SchemaColumn(
                            col.tableName, col.tableAlias,
                            col.columnName, col.alias,
                            col.expression);
                    AbstractExpression topExpr = null;
                    if (col.groupBy) {
                        topExpr = m_parsedSelect.m_groupByExpressions.get(col.alias);
                    }
                    else {
                        topExpr = col.expression;
                    }
                    top_schema_col = new SchemaColumn(
                            col.tableName, col.tableAlias,
                            col.columnName, col.alias, topExpr);
                }

                agg_schema.addColumn(schema_col);
                top_agg_schema.addColumn(top_schema_col);
                outputColumnIndex++;
            }// end for each ParsedColInfo in m_aggResultColumns

            for (ParsedColInfo col : m_parsedSelect.m_groupByColumns) {
                aggNode.addGroupByExpression(col.expression);

                if (topAggNode != null) {
                    topAggNode.addGroupByExpression(m_parsedSelect.m_groupByExpressions.get(col.alias));
                }
            }
            aggNode.setOutputSchema(agg_schema);
            if (topAggNode != null) {
                if (m_parsedSelect.hasComplexGroupby()) {
                    topAggNode.setOutputSchema(top_agg_schema);
                }
                else {
                    topAggNode.setOutputSchema(agg_schema);
                }
            }

            // Never push down aggregation for MV fix case.
            root = pushDownAggregate(root, aggNode, topAggNode, m_parsedSelect);
        }

        return handleDistinctWithGroupby(root);
    }

    // Sets IndexGroupByInfo for an IndexScan
    private void calculateIndexGroupByInfo(IndexScanPlanNode root,
            IndexGroupByInfo gbInfo) {
        String fromTableAlias = root.getTargetTableAlias();
        assert(fromTableAlias != null);

        Index index = root.getCatalogIndex();
        if ( ! IndexType.isScannable(index.getType())) {
            return;
        }

        ArrayList<AbstractExpression> bindings = new ArrayList<>();
        gbInfo.m_coveredGroupByColumns = calculateGroupbyColumnsCovered(
                index, fromTableAlias, bindings);
        gbInfo.m_canBeFullySerialized =
                (gbInfo.m_coveredGroupByColumns.size() ==
                m_parsedSelect.m_groupByColumns.size());
    }

    // Turn sequential scan to index scan for group by if possible
    private AbstractPlanNode indexAccessForGroupByExprs(SeqScanPlanNode root,
            IndexGroupByInfo gbInfo) {
        if (root.isSubQuery()) {
            // sub-query edge case will not be handled now
            return root;
        }

        String fromTableAlias = root.getTargetTableAlias();
        assert(fromTableAlias != null);

        ArrayList<ParsedColInfo> groupBys = m_parsedSelect.m_groupByColumns;
        Table targetTable = m_catalogDb.getTables().get(root.getTargetTableName());
        assert(targetTable != null);
        CatalogMap<Index> allIndexes = targetTable.getIndexes();

        List<Integer> maxCoveredGroupByColumns = new ArrayList<>();
        ArrayList<AbstractExpression> maxCoveredBindings = null;
        Index pickedUpIndex = null;
        boolean foundAllGroupByCoveredIndex = false;

        for (Index index : allIndexes) {
            if ( ! IndexType.isScannable(index.getType())) {
                continue;
            }

            if ( ! index.getPredicatejson().isEmpty()) {
                // do not try to look at Partial/Sparse index
                continue;
            }

            ArrayList<AbstractExpression> bindings = new ArrayList<>();
            List<Integer> coveredGroupByColumns = calculateGroupbyColumnsCovered(
                    index, fromTableAlias, bindings);

            if (coveredGroupByColumns.size() > maxCoveredGroupByColumns.size()) {
                maxCoveredGroupByColumns = coveredGroupByColumns;
                pickedUpIndex = index;
                maxCoveredBindings = bindings;

                if (maxCoveredGroupByColumns.size() == groupBys.size()) {
                    foundAllGroupByCoveredIndex = true;
                    break;
                }
            }
        }
        if (pickedUpIndex == null) {
            return root;
        }

        IndexScanPlanNode indexScanNode = new IndexScanPlanNode(
                root, null, pickedUpIndex, SortDirectionType.INVALID);
        indexScanNode.setForGroupingOnly();
        indexScanNode.setBindings(maxCoveredBindings);

        gbInfo.m_coveredGroupByColumns = maxCoveredGroupByColumns;
        gbInfo.m_canBeFullySerialized = foundAllGroupByCoveredIndex;
        return indexScanNode;
    }

    private List<Integer> calculateGroupbyColumnsCovered(Index index,
            String fromTableAlias,
            List<AbstractExpression> bindings) {
        List<Integer> coveredGroupByColumns = new ArrayList<>();

        ArrayList<ParsedColInfo> groupBys = m_parsedSelect.m_groupByColumns;
        String exprsjson = index.getExpressionsjson();
        if (exprsjson.isEmpty()) {
            List<ColumnRef> indexedColRefs =
                    CatalogUtil.getSortedCatalogItems(index.getColumns(), "index");

            for (int j = 0; j < indexedColRefs.size(); j++) {
                String indexColumnName = indexedColRefs.get(j).getColumn().getName();

                // ignore order of keys in GROUP BY expr
                int ithCovered = 0;
                boolean foundPrefixedColumn = false;
                for (; ithCovered < groupBys.size(); ithCovered++) {
                    AbstractExpression gbExpr = groupBys.get(ithCovered).expression;
                    if ( ! (gbExpr instanceof TupleValueExpression)) {
                        continue;
                    }

                    TupleValueExpression gbTVE = (TupleValueExpression) gbExpr;
                    // TVE column index has not been resolved currently
                    if (fromTableAlias.equals(gbTVE.getTableAlias()) &&
                            indexColumnName.equals(gbTVE.getColumnName())) {
                        foundPrefixedColumn = true;
                        break;
                    }
                }
                if ( ! foundPrefixedColumn) {
                    // no prefix match any more
                    break;
                }

                coveredGroupByColumns.add(ithCovered);

                if (coveredGroupByColumns.size() == groupBys.size()) {
                    // covered all group by columns already
                    break;
                }
            }
        }
        else {
            StmtTableScan fromTableScan = m_parsedSelect.getStmtTableScanByAlias(fromTableAlias);
            // either pure expression index or mix of expressions and simple columns
            List<AbstractExpression> indexedExprs = null;
            try {
                indexedExprs = AbstractExpression.fromJSONArrayString(exprsjson, fromTableScan);
            }
            catch (JSONException e) {
                e.printStackTrace();
                // This case sounds impossible
                return coveredGroupByColumns;
            }

            for (AbstractExpression indexExpr : indexedExprs) {
                // ignore order of keys in GROUP BY expr
                List<AbstractExpression> binding = null;
                for (int ithCovered = 0; ithCovered < groupBys.size(); ithCovered++) {
                    AbstractExpression gbExpr = groupBys.get(ithCovered).expression;
                    binding = gbExpr.bindingToIndexedExpression(indexExpr);
                    if (binding != null) {
                        bindings.addAll(binding);
                        coveredGroupByColumns.add(ithCovered);
                        break;
                    }
                }
                // no prefix match any more or covered all group by columns already
                if (binding == null || coveredGroupByColumns.size() == groupBys.size()) {
                    break;
                }
            }

        }
        return coveredGroupByColumns;
    }

    /**
     * This function is called once it's been determined that we can push down
     * an aggregation plan node.
     *
     * If an APPROX_COUNT_DISTINCT aggregate is distributed, then we need to
     * convert the distributed aggregate function to VALS_TO_HYPERLOGLOG,
     * and the coordinating aggregate function to HYPERLOGLOGS_TO_CARD.
     *
     * @param distNode    The aggregate node executed on each partition
     * @param coordNode   The aggregate node executed on the coordinator
     */
    private static void fixDistributedApproxCountDistinct(
            AggregatePlanNode distNode,
            AggregatePlanNode coordNode) {

        assert (distNode != null);
        assert (coordNode != null);

        // Patch up any APPROX_COUNT_DISTINCT on the distributed node.
        List<ExpressionType> distAggTypes = distNode.getAggregateTypes();
        boolean hasApproxCountDistinct = false;
        for (int i = 0; i < distAggTypes.size(); ++i) {
            ExpressionType et = distAggTypes.get(i);
            if (et == ExpressionType.AGGREGATE_APPROX_COUNT_DISTINCT) {
                hasApproxCountDistinct = true;
                distNode.updateAggregate(i, ExpressionType.AGGREGATE_VALS_TO_HYPERLOGLOG);
            }
        }

        if (hasApproxCountDistinct) {
            // Now, patch up any APPROX_COUNT_DISTINCT on the coordinating node.
            List<ExpressionType> coordAggTypes = coordNode.getAggregateTypes();
            for (int i = 0; i < coordAggTypes.size(); ++i) {
                ExpressionType et = coordAggTypes.get(i);
                if (et == ExpressionType.AGGREGATE_APPROX_COUNT_DISTINCT) {
                    coordNode.updateAggregate(i, ExpressionType.AGGREGATE_HYPERLOGLOGS_TO_CARD);
                }
            }
        }
    }

    /**
     * Push the given aggregate if the plan is distributed, then add the
     * coordinator node on top of the send/receive pair. If the plan
     * is not distributed, or coordNode is not provided, the distNode
     * is added at the top of the plan.
     *
     * Note: this works in part because the push-down node is also an acceptable
     * top level node if the plan is not distributed. This wouldn't be true
     * if we started pushing down something like (sum, count) to calculate
     * a distributed average.  (We already do something like this for
     * APPROX_COUNT_DISTINCT, which must be split into two different functions
     * for the pushed-down case.)
     *
     * @param root
     *            The root node
     * @param distNode
     *            The node to push down
     * @param coordNode [may be null]
     *            The top node to put on top of the send/receive pair after
     *            push-down. If this is null, no push-down will be performed.
     * @return The new root node.
     */
    private static AbstractPlanNode pushDownAggregate(AbstractPlanNode root,
                                       AggregatePlanNode distNode,
                                       AggregatePlanNode coordNode,
                                       ParsedSelectStmt selectStmt) {
        AggregatePlanNode rootAggNode;

        // remember that coordinating aggregation has a pushed-down
        // counterpart deeper in the plan. this allows other operators
        // to be pushed down past the receive as well.
        if (coordNode != null) {
            coordNode.m_isCoordinatingAggregator = true;
        }

        /*
         * Push this node down to partition if it's distributed. First remove
         * the send/receive pair, add the node, then put the send/receive pair
         * back on top of the node, followed by another top node at the
         * coordinator.
         */
        if (coordNode != null && root instanceof ReceivePlanNode) {
            AbstractPlanNode accessPlanTemp = root;
            root = accessPlanTemp.getChild(0).getChild(0);
            root.clearParents();
            accessPlanTemp.getChild(0).clearChildren();
            distNode.addAndLinkChild(root);

            if (selectStmt.hasPartitionColumnInGroupby()) {
                // Set post predicate for final distributed Aggregation node
                distNode.setPostPredicate(selectStmt.getHavingPredicate());

                // Edge case: GROUP BY clause contains the partition column
                // No related GROUP BY or even Re-agg will apply on coordinator
                // Projection plan node can just be pushed down also except for
                // a very edge ORDER BY case.
                if (selectStmt.isComplexOrderBy()) {
                    // Put the send/receive pair back into place
                    accessPlanTemp.getChild(0).addAndLinkChild(distNode);
                    root = processComplexAggProjectionNode(selectStmt, accessPlanTemp);
                    return root;
                }

                root = processComplexAggProjectionNode(selectStmt, distNode);
                // Put the send/receive pair back into place
                accessPlanTemp.getChild(0).addAndLinkChild(root);
                return accessPlanTemp;
            }

            // Without including partition column in GROUP BY clause,
            // there has to be a top GROUP BY plan node on coordinator.
            //
            // Now that we're certain the aggregate will be pushed down
            // (no turning back now!), fix any APPROX_COUNT_DISTINCT aggregates.
            fixDistributedApproxCountDistinct(distNode, coordNode);

            // Put the send/receive pair back into place
            accessPlanTemp.getChild(0).addAndLinkChild(distNode);
            // Add the top node
            coordNode.addAndLinkChild(accessPlanTemp);
            rootAggNode = coordNode;
        }
        else {
            distNode.addAndLinkChild(root);
            rootAggNode = distNode;
        }

        // Set post predicate for final Aggregation node.
        rootAggNode.setPostPredicate(selectStmt.getHavingPredicate());
        root = processComplexAggProjectionNode(selectStmt, rootAggNode);
        return root;
    }

    private static AbstractPlanNode processComplexAggProjectionNode(
            ParsedSelectStmt selectStmt, AbstractPlanNode root) {
        if ( ! selectStmt.hasComplexAgg()) {
            return root;
        }

        ProjectionPlanNode proj =
                new ProjectionPlanNode(selectStmt.getFinalProjectionSchema());
        proj.addAndLinkChild(root);
        return proj;
    }

    /**
     * Check if we can push the limit node down.
     *
     * Return a mid-plan send node, if one exists and can host a
     * distributed limit node.
     * There is guaranteed to be at most a single receive/send pair.
     * Abort the search if a node that a "limit" can't be pushed past
     * is found before its receive node.
     *
     * Can only push past:
     *   * coordinatingAggregator: a distributed aggregator
     *     a copy of which  has already been pushed down.
     *     Distributing a LIMIT to just above that aggregator is correct.
     *     (I've got some doubts that this is correct??? --paul)
     *
     *   * order by: if the plan requires a sort, getNextSelectPlan()
     *     will have already added an ORDER BY.
     *     A distributed LIMIT will be added above a copy
     *     of that ORDER BY node.
     *
     *   * projection: these have no effect on the application of limits.
     *
     * @param root
     * @return If we can push the limit down, the send plan node is returned.
     *         Otherwise null -- when the plan is single-partition when
     *         its "coordinator" part contains a push-blocking node type.
     */
    protected AbstractPlanNode checkLimitPushDownViability(
            AbstractPlanNode root) {
        AbstractPlanNode receiveNode = root;
        List<ParsedColInfo> orderBys = m_parsedSelect.orderByColumns();
        boolean orderByCoversAllGroupBy = m_parsedSelect.groupByIsAnOrderByPermutation();

        while ( ! (receiveNode instanceof ReceivePlanNode)) {

            // Limitation: can only push past some nodes (see above comment)
            // Delete the aggregate node case to handle ENG-6485,
            // or say we don't push down meeting aggregate node
            // TODO: We might want to optimize/push down "limit" for some cases
            if ( ! (receiveNode instanceof OrderByPlanNode) &&
                    ! (receiveNode instanceof ProjectionPlanNode) &&
                    ! isValidAggregateNodeForLimitPushdown(receiveNode,
                            orderBys, orderByCoversAllGroupBy) ) {
                return null;
            }

            if (receiveNode instanceof OrderByPlanNode) {
                // if grouping by the partition key,
                // limit can still push down if ordered by aggregate values.
                if (! m_parsedSelect.hasPartitionColumnInGroupby() &&
                        isOrderByAggregationValue(m_parsedSelect.orderByColumns())) {
                    return null;
                }
            }

            // Traverse...
            if (receiveNode.getChildCount() == 0) {
                return null;
            }

            // nothing that allows pushing past has multiple inputs
            assert(receiveNode.getChildCount() == 1);
            receiveNode = receiveNode.getChild(0);
        }
        return receiveNode.getChild(0);
    }

    private static boolean isOrderByAggregationValue(List<ParsedColInfo> orderBys) {
        for (ParsedColInfo col : orderBys) {
            AbstractExpression rootExpr = col.expression;
            // Fix ENG-3487: can't usually push down limits
            // when results are ordered by aggregate values.
            for (AbstractExpression tve :
                rootExpr.findAllTupleValueSubexpressions()) {
                if (((TupleValueExpression) tve).hasAggregate()) {
                    return true;
                }
            }
        }

        return false;
    }

    private static boolean isValidAggregateNodeForLimitPushdown(
            AbstractPlanNode aggregateNode,
            List<ParsedColInfo> orderBys,
            boolean orderByCoversAllGroupBy) {
        if (aggregateNode instanceof AggregatePlanNode == false) {
            return false;
        }

        if (aggregateNode.getParentCount() == 0) {
            return false;
        }

        // Limitation: can only push past coordinating aggregation nodes
        if ( ! ((AggregatePlanNode)aggregateNode).m_isCoordinatingAggregator) {
            return false;
        }

        AbstractPlanNode parent = aggregateNode.getParent(0);
        AbstractPlanNode orderByNode = null;
        if (parent instanceof OrderByPlanNode) {
            orderByNode = parent;
        }
        else if ( parent instanceof ProjectionPlanNode &&
                parent.getParentCount() > 0 &&
                parent.getParent(0) instanceof OrderByPlanNode) {
            // Xin really wants inline project with aggregation
            orderByNode = parent.getParent(0);
        }

        if (orderByNode == null) {
            // When an aggregate without order by and group by columns
            // does not contain the partition column,
            // the limit should not be pushed down.
            return false;
        }

        if (( ! orderByCoversAllGroupBy) || isOrderByAggregationValue(orderBys)) {
            return false;
        }

        return true;
    }

    /**
     * Handle DISTINCT with GROUP BY if it is not redundant with the
     * aggregation/grouping.
     * DISTINCT is basically rewritten with GROUP BY to benefit from
     * all kinds of GROUP BY optimizations.
     * Trivial case DISTINCT in a statement with no GROUP BY has been
     * rewritten very early at query parsing time.
     * In the non-trivial case, where an existing GROUP BY column is NOT
     * in the select list, DISTINCT can be implemented via a final aggregation
     * (never pushed down) added to the top of the plan.
     * @param root can be an aggregate plan node or projection plan node
     * @return
     */
    private AbstractPlanNode handleDistinctWithGroupby(AbstractPlanNode root) {
        if (! m_parsedSelect.hasDistinctWithGroupBy()) {
            return root;
        }

        assert(m_parsedSelect.isGrouped());

        // DISTINCT is redundant with GROUP BY IFF
        // all of the grouping columns are present in the display columns.
        if (m_parsedSelect.displayColumnsContainAllGroupByColumns()) {
            return root;
        }

        // Now non complex aggregation cases are handled already
        assert(m_parsedSelect.hasComplexAgg());

        AggregatePlanNode distinctAggNode = new HashAggregatePlanNode();
        distinctAggNode.setOutputSchema(m_parsedSelect.getDistinctProjectionSchema());

        for (ParsedColInfo col : m_parsedSelect.m_distinctGroupByColumns) {
            distinctAggNode.addGroupByExpression(col.expression);
        }

        // TODO(xin): push down the DISTINCT for certain cases
        // Ticket: ENG-7360
        /*
        boolean pushedDown = false;
        boolean canPushdownDistinctAgg =
                m_parsedSelect.hasPartitionColumnInDistinctGroupby();
        //
        // disable pushdown, DISTINCT push down turns out complex
        //
        canPushdownDistinctAgg = false;

        if (canPushdownDistinctAgg && !m_parsedSelect.m_mvFixInfo.needed()) {
            assert(m_parsedSelect.hasPartitionColumnInGroupby());
            AbstractPlanNode receive = root;

            if (receive instanceof ReceivePlanNode) {
                // Temporarily strip send/receive pair
                AbstractPlanNode distNode = receive.getChild(0).getChild(0);
                receive.getChild(0).unlinkChild(distNode);

                distinctAggNode.addAndLinkChild(distNode);
                receive.getChild(0).addAndLinkChild(distinctAggNode);

                pushedDown = true;
            }
        }*/

        distinctAggNode.addAndLinkChild(root);
        root = distinctAggNode;

        return root;
    }

    /**
     * Get the unique set of names of all columns that are part of an index on
     * the given table.
     *
     * @param table
     *            The table to build the list of index-affected columns with.
     * @return The set of column names affected by indexes with duplicates
     *         removed.
     */
    private static Set<String> getIndexedColumnSetForTable(Table table) {
        HashSet<String> columns = new HashSet<>();

        for (Index index : table.getIndexes()) {
            for (ColumnRef colRef : index.getColumns()) {
                columns.add(colRef.getColumn().getTypeName());
            }
        }

        return columns;
    }

    String getErrorMessage() {
        return m_recentErrorMsg;
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
     *          If expr rejects nulls introduced by n2 inner table, then
     *              - convert LEFT OUTER n2 to an INNER join.
     *              - convert FULL OUTER n2 to RIGHT OUTER join
     *          If expr rejects nulls introduced by n2 outer table, then
     *              - convert RIGHT OUTER n2 to an INNER join.
     *              - convert FULL OUTER n2 to LEFT OUTER join
     */
    private static void simplifyOuterJoin(BranchNode joinTree) {
        assert(joinTree != null);
        List<AbstractExpression> exprs = new ArrayList<>();
        JoinNode leftNode = joinTree.getLeftNode();
        JoinNode rightNode = joinTree.getRightNode();
        // For the top level node only,
        // WHERE expressions need to be evaluated for NULL-rejection
        if (leftNode.getWhereExpression() != null) {
            exprs.add(leftNode.getWhereExpression());
        }
        if (rightNode.getWhereExpression() != null) {
            exprs.add(rightNode.getWhereExpression());
        }
        simplifyOuterJoinRecursively(joinTree, exprs);
    }

    private static void simplifyOuterJoinRecursively(BranchNode joinNode,
            List<AbstractExpression> exprs) {
        assert (joinNode != null);
        JoinNode leftNode = joinNode.getLeftNode();
        JoinNode rightNode = joinNode.getRightNode();
        if (joinNode.getJoinType() == JoinType.LEFT) {
            // Get all the inner tables underneath this node and
            // see if the expression is NULL-rejecting for any of them
            if (isNullRejecting(rightNode.generateTableJoinOrder(), exprs)) {
                joinNode.setJoinType(JoinType.INNER);
            }
        }
        else if (joinNode.getJoinType() == JoinType.RIGHT) {
            // Get all the outer tables underneath this node and
            // see if the expression is NULL-rejecting for any of them
            if (isNullRejecting(leftNode.generateTableJoinOrder(), exprs)) {
                joinNode.setJoinType(JoinType.INNER);
            }
        }
        else if (joinNode.getJoinType() == JoinType.FULL) {
            // Get all the outer tables underneath this node and
            // see if the expression is NULL-rejecting for any of them
            if (isNullRejecting(leftNode.generateTableJoinOrder(), exprs)) {
                joinNode.setJoinType(JoinType.LEFT);
            }
            // Get all the inner tables underneath this node and
            // see if the expression is NULL-rejecting for any of them
            if (isNullRejecting(rightNode.generateTableJoinOrder(), exprs)) {
                if (JoinType.FULL == joinNode.getJoinType()) {
                    joinNode.setJoinType(JoinType.RIGHT);
                }
                else {
                    // LEFT join was just removed
                    joinNode.setJoinType(JoinType.INNER);
                }
            }
        }

        // Now add this node expression to the list and descend.
        // The WHERE expressions can be combined with the input list
        // because they simplify both inner and outer nodes.
        if (leftNode.getWhereExpression() != null) {
            exprs.add(leftNode.getWhereExpression());
        }
        if (rightNode.getWhereExpression() != null) {
            exprs.add(rightNode.getWhereExpression());
        }

        // The JOIN expressions (ON) are only applicable
        // to the INNER node of an outer join.
        List<AbstractExpression> exprsForInnerNode = new ArrayList<>(exprs);
        if (leftNode.getJoinExpression() != null) {
            exprsForInnerNode.add(leftNode.getJoinExpression());
        }
        if (rightNode.getJoinExpression() != null) {
            exprsForInnerNode.add(rightNode.getJoinExpression());
        }

        List<AbstractExpression> leftNodeExprs;
        List<AbstractExpression> rightNodeExprs;
        switch (joinNode.getJoinType()) {
            case INNER:
                leftNodeExprs = exprsForInnerNode;
                rightNodeExprs = exprsForInnerNode;
                break;
            case LEFT:
                leftNodeExprs = exprs;
                rightNodeExprs = exprsForInnerNode;
                break;
            case RIGHT:
                leftNodeExprs = exprsForInnerNode;
                rightNodeExprs = exprs;
                break;
            case FULL:
                leftNodeExprs = exprs;
                rightNodeExprs = exprs;
                break;
            default:
                // shouldn't get there
                leftNodeExprs = null;
                rightNodeExprs = null;
                assert(false);
        }
        if (leftNode instanceof BranchNode) {
            simplifyOuterJoinRecursively((BranchNode)leftNode, leftNodeExprs);
        }
        if (rightNode instanceof BranchNode) {
            simplifyOuterJoinRecursively((BranchNode)rightNode, rightNodeExprs);
        }
    }

    /**
     * Verify if an expression from the input list is NULL-rejecting
     * for any of the tables from the list
     * @param tableAliases list of tables
     * @param exprs list of expressions
     * @return TRUE if there is a NULL-rejecting expression
     */
    private static boolean isNullRejecting(Collection<String> tableAliases,
            List<AbstractExpression> exprs) {
        for (AbstractExpression expr : exprs) {
            for (String tableAlias : tableAliases) {
                if (ExpressionUtil.isNullRejectingExpression(expr, tableAlias)) {
                    // We are done at this level
                    return true;
                }
            }
        }
        return false;
    }

}
