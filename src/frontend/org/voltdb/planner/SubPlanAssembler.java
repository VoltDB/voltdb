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

import com.google_voltpatches.common.base.Preconditions;
import org.hsqldb_voltpatches.FunctionForVoltDB;
import org.json_voltpatches.JSONException;
import org.voltcore.utils.Pair;
import org.voltdb.VoltType;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.ColumnRef;
import org.voltdb.catalog.Index;
import org.voltdb.exceptions.PlanningErrorException;
import org.voltdb.expressions.AbstractExpression;
import org.voltdb.expressions.AbstractSubqueryExpression;
import org.voltdb.expressions.ComparisonExpression;
import org.voltdb.expressions.ConstantValueExpression;
import org.voltdb.expressions.ExpressionUtil;
import org.voltdb.expressions.FunctionExpression;
import org.voltdb.expressions.OperatorExpression;
import org.voltdb.expressions.ParameterValueExpression;
import org.voltdb.expressions.TupleValueExpression;
import org.voltdb.expressions.VectorValueExpression;
import org.voltdb.planner.parseinfo.JoinNode;
import org.voltdb.planner.parseinfo.StmtTableScan;
import org.voltdb.planner.parseinfo.StmtTargetTableScan;
import org.voltdb.plannodes.IndexSortablePlanNode;
import org.voltdb.plannodes.AbstractPlanNode;
import org.voltdb.plannodes.AbstractScanPlanNode;
import org.voltdb.plannodes.IndexScanPlanNode;
import org.voltdb.plannodes.IndexUseForOrderBy;
import org.voltdb.plannodes.MaterializedScanPlanNode;
import org.voltdb.plannodes.NestLoopIndexPlanNode;
import org.voltdb.plannodes.ReceivePlanNode;
import org.voltdb.plannodes.SendPlanNode;
import org.voltdb.plannodes.SeqScanPlanNode;
import org.voltdb.types.ExpressionType;
import org.voltdb.types.IndexLookupType;
import org.voltdb.types.IndexType;
import org.voltdb.types.JoinType;
import org.voltdb.types.SortDirectionType;
import org.voltdb.utils.CatalogUtil;

public abstract class SubPlanAssembler {

    /** The parsed statement structure that has the table and predicate info we need. */
    final AbstractParsedStmt m_parsedStmt;

    /** Describes the specified and inferred partition context. */
    final StatementPartitioning m_partitioning;

    /**
     * A description of a possible error condition that is considered recoverable/recovered
     * by the act of generating a viable alternative plan. The error should only be acknowledged
     * if it contributed to the complete failure to plan the statement.
     */
    String m_recentErrorMsg;

    // Constants to specify how getIndexableExpressionFromFilters should react
    // to finding a filter that matches the current criteria.
    /// For some calls, primarily related to index-based filtering,
    /// the matched filter is going to be implemented by indexing,
    /// so it needs to be "consumed" (removed from the list)
    /// to not get redundantly applied as a post-condition.
    private final static boolean EXCLUDE_FROM_POST_FILTERS = true;
    /// For other calls, related to index-based ordering,
    /// the matched filter must remain in the list
    /// to eventually be applied as a post-filter.
    private final static boolean KEEP_IN_POST_FILTERS = false;

    SubPlanAssembler(AbstractParsedStmt parsedStmt, StatementPartitioning partitioning) {
        m_parsedStmt = parsedStmt;
        m_partitioning = partitioning;
    }

    /**
     * Used to mark that current plan cannot be generated, but nevertheless should not abort
     * further plan generation.
     * See how it affects plan generation in PlanAssembler#getBestCostPlan().
     */
    public static final class SkipCurrentPlanException extends RuntimeException {
        public SkipCurrentPlanException() {
            super();
        }
    }

    /**
     * Called repeatedly to iterate through possible embedable select plans.
     * Returns null when no more plans exist.
     *
     * @return The next plan to solve the subselect or null if no more plans.
     */
    abstract AbstractPlanNode nextPlan();

    /**
     * Generate all possible access paths for given sets of join and filter
     * expressions for a table.
     * The list includes the naive (scan) pass and possible index scans
     *
     * @param tableScan Table to generate access path for
     * @param joinExprs join expressions this table is part of
     * @param filterExprs filter expressions this table is part of
     * @param postExprs post expressions this table is part of
     * @return List of valid access paths
     */
    List<AccessPath> getRelevantAccessPathsForTable(
            StmtTableScan tableScan, List<AbstractExpression> joinExprs,
            List<AbstractExpression> filterExprs, List<AbstractExpression> postExprs) {
        final List<AccessPath> paths = new ArrayList<>();
        final List<AbstractExpression> allJoinExprs = new ArrayList<>();
        final List<AbstractExpression> allExprs = new ArrayList<>();
        // add the empty seq-scan access path
        if (joinExprs != null) {
            allExprs.addAll(joinExprs);
            allJoinExprs.addAll(joinExprs);
        }
        if (postExprs != null) {
            allJoinExprs.addAll(postExprs);
        }
        if (filterExprs != null) {
            allExprs.addAll(filterExprs);
        }
        paths.add(getRelevantNaivePath(allJoinExprs, filterExprs));
        for (Index index : tableScan.getIndexes()) {
            AccessPath path = getRelevantAccessPathForIndex(tableScan, allExprs, index);
            // Process the index WHERE clause into a list of anded
            // sub-expressions and process each sub-expression, searching the
            // query (or matview) WHERE clause for an expression to cover each
            // of them. Coverage can be in the form of an identical filter or
            // a more restrictive filter. Specifically, comparison filters like
            // "X > 1" cover the index predicate filter "X is not null" but are
            // not an exact match. As such, they are not completely optimized
            // out by use of the partial index.
            // The partial index's WHERE sub-expressions must be covered to
            // allow ANY use of the index.
            // Of the covering sub-expressions (from the query or view),
            // those that also EXACTLY match their covered index sub-expression
            // are tracked in exactMatchCoveringExprs so that they
            // can be eliminated from the post-filter expressions.
            final List<AbstractExpression> exactMatchCoveringExprs = new ArrayList<>();
            String predicatejson = index.getPredicatejson();
            Pair<Boolean, AbstractExpression> partialIndexInfo =
                    evaluatePartialIndexPredicate(
                            tableScan, allExprs,
                            predicatejson, exactMatchCoveringExprs);
            boolean hasCoveredPredicate = partialIndexInfo.getFirst();
            if (path == null) {
                if (partialIndexInfo.getSecond() == null || ! hasCoveredPredicate) {
                    // Skip the uselessly irrelevant whole-table index.
                    // Skip the index with the inapplicable predicate.
                    continue;
                }
                // The partial index with a covered predicate can be used
                // solely to eliminate a post-filter or even just to reduce the
                // number of post-filtered tuples,
                // even though its indexed columns are irrelevant -- so
                // getRelevantAccessPathForIndex did not return a valid path.
                // Override the path for a forward scan of the entire partial
                // index.  The irrelevant keys of the index are ignored.
                path = getRelevantNaivePath(allJoinExprs, filterExprs);
                path.index = index;
                path.lookupType = IndexLookupType.GTE;
                path.setPartialIndexExpression(partialIndexInfo.getSecond());
            } else {
                assert(path.index != null);
                assert(path.index == index);
                // This index on relevant column(s) may need to be rejected if
                // its predicate is not applicable.
                if ( partialIndexInfo.getSecond() != null) {
                    if ( ! hasCoveredPredicate) {
                        // Skip the index with the inapplicable predicate.
                        continue;
                    }
                    path.setPartialIndexExpression(partialIndexInfo.getSecond());
                }
            }

            if (hasCoveredPredicate) {
                filterPostPredicateForPartialIndex(path, exactMatchCoveringExprs);
            }
            if (postExprs != null) {
                path.joinExprs.addAll(postExprs);
            }
            paths.add(path);
        }
        return paths;
    }

    /**
     * Process the partial index WHERE clause into a list of anded
     * sub-expressions and process each sub-expression, searching the
     * query (or matview) WHERE clause for an expression to cover each
     * of them. Coverage can be in the form of an identical filter or
     * a more restrictive filter. Specifically, comparison filters like
     * "X > 1" cover the index predicate filter "X is not null" but are
     * not an exact match. As such, they are not completely optimized
     * out by use of the partial index.
     * The partial index's WHERE sub-expressions must be covered to
     * allow ANY use of the index.
     * Of the covering sub-expressions (from the query or view),
     * those that also EXACTLY match their covered index sub-expression
     * are tracked in exactMatchCoveringExprs so that they
     * can be eliminated from the post-filter expressions.
     *
     * @param index
     * @param tableScan
     * @param path
     * @param allExprs
     * @param allJoinExprs
     * @param filterExprs
     * @return
     */
    public static AccessPath verifyIfPartialIndex(
            Index index, StmtTableScan tableScan, AccessPath path, Collection<AbstractExpression> allExprs,
            Collection<AbstractExpression> allJoinExprs, Collection<AbstractExpression> filterExprs) {
        final List<AbstractExpression> exactMatchCoveringExprs = new ArrayList<>();
        final String predicatejson = index.getPredicatejson();
        Pair<Boolean, AbstractExpression> partialIndexInfo =
                evaluatePartialIndexPredicate(tableScan, allExprs, predicatejson, exactMatchCoveringExprs);
        boolean hasCoveredPredicate = partialIndexInfo.getFirst();
        if (path == null) {
            if (partialIndexInfo.getSecond() == null || ! hasCoveredPredicate) {
                // Skip the uselessly irrelevant whole-table index.
                // Skip the index with the inapplicable predicate.
                return null;
            }
            // The partial index with a covered predicate can be used
            // solely to eliminate a post-filter or even just to reduce the
            // number of post-filtered tuples,
            // even though its indexed columns are irrelevant -- so
            // getRelevantAccessPathForIndex did not return a valid path.
            // Override the path for a forward scan of the entire partial
            // index.  The irrelevant keys of the index are ignored.
            path = getRelevantNaivePath(allJoinExprs, filterExprs);
            path.index = index;
            path.lookupType = IndexLookupType.GTE;
        } else {
            assert path.index == index;
            // This index on relevant column(s) may need to be rejected if
            // its predicate is not applicable.
            if (partialIndexInfo.getSecond() != null && ! hasCoveredPredicate) {
                // Skip the index with the inapplicable predicate.
                return null;
            }
        }

        path.setPartialIndexExpression(partialIndexInfo.getSecond());

        if (hasCoveredPredicate) {
            filterPostPredicateForPartialIndex(path, exactMatchCoveringExprs);
        }
        return path;
    }

    /**
     * Add access path details to an index scan.
     *
     * @param scanNode Initial index scan plan.
     * @param path The access path to access the data in the table (index/scan/etc).
     * @param tableIdx - 1 if a scan is an inner scan of the NJIJ. 0 otherwise.
     * @return An index scan plan node OR,
               in one edge case, an NLIJ of a MaterializedScan and an index scan plan node.
     */
    public static AbstractPlanNode buildIndexAccessPlanForTable(IndexScanPlanNode scanNode,
            AccessPath path, int tableIdx) {
        return buildIndexAccessPlanForTable(null, scanNode, path, tableIdx);
    }

    private static AbstractPlanNode buildIndexAccessPlanForTable(StmtTableScan tableScan,
            AccessPath path) {
        return buildIndexAccessPlanForTable(tableScan,
                new IndexScanPlanNode(tableScan, path.index), path, 0);
    }

    private static AbstractPlanNode buildIndexAccessPlanForTable(StmtTableScan tableScan,
            IndexScanPlanNode scanNode, AccessPath path, int tableIdx) {
        AbstractPlanNode resultNode = scanNode;
        // set sortDirection here because it might be used for IN list
        scanNode.setSortDirection(path.sortDirection);
        // Build the list of search-keys for the index in question
        // They are the rhs expressions of normalized indexExpr comparisons
        // except for geo indexes. For geo indexes, the search key is directly
        // the one element of indexExprs.
        for (AbstractExpression expr : path.indexExprs) {
            if (path.lookupType == IndexLookupType.GEO_CONTAINS) {
                scanNode.addSearchKeyExpression(expr);
                scanNode.addCompareNotDistinctFlag(false);
                continue;
            }
            AbstractExpression exprRightChild = expr.getRight();
            assert exprRightChild != null;
            if (expr.getExpressionType() == ExpressionType.COMPARE_IN) {
                // Replace this method's result with an injected NLIJ.
                resultNode = injectIndexedJoinWithMaterializedScan(exprRightChild, scanNode);
                // Extract a TVE from the LHS MaterializedScan for use by the IndexScan in its new role.
                MaterializedScanPlanNode matscan = (MaterializedScanPlanNode)resultNode.getChild(0);
                AbstractExpression elemExpr = matscan.getOutputExpression();
                assert elemExpr != null;
                // Replace the IN LIST condition in the end expression referencing all the list elements
                // with a more efficient equality filter referencing the TVE for each element in turn.
                replaceInListFilterWithEqualityFilter(path.endExprs, exprRightChild, elemExpr);
                // Set up the similar VectorValue --> TVE replacement of the search key expression.
                exprRightChild = elemExpr;
            }
            // The AbstractSubqueryExpression must be wrapped up into a
            // ScalarValueExpression which extracts the actual row/column from
            // the subquery
            // ENG-8175: this part of code seems not working for float/varchar type index ?!
            // DEAD CODE with the guards on index: ENG-8203
            Preconditions.checkState(! (exprRightChild instanceof AbstractSubqueryExpression));
            scanNode.addSearchKeyExpression(exprRightChild);
            // If the index expression is an "IS NOT DISTINCT FROM" comparison, let the NULL values go through. (ENG-11096)
            scanNode.addCompareNotDistinctFlag(expr.getExpressionType() == ExpressionType.COMPARE_NOTDISTINCT);
        }
        // create the IndexScanNode with all its metadata
        scanNode.setLookupType(path.lookupType);
        scanNode.setBindings(path.bindings);
        scanNode.setEndExpression(ExpressionUtil.combinePredicates(ExpressionType.CONJUNCTION_AND, path.endExprs));
        if (tableScan != null && ! path.index.getPredicatejson().isEmpty()) {
            try {
                scanNode.setPartialIndexPredicate(
                        AbstractExpression.fromJSONString(path.index.getPredicatejson(), tableScan));
            } catch (JSONException e) {
                throw new PlanningErrorException(e.getMessage(), 0);
            }
        }
        scanNode.setPredicate(path.otherExprs);
        // Propagate the sorting information
        // into the scan node from the access path.
        // The initial expression is needed to control a (short?) forward scan to adjust the start of a reverse
        // iteration after it had to initially settle for starting at "greater than a prefix key".
        scanNode.setInitialExpression(ExpressionUtil.combinePredicates(ExpressionType.CONJUNCTION_AND, path.initialExpr));
        scanNode.setSkipNullPredicate(tableIdx < 0 ? 0 : tableIdx);
        scanNode.setEliminatedPostFilters(path.eliminatedPostExprs);
        IndexUseForOrderBy indexUse = scanNode.indexUse();
        indexUse.setWindowFunctionUsesIndex(path.m_windowFunctionUsesIndex);
        indexUse.setSortOrderFromIndexScan(path.sortDirection);
        indexUse.setWindowFunctionIsCompatibleWithOrderBy(path.m_stmtOrderByIsCompatible);
        indexUse.setFinalExpressionOrderFromIndexScan(path.m_finalExpressionOrder);
        return resultNode;
    }

    /**
     * Generate the naive (scan) pass given a join and filter expressions
     *
     * @param joinExprs join expressions
     * @param filterExprs filter expressions
     * @return Naive access path
     */
    static AccessPath getRelevantNaivePath(
            Collection<AbstractExpression> joinExprs, Collection<AbstractExpression> filterExprs) {
        final AccessPath naivePath = new AccessPath();
        if (filterExprs != null) {
            naivePath.otherExprs.addAll(filterExprs);
        }
        if (joinExprs != null) {
            naivePath.joinExprs.addAll(joinExprs);
        }
        return naivePath;
    }

    /**
     * A utility class for returning the results of a match between an indexed
     * expression and a query filter expression that uses it in some form in
     * some useful fashion.
     * The "form" may be an exact match for the expression or some allowed parameterized variant.
     * The "fashion" may be in an equality or range comparison opposite something that can be
     * treated as a (sub)scan-time constant.
     */
    private static class IndexableExpression {
        // The matched expression, in its original form and
        // normalized so that its LHS is the part that matched the indexed expression.
        private final AbstractExpression m_originalFilter;
        private final ComparisonExpression m_filter;
        // The parameters, if any, that must be bound to enable use of the index
        // -- these have no effect on the current query,
        // but they effect the applicability of the resulting cached plan to other queries.
        private final List<AbstractExpression> m_bindings;

        public IndexableExpression(AbstractExpression originalExpr, ComparisonExpression normalizedExpr,
                                   List<AbstractExpression> bindings) {
            m_originalFilter = originalExpr;
            m_filter = normalizedExpr;
            m_bindings = bindings;
        }

        public AbstractExpression getOriginalFilter() { return m_originalFilter; }
        public AbstractExpression getFilter() { return m_filter; }
        public List<AbstractExpression> getBindings() { return m_bindings; }

        public IndexableExpression extractStartFromPrefixLike() {
            ComparisonExpression gteFilter = m_filter.getGteFilterFromPrefixLike();
            return new IndexableExpression(null, gteFilter, m_bindings);
        }

        public IndexableExpression extractEndFromPrefixLike() {
            ComparisonExpression ltFilter = m_filter.getLtFilterFromPrefixLike();
            return new IndexableExpression(null, ltFilter, m_bindings);
        }
    }

    /**
     * Split the index WHERE clause into a list of sub-expressions and process
     * each expression separately searching the query (or matview) for a
     * covering expression for each of these expressions.
     * All index WHERE sub-expressions must be covered to enable the index.
     * Collect the query expressions that EXACTLY match the index expression.
     * They can be eliminated from the post-filters as an optimization.
     *
     * @param tableScan The source table.
     * @param coveringExprs The set of query predicate expressions.
     * @param index The partial index to cover.
     * @param exactMatchCoveringExprs The output subset of the query predicates that EXACTLY match the
     *        index predicate expression(s)
     * @return Pair<Boolean, AbstractExpression>
     *         TRUE if the index has a predicate that is completely covered by the query expressions.
     *         Partial Predicate or NULL
     */
    public static Pair<Boolean, AbstractExpression> evaluatePartialIndexPredicate(
            StmtTableScan tableScan, Collection<AbstractExpression> coveringExprs,
            String predicatejson, List<AbstractExpression> exactMatchCoveringExprs) {
        if (predicatejson.isEmpty()) {
            // Skip the uselessly irrelevant whole-table index.
            return Pair.of(false,  null);
        }
        final AbstractExpression indexPredicate;
        try {
            indexPredicate = AbstractExpression.fromJSONString(predicatejson, tableScan);
        } catch (JSONException e) {
            throw new PlanningErrorException(e);
        }
        final List<AbstractExpression> exprsToCover = ExpressionUtil.uncombinePredicate(indexPredicate);
        for (AbstractExpression coveringExpr : coveringExprs) {
            if (exprsToCover.isEmpty()) {
                // We are done there. All the index predicate expressions are covered.
                break;
            }
            // Each covering expression and its reversed copy need to be tested for the index expression coverage.
            AbstractExpression reversedCoveringExpr = null;
            final ExpressionType reverseCoveringType = ComparisonExpression.reverses.get(coveringExpr.getExpressionType());
            if (reverseCoveringType != null) {
                // reverse the expression
                reversedCoveringExpr = new ComparisonExpression(
                        reverseCoveringType, coveringExpr.getRight(), coveringExpr.getLeft());
            }
            // Exact match first.
            if (removeExactMatchCoveredExpressions(coveringExpr, exprsToCover)) {
                exactMatchCoveringExprs.add(coveringExpr);
            }
            // Try the reversed expression for the exact match
            if (reversedCoveringExpr != null && removeExactMatchCoveredExpressions(reversedCoveringExpr, exprsToCover)) {
                // It is the original expression that we need to remember
                exactMatchCoveringExprs.add(coveringExpr);
            }
        }

        // Handle the remaining NOT NULL index predicate expressions that can be covered by NULL rejecting expressions
        // All index predicate expressions must be covered for index to be selected
        boolean isIndexPredicateCovered = removeNotNullCoveredExpressions(tableScan, coveringExprs, exprsToCover).isEmpty();
        return Pair.of(isIndexPredicateCovered, indexPredicate);
    }

    private static final class AccessPathLoopHelper {
        private final int m_nSpoilers;
        private final int[] m_orderSpoilers;
        private final int[] m_indexedColIds;
        private final StmtTableScan m_tableScan;
        private final AccessPath m_accessPath;
        private final List<AbstractExpression> m_indexedExpressions;
        private final List<AbstractExpression> m_filtersToCover;

        private int m_coveredCount = 0;
        private int m_coveringColId = -1;
        private AbstractExpression m_coveringExpression = null;
        private IndexableExpression m_inListExpr = null;
        private int m_nRecoveredSpoilers = 0;
        AccessPathLoopHelper(int nSpoilers, int keyComponentCount, int[] orderSpoilers, int[] indexedColIds,
                             StmtTableScan tblScan, AccessPath accessPath, List<AbstractExpression> indexedExprs,
                             List<AbstractExpression> filtersToCover) {
            m_nSpoilers = nSpoilers;
            m_orderSpoilers = orderSpoilers;
            m_indexedColIds = indexedColIds;
            m_tableScan = tblScan;
            m_accessPath = accessPath;
            m_indexedExpressions = indexedExprs;
            m_filtersToCover = filtersToCover;
            while(m_coveredCount < keyComponentCount && ! filtersToCover.isEmpty() && iterate()) {
                ++m_coveredCount;
            }
        }
        boolean iterate() {
            if (m_indexedExpressions == null) {
                m_coveringColId = m_indexedColIds[m_coveredCount];
            } else {
                m_coveringExpression = m_indexedExpressions.get(m_coveredCount);
            }
            IndexableExpression eqExpr = getIndexableExpressionFromFilters(
                    // NOT DISTINCT can be also considered as an equality comparison.
                    // The only difference is that NULL is not distinct from NULL, but NULL != NULL. (ENG-11096)
                    ExpressionType.COMPARE_EQUAL, ExpressionType.COMPARE_NOTDISTINCT,
                    m_coveringExpression, m_coveringColId, m_tableScan, m_filtersToCover,
                    m_inListExpr == null/* Equality filters get first priority*/, EXCLUDE_FROM_POST_FILTERS);

            if (eqExpr == null) {
                // For now, an IN LIST can only be indexed if any other indexed filters are based
                // solely on constants or parameters vs. other tables' columns or other IN LISTS.
                // Otherwise, there would need to be a three-way NLIJ implementation joining the
                // MaterializedScan, the source table of the other key component values,
                // and the indexed table.
                // So, only the first IN LIST filter matching a key component is considered.
                if (m_inListExpr == null) {
                    // Also, it can not be considered if there was a prior key component that has an
                    // equality filter that is based on another table.
                    // Accepting an IN LIST filter implies rejecting later any filters based on other
                    // tables' columns.
                    m_inListExpr = getIndexableExpressionFromFilters(
                            ExpressionType.COMPARE_IN, ExpressionType.COMPARE_IN,
                            m_coveringExpression, m_coveringColId, m_tableScan, m_filtersToCover,
                            false, EXCLUDE_FROM_POST_FILTERS);
                    if (m_inListExpr != null) {
                        // Make sure all prior key component equality filters
                        // were based on constants and/or parameters.
                        for (AbstractExpression eq_comparator : m_accessPath.indexExprs) {
                            AbstractExpression otherExpr = eq_comparator.getRight();
                            if (otherExpr.hasTupleValueSubexpression()) {
                                // Can't index this IN LIST filter without some kind of three-way NLIJ,
                                // so, add it to the post-filters.
                                AbstractExpression in_list_comparator = m_inListExpr.getOriginalFilter();
                                m_accessPath.otherExprs.add(in_list_comparator);
                                m_inListExpr = null;
                                return false;
                            }
                        }
                        eqExpr = m_inListExpr;
                    }
                }
                if (eqExpr == null) {
                    return false;
                }
            }
            final AbstractExpression comparator = eqExpr.getFilter();
            m_accessPath.indexExprs.add(comparator);
            m_accessPath.bindings.addAll(eqExpr.getBindings());
            // A non-empty endExprs has the later side effect of invalidating descending sort order
            // in all cases except the edge case of full coverage equality comparison.
            // Even that case must be further qualified to exclude indexed IN-LIST
            // unless/until the MaterializedScan can be configured to iterate in descending order
            // (vs. always ascending).
            // In the case of the IN LIST expression, both the search key and the end condition need
            // to be rewritten to enforce equality in turn with each list element "row" produced by
            // the MaterializedScan. This happens in buildIndexAccessPlanForTable.
            m_accessPath.endExprs.add(comparator);

            // If a provisional sort direction has been determined, the equality filter MAY confirm
            // that a "spoiler" index key component (one missing from the ORDER BY) is constant-valued
            // and so it can not spoil the scan result sort order established by other key components.
            // In this case, consider the spoiler recovered.
            if (m_nRecoveredSpoilers < m_nSpoilers && m_orderSpoilers[m_nRecoveredSpoilers] == m_coveredCount &&
                    // In the case of IN-LIST equality, the key component will not have a constant value.
                    eqExpr != m_inListExpr) {
                // One recovery closer to confirming the sort order.
                ++m_nRecoveredSpoilers;
            }
            return true;
        }
        IndexableExpression getIndexableExpression() {
            return m_inListExpr;
        }
        AbstractExpression getCoveringExpression() {
            return m_coveringExpression;
        }
        int getCoveredCount() {
            return m_coveredCount;
        }
        int getCoveringColId() {
            return m_coveringColId;
        }
        int getNumRecoveredSpoilers() {
            return m_nRecoveredSpoilers;
        }
    }

    private static class CoveringFilterProcessor {
        private final AccessPath m_accessPath;
        private final AbstractExpression m_coveringExpression;
        private final boolean m_withoutInListExpression;
        private final int m_coveringColId;
        private final StmtTableScan m_tableScan;
        private final List<AbstractExpression> m_filtersToCover;
        private IndexableExpression m_startingBoundExpr = null;
        private IndexableExpression m_endingBoundExpr = null;
        CoveringFilterProcessor(
                AccessPath access, AbstractExpression coveringExpression, List<AbstractExpression> filtersToCover,
                int coveringColId, boolean hasInListExpr, StmtTableScan tblScan) {
            m_accessPath = access;
            m_coveringExpression = coveringExpression;
            m_withoutInListExpression = hasInListExpr;
            m_tableScan = tblScan;
            m_filtersToCover = filtersToCover;
            m_coveringColId = coveringColId;
            process();
        }
        IndexableExpression getStartingBoundExpression() {
            return m_startingBoundExpr;
        }
        IndexableExpression getEndingBoundExpression() {
            return m_endingBoundExpr;
        }
        void process() {
            // A scannable index allows inequality matches, but only on the first key component
            // missing a usable equality comparator.

            // Look for a double-ended bound on it.
            // This is always the result of an edge case:
            // "indexed-general-expression LIKE prefix-constant".
            // The simpler case "column LIKE prefix-constant"
            // has already been re-written by the HSQL parser
            // into separate upper and lower bound inequalities.
            final IndexableExpression doubleBoundExpr = getIndexableExpressionFromFilters(
                    ExpressionType.COMPARE_LIKE, ExpressionType.COMPARE_LIKE,
                    m_coveringExpression, m_coveringColId, m_tableScan, m_filtersToCover,
                    false, EXCLUDE_FROM_POST_FILTERS);
            // For simplicity of implementation:
            // In some odd edge cases e.g.
            // " FIELD(DOC, 'title') LIKE 'a%' AND FIELD(DOC, 'title') > 'az' ",
            // arbitrarily choose to index-optimize the LIKE expression rather than the inequality
            // ON THAT SAME COLUMN.
            // This MIGHT not always provide the most selective filtering.
            if (doubleBoundExpr != null) {
                m_startingBoundExpr = doubleBoundExpr.extractStartFromPrefixLike();
                m_endingBoundExpr = doubleBoundExpr.extractEndFromPrefixLike();
            } else {
                final boolean allowIndexedJoinFilters = m_withoutInListExpression;
                // Look for a lower bound.
                m_startingBoundExpr = getIndexableExpressionFromFilters(
                        ExpressionType.COMPARE_GREATERTHAN, ExpressionType.COMPARE_GREATERTHANOREQUALTO,
                        m_coveringExpression, m_coveringColId, m_tableScan, m_filtersToCover,
                        allowIndexedJoinFilters, EXCLUDE_FROM_POST_FILTERS);

                // Look for an upper bound.
                m_endingBoundExpr = getIndexableExpressionFromFilters(
                        ExpressionType.COMPARE_LESSTHAN, ExpressionType.COMPARE_LESSTHANOREQUALTO,
                        m_coveringExpression, m_coveringColId, m_tableScan, m_filtersToCover,
                        allowIndexedJoinFilters, EXCLUDE_FROM_POST_FILTERS);
            }
            if (m_startingBoundExpr != null) {
                final AbstractExpression lowerBoundExpr = m_startingBoundExpr.getFilter();
                m_accessPath.indexExprs.add(lowerBoundExpr);
                m_accessPath.bindings.addAll(m_startingBoundExpr.getBindings());
                if (lowerBoundExpr.getExpressionType() == ExpressionType.COMPARE_GREATERTHAN) {
                    m_accessPath.lookupType = IndexLookupType.GT;
                } else {
                    assert lowerBoundExpr.getExpressionType() == ExpressionType.COMPARE_GREATERTHANOREQUALTO;
                    m_accessPath.lookupType = IndexLookupType.GTE;
                }
                m_accessPath.use = IndexUseType.INDEX_SCAN;
            }

            if (m_endingBoundExpr != null) {
                final AbstractExpression upperBoundComparator = m_endingBoundExpr.getFilter();
                m_accessPath.use = IndexUseType.INDEX_SCAN;
                m_accessPath.bindings.addAll(m_endingBoundExpr.getBindings());

                // if we already have a lower bound, or the sorting direction is already determined
                // do not do the reverse scan optimization
                if (m_accessPath.sortDirection != SortDirectionType.DESC &&
                        (m_startingBoundExpr != null || m_accessPath.sortDirection == SortDirectionType.ASC)) {
                    m_accessPath.endExprs.add(upperBoundComparator);
                    if (m_accessPath.lookupType == IndexLookupType.EQ) {
                        m_accessPath.lookupType = IndexLookupType.GTE;
                    }
                } else {
                    // Optimizable to use reverse scan.
                    // only do reverse scan optimization when no lowerBoundExpr and lookup type is either < or <=.
                    if (upperBoundComparator.getExpressionType() == ExpressionType.COMPARE_LESSTHAN) {
                        m_accessPath.lookupType = IndexLookupType.LT;
                    } else {
                        assert upperBoundComparator.getExpressionType() == ExpressionType.COMPARE_LESSTHANOREQUALTO;
                        m_accessPath.lookupType = IndexLookupType.LTE;
                    }
                    // Unlike a lower bound, an upper bound does not automatically filter out nulls
                    // as required by the comparison filter, so construct a NOT NULL comparator and
                    // add to post-filter
                    // TODO: Implement an abstract isNullable() method on AbstractExpression and use
                    // that here to optimize out the "NOT NULL" comparator for NOT NULL columns
                    if (m_startingBoundExpr == null) {
                        final AbstractExpression newComparator = new OperatorExpression(ExpressionType.OPERATOR_NOT,
                                new OperatorExpression(ExpressionType.OPERATOR_IS_NULL), null);
                        newComparator.getLeft().setLeft(upperBoundComparator.getLeft());
                        newComparator.finalizeValueTypes();
                        m_accessPath.otherExprs.add(newComparator);
                    } else {
                        m_accessPath.indexExprs.remove(m_accessPath.indexExprs.size() - 1);
                        m_accessPath.endExprs.add(m_startingBoundExpr.getFilter());
                    }

                    // add to indexExprs because it will be used as part of searchKey
                    m_accessPath.indexExprs.add(upperBoundComparator);
                    // initialExpr is set for both cases
                    // but will be used for LTE and only when overflow case of LT.
                    // The initial expression is needed to control a (short?) forward scan to
                    // adjust the start of a reverse iteration after it had to initially settle
                    // for starting at "greater than a prefix key".
                    m_accessPath.initialExpr.addAll(m_accessPath.indexExprs);
                }
            }
        }
    }


    /**
     * Given a table, a set of predicate expressions and a specific index, find the best way to
     * access the data using the given index, or return null if no good way exists.
     *
     * @param tableScan The table we want data from.
     * @param exprs The set of predicate expressions.
     * @param index The index we want to use to access the data.
     * @return A valid access path using the data or null if none found.
     */
    AccessPath getRelevantAccessPathForIndex(
            StmtTableScan tableScan, List<AbstractExpression> exprs, Index index) {
        if ( ! (tableScan instanceof StmtTargetTableScan)) {
            return null;
        }

        // Copy the expressions to a new working list that can be culled as filters are processed.
        final List<AbstractExpression> filtersToCover = new ArrayList<>(exprs);

        boolean indexIsGeographical;
        final String exprsjson = index.getExpressionsjson();
        // This list remains null if the index is just on simple columns.
        List<AbstractExpression> indexedExprs = null;
        // This vector of indexed columns remains null if indexedExprs is in use.
        List<ColumnRef> indexedColRefs = null;
        int[] indexedColIds = null;
        int keyComponentCount;
        if (exprsjson.isEmpty()) {
            // Don't bother to build a dummy indexedExprs list for a simple index on columns.
            // Just leave it null and handle this simpler case specially via indexedColRefs or
            // indexedColIds, all along the way.
            indexedColRefs = CatalogUtil.getSortedCatalogItems(index.getColumns(), "index");
            keyComponentCount = indexedColRefs.size();
            indexedColIds = new int[keyComponentCount];
            int ii = 0;
            for (ColumnRef cr : indexedColRefs) {
                indexedColIds[ii++] = cr.getColumn().getIndex();
            }
            indexIsGeographical = isAGeoColumnIndex(indexedColRefs);
        } else {
            try {
                // This MAY want to happen once when the plan is loaded from the catalog
                // and cached in a sticky cached index-to-expressions map?
                indexedExprs = AbstractExpression.fromJSONArrayString(exprsjson, tableScan);
                keyComponentCount = indexedExprs.size();
            } catch (JSONException e) {
                throw new PlanningErrorException(e);
            }
            indexIsGeographical = isAGeoExpressionIndex(indexedExprs);
        }

        final AccessPath retval = new AccessPath();
        retval.index = index;

        // An index on a single geography column is handled up front
        // as a special case with very specific matching criteria.
        // TODO: if/when we want to support multi-component hybrid indexes
        // containing one (trailing) Geography component, the filter
        // matching and AccessPath configuration for geo columns would
        // would have to be broken out of this self-contained code path and
        // integrated into the iterative component-by-component processing below.
        // OR maybe it would be more effective to keep geo indexes as single column
        // and instead implement bitmap indexing that would allow use of a geo index
        // in tandem with other indexes within one more powerful indexscan.
        if (indexIsGeographical) {
            return getRelevantAccessPathForGeoIndex(retval, tableScan, indexedExprs, indexedColRefs, filtersToCover);
        }

        // Hope for the best -- full coverage with equality matches on every expression in the index.
        retval.use = IndexUseType.COVERING_UNIQUE_EQUALITY;

        // Try to use the index scan's inherent ordering to implement the ORDER BY clause.
        // The effects of determineIndexOrdering are reflected in
        // retval.sortDirection, orderSpoilers, nSpoilers and bindingsForOrder.
        // In some borderline cases, the determination to use the index's order is optimistic and
        // provisional; it can be undone later in this function as new info comes to light.
        int[] orderSpoilers = new int[keyComponentCount];
        final List<AbstractExpression> bindingsForOrder = new ArrayList<>();
        final int nSpoilers = determineIndexOrdering(
                tableScan, keyComponentCount, indexedExprs, indexedColRefs, retval, orderSpoilers);

        // Use as many covering indexed expressions as possible to optimize comparator expressions that can use them.

        // If determineIndexOrdering found one or more spoilers,
        // index key components that might interfere with the desired ordering of the result,
        // their ill effects are eliminated when they are constrained to be equal to constants.
        // These are called "recovered spoilers".
        // When their count reaches the count of spoilers, the order of the result will be as desired.
        // Initial "prefix key component" spoilers can be recovered in the normal course
        // of finding prefix equality filters for those key components.
        // The spoiler key component positions are listed (ascending) in orderSpoilers.
        // After the last prefix equality filter has been found,
        // nRecoveredSpoilers in comparison to nSpoilers may indicate remaining unrecovered spoilers.
        // That edge case motivates a renewed search for (non-prefix) equality filters solely for the purpose
        // of recovering the spoilers and confirming the relevance of the result's index ordering.

        // Currently, an index can be used with at most one IN LIST filter expression.
        // Otherwise, MaterializedScans would have to be multi-column and populated by a cross-product
        // of multiple lists OR multiple MaterializedScans would have to be cross-joined to get a
        // multi-column LHS for the injected NestLoopIndexJoin used for IN LIST indexing.
        // So, note the one IN LIST filter when it is found, mostly to remember that one has been found.
        // This has implications for what kinds of filters on other key components can be included in
        // the index scan.

        // Start with equality comparisons on as many (prefix) indexed expressions as possible.
        final AccessPathLoopHelper helper = new AccessPathLoopHelper(nSpoilers, keyComponentCount, orderSpoilers,
                indexedColIds, tableScan, retval, indexedExprs, filtersToCover);
        final IndexableExpression inListExpr = helper.getIndexableExpression();
        final int coveredCount = helper.getCoveredCount();
        final int coveringColId = helper.getCoveringColId();
        final AbstractExpression coveringExpr = helper.getCoveringExpression();
        final int nRecoveredSpoilers = helper.getNumRecoveredSpoilers();

        // Make short work of the cases of full coverage with equality
        // which happens to be the only use case for non-scannable (i.e. HASH) indexes.
        if (coveredCount == keyComponentCount) {
            // All remaining filters get covered as post-filters
            // to be applied after the "random access" to the exact index key.
            retval.otherExprs.addAll(filtersToCover);
            if (retval.sortDirection != SortDirectionType.INVALID) {
                // This IS an odd (maybe non-existent) case
                // -- equality filters found on on all ORDER BY expressions?
                // That said, with all key components covered, there can't be any spoilers.
                retval.bindings.addAll(bindingsForOrder);
            }
            return retval;
        } else if (! IndexType.isScannable(index.getType()) ) {
            // Failure to equality-match all expressions in a non-scannable index is unacceptable.
            return null;
        }

        //
        // Scannable indexes provide more options...
        //

        // Confirm or deny some provisional matches between the index key components and
        // the ORDER BY columns.
        // If there are still unrecovered "orderSpoilers", index key components that had to be skipped
        // to find matches for the ORDER BY columns, determine whether that match was actually OK
        // by continuing the search for (non-prefix) constant equality filters.
        if (nRecoveredSpoilers < nSpoilers) {
            assert retval.sortDirection != SortDirectionType.INVALID; // There's an order to spoil.
            // Try to associate each skipped index key component with an equality filter.
            // If a key component equals a constant, its value can't actually spoil the ordering.
            // This extra checking is only needed when all of these conditions hold:
            //   -- There are three or more index key components.
            //   -- Two or more of them are in the ORDER BY clause
            //   -- One or more of them are "spoilers", i.e. are not in the ORDER BY clause.
            //   -- A "spoiler" falls between two non-spoilers in the index key component list.
            // e.g. "CREATE INDEX ... ON (A, B, C);" then "SELECT ... WHERE B=? ORDER BY A, C;"
            final List<AbstractExpression> otherBindingsForOrder =
                recoverOrderSpoilers(orderSpoilers, nSpoilers, nRecoveredSpoilers,
                                     indexedExprs, indexedColIds, tableScan, filtersToCover);
            if (otherBindingsForOrder == null) {
                // Some order spoiler didn't have an equality filter.
                // Invalidate the provisional indexed ordering.
                retval.sortDirection = SortDirectionType.INVALID;
                retval.m_stmtOrderByIsCompatible = false;
                retval.m_windowFunctionUsesIndex = WindowFunctionScoreboard.NO_INDEX_USE;
                bindingsForOrder.clear(); // suddenly irrelevant
            } else {
                // Any non-null bindings list, even an empty one,
                // denotes success -- all spoilers were equality filtered.
                bindingsForOrder.addAll(otherBindingsForOrder);
            }
        }

        final IndexableExpression startingBoundExpr;
        final IndexableExpression endingBoundExpr;
        if (! filtersToCover.isEmpty()) {
            CoveringFilterProcessor proc = new CoveringFilterProcessor(retval, coveringExpr, filtersToCover,
                    coveringColId, inListExpr == null, tableScan);
            startingBoundExpr = proc.getStartingBoundExpression();
            endingBoundExpr = proc.getEndingBoundExpression();
        } else {
            startingBoundExpr = endingBoundExpr = null;
        }
        if (endingBoundExpr == null) {
            if (retval.sortDirection == SortDirectionType.DESC) {
                // Optimizable to use reverse scan.
                if (retval.endExprs.isEmpty() && startingBoundExpr != null) { // no prefix equality filters
                    retval.indexExprs.clear();
                    retval.endExprs.add(startingBoundExpr.getFilter());
                    // The initial expression is needed to control a (short?) forward scan to
                    // adjust the start of a reverse iteration after it had to initially settle
                    // for starting at "greater than a prefix key".
                    retval.initialExpr.addAll(retval.indexExprs);
                    // Look up type here does not matter in EE, because the # of active search keys is 0.
                    // EE use m_index->moveToEnd(false) to get END, setting scan to reverse scan.
                    // retval.lookupType = IndexLookupType.LTE;
                } else if (! retval.endExprs.isEmpty()) {
                    // there are prefix equality filters -- possible for a reverse scan?

                    // set forward scan.
                    retval.sortDirection = SortDirectionType.INVALID;

                    // Turn this part on when we have EE support for reverse scan with query GT and GTE.
                    /*
                    boolean isReverseScanPossible = true;
                    if (filtersToCover.size() > 0) {
                        // Look forward to see the remaining filters.
                        for (int ii = coveredCount + 1; ii < keyComponentCount; ++ii) {
                            if (indexedExprs == null) {
                                coveringColId = indexedColIds[ii];
                            } else {
                                coveringExpr = indexedExprs.get(ii);
                            }
                            // Equality filters get first priority.
                            boolean allowIndexedJoinFilters = (inListExpr == null);
                            IndexableExpression eqExpr = getIndexableExpressionFromFilters(
                                ExpressionType.COMPARE_EQUAL, ExpressionType.COMPARE_EQUAL,
                                coveringExpr, coveringColId, table, filtersToCover,
                                allowIndexedJoinFilters, KEEP_IN_POST_FILTERS);
                            if (eqExpr == null) {
                                isReverseScanPossible = false;
                            }
                        }
                    }
                    if (isReverseScanPossible) {
                        if (startingBoundExpr != null) {
                            int lastIdx = retval.indexExprs.size() -1;
                            retval.indexExprs.remove(lastIdx);

                            AbstractExpression comparator = startingBoundExpr.getFilter();
                            retval.endExprs.add(comparator);
                            retval.initialExpr.addAll(retval.indexExprs);

                            retval.lookupType = IndexLookupType.LTE;
                        }

                    } else {
                        // set forward scan.
                        retval.sortDirection = SortDirectionType.INVALID;
                    }
                    */
                }
            }
        }
        return finalizeAccessPath(retval, keyComponentCount, startingBoundExpr, bindingsForOrder, filtersToCover);
    }

    /**
     * Calcite variant
     * Given a table, a set of predicate expressions and a specific index, find the best way to
     * access the data using the given index, or return null if no good way exists.
     *
     * @param table The table we want data from.
     * @param exprs The set of predicate expressions.
     * @param index The index we want to use to access the data.
     * @param sortDirection sort direction to use
     * @return A valid access path using the data or null if none found.
     */
    public static AccessPath getRelevantAccessPathForIndexForCalcite(
            StmtTableScan tableScan, Collection<AbstractExpression> exprs,
            Index index, SortDirectionType sortDirection) {
        if (! (tableScan instanceof StmtTargetTableScan)) {
            return null;
        }

        // Copy the expressions to a new working list that can be culled as filters are processed.
        final List<AbstractExpression> filtersToCover = new ArrayList<>(exprs);

        final boolean indexIsGeographical;
        final String exprsjson = index.getExpressionsjson();
        // This list remains null if the index is just on simple columns.
        List<AbstractExpression> indexedExprs = null;
        // This vector of indexed columns remains null if indexedExprs is in use.
        List<ColumnRef> indexedColRefs;
        int[] indexedColIds = null;
        int keyComponentCount;
        if (exprsjson.isEmpty()) {
            // Don't bother to build a dummy indexedExprs list for a simple index on columns.
            // Just leave it null and handle this simpler case specially via indexedColRefs or
            // indexedColIds, all along the way.
            indexedColRefs = CatalogUtil.getSortedCatalogItems(index.getColumns(), "index");
            keyComponentCount = indexedColRefs.size();
            indexedColIds = new int[keyComponentCount];
            int ii = 0;
            for (ColumnRef cr : indexedColRefs) {
                indexedColIds[ii++] = cr.getColumn().getIndex();
            }
            indexIsGeographical = SubPlanAssembler.isAGeoColumnIndex(indexedColRefs);
        } else {
            try {
                // This MAY want to happen once when the plan is loaded from the catalog
                // and cached in a sticky cached index-to-expressions map?
                indexedExprs = AbstractExpression.fromJSONArrayString(exprsjson, tableScan);
                keyComponentCount = indexedExprs.size();
            } catch (JSONException e) {
                e.printStackTrace();
                assert(false);
                return null;
            }
            indexIsGeographical = SubPlanAssembler.isAGeoExpressionIndex(indexedExprs);
        }

        final AccessPath retval = new AccessPath();
        retval.index = index;
        retval.sortDirection = sortDirection;

        if (indexIsGeographical) {
            return null;
        }

        // Hope for the best -- full coverage with equality matches on every expression in the index.
        retval.use = IndexUseType.COVERING_UNIQUE_EQUALITY;

        // Use as many covering indexed expressions as possible to optimize comparator expressions that can use them.

        // Start with equality comparisons on as many (prefix) indexed expressions as possible.
        int coveredCount;
        // If determineIndexOrdering found one or more spoilers,
        // index key components that might interfere with the desired ordering of the result,
        // their ill effects are eliminated when they are constrained to be equal to constants.
        // These are called "recovered spoilers".
        // When their count reaches the count of spoilers, the order of the result will be as desired.
        // Initial "prefix key component" spoilers can be recovered in the normal course
        // of finding prefix equality filters for those key components.
        // The spoiler key component positions are listed (ascending) in orderSpoilers.
        // After the last prefix equality filter has been found,
        // nRecoveredSpoilers in comparison to nSpoilers may indicate remaining unrecovered spoilers.
        // That edge case motivates a renewed search for (non-prefix) equality filters solely for the purpose
        // of recovering the spoilers and confirming the relevance of the result's index ordering.
        AbstractExpression coveringExpr = null;
        int coveringColId = -1;

        // Currently, an index can be used with at most one IN LIST filter expression.
        // Otherwise, MaterializedScans would have to be multi-column and populated by a cross-product
        // of multiple lists OR multiple MaterializedScans would have to be cross-joined to get a
        // multi-column LHS for the injected NestLoopIndexJoin used for IN LIST indexing.
        // So, note the one IN LIST filter when it is found, mostly to remember that one has been found.
        // This has implications for what kinds of filters on other key components can be included in
        // the index scan.
        IndexableExpression inListExpr = null;

        for (coveredCount = 0; coveredCount < keyComponentCount && ! filtersToCover.isEmpty(); ++coveredCount) {
            if (indexedExprs == null) {
                coveringColId = indexedColIds[coveredCount];
            } else {
                coveringExpr = indexedExprs.get(coveredCount);
            }
            // Equality filters get first priority.
            IndexableExpression eqExpr = getIndexableExpressionFromFilters(
                    // NOT DISTINCT can be also considered as an equality comparison.
                    // The only difference is that NULL is not distinct from NULL, but NULL != NULL. (ENG-11096)
                    ExpressionType.COMPARE_EQUAL, ExpressionType.COMPARE_NOTDISTINCT,
                    coveringExpr, coveringColId, tableScan, filtersToCover,
                    inListExpr == null, EXCLUDE_FROM_POST_FILTERS);

            if (eqExpr == null) {
                // For now, an IN LIST can only be indexed if any other indexed filters are based
                // solely on constants or parameters vs. other tables' columns or other IN LISTS.
                // Otherwise, there would need to be a three-way NLIJ implementation joining the
                // MaterializedScan, the source table of the other key component values,
                // and the indexed table.
                // So, only the first IN LIST filter matching a key component is considered.
                if (inListExpr == null) {
                    // Also, it can not be considered if there was a prior key component that has an
                    // equality filter that is based on another table.
                    // Accepting an IN LIST filter implies rejecting later any filters based on other
                    // tables' columns.
                    inListExpr = getIndexableExpressionFromFilters(
                            ExpressionType.COMPARE_IN, ExpressionType.COMPARE_IN,
                            coveringExpr, coveringColId, tableScan, filtersToCover,
                            false, EXCLUDE_FROM_POST_FILTERS);
                    if (inListExpr != null) {
                        // Make sure all prior key component equality filters
                        // were based on constants and/or parameters.
                        for (AbstractExpression eq_comparator : retval.indexExprs) {
                            AbstractExpression otherExpr = eq_comparator.getRight();
                            if (otherExpr.hasTupleValueSubexpression()) {
                                // Can't index this IN LIST filter without some kind of three-way NLIJ,
                                // so, add it to the post-filters.
                                AbstractExpression in_list_comparator = inListExpr.getOriginalFilter();
                                retval.otherExprs.add(in_list_comparator);
                                inListExpr = null;
                                break;
                            }
                        }
                        eqExpr = inListExpr;
                    }
                }
                if (eqExpr == null) {
                    break;
                }
            }
            AbstractExpression comparator = eqExpr.getFilter();
            retval.indexExprs.add(comparator);
            retval.bindings.addAll(eqExpr.getBindings());
            // A non-empty endExprs has the later side effect of invalidating descending sort order
            // in all cases except the edge case of full coverage equality comparison.
            // Even that case must be further qualified to exclude indexed IN-LIST
            // unless/until the MaterializedScan can be configured to iterate in descending order
            // (vs. always ascending).
            // In the case of the IN LIST expression, both the search key and the end condition need
            // to be rewritten to enforce equality in turn with each list element "row" produced by
            // the MaterializedScan. This happens in buildIndexAccessPlanForTable.
            retval.endExprs.add(comparator);
        }

        // Make short work of the cases of full coverage with equality
        // which happens to be the only use case for non-scannable (i.e. HASH) indexes.
        if (coveredCount == keyComponentCount) {
            // All remaining filters get covered as post-filters
            // to be applied after the "random access" to the exact index key.
            retval.otherExprs.addAll(filtersToCover);
            return retval;
        } else if (! IndexType.isScannable(index.getType()) ) {
            // Failure to equality-match all expressions in a non-scannable index is unacceptable.
            return null;
        }

        //
        // Scannable indexes provide more options...
        //

        IndexableExpression startingBoundExpr = null;
        IndexableExpression endingBoundExpr = null;
        if (! filtersToCover.isEmpty()) {
            // A scannable index allows inequality matches, but only on the first key component
            // missing a usable equality comparator.

            // Look for a double-ended bound on it.
            // This is always the result of an edge case:
            // "indexed-general-expression LIKE prefix-constant".
            // The simpler case "column LIKE prefix-constant"
            // has already been re-written by the HSQL parser
            // into separate upper and lower bound inequalities.
            IndexableExpression doubleBoundExpr = getIndexableExpressionFromFilters(
                    ExpressionType.COMPARE_LIKE, ExpressionType.COMPARE_LIKE,
                    coveringExpr, coveringColId, tableScan, filtersToCover,
                    false, EXCLUDE_FROM_POST_FILTERS);

            // For simplicity of implementation:
            // In some odd edge cases e.g.
            // " FIELD(DOC, 'title') LIKE 'a%' AND FIELD(DOC, 'title') > 'az' ",
            // arbitrarily choose to index-optimize the LIKE expression rather than the inequality
            // ON THAT SAME COLUMN.
            // This MIGHT not always provide the most selective filtering.
            if (doubleBoundExpr != null) {
                startingBoundExpr = doubleBoundExpr.extractStartFromPrefixLike();
                endingBoundExpr = doubleBoundExpr.extractEndFromPrefixLike();
            } else {
                final boolean allowIndexedJoinFilters = inListExpr == null;

                // Look for a lower bound.
                startingBoundExpr = getIndexableExpressionFromFilters(
                        ExpressionType.COMPARE_GREATERTHAN, ExpressionType.COMPARE_GREATERTHANOREQUALTO,
                        coveringExpr, coveringColId, tableScan, filtersToCover,
                        allowIndexedJoinFilters, EXCLUDE_FROM_POST_FILTERS);

                // Look for an upper bound.
                endingBoundExpr = getIndexableExpressionFromFilters(
                        ExpressionType.COMPARE_LESSTHAN, ExpressionType.COMPARE_LESSTHANOREQUALTO,
                        coveringExpr, coveringColId, tableScan, filtersToCover,
                        allowIndexedJoinFilters, EXCLUDE_FROM_POST_FILTERS);
            }

            if (startingBoundExpr != null) {
                AbstractExpression lowerBoundExpr = startingBoundExpr.getFilter();
                retval.indexExprs.add(lowerBoundExpr);
                retval.bindings.addAll(startingBoundExpr.getBindings());
                if (lowerBoundExpr.getExpressionType() == ExpressionType.COMPARE_GREATERTHAN) {
                    retval.lookupType = IndexLookupType.GT;
                } else {
                    assert(lowerBoundExpr.getExpressionType() == ExpressionType.COMPARE_GREATERTHANOREQUALTO);
                    retval.lookupType = IndexLookupType.GTE;
                }
                retval.use = IndexUseType.INDEX_SCAN;
            }

            if (endingBoundExpr != null) {
                AbstractExpression upperBoundComparator = endingBoundExpr.getFilter();
                retval.use = IndexUseType.INDEX_SCAN;
                retval.bindings.addAll(endingBoundExpr.getBindings());

                // if we already have a lower bound, or the sorting direction is already determined
                // do not do the reverse scan optimization
                if (retval.sortDirection != SortDirectionType.DESC &&
                        (startingBoundExpr != null || retval.sortDirection == SortDirectionType.ASC)) {
                    retval.endExprs.add(upperBoundComparator);
                    if (retval.lookupType == IndexLookupType.EQ) {
                        retval.lookupType = IndexLookupType.GTE;
                    }
                } else {
                    // Optimizable to use reverse scan.
                    // only do reverse scan optimization when no lowerBoundExpr and lookup type is either < or <=.
                    assert upperBoundComparator.getExpressionType() == ExpressionType.COMPARE_LESSTHAN ||
                            upperBoundComparator.getExpressionType() == ExpressionType.COMPARE_LESSTHANOREQUALTO;
                    switch (upperBoundComparator.getExpressionType()) {
                        case COMPARE_LESSTHAN:
                            retval.lookupType = IndexLookupType.LT;
                            break;
                        case COMPARE_LESSTHANOREQUALTO:
                            retval.lookupType = IndexLookupType.LTE;
                            break;
                        default:
                            throw new PlanningErrorException(String.format("Unsupported index expression type: %s",
                                    upperBoundComparator.getExpressionType().toString()));
                    }
                    // Unlike a lower bound, an upper bound does not automatically filter out nulls
                    // as required by the comparison filter, so construct a NOT NULL comparator and
                    // add to post-filter
                    // TODO: Implement an abstract isNullable() method on AbstractExpression and use
                    // that here to optimize out the "NOT NULL" comparator for NOT NULL columns
                    if (startingBoundExpr == null) {
                        final AbstractExpression newComparator = new OperatorExpression(
                                ExpressionType.OPERATOR_NOT,
                                new OperatorExpression(ExpressionType.OPERATOR_IS_NULL), null);
                        newComparator.getLeft().setLeft(upperBoundComparator.getLeft());
                        newComparator.finalizeValueTypes();
                        retval.otherExprs.add(newComparator);
                    } else {
                        retval.indexExprs.remove(retval.indexExprs.size() -1);
                        AbstractExpression lowerBoundComparator = startingBoundExpr.getFilter();
                        retval.endExprs.add(lowerBoundComparator);
                    }

                    // add to indexExprs because it will be used as part of searchKey
                    retval.indexExprs.add(upperBoundComparator);
                    // initialExpr is set for both cases
                    // but will be used for LTE and only when overflow case of LT.
                    // The initial expression is needed to control a (short?) forward scan to
                    // adjust the start of a reverse iteration after it had to initially settle
                    // for starting at "greater than a prefix key".
                    retval.initialExpr.addAll(retval.indexExprs);
                }
            }
        }

        if (endingBoundExpr == null) {
            if (retval.sortDirection == SortDirectionType.DESC) {
                // Optimizable to use reverse scan.
                if (retval.endExprs.isEmpty()) { // no prefix equality filters
                    if (startingBoundExpr != null) {
                        retval.indexExprs.clear();
                        AbstractExpression comparator = startingBoundExpr.getFilter();
                        retval.endExprs.add(comparator);
                        // The initial expression is needed to control a (short?) forward scan to
                        // adjust the start of a reverse iteration after it had to initially settle
                        // for starting at "greater than a prefix key".
                        retval.initialExpr.addAll(retval.indexExprs);
                        // Look up type here does not matter in EE, because the # of active search keys is 0.
                        // EE use m_index->moveToEnd(false) to get END, setting scan to reverse scan.
                        // retval.lookupType = IndexLookupType.LTE;
                    }
                } else {
                    retval.sortDirection = SortDirectionType.INVALID;
                }
            }
        }

        // index not relevant to expression
        if (retval.indexExprs.size() == 0 && retval.endExprs.size() == 0 &&
                retval.sortDirection == SortDirectionType.INVALID) {
            return null;
        } else {
            // If all of the index key components are not covered by comparisons
            // (but SOME are), then the scan may need to be reconfigured to account
            // for the scan key being padded in the EE with null values for the
            // components that are not being filtered.
            if (retval.indexExprs.size() < keyComponentCount) {
                correctAccessPathForPrefixKeyCoverage(retval, startingBoundExpr);
            }
            // All remaining filters get applied as post-filters
            // on tuples fetched from the index.
            retval.otherExprs.addAll(filtersToCover);
            return retval;
        }
    }

        private static AccessPath finalizeAccessPath(
                AccessPath retval, int keyComponentCount, IndexableExpression startingBoundExpr,
                List<AbstractExpression> bindingsForOrder, List<AbstractExpression> filtersToCover) {
            // index not relevant to expression
            if (retval.indexExprs.isEmpty() && retval.endExprs.isEmpty() && retval.sortDirection == SortDirectionType.INVALID) {
                return null;
            } else if (retval.indexExprs.size() < keyComponentCount) {
                // If all of the index key components are not covered by comparisons
                // (but SOME are), then the scan may need to be reconfigured to account
                // for the scan key being padded in the EE with null values for the
                // components that are not being filtered.
                correctAccessPathForPrefixKeyCoverage(retval, startingBoundExpr);
            }

            // All remaining filters get applied as post-filters
            // on tuples fetched from the index.
            retval.otherExprs.addAll(filtersToCover);
            if (retval.sortDirection != SortDirectionType.INVALID) {
                retval.bindings.addAll(bindingsForOrder);
            }
            return retval;
        }

        private static void correctAccessPathForPrefixKeyCoverage(AccessPath retval,
                IndexableExpression startingBoundExpr) {
            // If IndexUseType has the default value of COVERING_UNIQUE_EQUALITY, then the
            // scan can use GTE instead to match all values, not only the null values, for the
            // unfiltered components -- assuming that any value is considered >= null.
            if (retval.use == IndexUseType.COVERING_UNIQUE_EQUALITY) {
                correctEqualityForPrefixKey(retval);
            } else if (retval.lookupType == IndexLookupType.GT) {
                // GTE scans can have any number of null key components appended without changing
                // the effective value. So, that leaves GT scans.
                correctForwardScanForPrefixKey(retval, startingBoundExpr);
            }
        }

        private static void correctEqualityForPrefixKey(AccessPath retval) {
            retval.use = IndexUseType.INDEX_SCAN;
            // With no key, the lookup type will be ignored and the sort direction will
            // determine the scan direction; With prefix key and explicit DESC order by,
            // tell the EE to do reverse scan.
            if (retval.sortDirection == SortDirectionType.DESC && retval.indexExprs.size() > 0) {
                retval.lookupType = IndexLookupType.LTE;
                // The initial expression is needed to control a (short?) forward scan to
                // adjust the start of a reverse iteration after it had to initially settle
                // for starting at "greater than a prefix key".
                retval.initialExpr.addAll(retval.indexExprs);
            } else {
                retval.lookupType = IndexLookupType.GTE;
            }
        }

        private static void correctForwardScanForPrefixKey(AccessPath retval,
                IndexableExpression startingBoundExpr) {
            // GT scans pose a problem in that they would mistakenly match any
            // compound key in the index that was EQUAL on the prefix key(s) but
            // greater than the EE's null key padding (that is, any non-nulls)
            // for the non-filtered suffix key(s).
            // The current work-around for this is to add (back) the GT condition
            // to the set of "other" filter expressions that get evaluated for each
            // tuple found in the index scan.
            // This will eliminate the initial entries that are equal on the prefix key.
            // This is not as efficient as getting the index scan to start in the
            // "correct" place, but it puts off having to change the EE code.
            // TODO: ENG-3913 describes more ambitious alternative solutions that include:
            //  - padding with MAX values rather than null/MIN values for GT scans.
            //  - adding the GT condition as a special "initialExpr" post-condition
            //    that disables itself as soon as it evaluates to true for any row
            //    -- it would be expected to always evaluate to true after that.
            AbstractExpression comparator = startingBoundExpr.getOriginalFilter();
            retval.otherExprs.add(comparator);
        }

        private AccessPath getRelevantAccessPathForGeoIndex(AccessPath retval, StmtTableScan tableScan,
                List<AbstractExpression> indexedExprs, List<ColumnRef> indexedColRefs,
                List<AbstractExpression> filtersToCover) {
            Preconditions.checkArgument(indexedExprs == null,
                    "Geo expression not yet supported, so indxedExprs should be null");
            Preconditions.checkNotNull(indexedColRefs); // for now a geo COLUMN is required.
            Preconditions.checkState(isAGeoColumnIndex(indexedColRefs));
            Column geoCol = indexedColRefs.get(0).getColumn();
            // Match only the table's column that has the coveringColId
            // Handle a simple indexed column identified by its column id.
            int coveringColId = geoCol.getIndex();
            String tableAlias = tableScan.getTableAlias();
            // Iterate over the query filters looking for a matching CONTAINS-like predicate.
            // These are identified by their unique function type signature
            // -- safe for now, until we happen to add an
            // unrelated function with the same signature.
            // The alternative would be to import the specific function id for CONTAINS
            // from FunctionForVoltDB to match on that, and then probably expand that to
            // include APPROX_CONTAINS if/when we decide to explicitly support APPROX_CONTAINS
            // as a filter that COMPLETELY eliminates post-filtering on an indexed geography
            // column OR to export an additional optional "is geo indexable" attribute to the
            // VoltXML that sets up FunctionExpressions. Maybe we can revisit this if/when
            // we change FunctionForVoltDB to support APPROX_CONTAINS.
            for (AbstractExpression filter : filtersToCover) {
                if (filter.getExpressionType() != ExpressionType.FUNCTION) {
                    continue;
                } else if (filter.getValueType() != VoltType.BOOLEAN) {
                    continue;
                } else if (filter.getArgs().size() != 2) {
                    continue;
                }
                final FunctionExpression fn = (FunctionExpression) filter;
                //TODO: also support explicit APPROX_CONTAINS
                if ( ! fn.hasFunctionId(FunctionForVoltDB.FUNC_VOLT_ID_FOR_CONTAINS) ) {
                    continue;
                }

                final AbstractExpression indexableArg = filter.getArgs().get(0);
                assert indexableArg instanceof TupleValueExpression;
                assert indexableArg.getValueType() == VoltType.GEOGRAPHY;
                final TupleValueExpression geoTve = (TupleValueExpression) indexableArg;
                if (! tableAlias.equals(geoTve.getTableAlias())) {
                    continue;
                } else if (coveringColId != geoTve.getColumnIndex()) {
                    continue;
                }

                final AbstractExpression searchKeyArg = filter.getArgs().get(1);
                assert searchKeyArg.getValueType() == VoltType.GEOGRAPHY_POINT;
                // Search key operand must not be from the same table,
                // e.g. contains(t.a, t.b) is not indexable.
                if (! isOperandDependentOnTable(searchKeyArg, tableScan)) {
                    filtersToCover.remove(searchKeyArg);
                    retval.indexExprs.add(searchKeyArg);
                    retval.otherExprs.addAll(filtersToCover);
                    retval.lookupType = IndexLookupType.GEO_CONTAINS;
                    // It's unlikely but possible that the query has more than one
                    // CONTAINS filter that uses the same geography column, e.g.
                    //  "WHERE CONTAINS(place, point) AND CONTAINS(place, ?)"
                    //
                    // Since the search stops here on finding the first such filter,
                    // any others will be treated strictly as post-filters.
                    // At this point, there exists no reasonable criteria to prefer
                    // one similar filter over another.
                    // This is analogous to the handling of other toss-ups like
                    //  "WHERE int_key > 30 AND int_key > ?".
                    return retval;
                }
            }
            return null;
        }

        private static boolean isAGeoColumnIndex(List<ColumnRef> indexedColRefs) {
            // Initially, geographical indexing only supports a single indexed column of type geography.
            if (indexedColRefs.size() != 1) {
                return false;
            } else {
                return indexedColRefs.get(0).getColumn().getType() == VoltType.GEOGRAPHY.getValue();
            }
        }

        private static boolean isAGeoExpressionIndex(List<AbstractExpression> indexedExprs) {
            // Currently, geo indexes are strictly column-based,
            // never expression/function based.
            // This could change profoundly in the future if we use pseudo-function
            // expression syntax to configure each index.
            return false;
        }

        /**
         * Determine if an index, which is in the AccessPath argument
         * retval, can satisfy a parsed select statement's order by or
         * window function ordering requirements.
         *
         * @param tableScan          only used here to validate base table names of ORDER BY columns' .
         * @param keyComponentCount  the length of indexedExprs or indexedColRefs,
         *                           ONE of which must be valid
         * @param indexedExprs       expressions for key components in the general case
         * @param indexedColRefs     column references for key components in the simpler case
         * @param retval the eventual result of getRelevantAccessPathForIndex,
         *               the bearer of a (tentative) sortDirection determined here
         * @param orderSpoilers      positions of key components which MAY invalidate the tentative
         *                           sortDirection
         * @return the number of discovered orderSpoilers that will need to be recovered from,
         *         to maintain the established sortDirection - always 0 if no sort order was determined.
         */
        private int determineIndexOrdering(
                StmtTableScan tableScan, int keyComponentCount, List<AbstractExpression> indexedExprs,
                List<ColumnRef> indexedColRefs, AccessPath retval, int[] orderSpoilers) {
            // Organize a little bit.
            final ParsedSelectStmt pss = (m_parsedStmt instanceof ParsedSelectStmt) ?
                    ((ParsedSelectStmt) m_parsedStmt) : null;
            final boolean hasOrderBy = m_parsedStmt.hasOrderByColumns() && ! m_parsedStmt.orderByColumns().isEmpty();
            final boolean hasWindowFunctions = pss != null && pss.hasWindowFunctionExpression();
            //
            // If we have no statement level order by or window functions,
            // then we can't use this index for ordering, and we
            // return 0.
            //
            if (! hasOrderBy && ! hasWindowFunctions) {
                return 0;
            } else {
                //
                // We make a definition.  Let S1 and S2 be sequences of expressions,
                // and OS be an increasing sequence of indices into S2.  Let erase(S2, OS) be
                // the sequence of expressions which results from erasing all S2
                // expressions whose indices are in OS.  We say *S1 is a prefix of
                // S2 with OS-singular values* if S1 is a prefix of erase(S2, OS).
                // That is to say, if we erase the expressions in S2 whose indices are
                // in OS, S1 is a prefix of the result.
                //
                // What expressions must we match?
                //   1.) We have the parameters indexedExpressions and indexedColRefs.
                //       These are the expressions or column references in an index.
                //       Exactly one of them is non-null.  Since these are both an
                //       indexed sequence of expression-like things, denote the
                //       non-null one IS.
                // What expressions do we care about?  We have two kinds.
                //   1.) The expressions from the statement level order by clause, OO.
                //       This sequence of expressions must be a prefix of IS with OS
                //       singular values for some sequence OS.  The sequence OS will be
                //       called the order spoilers.  Later on we will test that the
                //       index expressions at the positions in OS can have only a
                //       single value.
                //   2.) The expressions in a window function's partition by list, WP,
                //       followed by the expressions in the window function's
                //       order by list, WO.  The partition by functions are a set not
                //       a sequence.  We need to find a sequence of expressions, S,
                //       such that S is a permutation of P and S+WO is a singular prefix
                //       of IS.
                //
                // So, in both cases, statement level order by and window function, we are looking for
                // a sequence of expressions, S1, and a list of IS indices, OS, such
                // that S1 is a prefix of IS with OS-singular values.
                //
                // If the statement level order by expression list is not a prefix of
                // the index, we still care to know about window functions.  The reverse
                // is not true.  If there are window functions but they all fail to match the index,
                // we must give up on this index, even if the statement level order
                // by list is still matching.  This is because the window functions will
                // require sorting before the statement level order by clause's
                // sort, and this window function sort will invalidate the statement level
                // order by sort.
                //
                // Note also that it's possible that the statement level order by and
                // the window function order by are compatible.  So this index may provide
                // all the sorting needed.
                //
                // There need to be enough indexed expressions to provide full sort coverage.
                // More indexed expressions are ok.
                //
                // We keep a scoreboard which keeps track of everything.  All the window
                // functions and statement level order by functions are kept in the scoreboard.
                //
                final WindowFunctionScoreboard windowFunctionScores = new WindowFunctionScoreboard(m_parsedStmt);
                // indexCtr is an index into the index expressions or columns.
                for (int indexCtr = 0; !windowFunctionScores.isDone() && indexCtr < keyComponentCount; ++indexCtr) {
                    // Figure out what to do with index expression or column at indexCtr.
                    // First, fetch it out.
                    final AbstractExpression indexExpr = indexedExprs == null ? null : indexedExprs.get(indexCtr);
                    final ColumnRef indexColRef = indexedColRefs == null ? null : indexedColRefs.get(indexCtr);
                    // Then see if it matches something.  If
                    // this doesn't match one thing it may match
                    // another.  If it doesn't match anything, it may
                    // be an order spoiler, which we will maintain in
                    // the scoreboard.
                    windowFunctionScores.matchIndexEntry(new ExpressionOrColumn(
                            indexCtr, tableScan, indexExpr, SortDirectionType.INVALID, indexColRef));
                }
                //
                // The result is the number of order spoilers, but
                // also the access path we have chosen, the order
                // spoilers themselves and the bindings.  Return these
                // by reference.
                //
                return windowFunctionScores.getResult(retval, orderSpoilers);
            }
        }

        /**
         * @param orderSpoilers  positions of index key components that would need to be
         *                       equality filtered to keep from interfering with the desired order
         * @param nSpoilers      the number of valid orderSpoilers
         * @param indexedExprs   the index key component expressions in the general case
         * @param colIds         the index key component columns in the simple case
         * @param tableScan      the index base table scan, used to validate column base tables
         * @param filtersToCover query conditions that may contain the desired equality filters
         */
        private static List<AbstractExpression> recoverOrderSpoilers(int[] orderSpoilers, int nSpoilers,
        int nRecoveredSpoilers, List<AbstractExpression> indexedExprs, int[] colIds,
        StmtTableScan tableScan, List<AbstractExpression> filtersToCover) {
            // Filters leveraged for an optimization, such as the skipping of an ORDER BY plan node
            // always risk adding a dependency on a particular parameterization, so be prepared to
            // add prerequisite parameter bindings to the plan.
            final List<AbstractExpression> otherBindingsForOrder = new ArrayList<>();
            // Order spoilers must be recovered in the order they were found
            // for the index ordering to be considered acceptable.
            // Each spoiler key component is recovered by the detection of an equality filter on it.
            for (int index = nRecoveredSpoilers; index < nSpoilers; ++index) {
                // There may be more equality filters that weren't useful for "coverage"
                // but may still serve to recover an otherwise order-spoiling index key component.
                // The filter will only be applied as a post-condition,
                // but that's good enough to satisfy the ORDER BY.
                final AbstractExpression coveringExpr;
                int coveringColId = -1;
                // This is a scaled down version of the coverage check in getRelevantAccessPathForIndex.
                // This version leaves intact any filter it finds,
                // so it will be picked up as a post-filter.
                if (indexedExprs == null) {
                    coveringColId = colIds[orderSpoilers[index]];
                    coveringExpr = null;
                } else {
                    coveringExpr = indexedExprs.get(orderSpoilers[index]);
                }
                final IndexableExpression eqExpr = getIndexableExpressionFromFilters(
                        ExpressionType.COMPARE_EQUAL, ExpressionType.COMPARE_EQUAL,
                        coveringExpr, coveringColId, tableScan, filtersToCover,
                        // A key component filter based on another table's column is enough to maintain ordering
                        // if this index is chosen, regardless of whether an NLIJ is injected for an IN LIST.
                        true /*alwaysAllowConstrainingJoinFilters*/, KEEP_IN_POST_FILTERS);
                if (eqExpr == null) {
                    return null;
                } else {
                    // The equality filter confirms that the "spoiler" index key component
                    // (one missing from the ORDER BY) is constant-valued,
                    // so it can't spoil the scan result sort order established by other key components.

                    // Accumulate bindings (parameter constraints) across all recovered spoilers.
                    otherBindingsForOrder.addAll(eqExpr.getBindings());
                }
            }
            return otherBindingsForOrder;
        }

        /**
         * For a given filter expression, return a normalized version of it that is always a comparison operator whose
         * left-hand-side references the table specified and whose right-hand-side does not.
         * Returns null if no such formulation of the filter expression is possible.
         * For example, "WHERE F_ID = 2" would return it input intact if F_ID is in the table passed in.
         * For join expressions like, "WHERE F_ID = Q_ID", it would also return the input expression if F_ID is in the table
         * but Q_ID is not. If only Q_ID were defined for the table, it would return an expression for (Q_ID = F_ID).
         * If both Q_ID and F_ID were defined on the table, null would be returned.
         * Ideally, the left-hand-side expression is intended to be an indexed expression on the table using the current
         * index. To help reduce false positives, the (base) columns and/or indexed expressions of the index are also
         * provided to help further reduce non-null returns in uninteresting cases.
         *
         * @param targetComparator An allowed comparison operator
         *                         -- its reverse is allowed in reversed expressions
         * @param altTargetComparator An alternatively allowed comparison operator
         *                            -- its reverse is allowed in reversed expressions
         * @param coveringExpr The indexed expression on the table's column
         *                     that might match a query filter, possibly null.
         * @param coveringColId When coveringExpr is null,
         *                      the id of the indexed column might match a query filter.
         * @param tableScan The table scan on which the indexed expression is based
         * @param filtersToCover the query conditions that may contain the desired filter
         * @param allowIndexedJoinFilters Whether filters referencing other tables' columns are acceptable
         * @param filterAction the desired disposition of the matched filter,
        either EXCLUDE_FROM_POST_FILTERS or KEEP_IN_POST_FILTERS
         * @return An IndexableExpression -- really just a pairing of a normalized form of expr with the
         * potentially indexed expression on the left-hand-side and the potential index key expression on
         * the right of a comparison operator, and a list of parameter bindings that are required for the
         * index scan to be applicable.
         * -- or null if there is no filter that matches the indexed expression
         */
        private static IndexableExpression getIndexableExpressionFromFilters(
                ExpressionType targetComparator, ExpressionType altTargetComparator,
                AbstractExpression coveringExpr, int coveringColId, StmtTableScan tableScan,
                List<AbstractExpression> filtersToCover, boolean allowIndexedJoinFilters, boolean filterAction) {
            List<AbstractExpression> binding = null;
            ComparisonExpression normalizedExpr = null;
            AbstractExpression originalFilter = null;
            for (AbstractExpression filter : filtersToCover) {
                AbstractExpression indexableExpr;
                AbstractExpression otherExpr;
                // ENG-8203: Not going to try to use index with sub-query expression
                if (filter.hasSubquerySubexpression()) {
                    // Including RowSubqueryExpression and SelectSubqueryExpression
                    // SelectSubqueryExpression also can be scalar sub-query
                } else if (filter.getExpressionType() == targetComparator ||
                        filter.getExpressionType() == altTargetComparator) { // Expression type must be resolvable by an index scan
                    normalizedExpr = (ComparisonExpression) filter;
                    indexableExpr = filter.getLeft();
                    otherExpr = filter.getRight();
                    binding = bindingIfValidIndexedFilterOperand(
                            tableScan, indexableExpr, otherExpr, coveringExpr, coveringColId);
                    if (binding != null) {
                        if (! allowIndexedJoinFilters && otherExpr.hasTupleValueSubexpression()) {
                            // This filter can not be used with the index, possibly due to interactions
                            // wih IN LIST processing that would require a three-way NLIJ.
                            binding = null;
                            continue;
                        } else {
                            switch (targetComparator) {
                                case COMPARE_LIKE:
                                    // Additional restrictions apply to LIKE pattern arguments
                                    if (otherExpr instanceof ParameterValueExpression) {
                                        final ParameterValueExpression pve = (ParameterValueExpression) otherExpr;
                                        // Can't use an index for parameterized LIKE filters,
                                        // e.g. "T1.column LIKE ?"
                                        // UNLESS the parameter was artificially substituted
                                        // for a user-specified constant AND that constant was a prefix pattern.
                                        // In that case, the parameter has to be added to the bound list
                                        // for this index/statement.
                                        final ConstantValueExpression cve = pve.getOriginalValue();
                                        if (cve == null || ! cve.isPrefixPatternString()) {
                                            binding = null; // the filter is not usable, so the binding is invalid
                                            continue;
                                        }
                                        // Remember that the binding list returned by
                                        // bindingIfValidIndexedFilterOperand above
                                        // is often a "shared object" and is intended to be treated as immutable.
                                        // To add a parameter to it, first copy the List.
                                        List<AbstractExpression> moreBinding = new ArrayList<>(binding);
                                        moreBinding.add(pve);
                                        binding = moreBinding;
                                    } else if (otherExpr instanceof ConstantValueExpression) {
                                        // Can't use an index for non-prefix LIKE filters,
                                        // e.g. " T1.column LIKE '%ish' "
                                        if (! ((ConstantValueExpression) otherExpr).isPrefixPatternString()) {
                                            // The constant is not an index-friendly prefix pattern.
                                            binding = null; // the filter is not usable, so the binding is invalid
                                            continue;
                                        }
                                    } else {
                                        // Other cases are not indexable, e.g. " T1.column LIKE T2.column "
                                        binding = null; // the filter is not usable, so the binding is invalid
                                        continue;
                                    }
                                    break;
                                case COMPARE_IN:
                                    if (otherExpr.hasTupleValueSubexpression()) {
                                        // This is a fancy edge case where the expression could only be indexed
                                        // if it:
                                        // A) does not reference the indexed table and
                                        // B) has ee support for a three-way NLIJ where the table referenced in
                                        // the list element expression feeds values from its current row to the
                                        // Materialized scan which then re-evaluates its expressions to
                                        // re-populate the temp table that drives the injected NLIJ with
                                        // this index scan.
                                        // This is a slightly more twisted variant of the three-way NLIJ that
                                        // would be needed to support compound key indexing on a combination
                                        // of (fixed) IN LIST elements and join key values from other tables.
                                        // Punt for now on indexing this IN LIST filter.
                                        binding = null; // the filter is not usable, so the binding is invalid
                                        continue;
                                    } else if (otherExpr instanceof ParameterValueExpression) {
                                        // It's OK to use an index for a parameterized IN filter,
                                        // e.g. "T1.column IN ?"
                                        // EVEN if the parameter was -- someday -- artificially substituted
                                        // for an entire user-specified list of constants.
                                        // As of now, that is beyond the capabilities of the ad hoc statement
                                        // parameterizer, so "T1.column IN (3, 4)" can use the plan for
                                        // "T1.column IN (?, ?)" that might have been originally cached for
                                        // "T1.column IN (1, 2)" but "T1.column IN (1, 2, 3)" would need its own
                                        // "T1.column IN (?, ?, ?)" plan, etc. per list element count.
                                    } else {
                                        //TODO: Some day, there may be an optimization here that allows an entire
                                        // IN LIST of constants to be serialized as a single value instead of a
                                        // VectorValue composed of ConstantValue arguments.
                                        // What's TBD is whether that would get its own AbstractExpression class or
                                        // just be a special case of ConstantValueExpression.
                                        assert (otherExpr instanceof VectorValueExpression);
                                    }
                                default:
                            }
                        }
                        originalFilter = filter;
                        if (filterAction == EXCLUDE_FROM_POST_FILTERS) {
                            filtersToCover.remove(filter);
                        }
                        break;
                    }
                }
                if (filter.getExpressionType() == ComparisonExpression.reverses.get(targetComparator) ||
                        filter.getExpressionType() == ComparisonExpression.reverses.get(altTargetComparator)) {
                    normalizedExpr = (ComparisonExpression) filter;
                    normalizedExpr = normalizedExpr.reverseOperator();
                    indexableExpr = filter.getRight();
                    otherExpr = filter.getLeft();
                    binding = bindingIfValidIndexedFilterOperand(
                            tableScan, indexableExpr, otherExpr, coveringExpr, coveringColId);
                    if (binding != null) {
                        if ( ! allowIndexedJoinFilters && otherExpr.hasTupleValueSubexpression()) {
                            // This filter can not be used with the index, probably due to interactions
                            // with IN LIST processing of another key component that would require a
                            // three-way NLIJ to be injected.
                            binding = null;
                        } else {
                            originalFilter = filter;
                            if (filterAction == EXCLUDE_FROM_POST_FILTERS) {
                                filtersToCover.remove(filter);
                            }
                            break;
                        }
                    }
                }
            }
            if (binding == null) { // ran out of candidate filters.
                return null;
            } else {
                return new IndexableExpression(originalFilter, normalizedExpr, binding);
            }
        }

        /**
         * Loop over the expressions to cover to find ones that exactly match the covering expression
         * and remove them from the original list. Returns true if there is at least one match. False otherwise.
         * @param coveringExpr
         * @param exprsToCover
         * @return true is the covering expression exactly matches to one or more expressions to cover
         */
        private static boolean removeExactMatchCoveredExpressions(
                AbstractExpression coveringExpr, List<AbstractExpression> exprsToCover) {
            boolean matched = false;
            for (int index = exprsToCover.size() - 1; index >= 0; --index) {
                if (coveringExpr.bindingToIndexedExpression(exprsToCover.get(index)) != null) {
                    exprsToCover.remove(index);
                    matched = true;
                }
            }
            return matched;
        }

        /**
         * Remove NOT NULL expressions that are covered by the NULL-rejecting expressions. For example,
         * 'COL IS NOT NULL' is covered by the 'COL > 0' NULL-rejecting comparison expression.
         *
         * @param tableScan
         * @param coveringExprs
         * @param exprsToCover
         * @return List<AbstractExpression>
         */
        private static List<AbstractExpression> removeNotNullCoveredExpressions(
                StmtTableScan tableScan, Collection<AbstractExpression> coveringExprs, List<AbstractExpression> exprsToCover) {
            // Collect all TVEs from NULL-rejecting covering expressions
            Set<TupleValueExpression> coveringTves = new HashSet<>();
            for (AbstractExpression coveringExpr : coveringExprs) {
                if (ExpressionUtil.isNullRejectingExpression(coveringExpr, tableScan.getTableAlias())) {
                    coveringTves.addAll(ExpressionUtil.getTupleValueExpressions(coveringExpr));
                }
            }
            // For each NOT NULL expression to cover extract the TVE expressions. If all of them are also part
            // of the covering NULL-rejecting collection then this NOT NULL expression is covered
            for (int index = exprsToCover.size() - 1; index >= 0; --index) {
                final AbstractExpression filter = exprsToCover.get(index);
                assert filter.getExpressionType() != ExpressionType.OPERATOR_NOT ||
                        (filter.getLeft() != null &&
                                (filter.getLeft().getExpressionType() != ExpressionType.OPERATOR_IS_NULL ||
                                        filter.getLeft().getLeft() != null));
                if (filter.getExpressionType() == ExpressionType.OPERATOR_NOT &&
                        filter.getLeft().getExpressionType() == ExpressionType.OPERATOR_IS_NULL &&
                        coveringTves.containsAll(ExpressionUtil.getTupleValueExpressions(filter.getLeft().getLeft()))) {
                    exprsToCover.remove(index);
                }
            }
            return exprsToCover;
        }

        private static boolean isOperandDependentOnTable(AbstractExpression expr, StmtTableScan tableScan) {
            return ExpressionUtil.getTupleValueExpressions(expr).stream()
                    .anyMatch(tve -> tableScan.getTableAlias().equals(tve.getTableAlias()));
        }

        private static List<AbstractExpression> bindingIfValidIndexedFilterOperand(
                StmtTableScan tableScan, AbstractExpression indexableExpr, AbstractExpression otherExpr,
                AbstractExpression coveringExpr, int coveringColId) {
            // Do some preliminary disqualifications.
            final VoltType keyType = indexableExpr.getValueType();
            final VoltType otherType = otherExpr.getValueType();
            // EE index key comparator should not lose precision when casting keys to the indexed type.
            // Do not choose an index that requires such a cast.
            // Except the EE DOES contain the necessary logic to avoid loss of SCALE
            // when the indexed type is just a narrower integer type.
            // This is very important, since the typing for integer constants
            // MAY not pay that much attention to minimizing scale.
            // This was behind issue ENG-4606 -- failure to index on constant equality.
            // So, accept any pair of integer types.
            final List<AbstractExpression> result;
            if (! keyType.canExactlyRepresentAnyValueOf(otherType) &&
                    ! (keyType.isBackendIntegerType() && otherType.isBackendIntegerType()))  {
                result = null;
            } else if (isOperandDependentOnTable(otherExpr, tableScan)) {
                // Left and right operands must not be from the same table,
                // e.g. where t.a = t.b is not indexable with the current technology.
                result = null;
            } else if (coveringExpr == null) {
                // Match only the table's column that has the coveringColId
                // There could be a CAST operator on the indexable expression side. Skip it
                //
                // ENG-20461: CAST is droppable only for safe conversions, i.e.
                // not casting between STRING and NUMBER. Remember that all "STRING"
                // types in VoltDB are variable lengths.
                if (indexableExpr.getExpressionType() == ExpressionType.OPERATOR_CAST &&
                        ((OperatorExpression) indexableExpr).isSafeCast()) {
                    // CASTING between other types can be safely dropped, to aid
                    // index coverage matching
                    indexableExpr = indexableExpr.getLeft();
                }
                if (indexableExpr.getExpressionType() != ExpressionType.VALUE_TUPLE) {
                    result = null;
                } else if ((coveringColId == ((TupleValueExpression) indexableExpr).getColumnIndex()) &&
                        (tableScan.getTableAlias().equals(((TupleValueExpression) indexableExpr).getTableAlias()))) {
                    // Handle a simple indexed column identified by its column id.
                    // A column match never requires parameter binding. Return an empty list.
                    result = ExpressionOrColumn.s_reusableImmutableEmptyBinding;
                } else {
                    result = null;
                }
            } else {
                // Do a possibly more extensive match with coveringExpr which MAY require bound parameters.
                result = indexableExpr.bindingToIndexedExpression(coveringExpr);
            }
            return result;
        }


        /**
         * Insert a send receive pair above the supplied scanNode.
         * @param scanNode that needs to be distributed
         * @return return the newly created receive node (which is linked to the new sends)
         */
        static AbstractPlanNode addSendReceivePair(AbstractPlanNode scanNode) {
            final SendPlanNode sendNode = new SendPlanNode();
            sendNode.addAndLinkChild(scanNode);

            final ReceivePlanNode recvNode = new ReceivePlanNode();
            recvNode.addAndLinkChild(sendNode);

            return recvNode;
        }

        /**
         * Given an access path, build the single-site or distributed plan that will
         * assess the data from the table according to the path.
         *
         * @param tableNode The table to get data from.
         * @return The root of a plan graph to get the data.
         */
        static AbstractPlanNode getAccessPlanForTable(JoinNode tableNode) {
            final StmtTableScan tableScan = tableNode.getTableScan();
            // Access path to access the data in the table (index/scan/etc).
            final AccessPath path = tableNode.m_currentAccessPath;
            Preconditions.checkNotNull(tableNode.m_currentAccessPath);

            // if no index, it is a sequential scan
            if (path.index == null) {
                return getScanAccessPlanForTable(tableScan, path);
            } else {
                return buildIndexAccessPlanForTable(tableScan, path);
            }
        }

        /**
         * Get a sequential scan access plan for a table. For multi-site plans/tables,
         * scans at all partitions.
         *
         * @param tableScan The table to scan.
         * @param path The access path to access the data in the table (index/scan/etc).
         * @return A scan plan node
         */
        private static AbstractScanPlanNode getScanAccessPlanForTable(StmtTableScan tableScan, AccessPath path) {
            // build the scan node
            final SeqScanPlanNode scanNode = new SeqScanPlanNode(tableScan);
            // build the predicate
            scanNode.setPredicate(path.otherExprs);
            return scanNode;
        }

        // Generate a plan for an IN-LIST-driven index scan
        private static AbstractPlanNode injectIndexedJoinWithMaterializedScan(
                AbstractExpression listElements, IndexScanPlanNode scanNode) {
            final MaterializedScanPlanNode matScan = new MaterializedScanPlanNode();
            Preconditions.checkArgument(
                    listElements instanceof VectorValueExpression || listElements instanceof ParameterValueExpression,
                    "Argument listElements must be either VVE or PVE");
            matScan.setRowData(listElements);
            matScan.setSortDirection(scanNode.getSortDirection());

            final NestLoopIndexPlanNode nlijNode = new NestLoopIndexPlanNode();
            nlijNode.setJoinType(JoinType.INNER);
            nlijNode.addInlinePlanNode(scanNode);
            nlijNode.addAndLinkChild(matScan);
            // resolve the sort direction
            nlijNode.resolveSortDirection();
            return nlijNode;
        }


        // Replace the IN LIST condition in the end expression referencing the first given rhs
        // with an equality filter referencing the second given rhs.
        private static void replaceInListFilterWithEqualityFilter(
                List<AbstractExpression> endExprs, AbstractExpression inListRhs, AbstractExpression equalityRhs) {
            for (AbstractExpression comparator : endExprs) {
                if (comparator.getRight() == inListRhs) {
                    endExprs.remove(comparator);
                    endExprs.add(new ComparisonExpression(ExpressionType.COMPARE_EQUAL, comparator.getLeft(), equalityRhs));
                    break;
                }
            }
        }

        /**
         * Partial index optimization: Remove query expressions that exactly match the index WHERE expression(s)
         * from the access path.
         *
         * @param path - Partial Index access path
         * @param exprToRemove - expressions to remove
         */
        private static void filterPostPredicateForPartialIndex(AccessPath path, List<AbstractExpression> exprToRemove) {
            path.otherExprs.removeAll(exprToRemove);
            // Keep the eliminated expressions for cost estimating purpose
            path.eliminatedPostExprs.addAll(exprToRemove);
        }
    }
