/* This file is part of VoltDB.
 * Copyright (C) 2008-2018 VoltDB Inc.
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
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.hsqldb_voltpatches.FunctionForVoltDB;
import org.json_voltpatches.JSONException;
import org.voltdb.VoltType;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.ColumnRef;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Index;
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
import org.voltdb.expressions.WindowFunctionExpression;
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

    /** The catalog's database object which contains tables and access path info */
    final Database m_db;

    /** Describes the specified and inferred partition context. */
    final StatementPartitioning m_partitioning;

    /**
     * A description of a possible error condition that is considered recoverable/recovered
     * by the act of generating a viable alternative plan. The error should only be acknowledged
     * if it contributed to the complete failure to plan the statement.
     */
    String m_recentErrorMsg;

    // This cached value saves work on the assumption that it is only used to return
    // final "leaf node" bindingLists that are never updated "in place",
    // but just get their contents dumped into a summary List that was created
    // inline and NOT initialized here.
    private final static List<AbstractExpression> s_reusableImmutableEmptyBinding =
        new ArrayList<>();

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

    SubPlanAssembler(Database db, AbstractParsedStmt parsedStmt, StatementPartitioning partitioning)
    {
        m_db = db;
        m_parsedStmt = parsedStmt;
        m_partitioning = partitioning;
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
     * @param table Table to generate access path for
     * @param joinExprs join expressions this table is part of
     * @param filterExprs filter expressions this table is part of
     * @param postExprs post expressions this table is part of
     * @return List of valid access paths
     */
    protected ArrayList<AccessPath> getRelevantAccessPathsForTable(StmtTableScan tableScan,
            List<AbstractExpression> joinExprs,
            List<AbstractExpression> filterExprs,
            List<AbstractExpression> postExprs) {
        ArrayList<AccessPath> paths = new ArrayList<>();
        List<AbstractExpression> allJoinExprs = new ArrayList<>();
        List<AbstractExpression> allExprs = new ArrayList<>();
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

        AccessPath naivePath = getRelevantNaivePath(allJoinExprs, filterExprs);
        paths.add(naivePath);

        Collection<Index> indexes = tableScan.getIndexes();
        for (Index index : indexes) {
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
            List<AbstractExpression> exactMatchCoveringExprs = null;
            boolean hasCoveredPredicate = false;
            String predicatejson = index.getPredicatejson();
            if (path == null) {
                if (predicatejson.isEmpty()) {
                    // Skip the uselessly irrelevant whole-table index.
                    continue;
                }
                exactMatchCoveringExprs = new ArrayList<>();
                hasCoveredPredicate = isPartialIndexPredicateCovered(
                            tableScan, allExprs,
                            predicatejson, exactMatchCoveringExprs);
                if ( ! hasCoveredPredicate) {
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
            }
            else {
                assert(path.index != null);
                assert(path.index == index);
                // This index on relevant column(s) may need to be rejected if
                // its predicate is not applicable.
                if ( ! predicatejson.isEmpty()) {
                    exactMatchCoveringExprs = new ArrayList<>();
                    hasCoveredPredicate = isPartialIndexPredicateCovered(
                            tableScan, allExprs, predicatejson, exactMatchCoveringExprs);
                    if ( ! hasCoveredPredicate) {
                        // Skip the index with the inapplicable predicate.
                        continue;
                    }
                }
            }

            assert(path != null);
            if (hasCoveredPredicate) {
                assert(exactMatchCoveringExprs != null);
                filterPostPredicateForPartialIndex(path,
                        exactMatchCoveringExprs);
            }
            if (postExprs != null) {
                path.joinExprs.addAll(postExprs);
            }
            paths.add(path);
        }

        return paths;
    }

    /**
     * Generate the naive (scan) pass given a join and filter expressions
     *
     * @param joinExprs join expressions
     * @param filterExprs filter expressions
     * @return Naive access path
     */
    protected static AccessPath getRelevantNaivePath(List<AbstractExpression> joinExprs, List<AbstractExpression> filterExprs) {
        AccessPath naivePath = new AccessPath();

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
    private static class IndexableExpression
    {
        // The matched expression, in its original form and
        // normalized so that its LHS is the part that matched the indexed expression.
        private final AbstractExpression m_originalFilter;
        private final ComparisonExpression m_filter;
        // The parameters, if any, that must be bound to enable use of the index
        // -- these have no effect on the current query,
        // but they effect the applicability of the resulting cached plan to other queries.
        private final List<AbstractExpression> m_bindings;

        public IndexableExpression(AbstractExpression originalExpr, ComparisonExpression normalizedExpr,
                                   List<AbstractExpression> bindings)
        {
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
    };

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
     * @return TRUE if the index has a predicate that is completely covered by the query expressions.
     */
    public static boolean isPartialIndexPredicateCovered(StmtTableScan tableScan,
            List<AbstractExpression> coveringExprs,
            String predicatejson,
            List<AbstractExpression> exactMatchCoveringExprs) {
        assert ( ! predicatejson.isEmpty());
        AbstractExpression indexPredicate = null;
        try {
            indexPredicate = AbstractExpression.fromJSONString(predicatejson, tableScan);
        } catch (JSONException e) {
            e.printStackTrace();
            assert(false);
            return false;
        }
        List<AbstractExpression> exprsToCover = ExpressionUtil.uncombinePredicate(indexPredicate);

        for (AbstractExpression coveringExpr : coveringExprs) {
            if (exprsToCover.isEmpty()) {
                // We are done there. All the index predicate expressions are covered.
                break;
            }

            // Each covering expression and its reversed copy need to be tested for the index expression coverage.
            AbstractExpression reversedCoveringExpr = null;
            ExpressionType reverseCoveringType = ComparisonExpression.reverses.get(coveringExpr.getExpressionType());
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
        exprsToCover = removeNotNullCoveredExpressions(tableScan, coveringExprs, exprsToCover);

        // All index predicate expressions must be covered for index to be selected
        return exprsToCover.isEmpty();
    }

    /**
     * Given a table, a set of predicate expressions and a specific index, find the best way to
     * access the data using the given index, or return null if no good way exists.
     *
     * @param table The table we want data from.
     * @param exprs The set of predicate expressions.
     * @param index The index we want to use to access the data.
     * @return A valid access path using the data or null if none found.
     */
    protected AccessPath getRelevantAccessPathForIndex(StmtTableScan tableScan, List<AbstractExpression> exprs, Index index)
    {
        if (tableScan instanceof StmtTargetTableScan == false) {
            return null;
        }

        // Copy the expressions to a new working list that can be culled as filters are processed.
        List<AbstractExpression> filtersToCover = new ArrayList<>(exprs);

        boolean indexIsGeographical;
        String exprsjson = index.getExpressionsjson();
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
                e.printStackTrace();
                assert(false);
                return null;
            }
            indexIsGeographical = isAGeoExpressionIndex(indexedExprs);
        }

        AccessPath retval = new AccessPath();
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
        int orderSpoilers[] = new int[keyComponentCount];
        List<AbstractExpression> bindingsForOrder = new ArrayList<>();
        int nSpoilers = determineIndexOrdering(tableScan,
                                               keyComponentCount,
                                               indexedExprs,
                                               indexedColRefs,
                                               retval,
                                               orderSpoilers,
                                               bindingsForOrder);

        // Use as many covering indexed expressions as possible to optimize comparator expressions that can use them.

        // Start with equality comparisons on as many (prefix) indexed expressions as possible.
        int coveredCount = 0;
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
        int nRecoveredSpoilers = 0;
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

        for ( ; (coveredCount < keyComponentCount) && ! filtersToCover.isEmpty(); ++coveredCount) {
            if (indexedExprs == null) {
                coveringColId = indexedColIds[coveredCount];
            } else {
                coveringExpr = indexedExprs.get(coveredCount);
            }
            // Equality filters get first priority.
            boolean allowIndexedJoinFilters = (inListExpr == null);
            IndexableExpression eqExpr = getIndexableExpressionFromFilters(
                // NOT DISTINCT can be also considered as an equality comparison.
                // The only difference is that NULL is not distinct from NULL, but NULL != NULL. (ENG-11096)
                ExpressionType.COMPARE_EQUAL, ExpressionType.COMPARE_NOTDISTINCT,
                coveringExpr, coveringColId, tableScan, filtersToCover,
                allowIndexedJoinFilters, EXCLUDE_FROM_POST_FILTERS);

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
            // the MaterializedScan. This happens in getIndexAccessPlanForTable.
            retval.endExprs.add(comparator);

            // If a provisional sort direction has been determined, the equality filter MAY confirm
            // that a "spoiler" index key component (one missing from the ORDER BY) is constant-valued
            // and so it can not spoil the scan result sort order established by other key components.
            // In this case, consider the spoiler recovered.
            if (nRecoveredSpoilers < nSpoilers &&
                orderSpoilers[nRecoveredSpoilers] == coveredCount) {
                // In the case of IN-LIST equality, the key component will not have a constant value.
                if (eqExpr != inListExpr) {
                    // One recovery closer to confirming the sort order.
                    ++nRecoveredSpoilers;
                }
            }
        }

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
        }

        if ( ! IndexType.isScannable(index.getType()) ) {
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
            assert(retval.sortDirection != SortDirectionType.INVALID); // There's an order to spoil.
            // Try to associate each skipped index key component with an equality filter.
            // If a key component equals a constant, its value can't actually spoil the ordering.
            // This extra checking is only needed when all of these conditions hold:
            //   -- There are three or more index key components.
            //   -- Two or more of them are in the ORDER BY clause
            //   -- One or more of them are "spoilers", i.e. are not in the ORDER BY clause.
            //   -- A "spoiler" falls between two non-spoilers in the index key component list.
            // e.g. "CREATE INDEX ... ON (A, B, C);" then "SELECT ... WHERE B=? ORDER BY A, C;"
            List<AbstractExpression> otherBindingsForOrder =
                recoverOrderSpoilers(orderSpoilers, nSpoilers, nRecoveredSpoilers,
                                     indexedExprs, indexedColIds,
                                     tableScan, filtersToCover);
            if (otherBindingsForOrder == null) {
                // Some order spoiler didn't have an equality filter.
                // Invalidate the provisional indexed ordering.
                retval.sortDirection = SortDirectionType.INVALID;
                retval.m_stmtOrderByIsCompatible = false;
                retval.m_windowFunctionUsesIndex = NO_INDEX_USE;
                bindingsForOrder.clear(); // suddenly irrelevant
            }
            else {
                // Any non-null bindings list, even an empty one,
                // denotes success -- all spoilers were equality filtered.
                bindingsForOrder.addAll(otherBindingsForOrder);
            }
        }

        IndexableExpression startingBoundExpr = null;
        IndexableExpression endingBoundExpr = null;
        if ( ! filtersToCover.isEmpty()) {
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
            }
            else {
                boolean allowIndexedJoinFilters = (inListExpr == null);

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
                }
                else {
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
                }
                else {
                    // Optimizable to use reverse scan.
                    // only do reverse scan optimization when no lowerBoundExpr and lookup type is either < or <=.
                    if (upperBoundComparator.getExpressionType() == ExpressionType.COMPARE_LESSTHAN) {
                        retval.lookupType = IndexLookupType.LT;
                    }
                    else {
                        assert upperBoundComparator.getExpressionType() == ExpressionType.COMPARE_LESSTHANOREQUALTO;
                        retval.lookupType = IndexLookupType.LTE;
                    }
                    // Unlike a lower bound, an upper bound does not automatically filter out nulls
                    // as required by the comparison filter, so construct a NOT NULL comparator and
                    // add to post-filter
                    // TODO: Implement an abstract isNullable() method on AbstractExpression and use
                    // that here to optimize out the "NOT NULL" comparator for NOT NULL columns
                    if (startingBoundExpr == null) {
                        AbstractExpression newComparator = new OperatorExpression(ExpressionType.OPERATOR_NOT,
                                new OperatorExpression(ExpressionType.OPERATOR_IS_NULL), null);
                        newComparator.getLeft().setLeft(upperBoundComparator.getLeft());
                        newComparator.finalizeValueTypes();
                        retval.otherExprs.add(newComparator);
                    }
                    else {
                        int lastIdx = retval.indexExprs.size() -1;
                        retval.indexExprs.remove(lastIdx);

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
                if (retval.endExprs.size() == 0) { // no prefix equality filters
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
                }
                else {
                    // there are prefix equality filters -- possible for a reverse scan?

                    // set forward scan.
                    retval.sortDirection = SortDirectionType.INVALID;

                    // Turn this part on when we have EE support for reverse scan with query GT and GTE.
                    /*
                    boolean isReverseScanPossible = true;
                    if (filtersToCover.size() > 0) {
                        // Look forward to see the remainning filters.
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

        // index not relevant to expression
        if (retval.indexExprs.size() == 0 &&
            retval.endExprs.size() == 0 &&
            retval.sortDirection == SortDirectionType.INVALID) {
            return null;
        }

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
        if (retval.sortDirection != SortDirectionType.INVALID) {
            retval.bindings.addAll(bindingsForOrder);
        }
        return retval;
    }

    private void correctAccessPathForPrefixKeyCoverage(AccessPath retval,
            IndexableExpression startingBoundExpr) {
        // If IndexUseType has the default value of COVERING_UNIQUE_EQUALITY, then the
        // scan can use GTE instead to match all values, not only the null values, for the
        // unfiltered components -- assuming that any value is considered >= null.
        if (retval.use == IndexUseType.COVERING_UNIQUE_EQUALITY) {
            correctEqualityForPrefixKey(retval);
            return;
        }

        // GTE scans can have any number of null key components appended without changing
        // the effective value. So, that leaves GT scans.
        if (retval.lookupType == IndexLookupType.GT) {
            correctForwardScanForPrefixKey(retval, startingBoundExpr);
        }
    }

    private void correctEqualityForPrefixKey(AccessPath retval) {
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
        }
        else {
            retval.lookupType = IndexLookupType.GTE;
        }
    }

    private void correctForwardScanForPrefixKey(AccessPath retval,
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
        assert indexedExprs == null; // geo expressions not yet supported
        assert indexedColRefs != null; // for now a geo COLUMN is required.
        assert isAGeoColumnIndex(indexedColRefs);
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
            }
            if (filter.getValueType() != VoltType.BOOLEAN) {
                continue;
            }
            List<AbstractExpression> args = filter.getArgs();
            if (args.size() != 2) {
                continue;
            }
            FunctionExpression fn = (FunctionExpression) filter;
            //TODO: also support explicit APPROX_CONTAINS
            if ( ! fn.hasFunctionId(FunctionForVoltDB.FUNC_VOLT_ID_FOR_CONTAINS) ) {
                continue;
            }

            AbstractExpression indexableArg = args.get(0);
            assert indexableArg instanceof TupleValueExpression;
            assert indexableArg.getValueType() == VoltType.GEOGRAPHY;
            TupleValueExpression geoTve = (TupleValueExpression) indexableArg;
            if (! tableAlias.equals(geoTve.getTableAlias())) {
                continue;
            }
            if (coveringColId != geoTve.getColumnIndex()) {
                continue;
            }

            AbstractExpression searchKeyArg = args.get(1);
            assert searchKeyArg.getValueType() == VoltType.GEOGRAPHY_POINT;
            // Search key operand must not be from the same table,
            // e.g. contains(t.a, t.b) is not indexable.
            if (isOperandDependentOnTable(searchKeyArg, tableScan)) {
                continue;
            }

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
        return null;
    }

    private static boolean isAGeoColumnIndex(List<ColumnRef> indexedColRefs) {
        // Initially, geographical indexing only supports a single indexed column of type geography.
        if (indexedColRefs.size() != 1) {
            return false;
        }
        Column geoCol = indexedColRefs.get(0).getColumn();
        return geoCol.getType() == VoltType.GEOGRAPHY.getValue();
    }

    private static boolean isAGeoExpressionIndex(List<AbstractExpression> indexedExprs) {
        // Currently, geo indexes are strictly column-based,
        // never expression/function based.
        // This could change profoundly in the future if we use pseudo-function
        // expression syntax to configure each index.
        return false;
    }

    /*
     * Try to use the index scan's inherent ordering to implement some ORDER BY
     * or WINDOW FUNCTION clauses.
     *
     * These clauses can be statement level ORDER BY clauses or else
     * window function PARTITION BY and ORDER BY specifications.
     * For example, if a table has a tree index on columns "(A, B, C)", then
     * "ORDER BY A", "ORDER BY A, B" and "ORDER BY A, B, C" are considered a match
     * but NOT "ORDER BY A, C", "ORDER BY A, D", "ORDER BY A, B, C, D",
     * "ORDER BY B" or "ORDER BY B, A".
     *
     * Similarly, these window functions would match:
     * <ul>
     *   <li>"MIN(E) OVER (PARTITION BY A, B ORDER BY C)"</li>
     *   <li>"MIN(E) OVER (PARTITION BY A ORDER BY B)"</br>
     *       The order expressions don't need to use all the
     *       index expressions.
     *   </li>
     *   <li>"MIN(E) OVER (PARTITION BY B, A ORDER BY B)"</li>
     *       We don't care about the order of PARTITION BY expressions.
     * </ul>
     * These, however, would not match.
     * <ul>
     *   <li>"MIN(E) OVER (PARTITION BY A, C ORDER BY B)"</br>
     *       Index expressions must match all the PARTITION BY
     *       expressions before they match any ORDER BY
     *       expressions.  They can't match B until they match
     *       A and C.
     *   </li>
     *   <li>"MIN(E) OVER (PARTITION BY A, D ORDER BY B)"</br>
     *       All partition by expressions must appear in the
     *       index.
     *   </li>
     * </ul>
     *
     * We do this selection for a particular index by iterating over
     * the index's expressions or column references.  We try to match
     * the expression-or-column-reference to each candidate window function
     * or statement level order by expression sequence.  We keep a
     * scoreboard with one score for each candidate.  If a candidate
     * peters out because it doesn't match at all, we mark it dead.
     * If a candidate runs out of expressions, and the sort orders
     * are sensible, it is marked done and is usable.  We currently
     * support only one window function for a select statement.  But the
     * same set of matching logic applies to window functions and any statement
     * level order by clause, so generalizing to multiple window functions
     * is essentially cost free.
     *
     * TODO: In theory, we COULD leverage index ordering when the index covers only a prefix of
     * the ordering requirement list, such as the "ORDER BY A, B, C, D" case listed above.
     * But that still requires an ORDER BY plan node.
     * To gain any substantial advantage, the ORDER BY plan node would have to be smart enough
     * to apply just an incremental "minor" sort on "C" to subsets of the result "grouped by"
     * equal A and B values.  The ORDER BY plan node is not yet that smart.
     * So, for now, this case is handled by tagging the index output as not sorted,
     * leaving the ORDER BY to do the full job.
     *
     * There are some additional considerations that might disqualify a match.
     * A match also requires that all columns are ordered in the same direction.
     * For example, if a table has a tree index on columns "(A, B, C)", then
     * "ORDER BY A, B" or "ORDER BY A DESC, B DESC, C DESC" are considered a match
     * but not "ORDER BY A, B DESC" or "ORDER BY A DESC, B".
     *
     * TODO: Currently only ascending key index definitions are supported
     * -- the DESC keyword is not supported in the index creation DDL.
     * If that is ever enabled, the checks here may need to be generalized
     * to maintain the current level of support for only exact matching or
     * "exact reverse" matching of the ASC/DESC qualifiers on all columns,
     * but no other cases.
     *
     * Caveat: "Reverse scans", that is, support for descending ORDER BYs using ascending key
     * indexes only work when the index scan can start at the very end of the index
     * (to work backwards).
     * That means no equality conditions or upper bound conditions can be allowed that would
     * interfere with the start of the backward scan.
     * To minimize re-work, those query qualifications are not checked here.
     * It is easier to tentatively claim the reverse sort order of the index output, here,
     * and later invalidate that sortDirection upon detecting that the reverse scan
     * is not supportable.
     *
     * Some special cases are supported in addition to the simple match of all the ORDER BY "columns":
     *
     * It is possible for an ORDER BY "column" to have a parameterized form that neither strictly
     * equals nor contradicts an index key component.
     * For example, an index might be defined on a particular character of a column "(substr(A, 1, 1))".
     * This trivially matches "ORDER BY substr(A, 1, 1)".
     * It trivially refuses to match "ORDER BY substr(A, 2, 1)" or even "ORDER BY substr(A, 2, ?)"
     * The more interesting case is "ORDER BY substr(A, ?, 1)" where a match
     * must be predicated on the user specifying the correct value "1" for the parameter.
     * This is handled by allowing the optimization but by generating and returning a
     * "parameter binding" that describes its inherent usage restriction.
     * Such parameterized plans are used for ad hoc statements only; it is easy to validate
     * immediately that they have been passed the correct parameter value.
     * Compiled stored procedure statements need to be more specific about constants
     * used in index expressions, even if that means compiling a separate statement with a
     * constant hard-coded in the place of the parameter to purposely match the indexed expression.
     *
     * It is possible for an index key to contain extra components that do not match the
     * ORDER BY columns and yet do not interfere with the intended ordering for a particular query.
     * For example, an index on "(A, B, C, D)" would not generally be considered a match for
     * "ORDER BY A, D" but in the narrow context of a query that also includes a clause like
     * "WHERE B = ? AND C = 1", the ORDER BY clause is functionally equivalent to
     * "ORDER BY A, B, C, D" so it CAN be considered a match.
     * This case is supported in 2 phases, one here and a later one in
     * getRelevantAccessPathForIndex as follows:
     * As long as each ORDER BY column is eventually found in the index key components
     * (in its correct major-to-minor order and ASC/DESC direction),
     * the positions of any non-matching key components that had to be
     * skipped are simply collected in order into an array of "orderSpoilers".
     * The index ordering is provisionally considered valid.
     * Later, in the processing of getRelevantAccessPathForIndex,
     * failure to find an equality filter for one of these "orderSpoilers"
     * causes an override of the provisional sortDirection established here.
     *
     * In theory, a similar (arguably less probable) case could arise in which the ORDER BY columns
     * contain values that are constrained by the WHERE clause to be equal to constants or parameters
     * and the other ORDER BY columns match the index key components in the usual way.
     * Such a case will simply fail to match, here, possibly resulting in suboptimal plans that
     * make unneccesary use of ORDER BY plan nodes, and possibly even use sequential scan plan nodes.
     * The rationale for not complicating this code to handle that case is that the case should be
     * detected by a statement pre-processor that simplifies the ORDER BY clause prior to any
     * "scan planning".
     *
     *TODO: Another case not accounted for is an ORDER BY list that uses a combination of
     * columns/expressions from different tables -- the most common missed case would be
     * when the major ORDER BY columns are from an outer table (index scan) of a join (NLIJ)
     * and the minor columns from its inner table index scan.
     * This would have to be detected from a wider perspective than that of a single table/index.
     * For now, there is some wasted effort in the join case, as this sort order determination is
     * carefully done for each scan in a join, but the result for all of them is ignored because
     * they are never at the top of the plan tree -- the join is there.
     * In theory, if the left-most child scan of a join tree
     * is an index scan with a valid sort order,
     * that should be enough to avoid an explicit sort.
     * Also, if one or more left-most child scans in a join tree
     * are constrained so that they are known to produce a single row result
     * AND the next-left-most child scan is an index scan with a valid sort order,
     * the explicit sort can be skipped.
     * So, the effort to determine the sort direction of an index scan that participates in a join
     * is currently ALWAYS wasted, and in the future, would continue to be wasted effort for the
     * majority of index scans that do not fall into one of the narrow special cases just described.
     */
    /**
     * An index can hold either an expression or a column reference.
     * This class tries to hide the difference between them for the
     * purposes of index selection.
     */
    private static class ExpressionOrColumn {
        // Exactly one of these can be null.
        AbstractExpression m_expr;
        ColumnRef          m_colRef;
        // If m_colRef is non-null, then m_tableScan must
        // be non-null and name the table scan.
        StmtTableScan      m_tableScan;
        // This is the expression or column position of this expression or
        // ColumnRef in the candidate index.
        int                m_indexKeyComponentPosition;
        // This is the sort direction of a statement level
        // order by.  If there is no statement level order
        // by clause this will be SortDirectionType.INVALID.
        public SortDirectionType m_sortDirection;

        ExpressionOrColumn(int indexEntryNumber, AbstractExpression expr, SortDirectionType sortDir) {
            this(indexEntryNumber, null, expr, sortDir, null);
        }

        ExpressionOrColumn(int aIndexEntryNumber,
                           StmtTableScan tableScan,
                           AbstractExpression expr,
                           SortDirectionType sortDir,
                           ColumnRef colRef) {
            // Exactly one of expr or colRef can be null.
            assert((expr == null) == (colRef != null));
            assert(colRef == null || tableScan != null);
            m_expr = expr;
            m_colRef = colRef;
            m_indexKeyComponentPosition = aIndexEntryNumber;
            m_tableScan = tableScan;
            m_sortDirection = sortDir;
        }

        @Override
        public boolean equals(Object otherObj) {
            if ( ! ( otherObj instanceof ExpressionOrColumn) ) {
                return false;
            }
            ExpressionOrColumn other = (ExpressionOrColumn)otherObj;
            //
            // The function findBindingsForOneIndexedExpression is almost
            // like equality, but it matches parameters specially.  It's
            // exactly what we want here, but we have to make sure the
            // ExpressionOrColumn from the index is the second parameter.
            //
            // If both are from the same place, then we reduce this to
            // equals.  I'm not sure this is possible, but better safe
            // than sorry.
            //
            if ((other.m_indexKeyComponentPosition < 0 && m_indexKeyComponentPosition < 0)
                    || (0 <= other.m_indexKeyComponentPosition && 0 <= m_indexKeyComponentPosition)) {
                // If they are both expressions, they must be equal.
                if ((other.m_expr != null) && (m_expr != null)) {
                    return m_expr.equals(other.m_expr);
                }
                // If they are both column references they must be equal.
                if ((other.m_colRef != null) && (m_colRef != null)) {
                    return m_colRef.equals(other.m_colRef);
                }
                // If they are mixed, sort out which is the column
                // reference and which is the expression.
                AbstractExpression expr = (m_expr != null) ? m_expr : other.m_expr;
                ColumnRef cref = (m_colRef != null) ? m_colRef : other.m_colRef;
                StmtTableScan tscan = (m_tableScan != null) ? m_tableScan : other.m_tableScan;
                assert(expr != null && cref != null);
                // Use the same matcher that findBindingsForOneIndexedExpression
                // uses.
                return matchExpressionAndColumnRef(expr, cref, tscan);
            }
            ExpressionOrColumn fromStmt = (m_indexKeyComponentPosition < 0) ? this : other;
            ExpressionOrColumn fromIndx = (m_indexKeyComponentPosition < 0) ? other : this;
            return (findBindingsForOneIndexedExpression(fromStmt, fromIndx) != null);
        }

    }

    /**
     * These are some constants for using indexes for window functions.
     */
    public final static int NO_INDEX_USE = -2;
    public final static int STATEMENT_LEVEL_ORDER_BY_INDEX = -1;

    /** When a match is attempted, it may match,
     * fail to match or else be a possible order spoiler.
     *
     */
    private enum MatchResults {
        MATCHED,                 /**
                                  * The attempt matched.
                                  */
        POSSIBLE_ORDER_SPOILER,  /**
                                  * The attempt did not match,
                                  * but this may be an order spoiler.
                                  */
        DONE_OR_DEAD             /**
                                  * The score is dead or done.
                                  * So don't do anything else with
                                  * this score.
                                  */
    }
    /**
     * A WindowFunctionScore keeps track of the score of
     * a single window function or else the statement level
     * order by expressions.
     */
    static class WindowFunctionScore {

        /**
         * A constructor for creating a score from a
         * WindowFunctionExpression.
         *
         * @param winfunc
         * @param tableAlias
         */
        public WindowFunctionScore(WindowFunctionExpression winfunc,
                                   int winFuncNum) {
            for (int idx = 0; idx < winfunc.getPartitionbySize(); idx += 1) {
                AbstractExpression ae = winfunc.getPartitionByExpressions().get(idx);
                m_partitionByExprs.add(new ExpressionOrColumn(-1, ae, SortDirectionType.INVALID));
            }
            for (int idx = 0; idx < winfunc.getOrderbySize(); idx += 1) {
                AbstractExpression ae = winfunc.getOrderByExpressions().get(idx);
                SortDirectionType sd = winfunc.getOrderByDirections().get(idx);
                m_unmatchedOrderByExprs.add(new ExpressionOrColumn(-1, ae, sd));
            }
            assert(0 <= winFuncNum);
            m_windowFunctionNumber = winFuncNum;
            m_sortDirection = SortDirectionType.INVALID;
        }

        /**
         * A constructor for creating a score from a
         * statement level order by expression.
         *
         * @param orderByExpressions The order by expressions.
         */
        public WindowFunctionScore(List<ParsedColInfo> orderByExpressions) {
            for (ParsedColInfo pci : orderByExpressions) {
                SortDirectionType sortDir = pci.m_ascending ? SortDirectionType.ASC : SortDirectionType.DESC;
                m_unmatchedOrderByExprs.add(new ExpressionOrColumn(-1, pci.m_expression, sortDir));
            }
            // Statement level order by expressions are number STATEMENT_LEVEL_ORDER_BY_INDEX.
            m_windowFunctionNumber = STATEMENT_LEVEL_ORDER_BY_INDEX;
            m_sortDirection = SortDirectionType.INVALID;
        }

        // Set of active partition by expressions
        // for this window function.
        List<ExpressionOrColumn> m_partitionByExprs = new ArrayList<>();
        // Sequence of expressions which
        // either match index expression or which have
        // single values.   These are from the partition
        // by list or else from the order by list.  At
        // the end this will be the concatenation of the
        // partition by list and the order by list.
        final List<AbstractExpression> m_orderedMatchingExpressions = new ArrayList<>();
        // List of order by expressions, originally from the
        // WindowFunctionExpression.  These migrate to
        // m_orderedMatchingExpressions as they match.
        final List<ExpressionOrColumn> m_unmatchedOrderByExprs = new ArrayList<>();
        // This is the index of the unmatched expression
        // we will be working on.
        int m_unmatchedOrderByCursor = 0;
        // This is the number of the window function.  It is
        // STATEMENT_LEVEL_ORDER_BY for the statement level order by list.
        int m_windowFunctionNumber;

        // A score can be dead, done or in progress.  Dead means
        // We have seen something we cannot match.  Done means we
        // have matched all the score's expressions.  In progress
        // means we are still matching expressions.
        private enum MatchingState {
            INVALID,
            INPROGRESS,
            DEAD,
            DONE
        }

        MatchingState m_matchingState = MatchingState.INPROGRESS;
        // This is the sort direction for this window function
        // or order by list.
        private SortDirectionType m_sortDirection = SortDirectionType.INVALID;
        // This is the set of bindings generated by matches in this
        // candidate.
        final List<AbstractExpression> m_bindings = new ArrayList<>();

        public int getNumberMatches() {
            return m_orderedMatchingExpressions.size();
        }

        public boolean isDead() {
            return m_matchingState == MatchingState.DEAD;
        }

        public boolean isDone() {
            if (m_matchingState == MatchingState.DONE) {
                return true;
            }
            if (m_partitionByExprs.isEmpty() && 0 == m_unmatchedOrderByExprs.size()) {
                markDone();
                // Settle on a sort direction.
                if (m_sortDirection == SortDirectionType.INVALID) {
                    m_sortDirection = SortDirectionType.ASC;
                }
                return true;
            }
            return false;
        }

        public MatchResults matchIndexEntry(ExpressionOrColumn indexEntry) {
            int idx;
            // Don't bother to do anything if
            // we are dead or done.
            if (isDead() || isDone()) {
                return MatchResults.DONE_OR_DEAD;
            }
            // If there are more partition by expressions, then
            // find one which matches the indexEntry and move it
            // to the end of the ordered  matching expressions.
            if ( ! m_partitionByExprs.isEmpty() ) {
                List<AbstractExpression> moreBindings = null;
                for (ExpressionOrColumn eorc : m_partitionByExprs) {
                    moreBindings = findBindingsForOneIndexedExpression(eorc, indexEntry);
                    if (moreBindings != null) {
                        // Good.  We matched.  Add the bindings.
                        // But we can't set the sort direction
                        // yet, because partition by doesn't
                        // care.  Note that eorc and indexEntry
                        // may not be equal.  They are just
                        // matching.  An expression in eorc
                        // might match a column reference in
                        // indexEntry, or eorc may have parameters
                        // which need to match expressions in indexEntry.
                        m_orderedMatchingExpressions.add(eorc.m_expr);
                        // If there are expressions later on in the
                        // m_partitionByExprs or m_unmatchedOrderByExprs
                        // which match this expression we need to
                        // delete them.  We can safely ignore them.
                        //
                        // If there are more than one instances of
                        // an expression, say with "PARTITION BY A, A",
                        // we need to remove them all.  But guard against
                        // an infinite loop here;
                        while (m_partitionByExprs.remove(eorc)) {
                            /* Do Nothing */;
                        }
                        m_bindings.addAll(moreBindings);
                        return MatchResults.MATCHED;
                    }
                }
                // Mark this as dead.  We are not going
                // to manage order spoilers with window functions.
                markDead();
                return MatchResults.DONE_OR_DEAD;
            }
            // If there are no partition by expressions,
            // we need to look at the unmatched order by expressions.
            // These need to be AbstractExpressions.  But we may
            // match with a ColumnRef in the index.
            ExpressionOrColumn nextStatementEOC = m_unmatchedOrderByExprs.get(0);
            // If we have not settled on a sort direction
            // yet, we have to decide now.
            if (m_sortDirection == SortDirectionType.INVALID) {
                m_sortDirection = nextStatementEOC.m_sortDirection;
            }
            if (nextStatementEOC.m_sortDirection != m_sortDirection) {
                // If the sort directions are not all
                // equal we can't use this index for this
                // candidate.  So just declare it dead.
                markDead();
                return MatchResults.DONE_OR_DEAD;
            }
            // NOTABENE: This is really the important part of
            //           this function.
            List<AbstractExpression> moreBindings
                = findBindingsForOneIndexedExpression(nextStatementEOC, indexEntry);
            if ( moreBindings != null ) {
                // We matched, because moreBindings != null, and
                // the sort direction matched as well.  So
                // add nextEOC to the order matching expressions
                // list and add the bindings to the bindings list.
                m_orderedMatchingExpressions.add(nextStatementEOC.m_expr);
                m_bindings.addAll(moreBindings);
                // Remove the next statement EOC from the unmatched OrderByExpressions
                // list since we matched it.  We need to remove all of them,
                while (m_unmatchedOrderByExprs.remove(nextStatementEOC)) {
                    /* Do Nothing */ ;
                }
                return MatchResults.MATCHED;
            }
            // No Bindings were found.  Mark this as a
            // potential order spoiler if it's in a
            // statement level order by.  The index entry
            // number is in the ordinal number of the
            // expression or column reference in the index.
            assert(0 <= indexEntry.m_indexKeyComponentPosition);
            if (isWindowFunction()) {
                // No order spoilers with window functions.
                markDead();
                return MatchResults.DONE_OR_DEAD;
            }
            return MatchResults.POSSIBLE_ORDER_SPOILER;
        }

        // Return true if this is a window function.  If it is a statement
        // level order by we return false.
        boolean isWindowFunction() {
            return 0 <= m_windowFunctionNumber;
        }

        public void markDone() {
            assert(m_matchingState == MatchingState.INPROGRESS);
            m_matchingState = MatchingState.DONE;
        }

        public void markDead() {
            assert(m_matchingState == MatchingState.INPROGRESS);
            m_matchingState = MatchingState.DEAD;
        }
    };

    /**
     * Objects of this class keep track of all window functions and
     * order by statement expressions.  We run the index expressions
     * over this scoreboard to see if any of them match appropriately.
     * If so we pull out the window function or statement level order by
     * which match.
     */
    class WindowFunctionScoreboard {
        public WindowFunctionScoreboard(AbstractParsedStmt stmt, StmtTableScan tableScan) {
            m_numWinScores = stmt.getWindowFunctionExpressionCount();
            m_numOrderByScores = stmt.hasOrderByColumns() ? 1 : 0;
            m_winFunctions = new WindowFunctionScore[m_numWinScores + m_numOrderByScores];
            for (int idx = 0; idx < m_numWinScores; idx += 1) {
                // stmt has to be a ParsedSelectStmt if 0 < m_numWinScores.
                // So this cast will be ok.
                ParsedSelectStmt pss = (ParsedSelectStmt)stmt;
                m_winFunctions[idx] = new WindowFunctionScore(pss.getWindowFunctionExpressions().get(idx),
                                                              idx);
            }
            if (m_numOrderByScores > 0) {
                m_winFunctions[m_numWinScores] = new WindowFunctionScore(stmt.orderByColumns());
            }
        }

        private WindowFunctionScore m_winFunctions[];
        private int m_numWinScores;
        private int m_numOrderByScores;
        private Set<Integer> m_orderSpoilers = new TreeSet<>();

        /*
         * Unfortunately the sort direction must be the
         * same for all the expressions or columns.
         */
        SortDirectionType m_sortDirection = SortDirectionType.INVALID;
        public boolean isDone() {
            for (WindowFunctionScore score : m_winFunctions) {
                if (!score.isDone()) {
                    return false;
                }
            }
            return true;
        }

        public boolean isDead(boolean windowFunctionsOnly) {
            for (WindowFunctionScore score : m_winFunctions) {
                if (windowFunctionsOnly && ( ! score.isWindowFunction())) {
                    continue;
                }
                if (!score.isDead()) {
                    return false;
                }
            }
            return true;
        }

        public void matchIndexEntry(ExpressionOrColumn indexEntry) {
            for (int idx = 0; idx < m_winFunctions.length; idx += 1) {
                WindowFunctionScore score = m_winFunctions[idx];
                MatchResults result = score.matchIndexEntry(indexEntry);
                // This can only happen with a statement level order by
                // clause.  We don't return POSSIBLE_ORDER_SPOILER for
                // window functions.
                if (result == MatchResults.POSSIBLE_ORDER_SPOILER) {
                    m_orderSpoilers.add(indexEntry.m_indexKeyComponentPosition);
                }
            }
        }

        /**
         * Return the number of order spoilers.  Also, fill in
         * the AccessPath, the order spoilers list and the bindings.
         *
         * We prefer window functions to statement level
         * order by expressions.  If there is more than one
         * window functions, we prefer the one with the least
         * number of order spoilers.  In the case of ties we
         * return the first one in the list, which is essentially
         * random.  Perhaps we can do better.
         *
         * @return The number of order spoilers in the candidate we choose.
         */
        public int getResult(AccessPath retval,
                             int orderSpoilers[],
                             List<AbstractExpression> bindings) {
            WindowFunctionScore answer = null;
            // Fill in the failing return values as a fallback.
            retval.bindings.clear();
            retval.m_windowFunctionUsesIndex = NO_INDEX_USE;
            retval.m_stmtOrderByIsCompatible = false;
            retval.sortDirection = SortDirectionType.INVALID;
            int numOrderSpoilers = 0;

            // The statement level order by expressions
            // are always the last in the list.  So, if there
            // are no window functions which match this
            // index, and the order by expressions match,
            // then the last time through this list will pick
            // the order by expressions.  If a window function
            // matches, we will pick it up first.  We
            // can only match the statement level order by
            // statements if on score from a window function
            // matches.
            for (WindowFunctionScore score : m_winFunctions) {
                if (score.isDone()) {
                    assert( ! score.isDead() );
                    // Prefer window functions, and prefer longer matches, but we'll
                    // take anything.
                    if ((answer == null)
                            || !answer.isWindowFunction()
                            || answer.getNumberMatches() < score.getNumberMatches()) {
                        answer = score;
                    }
                }
            }
            if (answer != null) {
                assert(answer.m_sortDirection != null);
                if (m_numOrderByScores > 0) {
                    assert(m_numOrderByScores + m_numWinScores <= m_winFunctions.length);
                    WindowFunctionScore orderByScore = m_winFunctions[m_numOrderByScores + m_numWinScores - 1];
                    assert(orderByScore != null);
                    // If the order by score is done and the
                    // sort directions match then this
                    // index may be usable for the statement level
                    // order by as well as for any
                    // window functions.
                    if ((orderByScore.m_sortDirection == answer.m_sortDirection)
                            && orderByScore.isDone()) {
                        retval.m_stmtOrderByIsCompatible = true;
                    } else {
                        retval.m_stmtOrderByIsCompatible = false;
                    }
               }
                if (answer.m_sortDirection != SortDirectionType.INVALID) {

                    // Mark how we are using this index.
                    retval.m_windowFunctionUsesIndex = answer.m_windowFunctionNumber;
                    // If we have an index for the Statement Level
                    // Order By clause but there is a window function
                    // that can't use the index, then we can't use the
                    // index at all. for ordering.  The window function
                    // will invalidate the ordering for the statment level
                    // order by clause.
                    if ((retval.m_windowFunctionUsesIndex == STATEMENT_LEVEL_ORDER_BY_INDEX)
                            && (0 < m_numWinScores)) {
                        retval.m_stmtOrderByIsCompatible = false;
                        retval.m_windowFunctionUsesIndex = NO_INDEX_USE;
                        retval.sortDirection = SortDirectionType.INVALID;
                        return 0;
                    }

                    // Add the bindings.
                    retval.bindings.addAll(answer.m_bindings);

                    // Mark the sort direction.
                    if (retval.m_windowFunctionUsesIndex == NO_INDEX_USE) {
                        retval.sortDirection = SortDirectionType.INVALID;
                    } else {
                        retval.sortDirection = answer.m_sortDirection;
                    }

                    // Add the order spoilers if the index is
                    // compatible with the statement level
                    // order by clause.
                    if (retval.m_stmtOrderByIsCompatible) {
                        assert(m_orderSpoilers.size() <= orderSpoilers.length);
                        int idx = 0;
                        for (Integer spoiler : m_orderSpoilers) {
                            orderSpoilers[idx++] = spoiler;
                        }
                        // We will return this.
                        numOrderSpoilers = m_orderSpoilers.size();
                    }
                    retval.m_finalExpressionOrder.addAll(answer.m_orderedMatchingExpressions);
                    // else numOrderSpoilers is already zero.
                }
            }
            return numOrderSpoilers;
        }
    }

    /**
     * Determine if an index, which is in the AccessPath argument
     * retval, can satisfy a parsed select statement's order by or
     * window function ordering requirements.
     *
     * @param table              only used here to validate base table names of ORDER BY columns' .
     * @param bindingsForOrder   restrictions on parameter settings that are prerequisite to the
     *                           any ordering optimization determined here
     * @param keyComponentCount  the length of indexedExprs or indexedColRefs,
     *                           ONE of which must be valid
     * @param indexedExprs       expressions for key components in the general case
     * @param indexedColRefs     column references for key components in the simpler case
     * @param retval the eventual result of getRelevantAccessPathForIndex,
     *               the bearer of a (tentative) sortDirection determined here
     * @param orderSpoilers      positions of key components which MAY invalidate the tentative
     *                           sortDirection
     * @param bindingsForOrder   restrictions on parameter settings that are prerequisite to the
     *                           any ordering optimization determined here.
     * @return the number of discovered orderSpoilers that will need to be recovered from,
     *         to maintain the established sortDirection - always 0 if no sort order was determined.
     */
    private int determineIndexOrdering(StmtTableScan tableScan,
            int keyComponentCount,
            List<AbstractExpression> indexedExprs,
            List<ColumnRef> indexedColRefs,
            AccessPath retval,
            int[] orderSpoilers,
            List<AbstractExpression> bindingsForOrder)
    {
        // Organize a little bit.
        ParsedSelectStmt pss = (m_parsedStmt instanceof ParsedSelectStmt)
                                  ? ((ParsedSelectStmt)m_parsedStmt)
                                  : null;
        boolean hasOrderBy = ( m_parsedStmt.hasOrderByColumns()
                                  && ( ! m_parsedStmt.orderByColumns().isEmpty() ) );
        boolean hasWindowFunctions = (pss != null && pss.hasWindowFunctionExpression());
        //
        // If we have no statement level order by or window functions,
        // then we can't use this index for ordering, and we
        // return 0.
        //
        if (! hasOrderBy && ! hasWindowFunctions) {
            return 0;
        }
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
        WindowFunctionScoreboard windowFunctionScores = new WindowFunctionScoreboard(m_parsedStmt, tableScan);
        // indexCtr is an index into the index expressions or columns.
        for (int indexCtr = 0; !windowFunctionScores.isDone() && indexCtr < keyComponentCount; indexCtr += 1) {
            // Figure out what to do with index expression or column at indexCtr.
            // First, fetch it out.
            AbstractExpression indexExpr = (indexedExprs == null) ? null : indexedExprs.get(indexCtr);
            ColumnRef indexColRef = (indexedColRefs == null) ? null : indexedColRefs.get(indexCtr);
            // Then see if it matches something.  If
            // this doesn't match one thing it may match
            // another.  If it doesn't match anything, it may
            // be an order spoiler, which we will maintain in
            // the scoreboard.
            windowFunctionScores.matchIndexEntry(new ExpressionOrColumn(indexCtr,
                                                                        tableScan,
                                                                        indexExpr,
                                                                        SortDirectionType.INVALID,
                                                                        indexColRef));
        }
        //
        // The result is the number of order spoilers, but
        // also the access path we have chosen, the order
        // spoilers themselves and the bindings.  Return these
        // by reference.
        //
        return windowFunctionScores.getResult(retval, orderSpoilers, bindingsForOrder);
    }


    /**
     * Match the indexEntry, which is from an index, with
     * a statement expression or column.  The nextStatementEOC
     * must be an expression, not a column reference.
     *
     * @param nextStatementEOC The expression or column in the SQL statement.
     * @param indexEntry The expression or column in the index.
     * @return A list of bindings for this match.  Return null if
     *         there is no match.  If there are no bindings but the
     *         expressions match, return an empty, non-null list.
     */
    private static List<AbstractExpression>
        findBindingsForOneIndexedExpression(ExpressionOrColumn nextStatementEOC,
                                            ExpressionOrColumn indexEntry) {
        assert(nextStatementEOC.m_expr != null);
        AbstractExpression nextStatementExpr = nextStatementEOC.m_expr;
        if (indexEntry.m_colRef != null) {
            ColumnRef indexColRef = indexEntry.m_colRef;
            // This is a column.  So try to match it
            // with the expression in nextStatementEOC.
            if (matchExpressionAndColumnRef(nextStatementExpr, indexColRef, indexEntry.m_tableScan)) {
                return s_reusableImmutableEmptyBinding;
            }
            return null;
        }
        // So, this index entry is an expression.
        List<AbstractExpression> moreBindings =
                nextStatementEOC.m_expr.bindingToIndexedExpression(indexEntry.m_expr);
        if (moreBindings != null) {
            return moreBindings;
        }
        return null;
    }

    private static boolean matchExpressionAndColumnRef(AbstractExpression statementExpr,
                                                       ColumnRef indexColRef,
                                                       StmtTableScan tableScan) {
        if (statementExpr instanceof TupleValueExpression) {
            TupleValueExpression tve = (TupleValueExpression)statementExpr;
            if (tve.getTableAlias().equals(tableScan.getTableAlias()) &&
                    tve.getColumnName().equals(indexColRef.getColumn().getTypeName())) {
                return true;
            }
        }
        return false;
    }

    /**
     * @param orderSpoilers  positions of index key components that would need to be
     *                       equality filtered to keep from interfering with the desired order
     * @param nSpoilers      the number of valid orderSpoilers
     * @param coveredCount   the number of prefix key components already known to be filtered --
     *                       orderSpoilers before this position are covered.
     * @param indexedExprs   the index key component expressions in the general case
     * @param colIds         the index key component columns in the simple case
     * @param tableScan      the index base table scan, used to validate column base tables
     * @param filtersToCover query conditions that may contain the desired equality filters
     */
    private static List<AbstractExpression> recoverOrderSpoilers(int[] orderSpoilers, int nSpoilers,
        int nRecoveredSpoilers,
        List<AbstractExpression> indexedExprs, int[] colIds,
        StmtTableScan tableScan, List<AbstractExpression> filtersToCover)
    {
        // Filters leveraged for an optimization, such as the skipping of an ORDER BY plan node
        // always risk adding a dependency on a particular parameterization, so be prepared to
        // add prerequisite parameter bindings to the plan.
        List<AbstractExpression> otherBindingsForOrder = new ArrayList<>();
        // Order spoilers must be recovered in the order they were found
        // for the index ordering to be considered acceptable.
        // Each spoiler key component is recovered by the detection of an equality filter on it.
        for (; nRecoveredSpoilers < nSpoilers; ++nRecoveredSpoilers) {
            // There may be more equality filters that weren't useful for "coverage"
            // but may still serve to recover an otherwise order-spoiling index key component.
            // The filter will only be applied as a post-condition,
            // but that's good enough to satisfy the ORDER BY.
            AbstractExpression coveringExpr = null;
            int coveringColId = -1;
            // This is a scaled down version of the coverage check in getRelevantAccessPathForIndex.
            // This version leaves intact any filter it finds,
            // so it will be picked up as a post-filter.
            if (indexedExprs == null) {
                coveringColId = colIds[orderSpoilers[nRecoveredSpoilers]];
            } else {
                coveringExpr = indexedExprs.get(orderSpoilers[nRecoveredSpoilers]);
            }
            // A key component filter based on another table's column is enough to maintain ordering
            // if this index is chosen, regardless of whether an NLIJ is injected for an IN LIST.
            boolean alwaysAllowConstrainingJoinFilters = true;

            List<AbstractExpression> moreBindings = null;
            IndexableExpression eqExpr = getIndexableExpressionFromFilters(
                ExpressionType.COMPARE_EQUAL, ExpressionType.COMPARE_EQUAL,
                coveringExpr, coveringColId, tableScan, filtersToCover,
                alwaysAllowConstrainingJoinFilters, KEEP_IN_POST_FILTERS);
            if (eqExpr == null) {
                return null;
            }
            // The equality filter confirms that the "spoiler" index key component
            // (one missing from the ORDER BY) is constant-valued,
            // so it can't spoil the scan result sort order established by other key components.
            moreBindings = eqExpr.getBindings();
            // Accumulate bindings (parameter constraints) across all recovered spoilers.
            otherBindingsForOrder.addAll(moreBindings);
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
        List<AbstractExpression> filtersToCover,
        boolean allowIndexedJoinFilters, boolean filterAction)
    {
        List<AbstractExpression> binding = null;
        AbstractExpression indexableExpr = null;
        AbstractExpression otherExpr = null;
        ComparisonExpression normalizedExpr = null;
        AbstractExpression originalFilter = null;
        for (AbstractExpression filter : filtersToCover) {

            // ENG-8203: Not going to try to use index with sub-query expression
            if (filter.hasSubquerySubexpression()) {
                // Including RowSubqueryExpression and SelectSubqueryExpression
                // SelectSubqueryExpression also can be scalar sub-query
                continue;
            }

            // Expression type must be resolvable by an index scan
            if ((filter.getExpressionType() == targetComparator) ||
                (filter.getExpressionType() == altTargetComparator)) {
                normalizedExpr = (ComparisonExpression) filter;
                indexableExpr = filter.getLeft();
                otherExpr = filter.getRight();
                binding = bindingIfValidIndexedFilterOperand(tableScan, indexableExpr, otherExpr,
                                                             coveringExpr, coveringColId);
                if (binding != null) {
                    if ( ! allowIndexedJoinFilters) {
                        if (otherExpr.hasTupleValueSubexpression()) {
                            // This filter can not be used with the index, possibly due to interactions
                            // wih IN LIST processing that would require a three-way NLIJ.
                            binding = null;
                            continue;
                        }
                    }
                    // Additional restrictions apply to LIKE pattern arguments
                    if (targetComparator == ExpressionType.COMPARE_LIKE) {
                        if (otherExpr instanceof ParameterValueExpression) {
                            ParameterValueExpression pve = (ParameterValueExpression)otherExpr;
                            // Can't use an index for parameterized LIKE filters,
                            // e.g. "T1.column LIKE ?"
                            // UNLESS the parameter was artificially substituted
                            // for a user-specified constant AND that constant was a prefix pattern.
                            // In that case, the parameter has to be added to the bound list
                            // for this index/statement.
                            ConstantValueExpression cve = pve.getOriginalValue();
                            if (cve == null || ! cve.isPrefixPatternString()) {
                                binding = null; // the filter is not usable, so the binding is invalid
                                continue;
                            }
                            // Remember that the binding list returned by
                            // bindingIfValidIndexedFilterOperand above
                            // is often a "shared object" and is intended to be treated as immutable.
                            // To add a parameter to it, first copy the List.
                            List<AbstractExpression> moreBinding =
                                new ArrayList<>(binding);
                            moreBinding.add(pve);
                            binding = moreBinding;
                        } else if (otherExpr instanceof ConstantValueExpression) {
                            // Can't use an index for non-prefix LIKE filters,
                            // e.g. " T1.column LIKE '%ish' "
                            ConstantValueExpression cve = (ConstantValueExpression)otherExpr;
                            if ( ! cve.isPrefixPatternString()) {
                                // The constant is not an index-friendly prefix pattern.
                                binding = null; // the filter is not usable, so the binding is invalid
                                continue;
                            }
                        } else {
                            // Other cases are not indexable, e.g. " T1.column LIKE T2.column "
                            binding = null; // the filter is not usable, so the binding is invalid
                            continue;
                        }
                    }
                    if (targetComparator == ExpressionType.COMPARE_IN) {
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
                        }
                        if (otherExpr instanceof ParameterValueExpression) {
                            // It's OK to use an index for a parameterized IN filter,
                            // e.g. "T1.column IN ?"
                            // EVEN if the parameter was -- someday -- artificially substituted
                            // for an entire user-specified list of constants.
                            // As of now, that is beyond the capabilities of the ad hoc statement
                            // parameterizer, so "T1.column IN (3, 4)" can use the plan for
                            // "T1.column IN (?, ?)" that might have been originally cached for
                            // "T1.column IN (1, 2)" but "T1.column IN (1, 2, 3)" would need its own
                            // "T1.column IN (?, ?, ?)" plan, etc. per list element count.
                        }
                        //TODO: Some day, there may be an optimization here that allows an entire
                        // IN LIST of constants to be serialized as a single value instead of a
                        // VectorValue composed of ConstantValue arguments.
                        // What's TBD is whether that would get its own AbstractExpression class or
                        // just be a special case of ConstantValueExpression.
                        else {
                            assert (otherExpr instanceof VectorValueExpression);
                        }
                    }
                    originalFilter = filter;
                    if (filterAction == EXCLUDE_FROM_POST_FILTERS) {
                        filtersToCover.remove(filter);
                    }
                    break;
                }
            }
            if ((filter.getExpressionType() == ComparisonExpression.reverses.get(targetComparator)) ||
                (filter.getExpressionType() == ComparisonExpression.reverses.get(altTargetComparator))) {
                normalizedExpr = (ComparisonExpression) filter;
                normalizedExpr = normalizedExpr.reverseOperator();
                indexableExpr = filter.getRight();
                otherExpr = filter.getLeft();
                binding = bindingIfValidIndexedFilterOperand(tableScan, indexableExpr, otherExpr,
                                                             coveringExpr, coveringColId);
                if (binding != null) {
                    if ( ! allowIndexedJoinFilters) {
                        if (otherExpr.hasTupleValueSubexpression()) {
                            // This filter can not be used with the index, probably due to interactions
                            // with IN LIST processing of another key component that would require a
                            // three-way NLIJ to be injected.
                            binding = null;
                            continue;
                        }
                    }
                    originalFilter = filter;
                    if (filterAction == EXCLUDE_FROM_POST_FILTERS) {
                        filtersToCover.remove(filter);
                    }
                    break;
                }
            }
        }

        if (binding == null) {
            // ran out of candidate filters.
            return null;
        }
        return new IndexableExpression(originalFilter, normalizedExpr, binding);
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

        boolean hasMatch = false;
        Iterator<AbstractExpression> iter = exprsToCover.iterator();
        while(iter.hasNext()) {
            AbstractExpression exprToCover = iter.next();
            if (coveringExpr.bindingToIndexedExpression(exprToCover) != null) {
                iter.remove();
                hasMatch = true;
                // need to keep going to remove all matches
            }
        }
        return hasMatch;
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
    private static List<AbstractExpression> removeNotNullCoveredExpressions(StmtTableScan tableScan, List<AbstractExpression> coveringExprs, List<AbstractExpression> exprsToCover) {
        // Collect all TVEs from NULL-rejecting covering expressions
        Set<TupleValueExpression> coveringTves = new HashSet<>();
        for (AbstractExpression coveringExpr : coveringExprs) {
            if (ExpressionUtil.isNullRejectingExpression(coveringExpr, tableScan.getTableAlias())) {
                coveringTves.addAll(ExpressionUtil.getTupleValueExpressions(coveringExpr));
            }
        }
        // For each NOT NULL expression to cover extract the TVE expressions. If all of them are also part
        // of the covering NULL-rejecting collection then this NOT NULL expression is covered
        Iterator<AbstractExpression> iter = exprsToCover.iterator();
        while (iter.hasNext()) {
            AbstractExpression filter = iter.next();
            if (ExpressionType.OPERATOR_NOT == filter.getExpressionType()) {
                assert(filter.getLeft() != null);
                if (ExpressionType.OPERATOR_IS_NULL == filter.getLeft().getExpressionType()) {
                    assert(filter.getLeft().getLeft() != null);
                    List<TupleValueExpression> tves = ExpressionUtil.getTupleValueExpressions(filter.getLeft().getLeft());
                    if (coveringTves.containsAll(tves)) {
                        iter.remove();
                    }
                }
            }
        }
        return exprsToCover;
    }

    private static boolean isOperandDependentOnTable(AbstractExpression expr, StmtTableScan tableScan) {
        for (TupleValueExpression tve : ExpressionUtil.getTupleValueExpressions(expr)) {
            if (tableScan.getTableAlias().equals(tve.getTableAlias())) {
                return true;
            }
        }
        return false;
    }

    private static List<AbstractExpression> bindingIfValidIndexedFilterOperand(StmtTableScan tableScan,
        AbstractExpression indexableExpr, AbstractExpression otherExpr,
        AbstractExpression coveringExpr, int coveringColId)
    {
        // Do some preliminary disqualifications.

        VoltType keyType = indexableExpr.getValueType();
        VoltType otherType = otherExpr.getValueType();
        // EE index key comparator should not lose precision when casting keys to the indexed type.
        // Do not choose an index that requires such a cast.
        if ( ! keyType.canExactlyRepresentAnyValueOf(otherType)) {
            // Except the EE DOES contain the necessary logic to avoid loss of SCALE
            // when the indexed type is just a narrower integer type.
            // This is very important, since the typing for integer constants
            // MAY not pay that much attention to minimizing scale.
            // This was behind issue ENG-4606 -- failure to index on constant equality.
            // So, accept any pair of integer types.
            if ( ! (keyType.isBackendIntegerType() && otherType.isBackendIntegerType()))  {
                return null;
            }
        }
        // Left and right operands must not be from the same table,
        // e.g. where t.a = t.b is not indexable with the current technology.
        if (isOperandDependentOnTable(otherExpr, tableScan)) {
            return null;
        }

        if (coveringExpr == null) {
            // Match only the table's column that has the coveringColId
            if ((indexableExpr.getExpressionType() != ExpressionType.VALUE_TUPLE)) {
                return null;
            }
            TupleValueExpression tve = (TupleValueExpression) indexableExpr;
            // Handle a simple indexed column identified by its column id.
            if ((coveringColId == tve.getColumnIndex()) &&
                (tableScan.getTableAlias().equals(tve.getTableAlias()))) {
                // A column match never requires parameter binding. Return an empty list.
                return s_reusableImmutableEmptyBinding;
            }
            return null;
        }
        // Do a possibly more extensive match with coveringExpr which MAY require bound parameters.
        List<AbstractExpression> binding = indexableExpr.bindingToIndexedExpression(coveringExpr);
        return binding;
    }


    /**
     * Insert a send receive pair above the supplied scanNode.
     * @param scanNode that needs to be distributed
     * @return return the newly created receive node (which is linked to the new sends)
     */
    protected static AbstractPlanNode addSendReceivePair(AbstractPlanNode scanNode) {
        SendPlanNode sendNode = new SendPlanNode();
        sendNode.addAndLinkChild(scanNode);

        ReceivePlanNode recvNode = new ReceivePlanNode();
        recvNode.addAndLinkChild(sendNode);

        return recvNode;
    }

    /**
     * Given an access path, build the single-site or distributed plan that will
     * assess the data from the table according to the path.
     *
     * @param table The table to get data from.
     * @return The root of a plan graph to get the data.
     */
    protected static AbstractPlanNode getAccessPlanForTable(JoinNode tableNode) {
        StmtTableScan tableScan = tableNode.getTableScan();
        // Access path to access the data in the table (index/scan/etc).
        AccessPath path = tableNode.m_currentAccessPath;
        assert(path != null);

        // if no index, it is a sequential scan
        if (path.index == null) {
            return getScanAccessPlanForTable(tableScan, path);
        }
        return getIndexAccessPlanForTable(tableScan, path);
    }

    /**
     * Get a sequential scan access plan for a table. For multi-site plans/tables,
     * scans at all partitions.
     *
     * @param table The table to scan.
     * @param path The access path to access the data in the table (index/scan/etc).
     * @return A scan plan node
     */
    private static AbstractScanPlanNode
    getScanAccessPlanForTable(StmtTableScan tableScan, AccessPath path)
    {
        // build the scan node
        SeqScanPlanNode scanNode = new SeqScanPlanNode(tableScan);
        // build the predicate
        scanNode.setPredicate(path.otherExprs);
        return scanNode;
    }

    /**
     * Get an index scan access plan for a table.
     *
     * @param tableAliasIndex The table to get data from.
     * @param path The access path to access the data in the table (index/scan/etc).
     * @return An index scan plan node OR,
               in one edge case, an NLIJ of a MaterializedScan and an index scan plan node.
     */
    private static AbstractPlanNode getIndexAccessPlanForTable(
            StmtTableScan tableScan, AccessPath path) {
        // now assume this will be an index scan and get the relevant index
        Index index = path.index;
        IndexScanPlanNode scanNode = new IndexScanPlanNode(tableScan, index);
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
            assert(exprRightChild != null);
            if (expr.getExpressionType() == ExpressionType.COMPARE_IN) {
                // Replace this method's result with an injected NLIJ.
                resultNode = injectIndexedJoinWithMaterializedScan(exprRightChild, scanNode);
                // Extract a TVE from the LHS MaterializedScan for use by the IndexScan in its new role.
                MaterializedScanPlanNode matscan = (MaterializedScanPlanNode)resultNode.getChild(0);
                AbstractExpression elemExpr = matscan.getOutputExpression();
                assert(elemExpr != null);
                // Replace the IN LIST condition in the end expression referencing all the list elements
                // with a more efficient equality filter referencing the TVE for each element in turn.
                replaceInListFilterWithEqualityFilter(path.endExprs, exprRightChild, elemExpr);
                // Set up the similar VectorValue --> TVE replacement of the search key expression.
                exprRightChild = elemExpr;
            }
            if (exprRightChild instanceof AbstractSubqueryExpression) {
                // The AbstractSubqueryExpression must be wrapped up into a
                // ScalarValueExpression which extracts the actual row/column from
                // the subquery
                // ENG-8175: this part of code seems not working for float/varchar type index ?!

                // DEAD CODE with the guards on index: ENG-8203
                assert(false);
            }
            scanNode.addSearchKeyExpression(exprRightChild);
            // If the index expression is an "IS NOT DISTINCT FROM" comparison, let the NULL values go through. (ENG-11096)
            scanNode.addCompareNotDistinctFlag(expr.getExpressionType() == ExpressionType.COMPARE_NOTDISTINCT);
        }
        // create the IndexScanNode with all its metadata
        scanNode.setLookupType(path.lookupType);
        scanNode.setBindings(path.bindings);
        scanNode.setEndExpression(ExpressionUtil.combinePredicates(path.endExprs));
        scanNode.setPredicate(path.otherExprs);
        // Propagate the sorting information
        // into the scan node from the access path.
        // The initial expression is needed to control a (short?) forward scan to adjust the start of a reverse
        // iteration after it had to initially settle for starting at "greater than a prefix key".
        scanNode.setInitialExpression(ExpressionUtil.combinePredicates(path.initialExpr));
        scanNode.setSkipNullPredicate();
        scanNode.setEliminatedPostFilters(path.eliminatedPostExprs);
        if (scanNode instanceof IndexSortablePlanNode) {
            IndexUseForOrderBy indexUse = ((IndexSortablePlanNode)scanNode).indexUse();
            indexUse.setWindowFunctionUsesIndex(path.m_windowFunctionUsesIndex);
            indexUse.setSortOrderFromIndexScan(path.sortDirection);
            indexUse.setWindowFunctionIsCompatibleWithOrderBy(path.m_stmtOrderByIsCompatible);
            indexUse.setFinalExpressionOrderFromIndexScan(path.m_finalExpressionOrder);
        }
        return resultNode;
    }


    // Generate a plan for an IN-LIST-driven index scan
    private static AbstractPlanNode injectIndexedJoinWithMaterializedScan(
            AbstractExpression listElements, IndexScanPlanNode scanNode) {
        MaterializedScanPlanNode matScan = new MaterializedScanPlanNode();
        assert(listElements instanceof VectorValueExpression || listElements instanceof ParameterValueExpression);
        matScan.setRowData(listElements);
        matScan.setSortDirection(scanNode.getSortDirection());

        NestLoopIndexPlanNode nlijNode = new NestLoopIndexPlanNode();
        nlijNode.setJoinType(JoinType.INNER);
        nlijNode.addInlinePlanNode(scanNode);
        nlijNode.addAndLinkChild(matScan);
        // resolve the sort direction
        nlijNode.resolveSortDirection();
        return nlijNode;
    }


    // Replace the IN LIST condition in the end expression referencing the first given rhs
    // with an equality filter referencing the second given rhs.
    private static void replaceInListFilterWithEqualityFilter(List<AbstractExpression> endExprs,
                                                       AbstractExpression inListRhs,
                                                       AbstractExpression equalityRhs)
    {
        for (AbstractExpression comparator : endExprs) {
            AbstractExpression otherExpr = comparator.getRight();
            if (otherExpr == inListRhs) {
                endExprs.remove(comparator);
                AbstractExpression replacement =
                    new ComparisonExpression(ExpressionType.COMPARE_EQUAL,
                                             comparator.getLeft(),
                                             equalityRhs);
                endExprs.add(replacement);
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
    private void filterPostPredicateForPartialIndex(AccessPath path, List<AbstractExpression> exprToRemove) {
        path.otherExprs.removeAll(exprToRemove);
        // Keep the eliminated expressions for cost estimating purpose
        path.eliminatedPostExprs.addAll(exprToRemove);
    }
}
