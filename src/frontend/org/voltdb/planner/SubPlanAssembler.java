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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.lang3.ArrayUtils;
import org.json_voltpatches.JSONException;
import org.voltdb.VoltType;
import org.voltdb.catalog.CatalogMap;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.ColumnRef;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Index;
import org.voltdb.catalog.Table;
import org.voltdb.expressions.AbstractExpression;
import org.voltdb.expressions.ComparisonExpression;
import org.voltdb.expressions.ExpressionUtil;
import org.voltdb.expressions.TupleValueExpression;
import org.voltdb.planner.AbstractParsedStmt.TablePair;
import org.voltdb.planner.ParsedSelectStmt.ParsedColInfo;
import org.voltdb.plannodes.AbstractPlanNode;
import org.voltdb.plannodes.AbstractScanPlanNode;
import org.voltdb.plannodes.IndexScanPlanNode;
import org.voltdb.plannodes.ReceivePlanNode;
import org.voltdb.plannodes.SendPlanNode;
import org.voltdb.plannodes.SeqScanPlanNode;
import org.voltdb.types.ExpressionType;
import org.voltdb.types.IndexLookupType;
import org.voltdb.types.IndexType;
import org.voltdb.types.SortDirectionType;
import org.voltdb.utils.CatalogUtil;

public abstract class SubPlanAssembler {

    /** The parsed statement structure that has the table and predicate info we need. */
    final AbstractParsedStmt m_parsedStmt;

    /** The catalog's database object which contains tables and access path info */
    final Database m_db;

    /** Describes the specified and inferred partition context. */
    final PartitioningForStatement m_partitioning;

    SubPlanAssembler(Database db, AbstractParsedStmt parsedStmt, PartitioningForStatement partitioning)
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
     * Given a table (and optionally the next table in the join order), using the
     * set of predicate expressions, figure out all the possible ways to get at
     * the data we want. One way will always be the naive sequential scan.
     *
     * @param table
     *     The table to get the data from.
     * @param nextTable
     *     The next tables in the join order or an empty array if there
     *     are none.
     * @return A list of access paths to access the data in the table.
     */
    protected ArrayList<AccessPath> getRelevantAccessPathsForTable(Table table, Table nextTables[]) {
        ArrayList<AccessPath> paths = new ArrayList<AccessPath>();
        // add the empty seq-scan access path
        AccessPath naivePath = new AccessPath();
        paths.add(naivePath);

        List<AbstractExpression> allExprs = new ArrayList<AbstractExpression>();

        List<AbstractExpression> filterExprs = m_parsedStmt.tableFilterList.get(table);
        if (filterExprs != null) {
            allExprs.addAll(filterExprs);
            naivePath.otherExprs.addAll(filterExprs);
        }

        for (int ii = 0; ii < nextTables.length; ii++) {
            final Table nextTable = nextTables[ii];
            // create a key to search the TablePair->Clause map
            TablePair pair = new TablePair();
            pair.t1 = table;
            pair.t2 = nextTable;
            List<AbstractExpression> joinExprs = m_parsedStmt.joinSelectionList.get(pair);

            if (joinExprs != null) {
                allExprs.addAll(joinExprs);
                naivePath.joinExprs.addAll(joinExprs);
            }
        }

        CatalogMap<Index> indexes = table.getIndexes();

        for (Index index : indexes) {
            AccessPath path = getRelevantAccessPathForIndex(table, allExprs, index);
            if (path != null) {
                paths.add(path);
            }
        }

        return paths;
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
    protected AccessPath getRelevantAccessPathForIndex(Table table, List<AbstractExpression> exprs, Index index) {
        assert(index != null);
        assert(table != null);

        // indexes are not useful if there are no filter expressions for this table
        if (exprs == null) {
            return null;
        }

        // Indexes on generalized expressions are handled separately in getRelevantAccessPathForExpressionIndex
        // Unfortunately, a lot of the logic here is replicated there.
        // TODO: Consider adapting this original code to the generalized Expression case by re-constituting
        // the indexed column indexes into the full-blown TupleValueExpressions that they imply.
        if ( ! index.getExpressionsjson().equals("")) {
            return getRelevantAccessPathForExpressionIndex(table, exprs, index);
        }

        AccessPath retval = new AccessPath();
        retval.use = IndexUseType.COVERING_UNIQUE_EQUALITY;
        retval.index = index;

        // Non-scannable indexes require equality, full coverage expressions
        final boolean indexScannable =
            (index.getType() == IndexType.BALANCED_TREE.getValue()) ||
            (index.getType() == IndexType.BTREE.getValue());

        List<ColumnRef> sortedColumns = CatalogUtil.getSortedCatalogItems(index.getColumns(), "index");
        int[] colIds = new int[sortedColumns.size()];
        int ii = 0;
        for (ColumnRef cr : sortedColumns) {
            colIds[ii++] = cr.getColumn().getIndex();
        }

        // build a set of all columns we can filter on (using equality for now)
        // sort expressions in to the proper buckets within the access path
        HashMap<Column, ArrayList<AbstractExpression>> eqColumns = new HashMap<Column, ArrayList<AbstractExpression>>();
        HashMap<Column, ArrayList<AbstractExpression>> gtColumns = new HashMap<Column, ArrayList<AbstractExpression>>();
        HashMap<Column, ArrayList<AbstractExpression>> ltColumns = new HashMap<Column, ArrayList<AbstractExpression>>();
        for (AbstractExpression ae : exprs)
        {
            AbstractExpression expr = getIndexableExpressionForFilter(table, ae, colIds, null);
            if (expr != null) {
                AbstractExpression indexable = expr.getLeft();
                assert(indexable.getExpressionType() == ExpressionType.VALUE_TUPLE);

                TupleValueExpression tve = (TupleValueExpression)indexable;
                Column col = getTableColumn(table, tve.getColumnName());
                assert(col != null);

                if (expr.getExpressionType() == ExpressionType.COMPARE_EQUAL)
                {
                    if (eqColumns.containsKey(col) == false)
                        eqColumns.put(col, new ArrayList<AbstractExpression>());
                    eqColumns.get(col).add(expr);
                    continue;
                }
                if ((expr.getExpressionType() == ExpressionType.COMPARE_GREATERTHAN) ||
                        (expr.getExpressionType() == ExpressionType.COMPARE_GREATERTHANOREQUALTO))
                {
                    if (gtColumns.containsKey(col) == false)
                        gtColumns.put(col, new ArrayList<AbstractExpression>());
                    gtColumns.get(col).add(expr);
                    continue;
                }
                if ((expr.getExpressionType() == ExpressionType.COMPARE_LESSTHAN) ||
                        (expr.getExpressionType() == ExpressionType.COMPARE_LESSTHANOREQUALTO))
                {
                    if (ltColumns.containsKey(col) == false)
                        ltColumns.put(col, new ArrayList<AbstractExpression>());
                    ltColumns.get(col).add(expr);
                    continue;
                }

            }
            retval.otherExprs.add(ae);
        }

        // See if we can use index scan for ORDER BY.
        // The only scenario where we can use index scan is when the ORDER BY
        // columns list is a prefix or match of the tree index columns,
        // in the same order from left to right. It also requires that all columns are ordered in the
        // same direction. For example, if a table has a tree index of columns
        // A, B, C, then 'ORDER BY A, B' or 'ORDER BY A DESC, B DESC, C DESC'
        // work, but not 'ORDER BY A, C' or 'ORDER BY A, B DESC'.
        // Currently only ascending key indexes can be defined and supported.
        if (indexScannable && m_parsedStmt instanceof ParsedSelectStmt) {
            ParsedSelectStmt parsedSelectStmt = (ParsedSelectStmt) m_parsedStmt;
            if (!parsedSelectStmt.orderColumns.isEmpty()) {
                Iterator<ColumnRef> colRefIter = sortedColumns.iterator();

                boolean ascending = parsedSelectStmt.orderColumns.get(0).ascending;
                for (ParsedColInfo colInfo : parsedSelectStmt.orderColumns) {
                    // Explicitly advance colRefIter in synch with the loop over orderColumns
                    // to match the index's "ON" columns with the query's "ORDER BY" columns
                    if (!colRefIter.hasNext()) {
                        retval.sortDirection = SortDirectionType.INVALID;
                        break;
                    }
                    ColumnRef colRef = colRefIter.next();
                    if (colInfo.expression instanceof TupleValueExpression &&
                        colInfo.tableName.equals(table.getTypeName()) &&
                        colInfo.columnName.equals(colRef.getColumn().getTypeName()) &&
                        colInfo.ascending == ascending) {

                        retval.sortDirection = ascending ? SortDirectionType.ASC : SortDirectionType.DESC;

                        retval.use = IndexUseType.INDEX_SCAN;
                    } else {
                        retval.sortDirection = SortDirectionType.INVALID;
                        break;
                    }
                }
            }
        }

        // cover as much of the index as possible with expressions that use
        // index columns
        for (ColumnRef colRef : CatalogUtil.getSortedCatalogItems(index.getColumns(), "index")) {
            Column col = colRef.getColumn();
            if (eqColumns.containsKey(col) && (eqColumns.get(col).size() >= 0)) {
                AbstractExpression expr = eqColumns.get(col).remove(0);
                retval.indexExprs.add(expr);
                retval.endExprs.add(expr);

                /*
                 * The index executor cannot handle desc order if the lookup
                 * type is equal. This includes partial index coverage cases
                 * with only equality expressions.
                 *
                 * Setting the sort direction to invalid here will make
                 * PlanAssembler generate a suitable order by plan node after
                 * the scan.
                 */
                if (retval.sortDirection == SortDirectionType.DESC) {
                    retval.sortDirection = SortDirectionType.INVALID;
                }
            } else {
                if (gtColumns.containsKey(col) && (gtColumns.get(col).size() >= 0)) {
                    AbstractExpression expr = gtColumns.get(col).remove(0);
                    if (retval.sortDirection != SortDirectionType.DESC)
                        retval.indexExprs.add(expr);
                    if (retval.sortDirection == SortDirectionType.DESC)
                        retval.endExprs.add(expr);

                    if (expr.getExpressionType() == ExpressionType.COMPARE_GREATERTHAN)
                        retval.lookupType = IndexLookupType.GT;
                    else if (expr.getExpressionType() == ExpressionType.COMPARE_GREATERTHANOREQUALTO) {
                        retval.lookupType = IndexLookupType.GTE;
                    } else
                        assert (false);

                    retval.use = IndexUseType.INDEX_SCAN;
                }

                if (ltColumns.containsKey(col) && (ltColumns.get(col).size() >= 0)) {
                    AbstractExpression expr = ltColumns.get(col).remove(0);
                    retval.endExprs.add(expr);
                    if (retval.indexExprs.size() == 0) {
                        // SearchKey is null,but has end key
                        retval.lookupType = IndexLookupType.GTE;
                    }
                }

                // if we didn't find an equality match, we can stop looking
                // whether we found a non-equality comp or not
                break;
            }
        }

        // index not relevant to expression
        if (retval.indexExprs.size() == 0 && retval.endExprs.size() == 0
                && retval.sortDirection == SortDirectionType.INVALID)
            return null;

        // If IndexUseType is the default of COVERING_UNIQUE_EQUALITY, and not
        // all columns are covered (but some are with equality)
        // then it is possible to scan use GTE. The columns not covered will have
        // null supplied. This will not execute if there is already a GT or LT lookup
        // type set because those also change the IndexUseType to INDEX_SCAN
        // Maybe setting the IndexUseType should be done separately from
        // determining if the last expression is GT/LT?
        if (retval.use == IndexUseType.COVERING_UNIQUE_EQUALITY &&
            retval.indexExprs.size() < index.getColumns().size())
        {
            retval.use = IndexUseType.INDEX_SCAN;
            retval.lookupType = IndexLookupType.GTE;
        }

        if ((indexScannable == false)) {
            // partial coverage
            if (retval.indexExprs.size() < index.getColumns().size())
                return null;

            // non-equality
            if ((retval.use == IndexUseType.INDEX_SCAN))
                return null;
        }

        // add all unused expressions to the retval's other list
        for (ArrayList<AbstractExpression> list : eqColumns.values()) {
            assert(list != null);
            for (AbstractExpression expr : list) {
                assert(expr != null);
                retval.otherExprs.add(expr);
            }
        }
        for (ArrayList<AbstractExpression> list : gtColumns.values()) {
            assert(list != null);
            for (AbstractExpression expr : list) {
                assert(expr != null);
                retval.otherExprs.add(expr);
            }
        }
        for (ArrayList<AbstractExpression> list : ltColumns.values()) {
            assert(list != null);
            for (AbstractExpression expr : list) {
                assert(expr != null);
                retval.otherExprs.add(expr);
            }
        }
        return retval;
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
    protected AccessPath getRelevantAccessPathForExpressionIndex(Table table, List<AbstractExpression> exprs, Index index) {

        // This MAY want to happen once when the plan is loaded from the catalog and cached in a sticky cached index-to-expressions map?
        List<AbstractExpression> indexedExprs;
        try {
            indexedExprs = AbstractExpression.fromJSONArrayString(index.getExpressionsjson(), null);
        } catch (JSONException e) {
            e.printStackTrace();
            assert(false);
            return null;
        }
        // There may be a missing step here in which the base column expressions get normalized to the table.

        AccessPath retval = new AccessPath();
        retval.use = IndexUseType.COVERING_UNIQUE_EQUALITY;
        retval.index = index;

        // Non-scannable indexes require equality, full coverage expressions
        final boolean indexScannable =
            (index.getType() == IndexType.BALANCED_TREE.getValue()) ||
            (index.getType() == IndexType.BTREE.getValue());

        List<ColumnRef> sortedColumns = CatalogUtil.getSortedCatalogItems(index.getColumns(), "index");
        int[] colIds = new int[sortedColumns.size()];
        int ii = 0;
        for (ColumnRef cr : sortedColumns) {
            colIds[ii++] = cr.getColumn().getIndex();
        }

        // build a set of all columns or exprs we can filter on
        // sort expressions into the proper buckets within the access path
        HashMap<AbstractExpression, ArrayList<AbstractExpression>> eqExprs = null;
        HashMap<AbstractExpression, ArrayList<AbstractExpression>> gtExprs = null;
        HashMap<AbstractExpression, ArrayList<AbstractExpression>> ltExprs = null;

        eqExprs = new HashMap<AbstractExpression, ArrayList<AbstractExpression>>();
        gtExprs = new HashMap<AbstractExpression, ArrayList<AbstractExpression>>();
        ltExprs = new HashMap<AbstractExpression, ArrayList<AbstractExpression>>();

        for (AbstractExpression ae : exprs)
        {
            AbstractExpression expr = getIndexableExpressionForFilter(table, ae, colIds, indexedExprs);
            if (expr != null) {
                AbstractExpression indexable = expr.getLeft();
                if (expr.getExpressionType() == ExpressionType.COMPARE_EQUAL) {
                    if (eqExprs.containsKey(indexable) == false) {
                        eqExprs.put(indexable, new ArrayList<AbstractExpression>());
                    }
                    eqExprs.get(indexable).add(expr);
                    continue;
                }
                if ((expr.getExpressionType() == ExpressionType.COMPARE_GREATERTHAN) ||
                        (expr.getExpressionType() == ExpressionType.COMPARE_GREATERTHANOREQUALTO)) {
                    if (gtExprs.containsKey(indexable) == false) {
                        gtExprs.put(indexable, new ArrayList<AbstractExpression>());
                    }
                    gtExprs.get(indexable).add(expr);
                    continue;
                }
                if ((expr.getExpressionType() == ExpressionType.COMPARE_LESSTHAN) ||
                        (expr.getExpressionType() == ExpressionType.COMPARE_LESSTHANOREQUALTO)) {
                    if (ltExprs.containsKey(indexable) == false) {
                        ltExprs.put(indexable, new ArrayList<AbstractExpression>());
                    }
                    ltExprs.get(indexable).add(expr);
                    continue;
                }
            }
            retval.otherExprs.add(ae);
        }

        // See if we can use index scan for ORDER BY.
        // The only scenario where we can use index scan is when the ORDER BY
        // columns/expressions list is a prefix or match of the tree index columns/expressions,
        // in the same order from left to right. It also requires that all columns are ordered in the
        // same direction. For example, if a table has a tree index of columns
        // A, B, C, then 'ORDER BY A, B' or 'ORDER BY A DESC, B DESC, C DESC'
        // work, but not 'ORDER BY A, C' or 'ORDER BY A, B DESC'.
        // Currently only ascending key indexes can be defined and supported.
        if (indexScannable && m_parsedStmt instanceof ParsedSelectStmt) {
            ParsedSelectStmt parsedSelectStmt = (ParsedSelectStmt) m_parsedStmt;
            if (!parsedSelectStmt.orderColumns.isEmpty()) {

                boolean ascending = parsedSelectStmt.orderColumns.get(0).ascending;
                int jj = 0;
                for (ParsedColInfo colInfo : parsedSelectStmt.orderColumns) {

                    // Explicitly advance nextExpr in synch with the loop over orderColumns
                    // to match the index's "ON" expressions with the query's "ORDER BY" expressions
                    if (jj >= indexedExprs.size()) {
                        retval.sortDirection = SortDirectionType.INVALID;
                        break;
                    }
                    AbstractExpression nextExpr = indexedExprs.get(jj++);

                    if (colInfo.expression.equals(nextExpr) && colInfo.ascending == ascending) {
                        retval.sortDirection = ascending ? SortDirectionType.ASC : SortDirectionType.DESC;
                        retval.use = IndexUseType.INDEX_SCAN;
                    } else {
                        retval.sortDirection = SortDirectionType.INVALID;
                        break;
                    }
                }
            }
        }

        // cover as much of the index as possible with comparator expressions that use indexed expressions
        for (AbstractExpression coveredExpr : indexedExprs) {
            if (eqExprs.containsKey(coveredExpr) && (eqExprs.get(coveredExpr).size() >= 0)) {
                AbstractExpression comparator = eqExprs.get(coveredExpr).remove(0);
                retval.indexExprs.add(comparator);
                retval.endExprs.add(comparator);

                /*
                 * The index executor cannot handle desc order if the lookup
                 * type is equal. This includes partial index coverage cases
                 * with only equality expressions.
                 *
                 * Setting the sort direction to invalid here will make
                 * PlanAssembler generate a suitable order by plan node after
                 * the scan.
                 */
                if (retval.sortDirection == SortDirectionType.DESC) {
                    retval.sortDirection = SortDirectionType.INVALID;
                }
            } else {
                if (gtExprs.containsKey(coveredExpr) && (gtExprs.get(coveredExpr).size() >= 0)) {
                    AbstractExpression comparator = gtExprs.get(coveredExpr).remove(0);
                    if (retval.sortDirection != SortDirectionType.DESC)
                        retval.indexExprs.add(comparator);
                    if (retval.sortDirection == SortDirectionType.DESC)
                        retval.endExprs.add(comparator);

                    if (comparator.getExpressionType() == ExpressionType.COMPARE_GREATERTHAN)
                        retval.lookupType = IndexLookupType.GT;
                    else if (comparator.getExpressionType() == ExpressionType.COMPARE_GREATERTHANOREQUALTO) {
                        retval.lookupType = IndexLookupType.GTE;
                    } else
                        assert (false);

                    retval.use = IndexUseType.INDEX_SCAN;
                }

                if (ltExprs.containsKey(coveredExpr) && (ltExprs.get(coveredExpr).size() >= 0)) {
                    AbstractExpression comparator = ltExprs.get(coveredExpr).remove(0);
                    retval.endExprs.add(comparator);
                    if (retval.indexExprs.size() == 0) {
                        // SearchKey is null,but has end key
                        retval.lookupType = IndexLookupType.GTE;
                    }
                }

                // if we didn't find an equality match, we can stop looking
                // whether we found a non-equality comp or not
                break;
            }
        }

        // index not relevant to expression
        if (retval.indexExprs.size() == 0 && retval.endExprs.size() == 0 && retval.sortDirection == SortDirectionType.INVALID) {
            return null;
        }

        // If IndexUseType is the default of COVERING_UNIQUE_EQUALITY, and not
        // all columns are covered (but some are with equality)
        // then it is possible to scan use GTE. The columns not covered will have
        // null supplied. This will not execute if there is already a GT or LT lookup
        // type set because those also change the IndexUseType to INDEX_SCAN
        // Maybe setting the IndexUseType should be done separately from
        // determining if the last expression is GT/LT?
        if (retval.use == IndexUseType.COVERING_UNIQUE_EQUALITY && retval.indexExprs.size() < indexedExprs.size())
        {
            retval.use = IndexUseType.INDEX_SCAN;
            retval.lookupType = IndexLookupType.GTE;
        }

        if ((indexScannable == false)) {
            // partial coverage
            if (retval.indexExprs.size() < indexedExprs.size())
                return null;

            // non-equality
            if ((retval.use == IndexUseType.INDEX_SCAN))
                return null;
        }

        // add all unused expressions to the retval's other list
        for (ArrayList<AbstractExpression> list : eqExprs.values()) {
            assert(list != null);
            for (AbstractExpression expr : list) {
                assert(expr != null);
                retval.otherExprs.add(expr);
            }
        }
        for (ArrayList<AbstractExpression> list : gtExprs.values()) {
            assert(list != null);
            for (AbstractExpression expr : list) {
                assert(expr != null);
                retval.otherExprs.add(expr);
            }
        }
        for (ArrayList<AbstractExpression> list : ltExprs.values()) {
            assert(list != null);
            for (AbstractExpression expr : list) {
                assert(expr != null);
                retval.otherExprs.add(expr);
            }
        }
        return retval;
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
     * @param table The table we want the column from.
     * @param expr The comparison expression to qualify and possibly normalize.
     * @param colIds The columns indexed by a column index -- any of the table's other columns are not interesting.
     * @param indexedExprs The indexed expressions on the table's columns, for a general expression index
     * @return A normalized form of expr with the potentially indexed expression on the left-hand-side and the
     * potential index key expression on the right of a comparison operator
     * -- or null if the filter can not be expressed in that way.
     */
    protected AbstractExpression getIndexableExpressionForFilter(Table table, AbstractExpression expr, int[] colIds, List<AbstractExpression> indexedExprs)
    {
        if (expr == null)
            return null;

        // Expression type must be resolvable by an index scan
        if ((expr.getExpressionType() != ExpressionType.COMPARE_EQUAL) &&
            (expr.getExpressionType() != ExpressionType.COMPARE_GREATERTHAN) &&
            (expr.getExpressionType() != ExpressionType.COMPARE_GREATERTHANOREQUALTO) &&
            (expr.getExpressionType() != ExpressionType.COMPARE_LESSTHAN) &&
            (expr.getExpressionType() != ExpressionType.COMPARE_LESSTHANOREQUALTO))
        {
            return null;
        }

        AbstractExpression indexableExpr = null;

        boolean indexableOnLeft = isIndexableFilterOperand(table, expr.getLeft(), colIds, indexedExprs);
        boolean indexableOnRight = isIndexableFilterOperand(table, expr.getRight(),  colIds, indexedExprs);

        // Left and right columns must not be from the same table,
        // e.g. where t.a = t.b is not indexable with the current technology.
        // It would require parallel iteration over two indexes,
        // looking for matching keys AND matching payloads.
        if (indexableOnLeft && indexableOnRight) {
            return null;
        }

        // EE index key comparator should not lose precision when casting keys to indexed type.
        // Do not choose an index that requires such a cast.
        VoltType otherType = null;
        ComparisonExpression normalizedExpr = (ComparisonExpression) expr;
        if (indexableOnLeft) {
            if (isOperandDependentOnTable(expr.getRight(), table)) {
                // Left and right operands must not be from the same table,
                // e.g. where t.a = t.b is not indexable with the current technology.
                return null;
            }
            indexableExpr = expr.getLeft();
            otherType = expr.getRight().getValueType();
        } else {
            if (isOperandDependentOnTable(expr.getLeft(), table)) {
                // Left and right operands must not be from the same table,
                // e.g. where t.a = t.b is not indexable with the current technology.
                return null;
            }
            indexableExpr = expr.getRight();
            normalizedExpr = normalizedExpr.reverseOperator();

            otherType = expr.getLeft().getValueType();
        }

        VoltType keyType = indexableExpr.getValueType();
        if (! keyType.canExactlyRepresentAnyValueOf(otherType))
        {
            return null;
        }

        return normalizedExpr;
    }

    boolean isOperandDependentOnTable(AbstractExpression expr, Table table) {
        for (TupleValueExpression tve : ExpressionUtil.getTupleValueExpressions(expr)) {
            //TODO: This clumsy testing of table names regardless of table aliases is
            // EXACTLY why we can't have nice things like self-joins.
            if (table.getTypeName().equals(tve.getTableName()))
            {
                return true;
            }
        }
        return false;
    }

    /* Facilitate consistent expression column, table column comparisons. */
    private Column getTableColumn(Table table, String searchColumnName) {
        return table.getColumns().getIgnoreCase(searchColumnName);
    }

    private boolean isIndexableFilterOperand(Table table, AbstractExpression expr, int[] colIds, List<AbstractExpression> indexedExprs) {
        assert(expr != null);
        if (expr.getExpressionType() == ExpressionType.VALUE_TUPLE) {
            // This is just a crude filter to eliminate column filters on completely unrelated columns of the index's table.
            // If the TVE filter is on a column of the index's base table and has a colId ("column index") used by the
            // index, tentatively accept the filter as relevant.
            // It may get thrown out later in lots of cases, including:
            // * when the filtered column is not actually indexed, just a BASE column for a more complex indexed expression, or
            // * when the column is indexed but AFTER another column or expression that does not have its own filter in the query
            //   or, on a HASH index, even if it is BEFORE such a non-filtered column or expression.
            TupleValueExpression tve = (TupleValueExpression)expr;
            if ( ! ArrayUtils.contains(colIds, tve.getColumnIndex())) {
                return false;
            }
            //FIXME: This clumsy testing of table names regardless of table aliases is
            // EXACTLY why we can't have nice things like self-joins.
            if (table.getTypeName().equals(tve.getTableName()))
            {
                return true;
            }
            return false;
        }
        // More general filtered expressions are pre-qualified more aggressively than simple columns.
        // They must exactly match an indexed expression.
        // They can still be thrown out later if the indexed expression is listed AFTER another expression or column
        // that does not have its own filter in the query
        // or, on a HASH index, even if it is BEFORE such a non-filtered expression or column.
        if (indexedExprs != null) {
            for (AbstractExpression abstractExpression : indexedExprs) {
                if (expr.equals(abstractExpression)) {
                    return true;
                }
            }
        }
        return false;
    }


    /**
     * Insert a send receive pair above the supplied scanNode.
     * @param scanNode that needs to be distributed
     * @return return the newly created receive node (which is linked to the new sends)
     */
    protected AbstractPlanNode addSendReceivePair(AbstractPlanNode scanNode) {

        SendPlanNode sendNode = new SendPlanNode();
        // this will make the child planfragment be sent to all partitions
        sendNode.isMultiPartition = true;
        sendNode.addAndLinkChild(scanNode);

        ReceivePlanNode recvNode = new ReceivePlanNode();
        recvNode.addAndLinkChild(sendNode);

        // receive node requires the schema of its output table
        recvNode.generateOutputSchema(m_db);
        return recvNode;
    }

    /**
     * Given an access path, build the single-site or distributed plan that will
     * assess the data from the table according to the path.
     *
     * @param table The table to get data from.
     * @param path The access path to access the data in the table (index/scan/etc).
     * @return The root of a plan graph to get the data.
     */
    protected AbstractPlanNode getAccessPlanForTable(Table table, AccessPath path) {
        assert(table != null);
        assert(path != null);

        AbstractScanPlanNode scanNode = null;
        // if no path is a sequential scan, call a subroutine for that
        if (path.index == null)
        {
            scanNode = getScanAccessPlanForTable(table, path.otherExprs);
        }
        else
        {
            scanNode = getIndexAccessPlanForTable(table, path);
        }
        // set the scan columns for this scan node based on the parsed SQL,
        // if any
        if (m_parsedStmt.scanColumns != null)
        {
            scanNode.setScanColumns(m_parsedStmt.scanColumns.get(table.getTypeName()));
        }
        scanNode.generateOutputSchema(m_db);
        return scanNode;
    }

    /**
     * Get a sequential scan access plan for a table. For multi-site plans/tables,
     * scans at all partitions and sends to one partition.
     *
     * @param table The table to scan.
     * @param exprs The predicate components.
     * @return A scan plan node
     */
    protected AbstractScanPlanNode
    getScanAccessPlanForTable(Table table, ArrayList<AbstractExpression> exprs)
    {
        // build the scan node
        SeqScanPlanNode scanNode = new SeqScanPlanNode();
        scanNode.setTargetTableName(table.getTypeName());

        // build the predicate
        AbstractExpression localWhere = null;
        if ((exprs != null) && (exprs.isEmpty() == false))
        {
            localWhere = ExpressionUtil.combine(exprs);
            scanNode.setPredicate(localWhere);
        }

        return scanNode;
    }

    /**
     * Get a index scan access plan for a table. For multi-site plans/tables,
     * scans at all partitions and sends to one partition.
     *
     * @param table The table to get data from.
     * @param path The access path to access the data in the table (index/scan/etc).
     * @return An index scan plan node
     */
    protected AbstractScanPlanNode getIndexAccessPlanForTable(Table table,
                                                              AccessPath path)
    {
        // now assume this will be an index scan and get the relevant index
        Index index = path.index;
        // build the list of search-keys for the index in question
        IndexScanPlanNode scanNode = new IndexScanPlanNode();
        for (AbstractExpression expr : path.indexExprs) {
            AbstractExpression expr2 = TupleValueExpression.getOtherTableExpression(expr, table);
            assert(expr2 != null);
            scanNode.addSearchKeyExpression(expr2);
        }
        // create the IndexScanNode with all its metadata
        scanNode.setCatalogIndex(index);
        scanNode.setKeyIterate(path.keyIterate);
        scanNode.setLookupType(path.lookupType);
        scanNode.setSortDirection(path.sortDirection);
        scanNode.setTargetTableName(table.getTypeName());
        scanNode.setTargetTableAlias(table.getTypeName());
        scanNode.setTargetIndexName(index.getTypeName());
        if (path.sortDirection != SortDirectionType.DESC) {
            List<AbstractExpression> predicate = new ArrayList<AbstractExpression>();
            predicate.addAll(path.indexExprs);
            predicate.addAll(path.otherExprs);
            scanNode.setPredicate(ExpressionUtil.combine(predicate));
            scanNode.setEndExpression(ExpressionUtil.combine(path.endExprs));
        }
        else {
            List<AbstractExpression> predicate = new ArrayList<AbstractExpression>();
            predicate.addAll(path.indexExprs);
            predicate.addAll(path.endExprs);
            predicate.addAll(path.otherExprs);
            scanNode.setPredicate(ExpressionUtil.combine(predicate));
        }
        return scanNode;
    }
}
