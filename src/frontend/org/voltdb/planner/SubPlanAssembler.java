/* This file is part of VoltDB.
 * Copyright (C) 2008-2011 VoltDB Inc.
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

import org.voltdb.VoltType;
import org.voltdb.catalog.CatalogMap;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.ColumnRef;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Index;
import org.voltdb.catalog.Table;
import org.voltdb.expressions.AbstractExpression;
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
import org.voltdb.utils.VoltTypeUtil;

public abstract class SubPlanAssembler {

    /** The parsed statement structure that has the table and predicate info we need. */
    final AbstractParsedStmt m_parsedStmt;

    /** The catalog's database object which contains tables and access path info */
    final Database m_db;

    /** Do the plan need to scan all partitions or just one? */
    final boolean m_singlePartition;

    SubPlanAssembler(Database db, AbstractParsedStmt parsedStmt, boolean singlePartition,
                     int partitionCount)
    {
        m_db = db;
        m_parsedStmt = parsedStmt;
        m_singlePartition = singlePartition;
    }

    /**
     * Determines whether a table will require a distributed scan.
     * @param table The table that may or may not require a distributed scan
     * @return true if the table requires a distributed scan, false otherwise
     */
    abstract protected boolean tableRequiresDistributedScan(Table table);

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
            for (AbstractExpression expr : filterExprs)
                expr.m_isJoiningClause = false;
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
                for (AbstractExpression expr : joinExprs)
                    expr.m_isJoiningClause = true;
                allExprs.addAll(joinExprs);
                naivePath.joinExprs.addAll(joinExprs);
            }
        }

        CatalogMap<Index> indexes = table.getIndexes();

        for (Index index : indexes) {
            AccessPath path = null;

            path = getRelevantAccessPathForIndex(table, allExprs, index);
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
        if (exprs == null)
            return null;

        AccessPath retval = new AccessPath();
        retval.use = IndexUseType.COVERING_UNIQUE_EQUALITY;
        retval.index = index;

        // Non-scannable indexes require equality, full coverage expressions
        // TODO: Should be metadata on the IndexType instance.
        final boolean indexScannable =
            (index.getType() == IndexType.BALANCED_TREE.getValue()) ||
            (index.getType() == IndexType.BTREE.getValue());

        // build a set of all columns we can filter on (using equality for now)
        // sort expressions in to the proper buckets within the access path
        HashMap<Column, ArrayList<AbstractExpression>> eqColumns = new HashMap<Column, ArrayList<AbstractExpression>>();
        HashMap<Column, ArrayList<AbstractExpression>> gtColumns = new HashMap<Column, ArrayList<AbstractExpression>>();
        HashMap<Column, ArrayList<AbstractExpression>> ltColumns = new HashMap<Column, ArrayList<AbstractExpression>>();
        for (AbstractExpression expr : exprs)
        {
            Column col = getColumnForFilterExpression(table, expr);
            if (col != null)
            {
                if (expr.getExpressionType() == ExpressionType.COMPARE_EQUAL)
                {
                    if (eqColumns.containsKey(col) == false)
                        eqColumns.put(col, new ArrayList<AbstractExpression>());
                    eqColumns.get(col).add(expr);
                }
                else if ((expr.getExpressionType() == ExpressionType.COMPARE_GREATERTHAN) ||
                        (expr.getExpressionType() == ExpressionType.COMPARE_GREATERTHANOREQUALTO))
                {
                    if (gtColumns.containsKey(col) == false)
                        gtColumns.put(col, new ArrayList<AbstractExpression>());
                    gtColumns.get(col).add(expr);
                }
                else if ((expr.getExpressionType() == ExpressionType.COMPARE_LESSTHAN) ||
                        (expr.getExpressionType() == ExpressionType.COMPARE_LESSTHANOREQUALTO))
                {
                    if (ltColumns.containsKey(col) == false)
                        ltColumns.put(col, new ArrayList<AbstractExpression>());
                    ltColumns.get(col).add(expr);
                }
                else
                {
                    retval.otherExprs.add(expr);
                }
            }
            else
            {
                retval.otherExprs.add(expr);
            }
        }

        // See if we can use index scan for ORDER BY.
        // The only scenario where we can use index scan is when the ORDER BY
        // columns is a subset of the tree index columns, in the same order from
        // left to right. It also requires that all columns are ordered in the
        // same direction. For example, if a table has a tree index of columns
        // A, B, C, then 'ORDER BY A, B', 'ORDER BY A DESC, B DESC, C DESC'
        // work, but not 'ORDER BY A, C' or 'ORDER BY A, B DESC'.
        if (indexScannable && m_parsedStmt instanceof ParsedSelectStmt) {
            ParsedSelectStmt parsedSelectStmt = (ParsedSelectStmt) m_parsedStmt;
            if (!parsedSelectStmt.orderColumns.isEmpty()) {
                List<ColumnRef> sortedColumns = CatalogUtil.getSortedCatalogItems(index.getColumns(), "index");
                Iterator<ColumnRef> colRefIter = sortedColumns.iterator();

                boolean ascending = parsedSelectStmt.orderColumns.get(0).ascending;
                for (ParsedColInfo colInfo : parsedSelectStmt.orderColumns) {
                    if (!colRefIter.hasNext()) {
                        retval.sortDirection = SortDirectionType.INVALID;
                        break;
                    }

                    ColumnRef colRef = colRefIter.next();
                    if (colInfo.tableName.equals(table.getTypeName()) &&
                        colInfo.columnName.equals(colRef.getColumn().getTypeName()) &&
                        colInfo.ascending == ascending) {

                        if (ascending)
                            retval.sortDirection = SortDirectionType.ASC;
                        else
                            retval.sortDirection = SortDirectionType.DESC;

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
                }

                // if we didn't find an equality match, we can stop looking
                // whether we found a non-equality comp or not
                break;
            }
        }

        // index not relevant to expression
        if (retval.indexExprs.size() == 0 && retval.sortDirection == SortDirectionType.INVALID)
            return null;

        // If IndexUseType is the default of COVERING_UNIQUE_EQUALITY, and not
        // all columns are covered (but some are with equality)
        // then it is possible to scan use GT. The columns not covered will have
        // null supplied. This will not execute
        // if there is already a GT or LT lookup type set because those also
        // change the IndexUseType to INDEX_SCAN
        // Maybe setting the IndexUseType should be done separately from
        // determining if the last expression is GT/LT?
        if (retval.use == IndexUseType.COVERING_UNIQUE_EQUALITY &&
            retval.indexExprs.size() < index.getColumns().size())
        {
            retval.use = IndexUseType.INDEX_SCAN;
            retval.lookupType = IndexLookupType.GT;
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
     * For a given filter expression, get the column involved that is part of the table
     * specified. For example, "WHERE F_ID = 2" would return F_ID if F_ID is in the table
     * passed in. For join expressions like, "WHERE F_ID = Q_ID", this returns the column
     * that is in the table passed in.
     *
     * This method just sanity-checks some conditions under which using an index
     * for the given expression would actually make sense and then hands off to
     * getColumnForFilterExpressionRecursive to do the real work.
     *
     * @param table The table we want the column from.
     * @param expr The comparison expression to search.
     * @return The column found or null if none found.
     */
    protected Column
    getColumnForFilterExpression(Table table, AbstractExpression expr)
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

        Column indexedColumn = getColumnForFilterExpressionRecursive(table, expr);
        if (indexedColumn == null)
            return indexedColumn;

        // EE index comparators do not support expressions on the key.
        // The indexed column must not be part of a sub-expression.
        boolean keyIsExpression = true;

        // Also remember which side contains the key - need this later
        boolean keyIsLeft = false;

        // Already know that the column appears on at most one side of the
        // expression. Can naively check both sides.
        if (expr.getLeft().getExpressionType() == ExpressionType.VALUE_TUPLE) {
            TupleValueExpression tve = (TupleValueExpression)(expr.getLeft());
            if (getTableColumn(table, tve.getColumnName()) == indexedColumn) {
                keyIsExpression = false;
                keyIsLeft = true;
            }
        }
        if (expr.getRight().getExpressionType() == ExpressionType.VALUE_TUPLE) {
            TupleValueExpression tve = (TupleValueExpression)(expr.getRight());
            if (getTableColumn(table, tve.getColumnName()) == indexedColumn) {
                keyIsExpression = false;
                keyIsLeft = false;
            }
        }

        if (keyIsExpression)
            return null;

        // EE index key comparator can not cast keys to the RHS type.
        // Do not choose an index that requires such a cast.
        // Sadly, this restriction is not globally true but I don't
        // think the planner can really predict the index type that
        // the EE's index factory might construct.
        VoltType keyType = keyIsLeft ? expr.getLeft().getValueType()
                                     : expr.getRight().getValueType();

        VoltType exprType = keyIsLeft ? expr.getRight().getValueType()
                                      : expr.getLeft().getValueType();

        if (!VoltTypeUtil.isAllowableCastForKeyComparator(exprType, keyType))
        {
            return null;
        }

        return indexedColumn;
    }

    /* Facilitate consistent expression column, table column comparisons. */
    private Column getTableColumn(Table table, String searchColumnName) {
        return table.getColumns().getIgnoreCase(searchColumnName);
    }

    protected Column
    getColumnForFilterExpressionRecursive(Table table, AbstractExpression expr) {
        // stopping steps:
        //
        if (expr == null)
            return null;

        if (expr.getExpressionType() == ExpressionType.VALUE_TUPLE) {
            TupleValueExpression tve = (TupleValueExpression)expr;
            if (table.getTypeName().equals(tve.getTableName()))
            {
                return getTableColumn(table, tve.getColumnName());
            }
            return null;
        }

        // recursive step
        //
        Column leftCol = getColumnForFilterExpressionRecursive(table, expr.getLeft());
        Column rightCol = getColumnForFilterExpressionRecursive(table, expr.getRight());

        assert(leftCol == null ||
               getTableColumn(table, leftCol.getTypeName()) != null);

        assert(rightCol == null ||
               getTableColumn(table, rightCol.getTypeName()) != null);

        // Left and right columns must not be from the same table,
        // e.g. where t.a = t.b is really a self join.
        if (leftCol != null && rightCol != null) {
            return null;
        }

        if (leftCol != null)
            return leftCol;

        if (rightCol != null)
            return rightCol;

        return null;
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

        AbstractPlanNode rootNode = scanNode;

        // if we need to scan everywhere...
        if (tableRequiresDistributedScan(table)) {
            // all sites to a scan -> send
            // root site has many recvs feeding into a union
            rootNode = addSendReceivePair(scanNode);
        }

        return rootNode;
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
            AbstractExpression expr2 = ExpressionUtil.getOtherTableExpression(expr, table.getTypeName());
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
