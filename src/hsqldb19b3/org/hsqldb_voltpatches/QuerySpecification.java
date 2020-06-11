/* Copyright (c) 2001-2009, The HSQL Development Group
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * Redistributions of source code must retain the above copyright notice, this
 * list of conditions and the following disclaimer.
 *
 * Redistributions in binary form must reproduce the above copyright notice,
 * this list of conditions and the following disclaimer in the documentation
 * and/or other materials provided with the distribution.
 *
 * Neither the name of the HSQL Development Group nor the names of its
 * contributors may be used to endorse or promote products derived from this
 * software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL HSQL DEVELOPMENT GROUP, HSQLDB.ORG,
 * OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
 * EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
 * PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */


package org.hsqldb_voltpatches;

import java.util.ArrayList;
import java.util.List;

import org.hsqldb_voltpatches.HsqlNameManager.HsqlName;
import org.hsqldb_voltpatches.HsqlNameManager.SimpleName;
import org.hsqldb_voltpatches.ParserDQL.CompileContext;
import org.hsqldb_voltpatches.RangeVariable.RangeIteratorBase;
import org.hsqldb_voltpatches.RangeVariable.RangeIteratorMain;
import org.hsqldb_voltpatches.index.Index;
import org.hsqldb_voltpatches.lib.ArrayListIdentity;
import org.hsqldb_voltpatches.lib.ArrayUtil;
import org.hsqldb_voltpatches.lib.HashMappedList;
import org.hsqldb_voltpatches.lib.HashSet;
import org.hsqldb_voltpatches.lib.HsqlArrayList;
import org.hsqldb_voltpatches.lib.HsqlList;
import org.hsqldb_voltpatches.lib.IntValueHashMap;
// BEGIN Cherry-picked code change from hsqldb-2.3.2
import org.hsqldb_voltpatches.lib.Iterator;
// END Cherry-picked code change from hsqldb-2.3.2
import org.hsqldb_voltpatches.lib.OrderedHashSet;
import org.hsqldb_voltpatches.lib.OrderedIntHashSet;
import org.hsqldb_voltpatches.lib.Set;
import org.hsqldb_voltpatches.navigator.RangeIterator;
import org.hsqldb_voltpatches.navigator.RowSetNavigatorData;
import org.hsqldb_voltpatches.result.Result;
import org.hsqldb_voltpatches.result.ResultMetaData;
import org.hsqldb_voltpatches.store.ValuePool;
import org.hsqldb_voltpatches.types.Type;

/**
 * Implementation of an SQL query specification, including SELECT.
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 *
 * @version 1.9.0
 * @since 1.9.0
 */
public class QuerySpecification extends QueryExpression {

    private int           resultRangePosition;
    public boolean        isDistinctSelect;
    public boolean        isAggregated;
    public boolean        isGrouped;
    private HashSet       groupColumnNames;
    RangeVariable[]       rangeVariables;
    private HsqlArrayList rangeVariableList;
    Expression            queryCondition;
    Expression            checkQueryCondition;
    private Expression    havingCondition;
    Expression[]          exprColumns;
    private HsqlArrayList exprColumnList;
    public int            indexLimitVisible;
    private int           indexLimitRowId;
    private int           groupByColumnCount;    // columns in 'group by'
    private int           havingColumnCount;     // columns in 'having' (0 or 1)
    public int            indexStartOrderBy;
    private int           indexStartAggregates;
    private int           indexLimitExpressions;
    private int           indexLimitData;

    //
    public boolean  isUniqueResultRows;
    private boolean simpleLimit;                 // true if maxrows can be uses as is
    private boolean acceptsSequences;

    //
    Type[]                    columnTypes;
    private ArrayListIdentity aggregateSet;

    //
    private ArrayListIdentity resolvedSubqueryExpressions = null;

    //
    //
    private boolean[] aggregateCheck;

    //
    private OrderedHashSet tempSet = new OrderedHashSet();

    //
    int[]                  columnMap;
    private Table          baseTable;
    private OrderedHashSet conditionTables;      // for view super-view references

    //
    public Index groupIndex;

    //
    QuerySpecification(Session session, Table table,
                       CompileContext compileContext) {

        this(compileContext);

        RangeVariable range = new RangeVariable(table, null, null, null,
            compileContext);

        range.addTableColumns(exprColumnList, 0, null);

        indexLimitVisible = exprColumnList.size();

        addRangeVariable(range);
        resolveReferences(session);
        resolveTypes(session);
        resolveTypes(session);

        sortAndSlice = SortAndSlice.noSort;
    }

    QuerySpecification(CompileContext compileContext) {

        super(compileContext);

        this.compileContext = compileContext;
        resultRangePosition = compileContext.getNextRangeVarIndex();
        rangeVariableList   = new HsqlArrayList();
        exprColumnList      = new HsqlArrayList();
        sortAndSlice        = SortAndSlice.noSort;
    }

    void addRangeVariable(RangeVariable rangeVar) {
        rangeVariableList.add(rangeVar);
    }

    private void finaliseRangeVariables() {

        if (rangeVariables == null
                || rangeVariables.length < rangeVariableList.size()) {
            rangeVariables = new RangeVariable[rangeVariableList.size()];

            rangeVariableList.toArray(rangeVariables);
        }
    }

    void addSelectColumnExpression(Expression e) {

        if (e.getType() == OpTypes.ROW) {
            throw Error.error(ErrorCode.X_42564);
        }

        exprColumnList.add(e);

        indexLimitVisible++;
    }

    void addQueryCondition(Expression e) {
        queryCondition = e;
    }

    void addGroupByColumnExpression(Expression e) {

        if (e.getType() == OpTypes.ROW) {
            throw Error.error(ErrorCode.X_42564);
        }

        exprColumnList.add(e);

        isGrouped = true;

        groupByColumnCount++;
    }

    void addHavingExpression(Expression e) {

        exprColumnList.add(e);

        havingCondition   = e;
        havingColumnCount = 1;
        isGrouped         = true;
    }

    @Override
    void addSortAndSlice(SortAndSlice sortAndSlice) {
        this.sortAndSlice = sortAndSlice;
    }

    @Override
    public void resolveReferences(Session session) {

        finaliseRangeVariables();
        resolveColumnReferencesForAsterisk();
        finaliseColumns();
        resolveColumnReferences();

        unionColumnTypes = new Type[indexLimitVisible];
        unionColumnMap   = new int[indexLimitVisible];

        ArrayUtil.fillSequence(unionColumnMap);
    }

    /**
     * Resolves all column expressions in the GROUP BY clause and beyond.
     * Replaces any alias column expression in the ORDER BY cluase
     * with the actual select column expression.
     */
    private void resolveColumnReferences() {

        if (isDistinctSelect || isGrouped) {
            acceptsSequences = false;
        }

        for (int i = 0; i < rangeVariables.length; i++) {
            Expression e = rangeVariables[i].nonIndexJoinCondition;

            if (e == null) {
                continue;
            }

            resolveColumnReferencesAndAllocate(e, i + 1, false);
        }

        resolveColumnReferencesAndAllocate(queryCondition,
                                           rangeVariables.length, false);

        for (int i = 0; i < indexLimitVisible; i++) {
            resolveColumnReferencesAndAllocate(exprColumns[i],
                                               rangeVariables.length,
                                               acceptsSequences);
        }

        for (int i = indexLimitVisible; i < indexStartOrderBy; i++) {
            resolveColumnReferencesAndAllocate(exprColumns[i],
                                               rangeVariables.length, false);
        }

    /************************* Volt DB Extensions *************************/
        resolveColumnReferencesInGroupBy();
    /**********************************************************************/

        resolveColumnReferencesInOrderBy(sortAndSlice);
    }

    /************************* Volt DB Extensions *************************/
    void resolveColumnReferencesInGroupBy() {
        if (unresolvedExpressions == null || unresolvedExpressions.isEmpty()) {
            return;
        }

        /**
         * Hsql HashSet does not work properly to remove duplicates, I doubt the hash
         * function or equal function on expression work properly or something else
         * is wrong. So use list
         *
         */
        // resolve GROUP BY columns/expressions
        HsqlList newUnresolvedExpressions = new ArrayListIdentity();

        int size = unresolvedExpressions.size();
        for (int i = 0; i < size; i++) {
            Object obj = unresolvedExpressions.get(i);
            newUnresolvedExpressions.add(obj);
            if (i + 1 < size && obj == unresolvedExpressions.get(i+1)) {
                // unresolvedExpressions is a public member that can be accessed from anywhere and
                // I (xin) am 90% percent sure about the hsql adds the unresolved expression twice
                // together for our targeted GROUP BY alias case.
                // so we want to skip the repeated expression also.
                // For other unresolved expression, it may differs.
                i += 1;
            }
            if (obj instanceof ExpressionColumn == false) {
                continue;
            }
            ExpressionColumn element = (ExpressionColumn) obj;
            if (element.tableName != null) {
                // this alias does not belong to any table
                continue;
            }

            // group by alias which is thought as an column
            if (element.getType() != OpTypes.COLUMN) {
                continue;
            }

            // find the unsolved expression in the groupBy list
            int k = indexLimitVisible;
            int endGroupbyIndex = indexLimitVisible + groupByColumnCount;
            for (; k < endGroupbyIndex; k++) {
                if (element == exprColumns[k]) {
                    break;
                }
            }
            if (k == endGroupbyIndex) {
                // not found in selected list
                continue;
            }
            assert(exprColumns[k].getType() == OpTypes.COLUMN);

            ExpressionColumn exprCol = (ExpressionColumn) exprColumns[k];
            String alias = exprCol.getColumnName();
            if (alias == null) {
                // we should not handle this case (group by constants)
                continue;
            }

            // Find it in the SELECT list.  We need to look at all
            // the select list elements to see if there are more
            // than one.
            int matchcount = 0;
            for (int j = 0; j < indexLimitVisible; j++) {
                Expression selectCol = exprColumns[j];
                if (selectCol.alias == null) {
                    // columns referenced by their alias must have an alias
                    continue;
                }
                if (alias.equals(selectCol.alias.name)) {
                    matchcount += 1;
                    // This may be an alias to an aggregate
                    // column.  But we'll find that later, so
                    // don't check for it here.
                    exprColumns[k] = selectCol;
                    exprColumnList.set(k, selectCol);
                    if (matchcount == 1) {
                        newUnresolvedExpressions.remove(element);
                    }
                }
            }
            if (matchcount > 1) {
                throw new HsqlException(String.format("Group by expression \"%s\" is ambiguous", alias), "", 0);
            }
        }
        unresolvedExpressions = newUnresolvedExpressions;
    }
    /**********************************************************************/

    void resolveColumnReferencesInOrderBy(SortAndSlice sortAndSlice) {

        // replace the aliases with expressions
        // replace column names with expressions and resolve the table columns
        int orderCount = sortAndSlice.getOrderLength();

        for (int i = 0; i < orderCount; i++) {
            ExpressionOrderBy e =
                (ExpressionOrderBy) sortAndSlice.exprList.get(i);

            replaceColumnIndexInOrderBy(e);

            if (e.getLeftNode().queryTableColumnIndex != -1) {
                continue;
            }

            if (sortAndSlice.sortUnion) {
                if (e.getLeftNode().getType() != OpTypes.COLUMN) {
                    throw Error.error(ErrorCode.X_42576);
                }
            }

            e.replaceAliasInOrderBy(exprColumns, indexLimitVisible);
            resolveColumnReferencesAndAllocate(e, rangeVariables.length,
                                               false);
        }

        sortAndSlice.prepare(this);
    }

    private boolean resolveColumnReferences(Expression e, int rangeCount,
            boolean withSequences) {

        if (e == null) {
            return true;
        }

        int oldSize = unresolvedExpressions == null ? 0
                                                    : unresolvedExpressions
                                                        .size();

        unresolvedExpressions = e.resolveColumnReferences(rangeVariables,
                rangeCount, unresolvedExpressions, withSequences);

        int newSize = unresolvedExpressions == null ? 0
                                                    : unresolvedExpressions
                                                        .size();

        return oldSize == newSize;
    }

    private void resolveColumnReferencesForAsterisk() {

        for (int pos = 0; pos < indexLimitVisible; ) {
            Expression e = (Expression) (exprColumnList.get(pos));

            if (e.getType() == OpTypes.MULTICOLUMN) {
                exprColumnList.remove(pos);

                String tablename = ((ExpressionColumn) e).getTableName();

                if (tablename == null) {
                    addAllJoinedColumns(e);
                } else {
                    int rangeIndex =
                        e.findMatchingRangeVariableIndex(rangeVariables);

                    if (rangeIndex == -1) {
                        throw Error.error(ErrorCode.X_42501, tablename);
                    }

                    RangeVariable range   = rangeVariables[rangeIndex];
                    HashSet       exclude = getAllNamedJoinColumns();

                    range.addTableColumns(e, exclude);
                }

                for (int i = 0; i < e.nodes.length; i++) {
                    exprColumnList.add(pos, e.nodes[i]);

                    pos++;
                }

                indexLimitVisible += e.nodes.length - 1;
            } else {
                pos++;
            }
        }
    }

    private void resolveColumnReferencesAndAllocate(Expression expression,
            int count, boolean withSequences) {

        if (expression == null) {
            return;
        }

        HsqlList list = expression.resolveColumnReferences(rangeVariables,
            count, null, withSequences);

        if (list != null) {
            for (int i = 0; i < list.size(); i++) {
                Expression e = (Expression) list.get(i);
                boolean    resolved;

                if (e.isSelfAggregate()) {
                    resolved = resolveColumnReferences(e.getLeftNode(), count,
                                                       false);
                } else {
                    resolved = resolveColumnReferences(e, count,
                                                       withSequences);
                }

                if (resolved) {
                    if (e.isSelfAggregate()) {
                        if (aggregateSet == null) {
                            aggregateSet = new ArrayListIdentity();
                        }

                        aggregateSet.add(e);

                        isAggregated           = true;
                        expression.isAggregate = true;
                    }

                    if (resolvedSubqueryExpressions == null) {
                        resolvedSubqueryExpressions = new ArrayListIdentity();
                    }

                    resolvedSubqueryExpressions.add(e);
                } else {
                    if (unresolvedExpressions == null) {
                        unresolvedExpressions = new ArrayListIdentity();
                    }

                    unresolvedExpressions.add(e);
                }
            }
        }
    }

    private HashSet getAllNamedJoinColumns() {

        HashSet set = null;

        for (int i = 0; i < rangeVariableList.size(); i++) {
            RangeVariable range = (RangeVariable) rangeVariableList.get(i);

            if (range.namedJoinColumns != null) {
                if (set == null) {
                    set = new HashSet();
                }

                set.addAll(range.namedJoinColumns);
            }
        }

        return set;
    }

    public Expression getEquiJoinExpressions(OrderedHashSet nameSet,
            RangeVariable rightRange, boolean fullList) {

        HashSet        set             = new HashSet();
        Expression     result          = null;
        OrderedHashSet joinColumnNames = new OrderedHashSet();

        for (int i = 0; i < rangeVariableList.size(); i++) {
            RangeVariable  range = (RangeVariable) rangeVariableList.get(i);
            HashMappedList columnList = range.rangeTable.columnList;

            for (int j = 0; j < columnList.size(); j++) {
                ColumnSchema column       = (ColumnSchema) columnList.get(j);
                String       name         = range.getColumnAlias(j);
                boolean      columnInList = nameSet.contains(name);
                boolean namedJoin = range.namedJoinColumns != null
                                    && range.namedJoinColumns.contains(name);
                boolean repeated = !namedJoin && !set.add(name);

                if (repeated && (!fullList || columnInList)) {
                    throw Error.error(ErrorCode.X_42578, name);
                }

                if (!columnInList) {
                    continue;
                }

                joinColumnNames.add(name);

                int position = rightRange.rangeTable.getColumnIndex(name);
                ColumnSchema rightColumn =
                    rightRange.rangeTable.getColumn(position);
                Expression e = new ExpressionLogical(range, column,
                                                     rightRange, rightColumn);

                result = ExpressionLogical.andExpressions(result, e);

                ExpressionColumn col = range.getColumnExpression(name);

                if (col == null) {
                    col = new ExpressionColumn(new Expression[] {
                        e.getLeftNode(), e.getRightNode()
                    }, name);

                    range.addNamedJoinColumnExpression(name, col);
                } else {
                    col.nodes = (Expression[]) ArrayUtil.resizeArray(col.nodes,
                            col.nodes.length + 1);
                    col.nodes[col.nodes.length - 1] = e.getRightNode();
                }

                rightRange.addNamedJoinColumnExpression(name, col);
            }
        }

        if (fullList && !joinColumnNames.containsAll(nameSet)) {
            throw Error.error(ErrorCode.X_42501);
        }

        rightRange.addNamedJoinColumns(joinColumnNames);

        return result;
    }

    private void addAllJoinedColumns(Expression e) {

        HsqlArrayList list = new HsqlArrayList();

        for (int i = 0; i < rangeVariables.length; i++) {
            rangeVariables[i].addTableColumns(list);
        }

        Expression[] nodes = new Expression[list.size()];

        list.toArray(nodes);

        e.nodes = nodes;
    }

    private void finaliseColumns() {

        indexLimitRowId = indexLimitVisible;
        indexStartOrderBy = indexLimitRowId + groupByColumnCount
                            + havingColumnCount;
        indexStartAggregates = indexStartOrderBy
                               + sortAndSlice.getOrderLength();
        indexLimitData = indexLimitExpressions = indexStartAggregates;
        exprColumns    = new Expression[indexLimitExpressions];

        exprColumnList.toArray(exprColumns);

        for (int i = 0; i < indexLimitVisible; i++) {
            exprColumns[i].queryTableColumnIndex = i;
        }

        if (sortAndSlice.hasOrder()) {
            for (int i = 0; i < sortAndSlice.getOrderLength(); i++) {
                exprColumns[indexStartOrderBy + i] =
                    (Expression) sortAndSlice.exprList.get(i);
            }
        }
    }

    private void replaceColumnIndexInOrderBy(Expression orderBy) {

        Expression e = orderBy.getLeftNode();

        if (e.dataType != null && e.dataType.isBooleanType()) {
            // Give "invalid ORDER BY expression" error if ORDER BY boolean.
            throw Error.error(ErrorCode.X_42576);
        }

        if (e.getDataType() == null) {
            return;
        }

        if (e.getType() != OpTypes.VALUE) {
            return;
        }

        if (e.getDataType().typeCode == Types.SQL_INTEGER) {
            int i = ((Integer) e.getValue(null)).intValue();

            if (0 < i && i <= indexLimitVisible) {
                orderBy.setLeftNode(exprColumns[i - 1]);

                return;
            }
        }

        throw Error.error(ErrorCode.X_42576);
    }

    void collectRangeVariables(RangeVariable[] rangeVars, Set set) {

        for (int i = 0; i < indexStartAggregates; i++) {
            exprColumns[i].collectRangeVariables(rangeVars, set);
        }

        if (queryCondition != null) {
            queryCondition.collectRangeVariables(rangeVars, set);
        }

        if (havingCondition != null) {
            havingCondition.collectRangeVariables(rangeVars, set);
        }
    }

    @Override
    public boolean hasReference(RangeVariable range) {

        if (unresolvedExpressions == null) {
            return false;
        }

        for (int i = 0; i < unresolvedExpressions.size(); i++) {
            if (((Expression) unresolvedExpressions.get(i)).hasReference(
                    range)) {
                return true;
            }
        }

        return false;
    }

    /**
     * Sets the types of all the expressions used in this SELECT list.
     */
    public void resolveExpressionTypes(Session session) {

        for (int i = 0; i < indexStartAggregates; i++) {
            Expression e = exprColumns[i];

            e.resolveTypes(session, null);

            if (e.getType() == OpTypes.ROW) {
                throw Error.error(ErrorCode.X_42564);
            }
        }

        for (int i = 0, len = rangeVariables.length; i < len; i++) {
            Expression e = rangeVariables[i].nonIndexJoinCondition;

            if (e != null) {
                e.resolveTypes(session, null);

                if (e.getDataType() != Type.SQL_BOOLEAN) {
                    throw Error.error(ErrorCode.X_42568);
                }
            }
        }

        if (queryCondition != null) {
            queryCondition.resolveTypes(session, null);

            if (queryCondition.getDataType() != Type.SQL_BOOLEAN) {
                throw Error.error(ErrorCode.X_42568);
            }
            if (queryCondition.opType == OpTypes.VALUE) {
                if (!((boolean)queryCondition.valueData)) { // WHERE false => LIMIT 0
                    SortAndSlice sortAndSlice = new SortAndSlice();
                    ExpressionValue limit0 = new ExpressionValue(ValuePool.INTEGER_0, Type.SQL_INTEGER);
                    ExpressionValue offset = new ExpressionValue(ValuePool.INTEGER_0, Type.SQL_INTEGER);
                    sortAndSlice.addLimitCondition(new ExpressionOp(OpTypes.LIMIT, offset, limit0));
                    addSortAndSlice(sortAndSlice);
                }
                // Leave out the original WHERE condition no matter it is WHERE true or WHERE false.
                queryCondition = null;
            }
            else {
                // A VoltDB extension to guard against abuse of aggregates in subqueries.
                // Make sure no aggregates in WHERE clause
                tempSet.clear();
                Expression.collectAllExpressions(
                        tempSet, queryCondition, Expression.aggregateFunctionSet,
                        Expression.subqueryExpressionSet);
                if (!tempSet.isEmpty()) {

                    // A top level WHERE clause can't have aggregate expressions.
                    // In theory, a subquery WHERE clause may have aggregate
                    // expressions in some edge cases where they reference only
                    // columns from parent query(s).
                    // Even these should be restricted to cases where the subquery
                    // is evaluated after the parent agg, such as from the HAVING
                    // or SELECT clause of the parent query defining the columns.
                    // TO be safe, VoltDB doesn't support ANY cases of aggs of
                    // parent columns. All this code block does is choose between
                    // two error messages for two different unsupported cases.
                    if ( ! isTopLevel) {
                        HsqlList columnSet = new OrderedHashSet();
                        Iterator aggIt = tempSet.iterator();
                        while (aggIt.hasNext()) {
                            Expression nextAggr = (Expression) aggIt.next();
                            Expression.collectAllExpressions(columnSet, nextAggr,
                                    Expression.columnExpressionSet, Expression.emptyExpressionSet);
                        }
                        Iterator columnIt = columnSet.iterator();
                        while (columnIt.hasNext()) {
                            Expression nextColumn = (Expression) columnIt.next();
                            assert(nextColumn instanceof ExpressionColumn);
                            ExpressionColumn nextColumnEx = (ExpressionColumn) nextColumn;
                            String tableName = nextColumnEx.rangeVariable.rangeTable.tableName.name;
                            String tableAlias = (nextColumnEx.rangeVariable.tableAlias != null) ?
                                    nextColumnEx.rangeVariable.tableAlias.name : null;
                            boolean resolved = false;
                            for (RangeVariable rv : rangeVariables) {
                                if (rv.rangeTable.tableName.name.equals(tableName)) {
                                    if (rv.tableAlias == null && tableAlias == null) {
                                        resolved = true;
                                    } else if (rv.tableAlias != null && tableAlias != null) {
                                        resolved = tableAlias.equals(rv.tableAlias.name);
                                    }
                                }
                            }
                            if (!resolved) {
                                throw Error.error(ErrorCode.X_47001);
                            }
                        }
                    }
                    // If we get here it means that WHERE expression has an aggregate expression
                    // with local columns
                    throw Error.error(ErrorCode.X_47000);
                }
                // End of VoltDB extension
            }
        }
    }

    private void resolveAggregates() {

        tempSet.clear();

        if (isAggregated) {
            aggregateCheck = new boolean[indexStartAggregates];
            tempSet.addAll(aggregateSet);
            indexLimitData = indexLimitExpressions = exprColumns.length
                    + tempSet.size();
            exprColumns = (Expression[]) ArrayUtil.resizeArray(exprColumns,
                    indexLimitExpressions);

            for (int i = indexStartAggregates, j = 0;
                    i < indexLimitExpressions; i++, j++) {
                ExpressionAggregate e = (ExpressionAggregate) tempSet.get(j);

                exprColumns[i]          = new ExpressionAggregate(e);
                exprColumns[i].dataType = e.dataType;
                exprColumns[i].queryTableColumnIndex = e.queryTableColumnIndex;
            }
            tempSet.clear();
        }
    }

    @Override
    public boolean areColumnsResolved() {
        return unresolvedExpressions == null
               || unresolvedExpressions.isEmpty();
    }

    private void setRangeVariableConditions() {

        RangeVariableResolver rangeResolver =
            new RangeVariableResolver(rangeVariables, queryCondition,
                                      compileContext);

        rangeResolver.processConditions();

        rangeVariables = rangeResolver.rangeVariables;

//        queryCondition = null;
    }

    @Override
    public void resolveTypes(Session session) {

        if (isResolved) {
            return;
        }

        resolveTypesPartOne(session);
        resolveTypesPartTwo(session);
        ArrayUtil.copyArray(resultTable.colTypes, unionColumnTypes,
                            unionColumnTypes.length);

        for (int i = 0; i < indexStartOrderBy; i++) {
            if (exprColumns[i].dataType == null) {
                throw Error.error(ErrorCode.X_42567);
            }
        }

        isResolved = true;

        return;
    }

    @Override
    void resolveTypesPartOne(Session session) {

        resolveExpressionTypes(session);
        setRangeVariableConditions();
        resolveAggregates();

        for (int i = 0; i < unionColumnMap.length; i++) {
            unionColumnTypes[i] = Type.getAggregateType(unionColumnTypes[i],
                    exprColumns[i].getDataType());
        }
    }

    @Override
    void resolveTypesPartTwo(Session session) {

        resolveGroups();

        for (int i = 0; i < unionColumnMap.length; i++) {
            Type type = unionColumnTypes[unionColumnMap[i]];

            if (type == null) {
                throw Error.error(ErrorCode.X_42567);
            }

            exprColumns[unionColumnMap[i]].setDataType(session, type);
        }

        for (int i = 0; i < indexStartOrderBy; i++) {
            if (exprColumns[i].dataType == null) {
                throw Error.error(ErrorCode.X_42567);
            }
        }

        setReferenceableColumns();
        setUpdatability();
        createResultMetaData();
        createTable(session);

        if (isUpdatable) {
            getMergedSelect();
        }
    }

    private void resolveGroups() {

        // - 1.9.0 is standard compliant but has more extended support for
        //   referencing columns
        // - check there is no direct aggregate expression in group by
        // - check each expression in select list can be
        //   decomposed into the expressions in group by or any aggregates
        //   this allows direct function of group by expressions, but
        //   doesn't allow indirect functions. e.g.
        //     select 2*abs(cola) , sum(colb) from t group by abs(cola) // ok
        //     select 2*(cola + 10) , sum(colb) from t group by cola + 10 // ok
        //     select abs(cola) , sum(colb) from t group by cola // ok
        //     select 2*cola + 20 , sum(colb) from t group by cola + 10 // not allowed although correct
        //     select cola , sum(colb) from t group by abs(cola) // not allowed because incorrect
        // - group by can introduce invisible, derived columns into the query table
        // - check the having expression can be decomposed into
        //   select list expresions plus group by expressions
        // - having cannot introduce additional, derived columns
        // - having cannot reference columns not in the select or group by list
        // - if there is any aggregate in select list but no group by, no
        //   non-aggregates is allowed
        // - check order by columns
        // - if distinct select, order by must be composed of the select list columns
        // - if grouped by, then order by should be decomposed into the
        //   select list plus group by list
        // - references to column aliases are allowed only in order by (Standard
        //   compliance) and take precendence over references to non-alias
        //   column names.
        // - references to table / correlation and column list in correlation
        //   names are handled according to the Standard
        //  fredt@users
        tempSet.clear();

        if (isGrouped) {
            for (int i = indexLimitVisible;
                    i < indexLimitVisible + groupByColumnCount; i++) {
                Expression.collectAllExpressions(
                    tempSet, exprColumns[i], Expression.aggregateFunctionSet,
                    Expression.subqueryExpressionSet);

                if (!tempSet.isEmpty()) {
                    // The sql is an aggregate function name, extracted
                    // from the SQL text of the query.  But the function
                    // getSQL is intended to call in a context which
                    // parameters to the string and then adds a trailing
                    // parenthesis. So, we add the trailing parenthesis
                    // here if it's necessary.
                    String sql = ((Expression) tempSet.get(0)).getSQL();
                    if (sql.endsWith("(")) {
                        sql += ")";
                    }
                    throw Error.error(ErrorCode.X_42572, sql);
                }
            }

            for (int i = 0; i < indexLimitVisible; i++) {
                if (!exprColumns[i].isComposedOf(
                        exprColumns, indexLimitVisible,
                        indexLimitVisible + groupByColumnCount,
                        Expression.subqueryAggregateExpressionSet)) {
                    tempSet.add(exprColumns[i]);
                }
            }

            for (int i = indexStartOrderBy; i < indexStartAggregates; i++) {
                if (!exprColumns[i].isComposedOf(
                        exprColumns, indexLimitVisible,
                        indexLimitVisible + groupByColumnCount,
                        Expression.subqueryAggregateExpressionSet)) {
                    tempSet.add(exprColumns[i]);
                }
            }

            if (!tempSet.isEmpty() && !resolveForGroupBy(tempSet)) {
                throw Error.error(ErrorCode.X_42574,
                                  ((Expression) tempSet.get(0)).getSQL());
            }
        } else if (isAggregated) {
            for (int i = 0; i < indexLimitVisible; i++) {
                Expression.collectAllExpressions(
                    tempSet, exprColumns[i], Expression.columnExpressionSet,
                    Expression.aggregateFunctionSet);

                if (!tempSet.isEmpty()) {
                    throw Error.error(ErrorCode.X_42574,
                                      ((Expression) tempSet.get(0)).getSQL());
                }
            }
        }

        tempSet.clear();

        if (havingCondition != null) {
            if (unresolvedExpressions != null) {
                tempSet.addAll(unresolvedExpressions);
            }

            for (int i = indexLimitVisible;
                    i < indexLimitVisible + groupByColumnCount; i++) {
                tempSet.add(exprColumns[i]);
            }

            if (!havingCondition.isComposedOf(
                    tempSet, Expression.subqueryAggregateExpressionSet)) {
                throw Error.error(ErrorCode.X_42573);
            }

            tempSet.clear();
        }

        if (isDistinctSelect) {
            int orderCount = sortAndSlice.getOrderLength();

            for (int i = 0; i < orderCount; i++) {
                Expression e = (Expression) sortAndSlice.exprList.get(i);

                if (e.queryTableColumnIndex != -1) {
                    continue;
                }

                if (!e.isComposedOf(exprColumns, 0, indexLimitVisible,
                                    Expression.emptyExpressionSet)) {
                    throw Error.error(ErrorCode.X_42576);
                }
            }
        }

        if (isGrouped) {
            int orderCount = sortAndSlice.getOrderLength();

            for (int i = 0; i < orderCount; i++) {
                Expression e = (Expression) sortAndSlice.exprList.get(i);

                if (e.queryTableColumnIndex != -1) {
                    continue;
                }

                if (!e.isComposedOf(exprColumns, 0,
                                    indexLimitVisible + groupByColumnCount,
                                    Expression.emptyExpressionSet)) {
                    throw Error.error(ErrorCode.X_42576);
                }
            }
        }

        simpleLimit = (!isDistinctSelect && !isGrouped
                       && !sortAndSlice.hasOrder());

        if (!isAggregated) {
            return;
        }

        OrderedHashSet expressions       = new OrderedHashSet();
        OrderedHashSet columnExpressions = new OrderedHashSet();

        for (int i = indexStartAggregates; i < indexLimitExpressions; i++) {
            Expression e = exprColumns[i];
            Expression c = new ExpressionColumn(e, i, resultRangePosition);
            expressions.addAlwaysIfAggregate(e);
            columnExpressions.add(c);
        }

        for (int i = 0; i < indexStartOrderBy; i++) {
            if (exprColumns[i].isAggregate) {
                continue;
            }

            Expression e = exprColumns[i];

            if (expressions.add(e)) {
                Expression c = new ExpressionColumn(e, i, resultRangePosition);

                columnExpressions.add(c);
            }
        }

        // order by with aggregate
        int orderCount = sortAndSlice.getOrderLength();

        for (int i = 0; i < orderCount; i++) {
            Expression e = (Expression) sortAndSlice.exprList.get(i);

            if (e.getLeftNode().isAggregate) {
                e.isAggregate = true;
            }
        }

        for (int i = indexStartOrderBy; i < indexStartAggregates; i++) {
            if (exprColumns[i].getLeftNode().isAggregate) {
                exprColumns[i].isAggregate = true;
            }
        }

        for (int i = 0; i < indexStartAggregates; i++) {
            Expression e = exprColumns[i];

            if (!e.isAggregate) {
                continue;
            }

            aggregateCheck[i] = true;

            e.convertToSimpleColumn(expressions, columnExpressions);
        }

        for (int i = 0; i < aggregateSet.size(); i++) {
            Expression e = (Expression) aggregateSet.get(i);

            e.convertToSimpleColumn(expressions, columnExpressions);
        }

        if (resolvedSubqueryExpressions != null) {
            for (int i = 0; i < resolvedSubqueryExpressions.size(); i++) {
                Expression e = (Expression) resolvedSubqueryExpressions.get(i);

                e.convertToSimpleColumn(expressions, columnExpressions);
            }
        }
    }

    boolean resolveForGroupBy(HsqlList unresolvedSet) {

        for (int i = indexLimitVisible;
                i < indexLimitVisible + groupByColumnCount; i++) {
            Expression e = exprColumns[i];

            if (e.getType() == OpTypes.COLUMN) {
                RangeVariable range    = e.getRangeVariable();
                int           colIndex = e.getColumnIndex();

                range.columnsInGroupBy[colIndex] = true;
            }
        }

        for (int i = 0; i < rangeVariables.length; i++) {
            RangeVariable range = rangeVariables[i];

            range.hasKeyedColumnInGroupBy =
                range.rangeTable.getUniqueNotNullColumnGroup(
                    range.columnsInGroupBy) != null;
        }

        OrderedHashSet set = null;

        for (int i = 0; i < unresolvedSet.size(); i++) {
            Expression e = (Expression) unresolvedSet.get(i);

            set = e.getUnkeyedColumns(set);
        }

        return set == null;
    }

    private int getLimitStart(Session session) {

        if (sortAndSlice.limitCondition != null) {
            Integer limit =
                (Integer) sortAndSlice.limitCondition.getLeftNode().getValue(
                    session);

            if (limit == null || limit.intValue() < 0) {
                throw Error.error(ErrorCode.X_2201X);
            }

            return limit.intValue();
        }

        return 0;
    }

    private int getLimitCount(Session session, int rowCount) {

        int limitCount = Integer.MAX_VALUE;

        // A VoltDB extension to support OFFSET without LIMIT
        if (sortAndSlice.limitCondition != null
                && sortAndSlice.limitCondition.getRightNode() != null) {
        // End of VoltDB extension
        /* disable 1 line ...
        if (sortAndSlice.limitCondition != null) {
        ... disabled 1 line */
            Integer limit =
                (Integer) sortAndSlice.limitCondition.getRightNode().getValue(
                    session);

            // A VoltDB extension to support LIMIT 0
            if (limit == null || limit.intValue() < 0) {
            /* disable 1 line ...
            if (limit == null || limit.intValue() <= 0) {
            ... disabled 1 line */
            // End of VoltDB extension
                throw Error.error(ErrorCode.X_2201W);
            }

            limitCount = limit.intValue();
        }

        if (rowCount != 0 && rowCount < limitCount) {
            limitCount = rowCount;
        }

        return limitCount;
    }

    /**
     * translate the rowCount into total number of rows needed from query,
     * including any rows skipped at the beginning
     */
    private int getMaxRowCount(Session session, int rowCount) {

        int limitStart = getLimitStart(session);
        int limitCount = getLimitCount(session, rowCount);

        if (simpleLimit) {
            if (rowCount == 0) {
                rowCount = limitCount;
            }

            // A VoltDB extension to support LIMIT 0
            if (rowCount > Integer.MAX_VALUE - limitStart) {
            /* disable 1 line ...
            if (rowCount == 0 || rowCount > Integer.MAX_VALUE - limitStart) {
            ... disabled 1 line */
            // End of VoltDB extension
                rowCount = Integer.MAX_VALUE;
            } else {
                rowCount += limitStart;
            }
        } else {
            rowCount = Integer.MAX_VALUE;
            // A VoltDB extension to support LIMIT 0
            // limitCount == 0 can be enforced/optimized as rowCount == 0 regardless of offset
            // even in non-simpleLimit cases (SELECT DISTINCT, GROUP BY, and/or ORDER BY).
            // This is an optimal handling of a hard-coded LIMIT 0, but it really shouldn't be the ONLY
            // enforcement for zero LIMITs -- what about "LIMIT ?" with 0 passed later as a parameter?
            // The HSQL executor ("HSQL back end") also needs runtime enforcement of zero limits.
            // The VoltDB executor has such enforcement.
            if (limitCount == 0) {
                rowCount = 0;
            }
            // End of VoltDB extension
        }

        return rowCount;
    }

    /**
     * Returns the result of executing this Select.
     *
     * @param maxrows may be 0 to indicate no limit on the number of rows.
     * Positive values limit the size of the result set.
     * @return the result of executing this Select
     */
    @Override
    Result getResult(Session session, int maxrows) {

        Result r;

        r = getSingleResult(session, maxrows);

        // fredt - now there is no need for the sort and group columns
//        r.setColumnCount(indexLimitVisible);
        r.getNavigator().reset();

        return r;
    }

    private Result getSingleResult(Session session, int rowCount) {

        int                 maxRows   = getMaxRowCount(session, rowCount);
        Result              r         = buildResult(session, maxRows);
        RowSetNavigatorData navigator = (RowSetNavigatorData) r.getNavigator();

        if (isDistinctSelect) {
            navigator.removeDuplicates();
        }

        navigator.sortOrder();
        navigator.trim(getLimitStart(session),
                       getLimitCount(session, rowCount));

        return r;
    }

    private Result buildResult(Session session, int limitcount) {

        RowSetNavigatorData navigator = new RowSetNavigatorData(session,
            this);
        Result result = Result.newResult(navigator);

        result.metaData = resultMetaData;

        result.setDataResultConcurrency(isUpdatable);

        // A VoltDB extension to support LIMIT 0
        // Test for early return case added by VoltDB to support LIMIT 0 in "HSQL backend".
        if (limitcount == 0) {
            return result;
        }
        // End of VoltDB extension
        int fullJoinIndex = 0;
        RangeIterator[] rangeIterators =
            new RangeIterator[rangeVariables.length];

        for (int i = 0; i < rangeVariables.length; i++) {
            rangeIterators[i] = rangeVariables[i].getIterator(session);
        }

        for (int currentIndex = 0; ; ) {
            if (currentIndex < fullJoinIndex) {
                boolean end = true;

                for (int i = fullJoinIndex + 1; i < rangeVariables.length;
                        i++) {
                    if (rangeVariables[i].isRightJoin) {
                        rangeIterators[i] = rangeVariables[i].getFullIterator(
                            session, (RangeIteratorMain) rangeIterators[i]);
                        fullJoinIndex = i;
                        currentIndex  = i;
                        end           = false;

                        break;
                    }
                }

                if (end) {
                    break;
                }
            }

            RangeIterator it = rangeIterators[currentIndex];

            if (it.next()) {
                if (currentIndex < rangeVariables.length - 1) {
                    currentIndex++;

                    continue;
                }
            } else {
                it.reset();

                currentIndex--;

                continue;
            }

            session.sessionData.startRowProcessing();

            Object[] data = new Object[indexLimitData];

            for (int i = 0; i < indexStartAggregates; i++) {
                if (isAggregated && aggregateCheck[i]) {
                    continue;
                } else {
                    data[i] = exprColumns[i].getValue(session);
                }
            }

            for (int i = indexLimitVisible; i < indexLimitRowId; i++) {
                data[i] = it.getRowidObject();
            }

            Object[] groupData = null;

            if (isAggregated || isGrouped) {
                groupData = navigator.getGroupData(data);

                if (groupData != null) {
                    data = groupData;
                }
            }

            for (int i = indexStartAggregates; i < indexLimitExpressions;
                    i++) {
                data[i] =
                    ((ExpressionAggregate) exprColumns[i])
                        .updateAggregatingValue(session, data[i]);
            }

            if (groupData == null) {
                navigator.add(data);
            }

            if (isAggregated || isGrouped) {
                continue;
            }

            if (navigator.getSize() >= limitcount) {
                break;
            }
        }

        navigator.reset();

        if (!isGrouped && !isAggregated) {
            return result;
        }

        if (isAggregated) {
            if (!isGrouped && navigator.getSize() == 0) {
                Object[] data = new Object[exprColumns.length];

                navigator.add(data);
            }

            RangeIteratorBase it = new RangeIteratorBase(session,
                navigator.store, navigator.table, resultRangePosition);

            session.sessionContext.setRangeIterator(it);

            while (it.next()) {
                for (int i = indexStartAggregates; i < indexLimitExpressions;
                        i++) {
                    ExpressionAggregate aggregate =
                        (ExpressionAggregate) exprColumns[i];

                    it.currentData[i] = aggregate.getAggregatedValue(session,
                            it.currentData[i]);
                }

                for (int i = 0; i < indexStartAggregates; i++) {
                    if (aggregateCheck[i]) {
                        it.currentData[i] = exprColumns[i].getValue(session);
                    }
                }
            }
        }

        navigator.reset();

        if (havingCondition != null) {
            while (navigator.hasNext()) {
                Object[] data = navigator.getNext();

                if (!Boolean.TRUE.equals(
                        data[indexLimitVisible + groupByColumnCount])) {
                    navigator.remove();
                }
            }

            navigator.reset();
        }

        return result;
    }

    void setReferenceableColumns() {

        accessibleColumns = new boolean[indexLimitVisible];

        IntValueHashMap aliases = new IntValueHashMap();
        // Bundle up all the user defined aliases here.
        // We can't import java.util.Set because there is a Set
        // already imported into this class from Hsql itself.
        java.util.Set<String> userAliases = new java.util.HashSet<>();
        // Bundle up all the generated aliases here.
        java.util.Map<String, Integer> genAliases = new java.util.HashMap<>();
        for (int i = 0; i < indexLimitVisible; i++) {
            Expression expression = exprColumns[i];
            String     alias      = expression.getAlias();

            if (alias.length() == 0) {
                SimpleName name = HsqlNameManager.getAutoColumnName(i);

                expression.setAlias(name);

                genAliases.put(name.name, i);

                continue;

            }

            int index = aliases.get(alias, -1);

            userAliases.add(alias);
            if (index == -1) {
                aliases.put(alias, i);

                accessibleColumns[i] = true;
            } else {
                accessibleColumns[index] = false;
            }
        }
        for (java.util.Map.Entry<String, Integer> genAlias : genAliases.entrySet()) {
            String alias = genAlias.getKey();
            while (userAliases.contains(alias)) {
                alias = "_" + alias;
            }
            if (!alias.equals(genAlias.getKey())) {
                int idx = genAlias.getValue();
                SimpleName realAlias = HsqlNameManager.getAutoColumnName(alias);
                exprColumns[idx].setAlias(realAlias);
            }
        }
    }

    private void createResultMetaData() {

        columnTypes = new Type[indexLimitData];

        for (int i = 0; i < indexStartAggregates; i++) {
            Expression e = exprColumns[i];

            columnTypes[i] = e.getDataType();
        }

        for (int i = indexLimitVisible; i < indexLimitRowId; i++) {
            columnTypes[i] = Type.SQL_BIGINT;
        }

        for (int i = indexLimitRowId; i < indexLimitData; i++) {
            Expression e = exprColumns[i];

            columnTypes[i] = e.getDataType();
        }

        resultMetaData = ResultMetaData.newResultMetaData(columnTypes,
                columnMap, indexLimitVisible, indexLimitRowId);

        for (int i = 0; i < indexLimitVisible; i++) {
            Expression e = exprColumns[i];

            resultMetaData.columnTypes[i] = e.getDataType();

            if (i < indexLimitVisible) {
                ColumnBase column = e.getColumn();

                if (column != null) {
                    resultMetaData.columns[i]      = column;
                    resultMetaData.columnLabels[i] = e.getAlias();

                    continue;
                }

                column = new ColumnBase();

                column.setType(e.getDataType());

                resultMetaData.columns[i]      = column;
                resultMetaData.columnLabels[i] = e.getAlias();
            }
        }
    }

    @Override
    void createTable(Session session) {

        createResultTable(session);

        mainIndex = resultTable.getPrimaryIndex();

        if (sortAndSlice.hasOrder()) {
            orderIndex = resultTable.createAndAddIndexStructure(null,
                    sortAndSlice.sortOrder, sortAndSlice.sortDescending,
                    sortAndSlice.sortNullsLast, false, false, false, false);
        }

        if (isDistinctSelect || isFullOrder) {
            int[] fullCols = new int[indexLimitVisible];

            ArrayUtil.fillSequence(fullCols);

            fullIndex = resultTable.createAndAddIndexStructure(null, fullCols,
                    null, null, false, false, false, false);
            resultTable.fullIndex = fullIndex;
        }

        if (isGrouped) {
            int[] groupCols = new int[groupByColumnCount];

            for (int i = 0; i < groupByColumnCount; i++) {
                groupCols[i] = indexLimitVisible + i;
            }

            groupIndex = resultTable.createAndAddIndexStructure(null,
                    groupCols, null, null, false, false, false, false);
        } else if (isAggregated) {
            groupIndex = mainIndex;
        }
    }

    @Override
    void createResultTable(Session session) {

        HsqlName       tableName;
        HashMappedList columnList;
        int            tableType;

        tableName = session.database.nameManager.getSubqueryTableName();
        tableType = persistenceScope == TableBase.SCOPE_STATEMENT
                    ? TableBase.SYSTEM_SUBQUERY
                    : TableBase.RESULT_TABLE;
        columnList = new HashMappedList();

        for (int i = 0; i < indexLimitVisible; i++) {
            Expression e          = exprColumns[i];
            SimpleName simpleName = e.getSimpleName();
            String     nameString = simpleName.name;
            HsqlName name =
                session.database.nameManager.newColumnSchemaHsqlName(tableName,
                    simpleName);

            if (!accessibleColumns[i]) {
                nameString = HsqlNameManager.getAutoNoNameColumnString(i);
            }

            ColumnSchema column = new ColumnSchema(name, e.dataType, true,
                                                   false, null);

            columnList.add(nameString, column);
        }

        try {
            resultTable = new TableDerived(session.database, tableName,
                                           tableType, columnTypes, columnList,
                                           null);
        } catch (Exception e) {}
    }

    public String getSQL() {

        StringBuffer sb = new StringBuffer();
        int          limit;

        sb.append(Tokens.T_SELECT).append(' ');

        limit = indexLimitVisible;

        for (int i = 0; i < limit; i++) {
            if (i > 0) {
                sb.append(',');
            }

            sb.append(exprColumns[i].getSQL());
        }

        sb.append(Tokens.T_FROM);

        limit = rangeVariables.length;

        for (int i = 0; i < limit; i++) {
            RangeVariable rangeVar = rangeVariables[i];

            if (i > 0) {
                if (rangeVar.isLeftJoin && rangeVar.isRightJoin) {
                    sb.append(Tokens.T_FULL).append(' ');
                } else if (rangeVar.isLeftJoin) {
                    sb.append(Tokens.T_LEFT).append(' ');
                } else if (rangeVar.isRightJoin) {
                    sb.append(Tokens.T_RIGHT).append(' ');
                }

                sb.append(Tokens.T_JOIN).append(' ');
            }

            sb.append(rangeVar.getTable().getName().statementName);
        }

        if (isGrouped) {
            sb.append(' ').append(Tokens.T_GROUP).append(' ').append(
                Tokens.T_BY);

            limit = indexLimitVisible + groupByColumnCount;

            for (int i = indexLimitVisible; i < limit; i++) {
                sb.append(exprColumns[i].getSQL());

                if (i < limit - 1) {
                    sb.append(',');
                }
            }
        }

        if (havingCondition != null) {
            sb.append(' ').append(Tokens.T_HAVING).append(' ');
            sb.append(havingCondition.getSQL());
        }

        if (sortAndSlice.hasOrder()) {
            limit = indexStartOrderBy + sortAndSlice.getOrderLength();

            sb.append(' ').append(Tokens.T_ORDER).append(' ').append(Tokens.T_BY);
            String sep = " ";
            for (int i = indexStartOrderBy; i < limit; i++) {
                sb.append(sep).append(exprColumns[i].getSQL());
                sep = ",";
            }
        }

        if (sortAndSlice.hasLimit()) {
            sb.append(sortAndSlice.limitCondition.getLeftNode().getSQL());
        }

        return sb.toString();
    }

    @Override
    public ResultMetaData getMetaData() {
        return resultMetaData;
    }

    @Override
    public String describe(Session session) {

        StringBuffer sb;
        String       temp;

/*
        // temporary :  it is currently unclear whether this may affect
        // later attempts to retrieve an actual result (calls getResult(1)
        // in preProcess mode).  Thus, toString() probably should not be called
        // on Select objects that will actually be used to retrieve results,
        // only on Select objects used by EXPLAIN PLAN FOR
        try {
            getResult(session, 1);
        } catch (HsqlException e) {}
*/
        sb = new StringBuffer();

        sb.append(super.toString()).append("[\n");

        if (sortAndSlice.limitCondition != null) {
        	if (sortAndSlice.limitCondition.getLeftNode() != null) {
        		sb.append("offset=[").append(
        				sortAndSlice.limitCondition.getLeftNode().describe(
        						session)).append("]\n");
        	}
        	if (sortAndSlice.limitCondition.getRightNode() != null) {
        		sb.append("limit=[").append(
        				sortAndSlice.limitCondition.getRightNode().describe(
        						session)).append("]\n");
        	}
        }

        sb.append("isDistinctSelect=[").append(isDistinctSelect).append("]\n");
        sb.append("isGrouped=[").append(isGrouped).append("]\n");
        sb.append("isAggregated=[").append(isAggregated).append("]\n");
        sb.append("columns=[");

        int columns = indexLimitVisible + groupByColumnCount
                      + havingColumnCount;

        for (int i = 0; i < columns; i++) {
            int index = i;

            if (exprColumns[i].getType() == OpTypes.SIMPLE_COLUMN) {
                index = exprColumns[i].columnIndex;
            }

            sb.append(exprColumns[index].describe(session));
        }

        sb.append("\n]\n");
        sb.append("range variables=[\n");

        for (int i = 0; i < rangeVariables.length; i++) {
            sb.append("[\n");
            sb.append(rangeVariables[i].describe(session));
            sb.append("\n]");
        }

        sb.append("]\n");

        temp = queryCondition == null ? "null"
                                      : queryCondition.describe(session);

        sb.append("queryCondition=[").append(temp).append("]\n");

        temp = havingCondition == null ? "null"
                                       : havingCondition.describe(session);

        sb.append("havingCondition=[").append(temp).append("]\n");
        sb.append("groupColumns=[").append(groupColumnNames).append("]\n");

        return sb.toString();
    }

    void setUpdatability() {

        if (isAggregated || isGrouped || isDistinctSelect || !isTopLevel) {
            return;
        }

        if (sortAndSlice.hasLimit() || sortAndSlice.hasOrder()) {
            return;
        }

        if (rangeVariables.length != 1) {
            return;
        }

        RangeVariable rangeVar  = rangeVariables[0];
        Table         table     = rangeVar.getTable();
        Table         baseTable = table.getBaseTable();

        isInsertable = table.isInsertable();
        isUpdatable  = table.isUpdatable();

        if (!isInsertable && !isUpdatable) {
            return;
        }

        IntValueHashMap columns = new IntValueHashMap();
        boolean[]       checkList;
        int[]           baseColumnMap = table.getBaseTableColumnMap();
        int[]           columnMap     = new int[indexLimitVisible];

        for (int i = 0; i < indexLimitVisible; i++) {
            Expression expression = exprColumns[i];

            if (expression.getType() == OpTypes.COLUMN) {
                String name = expression.getColumn().getName().name;

                if (columns.containsKey(name)) {
                    columns.put(name, 1);

                    continue;
                }

                columns.put(name, 0);
            }
        }

        isUpdatable = false;

        for (int i = 0; i < indexLimitVisible; i++) {
            if (accessibleColumns[i]) {
                Expression expression = exprColumns[i];

                if (expression.getType() == OpTypes.COLUMN) {
                    String name = expression.getColumn().getName().name;

                    if (columns.get(name) == 0) {
                        int index = table.findColumn(name);

                        columnMap[i] = baseColumnMap[index];

                        if (columnMap[i] != -1) {
                            isUpdatable = true;
                        }

                        continue;
                    }
                }
            }

            columnMap[i] = -1;
            isInsertable = false;
        }

        if (isInsertable) {
            checkList = baseTable.getColumnCheckList(columnMap);

            for (int i = 0; i < checkList.length; i++) {
                if (checkList[i]) {
                    continue;
                }

                ColumnSchema column = baseTable.getColumn(i);

                if (column.isIdentity() || column.isGenerated()
                        || column.hasDefault() || column.isNullable()) {}
                else {
                    isInsertable = false;

                    break;
                }
            }
        }

        if (!isUpdatable) {
            isInsertable = false;
        }

        if (isUpdatable) {
            this.columnMap = columnMap;
            this.baseTable = baseTable;

            if (persistenceScope == TableBase.SCOPE_STATEMENT) {
                return;
            }

            indexLimitRowId++;

            indexLimitData = indexLimitRowId;
        }
    }

    @Override
    public Table getBaseTable() {
        return baseTable;
    }

    @Override
    public void collectAllExpressions(HsqlList set, OrderedIntHashSet typeSet,
                                      OrderedIntHashSet stopAtTypeSet) {

        for (int i = 0; i < indexStartAggregates; i++) {
            Expression.collectAllExpressions(set, exprColumns[i], typeSet,
                                             stopAtTypeSet);
        }

        Expression.collectAllExpressions(set, queryCondition, typeSet,
                                         stopAtTypeSet);
        Expression.collectAllExpressions(set, havingCondition, typeSet,
                                         stopAtTypeSet);
    }

    @Override
    public void collectObjectNames(Set set) {

        for (int i = 0; i < indexStartAggregates; i++) {
            exprColumns[i].collectObjectNames(set);
        }

        if (queryCondition != null) {
            queryCondition.collectObjectNames(set);
        }

        if (havingCondition != null) {
            havingCondition.collectObjectNames(set);
        }
    }

    void getMergedSelect() {

        RangeVariable rangeVar            = rangeVariables[0];
        Table         table               = rangeVar.getTable();
        Expression    localQueryCondition = queryCondition;
        Expression    baseQueryCondition  = null;

        if (table instanceof TableDerived) {
            QuerySpecification baseSelect =
                ((TableDerived) table).queryExpression.getMainSelect();
            RangeVariable baseRangeVariable = baseSelect.rangeVariables[0];

            rangeVariables    = new RangeVariable[1];
            rangeVariables[0] = new RangeVariable(baseRangeVariable);

            Expression[] newExprColumns = new Expression[indexLimitRowId];

            for (int i = 0; i < indexLimitVisible; i++) {
                Expression e = exprColumns[i];

                newExprColumns[i] = e.replaceColumnReferences(rangeVar,
                        baseSelect.exprColumns);
            }

            exprColumns = newExprColumns;

            if (localQueryCondition != null) {
                localQueryCondition =
                    localQueryCondition.replaceColumnReferences(rangeVar,
                        baseSelect.exprColumns);
            }

            baseQueryCondition  = baseSelect.queryCondition;
            checkQueryCondition = baseSelect.checkQueryCondition;
        }

        queryCondition = ExpressionLogical.andExpressions(baseQueryCondition,
                localQueryCondition);

        if (queryCondition != null) {
            tempSet.clear();
            Expression.collectAllExpressions(tempSet, queryCondition,
                                             Expression.subqueryExpressionSet,
                                             Expression.emptyExpressionSet);

            int size = tempSet.size();

            for (int i = 0; i < size; i++) {
                Expression e = (Expression) tempSet.get(i);

                e.collectObjectNames(tempSet);
            }

            if (tempSet.contains(baseTable.getName())) {
                isUpdatable  = false;
                isInsertable = false;
            }
        }

        if (view != null) {
            switch (view.getCheckOption()) {

                case SchemaObject.ViewCheckModes.CHECK_LOCAL :
                    if (!isUpdatable) {
                        throw Error.error(ErrorCode.X_42537);
                    }

                    checkQueryCondition = localQueryCondition;
                    break;

                case SchemaObject.ViewCheckModes.CHECK_CASCADE :
                    if (!isUpdatable) {
                        throw Error.error(ErrorCode.X_42537);
                    }

                    checkQueryCondition = queryCondition;
                    break;
            }
        }

        setRangeVariableConditions();
    }

    /**
     * Not for views. Only used on root node.
     */
    @Override
    public void setAsTopLevel() {

        setReturningResultSet();

        acceptsSequences = true;
        isTopLevel       = true;
    }

    @Override
    void setReturningResultSet() {
        persistenceScope = TableBase.SCOPE_SESSION;
        columnMode       = TableBase.COLUMNS_UNREFERENCED;
    }

    @Override
    public boolean isSingleColumn() {
        return indexLimitVisible == 1;
    }

    @Override
    public String[] getColumnNames() {

        String[] names = new String[indexLimitVisible];

        for (int i = 0; i < indexLimitVisible; i++) {
            names[i] = exprColumns[i].getAlias();
        }

        return names;
    }

    @Override
    public Type[] getColumnTypes() {

        if (columnTypes.length == indexLimitVisible) {
            return columnTypes;
        }

        Type[] types = new Type[indexLimitVisible];

        ArrayUtil.copyArray(columnTypes, types, types.length);

        return types;
    }

    @Override
    public int getColumnCount() {
        return indexLimitVisible;
    }

    @Override
    public int[] getBaseTableColumnMap() {
        return columnMap;
    }

    @Override
    public Expression getCheckCondition() {
        return queryCondition;
    }

    @Override
    void getBaseTableNames(OrderedHashSet set) {

        for (int i = 0; i < rangeVariables.length; i++) {
            Table    rangeTable = rangeVariables[i].rangeTable;
            HsqlName name       = rangeTable.getName();

            if (rangeTable.isReadOnly() || rangeTable.isTemp()) {
                continue;
            }

            if (name.schema == SqlInvariants.SYSTEM_SCHEMA_HSQLNAME) {
                continue;
            }

            set.add(name);
        }
    }

    /************************* Volt DB Extensions *************************/
    Expression getHavingCondition() { return havingCondition; }

    // Display columns expressions
    List<Expression> displayCols = new ArrayList<>();

    /**
     * Dumps the exprColumns list for this query specification.
     * Writes to stdout.
     *
     * This method is useful to understand how the HSQL parser
     * transforms its data structures during parsing.  For example,
     * place call to this method at the beginning and end of
     * resolveGroups() to see what it does.
     * Since it has no callers in the production code, declaring
     * it private causes a warning, so it is arbitrarily declared
     * protected.
     *
     * @param header    A string to be prepended to output
     */
    protected void dumpExprColumns(String header){
        System.out.println("\n\n*********************************************");
        System.out.println(header);
        try {
            System.out.println(getSQL());
        } catch (Exception e) {
        }
        for (int i = 0; i < exprColumns.length; ++i) {
            if (i == 0)
                System.out.println("Visible columns:");
            if (i == indexStartOrderBy)
                System.out.println("start order by:");
            if (i == indexStartAggregates)
                System.out.println("start aggregates:");
            if (i == indexLimitVisible)
                System.out.println("After limit of visible columns:");

            System.out.println(i + ": " + exprColumns[i]);
        }

        System.out.println("\n\n");
    }
    /**********************************************************************/
}
