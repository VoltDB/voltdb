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

import org.hsqldb_voltpatches.HsqlNameManager.HsqlName;
import org.hsqldb_voltpatches.ParserDQL.CompileContext;
import org.hsqldb_voltpatches.index.Index;
import org.hsqldb_voltpatches.lib.ArrayListIdentity;
import org.hsqldb_voltpatches.lib.ArrayUtil;
import org.hsqldb_voltpatches.lib.HashMappedList;
import org.hsqldb_voltpatches.lib.HsqlList;
import org.hsqldb_voltpatches.lib.OrderedHashSet;
import org.hsqldb_voltpatches.lib.OrderedIntHashSet;
import org.hsqldb_voltpatches.lib.Set;
import org.hsqldb_voltpatches.navigator.RowSetNavigatorData;
import org.hsqldb_voltpatches.result.Result;
import org.hsqldb_voltpatches.result.ResultMetaData;
import org.hsqldb_voltpatches.types.Type;

/**
 * Implementation of an SQL query expression
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 1.9.0
 * @since 1.9.0
 */

/**
 * @todo 1.9.0 - review these
 * - work out usage of getMainSelect etc and add relevant methods
 * - Result metadata for the final result of QueryExpression
 *
 */
public class QueryExpression {

    public static final int NOUNION       = 0,
                            UNION         = 1,
                            UNION_ALL     = 2,
                            INTERSECT     = 3,
                            INTERSECT_ALL = 4,
                            EXCEPT_ALL    = 5,
                            EXCEPT        = 6,
                            UNION_TERM    = 7;

    //
    int                     columnCount;
    private QueryExpression leftQueryExpression;
    private QueryExpression rightQueryExpression;
    SortAndSlice            sortAndSlice;
    private int             unionType;
    private boolean         unionCorresponding;
    private OrderedHashSet  unionCorrespondingColumns;
    int[]                   unionColumnMap;
    Type[]                  unionColumnTypes;
    boolean                 isFullOrder;

    //
    HsqlList unresolvedExpressions;

    //
    boolean isResolved;

    //
    int persistenceScope = TableBase.SCOPE_STATEMENT;

    //
    int columnMode = TableBase.COLUMNS_REFERENCED;

    //
    ResultMetaData resultMetaData;
    boolean[]      accessibleColumns;

    //
    View    view;
    boolean isUpdatable;
    boolean isInsertable;
    boolean isCheckable;
    boolean isTopLevel;

    //
    public TableBase resultTable;
    public Index     mainIndex;
    public Index     fullIndex;
    public Index     orderIndex;

    //
    CompileContext compileContext;

    QueryExpression(CompileContext compileContext) {
        this.compileContext = compileContext;
        sortAndSlice        = SortAndSlice.noSort;
    }

    public QueryExpression(CompileContext compileContext,
                           QueryExpression leftQueryExpression) {

        this(compileContext);

        sortAndSlice             = SortAndSlice.noSort;
        this.leftQueryExpression = leftQueryExpression;
    }

    void addUnion(QueryExpression queryExpression, int unionType) {

        sortAndSlice              = SortAndSlice.noSort;
        this.rightQueryExpression = queryExpression;
        this.unionType            = unionType;

        setFullOrder();
    }

    void addSortAndSlice(SortAndSlice sortAndSlice) {
        this.sortAndSlice      = sortAndSlice;
        sortAndSlice.sortUnion = true;
    }

    public void setUnionCorresoponding() {
        unionCorresponding = true;
    }

    public void setUnionCorrespondingColumns(OrderedHashSet names) {
        unionCorrespondingColumns = names;
    }

    public void setFullOrder() {

        isFullOrder = true;

        if (leftQueryExpression == null) {
            return;
        }

        leftQueryExpression.setFullOrder();
        rightQueryExpression.setFullOrder();
    }

    public void resolve(Session session) {

        resolveReferences(session);
        ExpressionColumn.checkColumnsResolved(unresolvedExpressions);
        resolveTypes(session);
    }

    public void resolve(Session session, RangeVariable[] outerRanges) {

        resolveReferences(session);

        if (unresolvedExpressions != null) {
            for (int i = 0; i < unresolvedExpressions.size(); i++) {
                Expression e    = (Expression) unresolvedExpressions.get(i);
                HsqlList   list = e.resolveColumnReferences(outerRanges, null);

                ExpressionColumn.checkColumnsResolved(list);
            }
        }

        resolveTypes(session);
    }

    public void resolveReferences(Session session) {

        leftQueryExpression.resolveReferences(session);
        rightQueryExpression.resolveReferences(session);
        addUnresolvedExpressions(leftQueryExpression.unresolvedExpressions);
        addUnresolvedExpressions(rightQueryExpression.unresolvedExpressions);

        if (!unionCorresponding) {
            columnCount = leftQueryExpression.getColumnCount();

            int rightCount = rightQueryExpression.getColumnCount();

            if (columnCount != rightCount) {
                throw Error.error(ErrorCode.X_42594);
            }

            unionColumnTypes = new Type[columnCount];
            leftQueryExpression.unionColumnMap =
                rightQueryExpression.unionColumnMap = new int[columnCount];

            ArrayUtil.fillSequence(leftQueryExpression.unionColumnMap);
            resolveColumnRefernecesInUnionOrderBy();

            return;
        }

        String[] leftNames  = leftQueryExpression.getColumnNames();
        String[] rightNames = rightQueryExpression.getColumnNames();

        if (unionCorrespondingColumns == null) {
            unionCorrespondingColumns = new OrderedHashSet();

            OrderedIntHashSet leftColumns  = new OrderedIntHashSet();
            OrderedIntHashSet rightColumns = new OrderedIntHashSet();

            for (int i = 0; i < leftNames.length; i++) {
                String name  = leftNames[i];
                int    index = ArrayUtil.find(rightNames, name);

                if (name.length() > 0 && index != -1) {
                    leftColumns.add(i);
                    rightColumns.add(index);
                    unionCorrespondingColumns.add(name);
                }
            }

            if (unionCorrespondingColumns.isEmpty()) {
                throw Error.error(ErrorCode.X_42579);
            }

            leftQueryExpression.unionColumnMap  = leftColumns.toArray();
            rightQueryExpression.unionColumnMap = rightColumns.toArray();
        } else {
            leftQueryExpression.unionColumnMap =
                new int[unionCorrespondingColumns.size()];
            rightQueryExpression.unionColumnMap =
                new int[unionCorrespondingColumns.size()];

            for (int i = 0; i < unionCorrespondingColumns.size(); i++) {
                String name  = (String) unionCorrespondingColumns.get(i);
                int    index = ArrayUtil.find(leftNames, name);

                if (index == -1) {
                    throw Error.error(ErrorCode.X_42579);
                }

                leftQueryExpression.unionColumnMap[i] = index;
                index = ArrayUtil.find(rightNames, name);

                if (index == -1) {
                    throw Error.error(ErrorCode.X_42579);
                }

                rightQueryExpression.unionColumnMap[i] = index;
            }
        }

        columnCount      = unionCorrespondingColumns.size();
        unionColumnTypes = new Type[columnCount];

        resolveColumnRefernecesInUnionOrderBy();
    }

    /**
     * Only simple column reference or column position allowed
     */
    void resolveColumnRefernecesInUnionOrderBy() {

        int orderCount = sortAndSlice.getOrderLength();

        if (orderCount == 0) {
            return;
        }

        String[] unionColumnNames = getColumnNames();

        for (int i = 0; i < orderCount; i++) {
            Expression sort = (Expression) sortAndSlice.exprList.get(i);
            Expression e    = sort.getLeftNode();

            if (e.getType() == OpTypes.VALUE) {
                if (e.getDataType().typeCode == Types.SQL_INTEGER) {
                    int index = ((Integer) e.getValue(null)).intValue();

                    if (0 < index && index <= unionColumnNames.length) {
                        sort.getLeftNode().queryTableColumnIndex = index - 1;

                        continue;
                    }
                }
            } else if (e.getType() == OpTypes.COLUMN) {
                int index = ArrayUtil.find(unionColumnNames,
                                           e.getColumnName());

                if (index >= 0) {
                    sort.getLeftNode().queryTableColumnIndex = index;

                    continue;
                }
            }

            throw Error.error(ErrorCode.X_42576);
        }

        sortAndSlice.prepare(null);
    }

    private void addUnresolvedExpressions(HsqlList expressions) {

        if (expressions == null) {
            return;
        }

        if (unresolvedExpressions == null) {
            unresolvedExpressions = new ArrayListIdentity();
        }

        unresolvedExpressions.addAll(expressions);
    }

    public void resolveTypes(Session session) {

        if (isResolved) {
            return;
        }

        resolveTypesPartOne(session);
        resolveTypesPartTwo(session);

        isResolved = true;
    }

    void resolveTypesPartOne(Session session) {

        ArrayUtil.projectRowReverse(leftQueryExpression.unionColumnTypes,
                                    leftQueryExpression.unionColumnMap,
                                    unionColumnTypes);
        leftQueryExpression.resolveTypesPartOne(session);
        ArrayUtil.projectRow(leftQueryExpression.unionColumnTypes,
                             leftQueryExpression.unionColumnMap,
                             unionColumnTypes);
        ArrayUtil.projectRowReverse(rightQueryExpression.unionColumnTypes,
                                    rightQueryExpression.unionColumnMap,
                                    unionColumnTypes);
        rightQueryExpression.resolveTypesPartOne(session);
        ArrayUtil.projectRow(rightQueryExpression.unionColumnTypes,
                             rightQueryExpression.unionColumnMap,
                             unionColumnTypes);
    }

    void resolveTypesPartTwo(Session session) {

        ArrayUtil.projectRowReverse(leftQueryExpression.unionColumnTypes,
                                    leftQueryExpression.unionColumnMap,
                                    unionColumnTypes);
        leftQueryExpression.resolveTypesPartTwo(session);
        ArrayUtil.projectRowReverse(rightQueryExpression.unionColumnTypes,
                                    rightQueryExpression.unionColumnMap,
                                    unionColumnTypes);
        rightQueryExpression.resolveTypesPartTwo(session);

        //
        if (unionCorresponding) {
            resultMetaData = leftQueryExpression.getMetaData().getNewMetaData(
                leftQueryExpression.unionColumnMap);

            createTable(session);
        }

        if (sortAndSlice.hasOrder()) {
            QueryExpression queryExpression = this;

            while (true) {
                if (queryExpression.leftQueryExpression == null
                        || queryExpression.unionCorresponding) {
                    sortAndSlice.setIndex(queryExpression.resultTable);

                    break;
                }

                queryExpression = queryExpression.leftQueryExpression;
            }
        }
    }

    public Object[] getValues(Session session) {

        Result r    = getResult(session, 2);
        int    size = r.getNavigator().getSize();

        if (size == 0) {
            return new Object[r.metaData.getColumnCount()];
        } else if (size == 1) {
            return r.getSingleRowData();
        } else {
            throw Error.error(ErrorCode.X_21000);
        }
    }

    public Object[] getSingleRowValues(Session session) {

        Result r    = getResult(session, 2);
        int    size = r.getNavigator().getSize();

        if (size == 0) {
            return null;
        } else if (size == 1) {
            return r.getSingleRowData();
        } else {
            throw Error.error(ErrorCode.X_21000);
        }
    }

    public Object getValue(Session session) {

        Object[] values = getValues(session);

        return values[0];
    }

    Result getResult(Session session, int maxRows) {

        int    currentMaxRows = unionType == UNION_ALL ? maxRows
                                                       : Integer.MAX_VALUE;
        Result first = leftQueryExpression.getResult(session, currentMaxRows);
        RowSetNavigatorData navigator =
            (RowSetNavigatorData) first.getNavigator();
        Result second = rightQueryExpression.getResult(session,
            currentMaxRows);
        RowSetNavigatorData rightNavigator =
            (RowSetNavigatorData) second.getNavigator();

        if (unionCorresponding) {
            RowSetNavigatorData rowSet = new RowSetNavigatorData(session,
                this);

            rowSet.copy(navigator, leftQueryExpression.unionColumnMap);

            navigator = rowSet;

            first.setNavigator(navigator);

            first.metaData = this.getMetaData();
            rowSet         = new RowSetNavigatorData(session, this);

            if (unionType != UNION && unionType != UNION_ALL) {
                rowSet.copy(rightNavigator,
                            rightQueryExpression.unionColumnMap);

                rightNavigator = rowSet;
            }
        }

        switch (unionType) {

            case UNION :
                navigator.union(rightNavigator,
                                rightQueryExpression.unionColumnMap);
                break;

            case UNION_ALL :
                navigator.unionAll(rightNavigator,
                                   rightQueryExpression.unionColumnMap);
                break;

            case INTERSECT :
                navigator.intersect(rightNavigator);
                break;

            case INTERSECT_ALL :
                navigator.intersectAll(rightNavigator);
                break;

            case EXCEPT :
                navigator.except(rightNavigator);
                break;

            case EXCEPT_ALL :
                navigator.exceptAll(rightNavigator);
                break;

            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "QueryExpression");
        }

        if (sortAndSlice.hasOrder()) {
            RowSetNavigatorData nav =
                (RowSetNavigatorData) first.getNavigator();

            nav.sortUnion(sortAndSlice);
            nav.trim(sortAndSlice.getLimitStart(session),
                     sortAndSlice.getLimitCount(session, maxRows));
        }

        navigator.reset();

        return first;
    }

    public boolean isSingleColumn() {
        return leftQueryExpression.isSingleColumn();
    }

    public ResultMetaData getMetaData() {

        if (resultMetaData != null) {
            return resultMetaData;
        }

        return leftQueryExpression.getMetaData();
    }

    public QuerySpecification getMainSelect() {

        if (leftQueryExpression == null) {
            return (QuerySpecification) this;
        }

        return leftQueryExpression.getMainSelect();
    }

    /** @todo 1.9.0 review */
    public String describe(Session session) {
        return leftQueryExpression.describe(session);
    }

    public HsqlList getUnresolvedExpressions() {
        return unresolvedExpressions;
    }

    public boolean areColumnsResolved() {
        return unresolvedExpressions == null
               || unresolvedExpressions.isEmpty();
    }

    String[] getColumnNames() {

        if (unionCorrespondingColumns == null) {
            return leftQueryExpression.getColumnNames();
        }

        String[] names = new String[unionCorrespondingColumns.size()];

        unionCorrespondingColumns.toArray(names);

        return names;
    }

    public Type[] getColumnTypes() {
        return unionColumnTypes;
    }

    public int getColumnCount() {

        if (unionCorrespondingColumns == null) {
            int left  = leftQueryExpression.getColumnCount();
            int right = rightQueryExpression.getColumnCount();

            if (left != right) {
                throw Error.error(ErrorCode.X_42594);
            }

            return left;
        }

        return unionCorrespondingColumns.size();
    }

    public void collectAllExpressions(HsqlList set, OrderedIntHashSet typeSet,
                                      OrderedIntHashSet stopAtTypeSet) {

        leftQueryExpression.collectAllExpressions(set, typeSet, stopAtTypeSet);

        if (rightQueryExpression != null) {
            rightQueryExpression.collectAllExpressions(set, typeSet,
                    stopAtTypeSet);
        }
    }

    public void collectObjectNames(Set set) {

        leftQueryExpression.collectObjectNames(set);

        if (rightQueryExpression != null) {
            rightQueryExpression.collectObjectNames(set);
        }
    }

    public HashMappedList getColumns() {

        this.getResultTable();

        return ((TableDerived) getResultTable()).columnList;
    }

    /**
     * Used prior to type resolution
     */
    public void setView(View view) {
        this.view = view;
    }

    /**
     * Used in views after full type resolution
     */
    public void setTableColumnNames(HashMappedList list) {

        if (resultTable != null) {
            ((TableDerived) resultTable).columnList = list;

            return;
        }

        leftQueryExpression.setTableColumnNames(list);
    }

    void createTable(Session session) {

        createResultTable(session);

        mainIndex = resultTable.getPrimaryIndex();

        if (sortAndSlice.hasOrder()) {
            orderIndex = resultTable.createAndAddIndexStructure(null,
                    sortAndSlice.sortOrder, sortAndSlice.sortDescending,
                    sortAndSlice.sortNullsLast, false, false, false);
        }

        int[] fullCols = new int[columnCount];

        ArrayUtil.fillSequence(fullCols);

        fullIndex = resultTable.createAndAddIndexStructure(null, fullCols,
                null, null, false, false, false);
        resultTable.fullIndex = fullIndex;
    }

    void createResultTable(Session session) {

        HsqlName       tableName;
        HashMappedList columnList;
        int            tableType;

        tableName = session.database.nameManager.getSubqueryTableName();
        tableType = persistenceScope == TableBase.SCOPE_STATEMENT
                    ? TableBase.SYSTEM_SUBQUERY
                    : TableBase.RESULT_TABLE;
        columnList = leftQueryExpression.getUnionColumns();

        try {
            resultTable = new TableDerived(session.database, tableName,
                                           tableType, unionColumnTypes,
                                           columnList, null);
        } catch (Exception e) {}
    }

    public void setColumnsDefined() {

        columnMode = TableBase.COLUMNS_REFERENCED;

        if (leftQueryExpression != null) {
            leftQueryExpression.setColumnsDefined();
        }
    }

    /**
     * Not for views. Only used on root node.
     */
    public void setAsTopLevel() {

        if (compileContext.getSequences().length > 0) {
            throw Error.error(ErrorCode.X_42598);
        }

        isTopLevel = true;

        setReturningResultSet();
    }

    /**
     * Sets the scope to SESSION for the QueryExpression object that creates
     * the table
     */
    void setReturningResultSet() {

        if (unionCorresponding) {
            persistenceScope = TableBase.SCOPE_SESSION;
            columnMode       = TableBase.COLUMNS_UNREFERENCED;

            return;
        }

        leftQueryExpression.setReturningResultSet();
    }

    private HashMappedList getUnionColumns() {

        if (unionCorresponding || leftQueryExpression == null) {
            HashMappedList columns = ((TableDerived) resultTable).columnList;
            HashMappedList list    = new HashMappedList();

            for (int i = 0; i < unionColumnMap.length; i++) {
                ColumnSchema column = (ColumnSchema) columns.get(i);

                list.add(column.getName().name, column);
            }

            return list;
        }

        return leftQueryExpression.getUnionColumns();
    }

    public HsqlName[] getResultColumnNames() {

        if (resultTable == null) {
            return leftQueryExpression.getResultColumnNames();
        }

        HashMappedList list = ((TableDerived) resultTable).columnList;
        HsqlName[]     resultColumnNames = new HsqlName[list.size()];

        for (int i = 0; i < resultColumnNames.length; i++) {
            resultColumnNames[i] = ((ColumnSchema) list.get(i)).getName();
        }

        return resultColumnNames;
    }

    public TableBase getResultTable() {

        if (resultTable != null) {
            return resultTable;
        }

        if (leftQueryExpression != null) {
            return leftQueryExpression.getResultTable();
        }

        return null;
    }

    //
    public Table getBaseTable() {
        return null;
    }

    public boolean isUpdatable() {
        return isUpdatable;
    }

    public boolean isInsertable() {
        return isInsertable;
    }

    public int[] getBaseTableColumnMap() {
        return null;
    }

    public Expression getCheckCondition() {
        return null;
    }

    public boolean hasReference(RangeVariable range) {

        if (leftQueryExpression.hasReference(range)) {
            return true;
        }

        if (rightQueryExpression.hasReference(range)) {
            return true;
        }

        return false;
    }

    void getBaseTableNames(OrderedHashSet set) {
        leftQueryExpression.getBaseTableNames(set);
        rightQueryExpression.getBaseTableNames(set);
    }

    /************************* Volt DB Extensions *************************/

    private static final String[] m_setOperatorNames = new String[] {
        "NOUNION",
        "UNION",
        "UNION_ALL",
        "INTERSECT",
        "INTERSECT_ALL",
        "EXCEPT_ALL",
        "EXCEPT",
        "UNION_TERM",
        };

    public final String operatorName() {
        if (unionType < 0 || unionType >= m_setOperatorNames.length ) {
            return "INVALID";
        }
        return m_setOperatorNames[unionType];
    }

    /**
     * VoltDB added method to get a union operation type.
     * @return int representing union operation type.
     */
    public int getUnionType() {
        return unionType;
    }

    /**
     * VoltDB added method to get the left expression.
     * @return QueryExpression Left expression.
     */
    public QueryExpression getLeftQueryExpression() {
        return leftQueryExpression;
    }

    /**
     * VoltDB added method to get the right expression.
     * @return QueryExpression Right expression.
     */
    public QueryExpression getRightQueryExpression() {
        return rightQueryExpression;
    }
    /**********************************************************************/
}
