/* Copyright (c) 2001-2011, The HSQL Development Group
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
import org.hsqldb_voltpatches.error.Error;
import org.hsqldb_voltpatches.error.ErrorCode;
import org.hsqldb_voltpatches.lib.ArrayUtil;
import org.hsqldb_voltpatches.lib.HashMappedList;
import org.hsqldb_voltpatches.navigator.RowIterator;
import org.hsqldb_voltpatches.navigator.RowSetNavigatorData;
import org.hsqldb_voltpatches.navigator.RowSetNavigatorDataTable;
import org.hsqldb_voltpatches.persist.PersistentStore;
import org.hsqldb_voltpatches.result.Result;
import org.hsqldb_voltpatches.types.Type;

/**
 * Table with data derived from a query expression.
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 2.3.0
 * @since 1.9.0
 */
public class TableDerived extends Table {

    //
    public final static TableDerived[] emptyArray = new TableDerived[]{};

    //
    QueryExpression queryExpression;
    Expression      dataExpression;
    boolean         uniqueRows;
    boolean         uniquePredicate;
    String          sql;
    View            view;
    int             depth;
    boolean         canRecompile = false;
    // A VoltDB extension to support subquery serialization
    Expression      voltDataExpression; //  for VoltDB
    // End of VoltDB extension

    public TableDerived(Database database, HsqlName name, int type) {

        super(database, name, type);

        switch (type) {

            // for special use, not INFORMATION_SCHEMA views
            case TableBase.CHANGE_SET_TABLE :
            case TableBase.SYSTEM_TABLE :
            case TableBase.FUNCTION_TABLE :
            case TableBase.VIEW_TABLE :
            case TableBase.RESULT_TABLE :
            case TableBase.SYSTEM_SUBQUERY :
                break;

            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "Table");
        }
    }

    public TableDerived(Database database, HsqlName name, int type,
                        Type[] columnTypes, HashMappedList columnList,
                        int[] pkColumns) {

        this(database, name, type);

        this.colTypes   = columnTypes;
        this.columnList = columnList;
        columnCount     = columnList.size();

        createPrimaryKey(null, pkColumns, true);
    }

    public TableDerived(Database database, HsqlName name, int type,
                        QueryExpression queryExpression,
                        Expression dataExpression, int opType, int depth) {

        super(database, name, type);

        switch (type) {

            case TableBase.SYSTEM_SUBQUERY :
            case TableBase.VIEW_TABLE :
                break;

            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "Table");
        }

        this.queryExpression = queryExpression;
        this.dataExpression  = dataExpression;
        this.depth           = depth;

        switch (opType) {

            case OpTypes.EXISTS :
                queryExpression.setSingleRow();
                break;

            case OpTypes.IN :
                if (queryExpression != null) {
                    queryExpression.setFullOrder();
                }

                uniqueRows = true;
                break;

            case OpTypes.UNIQUE :
                queryExpression.setFullOrder();

                uniquePredicate = true;
                break;
        }

        if (dataExpression != null) {
            dataExpression.table = this;
        }
    }

    public TableDerived newDerivedTable(Session session) {

        TableDerived td = this;

        if (isRecompiled()) {
            ParserDQL p = new ParserDQL(session, new Scanner(),
                                        session.parser.compileContext);

            p.reset(sql);
            p.read();

            td = p.XreadSubqueryTableBody(tableName, OpTypes.TABLE_SUBQUERY);

            td.queryExpression.resolve(session);

            td.columnList   = columnList;
            td.columnCount  = columnList.size();
            td.triggerList  = triggerList;
            td.triggerLists = triggerLists;
            td.view         = view;

            td.createPrimaryKey();
        }

        return td;
    }

    public int getId() {
        return 0;
    }

    public boolean isQueryBased() {
        return true;
    }

    public boolean isWritable() {
        return true;
    }

    public boolean isInsertable() {

        if (view != null && view.isTriggerInsertable) {
            return false;
        }

        return queryExpression == null ? false
                                       : queryExpression.isInsertable();
    }

    public boolean isUpdatable() {

        if (view != null && view.isTriggerUpdatable) {
            return false;
        }

        return queryExpression == null ? false
                                       : queryExpression.isUpdatable();
    }

    public int[] getUpdatableColumns() {

        if (queryExpression != null) {
            return queryExpression.getBaseTableColumnMap();
        }

        return defaultColumnMap;
    }

    public boolean isTriggerInsertable() {

        if (view != null) {
            return view.isTriggerInsertable;
        }

        return false;
    }

    public boolean isTriggerUpdatable() {

        if (view != null) {
            return view.isTriggerUpdatable;
        }

        return false;
    }

    public boolean isTriggerDeletable() {

        if (view != null) {
            return view.isTriggerDeletable;
        }

        return false;
    }

    public Table getBaseTable() {
        return queryExpression == null ? this
                                       : queryExpression.getBaseTable();
    }

    public int[] getBaseTableColumnMap() {

        return queryExpression == null ? null
                                       : queryExpression
                                           .getBaseTableColumnMap();
    }

    public QueryExpression getQueryExpression() {
        return queryExpression;
    }

    public Expression getDataExpression() {
        return dataExpression;
    }

    public void prepareTable() {

        if (columnCount > 0) {
            return;
        }

        if (dataExpression != null) {
            if (columnCount == 0) {
                TableUtil.addAutoColumns(this, dataExpression.nodeDataTypes);
                setTableIndexesForSubquery();
            }
        }

        if (queryExpression != null) {
            columnList  = queryExpression.getColumns();
            columnCount = queryExpression.getColumnCount();

            setTableIndexesForSubquery();
        }
    }

    public void prepareTable(HsqlName[] columns) {

        prepareTable();

        if (columns != null) {
            if (columns.length != columnList.size()) {
                throw Error.error(ErrorCode.X_42593);
            }

            for (int i = 0; i < columnCount; i++) {
                columnList.setKey(i, columns[i].name);

                ColumnSchema col = (ColumnSchema) columnList.get(i);

                col.setName(columns[i]);
            }
        }
    }

    private void setTableIndexesForSubquery() {

        int[] cols = null;

        if (uniqueRows || uniquePredicate) {
            cols = new int[getColumnCount()];

            ArrayUtil.fillSequence(cols);
        }

        int pkcols[] = uniqueRows ? cols
                                  : null;

        if (primaryKeyCols == null) {
            createPrimaryKey(null, pkcols, false);
        }

        if (uniqueRows) {
            fullIndex = getPrimaryIndex();
        } else if (uniquePredicate) {
            fullIndex = createIndexForColumns(null, cols);
        }
    }

    void setCorrelated() {

        if (dataExpression != null) {
            dataExpression.isCorrelated = true;
        }

        if (queryExpression != null) {
            queryExpression.isCorrelated = true;
        }
    }

    boolean isCorrelated() {

        if (dataExpression != null) {
            return dataExpression.isCorrelated;
        }

        if (queryExpression != null) {
            return queryExpression.isCorrelated;
        }

        return false;
    }

    boolean hasUniqueNotNullRows(Session session) {
        return getNavigator(session).hasUniqueNotNullRows(session);
    }

    void resetToView() {
        queryExpression = view.getQueryExpression();
    }

    public void materialise(Session session) {

        session.sessionContext.pushStatementState();

        try {
            PersistentStore store;

            // table constructors
            if (dataExpression != null) {
                store = session.sessionData.getSubqueryRowStore(this);

                dataExpression.insertValuesIntoSubqueryTable(session, store);

                return;
            }

            if (queryExpression == null) {
                return;
            }

            Result result;

            result = queryExpression.getResult(session, 0);

            if (uniqueRows) {
                RowSetNavigatorData navigator =
                    ((RowSetNavigatorData) result.getNavigator());

                navigator.removeDuplicates(session);
            }

            store = session.sessionData.getSubqueryRowStore(this);

            insertResult(session, store, result);
            result.getNavigator().release();
        } finally {
            session.sessionContext.popStatementState();
        }
    }

    public void materialiseCorrelated(Session session) {

        if (isCorrelated()) {
            materialise(session);
        }
    }

    public boolean isRecompiled() {

        if (canRecompile && queryExpression instanceof QuerySpecification) {
            QuerySpecification qs = (QuerySpecification) queryExpression;

            if (qs.isAggregated || qs.isGrouped || qs.isOrderSensitive) {
                return false;
            } else {
                return true;
            }
        }

        return false;
    }

    public Object[] getValues(Session session) {

        RowIterator it = rowIterator(session);

        if (it.hasNext()) {
            Row row = it.getNextRow();

            if (it.hasNext()) {
                throw Error.error(ErrorCode.X_21000);
            }

            return row.getData();
        } else {
            return new Object[getColumnCount()];
        }
    }

    public Object getValue(Session session) {

        Object[] data = getValues(session);

        return data[0];
    }

    public RowSetNavigatorData getNavigator(Session session) {

        RowSetNavigatorData navigator = new RowSetNavigatorDataTable(session,
            this);

        return navigator;
    }

    public void setSQL(String sql) {
        this.sql = sql;
    }
}
