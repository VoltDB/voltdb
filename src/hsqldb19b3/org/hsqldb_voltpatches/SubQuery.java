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

import org.hsqldb_voltpatches.lib.ObjectComparator;
import org.hsqldb_voltpatches.navigator.RowIterator;
import org.hsqldb_voltpatches.navigator.RowSetNavigatorData;
import org.hsqldb_voltpatches.persist.PersistentStore;
import org.hsqldb_voltpatches.result.Result;

/**
 * Represents an SQL view or anonymous subquery (inline virtual table
 * descriptor) nested within an SQL statement. <p>
 *
 * Implements {@link org.hsqldb_voltpatches.lib.ObjectComparator ObjectComparator} to
 * provide the correct order of materialization for nested views / subqueries.
 *
 * @author Campbell Boucher-Burnett (boucherb@users dot sourceforge.net)
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 */
class SubQuery implements ObjectComparator {

    int                  level;
    private boolean      isCorrelated;
    private boolean      isExistsPredicate;
    private boolean      uniqueRows;
    private boolean      isUniquePredicate;
    QueryExpression      queryExpression;
    Database             database;
    private TableDerived table;
    View                 view;
    View                 parentView;

    // IN condition optimisation
    Expression dataExpression;
    boolean    isDataExpression;

    //
    public final static SubQuery[] emptySubqueryArray = new SubQuery[]{};

    SubQuery(Database database, int level, QueryExpression queryExpression,
             int mode) {

        this.level           = level;
        this.queryExpression = queryExpression;
        this.database        = database;

        switch (mode) {

            case OpTypes.EXISTS :
                isExistsPredicate = true;
                break;

            case OpTypes.IN :
                uniqueRows = true;

                if (queryExpression != null) {
                    queryExpression.setFullOrder();
                }
                break;

            case OpTypes.UNIQUE :
                isUniquePredicate = true;

                if (queryExpression != null) {
                    queryExpression.setFullOrder();
                }
        }
    }

    SubQuery(Database database, int level, QueryExpression queryExpression,
             View view) {

        this.level           = level;
        this.queryExpression = queryExpression;
        this.database        = database;
        this.view            = view;
    }

    SubQuery(Database database, int level, Expression dataExpression,
             int mode) {

        this.level              = level;
        this.database           = database;
        this.dataExpression     = dataExpression;
        dataExpression.subQuery = this;
        isDataExpression        = true;

        switch (mode) {

            case OpTypes.IN :
                uniqueRows = true;
                break;
        }
    }

    public boolean isCorrelated() {
        return isCorrelated;
    }

    public void setCorrelated() {
        isCorrelated = true;
    }

    public TableDerived getTable() {
        return table;
    }

    public void prepareTable(Session session) {

        if (table != null) {
            return;
        }

        if (view == null) {
            table = TableUtil.newSubqueryTable(database, null);

            if (isDataExpression) {
                TableUtil.setTableColumnsForSubquery(
                    table, dataExpression.nodeDataTypes,
                    uniqueRows || isUniquePredicate);
            } else {
                TableUtil.setTableColumnsForSubquery(table, queryExpression,
                                                     uniqueRows
                                                     || isUniquePredicate);
            }
        } else {
            table = new TableDerived(database, view.getName(),
                                     TableBase.VIEW_TABLE, queryExpression);
            table.columnList  = view.columnList;
            table.columnCount = table.columnList.size();

            table.createPrimaryKey();
        }
    }

    public void materialiseCorrelated(Session session) {

        if (isCorrelated) {
            materialise(session);
        }
    }

    /**
     * Fills the table with a result set
     */
    public void materialise(Session session) {

        PersistentStore store;

        // table constructors
        if (isDataExpression) {
            store = session.sessionData.getSubqueryRowStore(table);

            dataExpression.insertValuesIntoSubqueryTable(session, store);

            return;
        }

        Result result = queryExpression.getResult(session,
            isExistsPredicate ? 1
                              : 0);
        RowSetNavigatorData navigator =
            ((RowSetNavigatorData) result.getNavigator());

        if (uniqueRows) {
            navigator.removeDuplicates();
        }

        store = session.sessionData.getSubqueryRowStore(table);

        table.insertResult(store, result);
        result.getNavigator().close();
    }

    public boolean hasUniqueNotNullRows(Session session) {

        RowSetNavigatorData navigator = new RowSetNavigatorData(session,
            table);
        boolean result = navigator.hasUniqueNotNullRows();

        return result;
    }

    public Object[] getValues(Session session) {

        RowIterator it = table.rowIterator(session);

        if (it.hasNext()) {
            Row row = it.getNextRow();

            if (it.hasNext()) {
                throw Error.error(ErrorCode.X_21000);
            }

            return row.getData();
        } else {
            return new Object[table.getColumnCount()];
        }
    }

    public Object getValue(Session session) {

        Object[] data = getValues(session);

        return data[0];
    }

    /**
     * This results in the following sort order:
     *
     * view subqueries, then other subqueries
     *
     *    view subqueries:
     *        views sorted by creation order (earlier declaration first)
     *
     *    other subqueries:
     *        subqueries sorted by depth within select query (deep == higher level)
     *
     */
    @Override
    public int compare(Object a, Object b) {

        SubQuery sqa = (SubQuery) a;
        SubQuery sqb = (SubQuery) b;

        if (sqa.parentView == null && sqb.parentView == null) {
            return sqb.level - sqa.level;
        } else if (sqa.parentView != null && sqb.parentView != null) {
            int ia = database.schemaManager.getTableIndex(sqa.parentView);
            int ib = database.schemaManager.getTableIndex(sqb.parentView);

            if (ia == -1) {
                ia = database.schemaManager.getTables(
                    sqa.parentView.getSchemaName().name).size();
            }

            if (ib == -1) {
                ib = database.schemaManager.getTables(
                    sqb.parentView.getSchemaName().name).size();
            }

            int diff = ia - ib;

            return diff == 0 ? sqb.level - sqa.level
                             : diff;
        } else {
            return sqa.parentView == null ? 1
                                          : -1;
        }
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + level;
        result = prime * result + ((queryExpression == null) ? 0 : queryExpression.hashCode());
        result = prime * result + ((database == null) ? 0 : database.hashCode());
        result = prime * result + ((view == null) ? 0 : view.hashCode());
        result = prime * result + ((table == null) ? 0 : table.hashCode());
        result = prime * result + (isExistsPredicate ? 1231 : 1237);
        result = prime * result + (isUniquePredicate ? 1231 : 1237);
        result = prime * result + (isDataExpression ? 1231 : 1237);
        result = prime * result + (isCorrelated ? 1231 : 1237);
        result = prime * result + (uniqueRows ? 1231 : 1237);
        return result;
    }

    @Override
    public boolean equals(Object other) {
        if (other == this) {
            return true;
        }
        if (other == null) {
            return false;
        }

        if (other instanceof SubQuery) {
            return ((SubQuery) other).level == level
                   && ((SubQuery) other).queryExpression == queryExpression
                   && ((SubQuery) other).database == database
                   && ((SubQuery) other).view == view
                   && ((SubQuery) other).table == table
                   && ((SubQuery) other).isExistsPredicate == isExistsPredicate
                   && ((SubQuery) other).isUniquePredicate == isUniquePredicate
                   && ((SubQuery) other).isDataExpression == isDataExpression
                   && ((SubQuery) other).isCorrelated == isCorrelated
                   && ((SubQuery) other).uniqueRows == uniqueRows;
        }

        return false;
    }

}
