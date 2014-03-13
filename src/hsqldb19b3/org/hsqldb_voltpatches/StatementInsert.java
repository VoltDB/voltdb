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

import org.hsqldb_voltpatches.ParserDQL.CompileContext;
import org.hsqldb_voltpatches.RangeVariable.RangeIteratorBase;
import org.hsqldb_voltpatches.navigator.RowSetNavigator;
import org.hsqldb_voltpatches.navigator.RowSetNavigatorClient;
import org.hsqldb_voltpatches.persist.PersistentStore;
import org.hsqldb_voltpatches.result.Result;
import org.hsqldb_voltpatches.types.Type;

/**
 * Implementation of Statement for INSERT statements.<p>
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 1.9.0
 * @since 1.9.0
 */
public class StatementInsert extends StatementDML {

    /**
     * Instantiate this as an INSERT_VALUES statement.
     */
    StatementInsert(Session session, Table targetTable, int[] columnMap,
                    Expression insertExpression, boolean[] checkColumns,
                    CompileContext compileContext) {

        super(StatementTypes.INSERT, StatementTypes.X_SQL_DATA_CHANGE,
              session.currentSchema);

        this.targetTable            = targetTable;
        this.baseTable              = targetTable.getBaseTable();
        this.insertColumnMap        = columnMap;
        this.insertCheckColumns     = checkColumns;
        this.insertExpression       = insertExpression;
        this.isTransactionStatement = true;

        setDatabseObjects(compileContext);
        checkAccessRights(session);
    }

    /**
     * Instantiate this as an INSERT_SELECT statement.
     */
    StatementInsert(Session session, Table targetTable, int[] columnMap,
                    boolean[] checkColumns, QueryExpression queryExpression,
                    CompileContext compileContext) {

        super(StatementTypes.INSERT, StatementTypes.X_SQL_DATA_CHANGE,
              session.currentSchema);

        this.targetTable            = targetTable;
        this.baseTable              = targetTable.getBaseTable();
        this.insertColumnMap        = columnMap;
        this.insertCheckColumns     = checkColumns;
        this.queryExpression        = queryExpression;
        this.isTransactionStatement = true;

        setDatabseObjects(compileContext);
        checkAccessRights(session);
    }

    /**
     * Executes an INSERT_SELECT statement.  It is assumed that the argument
     * is of the correct type.
     *
     * @return the result of executing the statement
     */
    Result getResult(Session session) {

        Table           table              = baseTable;
        Result          resultOut          = null;
        RowSetNavigator generatedNavigator = null;
        PersistentStore store = session.sessionData.getRowStore(baseTable);

        if (generatedIndexes != null) {
            resultOut = Result.newUpdateCountResult(generatedResultMetaData,
                    0);
            generatedNavigator = resultOut.getChainedResult().getNavigator();
        }

        RowSetNavigator newDataNavigator = queryExpression == null
                                           ? getInsertValuesNavigator(session)
                                           : getInsertSelectNavigator(session);
        Expression        checkCondition = null;
        RangeIteratorBase checkIterator  = null;

        if (targetTable != baseTable) {
            QuerySpecification select =
                ((TableDerived) targetTable).getQueryExpression()
                    .getMainSelect();

            checkCondition = select.checkQueryCondition;

            if (checkCondition != null) {
                checkIterator = select.rangeVariables[0].getIterator(session);
            }
        }

        while (newDataNavigator.hasNext()) {
            Object[] data = newDataNavigator.getNext();

            if (checkCondition != null) {
                checkIterator.currentData = data;

                boolean check = checkCondition.testCondition(session);

                if (!check) {
                    throw Error.error(ErrorCode.X_44000);
                }
            }

            table.insertRow(session, store, data);

            if (generatedNavigator != null) {
                Object[] generatedValues = getGeneratedColumns(data);

                generatedNavigator.add(generatedValues);
            }
        }

        newDataNavigator.beforeFirst();
        table.fireAfterTriggers(session, Trigger.INSERT_AFTER,
                                newDataNavigator);

        if (resultOut == null) {
            resultOut =
                Result.getUpdateCountResult(newDataNavigator.getSize());
        } else {
            resultOut.setUpdateCount(newDataNavigator.getSize());
        }

        return resultOut;
    }

    RowSetNavigator getInsertSelectNavigator(Session session) {

        Type[] colTypes  = baseTable.getColumnTypes();
        int[]  columnMap = insertColumnMap;

        //
        Result                result = queryExpression.getResult(session, 0);
        RowSetNavigator       nav         = result.initialiseNavigator();
        Type[]                sourceTypes = result.metaData.columnTypes;
        RowSetNavigatorClient newData     = new RowSetNavigatorClient(2);

        while (nav.hasNext()) {
            Object[] data       = baseTable.getNewRowData(session);
            Object[] sourceData = (Object[]) nav.getNext();

            for (int i = 0; i < columnMap.length; i++) {
                int  j          = columnMap[i];
                Type sourceType = sourceTypes[i];

                data[j] = colTypes[j].convertToType(session, sourceData[i],
                                                    sourceType);
            }

            newData.add(data);
        }

        return newData;
    }

    RowSetNavigator getInsertValuesNavigator(Session session) {

        Type[] colTypes  = baseTable.getColumnTypes();
        int[]  columnMap = insertColumnMap;

        //
        Expression[]          list    = insertExpression.nodes;
        RowSetNavigatorClient newData = new RowSetNavigatorClient(list.length);

        for (int j = 0; j < list.length; j++) {
            Expression[] rowArgs = list[j].nodes;
            Object[]     data    = baseTable.getNewRowData(session);

            session.sessionData.startRowProcessing();

            for (int i = 0; i < rowArgs.length; i++) {
                Expression e        = rowArgs[i];
                int        colIndex = columnMap[i];

                if (e.getType() == OpTypes.DEFAULT) {
                    if (baseTable.identityColumn == colIndex) {
                        continue;
                    }

                    data[colIndex] =
                        baseTable.colDefaults[colIndex].getValue(session);

                    continue;
                }

                data[colIndex] = colTypes[colIndex].convertToType(session,
                        e.getValue(session), e.getDataType());
            }

            newData.add(data);
        }

        return newData;
    }
}
