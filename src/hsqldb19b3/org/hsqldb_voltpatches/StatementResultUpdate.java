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

import org.hsqldb_voltpatches.result.Result;
import org.hsqldb_voltpatches.types.Type;
import org.hsqldb_voltpatches.result.ResultConstants;
import org.hsqldb_voltpatches.persist.PersistentStore;
import org.hsqldb_voltpatches.lib.HashMappedList;
import org.hsqldb_voltpatches.lib.ArrayUtil;
import org.hsqldb_voltpatches.navigator.RowSetNavigator;
import org.hsqldb_voltpatches.navigator.RowSetNavigatorClient;
import org.hsqldb_voltpatches.navigator.RowSetNavigatorLinkedList;

public class StatementResultUpdate extends StatementDML {

    int    actionType;
    Type[] types;

    StatementResultUpdate() {

        super();

        isTransactionStatement = true;
    }

    public String describe(Session session) {
        return "";
    }

    public Result execute(Session session) {

        try {
            return getResult(session);
        } catch (HsqlException e) {
            return Result.newErrorResult(e);
        }
    }

    Result getResult(Session session) {

        checkAccessRights(session);

        Object[] args = session.sessionContext.dynamicArguments;

        switch (actionType) {

            case ResultConstants.UPDATE_CURSOR : {
                Long id = (Long) args[args.length - 1];
                PersistentStore store =
                    session.sessionData.getRowStore(baseTable);
                Row row = (Row) store.get((int) id.longValue(), false);
                HashMappedList list = new HashMappedList();
                Object[] data =
                    (Object[]) ArrayUtil.duplicateArray(row.getData());

                for (int i = 0; i < baseColumnMap.length; i++) {
                    if (types[i] == Type.SQL_ALL_TYPES) {
                        continue;
                    }

                    data[baseColumnMap[i]] = args[i];
                }

                list.add(row, data);
                update(session, baseTable, list);

                break;
            }
            case ResultConstants.DELETE_CURSOR : {
                Long id = (Long) args[args.length - 1];
                PersistentStore store =
                    session.sessionData.getRowStore(baseTable);
                Row row = (Row) store.get((int) id.longValue(), false);
                RowSetNavigator navigator = new RowSetNavigatorLinkedList();

                navigator.add(row);
                delete(session, baseTable, navigator);

                break;
            }
            case ResultConstants.INSERT_CURSOR : {
                Object[] data = baseTable.getNewRowData(session);

                for (int i = 0; i < data.length; i++) {
                    data[baseColumnMap[i]] = args[i];
                }

                PersistentStore store =
                    session.sessionData.getRowStore(baseTable);

                baseTable.insertRow(session, store, data);
            }
        }

        return Result.updateOneResult;
    }

    void setRowActionProperties(int action, Table table, Type[] types,
                                int[] columnMap) {

        this.actionType    = action;
        this.baseTable     = table;
        this.types         = types;
        this.baseColumnMap = columnMap;
    }

/*
    Result result = getAccessRightsResult(session);

    if (result != null) {
        return result;
    }

    if (this.isExplain) {
        return Result.newSingleColumnStringResult("OPERATION",
                describe(session));
    }

    try {
        materializeSubQueries(session, args);

        result = getResult(session);
    } catch (Throwable t) {
        String commandString = sql;

        if (session.database.getProperties().getErrorLevel()
                == HsqlDatabaseProperties.NO_MESSAGE) {
            commandString = null;
        }

        result = Result.newErrorResult(t, commandString);

        if (result.isError()) {
            result.getException().setStatementType(group, type);
        }
    }

    session.sessionContext.clearStructures(this);

    return result;
*/
/*
    long     id         = cmd.getResultId();
    int      actionType = cmd.getActionType();
    Result   result     = sessionData.getDataResult(id);
    Object[] pvals      = cmd.getParameterData();
    Type[]   types      = cmd.metaData.columnTypes;

    StatementQuery statement = (StatementQuery) result.getValueObject() ;
    QueryExpression qe = statement.queryExpression;

    Table baseTable = qe.getBaseTable();

    int[] columnMap = qe.getBaseTableColumnMap();


    switch (actionType) {

        case ResultConstants.UPDATE_CURSOR :
        case ResultConstants.DELETE_CURSOR :
        case ResultConstants.INSERT_CURSOR :
    }

    return Result.updateZeroResult;
*/
    void checkAccessRights(Session session) {

        switch (type) {

            case StatementTypes.CALL : {
                break;
            }
            case StatementTypes.INSERT : {
                session.getGrantee().checkInsert(targetTable,
                                                 insertCheckColumns);

                break;
            }
            case StatementTypes.SELECT_CURSOR :
                break;

            case StatementTypes.DELETE_WHERE : {
                session.getGrantee().checkDelete(targetTable);

                break;
            }
            case StatementTypes.UPDATE_WHERE : {
                session.getGrantee().checkUpdate(targetTable,
                                                 updateCheckColumns);

                break;
            }
            case StatementTypes.MERGE : {
                session.getGrantee().checkInsert(targetTable,
                                                 insertCheckColumns);
                session.getGrantee().checkUpdate(targetTable,
                                                 updateCheckColumns);

                break;
            }
        }
    }
}
