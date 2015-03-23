/* Copyright (c) 2001-2014, The HSQL Development Group
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
import org.hsqldb_voltpatches.error.Error;
import org.hsqldb_voltpatches.error.ErrorCode;
import org.hsqldb_voltpatches.lib.ArrayUtil;
import org.hsqldb_voltpatches.lib.HashSet;
import org.hsqldb_voltpatches.lib.OrderedHashSet;
import org.hsqldb_voltpatches.navigator.RangeIterator;
import org.hsqldb_voltpatches.navigator.RowIterator;
import org.hsqldb_voltpatches.navigator.RowSetNavigator;
import org.hsqldb_voltpatches.navigator.RowSetNavigatorClient;
import org.hsqldb_voltpatches.navigator.RowSetNavigatorDataChange;
import org.hsqldb_voltpatches.persist.PersistentStore;
import org.hsqldb_voltpatches.result.Result;
import org.hsqldb_voltpatches.result.ResultConstants;
import org.hsqldb_voltpatches.result.ResultMetaData;
import org.hsqldb_voltpatches.types.Type;
import org.hsqldb_voltpatches.types.Types;

/**
 * Implementation of Statement for DML statements.<p>
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 2.3.0
 * @since 1.9.0
 */

// support for MERGE statement originally contributed by Justin Spadea (jzs9783@users dot sourceforge.net)
public class StatementDML extends StatementDMQL {

    Expression[] targets;
    boolean      isTruncate;

    //
    boolean        isSimpleInsert;
    int            generatedType;
    ResultMetaData generatedInputMetaData;

    /** column indexes for generated values */
    int[] generatedIndexes;

    /** ResultMetaData for generated values */
    ResultMetaData generatedResultMetaData;

    public StatementDML(int type, int group, HsqlName schemaName) {
        super(type, group, schemaName);
    }

    /**
     * Instantiate this as a DELETE statement
     */
    StatementDML(Session session, Table targetTable,
                 RangeVariable targetRange, RangeVariable[] rangeVars,
                 CompileContext compileContext, boolean restartIdentity,
                 int type) {

        super(StatementTypes.DELETE_WHERE, StatementTypes.X_SQL_DATA_CHANGE,
              session.getCurrentSchemaHsqlName());

        this.targetTable = targetTable;
        this.baseTable   = targetTable.isTriggerDeletable() ? targetTable
                                                            : targetTable
                                                            .getBaseTable();
        this.targetRangeVariables = rangeVars;
        this.restartIdentity      = restartIdentity;

        setDatabseObjects(session, compileContext);
        checkAccessRights(session);

        if (type == StatementTypes.TRUNCATE) {
            isTruncate = true;
        }

        targetRange.addAllColumns();
    }

    /**
     * Instantiate this as an UPDATE statement.
     */
    StatementDML(Session session, Expression[] targets, Table targetTable,
                 RangeVariable targetRange, RangeVariable rangeVars[],
                 int[] updateColumnMap, Expression[] colExpressions,
                 boolean[] checkColumns, CompileContext compileContext) {

        super(StatementTypes.UPDATE_WHERE, StatementTypes.X_SQL_DATA_CHANGE,
              session.getCurrentSchemaHsqlName());

        this.targets     = targets;
        this.targetTable = targetTable;
        this.baseTable   = targetTable.isTriggerUpdatable() ? targetTable
                                                            : targetTable
                                                            .getBaseTable();
        this.updateColumnMap      = updateColumnMap;
        this.updateExpressions    = colExpressions;
        this.updateCheckColumns   = checkColumns;
        this.targetRangeVariables = rangeVars;

        setupChecks();
        setDatabseObjects(session, compileContext);
        checkAccessRights(session);
        targetRange.addAllColumns();
    }

    /**
     * Instantiate this as a MERGE statement.
     */
    StatementDML(Session session, Expression[] targets,
                 RangeVariable sourceRange, RangeVariable targetRange,
                 RangeVariable[] targetRangeVars, int[] insertColMap,
                 int[] updateColMap, boolean[] checkColumns,
                 Expression mergeCondition, Expression insertExpr,
                 Expression[] updateExpr, CompileContext compileContext) {

        super(StatementTypes.MERGE, StatementTypes.X_SQL_DATA_CHANGE,
              session.getCurrentSchemaHsqlName());

        this.targets     = targets;
        this.sourceTable = sourceRange.rangeTable;
        this.targetTable = targetRange.rangeTable;
        this.baseTable   = targetTable.isTriggerUpdatable() ? targetTable
                                                            : targetTable
                                                            .getBaseTable();
        this.insertCheckColumns   = checkColumns;
        this.insertColumnMap      = insertColMap;
        this.updateColumnMap      = updateColMap;
        this.insertExpression     = insertExpr;
        this.updateExpressions    = updateExpr;
        this.targetRangeVariables = targetRangeVars;
        this.condition            = mergeCondition;

        setupChecks();
        setDatabseObjects(session, compileContext);
        checkAccessRights(session);
    }

    /**
     * Instantiate this as a CURSOR operation statement.
     */
    StatementDML() {
        super(StatementTypes.UPDATE_CURSOR, StatementTypes.X_SQL_DATA_CHANGE,
              null);
    }

    void setupChecks() {

        if (targetTable != baseTable) {
            QuerySpecification select =
                ((TableDerived) targetTable).getQueryExpression()
                    .getMainSelect();

            this.updatableTableCheck = select.checkQueryCondition;
            this.checkRangeVariable =
                select.rangeVariables[select.rangeVariables.length - 1];
        }
    }

    Result getResult(Session session) {

        Result result = null;

        switch (type) {

            case StatementTypes.UPDATE_WHERE :
                result = executeUpdateStatement(session);
                break;

            case StatementTypes.MERGE :
                result = executeMergeStatement(session);
                break;

            case StatementTypes.DELETE_WHERE :
                if (isTruncate) {
                    result = executeDeleteTruncateStatement(session);
                } else {
                    result = executeDeleteStatement(session);
                }
                break;

            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "StatementDML");
        }

        session.sessionContext
            .diagnosticsVariables[ExpressionColumn.idx_row_count] =
                Integer.valueOf(result.getUpdateCount());

        return result;
    }

    // this fk references -> other  :  other read lock
    void collectTableNamesForRead(OrderedHashSet set) {

        if (baseTable.isView()) {
            getTriggerTableNames(set, false);
        } else if (!baseTable.isTemp()) {
            for (int i = 0; i < baseTable.fkConstraints.length; i++) {
                Constraint constraint = baseTable.fkConstraints[i];

                switch (type) {

                    case StatementTypes.UPDATE_WHERE : {
                        if (ArrayUtil.haveCommonElement(
                                constraint.getRefColumns(), updateColumnMap)) {
                            set.add(baseTable.fkConstraints[i].getMain()
                                .getName());
                        }

                        break;
                    }
                    case StatementTypes.INSERT : {
                        set.add(
                            baseTable.fkConstraints[i].getMain().getName());

                        break;
                    }
                    case StatementTypes.MERGE : {
                        if (updateColumnMap != null) {
                            if (ArrayUtil.haveCommonElement(
                                    constraint.getRefColumns(),
                                    updateColumnMap)) {
                                set.add(baseTable.fkConstraints[i].getMain()
                                    .getName());
                            }
                        }

                        if (insertExpression != null) {
                            set.add(baseTable.fkConstraints[i].getMain()
                                .getName());
                        }

                        break;
                    }
                }
            }

            if (type == StatementTypes.UPDATE_WHERE
                    || type == StatementTypes.MERGE) {
                baseTable.collectFKReadLocks(updateColumnMap, set);
            } else if (type == StatementTypes.DELETE_WHERE) {
                baseTable.collectFKReadLocks(null, set);
            }

            getTriggerTableNames(set, false);
        }

        for (int i = 0; i < rangeVariables.length; i++) {
            Table    rangeTable = rangeVariables[i].rangeTable;
            HsqlName name       = rangeTable.getName();

            if (rangeTable.isDataReadOnly() || rangeTable.isTemp()) {
                continue;
            }

            if (name.schema == SqlInvariants.SYSTEM_SCHEMA_HSQLNAME) {
                continue;
            }

            set.add(name);
        }

        for (int i = 0; i < subqueries.length; i++) {
            if (subqueries[i].queryExpression != null) {
                subqueries[i].queryExpression.getBaseTableNames(set);
            }
        }

        for (int i = 0; i < routines.length; i++) {
            set.addAll(routines[i].getTableNamesForRead());
        }
    }

    void collectTableNamesForWrite(OrderedHashSet set) {

        // other fk references this :  if constraint trigger action  : other write lock
        if (baseTable.isView()) {
            getTriggerTableNames(set, true);
        } else if (!baseTable.isTemp()) {
            set.add(baseTable.getName());

            if (type == StatementTypes.UPDATE_WHERE
                    || type == StatementTypes.MERGE) {
                if (updateExpressions.length != 0) {
                    baseTable.collectFKWriteLocks(updateColumnMap, set);
                }
            } else if (type == StatementTypes.DELETE_WHERE) {
                baseTable.collectFKWriteLocks(null, set);
            }

            getTriggerTableNames(set, true);
        }
    }

    /**
     * @todo - fredt - low priority - this does not work with different prepare calls
     * with the same SQL statement, but different generated column requests
     * To fix, add comment encapsulating the generated column list to SQL
     * to differentiate between the two invocations
     */
    public void setGeneratedColumnInfo(int generate, ResultMetaData meta) {

        // also supports INSERT_SELECT
        if (type != StatementTypes.INSERT) {
            return;
        }

        int idColIndex = baseTable.getIdentityColumnIndex();

        generatedType          = generate;
        generatedInputMetaData = meta;

        switch (generate) {

            case ResultConstants.RETURN_NO_GENERATED_KEYS :
                return;

            case ResultConstants.RETURN_GENERATED_KEYS_COL_INDEXES :
                generatedIndexes = meta.getGeneratedColumnIndexes();

                for (int i = 0; i < generatedIndexes.length; i++) {
                    if (generatedIndexes[i] < 0
                            || generatedIndexes[i]
                               >= baseTable.getColumnCount()) {
                        throw Error.error(ErrorCode.X_42501);
                    }
                }
                break;

            case ResultConstants.RETURN_GENERATED_KEYS :
                if (baseTable.hasGeneratedColumn()) {
                    if (idColIndex >= 0) {
                        int generatedCount =
                            ArrayUtil.countTrueElements(baseTable.colGenerated)
                            + 1;

                        generatedIndexes = new int[generatedCount];

                        for (int i = 0, j = 0;
                                i < baseTable.colGenerated.length; i++) {
                            if (baseTable.colGenerated[i] || i == idColIndex) {
                                generatedIndexes[j++] = i;
                            }
                        }
                    } else {
                        generatedIndexes = ArrayUtil.booleanArrayToIntIndexes(
                            baseTable.colGenerated);
                    }
                } else if (idColIndex >= 0) {
                    generatedIndexes = new int[]{ idColIndex };
                } else {
                    return;
                }
                break;

            case ResultConstants.RETURN_GENERATED_KEYS_COL_NAMES :
                String[] columnNames = meta.getGeneratedColumnNames();

                generatedIndexes = baseTable.getColumnIndexes(columnNames);

                for (int i = 0; i < generatedIndexes.length; i++) {
                    if (generatedIndexes[i] < 0) {
                        throw Error.error(ErrorCode.X_42501, columnNames[0]);
                    }
                }
                break;
        }

        generatedResultMetaData =
            ResultMetaData.newResultMetaData(generatedIndexes.length);

        for (int i = 0; i < generatedIndexes.length; i++) {
            ColumnSchema column = baseTable.getColumn(generatedIndexes[i]);

            generatedResultMetaData.columns[i] = column;
        }

        generatedResultMetaData.prepareData();

        isSimpleInsert = false;
    }

    Object[] getGeneratedColumns(Object[] data) {

        if (generatedIndexes == null) {
            return null;
        }

        Object[] values = new Object[generatedIndexes.length];

        for (int i = 0; i < generatedIndexes.length; i++) {
            values[i] = data[generatedIndexes[i]];
        }

        return values;
    }

    public boolean hasGeneratedColumns() {
        return generatedIndexes != null;
    }

    public ResultMetaData generatedResultMetaData() {
        return generatedResultMetaData;
    }

    void getTriggerTableNames(OrderedHashSet set, boolean write) {

        for (int i = 0; i < baseTable.triggerList.length; i++) {
            TriggerDef td = baseTable.triggerList[i];

            switch (type) {

                case StatementTypes.INSERT :
                    if (td.getStatementType() == StatementTypes.INSERT) {
                        break;
                    }

                    continue;
                case StatementTypes.UPDATE_WHERE :
                    if (td.getStatementType() == StatementTypes.UPDATE_WHERE) {
                        break;
                    }

                    continue;
                case StatementTypes.DELETE_WHERE :
                    if (td.getStatementType() == StatementTypes.DELETE_WHERE) {
                        break;
                    }

                    continue;
                case StatementTypes.MERGE :
                    if (td.getStatementType() == StatementTypes.INSERT
                            || td.getStatementType()
                               == StatementTypes.UPDATE_WHERE) {
                        break;
                    }

                    continue;
                default :
                    throw Error.runtimeError(ErrorCode.U_S0500,
                                             "StatementDML");
            }

            if (td.routine != null) {
                if (write) {
                    set.addAll(td.routine.getTableNamesForWrite());
                } else {
                    set.addAll(td.routine.getTableNamesForRead());
                }
            }
        }
    }

    /**
     * Executes an UPDATE statement.
     *
     * @return Result object
     */
    Result executeUpdateStatement(Session session) {

        int          count          = 0;
        Expression[] colExpressions = updateExpressions;
        RowSetNavigatorDataChange rowset =
            session.sessionContext.getRowSetDataChange();
        Type[] colTypes = baseTable.getColumnTypes();
        RangeIterator it = RangeVariable.getIterator(session,
            targetRangeVariables);
        Result          resultOut          = null;
        RowSetNavigator generatedNavigator = null;

        if (generatedIndexes != null) {
            resultOut = Result.newUpdateCountResult(generatedResultMetaData,
                    0);
            generatedNavigator = resultOut.getChainedResult().getNavigator();
        }

        session.sessionContext.rownum = 1;

        while (it.next()) {
            session.sessionData.startRowProcessing();

            Row      row  = it.getCurrentRow();
            Object[] data = row.getData();
            Object[] newData = getUpdatedData(session, targets, baseTable,
                                              updateColumnMap, colExpressions,
                                              colTypes, data);

            if (updatableTableCheck != null) {
                it.setCurrent(newData);

                boolean check = updatableTableCheck.testCondition(session);

                if (!check) {
                    it.release();

                    throw Error.error(ErrorCode.X_44000);
                }
            }

            rowset.addRow(session, row, newData, colTypes, updateColumnMap);

            session.sessionContext.rownum++;
        }

        rowset.endMainDataSet();
        it.release();
/* debug 190
        if (rowset.size() == 0) {
            System.out.println(targetTable.getName().name + " zero update: session "
                               + session.getId());
        } else if (rowset.size() >1) {
           System.out.println("multiple update: session "
                              + session.getId() + ", " + rowset.size());
       }

//* debug 190 */
        rowset.beforeFirst();

        count = update(session, baseTable, rowset, generatedNavigator);

        if (resultOut == null) {
            if (count == 1) {
                return Result.updateOneResult;
            } else if (count == 0) {
                session.addWarning(HsqlException.noDataCondition);

                return Result.updateZeroResult;
            }

            return new Result(ResultConstants.UPDATECOUNT, count);
        } else {
            resultOut.setUpdateCount(count);

            if (count == 0) {
                session.addWarning(HsqlException.noDataCondition);
            }

            return resultOut;
        }
    }

    static Object[] getUpdatedData(Session session, Expression[] targets,
                                   Table targetTable, int[] columnMap,
                                   Expression[] colExpressions,
                                   Type[] colTypes, Object[] oldData) {

        Object[] data = targetTable.getEmptyRowData();

        System.arraycopy(oldData, 0, data, 0, data.length);

        for (int i = 0, ix = 0; i < columnMap.length; ) {
            Expression expr = colExpressions[ix++];

            if (expr.getType() == OpTypes.ROW) {
                Object[] values = expr.getRowValue(session);

                for (int j = 0; j < values.length; j++, i++) {
                    int        colIndex = columnMap[i];
                    Expression e        = expr.nodes[j];

                    // transitional - still supporting null for identity generation
                    if (targetTable.identityColumn == colIndex) {
                        if (e.getType() == OpTypes.VALUE
                                && e.valueData == null) {
                            continue;
                        }
                    }

                    if (e.getType() == OpTypes.DEFAULT) {
                        if (targetTable.identityColumn == colIndex) {
                            continue;
                        }

                        if (targetTable.colDefaults[colIndex] == null) {
                            data[colIndex] = null;
                        } else {
                            data[colIndex] =
                                targetTable.colDefaults[colIndex].getValue(
                                    session);
                        }

                        continue;
                    }

                    data[colIndex] = colTypes[colIndex].convertToType(session,
                            values[j], e.dataType);
                }
            } else if (expr.getType() == OpTypes.ROW_SUBQUERY) {
                Object[] values = expr.getRowValue(session);

                for (int j = 0; j < values.length; j++, i++) {
                    int colIndex = columnMap[i];
                    Type colType =
                        expr.table.queryExpression.getMetaData()
                            .columnTypes[j];

                    data[colIndex] = colTypes[colIndex].convertToType(session,
                            values[j], colType);
                }
            } else {
                int colIndex = columnMap[i];

                if (expr.getType() == OpTypes.DEFAULT) {
                    if (targetTable.identityColumn == colIndex) {
                        i++;

                        continue;
                    }

                    if (targetTable.colDefaults[colIndex] == null) {
                        data[colIndex] = null;
                    } else {
                        data[colIndex] =
                            targetTable.colDefaults[colIndex].getValue(
                                session);
                    }

                    i++;

                    continue;
                }

                Object value = expr.getValue(session);

                if (targets[i].getType() == OpTypes.ARRAY_ACCESS) {
                    data[colIndex] =
                        ((ExpressionAccessor) targets[i]).getUpdatedArray(
                            session, (Object[]) data[colIndex], value, true);
                } else {
                    data[colIndex] = colTypes[colIndex].convertToType(session,
                            value, expr.dataType);
                }

                i++;
            }
        }

        return data;
    }

    /**
     * Executes a MERGE statement.
     *
     * @return Result object
     */
    Result executeMergeStatement(Session session) {

        Type[]          colTypes           = baseTable.getColumnTypes();
        Result          resultOut          = null;
        RowSetNavigator generatedNavigator = null;

        if (generatedIndexes != null) {
            resultOut = Result.newUpdateCountResult(generatedResultMetaData,
                    0);
            generatedNavigator = resultOut.getChainedResult().getNavigator();
        }

        int count = 0;

        // data generated for non-matching rows
        RowSetNavigatorClient newData = new RowSetNavigatorClient(8);

        // rowset for update operation
        RowSetNavigatorDataChange updateRowSet =
            session.sessionContext.getRowSetDataChange();
        RangeVariable[] joinRangeIterators = targetRangeVariables;

        // populate insert and update lists
        RangeIterator[] rangeIterators =
            new RangeIterator[joinRangeIterators.length];

        for (int i = 0; i < joinRangeIterators.length; i++) {
            rangeIterators[i] = joinRangeIterators[i].getIterator(session);
        }

        for (int currentIndex = 0; currentIndex >= 0; ) {
            RangeIterator it          = rangeIterators[currentIndex];
            boolean       beforeFirst = it.isBeforeFirst();

            if (it.next()) {
                if (currentIndex < joinRangeIterators.length - 1) {
                    currentIndex++;

                    continue;
                }
            } else {
                if (currentIndex == 1 && beforeFirst
                        && insertExpression != null) {
                    Object[] data =
                        getInsertData(session, colTypes,
                                      insertExpression.nodes[0].nodes);

                    if (data != null) {
                        newData.add(data);
                    }
                }

                it.reset();

                currentIndex--;

                continue;
            }

            // row matches!
            if (updateExpressions.length != 0) {
                Row row = it.getCurrentRow();    // this is always the second iterator

                session.sessionData.startRowProcessing();

                Object[] data = getUpdatedData(session, targets, baseTable,
                                               updateColumnMap,
                                               updateExpressions, colTypes,
                                               row.getData());

                try {
                    updateRowSet.addRow(session, row, data, colTypes,
                                        updateColumnMap);
                } catch (HsqlException e) {
                    for (int i = 0; i < joinRangeIterators.length; i++) {
                        rangeIterators[i].reset();
                    }

                    throw Error.error(ErrorCode.X_21000);
                }
            }
        }

        updateRowSet.endMainDataSet();

        for (int i = 0; i < joinRangeIterators.length; i++) {
            rangeIterators[i].reset();
        }

        // run the transaction as a whole, updating and inserting where needed
        // update any matched rows
        if (updateExpressions.length != 0) {
            count = update(session, baseTable, updateRowSet,
                           generatedNavigator);
        }

        // insert any non-matched rows
        if (newData.getSize() > 0) {
            insertRowSet(session, generatedNavigator, newData);

            count += newData.getSize();
        }

        if (insertExpression != null
                && baseTable.triggerLists[Trigger.INSERT_AFTER].length > 0) {
            baseTable.fireTriggers(session, Trigger.INSERT_AFTER, newData);
        }

        if (resultOut == null) {
            if (count == 1) {
                return Result.updateOneResult;
            }

            if (count == 0) {
                session.addWarning(HsqlException.noDataCondition);

                return Result.updateZeroResult;
            }

            return new Result(ResultConstants.UPDATECOUNT, count);
        } else {
            resultOut.setUpdateCount(count);

            if (count == 0) {
                session.addWarning(HsqlException.noDataCondition);
            }

            return resultOut;
        }
    }

    void insertRowSet(Session session, RowSetNavigator generatedNavigator,
                      RowSetNavigator newData) {

        PersistentStore store         = baseTable.getRowStore(session);
        RangeIterator   checkIterator = null;

        if (updatableTableCheck != null) {
            checkIterator = checkRangeVariable.getIterator(session);
        }

        newData.beforeFirst();

        if (baseTable.triggerLists[Trigger.INSERT_BEFORE_ROW].length > 0) {
            while (newData.hasNext()) {
                Object[] data = (Object[]) newData.getNext();

                baseTable.fireTriggers(session, Trigger.INSERT_BEFORE_ROW,
                                       null, data, null);
            }

            newData.beforeFirst();
        }

        while (newData.hasNext()) {
            Object[] data = (Object[]) newData.getNext();

            // for identity using global sequence
            session.sessionData.startRowProcessing();
            baseTable.insertSingleRow(session, store, data, null);

            if (checkIterator != null) {
                checkIterator.setCurrent(data);

                boolean check = updatableTableCheck.testCondition(session);

                if (!check) {
                    throw Error.error(ErrorCode.X_44000);
                }
            }

            if (generatedNavigator != null) {
                Object[] generatedValues = getGeneratedColumns(data);

                generatedNavigator.add(generatedValues);
            }
        }

        newData.beforeFirst();

        while (newData.hasNext()) {
            Object[] data = (Object[]) newData.getNext();

            performIntegrityChecks(session, baseTable, null, data, null);
        }

        newData.beforeFirst();

        if (baseTable.triggerLists[Trigger.INSERT_AFTER_ROW].length > 0) {
            while (newData.hasNext()) {
                Object[] data = (Object[]) newData.getNext();

                baseTable.fireTriggers(session, Trigger.INSERT_AFTER_ROW,
                                       null, data, null);
            }

            newData.beforeFirst();
        }
    }

    Result insertSingleRow(Session session, PersistentStore store,
                           Object[] data) {

        if (baseTable.triggerLists[Trigger.INSERT_BEFORE_ROW].length > 0) {
            baseTable.fireTriggers(session, Trigger.INSERT_BEFORE_ROW, null,
                                   data, null);
        }

        baseTable.insertSingleRow(session, store, data, null);
        performIntegrityChecks(session, baseTable, null, data, null);

        if (session.database.isReferentialIntegrity()) {
            for (int i = 0, size = baseTable.fkConstraints.length; i < size;
                    i++) {
                baseTable.fkConstraints[i].checkInsert(session, baseTable,
                                                       data, true);
            }
        }

        if (baseTable.triggerLists[Trigger.INSERT_AFTER_ROW].length > 0) {
            baseTable.fireTriggers(session, Trigger.INSERT_AFTER_ROW, null,
                                   data, null);
        }

        if (baseTable.triggerLists[Trigger.INSERT_AFTER].length > 0) {
            baseTable.fireTriggers(session, Trigger.INSERT_AFTER,
                                   (RowSetNavigator) null);
        }

        return Result.updateOneResult;
    }

    Object[] getInsertData(Session session, Type[] colTypes,
                           Expression[] rowArgs) {

        Object[] data = baseTable.getNewRowData(session);

        session.sessionData.startRowProcessing();

        for (int i = 0; i < rowArgs.length; i++) {
            Expression e        = rowArgs[i];
            int        colIndex = insertColumnMap[i];

            if (e.opType == OpTypes.DEFAULT) {
                if (baseTable.identityColumn == colIndex) {
                    continue;
                }

                if (baseTable.colDefaults[colIndex] != null) {
                    data[colIndex] =
                        baseTable.colDefaults[colIndex].getValue(session);
                }

                continue;
            }

            Object value = e.getValue(session);
            Type   type  = colTypes[colIndex];

            if (session.database.sqlSyntaxMys
                    || session.database.sqlSyntaxPgs) {
                try {
                    value = type.convertToType(session, value, e.dataType);
                } catch (HsqlException ex) {
                    if (type.typeCode == Types.SQL_DATE) {
                        value = Type.SQL_TIMESTAMP.convertToType(session,
                                value, e.dataType);
                        value = type.convertToType(session, value,
                                                   Type.SQL_TIMESTAMP);
                    } else if (type.typeCode == Types.SQL_TIMESTAMP) {
                        value = Type.SQL_DATE.convertToType(session, value,
                                                            e.dataType);
                        value = type.convertToType(session, value,
                                                   Type.SQL_DATE);
                    } else {
                        throw ex;
                    }
                }
            } else {

                // DYNAMIC_PARAM and PARAMETER expressions may have wider values
                if (e.dataType == null
                        || type.typeDataGroup != e.dataType.typeDataGroup
                        || type.isArrayType()) {
                    value = type.convertToType(session, value, e.dataType);
                }
            }

            data[colIndex] = value;
        }

        return data;
    }

    /**
     * Highest level multiple row update method.<p>
     *
     * Following clauses from SQL Standard section 11.8 are enforced 9) Let ISS
     * be the innermost SQL-statement being executed. 10) If evaluation of these
     * General Rules during the execution of ISS would cause an update of some
     * site to a value that is distinct from the value to which that site was
     * previously updated during the execution of ISS, then an exception
     * condition is raised: triggered data change violation. 11) If evaluation
     * of these General Rules during the execution of ISS would cause deletion
     * of a row containing a site that is identified for replacement in that
     * row, then an exception condition is raised: triggered data change
     * violation.
     *
     * @param session Session
     * @param table Table
     * @return int
     */
    int update(Session session, Table table,
               RowSetNavigatorDataChange navigator,
               RowSetNavigator generatedNavigator) {

        int rowCount = navigator.getSize();

        // set identity column where null and check columns
        for (int i = 0; i < rowCount; i++) {
            navigator.next();

            Object[] data = navigator.getCurrentChangedData();

            // for identity using global sequence
            session.sessionData.startRowProcessing();

            /**
             * @todo 1.9.0 - make optional using database property -
             * this means the identity column can be set to null to force
             * creation of a new identity value
             */
            table.setIdentityColumn(session, data);
            table.setGeneratedColumns(session, data);
        }

        navigator.beforeFirst();

        if (table.fkMainConstraints.length > 0) {
            HashSet path = session.sessionContext.getConstraintPath();

            for (int i = 0; i < rowCount; i++) {
                navigator.next();

                Row      row  = navigator.getCurrentRow();
                Object[] data = navigator.getCurrentChangedData();

                performReferentialActions(session, table, navigator, row,
                                          data, this.updateColumnMap, path);
                path.clear();
            }

            navigator.beforeFirst();
        }

        while (navigator.next()) {
            Row      row            = navigator.getCurrentRow();
            Object[] data           = navigator.getCurrentChangedData();
            int[]    changedColumns = navigator.getCurrentChangedColumns();
            Table    currentTable   = ((Table) row.getTable());

            if (currentTable instanceof TableDerived) {
                currentTable = ((TableDerived) currentTable).view;
            }

            if (currentTable.triggerLists[Trigger.UPDATE_BEFORE_ROW].length
                    > 0) {
                currentTable.fireTriggers(session, Trigger.UPDATE_BEFORE_ROW,
                                          row.getData(), data, changedColumns);
                currentTable.enforceRowConstraints(session, data);
            }
        }

        if (table.isView) {
            return rowCount;
        }

        navigator.beforeFirst();

        while (navigator.next()) {
            Row             row          = navigator.getCurrentRow();
            Table           currentTable = ((Table) row.getTable());
            int[] changedColumns = navigator.getCurrentChangedColumns();
            PersistentStore store        = currentTable.getRowStore(session);

            session.addDeleteAction(currentTable, store, row, changedColumns);
        }

        navigator.beforeFirst();

        while (navigator.next()) {
            Row             row          = navigator.getCurrentRow();
            Object[]        data         = navigator.getCurrentChangedData();
            Table           currentTable = ((Table) row.getTable());
            int[] changedColumns = navigator.getCurrentChangedColumns();
            PersistentStore store        = currentTable.getRowStore(session);

            if (data == null) {
                continue;
            }

            Row newRow = currentTable.insertSingleRow(session, store, data,
                changedColumns);

            if (generatedNavigator != null) {
                Object[] generatedValues = getGeneratedColumns(data);

                generatedNavigator.add(generatedValues);
            }

//            newRow.rowAction.updatedAction = row.rowAction;
        }

        navigator.beforeFirst();

        OrderedHashSet extraUpdateTables = null;
        boolean hasAfterRowTriggers =
            table.triggerLists[Trigger.UPDATE_AFTER_ROW].length > 0;

        while (navigator.next()) {
            Row      row            = navigator.getCurrentRow();
            Table    currentTable   = ((Table) row.getTable());
            Object[] changedData    = navigator.getCurrentChangedData();
            int[]    changedColumns = navigator.getCurrentChangedColumns();

            performIntegrityChecks(session, currentTable, row.getData(),
                                   changedData, changedColumns);

            if (currentTable != table) {
                if (extraUpdateTables == null) {
                    extraUpdateTables = new OrderedHashSet();
                }

                extraUpdateTables.add(currentTable);

                if (currentTable.triggerLists[Trigger.UPDATE_AFTER_ROW].length
                        > 0) {
                    hasAfterRowTriggers = true;
                }
            }
        }

        navigator.beforeFirst();

        if (hasAfterRowTriggers) {
            while (navigator.next()) {
                Row      row            = navigator.getCurrentRow();
                Object[] changedData    = navigator.getCurrentChangedData();
                int[]    changedColumns = navigator.getCurrentChangedColumns();
                Table    currentTable   = ((Table) row.getTable());

                currentTable.fireTriggers(session, Trigger.UPDATE_AFTER_ROW,
                                          row.getData(), changedData,
                                          changedColumns);
            }

            navigator.beforeFirst();
        }

        baseTable.fireTriggers(session, Trigger.UPDATE_AFTER, navigator);

        if (extraUpdateTables != null) {
            for (int i = 0; i < extraUpdateTables.size(); i++) {
                Table currentTable = (Table) extraUpdateTables.get(i);

                currentTable.fireTriggers(session, Trigger.UPDATE_AFTER,
                                          navigator);
            }
        }

        return rowCount;
    }

    /**
     * Executes a DELETE statement.
     *
     * @return the result of executing the statement
     */
    Result executeDeleteStatement(Session session) {

        int count = 0;
        RangeIterator it = RangeVariable.getIterator(session,
            targetRangeVariables);
        RowSetNavigatorDataChange rowset =
            session.sessionContext.getRowSetDataChange();

        session.sessionContext.rownum = 1;

        while (it.next()) {
            Row currentRow = it.getCurrentRow();

            rowset.addRow(currentRow);

            session.sessionContext.rownum++;
        }

        it.release();
        rowset.endMainDataSet();

        if (rowset.getSize() > 0) {
            count = delete(session, baseTable, rowset);
        } else {
            session.addWarning(HsqlException.noDataCondition);

            return Result.updateZeroResult;
        }

        if (count == 1) {
            return Result.updateOneResult;
        }

        return new Result(ResultConstants.UPDATECOUNT, count);
    }

    Result executeDeleteTruncateStatement(Session session) {

        PersistentStore store   = targetTable.getRowStore(session);
        RowIterator     it = targetTable.getPrimaryIndex().firstRow(store);
        boolean         hasData = it.hasNext();

        for (int i = 0; i < targetTable.fkMainConstraints.length; i++) {
            if (targetTable.fkMainConstraints[i].getRef() != targetTable) {
                HsqlName tableName =
                    targetTable.fkMainConstraints[i].getRef().getName();
                Table refTable =
                    session.database.schemaManager.getUserTable(session,
                        tableName);

                if (!refTable.isEmpty(session)) {
                    throw Error.error(ErrorCode.X_23504,
                                      refTable.getName().name);
                }
            }
        }

        try {
            while (it.hasNext()) {
                Row row = it.getNextRow();

                session.addDeleteAction((Table) row.getTable(), store, row,
                                        null);
            }

            if (restartIdentity && targetTable.identitySequence != null) {
                targetTable.identitySequence.reset();
            }
        } finally {
            it.release();
        }

        if (!hasData) {
            session.addWarning(HsqlException.noDataCondition);
        }

        return Result.updateOneResult;
    }

    /**
     *  Highest level multiple row delete method. Corresponds to an SQL
     *  DELETE.
     */
    int delete(Session session, Table table,
               RowSetNavigatorDataChange navigator) {

        int rowCount = navigator.getSize();

        navigator.beforeFirst();

        if (table.fkMainConstraints.length > 0) {
            HashSet path = session.sessionContext.getConstraintPath();

            for (int i = 0; i < rowCount; i++) {
                navigator.next();

                Row row = navigator.getCurrentRow();

                performReferentialActions(session, table, navigator, row,
                                          null, null, path);
                path.clear();
            }

            navigator.beforeFirst();
        }

        while (navigator.next()) {
            Row      row            = navigator.getCurrentRow();
            Object[] changedData    = navigator.getCurrentChangedData();
            int[]    changedColumns = navigator.getCurrentChangedColumns();
            Table    currentTable   = ((Table) row.getTable());

            if (currentTable instanceof TableDerived) {
                currentTable = ((TableDerived) currentTable).view;
            }

            if (changedData == null) {
                currentTable.fireTriggers(session, Trigger.DELETE_BEFORE_ROW,
                                          row.getData(), null, null);
            } else {
                currentTable.fireTriggers(session, Trigger.UPDATE_BEFORE_ROW,
                                          row.getData(), changedData,
                                          changedColumns);
            }
        }

        if (table.isView) {
            return rowCount;
        }

        navigator.beforeFirst();

        boolean hasUpdate = false;

        while (navigator.next()) {
            Row             row          = navigator.getCurrentRow();
            Object[]        data         = navigator.getCurrentChangedData();
            Table           currentTable = ((Table) row.getTable());
            PersistentStore store        = currentTable.getRowStore(session);

            session.addDeleteAction(currentTable, store, row, null);

            if (data != null) {
                hasUpdate = true;
            }
        }

        navigator.beforeFirst();

        if (hasUpdate) {
            while (navigator.next()) {
                Row             row          = navigator.getCurrentRow();
                Object[]        data = navigator.getCurrentChangedData();
                Table           currentTable = ((Table) row.getTable());
                int[] changedColumns = navigator.getCurrentChangedColumns();
                PersistentStore store = currentTable.getRowStore(session);

                if (data == null) {
                    continue;
                }

                Row newRow = currentTable.insertSingleRow(session, store,
                    data, changedColumns);

//                newRow.rowAction.updatedAction = row.rowAction;
            }

            navigator.beforeFirst();
        }

        OrderedHashSet extraUpdateTables = null;
        OrderedHashSet extraDeleteTables = null;
        boolean hasAfterRowTriggers =
            table.triggerLists[Trigger.DELETE_AFTER_ROW].length > 0;

        if (rowCount != navigator.getSize()) {
            while (navigator.next()) {
                Row      row            = navigator.getCurrentRow();
                Object[] changedData    = navigator.getCurrentChangedData();
                int[]    changedColumns = navigator.getCurrentChangedColumns();
                Table    currentTable   = ((Table) row.getTable());

                if (changedData != null) {
                    performIntegrityChecks(session, currentTable,
                                           row.getData(), changedData,
                                           changedColumns);
                }

                if (currentTable != table) {
                    if (changedData == null) {
                        if (currentTable.triggerLists[Trigger.DELETE_AFTER_ROW]
                                .length > 0) {
                            hasAfterRowTriggers = true;
                        }

                        if (extraDeleteTables == null) {
                            extraDeleteTables = new OrderedHashSet();
                        }

                        extraDeleteTables.add(currentTable);
                    } else {
                        if (currentTable.triggerLists[Trigger.UPDATE_AFTER_ROW]
                                .length > 0) {
                            hasAfterRowTriggers = true;
                        }

                        if (extraUpdateTables == null) {
                            extraUpdateTables = new OrderedHashSet();
                        }

                        extraUpdateTables.add(currentTable);
                    }
                }
            }

            navigator.beforeFirst();
        }

        if (hasAfterRowTriggers) {
            while (navigator.next()) {
                Row      row          = navigator.getCurrentRow();
                Object[] changedData  = navigator.getCurrentChangedData();
                Table    currentTable = ((Table) row.getTable());

                if (changedData == null) {
                    currentTable.fireTriggers(session,
                                              Trigger.DELETE_AFTER_ROW,
                                              row.getData(), null, null);
                } else {
                    currentTable.fireTriggers(session,
                                              Trigger.UPDATE_AFTER_ROW,
                                              row.getData(), changedData,
                                              null);
                }
            }

            navigator.beforeFirst();
        }

        table.fireTriggers(session, Trigger.DELETE_AFTER, navigator);

        if (extraUpdateTables != null) {
            for (int i = 0; i < extraUpdateTables.size(); i++) {
                Table currentTable = (Table) extraUpdateTables.get(i);

                currentTable.fireTriggers(session, Trigger.UPDATE_AFTER,
                                          navigator);
            }
        }

        if (extraDeleteTables != null) {
            for (int i = 0; i < extraDeleteTables.size(); i++) {
                Table currentTable = (Table) extraDeleteTables.get(i);

                currentTable.fireTriggers(session, Trigger.DELETE_AFTER,
                                          navigator);
            }
        }

        return rowCount;
    }

    static void performIntegrityChecks(Session session, Table table,
                                       Object[] oldData, Object[] newData,
                                       int[] updatedColumns) {

        if (newData == null) {
            return;
        }

        for (int i = 0, size = table.checkConstraints.length; i < size; i++) {
            table.checkConstraints[i].checkInsert(session, table, newData,
                                                  oldData == null);
        }

        if (!session.database.isReferentialIntegrity()) {
            return;
        }

        for (int i = 0, size = table.fkConstraints.length; i < size; i++) {
            boolean    check = oldData == null;
            Constraint c     = table.fkConstraints[i];

            if (!check) {
                check = ArrayUtil.haveCommonElement(c.getRefColumns(),
                                                    updatedColumns);
            }

            if (check) {
                c.checkInsert(session, table, newData, oldData == null);
            }
        }
    }

    static void performReferentialActions(Session session, Table table,
                                          RowSetNavigatorDataChange navigator,
                                          Row row, Object[] data,
                                          int[] changedCols, HashSet path) {

        if (!session.database.isReferentialIntegrity()) {
            return;
        }

        boolean delete = data == null;

        for (int i = 0, size = table.fkMainConstraints.length; i < size; i++) {
            Constraint c      = table.fkMainConstraints[i];
            int        action = delete ? c.core.deleteAction
                                       : c.core.updateAction;

            if (!delete) {
                if (!ArrayUtil.haveCommonElement(changedCols,
                                                 c.core.mainCols)) {
                    continue;
                }

                if (c.core.mainIndex.compareRowNonUnique(
                        session, row.getData(), data, c.core.mainCols) == 0) {
                    continue;
                }
            }

            RowIterator refiterator = c.findFkRef(session, row.getData());

            if (!refiterator.hasNext()) {
                refiterator.release();

                continue;
            }

            while (refiterator.hasNext()) {
                Row      refRow  = refiterator.getNextRow();
                Object[] refData = null;

                /** @todo use MATCH */
                if (c.core.refIndex.compareRowNonUnique(
                        session, refRow.getData(), row.getData(),
                        c.core.mainCols) != 0) {
                    break;
                }

                if (delete && refRow.getId() == row.getId()) {
                    continue;
                }

                switch (action) {

                    case SchemaObject.ReferentialAction.CASCADE : {
                        if (delete) {
                            boolean result;

                            try {
                                result = navigator.addRow(refRow);
                            } catch (HsqlException e) {
                                String[] info = getConstraintInfo(c);

                                refiterator.release();

                                throw Error.error(null, ErrorCode.X_27000,
                                                  ErrorCode.CONSTRAINT, info);
                            }

                            if (result) {
                                performReferentialActions(session,
                                                          c.core.refTable,
                                                          navigator, refRow,
                                                          null, null, path);
                            }

                            continue;
                        }

                        refData = c.core.refTable.getEmptyRowData();

                        System.arraycopy(refRow.getData(), 0, refData, 0,
                                         refData.length);

                        for (int j = 0; j < c.core.refCols.length; j++) {
                            refData[c.core.refCols[j]] =
                                data[c.core.mainCols[j]];
                        }

                        break;
                    }
                    case SchemaObject.ReferentialAction.SET_NULL : {
                        refData = c.core.refTable.getEmptyRowData();

                        System.arraycopy(refRow.getData(), 0, refData, 0,
                                         refData.length);

                        for (int j = 0; j < c.core.refCols.length; j++) {
                            refData[c.core.refCols[j]] = null;
                        }

                        break;
                    }
                    case SchemaObject.ReferentialAction.SET_DEFAULT : {
                        refData = c.core.refTable.getEmptyRowData();

                        System.arraycopy(refRow.getData(), 0, refData, 0,
                                         refData.length);

                        for (int j = 0; j < c.core.refCols.length; j++) {
                            ColumnSchema col =
                                c.core.refTable.getColumn(c.core.refCols[j]);

                            refData[c.core.refCols[j]] =
                                col.getDefaultValue(session);
                        }

                        break;
                    }
                    case SchemaObject.ReferentialAction.NO_ACTION :
                        if (navigator.containsDeletedRow(refRow)) {
                            continue;
                        }

                    // fall through
                    case SchemaObject.ReferentialAction.RESTRICT : {
                        int errorCode = c.core.deleteAction
                                        == SchemaObject.ReferentialAction
                                            .NO_ACTION ? ErrorCode.X_23504
                                                       : ErrorCode.X_23001;
                        String[] info = getConstraintInfo(c);

                        refiterator.release();

                        throw Error.error(null, errorCode,
                                          ErrorCode.CONSTRAINT, info);
                    }
                    default :
                        continue;
                }

                try {
                    refData =
                        navigator.addRow(session, refRow, refData,
                                         c.core.refTable.getColumnTypes(),
                                         c.core.refCols);
                } catch (HsqlException e) {
                    String[] info = getConstraintInfo(c);

                    refiterator.release();

                    throw Error.error(null, ErrorCode.X_27000,
                                      ErrorCode.CONSTRAINT, info);
                }

                if (refData == null) {

                    // happens only with enforceDeleteOrUpdate=false and updated row is already deleted
                    continue;
                }

                if (!path.add(c)) {
                    continue;
                }

                performReferentialActions(session, c.core.refTable, navigator,
                                          refRow, refData, c.core.refCols,
                                          path);
                path.remove(c);
            }

            refiterator.release();
        }
    }

    static String[] getConstraintInfo(Constraint c) {

        return new String[] {
            c.core.refName.name, c.core.refTable.getName().name
        };
    }

    public void clearStructures(Session session) {
        session.sessionContext.clearStructures(this);
    }
    /************************* Volt DB Extensions *************************/

    private SortAndSlice m_sortAndSlice = null;

    public void setSortAndSlice(SortAndSlice sas) {
        m_sortAndSlice = sas;
    }

    private void voltAppendTargetColumns(Session session, int[] columnMap, Expression[] expressions, VoltXMLElement xml)
    throws org.hsqldb_voltpatches.HSQLInterface.HSQLParseException
    {
        VoltXMLElement columns = new VoltXMLElement("columns");
        xml.children.add(columns);

        for (int i = 0; i < columnMap.length; i++)
        {
            VoltXMLElement column = new VoltXMLElement("column");
            columns.children.add(column);
            column.attributes.put("name", targetTable.getColumn(columnMap[i]).getName().name);
            if (expressions != null) {
                column.children.add(expressions[i].voltGetXML(session));
            }
        }
    }

    /**
     * Appends XML for ORDER BY/LIMIT/OFFSET to this statement's XML.
     * */
    private void voltAppendSortAndSlice(Session session, VoltXMLElement xml)
    throws org.hsqldb_voltpatches.HSQLInterface.HSQLParseException {
        if (m_sortAndSlice == null || m_sortAndSlice == SortAndSlice.noSort) {
            return;
        }

        // Is target a view?
        if (targetTable.getBaseTable() != targetTable) {
            // This check is unreachable, but if writable views are ever supported there
            // will be some more work to do to resolve columns in ORDER BY properly.
            throw new org.hsqldb_voltpatches.HSQLInterface.HSQLParseException("DELETE with ORDER BY, LIMIT or OFFSET is currently unsupported on views.");
        }

        if (m_sortAndSlice.hasLimit() && !m_sortAndSlice.hasOrder()) {
            throw new org.hsqldb_voltpatches.HSQLInterface.HSQLParseException("DELETE statement with LIMIT or OFFSET but no ORDER BY would produce "
                    + "non-deterministic results.  Please use an ORDER BY clause.");
        }
        else if (m_sortAndSlice.hasOrder() && !m_sortAndSlice.hasLimit()) {
            // This is harmless, but the order by is meaningless in this case.  Should
            // we let this slide?
            throw new org.hsqldb_voltpatches.HSQLInterface.HSQLParseException("DELETE statement with ORDER BY but no LIMIT or OFFSET is not allowed.  "
                    + "Consider removing the ORDER BY clause, as it has no effect here.");
        }

        java.util.List<VoltXMLElement> newElements = voltGetLimitOffsetXMLFromSortAndSlice(session, m_sortAndSlice);

        // This code isn't shared with how SELECT's ORDER BY clauses are serialized since there's
        // some extra work that goes on there to handle references to SELECT clauses aliases, etc.
        org.hsqldb_voltpatches.lib.HsqlArrayList exprList = m_sortAndSlice.exprList;
        if (exprList != null) {
            VoltXMLElement orderColumnsXml = new VoltXMLElement("ordercolumns");
            for (int i = 0; i < exprList.size(); ++i) {
                Expression e = (Expression)exprList.get(i);
                VoltXMLElement elem = e.voltGetXML(session);
                orderColumnsXml.children.add(elem);
            }

            newElements.add(orderColumnsXml);
        }

        xml.children.addAll(newElements);
    }

    private void voltAppendChildScans(Session session, VoltXMLElement xml)
    throws org.hsqldb_voltpatches.HSQLInterface.HSQLParseException
    {
        // Joins in DML statements are not yet supported, so, for now,
        // just represent the one (target) table scan.
        VoltXMLElement child = rangeVariables[0].voltGetRangeVariableXML(session);
        assert(child != null);
        xml.children.add(child);
    }

    /**
     * VoltDB added method to get a non-catalog-dependent
     * representation of this HSQLDB object.
     * @param session The current Session object may be needed to resolve
     * some names.
     * @return XML, correctly indented, representing this object.
     * @throws org.hsqldb_voltpatches.HSQLInterface.HSQLParseException
     */
    @Override
    VoltXMLElement voltGetStatementXML(Session session)
    throws org.hsqldb_voltpatches.HSQLInterface.HSQLParseException
    {
        VoltXMLElement xml;
        switch (type) {

        case StatementTypes.INSERT :
            xml = new VoltXMLElement("insert");

            assert(insertExpression != null || queryExpression != null);

            if (queryExpression == null) {
                voltAppendTargetColumns(session, insertColumnMap, insertExpression.nodes[0].nodes, xml);
            } else {
                voltAppendTargetColumns(session, insertColumnMap, null, xml);
                VoltXMLElement child = voltGetXMLExpression(queryExpression, parameters, session);
                xml.children.add(child);
            }
            break;

        case StatementTypes.UPDATE_CURSOR :
        case StatementTypes.UPDATE_WHERE :
            xml = new VoltXMLElement("update");
            voltAppendTargetColumns(session, updateColumnMap, updateExpressions, xml);
            voltAppendChildScans(session, xml);
            break;

        case StatementTypes.DELETE_CURSOR :
        case StatementTypes.DELETE_WHERE :
            xml = new VoltXMLElement("delete");
            // DELETE has no target columns
            voltAppendChildScans(session, xml);
            voltAppendSortAndSlice(session, xml);
            break;

        default:
            throw new org.hsqldb_voltpatches.HSQLInterface.HSQLParseException(
                "VoltDB does not support DML statements of type " + type);
        }

        voltAppendParameters(session, xml, parameters);
        xml.attributes.put("table", targetTable.getName().name);
        return xml;
    }
    /**********************************************************************/
}
