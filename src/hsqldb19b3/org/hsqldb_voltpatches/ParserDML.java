/* Copyright (c) 2001-2009, The HSQL Development Group
 * Copyright (c) 2010-2022, Volt Active Data Inc.
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

import org.hsqldb_voltpatches.lib.ArrayUtil;
import org.hsqldb_voltpatches.lib.HsqlArrayList;
import org.hsqldb_voltpatches.lib.HsqlList;
import org.hsqldb_voltpatches.lib.OrderedHashSet;
import org.hsqldb_voltpatches.types.Type;

/**
 * Parser for DML statements
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 1.9.0
 * @since 1.9.0
 */
public class ParserDML extends ParserDQL {

    ParserDML(Session session, Scanner t) {
        super(session, t);
    }

    /**
     * Retrieves an INSERT Statement from this parse context.
     */
    StatementDMQL compileInsertStatement(RangeVariable[] outerRanges) {

        read();
        readThis(Tokens.INTO);

        boolean[] columnCheckList;
        int[]     columnMap;
        int       colCount;
        Table     table                = readTableName();
        boolean   overridingUser       = false;
        boolean   overridingSystem     = false;
        int       enforcedDefaultIndex = table.getIdentityColumnIndex();
        boolean   assignsToIdentity    = false;

        columnCheckList = null;
        columnMap       = table.getColumnMap();
        colCount        = table.getColumnCount();

        int position = getPosition();

        if (!table.isInsertable()) {
            throw Error.error(ErrorCode.X_42550);
        }

        Table baseTable = table.getBaseTable();

        switch (token.tokenType) {

            case Tokens.DEFAULT : {
                read();
                readThis(Tokens.VALUES);

                Expression insertExpression = new Expression(OpTypes.ROW,
                    new Expression[]{});

                insertExpression = new Expression(OpTypes.TABLE,
                                                  new Expression[]{
                                                      insertExpression });
                columnCheckList = table.getNewColumnCheckList();

                for (int i = 0; i < table.colDefaults.length; i++) {
                    if (table.colDefaults[i] == null
                            && table.identityColumn != i) {
                        throw Error.error(ErrorCode.X_42544);
                    }
                }

                StatementDMQL cs = new StatementInsert(session, table,
                                                       columnMap,
                                                       insertExpression,
                                                       columnCheckList,
                                                       compileContext);

                return cs;
            }
            case Tokens.OPENBRACKET : {
                int brackets = readOpenBrackets();

                if (brackets == 1) {
                    boolean isQuery = false;

                    switch (token.tokenType) {

                        case Tokens.WITH :
                        case Tokens.SELECT :
                        case Tokens.TABLE : {
                            rewind(position);

                            isQuery = true;

                            break;
                        }
                        default :
                    }

                    if (isQuery) {
                        break;
                    }

                    OrderedHashSet columnNames = new OrderedHashSet();

                    readSimpleColumnNames(columnNames, table);
                    readThis(Tokens.CLOSEBRACKET);

                    colCount  = columnNames.size();
                    columnMap = table.getColumnIndexes(columnNames);

                    if (token.tokenType != Tokens.VALUES
                            && token.tokenType != Tokens.OVERRIDING) {
                        break;
                    }

                    // $FALL-THROUGH$
                } else {
                    rewind(position);

                    break;
                }
            }

            // $FALL-THROUGH$
            case Tokens.OVERRIDING : {
                if (token.tokenType == Tokens.OVERRIDING) {
                    read();

                    if (token.tokenType == Tokens.USER) {
                        read();

                        overridingUser = true;
                    } else if (token.tokenType == Tokens.SYSTEM) {
                        read();

                        overridingSystem = true;
                    } else {
                        unexpectedToken();
                    }
                }

                if (token.tokenType != Tokens.VALUES) {
                    break;
                }
            }

            // $FALL-THROUGH$
            case Tokens.VALUES : {
                read();

                columnCheckList = table.getColumnCheckList(columnMap);

                Expression insertExpressions =
                    XreadContextuallyTypedTable(colCount);
                HsqlList unresolved =
                    insertExpressions.resolveColumnReferences(outerRanges,
                        null);

                ExpressionColumn.checkColumnsResolved(unresolved);
                insertExpressions.resolveTypes(session, null);
                setParameterTypes(insertExpressions, table, columnMap);

                if (table != baseTable) {
                    int[] baseColumnMap = table.getBaseTableColumnMap();
                    int[] newColumnMap  = new int[columnMap.length];

                    ArrayUtil.projectRow(baseColumnMap, columnMap,
                                         newColumnMap);

                    columnMap = newColumnMap;
                }

                Expression[] rowList = insertExpressions.nodes;

                for (int j = 0; j < rowList.length; j++) {
                    Expression[] rowArgs = rowList[j].nodes;

                    for (int i = 0; i < rowArgs.length; i++) {
                        Expression e = rowArgs[i];

                        if (enforcedDefaultIndex == columnMap[i]) {
                            assignsToIdentity = true;

                            if (e.getType() != OpTypes.DEFAULT) {
                                if (table.identitySequence.isAlways()) {
                                    if (!overridingUser && !overridingSystem) {
                                        throw Error.error(ErrorCode.X_42543);
                                    }
                                } else {
/*
                                    if (overridingUser) {
                                        throw Trace.error(
                                            Trace.SQL_DEFAULT_CLAUSE_REQUITED);
                                    }
*/
                                }
                            }
                        }

                        if (e.isParam()) {
                            e.setAttributesAsColumn(
                                table.getColumn(columnMap[i]), true);
                        } else if (e.getType() == OpTypes.DEFAULT) {
                            if (table.colDefaults[i] == null
                                    && table.identityColumn != columnMap[i]) {
                                throw Error.error(ErrorCode.X_42544);
                            }
                        }
                    }
                }

                if (!assignsToIdentity
                        && (overridingUser || overridingSystem)) {
                    unexpectedTokenRequire(Tokens.T_OVERRIDING);
                }

                StatementDMQL cs = new StatementInsert(session, table,
                                                       columnMap,
                                                       insertExpressions,
                                                       columnCheckList,
                                                       compileContext);

                return cs;
            }
            case Tokens.WITH :
            case Tokens.SELECT :
            case Tokens.TABLE : {
                break;
            }
            default : {
                throw unexpectedToken();
            }
        }

        columnCheckList = table.getColumnCheckList(columnMap);

        QueryExpression queryExpression = XreadQueryExpression();

        queryExpression.setAsTopLevel();
        queryExpression.resolve(session, outerRanges);

        if (colCount != queryExpression.getColumnCount()) {
            throw Error.error(ErrorCode.X_42546);
        }

        if (table != baseTable) {
            int[] baseColumnMap = table.getBaseTableColumnMap();
            int[] newColumnMap  = new int[columnMap.length];

            ArrayUtil.projectRow(baseColumnMap, columnMap, newColumnMap);

            columnMap = newColumnMap;
        }

        if (enforcedDefaultIndex != -1
                && ArrayUtil.find(columnMap, enforcedDefaultIndex) > -1) {
            if (table.identitySequence.isAlways()) {
                if (!overridingUser && !overridingSystem) {
                    throw Error.error(ErrorCode.X_42543);
                }
            } else {
/*
                if (overridingUser) {
                    throw Trace.error(
                        Trace.SQL_DEFAULT_CLAUSE_REQUITED);
                }
*/
            }
        } else if (overridingUser || overridingSystem) {
            unexpectedTokenRequire(Tokens.T_OVERRIDING);
        }

        StatementDMQL cs = new StatementInsert(session, table, columnMap,
                                               columnCheckList,
                                               queryExpression,
                                               compileContext);

        return cs;
    }

    private static void setParameterTypes(Expression tableExpression,
                                          Table table, int[] columnMap) {

        for (int i = 0; i < tableExpression.nodes.length; i++) {
            Expression[] list = tableExpression.nodes[i].nodes;

            for (int j = 0; j < list.length; j++) {
                if (list[j].isParam()) {
                    list[j].setAttributesAsColumn(
                        table.getColumn(columnMap[j]), true);
                }
            }
        }
    }

    /**
     * Creates a MIGRATE-type statement from this parser context (i.e.
     * MIGRATE FROM tbl WHERE ...
     * @param outerRanges
     * @return compiled statement
     */
    StatementDMQL compileMigrateStatement(RangeVariable[] outerRanges) {

        final Expression condition;
        assert token.tokenType == Tokens.MIGRATE;
        read();
        readThis(Tokens.FROM);

        RangeVariable[] rangeVariables = { readSimpleRangeVariable(StatementTypes.MIGRATE_WHERE) };
        Table table     = rangeVariables[0].getTable();

        if (token.tokenType == Tokens.WHERE) {
            read();

            condition = XreadBooleanValueExpression();

            HsqlList unresolved =
                    condition.resolveColumnReferences(outerRanges, null);

            unresolved = Expression.resolveColumnSet(rangeVariables,
                    unresolved, null);

            ExpressionColumn.checkColumnsResolved(unresolved);
            condition.resolveTypes(session, null);

            if (condition.isParam()) {
                condition.dataType = Type.SQL_BOOLEAN;
            }

            if (condition.getDataType() != Type.SQL_BOOLEAN) {
                throw Error.error(ErrorCode.X_42568);
            }
        } else {
            throw Error.error(ErrorCode.X_47000);
        }
        // check WHERE condition
        RangeVariableResolver resolver =
                new RangeVariableResolver(rangeVariables, condition,
                        compileContext);
        resolver.processConditions();

        rangeVariables = resolver.rangeVariables;

        return new StatementDML(session, table, rangeVariables, compileContext);
    }

    /**
     * Creates a DELETE-type Statement from this parse context.
     */
    StatementDMQL compileDeleteStatement(RangeVariable[] outerRanges) {

        Expression condition       = null;
        boolean    truncate        = false;
        boolean    restartIdentity = false;

        switch (token.tokenType) {

            case Tokens.TRUNCATE : {
                read();
                readThis(Tokens.TABLE);

                truncate = true;

                break;
            }
            case Tokens.DELETE : {
                read();
                readThis(Tokens.FROM);

                break;
            }
        }

        RangeVariable[] rangeVariables = {
            readSimpleRangeVariable(StatementTypes.DELETE_WHERE) };
        Table table     = rangeVariables[0].getTable();
        Table baseTable = table.getBaseTable();

        /* A VoltDB Extension.
         * Views from Streams are now updatable.
         * Comment out this guard and check if it is a view
         * from Stream or PersistentTable in planner.
        if (!table.isUpdatable()) {
            throw Error.error(ErrorCode.X_42000);
        }
        A VoltDB Extension */

        if (truncate) {
            switch (token.tokenType) {

                case Tokens.CONTINUE : {
                    read();
                    readThis(Tokens.IDENTITY);

                    break;
                }
                case Tokens.RESTART : {
                    read();
                    readThis(Tokens.IDENTITY);

                    restartIdentity = true;

                    break;
                }
            }

            for (int i = 0; i < table.constraintList.length; i++) {
                if (table.constraintList[i].getConstraintType()
                        == Constraint.MAIN) {
                    throw Error.error(ErrorCode.X_23501);
                }
            }
        }

        if (truncate && table != baseTable) {
            throw Error.error(ErrorCode.X_42000);
        }

        if (!truncate && token.tokenType == Tokens.WHERE) {
            read();

            condition = XreadBooleanValueExpression();

            HsqlList unresolved =
                condition.resolveColumnReferences(outerRanges, null);

            unresolved = Expression.resolveColumnSet(rangeVariables,
                    unresolved, null);

            ExpressionColumn.checkColumnsResolved(unresolved);
            condition.resolveTypes(session, null);

            if (condition.isParam()) {
                condition.dataType = Type.SQL_BOOLEAN;
            }

            if (condition.getDataType() != Type.SQL_BOOLEAN) {
                throw Error.error(ErrorCode.X_42568);
            }
        }

        // VoltDB Extension:
        // baseTable could be null for stream views.
        if (baseTable != null && table != baseTable) {
            QuerySpecification select =
                ((TableDerived) table).getQueryExpression().getMainSelect();

            if (condition != null) {
                condition =
                    condition.replaceColumnReferences(rangeVariables[0],
                                                      select.exprColumns);
            }

            rangeVariables[0] = new RangeVariable(select.rangeVariables[0]);
            condition = ExpressionLogical.andExpressions(select.queryCondition,
                    condition);
        }

        if (condition != null) {
            RangeVariableResolver resolver =
                new RangeVariableResolver(rangeVariables, condition,
                                          compileContext);

            resolver.processConditions();

            rangeVariables = resolver.rangeVariables;
        }

        // VoltDB Extension:
        // This needs to be done before building the compiled statement
        // so that parameters in LIMIT or OFFSET are retrieved from
        // the compileContext
        SortAndSlice sas = voltGetSortAndSliceForDelete(rangeVariables);

        StatementDMQL cs = new StatementDML(session, table, rangeVariables,
                                            compileContext, restartIdentity);

        // VoltDB Extension:
        voltAppendDeleteSortAndSlice((StatementDML)cs, sas);

        return cs;
    }

    /**
     * Creates an UPDATE-type Statement from this parse context.
     */
    StatementDMQL compileUpdateStatement(RangeVariable[] outerRanges) {

        read();

        Expression[]   updateExpressions;
        int[]          columnMap;
        boolean[]      columnCheckList;
        OrderedHashSet colNames = new OrderedHashSet();
        HsqlArrayList  exprList = new HsqlArrayList();
        RangeVariable[] rangeVariables = {
            readSimpleRangeVariable(StatementTypes.UPDATE_WHERE) };
        Table table     = rangeVariables[0].rangeTable;
        Table baseTable = table.getBaseTable();

        readThis(Tokens.SET);
        readSetClauseList(rangeVariables, colNames, exprList);

        columnMap         = table.getColumnIndexes(colNames);
        columnCheckList   = table.getColumnCheckList(columnMap);
        updateExpressions = new Expression[exprList.size()];

        exprList.toArray(updateExpressions);

        Expression condition = null;

        if (token.tokenType == Tokens.WHERE) {
            read();

            condition = XreadBooleanValueExpression();

            HsqlList unresolved =
                condition.resolveColumnReferences(outerRanges, null);

            unresolved = Expression.resolveColumnSet(rangeVariables,
                    unresolved, null);

            ExpressionColumn.checkColumnsResolved(unresolved);
            condition.resolveTypes(session, null);

            if (condition.isParam()) {
                condition.dataType = Type.SQL_BOOLEAN;
            } else if (condition.getDataType() != Type.SQL_BOOLEAN) {
                throw Error.error(ErrorCode.X_42568);
            }
        }

        resolveUpdateExpressions(table, rangeVariables, columnMap,
                                 updateExpressions, outerRanges);

        if (baseTable != null && table != baseTable) {
            QuerySpecification select =
                ((TableDerived) table).getQueryExpression().getMainSelect();

            if (condition != null) {
                condition =
                    condition.replaceColumnReferences(rangeVariables[0],
                                                      select.exprColumns);
            }

            rangeVariables[0] = new RangeVariable(select.rangeVariables[0]);
            condition = ExpressionLogical.andExpressions(select.queryCondition,
                    condition);
        }

        if (condition != null) {
            RangeVariableResolver resolver =
                new RangeVariableResolver(rangeVariables, condition,
                                          compileContext);

            resolver.processConditions();

            rangeVariables = resolver.rangeVariables;
        }

        if (baseTable != null && table != baseTable) {
            int[] baseColumnMap = table.getBaseTableColumnMap();
            int[] newColumnMap  = new int[columnMap.length];

            ArrayUtil.projectRow(baseColumnMap, columnMap, newColumnMap);

            columnMap = newColumnMap;
        }

        StatementDMQL cs = new StatementDML(session, table, rangeVariables,
                                            columnMap, updateExpressions,
                                            columnCheckList, compileContext);

        return cs;
    }

    void resolveUpdateExpressions(Table targetTable,
                                  RangeVariable[] rangeVariables,
                                  int[] columnMap,
                                  Expression[] colExpressions,
                                  RangeVariable[] outerRanges) {

        HsqlList unresolved           = null;
        int      enforcedDefaultIndex = -1;

        if (targetTable.hasIdentityColumn()
                && targetTable.identitySequence.isAlways()) {
            enforcedDefaultIndex = targetTable.getIdentityColumnIndex();
        }

        for (int i = 0, ix = 0; i < columnMap.length; ix++) {
            Expression expr = colExpressions[ix];
            Expression e;

            if (expr.getType() == OpTypes.ROW) {
                Expression[] elements = expr.nodes;

                for (int j = 0; j < elements.length; j++, i++) {
                    e = elements[j];

                    if (enforcedDefaultIndex == columnMap[i]) {
                        if (e.getType() != OpTypes.DEFAULT) {
                            throw Error.error(ErrorCode.X_42541);
                        }
                    }

                    if (e.isParam()) {
                        e.setAttributesAsColumn(
                            targetTable.getColumn(columnMap[i]), true);
                    } else if (e.getType() == OpTypes.DEFAULT) {
                        if (targetTable.colDefaults[columnMap[i]] == null
                                && targetTable.identityColumn
                                   != columnMap[i]) {
                            throw Error.error(ErrorCode.X_42544);
                        }
                    } else {
                        unresolved = expr.resolveColumnReferences(outerRanges,
                                null);
                        unresolved =
                            Expression.resolveColumnSet(rangeVariables,
                                                        unresolved, null);

                        ExpressionColumn.checkColumnsResolved(unresolved);

                        unresolved = null;

                        e.resolveTypes(session, null);
                    }
                }
            } else if (expr.getType() == OpTypes.TABLE_SUBQUERY) {
                unresolved = expr.resolveColumnReferences(outerRanges, null);
                unresolved = Expression.resolveColumnSet(rangeVariables,
                        unresolved, null);

                ExpressionColumn.checkColumnsResolved(unresolved);
                expr.resolveTypes(session, null);

                int count = expr.subQuery.queryExpression.getColumnCount();

                for (int j = 0; j < count; j++, i++) {
                    if (enforcedDefaultIndex == columnMap[i]) {
                        throw Error.error(ErrorCode.X_42541);
                    }
                }
            } else {
                e = expr;

                if (enforcedDefaultIndex == columnMap[i]) {
                    if (e.getType() != OpTypes.DEFAULT) {
                        throw Error.error(ErrorCode.X_42541);
                    }
                }

                if (e.isParam()) {
                    e.setAttributesAsColumn(
                        targetTable.getColumn(columnMap[i]), true);
                } else if (e.getType() == OpTypes.DEFAULT) {
                    if (targetTable.colDefaults[columnMap[i]] == null
                            && targetTable.identityColumn != columnMap[i]) {
                        throw Error.error(ErrorCode.X_42544);
                    }
                } else {
                    unresolved = expr.resolveColumnReferences(outerRanges,
                            null);
                    unresolved = Expression.resolveColumnSet(rangeVariables,
                            unresolved, null);

                    ExpressionColumn.checkColumnsResolved(unresolved);
                    e.resolveTypes(session, null);
                }

                i++;
            }
        }

        /* Disabled for ENG-11832
        if (!targetTable.isView) {
            return;
        }

        QuerySpecification select =
            ((TableDerived) targetTable).getQueryExpression().getMainSelect();

        for (int i = 0; i < colExpressions.length; i++) {
            colExpressions[i] =
                colExpressions[i].replaceColumnReferences(rangeVariables[0],
                    select.exprColumns);
        }
        */
    }

    void readSetClauseList(RangeVariable[] rangeVars, OrderedHashSet colNames,
                           HsqlArrayList expressions) {

        while (true) {
            int degree;

            if (token.tokenType == Tokens.OPENBRACKET) {
                read();

                int oldCount = colNames.size();

                readColumnNames(colNames, rangeVars);

                degree = colNames.size() - oldCount;

                readThis(Tokens.CLOSEBRACKET);
            } else {
                ColumnSchema column = readColumnName(rangeVars);

                if (!colNames.add(column.getName().name)) {
                    throw Error.error(ErrorCode.X_42578,
                                      column.getName().name);
                }

                degree = 1;
            }

            readThis(Tokens.EQUALS);

            int position = getPosition();
            int brackets = readOpenBrackets();

            if (token.tokenType == Tokens.SELECT) {
                rewind(position);

                SubQuery sq = XreadSubqueryBody(false, OpTypes.ROW_SUBQUERY);

                if (degree != sq.queryExpression.getColumnCount()) {
                    throw Error.error(ErrorCode.X_42546);
                }

                Expression e = new Expression(OpTypes.ROW_SUBQUERY, sq);

                expressions.add(e);

                if (token.tokenType == Tokens.COMMA) {
                    read();

                    continue;
                }

                break;
            }

            if (brackets > 0) {
                rewind(position);
            }

            if (degree > 1) {
                readThis(Tokens.OPENBRACKET);

                Expression e = readRow();

                readThis(Tokens.CLOSEBRACKET);

                int rowDegree = e.getType() == OpTypes.ROW ? e.nodes.length
                                                           : 1;

                if (degree != rowDegree) {
                    throw Error.error(ErrorCode.X_42546);
                }

                expressions.add(e);
            } else {
                Expression e = XreadValueExpressionWithContext();

                expressions.add(e);
            }

            if (token.tokenType == Tokens.COMMA) {
                read();

                continue;
            }

            break;
        }
    }

    /**
     * Retrieves a MERGE Statement from this parse context.
     */
    StatementDMQL compileMergeStatement(RangeVariable[] outerRanges) {

        boolean[]     insertColumnCheckList;
        int[]         insertColumnMap = null;
        int[]         updateColumnMap = null;
        int[]         baseUpdateColumnMap;
        Table         table;
        RangeVariable targetRange;
        RangeVariable sourceRange;
        Expression    mergeCondition;
        HsqlArrayList updateList        = new HsqlArrayList();
        Expression[]  updateExpressions = null;
        HsqlArrayList insertList        = new HsqlArrayList();
        Expression    insertExpression  = null;

        read();
        readThis(Tokens.INTO);

        targetRange = readSimpleRangeVariable(StatementTypes.MERGE);
        table       = targetRange.rangeTable;

        readThis(Tokens.USING);

        sourceRange = readTableOrSubquery();

        // parse ON search conditions
        readThis(Tokens.ON);

        mergeCondition = XreadBooleanValueExpression();

        if (mergeCondition.getDataType() != Type.SQL_BOOLEAN) {
            throw Error.error(ErrorCode.X_42568);
        }

        RangeVariable[] fullRangeVars   = new RangeVariable[] {
            sourceRange, targetRange
        };
        RangeVariable[] sourceRangeVars = new RangeVariable[]{ sourceRange };
        RangeVariable[] targetRangeVars = new RangeVariable[]{ targetRange };

        // parse WHEN clause(s) and convert lists to arrays
        insertColumnMap       = table.getColumnMap();
        insertColumnCheckList = table.getNewColumnCheckList();

        OrderedHashSet updateColNames = new OrderedHashSet();
        OrderedHashSet insertColNames = new OrderedHashSet();

        readMergeWhen(insertColNames, updateColNames, insertList, updateList,
                      targetRangeVars, sourceRange);

        if (insertList.size() > 0) {
            int colCount = insertColNames.size();

            if (colCount != 0) {
                insertColumnMap = table.getColumnIndexes(insertColNames);
                insertColumnCheckList =
                    table.getColumnCheckList(insertColumnMap);
            }

            insertExpression = (Expression) insertList.get(0);

            setParameterTypes(insertExpression, table, insertColumnMap);
        }

        if (updateList.size() > 0) {
            updateExpressions = new Expression[updateList.size()];

            updateList.toArray(updateExpressions);

            updateColumnMap = table.getColumnIndexes(updateColNames);
        }

        if (updateExpressions != null) {
            Table baseTable = table.getBaseTable();

            baseUpdateColumnMap = updateColumnMap;

            if (table != baseTable) {
                baseUpdateColumnMap = new int[updateColumnMap.length];

                ArrayUtil.projectRow(table.getBaseTableColumnMap(),
                                     updateColumnMap, baseUpdateColumnMap);
            }

            resolveUpdateExpressions(table, sourceRangeVars, updateColumnMap,
                                     updateExpressions, outerRanges);
        }

        HsqlList unresolved = null;

        unresolved = mergeCondition.resolveColumnReferences(fullRangeVars,
                null);

        ExpressionColumn.checkColumnsResolved(unresolved);
        mergeCondition.resolveTypes(session, null);

        if (mergeCondition.isParam()) {
            mergeCondition.dataType = Type.SQL_BOOLEAN;
        }

        if (mergeCondition.getDataType() != Type.SQL_BOOLEAN) {
            throw Error.error(ErrorCode.X_42568);
        }

        RangeVariableResolver resolver =
            new RangeVariableResolver(fullRangeVars, mergeCondition,
                                      compileContext);

        resolver.processConditions();

        fullRangeVars = resolver.rangeVariables;

        if (insertExpression != null) {
            unresolved =
                insertExpression.resolveColumnReferences(sourceRangeVars,
                    unresolved);

            ExpressionColumn.checkColumnsResolved(unresolved);
            insertExpression.resolveTypes(session, null);
        }

        StatementDMQL cs = new StatementDML(session, fullRangeVars,
                                            insertColumnMap, updateColumnMap,
                                            insertColumnCheckList,
                                            mergeCondition, insertExpression,
                                            updateExpressions, compileContext);

        return cs;
    }

    /**
     * Parses a WHEN clause from a MERGE statement. This can be either a
     * WHEN MATCHED or WHEN NOT MATCHED clause, or both, and the appropriate
     * values will be updated.
     *
     * If the var that is to hold the data is not null, then we already
     * encountered this type of clause, which is only allowed once, and at least
     * one is required.
     */
    private void readMergeWhen(OrderedHashSet insertColumnNames,
                               OrderedHashSet updateColumnNames,
                               HsqlArrayList insertExpressions,
                               HsqlArrayList updateExpressions,
                               RangeVariable[] targetRangeVars,
                               RangeVariable sourceRangeVar) {

        Table table       = targetRangeVars[0].rangeTable;
        int   columnCount = table.getColumnCount();

        readThis(Tokens.WHEN);

        if (token.tokenType == Tokens.MATCHED) {
            if (updateExpressions.size() != 0) {
                throw Error.error(ErrorCode.X_42547);
            }

            read();
            readThis(Tokens.THEN);
            readThis(Tokens.UPDATE);
            readThis(Tokens.SET);
            readSetClauseList(targetRangeVars, updateColumnNames,
                              updateExpressions);
        } else if (token.tokenType == Tokens.NOT) {
            if (insertExpressions.size() != 0) {
                throw Error.error(ErrorCode.X_42548);
            }

            read();
            readThis(Tokens.MATCHED);
            readThis(Tokens.THEN);
            readThis(Tokens.INSERT);

            // parse INSERT statement
            // optional column list
            int brackets = readOpenBrackets();

            if (brackets == 1) {
                readSimpleColumnNames(insertColumnNames, targetRangeVars[0]);
                readThis(Tokens.CLOSEBRACKET);

                brackets = 0;
            }

            readThis(Tokens.VALUES);

            Expression e = XreadContextuallyTypedTable(columnCount);

            if (e.nodes.length != 1) {
                throw Error.error(ErrorCode.X_21000);
            }

            insertExpressions.add(e);
        } else {
            throw unexpectedToken();
        }

        if (token.tokenType == Tokens.WHEN) {
            readMergeWhen(insertColumnNames, updateColumnNames,
                          insertExpressions, updateExpressions,
                          targetRangeVars, sourceRangeVar);
        }
    }

    /**
     * Retrieves a CALL Statement from this parse context.
     */

    // to do call argument name and type resolution
    StatementDMQL compileCallStatement(RangeVariable[] outerRanges,
                                       boolean isStrictlyProcedure) {

        read();

        if (isIdentifier()) {
            checkValidCatalogName(token.namePrePrefix);

            RoutineSchema routineSchema =
                (RoutineSchema) database.schemaManager.findSchemaObject(
                    token.tokenString,
                    session.getSchemaName(token.namePrefix),
                    SchemaObject.PROCEDURE);

            if (routineSchema != null) {
                read();

                HsqlArrayList list = new HsqlArrayList();

                readThis(Tokens.OPENBRACKET);

                if (token.tokenType == Tokens.CLOSEBRACKET) {
                    read();
                } else {
                    while (true) {
                        Expression e = XreadValueExpression();

                        list.add(e);

                        if (token.tokenType == Tokens.COMMA) {
                            read();
                        } else {
                            readThis(Tokens.CLOSEBRACKET);

                            break;
                        }
                    }
                }

                Expression[] arguments = new Expression[list.size()];

                list.toArray(arguments);

                Routine routine =
                    routineSchema.getSpecificRoutine(arguments.length);
                HsqlList unresolved = null;

                for (int i = 0; i < arguments.length; i++) {
                    Expression e = arguments[i];

                    if (e.isParam()) {
                        e.setAttributesAsColumn(
                            routine.getParameter(i),
                            routine.getParameter(i).isWriteable());
                    } else {
                        int paramMode =
                            routine.getParameter(i).getParameterMode();

                        unresolved =
                            arguments[i].resolveColumnReferences(outerRanges,
                                unresolved);

                        if (paramMode
                                != SchemaObject.ParameterModes.PARAM_IN) {
                            if (e.getType() != OpTypes.VARIABLE) {
                                throw Error.error(ErrorCode.X_42603);
                            }
                        }
                    }
                }

                ExpressionColumn.checkColumnsResolved(unresolved);

                for (int i = 0; i < arguments.length; i++) {
                    arguments[i].resolveTypes(session, null);
                }

                StatementDMQL cs = new StatementProcedure(session, routine,
                    arguments, compileContext);

                return cs;
            }
        }

        if (isStrictlyProcedure) {
            throw Error.error(ErrorCode.X_42501, token.tokenString);
        }

        Expression expression = this.XreadValueExpression();
        HsqlList unresolved = expression.resolveColumnReferences(outerRanges,
            null);

        ExpressionColumn.checkColumnsResolved(unresolved);
        expression.resolveTypes(session, null);

//        expression.paramMode = PARAM_OUT;
        StatementDMQL cs = new StatementProcedure(session, expression,
            compileContext);

        return cs;
    }

    /************************* Volt DB Extensions *************************/

    /**
     * This is a Volt extension to allow
     *   DELETE FROM tab ORDER BY c LIMIT 1
     * Adds a SortAndSlice object to the statement if the next tokens
     * are ORDER BY, LIMIT or OFFSET.
     * @param deleteStmt
     */
    private SortAndSlice voltGetSortAndSliceForDelete(RangeVariable[] rangeVariables) {
        SortAndSlice sas = XreadOrderByExpression();
        if (sas == null || sas == SortAndSlice.noSort)
            return SortAndSlice.noSort;

        // Resolve columns in the ORDER BY clause.  This code modified
        // from how compileDelete resolves columns in its WHERE clause
        for (int i = 0; i < sas.exprList.size(); ++i) {
            Expression e = (Expression)sas.exprList.get(i);
            HsqlList unresolved =
                e.resolveColumnReferences(RangeVariable.emptyArray, null);

            unresolved = Expression.resolveColumnSet(rangeVariables,
                    unresolved, null);

            ExpressionColumn.checkColumnsResolved(unresolved);
            e.resolveTypes(session, null);
        }

        return sas;
    }

    private void voltAppendDeleteSortAndSlice(StatementDML deleteStmt, SortAndSlice sas) {
        deleteStmt.setSortAndSlice(sas);
    }
    /**********************************************************************/

}
