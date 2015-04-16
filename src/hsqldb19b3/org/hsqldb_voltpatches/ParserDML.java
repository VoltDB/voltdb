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
import org.hsqldb_voltpatches.RangeGroup.RangeGroupSimple;
import org.hsqldb_voltpatches.error.Error;
import org.hsqldb_voltpatches.error.ErrorCode;
import org.hsqldb_voltpatches.lib.ArrayUtil;
import org.hsqldb_voltpatches.lib.HsqlArrayList;
import org.hsqldb_voltpatches.lib.HsqlList;
import org.hsqldb_voltpatches.lib.LongDeque;
import org.hsqldb_voltpatches.lib.OrderedHashSet;
import org.hsqldb_voltpatches.types.Type;

/**
 * Parser for DML statements
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 2.3.2
 * @since 1.9.0
 */
public class ParserDML extends ParserDQL {

    ParserDML(Session session, Scanner t) {
        super(session, t, null);
    }

    /**
     * Retrieves an INSERT Statement from this parse context.
     */
    StatementDMQL compileInsertStatement(RangeGroup[] rangeGroups) {

        read();
        readThis(Tokens.INTO);

        boolean[]     columnCheckList;
        int[]         columnMap;
        int           colCount;
        Table         table;
        RangeVariable range;
        boolean       overridingUser    = false;
        boolean       overridingSystem  = false;
        boolean       assignsToIdentity = false;
        Token         tableToken;
        boolean       hasColumnList = false;

        tableToken = getRecordedToken();
        range      = readRangeVariableForDataChange(StatementTypes.INSERT);

        range.resolveRangeTableTypes(session, RangeVariable.emptyArray);

        table           = range.getTable();
        columnCheckList = null;
        columnMap       = table.getColumnMap();
        colCount        = table.getColumnCount();

        int   position  = getPosition();
        Table baseTable = table.isTriggerInsertable() ? table
                                                      : table.getBaseTable();

        switch (token.tokenType) {

            case Tokens.DEFAULT : {
                read();
                readThis(Tokens.VALUES);

                Expression insertExpression = new Expression(OpTypes.ROW,
                    new Expression[]{});

                insertExpression = new Expression(OpTypes.VALUELIST,
                                                  new Expression[]{
                                                      insertExpression });
                columnCheckList = table.getNewColumnCheckList();

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
                    boolean        withPrefix  = database.sqlSyntaxOra;

                    readSimpleColumnNames(columnNames, range, withPrefix);
                    readThis(Tokens.CLOSEBRACKET);

                    colCount      = columnNames.size();
                    columnMap     = table.getColumnIndexes(columnNames);
                    hasColumnList = true;

                    if (token.tokenType != Tokens.VALUES
                            && token.tokenType != Tokens.OVERRIDING) {
                        break;
                    }

                    // fall through
                } else {
                    rewind(position);

                    break;
                }
            }

            // fall through
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
                        throw unexpectedToken();
                    }

                    readThis(Tokens.VALUE);

                    if (token.tokenType != Tokens.VALUES) {
                        break;
                    }
                }
            }

            // fall through
            case Tokens.VALUES : {
                read();

                columnCheckList = table.getColumnCheckList(columnMap);

                Expression insertExpressions =
                    XreadContextuallyTypedTable(colCount);
                HsqlList unresolved =
                    insertExpressions.resolveColumnReferences(session,
                        RangeGroup.emptyGroup, rangeGroups, null);

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
                        ColumnSchema column =
                            baseTable.getColumn(columnMap[i]);

                        if (column.isIdentity()) {
                            assignsToIdentity = true;

                            if (e.getType() != OpTypes.DEFAULT) {
                                if (baseTable.identitySequence.isAlways()) {
                                    if (!overridingUser && !overridingSystem) {
                                        throw Error.error(ErrorCode.X_42543);
                                    }
                                }

                                if (overridingUser) {
                                    rowArgs[i] =
                                        new ExpressionColumn(OpTypes.DEFAULT);
                                }
                            }
                        } else if (column.hasDefault()) {

                            //
                        } else if (column.isGenerated()) {
                            if (e.getType() != OpTypes.DEFAULT) {
                                throw Error.error(ErrorCode.X_42541);
                            }
                        } else {

                            // no explicit default
                        }

                        if (e.isUnresolvedParam()) {
                            e.setAttributesAsColumn(column, true);
                        }
                    }
                }

                if (!assignsToIdentity
                        && (overridingUser || overridingSystem)) {
                    throw unexpectedTokenRequire(Tokens.T_OVERRIDING);
                }

                if (!hasColumnList) {
                    tableToken.setWithColumnList();
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

        if (table != baseTable) {
            int[] baseColumnMap = table.getBaseTableColumnMap();
            int[] newColumnMap  = new int[columnMap.length];

            ArrayUtil.projectRow(baseColumnMap, columnMap, newColumnMap);

            columnMap = newColumnMap;
        }

        int enforcedDefaultIndex = baseTable.getIdentityColumnIndex();
        int overrideIndex        = -1;

        if (enforcedDefaultIndex != -1
                && ArrayUtil.find(columnMap, enforcedDefaultIndex) > -1) {
            if (baseTable.identitySequence.isAlways()) {
                if (!overridingUser && !overridingSystem) {
                    throw Error.error(ErrorCode.X_42543);
                }
            }

            if (overridingUser) {
                overrideIndex = enforcedDefaultIndex;
            }
        } else if (overridingUser || overridingSystem) {
            throw unexpectedTokenRequire(Tokens.T_OVERRIDING);
        }

        Type[] types = new Type[columnMap.length];

        ArrayUtil.projectRow(baseTable.getColumnTypes(), columnMap, types);
        compileContext.setOuterRanges(rangeGroups);

        QueryExpression queryExpression = XreadQueryExpression();

        queryExpression.setReturningResult();
        queryExpression.resolve(session, rangeGroups, types);

        if (colCount != queryExpression.getColumnCount()) {
            throw Error.error(ErrorCode.X_42546);
        }

        if (!hasColumnList) {
            tableToken.setWithColumnList();
        }

        StatementDMQL cs = new StatementInsert(session, table, columnMap,
                                               columnCheckList,
                                               queryExpression,
                                               compileContext, overrideIndex);

        return cs;
    }

    private static void setParameterTypes(Expression tableExpression,
                                          Table table, int[] columnMap) {

        for (int i = 0; i < tableExpression.nodes.length; i++) {
            Expression[] list = tableExpression.nodes[i].nodes;

            for (int j = 0; j < list.length; j++) {
                if (list[j].isUnresolvedParam()) {
                    list[j].setAttributesAsColumn(
                        table.getColumn(columnMap[j]), true);
                }
            }
        }
    }

    Statement compileTruncateStatement() {

        boolean         isTable         = false;
        boolean         withCommit      = false;
        boolean         noCheck         = false;
        boolean         restartIdentity = false;
        HsqlName        objectName      = null;
        RangeVariable[] rangeVariables  = null;
        Table           table           = null;
        HsqlName[]      writeTableNames = null;
        RangeVariable   targetRange     = null;

        readThis(Tokens.TRUNCATE);

        if (token.tokenType == Tokens.TABLE) {
            readThis(Tokens.TABLE);

            targetRange =
                readRangeVariableForDataChange(StatementTypes.TRUNCATE);
            rangeVariables = new RangeVariable[]{ targetRange };
            table          = rangeVariables[0].getTable();
            objectName     = table.getName();
            isTable        = true;
        } else {
            readThis(Tokens.SCHEMA);

            objectName = readSchemaName();
        }

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

        if (!isTable) {
            checkIsThis(Tokens.AND);
        }

        if (readIfThis(Tokens.AND)) {
            readThis(Tokens.COMMIT);

            withCommit = true;

            if (readIfThis(Tokens.NO)) {
                readThis(Tokens.CHECK);

                noCheck = true;
            }
        }

        if (isTable) {
            writeTableNames = new HsqlName[]{ table.getName() };
        } else {
            writeTableNames =
                session.database.schemaManager.getCatalogAndBaseTableNames();
        }

        if (withCommit) {
            Object[] args = new Object[] {
                objectName, restartIdentity, noCheck
            };

            return new StatementCommand(StatementTypes.TRUNCATE, args, null,
                                        writeTableNames);
        }

        Statement cs = new StatementDML(session, table, targetRange,
                                        rangeVariables, compileContext,
                                        restartIdentity,
                                        StatementTypes.TRUNCATE);

        return cs;
    }

    /**
     * Creates a DELETE-type Statement from this parse context.
     */
    Statement compileDeleteStatement(RangeGroup[] rangeGroups) {

        Expression      condition       = null;
        boolean         restartIdentity = false;
        RangeVariable   targetRange;
        RangeVariable[] rangeVariables;
        Table           table;

        readThis(Tokens.DELETE);
        readThis(Tokens.FROM);

        targetRange =
            readRangeVariableForDataChange(StatementTypes.DELETE_WHERE);
        rangeVariables = new RangeVariable[]{ targetRange };
        table          = rangeVariables[0].getTable();

        compileContext.setOuterRanges(rangeGroups);

        if (token.tokenType == Tokens.WHERE) {
            read();

            condition = XreadBooleanValueExpression();

            RangeGroup rangeGroup = new RangeGroupSimple(rangeVariables,
                false);
            HsqlList unresolved = condition.resolveColumnReferences(session,
                rangeGroup, rangeGroups, null);

            ExpressionColumn.checkColumnsResolved(unresolved);
            condition.resolveTypes(session, null);

            if (condition.isUnresolvedParam()) {
                condition.dataType = Type.SQL_BOOLEAN;
            }

            if (condition.getDataType() != Type.SQL_BOOLEAN) {
                throw Error.error(ErrorCode.X_42568);
            }
        }

        Table baseTable = table.isTriggerDeletable() ? table
                                                     : table.getBaseTable();

        if (table != baseTable) {
            QuerySpecification baseSelect =
                table.getQueryExpression().getMainSelect();

            if (condition != null) {
                condition =
                    condition.replaceColumnReferences(rangeVariables[0],
                                                      baseSelect.exprColumns);
            }

            condition =
                ExpressionLogical.andExpressions(baseSelect.queryCondition,
                                                 condition);
            rangeVariables = baseSelect.rangeVariables;
        }

        if (condition != null) {
            rangeVariables[0].addJoinCondition(condition);

            RangeVariableResolver resolver =
                new RangeVariableResolver(rangeVariables, null,
                                          compileContext, false);

            resolver.processConditions(session);

            rangeVariables = resolver.rangeVariables;
        }

        for (int i = 0; i < rangeVariables.length; i++) {
            rangeVariables[i].resolveRangeTableTypes(session,
                    RangeVariable.emptyArray);
        }

        // A VoltDB extension to support LIMIT and OFFSET on DELETE statements.
        // This needs to be done before building the compiled statement
        // so that parameters in LIMIT or OFFSET are retrieved from
        // the compileContext
        SortAndSlice sas = voltGetSortAndSliceForDelete(session, rangeGroups, rangeVariables);

        // End of VoltDB extension
        Statement cs = new StatementDML(session, table, targetRange,
                                        rangeVariables, compileContext,
                                        restartIdentity,
                                        StatementTypes.DELETE_WHERE);

        // A VoltDB extension to support LIMIT and OFFSET on DELETE statements.
        voltAppendDeleteSortAndSlice((StatementDML)cs, sas);
        // End of VoltDB extension
        return cs;
    }

    /**
     * Creates an UPDATE-type Statement from this parse context.
     */
    StatementDMQL compileUpdateStatement(RangeGroup[] rangeGroups) {

        read();

        Expression[]    updateExpressions;
        int[]           columnMap;
        boolean[]       columnCheckList;
        OrderedHashSet  targetSet    = new OrderedHashSet();
        LongDeque       colIndexList = new LongDeque();
        HsqlArrayList   exprList     = new HsqlArrayList();
        RangeVariable   targetRange;
        RangeVariable[] rangeVariables;
        RangeGroup      rangeGroup;
        Table           table;
        Table           baseTable;

        targetRange =
            readRangeVariableForDataChange(StatementTypes.UPDATE_WHERE);
        rangeVariables = new RangeVariable[]{ targetRange };
        rangeGroup     = new RangeGroupSimple(rangeVariables, false);
        table          = rangeVariables[0].rangeTable;
        baseTable      = table.isTriggerUpdatable() ? table
                                                    : table.getBaseTable();

        readThis(Tokens.SET);
        readSetClauseList(rangeVariables, targetSet, colIndexList, exprList);

        columnMap = new int[colIndexList.size()];

        colIndexList.toArray(columnMap);

        Expression[] targets = new Expression[targetSet.size()];

        targetSet.toArray(targets);

        for (int i = 0; i < targets.length; i++) {
            this.resolveOuterReferencesAndTypes(rangeGroups, targets[i]);
        }

        columnCheckList   = table.getColumnCheckList(columnMap);
        updateExpressions = new Expression[exprList.size()];

        exprList.toArray(updateExpressions);

        Expression condition = null;

        if (token.tokenType == Tokens.WHERE) {
            read();

            condition = XreadBooleanValueExpression();

            HsqlList unresolved = condition.resolveColumnReferences(session,
                rangeGroup, rangeGroups, null);

            ExpressionColumn.checkColumnsResolved(unresolved);
            condition.resolveTypes(session, null);

            if (condition.isUnresolvedParam()) {
                condition.dataType = Type.SQL_BOOLEAN;
            }

            if (condition.getDataType() != Type.SQL_BOOLEAN) {
                throw Error.error(ErrorCode.X_42568);
            }
        }

        resolveUpdateExpressions(table, rangeGroup, columnMap,
                                 updateExpressions, rangeGroups);

        if (table != baseTable) {
            QuerySpecification baseSelect =
                ((TableDerived) table).getQueryExpression().getMainSelect();

            if (condition != null) {
                condition =
                    condition.replaceColumnReferences(rangeVariables[0],
                                                      baseSelect.exprColumns);
            }

            for (int i = 0; i < updateExpressions.length; i++) {
                updateExpressions[i] =
                    updateExpressions[i].replaceColumnReferences(
                        rangeVariables[0], baseSelect.exprColumns);
            }

            condition =
                ExpressionLogical.andExpressions(baseSelect.queryCondition,
                                                 condition);
            rangeVariables = baseSelect.rangeVariables;
        }

        if (condition != null) {
            rangeVariables[0].addJoinCondition(condition);

            RangeVariableResolver resolver =
                new RangeVariableResolver(rangeVariables, null,
                                          compileContext, false);

            resolver.processConditions(session);

            rangeVariables = resolver.rangeVariables;
        }

        for (int i = 0; i < rangeVariables.length; i++) {
            rangeVariables[i].resolveRangeTableTypes(session,
                    RangeVariable.emptyArray);
        }

        if (table != baseTable) {
            int[] baseColumnMap = table.getBaseTableColumnMap();
            int[] newColumnMap  = new int[columnMap.length];

            ArrayUtil.projectRow(baseColumnMap, columnMap, newColumnMap);

            columnMap = newColumnMap;

            for (int i = 0; i < columnMap.length; i++) {
                if (baseTable.colGenerated[columnMap[i]]) {
                    throw Error.error(ErrorCode.X_42513);
                }
            }
        }

        StatementDMQL cs = new StatementDML(session, targets, table,
                                            targetRange, rangeVariables,
                                            columnMap, updateExpressions,
                                            columnCheckList, compileContext);

        return cs;
    }

    void resolveUpdateExpressions(Table targetTable, RangeGroup rangeGroup,
                                  int[] columnMap,
                                  Expression[] colExpressions,
                                  RangeGroup[] rangeGroups) {

        HsqlList unresolved           = null;
        int      enforcedDefaultIndex = -1;

        if (targetTable.hasIdentityColumn()
                && targetTable.identitySequence.isAlways()) {
            enforcedDefaultIndex = targetTable.getIdentityColumnIndex();
        }

        for (int i = 0, ix = 0; i < columnMap.length; ix++) {
            Expression expr = colExpressions[ix];
            Expression e;

            // no generated column can be updated
            if (targetTable.colGenerated[columnMap[i]]) {
                throw Error.error(ErrorCode.X_42513);
            }

            if (expr.getType() == OpTypes.ROW) {
                Expression[] elements = expr.nodes;

                for (int j = 0; j < elements.length; j++, i++) {
                    e = elements[j];

                    if (enforcedDefaultIndex == columnMap[i]) {
                        if (e.getType() != OpTypes.DEFAULT) {
                            throw Error.error(ErrorCode.X_42541);
                        }
                    }

                    if (e.isUnresolvedParam()) {
                        e.setAttributesAsColumn(
                            targetTable.getColumn(columnMap[i]), true);
                    } else if (e.getType() == OpTypes.DEFAULT) {

                        //
                    } else {
                        unresolved = expr.resolveColumnReferences(session,
                                rangeGroup, rangeGroups, null);

                        ExpressionColumn.checkColumnsResolved(unresolved);

                        unresolved = null;

                        e.resolveTypes(session, null);
                    }
                }
            } else if (expr.getType() == OpTypes.ROW_SUBQUERY) {
                unresolved = expr.resolveColumnReferences(session, rangeGroup,
                        rangeGroups, null);

                ExpressionColumn.checkColumnsResolved(unresolved);
                expr.resolveTypes(session, null);

                int count = expr.table.queryExpression.getColumnCount();

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

                if (e.isUnresolvedParam()) {
                    e.setAttributesAsColumn(
                        targetTable.getColumn(columnMap[i]), true);
                } else if (e.getType() == OpTypes.DEFAULT) {

                    //
                } else {
                    unresolved = expr.resolveColumnReferences(session,
                            rangeGroup, rangeGroups, null);

                    ExpressionColumn.checkColumnsResolved(unresolved);
                    e.resolveTypes(session, null);
                }

                i++;
            }
        }
    }

    void readSetClauseList(RangeVariable[] rangeVars, OrderedHashSet targets,
                           LongDeque colIndexList, HsqlArrayList expressions) {

        while (true) {
            int degree;

            if (token.tokenType == Tokens.OPENBRACKET) {
                read();

                int oldCount = targets.size();

                readTargetSpecificationList(targets, rangeVars, colIndexList);

                degree = targets.size() - oldCount;

                readThis(Tokens.CLOSEBRACKET);
            } else {
                Expression target = XreadTargetSpecification(rangeVars,
                    colIndexList);

                if (!targets.add(target)) {
                    ColumnSchema col = target.getColumn();

                    throw Error.error(ErrorCode.X_42579, col.getName().name);
                }

                degree = 1;
            }

            readThis(Tokens.EQUALS);

            int position = getPosition();
            int brackets = readOpenBrackets();

            if (token.tokenType == Tokens.SELECT) {
                rewind(position);

                TableDerived td = XreadSubqueryTableBody(OpTypes.ROW_SUBQUERY);

                if (degree != td.queryExpression.getColumnCount()) {
                    throw Error.error(ErrorCode.X_42546);
                }

                Expression e = new Expression(OpTypes.ROW_SUBQUERY, td);

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

    void readGetClauseList(RangeVariable[] rangeVars, OrderedHashSet targets,
                           LongDeque colIndexList, HsqlArrayList expressions) {

        while (true) {
            Expression target = XreadTargetSpecification(rangeVars,
                colIndexList);

            if (!targets.add(target)) {
                ColumnSchema col = target.getColumn();

                throw Error.error(ErrorCode.X_42579, col.getName().name);
            }

            readThis(Tokens.EQUALS);

            switch (token.tokenType) {

                case Tokens.ROW_COUNT :
                case Tokens.MORE :
                    int columnIndex =
                        ExpressionColumn.diagnosticsList.getIndex(
                            token.tokenString);
                    Expression e =
                        new ExpressionColumn(OpTypes.DIAGNOSTICS_VARIABLE,
                                             columnIndex);

                    expressions.add(e);
                    read();
                    break;
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
    StatementDMQL compileMergeStatement(RangeGroup[] rangeGroups) {

        boolean[]     insertColumnCheckList;
        int[]         insertColumnMap = null;
        int[]         updateColumnMap = null;
        int[]         baseUpdateColumnMap;
        Table         table;
        RangeVariable targetRange;
        RangeVariable sourceRange;
        Expression    mergeCondition;
        Expression[]  targets           = null;
        HsqlArrayList updateList        = new HsqlArrayList();
        Expression[]  updateExpressions = Expression.emptyArray;
        HsqlArrayList insertList        = new HsqlArrayList();
        Expression    insertExpression  = null;

        read();
        readThis(Tokens.INTO);

        targetRange = readRangeVariableForDataChange(StatementTypes.MERGE);
        table       = targetRange.rangeTable;

        readThis(Tokens.USING);
        compileContext.setOuterRanges(rangeGroups);

        sourceRange = readTableOrSubquery();

        RangeVariable[] targetRanges = new RangeVariable[]{ targetRange };

        sourceRange.resolveRangeTable(
            session, new RangeGroupSimple(targetRanges, false), rangeGroups);;
        sourceRange.resolveRangeTableTypes(session, targetRanges);
        compileContext.setOuterRanges(RangeGroup.emptyArray);

        // parse ON search conditions
        readThis(Tokens.ON);

        mergeCondition = XreadBooleanValueExpression();

        RangeVariable[] fullRangeVars   = new RangeVariable[] {
            sourceRange, targetRange
        };
        RangeVariable[] sourceRangeVars = new RangeVariable[]{ sourceRange };
        RangeVariable[] targetRangeVars = new RangeVariable[]{ targetRange };
        RangeGroup fullRangeGroup = new RangeGroupSimple(fullRangeVars, false);
        RangeGroup sourceRangeGroup = new RangeGroupSimple(sourceRangeVars,
            false);

        // parse WHEN clause(s) and convert lists to arrays
        insertColumnMap       = table.getColumnMap();
        insertColumnCheckList = table.getNewColumnCheckList();

        OrderedHashSet updateTargetSet    = new OrderedHashSet();
        OrderedHashSet insertColNames     = new OrderedHashSet();
        LongDeque      updateColIndexList = new LongDeque();

        readMergeWhen(updateColIndexList, insertColNames, updateTargetSet,
                      insertList, updateList, targetRangeVars, sourceRange);

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
            targets = new Expression[updateTargetSet.size()];

            updateTargetSet.toArray(targets);

            for (int i = 0; i < targets.length; i++) {
                this.resolveOuterReferencesAndTypes(rangeGroups, targets[i]);
            }

            updateExpressions = new Expression[updateList.size()];

            updateList.toArray(updateExpressions);

            updateColumnMap = new int[updateColIndexList.size()];

            updateColIndexList.toArray(updateColumnMap);
        }

        if (updateExpressions.length != 0) {
            Table baseTable = table.isTriggerUpdatable() ? table
                                                         : table.getBaseTable();

            baseUpdateColumnMap = updateColumnMap;

            if (table != baseTable) {
                baseUpdateColumnMap = new int[updateColumnMap.length];

                ArrayUtil.projectRow(table.getBaseTableColumnMap(),
                                     updateColumnMap, baseUpdateColumnMap);
            }

            resolveUpdateExpressions(table, fullRangeGroup, updateColumnMap,
                                     updateExpressions, rangeGroups);
        }

        HsqlList unresolved = null;

        unresolved = mergeCondition.resolveColumnReferences(session,
                fullRangeGroup, rangeGroups, null);

        ExpressionColumn.checkColumnsResolved(unresolved);
        mergeCondition.resolveTypes(session, null);

        if (mergeCondition.isUnresolvedParam()) {
            mergeCondition.dataType = Type.SQL_BOOLEAN;
        }

        if (mergeCondition.getDataType() != Type.SQL_BOOLEAN) {
            throw Error.error(ErrorCode.X_42568);
        }

        fullRangeVars[1].addJoinCondition(mergeCondition);

        RangeVariableResolver resolver =
            new RangeVariableResolver(fullRangeVars, null, compileContext,
                                      false);

        resolver.processConditions(session);

        fullRangeVars = resolver.rangeVariables;

        for (int i = 0; i < fullRangeVars.length; i++) {
            fullRangeVars[i].resolveRangeTableTypes(session,
                    RangeVariable.emptyArray);
        }

        if (insertExpression != null) {
            unresolved = insertExpression.resolveColumnReferences(session,
                    sourceRangeGroup, RangeGroup.emptyArray, null);
            unresolved = Expression.resolveColumnSet(session,
                    RangeVariable.emptyArray, rangeGroups, unresolved);

            ExpressionColumn.checkColumnsResolved(unresolved);
            insertExpression.resolveTypes(session, null);
        }

        StatementDMQL cs = new StatementDML(session, targets, sourceRange,
                                            targetRange, fullRangeVars,
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
    private void readMergeWhen(LongDeque updateColIndexList,
                               OrderedHashSet insertColumnNames,
                               OrderedHashSet updateTargetSet,
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
            readSetClauseList(targetRangeVars, updateTargetSet,
                              updateColIndexList, updateExpressions);
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
                boolean withPrefix = database.sqlSyntaxOra;

                readSimpleColumnNames(insertColumnNames, targetRangeVars[0],
                                      withPrefix);

                columnCount = insertColumnNames.size();

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
            readMergeWhen(updateColIndexList, insertColumnNames,
                          updateTargetSet, insertExpressions,
                          updateExpressions, targetRangeVars, sourceRangeVar);
        }
    }

    /**
     * Retrieves a CALL Statement from this parse context.
     */

    // to do call argument name and type resolution
    StatementDMQL compileCallStatement(RangeGroup[] rangeGroups,
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

                HsqlArrayList list    = new HsqlArrayList();
                boolean       bracket = true;

                if (database.sqlSyntaxOra) {
                    bracket = readIfThis(Tokens.OPENBRACKET);
                } else {
                    readThis(Tokens.OPENBRACKET);
                }

                if (bracket) {
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
                }

                Expression[] arguments = new Expression[list.size()];

                list.toArray(arguments);

                Routine routine =
                    routineSchema.getSpecificRoutine(arguments.length);

                compileContext.addProcedureCall(routine);

                HsqlList unresolved = null;

                for (int i = 0; i < arguments.length; i++) {
                    Expression e = arguments[i];

                    if (e.isUnresolvedParam()) {
                        e.setAttributesAsColumn(
                            routine.getParameter(i),
                            routine.getParameter(i).isWriteable());
                    } else {
                        int paramMode =
                            routine.getParameter(i).getParameterMode();

                        unresolved =
                            arguments[i].resolveColumnReferences(session,
                                RangeGroup.emptyGroup, rangeGroups,
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

                    if (!routine.getParameter(
                            i).getDataType().canBeAssignedFrom(
                            arguments[i].getDataType())) {
                        throw Error.error(ErrorCode.X_42561);
                    }
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
        HsqlList unresolved = expression.resolveColumnReferences(session,
            RangeGroup.emptyGroup, rangeGroups, null);

        ExpressionColumn.checkColumnsResolved(unresolved);
        expression.resolveTypes(session, null);

        StatementDMQL cs = new StatementProcedure(session, expression,
            compileContext);

        return cs;
    }

    void resolveOuterReferencesAndTypes(RangeGroup[] rangeGroups,
                                        Expression e) {

        HsqlList unresolved = e.resolveColumnReferences(session,
            RangeGroup.emptyGroup, 0, rangeGroups, null, false);

        ExpressionColumn.checkColumnsResolved(unresolved);
        e.resolveTypes(session, null);
    }
    /************************* Volt DB Extensions *************************/
    /**
     * This is a Volt extension to allow
     *   DELETE FROM tab ORDER BY c LIMIT 1
     * Adds a SortAndSlice object to the statement if the next tokens
     * are ORDER BY, LIMIT or OFFSET.
     * @param deleteStmt
     */
    private SortAndSlice voltGetSortAndSliceForDelete(Session session,
            RangeGroup[] rangeGroups, RangeVariable[] rangeVariables) {
        SortAndSlice sas = XreadOrderByExpression();
        if (sas == null || sas == SortAndSlice.noSort)
            return SortAndSlice.noSort;

        // Resolve columns in the ORDER BY clause.  This code modified
        // from how compileDeleteStatement resolves columns in its WHERE clause
        RangeGroup rangeGroup = new RangeGroupSimple(rangeVariables,
                false);
        for (int i = 0; i < sas.exprList.size(); ++i) {
            Expression e = (Expression)sas.exprList.get(i);
            HsqlList unresolved = e.resolveColumnReferences(session,
                rangeGroup, rangeGroups, null);

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
