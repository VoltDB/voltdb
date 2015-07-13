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
import org.hsqldb_voltpatches.lib.HashMappedList;
import org.hsqldb_voltpatches.lib.HashSet;
import org.hsqldb_voltpatches.lib.OrderedHashSet;
import org.hsqldb_voltpatches.lib.OrderedIntHashSet;
import org.hsqldb_voltpatches.result.Result;
import org.hsqldb_voltpatches.result.ResultConstants;
import org.hsqldb_voltpatches.types.Type;

/**
 * Implementation of Statement for PSM compound statements.

 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 2.3.0
 * @since 1.9.0
 */
public class StatementCompound extends Statement implements RangeGroup {

    final boolean       isLoop;
    HsqlName            label;
    StatementHandler[]  handlers = StatementHandler.emptyExceptionHandlerArray;
    boolean             hasUndoHandler;
    StatementQuery      loopCursor;
    Statement[]         statements;
    StatementExpression condition;
    boolean             isAtomic;

    //
    ColumnSchema[]    variables      = ColumnSchema.emptyArray;
    StatementCursor[] cursors        = StatementCursor.emptyArray;
    HashMappedList    scopeVariables = new HashMappedList();
    RangeVariable[]   rangeVariables = RangeVariable.emptyArray;
    Table[]           tables         = Table.emptyArray;
    HashMappedList    scopeTables;

    //
    public static final StatementCompound[] emptyStatementArray =
        new StatementCompound[]{};

    StatementCompound(int type, HsqlName label) {

        super(type, StatementTypes.X_SQL_CONTROL);

        this.label             = label;
        isTransactionStatement = false;

        switch (type) {

            case StatementTypes.FOR :
            case StatementTypes.LOOP :
            case StatementTypes.WHILE :
            case StatementTypes.REPEAT :
                isLoop = true;
                break;

            case StatementTypes.BEGIN_END :
            case StatementTypes.IF :
                isLoop = false;
                break;

            default :
                throw Error.runtimeError(ErrorCode.U_S0500,
                                         "StatementCompound");
        }
    }

    public String getSQL() {

/*
        StringBuffer sb = new StringBuffer();

        if (label != null) {
            sb.append(label.getStatementName()).append(':').append(' ');
        }

        switch (type) {
            case StatementTypes.FOR :
                // todo
                break;

            case StatementTypes.LOOP :
                sb.append(Tokens.T_LOOP).append(' ');

                for (int i = 0; i < statements.length; i++) {
                    sb.append(statements[i].getSQL()).append(';');
                }

                sb.append(Tokens.T_END).append(' ').append(Tokens.T_LOOP);
                break;

            case StatementTypes.WHILE :
                sb.append(Tokens.T_WHILE).append(' ');
                sb.append(condition.getSQL()).append(' ').append(Tokens.T_DO);
                sb.append(' ');

                for (int i = 0; i < statements.length; i++) {
                    sb.append(statements[i].getSQL()).append(';');
                }

                sb.append(Tokens.T_END).append(' ').append(Tokens.T_WHILE);
                break;

            case StatementTypes.REPEAT :
                sb.append(Tokens.T_REPEAT).append(' ');

                for (int i = 0; i < statements.length; i++) {
                    sb.append(statements[i].getSQL()).append(';');
                }

                sb.append(Tokens.T_UNTIL).append(' ');
                sb.append(condition.getSQL()).append(' ');
                sb.append(Tokens.T_END).append(' ').append(Tokens.T_REPEAT);
                break;

            case StatementTypes.BEGIN_END :
                sb.append(Tokens.T_BEGIN).append(' ').append(Tokens.T_ATOMIC);
                sb.append(' ');

                for (int i = 0; i < handlers.length; i++) {
                    sb.append(handlers[i].getSQL()).append(';');
                }

                for (int i = 0; i < variables.length; i++) {
                    sb.append(Tokens.T_DECLARE).append(' ');
                    sb.append(variables[i].getSQL());

                    if (variables[i].hasDefault()) {
                        sb.append(' ').append(Tokens.T_DEFAULT).append(' ');
                        sb.append(variables[i].getDefaultSQL());
                    }

                    sb.append(';');
                }

                for (int i = 0; i < statements.length; i++) {
                    sb.append(statements[i].getSQL()).append(';');
                }

                sb.append(Tokens.T_END);
                break;

            case StatementTypes.IF :
                for (int i = 0; i < statements.length; i++) {
                    if (statements[i].type == StatementTypes.CONDITION) {
                        if (i != 0) {
                            sb.append(Tokens.T_ELSE).append(' ');
                        }

                        sb.append(Tokens.T_IF).append(' ');
                        sb.append(statements[i].getSQL()).append(' ');
                        sb.append(Tokens.T_THEN).append(' ');
                    } else {
                        sb.append(statements[i].getSQL()).append(';');
                    }
                }

                sb.append(Tokens.T_END).append(' ').append(Tokens.T_IF);
                break;
        }

        return sb.toString();
*/
        return sql;
    }

    protected String describe(Session session, int blanks) {

        StringBuffer sb = new StringBuffer();

        sb.append('\n');

        for (int i = 0; i < blanks; i++) {
            sb.append(' ');
        }

        sb.append(Tokens.T_STATEMENT);

        return sb.toString();
    }

    public void setLocalDeclarations(Object[] declarations) {

        int varCount     = 0;
        int handlerCount = 0;
        int cursorCount  = 0;
        int tableCount   = 0;

        for (int i = 0; i < declarations.length; i++) {
            if (declarations[i] instanceof ColumnSchema) {
                varCount++;
            } else if (declarations[i] instanceof StatementHandler) {
                handlerCount++;
            } else if (declarations[i] instanceof Table) {
                tableCount++;
            } else {
                cursorCount++;
            }
        }

        if (varCount > 0) {
            variables = new ColumnSchema[varCount];
        }

        if (handlerCount > 0) {
            handlers = new StatementHandler[handlerCount];
        }

        if (tableCount > 0) {
            tables = new Table[tableCount];
        }

        if (cursorCount > 0) {
            cursors = new StatementCursor[cursorCount];
        }

        varCount     = 0;
        handlerCount = 0;
        tableCount   = 0;
        cursorCount  = 0;

        for (int i = 0; i < declarations.length; i++) {
            if (declarations[i] instanceof ColumnSchema) {
                variables[varCount++] = (ColumnSchema) declarations[i];
            } else if (declarations[i] instanceof StatementHandler) {
                StatementHandler handler = (StatementHandler) declarations[i];

                handler.setParent(this);

                handlers[handlerCount++] = handler;

                if (handler.handlerType == StatementHandler.UNDO) {
                    hasUndoHandler = true;
                }
            } else if (declarations[i] instanceof Table) {
                Table table = (Table) declarations[i];

                tables[tableCount++] = table;
            } else {
                StatementCursor cursor = (StatementCursor) declarations[i];

                cursors[cursorCount++] = cursor;
            }
        }

        setVariables();
        setHandlers();
        setTables();
        setCursors();
    }

    public void setLoopStatement(StatementQuery cursorStatement) {

        loopCursor = cursorStatement;

        HsqlName[] colNames =
            cursorStatement.queryExpression.getResultColumnNames();
        Type[] colTypes = cursorStatement.queryExpression.getColumnTypes();
        ColumnSchema[] columns = new ColumnSchema[colNames.length];

        for (int i = 0; i < colNames.length; i++) {
            columns[i] = new ColumnSchema(colNames[i], colTypes[i], false,
                                          false, null);

            columns[i].setParameterMode(SchemaObject.ParameterModes.PARAM_IN);
        }

        setLocalDeclarations(columns);
    }

    void setStatements(Statement[] statements) {

        for (int i = 0; i < statements.length; i++) {
            statements[i].setParent(this);
        }

        this.statements = statements;
    }

    public void setCondition(StatementExpression condition) {
        this.condition = condition;
    }

    public Result execute(Session session) {

        Result result;

        switch (type) {

            case StatementTypes.BEGIN_END : {
                initialiseVariables(session);

                result = executeBlock(session);

                break;
            }
            case StatementTypes.FOR :
                result = executeForLoop(session);
                break;

            case StatementTypes.LOOP :
            case StatementTypes.WHILE :
            case StatementTypes.REPEAT : {
                result = executeLoop(session);

                break;
            }
            case StatementTypes.IF : {
                result = executeIf(session);

                break;
            }
            default :
                throw Error.runtimeError(ErrorCode.U_S0500,
                                         "StatementCompound");
        }

        if (result.isError()) {
            result.getException().setStatementType(group, type);
        }

        return result;
    }

    private Result executeBlock(Session session) {

        Result  result = Result.updateZeroResult;
        boolean push   = !root.isTrigger();

        if (push) {
            session.sessionContext.push();

            if (hasUndoHandler) {
                String name = HsqlNameManager.getAutoSavepointNameString(
                    session.actionTimestamp, session.sessionContext.depth);

                session.savepoint(name);
            }
        }

        for (int i = 0; i < statements.length; i++) {
            result = executeProtected(session, statements[i]);
            result = handleCondition(session, result);

            if (result.isError()) {
                break;
            }

            if (result.getType() == ResultConstants.VALUE) {
                break;
            }

            if (result.getType() == ResultConstants.DATA) {
                break;
            }
        }

        if (result.getType() == ResultConstants.VALUE) {
            if (result.getErrorCode() == StatementTypes.LEAVE) {
                if (result.getMainString() == null) {
                    result = Result.updateZeroResult;
                } else if (label != null
                           && label.name.equals(result.getMainString())) {
                    result = Result.updateZeroResult;
                }
            }
        }

        if (push) {
            session.sessionContext.pop();
        }

        return result;
    }

    private Result handleCondition(Session session, Result result) {

        String sqlState = null;

        if (result.isError()) {
            sqlState = result.getSubString();
        } else if (session.getLastWarning() != null) {
            sqlState = session.getLastWarning().getSQLState();
        } else {
            return result;
        }

        if (sqlState != null) {
            for (int i = 0; i < handlers.length; i++) {
                StatementHandler handler = handlers[i];

                session.clearWarnings();

                /**
                 * @todo - if condition is "transaction rollback" promote to
                 * top call level without any further action
                 * if condition is system related promote to top level
                 * schema manipulation conditions are never handled
                 */
                if (handler.handlesCondition(sqlState)) {
                    String labelString = label == null ? null
                                                       : label.name;

                    switch (handler.handlerType) {

                        case StatementHandler.CONTINUE :
                            result = Result.updateZeroResult;
                            break;

                        case StatementHandler.UNDO :
                            session.rollbackToSavepoint();

                            result = Result.newPSMResult(StatementTypes.LEAVE,
                                                         labelString, null);
                            break;

                        case StatementHandler.EXIT :
                            result = Result.newPSMResult(StatementTypes.LEAVE,
                                                         labelString, null);
                            break;
                    }

                    Result actionResult = executeProtected(session,
                                                           handler.statement);

                    if (actionResult.isError()) {
                        result = actionResult;

                        // parent should handle this
                    } else if (actionResult.getType()
                               == ResultConstants.VALUE) {
                        result = actionResult;
                    }
                }
            }

            if (result.isError() && parent != null) {

                // unhandled exception condition
                return parent.handleCondition(session, result);
            }
        }

        return result;
    }

    private Result executeForLoop(Session session) {

        Result queryResult = loopCursor.execute(session);

        if (queryResult.isError()) {
            return queryResult;
        }

        Result result = Result.updateZeroResult;

        while (queryResult.navigator.hasNext()) {
            queryResult.navigator.next();

            Object[] data = queryResult.navigator.getCurrent();

            initialiseVariables(session, data,
                                queryResult.metaData.getColumnCount());

            for (int i = 0; i < statements.length; i++) {
                result = executeProtected(session, statements[i]);
                result = handleCondition(session, result);

                if (result.isError()) {
                    break;
                }

                if (result.getType() == ResultConstants.VALUE) {
                    break;
                }

                if (result.getType() == ResultConstants.DATA) {
                    break;
                }
            }

            if (result.isError()) {
                break;
            }

            if (result.getType() == ResultConstants.VALUE) {
                if (result.getErrorCode() == StatementTypes.ITERATE) {
                    if (result.getMainString() == null) {
                        continue;
                    }

                    if (label != null
                            && label.name.equals(result.getMainString())) {
                        continue;
                    }

                    break;
                }

                if (result.getErrorCode() == StatementTypes.LEAVE) {
                    break;
                }

                // return
                break;
            }

            if (result.getType() == ResultConstants.DATA) {
                break;
            }
        }

        queryResult.navigator.release();

        return result;
    }

    private Result executeLoop(Session session) {

        Result result = Result.updateZeroResult;

        while (true) {
            if (type == StatementTypes.WHILE) {
                result = condition.execute(session);

                if (result.isError()) {
                    break;
                }

                if (!Boolean.TRUE.equals(result.getValueObject())) {
                    result = Result.updateZeroResult;

                    break;
                }
            }

            for (int i = 0; i < statements.length; i++) {
                result = executeProtected(session, statements[i]);
                result = handleCondition(session, result);

                if (result.getType() == ResultConstants.VALUE) {
                    break;
                }

                if (result.getType() == ResultConstants.DATA) {
                    break;
                }
            }

            if (result.isError()) {
                break;
            }

            if (result.getType() == ResultConstants.VALUE) {
                if (result.getErrorCode() == StatementTypes.ITERATE) {
                    if (result.getMainString() == null) {
                        continue;
                    }

                    if (label != null
                            && label.name.equals(result.getMainString())) {
                        continue;
                    }

                    break;
                }

                if (result.getErrorCode() == StatementTypes.LEAVE) {
                    if (result.getMainString() == null) {
                        result = Result.updateZeroResult;
                    }

                    if (label != null
                            && label.name.equals(result.getMainString())) {
                        result = Result.updateZeroResult;
                    }

                    break;
                }

                // return
                break;
            }

            if (result.getType() == ResultConstants.DATA) {
                break;
            }

            if (type == StatementTypes.REPEAT) {
                result = condition.execute(session);

                if (result.isError()) {
                    break;
                }

                if (Boolean.TRUE.equals(result.getValueObject())) {
                    result = Result.updateZeroResult;

                    break;
                }
            }
        }

        return result;
    }

    private Result executeIf(Session session) {

        Result  result  = Result.updateZeroResult;
        boolean execute = false;

        for (int i = 0; i < statements.length; i++) {
            if (statements[i].getType() == StatementTypes.CONDITION) {
                if (execute) {
                    break;
                }

                result = executeProtected(session, statements[i]);

                if (result.isError()) {
                    break;
                }

                Object value = result.getValueObject();

                execute = Boolean.TRUE.equals(value);

                i++;
            }

            result = Result.updateZeroResult;

            if (!execute) {
                continue;
            }

            result = executeProtected(session, statements[i]);
            result = handleCondition(session, result);

            if (result.isError()) {
                break;
            }

            if (result.getType() == ResultConstants.VALUE) {
                break;
            }
        }

        return result;
    }

    private Result executeProtected(Session session, Statement statement) {

        int actionIndex = session.rowActionList.size();

        session.actionTimestamp =
            session.database.txManager.getNextGlobalChangeTimestamp();

        Result result = statement.execute(session);

        if (result.isError()) {
            session.rollbackAction(actionIndex, session.actionTimestamp);
        }

        return result;
    }

    public void resolve(Session session) {

        for (int i = 0; i < statements.length; i++) {
            if (statements[i].getType() == StatementTypes.LEAVE
                    || statements[i].getType() == StatementTypes.ITERATE) {
                if (!findLabel((StatementSimple) statements[i])) {
                    throw Error.error(
                        ErrorCode.X_42508,
                        ((StatementSimple) statements[i]).label.name);
                }

                continue;
            }

            if (statements[i].getType() == StatementTypes.RETURN) {
                if (!root.isFunction()) {
                    throw Error.error(ErrorCode.X_42602, Tokens.T_RETURN);
                }
            }
        }

        for (int i = 0; i < statements.length; i++) {
            statements[i].resolve(session);
        }

        for (int i = 0; i < handlers.length; i++) {
            handlers[i].resolve(session);
        }

        OrderedHashSet writeTableNamesSet = new OrderedHashSet();
        OrderedHashSet readTableNamesSet  = new OrderedHashSet();
        OrderedHashSet set                = new OrderedHashSet();

        for (int i = 0; i < variables.length; i++) {
            OrderedHashSet refs = variables[i].getReferences();

            if (refs != null) {
                set.addAll(refs);
            }
        }

        if (condition != null) {
            set.addAll(condition.getReferences());
            readTableNamesSet.addAll(condition.getTableNamesForRead());
        }

        for (int i = 0; i < statements.length; i++) {
            set.addAll(statements[i].getReferences());
            readTableNamesSet.addAll(statements[i].getTableNamesForRead());
            writeTableNamesSet.addAll(statements[i].getTableNamesForWrite());
        }

        for (int i = 0; i < handlers.length; i++) {
            set.addAll(handlers[i].getReferences());
            readTableNamesSet.addAll(handlers[i].getTableNamesForRead());
            writeTableNamesSet.addAll(handlers[i].getTableNamesForWrite());
        }

        readTableNamesSet.removeAll(writeTableNamesSet);

        readTableNames = new HsqlName[readTableNamesSet.size()];

        readTableNamesSet.toArray(readTableNames);

        writeTableNames = new HsqlName[writeTableNamesSet.size()];

        writeTableNamesSet.toArray(writeTableNames);

        references = set;
    }

    public void setRoot(Routine routine) {

        root = routine;
/*
        if (condition != null) {
            condition.setRoot(routine);
        }

        for (int i = 0; i < statements.length; i++) {
            statements[i].setRoot(routine);
        }
*/
    }

    public String describe(Session session) {
        return "";
    }

    public OrderedHashSet getReferences() {
        return references;
    }

    public void setAtomic(boolean atomic) {
        this.isAtomic = atomic;
    }

    //
    private void setVariables() {

        HashMappedList list = new HashMappedList();

        if (variables.length == 0) {
            if (parent == null) {
                rangeVariables = root.getRangeVariables();
            } else {
                rangeVariables = parent.rangeVariables;
            }

            scopeVariables = list;

            return;
        }

        if (parent != null && parent.scopeVariables != null) {
            for (int i = 0; i < parent.scopeVariables.size(); i++) {
                list.add(parent.scopeVariables.getKey(i),
                         parent.scopeVariables.get(i));
            }
        }

        for (int i = 0; i < variables.length; i++) {
            String  name  = variables[i].getName().name;
            boolean added = list.add(name, variables[i]);

            if (!added) {
                throw Error.error(ErrorCode.X_42606, name);
            }

            if (root.getParameterIndex(name) != -1) {
                throw Error.error(ErrorCode.X_42606, name);
            }
        }

        scopeVariables = list;

        RangeVariable[] parameterRangeVariables = root.getRangeVariables();
        RangeVariable range = new RangeVariable(list, null, true,
            RangeVariable.VARIALBE_RANGE);

        rangeVariables = new RangeVariable[parameterRangeVariables.length + 1];

        for (int i = 0; i < parameterRangeVariables.length; i++) {
            rangeVariables[i] = parameterRangeVariables[i];
        }

        rangeVariables[parameterRangeVariables.length] = range;
        root.variableCount                             = list.size();
    }

    private void setHandlers() {

        if (handlers.length == 0) {
            return;
        }

        HashSet           statesSet = new HashSet();
        OrderedIntHashSet typesSet  = new OrderedIntHashSet();

        for (int i = 0; i < handlers.length; i++) {
            int[] types = handlers[i].getConditionTypes();

            for (int j = 0; j < types.length; j++) {
                if (!typesSet.add(types[j])) {
                    throw Error.error(ErrorCode.X_42601);
                }
            }

            String[] states = handlers[i].getConditionStates();

            for (int j = 0; j < states.length; j++) {
                if (!statesSet.add(states[j])) {
                    throw Error.error(ErrorCode.X_42601);
                }
            }
        }
    }

    private void setTables() {

        if (tables.length == 0) {
            return;
        }

        HashMappedList list = new HashMappedList();

        if (parent != null && parent.scopeTables != null) {
            for (int i = 0; i < parent.scopeTables.size(); i++) {
                list.add(parent.scopeTables.getKey(i),
                         parent.scopeTables.get(i));
            }
        }

        for (int i = 0; i < tables.length; i++) {
            String  name  = tables[i].getName().name;
            boolean added = list.add(name, tables[i]);

            if (!added) {
                throw Error.error(ErrorCode.X_42606, name);
            }
        }

        scopeTables = list;
    }

    private void setCursors() {

        if (cursors.length == 0) {
            return;
        }

        HashSet list = new HashSet();

        for (int i = 0; i < cursors.length; i++) {
            StatementCursor cursor = cursors[i];
            boolean         added  = list.add(cursor.getCursorName().name);

            if (!added) {
                throw Error.error(ErrorCode.X_42606,
                                  cursor.getCursorName().name);
            }
        }
    }

    private boolean findLabel(StatementSimple statement) {

        if (label != null && statement.label.name.equals(label.name)) {
            if (!isLoop && statement.getType() == StatementTypes.ITERATE) {
                return false;
            }

            return true;
        }

        if (parent == null) {
            return false;
        }

        return parent.findLabel(statement);
    }

    private void initialiseVariables(Session session) {

        Object[] vars   = session.sessionContext.routineVariables;
        int      offset = parent == null ? 0
                                         : parent.scopeVariables.size();

        for (int i = 0; i < variables.length; i++) {
            try {
                vars[offset + i] = variables[i].getDefaultValue(session);
            } catch (HsqlException e) {}
        }
    }

    private void initialiseVariables(Session session, Object[] data,
                                     int count) {

        Object[] vars   = session.sessionContext.routineVariables;
        int      offset = parent == null ? 0
                                         : parent.scopeVariables.size();

        for (int i = 0; i < count; i++) {
            try {
                vars[offset + i] = data[i];
            } catch (HsqlException e) {}
        }
    }

    public RangeVariable[] getRangeVariables() {
        return rangeVariables;
    }

    public void setCorrelated() {

        //
    }

    public boolean isVariable() {
        return true;
    }

    // A VoltDB extension to print HSQLDB ASTs
    public String voltDescribe(Session session, int blanks) {
        return "";
    }
    // End of VoltDB extension
}
