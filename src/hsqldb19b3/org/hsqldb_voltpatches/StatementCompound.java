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
import org.hsqldb_voltpatches.lib.HashMappedList;
import org.hsqldb_voltpatches.lib.HashSet;
import org.hsqldb_voltpatches.lib.OrderedIntHashSet;
import org.hsqldb_voltpatches.result.Result;
import org.hsqldb_voltpatches.result.ResultConstants;

/**
 * Implementation of Statement for PSM compound statements.

 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 1.9.0
 * @since 1.9.0
 */
public class StatementCompound extends Statement {

    final boolean      isLoop;
    HsqlName           label;
    StatementHandler[] handlers = StatementHandler.emptyExceptionHandlerArray;
    Statement          loopCursor;
    Statement[]        statements;
    StatementSimple    condition;
    boolean            isAtomic;

    //
    ColumnSchema[]  variables = ColumnSchema.emptyArray;
    HashMappedList  scopeVariables;
    RangeVariable[] rangeVariables = RangeVariable.emptyArray;

    //
    public static final StatementCompound[] emptyStatementArray =
        new StatementCompound[]{};

    StatementCompound(int type, HsqlName label) {

        super(type, StatementTypes.X_SQL_CONTROL);

        this.label             = label;
        isTransactionStatement = false;

        switch (type) {

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
                throw Error.runtimeError(ErrorCode.U_S0500, "");
        }
    }

    public String getSQL() {

/*
        StringBuffer sb = new StringBuffer();

        if (label != null) {
            sb.append(label.getStatementName()).append(':').append(' ');
        }

        switch (type) {

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
                        sb.append(variables[i].getDefaultDDL());
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

        for (int i = 0; i < declarations.length; i++) {
            if (declarations[i] instanceof ColumnSchema) {
                varCount++;
            } else {
                handlerCount++;
            }
        }

        variables    = new ColumnSchema[varCount];
        handlers     = new StatementHandler[handlerCount];
        varCount     = 0;
        handlerCount = 0;

        for (int i = 0; i < declarations.length; i++) {
            if (declarations[i] instanceof ColumnSchema) {
                variables[varCount++] = (ColumnSchema) declarations[i];
            } else {
                StatementHandler handler = (StatementHandler) declarations[i];

                handler.setParent(this);

                handlers[handlerCount++] = handler;
            }
        }

        setVariables();
        setHandlers();
    }

    public void setLoopStatement(Statement cursorStatement) {
        loopCursor = cursorStatement;
    }

    void setStatements(Statement[] statements) {

        for (int i = 0; i < statements.length; i++) {
            statements[i].setParent(this);
        }

        this.statements = statements;
    }

    public void setCondition(StatementSimple condition) {
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
                throw Error.runtimeError(ErrorCode.U_S0500, "");
        }

        if (result.isError()) {
            result.getException().setStatementType(group, type);
        }

        return result;
    }

    private Result executeBlock(Session session) {

        Result result = Result.updateZeroResult;
        int    i      = 0;

        session.sessionContext.push();

        for (; i < statements.length; i++) {
            result = statements[i].execute(session);
            result = handleCondition(session, result);

            if (result.isError()) {
                break;
            }

            if (result.getType() == ResultConstants.VALUE) {
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

        session.sessionContext.pop();

        return result;
    }

    private Result handleCondition(Session session, Result result) {

        String sqlState = null;

        if (result.isError()) {
            sqlState = result.getSubString();
        } else if (session.getLastWarnings() != null) {
            sqlState = session.getLastWarnings().getSQLState();
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
                if (handler.handlesCondition(result.getSubString())) {
                    session.resetSchema();

                    switch (handler.handlerType) {

                        case StatementHandler.CONTINUE :
                            result = Result.updateZeroResult;
                            break;

                        case StatementHandler.UNDO :
                            session.rollbackToSavepoint();

                            result = Result.newPSMResult(StatementTypes.LEAVE,
                                                         null, null);
                            break;

                        case StatementHandler.EXIT :
                            result = Result.newPSMResult(StatementTypes.LEAVE,
                                                         null, null);
                            break;
                    }

                    Result actionResult = handler.statement.execute(session);

                    if (actionResult.isError()) {
                        result = actionResult;

                        handleCondition(session, result);
                    } else {
                        return result;
                    }
                }
            }

            if (parent != null) {

                // unhandled exception condition
                return parent.handleCondition(session, result);
            }
        }

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
                result = statements[i].execute(session);

                if (result.isError()) {
                    break;
                }

                if (result.getType() == ResultConstants.VALUE) {
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

                if (result.getErrorCode() == StatementTypes.RETURN) {
                    break;
                }
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

                result = statements[i].execute(session);

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

            result = statements[i].execute(session);

            if (result.isError()) {
                break;
            }

            if (result.getType() == ResultConstants.VALUE) {
                break;
            }
        }

        return result;
    }

    public void resolve() {

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
            statements[i].resolve();
        }
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

    public void setAtomic(boolean atomic) {
        this.isAtomic = atomic;
    }

    //
    private void setVariables() {

        if (variables.length == 0) {
            if (parent == null) {
                rangeVariables = root.getParameterRangeVariables();
            } else {
                rangeVariables = parent.rangeVariables;
            }

            return;
        }

        HashMappedList list = new HashMappedList();

        if (parent != null) {
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

        RangeVariable range = new RangeVariable(list, true);

        rangeVariables     = new RangeVariable[] {
            root.getParameterRangeVariables()[0], range
        };
        root.variableCount = list.size();
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

    public RangeVariable[] getRangeVariables() {
        return rangeVariables;
    }
}
