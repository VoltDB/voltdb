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
import org.hsqldb_voltpatches.result.Result;
import org.hsqldb_voltpatches.result.ResultMetaData;
import org.hsqldb_voltpatches.store.ValuePool;

/**
 * Implementation of Statement for simple PSM control statements.
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 1.9.0
 * @since 1.9.0
 */
public class StatementSimple extends Statement {

    String     sqlState;
    HsqlName   label;
    Expression expression;

    //
    ColumnSchema[] variables;
    int[]          variableIndexes;

    /**
     * for RETURN and flow control
     */
    StatementSimple(int type, Expression expression) {

        super(type, StatementTypes.X_SQL_CONTROL);

        isTransactionStatement = false;
        this.expression        = expression;
    }

    StatementSimple(int type, HsqlName label) {

        super(type, StatementTypes.X_SQL_CONTROL);

        isTransactionStatement = false;
        this.label             = label;
    }

    StatementSimple(int type, String sqlState) {

        super(type, StatementTypes.X_SQL_CONTROL);

        isTransactionStatement = false;
        this.sqlState          = sqlState;
    }

    StatementSimple(int type, ColumnSchema[] variables, Expression e,
                    int[] indexes) {

        super(type, StatementTypes.X_SQL_CONTROL);

        isTransactionStatement = false;
        this.expression        = e;
        this.variables         = variables;
        variableIndexes        = indexes;
    }

    public String getSQL() {

        StringBuffer sb = new StringBuffer();

        switch (type) {

            /** @todo 1.9.0 - add the exception */
            case StatementTypes.SIGNAL :
                sb.append(Tokens.T_SIGNAL);
                break;

            case StatementTypes.RESIGNAL :
                sb.append(Tokens.T_RESIGNAL);
                break;

            case StatementTypes.ITERATE :
                sb.append(Tokens.T_ITERATE).append(' ').append(label);
                break;

            case StatementTypes.LEAVE :
                sb.append(Tokens.T_LEAVE).append(' ').append(label);
                break;

            case StatementTypes.RETURN :
/*
                sb.append(Tokens.T_RETURN);

                if (expression != null) {
                    sb.append(' ').append(expression.getSQL());
                }
                break;
*/
                return sql;

            case StatementTypes.CONDITION :
                sb.append(expression.getSQL());
                break;

            case StatementTypes.ASSIGNMENT :

                /** @todo - cover row assignment */
                sb.append(Tokens.T_SET).append(' ');
                sb.append(variables[0].getName().statementName).append(' ');
                sb.append('=').append(' ').append(expression.getSQL());
                break;
        }

        return sb.toString();
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

    public Result execute(Session session) {

        Result result = getResult(session);

        if (result.isError()) {
            result.getException().setStatementType(group, type);
        }

        return result;
    }

    Result getResult(Session session) {

        switch (type) {

            /** @todo - check sqlState against allowed values */
            case StatementTypes.SIGNAL :
            case StatementTypes.RESIGNAL :
                HsqlException ex = Error.error("sql routine error", sqlState,
                                               -1);

                return Result.newErrorResult(ex);

            case StatementTypes.ITERATE :
            case StatementTypes.LEAVE :
            case StatementTypes.RETURN :
            case StatementTypes.CONDITION :
                return this.getResultValue(session);

            case StatementTypes.ASSIGNMENT : {
                try {
                    performAssignment(session);

                    return Result.updateZeroResult;
                } catch (HsqlException e) {
                    return Result.newErrorResult(e);
                }
            }
            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "");
        }
    }

    void performAssignment(Session session) {

        Object[] values;

        if (expression.getType() == OpTypes.ROW) {
            values = expression.getRowValue(session);
        } else if (expression.getType() == OpTypes.TABLE_SUBQUERY) {
            values = expression.subQuery.queryExpression.getSingleRowValues(
                session);

            if (values == null) {
                return;
            }
        } else {
            values = new Object[1];
            values[0] = expression.getValue(session,
                                            variables[0].getDataType());
        }

        for (int j = 0; j < values.length; j++) {
            Object[] data = ValuePool.emptyObjectArray;

            switch (variables[j].getType()) {

                case SchemaObject.PARAMETER :
                    data = session.sessionContext.routineArguments;
                    break;

                case SchemaObject.VARIABLE :
                    data = session.sessionContext.routineVariables;
                    break;
            }

            int colIndex = variableIndexes[j];

            data[colIndex] =
                variables[j].getDataType().convertToDefaultType(session,
                    values[j]);
        }
    }

    public void resolve() {

        boolean resolved = false;

        switch (type) {

            case StatementTypes.SIGNAL :
            case StatementTypes.RESIGNAL :
                resolved = true;
                break;

            case StatementTypes.RETURN :
                if (root.isProcedure()) {
                    throw Error.error(ErrorCode.X_42602);
                }

                resolved = true;
                break;

            case StatementTypes.ITERATE : {
                StatementCompound statement = parent;

                while (statement != null) {
                    if (statement.isLoop) {
                        if (label == null) {
                            resolved = true;

                            break;
                        }

                        if (statement.label != null
                                && label.name.equals(statement.label.name)) {
                            resolved = true;

                            break;
                        }
                    }

                    statement = statement.parent;
                }

                break;
            }
            case StatementTypes.LEAVE :
                resolved = true;
                break;

            case StatementTypes.ASSIGNMENT :
                resolved = true;
                break;

            case StatementTypes.CONDITION :
                resolved = true;
                break;

            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "");
        }

        if (!resolved) {
            throw Error.error(ErrorCode.X_42602);
        }
    }

    public void setParent(StatementCompound statement) {
        parent = statement;
    }

    public void setRoot(Routine routine) {
        root = routine;
    }

    public boolean hasGeneratedColumns() {
        return false;
    }

    public String describe(Session session) {
        return "";
    }

    private Result getResultValue(Session session) {

        try {
            Object value = null;

            if (expression != null) {
                value = expression.getValue(session);
            }

            return Result.newPSMResult(type, label == null ? null
                                                           : label
                                                           .name, value);
        } catch (HsqlException e) {
            return Result.newErrorResult(e);
        }
    }
}
