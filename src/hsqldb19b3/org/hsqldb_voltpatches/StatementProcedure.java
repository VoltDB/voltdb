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
import org.hsqldb_voltpatches.lib.OrderedHashSet;
import org.hsqldb_voltpatches.result.Result;
import org.hsqldb_voltpatches.result.ResultMetaData;
import org.hsqldb_voltpatches.store.ValuePool;

/**
 * Implementation of Statement for callable procedures.<p>
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 1.9.0
 * @since 1.9.0
 */
public class StatementProcedure extends StatementDMQL {

    /** Expression to evaluate */
    Expression expression;

    /** Routine to execute */
    Routine procedure;

    /** arguments to Routine */
    Expression[]   arguments;
    ResultMetaData resultMetaData;

    /**
     * Constructor for CALL statements for expressions.
     */
    StatementProcedure(Session session, Expression expression,
                       CompileContext compileContext) {

        super(StatementTypes.CALL, StatementTypes.X_SQL_DATA,
              session.currentSchema);

        this.expression = expression;

        setDatabaseObjects(compileContext);
        checkAccessRights(session);
    }

    /**
     * Constructor for CALL statements for procedures.
     */
    StatementProcedure(Session session, Routine procedure,
                       Expression[] arguments, CompileContext compileContext) {

        super(StatementTypes.CALL, StatementTypes.X_SQL_DATA,
              session.currentSchema);

        this.procedure = procedure;
        this.arguments = arguments;

        setDatabaseObjects(compileContext);
        checkAccessRights(session);
    }

    Result getResult(Session session) {
        return expression == null ? getProcedureResult(session)
                                  : getExpressionResult(session);
    }

    Result getProcedureResult(Session session) {

        Object[] data = ValuePool.emptyObjectArray;

        if (arguments.length > 0) {
            data = new Object[arguments.length];
        }

        for (int i = 0; i < arguments.length; i++) {
            Expression e = arguments[i];

            if (e != null) {
                data[i] = e.getValue(session, e.dataType);
            }
        }

        int variableCount = procedure.getVariableCount();

        session.sessionContext.push();

        session.sessionContext.routineArguments = data;
        session.sessionContext.routineVariables = ValuePool.emptyObjectArray;

        if (variableCount > 0) {
            session.sessionContext.routineVariables =
                new Object[variableCount];
        }

        // fixed? temp until assignment of dynamicArguments in materialiseSubqueries is fixed
//        Object[] args   = session.sessionContext.dynamicArguments;
        Result result = procedure.statement.execute(session);

//        session.sessionContext.dynamicArguments = args;
        if (!result.isError()) {
            result = Result.updateZeroResult;
        }

        Object[] callArguments = session.sessionContext.routineArguments;

        session.sessionContext.pop();

        if (result.isError()) {
            return result;
        }

        boolean returnParams = false;

        for (int i = 0; i < procedure.getParameterCount(); i++) {
            ColumnSchema param = procedure.getParameter(i);
            int          mode  = param.getParameterMode();

            if (mode != SchemaObject.ParameterModes.PARAM_IN) {
                if (this.arguments[i].isParam) {
                    int paramIndex = arguments[i].parameterIndex;

                    session.sessionContext.dynamicArguments[paramIndex] =
                        callArguments[i];
                    returnParams = true;
                } else {
                    int varIndex = arguments[i].getColumnIndex();

                    session.sessionContext.routineVariables[varIndex] =
                        callArguments[i];
                }
            }
        }

        if (returnParams) {
            result = Result.newCallResponse(
                this.getParametersMetaData().getParameterTypes(), this.id,
                session.sessionContext.dynamicArguments);
        }

        return result;
    }

    Result getExpressionResult(Session session) {

        Expression e = expression;             // representing CALL
        Object     o = e.getValue(session);    // expression return value
        Result     r;

        if (o instanceof Result) {
            return (Result) o;
        }

        if (resultMetaData == null) {
            getResultMetaData();
        }

        /**
         * @todo 1.9.0 For table functions implment handling of Result objects
         * returned from Java functions. Review and document instantiation and usage
         * of relevant implementation of Result and JDBCResultSet for returning
         * from Java functions?
         * else if (o instanceof JDBCResultSet) {
         *   return ((JDBCResultSet) o).getResult();
         * }
         */
        r = Result.newSingleColumnResult(resultMetaData);

        Object[] row = new Object[1];

        row[0] = o;

        r.getNavigator().add(row);

        return r;
    }

    public ResultMetaData getResultMetaData() {

        if (resultMetaData != null) {
            return resultMetaData;
        }

        switch (type) {

            case StatementTypes.CALL : {
                if (expression == null) {
                    return ResultMetaData.emptyResultMetaData;
                }

                // TODO:
                //
                // 1.) standard to register metadata for columns of
                // the primary result set, if any, generated by call
                //
                // 2.) Represent the return value, if any (which is
                // not, in truth, a result set), as an OUT parameter
                //
                // For now, I've reverted a bunch of code I had in place
                // and instead simply reflect things as the are, describing
                // a single column result set that communicates
                // the return value.  If the expression generating the
                // return value has a void return type, a result set
                // is described whose single column is of type NULL
                ResultMetaData md = ResultMetaData.newResultMetaData(1);
                ColumnBase column =
                    new ColumnBase(null, null, null,
                                   StatementDMQL.RETURN_COLUMN_NAME);

                column.setType(expression.getDataType());

                md.columns[0] = column;

                md.prepareData();

                resultMetaData = md;

                return md;
            }
            default :
                throw Error.runtimeError(
                    ErrorCode.U_S0500,
                    "CompiledStatement.getResultMetaData()");
        }
    }

    /**
     * Returns the metadata for the placeholder parameters.
     */
    public ResultMetaData getParametersMetaData() {

        /** @todo - change the auto-names to the names of params */
        return super.getParametersMetaData();
    }

    void getTableNamesForRead(OrderedHashSet set) {}

    void getTableNamesForWrite(OrderedHashSet set) {}
}
