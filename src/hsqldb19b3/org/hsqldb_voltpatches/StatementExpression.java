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

import org.hsqldb_voltpatches.ParserDQL.CompileContext;
import org.hsqldb_voltpatches.error.Error;
import org.hsqldb_voltpatches.error.ErrorCode;
import org.hsqldb_voltpatches.lib.OrderedHashSet;
import org.hsqldb_voltpatches.navigator.RowSetNavigatorData;
import org.hsqldb_voltpatches.result.Result;

/**
 * Implementation of Statement for PSM statements with expressions.
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 2.2.9
 * @since 1.9.0
 */
public class StatementExpression extends StatementDMQL {

    Expression expression;

    /**
     * for RETURN and flow control
     */
    StatementExpression(Session session, CompileContext compileContext,
                        int type, Expression expression) {

        super(type, StatementTypes.X_SQL_CONTROL, null);

        switch (type) {

            case StatementTypes.RETURN :
            case StatementTypes.CONDITION :
                break;

            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "");
        }

        isTransactionStatement = false;
        this.expression        = expression;

        setDatabseObjects(session, compileContext);
        checkAccessRights(session);
    }

    public String getSQL() {

        StringBuffer sb = new StringBuffer();

        switch (type) {

            case StatementTypes.RETURN :
                return sql;

            case StatementTypes.CONDITION :
                sb.append(expression.getSQL());
                break;
        }

        return sb.toString();
    }

    TableDerived[] getSubqueries(Session session) {

        OrderedHashSet subQueries = null;

        if (expression != null) {
            subQueries = expression.collectAllSubqueries(subQueries);
        }

        if (subQueries == null || subQueries.size() == 0) {
            return TableDerived.emptyArray;
        }

        TableDerived[] subQueryArray = new TableDerived[subQueries.size()];

        subQueries.toArray(subQueryArray);

        for (int i = 0; i < subqueries.length; i++) {
            subQueryArray[i].prepareTable();
        }

        return subQueryArray;
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

        Result result;

        try {
            if (subqueries.length > 0) {
                materializeSubQueries(session);
            }

            result = getResult(session);
        } catch (Throwable t) {
            result = Result.newErrorResult(t, null);
        }

        if (result.isError()) {
            result.getException().setStatementType(group, type);
        }

        return result;
    }

    Result getResult(Session session) {

        switch (type) {

            case StatementTypes.RETURN :
            case StatementTypes.CONDITION :
                Result result = expression.getResult(session);

                // data navigator has statement scope and will be cleared at the end of statement
                if (result.isData()) {
                    RowSetNavigatorData navigator =
                        new RowSetNavigatorData(session,
                                                result.getNavigator());

                    result.setNavigator(navigator);
                }

                return result;

            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "");
        }
    }

    public void resolve(Session session) {}

    String describeImpl(Session session) throws Exception {
        return getSQL();
    }

    void collectTableNamesForRead(OrderedHashSet set) {

        for (int i = 0; i < subqueries.length; i++) {
            if (subqueries[i].queryExpression != null) {
                subqueries[i].queryExpression.getBaseTableNames(set);
            }
        }

        for (int i = 0; i < routines.length; i++) {
            set.addAll(routines[i].getTableNamesForRead());
        }
    }

    void collectTableNamesForWrite(OrderedHashSet set) {}
}
