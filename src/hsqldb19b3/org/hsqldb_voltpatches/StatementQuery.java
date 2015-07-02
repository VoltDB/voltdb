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
import org.hsqldb_voltpatches.result.Result;
import org.hsqldb_voltpatches.result.ResultMetaData;
import org.hsqldb_voltpatches.result.ResultProperties;

/**
 * Implementation of Statement for query expressions.<p>
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 2.2.9
 * @since 1.9.0
 */
public class StatementQuery extends StatementDMQL {

    StatementQuery(Session session, QueryExpression queryExpression,
                   CompileContext compileContext) {

        super(StatementTypes.SELECT_CURSOR, StatementTypes.X_SQL_DATA,
              session.getCurrentSchemaHsqlName());

        this.statementReturnType = StatementTypes.RETURN_RESULT;
        this.queryExpression     = queryExpression;

        setDatabseObjects(session, compileContext);
        checkAccessRights(session);
    }

    Result getResult(Session session) {

        Result result = queryExpression.getResult(session,
            session.getMaxRows());

        result.setStatement(this);

        return result;
    }

    public ResultMetaData getResultMetaData() {

        switch (type) {

            case StatementTypes.SELECT_CURSOR :
                return queryExpression.getMetaData();

            case StatementTypes.SELECT_SINGLE :
                return queryExpression.getMetaData();

            default :
                throw Error.runtimeError(ErrorCode.U_S0500,
                                         "StatementQuery.getResultMetaData()");
        }
    }

    void collectTableNamesForRead(OrderedHashSet set) {

        queryExpression.getBaseTableNames(set);

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

        if (queryExpression.isUpdatable) {
            queryExpression.getBaseTableNames(set);
        }
    }

    public int getResultProperties() {

        return queryExpression.isUpdatable
               ? ResultProperties.updatablePropsValue
               : ResultProperties.defaultPropsValue;
    }
    // A VoltDB extension to export abstract parse trees

    /**
     * VoltDB added method to get an abstract parse tree
     * representation of this HSQLDB object.
     * @param session The current Session object may be needed to resolve
     * some names.
     * @return VoltXMLElement, a free-form (schema-less) abstract parse tree.
     * @throws org.hsqldb_voltpatches.HSQLInterface.HSQLParseException
     */
    @Override
    VoltXMLElement voltGetStatementXML(Session session)
    throws org.hsqldb_voltpatches.HSQLInterface.HSQLParseException
    {
        return voltGetXMLExpression(queryExpression, parameters, session);
    }
    // End of VoltDB extension
}
