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
import org.hsqldb_voltpatches.lib.HsqlArrayList;
import org.hsqldb_voltpatches.result.Result;

/**
 * Implementation of Statement for CREATE SCHEMA statements.<p>
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 1.9.0
 * @since 1.9.0
 */
public class StatementSchemaDefinition extends StatementSchema {

    StatementSchema[] statements;

    StatementSchemaDefinition(StatementSchema[] statements) {

        super();

        this.statements = statements;
    }

    public Result execute(Session session) {

        Result result = getResult(session);

        if (result.isError()) {
            result.getException().setStatementType(group, type);
        }

        return result;
    }

    Result getResult(Session session) {

        schemaName = statements[0].getSchemaName();

        if (this.isExplain) {
            return Result.newSingleColumnStringResult("OPERATION",
                    describe(session));
        }

        StatementSchema cs;
        Result          result      = statements[0].execute(session);
        HsqlArrayList   constraints = new HsqlArrayList();

        if (statements.length == 1 || result.isError()) {
            return result;
        }

        HsqlName oldSessionSchema = session.getCurrentSchemaHsqlName();

        for (int i = 1; i < statements.length; i++) {
            try {
                session.setSchema(schemaName.name);
            } catch (HsqlException e) {}

            statements[i].setSchemaHsqlName(schemaName);
            session.parser.reset(statements[i].getSQL());

            try {
                session.parser.read();

                switch (statements[i].getType()) {

                    case StatementTypes.GRANT :
                    case StatementTypes.GRANT_ROLE :
                        result = statements[i].execute(session);
                        break;

                    case StatementTypes.CREATE_TABLE :
                        cs                    = session.parser.compileCreate();
                        cs.isSchemaDefinition = true;

                        cs.setSchemaHsqlName(schemaName);

                        if (session.parser.token.tokenType
                                != Tokens.X_ENDPARSE) {
                            throw session.parser.unexpectedToken();
                        }

                        result = cs.execute(session);

                        constraints.addAll((HsqlArrayList) cs.arguments[1]);
                        ((HsqlArrayList) cs.arguments[1]).clear();
                        break;

                    case StatementTypes.CREATE_ROLE :
                    case StatementTypes.CREATE_SEQUENCE :
                    case StatementTypes.CREATE_TYPE :
                    case StatementTypes.CREATE_CHARACTER_SET :
                    case StatementTypes.CREATE_COLLATION :
                        result = statements[i].execute(session);
                        break;

                    case StatementTypes.CREATE_INDEX :
                    case StatementTypes.CREATE_TRIGGER :
                    case StatementTypes.CREATE_VIEW :
                    case StatementTypes.CREATE_DOMAIN :
                    case StatementTypes.CREATE_ROUTINE :
                        cs                    = session.parser.compileCreate();
                        cs.isSchemaDefinition = true;

                        cs.setSchemaHsqlName(schemaName);

                        if (session.parser.token.tokenType
                                != Tokens.X_ENDPARSE) {
                            throw session.parser.unexpectedToken();
                        }

                        result = cs.execute(session);
                        break;

                    case StatementTypes.CREATE_ASSERTION :
                    case StatementTypes.CREATE_TRANSFORM :
                    case StatementTypes.CREATE_TRANSLATION :
                    case StatementTypes.CREATE_CAST :
                    case StatementTypes.CREATE_ORDERING :
                        throw session.parser.unsupportedFeature();
                    default :
                        throw Error.runtimeError(ErrorCode.U_S0500, "");
                }

                if (result.isError()) {
                    break;
                }
            } catch (HsqlException e) {
                result = Result.newErrorResult(e, statements[i].getSQL());
            }
        }

        if (!result.isError()) {
            try {
                for (int i = 0; i < constraints.size(); i++) {
                    Constraint c = (Constraint) constraints.get(i);
                    Table table =
                        session.database.schemaManager.getUserTable(session,
                            c.core.refTableName);

                    ParserDDL.addForeignKey(session, table, c, null);
                }
            } catch (HsqlException e) {
                result = Result.newErrorResult(e, sql);
            }
        }

        if (result.isError()) {
            try {
                session.database.schemaManager.dropSchema(schemaName.name,
                        true);
                session.database.logger.writeToLog(
                    session, getDropSchemaStatement(schemaName));
            } catch (HsqlException e) {}
        }

        try {
            // A VoltDB extension to disable 
            // Try not to explicitly throw an exception, just to catch and ignore it,
            // but accidents can happen, so keep the try/catch anyway.
            session.setSchemaNoThrow(oldSessionSchema.name);
            /* disable 1 line ...
            session.setSchema(oldSessionSchema.name);
            ... disabled 1 line */
            // End of VoltDB extension
        } catch (Exception e) {}

        return result;
    }

/*
    if (constraintList != null && constraintList.size() > 0) {
        try {
            for (int i = 0; i < constraintList.size(); i++) {
                Constraint c = (Constraint) constraintList.get(i);
                Table table = database.schemaManager.getUserTable(session,
                    c.core.refTableName);

                addForeignKey(table, c);
            }
        } finally {
            constraintList.clear();
        }
    }
*/
    String getDropSchemaStatement(HsqlName schema) {
        return "DROP SCHEMA " + schema.statementName + " " + Tokens.T_CASCADE;
    }
}
