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
import org.hsqldb_voltpatches.rights.Grantee;
import org.hsqldb_voltpatches.rights.User;
import org.hsqldb_voltpatches.types.DTIType;
import org.hsqldb_voltpatches.types.IntervalSecondData;

/**
 * Implementation of Statement for SQL commands.<p>
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 1.9.0
 * @since 1.9.0
 */
public class StatementSession extends Statement {

    Expression[] expressions;
    Object[]     parameters;

    StatementSession(int type, Expression[] args) {

        super(type);

        this.expressions       = args;
        isTransactionStatement = false;

        switch (type) {

            case StatementTypes.SET_PATH :
            case StatementTypes.SET_TIME_ZONE :
            case StatementTypes.SET_NAMES :
            case StatementTypes.SET_ROLE :
            case StatementTypes.SET_SCHEMA :
            case StatementTypes.SET_SESSION_AUTHORIZATION :
            case StatementTypes.SET_COLLATION :
                group = StatementTypes.X_SQL_SESSION;
                break;

            default :
                throw Error.runtimeError(ErrorCode.U_S0500,
                                         "StateemntCommand");
        }
    }

    StatementSession(int type, Object[] args) {

        super(type);

        this.parameters        = args;
        isTransactionStatement = false;
        isLogged               = false;

        switch (type) {

            // logged by statement
            case StatementTypes.SET_SCHEMA :
                group    = StatementTypes.X_SQL_SESSION;
                isLogged = true;
                break;

            case StatementTypes.DECLARE_VARIABLE :
                group    = StatementTypes.X_HSQLDB_SESSION;
                isLogged = true;
                break;

            // cursor
            case StatementTypes.ALLOCATE_CURSOR :
                group = StatementTypes.X_SQL_DATA;
                break;

            case StatementTypes.ALLOCATE_DESCRIPTOR :
            case StatementTypes.DEALLOCATE_DESCRIPTOR :
            case StatementTypes.DEALLOCATE_PREPARE :
                group = StatementTypes.X_DYNAMIC;
                break;

            //
            case StatementTypes.DYNAMIC_DELETE_CURSOR :
                group = StatementTypes.X_SQL_DATA_CHANGE;
                break;

            case StatementTypes.DYNAMIC_CLOSE :
            case StatementTypes.DYNAMIC_FETCH :
            case StatementTypes.DYNAMIC_OPEN :
                group = StatementTypes.X_SQL_DATA;
                break;

            //
            case StatementTypes.OPEN :
            case StatementTypes.FETCH :
            case StatementTypes.FREE_LOCATOR :
            case StatementTypes.GET_DESCRIPTOR :
            case StatementTypes.HOLD_LOCATOR :
                group = StatementTypes.X_SQL_DATA;
                break;

            //
            case StatementTypes.PREPARABLE_DYNAMIC_DELETE_CURSOR :
            case StatementTypes.PREPARABLE_DYNAMIC_UPDATE_CURSOR :
            case StatementTypes.PREPARE :
                group = StatementTypes.X_DYNAMIC;
                break;

            // logged by session
            case StatementTypes.DISCONNECT :
                group = StatementTypes.X_SQL_CONNECTION;
                break;

            //
            case StatementTypes.SET_CONNECTION :
            case StatementTypes.SET_CONSTRAINT :
            case StatementTypes.SET_DESCRIPTOR :
            case StatementTypes.SET_CATALOG :
            case StatementTypes.SET_SESSION_CHARACTERISTICS :
            case StatementTypes.SET_TRANSFORM_GROUP :
            case StatementTypes.SET_SESSION_RESULT_MAX_ROWS :
            case StatementTypes.SET_SESSION_RESULT_MEMORY_ROWS :
            case StatementTypes.SET_SESSION_AUTOCOMMIT :
                group = StatementTypes.X_HSQLDB_SESSION;
                break;

            // logged by session if necessary
            case StatementTypes.COMMIT_WORK :
            case StatementTypes.RELEASE_SAVEPOINT :
            case StatementTypes.ROLLBACK_SAVEPOINT :
            case StatementTypes.ROLLBACK_WORK :
            case StatementTypes.SAVEPOINT :
            case StatementTypes.SET_TRANSACTION :
            case StatementTypes.START_TRANSACTION :
                group = StatementTypes.X_SQL_TRANSACTION;
                break;

            default :
                throw Error.runtimeError(ErrorCode.U_S0500,
                                         "StatementCommand");
        }
    }

    StatementSession(int type, HsqlName[] readNames, HsqlName[] writeNames) {

        super(type);

        this.isTransactionStatement = true;
        this.readTableNames         = readNames;
        writeTableNames             = writeNames;

        switch (type) {

            case StatementTypes.TRANSACTION_LOCK_TABLE :
                group = StatementTypes.X_HSQLDB_TRANSACTION;
                break;

            default :
                throw Error.runtimeError(ErrorCode.U_S0500,
                                         "StatementCommand");
        }
    }

    public Result execute(Session session) {

        Result result = getResult(session);

        if (result.isError()) {
            result.getException().setStatementType(group, type);

            return result;
        }

        try {
            if (isLogged) {
                session.database.logger.writeToLog(session, sql);
            }
        } catch (HsqlException e) {
            return Result.newErrorResult(e, sql);
        }

        return result;
    }

    Result getResult(Session session) {

        boolean startTransaction = false;

        if (this.isExplain) {
            return Result.newSingleColumnStringResult("OPERATION",
                    describe(session));
        }

        switch (type) {

            // cursor
            case StatementTypes.ALLOCATE_CURSOR :
            case StatementTypes.ALLOCATE_DESCRIPTOR :
                return Result.updateZeroResult;

            //
            case StatementTypes.COMMIT_WORK : {
                try {
                    boolean chain = parameters != null;

                    session.commit(chain);

                    return Result.updateZeroResult;
                } catch (HsqlException e) {
                    return Result.newErrorResult(e, sql);
                }
            }
            case StatementTypes.DEALLOCATE_DESCRIPTOR :
            case StatementTypes.DEALLOCATE_PREPARE :
                return Result.updateZeroResult;

            case StatementTypes.DISCONNECT :
                session.close();

                return Result.updateZeroResult;

            //
            case StatementTypes.DYNAMIC_CLOSE :
            case StatementTypes.DYNAMIC_DELETE_CURSOR :
            case StatementTypes.DYNAMIC_FETCH :
            case StatementTypes.DYNAMIC_OPEN :

            //
            case StatementTypes.FETCH :
            case StatementTypes.FREE_LOCATOR :
            case StatementTypes.GET_DESCRIPTOR :
            case StatementTypes.HOLD_LOCATOR :

            //
            case StatementTypes.OPEN :
            case StatementTypes.PREPARABLE_DYNAMIC_DELETE_CURSOR :
            case StatementTypes.PREPARABLE_DYNAMIC_UPDATE_CURSOR :
            case StatementTypes.PREPARE :
                return Result.updateZeroResult;

            case StatementTypes.TRANSACTION_LOCK_TABLE : {
                return Result.updateZeroResult;
            }
            case StatementTypes.RELEASE_SAVEPOINT : {
                String savepoint = (String) parameters[0];

                try {
                    session.releaseSavepoint(savepoint);

                    return Result.updateZeroResult;
                } catch (HsqlException e) {
                    return Result.newErrorResult(e, sql);
                }
            }
            case StatementTypes.ROLLBACK_WORK : {
                boolean chain = ((Boolean) parameters[0]).booleanValue();

                session.rollback(chain);

                return Result.updateZeroResult;
            }
            case StatementTypes.ROLLBACK_SAVEPOINT : {
                String savepoint = (String) parameters[0];

                try {
                    session.rollbackToSavepoint(savepoint);

                    return Result.updateZeroResult;
                } catch (HsqlException e) {
                    return Result.newErrorResult(e, sql);
                }
            }
            case StatementTypes.SAVEPOINT : {
                String savepoint = (String) parameters[0];

                session.savepoint(savepoint);

                return Result.updateZeroResult;
            }
            case StatementTypes.SET_CATALOG : {
                String name;

                try {
                    name = (String) expressions[0].getValue(session);

                    if (session.database.getCatalogName().equals(name)) {
                        return Result.updateZeroResult;
                    }

                    return Result.newErrorResult(
                        Error.error(ErrorCode.X_3D000), sql);
                } catch (HsqlException e) {
                    return Result.newErrorResult(e, sql);
                }
            }
            case StatementTypes.SET_CONNECTION :
            case StatementTypes.SET_CONSTRAINT :
            case StatementTypes.SET_DESCRIPTOR :
                return Result.updateZeroResult;

            case StatementTypes.SET_TIME_ZONE : {
                Object value = null;

                if (expressions[0].getType() == OpTypes.VALUE
                        && expressions[0].getConstantValueNoCheck(session)
                           == null) {
                    session.timeZoneSeconds = session.sessionTimeZoneSeconds;

                    return Result.updateZeroResult;
                }

                try {
                    value = expressions[0].getValue(session);
                } catch (HsqlException e) {}

                if (value instanceof Result) {
                    Result result = (Result) value;

                    if (result.isData()) {
                        Object[] data =
                            (Object[]) result.getNavigator().getNext();
                        boolean single = !result.getNavigator().next();

                        if (single && data != null && data[0] != null) {
                            value = data[0];

                            result.getNavigator().close();
                        } else {
                            result.getNavigator().close();

                            return Result.newErrorResult(
                                Error.error(ErrorCode.X_22009), sql);
                        }
                    } else {
                        return Result.newErrorResult(
                            Error.error(ErrorCode.X_22009), sql);
                    }
                } else {
                    if (value == null) {
                        return Result.newErrorResult(
                            Error.error(ErrorCode.X_22009), sql);
                    }
                }

                long seconds = ((IntervalSecondData) value).getSeconds();

                if (-DTIType.timezoneSecondsLimit <= seconds
                        && seconds <= DTIType.timezoneSecondsLimit) {
                    session.timeZoneSeconds = (int) seconds;

                    return Result.updateZeroResult;
                }

                return Result.newErrorResult(Error.error(ErrorCode.X_22009),
                                             sql);
            }
            case StatementTypes.SET_NAMES :
                return Result.updateZeroResult;

            case StatementTypes.SET_PATH :
                return Result.updateZeroResult;

            case StatementTypes.SET_ROLE : {
                String  name;
                Grantee role;

                try {
                    name = (String) expressions[0].getValue(session);
                    role = session.database.granteeManager.getRole(name);
                } catch (HsqlException e) {
                    return Result.newErrorResult(
                        Error.error(ErrorCode.X_0P000), sql);
                }

                if (role == null) {
                    return Result.newErrorResult(
                        Error.error(ErrorCode.X_0P000), sql);
                }

                if (session.getGrantee().hasRole(role)) {

                    /** @todo 1.9.0 - implement */
                    return Result.updateZeroResult;
                } else {
                    return Result.newErrorResult(
                        Error.error(ErrorCode.X_0P000), sql);
                }
            }
            case StatementTypes.SET_SCHEMA : {
                String   name;
                HsqlName schema;

                try {
                    if (expressions == null) {
                        name = ((HsqlName) parameters[0]).name;
                    } else {
                        name = (String) expressions[0].getValue(session);
                    }

                    schema =
                        session.database.schemaManager.getSchemaHsqlName(name);

                    session.setSchema(schema.name);

                    return Result.updateZeroResult;
                } catch (HsqlException e) {
                    return Result.newErrorResult(e, sql);
                }
            }
            case StatementTypes.SET_SESSION_AUTHORIZATION : {
                if (session.isInMidTransaction()) {
                    return Result.newErrorResult(
                        Error.error(ErrorCode.X_25001), sql);
                }

                try {
                    String user;
                    String password = null;

                    user = (String) expressions[0].getValue(session);

                    if (expressions[1] != null) {
                        password = (String) expressions[1].getValue(session);
                    }

                    User userObject;

                    if (password == null) {
                        userObject = session.database.userManager.get(user);
                    } else {
                        userObject =
                            session.database.getUserManager().getUser(user,
                                password);
                    }

                    if (userObject == null) {
                        throw Error.error(ErrorCode.X_28501);
                    }

                    sql = userObject.getConnectUserSQL();

                    if (userObject == session.getGrantee()) {
                        return Result.updateZeroResult;
                    }

                    if (session.getGrantee().canChangeAuthorisation()) {
                        session.setUser((User) userObject);
                        session.resetSchema();

                        return Result.updateZeroResult;
                    }

                    /** @todo may need different error code */
                    throw Error.error(ErrorCode.X_28000);
                } catch (HsqlException e) {
                    return Result.newErrorResult(e, sql);
                }
            }
            case StatementTypes.SET_SESSION_CHARACTERISTICS : {
                try {
                    if (parameters[0] != null) {
                        boolean readonly =
                            ((Boolean) parameters[0]).booleanValue();

                        session.setReadOnlyDefault(readonly);
                    }

                    if (parameters[1] != null) {
                        int level = ((Integer) parameters[1]).intValue();

                        session.setIsolationDefault(level);
                    }

                    return Result.updateZeroResult;
                } catch (HsqlException e) {
                    return Result.newErrorResult(e, sql);
                }
            }
            case StatementTypes.SET_COLLATION :
                return Result.updateZeroResult;

            case StatementTypes.SET_TRANSFORM_GROUP :
                return Result.updateZeroResult;

            case StatementTypes.START_TRANSACTION :
                startTransaction = true;

            // $FALL-THROUGH$
            case StatementTypes.SET_TRANSACTION : {
                try {
                    if (parameters[0] != null) {
                        boolean readonly =
                            ((Boolean) parameters[0]).booleanValue();

                        session.setReadOnly(readonly);
                    }

                    if (parameters[1] != null) {
                        int level = ((Integer) parameters[1]).intValue();

                        session.setIsolation(level);
                    }

                    if (startTransaction) {
                        session.startTransaction();
                    }

                    return Result.updateZeroResult;
                } catch (HsqlException e) {
                    return Result.newErrorResult(e, sql);
                }
            }

            //
            case StatementTypes.SET_SESSION_AUTOCOMMIT : {
                boolean mode = ((Boolean) parameters[0]).booleanValue();

                try {
                    session.setAutoCommit(mode);

                    return Result.updateZeroResult;
                } catch (HsqlException e) {
                    return Result.newErrorResult(e, sql);
                }
            }
            case StatementTypes.DECLARE_VARIABLE : {
                ColumnSchema variable = (ColumnSchema) parameters[0];

                try {
                    session.sessionContext.addSessionVariable(variable);

                    return Result.updateZeroResult;
                } catch (HsqlException e) {
                    return Result.newErrorResult(e, sql);
                }
            }
            case StatementTypes.SET_SESSION_RESULT_MAX_ROWS : {
                int size = ((Integer) parameters[0]).intValue();

                session.setSQLMaxRows(size);

                return Result.updateZeroResult;
            }
            case StatementTypes.SET_SESSION_RESULT_MEMORY_ROWS : {
                int size = ((Integer) parameters[0]).intValue();

                session.setResultMemoryRowCount(size);

                return Result.updateZeroResult;
            }
            default :
                throw Error.runtimeError(ErrorCode.U_S0500,
                                         "CompiledStateemntCommand");
        }
    }

    public boolean isAutoCommitStatement() {
        return false;
    }

    public String describe(Session session) {
        return sql;
    }
}
