/* Copyright (c) 1995-2000, The Hypersonic SQL Group.
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
 * Neither the name of the Hypersonic SQL Group nor the names of its
 * contributors may be used to endorse or promote products derived from this
 * software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE HYPERSONIC SQL GROUP,
 * OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
 * EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
 * PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 * This software consists of voluntary contributions made by many individuals
 * on behalf of the Hypersonic SQL Group.
 *
 *
 * For work added by the HSQL Development Group:
 *
 * Copyright (c) 2001-2009, The HSQL Development Group
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

import java.io.InputStream;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.Locale;
import java.util.Random;
import java.util.SimpleTimeZone;
import java.util.TimeZone;

import org.hsqldb_voltpatches.HsqlNameManager.HsqlName;
import org.hsqldb_voltpatches.jdbc.JDBCConnection;
import org.hsqldb_voltpatches.lib.ArrayUtil;
import org.hsqldb_voltpatches.lib.CountUpDownLatch;
import org.hsqldb_voltpatches.lib.HsqlArrayList;
import org.hsqldb_voltpatches.lib.OrderedHashSet;
import org.hsqldb_voltpatches.lib.SimpleLog;
import org.hsqldb_voltpatches.lib.java.JavaSystem;
import org.hsqldb_voltpatches.navigator.RowSetNavigator;
import org.hsqldb_voltpatches.navigator.RowSetNavigatorClient;
import org.hsqldb_voltpatches.persist.HsqlDatabaseProperties;
import org.hsqldb_voltpatches.result.Result;
import org.hsqldb_voltpatches.result.ResultConstants;
import org.hsqldb_voltpatches.result.ResultLob;
import org.hsqldb_voltpatches.rights.Grantee;
import org.hsqldb_voltpatches.rights.User;
import org.hsqldb_voltpatches.store.ValuePool;
import org.hsqldb_voltpatches.types.BlobDataID;
import org.hsqldb_voltpatches.types.ClobDataID;
import org.hsqldb_voltpatches.types.TimeData;
import org.hsqldb_voltpatches.types.TimestampData;
import org.hsqldb_voltpatches.types.Type;

/**
 * Implementation of SQL sessions.
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 1.9.0
 * @since 1.7.0
 */
public class Session implements SessionInterface {

    //
    private volatile boolean isClosed;

    //
    public Database database;
    private User    user;

    // transaction support
    private volatile boolean isAutoCommit;
    private volatile boolean isReadOnly;
    boolean                  isReadOnlyDefault;
    int isolationModeDefault = SessionInterface.TX_READ_COMMITTED;
    int isolationMode        = SessionInterface.TX_READ_COMMITTED;
    int                      actionIndex;
    long                     actionTimestamp;
    long                     transactionTimestamp;
    boolean                  isTransaction;
    boolean                  isBatch;
    volatile boolean         abortTransaction;
    volatile boolean         redoAction;
    HsqlArrayList            rowActionList;
    volatile boolean         tempUnlocked;
    OrderedHashSet           waitingSessions;
    OrderedHashSet           tempSet;
    CountUpDownLatch         latch = new CountUpDownLatch();
    Statement                currentStatement;
    Statement                lockStatement;

    // current settings
    final int          sessionTimeZoneSeconds;
    int                timeZoneSeconds;
    boolean            isNetwork;
    private int        currentMaxRows;
    private int        sessionMaxRows;
    private Number     lastIdentity = ValuePool.INTEGER_0;
    private final long sessionId;
    private boolean    script;

    // internal connection
    private JDBCConnection intConnection;

    // schema
    public HsqlName currentSchema;
    public HsqlName loggedSchema;

    // query processing
    ParserCommand         parser;
    boolean               isProcessingScript;
    boolean               isProcessingLog;
    public SessionContext sessionContext;
    int                   resultMaxMemoryRows;

    //
    public SessionData sessionData;

    /** @todo 1.9.0 fredt - clarify in which circumstances Session has to disconnect */
    Session getSession() {
        return this;
    }

    /**
     * Constructs a new Session object.
     *
     * @param  db the database to which this represents a connection
     * @param  user the initial user
     * @param  autocommit the initial autocommit value
     * @param  readonly the initial readonly value
     * @param  id the session identifier, as known to the database
     */
    Session(Database db, User user, boolean isInitiallyLogged,
            boolean autocommit, boolean readonly, long id,
            int timeZoneSeconds) {

        sessionId                   = id;
        database                    = db;
        this.user                   = user;
        this.sessionTimeZoneSeconds = timeZoneSeconds;
        this.timeZoneSeconds        = timeZoneSeconds;
        rowActionList               = new HsqlArrayList(true);
        waitingSessions             = new OrderedHashSet();
        tempSet                     = new OrderedHashSet();
        isAutoCommit                = autocommit;
        isReadOnly                  = readonly;
        isolationMode               = isolationModeDefault;
        sessionContext              = new SessionContext(this);
        parser                      = new ParserCommand(this, new Scanner());

        setResultMemoryRowCount(database.getResultMaxMemoryRows());
        resetSchema();

        sessionData = new SessionData(database, this);

        try {
            if (isInitiallyLogged) {
                database.logger.writeToLog(this, getSetSchemaStatement());
            }
        } catch (HsqlException e) {}
    }

    void resetSchema() {
        loggedSchema  = null;
        currentSchema = user.getInitialOrDefaultSchema();
    }

    /**
     *  Retrieves the session identifier for this Session.
     *
     * @return the session identifier for this Session
     */
    public long getId() {
        return sessionId;
    }

    /**
     * Closes this Session.
     */
    public synchronized void close() {

        if (isClosed) {
            return;
        }

        rollback(false);

        try {
            database.logger.writeToLog(this, Tokens.T_DISCONNECT);
        } catch (HsqlException e) {}

        sessionData.closeAllNavigators();
        sessionData.persistentStoreCollection.clearAllTables();
        sessionData.closeResultCache();
        database.compiledStatementManager.removeSession(sessionId);
        database.sessionManager.removeSession(this);
        database.closeIfLast();

        database                  = null;
        user                      = null;
        rowActionList             = null;
        sessionContext.savepoints = null;
        intConnection             = null;
        sessionContext            = null;
        lastIdentity              = null;
        isClosed                  = true;
    }

    /**
     * Retrieves whether this Session is closed.
     *
     * @return true if this Session is closed
     */
    public boolean isClosed() {
        return isClosed;
    }

    public synchronized void setIsolationDefault(int level) {

        if (level == SessionInterface.TX_READ_UNCOMMITTED) {
            isReadOnlyDefault = true;
        }

        if (level == isolationModeDefault) {
            return;
        }

        isolationModeDefault = level;

        if (!isInMidTransaction()) {
            isolationMode = isolationModeDefault;
        }

        database.logger.writeToLog(this, getSessionIsolationSQL());
    }

    /**
     * sets ISOLATION for the next transaction only
     */
    public void setIsolation(int level) {

        if (isInMidTransaction()) {
            throw Error.error(ErrorCode.X_25001);
        }

        if (level == SessionInterface.TX_READ_UNCOMMITTED) {
            isReadOnly = true;
        }

        isolationMode = level;

        if (isolationMode != isolationModeDefault) {
            database.logger.writeToLog(this, getTransactionIsolationSQL());
        }
    }

    public synchronized int getIsolation() {
        return isolationMode;
    }

    /**
     * Setter for iLastIdentity attribute.
     *
     * @param  i the new value
     */
    void setLastIdentity(Number i) {
        lastIdentity = i;
    }

    /**
     * Getter for iLastIdentity attribute.
     *
     * @return the current value
     */
    public Number getLastIdentity() {
        return lastIdentity;
    }

    /**
     * Retrieves the Database instance to which this
     * Session represents a connection.
     *
     * @return the Database object to which this Session is connected
     */
    public Database getDatabase() {
        return database;
    }

    /**
     * Retrieves the name, as known to the database, of the
     * user currently controlling this Session.
     *
     * @return the name of the user currently connected within this Session
     */
    public String getUsername() {
        return user.getNameString();
    }

    /**
     * Retrieves the User object representing the user currently controlling
     * this Session.
     *
     * @return this Session's User object
     */
    public User getUser() {
        return (User) user;
    }

    public Grantee getGrantee() {
        return user;
    }

    /**
     * Sets this Session's User object to the one specified by the
     * user argument.
     *
     * @param  user the new User object for this session
     */
    void setUser(User user) {
        this.user = user;
    }

    int getMaxRows() {
        return currentMaxRows;
    }

    public int getSQLMaxRows() {
        return sessionMaxRows;
    }

    /**
     * The SQL command SET MAXROWS n will override the Statement.setMaxRows(n)
     * until SET MAXROWS 0 is issued.
     *
     * NB this is dedicated to the SET MAXROWS sql statement and should not
     * otherwise be called. (fredt@users)
     */
    void setSQLMaxRows(int rows) {
        currentMaxRows = sessionMaxRows = rows;
    }

    /**
     * Checks whether this Session's current User has the privileges of
     * the ADMIN role.
     */
    void checkAdmin() {
        user.checkAdmin();
    }

    /**
     * This is used for reading - writing to existing tables.
     * @throws  HsqlException
     */
    void checkReadWrite() {

        if (isReadOnly) {
            throw Error.error(ErrorCode.X_25006);
        }
    }

    /**
     * This is used for creating new database objects such as tables.
     * @throws  HsqlException
     */
    void checkDDLWrite() {

        checkReadWrite();

        if (isProcessingScript || isProcessingLog) {
            return;
        }

        if (database.isFilesReadOnly()) {
            throw Error.error(ErrorCode.DATABASE_IS_READONLY);
        }
    }

    public long getActionTimestamp() {
        return actionTimestamp;
    }

    /**
     *  Adds a delete action to the row and the transaction manager.
     *
     * @param  table the table of the row
     * @param  row the deleted row
     * @throws  HsqlException
     */
    void addDeleteAction(Table table, Row row) {

//        tempActionHistory.add("add delete action " + actionTimestamp);
        if (abortTransaction) {

//            throw Error.error(ErrorCode.X_40001);
        }

        database.txManager.addDeleteAction(this, table, row);
    }

    void addInsertAction(Table table, Row row) {

//        tempActionHistory.add("add insert to transaction " + actionTimestamp);
        database.txManager.addInsertAction(this, table, row);

        // abort only after adding so that the new row gets removed from indexes
        if (abortTransaction) {

//            throw Error.error(ErrorCode.X_40001);
        }
    }

    /**
     *  Setter for the autocommit attribute.
     *
     * @param  autocommit the new value
     * @throws  HsqlException
     */
    public synchronized void setAutoCommit(boolean autocommit) {

        if (isClosed) {
            return;
        }

        if (autocommit != isAutoCommit) {
            commit(false);

            isAutoCommit = autocommit;
        }
    }

    public void beginAction(Statement cs) {

        actionIndex = rowActionList.size();

        database.txManager.beginAction(this, cs);

//        tempActionHistory.add("beginAction ends " + actionTimestamp);
    }

    public void endAction(Result r) {

//        tempActionHistory.add("endAction " + actionTimestamp);
        sessionData.persistentStoreCollection.clearStatementTables();

        if (r.isError()) {
            sessionData.persistentStoreCollection.clearResultTables(
                actionTimestamp);
            database.txManager.rollbackAction(this);
        } else {
            database.txManager.completeActions(this);
        }

//        tempActionHistory.add("endAction ends " + actionTimestamp);
    }

    public boolean hasLocks() {
        return currentStatement == lockStatement;
    }

    public void startTransaction() {
        database.txManager.beginTransaction(this);
    }

    public synchronized void startPhasedTransaction() {}

    /**
     * @todo - fredt - for two phased pre-commit - after this call, further
     * state changing calls should fail
     */
    public synchronized void prepareCommit() {

        if (isClosed) {
            throw Error.error(ErrorCode.X_08003);
        }

        if (!database.txManager.prepareCommitActions(this)) {

//            tempActionHistory.add("commit aborts " + actionTimestamp);
            rollback(false);

            throw Error.error(ErrorCode.X_40001);
        }
    }

    /**
     * Commits any uncommited transaction this Session may have open
     *
     * @throws  HsqlException
     */
    public synchronized void commit(boolean chain) {

//        tempActionHistory.add("commit " + actionTimestamp);
        if (isClosed) {
            return;
        }

        if (!isTransaction) {
            isReadOnly    = isReadOnlyDefault;
            isolationMode = isolationModeDefault;

            return;
        }

        if (!database.txManager.commitTransaction(this)) {

//            tempActionHistory.add("commit aborts " + actionTimestamp);
            rollback(false);

            throw Error.error(ErrorCode.X_40001);
        }

        endTransaction(true);
    }

    /**
     * Rolls back any uncommited transaction this Session may have open.
     *
     * @throws  HsqlException
     */
    public synchronized void rollback(boolean chain) {

        //        tempActionHistory.add("rollback " + actionTimestamp);
        if (isClosed) {
            return;
        }

        if (!isTransaction) {
            isReadOnly    = isReadOnlyDefault;
            isolationMode = isolationModeDefault;

            return;
        }

        try {
            database.logger.writeToLog(this, Tokens.T_ROLLBACK);
        } catch (HsqlException e) {}

        database.txManager.rollback(this);
        endTransaction(false);
    }

    private void endTransaction(boolean commit) {

        sessionData.updateLobUsage(commit);
        sessionContext.savepoints.clear();
        sessionContext.savepointTimestamps.clear();
        rowActionList.clear();
        sessionData.persistentStoreCollection.clearTransactionTables();
        sessionData.closeAllTransactionNavigators();

        isReadOnly    = isReadOnlyDefault;
        isolationMode = isolationModeDefault;
        lockStatement = null;

/* debug 190
        tempActionHistory.add("commit ends " + actionTimestamp);
        tempActionHistory.clear();
//*/
        if (database != null && database.logger.needsCheckpoint()) {
            try {
                database.logger.checkpoint(false);
            } catch (HsqlException e) {
                database.logger.appLog.logContext(
                    SimpleLog.LOG_ERROR, "checkpoint did not complete");
            }
        }
    }

    /**
     * @todo no-op in this implementation. To be implemented for connection pooling
     */
    public synchronized void resetSession() {
        throw new HsqlException("", "", 0);
    }

    /**
     *  Registers a transaction SAVEPOINT. A new SAVEPOINT with the
     *  name of an existing one replaces the old SAVEPOINT.
     *
     * @param  name name of the savepoint
     * @throws  HsqlException if there is no current transaction
     */
    public synchronized void savepoint(String name) {

        int index = sessionContext.savepoints.getIndex(name);

        if (index != -1) {
            sessionContext.savepoints.remove(name);
            sessionContext.savepointTimestamps.remove(index);
        }

        sessionContext.savepoints.add(name,
                                      ValuePool.getInt(rowActionList.size()));
        sessionContext.savepointTimestamps.addLast(actionTimestamp);

        try {
            database.logger.writeToLog(this, getSavepointSQL(name));
        } catch (HsqlException e) {}
    }

    /**
     *  Performs a partial transaction ROLLBACK to savepoint.
     *
     * @param  name name of savepoint
     * @throws  HsqlException
     */
    public synchronized void rollbackToSavepoint(String name) {

        if (isClosed) {
            return;
        }

        int index = sessionContext.savepoints.getIndex(name);

        if (index < 0) {
            throw Error.error(ErrorCode.X_3B001, name);
        }

        database.txManager.rollbackSavepoint(this, index);

        try {
            database.logger.writeToLog(this, getSavepointRollbackSQL(name));
        } catch (HsqlException e) {}
    }

    /**
     * Performs a partial transaction ROLLBACK of current savepoint level.
     *
     * @throws  HsqlException
     */
    public synchronized void rollbackToSavepoint() {

        if (isClosed) {
            return;
        }

        String name = (String) sessionContext.savepoints.getKey(0);

        database.txManager.rollbackSavepoint(this, 0);

        try {
            database.logger.writeToLog(this, getSavepointRollbackSQL(name));
        } catch (HsqlException e) {}
    }

    /**
     * Releases a savepoint
     *
     * @param  name name of savepoint
     * @throws  HsqlException if name does not correspond to a savepoint
     */
    public synchronized void releaseSavepoint(String name) {

        // remove this and all later savepoints
        int index = sessionContext.savepoints.getIndex(name);

        if (index < 0) {
            throw Error.error(ErrorCode.X_3B001, name);
        }

        while (sessionContext.savepoints.size() > index) {
            sessionContext.savepoints.remove(sessionContext.savepoints.size()
                                             - 1);
            sessionContext.savepointTimestamps.removeLast();
        }
    }

    /**
     * sets READ ONLY for next transaction only
     *
     * @param  readonly the new value
     */
    public void setReadOnly(boolean readonly) {

        if (!readonly && database.databaseReadOnly) {
            throw Error.error(ErrorCode.DATABASE_IS_READONLY);
        }

        if (isInMidTransaction()) {
            throw Error.error(ErrorCode.X_25001);
        }

        isReadOnly = readonly;
    }

    public synchronized void setReadOnlyDefault(boolean readonly) {

        if (!readonly && database.databaseReadOnly) {
            throw Error.error(ErrorCode.DATABASE_IS_READONLY);
        }

        isReadOnlyDefault = readonly;

        if (!isInMidTransaction()) {
            isReadOnly = isReadOnlyDefault;
        }
    }

    /**
     *  Getter for readonly attribute.
     *
     * @return the current value
     */
    public boolean isReadOnly() {
        return isReadOnly;
    }

    public synchronized boolean isReadOnlyDefault() {
        return isReadOnlyDefault;
    }

    /**
     *  Getter for autoCommit attribute.
     *
     * @return the current value
     */
    public synchronized boolean isAutoCommit() {
        return isAutoCommit;
    }

    public synchronized int getStreamBlockSize() {
        return 512 * 1024;
    }

    public boolean isInMidTransaction() {
        return isTransaction;
    }

    /**
     *  A switch to set scripting on the basis of type of statement executed.
     *  Afterwards the method reponsible for logging uses
     *  isScripting() to determine if logging is required for the executed
     *  statement. (fredt@users)
     *
     * @param  script The new scripting value
     */
    void setScripting(boolean script) {
        this.script = script;
    }

    /**
     * Getter for scripting attribute.
     *
     * @return  scripting for the last statement.
     */
    boolean isScripting() {
        return script;
    }

    /**
     * Retrieves an internal Connection object equivalent to the one
     * that created this Session.
     *
     * @return  internal connection.
     */
    JDBCConnection getInternalConnection() {

        if (intConnection == null) {
            intConnection = new JDBCConnection(this);
        }

        return intConnection;
    }

// boucherb@users 20020810 metadata 1.7.2
//----------------------------------------------------------------
    private final long connectTime = System.currentTimeMillis();

// more effecient for MetaData concerns than checkAdmin

    /**
     * Getter for admin attribute.
     *
     * @return the current value
     */
    public boolean isAdmin() {
        return user.isAdmin();
    }

    /**
     * Getter for connectTime attribute.
     *
     * @return the value
     */
    public long getConnectTime() {
        return connectTime;
    }

    /**
     * Count of acctions in current transaction.
     *
     * @return the current value
     */
    public int getTransactionSize() {
        return rowActionList.size();
    }

    public long getTransactionTimestamp() {
        return transactionTimestamp;
    }

    public Statement compileStatement(String sql) {

        parser.reset(sql);

        Statement cs = parser.compileStatement();

        return cs;
    }

    /**
     * Executes the command encapsulated by the cmd argument.
     *
     * @param cmd the command to execute
     * @return the result of executing the command
     */
    public synchronized Result execute(Result cmd) {

        if (isClosed) {
            return Result.newErrorResult(Error.error(ErrorCode.X_08503));
        }

//        synchronized (database) {
        int type = cmd.getType();

        if (sessionMaxRows == 0) {
            currentMaxRows = cmd.getUpdateCount();
        }

        JavaSystem.gc();

        switch (type) {

            case ResultConstants.LARGE_OBJECT_OP : {
                return performLOBOperation((ResultLob) cmd);
            }
            case ResultConstants.EXECUTE : {
                Result result = executeCompiledStatement(cmd);

                result = performPostExecute(cmd, result);

                return result;
            }
            case ResultConstants.BATCHEXECUTE : {
                Result result = executeCompiledBatchStatement(cmd);

                result = performPostExecute(cmd, result);

                return result;
            }
            case ResultConstants.EXECDIRECT : {
                Result result = executeDirectStatement(cmd);

                result = performPostExecute(cmd, result);

                return result;
            }
            case ResultConstants.BATCHEXECDIRECT : {
                Result result = executeDirectBatchStatement(cmd);

                result = performPostExecute(cmd, result);

                return result;
            }
            case ResultConstants.PREPARE : {
                Statement cs;

                try {
                    cs = database.compiledStatementManager.compile(this, cmd);
                } catch (Throwable t) {
                    String errorString = cmd.getMainString();

                    if (database.getProperties().getErrorLevel()
                            == HsqlDatabaseProperties.NO_MESSAGE) {
                        errorString = null;
                    }

                    return Result.newErrorResult(t, errorString);
                }

                cs.setGeneratedColumnInfo(cmd.getGeneratedResultType(),
                                          cmd.getGeneratedResultMetaData());

                Result result = Result.newPrepareResponse(cs);

                if (cs.getType() == StatementTypes.SELECT_CURSOR) {
                    sessionData.setResultSetProperties(cmd, result);
                }

                return result;
            }
            case ResultConstants.CLOSE_RESULT : {
                closeNavigator(cmd.getResultId());

                return Result.updateZeroResult;
            }
            case ResultConstants.UPDATE_RESULT : {
                Result result = this.executeResultUpdate(cmd);

                return result;
            }
            case ResultConstants.FREESTMT : {
                database.compiledStatementManager.freeStatement(
                    cmd.getStatementID(), sessionId, false);

                return Result.updateZeroResult;
            }
            case ResultConstants.GETSESSIONATTR : {
                int id = cmd.getStatementType();

                return getAttributesResult(id);
            }
            case ResultConstants.SETSESSIONATTR : {
                return setAttributes(cmd);
            }
            case ResultConstants.ENDTRAN : {
                switch (cmd.getActionType()) {

                    case ResultConstants.TX_COMMIT :
                        try {
                            commit(false);
                        } catch (Throwable t) {
                            return Result.newErrorResult(t);
                        }
                        break;

                    case ResultConstants.TX_COMMIT_AND_CHAIN :
                        try {
                            commit(true);
                        } catch (Throwable t) {
                            return Result.newErrorResult(t);
                        }
                        break;

                    case ResultConstants.TX_ROLLBACK :
                        rollback(false);
                        break;

                    case ResultConstants.TX_ROLLBACK_AND_CHAIN :
                        rollback(true);
                        break;

                    case ResultConstants.TX_SAVEPOINT_NAME_RELEASE :
                        try {
                            String name = cmd.getMainString();

                            releaseSavepoint(name);
                        } catch (Throwable t) {
                            return Result.newErrorResult(t);
                        }
                        break;

                    case ResultConstants.TX_SAVEPOINT_NAME_ROLLBACK :
                        try {
                            rollbackToSavepoint(cmd.getMainString());
                        } catch (Throwable t) {
                            return Result.newErrorResult(t);
                        }
                        break;
                }

                return Result.updateZeroResult;
            }
            case ResultConstants.SETCONNECTATTR : {
                switch (cmd.getConnectionAttrType()) {

                    case ResultConstants.SQL_ATTR_SAVEPOINT_NAME :
                        try {
                            savepoint(cmd.getMainString());
                        } catch (Throwable t) {
                            return Result.newErrorResult(t);
                        }

                    // case ResultConstants.SQL_ATTR_AUTO_IPD
                    //   - always true
                    // default: throw - case never happens
                }

                return Result.updateZeroResult;
            }
            case ResultConstants.REQUESTDATA : {
                return sessionData.getDataResultSlice(cmd.getResultId(),
                                                      cmd.getUpdateCount(),
                                                      cmd.getFetchSize());
            }
            case ResultConstants.DISCONNECT : {
                close();

                return Result.updateZeroResult;
            }
            default : {
                return Result.newErrorResult(
                    Error.runtimeError(
                        ErrorCode.U_S0500, "Session.execute()"));
            }
        }

//      }
    }

    private Result performPostExecute(Result command, Result result) {

        if (result.isData()) {
            result = sessionData.getDataResultHead(command, result, isNetwork);
        }

        return result;
    }

    public RowSetNavigatorClient getRows(long navigatorId, int offset,
                                         int blockSize) {
        return sessionData.getRowSetSlice(navigatorId, offset, blockSize);
    }

    public synchronized void closeNavigator(long id) {
        sessionData.closeNavigator(id);
    }

    public Result executeDirectStatement(Result cmd) {

        String        sql = cmd.getMainString();
        HsqlArrayList list;

        try {
            list = parser.compileStatements(sql, cmd.getStatementType());
        } catch (HsqlException e) {
            return Result.newErrorResult(e);
        }

        Result result = null;

        for (int i = 0; i < list.size(); i++) {
            Statement cs = (Statement) list.get(i);

            result = executeCompiledStatement(cs, ValuePool.emptyObjectArray);

            if (result.isError()) {
                break;
            }
        }

        return result;
    }

    public Result executeDirectStatement(String sql) {

        Statement cs;

        parser.reset(sql);

        try {
            cs = parser.compileStatement();
        } catch (HsqlException e) {
            return Result.newErrorResult(e);
        }

        Result result = executeCompiledStatement(cs,
            ValuePool.emptyObjectArray);

        return result;
    }

    public Result executeCompiledStatement(Statement cs, Object[] pvals) {

        Result r;

        if (abortTransaction) {

//            tempActionHistory.add("beginAction aborts" + actionTimestamp);
            rollback(false);

            return Result.newErrorResult(Error.error(ErrorCode.X_40001));
        }

        currentStatement = cs;

        if (cs.isAutoCommitStatement()) {
            try {
                if (isReadOnly()) {
                    throw Error.error(ErrorCode.X_25006);
                }

                /** @todo - special autocommit for backward compatibility */
                commit(false);
            } catch (HsqlException e) {}
        }

        if (!cs.isTransactionStatement()) {
            r                = cs.execute(this);
            currentStatement = null;

            return r;
        }

        while (true) {
            beginAction(cs);

            if (abortTransaction) {
                rollback(false);

                currentStatement = null;

                return Result.newErrorResult(Error.error(ErrorCode.X_40001));
            }

            try {
                latch.await();
            } catch (InterruptedException e) {

                // System.out.println("interrupted");
            }

            if (abortTransaction) {
                rollback(false);

                currentStatement = null;

                return Result.newErrorResult(Error.error(ErrorCode.X_40001));
            }

            //        tempActionHistory.add("sql execute " + cs.sql + " " + actionTimestamp + " " + rowActionList.size());
            sessionContext.pushDynamicArguments(pvals);

            r = cs.execute(this);

            sessionContext.popDynamicArguments();

            lockStatement = currentStatement;

            //        tempActionHistory.add("sql execute end " + actionTimestamp + " " + rowActionList.size());
            endAction(r);

            if (abortTransaction) {
                rollback(false);

                currentStatement = null;

                return Result.newErrorResult(Error.error(ErrorCode.X_40001));
            }

            if (redoAction) {
                redoAction = false;

                try {
                    latch.await();
                } catch (InterruptedException e) {

                    //
                    System.out.println("interrupted");
                }
            } else {
                break;
            }
        }

        if (sessionContext.depth == 0
                && (isAutoCommit || cs.isAutoCommitStatement())) {
            try {
                if (r.isError()) {
                    rollback(false);
                } else {
                    commit(false);
                }
            } catch (Exception e) {
                currentStatement = null;

                return Result.newErrorResult(Error.error(ErrorCode.X_40001));
            }
        }

        currentStatement = null;

        return r;
    }

    private Result executeCompiledBatchStatement(Result cmd) {

        long      csid;
        Statement cs;
        int[]     updateCounts;
        int       count;

        csid = cmd.getStatementID();
        cs   = database.compiledStatementManager.getStatement(this, csid);

        if (cs == null) {

            // invalid sql has been removed already
            return Result.newErrorResult(Error.error(ErrorCode.X_07501));
        }

        count = 0;

        RowSetNavigator nav = cmd.initialiseNavigator();

        updateCounts = new int[nav.getSize()];

        Result generatedResult = null;

        if (cs.hasGeneratedColumns()) {
            generatedResult =
                Result.newDataResult(cs.generatedResultMetaData());
        }

        Result error = null;

        isBatch = true;

        while (nav.hasNext()) {
            Object[] pvals = (Object[]) nav.getNext();
            Result   in    = executeCompiledStatement(cs, pvals);

            // On the client side, iterate over the vals and throw
            // a BatchUpdateException if a batch status value of
            // esultConstants.EXECUTE_FAILED is encountered in the result
            if (in.isUpdateCount()) {
                if (cs.hasGeneratedColumns()) {
                    Object generatedRow = in.getNavigator().getNext();

                    generatedResult.getNavigator().add(generatedRow);
                }

                updateCounts[count++] = in.getUpdateCount();
            } else if (in.isData()) {

                // FIXME:  we don't have what it takes yet
                // to differentiate between things like
                // stored procedure calls to methods with
                // void return type and select statements with
                // a single row/column containg null
                updateCounts[count++] = ResultConstants.SUCCESS_NO_INFO;
            } else if (in.isError()) {
                updateCounts = ArrayUtil.arraySlice(updateCounts, 0, count);
                error        = in;

                break;
            } else {
                throw Error.runtimeError(ErrorCode.U_S0500, "Session");
            }
        }

        isBatch = false;

        sessionData.updateLobUsageForBatch();

        return Result.newBatchedExecuteResponse(updateCounts, generatedResult,
                error);
    }

    private Result executeDirectBatchStatement(Result cmd) {

        int[] updateCounts;
        int   count;

        count = 0;

        RowSetNavigator nav = cmd.initialiseNavigator();

        updateCounts = new int[nav.getSize()];

        Result error = null;

        isBatch = true;

        while (nav.hasNext()) {
            Result   in;
            Object[] data = (Object[]) nav.getNext();
            String   sql  = (String) data[0];

            try {
                in = executeDirectStatement(sql);
            } catch (Throwable t) {
                in = Result.newErrorResult(t);

                // if (t instanceof OutOfMemoryError) {
                // System.gc();
                // }
                // "in" alread equals "err"
                // maybe test for OOME and do a gc() ?
                // t.printStackTrace();
            }

            if (in.isUpdateCount()) {
                updateCounts[count++] = in.getUpdateCount();
            } else if (in.isData()) {

                // FIXME:  we don't have what it takes yet
                // to differentiate between things like
                // stored procedure calls to methods with
                // void return type and select statements with
                // a single row/column containg null
                updateCounts[count++] = ResultConstants.SUCCESS_NO_INFO;
            } else if (in.isError()) {
                updateCounts = ArrayUtil.arraySlice(updateCounts, 0, count);
                error        = in;

                break;
            } else {
                throw Error.runtimeError(ErrorCode.U_S0500, "Session");
            }
        }

        isBatch = false;

        sessionData.updateLobUsageForBatch();

        return Result.newBatchedExecuteResponse(updateCounts, null, error);
    }

    /**
     * Retrieves the result of executing the prepared statement whose csid
     * and parameter values/types are encapsulated by the cmd argument.
     *
     * @return the result of executing the statement
     */
    private Result executeCompiledStatement(Result cmd) {

        Statement cs = cmd.getStatement();

        if (!cs.isValid()) {
            long csid = cmd.getStatementID();

            cs = database.compiledStatementManager.getStatement(this, csid);

            if (cs == null) {

                // invalid sql has been removed already
                return Result.newErrorResult(Error.error(ErrorCode.X_07502));
            }
        }

        Object[] pvals = cmd.getParameterData();

        return executeCompiledStatement(cs, pvals);
    }

    /**
     * Retrieves the result of inserting, updating or deleting a row
     * from an updatable result.
     *
     * @return the result of executing the statement
     */
    private Result executeResultUpdate(Result cmd) {

        long   id         = cmd.getResultId();
        int    actionType = cmd.getActionType();
        Result result     = sessionData.getDataResult(id);

        if (result == null) {
            return Result.newErrorResult(Error.error(ErrorCode.X_24501));
        }

        Object[]        pvals     = cmd.getParameterData();
        Type[]          types     = cmd.metaData.columnTypes;
        StatementQuery  statement = (StatementQuery) result.getStatement();
        QueryExpression qe        = statement.queryExpression;
        Table           baseTable = qe.getBaseTable();
        int[]           columnMap = qe.getBaseTableColumnMap();

        sessionContext.rowUpdateStatement.setRowActionProperties(actionType,
                baseTable, types, columnMap);

        Result resultOut =
            executeCompiledStatement(sessionContext.rowUpdateStatement, pvals);

        return resultOut;
    }

// session DATETIME functions
    long                  currentDateSCN;
    long                  currentTimestampSCN;
    long                  currentMillis;
    private TimestampData currentDate;
    private TimestampData currentTimestamp;
    private TimestampData localTimestamp;
    private TimeData      currentTime;
    private TimeData      localTime;

    /**
     * Returns the current date, unchanged for the duration of the current
     * execution unit (statement).<p>
     *
     * SQL standards require that CURRENT_DATE, CURRENT_TIME and
     * CURRENT_TIMESTAMP are all evaluated at the same point of
     * time in the duration of each SQL statement, no matter how long the
     * SQL statement takes to complete.<p>
     *
     * When this method or a corresponding method for CURRENT_TIME or
     * CURRENT_TIMESTAMP is first called in the scope of a system change
     * number, currentMillis is set to the current system time. All further
     * CURRENT_XXXX calls in this scope will use this millisecond value.
     * (fredt@users)
     */
    public synchronized TimestampData getCurrentDate() {

        resetCurrentTimestamp();

        if (currentDate == null) {
            currentDate = (TimestampData) Type.SQL_DATE.getValue(currentMillis
                    / 1000, 0, timeZoneSeconds);
        }

        return currentDate;
    }

    /**
     * Returns the current time, unchanged for the duration of the current
     * execution unit (statement)
     */
    synchronized TimeData getCurrentTime(boolean withZone) {

        resetCurrentTimestamp();

        if (withZone) {
            if (currentTime == null) {
                int seconds =
                    (int) (HsqlDateTime.getNormalisedTime(currentMillis))
                    / 1000;
                int nanos = (int) (currentMillis % 1000) * 1000000;

                currentTime = new TimeData(seconds, nanos, timeZoneSeconds);
            }

            return currentTime;
        } else {
            if (localTime == null) {
                int seconds =
                    (int) (HsqlDateTime.getNormalisedTime(
                        currentMillis + +timeZoneSeconds * 1000)) / 1000;
                int nanos = (int) (currentMillis % 1000) * 1000000;

                localTime = new TimeData(seconds, nanos, 0);
            }

            return localTime;
        }
    }

    /**
     * Returns the current timestamp, unchanged for the duration of the current
     * execution unit (statement)
     */
    synchronized TimestampData getCurrentTimestamp(boolean withZone) {

        resetCurrentTimestamp();

        if (withZone) {
            if (currentTimestamp == null) {
                int nanos = (int) (currentMillis % 1000) * 1000000;

                currentTimestamp = new TimestampData((currentMillis / 1000),
                                                     nanos, timeZoneSeconds);
            }

            return currentTimestamp;
        } else {
            if (localTimestamp == null) {
                int nanos = (int) (currentMillis % 1000) * 1000000;

                localTimestamp = new TimestampData(currentMillis / 1000
                                                   + timeZoneSeconds, nanos,
                                                       0);
            }

            return localTimestamp;
        }
    }

    private void resetCurrentTimestamp() {

        if (currentTimestampSCN != actionTimestamp) {
            currentTimestampSCN = actionTimestamp;
            currentMillis       = System.currentTimeMillis();
            currentDate         = null;
            currentTimestamp    = null;
            localTimestamp      = null;
            currentTime         = null;
            localTime           = null;
        }
    }

    public int getZoneSeconds() {
        return timeZoneSeconds;
    }

    private Result getAttributesResult(int id) {

        Result   r    = Result.newSessionAttributesResult();
        Object[] data = r.getSingleRowData();

        data[SessionInterface.INFO_ID] = ValuePool.getInt(id);

        switch (id) {

            case SessionInterface.INFO_ISOLATION :
                data[SessionInterface.INFO_INTEGER] =
                    ValuePool.getInt(isolationMode);
                break;

            case SessionInterface.INFO_AUTOCOMMIT :
                data[SessionInterface.INFO_BOOLEAN] =
                    ValuePool.getBoolean(isAutoCommit);
                break;

            case SessionInterface.INFO_CONNECTION_READONLY :
                data[SessionInterface.INFO_BOOLEAN] =
                    ValuePool.getBoolean(isReadOnly);
                break;

            case SessionInterface.INFO_CATALOG :
                data[SessionInterface.INFO_VARCHAR] =
                    database.getCatalogName().name;
                break;
        }

        return r;
    }

    private Result setAttributes(Result r) {

        Object[] row = r.getSessionAttributes();
        int      id  = ((Integer) row[SessionInterface.INFO_ID]).intValue();

        try {
            switch (id) {

                case SessionInterface.INFO_AUTOCOMMIT : {
                    boolean value =
                        ((Boolean) row[SessionInterface.INFO_BOOLEAN])
                            .booleanValue();

                    this.setAutoCommit(value);

                    break;
                }
                case SessionInterface.INFO_CONNECTION_READONLY : {
                    boolean value =
                        ((Boolean) row[SessionInterface.INFO_BOOLEAN])
                            .booleanValue();

                    this.setReadOnlyDefault(value);

                    break;
                }
                case SessionInterface.INFO_ISOLATION : {
                    int value =
                        ((Integer) row[SessionInterface.INFO_INTEGER])
                            .intValue();

                    this.setIsolation(value);

                    break;
                }
                case SessionInterface.INFO_CATALOG : {
                    String value =
                        ((String) row[SessionInterface.INFO_VARCHAR]);

                    this.setCatalog(value);
                }
            }
        } catch (HsqlException e) {
            return Result.newErrorResult(e);
        }

        return Result.updateZeroResult;
    }

    public synchronized Object getAttribute(int id) {

        switch (id) {

            case SessionInterface.INFO_ISOLATION :
                return ValuePool.getInt(isolationMode);

            case SessionInterface.INFO_AUTOCOMMIT :
                return ValuePool.getBoolean(isAutoCommit);

            case SessionInterface.INFO_CONNECTION_READONLY :
                return ValuePool.getBoolean(isReadOnly);

            case SessionInterface.INFO_CATALOG :
                return database.getCatalogName().name;
        }

        return null;
    }

    public synchronized void setAttribute(int id, Object object) {

        switch (id) {

            case SessionInterface.INFO_AUTOCOMMIT : {
                boolean value = ((Boolean) object).booleanValue();

                this.setAutoCommit(value);

                break;
            }
            case SessionInterface.INFO_CONNECTION_READONLY : {
                boolean value = ((Boolean) object).booleanValue();

                this.setReadOnlyDefault(value);

                break;
            }
            case SessionInterface.INFO_ISOLATION : {
                int value = ((Integer) object).intValue();

                this.setIsolation(value);

                break;
            }
            case SessionInterface.INFO_CATALOG : {
                String value = ((String) object);

                this.setCatalog(value);
            }
        }
    }

    // lobs
    public BlobDataID createBlob(long length) {

        long lobID = database.lobManager.createBlob(length);

        if (lobID == 0) {
            throw Error.error(ErrorCode.X_0F502);
        }

        sessionData.addToCreatedLobs(lobID);

        return new BlobDataID(lobID);
    }

    public ClobDataID createClob(long length) {

        long lobID = database.lobManager.createClob(length);

        if (lobID == 0) {
            throw Error.error(ErrorCode.X_0F502);
        }

        sessionData.addToCreatedLobs(lobID);

        return new ClobDataID(lobID);
    }

    public void registerResultLobs(Result result) {
        sessionData.registerLobForResult(result);
    }

    public void allocateResultLob(ResultLob result, InputStream inputStream) {
        sessionData.allocateLobForResult(result, inputStream);
    }

    Result performLOBOperation(ResultLob cmd) {

        long id        = cmd.getLobID();
        int  operation = cmd.getSubType();

        switch (operation) {

            case ResultLob.LobResultTypes.REQUEST_GET_LOB : {
                return database.lobManager.getLob(this, id, cmd.getOffset(),
                                                  cmd.getBlockLength());
            }
            case ResultLob.LobResultTypes.REQUEST_GET_LENGTH : {
                return database.lobManager.getLength(this, id);
            }
            case ResultLob.LobResultTypes.REQUEST_GET_BYTES : {
                return database.lobManager.getBytes(
                    this, id, cmd.getOffset(), (int) cmd.getBlockLength());
            }
            case ResultLob.LobResultTypes.REQUEST_SET_BYTES : {
                return database.lobManager.setBytes(this, id,
                                                    cmd.getByteArray(),
                                                    cmd.getOffset());
            }
            case ResultLob.LobResultTypes.REQUEST_GET_CHARS : {
                return database.lobManager.getChars(
                    this, id, cmd.getOffset(), (int) cmd.getBlockLength());
            }
            case ResultLob.LobResultTypes.REQUEST_SET_CHARS : {
                return database.lobManager.setChars(this, id, cmd.getOffset(),
                                                    cmd.getCharArray());
            }
            case ResultLob.LobResultTypes.REQUEST_TRUNCATE : {
                return database.lobManager.truncate(this, id, cmd.getOffset());
            }
            case ResultLob.LobResultTypes.REQUEST_CREATE_BYTES :
            case ResultLob.LobResultTypes.REQUEST_CREATE_CHARS :
            case ResultLob.LobResultTypes.REQUEST_GET_BYTE_PATTERN_POSITION :
            case ResultLob.LobResultTypes.REQUEST_GET_CHAR_PATTERN_POSITION :
            default : {
                throw Error.runtimeError(ErrorCode.U_S0500, "Session");
            }
        }
    }

    // DatabaseMetaData.getURL should work as specified for
    // internal connections too.
    public String getInternalConnectionURL() {
        return DatabaseURL.S_URL_PREFIX + database.getURI();
    }

    boolean isProcessingScript() {
        return isProcessingScript;
    }

    boolean isProcessingLog() {
        return isProcessingLog;
    }

    // schema object methods
    public void setSchema(String schema) {
        currentSchema = database.schemaManager.getSchemaHsqlName(schema);
    }

    public void setCatalog(String catalog) {

        if (database.getCatalogName().name.equals(catalog)) {
            return;
        }

        throw Error.error(ErrorCode.X_3D000);
    }

    /**
     * If schemaName is null, return the current schema name, else return
     * the HsqlName object for the schema. If schemaName does not exist,
     * throw.
     */
    HsqlName getSchemaHsqlName(String name) {
        return name == null ? currentSchema
                            : database.schemaManager.getSchemaHsqlName(name);
    }

    /**
     * Same as above, but return string
     */
    public String getSchemaName(String name) {
        return name == null ? currentSchema.name
                            : database.schemaManager.getSchemaName(name);
    }

    public HsqlName getCurrentSchemaHsqlName() {
        return currentSchema;
    }

// session tables
    Table[] transitionTables = Table.emptyArray;

    public void setSessionTables(Table[] tables) {
        transitionTables = tables;
    }

    public Table findSessionTable(String name) {

        for (int i = 0; i < transitionTables.length; i++) {
            if (name.equals(transitionTables[i].getName().name)) {
                return transitionTables[i];
            }
        }

        return null;
    }

//
    public int getResultMemoryRowCount() {
        return resultMaxMemoryRows;
    }

    public void setResultMemoryRowCount(int count) {

        if (database.getTempDirectoryPath() != null) {
            if (count <= 0) {
                count = Integer.MAX_VALUE;
            }

            resultMaxMemoryRows = count;
        }
    }

    // warnings
    HsqlArrayList sqlWarnings;

    public void addWarning(HsqlException warning) {

        if (sqlWarnings == null) {
            sqlWarnings = new HsqlArrayList(true);
        }
// A VoltDB extension to avoid memory waste.
        // Only the last warning is ever asked for, so just keep overwriting any existing one.
        // So, why do we need a List? Good question. Just trying to minimize the code change for now.
        else {
            sqlWarnings.set(0, warning);
            return;
        }
// End of VoltDB extension
        sqlWarnings.add(warning);
    }

    public HsqlException[] getAndClearWarnings() {

        if (sqlWarnings == null) {
            return new HsqlException[0];
        }

        HsqlException[] array = new HsqlException[sqlWarnings.size()];

        sqlWarnings.toArray(array);
        sqlWarnings.clear();

        return array;
    }

    public HsqlException getLastWarnings() {

        if (sqlWarnings == null || sqlWarnings.size() == 0) {
            return null;
        }

        return (HsqlException) sqlWarnings.get(sqlWarnings.size() - 1);
    }

    public void clearWarnings() {

        if (sqlWarnings != null) {
            sqlWarnings.clear();
        }
    }

    // services
    Scanner          secondaryScanner;
    SimpleDateFormat simpleDateFormat;
    SimpleDateFormat simpleDateFormatGMT;
    Random           randomGenerator = new Random();

    public double random(long seed) {

        randomGenerator.setSeed(seed);

        return randomGenerator.nextDouble();
    }

    public double random() {
        return randomGenerator.nextDouble();
    }

    public Scanner getScanner() {

        if (secondaryScanner == null) {
            secondaryScanner = new Scanner();
        }

        return secondaryScanner;
    }

    public SimpleDateFormat getSimpleDateFormat() {

        if (simpleDateFormat == null) {
            simpleDateFormat = new SimpleDateFormat("MMMM", Locale.ENGLISH);

            SimpleTimeZone zone = new SimpleTimeZone(timeZoneSeconds,
                "hsqldb");
            Calendar cal = new GregorianCalendar(zone);

            simpleDateFormat.setCalendar(cal);
        }

        return simpleDateFormat;
    }

    public SimpleDateFormat getSimpleDateFormatGMT() {

        if (simpleDateFormatGMT == null) {
            simpleDateFormatGMT = new SimpleDateFormat("MMMM", Locale.ENGLISH);

            Calendar cal = new GregorianCalendar(TimeZone.getTimeZone("GMT"));

            simpleDateFormatGMT.setCalendar(cal);
        }

        return simpleDateFormatGMT;
    }

    // SEQUENCE current values
    void logSequences() {

        OrderedHashSet set = sessionData.sequenceUpdateSet;

        if (set == null || set.isEmpty()) {
            return;
        }

        for (int i = 0, size = set.size(); i < size; i++) {
            NumberSequence sequence = (NumberSequence) set.get(i);

            database.logger.writeSequenceStatement(this, sequence);
        }

        sessionData.sequenceUpdateSet.clear();
    }

    //
    static String getSavepointSQL(String name) {

        StringBuffer sb = new StringBuffer(Tokens.T_SAVEPOINT);

        sb.append(' ').append('"').append(name).append('"');

        return sb.toString();
    }

    static String getSavepointRollbackSQL(String name) {

        StringBuffer sb = new StringBuffer();

        sb.append(Tokens.T_ROLLBACK).append(' ').append(Tokens.T_TO).append(
            ' ');
        sb.append(Tokens.T_SAVEPOINT).append(' ');
        sb.append('"').append(name).append('"');

        return sb.toString();
    }

    String getStartTransactionSQL() {

        StringBuffer sb = new StringBuffer();

        sb.append(Tokens.T_START).append(' ').append(Tokens.T_TRANSACTION);

        if (isolationMode != isolationModeDefault) {
            sb.append(' ');
            appendIsolationSQL(sb, isolationMode);
        }

        return sb.toString();
    }

    String getTransactionIsolationSQL() {

        StringBuffer sb = new StringBuffer();

        sb.append(Tokens.T_SET).append(' ').append(Tokens.T_TRANSACTION);
        sb.append(' ');
        appendIsolationSQL(sb, isolationMode);

        return sb.toString();
    }

    String getSessionIsolationSQL() {

        StringBuffer sb = new StringBuffer();

        sb.append(Tokens.T_SET).append(' ').append(Tokens.T_SESSION);
        sb.append(' ').append(Tokens.T_CHARACTERISTICS).append(' ');
        sb.append(Tokens.T_AS).append(' ');
        appendIsolationSQL(sb, isolationModeDefault);

        return sb.toString();
    }

    static void appendIsolationSQL(StringBuffer sb, int isolationLevel) {

        sb.append(Tokens.T_ISOLATION).append(' ');
        sb.append(Tokens.T_LEVEL).append(' ');

        switch (isolationLevel) {

            case SessionInterface.TX_READ_UNCOMMITTED :
            case SessionInterface.TX_READ_COMMITTED :
                sb.append(Tokens.T_READ).append(' ');
                sb.append(Tokens.T_COMMITTED);
                break;

            case SessionInterface.TX_REPEATABLE_READ :
            case SessionInterface.TX_SERIALIZABLE :
                sb.append(Tokens.T_SERIALIZABLE);
                break;
        }
    }

    String getSetSchemaStatement() {
        return "SET SCHEMA " + currentSchema.statementName;
    }

    /************************* Volt DB Extensions *************************/

    // schema object methods
    public void setSchemaNoThrow(String schema) {
        currentSchema = database.schemaManager.getSchemaHsqlNameNoThrow(schema, currentSchema);
    }

    long nextExpressionNodeId = 1;
    java.util.Map<Long, Long> hsqlExpressionNodeIdsToVoltNodeIds = new java.util.HashMap<Long, Long>();

    public long getNodeIdForExpression(long hsqlId) {
        Long id = hsqlExpressionNodeIdsToVoltNodeIds.get(hsqlId);
        if (id == null) {
            id = nextExpressionNodeId++;
            hsqlExpressionNodeIdsToVoltNodeIds.put(hsqlId, id);
        }
        return id;
    }

    public void resetVoltNodeIds() {
        nextExpressionNodeId = 1;
        hsqlExpressionNodeIdsToVoltNodeIds.clear();
    }
    /**********************************************************************/
}
