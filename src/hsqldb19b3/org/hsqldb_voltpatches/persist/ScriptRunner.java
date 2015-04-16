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


package org.hsqldb_voltpatches.persist;

import java.io.EOFException;
import java.io.InputStream;

import org.hsqldb_voltpatches.ColumnSchema;
import org.hsqldb_voltpatches.Database;
import org.hsqldb_voltpatches.HsqlException;
import org.hsqldb_voltpatches.HsqlNameManager.HsqlName;
import org.hsqldb_voltpatches.Row;
import org.hsqldb_voltpatches.Session;
import org.hsqldb_voltpatches.Statement;
import org.hsqldb_voltpatches.StatementDML;
import org.hsqldb_voltpatches.StatementSchema;
import org.hsqldb_voltpatches.StatementTypes;
import org.hsqldb_voltpatches.Table;
import org.hsqldb_voltpatches.error.Error;
import org.hsqldb_voltpatches.error.ErrorCode;
import org.hsqldb_voltpatches.lib.IntKeyHashMap;
import org.hsqldb_voltpatches.lib.StopWatch;
import org.hsqldb_voltpatches.map.ValuePool;
import org.hsqldb_voltpatches.result.Result;
import org.hsqldb_voltpatches.scriptio.ScriptReaderBase;
import org.hsqldb_voltpatches.scriptio.ScriptReaderDecode;
import org.hsqldb_voltpatches.scriptio.ScriptReaderText;
import org.hsqldb_voltpatches.types.Type;

/**
 * Restores the state of a Database instance from an SQL log file. <p>
 *
 * If there is an error, processing stops at that line and the message is
 * logged to the application log. If memory runs out, an exception is thrown.
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 2.2.7
 * @since 1.7.2
 */
public class ScriptRunner {

    public static void runScript(Database database, InputStream inputStream) {

        Crypto           crypto = database.logger.getCrypto();
        ScriptReaderBase scr;

        if (crypto == null) {
            scr = new ScriptReaderText(database, inputStream);
        } else {
            try {
                scr = new ScriptReaderDecode(database, inputStream, crypto,
                                             true);
            } catch (Throwable e) {
                database.logger.logSevereEvent("opening log file", e);

                return;
            }
        }

        runScript(database, scr);
    }

    /**
     *  This is used to read the *.log file and manage any necessary
     *  transaction rollback.
     */
    public static void runScript(Database database, String logFilename) {

        Crypto           crypto = database.logger.getCrypto();
        ScriptReaderBase scr;

        try {
            if (crypto == null) {
                scr = new ScriptReaderText(database, logFilename, false);
            } else {
                scr = new ScriptReaderDecode(database, logFilename, crypto,
                                             true);
            }
        } catch (Throwable e) {

            // catch out-of-memory errors and terminate
            if (e instanceof EOFException) {

                // end of file - normal end
            } else {

                // stop processing on bad script line
                database.logger.logSevereEvent("opening log file", e);
            }

            return;
        }

        runScript(database, scr);
    }

    private static void runScript(Database database, ScriptReaderBase scr) {

        IntKeyHashMap sessionMap = new IntKeyHashMap();
        Session       current    = null;
        int           currentId  = 0;
        String        statement;
        int           statementType;
        Statement dummy = new StatementDML(StatementTypes.UPDATE_CURSOR,
                                           StatementTypes.X_SQL_DATA_CHANGE,
                                           null);
        String databaseFile = database.getPath();
        boolean fullReplay = database.getURLProperties().isPropertyTrue(
            HsqlDatabaseProperties.hsqldb_full_log_replay);

        dummy.setCompileTimestamp(Long.MAX_VALUE);
        database.setReferentialIntegrity(false);

        try {
            StopWatch sw = new StopWatch();

            while (scr.readLoggedStatement(current)) {
                int sessionId = scr.getSessionNumber();

                if (current == null || currentId != sessionId) {
                    currentId = sessionId;
                    current   = (Session) sessionMap.get(currentId);

                    if (current == null) {
                        current =
                            database.getSessionManager().newSessionForLog(
                                database);

                        sessionMap.put(currentId, current);
                    }
                }

                if (current.isClosed()) {
                    sessionMap.remove(currentId);

                    continue;
                }

                Result result = null;

                statementType = scr.getStatementType();

                switch (statementType) {

                    case ScriptReaderBase.ANY_STATEMENT :
                        statement = scr.getLoggedStatement();

                        Statement cs;

                        try {
                            cs = current.compileStatement(statement);

                            if (database.getProperties().isVersion18()) {

                                // convert BIT columns in .log to BOOLEAN
                                if (cs.getType()
                                        == StatementTypes.CREATE_TABLE) {
                                    Table table =
                                        (Table) ((StatementSchema) cs)
                                            .getArguments()[0];

                                    for (int i = 0; i < table.getColumnCount();
                                            i++) {
                                        ColumnSchema column =
                                            table.getColumn(i);

                                        if (column.getDataType().isBitType()) {
                                            column.setType(Type.SQL_BOOLEAN);
                                        }
                                    }
                                }
                            }

                            result = current.executeCompiledStatement(cs,
                                    ValuePool.emptyObjectArray, 0);
                        } catch (Throwable e) {
                            result = Result.newErrorResult(e);
                        }

                        if (result != null && result.isError()) {
                            if (result.getException() != null) {
                                throw result.getException();
                            }

                            throw Error.error(result);
                        }
                        break;

                    case ScriptReaderBase.COMMIT_STATEMENT :
                        current.commit(false);
                        break;

                    case ScriptReaderBase.INSERT_STATEMENT : {
                        current.sessionContext.currentStatement = dummy;

                        current.beginAction(dummy);

                        Object[] data = scr.getData();

                        scr.getCurrentTable().insertNoCheckFromLog(current,
                                data);
                        current.endAction(Result.updateOneResult);

                        break;
                    }
                    case ScriptReaderBase.DELETE_STATEMENT : {
                        current.sessionContext.currentStatement = dummy;

                        current.beginAction(dummy);

                        Table           table = scr.getCurrentTable();
                        PersistentStore store = table.getRowStore(current);
                        Object[]        data  = scr.getData();
                        Row row = table.getDeleteRowFromLog(current, data);

                        if (row != null) {
                            current.addDeleteAction(table, store, row, null);
                        }

                        current.endAction(Result.updateOneResult);

                        break;
                    }
                    case ScriptReaderBase.SET_SCHEMA_STATEMENT : {
                        HsqlName name =
                            database.schemaManager.findSchemaHsqlName(
                                scr.getCurrentSchema());

                        current.setCurrentSchemaHsqlName(name);

                        break;
                    }
                    case ScriptReaderBase.SESSION_ID : {
                        break;
                    }
                }

                if (current.isClosed()) {
                    sessionMap.remove(currentId);
                }
            }
        } catch (HsqlException e) {

            // stop processing on bad log line
            String error = "statement error processing log " + databaseFile
                           + "line: " + scr.getLineNumber();

            database.logger.logSevereEvent(error, e);

            if (fullReplay) {
                throw Error.error(e, ErrorCode.ERROR_IN_SCRIPT_FILE, error);
            }
        } catch (OutOfMemoryError e) {
            String error = "out of memory processing log" + databaseFile
                           + " line: " + scr.getLineNumber();

            // catch out-of-memory errors and terminate
            database.logger.logSevereEvent(error, e);

            throw Error.error(ErrorCode.OUT_OF_MEMORY);
        } catch (Throwable e) {

            // stop processing on bad script line
            String error = "statement error processing log " + databaseFile
                           + "line: " + scr.getLineNumber();

            database.logger.logSevereEvent(error, e);

            if (fullReplay) {
                throw Error.error(e, ErrorCode.ERROR_IN_SCRIPT_FILE, error);
            }
        } finally {
            if (scr != null) {
                scr.close();
            }

            database.getSessionManager().closeAllSessions();
            database.setReferentialIntegrity(true);
        }
    }
}
