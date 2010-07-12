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


package org.hsqldb_voltpatches.persist;

import java.io.EOFException;

import org.hsqldb_voltpatches.Database;
import org.hsqldb_voltpatches.Error;
import org.hsqldb_voltpatches.ErrorCode;
import org.hsqldb_voltpatches.Session;
import org.hsqldb_voltpatches.lib.IntKeyHashMap;
import org.hsqldb_voltpatches.lib.SimpleLog;
import org.hsqldb_voltpatches.lib.StopWatch;
import org.hsqldb_voltpatches.result.Result;
import org.hsqldb_voltpatches.scriptio.ScriptReaderBase;

/**
 * Restores the state of a Database instance from an SQL log file. <p>
 *
 * If there is an error, processing stops at that line and the message is
 * logged to the application log. If memory runs out, an exception is thrown.
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 1.9.0
 * @since 1.7.2
 */
public class ScriptRunner {

    /**
     *  This is used to read the *.log file and manage any necessary
     *  transaction rollback.
     */
    public static void runScript(Database database, String logFilename,
                                 int logType) {

        IntKeyHashMap sessionMap = new IntKeyHashMap();
        Session       current    = null;
        int           currentId  = 0;

        database.setReferentialIntegrity(false);

        ScriptReaderBase scr = null;
        String           statement;
        int              statementType;

        try {
            StopWatch sw = new StopWatch();

            scr = ScriptReaderBase.newScriptReader(database, logFilename,
                                                   logType);

            while (scr.readLoggedStatement(current)) {
                int sessionId = scr.getSessionNumber();

                if (current == null || currentId != sessionId) {
                    currentId = sessionId;
                    current   = (Session) sessionMap.get(currentId);

                    if (current == null) {
                        current =
                            database.getSessionManager().newSession(database,
                                database.getUserManager().getSysUser(), false,
                                true, 0);

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
                        result    = current.executeDirectStatement(statement);

                        if (result != null && result.isError()) {
                            if (result.getException() != null) {
                                throw result.getException();
                            }

                            throw Error.error(result);
                        }
                        break;

                    case ScriptReaderBase.SEQUENCE_STATEMENT :
                        scr.getCurrentSequence().reset(scr.getSequenceValue());
                        break;

                    case ScriptReaderBase.COMMIT_STATEMENT :
                        current.commit(false);
                        break;

                    case ScriptReaderBase.INSERT_STATEMENT : {
                        current.beginAction(null);

                        Object[] data = scr.getData();

                        scr.getCurrentTable().insertNoCheckFromLog(current,
                                data);
                        current.endAction(Result.updateOneResult);

                        break;
                    }
                    case ScriptReaderBase.DELETE_STATEMENT : {
                        current.beginAction(null);

                        Object[] data = scr.getData();

                        scr.getCurrentTable().deleteNoCheckFromLog(current,
                                data);
                        current.endAction(Result.updateOneResult);

                        break;
                    }
                    case ScriptReaderBase.SET_SCHEMA_STATEMENT : {
                        current.setSchema(scr.getCurrentSchema());
                    }
                }

                if (current.isClosed()) {
                    sessionMap.remove(currentId);
                }
            }
        } catch (Throwable e) {
            String message;

            // catch out-of-memory errors and terminate
            if (e instanceof EOFException) {

                // end of file - normal end
            } else if (e instanceof OutOfMemoryError) {
                message = "out of memory processing " + logFilename
                          + " line: " + scr.getLineNumber();

                database.logger.appLog.logContext(SimpleLog.LOG_ERROR,
                                                  message);

                throw Error.error(ErrorCode.OUT_OF_MEMORY);
            } else {

                // stop processing on bad log line
                message = logFilename + " line: " + scr.getLineNumber() + " "
                          + e.toString();

                database.logger.appLog.logContext(SimpleLog.LOG_ERROR,
                                                  message);
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
