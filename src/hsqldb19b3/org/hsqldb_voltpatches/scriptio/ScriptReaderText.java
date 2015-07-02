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


package org.hsqldb_voltpatches.scriptio;

import java.io.BufferedInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.util.zip.GZIPInputStream;

import org.hsqldb_voltpatches.Database;
import org.hsqldb_voltpatches.HsqlException;
import org.hsqldb_voltpatches.Session;
import org.hsqldb_voltpatches.Statement;
import org.hsqldb_voltpatches.StatementTypes;
import org.hsqldb_voltpatches.error.Error;
import org.hsqldb_voltpatches.error.ErrorCode;
import org.hsqldb_voltpatches.lib.LineReader;
import org.hsqldb_voltpatches.lib.StringConverter;
import org.hsqldb_voltpatches.map.ValuePool;
import org.hsqldb_voltpatches.result.Result;
import org.hsqldb_voltpatches.rowio.RowInputTextLog;
import org.hsqldb_voltpatches.types.Type;

/**
 * Handles operations involving reading back a script or log file written
 * out by ScriptWriterText. This implementation
 * corresponds to ScriptWriterText.
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 *  @version 2.3.0
 *  @since 1.7.2
 */
public class ScriptReaderText extends ScriptReaderBase {

    // this is used only to enable reading one logged line at a time
    LineReader      dataStreamIn;
    RowInputTextLog rowIn;
    boolean         isInsert;

    public ScriptReaderText(Database db) {
        super(db);
    }

    public ScriptReaderText(Database db, String fileName,
                            boolean compressed) throws IOException {

        super(db);

        InputStream inputStream =
            database.logger.getFileAccess().openInputStreamElement(fileName);

        inputStream = new BufferedInputStream(inputStream);

        if (compressed) {
            inputStream = new GZIPInputStream(inputStream);
        }

        dataStreamIn = new LineReader(inputStream,
                                      ScriptWriterText.ISO_8859_1);
        rowIn = new RowInputTextLog(db.databaseProperties.isVersion18());
    }

    public ScriptReaderText(Database db, InputStream inputStream) {

        super(db);

//        inputStream = new BufferedInputStream(inputStream);
        dataStreamIn = new LineReader(inputStream,
                                      ScriptWriterText.ISO_8859_1);
        rowIn = new RowInputTextLog(db.databaseProperties.isVersion18());
    }

    protected void readDDL(Session session) {

        for (; readLoggedStatement(session); ) {
            Statement cs     = null;
            Result    result = null;

            if (rowIn.getStatementType() == INSERT_STATEMENT) {
                isInsert = true;

                break;
            }

            try {
                cs = session.compileStatement(statement);
                result = session.executeCompiledStatement(cs,
                        ValuePool.emptyObjectArray, 0);
            } catch (HsqlException e) {
                result = Result.newErrorResult(e);
            }

            if (result.isError()) {

                // handle grants on math and library routines in old versions
                if (cs == null) {}
                else if (cs.getType() == StatementTypes.GRANT) {
                    continue;
                } else if (cs.getType() == StatementTypes.CREATE_ROUTINE) {

                    // ignore legacy references
                    if (result.getMainString().indexOf("org.hsqldb_voltpatches.Library")
                            > -1) {
                        continue;
                    }
                }
            }

            if (result.isError()) {
                database.logger.logWarningEvent(result.getMainString(),
                                                result.getException());

                if (cs != null
                        && cs.getType() == StatementTypes.CREATE_ROUTINE) {
                    continue;
                }

                HsqlException e =
                    Error.error(result.getException(),
                                ErrorCode.ERROR_IN_SCRIPT_FILE,
                                ErrorCode.M_DatabaseScriptReader_read,
                                new Object[] {database.getCanonicalPath(),
                    new Integer(lineCount), result.getMainString()
                });

                handleException(e);
            }
        }
    }

    protected void readExistingData(Session session) {

        try {
            String tablename = null;

            // fredt - needed for forward referencing FK constraints
            database.setReferentialIntegrity(false);

            for (; isInsert || readLoggedStatement(session);
                    isInsert = false) {
                if (statementType == SET_SCHEMA_STATEMENT) {
                    session.setSchema(currentSchema);

                    continue;
                } else if (statementType == INSERT_STATEMENT) {
                    if (!rowIn.getTableName().equals(tablename)) {
                        tablename = rowIn.getTableName();

                        String schema = session.getSchemaName(currentSchema);

                        currentTable =
                            database.schemaManager.getUserTable(session,
                                tablename, schema);
                        currentStore =
                            database.persistentStoreCollection.getStore(
                                currentTable);
                    }

                    try {
                        currentTable.insertFromScript(session, currentStore,
                                                      rowData);
                    } catch (HsqlException ex) {
                        handleException(ex);
                    }
                } else {
                    throw Error.error(ErrorCode.ERROR_IN_SCRIPT_FILE,
                                      statement);
                }
            }

            database.setReferentialIntegrity(true);
        } catch (Throwable t) {
            database.logger.logSevereEvent("readExistingData failed", t);

            throw Error.error(t, ErrorCode.ERROR_IN_SCRIPT_FILE,
                              ErrorCode.M_DatabaseScriptReader_read,
                              new Object[] {
                new Integer(lineCount), t.toString()
            });
        }
    }

    public boolean readLoggedStatement(Session session) {

        if (!sessionChanged) {

            //fredt temporary solution - should read bytes directly from buffer
            try {
                rawStatement = dataStreamIn.readLine();
            } catch (EOFException e) {
                return false;
            } catch (IOException e) {
                throw Error.error(e, ErrorCode.FILE_IO_ERROR, null);
            }

            lineCount++;

            //        System.out.println(lineCount);
            statement = StringConverter.unicodeStringToString(rawStatement);

            if (statement == null) {
                return false;
            }
        }

        processStatement(session);

        return true;
    }

    void processStatement(Session session) {

        if (statement.startsWith("/*C")) {
            int endid = statement.indexOf('*', 4);

            sessionNumber  = Integer.parseInt(statement.substring(3, endid));
            statement      = statement.substring(endid + 2);
            sessionChanged = true;
            statementType  = SESSION_ID;

            return;
        }

        sessionChanged = false;

        rowIn.setSource(statement);

        statementType = rowIn.getStatementType();

        if (statementType == ANY_STATEMENT) {
            rowData      = null;
            currentTable = null;

            return;
        } else if (statementType == COMMIT_STATEMENT) {
            rowData      = null;
            currentTable = null;

            return;
        } else if (statementType == SET_SCHEMA_STATEMENT) {
            rowData       = null;
            currentTable  = null;
            currentSchema = rowIn.getSchemaName();

            return;
        }

        String name   = rowIn.getTableName();
        String schema = session.getCurrentSchemaHsqlName().name;

        currentTable = database.schemaManager.getUserTable(session, name,
                schema);
        currentStore =
            database.persistentStoreCollection.getStore(currentTable);

        Type[] colTypes;

        if (statementType == INSERT_STATEMENT) {
            colTypes = currentTable.getColumnTypes();
        } else if (currentTable.hasPrimaryKey()) {
            colTypes = currentTable.getPrimaryKeyTypes();
        } else {
            colTypes = currentTable.getColumnTypes();
        }

        try {
            rowData = rowIn.readData(colTypes);
        } catch (IOException e) {
            throw Error.error(e, ErrorCode.FILE_IO_ERROR, null);
        }
    }

    public void close() {

        try {
            dataStreamIn.close();

            if (scrwriter != null) {
                scrwriter.close();
            }

            database.recoveryMode = 0;
        } catch (Exception e) {}
    }

    private void handleException(HsqlException e) {

        if (database.recoveryMode == 0) {
            throw e;
        }

        if (scrwriter == null) {
            String name = database.getPath() + ".reject";

            scrwriter = new ScriptWriterText(database, name, true, true, true);
        }

        try {
            scrwriter.writeLogStatement(null, rawStatement);
        } catch (Throwable t) {}
    }
}
