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


package org.hsqldb_voltpatches.scriptio;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import org.hsqldb_voltpatches.Statement;
import org.hsqldb_voltpatches.Database;
import org.hsqldb_voltpatches.Error;
import org.hsqldb_voltpatches.ErrorCode;
import org.hsqldb_voltpatches.HsqlException;
import org.hsqldb_voltpatches.Session;
import org.hsqldb_voltpatches.StatementTypes;
import org.hsqldb_voltpatches.lib.SimpleLog;
import org.hsqldb_voltpatches.lib.StringConverter;
import org.hsqldb_voltpatches.result.Result;
import org.hsqldb_voltpatches.rowio.RowInputTextLog;
import org.hsqldb_voltpatches.types.Type;
import org.hsqldb_voltpatches.HsqlNameManager.HsqlName;
import org.hsqldb_voltpatches.store.ValuePool;

/**
 * Handles operations involving reading back a script or log file written
 * out by ScriptWriterText. This implementation
 * corresponds to ScriptWriterText.
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 *  @version 1.8.0
 *  @since 1.7.2
 */
public class ScriptReaderText extends ScriptReaderBase {

    // this is used only to enable reading one logged line at a time
    BufferedReader  dataStreamIn;
    RowInputTextLog rowIn;
    boolean         isInsert;

    ScriptReaderText(Database db, String file) throws IOException {

        super(db, file);

        rowIn = new RowInputTextLog();
    }

    protected void openFile() throws IOException {

        InputStream d = db.isFilesInJar()
                        ? getClass().getResourceAsStream(fileName)
                        : db.getFileAccess().openInputStreamElement(fileName);

        dataStreamIn = new BufferedReader(
            new InputStreamReader(new BufferedInputStream(d)));
    }

    protected void readDDL(Session session) throws IOException {

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
                        ValuePool.emptyObjectArray);

                if (cs.getType() == StatementTypes.CREATE_SCHEMA) {
                    HsqlName name = cs.getSchemalName();

                    session.setSchema(name.name);
                }
            } catch (HsqlException e) {
                result = Result.newErrorResult(e);
            }

            if (result.isError()) {

                // handle grants on math and library routines in old versions
                if (cs == null) {}
                else {
                    if (cs.getType() == StatementTypes.GRANT) {
                        continue;
                    }
                }

                //
            }

            if (result.isError()) {
                db.logger.appLog.logContext(SimpleLog.LOG_ERROR,
                                            result.getMainString());

                throw Error.error(ErrorCode.ERROR_IN_SCRIPT_FILE,
                                  ErrorCode.M_DatabaseScriptReader_readDDL,
                                  new Object[] {
                    new Integer(lineCount), result.getMainString()
                });
            }
        }
    }

    protected void readExistingData(Session session)
    throws IOException {

        try {
            String tablename = null;

            // fredt - needed for forward referencing FK constraints
            db.setReferentialIntegrity(false);

            for (; isInsert || readLoggedStatement(session);
                    isInsert = false) {
                if (statementType == SET_SCHEMA_STATEMENT) {
                    session.setSchema(currentSchema);

                    continue;
                } else if (statementType == INSERT_STATEMENT) {
                    if (!rowIn.getTableName().equals(tablename)) {
                        tablename = rowIn.getTableName();

                        String schema = session.getSchemaName(currentSchema);

                        currentTable = db.schemaManager.getUserTable(session,
                                tablename, schema);
                        currentStore = db.persistentStoreCollection.getStore(
                            currentTable);
                    }

                    currentTable.insertFromScript(currentStore, rowData);
                }
            }

            db.setReferentialIntegrity(true);
        } catch (Exception e) {
            db.logger.appLog.logContext(e, null);

            throw Error.error(
                ErrorCode.ERROR_IN_SCRIPT_FILE,
                ErrorCode.M_DatabaseScriptReader_readExistingData,
                new Object[] {
                new Integer(lineCount), e.toString()
            });
        }
    }

    public boolean readLoggedStatement(Session session) throws IOException {

        //fredt temporary solution - should read bytes directly from buffer
        String s = dataStreamIn.readLine();

        lineCount++;

//        System.out.println(lineCount);
        statement = StringConverter.unicodeStringToString(s);

        if (statement == null) {
            return false;
        }

        processStatement(session);

        return true;
    }

    private void processStatement(Session session) throws IOException {

        try {
            if (statement.startsWith("/*C")) {
                int endid = statement.indexOf('*', 4);

                sessionNumber = Integer.parseInt(statement.substring(3,
                        endid));
                statement = statement.substring(endid + 2);
            }

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

            currentTable = db.schemaManager.getUserTable(session, name,
                    schema);
            currentStore = db.persistentStoreCollection.getStore(currentTable);

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
            } catch (Exception e) {
                throw e;
            }
        } catch (Exception e) {
            throw new IOException(e.toString());
        }
    }

    public void close() {

        try {
            dataStreamIn.close();
        } catch (Exception e) {}
    }
}
