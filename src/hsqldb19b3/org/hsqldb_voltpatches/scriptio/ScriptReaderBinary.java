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
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;

import org.hsqldb_voltpatches.Database;
import org.hsqldb_voltpatches.Error;
import org.hsqldb_voltpatches.ErrorCode;
import org.hsqldb_voltpatches.Session;
import org.hsqldb_voltpatches.Table;
import org.hsqldb_voltpatches.lib.SimpleLog;
import org.hsqldb_voltpatches.navigator.RowSetNavigator;
import org.hsqldb_voltpatches.persist.PersistentStore;
import org.hsqldb_voltpatches.result.Result;
import org.hsqldb_voltpatches.rowio.RowInputBinary;
import org.hsqldb_voltpatches.rowio.RowInputInterface;

/**
 * Reader corresponding to BinaryDatabaseScritReader.
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 1.7.2
 * @since 1.7.2
 */
class ScriptReaderBinary extends ScriptReaderBase {

    private RowInputBinary    rowIn;
    protected DataInputStream dataStreamIn;

    ScriptReaderBinary(Database db, String file) throws IOException {

        super(db, file);

        rowIn = new RowInputBinary();
    }

    protected void openFile() throws IOException {

        InputStream d = db.isFilesInJar()
                        ? getClass().getResourceAsStream(fileName)
                        : db.getFileAccess().openInputStreamElement(fileName);

        dataStreamIn = new DataInputStream(new BufferedInputStream(d,
                1 << 13));
    }

    protected void readDDL(Session session) throws IOException {

        Result r = Result.newResult(dataStreamIn, rowIn);

        r.readAdditionalResults(session, dataStreamIn, rowIn);

        RowSetNavigator nav = r.initialiseNavigator();

        while (nav.hasNext()) {
            Object[] data   = (Object[]) nav.getNext();
            String   s      = (String) data[0];
            Result   result = session.executeDirectStatement(s);

            if (result.isError()) {
                db.logger.appLog.logContext(SimpleLog.LOG_ERROR,
                                            result.getMainString());

                throw Error.error(result);
            }
        }
    }

    protected void readExistingData(Session session) throws IOException {

        for (;;) {
            String s = readTableInit();

            if (s == null) {
                break;
            }

            String          schema = session.getSchemaName(currentSchema);
            Table t = db.schemaManager.getUserTable(session, s, schema);
            PersistentStore store  = db.persistentStoreCollection.getStore(t);
            int             j      = 0;

            for (j = 0; ; j++) {
                if (!readRow(store, t)) {
                    break;
                }
            }

            int checkCount = readTableTerm();

            if (j != checkCount) {
                throw Error.error(ErrorCode.ERROR_IN_SCRIPT_FILE,
                                  ErrorCode.M_ERROR_IN_BINARY_SCRIPT_1,
                                  new Object[] {
                    s, new Integer(j), new Integer(checkCount)
                });
            }
        }
    }

    // int : row size (0 if no more rows) ,
    // BinaryServerRowInput : row (column values)
    protected boolean readRow(PersistentStore store,
                              Table t) throws IOException {

        boolean more = readRow(rowIn, 0);

        if (!more) {
            return false;
        }

        Object[] data = rowIn.readData(t.getColumnTypes());

        t.insertFromScript(store, data);

        return true;
    }

    // int : rowcount
    protected int readTableTerm() throws IOException {
        return dataStreamIn.readInt();
    }

    // int : headersize (0 if no more tables), String : tablename, int : operation,
    // String : schemaname
    protected String readTableInit() throws IOException {

        boolean more = readRow(rowIn, 0);

        if (!more) {
            return null;
        }

        String s = rowIn.readString();

        // operation is always INSERT
        int checkOp = rowIn.readInt();

        if (checkOp == ScriptWriterBase.INSERT_WITH_SCHEMA) {
            currentSchema = rowIn.readString();
        } else {
            currentSchema = null;
        }

        if (checkOp != ScriptWriterBase.INSERT
                && checkOp != ScriptWriterBase.INSERT_WITH_SCHEMA) {
            throw Error.error(ErrorCode.ERROR_IN_SCRIPT_FILE,
                              ErrorCode.M_ERROR_IN_BINARY_SCRIPT_2);
        }

        return s;
    }

    boolean readRow(RowInputInterface rowin, int pos) throws IOException {

        try {
            int length = dataStreamIn.readInt();
            int count  = 4;

            if (length == 0) {
                return false;
            }

            rowin.resetRow(pos, length);
            dataStreamIn.readFully(rowin.getBuffer(), count, length - count);

            return true;
        } catch (EOFException e) {
            return false;
        }
    }

    public boolean readLoggedStatement(Session session) throws IOException {
        return false;
    }

    public void close() {

        try {
            dataStreamIn.close();
        } catch (IOException e) {}
    }
}
