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

import java.io.IOException;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.util.zip.GZIPOutputStream;

import org.hsqldb_voltpatches.Database;
import org.hsqldb_voltpatches.HsqlNameManager;
import org.hsqldb_voltpatches.HsqlNameManager.HsqlName;
import org.hsqldb_voltpatches.NumberSequence;
import org.hsqldb_voltpatches.Row;
import org.hsqldb_voltpatches.Session;
import org.hsqldb_voltpatches.Table;
import org.hsqldb_voltpatches.error.Error;
import org.hsqldb_voltpatches.error.ErrorCode;
import org.hsqldb_voltpatches.lib.FileAccess;
import org.hsqldb_voltpatches.rowio.RowOutputTextLog;

/**
 * Handles all scripting and logging operations. A script consists of two blocks:<p>
 *
 * DDL: SQL statements for table and user definitions
 * DATA: INSERT statements for memory tables
 *
 * This happens as part of the CHECKPOINT and SHUTDOWN COMPACT
 * process. In this case, the
 * DATA block contains the CACHED table data as well.<p>
 *
 * A related use for this class is for saving a current snapshot of the
 * database data to a user-defined file with the SCRIPT command
 *
 * A log consists of SQL statements of different types. Each statement is
 * encoded as ASCII and saved.
 *
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 2.3.0
 * @since 1.7.2
 */
public class ScriptWriterText extends ScriptWriterBase {

    RowOutputTextLog           rowOut;
    public static final String ISO_8859_1 = "ISO-8859-1";

    /** @todo - perhaps move this global into a lib utility class */
    public static byte[] BYTES_LINE_SEP;
    static byte[]        BYTES_COMMIT;
    static byte[]        BYTES_INSERT_INTO;
    static byte[]        BYTES_VALUES;
    static byte[]        BYTES_TERM;
    static byte[]        BYTES_DELETE_FROM;
    static byte[]        BYTES_WHERE;
    static byte[]        BYTES_SEQUENCE;
    static byte[]        BYTES_SEQUENCE_MID;
    static byte[]        BYTES_C_ID_INIT;
    static byte[]        BYTES_C_ID_TERM;
    static byte[]        BYTES_SCHEMA;

    static {
        String sLineSep = System.getProperty("line.separator", "\n");

        try {
            BYTES_LINE_SEP     = sLineSep.getBytes();
            BYTES_COMMIT       = "COMMIT".getBytes(ISO_8859_1);
            BYTES_INSERT_INTO  = "INSERT INTO ".getBytes(ISO_8859_1);
            BYTES_VALUES       = " VALUES(".getBytes(ISO_8859_1);
            BYTES_TERM         = ")".getBytes(ISO_8859_1);
            BYTES_DELETE_FROM  = "DELETE FROM ".getBytes(ISO_8859_1);
            BYTES_WHERE        = " WHERE ".getBytes(ISO_8859_1);
            BYTES_SEQUENCE     = "ALTER SEQUENCE ".getBytes(ISO_8859_1);
            BYTES_SEQUENCE_MID = " RESTART WITH ".getBytes(ISO_8859_1);
            BYTES_C_ID_INIT    = "/*C".getBytes(ISO_8859_1);
            BYTES_C_ID_TERM    = "*/".getBytes(ISO_8859_1);
            BYTES_SCHEMA       = "SET SCHEMA ".getBytes(ISO_8859_1);
        } catch (UnsupportedEncodingException e) {
            Error.runtimeError(ErrorCode.U_S0500, "ScriptWriterText");
        }
    }

    public ScriptWriterText(Database db, OutputStream outputStream,
                            FileAccess.FileSync descriptor,
                            boolean includeCachedData) {
        super(db, outputStream, descriptor, includeCachedData);
    }

    public ScriptWriterText(Database db, String file,
                            boolean includeCachedData, boolean newFile,
                            boolean isDump) {
        super(db, file, includeCachedData, newFile, isDump);
    }

    public ScriptWriterText(Database db, String file,
                            boolean includeCachedData, boolean compressed) {

        super(db, file, includeCachedData, true, false);

        if (compressed) {
            isCompressed = true;

            try {
                fileStreamOut = new GZIPOutputStream(fileStreamOut);
            } catch (IOException e) {
                throw Error.error(e, ErrorCode.FILE_IO_ERROR,
                                  ErrorCode.M_Message_Pair, new Object[] {
                    e.toString(), outFile
                });
            }
        }
    }

    protected void initBuffers() {
        rowOut = new RowOutputTextLog();
    }

    protected void writeDataTerm() throws IOException {}

    protected void writeSessionIdAndSchema(Session session)
    throws IOException {

        if (session == null) {
            return;
        }

        if (session != currentSession) {
            rowOut.reset();
            rowOut.write(BYTES_C_ID_INIT);
            rowOut.writeLong(session.getId());
            rowOut.write(BYTES_C_ID_TERM);

            currentSession = session;

            writeRowOutToFile();
        }

        if (schemaToLog != session.loggedSchema) {
            rowOut.reset();
            writeSchemaStatement(schemaToLog);

            session.loggedSchema = schemaToLog;

            writeRowOutToFile();
        }
    }

    private void writeSchemaStatement(HsqlName schema) {

        rowOut.write(BYTES_SCHEMA);
        rowOut.writeString(schema.statementName);
        rowOut.write(BYTES_LINE_SEP);
    }

    public void writeLogStatement(Session session,
                                  String s) throws IOException {

        if (session != null) {
            schemaToLog = session.currentSchema;

            writeSessionIdAndSchema(session);
        }

        rowOut.reset();
        rowOut.writeString(s);
        rowOut.write(BYTES_LINE_SEP);
        writeRowOutToFile();

        needsSync = true;
    }

    public void writeRow(Session session, Row row,
                         Table table) throws IOException {

        schemaToLog = table.getName().schema;

        writeSessionIdAndSchema(session);
        rowOut.reset();
        ((RowOutputTextLog) rowOut).setMode(RowOutputTextLog.MODE_INSERT);
        rowOut.write(BYTES_INSERT_INTO);
        rowOut.writeString(table.getName().statementName);
        rowOut.write(BYTES_VALUES);
        rowOut.writeData(row, table.getColumnTypes());
        rowOut.write(BYTES_TERM);
        rowOut.write(BYTES_LINE_SEP);
        writeRowOutToFile();
    }

    public void writeTableInit(Table t) throws IOException {

        if (t.isEmpty(currentSession)) {
            return;
        }

        if (schemaToLog == currentSession.loggedSchema) {
            return;
        }

        rowOut.reset();
        writeSchemaStatement(t.getName().schema);
        writeRowOutToFile();

        currentSession.loggedSchema = schemaToLog;
    }

    public void writeOtherStatement(Session session,
                                    String s) throws IOException {

        writeLogStatement(session, s);

        if (writeDelay == 0) {
            sync();
        }
    }

    public void writeInsertStatement(Session session, Row row,
                                     Table table) throws IOException {

        schemaToLog = table.getName().schema;

        writeRow(session, row, table);
    }

    public void writeDeleteStatement(Session session, Table table,
                                     Object[] data) throws IOException {

        schemaToLog = table.getName().schema;

        writeSessionIdAndSchema(session);
        rowOut.reset();
        ((RowOutputTextLog) rowOut).setMode(RowOutputTextLog.MODE_DELETE);
        rowOut.write(BYTES_DELETE_FROM);
        rowOut.writeString(table.getName().statementName);
        rowOut.write(BYTES_WHERE);
        rowOut.writeData(table.getColumnCount(), table.getColumnTypes(), data,
                         table.columnList, table.getPrimaryKey());
        rowOut.write(BYTES_LINE_SEP);
        writeRowOutToFile();
    }

    public void writeSequenceStatement(Session session,
                                       NumberSequence seq) throws IOException {

        schemaToLog = seq.getName().schema;

        writeSessionIdAndSchema(session);
        rowOut.reset();
        rowOut.write(BYTES_SEQUENCE);
        rowOut.writeString(seq.getSchemaName().statementName);
        rowOut.write('.');
        rowOut.writeString(seq.getName().statementName);
        rowOut.write(BYTES_SEQUENCE_MID);
        rowOut.writeLong(seq.peek());
        rowOut.write(BYTES_LINE_SEP);
        writeRowOutToFile();

        needsSync = true;
    }

    public void writeCommitStatement(Session session) throws IOException {

        writeSessionIdAndSchema(session);
        rowOut.reset();
        rowOut.write(BYTES_COMMIT);
        rowOut.write(BYTES_LINE_SEP);
        writeRowOutToFile();

        needsSync = true;

        if (writeDelay == 0) {
            sync();
        }
    }

    protected void finishStream() throws IOException {

        if (isCompressed) {
            ((GZIPOutputStream) fileStreamOut).finish();
        }
    }

    void writeRowOutToFile() throws IOException {

        synchronized (fileStreamOut) {
            fileStreamOut.write(rowOut.getBuffer(), 0, rowOut.size());

            byteCount += rowOut.size();

            lineCount++;
        }
    }
}
