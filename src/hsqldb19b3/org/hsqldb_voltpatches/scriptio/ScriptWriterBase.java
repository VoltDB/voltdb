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

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import org.hsqldb_voltpatches.Database;
import org.hsqldb_voltpatches.DatabaseManager;
import org.hsqldb_voltpatches.HsqlNameManager;
import org.hsqldb_voltpatches.HsqlNameManager.HsqlName;
import org.hsqldb_voltpatches.NumberSequence;
import org.hsqldb_voltpatches.Row;
import org.hsqldb_voltpatches.SchemaObject;
import org.hsqldb_voltpatches.Session;
import org.hsqldb_voltpatches.Table;
import org.hsqldb_voltpatches.TableBase;
import org.hsqldb_voltpatches.Tokens;
import org.hsqldb_voltpatches.error.Error;
import org.hsqldb_voltpatches.error.ErrorCode;
import org.hsqldb_voltpatches.lib.FileAccess;
import org.hsqldb_voltpatches.lib.FileUtil;
import org.hsqldb_voltpatches.lib.HsqlTimer;
import org.hsqldb_voltpatches.lib.Iterator;
import org.hsqldb_voltpatches.navigator.RowIterator;
import org.hsqldb_voltpatches.navigator.RowSetNavigator;
import org.hsqldb_voltpatches.result.Result;

//import org.hsqldb_voltpatches.lib.StopWatch;

/**
 * @todo - can lock the database engine as readonly in a wrapper for this when
 * used at checkpoint
 */

/**
 * Handles all logging to file operations. A log consists of three kinds of
 * blocks:<p>
 *
 * DDL BLOCK: definition of DB objects, users and rights at startup time<br>
 * DATA BLOCK: all data for MEMORY tables at startup time<br>
 * LOG BLOCK: SQL statements logged since startup or the last CHECKPOINT<br>
 *
 * The implementation of this class and its subclasses support the formats
 * used for writing the data. Since 1.7.2 the data can also be
 * written as binray in order to speed up shutdown and startup.<p>
 *
 * From 1.7.2, two separate files are used, one for the DDL + DATA BLOCK and
 * the other for the LOG BLOCK.<p>
 *
 * A related use for this class is for saving a current snapshot of the
 * database data to a user-defined file. This happens in the SHUTDOWN COMPACT
 * process or done as a result of the SCRIPT command. In this case, the
 * DATA block contains the CACHED table data as well.<p>
 *
 * DatabaseScriptReader and its subclasses read back the data at startup time.
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 2.1.1
 * @since 1.7.2
 */
public abstract class ScriptWriterBase implements Runnable {

    Database            database;
    String              outFile;
    OutputStream        fileStreamOut;
    FileAccess.FileSync outDescriptor;
    int                 tableRowCount;
    HsqlName            schemaToLog;
    boolean             isClosed;

    //
    boolean isCompressed;
    boolean isCrypt;

    /**
     * this determines if the script is the normal script (false) used
     * internally by the engine or a user-initiated snapshot of the DB (true)
     */
    boolean          isDump;
    boolean          includeCachedData;
    boolean          includeIndexRoots;
    long             byteCount;
    long             lineCount;
    volatile boolean needsSync;
    private int      syncCount;
    static final int INSERT             = 0;
    static final int INSERT_WITH_SCHEMA = 1;

    /** the last schema for last sessionId */
    Session                      currentSession;
    public static final String[] LIST_SCRIPT_FORMATS = new String[] {
        Tokens.T_TEXT, Tokens.T_BINARY, null, Tokens.T_COMPRESSED
    };

    ScriptWriterBase(Database db, OutputStream outputStream,
                     FileAccess.FileSync descriptor,
                     boolean includeCachedData) {

        initBuffers();

        this.database          = db;
        this.includeCachedData = includeCachedData;
        this.includeIndexRoots = !includeCachedData;
        currentSession         = database.sessionManager.getSysSession();

        // start with neutral schema - no SET SCHEMA to log
        schemaToLog = currentSession.loggedSchema =
            currentSession.currentSchema;
        fileStreamOut = new BufferedOutputStream(outputStream, 1 << 14);
        outDescriptor = descriptor;
    }

    ScriptWriterBase(Database db, String file, boolean includeCachedData,
                     boolean isNewFile, boolean isDump) {

        initBuffers();

        boolean exists = false;

        if (isDump) {
            exists = FileUtil.getFileUtil().exists(file);
        } else {
            exists = db.logger.getFileAccess().isStreamElement(file);
        }

        if (exists && isNewFile) {
            throw Error.error(ErrorCode.FILE_IO_ERROR, file);
        }

        this.database          = db;
        this.isDump            = isDump;
        this.includeCachedData = includeCachedData;
        this.includeIndexRoots = !includeCachedData;
        outFile                = file;
        currentSession         = database.sessionManager.getSysSession();

        // start with neutral schema - no SET SCHEMA to log
        schemaToLog = currentSession.loggedSchema =
            currentSession.currentSchema;

        openFile();
    }

    public void setIncludeIndexRoots(boolean include) {
        this.includeIndexRoots = include;
    }

    public void setIncludeCachedData(boolean include) {
        this.includeCachedData = include;
    }

    protected abstract void initBuffers();

    /**
     *  Called internally or externally in write delay intervals.
     */
    public void sync() {

        if (isClosed) {
            return;
        }

        if (needsSync) {
            forceSync();
        }
    }

    public void forceSync() {

        if (isClosed) {
            return;
        }

        needsSync = false;

        synchronized (fileStreamOut) {
            try {
                fileStreamOut.flush();
                outDescriptor.sync();

                syncCount++;
/*
                System.out.println(
                    this.outFile + " FD.sync done at "
                    + new java.sql.Timestamp(System.currentTimeMillis()));
*/
            } catch (IOException e) {
                database.logger.logWarningEvent("ScriptWriter synch error: ",
                                                e);
            }
        }
    }

    public void close() {

        stop();

        if (isClosed) {
            return;
        }

        try {
            synchronized (fileStreamOut) {
                finishStream();
                forceSync();
                fileStreamOut.close();

                fileStreamOut = null;
                outDescriptor = null;
                isClosed      = true;
            }
        } catch (IOException e) {
            throw Error.error(ErrorCode.FILE_IO_ERROR);
        }

        byteCount = 0;
        lineCount = 0;
    }

    public long size() {
        return byteCount;
    }

    public void writeAll() {

        try {
            writeDDL();
            writeExistingData();
        } catch (IOException e) {
            throw Error.error(ErrorCode.FILE_IO_ERROR);
        }
    }

    /**
     *  File is opened in append mode although in current usage the file
     *  never pre-exists
     */
    protected void openFile() {

        try {
            FileAccess   fa  = isDump ? FileUtil.getFileUtil()
                                      : database.logger.getFileAccess();
            OutputStream fos = fa.openOutputStreamElement(outFile);

            outDescriptor = fa.getFileSync(fos);
            fileStreamOut = fos;
            fileStreamOut = new BufferedOutputStream(fos, 1 << 14);
        } catch (IOException e) {
            throw Error.error(e, ErrorCode.FILE_IO_ERROR,
                              ErrorCode.M_Message_Pair, new Object[] {
                e.toString(), outFile
            });
        }
    }

    protected void finishStream() throws IOException {}

    public void writeDDL() throws IOException {

        Result ddlPart = database.getScript(includeIndexRoots);

        writeSingleColumnResult(ddlPart);
    }

    public void writeExistingData() throws IOException {

        // start with blank schema - SET SCHEMA to log
        currentSession.loggedSchema = null;

        String[] schemas = database.schemaManager.getSchemaNamesArray();

        for (int i = 0; i < schemas.length; i++) {
            String schema = schemas[i];
            Iterator tables =
                database.schemaManager.databaseObjectIterator(schema,
                    SchemaObject.TABLE);

            while (tables.hasNext()) {
                Table t = (Table) tables.next();

                // write all memory table data
                // write cached table data unless index roots have been written
                // write all text table data apart from readonly text tables
                // unless index roots have been written
                boolean script = false;

                switch (t.getTableType()) {

                    case TableBase.MEMORY_TABLE :
                        script = true;
                        break;

                    case TableBase.CACHED_TABLE :
                        script = includeCachedData;
                        break;

                    case TableBase.TEXT_TABLE :
                        script = includeCachedData && !t.isDataReadOnly();
                        break;
                }

                try {
                    if (script) {
                        schemaToLog = t.getName().schema;

                        writeTableInit(t);

                        RowIterator it =
                            t.rowIteratorClustered(currentSession);

                        while (it.hasNext()) {
                            Row row = it.getNextRow();

                            writeRow(currentSession, row, t);
                        }

                        writeTableTerm(t);
                    }
                } catch (Exception e) {
                    throw Error.error(ErrorCode.FILE_IO_ERROR, e.toString());
                }
            }
        }

        writeDataTerm();
    }

    public void writeTableInit(Table t) throws IOException {}

    public void writeTableTerm(Table t) throws IOException {}

    protected void writeSingleColumnResult(Result r) throws IOException {

        RowSetNavigator nav = r.initialiseNavigator();

        while (nav.hasNext()) {
            Object[] data = (Object[]) nav.getNext();

            writeLogStatement(currentSession, (String) data[0]);
        }
    }

    public abstract void writeRow(Session session, Row row,
                                  Table table) throws IOException;

    protected abstract void writeDataTerm() throws IOException;

    protected abstract void writeSessionIdAndSchema(Session session)
    throws IOException;

    public abstract void writeLogStatement(Session session,
                                           String s) throws IOException;

    public abstract void writeOtherStatement(Session session,
            String s) throws IOException;

    public abstract void writeInsertStatement(Session session, Row row,
            Table table) throws IOException;

    public abstract void writeDeleteStatement(Session session, Table table,
            Object[] data) throws IOException;

    public abstract void writeSequenceStatement(Session session,
            NumberSequence seq) throws IOException;

    public abstract void writeCommitStatement(Session session)
    throws IOException;

    //
    private Object timerTask;

    // long write delay for scripts : 60s
    protected volatile int writeDelay = 60000;

    public void run() {

        try {
            if (writeDelay != 0) {
                sync();
            }

            /** @todo: can do Cache.cleanUp() here, too */
        } catch (Exception e) {

            // ignore exceptions
            // may be InterruptedException or IOException
        }
    }

    public void setWriteDelay(int delay) {
        writeDelay = delay;
    }

    public void start() {

        if (writeDelay > 0) {
            timerTask = DatabaseManager.getTimer().schedulePeriodicallyAfter(0,
                    writeDelay, this, false);
        }
    }

    public void stop() {

        if (timerTask != null) {
            HsqlTimer.cancel(timerTask);

            timerTask = null;
        }
    }

    public int getWriteDelay() {
        return writeDelay;
    }
}
