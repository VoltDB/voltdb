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

import java.io.IOException;
import java.io.File;
import java.text.SimpleDateFormat;

import org.hsqldb_voltpatches.Database;
import org.hsqldb_voltpatches.NumberSequence;
import org.hsqldb_voltpatches.Session;
import org.hsqldb_voltpatches.Table;
import org.hsqldb_voltpatches.TableBase;
import org.hsqldb_voltpatches.lib.SimpleLog;
import org.hsqldb_voltpatches.lib.tar.DbBackup;
import org.hsqldb_voltpatches.Error;
import org.hsqldb_voltpatches.ErrorCode;
import org.hsqldb_voltpatches.lib.tar.TarMalformatException;

// boucherb@users 20030510 - patch 1.7.2 - added cooperative file locking

/**
 *  The public interface of logging and cache classes.<p>
 *
 *  Implements a storage manager wrapper that provides a consistent,
 *  always available interface to storage management for the Database
 *  class, despite the fact not all Database objects actually use file
 *  storage.<p>
 *
 *  The Logger class makes it possible to avoid testing for a
 *  null Log Database attribute again and again, in many different places,
 *  and generally avoids tight coupling between Database and Log, opening
 *  the doors for multiple logs/caches in the future. In this way, the
 *  Database class does not need to know the details of the Logging/Cache
 *  implementation, lowering its breakability factor and promoting
 *  long-term code flexibility.
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 1.9.0
 * @since 1.7.0
 */
public class Logger {

    public SimpleLog appLog;

    /**
     *  The Log object this Logger object wraps
     */
    private Log      log;
    private Database database;

    /**
     *  The LockFile object this Logger uses to cooperatively lock
     *  the database files
     */
    private LockFile lockFile;
    boolean          needsCheckpoint;
    private boolean  logsStatements;
    private boolean  logStatements;
    private boolean  syncFile = false;

    public Logger() {
        appLog = new SimpleLog(null, SimpleLog.LOG_NONE, false);
    }

    /**
     *  Opens the specified Database object's database files and starts up
     *  the logging process. <p>
     *
     *  If the specified Database object is a new database, its database
     *  files are first created.
     *
     * @param  db the Database
     * @throws  HsqlException if there is a problem, such as the case when
     *      the specified files are in use by another process
     */
    public void openLog(Database db) {

        needsCheckpoint = false;

        String path = db.getPath();
        int loglevel = db.getProperties().getIntegerProperty(
            HsqlDatabaseProperties.hsqldb_applog, 0);

        this.database = db;

        if (loglevel != SimpleLog.LOG_NONE) {
            appLog = new SimpleLog(path + ".app.log", loglevel,
                                   !db.isFilesReadOnly());
        }

        appLog.sendLine(SimpleLog.LOG_ERROR, "Database (re)opened");

        logStatements = false;

        boolean useLock = db.getProperties().isPropertyTrue(
            HsqlDatabaseProperties.hsqldb_lock_file);

        if (useLock && !db.isFilesReadOnly()) {
            acquireLock(path);
        }

        log = new Log(db);

        log.open();

        logsStatements = logStatements = !db.isFilesReadOnly();
    }

// fredt@users 20020130 - patch 495484 by boucherb@users

    /**
     *  Shuts down the logging process using the specified mode. <p>
     *
     * @param  closemode The mode in which to shut down the logging
     *      process
     *      <OL>
     *        <LI> closemode -1 performs SHUTDOWN IMMEDIATELY, equivalent
     *        to  a poweroff or crash.
     *        <LI> closemode 0 performs a normal SHUTDOWN that
     *        checkpoints the database normally.
     *        <LI> closemode 1 performs a shutdown compact that scripts
     *        out the contents of any CACHED tables to the log then
     *        deletes the existing *.data file that contains the data
     *        for all CACHED table before the normal checkpoint process
     *        which in turn creates a new, compact *.data file.
     *        <LI> closemode 2 performs a SHUTDOWN SCRIPT.
     *      </OL>
     *
     * @return  true if closed with no problems or false if a problem was
     *        encountered.
     */
    public boolean closeLog(int closemode) {

        if (log == null) {
            return true;
        }

        try {
            switch (closemode) {

                case Database.CLOSEMODE_IMMEDIATELY :
                    log.shutdown();
                    break;

                case Database.CLOSEMODE_NORMAL :
                    log.close(false);
                    break;

                case Database.CLOSEMODE_COMPACT :
                case Database.CLOSEMODE_SCRIPT :
                    log.close(true);
                    break;
            }
        } catch (Throwable e) {
            appLog.logContext(e, "error closing log");
            appLog.close();

            log = null;

            return false;
        }

        appLog.sendLine(SimpleLog.LOG_ERROR, "Database closed");
        appLog.close();

        log = null;

        return true;
    }

    /**
     *  Determines if the logging process actually does anything. <p>
     *
     *  In-memory Database objects do not need to log anything. This
     *  method is essentially equivalent to testing whether this logger's
     *  database is an in-memory mode database.
     *
     * @return  true if this object encapsulates a non-null Log instance,
     *      else false
     */
    public boolean hasLog() {
        return log != null;
    }

    /**
     *  Returns the Cache object or null if one doesn't exist.
     */
    public DataFileCache getCache() {

        if (log == null) {
            return null;
        } else {
            return log.getCache();
        }
    }

    /**
     *  Returns the Cache object or null if one doesn't exist.
     */
    public boolean hasCache() {

        if (log == null) {
            return false;
        } else {
            return log.hasCache();
        }
    }

    /**
     *  Records a Log entry representing a new connection action on the
     *  specified Session object.
     *
     * @param  session the Session object for which to record the log
     *      entry
     * @throws  HsqlException if there is a problem recording the Log
     *      entry
     */
    public synchronized void logStartSession(Session session) {

        if (logStatements) {
            writeToLog(session, session.getUser().getConnectUserSQL());
        }
    }

    /**
     *  Records a Log entry for the specified SQL statement, on behalf of
     *  the specified Session object.
     *
     * @param  session the Session object for which to record the Log
     *      entry
     * @param  statement the SQL statement to Log
     * @throws  HsqlException if there is a problem recording the entry
     */
    public synchronized void writeToLog(Session session, String statement) {

        if (logStatements && log != null) {
            log.writeStatement(session, statement);
        }
    }

    public synchronized void writeInsertStatement(Session session,
            Table table, Object[] row) {

        if (logStatements) {
            log.writeInsertStatement(session, table, row);
        }
    }

    public synchronized void writeDeleteStatement(Session session, Table t,
            Object[] row) {

        if (logStatements) {
            log.writeDeleteStatement(session, t, row);
        }
    }

    public synchronized void writeSequenceStatement(Session session,
            NumberSequence s) {

        if (logStatements) {
            log.writeSequenceStatement(session, s);
        }
    }

    public synchronized void writeCommitStatement(Session session) {

        if (logStatements) {
            log.writeCommitStatement(session);
            synchLog();
        }
    }

    /**
     * Called after commits or after each statement when autocommit is on
     */
    public synchronized void synchLog() {

        if (logStatements && syncFile) {
            log.synchLog();
        }
    }

    public synchronized void synchLogForce() {

        if (logStatements) {
            log.synchLog();
        }
    }

    /**
     *  Checkpoints the database. <p>
     *
     *  The most important effect of calling this method is to cause the
     *  log file to be rewritten in the most efficient form to
     *  reflect the current state of the database, i.e. only the DDL and
     *  insert DML required to recreate the database in its present state.
     *  Other house-keeping duties are performed w.r.t. other database
     *  files, in order to ensure as much as possible the ACID properites
     *  of the database.
     *
     * @throws  HsqlException if there is a problem checkpointing the
     *      database
     */
    public synchronized void checkpoint(boolean mode) {

        if (logStatements) {
            appLog.logContext(SimpleLog.LOG_NORMAL, "start");

            needsCheckpoint = false;

            log.checkpoint(mode);
            database.sessionManager.resetLoggedSchemas();
            appLog.logContext(SimpleLog.LOG_NORMAL, "end");
        }
    }

    /**
     *  Sets the maximum size to which the log file can grow
     *  before being automatically checkpointed.
     *
     * @param  megas size in MB
     */
    public synchronized void setLogSize(int megas) {

        if (log != null) {
            log.setLogSize(megas);
        }
    }

    /**
     *  Sets the type of script file, currently 0 for text (default)
     *  1 for binary and 3 for compressed
     *
     * @param  i The type
     */
    public synchronized void setScriptType(int i) {

        if (log != null) {
            log.setScriptType(i);
        }
    }

    /**
     *  Sets the log write delay mode to number of seconds. By default
     *  executed commands written to the log are committed fully at most
     *  60 second after they are executed. This improves performance for
     *  applications that execute a large number
     *  of short running statements in a short period of time, but risks
     *  failing to log some possibly large number of statements in the
     *  event of a crash. A small value improves recovery.
     *  A value of 0 will severly slow down logging when autocommit is on,
     *  or many short transactions are committed.
     *
     * @param  delay in seconds
     */
    public synchronized void setWriteDelay(int delay) {

        if (log != null) {
            syncFile = (delay == 0);

            log.setWriteDelay(delay);
        }
    }

    public int getWriteDelay() {
        return log != null ? log.getWriteDelay()
                           : 0;
    }

    public int getLogSize() {
        return log != null ? log.getLogSize()
                           : 0;
    }

    public int getScriptType() {
        return log != null ? log.getScriptType()
                           : 0;
    }

    public synchronized void setIncrementalBackup(boolean val) {

        if (log != null) {
            log.setIncrementalBackup(val);
        }
    }

    /**
     *  Opens the TextCache object.
     */
    public DataFileCache openTextCache(Table table, String source,
                                       boolean readOnlyData,
                                       boolean reversed) {
        return log.openTextCache(table, source, readOnlyData, reversed);
    }

    /**
     *  Closes the TextCache object.
     */
    public void closeTextCache(Table table) {
        log.closeTextCache(table);
    }

    public boolean needsCheckpoint() {
        return needsCheckpoint;
    }

    public void stopLogging() {
        logStatements = false;
    }

    public void restartLogging() {
        logStatements = logsStatements;
    }

    /**
     * Attempts to aquire a cooperative lock condition on the database files
     */
    public void acquireLock(String path) {

        if (lockFile != null) {
            return;
        }

        lockFile = LockFile.newLockFileLock(path);
    }

    public void releaseLock() {

        try {
            if (lockFile != null) {
                lockFile.tryRelease();
            }
        } catch (Exception e) {}

        lockFile = null;
    }

    public PersistentStore newStore(Session session,
                                    PersistentStoreCollection collection,
                                    TableBase table, boolean diskBased) {

        switch (table.getTableType()) {

            case TableBase.CACHED_TABLE :
                DataFileCache cache = getCache();

                if (cache == null) {
                    break;
                }

                return new RowStoreAVLDisk(collection, cache, (Table) table);

            case TableBase.MEMORY_TABLE :
            case TableBase.SYSTEM_TABLE :
                return new RowStoreAVLMemory(collection, (Table) table);

            case TableBase.TEXT_TABLE :
                return new RowStoreAVLDiskData(collection, (Table) table);

            case TableBase.TEMP_TABLE :
                diskBased = false;

            // $FALL-THROUGH$
            case TableBase.RESULT_TABLE :
            case TableBase.SYSTEM_SUBQUERY :
            case TableBase.VIEW_TABLE :
            case TableBase.TRANSITION_TABLE :
                if (session == null) {
                    return null;
                }

                switch (table.persistenceScope) {

                    case TableBase.SCOPE_STATEMENT :
                        return new RowStoreAVLHybrid(session, collection,
                                                     table, diskBased);

                    case TableBase.SCOPE_TRANSACTION :
                        return new RowStoreAVLHybrid(session, collection,
                                                     table, diskBased);

                    case TableBase.SCOPE_SESSION :
                        return new RowStoreAVLHybrid(session, collection,
                                                     table, diskBased);
                }
        }

        throw Error.runtimeError(ErrorCode.U_S0500, "PSCS");
    }

    //
    static private SimpleDateFormat backupFileFormat =
        new SimpleDateFormat("yyyyMMdd'T'HHmmss");
    static private Character runtimeFileDelim = null;

    public synchronized void backup(String destPath, String dbPath,
                                    boolean script, boolean blocking,
                                    boolean compressed) {

        /* If want to add db Id also, will need to pass either Database
         * instead of dbPath, or pass dbPath + Id from CommandStatement.
         */
        if (runtimeFileDelim == null) {
            runtimeFileDelim =
                new Character(System.getProperty("file.separator").charAt(0));
        }

        String instanceName = new File(dbPath).getName();

        if (destPath == null || destPath.length() < 1) {
            throw Error.error(ErrorCode.X_2200F, "0-length destination path");
        }

        char lastChar = destPath.charAt(destPath.length() - 1);
        boolean generateName = (lastChar == '/'
                                || lastChar == runtimeFileDelim.charValue());
        String defaultCompressionSuffix = compressed ? ".tar.gz"
                                                     : ".tar";
        File archiveFile =
            generateName
            ? (new File(destPath.substring(0, destPath.length() - 1),
                        instanceName + '-'
                        + backupFileFormat.format(new java.util.Date())
                        + defaultCompressionSuffix))
            : (new File(destPath));
        boolean nameImpliesCompress =
            archiveFile.getName().endsWith(".tar.gz")
            || archiveFile.getName().endsWith(".tgz");

        if ((!nameImpliesCompress)
                && !archiveFile.getName().endsWith(".tar")) {
            throw Error.error(ErrorCode.UNSUPPORTED_FILENAME_SUFFIX, 0,
                              new String[] {
                archiveFile.getName(), ".tar, .tar.gz, .tgz"
            });
        }

        if (compressed != nameImpliesCompress) {
            throw Error.error(ErrorCode.COMPRESSION_SUFFIX_MISMATCH, 0,
                              new Object[] {
                new Boolean(compressed), archiveFile.getName()
            });
        }

        log.closeForBackup();

        try {
            appLog.logContext(SimpleLog.LOG_NORMAL,
                              "Initiating backup of instance '" + instanceName
                              + "'");

            // By default, DbBackup will throw if archiveFile (or
            // corresponding work file) already exist.  That's just what we
            // want here.
            DbBackup backup = new DbBackup(archiveFile, dbPath);

            backup.setAbortUponModify(false);
            backup.write();
            appLog.logContext(SimpleLog.LOG_NORMAL,
                              "Successfully backed up instance '"
                              + instanceName + "' to '" + destPath + "'");

            // RENAME tempPath to destPath
        } catch (IllegalArgumentException iae) {
            throw Error.error(ErrorCode.X_HV00A, iae.getMessage());
        } catch (IOException ioe) {
            throw Error.error(ErrorCode.FILE_IO_ERROR, ioe.getMessage());
        } catch (TarMalformatException tme) {
            throw Error.error(ErrorCode.FILE_IO_ERROR, tme.getMessage());
        } finally {
            log.openAfterBackup();

            needsCheckpoint = false;
        }
    }
}
