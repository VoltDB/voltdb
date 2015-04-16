/* Copyright (c) 2001-2014, The HSQL Development Group
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

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.hsqldb_voltpatches.Database;
import org.hsqldb_voltpatches.HsqlException;
import org.hsqldb_voltpatches.error.Error;
import org.hsqldb_voltpatches.error.ErrorCode;
import org.hsqldb_voltpatches.lib.FileAccess;
import org.hsqldb_voltpatches.lib.FileArchiver;
import org.hsqldb_voltpatches.lib.FileUtil;
import org.hsqldb_voltpatches.lib.Iterator;
import org.hsqldb_voltpatches.map.BitMap;
import org.hsqldb_voltpatches.rowio.RowInputBinary180;
import org.hsqldb_voltpatches.rowio.RowInputBinaryDecode;
import org.hsqldb_voltpatches.rowio.RowInputInterface;
import org.hsqldb_voltpatches.rowio.RowOutputBinary180;
import org.hsqldb_voltpatches.rowio.RowOutputBinaryEncode;
import org.hsqldb_voltpatches.rowio.RowOutputInterface;

/**
 * Acts as a manager for CACHED table persistence.<p>
 *
 * This contains the top level functionality. Provides file management services
 * and access.<p>
 *
 * Rewritten for 1.8.0 and 2.x
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 2.3.2
 * @since 1.7.2
 */
public class DataFileCache {

    protected FileAccess fa;

    // flags
    public static final int FLAG_ISSHADOWED = 1;
    public static final int FLAG_ISSAVED    = 2;
    public static final int FLAG_ROWINFO    = 3;
    public static final int FLAG_190        = 4;
    public static final int FLAG_HX         = 5;

    // file format fields
    static final int LONG_EMPTY_SIZE      = 4;        // empty space size
    static final int LONG_FREE_POS_POS    = 12;       // where iFreePos is saved
    static final int INT_SPACE_LIST_POS   = 24;       // empty space index
    static final int FLAGS_POS            = 28;
    static final int MIN_INITIAL_FREE_POS = 32;

    //
    public DataSpaceManager  spaceManager;
    static final int         initIOBufferSize = 4096;
    private static final int diskBlockSize    = 4096;

    //
    protected String   dataFileName;
    protected String   backupFileName;
    protected Database database;
    protected boolean  logEvents = true;

    // this flag is used externally to determine if a backup is required
    protected boolean fileModified;
    protected boolean cacheModified;
    protected int     dataFileScale;

    // post opening constant fields
    protected boolean cacheReadonly;

    //
    protected int cachedRowPadding;

    //
    protected long    initialFreePos;
    protected long    lostSpaceSize;
    protected long    spaceManagerPosition;
    protected long    fileStartFreePosition;
    protected boolean hasRowInfo = false;
    protected int     storeCount;

    // reusable input / output streams
    protected RowInputInterface rowIn;
    public RowOutputInterface   rowOut;

    //
    public long maxDataFileSize;

    //
    boolean is180;

    //
    protected RandomAccessInterface dataFile;
    protected volatile long         fileFreePosition;
    protected int                   maxCacheRows;     // number of Rows
    protected long                  maxCacheBytes;    // number of bytes
    protected Cache                 cache;

    //
    private RAShadowFile shadowFile;

    //
    ReadWriteLock lock      = new ReentrantReadWriteLock();
    Lock          readLock  = lock.readLock();
    Lock          writeLock = lock.writeLock();

    public DataFileCache(Database db, String baseFileName) {

        initParams(db, baseFileName, false);

        cache = new Cache(this);
    }

    public DataFileCache(Database db, String baseFileName, boolean defrag) {

        initParams(db, baseFileName, true);

        cache = new Cache(this);

        try {
            if (database.logger.isStoredFileAccess()) {
                dataFile = RAFile.newScaledRAFile(database, dataFileName,
                                                  false,
                                                  RAFile.DATA_FILE_STORED);
            } else {
                dataFile = new RAFileSimple(database, dataFileName, "rw");
            }
        } catch (Throwable t) {
            throw Error.error(ErrorCode.FILE_IO_ERROR, t);
        }

        initNewFile();
        initBuffers();

        if (database.logger.getDataFileSpaces() > 0) {
            spaceManager = new DataSpaceManagerBlocks(this,
                    database.logger.getDataFileSpaces());
        } else {
            spaceManager = new DataSpaceManagerSimple(this);
        }
    }

    /**
     * initial external parameters are set here.
     */
    protected void initParams(Database database, String baseFileName,
                              boolean defrag) {

        this.dataFileName   = baseFileName + Logger.dataFileExtension;
        this.backupFileName = baseFileName + Logger.backupFileExtension;
        this.database       = database;
        fa                  = database.logger.getFileAccess();
        dataFileScale       = database.logger.getDataFileScale();
        cachedRowPadding    = 8;

        if (dataFileScale > 8) {
            cachedRowPadding = dataFileScale;
        }

        initialFreePos = MIN_INITIAL_FREE_POS;

        if (initialFreePos < dataFileScale) {
            initialFreePos = dataFileScale;
        }

        cacheReadonly = database.isFilesReadOnly();
        maxCacheRows  = database.logger.propCacheMaxRows;
        maxCacheBytes = database.logger.propCacheMaxSize;
        maxDataFileSize = (long) Integer.MAX_VALUE * dataFileScale
                          * database.logger.getDataFileFactor();

        if (defrag) {
            this.dataFileName   = baseFileName + Logger.newFileExtension;
            this.backupFileName = baseFileName + Logger.newFileExtension;
            this.maxCacheRows   = 1024;
            this.maxCacheBytes  = 1024 * 4096;
        }
    }

    /**
     * Opens the *.data file for this cache, setting the variables that
     * allow access to the particular database version of the *.data file.
     */
    public void open(boolean readonly) {

        if (database.logger.isStoredFileAccess()) {
            openStoredFileAccess(readonly);

            return;
        }

        fileFreePosition = initialFreePos;

        logInfoEvent("dataFileCache open start");

        try {
            boolean isNio = database.logger.propNioDataFile;
            int     fileType;

            if (database.isFilesInJar()) {
                fileType = RAFile.DATA_FILE_JAR;
            } else if (isNio) {
                fileType = RAFile.DATA_FILE_NIO;
            } else {
                fileType = RAFile.DATA_FILE_RAF;
            }

            if (readonly || database.isFilesInJar()) {
                dataFile = RAFile.newScaledRAFile(database, dataFileName,
                                                  readonly, fileType);

                dataFile.seek(FLAGS_POS);

                int flags = dataFile.readInt();

                is180 = !BitMap.isSet(flags, FLAG_190);

                if (BitMap.isSet(flags, FLAG_HX)) {
                    throw Error.error(ErrorCode.WRONG_DATABASE_FILE_VERSION);
                }

                dataFile.seek(LONG_FREE_POS_POS);

                fileFreePosition = dataFile.readLong();

                dataFile.seek(INT_SPACE_LIST_POS);

                spaceManagerPosition = (long) dataFile.readInt()
                                       * DataSpaceManager.fixedBlockSizeUnit;

                initBuffers();

                spaceManager = new DataSpaceManagerSimple(this);

                return;
            }

            boolean preexists     = fa.isStreamElement(dataFileName);
            boolean isIncremental = database.logger.propIncrementBackup;
            boolean isSaved       = false;

            if (preexists) {
                dataFile = new RAFileSimple(database, dataFileName, "r");

                long    length       = dataFile.length();
                boolean wrongVersion = false;

                if (length > initialFreePos) {
                    dataFile.seek(FLAGS_POS);

                    int flags = dataFile.readInt();

                    isSaved       = BitMap.isSet(flags, FLAG_ISSAVED);
                    isIncremental = BitMap.isSet(flags, FLAG_ISSHADOWED);
                    is180         = !BitMap.isSet(flags, FLAG_190);

                    if (BitMap.isSet(flags, FLAG_HX)) {
                        wrongVersion = true;
                    }
                }

                dataFile.close();

                if (wrongVersion) {
                    throw Error.error(ErrorCode.WRONG_DATABASE_FILE_VERSION);
                }

                if (!database.logger.propLargeData) {
                    if (length > (maxDataFileSize / 8) * 7) {
                        database.logger.propLargeData = true;
                        maxDataFileSize =
                            (long) Integer.MAX_VALUE * dataFileScale
                            * database.logger.getDataFileFactor();
                    }
                }

                if (length > maxDataFileSize) {
                    throw Error.error(ErrorCode.DATA_FILE_IS_FULL,
                                      String.valueOf(maxDataFileSize));
                }

                if (isSaved && isIncremental) {
                    boolean existsBackup = fa.isStreamElement(backupFileName);

                    if (existsBackup) {
                        int dbState =
                            database.databaseProperties.getDBModified();

                        if (dbState == HsqlDatabaseProperties.FILES_MODIFIED) {
                            isSaved = false;

                            logInfoEvent(
                                "data file was saved but inc backup exists - restoring");
                        }
                    }
                }
            }

            if (isSaved) {
                if (isIncremental) {
                    deleteBackup();
                } else {
                    boolean existsBackup = fa.isStreamElement(backupFileName);

                    if (!existsBackup) {
                        backupDataFile(false);
                    }
                }
            } else {
                if (isIncremental) {
                    preexists = restoreBackupIncremental();
                } else {
                    preexists = restoreBackup();
                }
            }

            dataFile = RAFile.newScaledRAFile(database, dataFileName,
                                              readonly, fileType);

            if (preexists) {
                dataFile.seek(FLAGS_POS);

                int flags = dataFile.readInt();

                is180 = !BitMap.isSet(flags, FLAG_190);

                dataFile.seek(LONG_EMPTY_SIZE);

                lostSpaceSize = dataFile.readLong();

                dataFile.seek(LONG_FREE_POS_POS);

                fileFreePosition      = dataFile.readLong();
                fileStartFreePosition = fileFreePosition;

                dataFile.seek(INT_SPACE_LIST_POS);

                spaceManagerPosition = (long) dataFile.readInt()
                                       * DataSpaceManager.fixedBlockSizeUnit;

                openShadowFile();
            } else {
                initNewFile();
            }

            initBuffers();

            fileModified  = false;
            cacheModified = false;

            if (spaceManagerPosition != 0
                    || database.logger.getDataFileSpaces() > 0) {
                spaceManager = new DataSpaceManagerBlocks(this,
                        database.logger.getDataFileSpaces());
            } else {
                spaceManager = new DataSpaceManagerSimple(this);
            }

            logInfoEvent("dataFileCache open end");
        } catch (Throwable t) {
            logSevereEvent("DataFileCache.open", t);
            release();

            throw Error.error(t, ErrorCode.FILE_IO_ERROR,
                              ErrorCode.M_DataFileCache_open, new Object[] {
                t.toString(), dataFileName
            });
        }
    }

    boolean setTableSpaceManager(int tableSpaceSize) {

        if (tableSpaceSize > 0 && spaceManagerPosition == 0) {
            spaceManager.reset();

            spaceManager = new DataSpaceManagerBlocks(this, tableSpaceSize);

            return true;
        }

        if (tableSpaceSize == 0 && spaceManagerPosition != 0) {
            spaceManager.reset();

            spaceManager = new DataSpaceManagerSimple(this);

            return true;
        }

        return false;
    }

    void openStoredFileAccess(boolean readonly) {

        fileFreePosition = initialFreePos;

        logInfoEvent("dataFileCache open start");

        try {
            int fileType = RAFile.DATA_FILE_STORED;

            if (readonly) {
                dataFile = RAFile.newScaledRAFile(database, dataFileName,
                                                  readonly, fileType);

                dataFile.seek(FLAGS_POS);

                int flags = dataFile.readInt();

                is180 = !BitMap.isSet(flags, FLAG_190);

                dataFile.seek(LONG_FREE_POS_POS);

                fileFreePosition = dataFile.readLong();

                initBuffers();

                return;
            }

            long    freesize      = 0;
            boolean preexists     = fa.isStreamElement(dataFileName);
            boolean isIncremental = database.logger.propIncrementBackup;
            boolean restore = database.getProperties().getDBModified()
                              == HsqlDatabaseProperties.FILES_MODIFIED;

            if (preexists && restore) {
                if (isIncremental) {
                    preexists = restoreBackupIncremental();
                } else {
                    preexists = restoreBackup();
                }
            }

            dataFile = RAFile.newScaledRAFile(database, dataFileName,
                                              readonly, fileType);

            if (preexists) {
                dataFile.seek(LONG_EMPTY_SIZE);

                freesize = dataFile.readLong();

                dataFile.seek(LONG_FREE_POS_POS);

                fileFreePosition      = dataFile.readLong();
                fileStartFreePosition = fileFreePosition;

                dataFile.seek(FLAGS_POS);

                int flags = dataFile.readInt();

                is180 = !BitMap.isSet(flags, FLAG_190);

                openShadowFile();
            } else {
                initNewFile();
            }

            initBuffers();

            fileModified  = false;
            cacheModified = false;
            spaceManager  = new DataSpaceManagerSimple(this);

            logInfoEvent("dataFileCache open end");
        } catch (Throwable t) {
            logSevereEvent("dataFileCache open failed", t);
            release();

            throw Error.error(t, ErrorCode.FILE_IO_ERROR,
                              ErrorCode.M_DataFileCache_open, new Object[] {
                t.toString(), dataFileName
            });
        }
    }

    void initNewFile() {

        try {
            fileFreePosition      = initialFreePos;
            fileStartFreePosition = initialFreePos;

            dataFile.seek(LONG_FREE_POS_POS);
            dataFile.writeLong(fileFreePosition);

            // set shadowed flag;
            int flags = 0;

            if (database.logger.propIncrementBackup) {
                flags = BitMap.set(flags, FLAG_ISSHADOWED);
            }

            flags = BitMap.set(flags, FLAG_ISSAVED);
            flags = BitMap.set(flags, FLAG_190);

            dataFile.seek(FLAGS_POS);
            dataFile.writeInt(flags);
            dataFile.synch();

            is180 = false;
        } catch (Throwable t) {
            throw Error.error(ErrorCode.FILE_IO_ERROR, t);
        }
    }

    private void openShadowFile() {

        if (database.logger.propIncrementBackup
                && fileFreePosition != initialFreePos) {
            shadowFile = new RAShadowFile(database, dataFile, backupFileName,
                                          fileFreePosition, 1 << 14);
        }
    }

    void setIncrementBackup(boolean value) {

        writeLock.lock();

        try {
            dataFile.seek(FLAGS_POS);

            int flags = dataFile.readInt();

            if (value) {
                flags = BitMap.set(flags, FLAG_ISSHADOWED);
            } else {
                flags = BitMap.unset(flags, FLAG_ISSHADOWED);
            }

            dataFile.seek(FLAGS_POS);
            dataFile.writeInt(flags);
            dataFile.synch();

            fileModified = true;
        } catch (Throwable t) {
            logSevereEvent("DataFileCache.setIncrementalBackup", t);
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * Restores a compressed backup or the .data file.
     */
    private boolean restoreBackup() {
        return restoreBackup(database, dataFileName, backupFileName);
    }

    static boolean restoreBackupFile(Database database, String dataFileName,
                                     String backupFileName) {

        if (database.logger.propIncrementBackup) {
            return restoreBackupIncremental(database, dataFileName,
                                            backupFileName);
        } else {
            return restoreBackup(database, dataFileName, backupFileName);
        }
    }

    static boolean restoreBackup(Database database, String dataFileName,
                                 String backupFileName) {

        try {
            FileAccess fileAccess = database.logger.getFileAccess();

            // todo - in case data file cannot be deleted, reset it
            deleteFile(database, dataFileName);

            if (fileAccess.isStreamElement(backupFileName)) {
                FileArchiver.unarchive(backupFileName, dataFileName,
                                       fileAccess,
                                       FileArchiver.COMPRESSION_ZIP);

                return true;
            }

            return false;
        } catch (Throwable t) {
            database.logger.logSevereEvent("DataFileCache.restoreBackup", t);

            throw Error.error(t, ErrorCode.FILE_IO_ERROR,
                              ErrorCode.M_Message_Pair, new Object[] {
                t.toString(), backupFileName
            });
        }
    }

    /**
     * Restores in from an incremental backup
     */
    private boolean restoreBackupIncremental() {
        return restoreBackupIncremental(database, dataFileName,
                                        backupFileName);
    }

    /**
     * Restores in from an incremental backup
     */
    static boolean restoreBackupIncremental(Database database,
            String dataFileName, String backupFileName) {

        try {
            FileAccess fileAccess = database.logger.getFileAccess();

            if (fileAccess.isStreamElement(backupFileName)) {
                RAShadowFile.restoreFile(database, backupFileName,
                                         dataFileName);
                deleteFile(database, backupFileName);

                return true;
            }

            return false;
        } catch (Throwable e) {
            database.logger.logSevereEvent(
                "DataFileCache.restoreBackupIncremental", e);

            throw Error.error(ErrorCode.FILE_IO_ERROR, e);
        }
    }

    /**
     *  Abandons changed rows and closes the .data file.
     */
    public void release() {

        writeLock.lock();

        try {
            if (dataFile == null) {
                return;
            }

            if (shadowFile != null) {
                shadowFile.close();

                shadowFile = null;
            }

            dataFile.close();
            logDetailEvent("dataFileCache file closed");

            dataFile = null;
        } catch (Throwable t) {
            logSevereEvent("DataFileCache.release", t);
        } finally {
            writeLock.unlock();
        }
    }

    /**
     *  Writes out all cached rows that have been modified and the
     *  free position pointer for the *.data file and then closes the file.
     */
    public void close() {

        writeLock.lock();

        try {
            if (dataFile == null) {
                return;
            }

            reset();
            dataFile.close();
            logDetailEvent("dataFileCache file close end");

            dataFile = null;

            boolean empty = fileFreePosition == initialFreePos;

            if (empty) {
                deleteFile();
                deleteBackup();
            }
        } catch (HsqlException e) {
            throw e;
        } catch (Throwable t) {
            logSevereEvent("DataFileCache.close", t);

            throw Error.error(t, ErrorCode.FILE_IO_ERROR,
                              ErrorCode.M_DataFileCache_close, new Object[] {
                t.toString(), dataFileName
            });
        } finally {
            writeLock.unlock();
        }
    }

    protected void clear() {

        writeLock.lock();

        try {
            cache.clear();
        } finally {
            writeLock.unlock();
        }
    }

    public void adjustStoreCount(int adjust) {

        writeLock.lock();

        try {
            storeCount += adjust;
        } finally {
            writeLock.unlock();
        }
    }

    public void reopen() {
        spaceManager.initialiseSpaces();
        openShadowFile();
    }

    /**
     * Commits all the changes to the file
     */
    public void reset() {

        writeLock.lock();

        try {
            if (cacheReadonly) {
                return;
            }

            logInfoEvent("dataFileCache commit start");
            spaceManager.reset();
            cache.saveAll();

            // set empty
            dataFile.seek(LONG_EMPTY_SIZE);
            dataFile.writeLong(spaceManager.getLostBlocksSize());

            // set end
            dataFile.seek(LONG_FREE_POS_POS);
            dataFile.writeLong(fileFreePosition);
            dataFile.seek(INT_SPACE_LIST_POS);

            int pos = (int) (spaceManagerPosition
                             / DataSpaceManager.fixedBlockSizeUnit);

            dataFile.writeInt(pos);

            // set saved flag;
            dataFile.seek(FLAGS_POS);

            int flags = dataFile.readInt();

            flags = BitMap.set(flags, FLAG_ISSAVED);

            dataFile.seek(FLAGS_POS);
            dataFile.writeInt(flags);
            dataFile.synch();
            logDetailEvent("file sync end");

            fileModified          = false;
            cacheModified         = false;
            fileStartFreePosition = fileFreePosition;

            if (shadowFile != null) {
                shadowFile.close();

                shadowFile = null;
            }

            logInfoEvent("dataFileCache commit end");
        } catch (Throwable t) {
            logSevereEvent("DataFileCache.reset commit", t);

            throw Error.error(t, ErrorCode.FILE_IO_ERROR,
                              ErrorCode.M_DataFileCache_close, new Object[] {
                t.toString(), dataFileName
            });
        } finally {
            writeLock.unlock();
        }
    }

    protected void initBuffers() {

        if (rowOut == null) {
            if (is180) {
                rowOut = new RowOutputBinary180(initIOBufferSize,
                                                cachedRowPadding);
            } else {
                rowOut = new RowOutputBinaryEncode(database.logger.getCrypto(),
                                                   initIOBufferSize,
                                                   cachedRowPadding);
            }
        }

        if (rowIn == null) {
            if (is180) {
                rowIn = new RowInputBinary180(new byte[initIOBufferSize]);
            } else {
                rowIn = new RowInputBinaryDecode(database.logger.getCrypto(),
                                                 new byte[initIOBufferSize]);
            }
        }
    }

    DataFileDefrag defrag() {

        writeLock.lock();

        try {
            cache.saveAll();

            DataFileDefrag dfd = new DataFileDefrag(database, this,
                dataFileName);

            dfd.process();
            close();
            cache.clear();

            if (!database.logger.propIncrementBackup) {
                backupNewDataFile(true);
            }

            database.schemaManager.setTempIndexRoots(dfd.getIndexRoots());

            try {
                database.logger.log.writeScript(false);
            } finally {
                database.schemaManager.setTempIndexRoots(null);
            }

            database.getProperties().setProperty(
                HsqlDatabaseProperties.hsqldb_script_format,
                database.logger.propScriptFormat);
            database.getProperties().setDBModified(
                HsqlDatabaseProperties.FILES_MODIFIED_NEW);
            database.logger.log.closeLog();
            database.logger.log.deleteLog();
            database.logger.log.renameNewScript();
            renameBackupFile();
            renameDataFile();
            database.getProperties().setDBModified(
                HsqlDatabaseProperties.FILES_NOT_MODIFIED);
            open(false);
            database.schemaManager.setIndexRoots(dfd.getIndexRoots());

            if (database.logger.log.dbLogWriter != null) {
                database.logger.log.openLog();
            }

            return dfd;
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * Used when a row is deleted as a result of some DML or DDL statement.
     * Removes the row from the cache data structures.
     * Adds the file space for the row to the list of free positions.
     */
    public void remove(CachedObject object) {

        writeLock.lock();

        try {
            release(object.getPos());
        } finally {
            writeLock.unlock();
        }
    }

    public void removePersistence(CachedObject object) {}

    public void add(CachedObject object) {

        writeLock.lock();

        try {
            cacheModified = true;

            cache.put(object);

            if (object.getStorageSize() > initIOBufferSize) {
                rowOut.reset(object.getStorageSize());
            }
        } finally {
            writeLock.unlock();
        }
    }

    public int getStorageSize(long i) {

        readLock.lock();

        try {
            CachedObject value = cache.get(i);

            if (value != null) {
                return value.getStorageSize();
            }
        } finally {
            readLock.unlock();
        }

        return readSize(i);
    }

    public void replace(CachedObject object) {

        writeLock.lock();

        try {
            long pos = object.getPos();

            cache.replace(pos, object);
        } finally {
            writeLock.unlock();
        }
    }

    public CachedObject get(CachedObject object, PersistentStore store,
                            boolean keep) {

        readLock.lock();

        long pos;

        try {
            if (object.isInMemory()) {
                if (keep) {
                    object.keepInMemory(true);
                }

                return object;
            }

            pos = object.getPos();

            if (pos < 0) {
                return null;
            }

            object = cache.get(pos);

            if (object != null) {
                if (keep) {
                    object.keepInMemory(true);
                }

                return object;
            }
        } finally {
            readLock.unlock();
        }

        return getFromFile(pos, store, keep);
    }

    public CachedObject get(long pos, int size, PersistentStore store,
                            boolean keep) {

        CachedObject object;

        if (pos < 0) {
            return null;
        }

        readLock.lock();

        try {
            object = cache.get(pos);

            if (object != null) {
                if (keep) {
                    object.keepInMemory(true);
                }

                return object;
            }
        } finally {
            readLock.unlock();
        }

        return getFromFile(pos, size, store, keep);
    }

    public CachedObject get(long pos, PersistentStore store, boolean keep) {

        CachedObject object;

        if (pos < 0) {
            return null;
        }

        readLock.lock();

        try {
            object = cache.get(pos);

            if (object != null) {
                if (keep) {
                    object.keepInMemory(true);
                }

                return object;
            }
        } finally {
            readLock.unlock();
        }

        return getFromFile(pos, store, keep);
    }

    private CachedObject getFromFile(long pos, PersistentStore store,
                                     boolean keep) {

        CachedObject object = null;

        writeLock.lock();

        try {
            object = cache.get(pos);

            if (object != null) {
                if (keep) {
                    object.keepInMemory(true);
                }

                return object;
            }

            for (int j = 0; j < 2; j++) {
                try {
                    readObject(pos);

                    object = store.get(rowIn);

                    break;
                } catch (Throwable t) {
                    if (t instanceof OutOfMemoryError) {
                        cache.clearUnchanged();
                        System.gc();
                        logSevereEvent(dataFileName
                                       + " getFromFile out of mem " + pos, t);

                        if (j > 0) {
                            HsqlException ex =
                                Error.error(ErrorCode.OUT_OF_MEMORY, t);

                            ex.info = rowIn;

                            throw ex;
                        }
                    } else {
                        HsqlException ex =
                            Error.error(ErrorCode.GENERAL_IO_ERROR, t);

                        ex.info = rowIn;

                        throw ex;
                    }
                }
            }

            // for text tables with empty rows at the beginning,
            // pos may move forward in readObject
            cache.put(object);

            if (keep) {
                object.keepInMemory(true);
            }

            store.set(object);

            return object;
        } catch (HsqlException e) {
            logSevereEvent(dataFileName + " getFromFile failed " + pos, e);

            throw e;
        } finally {
            writeLock.unlock();
        }
    }

    private CachedObject getFromFile(long pos, int size,
                                     PersistentStore store, boolean keep) {

        CachedObject object = null;

        writeLock.lock();

        try {
            object = cache.get(pos);

            if (object != null) {
                if (keep) {
                    object.keepInMemory(true);
                }

                return object;
            }

            for (int j = 0; j < 2; j++) {
                try {
                    readObject(pos, size);

                    object = store.get(rowIn);

                    break;
                } catch (OutOfMemoryError err) {
                    cache.clearUnchanged();
                    System.gc();
                    logSevereEvent(dataFileName + " getFromFile out of mem "
                                   + pos, err);

                    if (j > 0) {
                        throw err;
                    }
                }
            }

            // for text tables with empty rows at the beginning,
            // pos may move forward in readObject
            cache.put(object);

            if (keep) {
                object.keepInMemory(true);
            }

            store.set(object);

            return object;
        } catch (HsqlException e) {
            logSevereEvent(dataFileName + " getFromFile failed " + pos, e);

            throw e;
        } finally {
            writeLock.unlock();
        }
    }

    RowInputInterface getRaw(int i) {

        writeLock.lock();

        try {
            readObject(i);

            return rowIn;
        } finally {
            writeLock.unlock();
        }
    }

    private int readSize(long pos) {

        writeLock.lock();

        try {
            dataFile.seek(pos * dataFileScale);

            return dataFile.readInt();
        } catch (Throwable t) {
            logSevereEvent("DataFileCache.readSize", t, pos);

            throw Error.error(ErrorCode.DATA_FILE_ERROR, t);
        } finally {
            writeLock.unlock();
        }
    }

    private void readObject(long pos) {

        try {
            dataFile.seek(pos * dataFileScale);

            int size = dataFile.readInt();

            rowIn.resetRow(pos, size);
            dataFile.read(rowIn.getBuffer(), 4, size - 4);
        } catch (Throwable t) {
            logSevereEvent("DataFileCache.readObject", t, pos);

            HsqlException ex = Error.error(ErrorCode.DATA_FILE_ERROR, t);

            if (rowIn.getPos() != pos) {
                rowIn.resetRow(pos, 0);
            }

            ex.info = rowIn;

            throw ex;
        }
    }

    protected void readObject(long pos, int size) {

        try {
            rowIn.resetBlock(pos, size);
            dataFile.seek(pos * dataFileScale);
            dataFile.read(rowIn.getBuffer(), 0, size);
        } catch (Throwable t) {
            logSevereEvent("DataFileCache.readObject", t, pos);

            HsqlException ex = Error.error(ErrorCode.DATA_FILE_ERROR, t);

            ex.info = rowIn;

            throw ex;
        }
    }

    public void releaseRange(long start, long limit) {

        writeLock.lock();

        try {
            Iterator it = cache.getIterator();

            while (it.hasNext()) {
                CachedObject o   = (CachedObject) it.next();
                long         pos = o.getPos();

                if (pos >= start && pos < limit) {
                    o.setInMemory(false);
                    it.remove();
                }
            }
        } finally {
            writeLock.unlock();
        }
    }

    public CachedObject release(long pos) {

        writeLock.lock();

        try {
            return cache.release(pos);
        } finally {
            writeLock.unlock();
        }
    }

    protected void saveRows(CachedObject[] rows, int offset, int count) {

        if (count == 0) {
            return;
        }

        copyShadow(rows, offset, count);
        setFileModified();

        for (int i = offset; i < offset + count; i++) {
            CachedObject r = rows[i];

            saveRowNoLock(r);

            rows[i] = null;
        }
    }

    /**
     * Writes out the specified Row. Will write only the Nodes or both Nodes
     * and table row data depending on what is not already persisted to disk.
     */
    public void saveRow(CachedObject row) {

        writeLock.lock();

        try {
            copyShadow(row);
            setFileModified();
            saveRowNoLock(row);
        } finally {
            writeLock.unlock();
        }
    }

    public void saveRowOutput(long pos) {

        try {
            dataFile.seek(pos * dataFileScale);
            dataFile.write(rowOut.getOutputStream().getBuffer(), 0,
                           rowOut.getOutputStream().size());
        } catch (Throwable t) {
            logSevereEvent("DataFileCache.saveRowOutput", t, pos);

            throw Error.error(ErrorCode.DATA_FILE_ERROR, t);
        }
    }

    protected void saveRowNoLock(CachedObject row) {

        try {
            rowOut.reset();
            row.write(rowOut);
            dataFile.seek(row.getPos() * dataFileScale);
            dataFile.write(rowOut.getOutputStream().getBuffer(), 0,
                           rowOut.getOutputStream().size());
        } catch (Throwable t) {
            logSevereEvent("DataFileCache.saveRowNoLock", t, row.getPos());

            throw Error.error(ErrorCode.DATA_FILE_ERROR, t);
        }
    }

    protected void copyShadow(CachedObject[] rows, int offset, int count) {

        if (shadowFile != null) {
            long time    = cache.saveAllTimer.elapsedTime();
            long seekpos = 0;

            try {
                for (int i = offset; i < offset + count; i++) {
                    CachedObject row = rows[i];

                    seekpos = row.getPos() * dataFileScale;

                    shadowFile.copy(seekpos, row.getStorageSize());
                }

                shadowFile.synch();
            } catch (Throwable t) {
                logSevereEvent("DataFileCache.copyShadow", t, seekpos);

                throw Error.error(ErrorCode.DATA_FILE_ERROR, t);
            }

            time = cache.saveAllTimer.elapsedTime() - time;

            logDetailEvent("copyShadow [size, time] "
                           + shadowFile.getSavedLength() + " " + time);
        }
    }

    protected void copyShadow(CachedObject row) {

        if (shadowFile != null) {
            long seekpos = row.getPos() * dataFileScale;

            try {
                shadowFile.copy(seekpos, row.getStorageSize());
                shadowFile.synch();
            } catch (Throwable t) {
                logSevereEvent("DataFileCache.copyShadow", t, row.getPos());

                throw Error.error(ErrorCode.DATA_FILE_ERROR, t);
            }
        }
    }

    /**
     *  Saves the *.data file as compressed *.backup.
     *
     * @throws  HsqlException
     */
    void backupDataFile(boolean newFile) {
        backupFile(database, dataFileName, backupFileName, newFile);
    }

    void backupNewDataFile(boolean newFile) {
        backupFile(database, dataFileName + Logger.newFileExtension,
                   backupFileName, newFile);
    }

    static void backupFile(Database database, String fileName,
                           String backupFileName, boolean newFile) {

        try {
            FileAccess fa = database.logger.getFileAccess();

            if (database.logger.propIncrementBackup) {
                if (fa.isStreamElement(backupFileName)) {
                    deleteFile(database, backupFileName);

                    if (fa.isStreamElement(backupFileName)) {
                        throw Error.error(ErrorCode.DATA_FILE_ERROR,
                                          "cannot delete old backup file");
                    }
                }

                return;
            }

            if (fa.isStreamElement(fileName)) {
                if (newFile) {
                    backupFileName += Logger.newFileExtension;
                } else {
                    deleteFile(database, backupFileName);

                    if (fa.isStreamElement(backupFileName)) {
                        throw Error.error(ErrorCode.DATA_FILE_ERROR,
                                          "cannot delete old backup file");
                    }
                }

                FileArchiver.archive(fileName, backupFileName, fa,
                                     FileArchiver.COMPRESSION_ZIP);
            }
        } catch (Throwable t) {
            database.logger.logSevereEvent("DataFileCache.backupFile", t);

            throw Error.error(ErrorCode.DATA_FILE_ERROR, t);
        }
    }

    void renameBackupFile() {
        renameBackupFile(database, backupFileName);
    }

    static void renameBackupFile(Database database, String backupFileName) {

        FileAccess fileAccess = database.logger.getFileAccess();

        if (database.logger.propIncrementBackup) {
            deleteFile(database, backupFileName);

            return;
        }

        if (fileAccess.isStreamElement(backupFileName
                                       + Logger.newFileExtension)) {
            deleteFile(database, backupFileName);
            fileAccess.renameElement(backupFileName + Logger.newFileExtension,
                                     backupFileName);
        }
    }

    /**
     *  Renames the *.data.new file.
     *
     * @throws  HsqlException
     */
    void renameDataFile() {
        renameDataFile(database, dataFileName);
    }

    static void renameDataFile(Database database, String dataFileName) {

        FileAccess fileAccess = database.logger.getFileAccess();

        if (fileAccess.isStreamElement(dataFileName
                                       + Logger.newFileExtension)) {
            deleteFile(database, dataFileName);
            fileAccess.renameElement(dataFileName + Logger.newFileExtension,
                                     dataFileName);
        }
    }

    void deleteFile() {
        deleteFile(database, dataFileName);
    }

    static void deleteFile(Database database, String fileName) {

        FileAccess fileAccess = database.logger.getFileAccess();

        // first attemp to delete
        fileAccess.removeElement(fileName);

        if (database.logger.isStoredFileAccess()) {
            return;
        }

        if (fileAccess.isStreamElement(fileName)) {
            database.logger.log.deleteOldDataFiles();
            fileAccess.removeElement(fileName);

            if (fileAccess.isStreamElement(fileName)) {
                String discardName = FileUtil.newDiscardFileName(fileName);

                fileAccess.renameElement(fileName, discardName);
            }
        }
    }

    void deleteBackup() {
        deleteFile(database, backupFileName);
    }

    /**
     * Delta must always result in block multiples
     */
    public long enlargeFileSpace(long delta) {

        writeLock.lock();

        try {
            long position = fileFreePosition;

            if (position + delta > maxDataFileSize) {
                logSevereEvent("data file reached maximum size "
                               + this.dataFileName, null);

                throw Error.error(ErrorCode.DATA_FILE_IS_FULL);
            }

            boolean result = dataFile.ensureLength(position + delta);

            if (!result) {
                logSevereEvent("data file cannot be enlarged - disk spacee "
                               + this.dataFileName, null);

                throw Error.error(ErrorCode.DATA_FILE_IS_FULL);
            }

            fileFreePosition += delta;

            return position;
        } finally {
            writeLock.unlock();
        }
    }

    public int capacity() {
        return maxCacheRows;
    }

    public long bytesCapacity() {
        return maxCacheBytes;
    }

    public long getTotalCachedBlockSize() {
        return cache.getTotalCachedBlockSize();
    }

    public long getLostBlockSize() {

        readLock.lock();

        try {
            return spaceManager.getLostBlocksSize();
        } finally {
            readLock.unlock();
        }
    }

    public long getFileFreePos() {
        return fileFreePosition;
    }

    public int getCachedObjectCount() {
        return cache.size();
    }

    public int getAccessCount() {
        return cache.incrementAccessCount();
    }

    public String getFileName() {
        return dataFileName;
    }

    public int getDataFileScale() {
        return dataFileScale;
    }

    public boolean hasRowInfo() {
        return hasRowInfo;
    }

    public boolean isFileModified() {
        return fileModified;
    }

    public boolean isModified() {
        return cacheModified;
    }

    public boolean isFileOpen() {
        return dataFile != null;
    }

    protected void setFileModified() {

        try {
            if (!fileModified) {

                // unset saved flag;
                long start = cache.saveAllTimer.elapsedTime();

                dataFile.seek(FLAGS_POS);

                int flags = dataFile.readInt();

                flags = BitMap.unset(flags, FLAG_ISSAVED);

                dataFile.seek(FLAGS_POS);
                dataFile.writeInt(flags);
                dataFile.synch();
                logDetailEvent("setFileModified flag set ");

                fileModified = true;
            }
        } catch (Throwable t) {
            logSevereEvent("DataFileCache.setFileModified", t);

            throw Error.error(ErrorCode.DATA_FILE_ERROR, t);
        }
    }

    public int getFlags() {

        try {
            dataFile.seek(FLAGS_POS);

            int flags = dataFile.readInt();

            return flags;
        } catch (Throwable t) {
            logSevereEvent("DataFileCache.setFlags", t);
        }

        return 0;
    }

    public boolean isDataReadOnly() {
        return this.cacheReadonly;
    }

    public RAShadowFile getShadowFile() {
        return shadowFile;
    }

    private void logSevereEvent(String message, Throwable t, long position) {

        if (logEvents) {
            StringBuffer sb = new StringBuffer(message);

            sb.append(' ').append(position);

            message = sb.toString();

            database.logger.logSevereEvent(message, t);
        }
    }

    void logSevereEvent(String message, Throwable t) {

        if (logEvents) {
            database.logger.logSevereEvent(message, t);
        }
    }

    void logInfoEvent(String message) {

        if (logEvents) {
            database.logger.logInfoEvent(message);
        }
    }

    void logDetailEvent(String message) {

        if (logEvents) {
            database.logger.logDetailEvent(message);
        }
    }
}
