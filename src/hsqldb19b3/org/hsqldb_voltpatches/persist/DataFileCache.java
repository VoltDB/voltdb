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

import java.io.File;
import java.io.IOException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.hsqldb_voltpatches.Database;
import org.hsqldb_voltpatches.Error;
import org.hsqldb_voltpatches.ErrorCode;
import org.hsqldb_voltpatches.HsqlException;
import org.hsqldb_voltpatches.lib.FileAccess;
import org.hsqldb_voltpatches.lib.FileArchiver;
import org.hsqldb_voltpatches.lib.FileUtil;
import org.hsqldb_voltpatches.lib.SimpleLog;
import org.hsqldb_voltpatches.lib.StopWatch;
import org.hsqldb_voltpatches.lib.Storage;
import org.hsqldb_voltpatches.rowio.RowInputBinary;
import org.hsqldb_voltpatches.rowio.RowInputInterface;
import org.hsqldb_voltpatches.rowio.RowOutputBinary;
import org.hsqldb_voltpatches.rowio.RowOutputInterface;
import org.hsqldb_voltpatches.store.BitMap;

/**
 * Acts as a manager for CACHED table persistence.<p>
 *
 * This contains the top level functionality. Provides file management services
 * and access.<p>
 *
 * Rewritten for 1.8.0 together with Cache.
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 1.9.0
 * @since 1.7.2
 */
public class DataFileCache {

    protected FileAccess fa;

    // flags
//    public static final int FLAG_ISSHADOWED = 1;
    public static final int FLAG_ISSAVED = 2;
    public static final int FLAG_ROWINFO = 3;

    // file format fields
    static final int LONG_EMPTY_SIZE      = 4;     // empty space size
    static final int LONG_FREE_POS_POS    = 12;    // where iFreePos is saved
    static final int LONG_EMPTY_INDEX_POS = 20;    // empty space index
    static final int FLAGS_POS            = 28;
    static final int INITIAL_FREE_POS     = 32;

    //
    DataFileBlockManager     freeBlocks;
    private static final int initIOBufferSize = 256;

    //
    protected String   fileName;
    protected String   backupFileName;
    protected Database database;

    // this flag is used externally to determine if a backup is required
    protected boolean fileModified;
    protected int     cacheFileScale;

    // post openning constant fields
    protected boolean cacheReadonly;

    // cache operation mode
    protected boolean storeOnInsert;

    //
    protected int     cachedRowPadding = 8;
    protected boolean hasRowInfo       = false;

    // reusable input / output streams
    protected RowInputInterface rowIn;
    public RowOutputInterface   rowOut;

    //
    public long maxDataFileSize;

    //
    boolean incBackup;

    //
    protected Storage       dataFile;
    protected volatile long fileFreePosition;
    protected int           maxCacheSize;          // number of Rows
    protected long          maxCacheBytes;         // number of bytes
    protected int           maxFreeBlocks;
    protected Cache         cache;

    //
    private RAShadowFile shadowFile;

    //
    ReadWriteLock lock      = new ReentrantReadWriteLock();
    Lock          readLock  = lock.readLock();
    Lock          writeLock = lock.writeLock();

    public DataFileCache(Database db, String baseFileName) {

        initParams(db, baseFileName);

        cache = new Cache(this);
    }

    /**
     * initial external parameters are set here.
     */
    protected void initParams(Database database, String baseFileName) {

        HsqlDatabaseProperties props = database.getProperties();

        fileName       = baseFileName + ".data";
        backupFileName = baseFileName + ".backup";
        this.database  = database;
        fa             = database.getFileAccess();

        int cacheScale =
            props.getIntegerProperty(HsqlDatabaseProperties.hsqldb_cache_scale,
                                     14, 8, 18);
        int cacheSizeScale = props.getIntegerProperty(
            HsqlDatabaseProperties.hsqldb_cache_size_scale, 10, 6, 20);
        int cacheFreeCountScale = props.getIntegerProperty(
            HsqlDatabaseProperties.hsqldb_cache_free_count_scale, 9, 6, 12);

        incBackup = database.getProperties().isPropertyTrue(
            HsqlDatabaseProperties.hsqldb_inc_backup);
        cacheFileScale = database.getProperties().getIntegerProperty(
            HsqlDatabaseProperties.hsqldb_cache_file_scale, 8);

        if (cacheFileScale != 1) {
            cacheFileScale = 8;
        }

        cachedRowPadding = 8;

        if (cacheFileScale > 8) {
            cachedRowPadding = cacheFileScale;
        }

        cacheReadonly = database.isFilesReadOnly();

        int lookupTableLength = 1 << cacheScale;
        int avgRowBytes       = 1 << cacheSizeScale;

        maxCacheSize    = lookupTableLength * 3;
        maxCacheBytes   = maxCacheSize * avgRowBytes;
        maxDataFileSize = cacheFileScale == 1 ? Integer.MAX_VALUE
                                              : (long) Integer.MAX_VALUE
                                              * cacheFileScale;
        maxFreeBlocks = 1 << cacheFreeCountScale;
        dataFile      = null;
        shadowFile    = null;
    }

    /**
     * Opens the *.data file for this cache, setting the variables that
     * allow access to the particular database version of the *.data file.
     */
    public void open(boolean readonly) {

        fileFreePosition = 0;

        database.logger.appLog.logContext(SimpleLog.LOG_NORMAL, "start");

        try {
            boolean preexists = database.isFilesInJar();
            long    freesize  = 0;

            if (!preexists && fa.isStreamElement(fileName)) {

                // OOo related code
                if (database.isStoredFileAccess()) {
                    preexists = true;
                }

                // OOo end
                if (!preexists) {

                    // discard "empty" databases
                    File f = new File(fileName);

                    preexists = f.length() > INITIAL_FREE_POS;
                }
            }

            if (preexists) {
                String version = database.getProperties().getProperty(
                    HsqlDatabaseProperties.hsqldb_cache_version);
                boolean v17 =
                    HsqlDatabaseProperties.VERSION_STRING_1_7_0.equals(
                        version);

                // for later versions
                boolean v18 =
                    HsqlDatabaseProperties.VERSION_STRING_1_8_0.equals(
                        version);

                if (!v17) {
                    throw Error.error(ErrorCode.WRONG_DATABASE_FILE_VERSION);
                }
            }

            boolean isNio = database.getProperties().isPropertyTrue(
                HsqlDatabaseProperties.hsqldb_nio_data_file);
            int fileType = isNio ? ScaledRAFile.DATA_FILE_NIO
                                 : ScaledRAFile.DATA_FILE_RAF;

            if (database.isFilesInJar()) {
                fileType = ScaledRAFile.DATA_FILE_JAR;
            }

            // oj@openofice.org - change to file access api
            String cname =
                database.getURLProperties().getProperty("storage_class_name");
            String skey =
                database.getURLProperties().getProperty("storage_key");

            dataFile = ScaledRAFile.newScaledRAFile(database, fileName,
                    readonly, fileType, cname, skey);

            if (preexists) {
                dataFile.seek(FLAGS_POS);

                int flags = dataFile.readInt();

                hasRowInfo = BitMap.isSet(flags, FLAG_ROWINFO);

                dataFile.seek(LONG_EMPTY_SIZE);

                freesize = dataFile.readLong();

                dataFile.seek(LONG_FREE_POS_POS);

                fileFreePosition = dataFile.readLong();

                if (fileFreePosition < INITIAL_FREE_POS) {
                    fileFreePosition = INITIAL_FREE_POS;
                }

                if (!readonly && fileFreePosition != INITIAL_FREE_POS
                        && incBackup) {
                    shadowFile = new RAShadowFile(database, dataFile,
                                                  backupFileName,
                                                  fileFreePosition, 1 << 14);
                }
            } else {
                fileFreePosition = INITIAL_FREE_POS;

                dataFile.seek(LONG_FREE_POS_POS);
                dataFile.writeLong(INITIAL_FREE_POS);

                // set unsaved flag;
                dataFile.seek(FLAGS_POS);
                dataFile.writeInt(0);
            }

            initBuffers();

            fileModified = false;
            freeBlocks = new DataFileBlockManager(maxFreeBlocks,
                                                  cacheFileScale, freesize);

            database.logger.appLog.logContext(SimpleLog.LOG_NORMAL, "end");
        } catch (Throwable e) {
            database.logger.appLog.logContext(e, "failed");
            close(false);

            throw Error.error(ErrorCode.FILE_IO_ERROR,
                              ErrorCode.M_DataFileCache_open, new Object[] {
                e, fileName
            });
        }
    }

    /**
     *  Parameter write indicates either an orderly close, or a fast close
     *  without backup.
     *
     *  When false, just closes the file.
     *
     *  When true, writes out all cached rows that have been modified and the
     *  free position pointer for the *.data file and then closes the file.
     */
    public void close(boolean write) {

        SimpleLog appLog = database.logger.appLog;

        try {
            if (cacheReadonly) {
                if (dataFile != null) {
                    dataFile.close();

                    dataFile = null;
                }

                return;
            }

            StopWatch sw = new StopWatch();

            appLog.sendLine(SimpleLog.LOG_NORMAL,
                            "DataFileCache.close(" + write + ") : start");

            if (write) {
                cache.saveAll();
                Error.printSystemOut("saveAll: " + sw.elapsedTime());
                appLog.sendLine(SimpleLog.LOG_NORMAL,
                                "DataFileCache.close() : save data");

                if (fileModified || freeBlocks.isModified()) {

                    // set empty
                    dataFile.seek(LONG_EMPTY_SIZE);
                    dataFile.writeLong(freeBlocks.getLostBlocksSize());

                    // set end
                    dataFile.seek(LONG_FREE_POS_POS);
                    dataFile.writeLong(fileFreePosition);

                    // set saved flag;
                    dataFile.seek(FLAGS_POS);

                    int flag = BitMap.set(0, FLAG_ISSAVED);

                    if (hasRowInfo) {
                        flag = BitMap.set(flag, FLAG_ROWINFO);
                    }

                    dataFile.writeInt(flag);
                    appLog.sendLine(SimpleLog.LOG_NORMAL,
                                    "DataFileCache.close() : flags");

                    //
                    if (dataFile.length() != fileFreePosition) {
                        dataFile.seek(fileFreePosition);
                    }

                    appLog.sendLine(SimpleLog.LOG_NORMAL,
                                    "DataFileCache.close() : seek end");
                    Error.printSystemOut("pos and flags: " + sw.elapsedTime());
                }
            }

            if (dataFile != null) {
                dataFile.close();
                appLog.sendLine(SimpleLog.LOG_NORMAL,
                                "DataFileCache.close() : close");

                dataFile = null;

                Error.printSystemOut("close: " + sw.elapsedTime());
            }

            boolean empty = fileFreePosition == INITIAL_FREE_POS;

            if (empty) {
                fa.removeElement(fileName);
                fa.removeElement(backupFileName);
            }
        } catch (Throwable e) {
            appLog.logContext(e, null);

            throw Error.error(ErrorCode.FILE_IO_ERROR,
                              ErrorCode.M_DataFileCache_close, new Object[] {
                e, fileName
            });
        }
    }

    protected void initBuffers() {

        if (rowOut == null
                || rowOut.getOutputStream().getBuffer().length
                   > initIOBufferSize) {
            rowOut = new RowOutputBinary(256, cachedRowPadding);
        }

        if (rowIn == null || rowIn.getBuffer().length > initIOBufferSize) {
            rowIn = new RowInputBinary(new byte[256]);
        }
    }

    /**
     *  Writes out all the rows to a new file without fragmentation.
     */
    public void defrag() {

        if (cacheReadonly) {
            return;
        }

        if (fileFreePosition == INITIAL_FREE_POS) {
            return;
        }

        database.logger.appLog.logContext(SimpleLog.LOG_NORMAL, "start");

        try {
            boolean wasNio = dataFile.wasNio();

            cache.saveAll();

            DataFileDefrag dfd = new DataFileDefrag(database, this, fileName);

            dfd.process();
            close(false);
            deleteFile(wasNio);
            renameDataFile(wasNio);
            backupFile();
            database.getProperties().setProperty(
                HsqlDatabaseProperties.hsqldb_cache_version,
                HsqlDatabaseProperties.THIS_CACHE_VERSION);
            database.getProperties().save();
            cache.clear();

            cache = new Cache(this);

            open(cacheReadonly);
            dfd.updateTableIndexRoots();
            dfd.updateTransactionRowIDs();
        } catch (Throwable e) {
            database.logger.appLog.logContext(e, null);

            if (e instanceof HsqlException) {
                throw (HsqlException) e;
            } else {
                throw new HsqlException(
                    e, Error.getMessage(ErrorCode.GENERAL_IO_ERROR),
                    ErrorCode.GENERAL_IO_ERROR);
            }
        }

        database.logger.appLog.logContext(SimpleLog.LOG_NORMAL, "end");
    }

    /**
     * Used when a row is deleted as a result of some DML or DDL statement.
     * Removes the row from the cache data structures.
     * Adds the file space for the row to the list of free positions.
     */
    public void remove(int i, PersistentStore store) {

        writeLock.lock();

        try {
            CachedObject r = release(i);

            if (r != null) {
                int size = r.getStorageSize();

                freeBlocks.add(i, size);
            }
        } finally {
            writeLock.unlock();
        }
    }

    public void removePersistence(int i) {}

    /**
     * Allocates file space for the row. <p>
     *
     * Free space is requested from the block manager if it exists.
     * Otherwise the file is grown to accommodate it.
     */
    private int setFilePos(CachedObject r) {

        int rowSize = r.getStorageSize();
        int i       = freeBlocks == null ? -1
                                         : freeBlocks.get(rowSize);

        if (i == -1) {
            i = (int) (fileFreePosition / cacheFileScale);

            long newFreePosition = fileFreePosition + rowSize;

            if (newFreePosition > maxDataFileSize) {
                throw Error.error(ErrorCode.DATA_FILE_IS_FULL);
            }

            fileFreePosition = newFreePosition;
        }

        r.setPos(i);

        return i;
    }

    public void add(CachedObject object) {

        writeLock.lock();

        try {
            int i = setFilePos(object);

            cache.put(i, object);

            // was previously used for text tables
            if (storeOnInsert) {
                saveRow(object);
            }
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * For a CacheObject that had been previously released from the cache.
     * A new version is introduced, using the preallocated space for the object.
     */
    public void restore(CachedObject object) {

        writeLock.lock();

        try {
            int i = object.getPos();

            cache.put(i, object);

            // was previously used for text tables
            if (storeOnInsert) {
                saveRow(object);
            }
        } finally {
            writeLock.unlock();
        }
    }

    public int getStorageSize(int i) {

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

    public CachedObject get(CachedObject object, PersistentStore store,
                            boolean keep) {

        readLock.lock();

        int pos;

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

    public CachedObject get(int pos, PersistentStore store, boolean keep) {

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

    private CachedObject getFromFile(int pos, PersistentStore store,
                                     boolean keep) {

        CachedObject object      = null;
        boolean      outOfMemory = false;

        writeLock.lock();

        try {
            for (int j = 0; j < 5; j++) {
                outOfMemory = false;

                try {
                    RowInputInterface rowInput = readObject(pos);

                    if (rowInput == null) {
                        return null;
                    }

                    object = store.get(rowInput);

                    break;
                } catch (OutOfMemoryError err) {
                    cache.cleanUp();

                    outOfMemory = true;

                    database.logger.appLog.logContext(err, null);
                }
            }

            if (outOfMemory) {
                throw Error.error(ErrorCode.OUT_OF_MEMORY);
            }

            // for text tables with empty rows at the beginning,
            // pos may move forward in readObject
            pos = object.getPos();

            cache.put(pos, object);

            if (keep) {
                object.keepInMemory(true);
            }

            store.set(object);

            return object;
        } catch (HsqlException e) {
            database.logger.appLog.logContext(e, fileName + " get pos: "
                                              + pos);

            throw e;
        } finally {
            writeLock.unlock();
        }
    }

    RowInputInterface getRaw(int i) {
        return readObject(i);
    }

    protected int readSize(int pos) {

        writeLock.lock();

        try {
            dataFile.seek((long) pos * cacheFileScale);

            return dataFile.readInt();
        } catch (IOException e) {
            throw new HsqlException(e.getMessage(), "", 0);
        } finally {
            writeLock.unlock();
        }
    }

    protected RowInputInterface readObject(int pos) {

        writeLock.lock();

        try {
            dataFile.seek((long) pos * cacheFileScale);

            int size = dataFile.readInt();

            rowIn.resetRow(pos, size);
            dataFile.read(rowIn.getBuffer(), 4, size - 4);

            return rowIn;
        } catch (IOException e) {
            throw new HsqlException(e.getMessage(), "", 0);
        } finally {
            writeLock.unlock();
        }
    }

    public CachedObject release(int pos) {

        writeLock.lock();

        try {
            return cache.release(pos);
        } finally {
            writeLock.unlock();
        }
    }

    protected void saveRows(CachedObject[] rows, int offset, int count) {

        writeLock.lock();

        try {
            setFileModified();
            copyShadow(rows, offset, count);

            for (int i = offset; i < offset + count; i++) {
                CachedObject r = rows[i];

                saveRowNoLock(r);

                rows[i] = null;
            }
        } catch (HsqlException e) {
            database.logger.appLog.logContext(e, null);

            throw new HsqlException(e, "", 0);
        } catch (Throwable e) {
            database.logger.appLog.logContext(e, null);

            throw new HsqlException(e.toString(), "", 0);
        } finally {
            initBuffers();
            writeLock.unlock();
        }
    }

    /**
     * Writes out the specified Row. Will write only the Nodes or both Nodes
     * and table row data depending on what is not already persisted to disk.
     */
    public void saveRow(CachedObject row) {

        writeLock.lock();

        try {
            saveRowNoLock(row);
        } finally {
            writeLock.unlock();
        }
    }

    public void saveRowNoLock(CachedObject row) {

        try {
            setFileModified();
            rowOut.reset();
            row.write(rowOut);
            dataFile.seek((long) row.getPos() * cacheFileScale);
            dataFile.write(rowOut.getOutputStream().getBuffer(), 0,
                           rowOut.getOutputStream().size());
        } catch (IOException e) {
            throw new HsqlException(e.getMessage(), "", 0);
        }
    }

    protected void copyShadow(CachedObject[] rows, int offset,
                              int count) throws IOException {

        if (shadowFile != null) {
            for (int i = offset; i < offset + count; i++) {
                CachedObject row     = rows[i];
                long         seekpos = (long) row.getPos() * cacheFileScale;

                shadowFile.copy(seekpos, row.getStorageSize());
            }

            shadowFile.close();
        }
    }

    /**
     *  Saves the *.data file as compressed *.backup.
     *
     * @throws  HsqlException
     */
    void backupFile() {

        writeLock.lock();

        try {
            if (incBackup) {
                if (fa.isStreamElement(backupFileName)) {
                    fa.removeElement(backupFileName);
                }

                return;
            }

            if (fa.isStreamElement(fileName)) {
                FileArchiver.archive(fileName, backupFileName + ".new",
                                     database.getFileAccess(),
                                     FileArchiver.COMPRESSION_ZIP);
            }
        } catch (IOException e) {
            database.logger.appLog.logContext(e, null);

            throw new HsqlException(e.getMessage(), "", 0);
        } finally {
            writeLock.unlock();
        }
    }

    void renameBackupFile() {

        writeLock.lock();

        try {
            if (incBackup) {
                if (fa.isStreamElement(backupFileName)) {
                    fa.removeElement(backupFileName);
                }

                return;
            }

            if (fa.isStreamElement(backupFileName + ".new")) {
                fa.removeElement(backupFileName);
                fa.renameElement(backupFileName + ".new", backupFileName);
            }
        } finally {
            writeLock.unlock();
        }
    }

    /**
     *  Renames the *.data.new file.
     *
     * @throws  HsqlException
     */
    void renameDataFile(boolean wasNio) {

        writeLock.lock();

        try {
            if (fa.isStreamElement(fileName + ".new")) {
                deleteFile(wasNio);
                fa.renameElement(fileName + ".new", fileName);
            }
        } finally {
            writeLock.unlock();
        }
    }

    void deleteFile(boolean wasNio) {

        writeLock.lock();

        try {

            // first attemp to delete
            fa.removeElement(fileName);

            // OOo related code
            if (database.isStoredFileAccess()) {
                return;
            }

            // OOo end
            if (fa.isStreamElement(fileName)) {
                if (wasNio) {
                    System.gc();
                    fa.removeElement(fileName);
                }

                if (fa.isStreamElement(fileName)) {
                    fa.renameElement(fileName, fileName + ".old");

                    File oldfile = new File(fileName + ".old");

                    FileUtil.getDefaultInstance().deleteOnExit(oldfile);
                }
            }
        } finally {
            writeLock.unlock();
        }
    }

    void deleteBackup() {

        writeLock.lock();

        try {
            fa.removeElement(backupFileName);
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * This method deletes a data file or resets its free position.
     * this is used only for nio files - not OOo files
     */
    static void deleteOrResetFreePos(Database database, String filename) {

        ScaledRAFile raFile = null;

        database.getFileAccess().removeElement(filename);

        // OOo related code
        if (database.isStoredFileAccess()) {
            return;
        }

        // OOo end
        if (!database.getFileAccess().isStreamElement(filename)) {
            return;
        }

        try {
            raFile = new ScaledRAFile(database, filename, false);

            raFile.seek(LONG_FREE_POS_POS);
            raFile.writeLong(INITIAL_FREE_POS);
        } catch (IOException e) {
            database.logger.appLog.logContext(e, null);
        } finally {
            if (raFile != null) {
                try {
                    raFile.close();
                } catch (IOException e) {
                    database.logger.appLog.logContext(e, null);
                }
            }
        }
    }

    public int capacity() {
        return maxCacheSize;
    }

    public long bytesCapacity() {
        return maxCacheBytes;
    }

    public long getTotalCachedBlockSize() {
        return cache.getTotalCachedBlockSize();
    }

    public int getFreeBlockCount() {
        return freeBlocks.size();
    }

    public int getTotalFreeBlockSize() {
        return 0;
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
        return fileName;
    }

    public boolean hasRowInfo() {
        return hasRowInfo;
    }

    public boolean isFileModified() {
        return fileModified;
    }

    public boolean isFileOpen() {
        return dataFile != null;
    }

    protected void setFileModified() {

        writeLock.lock();

        try {
            if (!fileModified) {
/*
                // unset saved flag;
                dataFile.seek(FLAGS_POS);

                int flag = BitMap.set(0, FLAG_ISSAVED);

                if (hasRowInfo) {
                    flag = BitMap.set(flag, FLAG_ROWINFO);
                }

                dataFile.writeInt(flag);
*/
                fileModified = true;
            }
        } finally {
            writeLock.unlock();
        }
    }
}
