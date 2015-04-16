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

import org.hsqldb_voltpatches.error.Error;
import org.hsqldb_voltpatches.error.ErrorCode;
import org.hsqldb_voltpatches.lib.ArraySort;
import org.hsqldb_voltpatches.lib.Iterator;
import org.hsqldb_voltpatches.lib.ObjectComparator;
import org.hsqldb_voltpatches.lib.StopWatch;
import org.hsqldb_voltpatches.map.BaseHashMap;

/**
 * New implementation of row caching for CACHED tables.<p>
 *
 * Manages memory for the cache map and its contents based on least recently
 * used clearup.<p>
 * Also provides services for selecting rows to be saved and passing them
 * to DataFileCache.<p>
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 2.3.0
 * @since 1.8.0
 */
public class Cache extends BaseHashMap {

    final DataFileCache                        dataFileCache;
    private int                                capacity;         // number of Rows
    private long                               bytesCapacity;    // number of bytes
    private final CachedObjectComparator       rowComparator;
    private final BaseHashMap.BaseHashIterator objectIterator;
    private boolean                            updateAccess;
    private long                               maxPositionOnCleanup;

//
    private CachedObject[] rowTable;
    long                   cacheBytesLength;

    // for testing
    StopWatch saveAllTimer = new StopWatch(false);
    StopWatch sortTimer    = new StopWatch(false);
    int       saveRowCount = 0;

    Cache(DataFileCache dfc) {

        super(dfc.capacity(), BaseHashMap.objectKeyOrValue,
              BaseHashMap.noKeyOrValue, true);

        maxCapacity      = dfc.capacity();
        dataFileCache    = dfc;
        capacity         = dfc.capacity();
        bytesCapacity    = dfc.bytesCapacity();
        rowComparator    = new CachedObjectComparator();
        rowTable         = new CachedObject[capacity];
        cacheBytesLength = 0;
        objectIterator   = new BaseHashIterator(true);
        updateAccess     = true;
        comparator       = rowComparator;
    }

    /**
     *  Structural initialisations take place here. This allows the Cache to
     *  be resized while the database is in operation.
     */
    void resize(int capacity, long bytesCapacity) {}

    long getTotalCachedBlockSize() {
        return cacheBytesLength;
    }

    /**
     * Returns a row if in memory cache.
     */
    public CachedObject get(long pos) {

        if (accessCount > ACCESS_MAX && updateAccess) {
            updateAccessCounts();
            resetAccessCount();
            updateObjectAccessCounts();
        }

        int lookup = getObjectLookup(pos);

        if (lookup == -1) {
            return null;
        }

        accessTable[lookup] = ++accessCount;

        CachedObject object = (CachedObject) objectKeyTable[lookup];

        return object;
    }

    /**
     * Adds a row to the cache.
     */
    void put(CachedObject row) {

        int storageSize = row.getStorageSize();

        if (size() >= capacity
                || storageSize + cacheBytesLength > bytesCapacity) {
            cleanUp(false);

            if (size() >= capacity) {
                clearUnchanged();
            }

            if (size() >= capacity) {
                cleanUp(true);
            }

            if (size() >= capacity) {
                throw Error.error(ErrorCode.DATA_CACHE_IS_FULL,
                                  String.valueOf(capacity));
            }
        }

        if (accessCount > ACCESS_MAX && updateAccess) {
            updateAccessCounts();
            resetAccessCount();
            updateObjectAccessCounts();
        }

        super.addOrRemoveObject(row, row.getPos(), false);
        row.setInMemory(true);

        cacheBytesLength += storageSize;
    }

    /**
     * Removes an object from memory cache. Does not release the file storage.
     */
    CachedObject release(long pos) {

        CachedObject r = (CachedObject) super.addOrRemoveObject(null, pos,
            true);

        if (r == null) {
            return null;
        }

        cacheBytesLength -= r.getStorageSize();

        r.setInMemory(false);

        return r;
    }

    /**
     * Replace a row in the cache.
     */
    void replace(long key, CachedObject row) {

        int lookup = super.getLookup(key);

        objectKeyTable[lookup] = row;
    }

    private void updateAccessCounts() {

        CachedObject r;
        int          count;

        for (int i = 0; i < objectKeyTable.length; i++) {
            r = (CachedObject) objectKeyTable[i];

            if (r != null) {
                count = r.getAccessCount();

                if (count > accessTable[i]) {
                    accessTable[i] = count;
                }
            }
        }
    }

    private void updateObjectAccessCounts() {

        CachedObject r;
        int          count;

        for (int i = 0; i < objectKeyTable.length; i++) {
            r = (CachedObject) objectKeyTable[i];

            if (r != null) {
                count = accessTable[i];

                r.updateAccessCount(count);
            }
        }
    }

    /**
     * Reduces the number of rows held in this Cache object. <p>
     *
     * Cleanup is done by checking the accessCount of the Rows and removing
     * the rows with the lowest access count.
     *
     * Index operations require that up to 5 recently accessed rows remain
     * in the cache. This is ensured by prior calling keepInMemory().
     *
     */
    private void cleanUp(boolean all) {

        if (updateAccess) {
            updateAccessCounts();
        }

        int removeCount  = size() / 2;
        int accessTarget = getAccessCountCeiling(removeCount, removeCount / 8);
        int savecount    = 0;

        if (all) {
            accessTarget = accessCount + 1;
        }

        objectIterator.reset();

        for (; objectIterator.hasNext(); ) {
            CachedObject row = (CachedObject) objectIterator.next();
            int          currentAccessCount = objectIterator.getAccessCount();
            boolean      oldRow = currentAccessCount < accessTarget;
            boolean newRow = row.isNew()
                             && row.getStorageSize()
                                >= DataFileCache.initIOBufferSize;

            if (!oldRow && !newRow) {
                continue;
            }

            objectIterator.setAccessCount(accessTarget);

            synchronized (row) {
                if (row.isKeepInMemory()) {
                    continue;
                }

                if (row.hasChanged()) {
                    rowTable[savecount++] = row;
                }

                if (oldRow) {
                    row.setInMemory(false);
                    objectIterator.remove();

                    cacheBytesLength -= row.getStorageSize();
                }
            }

            if (savecount == rowTable.length) {
                saveRows(savecount);

                savecount = 0;
            }
        }

        super.setAccessCountFloor(accessTarget);
        saveRows(savecount);

        this.maxPositionOnCleanup = dataFileCache.fileFreePosition
                                    / dataFileCache.dataFileScale;
    }

    void clearUnchanged() {

        objectIterator.reset();

        for (; objectIterator.hasNext(); ) {
            CachedObject row = (CachedObject) objectIterator.next();

            synchronized (row) {
                if (!row.isKeepInMemory() && !row.hasChanged()) {
                    row.setInMemory(false);
                    objectIterator.remove();

                    cacheBytesLength -= row.getStorageSize();
                }
            }
        }
    }

    private synchronized void saveRows(int count) {

        if (count == 0) {
            return;
        }

        long startTime = saveAllTimer.elapsedTime();

        rowComparator.setType(CachedObjectComparator.COMPARE_POSITION);
        sortTimer.zero();
        sortTimer.start();
        ArraySort.sort(rowTable, 0, count, rowComparator);
        sortTimer.stop();
        saveAllTimer.start();
        dataFileCache.saveRows(rowTable, 0, count);

        saveRowCount += count;

        saveAllTimer.stop();
        logSaveRowsEvent(count, startTime);
    }

    /**
     * Writes out all modified cached Rows.
     */
    void saveAll() {

        int savecount = 0;

        objectIterator.reset();

        for (; objectIterator.hasNext(); ) {
            if (savecount == rowTable.length) {
                saveRows(savecount);

                savecount = 0;
            }

            CachedObject r = (CachedObject) objectIterator.next();

            if (r.hasChanged()) {
                rowTable[savecount] = r;

                savecount++;
            }
        }

        saveRows(savecount);
    }

    void logSaveRowsEvent(int saveCount, long startTime) {

        StringBuffer sb = new StringBuffer();

        sb.append("cache save rows [count,time] totals ");
        sb.append(saveRowCount);
        sb.append(',').append(saveAllTimer.elapsedTime()).append(' ');
        sb.append("operation ").append(saveCount).append(',');
        sb.append(saveAllTimer.elapsedTime() - startTime).append(' ');

//
        sb.append("txts ");
        sb.append(dataFileCache.database.txManager.getGlobalChangeTimestamp());

//
        dataFileCache.logDetailEvent(sb.toString());
    }

    /**
     * clears out the memory cache
     */
    public void clear() {

        super.clear();

        cacheBytesLength = 0;
    }

    public Iterator getIterator() {

        objectIterator.reset();

        return objectIterator;
    }

    static final class CachedObjectComparator implements ObjectComparator {

        static final int COMPARE_LAST_ACCESS = 0;
        static final int COMPARE_POSITION    = 1;
        static final int COMPARE_SIZE        = 2;
        private int      compareType         = COMPARE_POSITION;

        CachedObjectComparator() {}

        void setType(int type) {
            compareType = type;
        }

        public int compare(Object a, Object b) {

            long diff;

            switch (compareType) {

                case COMPARE_POSITION :
                    diff = ((CachedObject) a).getPos()
                           - ((CachedObject) b).getPos();
                    break;

                case COMPARE_SIZE :
                    diff = ((CachedObject) a).getStorageSize()
                           - ((CachedObject) b).getStorageSize();
                    break;

                default :
                    return 0;
            }

            return diff == 0 ? 0
                             : diff > 0 ? 1
                                        : -1;
        }

        public int hashCode(Object o) {
            return o.hashCode();
        }

        public long longKey(Object o) {
            return ((CachedObject) o).getPos();
        }
    }
}
