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

import org.hsqldb_voltpatches.Error;
import org.hsqldb_voltpatches.ErrorCode;
import org.hsqldb_voltpatches.HsqlException;
import org.hsqldb_voltpatches.Row;
import org.hsqldb_voltpatches.RowAVL;
import org.hsqldb_voltpatches.RowAVLDisk;
import org.hsqldb_voltpatches.RowAction;
import org.hsqldb_voltpatches.Session;
import org.hsqldb_voltpatches.TableBase;
import org.hsqldb_voltpatches.index.Index;
import org.hsqldb_voltpatches.lib.ArrayUtil;
import org.hsqldb_voltpatches.lib.IntKeyHashMapConcurrent;
import org.hsqldb_voltpatches.navigator.RowIterator;
import org.hsqldb_voltpatches.rowio.RowInputInterface;
import org.hsqldb_voltpatches.Table;

/*
 * Implementation of PersistentStore for result set and temporary tables.
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 1.9.0
 * @since 1.9.0
 */
public class RowStoreAVLHybrid extends RowStoreAVL implements PersistentStore {

    TableBase                       table;
    final Session                   session;
    DataFileCacheSession            cache;
    private int                     maxMemoryRowCount;
    private int                     memoryRowCount;
    private boolean                 useCache;
    private boolean                 isCached;
    private final boolean           isTempTable;
    private IntKeyHashMapConcurrent rowIdMap;
    int                             rowIdSequence = 0;

    public RowStoreAVLHybrid(Session session,
                             PersistentStoreCollection manager,
                             TableBase table, boolean useCache) {

        this.session           = session;
        this.manager           = manager;
        this.table             = table;
        this.maxMemoryRowCount = session.getResultMemoryRowCount();
        this.rowIdMap          = new IntKeyHashMapConcurrent();
        this.useCache          = useCache;
        this.isTempTable       = table.getTableType() == TableBase.TEMP_TABLE;

        if (table.getTableType() == TableBase.RESULT_TABLE) {
            timestamp = session.getActionTimestamp();
        }

// temp code to force use of cache
/*
        if (useCache) {
            cache = session.sessionData.getResultCache();

            if (cache != null) {
                isCached = useCache;

                cache.storeCount++;
            }
        }
*/

//
        resetAccessorKeys(table.getIndexList());
        manager.setStore(table, this);
    }

    public boolean isMemory() {
        return !isCached;
    }

    public int getAccessCount() {
        return isCached ? cache.getAccessCount()
                        : 0;
    }

    public void set(CachedObject object) {}

    public CachedObject get(int i) {

        try {
            if (isCached) {
                return cache.get(i, this, false);
            } else {
                return (CachedObject) rowIdMap.get(i);
            }
        } catch (HsqlException e) {
            return null;
        }
    }

    public CachedObject getKeep(int i) {

        try {
            if (isCached) {
                return cache.get(i, this, true);
            } else {
                return (CachedObject) rowIdMap.get(i);
            }
        } catch (HsqlException e) {
            return null;
        }
    }

    public CachedObject get(int i, boolean keep) {

        try {
            if (isCached) {
                return cache.get(i, this, keep);
            } else {
                return (CachedObject) rowIdMap.get(i);
            }
        } catch (HsqlException e) {
            return null;
        }
    }

    public CachedObject get(CachedObject object, boolean keep) {

        try {
            if (isCached) {
                return cache.get(object, this, keep);
            } else {
                return object;
            }
        } catch (HsqlException e) {
            return null;
        }
    }

    public int getStorageSize(int i) {

        try {
            if (isCached) {
                return cache.get(i, this, false).getStorageSize();
            } else {
                return 0;
            }
        } catch (HsqlException e) {
            return 0;
        }
    }

    public void add(CachedObject object) {

        if (isCached) {
            int size = object.getRealSize(cache.rowOut);

            size = cache.rowOut.getStorageSize(size);

            object.setStorageSize(size);
            cache.add(object);
        }
    }

    public CachedObject get(RowInputInterface in) {

        try {
            if (isCached) {
                return new RowAVLDisk(table, in);
            }
        } catch (HsqlException e) {
            return null;
        } catch (IOException e1) {
            return null;
        }

        return null;
    }

    public CachedObject getNewCachedObject(Session session, Object object) {

        if (isCached) {
            Row row = new RowAVLDisk(table, (Object[]) object);

            add(row);

            if (isTempTable) {
                RowAction.addAction(session, RowAction.ACTION_INSERT,
                                    (Table) table, row);
            }

            return row;
        } else {
            memoryRowCount++;

            if (useCache && memoryRowCount > maxMemoryRowCount) {
                changeToDiskTable();

                return getNewCachedObject(session, object);
            }

            Row row = new RowAVL(table, (Object[]) object);
            int id  = rowIdSequence++;

            row.setPos(id);
            rowIdMap.put(id, row);

            if (isTempTable) {
                RowAction.addAction(session, RowAction.ACTION_INSERT,
                                    (Table) table, row);
            }

            return row;
        }
    }

    public void removeAll() {

        if (!isCached) {
            rowIdMap.clear();
        }

        ArrayUtil.fillArray(accessorList, null);
    }

    public void remove(int i) {

        if (isCached) {
            cache.remove(i, this);
        } else {
            rowIdMap.remove(i);
        }
    }

    public void removePersistence(int i) {}

    public void release(int i) {

        if (isCached) {
            cache.release(i);
        }
    }

    public void commitPersistence(CachedObject row) {}

    public DataFileCache getCache() {
        return cache;
    }

    public void setCache(DataFileCache cache) {
        throw Error.runtimeError(ErrorCode.U_S0500, "");
    }

    public void release() {

        ArrayUtil.fillArray(accessorList, null);

        if (isCached) {
            cache.storeCount--;

            if (cache.storeCount == 0) {
                cache.clear();
            }

            cache    = null;
            isCached = false;
        } else {
            rowIdMap.clear();
        }

        manager.setStore(table, null);
    }

    public void setAccessor(Index key, CachedObject accessor) {

        Index index = (Index) key;

        accessorList[index.getPosition()] = accessor;
    }

    public void setAccessor(Index key, int accessor) {}

    public void resetAccessorKeys(Index[] keys) {

        if (indexList.length == 0 || indexList[0] == null
                || accessorList[0] == null) {
            indexList    = keys;
            accessorList = new CachedObject[indexList.length];

            return;
        }

        throw Error.runtimeError(ErrorCode.U_S0500, "RowStoreCached");
    }

    public void changeToDiskTable() {

        cache = session.sessionData.getResultCache();

        if (cache != null) {
            RowIterator iterator = table.rowIterator(this);

            ArrayUtil.fillArray(accessorList, null);

            isCached = true;

            cache.storeCount++;

            while (iterator.hasNext()) {
                Row row    = iterator.getNextRow();
                Row newRow = (Row) getNewCachedObject(session, row.getData());

                indexRow(null, newRow);
            }

            rowIdMap.clear();
        }

        maxMemoryRowCount = Integer.MAX_VALUE;
    }

    public CachedObject getNewInstance(int size) {
        return null;
    }
}
