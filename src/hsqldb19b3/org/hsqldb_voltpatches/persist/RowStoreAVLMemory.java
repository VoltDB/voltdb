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

import org.hsqldb_voltpatches.Error;
import org.hsqldb_voltpatches.ErrorCode;
import org.hsqldb_voltpatches.HsqlException;
import org.hsqldb_voltpatches.Row;
import org.hsqldb_voltpatches.RowAVL;
import org.hsqldb_voltpatches.RowAction;
import org.hsqldb_voltpatches.Session;
import org.hsqldb_voltpatches.Table;
import org.hsqldb_voltpatches.index.Index;
import org.hsqldb_voltpatches.index.NodeAVL;
import org.hsqldb_voltpatches.lib.ArrayUtil;
import org.hsqldb_voltpatches.lib.IntKeyHashMapConcurrent;
import org.hsqldb_voltpatches.navigator.RowIterator;
import org.hsqldb_voltpatches.rowio.RowInputInterface;

/*
 * Implementation of PersistentStore for MEMORY tables.
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 1.9.0
 * @since 1.9.0
 */
public class RowStoreAVLMemory extends RowStoreAVL implements PersistentStore {

    Table                           table;
    private IntKeyHashMapConcurrent rowIdMap;
    int                             rowIdSequence = 0;

    public RowStoreAVLMemory(PersistentStoreCollection manager, Table table) {

        this.manager      = manager;
        this.table        = table;
        this.indexList    = table.getIndexList();
        this.accessorList = new CachedObject[indexList.length];
        rowIdMap          = new IntKeyHashMapConcurrent();

        manager.setStore(table, this);
    }

    public boolean isMemory() {
        return true;
    }

    public int getAccessCount() {
        return 0;
    }

    public void set(CachedObject object) {}

    public CachedObject get(int i) {
        return (CachedObject) rowIdMap.get(i);
    }

    public CachedObject getKeep(int i) {
        return (CachedObject) rowIdMap.get(i);
    }

    public CachedObject get(int i, boolean keep) {
        return (CachedObject) rowIdMap.get(i);
    }

    public CachedObject get(CachedObject object, boolean keep) {
        return (CachedObject) rowIdMap.get(object.getPos());
    }

    public int getStorageSize(int i) {
        return 0;
    }

    public void add(CachedObject object) {}

    public CachedObject get(RowInputInterface in) {
        return null;
    }

    public CachedObject getNewCachedObject(Session session,
                                           Object object)
                                           {

        Row row = new RowAVL(table, (Object[]) object);

        if (session != null) {
            RowAction.addAction(session, RowAction.ACTION_INSERT, table, row);
        }

        int id = rowIdSequence++;

        row.setPos(id);
        rowIdMap.put(id, row);

        return row;
    }

    public void removeAll() {
        rowIdMap.clear();
        ArrayUtil.fillArray(accessorList, null);
    }

    public void remove(int i) {
        rowIdMap.remove(i);
    }

    public void removePersistence(int i) {}

    public void release(int i) {}

    public void commitPersistence(CachedObject row) {}

    public DataFileCache getCache() {
        return null;
    }

    public void setCache(DataFileCache cache) {}

    public void release() {
        ArrayUtil.fillArray(accessorList, null);
        rowIdMap.clear();
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

        CachedObject[] oldAccessors = accessorList;
        Index[]        oldIndexList = indexList;
        int            limit        = indexList.length;
        int            diff         = 1;
        int            position     = 0;

        if (keys.length < indexList.length) {
            diff  = -1;
            limit = keys.length;
        }

        for (; position < limit; position++) {
            if (indexList[position] != keys[position]) {
                break;
            }
        }

        accessorList = (CachedObject[]) ArrayUtil.toAdjustedArray(accessorList,
                null, position, diff);
        indexList = keys;

        try {
            if (diff > 0) {
                insertIndexNodes(indexList[0], indexList[position]);
            } else {
                dropIndexFromRows(indexList[0], oldIndexList[position]);
            }
        } catch (HsqlException e) {
            accessorList = oldAccessors;
            indexList    = oldIndexList;

            throw e;
        }
    }

    public CachedObject getNewInstance(int size) {
        return null;
    }

    void dropIndexFromRows(Index primaryIndex,
                           Index oldIndex) {

        RowIterator it       = primaryIndex.firstRow(this);
        int         position = oldIndex.getPosition() - 1;

        while (it.hasNext()) {
            Row     row      = it.getNextRow();
            int     i        = position - 1;
            NodeAVL backnode = ((RowAVL) row).getNode(0);

            while (i-- > 0) {
                backnode = backnode.nNext;
            }

            backnode.nNext = backnode.nNext.nNext;
        }
    }

    boolean insertIndexNodes(Index primaryIndex,
                             Index newIndex) {

        int           position = newIndex.getPosition();
        RowIterator   it       = primaryIndex.firstRow(this);
        int           rowCount = 0;
        HsqlException error    = null;

        try {
            while (it.hasNext()) {
                Row row = it.getNextRow();

                ((RowAVL) row).insertNode(position);

                // count before inserting
                rowCount++;

                newIndex.insert(null, this, row);
            }

            return true;
        } catch (java.lang.OutOfMemoryError e) {
            error = Error.error(ErrorCode.OUT_OF_MEMORY);
        } catch (HsqlException e) {
            error = e;
        }

        // backtrack on error
        // rowCount rows have been modified
        it = primaryIndex.firstRow(this);

        for (int i = 0; i < rowCount; i++) {
            Row     row      = it.getNextRow();
            NodeAVL backnode = ((RowAVL) row).getNode(0);
            int     j        = position;

            while (--j > 0) {
                backnode = backnode.nNext;
            }

            backnode.nNext = backnode.nNext.nNext;
        }

        throw error;
    }

    /**
     * for result tables
     */
    void reindex(Session session, Index index) {

        setAccessor(index, null);

        RowIterator it = table.rowIterator(session);

        while (it.hasNext()) {
            Row row = it.getNextRow();

            // may need to clear the node before insert
            index.insert(session, this, row);
        }
    }
}
