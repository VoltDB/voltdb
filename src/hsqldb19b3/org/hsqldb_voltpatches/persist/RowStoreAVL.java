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

import java.util.concurrent.atomic.AtomicLong;

import org.hsqldb_voltpatches.ColumnSchema;
import org.hsqldb_voltpatches.Database;
import org.hsqldb_voltpatches.HsqlException;
import org.hsqldb_voltpatches.OpTypes;
import org.hsqldb_voltpatches.Row;
import org.hsqldb_voltpatches.RowAVL;
import org.hsqldb_voltpatches.RowAction;
import org.hsqldb_voltpatches.Session;
import org.hsqldb_voltpatches.Table;
import org.hsqldb_voltpatches.TableBase;
import org.hsqldb_voltpatches.TransactionManager;
import org.hsqldb_voltpatches.error.Error;
import org.hsqldb_voltpatches.error.ErrorCode;
import org.hsqldb_voltpatches.index.Index;
import org.hsqldb_voltpatches.index.IndexAVL;
import org.hsqldb_voltpatches.index.NodeAVL;
import org.hsqldb_voltpatches.lib.ArrayUtil;
import org.hsqldb_voltpatches.navigator.RowIterator;
import org.hsqldb_voltpatches.rowio.RowInputInterface;
import org.hsqldb_voltpatches.types.Type;

/*
 * Base implementation of PersistentStore for different table types.
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 2.3.0
 * @since 1.9.0
 */
public abstract class RowStoreAVL implements PersistentStore {

    Database                  database;
    PersistentStoreCollection manager;
    TableSpaceManager         tableSpace;
    Index[]                   indexList    = Index.emptyArray;
    CachedObject[]            accessorList = CachedObject.emptyArray;
    TableBase                 table;
    long                      baseElementCount;
    AtomicLong                elementCount = new AtomicLong();
    long                      storageSize;
    boolean[]                 nullsList;
    double[][]                searchCost;
    boolean                   isSchemaStore;

    // for result tables
    // for INFORMATION SCHEMA tables
    private long timestamp;

    //
    PersistentStore[] subStores = PersistentStore.emptyArray;

    public TableBase getTable() {
        return table;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public abstract boolean isMemory();

    public void setMemory(boolean mode) {}

    public abstract int getAccessCount();

    public abstract void set(CachedObject object);

    public abstract CachedObject get(long key, boolean keep);

    public abstract CachedObject get(CachedObject object, boolean keep);

    public abstract void add(Session session, CachedObject object, boolean tx);

    public boolean canRead(Session session, long pos, int mode, int[] colMap) {
        return true;
    }

    public boolean canRead(Session session, CachedObject object, int mode,
                           int[] colMap) {

        RowAction action = ((Row) object).rowAction;

        if (action == null) {
            return true;
        }

        return action.canRead(session, mode);
    }

    public abstract CachedObject get(RowInputInterface in);

    public CachedObject get(CachedObject object, RowInputInterface in) {
        return object;
    }

    public CachedObject getNewInstance(int size) {
        throw Error.runtimeError(ErrorCode.U_S0500, "RowStoreAVL");
    }

    public int getDefaultObjectSize() {
        throw Error.runtimeError(ErrorCode.U_S0500, "RowStoreAVL");
    }

    public abstract CachedObject getNewCachedObject(Session session,
            Object object, boolean tx);

    public abstract void removeAll();

    public abstract void remove(CachedObject object);

    public abstract void commitPersistence(CachedObject object);

    public void postCommitAction(Session session, RowAction action) {}

    public abstract DataFileCache getCache();

    public TableSpaceManager getSpaceManager() {
        return tableSpace;
    }

    public void setSpaceManager(TableSpaceManager manager) {
        tableSpace = manager;
    }

    public abstract void setCache(DataFileCache cache);

    public abstract void release();

    public PersistentStore getAccessorStore(Index index) {
        return null;
    }

    public CachedObject getAccessor(Index key) {

        int position = key.getPosition();

        if (position >= accessorList.length) {
            throw Error.runtimeError(ErrorCode.U_S0500, "RowStoreAVL");
        }

        return accessorList[position];
    }

    /**
     * Basic delete with no logging or referential checks.
     */
    public void delete(Session session, Row row) {

        row = (Row) get(row, false);

        for (int i = 0; i < indexList.length; i++) {
            indexList[i].delete(session, this, row);
        }

        for (int i = 0; i < subStores.length; i++) {
            subStores[i].delete(session, row);
        }

        row.delete(this);

        long count = elementCount.decrementAndGet();

        if (count > 16 * 1024 && count < baseElementCount / 2) {
            synchronized (this) {
                baseElementCount = count;
                searchCost       = null;
            }
        }
    }

    public void indexRow(Session session, Row row) {

        int i = 0;

        try {
            for (; i < indexList.length; i++) {
                indexList[i].insert(session, this, row);
            }

            int j = 0;

            try {
                for (j = 0; j < subStores.length; j++) {
                    subStores[j].indexRow(session, row);
                }
            } catch (HsqlException e) {

                // unique index violation - rollback insert
                int count = j;

                j = 0;

                for (; j < count; j++) {
                    subStores[j].delete(session, row);
                }

                throw e;
            }

            long count = elementCount.incrementAndGet();

            if (count > 16 * 1024 && count > baseElementCount * 2) {
                synchronized (this) {
                    baseElementCount = count;
                    searchCost       = null;
                }
            }
        } catch (HsqlException e) {
            int count = i;

            i = 0;

            // unique index violation - rollback insert
            for (; i < count; i++) {
                indexList[i].delete(session, this, row);
            }

            remove(row);

            throw e;
        }
    }

    //
    public final void indexRows(Session session) {

        for (int i = 1; i < indexList.length; i++) {
            setAccessor(indexList[i], null);
        }

        RowIterator it = rowIterator();

        while (it.hasNext()) {
            Row row = it.getNextRow();

            ((RowAVL) row).clearNonPrimaryNodes();

            for (int i = 1; i < indexList.length; i++) {
                indexList[i].insert(session, this, row);
            }
        }
    }

    public final RowIterator rowIterator() {

        Index index = indexList[0];

        for (int i = 0; i < indexList.length; i++) {
            if (indexList[i].isClustered()) {
                index = indexList[i];

                break;
            }
        }

        return index.firstRow(this);
    }

    public void setAccessor(Index key, CachedObject accessor) {

        Index index = (Index) key;

        accessorList[index.getPosition()] = accessor;
    }

    public void setAccessor(Index key, long accessor) {}

    public void resetAccessorKeys(Session session, Index[] keys) {

        Index[] oldIndexList = indexList;

        searchCost = null;

        if (indexList.length == 0 || accessorList[0] == null) {
            indexList    = keys;
            accessorList = new CachedObject[indexList.length];

            return;
        }

        // method might be called twice
        if (indexList == keys) {
            return;
        }

        CachedObject[] oldAccessors = accessorList;
        int            limit        = indexList.length;
        int            diff         = keys.length - indexList.length;
        int            position     = 0;

        if (diff < -1) {
            throw Error.runtimeError(ErrorCode.U_S0500, "RowStoreAVL");
        } else if (diff == -1) {
            limit = keys.length;
        } else if (diff == 0) {
            throw Error.runtimeError(ErrorCode.U_S0500, "RowStoreAVL");
        } else if (diff == 1) {

            //
        } else {
            for (; position < limit; position++) {
                if (indexList[position] != keys[position]) {
                    break;
                }
            }

            Index[] tempKeys = (Index[]) ArrayUtil.toAdjustedArray(indexList,
                null, position, 1);

            tempKeys[position] = keys[position];

            resetAccessorKeys(session, tempKeys);
            resetAccessorKeys(session, keys);

            return;
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
                insertIndexNodes(session, indexList[0], indexList[position]);
            } else {
                dropIndexFromRows(indexList[0], oldIndexList[position]);
            }
        } catch (HsqlException e) {
            accessorList = oldAccessors;
            indexList    = oldIndexList;

            throw e;
        }
    }

    public Index[] getAccessorKeys() {
        return indexList;
    }

    public synchronized double searchCost(Session session, Index index,
                                          int count, int opType) {

        if (opType != OpTypes.EQUAL) {
            return elementCount.get() / 2.0;
        }

        if (index.isUnique() && count == index.getColumnCount()) {
            return 1;
        }

        int position = index.getPosition();

        if (searchCost == null || searchCost.length != indexList.length) {
            searchCost = new double[indexList.length][];
        }

        if (searchCost[position] == null) {
            searchCost[position] = indexList[position].searchCost(session,
                    this);
        }

        return searchCost[index.getPosition()][count - 1];
    }

    public long elementCount() {

        Index index = this.indexList[0];

        if (elementCount.get() < 0) {
            elementCount.set(index.getNodeCount(null, this));
        }

        return elementCount.get();
    }

    public long elementCount(Session session) {

        Index index = this.indexList[0];

        if (elementCount.get() < 0) {
            elementCount.set(index.getNodeCount(session, this));
        }

        if (session != null) {
            int txControl = session.database.txManager.getTransactionControl();

            if (txControl != TransactionManager.LOCKS) {
                switch (table.getTableType()) {

                    case TableBase.MEMORY_TABLE :
                    case TableBase.CACHED_TABLE :
                    case TableBase.TEXT_TABLE :
                        return index.getNodeCount(session, this);

                    default :
                }
            }
        }

        return elementCount.get();
    }

    public long elementCountUnique(Index index) {
        return 0;
    }

    public void setElementCount(Index key, long size, long uniqueSize) {
        elementCount.set(size);
    }

    public boolean hasNull(int pos) {
        return false;
    }

    public void moveDataToSpace(Session session) {}

    /**
     * Moves the data from an old store to new after changes to table
     * The colindex argument is the index of the column that was
     * added or removed. The adjust argument is {-1 | 0 | +1}
     */
    public final void moveData(Session session, PersistentStore other,
                               int colindex, int adjust) {

        Type   oldtype  = null;
        Type   newtype  = null;
        Object colvalue = null;

        if (adjust >= 0 && colindex != -1) {
            ColumnSchema column = ((Table) table).getColumn(colindex);

            colvalue = column.getDefaultValue(session);
            newtype  = ((Table) table).getColumnTypes()[colindex];
        }

        if (adjust <= 0 && colindex != -1) {
            oldtype = ((Table) other.getTable()).getColumnTypes()[colindex];
        }

        try {
            Table       table = (Table) this.table;
            RowIterator it    = other.rowIterator();

            while (it.hasNext()) {
                Row      row      = it.getNextRow();
                Object[] olddata  = row.getData();
                Object[] data     = table.getEmptyRowData();
                Object   oldvalue = null;

                if (adjust == 0 && colindex != -1) {
                    oldvalue = olddata[colindex];
                    colvalue = newtype.convertToType(session, oldvalue,
                                                     oldtype);
                }

                ArrayUtil.copyAdjustArray(olddata, data, colvalue, colindex,
                                          adjust);
                table.systemSetIdentityColumn(session, data);

                if (table.hasGeneratedColumn()) {
                    ((Table) table).setGeneratedColumns(session, data);
                }

                table.enforceTypeLimits(session, data);
                table.enforceRowConstraints(session, data);

                // get object without RowAction
                Row newrow = (Row) getNewCachedObject(session, data, false);

                indexRow(session, newrow);
            }

            if (table.isTemp()) {
                return;
            }

            if (oldtype != null && oldtype.isLobType()) {
                it = other.rowIterator();

                while (it.hasNext()) {
                    Row      row      = it.getNextRow();
                    Object[] olddata  = row.getData();
                    Object   oldvalue = olddata[colindex];

                    if (oldvalue != null) {
                        session.sessionData.adjustLobUsageCount(oldvalue, -1);
                    }
                }
            }

            if (newtype != null && newtype.isLobType()) {
                it = rowIterator();

                while (it.hasNext()) {
                    Row      row   = it.getNextRow();
                    Object[] data  = row.getData();
                    Object   value = data[colindex];

                    if (value != null) {
                        session.sessionData.adjustLobUsageCount(value, +1);
                    }
                }
            }
        } catch (java.lang.OutOfMemoryError e) {
            throw Error.error(ErrorCode.OUT_OF_MEMORY);
        }
    }

    public void reindex(Session session, Index index) {

        setAccessor(index, null);

        RowIterator it = table.rowIterator(this);

        while (it.hasNext()) {
            RowAVL row = (RowAVL) it.getNextRow();

            row.getNode(index.getPosition()).delete();
            index.insert(session, this, row);
        }
    }

    public void setReadOnly(boolean readOnly) {}

    public void writeLock() {}

    public void writeUnlock() {}

    void dropIndexFromRows(Index primaryIndex, Index oldIndex) {

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

        it.release();
    }

    boolean insertIndexNodes(Session session, Index primaryIndex,
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

                newIndex.insert(session, this, row);
            }

            it.release();

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

        it.release();

        throw error;
    }

    /**
     * Used with memory indexes
     */
    void destroy() {

        if (indexList.length == 0) {
            return;
        }

        IndexAVL idx  = (IndexAVL) indexList[0];
        NodeAVL  root = (NodeAVL) accessorList[0];

        idx.unlinkNodes(root);
    }
}
