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
import org.hsqldb_voltpatches.Table;
import org.hsqldb_voltpatches.index.Index;
import org.hsqldb_voltpatches.index.NodeAVL;
import org.hsqldb_voltpatches.lib.ArrayUtil;
import org.hsqldb_voltpatches.lib.IntKeyHashMapConcurrent;
import org.hsqldb_voltpatches.rowio.RowInputInterface;

/*
 * Implementation of PersistentStore for CACHED tables.
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 1.9.0
 * @since 1.9.0
 */
public class RowStoreAVLDisk extends RowStoreAVL {

    DataFileCache           cache;
    Table                   table;
    IntKeyHashMapConcurrent rowActionMap;

    public RowStoreAVLDisk(PersistentStoreCollection manager,
                           DataFileCache cache, Table table) {

        this.manager      = manager;
        this.table        = table;
        this.indexList    = table.getIndexList();
        this.accessorList = new CachedObject[indexList.length];
        this.cache        = cache;

        manager.setStore(table, this);

        rowActionMap = table.database.txManager.rowActionMap;
    }

    public boolean isMemory() {
        return false;
    }

    public int getAccessCount() {
        return cache.getAccessCount();
    }

    public void set(CachedObject object) {

        Row row = ((Row) object);

        row.rowAction = (RowAction) rowActionMap.get(row.getPos());
    }

    public CachedObject get(int key) {

        CachedObject object = cache.get(key, this, false);

        return object;
    }

    public CachedObject getKeep(int key) {

        CachedObject object = cache.get(key, this, true);

        return object;
    }

    public CachedObject get(int key, boolean keep) {

        CachedObject object = cache.get(key, this, keep);

        return object;
    }

    public CachedObject get(CachedObject object, boolean keep) {

        object = cache.get(object, this, keep);

        return object;
    }

    public int getStorageSize(int i) {
        return cache.get(i, this, false).getStorageSize();
    }

    public void add(CachedObject object) {

        int size = object.getRealSize(cache.rowOut);

        size = cache.rowOut.getStorageSize(size);

        object.setStorageSize(size);
        cache.add(object);
    }

    public CachedObject get(RowInputInterface in) {

        try {
            return new RowAVLDisk(table, in);
        } catch (IOException e) {
            throw Error.error(ErrorCode.DATA_FILE_ERROR, e);
        }
    }

    public CachedObject getNewCachedObject(Session session, Object object) {

        Row row = new RowAVLDisk(table, (Object[]) object);

        add(row);

        if (session != null) {
            RowAction.addAction(session, RowAction.ACTION_INSERT, table, row);
        }

        return row;
    }

    public void removeAll() {
        ArrayUtil.fillArray(accessorList, null);
    }

    public void remove(int i) {
        cache.remove(i, this);
    }

    public void removePersistence(int i) {}

    public void release(int i) {
        cache.release(i);
    }

    public void commitPersistence(CachedObject row) {}

    public DataFileCache getCache() {
        return cache;
    }

    public void setCache(DataFileCache cache) {
        this.cache = cache;
    }

    public void release() {

        ArrayUtil.fillArray(accessorList, null);

        cache = null;
    }

    public void setAccessor(Index key, CachedObject accessor) {

        Index index = (Index) key;

        accessorList[index.getPosition()] = accessor;
    }

    public CachedObject getAccessor(Index key) {

        NodeAVL node = (NodeAVL) accessorList[key.getPosition()];

        if (node == null) {
            return null;
        }

        if (!node.isInMemory()) {
            RowAVL row = (RowAVL) get(node.getPos(), false);

            node                            = row.getNode(key.getPosition());
            accessorList[key.getPosition()] = node;
        }

        return node;
    }

    public void setAccessor(Index key, int accessor) {

        CachedObject object = get(accessor, false);

        if (object != null) {
            NodeAVL node = ((RowAVL) object).getNode(key.getPosition());

            object = node;
        }

        setAccessor(key, object);
    }

    public void resetAccessorKeys(Index[] keys) {

        if (indexList.length == 0 || indexList[0] == null
                || accessorList[0] == null) {
            indexList    = keys;
            accessorList = new CachedObject[indexList.length];

            return;
        }

        throw Error.runtimeError(ErrorCode.U_S0500, "RowStoreCached");
    }

    public CachedObject getNewInstance(int size) {
        return null;
    }
}
