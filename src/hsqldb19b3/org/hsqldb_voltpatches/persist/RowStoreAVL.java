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
import org.hsqldb_voltpatches.Session;
import org.hsqldb_voltpatches.index.Index;
import org.hsqldb_voltpatches.navigator.RowIterator;
import org.hsqldb_voltpatches.rowio.RowInputInterface;

public abstract class RowStoreAVL implements PersistentStore {

    PersistentStoreCollection manager;
    Index[]                   indexList    = Index.emptyArray;
    CachedObject[]            accessorList = CachedObject.emptyArray;

    // for result tables
    long                      timestamp;

    public boolean isMemory() {
        return false;
    }

    public abstract int getAccessCount();

    public abstract void set(CachedObject object);

    public abstract CachedObject get(int key, boolean keep);

    public abstract CachedObject get(CachedObject object, boolean keep);

    public abstract int getStorageSize(int key);

    public abstract void add(CachedObject object);

    public abstract CachedObject get(RowInputInterface in)
    ;

    public abstract CachedObject getNewInstance(int size);

    public abstract CachedObject getNewCachedObject(Session session,
            Object object);

    public abstract void removePersistence(int i);

    public abstract void removeAll();

    public abstract void remove(int i);

    public abstract void release(int i);

    public abstract void commitPersistence(CachedObject object);

    public abstract DataFileCache getCache();

    public abstract void setCache(DataFileCache cache);

    public abstract void release();

    public PersistentStore getAccessorStore(Index index) {
        return null;
    }

    public CachedObject getAccessor(Index key) {

        int position = key.getPosition();

        if (position >= accessorList.length) {
            throw Error.runtimeError(ErrorCode.U_S0500, "RowStoreAV");
        }

        return accessorList[position];
    }

    public abstract void setAccessor(Index key, CachedObject accessor);

    public abstract void setAccessor(Index key, int accessor);

    public abstract void resetAccessorKeys(Index[] keys);

    /**
     * Basic delete with no logging or referential checks.
     */
    public final void delete(Row row) {

        for (int i = indexList.length - 1; i >= 0; i--) {
            indexList[i].delete(this, row);
        }

        remove(row.getPos());
    }

    public final void indexRow(Session session, Row row) {

        int i = 0;

        try {
            for (; i < indexList.length; i++) {
                indexList[i].insert(session, this, row);
            }
        } catch (HsqlException e) {

            // unique index violation - rollback insert
            for (--i; i >= 0; i--) {
                indexList[i].delete(this, row);
            }

            remove(row.getPos());

            throw e;
        }
    }

    public final void indexRows() {

        RowIterator it = rowIterator();

        for (int i = 1; i < indexList.length; i++) {
            setAccessor(indexList[i], null);
        }

        while (it.hasNext()) {
            Row row = it.getNextRow();

            if (row instanceof RowAVL) {
                ((RowAVL) row).clearNonPrimaryNodes();
            }

            for (int i = 1; i < indexList.length; i++) {
                indexList[i].insert(null, this, row);
            }
        }
    }

    public final RowIterator rowIterator() {

        if (indexList.length == 0 || indexList[0] == null) {
            throw Error.runtimeError(ErrorCode.U_S0500, "RowStoreAV");
        }

        return indexList[0].firstRow(this);
    }
}
