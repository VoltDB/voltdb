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

import org.hsqldb_voltpatches.Database;
import org.hsqldb_voltpatches.Table;
import org.hsqldb_voltpatches.TableBase;
import org.hsqldb_voltpatches.lib.Iterator;
import org.hsqldb_voltpatches.lib.LongKeyHashMap;

/**
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 2.3.0
 * @since 1.9.0
 */
public class PersistentStoreCollectionDatabase
implements PersistentStoreCollection {

    private Database             database;
    private long                 persistentStoreIdSequence;
    private final LongKeyHashMap rowStoreMap = new LongKeyHashMap();

    public PersistentStoreCollectionDatabase(Database db) {
        this.database = db;
    }

    public void setStore(Object key, PersistentStore store) {

        long persistenceId = ((TableBase) key).getPersistenceId();

        if (store == null) {
            rowStoreMap.remove(persistenceId);
        } else {
            rowStoreMap.put(persistenceId, store);
        }
    }

    synchronized public PersistentStore getStore(Object key) {

        long persistenceId = ((TableBase) key).getPersistenceId();
        PersistentStore store =
            (PersistentStore) rowStoreMap.get(persistenceId);

        if (store == null) {
            store = database.logger.newStore(null, this, (TableBase) key);
            ((TableBase) key).store = store;
        }

        return store;
    }

    public void release() {

        if (rowStoreMap.isEmpty()) {
            return;
        }

        Iterator it = rowStoreMap.values().iterator();

        while (it.hasNext()) {
            PersistentStore store = (PersistentStore) it.next();

            store.release();
        }

        rowStoreMap.clear();
    }

    public void removeStore(Table table) {

        PersistentStore store =
            (PersistentStore) rowStoreMap.get(table.getPersistenceId());

        if (store != null) {
            store.removeAll();
            store.release();
            rowStoreMap.remove(table.getPersistenceId());
        }
    }

    public long getNextId() {
        return persistentStoreIdSequence++;
    }

    public void setNewTableSpaces() {

        DataFileCache dataCache = database.logger.getCache();

        if (dataCache == null) {
            return;
        }

        Iterator it = rowStoreMap.values().iterator();

        while (it.hasNext()) {
            PersistentStore store = (PersistentStore) it.next();

            if (store == null) {
                continue;
            }

            TableBase table = store.getTable();

            if (table.getTableType() == TableBase.CACHED_TABLE) {
                TableSpaceManager tableSpace =
                    dataCache.spaceManager.getTableSpace(table.getSpaceID());

                store.setSpaceManager(tableSpace);
            }
        }
    }
}
