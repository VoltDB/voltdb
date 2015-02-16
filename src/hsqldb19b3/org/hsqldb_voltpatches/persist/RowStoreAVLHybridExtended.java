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

import org.hsqldb_voltpatches.Row;
import org.hsqldb_voltpatches.RowAVL;
import org.hsqldb_voltpatches.RowAction;
import org.hsqldb_voltpatches.Session;
import org.hsqldb_voltpatches.Table;
import org.hsqldb_voltpatches.TableBase;
import org.hsqldb_voltpatches.index.Index;
import org.hsqldb_voltpatches.index.NodeAVL;
import org.hsqldb_voltpatches.index.NodeAVLDisk;
import org.hsqldb_voltpatches.navigator.RowIterator;

/*
 * Implementation of PersistentStore for information schema and temp tables.
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 2.3.2
 * @since 2.0.1
 */
public class RowStoreAVLHybridExtended extends RowStoreAVLHybrid {

    Session session;

    public RowStoreAVLHybridExtended(Session session,
                                     PersistentStoreCollection manager,
                                     TableBase table, boolean diskBased) {

        super(session, manager, table, diskBased);

        this.session = session;
    }

    public void set(CachedObject object) {}

    public CachedObject getNewCachedObject(Session session, Object object,
                                           boolean tx) {

        if (indexList != table.getIndexList()) {
            resetAccessorKeys(session, table.getIndexList());
        }

        return super.getNewCachedObject(session, object, tx);
    }

    public void add(Session session, CachedObject object, boolean tx) {

        if (isCached) {
            int size = object.getRealSize(cache.rowOut);

            size += indexList.length * NodeAVLDisk.SIZE_IN_BYTE;
            size = cache.rowOut.getStorageSize(size);

            object.setStorageSize(size);

            long pos = tableSpace.getFilePosition(size, false);

            object.setPos(pos);

            if (tx) {
                RowAction.addInsertAction(session, (Table) table,
                                          (Row) object);
            }

            cache.add(object);
        } else {
            if (tx) {
                RowAction.addInsertAction(session, (Table) table,
                                          (Row) object);
            }
        }

        Object[] data = ((Row) object).getData();

        for (int i = 0; i < nullsList.length; i++) {
            if (data[i] == null) {
                nullsList[i] = true;
            }
        }
    }

    public void indexRow(Session session, Row row) {

        if (indexList != table.getIndexList()) {
            resetAccessorKeys(session, table.getIndexList());
            ((RowAVL) row).setNewNodes(this);
        }

        super.indexRow(session, row);
    }

    public void commitRow(Session session, Row row, int changeAction,
                          int txModel) {

        switch (changeAction) {

            case RowAction.ACTION_DELETE :
                remove(row);
                break;

            case RowAction.ACTION_INSERT :
                break;

            case RowAction.ACTION_INSERT_DELETE :

                // INSERT + DELEETE
                remove(row);
                break;

            case RowAction.ACTION_DELETE_FINAL :
                delete(session, row);
                remove(row);
                break;
        }
    }

    public void rollbackRow(Session session, Row row, int changeAction,
                            int txModel) {

        switch (changeAction) {

            case RowAction.ACTION_DELETE :
                row = (Row) get(row, true);

                ((RowAVL) row).setNewNodes(this);
                row.keepInMemory(false);
                indexRow(session, row);
                break;

            case RowAction.ACTION_INSERT :
                delete(session, row);
                remove(row);
                break;

            case RowAction.ACTION_INSERT_DELETE :

                // INSERT + DELEETE
                remove(row);
                break;
        }
    }

    /**
     * Row might have changed from memory to disk or indexes added
     */
    public void delete(Session session, Row row) {

        NodeAVL node  = ((RowAVL) row).getNode(0);
        int     count = 0;

        while (node != null) {
            count++;

            node = node.nNext;
        }

        if ((isCached && row.isMemory()) || count != indexList.length) {
            row = ((Table) table).getDeleteRowFromLog(session, row.getData());
        }

        if (row != null) {
            super.delete(session, row);
        }
    }

    public CachedObject getAccessor(Index key) {

        int position = key.getPosition();

        if (position >= accessorList.length || indexList[position] != key) {
            resetAccessorKeys(session, table.getIndexList());

            return getAccessor(key);
        }

        return accessorList[position];
    }

    public synchronized void resetAccessorKeys(Session session, Index[] keys) {

        if (indexList.length == 0 || accessorList[0] == null) {
            indexList    = keys;
            accessorList = new CachedObject[indexList.length];

            return;
        }

        if (isCached) {
            resetAccessorKeysForCached();

            return;
        }

        super.resetAccessorKeys(session, keys);
    }

    private void resetAccessorKeysForCached() {

        RowStoreAVLHybrid tempStore = new RowStoreAVLHybridExtended(session,
            manager, table, true);

        tempStore.changeToDiskTable(session);

        RowIterator iterator = table.rowIterator(this);

        while (iterator.hasNext()) {
            Row row = iterator.getNextRow();
            Row newRow = (Row) tempStore.getNewCachedObject(session,
                row.getData(), false);

            tempStore.indexRow(session, newRow);
        }

        indexList    = tempStore.indexList;
        accessorList = tempStore.accessorList;
    }
}
