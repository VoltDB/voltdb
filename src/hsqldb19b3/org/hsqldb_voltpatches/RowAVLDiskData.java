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


package org.hsqldb_voltpatches;

import java.io.IOException;

import org.hsqldb_voltpatches.index.NodeAVL;
import org.hsqldb_voltpatches.persist.PersistentStore;
import org.hsqldb_voltpatches.rowio.RowInputInterface;
import org.hsqldb_voltpatches.rowio.RowOutputInterface;

// fredt@users 20021205 - path 1.7.2 - enhancements
// fredt@users 20021215 - doc 1.7.2 - javadoc comments

/**
 * Implementation of rows for tables with memory resident indexes and
 * disk-based data, such as TEXT tables.
 *
 * @author Bob Preston (sqlbob@users dot sourceforge.net)
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 2.2.9
 * @version 1.7.0
 */
public class RowAVLDiskData extends RowAVL {

    PersistentStore store;
    int             accessCount;
    boolean         hasDataChanged;
    int             storageSize;

    /**
     *  Constructor for new rows.
     */
    public RowAVLDiskData(PersistentStore store, TableBase t, Object[] o) {

        super(t, o);

        setNewNodes(store);

        this.store     = store;
        hasDataChanged = true;
    }

    /**
     *  Constructor when read from the disk into the Cache. The link with
     *  the Nodes is made separetly.
     */
    public RowAVLDiskData(PersistentStore store, TableBase t,
                          RowInputInterface in) throws IOException {

        super(t, (Object[]) null);

        setNewNodes(store);

        position       = in.getPos();
        storageSize    = in.getSize();
        rowData        = in.readData(table.getColumnTypes());
        hasDataChanged = false;
        this.store     = store;
    }

    public void getRowData(TableBase t,
                                      RowInputInterface in)
                                      throws IOException {
        rowData = in.readData(t.getColumnTypes());
    }

    public void setData(Object[] data) {
        this.rowData = data;
    }

    public Object[] getData() {

        Object[] data = rowData;

        if (data == null) {
            store.get(this, true);

            data = rowData;

            this.keepInMemory(false);
        } else {
            accessCount++;
        }

        return data;
    }

    /**
     *  Used when data is read from the disk into the Cache the first time.
     *  New Nodes are created which are then indexed.
     */
    public void setNewNodes(PersistentStore store) {

        int index = store.getAccessorKeys().length;

        nPrimaryNode = new NodeAVL(this);

        NodeAVL n = nPrimaryNode;

        for (int i = 1; i < index; i++) {
            n.nNext = new NodeAVL(this);
            n       = n.nNext;
        }
    }

    public NodeAVL insertNode(int index) {

        NodeAVL backnode = getNode(index - 1);
        NodeAVL newnode  = new NodeAVL(this);

        newnode.nNext  = backnode.nNext;
        backnode.nNext = newnode;

        return newnode;
    }

    public int getRealSize(RowOutputInterface out) {
        return out.getSize(this);
    }

    /**
     *  Writes the data to disk. Unlike CachedRow, hasChanged is never set
     *  to true when changes are made to the Nodes. (Nodes are in-memory).
     *  The only time this is used is when a new Row is added to the Caches.
     */
    public void write(RowOutputInterface out) {

        out.writeSize(storageSize);
        out.writeData(this, table.colTypes);
        out.writeEnd();

        hasDataChanged = false;
    }

    public synchronized void setChanged(boolean changed) {
        hasDataChanged = changed;
    }

    public boolean isNew() {
        return false;
    }

    public boolean hasChanged() {
        return hasDataChanged;
    }

    public void updateAccessCount(int count) {
        accessCount = count;
    }

    public int getAccessCount() {
        return accessCount;
    }

    public int getStorageSize() {
        return storageSize;
    }

    public void setStorageSize(int size) {
        storageSize = size;
    }

    /**
     * Sets the file position for the row and registers the row with
     * the table.
     *
     * @param pos position in data file
     */
    public void setPos(long pos) {
        position = pos;
    }

    public boolean isMemory() {
        return true;
    }

    /** used by Index, nodes are always in memory */
    public boolean isInMemory() {
        return rowData != null;
    }

    public boolean isKeepInMemory() {
        return false;
    }

    public boolean keepInMemory(boolean keep) {
        return true;
    }

    /** required to purge cache */
    public void setInMemory(boolean in) {

        if (!in) {
            rowData = null;
        }
    }
}
