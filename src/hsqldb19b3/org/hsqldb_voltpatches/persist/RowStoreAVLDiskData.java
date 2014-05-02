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

import org.hsqldb_voltpatches.HsqlException;
import org.hsqldb_voltpatches.Row;
import org.hsqldb_voltpatches.RowAVLDiskData;
import org.hsqldb_voltpatches.RowAction;
import org.hsqldb_voltpatches.Session;
import org.hsqldb_voltpatches.Table;
import org.hsqldb_voltpatches.lib.ArrayUtil;
import org.hsqldb_voltpatches.rowio.RowInputInterface;
import org.hsqldb_voltpatches.ErrorCode;
import org.hsqldb_voltpatches.Error;

/*
 * Implementation of PersistentStore for TEXT tables.
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 1.9.0
 * @since 1.9.0
 */
public class RowStoreAVLDiskData extends RowStoreAVLDisk {

    public RowStoreAVLDiskData(PersistentStoreCollection manager,
                               Table table) {
        super(manager, null, table);
    }

    public boolean isMemory() {
        return false;
    }

    public void add(CachedObject object) {

        int size = object.getRealSize(cache.rowOut);

        object.setStorageSize(size);
        cache.add(object);
    }

    public CachedObject get(RowInputInterface in) {

        try {
            return new RowAVLDiskData(table, in);
        } catch (IOException e) {
            throw Error.error(ErrorCode.TEXT_FILE_IO, e);
        }
    }

    public CachedObject getNewCachedObject(Session session, Object object) {

        Row row = new RowAVLDiskData(table, (Object[]) object);

        add(row);
        RowAction.addAction(session, RowAction.ACTION_INSERT, table, row);

        return row;
    }

    public void removeAll() {

        // does not yet clear the storage
    }

    public void remove(int i) {

        if (cache != null) {
            cache.remove(i, this);
        }
    }

    public void removePersistence(int i) {

        if (cache != null) {
            cache.removePersistence(i);
        }
    }

    public void release(int i) {

        if (cache != null) {
            cache.release(i);
        }
    }

    public void commitPersistence(CachedObject row) {

        try {
            if (cache != null) {
                cache.saveRow(row);
            }
        } catch (HsqlException e1) {}
    }

    public void release() {

        ArrayUtil.fillArray(accessorList, null);
        table.database.logger.closeTextCache((Table) table);

        cache = null;
    }
}
