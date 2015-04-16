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
import org.hsqldb_voltpatches.RowAction;
import org.hsqldb_voltpatches.Session;
import org.hsqldb_voltpatches.TableBase;
import org.hsqldb_voltpatches.index.Index;
import org.hsqldb_voltpatches.navigator.RowIterator;
import org.hsqldb_voltpatches.rowio.RowInputInterface;

/**
 * Interface for a store for CachedObject objects.
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 2.3.2
 * @since 1.9.0
 */
public interface PersistentStore {

    int               SHORT_STORE_SIZE = 2;
    int               INT_STORE_SIZE   = 4;
    int               LONG_STORE_SIZE  = 8;
    PersistentStore[] emptyArray       = new PersistentStore[]{};

    TableBase getTable();

    long getTimestamp();

    void setTimestamp(long timestamp);

    boolean isMemory();

    void setMemory(boolean mode);

    int getAccessCount();

    void set(CachedObject object);

    /** get object */
    CachedObject get(long key);

    /** get object with keep, ensuring future gets will return the same instance of the object */
    CachedObject get(long key, boolean keep);

    CachedObject get(CachedObject object, boolean keep);

    /** add new object */
    void add(Session session, CachedObject object, boolean tx);

    boolean canRead(Session session, long key, int mode, int[] colMap);

    boolean canRead(Session session, CachedObject object, int mode,
                    int[] colMap);

    CachedObject get(RowInputInterface in);

    CachedObject get(CachedObject object, RowInputInterface in);

    CachedObject getNewInstance(int size);

    int getDefaultObjectSize();

    CachedObject getNewCachedObject(Session session, Object object,
                                    boolean tx);

    void removeAll();

    /** remove both persisted and cached copies */
    void remove(CachedObject object);

    /** commit persisted image */
    void commitPersistence(CachedObject object);

    //
    void delete(Session session, Row row);

    void indexRow(Session session, Row row);

    void commitRow(Session session, Row row, int changeAction, int txModel);

    void rollbackRow(Session session, Row row, int changeAction, int txModel);

    void postCommitAction(Session session, RowAction rowAction);

    //
    void indexRows(Session session);

    RowIterator rowIterator();

    //
    DataFileCache getCache();

    void setCache(DataFileCache cache);

    TableSpaceManager getSpaceManager();

    void setSpaceManager(TableSpaceManager manager);

    void release();

    PersistentStore getAccessorStore(Index index);

    CachedObject getAccessor(Index key);

    void setAccessor(Index key, CachedObject accessor);

    void setAccessor(Index key, long accessor);

    double searchCost(Session session, Index idx, int count, int opType);

    long elementCount();

    long elementCount(Session session);

    long elementCountUnique(Index index);

    void setElementCount(Index key, long size, long uniqueSize);

    boolean hasNull(int pos);

    void resetAccessorKeys(Session session, Index[] keys);

    Index[] getAccessorKeys();

    void moveDataToSpace(Session session);

    void moveData(Session session, PersistentStore other, int colindex,
                  int adjust);

    void reindex(Session session, Index index);

    void setReadOnly(boolean readonly);

    void writeLock();

    void writeUnlock();
}
