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


package org.hsqldb_voltpatches;

import java.io.InputStream;

import org.hsqldb_voltpatches.HsqlNameManager.HsqlName;
import org.hsqldb_voltpatches.lib.CountdownInputStream;
import org.hsqldb_voltpatches.lib.HashMap;
import org.hsqldb_voltpatches.lib.Iterator;
import org.hsqldb_voltpatches.lib.LongDeque;
import org.hsqldb_voltpatches.lib.LongKeyHashMap;
import org.hsqldb_voltpatches.lib.LongKeyIntValueHashMap;
import org.hsqldb_voltpatches.lib.LongKeyLongValueHashMap;
import org.hsqldb_voltpatches.lib.OrderedHashSet;
import org.hsqldb_voltpatches.lib.ReaderInputStream;
import org.hsqldb_voltpatches.navigator.RowSetNavigator;
import org.hsqldb_voltpatches.navigator.RowSetNavigatorClient;
import org.hsqldb_voltpatches.persist.DataFileCacheSession;
import org.hsqldb_voltpatches.persist.PersistentStore;
import org.hsqldb_voltpatches.persist.PersistentStoreCollectionSession;
import org.hsqldb_voltpatches.result.Result;
import org.hsqldb_voltpatches.result.ResultConstants;
import org.hsqldb_voltpatches.result.ResultLob;
import org.hsqldb_voltpatches.types.BlobData;
import org.hsqldb_voltpatches.types.ClobData;

/*
 * Session semi-persistent data structures
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 1.9.0
 * @since 1.9.0
 */
public class SessionData {

    private final Database           database;
    private final Session            session;
    PersistentStoreCollectionSession persistentStoreCollection;

    // large results
    LongKeyHashMap       resultMap;
    DataFileCacheSession resultCache;

    // VALUE
    Object currentValue;

    // SEQUENCE
    HashMap        sequenceMap;
    OrderedHashSet sequenceUpdateSet;

    public SessionData(Database database, Session session) {

        this.database = database;
        this.session  = session;
        persistentStoreCollection =
            new PersistentStoreCollectionSession(session);
    }

    // transitional feature
    public PersistentStore getRowStore(TableBase table) {

        if (table.store != null) {
            return table.store;
        }

        if (table.isSessionBased) {
            return persistentStoreCollection.getStore(table);
        }

        return database.persistentStoreCollection.getStore(table);
    }

    public PersistentStore getSubqueryRowStore(TableBase table) {

        PersistentStore store = persistentStoreCollection.getStore(table);

        store.removeAll();

        return store;
    }

    public PersistentStore getNewResultRowStore(TableBase table,
            boolean isCached) {

        try {
            PersistentStore store = session.database.logger.newStore(session,
                    persistentStoreCollection, table, isCached);

            return store;
        } catch (HsqlException e) {}

        throw Error.runtimeError(ErrorCode.U_S0500, "SessionData");
    }

    // result
    void setResultSetProperties(Result command, Result result) {

        /**
         * @todo - fredt - this does not work with different prepare calls
         * with the same SQL statement, but different generated column requests
         * To fix, add comment encapsulating the generated column list to SQL
         * to differentiate between the two invocations
         */
        if (command.rsConcurrency == ResultConstants.CONCUR_READ_ONLY) {
            result.setDataResultConcurrency(ResultConstants.CONCUR_READ_ONLY);
            result.setDataResultHoldability(command.rsHoldability);
        } else {
            if (result.rsConcurrency == ResultConstants.CONCUR_READ_ONLY) {
                result.setDataResultHoldability(command.rsHoldability);

                // add warning for concurrency conflict
            } else {
                result.setDataResultHoldability(
                    ResultConstants.CLOSE_CURSORS_AT_COMMIT);
            }
        }
    }

    Result getDataResultHead(Result command, Result result,
                             boolean isNetwork) {

        int fetchSize = command.getFetchSize();

        result.setResultId(session.actionTimestamp);

        if (command.rsConcurrency == ResultConstants.CONCUR_READ_ONLY) {
            result.setDataResultConcurrency(ResultConstants.CONCUR_READ_ONLY);
            result.setDataResultHoldability(command.rsHoldability);
        } else {
            if (result.rsConcurrency == ResultConstants.CONCUR_READ_ONLY) {
                result.setDataResultHoldability(command.rsHoldability);

                // add warning for concurrency conflict
            } else {
                if (session.isAutoCommit()) {
                    result.setDataResultConcurrency(
                        ResultConstants.CONCUR_READ_ONLY);
                    result.setDataResultHoldability(
                        ResultConstants.HOLD_CURSORS_OVER_COMMIT);
                } else {
                    result.setDataResultHoldability(
                        ResultConstants.CLOSE_CURSORS_AT_COMMIT);
                }
            }
        }

        result.setDataResultScrollability(command.rsScrollability);

        boolean hold = false;
        boolean copy = false;

        if (result.rsConcurrency == ResultConstants.CONCUR_UPDATABLE) {
            hold = true;
        }

        if (isNetwork) {
            if (fetchSize != 0
                    && result.getNavigator().getSize() > fetchSize) {
                copy = true;
                hold = true;
            }
        } else {
            if (!result.getNavigator().isMemory()) {
                hold = true;
            }
        }

        if (hold) {
            if (resultMap == null) {
                resultMap = new LongKeyHashMap();
            }

            resultMap.put(result.getResultId(), result);
        }

        if (copy) {
            result = Result.newDataHeadResult(session, result, 0, fetchSize);
        }

        return result;
    }

    Result getDataResultSlice(long id, int offset, int count) {

        RowSetNavigatorClient navigator = getRowSetSlice(id, offset, count);

        return Result.newDataRowsResult(navigator);
    }

    Result getDataResult(long id) {

        Result result = (Result) resultMap.get(id);

        return result;
    }

    RowSetNavigatorClient getRowSetSlice(long id, int offset, int count) {

        Result          result = (Result) resultMap.get(id);
        RowSetNavigator source = result.getNavigator();

        if (offset + count > source.getSize()) {
            count = source.getSize() - offset;
        }

        return new RowSetNavigatorClient(source, offset, count);
    }

    public void closeNavigator(long id) {

        Result result = (Result) resultMap.remove(id);

        result.getNavigator().close();
    }

    public void closeAllNavigators() {

        if (resultMap == null) {
            return;
        }

        Iterator it = resultMap.values().iterator();

        while (it.hasNext()) {
            Result result = (Result) it.next();

            result.getNavigator().close();
        }

        resultMap.clear();
    }

    public void closeAllTransactionNavigators() {

        if (resultMap == null) {
            return;
        }

        Iterator it = resultMap.values().iterator();

        while (it.hasNext()) {
            Result result = (Result) it.next();

            if (result.rsHoldability
                    == ResultConstants.CLOSE_CURSORS_AT_COMMIT) {
                result.getNavigator().close();
                it.remove();
            }
        }

        resultMap.clear();
    }

    public DataFileCacheSession getResultCache() {

        if (resultCache == null) {
            String path = database.getTempDirectoryPath();

            if (path == null) {
                return null;
            }

            try {
                resultCache =
                    new DataFileCacheSession(database,
                                             path + "/session_"
                                             + Long.toString(session.getId()));

                resultCache.open(false);
            } catch (Throwable t) {
                return null;
            }
        }

        return resultCache;
    }

    synchronized void closeResultCache() {

        if (resultCache != null) {
            try {
                resultCache.close(false);
            } catch (HsqlException e) {}

            resultCache = null;
        }
    }

    // lobs in results
    LongKeyLongValueHashMap resultLobs = new LongKeyLongValueHashMap();

    // lobs in transaction
    LongKeyIntValueHashMap lobUsageCount = new LongKeyIntValueHashMap();
    LongDeque              createdLobs   = new LongDeque();
    boolean                hasLobOps;

    // LOBs
    // if rolled back, delete created lobs and ignore all changes
    // if committed,
    // delete created lobs that have no usage count (due to constraint violation or savepoint rollback)
    // update LobManager user counts, delete lobs that have no usage
    public void updateLobUsage(boolean commit) {

        if (!hasLobOps) {
            return;
        }

        hasLobOps = false;

        if (commit) {
            for (int i = 0; i < createdLobs.size(); i++) {
                long lobID = createdLobs.get(i);
                int  delta = lobUsageCount.get(lobID, 0);

                if (delta == 1) {
                    lobUsageCount.remove(lobID);
                    createdLobs.remove(i);

                    i--;
                } else if (!session.isBatch) {
                    database.lobManager.adjustUsageCount(lobID, delta - 1);
                    lobUsageCount.remove(lobID);
                    createdLobs.remove(i);

                    i--;
                }
            }

            if (!lobUsageCount.isEmpty()) {
                Iterator it = lobUsageCount.keySet().iterator();

                while (it.hasNext()) {
                    long lobID = it.nextLong();
                    int  delta = lobUsageCount.get(lobID);

                    database.lobManager.adjustUsageCount(lobID, delta - 1);
                }

                lobUsageCount.clear();
            }

            return;
        } else {
            for (int i = 0; i < createdLobs.size(); i++) {
                long lobID = createdLobs.get(i);

                database.lobManager.deleteLob(lobID);
            }

            createdLobs.clear();
            lobUsageCount.clear();

            return;
        }
    }

    public void updateLobUsageForBatch() {

        for (int i = 0; i < createdLobs.size(); i++) {
            long lobID = createdLobs.get(i);

            database.lobManager.deleteLob(lobID);
        }

        createdLobs.clear();
    }

    public void addToCreatedLobs(long lobID) {

        hasLobOps = true;

        createdLobs.add(lobID);
    }

    public void addLobUsageCount(long lobID) {

        int count = lobUsageCount.get(lobID, 0);

        hasLobOps = true;

        lobUsageCount.put(lobID, count + 1);
    }

    public void removeUsageCount(long lobID) {

        int count = lobUsageCount.get(lobID, 0);

        hasLobOps = true;

        lobUsageCount.put(lobID, count - 1);
    }

    /**
     * allocate storage for a new LOB
     */
    public void allocateLobForResult(ResultLob result,
                                     InputStream inputStream) {

        long                 resultLobId = result.getLobID();
        CountdownInputStream countStream;

        switch (result.getSubType()) {

            case ResultLob.LobResultTypes.REQUEST_CREATE_BYTES : {
                long blobId;
                long blobLength = result.getBlockLength();

                if (inputStream == null) {
                    blobId      = resultLobId;
                    inputStream = result.getInputStream();
                } else {
                    BlobData blob = session.createBlob(blobLength);

                    blobId = blob.getId();

                    resultLobs.put(resultLobId, blobId);
                }

                countStream = new CountdownInputStream(inputStream);

                countStream.setCount(blobLength);
                database.lobManager.setBytesForNewBlob(
                    blobId, countStream, result.getBlockLength());

                break;
            }
            case ResultLob.LobResultTypes.REQUEST_CREATE_CHARS : {
                long clobId;
                long clobLength = result.getBlockLength();

                if (inputStream == null) {
                    clobId = resultLobId;

                    if (result.getReader() != null) {
                        inputStream =
                            new ReaderInputStream(result.getReader());
                    } else {
                        inputStream = result.getInputStream();
                    }
                } else {
                    ClobData clob = session.createClob(clobLength);

                    clobId = clob.getId();

                    resultLobs.put(resultLobId, clobId);
                }

                countStream = new CountdownInputStream(inputStream);

                countStream.setCount(clobLength * 2);
                database.lobManager.setCharsForNewClob(
                    clobId, countStream, result.getBlockLength());

                break;
            }
        }
    }

    public void registerLobForResult(Result result) {

        RowSetNavigator navigator = result.getNavigator();

        while (navigator.next()) {
            Object[] data = navigator.getCurrent();

            for (int i = 0; i < data.length; i++) {
                if (data[i] instanceof BlobData) {
                    BlobData blob = (BlobData) data[i];
                    long     id   = resultLobs.get(blob.getId());

                    data[i] = database.lobManager.getBlob(session, id);
                } else if (data[i] instanceof ClobData) {
                    ClobData clob = (ClobData) data[i];
                    long     id   = resultLobs.get(clob.getId());

                    data[i] = database.lobManager.getClob(session, id);
                }
            }
        }

        resultLobs.clear();
        navigator.reset();
    }

    //
    public void startRowProcessing() {

        if (sequenceMap != null) {
            sequenceMap.clear();
        }
    }

    public Object getSequenceValue(NumberSequence sequence) {

        if (sequenceMap == null) {
            sequenceMap       = new HashMap();
            sequenceUpdateSet = new OrderedHashSet();
        }

        HsqlName key   = sequence.getName();
        Object   value = sequenceMap.get(key);

        if (value == null) {
            value = sequence.getValueObject();

            sequenceMap.put(key, value);
            sequenceUpdateSet.add(sequence);
        }

        return value;
    }
}
