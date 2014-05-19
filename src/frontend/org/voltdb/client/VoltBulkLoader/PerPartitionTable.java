/* This file is part of VoltDB.
 * Copyright (C) 2008-2014 VoltDB Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.voltdb.client.VoltBulkLoader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.EmptyStackException;
import java.util.List;
import java.util.ListIterator;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;
import org.voltdb.ClientResponseImpl;

import org.voltdb.ParameterConverter;
import org.voltdb.client.HashinatorLite;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.VoltTypeException;
import org.voltdb.client.ClientImpl;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcedureCallback;
import org.voltdb.client.VoltBulkLoader.VoltBulkLoader.LoaderSpecificRowCnt;
import org.voltdb.client.VoltBulkLoader.VoltBulkLoaderRow.BulkLoaderNotification;

/**
 * Partition specific table potentially shared by multiple VoltBulkLoader instances,
 * provided that they are all inserting to the same table.
 */
public class PerPartitionTable {
    // Client we are tied to
    final ClientImpl m_clientImpl;
    //Thread dedicated to each partition and an additional thread for all MP tables
    private final PartitionProcessor m_partitionProcessor;
    //The index in loader tables and the PartitionProcessor number
    final int m_partitionId;
    //Queue for processing pending rows for this table
    LinkedBlockingQueue<VoltBulkLoaderRow> m_partitionRowQueue;
    //Queue of tables that the PartitionProcessor thread is waiting on
    final LinkedBlockingQueue<PerPartitionTable> m_partitionProcessorQueue;

    //Zero based index of the partitioned column in the table
    final int m_partitionedColumnIndex;
    //Partitioned column type
    final VoltType m_partitionColumnType;
    //Table used to build up requests to the PartitionProcessor
    VoltTable table;
    //Column information
    final VoltTable.ColumnInfo m_columnInfo[];
    //Column types
    final VoltType[] m_columnTypes;
    //The number of rows in m_partitionProcessorQueue.
    final AtomicLong m_partitionQueuedRowCnt = new AtomicLong(0);
    //Size of the batches this table submits (minimum of all values provided by VoltBulkLoaders)
    int m_minBatchTriggerSize;
    //Insert procedure name
    final String m_procName;
    //Name of table
    final String m_tableName;
    //List of callbacks for which we have not seen a response from the client.
    LinkedCallbackList m_activeCallbacks = new LinkedCallbackList();
    boolean m_okToFlush = true;

    // Node wrapping each Client callback that is still outstanding.
    static class WrappedCallback {
        PartitionProcedureCallback callback;
        WrappedCallback nextCallback;
        WrappedCallback prevCallback;

        WrappedCallback(WrappedCallback prev, PartitionProcedureCallback callback, WrappedCallback next) {
            this.callback = callback;
            this.nextCallback = next;
            this.prevCallback = prev;
        }
    }

    class LinkedCallbackList {
        transient int callbackCnt = 0;
        transient WrappedCallback head;
        transient WrappedCallback tail;

        WrappedCallback addCallbackToTail(PartitionProcedureCallback callback) {
            final WrappedCallback newCallback = new WrappedCallback(tail, callback, null);
            if (tail == null) {
                head = newCallback;
            }
            else {
                tail.nextCallback = newCallback;
            }
            tail = newCallback;
            callbackCnt++;
            return newCallback;
        }

        void removeCallback(WrappedCallback removed) {
            final WrappedCallback next = removed.nextCallback;
            final WrappedCallback prev = removed.prevCallback;

            if (head == removed) {
                head = next;
            } else {
                prev.nextCallback = next;
                removed.prevCallback = null;
            }

            if (removed == tail) {
                tail = prev;
            } else {
                next.prevCallback = prev;
                removed.nextCallback = null;
            }

            // deallocate wrapper
            removed.callback = null;
            callbackCnt--;
        }

        WrappedCallback getListHead() {
            return head;
        }

        WrappedCallback getListNext(WrappedCallback prevCallback) {
            if (prevCallback == tail)
                return null;
            return prevCallback.nextCallback;
        }
    }

    // Callback for batch submissions to the Client. A failed request submits the entire
    // batch of rows to m_failedQueue for row by row processing on m_failureProcessor.
    class PartitionProcedureCallback implements ProcedureCallback {
        final List<VoltBulkLoaderRow> m_batchRowList;
        final private List<LoaderSpecificRowCnt> m_waitingLoaders;
        WrappedCallback m_thisWrappedCallback;
        List<VoltBulkLoaderRow> m_notificationRows;

        PartitionProcedureCallback(List<VoltBulkLoaderRow> batchRowList, List<LoaderSpecificRowCnt> waitingLoaders,
                ArrayList<VoltBulkLoaderRow> notificationRows) {
            m_batchRowList = batchRowList;
            m_waitingLoaders = waitingLoaders;
            m_notificationRows = notificationRows;
            if (batchRowList.size() > 0)
                m_thisWrappedCallback = m_activeCallbacks.addCallbackToTail(this);
        }

        void addNotifications(ArrayList<VoltBulkLoaderRow> notifications) {
            m_notificationRows.addAll(notifications);
        }

        // Called by Client to inform us of the status of the bulk insert.
        @Override
        public void clientCallback(ClientResponse response) {
            if (response.getStatus() != ClientResponse.SUCCESS) {
                // Bulk Insert failed (update per BulkLoader statistics
                for (LoaderSpecificRowCnt currPair : m_waitingLoaders) {
                    currPair.loader.m_loaderBatchedRowCnt.addAndGet(-1*currPair.rowCnt);
                    currPair.loader.m_failedBatchQueuedRowCnt.addAndGet(currPair.rowCnt);
                    currPair.loader.m_availLoaderPairs.push(currPair);
                }
                // Queue up all rows for individual processing by originating BulkLoader's FailureProcessor.
                for (VoltBulkLoaderRow currRow : m_batchRowList)
                    currRow.m_loader.m_failedQueue.add(currRow);
            }
            else {
                // Update statistics for all BulkLoaders
                for (LoaderSpecificRowCnt currPair : m_waitingLoaders) {
                    currPair.loader.m_loaderBatchedRowCnt.addAndGet(-1*currPair.rowCnt);
                    currPair.loader.m_loaderCompletedCnt.addAndGet(currPair.rowCnt);
                    currPair.loader.m_availLoaderPairs.push(currPair);
                }
            }
            synchronized (PerPartitionTable.this) {
                // If there are any pending notifications, loop through them
                for (VoltBulkLoaderRow currNotifyRow: m_notificationRows) {
                    ((BulkLoaderNotification)currNotifyRow.m_rowHandle).notifyOfClientResponse();
                }
                // Remove this callback from the active callbacks list
                m_activeCallbacks.removeCallback(m_thisWrappedCallback);
            }
        }
    }

    PerPartitionTable(ClientImpl clientImpl, String tableName, PartitionProcessor partitionProcessor,
            VoltBulkLoader firstLoader, int minBatchTriggerSize, int rowQueueSize) {
        m_clientImpl = clientImpl;
        m_partitionProcessor = partitionProcessor;
        m_partitionId = partitionProcessor.m_partitionId;
        m_partitionProcessorQueue = m_partitionProcessor.m_PendingTables;
        m_procName = firstLoader.m_procName;
        m_partitionRowQueue = new LinkedBlockingQueue<VoltBulkLoaderRow>(rowQueueSize*minBatchTriggerSize);
        m_minBatchTriggerSize = minBatchTriggerSize;
        m_columnInfo = firstLoader.m_colInfo;
        m_partitionedColumnIndex = firstLoader.m_partitionedColumnIndex;
        m_columnTypes = firstLoader.m_columnTypes;
        m_partitionColumnType = firstLoader.m_partitionColumnType;
        m_tableName = tableName;

        table = new VoltTable(m_columnInfo);
    }

    boolean updateMinBatchTriggerSize(int minBatchTriggerSize) {
        if (m_minBatchTriggerSize >= minBatchTriggerSize) {
            // This will generate a batch of arbitrary length when the next insert is made
            m_minBatchTriggerSize = minBatchTriggerSize;
            return true;
        }
        else {
            return false;
        }
     }

    // Synchronized to prevent insertRow from manipulating m_partitionProcessorQueue and m_partitionRowQueue
    synchronized void abortFromLoader(VoltBulkLoader loader)
    {
        int abortedRows = 0;
        // First we need to remove ourselves from the PartitionProcessor thread
        while (m_partitionProcessorQueue.remove(this)) ;

        // Now remove all rows from the partition row queue
        List<VoltBulkLoaderRow> allPartitionRows = new ArrayList<VoltBulkLoaderRow>();
        m_partitionRowQueue.drainTo(allPartitionRows);
        // Remove all rows matching the requesting loader from the list
        ListIterator<VoltBulkLoaderRow> it=allPartitionRows.listIterator();
        while (it.hasNext()) {
            VoltBulkLoaderRow currRow = it.next();
            if (currRow.m_loader == loader && !currRow.isNotificationRow()) {
                it.remove();
                abortedRows++;
            }
        }
        // Add back in whatever rows from other loaders are left over
        try {
            for (VoltBulkLoaderRow currRow : allPartitionRows) {
                m_partitionRowQueue.put(currRow);
            }
        }
        catch (InterruptedException e) {
        }
        m_partitionQueuedRowCnt.addAndGet(-1*abortedRows);
        loader.m_loaderQueuedRowCnt.addAndGet(-1*abortedRows);
        long batchCnt = m_partitionQueuedRowCnt.get() / m_minBatchTriggerSize;
        for (long i=0; i<batchCnt; i++) {
            m_partitionProcessorQueue.add(this);
        }
    }

    synchronized void insertRowInTable(VoltBulkLoaderRow nextRow) throws InterruptedException {
        if (!m_partitionRowQueue.offer(nextRow)) {
            m_partitionRowQueue.put(nextRow);
        }
        if (m_partitionQueuedRowCnt.incrementAndGet() % m_minBatchTriggerSize == 0) {
            // A sync row will typically cause the table to be split into 2 requests
            m_okToFlush = false;
            m_partitionProcessorQueue.add(this);
        }
    }

    synchronized void flushAllTableQueues(boolean force) {
        if (m_partitionQueuedRowCnt.get() % m_minBatchTriggerSize != 0 && (m_okToFlush || force)) {
            // A flush will typically cause the table to be split into 2 batches
            m_partitionProcessorQueue.add(this);
        }
        if (!force) {
            m_okToFlush = true;
        }
    }

    synchronized void drainTableQueue(VoltBulkLoaderRow nextRow) throws InterruptedException {
        if (!m_partitionRowQueue.offer(nextRow)) {
            m_partitionRowQueue.put(nextRow);
        }
        m_partitionQueuedRowCnt.incrementAndGet();
        m_partitionProcessorQueue.add(this);
    }

    private PartitionProcedureCallback buildTable() {
        PartitionProcedureCallback nextCallback;
        List<VoltBulkLoaderRow> batchList = new ArrayList<VoltBulkLoaderRow>();
        m_partitionRowQueue.drainTo(batchList, m_minBatchTriggerSize);
        m_partitionQueuedRowCnt.addAndGet(-1*batchList.size());
        ArrayList<LoaderSpecificRowCnt> usedLoaderList = new
                ArrayList<LoaderSpecificRowCnt>();
        ArrayList<VoltBulkLoaderRow> notificationList = new ArrayList<VoltBulkLoaderRow>();
        ListIterator<VoltBulkLoaderRow> it = batchList.listIterator();
        while (it.hasNext()) {
            VoltBulkLoaderRow currRow = it.next();
            VoltBulkLoader loader = currRow.m_loader;
            if (currRow.isNotificationRow()) {
                notificationList.add(currRow);
                it.remove();
                continue;
            }

            Object row_args[];
            row_args = new Object[currRow.m_rowData.length];
            try {
                for (int i = 0; i < row_args.length; i++) {
                    final VoltType type = m_columnTypes[i];
                    row_args[i] = ParameterConverter.tryToMakeCompatible(type.classFromType(),
                            currRow.m_rowData[i]);
                }
            } catch (VoltTypeException e) {
                loader.generateError(currRow.m_rowHandle, currRow.m_rowData, e.getMessage());
                loader.m_loaderQueuedRowCnt.decrementAndGet();
                it.remove();
                continue;
            }
            table.addRow(row_args);

            // Maintain per loader state
            LoaderSpecificRowCnt currLoaderPair = loader.m_currBatchPair[m_partitionId];
            if (currLoaderPair == null) {
                try {
                    currLoaderPair = (LoaderSpecificRowCnt)loader.m_availLoaderPairs.pop();
                } catch (EmptyStackException e) {
                    currLoaderPair = new LoaderSpecificRowCnt(loader, 0);
                    loader.m_outstandingRowCnts[m_partitionId].add(currLoaderPair);
                }
                currLoaderPair.rowCnt = 0;
                usedLoaderList.add(currLoaderPair);
                loader.m_currBatchPair[m_partitionId] = currLoaderPair;
            }
            currLoaderPair.rowCnt++;
        }
        for (LoaderSpecificRowCnt currPair : usedLoaderList) {
            currPair.loader.m_currBatchPair[m_partitionId] = null;
            currPair.loader.m_loaderBatchedRowCnt.addAndGet(currPair.rowCnt);
            currPair.loader.m_loaderQueuedRowCnt.addAndGet(-1*currPair.rowCnt);
        }
        synchronized(PerPartitionTable.this) {
            if (notificationList.size() > 0) {
                // First add the notification(s) to each Callback so we can keep track
                // of doneness on a per Loader basis
                WrappedCallback currWrappedCallback = m_activeCallbacks.getListHead();
                while (currWrappedCallback != null) {
                    currWrappedCallback.callback.m_notificationRows.addAll(notificationList);
                    currWrappedCallback = m_activeCallbacks.getListNext(currWrappedCallback);
                }
            }
            nextCallback = new PartitionProcedureCallback(batchList, usedLoaderList, notificationList);
            if (m_activeCallbacks.callbackCnt > 0) {
                // There is at least one batch in process (including this one) so set the batch count
                for (VoltBulkLoaderRow currRow : notificationList) {
                    ((BulkLoaderNotification)currRow.m_rowHandle).setBatchCount(m_activeCallbacks.callbackCnt);
                }
            }
            else {
                // There are no batches in process (and this batch is empty)
                for (VoltBulkLoaderRow currRow : notificationList)
                    ((BulkLoaderNotification)currRow.m_rowHandle).notifyOfClientResponse();
            }
        }
        return nextCallback;
    }

    void processSpNextTable() {
        PartitionProcedureCallback callback = buildTable();
        if (table.getRowCount() <= 0) {
            assert (callback.m_batchRowList.isEmpty());
            return;
        }

        Object rpartitionParam = HashinatorLite.valueToBytes(table.fetchRow(0).get(
                m_partitionedColumnIndex, m_partitionColumnType));
        try {
            m_clientImpl.callProcedure(callback, m_procName, rpartitionParam, m_tableName, table);
        } catch (IOException e) {
            final ClientResponse r = new ClientResponseImpl(
                    ClientResponse.CONNECTION_LOST, new VoltTable[0],
                    "Connection to database was lost");
            callback.clientCallback(r);
        }
        table.clearRowData();
    }

    void processMpNextTable() {
        PartitionProcedureCallback callback = buildTable();
        if (table.getRowCount() <= 0) {
            assert(callback.m_batchRowList.size() == 0);
            return;
        }

        try {
            m_clientImpl.callProcedure(callback, m_procName, m_tableName, table);
        } catch (IOException e) {
            final ClientResponse r = new ClientResponseImpl(
                    ClientResponse.CONNECTION_LOST, new VoltTable[0],
                    "Connection to database was lost");
            callback.clientCallback(r);
        }
        table.clearRowData();
    }

}
