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
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

import org.voltcore.logging.VoltLogger;
import org.voltdb.ParameterConverter;
import org.voltdb.TheHashinator;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.VoltTypeException;
import org.voltdb.client.ClientImpl;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcedureCallback;
import org.voltdb.client.VoltBulkLoader.VoltBulkLoader.LoaderPair;
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
    VoltTable.ColumnInfo m_columnInfo[];
    //Column types
    VoltType[] m_columnTypes;
    //Counter that goes down when acknowledged so that drain is not needed.
    final AtomicLong m_partitionOutstandingBatchCnt = new AtomicLong(0);
    //The number of rows in m_partitionProcessorQueue.
    final AtomicLong m_partitionQueuedRowCnt = new AtomicLong(0);
    //Size of the batches this table submits (minimum of all values provided by VoltBulkLoaders)
    int m_minBatchTriggerSize;
    //Shared log.
    private static VoltLogger m_log;
    //Insert procedure name
    final String m_procName;
    //Name of table
    final String m_tableName;
    //List of callbacks for which we have not seen a response from the client.
    rmLinkedList<PartitionProcedureCallback> m_activeCallbacks = new rmLinkedList<PartitionProcedureCallback>();

    static public void initializeLogger(VoltLogger logger) {
        m_log = logger;
    }

    // Node wrapping each Client callback that is still outstanding.
    static class Node<E> {
        E item;
        Node<E> next;
        Node<E> prev;

        Node(Node<E> prev, E element, Node<E> next) {
            this.item = element;
            this.next = next;
            this.prev = prev;
        }
    }

    // Taken from LinkedList<>
    class rmLinkedList<E> {
        transient int size = 0;

        /**
         * Pointer to first node.
         * Invariant: (first == null && last == null) ||
         *            (first.prev == null && first.item != null)
         */
        transient Node<E> first;

        /**
         * Pointer to last node.
         * Invariant: (first == null && last == null) ||
         *            (last.next == null && last.item != null)
         */
        transient Node<E> last;

        Node<E> addLast(E e) {
            final Node<E> l = last;
            final Node<E> newNode = new Node<>(l, e, null);
            last = newNode;
            if (l == null)
                first = newNode;
            else
                l.next = newNode;
            size++;
            return newNode;
        }

        /**
         * Unlinks non-null node x.
         */
        void unlink(Node<E> x) {
            // assert x != null;
            final Node<E> next = x.next;
            final Node<E> prev = x.prev;

            if (prev == null) {
                first = next;
            } else {
                prev.next = next;
                x.prev = null;
            }

            if (next == null) {
                last = prev;
            } else {
                next.prev = prev;
                x.next = null;
            }

            x.item = null;
            size--;
        }

        Node<E> getFirst() {
            return first;
        }

        Node<E> getNext(Node<E> x) {
            if (x == last)
                return null;
            return x.next;
        }
    }

    // Callback for batch submissions to the Client. A failed request submits the entire
    // batch of rows to m_failedQueue for row by row processing on m_failureProcessor.
    class PartitionProcedureCallback implements ProcedureCallback {
        final List<VoltBulkLoaderRow> m_batchRowList;
        final private List<LoaderPair> m_waitingLoaders;
        Node<PartitionProcedureCallback> thisCallbackNode;
        List<VoltBulkLoaderRow> m_notificationRows;

        PartitionProcedureCallback(List<VoltBulkLoaderRow> batchRowList, List<LoaderPair> waitingLoaders,
                ArrayList<VoltBulkLoaderRow> notificationRows) {
            m_batchRowList = batchRowList;
            m_waitingLoaders = waitingLoaders;
            m_notificationRows = notificationRows;
            if (batchRowList.size() > 0)
                thisCallbackNode = m_activeCallbacks.addLast(this);
        }

        void addNotifications(ArrayList<VoltBulkLoaderRow> notifications) {
            m_notificationRows.addAll(notifications);
        }

        public void clientCallback(ClientResponse response) throws Exception {
            m_partitionOutstandingBatchCnt.decrementAndGet();
            if (response.getStatus() != ClientResponse.SUCCESS) {
                // Batch failed queue it for individual processing and find out which actually m_errored.
                m_log.info("Unable to insert rows in a batch.  Attempting to insert them one-by-one.");
                m_log.info("Note: this will result in reduced insertion performance.");
                m_log.debug("Batch Failed Will be processed by Failure Processor: " + response.getStatusString());
                // Update statistics for all loaders
                for (LoaderPair currPair : m_waitingLoaders) {
                    currPair.loader.m_loaderBatchedRowCnt.addAndGet(-1*currPair.rowCnt);
                    currPair.loader.m_failedBatchQueuedRowCnt.addAndGet(currPair.rowCnt);
                    currPair.loader.m_availLoaderPairs.push(currPair);
                }
                for (VoltBulkLoaderRow currRow : m_batchRowList) {
                    currRow.loader.m_failedQueue.add(currRow);
                }
            }
            else {
                // Update statistics for all loaders
                for (LoaderPair currPair : m_waitingLoaders) {
                    currPair.loader.m_loaderBatchedRowCnt.addAndGet(-1*currPair.rowCnt);
                    currPair.loader.m_loaderCompletedCnt.addAndGet(currPair.rowCnt);
                    currPair.loader.m_availLoaderPairs.push(currPair);
                }
            }
            synchronized (PerPartitionTable.this) {
                for (VoltBulkLoaderRow currNotifyRow: m_notificationRows) {
                    ((BulkLoaderNotification)currNotifyRow.rowHandle).notifyOfClientResponse();
                }
                m_activeCallbacks.unlink(thisCallbackNode);
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
        else
            return false;
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
        for (Iterator<VoltBulkLoaderRow> it=allPartitionRows.iterator(); it.hasNext(); ) {
            VoltBulkLoaderRow currRow = it.next();
            if (currRow.loader == loader && !currRow.isNotificationRow()) {
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
        for (long i=0; i<batchCnt; i++)
            m_partitionProcessorQueue.add(this);
    }

    synchronized void insertRowInTable(VoltBulkLoaderRow nextRow) {
        try {
            if (!m_partitionRowQueue.offer(nextRow)) {
                m_log.debug("Failed to insert row in table queue, waiting and doing put.");
                m_partitionRowQueue.put(nextRow);
            }
        } catch (InterruptedException e) {
            if (nextRow.rowHandle == null)
                m_log.error("Failed to insert row (interrupted)");
            else
                m_log.error("Failed to insert row (interrupted): " + nextRow.rowHandle.toString());
            return;
        }
        if (m_partitionQueuedRowCnt.incrementAndGet() % m_minBatchTriggerSize == 0)
            // A sync row will typically cause the table to be split into 2 requests
            m_partitionProcessorQueue.add(this);
    }

    synchronized void flushAllTableQueues() {
        if (m_partitionQueuedRowCnt.get() % m_minBatchTriggerSize != 0)
            // A flush will typically cause the table to be split into 2 batches
            m_partitionProcessorQueue.add(this);
    }

    synchronized void drainTableQueue(VoltBulkLoaderRow nextRow) {
        try {
            if (!m_partitionRowQueue.offer(nextRow)) {
                m_log.debug("Failed to insert row in table queue, waiting and doing put.");
                m_partitionRowQueue.put(nextRow);
            }
        } catch (InterruptedException e) {
            if (nextRow.rowHandle == null)
                m_log.error("Failed to insert row (interrupted)");
            else
                m_log.error("Failed to insert row (interrupted): " + nextRow.rowHandle.toString());
            return;
        }
        m_partitionQueuedRowCnt.incrementAndGet();
        m_partitionProcessorQueue.add(this);
    }

    private PartitionProcedureCallback buildTable() {
        PartitionProcedureCallback nextCallback;
        List<VoltBulkLoaderRow> batchList = new ArrayList<VoltBulkLoaderRow>();
        m_partitionRowQueue.drainTo(batchList, m_minBatchTriggerSize);
        m_partitionQueuedRowCnt.addAndGet(-1*batchList.size());
        ArrayList<LoaderPair> usedLoaderList = new ArrayList<LoaderPair>();
        ArrayList<VoltBulkLoaderRow> notificationList = new ArrayList<VoltBulkLoaderRow>();
        for (Iterator<VoltBulkLoaderRow> it = batchList.iterator(); it.hasNext();) {
            VoltBulkLoaderRow currRow = it.next();
            VoltBulkLoader loader = currRow.loader;
            if (currRow.isNotificationRow()) {
                notificationList.add(currRow);
                it.remove();
                continue;
            }

            Object row_args[];
            row_args = new Object[currRow.objectList.length];
            try {
                for (int i = 0; i < row_args.length; i++) {
                    final VoltType type = m_columnTypes[i];
                    row_args[i] = ParameterConverter.tryToMakeCompatible(type.classFromType(), currRow.objectList[i]);
                }
            } catch (VoltTypeException e) {
                loader.generateError(currRow.rowHandle, currRow.objectList, e.getMessage());
                loader.m_loaderQueuedRowCnt.decrementAndGet();
                it.remove();
                continue;
            }
            table.addRow(row_args);

            // Maintain per loader state
            LoaderPair currLoaderPair = loader.m_currBatchPair[m_partitionId];
            if (currLoaderPair == null) {
                try {
                    currLoaderPair = (LoaderPair) loader.m_availLoaderPairs.pop();
                } catch (EmptyStackException e) {
                    currLoaderPair = new LoaderPair(loader, 0);
                    loader.m_outstandingRowCnts[m_partitionId].add(currLoaderPair);
                }
                currLoaderPair.rowCnt = 0;
                usedLoaderList.add(currLoaderPair);
                loader.m_currBatchPair[m_partitionId] = currLoaderPair;
            }
            currLoaderPair.rowCnt++;
        }
        for (LoaderPair currPair : usedLoaderList) {
            currPair.loader.m_currBatchPair[m_partitionId] = null;
            currPair.loader.m_loaderBatchedRowCnt.addAndGet(currPair.rowCnt);
            currPair.loader.m_loaderQueuedRowCnt.addAndGet(-1*currPair.rowCnt);
        }
        synchronized(PerPartitionTable.this) {
            if (notificationList.size() > 0) {
                // First add the notification(s) to each Callback so we can keep track of doneness on a per Loader basis
                Node<PartitionProcedureCallback> currCallbackNode = m_activeCallbacks.getFirst();
                while (currCallbackNode != null) {
                    currCallbackNode.item.m_notificationRows.addAll(notificationList);
                    currCallbackNode = m_activeCallbacks.getNext(currCallbackNode);
                }
            }
            nextCallback = new PartitionProcedureCallback(batchList, usedLoaderList, notificationList);
            if (m_activeCallbacks.size > 0) {
                // There is at least one batch in process (including this one) so set the batch count
                for (VoltBulkLoaderRow currRow : notificationList)
                    ((BulkLoaderNotification)currRow.rowHandle).setBatchCount(m_activeCallbacks.size);
            }
            else {
                // There are no batches in process (and this batch is empty)
                for (VoltBulkLoaderRow currRow : notificationList)
                    ((BulkLoaderNotification)currRow.rowHandle).notifyOfClientResponse();
            }
        }
        return nextCallback;
    }

    void processSpNextTable() {
        PartitionProcedureCallback callback = buildTable();
        if (table.getRowCount() <= 0) {
            assert(callback.m_batchRowList.size() == 0);
            return;
        }

        Object rpartitionParam =
                TheHashinator.valueToBytes(table.fetchRow(0).get(
                m_partitionedColumnIndex, m_partitionColumnType));
        try {
            m_clientImpl.callProcedure(callback, m_procName, rpartitionParam, m_tableName, table);
            m_partitionOutstandingBatchCnt.incrementAndGet();
        } catch (IOException e) {
            m_partitionOutstandingBatchCnt.decrementAndGet();
        }
        m_partitionOutstandingBatchCnt.incrementAndGet();
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
        }
        m_partitionOutstandingBatchCnt.incrementAndGet();
        table.clearRowData();
    }

}
