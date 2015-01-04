/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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
import java.util.List;
import java.util.ListIterator;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.voltcore.logging.VoltLogger;
import org.voltcore.utils.CoreUtils;
import org.voltdb.ClientResponseImpl;

import org.voltdb.ParameterConverter;
import org.voltdb.client.HashinatorLite;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.VoltTypeException;
import org.voltdb.client.ClientImpl;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcedureCallback;

/**
 * Partition specific table potentially shared by multiple VoltBulkLoader instances,
 * provided that they are all inserting to the same table.
 */
public class PerPartitionTable {
    private static final VoltLogger loaderLog = new VoltLogger("LOADER");

    // Client we are tied to
    final ClientImpl m_clientImpl;
    //The index in loader tables and the PartitionProcessor number
    final int m_partitionId;
    final boolean m_isMP;
    //Queue for processing pending rows for this table
    LinkedBlockingQueue<VoltBulkLoaderRow> m_partitionRowQueue;

    final ExecutorService m_es;

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
    //Size of the batches this table submits (minimum of all values provided by VoltBulkLoaders)
    volatile int m_minBatchTriggerSize;
    //Insert procedure name
    final String m_procName;
    //Name of table
    final String m_tableName;

    // Callback for batch submissions to the Client. A failed request submits the entire
    // batch of rows to m_failedQueue for row by row processing on m_failureProcessor.
    class PartitionProcedureCallback implements ProcedureCallback {
        final List<VoltBulkLoaderRow> m_batchRowList;

        PartitionProcedureCallback(List<VoltBulkLoaderRow> batchRowList) {
            m_batchRowList = batchRowList;
        }

        // Called by Client to inform us of the status of the bulk insert.
        @Override
        public void clientCallback(ClientResponse response) throws InterruptedException {
            if (response.getStatus() != ClientResponse.SUCCESS) {
                // Queue up all rows for individual processing by originating BulkLoader's FailureProcessor.
                m_es.execute(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            reinsertFailed(m_batchRowList);
                        } catch (Exception e) {
                            loaderLog.error("Failed to re-insert failed batch", e);
                        }
                    }
                });
            }
            else {
                m_batchRowList.get(0).m_loader.m_outstandingRowCount.addAndGet(-1 * m_batchRowList.size());
                m_batchRowList.get(0).m_loader.m_loaderCompletedCnt.addAndGet(m_batchRowList.size());
            }
        }
    }

    PerPartitionTable(ClientImpl clientImpl, String tableName, int partitionId, boolean isMP,
            VoltBulkLoader firstLoader, int minBatchTriggerSize) {
        m_clientImpl = clientImpl;
        m_partitionId = partitionId;
        m_isMP = isMP;
        m_procName = firstLoader.m_procName;
        m_partitionRowQueue = new LinkedBlockingQueue<VoltBulkLoaderRow>(minBatchTriggerSize*5);
        m_minBatchTriggerSize = minBatchTriggerSize;
        m_columnInfo = firstLoader.m_colInfo;
        m_partitionedColumnIndex = firstLoader.m_partitionedColumnIndex;
        m_columnTypes = firstLoader.m_columnTypes;
        m_partitionColumnType = firstLoader.m_partitionColumnType;
        m_tableName = tableName;

        table = new VoltTable(m_columnInfo);

        m_es = CoreUtils.getSingleThreadExecutor(tableName + "-" + partitionId);
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

    /**
     * Synchronized so that when the a single batch is filled up, we only queue one task to
     * drain the queue. The task will drain the queue until it doesn't contain a single batch.
     */
    synchronized void insertRowInTable(final VoltBulkLoaderRow nextRow) throws InterruptedException {
        m_partitionRowQueue.put(nextRow);
        if (m_partitionRowQueue.size() == m_minBatchTriggerSize) {
            m_es.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        while (m_partitionRowQueue.size() >= m_minBatchTriggerSize) {
                            loadTable(buildTable(), table);
                        }
                    } catch (Exception e) {
                        loaderLog.error("Failed to load batch", e);
                    }
                }
            });
        }
    }

    /**
     * Flush all queued rows even if they are smaller than the batch size. This does not
     * guarantee that they will be reinserted if any of them fail. To make sure all rows
     * are either inserted or failed definitively, call shutdown().
     */
    Future<?> flushAllTableQueues() throws InterruptedException {
        return m_es.submit(new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                loadTable(buildTable(), table);
                return true;
            }
        });
    }

    void shutdown() throws Exception {
        try {
            flushAllTableQueues().get();
        } catch (ExecutionException e) {
            throw (Exception) e.getCause();
        }
        m_es.shutdown();
        m_es.awaitTermination(365, TimeUnit.DAYS);
    }

    private void reinsertFailed(List<VoltBulkLoaderRow> rows) throws Exception {
        VoltTable tmpTable = new VoltTable(m_columnInfo);
        for (final VoltBulkLoaderRow row : rows) {
            // No need to check error here if a correctedLine has come here it was
            // previously successful.
            try {
                Object row_args[] = new Object[row.m_rowData.length];
                for (int i = 0; i < row_args.length; i++) {
                    final VoltType type = m_columnTypes[i];
                    row_args[i] = ParameterConverter.tryToMakeCompatible(type.classFromType(),
                            row.m_rowData[i]);
                }
                tmpTable.addRow(row_args);
            } catch (VoltTypeException ex) {
                // Should never happened because the bulk conversion in PerPartitionProcessor
                // should have caught this
                continue;
            }

            ProcedureCallback callback = new ProcedureCallback() {
                @Override
                public void clientCallback(ClientResponse response) throws Exception {
                    row.m_loader.m_outstandingRowCount.decrementAndGet();
                    row.m_loader.m_loaderCompletedCnt.incrementAndGet();

                    //one insert at a time callback
                    if (response.getStatus() != ClientResponse.SUCCESS) {
                        row.m_loader.m_notificationCallBack.failureCallback(row.m_rowHandle, row.m_rowData, response);
                    }
                }
            };

            loadTable(callback, tmpTable);
        }
    }

    private PartitionProcedureCallback buildTable() {
        ArrayList<VoltBulkLoaderRow> buf = new ArrayList<VoltBulkLoaderRow>(m_minBatchTriggerSize);
        m_partitionRowQueue.drainTo(buf, m_minBatchTriggerSize);
        ListIterator<VoltBulkLoaderRow> it = buf.listIterator();
        while (it.hasNext()) {
            VoltBulkLoaderRow currRow = it.next();
            VoltBulkLoader loader = currRow.m_loader;
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
                loader.m_outstandingRowCount.decrementAndGet();
                it.remove();
                continue;
            }
            table.addRow(row_args);
        }

        return new PartitionProcedureCallback(buf);
    }

    private void loadTable(ProcedureCallback callback, VoltTable toSend) throws Exception {
        if (toSend.getRowCount() <= 0) {
            return;
        }

        try {
            if (m_isMP) {
                m_clientImpl.callProcedure(callback, m_procName, m_tableName, toSend);
            } else {
                Object rpartitionParam = HashinatorLite.valueToBytes(toSend.fetchRow(0).get(
                        m_partitionedColumnIndex, m_partitionColumnType));
                m_clientImpl.callProcedure(callback, m_procName, rpartitionParam, m_tableName, toSend);
            }
        } catch (IOException e) {
            final ClientResponse r = new ClientResponseImpl(
                    ClientResponse.CONNECTION_LOST, new VoltTable[0],
                    "Connection to database was lost");
            callback.clientCallback(r);
        }
        toSend.clearRowData();
    }
}
