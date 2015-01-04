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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.voltcore.logging.VoltLogger;
import org.voltcore.utils.CoreUtils;

import org.voltdb.ClientResponseImpl;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.VoltTypeException;
import org.voltdb.client.ClientImpl;
import org.voltdb.client.ClientResponse;

/**
 * VoltBulkLoader is meant to run for long periods of time. Multiple threads can
 * operate on a single instance of the VoltBulkLoader to feed and bulk load a
 * single table. It is also possible for multiple instances of the
 * VoltBulkLoader to operate concurrently no the same table or different tables
 * as long as they share the same Client instance.
 *
 * All instances of VoltBulkLoader using a common Client share a pool of threads
 * dedicated to feeding individual partitions. There is also a thread dedicated
 * to processing multi-partition tables. Finally, each instance of
 * VoltBulkLoader has a thread, dedicated to processing failed bulk inserts. If
 * multiple threads are inserting rows into the same VoltBulkLoader instance and
 * batch inserts are relatively frequent, using multiple VoltBulkLoaders should
 * improve performance characteristics particularly if one thread is injecting
 * more failed rows than other threads. If one thread is injecting too many
 * failures, relative to other threads operating on the same table, that thread
 * should be isolated a different process. If failed rows are a common problem,
 * throughput can also be improved by using multiple instances of VoltBulkLoader.
 */
public class VoltBulkLoader {
    private static final VoltLogger loaderLog = new VoltLogger("LOADER");

    final BulkLoaderState m_vblGlobals;
    final ClientImpl m_clientImpl;
    // Batch size requested for this instance of VoltBulkLoader
    final int m_maxBatchSize;
    // Callback used to notify users of failed row inserts
    final BulkLoaderFailureCallBack m_notificationCallBack;
    //Array of PerPartitionTables from which this VoltBulkLoader chooses to put a row in
    PerPartitionTable[] m_partitionTable = null;
    //Index in m_partitionTable of first partition
    final int m_firstPartitionTable;
    //Index in m_partitionTable of last partition
    final int m_lastPartitionTable;

    // Name of procedure used by this instance of the VoltBulkLoader
    final String m_procName;
    //Table name to insert into.
    String m_tableName;
    //Type of partitioned column
    VoltType m_partitionColumnType = VoltType.NULL;
    //Column information
    VoltTable.ColumnInfo m_colInfo[];
    //Column types
    //Map<Integer, VoltType> m_columnTypes;
    TreeMap<Integer, VoltType> m_mappedColumnTypes;
    //In array form
    final VoltType[] m_columnTypes;
    //Index of partitioned column in table
    int m_partitionedColumnIndex = -1;
    //Column Names
    Map<Integer, String> m_colNames;
    //Number of columns
    int m_columnCnt = 0;
    //Is this a MP transaction
    private boolean m_isMP = false;
    //Total number of partition processors including the MP processor.
    private int m_maxPartitionProcessors = -1;

    //Scheduled Executor for periodic flush by default no periodic flush is enabled. Kafka loader enables it
    private final ScheduledThreadPoolExecutor m_ses = CoreUtils.getScheduledThreadPoolExecutor("Periodic-Flush", 1, CoreUtils.SMALL_STACK_SIZE);
    private ScheduledFuture<?> m_flush = null;

    // Number of rows currently being processed.
    final AtomicLong m_outstandingRowCount = new AtomicLong(0);
    //Number of rows for which we have received a definitive success or failure.
    final AtomicLong m_loaderCompletedCnt = new AtomicLong(0);

    // Constructor allocated through the Client to ensure consistency of VoltBulkLoaderGlobals
    public VoltBulkLoader(BulkLoaderState vblGlobals, String tableName, int maxBatchSize,
                BulkLoaderFailureCallBack blfcb) throws Exception {
        this.m_clientImpl = vblGlobals.m_clientImpl;
        this.m_maxBatchSize = maxBatchSize;
        this.m_notificationCallBack = blfcb;

        m_vblGlobals = vblGlobals;

        // Get table details and then build partition and column information.
        // Table analysis could be done once per unique table name but then the
        // m_TableNameToLoader lock would have to be held for a much longer.
        m_tableName = tableName;
        VoltTable procInfo = m_clientImpl.callProcedure("@SystemCatalog",
                "COLUMNS").getResults()[0];

        m_mappedColumnTypes = new TreeMap<Integer, VoltType>();
        m_colNames = new TreeMap<Integer, String>();
        m_partitionedColumnIndex = -1;
        m_partitionColumnType = VoltType.NULL;

        int sleptTimes = 0;
        while (!m_clientImpl.isHashinatorInitialized() && sleptTimes < 120) {
            try {
                Thread.sleep(500);
                sleptTimes++;
            } catch (InterruptedException ex) {}
        }

        if (sleptTimes >= 120) {
            throw new IllegalStateException("VoltBulkLoader unable to start due to uninitialized Client.");
        }

        while (procInfo.advanceRow()) {
            String table = procInfo.getString("TABLE_NAME");
            if (tableName.equalsIgnoreCase(table)) {
                VoltType vtype = VoltType.typeFromString(procInfo.getString("TYPE_NAME"));
                int idx = (int) procInfo.getLong("ORDINAL_POSITION") - 1;
                m_mappedColumnTypes.put(idx, vtype);
                m_colNames.put(idx, procInfo.getString("COLUMN_NAME"));
                String remarks = procInfo.getString("REMARKS");
                if (remarks != null && remarks.equalsIgnoreCase("PARTITION_COLUMN")) {
                    m_partitionColumnType = vtype;
                    m_partitionedColumnIndex = idx;
                }
            }
        }
        m_columnCnt = m_mappedColumnTypes.size();

        if (m_columnCnt == 0) {
            //VoltBulkLoader will exit.
            throw new IllegalArgumentException("Table Name parameter does not match any known table.");
        }
        m_columnTypes = getColumnTypes();

        //Build column info so we can build VoltTable
        m_colInfo = new VoltTable.ColumnInfo[m_columnCnt];
        for (int i = 0; i < m_columnCnt; i++) {
            VoltType type = m_columnTypes[i];
            String cname = m_colNames.get(i);
            VoltTable.ColumnInfo ci = new VoltTable.ColumnInfo(cname, type);
            m_colInfo[i] = ci;
        }

        int sitesPerHost = 1;
        int kfactor = 0;
        int hostcount = 1;
        procInfo = m_clientImpl.callProcedure("@SystemInformation",
                "deployment").getResults()[0];
        while (procInfo.advanceRow()) {
            String prop = procInfo.getString("PROPERTY");
            if (prop != null && prop.equalsIgnoreCase("sitesperhost")) {
                sitesPerHost = Integer.parseInt(procInfo.getString("VALUE"));
            }
            if (prop != null && prop.equalsIgnoreCase("hostcount")) {
                hostcount = Integer.parseInt(procInfo.getString("VALUE"));
            }
            if (prop != null && prop.equalsIgnoreCase("kfactor")) {
                kfactor = Integer.parseInt(procInfo.getString("VALUE"));
            }
        }

        m_isMP = (m_partitionedColumnIndex == -1);
        // Dedicate a PartitionProcessor to MP tables
        m_maxPartitionProcessors = ((hostcount * sitesPerHost) / (kfactor + 1)) + 1;

        if (!m_isMP) {
            m_firstPartitionTable = 0;
            m_lastPartitionTable = m_maxPartitionProcessors-2;
            m_procName = "@LoadSinglepartitionTable" ;
        }
        else {
            m_firstPartitionTable = m_maxPartitionProcessors-1;
            m_lastPartitionTable = m_maxPartitionProcessors-1;
            m_procName = "@LoadMultipartitionTable" ;
        }

        List<VoltBulkLoader> loaderList = m_vblGlobals.m_TableNameToLoader.get(m_tableName);
        if (loaderList == null) {
            // First BulkLoader for this table
            m_partitionTable = new PerPartitionTable[m_maxPartitionProcessors];
            // Set up the BulkLoaderPerPartitionTables
            for(int i=m_firstPartitionTable; i<=m_lastPartitionTable; i++) {
                m_partitionTable[i] = new PerPartitionTable(m_clientImpl, m_tableName,
                        i, i == m_maxPartitionProcessors-1, this, maxBatchSize);
            }
            loaderList = new ArrayList<VoltBulkLoader>();
            loaderList.add(this);
            m_vblGlobals.m_TableNameToLoader.put(m_tableName, loaderList);
        }
        else {
            // Nth loader for this table
            VoltBulkLoader primary = loaderList.get(0);
            m_partitionTable = primary.m_partitionTable;
            loaderList.add(this);
            for(int i=m_firstPartitionTable; i<=m_lastPartitionTable; i++) {
                if (primary.m_maxBatchSize != maxBatchSize) {
                    m_partitionTable[i].updateMinBatchTriggerSize(maxBatchSize);
                }
            }
        }
    }

    /**
     * Set periodic flush interval and initial delay in seconds.
     *
     * @param delay Initial delay in seconds
     * @param seconds Interval in seconds, passing <code>seconds <= 0</code> value will cancel periodic flush
     */
    public synchronized void setFlushInterval(long delay, long seconds) {
        if (m_flush != null) {
            m_flush.cancel(false);
            m_flush = null;
        }
        if (seconds > 0) {
            m_flush = m_ses.scheduleAtFixedRate(new Runnable() {
                @Override
                public void run() {
                    try {
                        flush();
                    } catch (Exception e) {
                        loaderLog.error("Failed to flush loader buffer, some tuples may not be inserted.", e);
                    }
                }
            }, delay, seconds, TimeUnit.SECONDS);
        }
    }

    void generateError(Object rowHandle, Object[] objectList, String errMessage) {
        VoltTable[] dummyTable = new VoltTable[1];
        dummyTable[0] = new VoltTable(m_colInfo);
        ClientResponse dummyResponse = new ClientResponseImpl(ClientResponse.GRACEFUL_FAILURE,
                dummyTable, errMessage);
        m_notificationCallBack.failureCallback(rowHandle, objectList, dummyResponse);
        m_loaderCompletedCnt.incrementAndGet();
    }

    /**
     *  <p>Add new row to VoltBulkLoader table.</p>
     *
     * @param rowHandle User supplied object used to distinguish failed insert attempts
     * @param fieldList List of fields associated with a single row insertion
     * @throws java.lang.InterruptedException
     */
    public void insertRow(Object rowHandle, Object... fieldList)  throws InterruptedException {
        int partitionId = 0;
        //Find partition to send this row to and put on correct PerPartitionTable.
        if (fieldList == null || fieldList.length <= 0) {
            String errMsg;
            if (rowHandle == null)
                errMsg = "Error: insertRow received empty fieldList";
            else
                errMsg = "Error: insertRow received empty fieldList for row: " + rowHandle.toString();
            generateError(rowHandle, fieldList, errMsg);
            return;
        }
        if (fieldList.length != m_columnCnt) {
            String errMsg;
            if (rowHandle == null)
                errMsg = "Error: insertRow received incorrect number of columns; " + fieldList.length +
                        " found, " + m_columnCnt + " expected";
            else
                errMsg = "Error: insertRow received incorrect number of columns; " + fieldList.length +
                        " found, " + m_columnCnt + " expected for row: " + rowHandle.toString();
            generateError(rowHandle, fieldList, errMsg);
            return;
        }
        VoltBulkLoaderRow newRow = new VoltBulkLoaderRow(this, rowHandle, fieldList);
        if (m_isMP) {
            m_partitionTable[m_firstPartitionTable].insertRowInTable(newRow);
        }
        else {
            try {
                partitionId = (int)m_clientImpl.getPartitionForParameter(
                        m_partitionColumnType.getValue(), fieldList[m_partitionedColumnIndex]);
                m_partitionTable[partitionId].insertRowInTable(newRow);
            } catch (VoltTypeException e) {
                generateError(rowHandle, fieldList, e.getMessage());
                return;
            }
        }
        m_outstandingRowCount.incrementAndGet();
    }

    /**
     * Called to asynchronously force the VoltBulkLoader to submit all the partially full batches
     * in all partitions of the table to the Client for insert. To wait for all rows to be processed,
     * use drain(). This method will also flush pending rows submitted by other VoltBulkLoader
     * instances working on the same table and using the same instance of Client.
     */
    public void flush() throws ExecutionException, InterruptedException {
        for (int i = m_firstPartitionTable; i <= m_lastPartitionTable; i++) {
            m_partitionTable[i].flushAllTableQueues();
        }
    }

    /**
     * Called to synchronously force the VoltBulkLoader to submit all the partially full batches
     * in all partitions of the table to the Client for insert. This call will wait until all
     * previously submitted rows (including retried failed batch rows) have been processed and
     * received responses from the Client.
     * @throws java.lang.InterruptedException
     */
    public synchronized void drain() throws InterruptedException {
        // Wait for number of PerPartitionTables we are using and the Failure Processor
        for (int i=m_firstPartitionTable; i<=m_lastPartitionTable; i++) {
            try {
                m_partitionTable[i].flushAllTableQueues().get();
            } catch (ExecutionException e) {
                loaderLog.error("Failed to drain all buffers, some tuples may not be inserted yet.", e);
            }
        }

        // Draining the client doesn't guarantee that all failed rows are re-inserted, need to
        // loop until the outstanding row count reaches 0.
        while (m_outstandingRowCount.get() != 0) {
            m_clientImpl.drain();
            Thread.yield();
        }
    }

    /**
     * Waits for all pending inserts to be acknowledged and then closes this instance of the
     * VoltBulkLoader. During and after the invocation of close(), calls to insertRow will get
     * an Exception. All other instances of VoltBulkLoader will continue to function.
     * @throws java.lang.InterruptedException
     */
    public synchronized void close() throws Exception {
        //Stop the periodic flush as we will flush laster
        if (m_flush != null) {
            m_flush.cancel(false);
        }
        m_ses.shutdown();

        // Remove this VoltBulkLoader from the active set.
        synchronized (m_vblGlobals) {
            List<VoltBulkLoader> loaderList = m_vblGlobals.m_TableNameToLoader.get(m_tableName);
            if (loaderList.size() == 1) {
                m_vblGlobals.m_TableNameToLoader.remove(m_tableName);
            }
            else
                loaderList.remove(this);

            // First flush the tables
            // keep one PerPartitionTable around so we can use it as the poisoned
            // table for the PartitionProcessors
            drain();
            for (PerPartitionTable ppt : m_partitionTable) {
                if (ppt != null) {
                    try {
                        ppt.shutdown();
                    } catch (Exception e) {
                        loaderLog.error("Failed to close processor for partition " + ppt.m_partitionId, e);
                    }
                }
            }
        }

        assert m_outstandingRowCount.get() == 0;
    }

    /**
     * As other instances of VoltBulkLoader working on the same table could alter the size
     * of batches, this method provides the means to examine the current batch size.
     *
     * @return The size of batches currently being submitted for this table
     */
    public int getMaxBatchSize() {
        return m_partitionTable[m_firstPartitionTable].m_minBatchTriggerSize;
    }

    /**
     * @return The number rows that have been received by this instance of VoltBulkLoader
     *  but have not been processed by the Client
     */
    public long getOutstandingRowCount() {
        return m_outstandingRowCount.get();
    }

    /**
     * @return The number of rows that have been received by this instance of
     *  VoltBulkLoader and processed by the Client including failed inserts that
     *  have performed callbacks using BulkLoaderFailureCallBack.
     */
    public long getCompletedRowCount() {
        return m_loaderCompletedCnt.get();
    }

    public VoltType[] getColumnTypes() {
        return (VoltType[])m_mappedColumnTypes.values().toArray(new VoltType[m_mappedColumnTypes.size()]);
    }

}
