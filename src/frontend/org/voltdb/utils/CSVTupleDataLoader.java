/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
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

package org.voltdb.utils;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.voltcore.logging.VoltLogger;
import org.voltcore.utils.CoreUtils;
import org.voltdb.ClientResponseImpl;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcCallException;
import org.voltdb.client.ProcedureCallback;
import org.voltdb.client.VoltBulkLoader.BulkLoaderSuccessCallback;

import com.google_voltpatches.common.collect.Lists;

/**
 * A CSVDataLoader implementation that inserts one row at a time.
 */
public class CSVTupleDataLoader implements CSVDataLoader {
    private static final VoltLogger m_log = new VoltLogger("CSVLOADER");

    private final Client m_client;
    private final String m_insertProcedure;
    private final VoltType[] m_columnTypes;
    private final BulkLoaderErrorHandler m_errHandler;
    private final ExecutorService m_callbackExecutor;
    private final ExecutorService m_reinsertExecutor;
    private final boolean m_autoReconnect;

    final AtomicLong m_processedCount = new AtomicLong(0);
    final AtomicLong m_failedCount = new AtomicLong(0);
    final AtomicLong m_totalRowCount = new AtomicLong(0);
    final int m_reportEveryNRows = 10000;

    final BulkLoaderSuccessCallback m_successCallback;

    @Override
    public void setFlushInterval(int delay, int seconds) {
        //no op
    }

    @Override
    public void flush() {
        if (m_client != null) {
            try {
                m_client.drain();
            } catch (NoConnectionsException ex) {
                m_log.info("Failed to flush: " + ex);
            } catch (InterruptedException ex) {
                m_log.info("Failed to flush: " + ex);
            }
        }
    }

    //Callback for single row procedure invoke called for rows in failed batch.
    private class PartitionSingleExecuteProcedureCallback implements ProcedureCallback {
        final RowWithMetaData m_csvLine;
        final Object[] m_values;

        public PartitionSingleExecuteProcedureCallback(RowWithMetaData csvLine, Object[] values) {
            m_csvLine = csvLine;
            m_values = values;
        }

        //one insert at a time callback
        @Override
        public void clientCallback(final ClientResponse response) throws Exception {
            byte status = response.getStatus();
            if (status == ClientResponse.SUCCESS) {
                if (m_callbackExecutor != null && m_successCallback != null) {
                    // If the client is keeping track of offsets, notify it (but run on a service thread)
                    m_callbackExecutor.execute(new Runnable() {
                        @Override
                        public void run() {
                            m_successCallback.success(m_csvLine, response);
                        }
                    });
                }
            }
            else if (status == ClientResponse.CONNECTION_LOST && m_autoReconnect) {
                m_reinsertExecutor.execute(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            insert(m_csvLine, m_values);
                        } catch (InterruptedException e) {
                            m_log.error("CSVLoader interrupted: " + e);
                        }
                    }
                });
                return;
            }
            else {
                m_failedCount.incrementAndGet();
                m_errHandler.handleError(m_csvLine, response, response.getStatusString());
            }
            long currentCount = m_processedCount.incrementAndGet();

            if (currentCount % m_reportEveryNRows == 0) {
                m_log.info("Inserted " + (currentCount - m_failedCount.get()) + " rows");
            }
        }
    }

    public CSVTupleDataLoader(Client client, String procName, BulkLoaderErrorHandler errHandler)
            throws IOException, ProcCallException
    {
        this(client, procName, errHandler, null, null);
    }

    public CSVTupleDataLoader(Client client, String procName, BulkLoaderErrorHandler errHandler, ExecutorService callbackExecutor)
            throws IOException, ProcCallException {
        this(client, procName, errHandler, callbackExecutor, null);
    }

    public CSVTupleDataLoader(Client client, String procName, BulkLoaderErrorHandler errHandler, ExecutorService callbackExecutor, BulkLoaderSuccessCallback successCallback)
            throws IOException, ProcCallException
    {
        m_client = client;
        m_insertProcedure = procName;
        m_errHandler = errHandler;
        m_callbackExecutor = callbackExecutor;
        m_successCallback = successCallback;
        m_autoReconnect = client.isAutoReconnectEnabled();
        if (m_autoReconnect) {
            m_reinsertExecutor = CoreUtils.getSingleThreadExecutor(procName);
        } else {
            m_reinsertExecutor = null;
        }


        List<VoltType> typeList = Lists.newArrayList();
        VoltTable procInfo = client.callProcedure("@SystemCatalog", "PROCEDURECOLUMNS").getResults()[0];
        while (procInfo.advanceRow()) {
            if (m_insertProcedure.equalsIgnoreCase(procInfo.getString("PROCEDURE_NAME"))) {
                String typeStr = (String) procInfo.get("TYPE_NAME", VoltType.STRING);
                typeList.add(VoltType.typeFromString(typeStr));
            }
        }
        if (typeList.isEmpty()) {
            //csvloader will exit
            throw new RuntimeException("No matching insert procedure available");
        }
        m_columnTypes = typeList.toArray(new VoltType[0]);

        if (!m_client.waitForTopology(60_000)) {
            throw new RuntimeException("Unable to start due to uninitialized Client.");
        }
    }

    @Override
    public VoltType[] getColumnTypes()
    {
        return m_columnTypes;
    }

    @Override
    public void insertRow(RowWithMetaData metaData, Object[] values) throws InterruptedException {
        m_totalRowCount.incrementAndGet();
        insert(metaData, values);

    }

    private void insert(RowWithMetaData metaData, Object[] values) throws InterruptedException {
        if (m_autoReconnect) {
            while (true) {
                try {
                    PartitionSingleExecuteProcedureCallback cbmt =
                            new PartitionSingleExecuteProcedureCallback(metaData, values);
                    if (!m_client.callProcedure(cbmt, m_insertProcedure, values)) {
                        m_log.fatal("Failed to send CSV insert to VoltDB cluster.");
                        ClientResponse response = new ClientResponseImpl(ClientResponseImpl.SERVER_UNAVAILABLE,
                                new VoltTable[0], "Failed to call procedure.", 0);
                        m_errHandler.handleError(metaData, response, "Failed to call procedure.");
                    }
                    // Row inserted successfully. So move on
                    break;
                } catch (IOException e) {
                    // If the connection is lost, suspend and wait for reconnect listener's notification
                    synchronized (this) {
                        this.wait();
                    }
                } catch (Exception e) {
                    m_errHandler.handleError(metaData, null, e.toString());
                }
            }
        } else {
            try {
                PartitionSingleExecuteProcedureCallback cbmt =
                        new PartitionSingleExecuteProcedureCallback(metaData, values);
                if (!m_client.callProcedure(cbmt, m_insertProcedure, values)) {
                    m_log.fatal("Failed to send CSV insert to VoltDB cluster.");
                    ClientResponse response = new ClientResponseImpl(ClientResponseImpl.SERVER_UNAVAILABLE,
                            new VoltTable[0], "Failed to call procedure.", 0);
                    m_errHandler.handleError(metaData, response, "Failed to call procedure.");
                }
            } catch (IOException e) {
                ClientResponse response = new ClientResponseImpl(ClientResponseImpl.SERVER_UNAVAILABLE,
                        new VoltTable[0], "Failed to call procedure.", 0);
                m_errHandler.handleError(metaData, response, "Failed to call procedure.");
            } catch (Exception e) {
                m_errHandler.handleError(metaData, null, e.toString());
            }
        }
    }

    @Override
    public void close() throws InterruptedException, NoConnectionsException
    {
        while (m_processedCount.get() != m_totalRowCount.get()) {
            m_client.drain();
            Thread.yield();
        }
        if (m_reinsertExecutor != null) {
            m_reinsertExecutor.shutdown();
            m_reinsertExecutor.awaitTermination(365, TimeUnit.DAYS);
        }
        // Don't close the client because it may be shared with other loaders
    }

    @Override
    public long getProcessedRows()
    {
        return m_processedCount.get();
    }

    @Override
    public long getFailedRows()
    {
        return m_failedCount.get();
    }

    @Override
    public Map<Integer, String> getColumnNames()
    {
        //No operation.
        return null;
    }

    @Override
    public void resumeLoading() {
        synchronized (this) {
            this.notifyAll();
        }
    }
}
