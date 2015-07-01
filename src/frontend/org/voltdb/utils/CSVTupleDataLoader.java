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

package org.voltdb.utils;

import com.google_voltpatches.common.collect.Lists;
import org.voltcore.logging.VoltLogger;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.client.Client;
import org.voltdb.client.ClientImpl;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcCallException;
import org.voltdb.client.ProcedureCallback;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.voltdb.ClientResponseImpl;

/**
 * A CSVDataLoader implementation that inserts one row at a time.
 */
public class CSVTupleDataLoader implements CSVDataLoader {
    private static final VoltLogger m_log = new VoltLogger("CSVLOADER");

    private final Client m_client;
    private final String m_insertProcedure;
    private final VoltType[] m_columnTypes;
    private final BulkLoaderErrorHandler m_errHandler;

    final AtomicLong m_processedCount = new AtomicLong(0);
    final AtomicLong m_failedCount = new AtomicLong(0);
    final int m_reportEveryNRows = 10000;

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

        public PartitionSingleExecuteProcedureCallback(RowWithMetaData csvLine) {
            m_csvLine = csvLine;
        }

        //one insert at a time callback
        @Override
        public void clientCallback(ClientResponse response) throws Exception {
            byte status = response.getStatus();
            if (status != ClientResponse.SUCCESS) {
                m_failedCount.incrementAndGet();
                m_errHandler.handleError(m_csvLine, response, response.getStatusString());
            }
            long currentCount = m_processedCount.incrementAndGet();

            if (currentCount % m_reportEveryNRows == 0) {
                m_log.info("Inserted " + (currentCount - m_failedCount.get()) + " rows");
            }
        }
    }

    public CSVTupleDataLoader(ClientImpl client, String procName, BulkLoaderErrorHandler errHandler)
            throws IOException, ProcCallException
    {
        m_client = client;
        m_insertProcedure = procName;
        m_errHandler = errHandler;

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

        int sleptTimes = 0;
        while (!client.isHashinatorInitialized() && sleptTimes < 120) {
            try {
                Thread.sleep(500);
                sleptTimes++;
            } catch (InterruptedException ex) {}
        }
    }

    @Override
    public VoltType[] getColumnTypes()
    {
        return m_columnTypes;
    }

    @Override
    public void insertRow(RowWithMetaData metaData, Object[] values) throws InterruptedException {
        try {
            PartitionSingleExecuteProcedureCallback cbmt =
                    new PartitionSingleExecuteProcedureCallback(metaData);
            if (!m_client.callProcedure(cbmt, m_insertProcedure, values)) {
                m_log.fatal("Failed to send CSV insert to VoltDB cluster.");
                ClientResponse response = new ClientResponseImpl(ClientResponseImpl.SERVER_UNAVAILABLE,
                        new VoltTable[0], "Failed to call procedure.", 0);
                m_errHandler.handleError(metaData, response, "Failed to call procedure.");
            }
        } catch (NoConnectionsException ex) {
            ClientResponse response = new ClientResponseImpl(ClientResponseImpl.SERVER_UNAVAILABLE,
                    new VoltTable[0], "Failed to call procedure.", 0);
            m_errHandler.handleError(metaData, response, "Failed to call procedure.");
        } catch (IOException ex) {
            ClientResponse response = new ClientResponseImpl(ClientResponseImpl.SERVER_UNAVAILABLE,
                    new VoltTable[0], "Failed to call procedure.", 0);
            m_errHandler.handleError(metaData, response, "Failed to call procedure.");
        } catch (Exception ex) {
            m_errHandler.handleError(metaData, null, ex.toString());
        }
    }

    @Override
    public void close() throws InterruptedException, NoConnectionsException
    {
        m_client.drain();
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
}
