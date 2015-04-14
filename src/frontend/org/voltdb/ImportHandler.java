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

package org.voltdb;

import com.google_voltpatches.common.util.concurrent.ListeningExecutorService;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.voltcore.logging.VoltLogger;
import org.voltcore.utils.CoreUtils;
import static org.voltdb.ClientInterface.getPartitionForProcedure;
import org.voltdb.catalog.Procedure;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcedureInvocationType;
import org.voltdb.importer.ImportClientResponseAdapter;
import org.voltdb.importer.ImportContext;


/**
 * This class packs the parameters and dispatches the transactions.
 * @author akhanzode
 */
public class ImportHandler {

    private static final VoltLogger m_logger = new VoltLogger("IMPORT");

    // Atomically allows the catalog reference to change between access
    private final CatalogContext m_catalogContext;
    private final AtomicLong m_failedCount = new AtomicLong();
    private final AtomicLong m_successCount = new AtomicLong();
    private final AtomicLong m_submitSuccessCount = new AtomicLong();
    private final AtomicLong m_unpartitionedCount = new AtomicLong();
    private final AtomicLong m_callCount = new AtomicLong();
    private final List<Integer> m_partitions;
    private final ListeningExecutorService m_es;
    private final ImportContext m_importContext;
    private static final ImportClientResponseAdapter m_adapter =
            new ImportClientResponseAdapter(ClientInterface.IMPORTER_CID, "Importer", true);
    private final static AtomicLong m_idGenerator = new AtomicLong(0);
    private final long m_id;

    static {
        VoltDB.instance().getClientInterface().bindAdapter(m_adapter, null);
    }

    private class ImportCallback implements ImportClientResponseAdapter.Callback {

        //TODO: This is when we need to callback into the bundle.
        private final ImportContext m_importerContext;
        public ImportCallback(ImportContext importContext) {
            m_importerContext = importContext;
        }

        @Override
        public void handleResponse(ClientResponse response) {
            if (response.getStatus() == ClientResponse.SUCCESS) {
                m_successCount.incrementAndGet();
            } else {
                m_failedCount.incrementAndGet();
            }
        }

    }

    // The real handler gets created for each importer.
    public ImportHandler(ImportContext importContext, CatalogContext catContext, List<Integer> partitions) {
        m_catalogContext = catContext;
        m_partitions = partitions;

        m_id = m_idGenerator.incrementAndGet();

        //Need 2 threads one for data processing and one for stop.
        m_es = CoreUtils.getListeningExecutorService("ImportHandler - " + System.currentTimeMillis(), 2);
        m_importContext = importContext;
        m_adapter.registerHandler(this);
    }

    public long getId() {
        return m_id;
    }

    /**
     * Submit ready for data
     */
    public void readyForData() {
        m_es.submit(new Runnable() {
            @Override
            public void run() {
                m_importContext.readyForData();
                m_logger.info("Importer finished importing data for: " + m_importContext.getName());
            }
        });
    }

    public void stop() {
        m_es.submit(new Runnable() {

            @Override
            public void run() {
                try {
                    m_importContext.stop();
                } catch (Exception ex) {
                    ex.printStackTrace();
                }
                m_logger.info("Importer stopped: " + m_importContext.getName());
            }
        });
        try {
            m_adapter.unregisterHandler(this);
            m_es.shutdown();
            m_es.awaitTermination(1, TimeUnit.DAYS);
        } catch (InterruptedException ex) {
            m_logger.warn("Importer did not gracefully stopped.", ex);
        }
    }

    //TODO
    public void hadBackPressure() {
        //Handle back pressure
    }

    public boolean callProcedure(ImportContext ic, String proc, Object... fieldList) {
        m_callCount.incrementAndGet();
        // Check for admin mode restrictions before proceeding any further
        if (VoltDB.instance().getMode() == OperationMode.PAUSED) {
            m_logger.warn("Can not invoke procedure from streaming interface when server is paused.");
            m_failedCount.incrementAndGet();
            return false;
        }

        final long nowNanos = System.nanoTime();
        StoredProcedureInvocation task = new StoredProcedureInvocation();
        ParameterSet pset = ParameterSet.fromArrayWithCopy(fieldList);
        //type + procname(len + name) + connectionId (long) + params
        int sz = 1 + 4 + proc.length() + 8 + pset.getSerializedSize();
        ByteBuffer buf = ByteBuffer.allocateDirect(sz);
        try {
            buf.put((byte )ProcedureInvocationType.ORIGINAL.getValue());
            buf.putInt((int )proc.length());
            buf.put(proc.getBytes());
            buf.putLong(ImportHandler.m_adapter.connectionId());
            ByteBuffer pbuf = ByteBuffer.allocateDirect(pset.getSerializedSize());
            pset.flattenToBuffer(pbuf);
            pbuf.flip();
            buf.put(pbuf);
            buf.flip();
            task.initFromBuffer(buf);
            task.setClientHandle(m_adapter.registerCallback(new ImportCallback(ic)));

        } catch (IOException ex) {
            m_failedCount.incrementAndGet();
            m_logger.error("Failed to serialize parameters for stream: " + proc, ex);
            return false;
        }

        Procedure catProc = m_catalogContext.procedures.get(task.procName);
        if (catProc == null) {
            catProc = m_catalogContext.m_defaultProcs.checkForDefaultProcedure(task.procName);
        }

        if (catProc == null) {
            m_logger.error("Can not invoke procedure from streaming interface procedure not found.");
            m_failedCount.incrementAndGet();
            return false;
        }
        final CatalogContext.ProcedurePartitionInfo ppi = (CatalogContext.ProcedurePartitionInfo)catProc.getAttachment();

        int partition = -1;
        if (catProc.getSinglepartition()) {
            // break out the Hashinator and calculate the appropriate partition
            try {
                partition = getPartitionForProcedure(ppi.index, ppi.type, task);
            } catch (Exception e) {
                m_logger.error("Can not invoke SP procedure from streaming interface partition not found.");
                m_failedCount.incrementAndGet();
                return false;
            }
            //TODO: this should be property
            if (!m_partitions.contains(partition)) {
                //Not our partition dont do anything.
                m_unpartitionedCount.incrementAndGet();
                return false;
            }
        }

        //Submmit the transaction.
        boolean success = VoltDB.instance().getClientInterface().createTransaction(m_adapter.connectionId(), task,
                catProc.getReadonly(), catProc.getSinglepartition(), catProc.getEverysite(), partition,
                task.getSerializedSize(), nowNanos);
        if (success) {
            m_submitSuccessCount.incrementAndGet();
        }
        return success;
    }
}
