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

import static org.voltdb.ClientInterface.getPartitionForProcedure;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicLong;

import org.voltcore.logging.VoltLogger;
import org.voltcore.utils.DBBPool;
import org.voltcore.utils.DBBPool.BBContainer;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Table;
import org.voltdb.client.ProcedureInvocationType;

/**
 * This class packs the parameters and dispatches the transactions.
 * Make sure responses over network thread does not touch this class.
 * @author akhanzode
 */
public class InternalConnectionHandler {

    private static final VoltLogger m_logger = new VoltLogger("InternalConnectionHandler");

    // Atomically allows the catalog reference to change between access
    private final AtomicLong m_failedCount = new AtomicLong();
    private final AtomicLong m_submitSuccessCount = new AtomicLong();
    private final InternalClientResponseAdapter m_adapter;

    private static final long MAX_PENDING_TRANSACTIONS = Integer.getInteger("IMPORTER_MAX_PENDING_TRANSACTIONS", 5000);

    public InternalConnectionHandler(InternalClientResponseAdapter adapter) {
        m_adapter = adapter;
    }

    //Allocate and pool similar row sizes will reuse the buffers.
    private BBContainer getBuffer(int sz) {
        return DBBPool.allocateDirectAndPool(sz);
    }

    /**
     * Returns true if a table with the given name exists in the server catalog.
     */
    public boolean hasTable(String name) {
        Table table = VoltDB.instance().getCatalogContext().tables.get(name);
        return (table!=null);
    }

    // Use backPressureTimeout value <= 0  for no back pressure timeout
    public boolean callProcedure(long backPressureTimeout, String proc, Object... fieldList) {
        // Check for admin mode restrictions before proceeding any further
        if (VoltDB.instance().getMode() == OperationMode.PAUSED) {
            m_logger.warn("Server is paused and is currently unavailable - please try again later.");
            m_failedCount.incrementAndGet();
            return false;
        }
        Procedure catProc = VoltDB.instance().getCatalogContext().procedures.get(proc);
        if (catProc == null) {
            catProc = VoltDB.instance().getCatalogContext().m_defaultProcs.checkForDefaultProcedure(proc);
        }

        if (catProc == null) {
            if (proc.equals("@AdHoc")) {
                // Map @AdHoc... to @AdHoc_RW_MP for validation. In the future if security is
                // configured differently for @AdHoc... variants this code will have to
                // change in order to use the proper variant based on whether the work
                // is single or multi partition and read-only or read-write.
                proc = "@AdHoc_RW_MP";
            }
            SystemProcedureCatalog.Config sysProc = SystemProcedureCatalog.listing.get(proc);
            if (sysProc != null) {
                catProc = sysProc.asCatalogProcedure();
            }
            if (catProc == null) {
                m_logger.error("Can not invoke procedure from streaming interface procedure not found.");
                m_failedCount.incrementAndGet();
                return false;
            }
        }

        int counter = 1;
        int maxSleepNano = 100000;
        long start = System.nanoTime();
        while ((backPressureTimeout > 0) && (m_adapter.getPendingCount() > MAX_PENDING_TRANSACTIONS)) {
            try {
                int nanos = 500 * counter++;
                Thread.sleep(0, nanos > maxSleepNano ? maxSleepNano : nanos);
                //We have reached max timeout.
                if (System.nanoTime() - start > backPressureTimeout) {
                    return false;
                }
            } catch (InterruptedException ex) { }
        }
        final long nowNanos = System.nanoTime();
        StoredProcedureInvocation task = new StoredProcedureInvocation();
        ParameterSet pset = ParameterSet.fromArrayWithCopy(fieldList);
        //type + procname(len + name) + connectionId (long) + params
        int sz = 1 + 4 + proc.length() + 8 + pset.getSerializedSize();
        //This is released in callback from adapter side.
        final BBContainer tcont = getBuffer(sz);
        final ByteBuffer taskbuf = tcont.b();
        try {
            taskbuf.put(ProcedureInvocationType.ORIGINAL.getValue());
            taskbuf.putInt(proc.length());
            taskbuf.put(proc.getBytes());
            taskbuf.putLong(m_adapter.connectionId());
            pset.flattenToBuffer(taskbuf);
            taskbuf.flip();
            task.initFromBuffer(taskbuf);
        } catch (IOException ex) {
            m_failedCount.incrementAndGet();
            m_logger.error("Failed to serialize parameters for stream: " + proc, ex);
            tcont.discard();
            return false;
        }

        final CatalogContext.ProcedurePartitionInfo ppi = (CatalogContext.ProcedurePartitionInfo)catProc.getAttachment();

        int partition = -1;
        if (catProc.getSinglepartition()) {
            try {
                partition = getPartitionForProcedure(ppi.index, ppi.type, task);
            } catch (Exception e) {
                m_logger.error("Can not invoke SP procedure from streaming interface partition not found.");
                m_failedCount.incrementAndGet();
                tcont.discard();
                return false;
            }
        }

        boolean success;
        //Synchronize this to create good handles across all ImportHandlers
        synchronized(InternalConnectionHandler.class) {
            success = m_adapter.createTransaction(catProc, null, task, tcont, partition, nowNanos);
        }
        if (!success) {
            tcont.discard();
            m_failedCount.incrementAndGet();
        } else {
            m_submitSuccessCount.incrementAndGet();
        }
        return success;
    }
}
