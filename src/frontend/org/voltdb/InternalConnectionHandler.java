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

import org.voltcore.logging.Level;
import org.voltcore.logging.VoltLogger;
import org.voltcore.utils.DBBPool;
import org.voltcore.utils.DBBPool.BBContainer;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Table;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcedureCallback;
import org.voltdb.client.ProcedureInvocationType;

/**
 * This class packs the parameters and dispatches the transactions.
 * Make sure responses over network thread does not touch this class.
 * @author akhanzode
 */
public class InternalConnectionHandler {

    public final static long SUPPRESS_INTERVAL = 60;
    private static final VoltLogger m_logger = new VoltLogger("InternalConnectionHandler");

    // Atomically allows the catalog reference to change between access
    private final AtomicLong m_failedCount = new AtomicLong();
    private final AtomicLong m_submitSuccessCount = new AtomicLong();
    private final InternalClientResponseAdapter m_adapter;

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

    public class NullCallback implements ProcedureCallback {
        @Override
        public void clientCallback(ClientResponse response) throws Exception {
        }
    }

    public boolean callProcedure(InternalConnectionContext caller, long backPressureTimeout, String proc, Object... fieldList) {
        return callProcedure(caller, backPressureTimeout, new NullCallback(), proc, fieldList);
    }

    // Use backPressureTimeout value <= 0  for no back pressure timeout
    public boolean callProcedure(InternalConnectionContext caller, long backPressureTimeout, ProcedureCallback procCallback, String proc, Object... fieldList) {
        Procedure catProc = VoltDB.instance().getClientInterface().getProcedureFromName(proc, VoltDB.instance().getCatalogContext());
        if (catProc == null) {
            String fmt = "Cannot invoke procedure %s from streaming interface %s. Procedure not found.";
            m_logger.rateLimitedLog(SUPPRESS_INTERVAL, Level.ERROR, null, fmt, proc, caller);
            m_failedCount.incrementAndGet();
            return false;
        }

        //Indicate backpressure or not.
        boolean b = m_adapter.hasBackPressure();
        caller.setBackPressure(b);
        if (b) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException ex) {
            }
        }

        final long nowNanos = System.nanoTime();
        StoredProcedureInvocation task = new StoredProcedureInvocation();
        ParameterSet pset = ParameterSet.fromArrayWithCopy(fieldList);
        // Another place with hard code size calculations
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

        int partition = -1;
        try {
            partition = getPartitionForProcedure(catProc, task);
        } catch (Exception e) {
            String fmt = "Can not invoke procedure %s from streaming interface %s. Partition not found.";
            m_logger.rateLimitedLog(SUPPRESS_INTERVAL, Level.ERROR, e, fmt, proc, caller);
            m_failedCount.incrementAndGet();
            tcont.discard();
            return false;
        }

        boolean success = m_adapter.createTransaction(caller, proc, catProc, procCallback, task, tcont, partition, nowNanos);
        if (!success) {
            tcont.discard();
            m_failedCount.incrementAndGet();
        } else {
            m_submitSuccessCount.incrementAndGet();
        }

        return success;
    }

    public boolean hasBackPressure() {
        return m_adapter.hasBackPressure();
    }
}
