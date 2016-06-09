/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
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

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.voltcore.logging.Level;
import org.voltcore.logging.VoltLogger;
import org.voltdb.AuthSystem.AuthUser;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Table;
import org.voltdb.client.BatchTimeoutOverrideType;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcedureCallback;
import org.voltdb.utils.MiscUtils;

/**
 * This class packs the parameters and dispatches the transactions.
 * Make sure responses over network thread does not touch this class.
 */
public class InternalConnectionHandler {
    final static String DEFAULT_INTERNAL_ADAPTER_NAME = "+!_InternalAdapter_!+";

    public final static long SUPPRESS_INTERVAL = 60;
    private static final VoltLogger m_logger = new VoltLogger("InternalConnectionHandler");

    // Atomically allows the catalog reference to change between access
    private final AtomicLong m_failedCount = new AtomicLong();
    private final AtomicLong m_submitSuccessCount = new AtomicLong();
    private final AtomicInteger m_backpressureIndication = new AtomicInteger(0);
    private final InternalClientResponseAdapter m_adapter;
    private final ClientInterfaceHandleManager m_cihm;

    public InternalConnectionHandler(InternalClientResponseAdapter adapter, ClientInterfaceHandleManager cihm) {
        m_adapter = adapter;
        m_cihm = cihm;
    }

    /**
     * Returns true if a table with the given name exists in the server catalog.
     */
    public boolean hasTable(String name) {
        Table table = getCatalogContext().tables.get(name);
        return (table!=null);
    }

    public ClientInterfaceHandleManager getClientInterfaceHandleManager() {
        return m_cihm;
    }

    public class NullCallback implements ProcedureCallback {
        @Override
        public void clientCallback(ClientResponse response) throws Exception {
        }
    }

    public boolean callProcedure(InternalConnectionContext caller, InternalConnectionStatsCollector statsCollector, String proc, Object... fieldList) {
        return callProcedure(caller, statsCollector, new NullCallback(), proc, fieldList);
    }

    private CatalogContext getCatalogContext() {
        return VoltDB.instance().getCatalogContext();
    }

    public boolean callProcedure(
            AuthUser user,
            boolean isAdmin,
            int timeout,
            ProcedureCallback cb,
            String procName,
            Object...args)
    {

        Procedure catProc = InvocationDispatcher.getProcedureFromName(procName, getCatalogContext());
        if (catProc == null) {
            String fmt = "Cannot invoke procedure %s. Procedure not found.";
            m_logger.rateLimitedLog(SUPPRESS_INTERVAL, Level.ERROR, null, fmt, procName);
            m_failedCount.incrementAndGet();
            return false;
        }

        //Indicate backpressure or not.
        boolean b = hasBackPressure();
        if (b) {
            applyBackPressure();
        }

        StoredProcedureInvocation task = new StoredProcedureInvocation();
        task.setProcName(procName);
        task.setParams(args);

        try {
            task = MiscUtils.roundTripForCL(task);
            task.setClientHandle(m_adapter.connectionId());
        } catch (Exception e) {
            String fmt = "Cannot invoke procedure %s. failed to create task.";
            m_logger.rateLimitedLog(SUPPRESS_INTERVAL, Level.ERROR, null, fmt, procName);
            m_failedCount.incrementAndGet();
            return false;
        }

        if (timeout != BatchTimeoutOverrideType.NO_TIMEOUT) {
            task.setBatchTimeout(timeout);
        }

        int partition = -1;
        try {
            partition = InvocationDispatcher.getPartitionForProcedure(catProc, task);
        } catch (Exception e) {
            String fmt = "Can not invoke procedure %s. Partition not found.";
            m_logger.rateLimitedLog(SUPPRESS_INTERVAL, Level.ERROR, e, fmt, procName);
            m_failedCount.incrementAndGet();
            return false;
        }

        InternalAdapterTaskAttributes kattrs = new InternalAdapterTaskAttributes(
                DEFAULT_INTERNAL_ADAPTER_NAME, isAdmin, m_adapter.connectionId());

        if (!m_adapter.createTransaction(kattrs, procName, catProc, cb, null, task, user, partition, System.nanoTime())) {
            m_failedCount.incrementAndGet();
            return false;
        }
        m_submitSuccessCount.incrementAndGet();
        return true;
    }

    // Use backPressureTimeout value <= 0  for no back pressure timeout
    public boolean callProcedure(InternalConnectionContext caller, InternalConnectionStatsCollector statsCollector,
            ProcedureCallback procCallback, String proc, Object... fieldList) {
        Procedure catProc = InvocationDispatcher.getProcedureFromName(proc, getCatalogContext());
        if (catProc == null) {
            String fmt = "Cannot invoke procedure %s from streaming interface %s. Procedure not found.";
            m_logger.rateLimitedLog(SUPPRESS_INTERVAL, Level.ERROR, null, fmt, proc, caller);
            m_failedCount.incrementAndGet();
            return false;
        }

        //Indicate backpressure or not.
        boolean b = hasBackPressure();
        caller.setBackPressure(b);
        if (b) {
            applyBackPressure();
        }

        StoredProcedureInvocation task = new StoredProcedureInvocation();

        task.setProcName(proc);
        task.setParams(fieldList);
        try {
            task = MiscUtils.roundTripForCL(task);
            task.setClientHandle(m_adapter.connectionId());
        } catch (Exception e) {
            String fmt = "Cannot invoke procedure %s from streaming interface %s. failed to create task.";
            m_logger.rateLimitedLog(SUPPRESS_INTERVAL, Level.ERROR, null, fmt, proc, caller);
            m_failedCount.incrementAndGet();
            return false;
        }
        int partition = -1;
        try {
            partition = InvocationDispatcher.getPartitionForProcedure(catProc, task);
        } catch (Exception e) {
            String fmt = "Can not invoke procedure %s from streaming interface %s. Partition not found.";
            m_logger.rateLimitedLog(SUPPRESS_INTERVAL, Level.ERROR, e, fmt, proc, caller);
            m_failedCount.incrementAndGet();
            return false;
        }

        InternalAdapterTaskAttributes kattrs = new InternalAdapterTaskAttributes(caller,  m_adapter.connectionId());

        final AuthUser user = getCatalogContext().authSystem.getImporterUser();

        if (!m_adapter.createTransaction(kattrs, proc, catProc, procCallback, statsCollector, task, user, partition, System.nanoTime())) {
            m_failedCount.incrementAndGet();
            return false;
        }
        m_submitSuccessCount.incrementAndGet();
        return true;
    }

    private boolean hasBackPressure() {
        final boolean b = m_adapter.hasBackPressure();
        int prev = m_backpressureIndication.get();
        int delta = b ? 1 : -(prev > 1 ? prev >> 1 : 1);
        int next = prev + delta;
        while (next >= 0 && next <= 8 && !m_backpressureIndication.compareAndSet(prev, next)) {
            prev = m_backpressureIndication.get();
            delta = b ? 1 : -(prev > 1 ? prev >> 1 : 1);
            next = prev + delta;
        }
        return b;
    }

    private void applyBackPressure() {
        final int count = m_backpressureIndication.get();
        if (count > 0) {
            try { // increase sleep time exponentially to a max of 256ms
                if (count > 8) {
                    Thread.sleep(256);
                } else {
                    Thread.sleep(1<<count);
                }
            } catch(InterruptedException e) {
                if (m_logger.isDebugEnabled()) {
                    m_logger.debug("Sleep for back pressure interrupted", e);
                }
            }
        }
    }
}
