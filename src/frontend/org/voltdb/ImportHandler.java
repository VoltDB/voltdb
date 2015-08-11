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

import com.google_voltpatches.common.base.Throwables;
import static org.voltdb.ClientInterface.getPartitionForProcedure;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.voltcore.logging.VoltLogger;
import org.voltcore.utils.CoreUtils;
import org.voltcore.utils.DBBPool;
import org.voltcore.utils.DBBPool.BBContainer;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Table;
import org.voltdb.client.ProcedureInvocationType;
import org.voltdb.importer.ImportClientResponseAdapter;
import org.voltdb.importer.ImportContext;

import com.google_voltpatches.common.util.concurrent.ListeningExecutorService;
import java.util.concurrent.ExecutionException;
import org.voltcore.logging.Level;
import org.voltcore.utils.EstTime;
import org.voltcore.utils.RateLimitedLogger;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcedureCallback;

/**
 * This class packs the parameters and dispatches the transactions.
 * Make sure responses over network thread does not touch this class.
 * @author akhanzode
 */
public class ImportHandler {

    private static final VoltLogger m_logger = new VoltLogger("IMPORT");

    // Atomically allows the catalog reference to change between access
    private final CatalogContext m_catalogContext;
    private final AtomicLong m_failedCount = new AtomicLong();
    private final AtomicLong m_submitSuccessCount = new AtomicLong();
    private final ListeningExecutorService m_es;
    private final ImportContext m_importContext;

    private static final ImportClientResponseAdapter m_adapter = new ImportClientResponseAdapter(ClientInterface.IMPORTER_CID, "Importer");

    public final static long SUPPRESS_INTERVAL = 120;

    // The real handler gets created for each importer.
    public ImportHandler(ImportContext importContext, CatalogContext catContext) {
        m_catalogContext = catContext;

        //Need 2 threads one for data processing and one for stop.
        m_es = CoreUtils.getListeningExecutorService("ImportHandler - " + importContext.getName(), 2);
        m_importContext = importContext;
        VoltDB.instance().getClientInterface().bindAdapter(m_adapter, null);
    }

    /**
     * Submit ready for data
     */
    public void readyForData() {
        m_es.submit(new Runnable() {
            @Override
            public void run() {
                m_logger.info("Importer ready importing data for: " + m_importContext.getName());
                try {
                    m_importContext.readyForData();
                } catch (Throwable t) {
                    m_logger.error("ImportContext stopped with following exception", t);
                }
                m_logger.info("Importer finished importing data for: " + m_importContext.getName());
            }
        });
    }

    public void stop() {
        try {
            m_es.submit(new Runnable() {

                @Override
                public void run() {
                    try {
                        //Stop the context first so no more work is submitted.
                        m_importContext.stop();
                    } catch (Exception ex) {
                        Throwables.propagate(ex);
                    }
                    m_logger.info("Importer stopped: " + m_importContext.getName());
                }
            }).get();
        } catch (InterruptedException ex) {
            m_logger.warn("Failed to successfully stop import context for: " + m_importContext.getName(), ex);
        } catch (ExecutionException ex) {
            m_logger.warn("Failed to successfully stop import context for: " + m_importContext.getName(), ex);
        }
        try {
            m_es.shutdown();
            m_es.awaitTermination(1, TimeUnit.DAYS);
        } catch (Exception ex) {
            m_logger.warn("Importer did not stop gracefully.", ex);
        }
    }

    //Allocate and pool similar row sizes will reuse the buffers.
    private BBContainer getBuffer(int sz) {
        return DBBPool.allocateDirectAndPool(sz);
    }

    /**
     * Returns true if a table with the given name exists in the server catalog.
     */
    public boolean hasTable(String name) {
        Table table = m_catalogContext.tables.get(name);
        return (table!=null);
    }

    public class NullCallback implements ProcedureCallback {
        @Override
        public void clientCallback(ClientResponse response) throws Exception {
        }
    }

    public boolean callProcedure(ImportContext ic, String proc, Object... fieldList) {
        return callProcedure(ic, new NullCallback(), proc, fieldList);
    }

    public boolean callProcedure(ImportContext ic, ProcedureCallback cb, String proc, Object... fieldList) {
        Procedure catProc = m_catalogContext.procedures.get(proc);
        if (catProc == null) {
            catProc = m_catalogContext.m_defaultProcs.checkForDefaultProcedure(proc);
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
                String fmt = "Can not invoke procedure %s from streaming interface %s procedure not found.";
                error(null, fmt, proc, ic.getName());
                m_failedCount.incrementAndGet();
                return false;
            }
        }

        //Indicate backpressure or not.
        boolean b = m_adapter.hasBackPressure();
        m_importContext.hasBackPressure(b);
        if (b) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException ex) {
            }
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
                String fmt = "Can not invoke procedure %s from streaming interface %s partition not found.";
                error(null, fmt, proc, ic.getName());
                m_failedCount.incrementAndGet();
                tcont.discard();
                return false;
            }
        }

        boolean success;
        success = m_adapter.createTransaction(m_importContext, proc, catProc, cb, task, tcont, partition, nowNanos);
        if (!success) {
            tcont.discard();
            m_failedCount.incrementAndGet();
        } else {
            m_submitSuccessCount.incrementAndGet();
        }
        return success;
    }

    //Do rate limited logging for messages.
    private void rateLimitedLog(Level level, Throwable cause, String format, Object...args) {
        RateLimitedLogger.tryLogForMessage(
                EstTime.currentTimeMillis(),
                SUPPRESS_INTERVAL, TimeUnit.SECONDS,
                m_logger, level,
                cause, format, args
                );
    }

    /**
     * Log info message
     * @param message
     */
    public void info(String message) {
        m_logger.info(message);
    }

    /**
     * Log error message
     * @param message
     */
    public void error(String message) {
        m_logger.error(message);
    }

    /**
     * Log warn message
     * @param message
     */
    public void warn(String message) {
        m_logger.warn(message);
    }

    public boolean isDebugEnabled() {
        return m_logger.isDebugEnabled();
    }

    public boolean isTraceEnabled() {
        return m_logger.isTraceEnabled();
    }

    public boolean isInfoEnabled() {
        return m_logger.isInfoEnabled();
    }

    /**
     * Log debug message
     * @param message
     */
    public void debug(String message) {
        m_logger.debug(message);
    }

    /**
     * Log error message
     * @param message
     */
    public void error(String message, Throwable t) {
        m_logger.error(message, t);
    }

    public void error(Throwable t, String format, Object...args) {
        rateLimitedLog(Level.ERROR, t, format, args);
    }

    public void warn(Throwable t, String format, Object...args) {
        rateLimitedLog(Level.WARN, t, format, args);
    }

}
