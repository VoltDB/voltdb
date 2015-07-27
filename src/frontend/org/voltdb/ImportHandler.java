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

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.voltcore.logging.Level;
import org.voltcore.logging.VoltLogger;
import org.voltcore.utils.CoreUtils;
import org.voltcore.utils.EstTime;
import org.voltcore.utils.RateLimitedLogger;
import org.voltdb.importer.ImportContext;

import com.google_voltpatches.common.util.concurrent.ListeningExecutorService;

/**
 * This class packs the parameters and dispatches the transactions.
 * Make sure responses over network thread does not touch this class.
 * @author akhanzode
 */
public class ImportHandler {

    private static final VoltLogger m_logger = new VoltLogger("IMPORT");

    private final ListeningExecutorService m_es;
    private final ImportContext m_importContext;
    private final CatalogContext m_catalogContext;
    private boolean m_stopped = false;

    private static final InternalClientResponseAdapter m_adapter = new InternalClientResponseAdapter(ClientInterface.INTERNAL_CID, "Importer");
    private static final AtomicLong m_lock = new AtomicLong(0);

    private static final long MAX_PENDING_TRANSACTIONS = Integer.getInteger("IMPORTER_MAX_PENDING_TRANSACTION", 5000);
    final static long SUPPRESS_INTERVAL = 60;

    // The real handler gets created for each importer.
    public ImportHandler(ImportContext importContext,
                         CatalogContext catContext) {
        //Need 2 threads one for data processing and one for stop.
        m_es = CoreUtils.getListeningExecutorService("ImportHandler - " + importContext.getName(), 2);
        m_importContext = importContext;
        m_catalogContext = catContext;
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
        m_stopped = true;
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
            m_es.shutdown();
            m_es.awaitTermination(1, TimeUnit.DAYS);
        } catch (Exception ex) {
            m_logger.warn("Importer did not stop gracefully.", ex);
        }
    }

    /**
     * Returns true if a table with the given name exists in the server catalog.
     */
    public boolean hasTable(String name) {
        return VoltDB.instance().getClientInterface().getInternalConnectionHandler().hasTable(name);
    }

    /*
    public class NullCallback implements ProcedureCallback {
        @Override
        public void clientCallback(ClientResponse response) throws Exception {
        }
    }
    */

    public boolean callProcedure(ImportContext ic, String proc, Object... fieldList) {
        return VoltDB.instance().getClientInterface().getInternalConnectionHandler()
                .callProcedure(ic.getBackpressureTimeout(), proc, fieldList);
        //return callProcedure(ic, new NullCallback(), proc, fieldList);
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

}
