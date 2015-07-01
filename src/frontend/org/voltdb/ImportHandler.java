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

import org.voltcore.logging.VoltLogger;
import org.voltcore.utils.CoreUtils;
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
    private final InternalConnectionHandler m_internalConnectionHandler;
    private boolean m_stopped = false;

    // The real handler gets created for each importer.
    public ImportHandler(InternalConnectionHandler internalConnectionHandler,
                         ImportContext importContext,
                         CatalogContext catContext) {
        m_internalConnectionHandler = internalConnectionHandler;

        //Need 2 threads one for data processing and one for stop.
        m_es = CoreUtils.getListeningExecutorService("ImportHandler - " + importContext.getName(), 2);
        m_importContext = importContext;
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
        return m_internalConnectionHandler.hasTable(name);
    }

    public boolean callProcedure(ImportContext ic, String proc, Object... fieldList) {
        if (!m_stopped) {
            return m_internalConnectionHandler.callProcedure(ic.getBackpressureTimeout(), proc, fieldList);
        } else {
            m_logger.warn("Importer is in stopped state. Cannot execute procedures");
            return false;
        }
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
}
