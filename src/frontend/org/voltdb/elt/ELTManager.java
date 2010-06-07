/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB L.L.C.
 *
 * VoltDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * VoltDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.voltdb.elt;

import java.util.*;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.voltdb.catalog.*;
import org.voltdb.dtxn.SiteTracker;
import org.voltdb.elt.processors.RawProcessor.ELTInternalMessage;
import org.voltdb.network.InputHandler;
import org.voltdb.utils.*;

/**
 * Bridges the connection to an OLAP system and the buffers passed
 * between the OLAP connection and the execution engine. Each processor
 * implements ELTDataProcessor interface. The processors are passed one
 * or more ELTDataSources. The sources map, currently, 1:1 with ELT
 * enabled tables. The ELTDataSource has poll() and ack() methods that
 * processors may use to pull and acknowledge as processed, EE ELT data.
 * Data passed to processors is wrapped in ELTDataBlocks which in turn
 * wrap a BBContainer.
 *
 * Processors are loaded by reflection based on configuration in project.xml.
 */
public class ELTManager
{
    /**
     * Processors also log using this facility.
     */
    private static final Logger eltLog =
        Logger.getLogger("ELT", VoltLoggerFactory.instance());

    /**
     * Thrown if the initial setup of the loader fails
     */
    public static class SetupException extends Exception {
        private static final long serialVersionUID = 1L;
        private final String m_msg;
        SetupException(final String msg) {
            m_msg = msg;
        }
        @Override
        public String getMessage() {
            return m_msg;
        }
    }

    /**
     * Connections OLAP loaders. Currently at most one loader allowed.
     * Supporting multiple loaders mainly involves reference counting
     * the EE data blocks and bookkeeping ACKs from processors.
     */
    ArrayDeque<ELTDataProcessor> m_processors = new ArrayDeque<ELTDataProcessor>();


    /** Obtain the global ELTManager via its instance() method */
    private static ELTManager m_self;

    /**
     * Construct ELTManager using catalog.
     * @param myHostId
     * @param catalog
     * @param siteTracker
     * @throws ELTManager.SetupException
     */
    public static synchronized void initialize(int myHostId,
                                               final Catalog catalog,
                                               SiteTracker siteTracker)
    throws ELTManager.SetupException
    {
        ELTManager tmp = new ELTManager(myHostId, catalog, siteTracker);
        m_self = tmp;
    }

    /**
     * Get the global instance of the ELTManager.
     * @return The global single instance of the ELTManager.
     */
    public static ELTManager instance() {
        assert (m_self != null);
        return m_self;
    }

    /** Read the catalog to setup manager and loader(s)
     * @param myHostId
     * @param siteTracker */
    private ELTManager(int myHostId, final Catalog catalog,
                       SiteTracker siteTracker)
    throws ELTManager.SetupException
    {
        final Cluster cluster = catalog.getClusters().get("cluster");
        final Database db = cluster.getDatabases().get("database");
        final Connector conn= db.getConnectors().get("0");

        if (conn == null) {
            return;
        }

        if (conn.getEnabled() == false) {
            eltLog.info("Export is disabled by user configuration.");
            return;
        }

        final String elloader = conn.getLoaderclass();
        try {
            eltLog.info("Creating connector " + elloader);
            ELTDataProcessor newProcessor = null;
            final Class<?> loaderClass = Class.forName(elloader);
            newProcessor = (ELTDataProcessor)loaderClass.newInstance();
            newProcessor.addLogger(eltLog);

            Iterator<ConnectorTableInfo> tableInfoIt = conn.getTableinfo().iterator();
            while (tableInfoIt.hasNext()) {
                ConnectorTableInfo next = tableInfoIt.next();
                Table table = next.getTable();
                addDataSources(newProcessor, table, myHostId, siteTracker);
            }
            newProcessor.readyForData();
            m_processors.add(newProcessor);
        }
        catch (final ClassNotFoundException e) {
            eltLog.l7dlog( Level.ERROR, LogKeys.elt_ELTManager_NoLoaderExtensions.name(), e);
            throw new ELTManager.SetupException(e.getMessage());
        }
        catch (final Exception e) {
            throw new ELTManager.SetupException(e.getMessage());
        }
    }

    // silly helper to add datasources for a table catalog object
    private void addDataSources(ELTDataProcessor newProcessor,
            Table table, int hostId, SiteTracker siteTracker)
    {
        ArrayList<Integer> sites = siteTracker.getLiveExecutionSitesForHost(hostId);
        for (Integer site : sites) {
            newProcessor.addDataSource(
                 new ELTDataSource("database",
                                   table.getTypeName(),
                                   table.getIsreplicated(),
                                   siteTracker.getPartitionForSite(site),
                                   site,
                                   table.getRelativeIndex(),
                                   table.getColumns())
            );
        }
    }

    /**
     * Add a message to the processor "mailbox".
     * @param mbp
     */
    public void queueMessage(ELTInternalMessage mbp) {
        // TODO: supporting multiple processors requires slicing the
        // data buffer so each processor gets a readonly buffer.
        m_processors.getFirst().queueMessage(mbp);
    }

    public void shutdown() {
        for (ELTDataProcessor p : m_processors) {
            p.shutdown();
        }

    }

    /**
     * Factory for input handlers
     * @return InputHandler for new client connection
     */
    public InputHandler createInputHandler(String service) {
        InputHandler handler = null;
        for (ELTDataProcessor p : m_processors) {
            handler = p.createInputHandler(service);
            if (handler != null) {
                return handler;
            }
        }
        return null;
    }


    /**
     * Map service strings to connector class names
     * @param service
     * @return classname responsible for service
     */
    public String getConnectorForService(String service) {
        for (ELTDataProcessor p : m_processors) {
            if (p.isConnectorForService(service)) {
                return p.getClass().getCanonicalName();
            }
        }
        return null;
    }
}
