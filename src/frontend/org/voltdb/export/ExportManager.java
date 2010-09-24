/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB Inc.
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

package org.voltdb.export;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.TreeSet;

import org.voltdb.CatalogContext;
import org.voltdb.catalog.Cluster;
import org.voltdb.catalog.Connector;
import org.voltdb.catalog.ConnectorTableInfo;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Table;
import org.voltdb.dtxn.SiteTracker;
import org.voltdb.export.processors.RawProcessor.ExportInternalMessage;
import org.voltdb.logging.Level;
import org.voltdb.logging.VoltLogger;
import org.voltdb.network.InputHandler;
import org.voltdb.utils.LogKeys;

/**
 * Bridges the connection to an OLAP system and the buffers passed
 * between the OLAP connection and the execution engine. Each processor
 * implements ExportDataProcessor interface. The processors are passed one
 * or more ExportDataSources. The sources map, currently, 1:1 with Export
 * enabled tables. The ExportDataSource has poll() and ack() methods that
 * processors may use to pull and acknowledge as processed, EE Export data.
 * Data passed to processors is wrapped in ExportDataBlocks which in turn
 * wrap a BBContainer.
 *
 * Processors are loaded by reflection based on configuration in project.xml.
 */
public class ExportManager
{
    /**
     * Processors also log using this facility.
     */
    private static final VoltLogger exportLog = new VoltLogger("EXPORT");

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
    ArrayDeque<ExportDataProcessor> m_processors = new ArrayDeque<ExportDataProcessor>();

    /**
     * Existing datasources that have been advertised to processors
     */
    TreeSet<ExportDataSource> m_dataSources = new TreeSet<ExportDataSource>();

    /** Obtain the global ExportManager via its instance() method */
    private static ExportManager m_self;
    private final int m_hostId;

    /**
     * Construct ExportManager using catalog.
     * @param myHostId
     * @param catalogContext
     * @throws ExportManager.SetupException
     */
    public static synchronized void initialize(int myHostId, CatalogContext catalogContext)
        throws ExportManager.SetupException
    {
        ExportManager tmp = new ExportManager(myHostId, catalogContext);
        m_self = tmp;
    }

    /**
     * Get the global instance of the ExportManager.
     * @return The global single instance of the ExportManager.
     */
    public static ExportManager instance() {
        assert (m_self != null);
        return m_self;
    }

    /**
     * Read the catalog to setup manager and loader(s)
     * @param siteTracker
     */
    private ExportManager(int myHostId, CatalogContext catalogContext)
    throws ExportManager.SetupException
    {
        m_hostId = myHostId;

        final Cluster cluster = catalogContext.catalog.getClusters().get("cluster");
        final Database db = cluster.getDatabases().get("database");
        final Connector conn= db.getConnectors().get("0");

        if (conn == null) {
            return;
        }

        if (conn.getEnabled() == false) {
            exportLog.info("Export is disabled by user configuration.");
            return;
        }

        final String elloader = conn.getLoaderclass();
        try {
            exportLog.info("Creating connector " + elloader);
            ExportDataProcessor newProcessor = null;
            final Class<?> loaderClass = Class.forName(elloader);
            newProcessor = (ExportDataProcessor)loaderClass.newInstance();
            newProcessor.addLogger(exportLog);
            addTableInfos(catalogContext, conn, newProcessor);
            newProcessor.readyForData();
            m_processors.add(newProcessor);
        }
        catch (final ClassNotFoundException e) {
            exportLog.l7dlog( Level.ERROR, LogKeys.export_ExportManager_NoLoaderExtensions.name(), e);
            throw new ExportManager.SetupException(e.getMessage());
        }
        catch (final Exception e) {
            throw new ExportManager.SetupException(e.getMessage());
        }
    }

    public void updateCatalog(CatalogContext catalogContext)
    {
        final Cluster cluster = catalogContext.catalog.getClusters().get("cluster");
        final Database db = cluster.getDatabases().get("database");
        final Connector conn= db.getConnectors().get("0");

        for (ExportDataProcessor processor : m_processors) {
            addTableInfos(catalogContext, conn, processor);
        }
    }

    private void addTableInfos(CatalogContext catalogContext,
            final Connector conn, ExportDataProcessor processor)
    {
        Iterator<ConnectorTableInfo> tableInfoIt = conn.getTableinfo().iterator();
        while (tableInfoIt.hasNext()) {
            ConnectorTableInfo next = tableInfoIt.next();
            Table table = next.getTable();
            addDataSources(processor, table, m_hostId, catalogContext);
        }
    }


    // silly helper to add datasources for a table catalog object
    private void addDataSources(ExportDataProcessor newProcessor,
            Table table, int hostId, CatalogContext catalogContext)
    {
        SiteTracker siteTracker = catalogContext.siteTracker;
        ArrayList<Integer> sites = siteTracker.getLiveExecutionSitesForHost(hostId);

        // make the catalog versioned table id. there is coordinated logic
        // common/CatalogDelegate.cpp
        long tmp = (long)(catalogContext.catalogVersion) << 32L;
        long delegateId = tmp + table.getRelativeIndex();

        for (Integer site : sites) {

            ExportDataSource exportDataSource = new ExportDataSource("database",
                              table.getTypeName(),
                              table.getIsreplicated(),
                              siteTracker.getPartitionForSite(site),
                              site,
                              delegateId,
                              table.getColumns());

            if (!m_dataSources.contains(exportDataSource)) {
                m_dataSources.add(exportDataSource);
                newProcessor.addDataSource(exportDataSource);
            }
        }
    }

    /**
     * Add a message to the processor "mailbox".
     * @param mbp
     */
    public void queueMessage(ExportInternalMessage mbp) {
        // TODO: supporting multiple processors requires slicing the
        // data buffer so each processor gets a readonly buffer.
        m_processors.getFirst().queueMessage(mbp);
    }

    public void shutdown() {
        for (ExportDataProcessor p : m_processors) {
            p.shutdown();
        }

    }

    /**
     * Factory for input handlers
     * @return InputHandler for new client connection
     */
    public InputHandler createInputHandler(String service) {
        InputHandler handler = null;
        for (ExportDataProcessor p : m_processors) {
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
        for (ExportDataProcessor p : m_processors) {
            if (p.isConnectorForService(service)) {
                return p.getClass().getCanonicalName();
            }
        }
        return null;
    }
}
