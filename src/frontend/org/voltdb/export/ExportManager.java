/* This file is part of VoltDB.
 * Copyright (C) 2008-2011 VoltDB Inc.
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

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicReference;

import org.voltdb.CatalogContext;
import org.voltdb.TransactionIdManager;
import org.voltdb.VoltDB;
import org.voltdb.catalog.Cluster;
import org.voltdb.catalog.Connector;
import org.voltdb.catalog.Database;
import org.voltdb.logging.Level;
import org.voltdb.logging.VoltLogger;
import org.voltdb.network.InputHandler;
import org.voltdb.utils.DBBPool;
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
    public static final int DEFAULT_WINDOW_MS = 60 * 5 * 1000; // 5 minutes


    /**
     * Processors also log using this facility.
     */
    private static final VoltLogger exportLog = new VoltLogger("EXPORT");

    public final ExportGenerationDirectory m_windowDirectory;

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
    AtomicReference<ExportDataProcessor> m_processor = new AtomicReference<ExportDataProcessor>();

    /** Obtain the global ExportManager via its instance() method */
    private static ExportManager m_self;
    private final int m_hostId;

    private String m_loaderClass;

    final Runnable m_onGenerationDrained = new Runnable() {
        @Override
        public void run() {
            /*
             * Do all the work to switch to a new generation in the thread for the processor
             * of the old generation
             */
            ExportDataProcessor proc = m_processor.get();
            if (proc == null) {
                VoltDB.crashVoltDB();
            }
            proc.queueWork(new Runnable() {
                @Override
                public void run() {
                    ExportGeneration head = m_windowDirectory.popWindow();

                    exportLog.info("Creating connector " + m_loaderClass);
                    ExportDataProcessor newProcessor = null;
                    try {
                        final Class<?> loaderClass = Class.forName(m_loaderClass);
                        newProcessor = (ExportDataProcessor)loaderClass.newInstance();
                        newProcessor.addLogger(exportLog);
                        newProcessor.setExportGeneration(m_windowDirectory.peekWindow());
                        newProcessor.readyForData();
                    } catch (ClassNotFoundException e) {} catch (InstantiationException e) {
                        exportLog.error(e);
                    } catch (IllegalAccessException e) {
                        exportLog.error(e);
                    }

                    m_processor.getAndSet(newProcessor).shutdown();
                    try {
                        exportLog.info("Finished draining generation " + head.m_timestamp);
                        head.closeAndDelete();
                    } catch (IOException e) {
                        exportLog.error(e);
                    }
                }
            });
        }
    };

    /**
     * Construct ExportManager using catalog.
     * @param myHostId
     * @param catalogContext
     * @throws ExportManager.SetupException
     */
    public static synchronized void initialize(int myHostId, CatalogContext catalogContext, boolean isRejoin)
    throws ExportManager.SetupException
    {
        ExportGenerationDirectory exportWindowDirectory;
        try {
            exportWindowDirectory = new ExportGenerationDirectory(isRejoin, catalogContext);
        } catch (IOException e) {
            throw new ExportManager.SetupException(e.getMessage());
        }

        ExportManager tmp = new ExportManager(myHostId, catalogContext, exportWindowDirectory);
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

    public static void setInstanceForTest(ExportManager self) {
        m_self = self;
    }


    /**
     * Read the catalog to setup manager and loader(s)
     * @param siteTracker
     */
    private ExportManager(int myHostId, CatalogContext catalogContext, ExportGenerationDirectory windowDirectory)
    throws ExportManager.SetupException
    {
        m_hostId = myHostId;
        m_windowDirectory = windowDirectory;

        final Cluster cluster = catalogContext.catalog.getClusters().get("cluster");
        final Database db = cluster.getDatabases().get("database");
        final Connector conn= db.getConnectors().get("0");

        if (conn == null) {
            exportLog.info("System is not using any export functionality.");
            return;
        }

        if (conn.getEnabled() == false) {
            exportLog.info("Export is disabled by user configuration.");
            return;
        }

        exportLog.info(String.format("Export is enabled and can overflow to %s.", cluster.getExportoverflow()));

        m_loaderClass = conn.getLoaderclass();

        try {
            exportLog.info("Creating connector " + m_loaderClass);
            ExportDataProcessor newProcessor = null;
            final Class<?> loaderClass = Class.forName(m_loaderClass);
            newProcessor = (ExportDataProcessor)loaderClass.newInstance();
            m_processor.set(newProcessor);
            newProcessor.addLogger(exportLog);

            // add the disk generation(s) to the directory
            m_windowDirectory.initializePersistedWindows(m_onGenerationDrained);

            // add the current in-memory generation to the directory
            File exportOverflowDirectory = new File(catalogContext.cluster.getExportoverflow());
            ExportGeneration currentGeneration =
                new ExportGeneration( catalogContext.m_transactionId, m_onGenerationDrained, exportOverflowDirectory);
            currentGeneration.initializeGenerationFromCatalog(catalogContext, conn, m_hostId);
            m_windowDirectory.pushWindow(catalogContext.m_transactionId, currentGeneration);

            // once windows are loaded, the processor can be started
            m_processor.get().setExportGeneration(m_windowDirectory.peekWindow());
            m_processor.get().readyForData();
        }
        catch (final ClassNotFoundException e) {
            exportLog.l7dlog( Level.ERROR, LogKeys.export_ExportManager_NoLoaderExtensions.name(), e);
            throw new ExportManager.SetupException(e.getMessage());
        }
        catch (final Exception e) {
            throw new ExportManager.SetupException(e.getMessage());
        }
    }



    public void notifyOfClusterTopologyChange() {
        exportLog.info("Attempting to boot export client due to rejoin or other cluster topology change");
        if (m_loaderClass == null) {
            return;
        }
        ExportDataProcessor proc = m_processor.get();
        while (proc == null) {
            Thread.yield();
            proc = m_processor.get();
        }
        proc.bootClient();
    }

    public void updateCatalog(CatalogContext catalogContext)
    {
        final Cluster cluster = catalogContext.catalog.getClusters().get("cluster");
        final Database db = cluster.getDatabases().get("database");
        final Connector conn= db.getConnectors().get("0");
        if (conn == null) {
            m_loaderClass = null;
            return;
        }

        m_loaderClass = conn.getLoaderclass();

        File exportOverflowDirectory = new File(catalogContext.cluster.getExportoverflow());

        ExportGeneration newGeneration = null;
        try {
            newGeneration = new ExportGeneration(
                    catalogContext.m_transactionId,
                    m_onGenerationDrained,
                    exportOverflowDirectory);
        } catch (IOException e1) {
            exportLog.error(e1);
            VoltDB.crashVoltDB();
        }
        newGeneration.initializeGenerationFromCatalog(catalogContext, conn, m_hostId);
        m_windowDirectory.pushWindow(catalogContext.m_transactionId, newGeneration);
    }

    public void shutdown() {
        ExportDataProcessor proc = m_processor.getAndSet(null);
        if (proc != null) {
            proc.shutdown();
        }

        m_windowDirectory.closeAllWindows();
    }

    /**
     * Factory for input handlers
     * @return InputHandler for new client connection
     */
    public InputHandler createInputHandler(String service, boolean isAdminPort)
    {
        ExportDataProcessor proc = m_processor.get();
        if (proc != null) {
            return proc.createInputHandler(service, isAdminPort);
        }
        return null;
    }

    // extract generation id from a stream name.
    private long getGenerationIdFromStreamName(String streamname) {
        try {
            String[] parts = streamname.split("-", 3);
            if (parts.length != 3) {
                return -1L;
            }
            return Long.parseLong(parts[0]);
        } catch (NumberFormatException e) {
            return -1L;
        }
    }

    // extract partition id from a stream name
    private int getPartitionIdFromStreamName(String streamname) {
        try {
            String[] parts = streamname.split("-", 3);
            if (parts.length != 3) {
                return -1;
            }
            return Integer.parseInt(parts[1]);
        } catch (NumberFormatException e) {
            return -1;
        }
    }

    // extract signature from a stream name
    private String getSignatureFromStreamName(String streamname) {
        String[] parts = streamname.split("-", 3);
        if (parts.length != 3) {
            return "";
        }
        return parts[2];
    }

    /**
     * Create ExportDataStreams for the specified stream name.
     * streamname: generationid-partitionid-signature
     */
    public InputHandler createExportClientStream(String streamname)
    {
        exportLog.info("Creating export data stream for " + streamname);
        long generationId = getGenerationIdFromStreamName(streamname);
        int partitionId = getPartitionIdFromStreamName(streamname);
        String signature = getSignatureFromStreamName(streamname);

        ExportGeneration gen = m_windowDirectory.getWindow(generationId);
        if (gen == null) {
            exportLog.error("Rejecting export data stream. Generation " + generationId + " does not exist.");
            return null;
        }

        StreamBlockQueue sbq =
            gen.checkoutExportStreamBlockQueue(partitionId, signature);

        if (sbq == null) {
            exportLog.error("Rejecting export data stream. Stream " +  signature +
                " busy or not present in generation " + generationId);
            return null;
        }

        return new ExportClientStream(streamname, sbq);
    }

    public InputHandler createExportListingService() {
        return new ExportListingService();
    }


    /**
     * Map service strings to connector class names
     * @param service
     * @return classname responsible for service
     */
    public String getConnectorForService(String service) {
        ExportDataProcessor proc = m_processor.get();
        if (proc != null && proc.isConnectorForService(service)) {
            return proc.getClass().getCanonicalName();
        }
        return null;
    }

    public static long getQueuedExportBytes(int partitionId, String signature) {
        ExportManager instance = instance();
        try {
            long exportBytes = instance.m_windowDirectory.estimateQueuedBytes(partitionId, signature);
            return exportBytes;
        } catch (Exception e) {
            //Don't let anything take down the execution site thread
            exportLog.error(e);
        }
        return 0;
    }

    /*
     * This method pulls double duty as a means of pushing export buffers
     * and "syncing" export data to disk. Syncing doesn't imply fsync, it just means
     * writing the data to a file instead of keeping it all in memory.
     * End of stream indicates that no more data is coming from this source (signature)
     * for this generation.
     */
    public static void pushExportBuffer(
            long generationId,
            int partitionId,
            String signature,
            long uso,
            long txnId,
            long bufferPtr,
            ByteBuffer buffer,
            boolean sync,
            boolean endOfStream)
    {
        // The EE sends the right export generation id. If this is the
        // first time the generation has been seen, make a new one.

        ExportManager instance = instance();
        ExportGeneration generation = null;
        try {
            if ((generation = instance.m_windowDirectory.getWindow(generationId)) == null) {
                generation = new ExportGeneration(
                    generationId,
                    instance.m_onGenerationDrained,
                    instance.m_windowDirectory.m_exportOverflowDirectory);

                // BUG: Guess that the current catalog context is the right one.
                // this is a false assumption - will have to fix this.
                CatalogContext catalogContext = VoltDB.instance().getCatalogContext();
                Connector conn = catalogContext.catalog.getClusters().get("cluster").
                    getDatabases().get("database").
                    getConnectors().get("0");
                generation.initializeGenerationFromCatalog(catalogContext, conn, instance.m_hostId);
                instance.m_windowDirectory.pushWindow(generationId, generation);
            }

            generation.pushExportBuffer(partitionId, signature, uso, bufferPtr, buffer, sync, endOfStream);
        } catch (Exception e) {
            //Don't let anything take down the execution site thread
            exportLog.error(e);
        }
    }

    public void truncateExportToTxnId(long snapshotTxnId) {
        exportLog.info("Truncating export data after txnId " + snapshotTxnId);
        m_windowDirectory.truncateExportToTxnId(snapshotTxnId);
    }

}
