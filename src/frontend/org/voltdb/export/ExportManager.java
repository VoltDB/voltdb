/* This file is part of VoltDB.
 * Copyright (C) 2008-2012 VoltDB Inc.
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
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;

import org.voltcore.logging.Level;
import org.voltcore.logging.VoltLogger;
import org.voltcore.messaging.HostMessenger;
import org.voltcore.network.InputHandler;
import org.voltcore.utils.COWSortedMap;
import org.voltcore.utils.DBBPool;
import org.voltdb.CatalogContext;
import org.voltdb.VoltDB;
import org.voltdb.catalog.Cluster;
import org.voltdb.catalog.Connector;
import org.voltdb.catalog.ConnectorProperty;
import org.voltdb.catalog.Database;
import org.voltdb.export.processors.RawProcessor;
import org.voltdb.utils.LogKeys;
import org.voltdb.utils.VoltFile;

import com.google.common.base.Preconditions;

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

    private final COWSortedMap<Long,ExportGeneration> m_generations =
            new COWSortedMap<Long, ExportGeneration>();
    /*
     * When a generation is drained store a the id so
     * we can tell if a buffer comes late
     */
    private final CopyOnWriteArrayList<Long> m_generationGhosts =
            new CopyOnWriteArrayList<Long>();

    private final HostMessenger m_messenger;

    /**
     * Set of partition ids for which this export manager instance is master of
     */
    private final Set<Integer> m_masterOfPartitions = new HashSet<Integer>();

    /**
     * Thrown if the initial setup of the loader fails
     */
    public static class SetupException extends Exception {
        private static final long serialVersionUID = 1L;

        SetupException(final String msg) {
            super(msg);
        }

        SetupException(final Throwable cause) {
            super(cause);
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

    private volatile Properties m_processorConfig = new Properties();

    private final Runnable m_onGenerationDrained = new Runnable() {
        @Override
        public void run() {
            /*
             * Do all the work to switch to a new generation in the thread for the processor
             * of the old generation
             */
            ExportDataProcessor proc = m_processor.get();
            if (proc == null) {
                VoltDB.crashLocalVoltDB("No export data processor found", true, null);
            }
            proc.queueWork(new Runnable() {
                @Override
                public void run() {

                    ExportGeneration generation = m_generations.firstEntry().getValue();
                    ExportDataProcessor newProcessor = null;

                    synchronized (ExportManager.this) {

                        m_generationGhosts.add(m_generations.remove(m_generations.firstEntry().getKey()).m_timestamp);
                        exportLog.info("Finished draining generation " + generation.m_timestamp);

                        exportLog.info("Creating connector " + m_loaderClass);
                        try {
                            final Class<?> loaderClass = Class.forName(m_loaderClass);
                            //Make it so
                            ExportGeneration nextGeneration = m_generations.firstEntry().getValue();
                            newProcessor = (ExportDataProcessor)loaderClass.newInstance();
                            newProcessor.addLogger(exportLog);
                            newProcessor.setExportGeneration(nextGeneration);
                            newProcessor.setProcessorConfig(m_processorConfig);
                            newProcessor.readyForData();

                            if (!m_loaderClass.equals(RawProcessor.class.getName())) {
                                if (nextGeneration.isDiskBased()) {
                                    /*
                                     * Changes in partition count can make the load balancing strategy not capture
                                     * all partitions for data that was from a previously larger cluster.
                                     * For those use a naive leader election strategy that is implemented
                                     * by export generation.
                                     */
                                    nextGeneration.kickOffLeaderElection();
                                } else {
                                    /*
                                     * This strategy is the one that piggy backs on
                                     * regular partition mastership distribution to determine
                                     * who will process export data for different partitions.
                                     * We stashed away all the ones we have mastership of
                                     * in m_masterOfPartitions
                                     */
                                    for( Integer partitionId: m_masterOfPartitions) {
                                        nextGeneration.acceptMastershipTask(partitionId);
                                    }
                                }
                            }
                        } catch (ClassNotFoundException e) {} catch (InstantiationException e) {
                            exportLog.error(e);
                        } catch (IllegalAccessException e) {
                            exportLog.error(e);
                        }

                    }

                    m_processor.getAndSet(newProcessor).shutdown();
                    try {
                        generation.closeAndDelete();
                    } catch (IOException e) {
                        e.printStackTrace();
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
    public static synchronized void initialize(
            int myHostId,
            CatalogContext catalogContext,
            boolean isRejoin,
            HostMessenger messenger)
    throws ExportManager.SetupException
    {
        /*
         * If a node is rejoining it is because it crashed. Export overflow isn't crash safe so it isn't possible
         * to recover the data. Delete it instead.
         */
        if (isRejoin) {
            deleteExportOverflowData(catalogContext);
        }
        ExportManager tmp = new ExportManager(myHostId, catalogContext, messenger);
        m_self = tmp;
    }

    /**
     * Indicate to associated {@link ExportGeneration}s to become
     * masters for the given partition id
     * @param partitionId
     */
    synchronized public void acceptMastership(int partitionId) {
        if (m_loaderClass == null || m_loaderClass.equals(RawProcessor.class.getName())) return;
        Preconditions.checkArgument(
                m_masterOfPartitions.add(partitionId),
                "can't acquire mastership twice for partition id: " + partitionId
                );
        /*
         * Only the first generation will have a processor which
         * makes it safe to accept mastership.
         */
        ExportGeneration gen = m_generations.firstEntry().getValue();
        if (gen != null && !gen.isDiskBased()) {
            gen.acceptMastershipTask(partitionId);
        }
    }

    private static void deleteExportOverflowData(CatalogContext context) {
        File exportOverflowDirectory = new File(context.cluster.getExportoverflow());
        exportLog.info("Deleting export overflow data from " + exportOverflowDirectory);
        if (!exportOverflowDirectory.exists()) {
            return;
        }
        File files[] = exportOverflowDirectory.listFiles();
        if (files != null) {
            for (File f : files) {
                try {
                    VoltFile.recursivelyDelete(f);
                } catch (IOException e) {
                    VoltDB.crashLocalVoltDB(e.getMessage(), true, e);
                }
            }
        }
    }
    /**
     * Get the global instance of the ExportManager.
     * @return The global single instance of the ExportManager.
     */
    public static ExportManager instance() {
        return m_self;
    }

    public static void setInstanceForTest(ExportManager self) {
        m_self = self;
    }

    protected ExportManager() {
        m_hostId = 0;
        m_messenger = null;
    }

    /**
     * Read the catalog to setup manager and loader(s)
     * @param siteTracker
     */
    private ExportManager(int myHostId, CatalogContext catalogContext, HostMessenger messenger)
    throws ExportManager.SetupException
    {
        m_hostId = myHostId;
        m_messenger = messenger;
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

        updateProcessorConfig(conn);

        exportLog.info(String.format("Export is enabled and can overflow to %s.", cluster.getExportoverflow()));

        m_loaderClass = conn.getLoaderclass();

        try {
            exportLog.info("Creating connector " + m_loaderClass);
            ExportDataProcessor newProcessor = null;
            final Class<?> loaderClass = Class.forName(m_loaderClass);
            newProcessor = (ExportDataProcessor)loaderClass.newInstance();
            m_processor.set(newProcessor);
            newProcessor.addLogger(exportLog);
            File exportOverflowDirectory = new File(catalogContext.cluster.getExportoverflow());
            initializePersistedGenerations(
                    exportOverflowDirectory,
                    catalogContext, conn);
            ExportGeneration currentGeneration =
                new ExportGeneration(
                        catalogContext.m_timestamp,
                        m_onGenerationDrained,
                        exportOverflowDirectory);
            currentGeneration.initializeGenerationFromCatalog(conn, m_hostId, messenger);
            m_generations.put( catalogContext.m_timestamp, currentGeneration);
            newProcessor.setExportGeneration(m_generations.firstEntry().getValue());
            newProcessor.setProcessorConfig(m_processorConfig);
            newProcessor.readyForData();
            if (m_generations.firstEntry().getValue().isDiskBased() &&
                    !m_loaderClass.equals(RawProcessor.class.getName())) {
                m_generations.firstEntry().getValue().kickOffLeaderElection();
            }
        }
        catch (final ClassNotFoundException e) {
            exportLog.l7dlog( Level.ERROR, LogKeys.export_ExportManager_NoLoaderExtensions.name(), e);
            throw new ExportManager.SetupException(e);
        }
        catch (final Exception e) {
            throw new ExportManager.SetupException(e);
        }
    }

    private void initializePersistedGenerations(
            File exportOverflowDirectory, CatalogContext catalogContext,
            Connector conn) throws IOException {
        TreeSet<File> generationDirectories = new TreeSet<File>();
        for (File f : exportOverflowDirectory.listFiles()) {
            if (f.isDirectory()) {
                if (!f.canRead() || !f.canWrite() || !f.canExecute()) {
                    throw new RuntimeException("Can't one of read/write/execute directory " + f);
                }
                generationDirectories.add(f);
            }
        }

        //Only give the processor to the oldest generation
        for (File generationDirectory : generationDirectories) {
            ExportGeneration generation =
                new ExportGeneration(
                        m_onGenerationDrained,
                        generationDirectory);
            if (generation.initializeGenerationFromDisk(conn, m_messenger)) {
                m_generations.put( generation.m_timestamp, generation);
            } else {
                exportLog.error("Invalid export generation in overflow directory " + generationDirectory +
                        " this will have to be cleaned up manually.");
            }
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

    private void updateProcessorConfig(final Connector conn) {
        Properties newConfig = new Properties();

        if (conn.getConfig() != null) {
            Iterator<ConnectorProperty> connPropIt = conn.getConfig().iterator();
            while (connPropIt.hasNext()) {
                ConnectorProperty prop = connPropIt.next();
                newConfig.put(prop.getName(), prop.getValue());
            }
        }
        m_processorConfig = newConfig;
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
        updateProcessorConfig(conn);

        File exportOverflowDirectory = new File(catalogContext.cluster.getExportoverflow());

        ExportGeneration newGeneration = null;
        try {
            newGeneration = new ExportGeneration(
                    catalogContext.m_timestamp,
                    m_onGenerationDrained,
                    exportOverflowDirectory);
        } catch (IOException e1) {
            VoltDB.crashLocalVoltDB("Error processing catalog update in export system", true, e1);
        }
        newGeneration.initializeGenerationFromCatalog(conn, m_hostId, m_messenger);

        m_generations.put(catalogContext.m_timestamp, newGeneration);
    }

    public void shutdown() {
        ExportDataProcessor proc = m_processor.getAndSet(null);
        if (proc != null) {
            proc.shutdown();
        }
        for (ExportGeneration generation : m_generations.values()) {
            generation.close();
        }
        m_generations.clear();
        m_loaderClass = null;
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
            Map<Long, ExportGeneration> generations = instance.m_generations;
            if (generations.isEmpty()) {
                assert(false);
                return -1;
            }

            long exportBytes = 0;
            for (ExportGeneration generation : generations.values()) {
                exportBytes += generation.getQueuedExportBytes( partitionId, signature);
            }

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
     * End of stream indicates that no more data is coming from this source
     * for this generation.
     */
    public static void pushExportBuffer(
            long exportGeneration,
            int partitionId,
            String signature,
            long uso,
            long bufferPtr,
            ByteBuffer buffer,
            boolean sync,
            boolean endOfStream) {
        ExportManager instance = instance();
        try {
            ExportGeneration generation = instance.m_generations.get(exportGeneration);
            if (generation == null) {
                DBBPool.deleteCharArrayMemory(bufferPtr);
                /*
                 * If the generation was already drained it is fine for a buffer to come late and miss it
                 */
                synchronized(instance) {
                    if (!instance.m_generationGhosts.contains(exportGeneration)) {
                        assert(false);
                        exportLog.error("Could not a find an export generation " + exportGeneration +
                        ". Should be impossible. Discarding export data");
                    }
                }
                return;
            }

            generation.pushExportBuffer(partitionId, signature, uso, bufferPtr, buffer, sync, endOfStream);
        } catch (Exception e) {
            //Don't let anything take down the execution site thread
            exportLog.error(e);
        }
    }

    public void truncateExportToTxnId(long snapshotTxnId, long[] perPartitionTxnIds) {
        exportLog.info("Truncating export data after txnId " + snapshotTxnId);
        for (ExportGeneration generation : m_generations.values()) {
            generation.truncateExportToTxnId(snapshotTxnId, perPartitionTxnIds);
        }
    }
}
