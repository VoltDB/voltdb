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

package org.voltdb.export;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicReference;

import org.voltcore.logging.Level;
import org.voltcore.logging.VoltLogger;
import org.voltcore.messaging.HostMessenger;
import org.voltcore.utils.COWSortedMap;
import org.voltcore.utils.DBBPool;
import org.voltdb.CatalogContext;
import org.voltdb.VoltDB;
import org.voltdb.catalog.Cluster;
import org.voltdb.catalog.Connector;
import org.voltdb.catalog.ConnectorProperty;
import org.voltdb.catalog.Database;
import org.voltdb.utils.LogKeys;
import org.voltdb.utils.VoltFile;

import com.google_voltpatches.common.base.Preconditions;
import com.google_voltpatches.common.base.Throwables;

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

    /*
     * Issue a permit when a generation is drained so that when we are truncating if a generation
     * is completely truncated we can wait for the on generation drained task to finish.
     *
     * This eliminates a race with CL replay where it may do catalog updates and such while truncation
     * is still running on generation drained.
     */
    private final Semaphore m_onGenerationDrainedForTruncation = new Semaphore(0);

    public class GenerationDrainRunnable implements Runnable {

        private final ExportGeneration m_generation;

        public GenerationDrainRunnable(ExportGeneration generation) {
            m_generation = generation;
        }

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
                    try {
                        rollToNextGeneration(m_generation);
                    } catch (RuntimeException e) {
                        exportLog.error("Error rolling to next export generation", e);
                    } catch (Exception e) {
                        exportLog.error("Error rolling to next export generation", e);
                    } finally {
                        m_onGenerationDrainedForTruncation.release();
                    }
                }

            });
        }

    }

    private void rollToNextGeneration(ExportGeneration drainedGeneration) throws Exception {
        ExportDataProcessor newProcessor = null;
        ExportDataProcessor oldProcessor = null;
        synchronized (ExportManager.this) {
            boolean installNewProcessor = false;
            if (m_generations.containsValue(drainedGeneration)) {
                m_generations.remove(drainedGeneration.m_timestamp);
                m_generationGhosts.add(drainedGeneration.m_timestamp);
                installNewProcessor = (m_processor.get().getExportGeneration() == drainedGeneration);
                exportLog.info("Finished draining generation " + drainedGeneration.m_timestamp);
            } else {
                exportLog.warn("Finished draining a generation that is not known to export generations.");
            }

            try {
                if (m_loaderClass != null && !m_generations.isEmpty() && installNewProcessor) {
                    exportLog.info("Creating connector " + m_loaderClass);
                    final Class<?> loaderClass = Class.forName(m_loaderClass);
                    //Make it so
                    ExportGeneration nextGeneration = m_generations.firstEntry().getValue();
                    newProcessor = (ExportDataProcessor) loaderClass.newInstance();
                    newProcessor.addLogger(exportLog);
                    newProcessor.setExportGeneration(nextGeneration);
                    newProcessor.setProcessorConfig(m_processorConfig);
                    newProcessor.readyForData();

                    if (!nextGeneration.isContinueingGeneration()) {
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
                        for (Integer partitionId : m_masterOfPartitions) {
                            nextGeneration.acceptMastershipTask(partitionId);
                        }
                    }
                    oldProcessor = m_processor.getAndSet(newProcessor);
                }
            } catch (Exception e) {
                VoltDB.crashLocalVoltDB("Error creating next export processor", true, e);
            }
        }

        /*
         * The old processor should shutdown if we installed a new processor.
         */
        if (oldProcessor != null) {
            oldProcessor.shutdown();
        }
        try {
            //We close and delete regardless
            drainedGeneration.closeAndDelete();
        } catch (IOException e) {
            e.printStackTrace();
            exportLog.error(e);
        }
    }

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
            HostMessenger messenger,
            List<Integer> partitions)
            throws ExportManager.SetupException
    {
        ExportManager em = new ExportManager(myHostId, catalogContext, messenger, partitions);
        Connector connector = getConnector(catalogContext);

        m_self = em;
        if (connector != null && connector.getEnabled() == true) {
            em.createInitialExportProcessor(catalogContext, connector, true, partitions, isRejoin);
        }
    }

    /**
     * Indicate to associated {@link ExportGeneration}s to become
     * masters for the given partition id
     * @param partitionId
     */
    synchronized public void acceptMastership(int partitionId) {
        if (m_loaderClass == null) {
            return;
        }
        Preconditions.checkArgument(
                m_masterOfPartitions.add(partitionId),
                "can't acquire mastership twice for partition id: " + partitionId
                );
        exportLog.info("ExportManager accepting mastership for partition " + partitionId);
        /*
         * Only the first generation will have a processor which
         * makes it safe to accept mastership.
         */
        ExportGeneration gen = m_generations.firstEntry().getValue();
        if (gen != null && gen.isContinueingGeneration()) {
            gen.acceptMastershipTask(partitionId);
        } else {
            exportLog.info("Failed to run accept mastership tasks for partition: " + partitionId);
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

    private static Connector getConnector(CatalogContext catalogContext) {
        final Cluster cluster = catalogContext.catalog.getClusters().get("cluster");
        final Database db = cluster.getDatabases().get("database");
        final Connector conn= db.getConnectors().get("0");
        return conn;
    }

    /**
     * Read the catalog to setup manager and loader(s)
     * @param siteTracker
     */
    private ExportManager(
            int myHostId,
            CatalogContext catalogContext,
            HostMessenger messenger,
            List<Integer> partitions)
    throws ExportManager.SetupException
    {
        m_hostId = myHostId;
        m_messenger = messenger;
        final Cluster cluster = catalogContext.catalog.getClusters().get("cluster");
        final Connector conn = getConnector(catalogContext);

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
    }

    private synchronized void createInitialExportProcessor(
            CatalogContext catalogContext,
            final Connector conn,
            boolean startup,
            List<Integer> partitions,
            boolean isRejoin) {
        try {
            exportLog.info("Creating connector " + m_loaderClass);
            ExportDataProcessor newProcessor = null;
            final Class<?> loaderClass = Class.forName(m_loaderClass);
            newProcessor = (ExportDataProcessor)loaderClass.newInstance();
            newProcessor.addLogger(exportLog);
            newProcessor.setProcessorConfig(m_processorConfig);
            m_processor.set(newProcessor);

            File exportOverflowDirectory = new File(catalogContext.cluster.getExportoverflow());

            /*
             * If this is a catalog update providing an existing generation,
             * the persisted stuff has already been initialized
             */
            if (startup) {
                initializePersistedGenerations(
                        exportOverflowDirectory,
                        catalogContext, conn);
            }

            /*
             * If this is startup there is no existing generation created for new export data
             * So construct one here, otherwise use the one provided
             */
            if (startup) {
                if (!m_generations.containsKey(catalogContext.m_uniqueId)) {
                    final ExportGeneration currentGeneration = new ExportGeneration(
                            catalogContext.m_uniqueId,
                            exportOverflowDirectory, isRejoin);
                    currentGeneration.setGenerationDrainRunnable(new GenerationDrainRunnable(currentGeneration));
                    currentGeneration.initializeGenerationFromCatalog(conn, m_hostId, m_messenger, partitions);
                    m_generations.put(catalogContext.m_uniqueId, currentGeneration);
                } else {
                    exportLog.info("Persisted export generation same as catalog exists. Persisted generation will be used and appended to");
                    ExportGeneration currentGeneration = m_generations.get(catalogContext.m_uniqueId);
                    currentGeneration.initializeMissingPartitionsFromCatalog(conn, m_hostId, m_messenger, partitions);
                }
            }
            final ExportGeneration nextGeneration = m_generations.firstEntry().getValue();
            /*
             * For the newly constructed processor, provide it the oldest known generation
             */
            newProcessor.setExportGeneration(nextGeneration);
            newProcessor.readyForData();

            if (startup) {
                /*
                 * If the oldest known generation was disk based,
                 * and we are using server side export we need to kick off a leader election
                 * to choose which server is going to export each partition
                 */
                if (!nextGeneration.isContinueingGeneration()) {
                    nextGeneration.kickOffLeaderElection();
                }
            } else {
                /*
                 * When it isn't startup, it is necessary to kick things off with the mastership
                 * settings that already exist
                 */
                if (!nextGeneration.isContinueingGeneration()) {
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
        }
        catch (final ClassNotFoundException e) {
            exportLog.l7dlog( Level.ERROR, LogKeys.export_ExportManager_NoLoaderExtensions.name(), e);
            Throwables.propagate(e);
        }
        catch (final Exception e) {
            Throwables.propagate(e);
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
            ExportGeneration generation = new ExportGeneration(generationDirectory, catalogContext.m_uniqueId);
            generation.setGenerationDrainRunnable(new GenerationDrainRunnable(generation));

            if (generation.initializeGenerationFromDisk(conn, m_messenger)) {
                m_generations.put( generation.m_timestamp, generation);
            } else {
                String list[] = generationDirectory.list();
                if (list != null && list.length == 0) {
                    try {
                        VoltFile.recursivelyDelete(generationDirectory);
                    } catch (IOException ioe) {
                    }
                } else {
                    exportLog.error("Invalid export generation in overflow directory " + generationDirectory
                            + " this will need to be manually cleaned up. number of files left: "
                            + (list != null ? list.length : 0));
                }
            }
        }
    }

    private void updateProcessorConfig(final Connector conn) {
        Properties newConfig = new Properties();

        if (conn.getConfig() != null) {
            Iterator<ConnectorProperty> connPropIt = conn.getConfig().iterator();
            while (connPropIt.hasNext()) {
                ConnectorProperty prop = connPropIt.next();
                newConfig.put(prop.getName(), prop.getValue().trim());
            }
        }
        m_processorConfig = newConfig;
    }

    public synchronized void updateCatalog(CatalogContext catalogContext, List<Integer> partitions)
    {
        final Cluster cluster = catalogContext.catalog.getClusters().get("cluster");
        final Database db = cluster.getDatabases().get("database");
        final Connector conn= db.getConnectors().get("0");
        if (conn == null || !conn.getEnabled()) {
            m_loaderClass = null;
            return;
        }

        m_loaderClass = conn.getLoaderclass();
        updateProcessorConfig(conn);

        File exportOverflowDirectory = new File(catalogContext.cluster.getExportoverflow());

        ExportGeneration newGeneration = null;
        try {
            newGeneration = new ExportGeneration(
                    catalogContext.m_uniqueId, exportOverflowDirectory, false);
            newGeneration.setGenerationDrainRunnable(new GenerationDrainRunnable(newGeneration));
            newGeneration.initializeGenerationFromCatalog(conn, m_hostId, m_messenger, partitions);
            m_generations.put(catalogContext.m_uniqueId, newGeneration);
        } catch (IOException e1) {
            VoltDB.crashLocalVoltDB("Error processing catalog update in export system", true, e1);
        }

        /*
         * If there is no existing export processor, create an initial one.
         * This occurs when export is turned on/off at runtime.
         */
        if (m_processor.get() == null) {
            createInitialExportProcessor(catalogContext, conn, false, partitions, false);
        }
    }

    public void shutdown() {
        for (ExportGeneration generation : m_generations.values()) {
            generation.close();
        }
        ExportDataProcessor proc = m_processor.getAndSet(null);
        if (proc != null) {
            proc.shutdown();
        }
        m_generations.clear();
        m_loaderClass = null;
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
        //For validating that the memory is released
        if (bufferPtr != 0) DBBPool.registerUnsafeMemory(bufferPtr);
        ExportManager instance = instance();
        try {
            ExportGeneration generation = instance.m_generations.get(exportGeneration);
            if (generation == null) {
                if (buffer != null) {
                    DBBPool.wrapBB(buffer).discard();
                }

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

            generation.pushExportBuffer(partitionId, signature, uso, buffer, sync, endOfStream);
        } catch (Exception e) {
            //Don't let anything take down the execution site thread
            exportLog.error("Error pushing export buffer", e);
        }
    }

    public void truncateExportToTxnId(long snapshotTxnId, long[] perPartitionTxnIds) {
        exportLog.info("Truncating export data after txnId " + snapshotTxnId);
        for (ExportGeneration generation : m_generations.values()) {
            //If the generation was completely drained, wait for the task to finish running
            //by waiting for the permit that will be generated
            if (generation.truncateExportToTxnId(snapshotTxnId, perPartitionTxnIds)) {
                try {
                    m_onGenerationDrainedForTruncation.acquire();
                } catch (InterruptedException e) {
                    VoltDB.crashLocalVoltDB("Interrupted truncating export data", true, e);
                }
            }
        }
    }
}
