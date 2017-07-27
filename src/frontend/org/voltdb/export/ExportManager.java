/* This file is part of VoltDB.
 * Copyright (C) 2008-2017 VoltDB Inc.
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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicReference;

import org.voltcore.logging.Level;
import org.voltcore.logging.VoltLogger;
import org.voltcore.messaging.HostMessenger;
import org.voltcore.utils.CoreUtils;
import org.voltcore.utils.DBBPool;
import org.voltcore.utils.Pair;
import org.voltdb.CatalogContext;
import org.voltdb.VoltDB;
import org.voltdb.catalog.CatalogMap;
import org.voltdb.catalog.Cluster;
import org.voltdb.catalog.Connector;
import org.voltdb.catalog.ConnectorProperty;
import org.voltdb.catalog.ConnectorTableInfo;
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
     * the only supported processor class
     */
    public static final String PROCESSOR_CLASS =
            "org.voltdb.export.processors.GuestProcessor";
    /**
     * This is property used for checking Export clients for validation only.
     */
    public final static String CONFIG_CHECK_ONLY = "__voltdb_config_check_only__";

    /**
     * Processors also log using this facility.
     */
    private static final VoltLogger exportLog = new VoltLogger("EXPORT");

    private final AtomicReference<ExportGeneration> m_generation = new AtomicReference<>(null);

    //Keep track of initial or last generation that was set in EE but has no Java side as export is disabled and misconfigured.
    //Typically start with table pointing to bad target. fix target and unfix it again...keep doing this and you will
    //have a generation thats last one which has not java side as export s disabled.
    // TODO this comment looks like it needs to be revisited
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

    // this used to be flexible, but no longer - now m_loaderClass is just null or default value
    public static final String DEFAULT_LOADER_CLASS = "org.voltdb.export.processors.GuestProcessor";
    private final String m_loaderClass = DEFAULT_LOADER_CLASS;

    private volatile Map<String, Pair<Properties, Set<String>>> m_processorConfig = new HashMap<>();

    private int m_exportTablesCount = 0;

    private int m_connCount = 0;

    private final ExecutorService m_dataProcessorHandler = CoreUtils.getSingleThreadExecutor("Export-UAC-Handler");


    /**
     * Construct ExportManager using catalog.
     * @param myHostId
     * @param catalogContext
     * @throws ExportManager.SetupException
     */
    // FIXME - this synchronizes on the ExportManager class, but everyone else synchronizes on the instance.
    public static synchronized void initialize(
            int myHostId,
            CatalogContext catalogContext,
            boolean isRejoin,
            boolean forceCreate,
            HostMessenger messenger,
            List<Integer> partitions)
            throws ExportManager.SetupException
    {
        ExportManager em = new ExportManager(myHostId, catalogContext, messenger);
        if (forceCreate) {
            em.clearOverflowData();
        }
        CatalogMap<Connector> connectors = getConnectors(catalogContext);

        if (hasEnabledConnectors(connectors)) {
            em.initialize(connectors, partitions, isRejoin);
        }
        m_self = em;
    }

    static boolean hasEnabledConnectors(CatalogMap<Connector> connectors) {
        for (Connector conn : connectors) {
            if (conn.getEnabled() && !conn.getTableinfo().isEmpty()) {
                return true;
            }
        }
        return false;
    }

    /**
     * Indicate to associated {@link ExportGeneration}s to become
     * masters for the given partition id
     * @param partitionId
     */
    synchronized public void acceptMastership(int partitionId) {
        Preconditions.checkArgument(
                m_masterOfPartitions.add(partitionId),
                "can't acquire mastership twice for partition id: " + partitionId
                );
        exportLog.info("ExportManager accepting mastership for partition " + partitionId);
        /*
         * Only the first generation will have a processor which
         * makes it safe to accept mastership.
         */
        ExportGeneration generation = m_generation.get();
        if (generation == null) {
            return;
        }
        generation.acceptMastershipTask(partitionId);
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

    private static CatalogMap<Connector> getConnectors(CatalogContext catalogContext) {
        final Cluster cluster = catalogContext.catalog.getClusters().get("cluster");
        final Database db = cluster.getDatabases().get("database");
        return db.getConnectors();
    }

    /**
     * Read the catalog to setup manager and loader(s)
     */
    private ExportManager(
            int myHostId,
            CatalogContext catalogContext,
            HostMessenger messenger)
    throws ExportManager.SetupException
    {
        m_hostId = myHostId;
        m_messenger = messenger;
        final CatalogMap<Connector> connectors = getConnectors(catalogContext);

        if (!hasEnabledConnectors(connectors)) {
            if (connectors.isEmpty()) {
                exportLog.info("System is not using any export functionality.");
                return;
            }
            else {
                exportLog.info("Export is disabled by user configuration.");
                return;
            }
        }

        updateProcessorConfig(connectors);

        exportLog.info(String.format("Export is enabled and can overflow to %s.", VoltDB.instance().getExportOverflowPath()));
    }

    public HostMessenger getHostMessenger() {
        return m_messenger;
    }

    private void clearOverflowData() throws ExportManager.SetupException {
        String overflowDir = VoltDB.instance().getExportOverflowPath();
        try {
            exportLog.info(
                String.format("Cleaning out contents of export overflow directory %s for create with force", overflowDir));
            VoltFile.recursivelyDelete(new File(overflowDir), false);
        } catch(IOException e) {
            String msg = String.format("Error cleaning out export overflow directory %s: %s",
                    overflowDir, e.getMessage());
            if (exportLog.isDebugEnabled()) {
                exportLog.debug(msg, e);
            }
            throw new ExportManager.SetupException(msg);
        }

    }

    public void startPolling(CatalogContext catalogContext) {
        final CatalogMap<Connector> connectors = getConnectors(catalogContext);

        if(!hasEnabledConnectors(connectors)) return;

        ExportDataProcessor processor = m_processor.get();
        Preconditions.checkState(processor != null, "guest processor is not set");

        processor.startPolling();
    }

    private void initializePersistedGenerations() throws IOException {

        File exportOverflowDirectory = new File(VoltDB.instance().getExportOverflowPath());
        File files[] = exportOverflowDirectory.listFiles();
        if (files == null) {
            //Clean export overflow no generations seen.
            return;
        }

        for (File f : files) {
            if (f.isDirectory()) {
                // TODO this is not approved UX
                VoltDB.crashLocalVoltDB("BSDBG: there's more than one generation in export_overflow");
            }
        }

        ExportGeneration generation = new ExportGeneration(exportOverflowDirectory, false);

        if (generation.initializeGenerationFromDisk(m_messenger)) {
            assert (m_generation.get() == null);
            m_generation.set(generation);
        } else {
            String list[] = exportOverflowDirectory.list();
            if (list != null && list.length == 0) {
                try {
                    VoltFile.recursivelyDelete(exportOverflowDirectory);
                } catch (IOException ioe) {
                }
            } else {
                exportLog.error("Invalid export generation in overflow directory " + exportOverflowDirectory
                        + " this will need to be manually cleaned up. number of files left: "
                        + (list != null ? list.length : 0));
            }
        }
    }

    private void updateProcessorConfig(final CatalogMap<Connector> connectors) {
        Map<String, Pair<Properties, Set<String>>> config = new HashMap<>();

        // If the export source changes before the previous generation drains
        // then the outstanding exports will go to the new source when export resumes.
        int connCount = 0;
        int tableCount = 0;
        for (Connector conn : connectors) {
            // skip disabled connectors
            if (!conn.getEnabled() || conn.getTableinfo().isEmpty()) {
                continue;
            }

            connCount++;
            Properties properties = new Properties();
            Set<String> tables = new HashSet<>();

            String targetName = conn.getTypeName();

            for (ConnectorTableInfo ti : conn.getTableinfo()) {
                tables.add(ti.getTable().getTypeName());
                tableCount++;
            }

            if (conn.getConfig() != null) {
                Iterator<ConnectorProperty> connPropIt = conn.getConfig().iterator();
                while (connPropIt.hasNext()) {
                    ConnectorProperty prop = connPropIt.next();
                    properties.put(prop.getName(), prop.getValue().trim());
                    if (!prop.getName().toLowerCase().contains("password")) {
                        properties.put(prop.getName(), prop.getValue().trim());
                    } else {
                        //Dont trim passwords
                        properties.put(prop.getName(), prop.getValue());
                    }
                }
            }

            Pair<Properties, Set<String>> connConfig = new Pair<>(properties, tables);
            config.put(targetName, connConfig);
        }

        m_connCount = connCount;
        m_exportTablesCount = tableCount;
        m_processorConfig = config;
    }

    public int getExportTablesCount() {
        return m_exportTablesCount;
    }

    public int getConnCount() {
        return m_connCount;
    }

    private ExportGeneration createGenerationIfNeeded(boolean isRejoin) {
        ExportGeneration generation = m_generation.get();
        if (generation == null) {
            try {
                File exportOverflowDirectory = new File(VoltDB.instance().getExportOverflowPath());
                generation = new ExportGeneration(exportOverflowDirectory, isRejoin);
                m_generation.set(generation);
            } catch (IOException e1) {
                VoltDB.crashLocalVoltDB("Error processing catalog update in export system", true, e1);
            }
        }
        return generation;
    }

    /** Creates the initial export processor if export is enabled */
    private void initialize(CatalogMap<Connector> connectors, List<Integer> partitions, boolean isRejoin) {
        try {
            exportLog.info("Creating connector " + m_loaderClass);
            ExportDataProcessor newProcessor = getNewProcessorWithProcessConfigSet(m_processorConfig);
            m_processor.set(newProcessor);

            initializePersistedGenerations();

            ExportGeneration generation = m_generation.get();
            if (m_generation.get() == null) {
                generation = createGenerationIfNeeded(isRejoin);
                generation.initializeGenerationFromCatalog(connectors, m_hostId, m_messenger, partitions);
            } else {
                // TODO - change or remove log message?
                exportLog.info("Persisted export generation same as catalog exists. Persisted generation will be used and appended to");
            }

            newProcessor.setExportGeneration(generation);
            newProcessor.readyForData(true);
        }
        catch (final ClassNotFoundException e) {
            exportLog.l7dlog( Level.ERROR, LogKeys.export_ExportManager_NoLoaderExtensions.name(), e);
            Throwables.propagate(e);
        }
        catch (final Exception e) {
            Throwables.propagate(e);
        }
    }

    public synchronized void updateCatalog(CatalogContext catalogContext, boolean requireCatalogDiffCmdsApplyToEE, boolean requiresNewExportGeneration, List<Integer> partitions)
    {
        final Cluster cluster = catalogContext.catalog.getClusters().get("cluster");
        final Database db = cluster.getDatabases().get("database");
        final CatalogMap<Connector> connectors = db.getConnectors();

        Map<String, Pair<Properties, Set<String>>> processorConfigBeforeUpdate = m_processorConfig;
        updateProcessorConfig(connectors);
        if (m_processorConfig.isEmpty() && processorConfigBeforeUpdate.isEmpty()) {
            return;
        }

        if (!requiresNewExportGeneration) {
            exportLog.info("Skipped rolling generations as no stream related changes happened during this update.");
            return;
        }
        /**
         * This checks if the catalogUpdate was done in EE or not. If catalog update is skipped for @UpdateClasses and such
         * EE does not roll to new generation and thus we need to ignore creating new generation roll with the current generation.
         * If anything changes in getDiffCommandsForEE or design changes pay attention to fix this.
         */
        if (requireCatalogDiffCmdsApplyToEE == false) {
            exportLog.info("Skipped rolling generations as generation not created in EE.");
            return;
        }

        ExportGeneration generation = createGenerationIfNeeded(false);
        generation.initializeGenerationFromCatalog(connectors, m_hostId, m_messenger, partitions);

        /*
         * If there is no existing export processor, create an initial one.
         * This occurs when export is turned on/off at runtime.
         */
        if (m_processor.get() == null) {
            exportLog.info("First stream created processor will be initialized: " + m_loaderClass);

            try {
                exportLog.info("Creating connector " + m_loaderClass);
                ExportDataProcessor newProcessor = getNewProcessorWithProcessConfigSet(m_processorConfig);
                m_processor.set(newProcessor);

                newProcessor.setExportGeneration(generation);
                newProcessor.readyForData(false);

                /*
                 * When it isn't startup, it is necessary to kick things off with the mastership
                 * settings that already exist
                 *
                 * This strategy is the one that piggy backs on
                 * regular partition mastership distribution to determine
                 * who will process export data for different partitions.
                 * We stashed away all the ones we have mastership of
                 * in m_masterOfPartitions
                 */
                for (Integer partitionId: m_masterOfPartitions) {
                    generation.acceptMastershipTask(partitionId);
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
        else {
            // install new export processors with export config from this time
            swapWithNewProcessor(generation, partitions, m_processorConfig);
        }
    }

    private  ExportDataProcessor getNewProcessorWithProcessConfigSet(Map<String, Pair<Properties, Set<String>>> config) throws ClassNotFoundException, InstantiationException, IllegalAccessException {
        final Class<?> loaderClass = Class.forName(m_loaderClass);
        ExportDataProcessor newProcessor = (ExportDataProcessor)loaderClass.newInstance();
        newProcessor.addLogger(exportLog);
        newProcessor.setProcessorConfig(config);
        return newProcessor;
    }

    // remove and install new processor
    private void swapWithNewProcessor(
            ExportGeneration generation,
            List<Integer> partitions,
            Map<String, Pair<Properties, Set<String>>> config) {
        generation.pausePolling(partitions);
        m_dataProcessorHandler.submit(new Runnable() {
            @Override
            public void run() {
                ExportDataProcessor oldProcessor = m_processor.get();
                exportLog.info("Shutdown guestprocessor");
                oldProcessor.shutdown();
                if (config.isEmpty()) {
                    m_processor.getAndSet(null);
                    exportLog.info("Processor shutdown completed");
                    return;
                }
                exportLog.info("Processor shutdown completed, install new export processor");
                ExportDataProcessor newProcessor = null;
                try {
                    newProcessor = getNewProcessorWithProcessConfigSet(config);
                }
                catch (Exception crash) {
                    VoltDB.crashLocalVoltDB("Error creating next export processor", true, crash);
                }
                m_processor.getAndSet(newProcessor);
                newProcessor.setExportGeneration(generation);
                newProcessor.readyForData(false);
                for ( Integer partitionId: m_masterOfPartitions) {
                    generation.acceptMastershipTask(partitionId);
                }
            }
        });

    }

    public void shutdown() {
        ExportGeneration generation = m_generation.getAndSet(null);
        if (generation != null) {
            generation.close(m_messenger);
        }
        ExportDataProcessor proc = m_processor.getAndSet(null);
        if (proc != null) {
            proc.shutdown();
        }
    }

    public static long getQueuedExportBytes(int partitionId, String signature) {
        ExportManager instance = instance();
        try {
            ExportGeneration generation = instance.m_generation.get();
            if (generation == null) {
                //This is now fine as you could get a stats tick and have no generation if you dropped last table.
                return 0;
            }
            return generation.getQueuedExportBytes( partitionId, signature);
        } catch (Exception e) {
            //Don't let anything take down the execution site thread
            exportLog.error("Failed to get export queued bytes.", e);
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
            int partitionId,
            String signature,
            long uso,
            long bufferPtr,
            ByteBuffer buffer,
            boolean sync) {
        //For validating that the memory is released
        if (bufferPtr != 0) DBBPool.registerUnsafeMemory(bufferPtr);
        ExportManager instance = instance();
        try {
            ExportGeneration generation = instance.m_generation.get();
            if (generation == null) {
                // TODO: this should probably be an error, not a crash. It still should not happen.
                if (buffer != null) {
                    DBBPool.wrapBB(buffer).discard();
                }
                VoltDB.crashLocalVoltDB("BSDBG: pushed export buffer before generation was initialized");
                return;
            }
            generation.pushExportBuffer(partitionId, signature, uso, buffer, sync);
        } catch (Exception e) {
            //Don't let anything take down the execution site thread
            exportLog.error("Error pushing export buffer", e);
        }
    }

    public void truncateExportToTxnId(long snapshotTxnId, long[] perPartitionTxnIds) {
        exportLog.info("Truncating export data after txnId " + snapshotTxnId);
        //If the generation was completely drained, wait for the task to finish running
        //by waiting for the permit that will be generated
        ExportGeneration generation = m_generation.get();
        if (generation != null) {
            generation.truncateExportToTxnId(snapshotTxnId, perPartitionTxnIds);
        }
    }

    public static synchronized void sync(final boolean nofsync) {
        exportLog.info("Syncing export data");
        ExportGeneration generation = instance().m_generation.get();
        if (generation != null) {
            generation.sync(nofsync);
        }
    }
}
