/* This file is part of VoltDB.
 * Copyright (C) 2008-2019 VoltDB Inc.
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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import org.voltcore.logging.Level;
import org.voltcore.logging.VoltLogger;
import org.voltcore.messaging.HostMessenger;
import org.voltcore.utils.DBBPool;
import org.voltcore.utils.Pair;
import org.voltdb.CatalogContext;
import org.voltdb.ClientInterface;
import org.voltdb.ExportStatsBase.ExportStatsRow;
import org.voltdb.SimpleClientResponseAdapter;
import org.voltdb.SnapshotCompletionMonitor.ExportSnapshotTuple;
import org.voltdb.TTLManager;
import org.voltdb.VoltDB;
import org.voltdb.VoltTable;
import org.voltdb.catalog.CatalogMap;
import org.voltdb.catalog.Connector;
import org.voltdb.catalog.ConnectorProperty;
import org.voltdb.catalog.ConnectorTableInfo;
import org.voltdb.client.ProcedureCallback;
import org.voltdb.export.ExportDataSource.StreamStartAction;
import org.voltdb.sysprocs.ExportControl.OperationMode;
import org.voltdb.utils.CatalogUtil;
import org.voltdb.utils.LogKeys;
import org.voltdb.utils.VoltFile;

import com.google_voltpatches.common.base.Preconditions;

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
public class ExportManager implements ExportManagerInterface
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

    private final HostMessenger m_messenger;

    /**
     * Set of partition ids for which this export manager instance is master of
     */
    private final Set<Integer> m_masterOfPartitions = new HashSet<Integer>();

    /**
     * Master sends RELEASE_BUFFER to all its replicas to discard buffer.
     */
    public static final byte RELEASE_BUFFER = 1;

    /**
     * Connections OLAP loaders. Currently at most one loader allowed.
     * Supporting multiple loaders mainly involves reference counting
     * the EE data blocks and bookkeeping ACKs from processors.
     */
    AtomicReference<ExportDataProcessor> m_processor = new AtomicReference<ExportDataProcessor>();

    private ExportStats m_exportStats;
    private final int m_hostId;

    // this used to be flexible, but no longer - now m_loaderClass is just null or default value
    public static final String DEFAULT_LOADER_CLASS = "org.voltdb.export.processors.GuestProcessor";
    private final String m_loaderClass = DEFAULT_LOADER_CLASS;

    private volatile Map<String, Pair<Properties, Set<String>>> m_processorConfig = new HashMap<>();

    private int m_exportTablesCount = 0;
    private int m_connCount = 0;
    private boolean m_startPolling = false;
    private SimpleClientResponseAdapter m_adapter;
    private ClientInterface m_ci;

    @Override
    public ExportManagerInterface.ExportMode getExportMode() {
        return ExportManagerInterface.ExportMode.BASIC;
    }

    /**
     * Indicate to associated {@link ExportGeneration}s to become
     * leaders for the given partition id
     * @param partitionId
     */
    @Override
    synchronized public void becomeLeader(int partitionId) {
        m_masterOfPartitions.add(partitionId);
        ExportGeneration generation = m_generation.get();
        if (generation == null) {
            return;
        }
        generation.becomeLeader(partitionId);
    }

    protected ExportManager() {
        m_hostId = 0;
        m_messenger = null;
    }

    /**
     * Read the catalog to setup manager and loader(s)
     */
    public ExportManager(
            int myHostId,
            CatalogContext catalogContext,
            HostMessenger messenger)
    throws ExportManager.SetupException
    {
        exportLog.info("Export starting in BASIC mode");
        m_hostId = myHostId;
        m_messenger = messenger;
        m_exportStats = new ExportStats();

        exportLog.info("Running " + this.getClass().getName());

        boolean compress = !Boolean.getBoolean(StreamBlockQueue.EXPORT_DISABLE_COMPRESSION_OPTION);
        exportLog.info("Export has compression "
                + (compress ? "enabled" : "disabled") + " in " + VoltDB.instance().getExportOverflowPath());

        CatalogMap<Connector> connectors = CatalogUtil.getConnectors(catalogContext);
        if (!CatalogUtil.hasEnabledConnectors(connectors)) {
            exportLog.info("System is not using any export functionality or connectors configured are disabled.");
            return;
        }
        updateProcessorConfig(connectors);

        exportLog.info(String.format("Export is enabled and can overflow to %s.", VoltDB.instance().getExportOverflowPath()));
    }

    public HostMessenger getHostMessenger() {
        return m_messenger;
    }

    @Override
    public void clearOverflowData() throws ExportManagerInterface.SetupException {
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

    @Override
    public synchronized void startPolling(CatalogContext catalogContext, StreamStartAction action) {
        m_startPolling = true;

        CatalogMap<Connector> connectors = CatalogUtil.getConnectors(catalogContext);
        if(!CatalogUtil.hasEnabledConnectors(connectors)) {
            exportLog.info("System is not using any export functionality or connectors configured are disabled.");
            return;
        }

        ExportDataProcessor processor = m_processor.get();
        Preconditions.checkState(processor != null, "guest processor is not set");

        processor.startPolling();
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

    @Override
    public int getExportTablesCount() {
        return m_exportTablesCount;
    }

    @Override
    public int getConnCount() {
        return m_connCount;
    }

    /** Creates the initial export processor if export is enabled */
    @Override
    public void initialize(CatalogContext catalogContext, List<Pair<Integer, Integer>> localPartitionsToSites,
            boolean isRejoin) {
        try {
            CatalogMap<Connector> connectors = CatalogUtil.getConnectors(catalogContext);
            if (exportLog.isDebugEnabled()) {
                exportLog.debug("initialize for " + connectors.size() + " connectors.");
                CatalogUtil.dumpConnectors(exportLog, connectors);
            }
            if (!CatalogUtil.hasExportedTables(connectors)) {
                return;
            }

            if (exportLog.isDebugEnabled()) {
                exportLog.debug("Creating processor " + m_loaderClass);
            }
            ExportDataProcessor newProcessor = getNewProcessorWithProcessConfigSet(m_processorConfig);
            m_processor.set(newProcessor);

            File exportOverflowDirectory = new File(VoltDB.instance().getExportOverflowPath());
            ExportGeneration generation = new ExportGeneration(exportOverflowDirectory, m_messenger);
            generation.initialize(m_hostId, catalogContext,
                    connectors, newProcessor, localPartitionsToSites, exportOverflowDirectory);

            m_generation.set(generation);
            newProcessor.setExportGeneration(generation);
            newProcessor.readyForData();
        }
        catch (final ClassNotFoundException e) {
            exportLog.l7dlog( Level.ERROR, LogKeys.export_ExportManager_NoLoaderExtensions.name(), e);
            throw new RuntimeException(e);
        }
        catch (final Exception e) {
            exportLog.error("Initialize failed with:", e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public synchronized void updateCatalog(CatalogContext catalogContext, boolean requireCatalogDiffCmdsApplyToEE,
            boolean requiresNewExportGeneration, List<Pair<Integer, Integer>> localPartitionsToSites)
    {
        final CatalogMap<Connector> connectors = CatalogUtil.getConnectors(catalogContext);

        if (exportLog.isDebugEnabled()) {
            exportLog.debug("UpdateCatalog: requiresNewGeneration: " + requiresNewExportGeneration
                    + ", for " + connectors.size() + " connectors.");
            CatalogUtil.dumpConnectors(exportLog, connectors);
        }

        // Update processor config: note that we want to run a generation update even if the
        // processor config has no changes; we still need to handle changes in the exported tables
        updateProcessorConfig(connectors);

        if (!requiresNewExportGeneration) {
            // Even for catalog update doesn't affect export, genId still need to pass to EDS.
            // Because each ACK message is associated with a genId.
            if (m_generation.get() != null) {
                m_generation.get().updateGenerationId(catalogContext.m_genId);
            }
            exportLog.info("No stream related changes in update catalog.");
            return;
        }
        /*
         * This checks if the catalogUpdate was done in EE or not. If catalog update is skipped for @UpdateClasses and such
         * EE does not roll to new generation and thus we need to ignore creating new generation roll with the current generation.
         * If anything changes in getDiffCommandsForEE or design changes pay attention to fix this.
         */
        if (requireCatalogDiffCmdsApplyToEE == false) {
            exportLog.info("Skipped rolling generations as generation not created in EE.");
            return;
        }
        if (m_generation.get() == null) {
            File exportOverflowDirectory = new File(VoltDB.instance().getExportOverflowPath());
            try {
                ExportGeneration gen = new ExportGeneration(exportOverflowDirectory, m_messenger);
                m_generation.set(gen);
            } catch (IOException crash) {
                //This means durig UAC we had a bad disk on a node or bad directory.
                VoltDB.crashLocalVoltDB("Error creating export generation", true, crash);
                return;
            }
        }
        ExportGeneration generation = m_generation.get();
        assert(generation != null);
        /*
         * If there is no existing export processor, create an initial one.
         * This occurs when export is turned on/off at runtime.
         */
        if (m_processor.get() == null) {
            if (exportLog.isDebugEnabled()) {
                exportLog.debug("First stream created processor will be initialized: " + m_loaderClass);
            }
            try {
                ExportDataProcessor newProcessor = getNewProcessorWithProcessConfigSet(m_processorConfig);
                m_processor.set(newProcessor);
                generation.initializeGenerationFromCatalog(catalogContext,
                        connectors, newProcessor, m_hostId, localPartitionsToSites, true);
                if (exportLog.isDebugEnabled()) {
                    exportLog.debug("Creating connector " + m_loaderClass);
                }
                newProcessor.setExportGeneration(generation);
                if (m_startPolling && !m_processorConfig.isEmpty()) {
                    newProcessor.startPolling();
                }
                newProcessor.readyForData();

                /*
                 * When it isn't startup, it is necessary to kick things off with the leadership
                 * settings that already exist
                 *
                 * This strategy is the one that piggy backs on
                 * regular partition leadership distribution to determine
                 * who will process export data for different partitions.
                 * We stashed away all the ones we have leadership of
                 * in m_masterOfPartitions
                 */
                for (Integer partitionId: m_masterOfPartitions) {
                    generation.becomeLeader(partitionId);
                }
            }
            catch (final ClassNotFoundException e) {
                exportLog.l7dlog( Level.ERROR, LogKeys.export_ExportManager_NoLoaderExtensions.name(), e);
                throw new RuntimeException(e);
            }
            catch (final Exception e) {
                throw new RuntimeException(e);
            }
        }
        else {
            swapWithNewProcessor(catalogContext, generation,
                    connectors, localPartitionsToSites, m_processorConfig);
        }
    }

    // remove and install new processor
    private void swapWithNewProcessor(
            final CatalogContext catalogContext,
            ExportGeneration generation,
            CatalogMap<Connector> connectors,
            List<Pair<Integer, Integer>> partitions,
            Map<String, Pair<Properties, Set<String>>> config)
    {
        ExportDataProcessor oldProcessor = m_processor.get();
        if (exportLog.isDebugEnabled()) {
            exportLog.debug("Shutdown guestprocessor");
        }
        oldProcessor.shutdown();
        if (exportLog.isDebugEnabled()) {
            exportLog.debug("Processor shutdown completed, install new export processor");
        }
        generation.onProcessorShutdown();
        if (exportLog.isDebugEnabled()) {
            exportLog.debug("Existing export datasources unassigned.");
        }
        try {
            ExportDataProcessor newProcessor = getNewProcessorWithProcessConfigSet(config);
            //Load any missing tables.
            generation.initializeGenerationFromCatalog(catalogContext, connectors, newProcessor,
                    m_hostId, partitions, true);
            for (Pair<Integer, Integer> partition : partitions) {
                generation.updateAckMailboxes(partition.getFirst(), null);
            }
            //We create processor even if we dont have any streams.
            newProcessor.setExportGeneration(generation);
            if (m_startPolling && !config.isEmpty()) {
                newProcessor.startPolling();
            }
            m_processor.getAndSet(newProcessor);
            newProcessor.readyForData();
        } catch (Exception crash) {
            VoltDB.crashLocalVoltDB("Error creating next export processor", true, crash);
        }

        for (int partitionId : m_masterOfPartitions) {
            if (exportLog.isDebugEnabled()) {
                exportLog.debug("Set mastership on partition " + partitionId);
            }
            generation.becomeLeader(partitionId);
        }
    }

    private  ExportDataProcessor getNewProcessorWithProcessConfigSet(Map<String, Pair<Properties, Set<String>>> config) throws ClassNotFoundException, InstantiationException, IllegalAccessException {
        final Class<?> loaderClass = Class.forName(m_loaderClass);
        ExportDataProcessor newProcessor = (ExportDataProcessor)loaderClass.newInstance();
        newProcessor.setProcessorConfig(config);
        return newProcessor;
    }

    @Override
    public void shutdown() {
        ExportGeneration generation = m_generation.getAndSet(null);
        if (generation != null) {
            generation.close();
        }
        ExportDataProcessor proc = m_processor.getAndSet(null);
        if (proc != null) {
            proc.shutdown();
        }
    }

    @Override
    public List<ExportStatsRow> getStats(final boolean interval) {
        try {
            ExportGeneration generation = m_generation.get();
            if (generation != null) {
                return generation.getStats(interval);
            }
        } catch (Exception e) {
            //Don't let anything take down the execution site thread
            exportLog.error("Failed to get export queued bytes.", e);
        }
        return new ArrayList<ExportStatsRow>();
    }

    /*
     * End of stream indicates that no more data is coming from this source
     * for this generation.
     */
    public static void pushEndOfStream(
            int partitionId,
            String tableName) {
    }
    /*
     * Push an export buffer
     */
    public static void pushExportBuffer(
            int partitionId,
            String tableName,
            long startSequenceNumber,
            long committedSequenceNumber,
            long tupleCount,
            long uniqueId,
            long bufferPtr,
            ByteBuffer buffer) {
        //For validating that the memory is released
        if (bufferPtr != 0) DBBPool.registerUnsafeMemory(bufferPtr);
        ExportManagerInterface instance = ExportManagerInterface.instance();
        instance.pushBuffer(partitionId, tableName,
                startSequenceNumber, committedSequenceNumber,
                tupleCount, uniqueId, buffer);
    }

    @Override
    public void pushBuffer(
            int partitionId,
            String tableName,
            long startSequenceNumber,
            long committedSequenceNumber,
            long tupleCount,
            long uniqueId,
            ByteBuffer buffer) {

        try {
            Generation generation = getGeneration();
            if (generation == null) {
                if (buffer != null) {
                    DBBPool.wrapBB(buffer).discard();
                }
                return;
            }
            generation.pushExportBuffer(partitionId, tableName,
                    startSequenceNumber, committedSequenceNumber,
                    (int)tupleCount, uniqueId, buffer);
        } catch (Exception e) {
            //Don't let anything take down the execution site thread
            exportLog.error("Error pushing export buffer", e);
        }
    }


    @Override
    public void updateInitialExportStateToSeqNo(int partitionId, String signature,
                                                StreamStartAction action,
                                                Map<Integer, ExportSnapshotTuple> sequenceNumberPerPartition) {
        ExportGeneration generation = m_generation.get();
        if (generation != null) {
            generation.updateInitialExportStateToSeqNo(partitionId, signature,
                                                       action,
                                                       sequenceNumberPerPartition);
        }
    }

    public void updateDanglingExportStates(StreamStartAction action,
            Map<String, Map<Integer, ExportSnapshotTuple>> exportSequenceNumbers) {
        ExportGeneration generation = m_generation.get();
        if (generation != null) {
            generation.updateDanglingExportStates(action, exportSequenceNumbers);
        }
    }

    // FIXME: Review synchronization
    @Override
    public synchronized void sync() {
        if (exportLog.isDebugEnabled()) {
            exportLog.debug("Syncing export data");
        }
        syncSources();
    }

    private static void syncSources() {

        Generation generation = ExportManagerInterface.instance().getGeneration();
        if (generation != null) {
            generation.sync();
        }
    }

    @Override
    public ExportStats getExportStats() {
        return m_exportStats;
    }

    @Override
    public Generation getGeneration() {
        return m_generation.get();
    }

    @Override
    public void processStreamControl(String exportStream, List<String> exportTargets, OperationMode operation, VoltTable results) {
        if (m_generation.get() != null) {
           m_generation.get().processStreamControl(exportStream, exportTargets, operation, results);
        }
    }

    @Override
    public void clientInterfaceStarted(ClientInterface clientInterface) {
        m_ci = clientInterface;
        m_adapter = new SimpleClientResponseAdapter(ClientInterface.MIGRATE_ROWS_DELETE_CID,
                                                    "MigrateRowsAdapter");
        m_ci.bindAdapter(m_adapter, null);
    }

    @Override
    public void invokeMigrateRowsDelete(int partition, String tableName, long deletableTxnId,  ProcedureCallback cb) {
        m_ci.getDispatcher().getInternelAdapterNT().callProcedure(m_ci.getInternalUser(), true, TTLManager.NT_PROC_TIMEOUT, cb,
                "@MigrateRowsDeleterNT", new Object[] {partition, tableName, deletableTxnId});
    }
}
