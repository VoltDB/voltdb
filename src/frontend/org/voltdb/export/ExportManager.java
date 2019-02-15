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
import java.util.NoSuchElementException;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import org.voltcore.logging.Level;
import org.voltcore.logging.VoltLogger;
import org.voltcore.messaging.HostMessenger;
import org.voltcore.utils.DBBPool;
import org.voltcore.utils.Pair;
import org.voltdb.CatalogContext;
import org.voltdb.ExportStatsBase;
import org.voltdb.ExportStatsBase.ExportStatsRow;
import org.voltdb.RealVoltDB;
import org.voltdb.StatsSelector;
import org.voltdb.VoltDB;
import org.voltdb.VoltTable;
import org.voltdb.catalog.CatalogMap;
import org.voltdb.catalog.Cluster;
import org.voltdb.catalog.Connector;
import org.voltdb.catalog.ConnectorProperty;
import org.voltdb.catalog.ConnectorTableInfo;
import org.voltdb.catalog.Database;
import org.voltdb.sysprocs.ExportControl.OperationMode;
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
     * Master sends GIVE_MASTERSHIP to one replica to transfer leadership.
     */
    public static final byte GIVE_MASTERSHIP = 2;

    /**
     * Master sends GAP_QUERY to all nodes to know: can you cover the next sequence number?
     *
     * This is called when master hits gap in the stream.
     */
    public static final byte GAP_QUERY = 3;

    /**
     * Node that receives GAP_QUERY sends back QUERY_RESPONSE with the information that whether
     * it has data for the next sequence number.
     */
    public static final byte QUERY_RESPONSE = 4;

    /**
     * Data sources under new SPI or SPI who receives failed host notification
     * sends TASK_MASTERSHIP to all nodes to ask master to transfer leadership back.
     * If master doesn't exist promote itself to be master.
     */
    public static final byte TAKE_MASTERSHIP = 5;

    /**
     * Node that receives TAKE_MASTERSHIP sends back TAKE_MASTERSHIP_RESPONSE to indicate
     * it's not master.
     */
    public static final byte TAKE_MASTERSHIP_RESPONSE = 6;

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
    private ExportStats m_exportStats;
    private final int m_hostId;

    // this used to be flexible, but no longer - now m_loaderClass is just null or default value
    public static final String DEFAULT_LOADER_CLASS = "org.voltdb.export.processors.GuestProcessor";
    private final String m_loaderClass = DEFAULT_LOADER_CLASS;

    private volatile Map<String, Pair<Properties, Set<String>>> m_processorConfig = new HashMap<>();

    private int m_exportTablesCount = 0;
    private int m_connCount = 0;
    private boolean m_startPolling = false;


    public class ExportStats extends ExportStatsBase {
        List<ExportStatsRow> m_stats;

        ExportStats() {
            super();
        }

        @Override
        public Iterator<Object> getStatsRowKeyIterator(boolean interval) {
            m_stats = getStats(interval);
            return buildIterator();
        }

        private Iterator<Object> buildIterator() {
            return new Iterator<Object>() {
                int index = 0;

                @Override
                public boolean hasNext() {
                    return index < m_stats.size();
                }

                @Override
                public Object next() {
                    if (index < m_stats.size()) {
                        return index++;
                    }
                    throw new NoSuchElementException();
                }

                @Override
                public void remove() {
                    throw new UnsupportedOperationException();
                }

            };
        }

        @Override
        protected void updateStatsRow(Object rowKey, Object rowValues[]) {
            super.updateStatsRow(rowKey, rowValues);
            int rowIndex = (Integer) rowKey;
            assert (rowIndex >= 0);
            assert (rowIndex < m_stats.size());
            ExportStatsRow stat = m_stats.get(rowIndex);
            rowValues[columnNameToIndex.get(Columns.SITE_ID)] = stat.m_siteId;
            rowValues[columnNameToIndex.get(Columns.PARTITION_ID)] = stat.m_partitionId;
            rowValues[columnNameToIndex.get(Columns.SOURCE_NAME)] = stat.m_sourceName;
            rowValues[columnNameToIndex.get(Columns.EXPORT_TARGET)] = stat.m_exportTarget;
            rowValues[columnNameToIndex.get(Columns.ACTIVE)] = stat.m_exportingRole;
            rowValues[columnNameToIndex.get(Columns.TUPLE_COUNT)] = stat.m_tupleCount;
            rowValues[columnNameToIndex.get(Columns.TUPLE_PENDING)] = stat.m_tuplesPending;
            rowValues[columnNameToIndex.get(Columns.LAST_QUEUED_TIMESTAMP)] = stat.m_lastQueuedTimestamp;
            rowValues[columnNameToIndex.get(Columns.LAST_ACKED_TIMESTAMP)] = stat.m_lastAckedTimestamp;
            rowValues[columnNameToIndex.get(Columns.AVERAGE_LATENCY)] = stat.m_averageLatency;
            rowValues[columnNameToIndex.get(Columns.MAX_LATENCY)] = stat.m_maxLatency;
            rowValues[columnNameToIndex.get(Columns.QUEUE_GAP)] = stat.m_queueGap;
            rowValues[columnNameToIndex.get(Columns.STATUS)] = stat.m_status;
        }

        public ExportStatsRow getStatsRow(Object rowKey) {
            int rowIndex = (Integer) rowKey;
            assert (rowIndex >= 0);
            assert (rowIndex < m_stats.size());
            ExportStatsRow stat = m_stats.get(rowIndex);
            return stat;
        }
    }

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
            List<Pair<Integer, Integer>> partitions)
            throws ExportManager.SetupException
    {
        ExportManager em = new ExportManager(myHostId, catalogContext, messenger);
        m_self = em;
        if (forceCreate) {
            em.clearOverflowData();
        }
        em.initialize(catalogContext, partitions, isRejoin);

        RealVoltDB db=(RealVoltDB)VoltDB.instance();
        db.getStatsAgent().registerStatsSource(StatsSelector.EXPORT,
                myHostId, // m_siteId,
                em.getExportStats());
    }

    private CatalogMap<Connector> getConnectors(CatalogContext catalogContext) {
        final Cluster cluster = catalogContext.catalog.getClusters().get("cluster");
        final Database db = cluster.getDatabases().get("database");
        return db.getConnectors();
    }

    private boolean hasEnabledConnectors(CatalogMap<Connector> connectors) {
        for (Connector conn : connectors) {
            if (conn.getEnabled() && !conn.getTableinfo().isEmpty()) {
                return true;
            }
        }
        return false;
    }

    private boolean hasExportedTables(CatalogMap<Connector> connectors) {
        for (Connector conn : connectors) {
            if (!conn.getTableinfo().isEmpty()) {
                return true;
            }
        }
        return false;
    }

    private void dumpConnectors(CatalogMap<Connector> connectors) {

        if (!exportLog.isDebugEnabled()) return;
        StringBuilder sb = new StringBuilder("Connectors:\n");
        for (Connector conn : connectors) {
            sb.append("\tname:    " + conn.getTypeName() + "\n");
            sb.append("\tenabled: " + conn.getEnabled() + "\n");
            if (conn.getTableinfo().isEmpty()) {
                sb.append("\tno tables ...\n");
            }
            else {
                sb.append("\ttables:\n");
                for (ConnectorTableInfo ti : conn.getTableinfo()) {
                    sb.append("\t\t table name: " + ti.getTypeName() + "\n");
                }
            }
        }
        exportLog.debug(sb.toString());
    }
    /**
     * Indicate to associated {@link ExportGeneration}s to become
     * masters for the given partition id
     * @param partitionId
     */
    synchronized public void takeMastership(int partitionId) {
        m_masterOfPartitions.add(partitionId);
        ExportGeneration generation = m_generation.get();
        if (generation == null) {
            return;
        }
        generation.takeMastership(partitionId);
    }

    /**
     * Indicate local partition became the SPI Leader
     * still waiting for old leader (ack) to trigger take over mastership
     * @param partitionId
     */
    synchronized public void prepareAcceptMastership(int partitionId) {
        // can't acquire mastership twice for the same partition id
        if (!m_masterOfPartitions.add(partitionId)) {
            return;
        }
        if (exportLog.isDebugEnabled()) {
            exportLog.debug("Export streams on local partition " + partitionId + " will become master.");
        }
    }

    /**
     * Indicate to associated {@link ExportGeneration}s to
     * prepare give up mastership for the given partition id to hostId
     * @param partitionId
     */
    synchronized public void prepareTransferMastership(int partitionId, int hostId) {
        // remove mastership for partition id, so when failure happen during the mastership transfer
        // this node can be elected as new master again.
        m_masterOfPartitions.remove(partitionId);

        if (exportLog.isDebugEnabled()) {
            exportLog.debug("Export stream masters on " + partitionId + " are going to migrate away");
        }
        ExportGeneration generation = m_generation.get();
        if (generation == null) {
            return;
        }
        generation.prepareTransferMastership(partitionId, hostId);
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
     */
    private ExportManager(
            int myHostId,
            CatalogContext catalogContext,
            HostMessenger messenger)
    throws ExportManager.SetupException
    {
        m_hostId = myHostId;
        m_messenger = messenger;
        m_exportStats = new ExportStats();

        boolean compress = !Boolean.getBoolean(StreamBlockQueue.EXPORT_DISABLE_COMPRESSION_OPTION);
        exportLog.info("Export has compression "
                + (compress ? "enabled" : "disabled") + " in " + VoltDB.instance().getExportOverflowPath());

        CatalogMap<Connector> connectors = getConnectors(catalogContext);
        if (!hasEnabledConnectors(connectors)) {
            exportLog.info("System is not using any export functionality or connectors configured are disabled.");
            return;
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

    public synchronized void startPolling(CatalogContext catalogContext) {
        m_startPolling = true;

        CatalogMap<Connector> connectors = getConnectors(catalogContext);
        if(!hasEnabledConnectors(connectors)) {
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

    public int getExportTablesCount() {
        return m_exportTablesCount;
    }

    public int getConnCount() {
        return m_connCount;
    }

    /** Creates the initial export processor if export is enabled */
    private void initialize(CatalogContext catalogContext, List<Pair<Integer, Integer>> localPartitionsToSites,
            boolean isRejoin) {
        try {
            CatalogMap<Connector> connectors = getConnectors(catalogContext);
            if (exportLog.isDebugEnabled()) {
                exportLog.debug("initialize for " + connectors.size() + " connectors.");
                dumpConnectors(connectors);
            }
            if (!hasExportedTables(connectors)) {
                return;
            }

            if (exportLog.isDebugEnabled()) {
                exportLog.debug("Creating processor " + m_loaderClass);
            }
            ExportDataProcessor newProcessor = getNewProcessorWithProcessConfigSet(m_processorConfig);
            m_processor.set(newProcessor);

            File exportOverflowDirectory = new File(VoltDB.instance().getExportOverflowPath());
            ExportGeneration generation = new ExportGeneration(exportOverflowDirectory);
            generation.initialize(m_messenger, m_hostId, catalogContext,
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

    public synchronized void updateCatalog(CatalogContext catalogContext, boolean requireCatalogDiffCmdsApplyToEE,
            boolean requiresNewExportGeneration, List<Pair<Integer, Integer>> localPartitionsToSites)
    {
        final CatalogMap<Connector> connectors = getConnectors(catalogContext);

        if (exportLog.isDebugEnabled()) {
            exportLog.debug("UpdateCatalog: requiresNewGeneration: " + requiresNewExportGeneration
                    + ", for " + connectors.size() + " connectors.");
            dumpConnectors(connectors);
        }

        // Update processor config: note that we want to run a generation update even if the
        // processor config has no changes; we still need to handle changes in the exported tables
        updateProcessorConfig(connectors);

        if (!requiresNewExportGeneration) {
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
                ExportGeneration gen = new ExportGeneration(exportOverflowDirectory);
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
                        connectors, newProcessor, m_hostId, m_messenger, localPartitionsToSites);
                if (exportLog.isDebugEnabled()) {
                    exportLog.debug("Creating connector " + m_loaderClass);
                }
                newProcessor.setExportGeneration(generation);
                if (m_startPolling && !m_processorConfig.isEmpty()) {
                    newProcessor.startPolling();
                }
                newProcessor.readyForData();

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
                    generation.acceptMastership(partitionId);
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
        generation.unacceptMastership();
        if (exportLog.isDebugEnabled()) {
            exportLog.debug("Existing export datasources unassigned.");
        }
        try {
            ExportDataProcessor newProcessor = getNewProcessorWithProcessConfigSet(config);
            //Load any missing tables.
            generation.initializeGenerationFromCatalog(catalogContext, connectors, newProcessor,
                    m_hostId, m_messenger, partitions);
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
            generation.acceptMastership(partitionId);
        }
    }

    private  ExportDataProcessor getNewProcessorWithProcessConfigSet(Map<String, Pair<Properties, Set<String>>> config) throws ClassNotFoundException, InstantiationException, IllegalAccessException {
        final Class<?> loaderClass = Class.forName(m_loaderClass);
        ExportDataProcessor newProcessor = (ExportDataProcessor)loaderClass.newInstance();
        newProcessor.addLogger(exportLog);
        newProcessor.setProcessorConfig(config);
        return newProcessor;
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

    private List<ExportStatsRow> getStats(final boolean interval) {
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
     * This method pulls double duty as a means of pushing export buffers
     * and "syncing" export data to disk. Syncing doesn't imply fsync, it just means
     * writing the data to a file instead of keeping it all in memory.
     * End of stream indicates that no more data is coming from this source
     * for this generation.
     */
    public static void pushEndOfStream(
            int partitionId,
            String signature) {
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
            long startSequenceNumber,
            long tupleCount,
            long uniqueId,
            long bufferPtr,
            ByteBuffer buffer,
            boolean sync) {
        //For validating that the memory is released
        if (bufferPtr != 0) DBBPool.registerUnsafeMemory(bufferPtr);
        ExportManager instance = instance();
        try {
            ExportGeneration generation = instance.m_generation.get();
            if (generation == null) {
                if (buffer != null) {
                    DBBPool.wrapBB(buffer).discard();
                }
                return;
            }
            generation.pushExportBuffer(partitionId, signature, startSequenceNumber,
                    (int)tupleCount, uniqueId, buffer, sync);
        } catch (Exception e) {
            //Don't let anything take down the execution site thread
            exportLog.error("Error pushing export buffer", e);
        }
    }

    public void updateInitialExportStateToSeqNo(int partitionId, String signature,
                                                boolean isRecover, boolean isRejoin,
                                                Map<Integer, Pair<Long, Long>> sequenceNumberPerPartition,
                                                boolean isLowestSite) {
        //If the generation was completely drained, wait for the task to finish running
        //by waiting for the permit that will be generated
        ExportGeneration generation = m_generation.get();
        if (generation != null) {
            generation.updateInitialExportStateToSeqNo(partitionId, signature,
                                                       isRecover, isRejoin,
                                                       sequenceNumberPerPartition, isLowestSite);
        }
    }

    public static synchronized void sync(final boolean nofsync) {
        if (exportLog.isDebugEnabled()) {
            exportLog.debug("Syncing export data");
        }
        ExportGeneration generation = instance().m_generation.get();
        if (generation != null) {
            generation.sync(nofsync);
        }
    }

    public ExportStats getExportStats() {
        return m_exportStats;
    }

    public void processStreamControl(String exportStream, List<String> exportTargets, OperationMode operation, VoltTable results) {
        if (m_generation.get() != null) {
           m_generation.get().processStreamControl(exportStream, exportTargets, operation, results);
        }
    }
}
