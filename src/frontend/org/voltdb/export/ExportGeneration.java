/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
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
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper_voltpatches.AsyncCallback;
import org.apache.zookeeper_voltpatches.CreateMode;
import org.apache.zookeeper_voltpatches.KeeperException;
import org.apache.zookeeper_voltpatches.WatchedEvent;
import org.apache.zookeeper_voltpatches.Watcher;
import org.apache.zookeeper_voltpatches.ZooDefs.Ids;
import org.voltcore.logging.Level;
import org.voltcore.logging.VoltLogger;
import org.voltcore.messaging.BinaryPayloadMessage;
import org.voltcore.messaging.HostMessenger;
import org.voltcore.messaging.Mailbox;
import org.voltcore.messaging.VoltMessage;
import org.voltcore.utils.CoreUtils;
import org.voltcore.utils.DBBPool.BBContainer;
import org.voltcore.utils.EstTime;
import org.voltcore.utils.Pair;
import org.voltcore.utils.RateLimitedLogger;
import org.voltcore.zk.ZKUtil;
import org.voltdb.CatalogContext;
import org.voltdb.ExportStatsBase.ExportStatsRow;
import org.voltdb.RealVoltDB;
import org.voltdb.SnapshotCompletionMonitor.ExportSnapshotTuple;
import org.voltdb.TableType;
import org.voltdb.VoltDB;
import org.voltdb.VoltTable;
import org.voltdb.VoltZK;
import org.voltdb.catalog.CatalogMap;
import org.voltdb.catalog.Connector;
import org.voltdb.catalog.Table;
import org.voltdb.common.Constants;
import org.voltdb.export.ExportDataSource.StreamStartAction;
import org.voltdb.exportclient.ExportClientBase;
import org.voltdb.iv2.MpInitiator;
import org.voltdb.messaging.LocalMailbox;
import org.voltdb.utils.CatalogUtil;
import org.voltdb.utils.PbdSegmentName;
import org.voltdb.utils.PbdSegmentName.Result;

import com.google_voltpatches.common.collect.ImmutableList;
import com.google_voltpatches.common.collect.Sets;
import com.google_voltpatches.common.util.concurrent.Futures;
import com.google_voltpatches.common.util.concurrent.ListenableFuture;
import com.google_voltpatches.common.util.concurrent.ListeningExecutorService;

/**
 * Export data from a single catalog version and database instance.
 *
 */
public class ExportGeneration implements Generation {
    /**
     * Processors also log using this facility.
     */
    private static final VoltLogger exportLog = new VoltLogger("EXPORT");
    // Rate-limit message delivery warnings to 1 per minutes
    private static final RateLimitedLogger exportLogLimited =  new RateLimitedLogger(TimeUnit.MINUTES.toMillis(1), exportLog, Level.INFO);
    private static final RateLimitedLogger exportLogLimitedPush =  new RateLimitedLogger(TimeUnit.MINUTES.toMillis(1), exportLog, Level.INFO);

    public final File m_directory;

    private String m_mailboxesZKPath;

    /**
     * Data sources, one per table per site, provide the interface to
     * poll() and ack() Export data from the execution engines. Data sources
     * are configured by the Export manager at initialization time.
     * partitionid : <tableid : datasource>.
     */
    private final Map<Integer, Map<String, ExportDataSource>> m_dataSourcesByPartition
            =        new HashMap<Integer, Map<String, ExportDataSource>>();

    // Export generation mailboxes under the same partition id, excludes the local one.
    private Map<Integer, ImmutableList<Long>> m_replicasHSIds = new HashMap<>();

    private Mailbox m_mbox = null;

    private final HostMessenger m_messenger;

    private volatile boolean m_shutdown = false;

    private static final ListeningExecutorService m_childUpdatingThread =
            CoreUtils.getListeningExecutorService("Export ZK Watcher", 1);

    // The version of the current catalog
    public volatile int m_catalogVersion;

    private Set<Integer> m_removingPartitions = ConcurrentHashMap.newKeySet();

    /**
     * Constructor to create a new generation of export data
     * @param exportOverflowDirectory
     * @throws IOException
     */
    public ExportGeneration(File exportOverflowDirectory, HostMessenger messenger) throws IOException {
        m_directory = exportOverflowDirectory;
        m_messenger = messenger;
        if (!m_directory.canWrite()) {
            if (!m_directory.mkdirs()) {
                throw new IOException("Could not create " + m_directory);
            }
        }

        if (exportLog.isDebugEnabled()) {
            exportLog.debug("Creating new export generation.");
        }
    }

    void initialize(int hostId,
            CatalogContext catalogContext,
            final CatalogMap<Connector> connectors,
            final ExportDataProcessor processor,
            Map<Integer, Integer> localPartitionsToSites,
            File exportOverflowDirectory)
    {
        File files[] = exportOverflowDirectory.listFiles();
        if (files != null) {
            initializeGenerationFromDisk(connectors, processor, files, localPartitionsToSites, catalogContext.m_genId);
        }
        initializeGenerationFromCatalog(catalogContext, connectors, processor, hostId, localPartitionsToSites, false);
    }

    /**
     * Initialize generation from disk, creating data sources from the PBD files.
     *
     * Called immediately before calling {@code initializeGenerationFromCatalog}.
     *
     * @param connectors
     * @param messenger
     * @param processor new {@code ExportDataProcessor}, with decoders not started yet
     * @param files the contents of the export overflow directory
     * @param localPartitionsToSites
     */
    private void initializeGenerationFromDisk(final CatalogMap<Connector> connectors,
            final ExportDataProcessor processor,
            File[] files,
            Map<Integer, Integer> localPartitionsToSites,
            long genId) {

        List<Integer> onDiskPartitions = new ArrayList<Integer>();
        NavigableSet<Table> streams = CatalogUtil.getExportTablesExcludeViewOnly(connectors);
        Set<String> exportedTables = new HashSet<>();
        for (Table stream : streams) {
            exportedTables.add(stream.getTypeName());
        }
        /*
         * Find all the data files. Once one is found, extract the nonce
         * and check for any advertisements related to the data files. If
         * there are orphaned advertisements, delete them.
         */
        Map<String, File> dataFiles = new HashMap<>();
        for (File data: files) {
            if (data.getName().endsWith(".pbd")) {
                PbdSegmentName pbdName = PbdSegmentName.parseFile(exportLog, data);
                if (pbdName.m_nonce != null) {
                    String nonce = pbdName.m_nonce;
                    String streamName = getStreamNameFromNonce(nonce);
                    if (exportedTables.contains(streamName)) {
                        dataFiles.put(nonce, data);
                    } else {
                        // ENG-15740, stream can be dropped while node is offline, delete .pbd files
                        // if stream is no longer in catalog
                        data.delete();
                    }
                } else if (pbdName.m_result == Result.NOT_PBD) {
                    exportLog.warn(data.getAbsolutePath() + " is not a PBD file.");
                } else if (pbdName.m_result == Result.INVALID_NAME) {
                    exportLog.warn(data.getAbsolutePath() + " doesn't have valid PBD name.");
                }

            }
        }
        for (File ad: files) {
            if (ad.getName().endsWith(".ad")) {
                String nonce = getNonceFromAdFile(ad);
                File dataFile = dataFiles.get(nonce);
                if (dataFile != null) {
                    try {
                        addDataSource(ad, localPartitionsToSites, onDiskPartitions, processor, genId);
                    } catch (IOException e) {
                        VoltDB.crashLocalVoltDB("Error intializing export datasource " + ad, true, e);
                    }
                } else {
                    //Delete ads that have no data
                    ad.delete();
                }
            }
        }

        // Count unique partitions only
        Set<Integer> allLocalPartitions = localPartitionsToSites.keySet();
        Set<Integer> onDIskPartitionsSet = new HashSet<Integer>(onDiskPartitions);
        onDIskPartitionsSet.removeAll(allLocalPartitions);
        // One export mailbox per node, since we only keep one generation
        if (!onDIskPartitionsSet.isEmpty()) {
            createAckMailboxesIfNeeded(onDIskPartitionsSet);
        }
    }

    /**
     * Initialize generation from catalog.
     *
     * Notes on catalog update:
     * - If present, the old {@code ExportDataProcessor} has been shut down.
     * - the new {@code ExportDataProcessor} has not started its decoders.
     *
     * @param catalogContext
     * @param connectors
     * @param processor
     * @param hostId
     * @param messenger
     * @param localPartitionsToSites
     */
    void initializeGenerationFromCatalog(CatalogContext catalogContext,
            final CatalogMap<Connector> connectors,
            final ExportDataProcessor processor,
            int hostId,
            Map<Integer, Integer> localPartitionsToSites,
            boolean isCatalogUpdate)
    {
        // Update catalog version so that datasources use this version when propagating acks
        m_catalogVersion = catalogContext.catalogVersion;
        if (exportLog.isDebugEnabled()) {
            exportLog.debug("Updating to catalog version : " + m_catalogVersion);
        }

        // Collect table names of existing datasources
        Set<String> currentTables = new HashSet<>();
        synchronized(m_dataSourcesByPartition) {
            for (Iterator<Map<String, ExportDataSource>> it = m_dataSourcesByPartition.values().iterator(); it.hasNext();) {
                Map<String, ExportDataSource> sources = it.next();
                currentTables.addAll(sources.keySet());
            }
        }
        if (exportLog.isDebugEnabled()) {
            exportLog.debug("Current tables: " + currentTables);
        }

        // Now create datasources based on the catalog (if already present will not be re-created).
        // Note that we create sources on disabled connectors.

        Set<Integer> partitionsInUse = localPartitionsToSites.keySet();

        boolean createdSources = false;
        NavigableSet<Table> streams = CatalogUtil.getExportTablesExcludeViewOnly(connectors);
        Set<String> exportedTables = new HashSet<>();
        for (Table stream : streams) {
            addDataSources(stream, hostId, localPartitionsToSites, partitionsInUse,
                    processor, catalogContext.m_genId, isCatalogUpdate);
            exportedTables.add(stream.getTypeName());
            createdSources = true;
        }

        updateStreamStatus(exportedTables);

        // Remove datasources that are not exported anymore
        for (String table : exportedTables) {
            currentTables.remove(table);
        }
        if (!currentTables.isEmpty()) {
            removeDataSources(currentTables);
        }

        //Only populate partitions in use if export is actually happening
        createAckMailboxesIfNeeded(createdSources ? partitionsInUse : new HashSet<Integer>());
    }

    // Mark a DataSource as dropped if its not present in the connectors.
    private void updateStreamStatus( Set<String> exportedTables) {
        synchronized(m_dataSourcesByPartition) {
            for (Iterator<Map<String, ExportDataSource>> it = m_dataSourcesByPartition.values().iterator(); it.hasNext();) {
                Map<String, ExportDataSource> sources = it.next();
                for (String tableName: sources.keySet()) {
                    ExportDataSource src = sources.get(tableName);
                    if (!exportedTables.contains(tableName)) {
                        src.setStatus(ExportDataSource.StreamStatus.DROPPED);
                    } else if (src.getStatus() == ExportDataSource.StreamStatus.DROPPED) {
                        src.setStatus(ExportDataSource.StreamStatus.ACTIVE);
                    }
                }
            }
        }
    }

    /**
     * Create export ack mailbox during generation initialization, do nothing if generation has already initialized.
     * @param localPartitions  locally covered partitions
     */
    private void createAckMailboxesIfNeeded(final Set<Integer> localPartitions) {
        m_mailboxesZKPath = VoltZK.exportGenerations + "/" + "mailboxes";
        if (m_mbox == null) {
            m_mbox = new LocalMailbox(m_messenger) {
                @Override
                public void deliver(VoltMessage message) {
                    if (message instanceof BinaryPayloadMessage) {
                        BinaryPayloadMessage bpm = (BinaryPayloadMessage)message;
                        ByteBuffer buf = ByteBuffer.wrap(bpm.m_payload);
                        final byte msgType = buf.get();
                        final int partition = buf.getInt();
                        final Map<String, ExportDataSource> partitionSources = m_dataSourcesByPartition.get(partition);

                        final int length = buf.getInt();
                        byte stringBytes[] = new byte[length];
                        buf.get(stringBytes);
                        String tableName = new String(stringBytes, Constants.UTF8ENCODING);
                        if (partitionSources == null) {
                            if (!m_removingPartitions.contains(partition)) {
                                exportLogLimited.log("Received an export message " + msgType + " for partition " + partition +
                                        " which does not exist on this node: this should be a transient condition.",
                                        EstTime.currentTimeMillis());
                            }
                            return;
                        }
                        final ExportDataSource eds = partitionSources.get(tableName);
                        if (eds == null) {
                            exportLogLimited.log("Received export message " + msgType + " for partition "
                                    + partition + " source " + tableName +
                                    " which does not exist on this node: this should be a transient condition."
                                    + " Sources = " + partitionSources,
                                    EstTime.currentTimeMillis());
                            return;
                        }

                        if (msgType == ExportManager.RELEASE_BUFFER) {
                            final long seqNo = buf.getLong();
                            final long generationIdCreated = buf.getLong();
                            try {
                                if (generationIdCreated < eds.getGenerationIdCreated()) {
                                    if (exportLog.isDebugEnabled()) {
                                        exportLog.debug("Ignored stale RELEASE_BUFFER message for " + eds.toString() +
                                                " , sequence number: " + seqNo + ", generationIdCreated: " + generationIdCreated +
                                                " from " + CoreUtils.hsIdToString(message.m_sourceHSId) +
                                                " to " + CoreUtils.hsIdToString(m_mbox.getHSId()));
                                    }
                                    return;
                                }
                                if (exportLog.isDebugEnabled()) {
                                    exportLog.debug("Received RELEASE_BUFFER message for " + eds.toString() +
                                            " , sequence number: " + seqNo + ", generationIdCreated: " + generationIdCreated +
                                            " from " + CoreUtils.hsIdToString(message.m_sourceHSId) +
                                            " to " + CoreUtils.hsIdToString(m_mbox.getHSId()));
                                }
                                eds.remoteAck(seqNo);
                            } catch (RejectedExecutionException ignoreIt) {
                                // ignore it: as it is already shutdown
                            }
                        } else {
                            exportLog.error("Received unsupported message type " + message + " in export subsystem");
                        }
                    } else {
                        exportLog.error("Received unexpected message " + message + " in export subsystem");
                    }
                }
            };
            m_messenger.createMailbox(null, m_mbox);
        }

        // Rejoining node may receives gap query message before childUpdating thread gets back result,
        // in case it couldn't find local mailbox to send back response, add local mailbox to the list first.
        for (Integer partition : localPartitions) {
            updateAckMailboxes(partition, null);
        }
        // Update latest replica list to each data source.
        updateReplicaList(localPartitions);
    }

    // Access by multiple threads
    public void updateAckMailboxes(int partition, Set<Long> newHSIds) {
        ImmutableList<Long> replicaHSIds = m_replicasHSIds.get(partition);
        synchronized (m_dataSourcesByPartition) {
            Map<String, ExportDataSource> partitionMap = m_dataSourcesByPartition.get(partition);
            if (partitionMap == null) {
                return;
            }
            for( ExportDataSource eds: partitionMap.values()) {
                eds.updateAckMailboxes(Pair.of(m_mbox, replicaHSIds));
            }
        }
    }

    private void removeMailbox(int partition) {
        final String partitionDN =  m_mailboxesZKPath + "/" + partition;
        m_removingPartitions.add(partition);
        AsyncCallback.VoidCallback cb = new ZKUtil.VoidCallback() {
            @Override
            public void processResult(int rc, String path, Object ctx) {
                super.processResult(rc, path, ctx);
                // Now the mailbox is gone
                m_removingPartitions.remove(partition);
            }
        };
        m_messenger.getZK().delete(partitionDN + "/" + m_mbox.getHSId(), -1, cb, null);
        m_replicasHSIds.remove(partition);
    }

    private void updateReplicaList(Set<Integer> newPartitions) {
        //If we have new partitions create mailbox paths.
        for (Integer partition : newPartitions) {
            final String partitionDN =  m_mailboxesZKPath + "/" + partition;
            ZKUtil.asyncMkdirs(m_messenger.getZK(), partitionDN);

            ZKUtil.StringCallback cb = new ZKUtil.StringCallback();
            m_messenger.getZK().create(
                    partitionDN + "/" + m_mbox.getHSId(),
                    null,
                    Ids.OPEN_ACL_UNSAFE,
                    CreateMode.EPHEMERAL,
                    cb,
                    null);
        }

        ListenableFuture<?> fut = m_childUpdatingThread.submit(new Runnable() {
            @Override
            public void run() {
                List<Pair<Integer,ZKUtil.ChildrenCallback>> callbacks =
                        new ArrayList<Pair<Integer, ZKUtil.ChildrenCallback>>();
                for (Integer partition : newPartitions) {
                    ZKUtil.ChildrenCallback callback = new ZKUtil.ChildrenCallback();
                    m_messenger.getZK().getChildren(
                            m_mailboxesZKPath + "/" + partition,
                            constructMailboxChildWatcher(),
                            callback,
                            null);
                    callbacks.add(Pair.of(partition, callback));
                }
                for (Pair<Integer, ZKUtil.ChildrenCallback> p : callbacks) {
                    final Integer partition = p.getFirst();
                    List<String> children = null;
                    try {
                        children = p.getSecond().get();
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    } catch (KeeperException e) {
                        throw new RuntimeException(e);
                    }
                    ImmutableList.Builder<Long> mailboxes = ImmutableList.builder();

                    for (String child : children) {
                        if (child.equals(Long.toString(m_mbox.getHSId()))) {
                            continue;
                        }
                        mailboxes.add(Long.valueOf(child));
                    }
                    ImmutableList<Long> mailboxHsids = mailboxes.build();
                    m_replicasHSIds.put(partition, mailboxHsids);
                    updateAckMailboxes(partition, null);
                }
            }
        });

        try {
            fut.get();
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    private Watcher constructMailboxChildWatcher() {
        if (m_shutdown) {
            return null;
        }
        return new Watcher() {

            @Override
            public void process(final WatchedEvent event) {
                m_childUpdatingThread.submit(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            handleChildUpdate(event);
                        } catch (Throwable t) {
                            VoltDB.crashLocalVoltDB("Error in export ack handling", true, t);
                        }
                    }
                });
            }

        };
    }

    private void handleChildUpdate(final WatchedEvent event) {
        if (m_shutdown) {
            return;
        }
        m_messenger.getZK().getChildren(event.getPath(),
                constructMailboxChildWatcher(),
                constructChildRetrievalCallback(),
                null);
    }

    private AsyncCallback.ChildrenCallback constructChildRetrievalCallback() {
        if (m_shutdown) {
            return null;
        }
        return new AsyncCallback.ChildrenCallback() {
            @Override
            public void processResult(final int rc, final String path, Object ctx,
                    final List<String> children) {
                m_childUpdatingThread.submit(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            if (m_shutdown) {
                                return;
                            }
                            KeeperException.Code code = KeeperException.Code.get(rc);
                            //Other node must have drained so ignore.
                            if (code == KeeperException.Code.NONODE) {
                                if (exportLog.isDebugEnabled()) {
                                    exportLog.debug("Path not found generation drain most likely finished on other node: " + path);
                                }
                                //Fallthrough to rebuild the mailboxes.
                            } else if (code != KeeperException.Code.OK) {
                                throw KeeperException.create(code);
                            }
                            final String split[] = path.split("/");
                            final int partition = Integer.valueOf(split[split.length - 1]);
                            ImmutableList<Long> existingReplicas = m_replicasHSIds.get(partition);
                            if (existingReplicas == null) {
                                // Dangling data source is drained and removed from datasource map.
                                // This host no longer accepts/sends message for this partition.
                                return;
                            }
                            if (exportLog.isDebugEnabled()) {
                                exportLog.debug("Process children change: " + path);
                            }
                            ImmutableList.Builder<Long> mailboxes = ImmutableList.builder();
                            for (String child : children) {
                                if (child.equals(Long.toString(m_mbox.getHSId()))) {
                                    continue;
                                }
                                mailboxes.add(Long.valueOf(child));
                            }
                            ImmutableList<Long> mailboxHsids = mailboxes.build();
                            Set<Long> newHSIds = Sets.difference(new HashSet<Long>(mailboxHsids),
                                    new HashSet<Long>(existingReplicas));
                            if (exportLog.isDebugEnabled()) {
                                Set<Long> removedHSIds = Sets.difference(new HashSet<Long>(m_replicasHSIds.get(partition)),
                                        new HashSet<Long>(mailboxHsids));
                                exportLog.debug("Current export generation added mailbox: " + CoreUtils.hsIdCollectionToString(newHSIds) +
                                        ", removed mailbox: " + CoreUtils.hsIdCollectionToString(removedHSIds));
                            }
                            m_replicasHSIds.put(partition, mailboxHsids);
                            updateAckMailboxes(partition, newHSIds);
                        } catch (Throwable t) {
                            VoltDB.crashLocalVoltDB("Error in export ack handling", true, t);
                        }
                    }
                });
            }

        };
    }

    @Override
    public List<ExportStatsRow> getStats(boolean interval) {
        List<ListenableFuture<ExportStatsRow>> tasks = new ArrayList<ListenableFuture<ExportStatsRow>>();
        Map<Integer, Map<String, ExportDataSource>> dataSourcesByPartition
            = new HashMap<Integer, Map<String, ExportDataSource>>();
        synchronized(m_dataSourcesByPartition) {
            dataSourcesByPartition.putAll(m_dataSourcesByPartition);
        }
        for (Map<String, ExportDataSource> dataSources : dataSourcesByPartition.values()) {
            for (ExportDataSource source : dataSources.values()) {
                ListenableFuture<ExportStatsRow> syncFuture = source.getImmutableStatsRow(interval);
                if (syncFuture != null) {
                    tasks.add(syncFuture);
                }
            }
        }

        try {
            if (!tasks.isEmpty()) {
                return Futures.allAsList(tasks).get();
            }
        } catch (Exception e) {
            exportLog.error("Unexpected exception syncing export data during snapshot save.", e);
        }
        return new ArrayList<>();
    }

    /*
     * Create a datasource based on an ad file
     */
    private void addDataSource(File adFile,
            Map<Integer, Integer> localPartitionsToSites,
            List<Integer> adFilePartitions,
            final ExportDataProcessor processor,
            final long genId) throws IOException {
        ExportDataSource source = new ExportDataSource(this, adFile, localPartitionsToSites, processor, genId);
        source.setCoordination(m_messenger.getZK(), m_messenger.getHostId());
        adFilePartitions.add(source.getPartitionId());

        // Setup delete for migrate table
        if (CatalogUtil.isPersistentMigrate(source.getTableName())) {
            source.setupMigrateRowsDeleter(
                    CatalogUtil.getIsreplicated(source.getTableName()) ? MpInitiator.MP_INIT_PID : source.getPartitionId());
        }
        if (exportLog.isDebugEnabled()) {
            exportLog.debug("Creating " + source.toString() + " for " + adFile + " bytes " + source.sizeInBytes());
        }
        synchronized(m_dataSourcesByPartition) {
            Map<String, ExportDataSource> dataSourcesForPartition = m_dataSourcesByPartition.get(source.getPartitionId());
            if (dataSourcesForPartition == null) {
                dataSourcesForPartition = new HashMap<String, ExportDataSource>();
                m_dataSourcesByPartition.put(source.getPartitionId(), dataSourcesForPartition);
            } else {
                if (dataSourcesForPartition.get(source.getTableName()) != null) {
                    exportLog.warn("On Disk generation with same table, partition already exists using known data source.");
                    return;
                }
            }
            dataSourcesForPartition.put(source.getTableName(), source);
        }
    }

    // silly helper to add datasources for a table catalog object
    /**
     * Add datasources for a catalog table in all partitions
     *
     * @param table
     * @param hostId
     * @param localPartitionsToSites
     * @param partitionsInUse
     * @param processor
     */
    private void addDataSources(Table table, int hostId,
            Map<Integer, Integer> localPartitionsToSites,
            Set<Integer> partitionsInUse,
            final ExportDataProcessor processor,
            final long genId,
            boolean isCatalogUpdate)
    {
        for (Map.Entry<Integer, Integer> partitionAndSiteId : localPartitionsToSites.entrySet()) {

            /*
             * IOException can occur if there is a problem
             * with the persistent aspects of the datasource storage
             */
            int partition = partitionAndSiteId.getKey();
            int siteId = partitionAndSiteId.getValue();
            synchronized(m_dataSourcesByPartition) {
                try {
                    Map<String, ExportDataSource> dataSourcesForPartition = m_dataSourcesByPartition.get(partition);
                    if (dataSourcesForPartition == null) {
                        dataSourcesForPartition = new HashMap<String, ExportDataSource>();
                        m_dataSourcesByPartition.put(partition, dataSourcesForPartition);
                    }
                    final String key = table.getTypeName();
                    if (!dataSourcesForPartition.containsKey(key)) {
                        // Create a new EDS, discarding any pre-existing data
                        ExportDataSource exportDataSource = new ExportDataSource(this,
                                processor,
                                key,
                                partition,
                                siteId,
                                genId,
                                table.getColumns(),
                                table.getPartitioncolumn(),
                                m_directory.getPath());
                        exportDataSource.setCoordination(m_messenger.getZK(), m_messenger.getHostId());

                        // Setup delete for migrate table
                        if (TableType.isPersistentMigrate(table.getTabletype())) {
                            exportDataSource.setupMigrateRowsDeleter(table.getIsreplicated() ? MpInitiator.MP_INIT_PID : exportDataSource.getPartitionId());
                        }
                        if (exportLog.isDebugEnabled()) {
                            exportLog.debug("Creating ExportDataSource for table in catalog " + key
                                    + " partition " + partition + " site " + siteId);
                        }
                        dataSourcesForPartition.put(key, exportDataSource);
                        if (isCatalogUpdate) {
                            exportDataSource.updateCatalog(table, genId);
                        }
                    } else {
                        // Associate any existing EDS to the export client in the new processor
                        ExportDataSource eds = dataSourcesForPartition.get(key);

                        ExportClientBase client = processor.getExportClient(key);
                        if (client != null) {
                            // Associate to an existing export client
                            eds.setClient(client);
                            eds.setRunEveryWhere(client.isRunEverywhere());
                        } else {
                            // Reset to no export client
                            eds.setClient(null);
                            eds.setRunEveryWhere(false);
                        }

                        // Mark in catalog only if partition is in use
                        eds.markInCatalog(partitionsInUse.contains(partition));
                        if (isCatalogUpdate) {
                            eds.updateCatalog(table, genId);
                        }
                    }
                } catch (IOException e) {
                    VoltDB.crashLocalVoltDB(
                            "Error creating datasources for table " +
                            table.getTypeName() + " host id " + hostId, true, e);
                }
            }
        }
    }

    /**
     * Close and delete the {@code ExportDataSource} instances that were dropped.
     *
     * Note that this method waits on the completion of the closeAndDelete calls on
     * the {@code ExportDataSource} instances
     *
     * @param doneTables set of table names
     */
    private void removeDataSources(Set<String> doneTables) {

        List<ExportDataSource> doneSources = new LinkedList<>();
        synchronized(m_dataSourcesByPartition) {
            for (Iterator<Map<String, ExportDataSource>> it = m_dataSourcesByPartition.values().iterator(); it.hasNext();) {

                Map<String, ExportDataSource> sources = it.next();
                for (String doneTable : doneTables) {
                    ExportDataSource eds = sources.get(doneTable);
                    if (eds == null) {
                        continue;
                    }
                    doneSources.add(eds);
                    sources.remove(doneTable);
                }
            }
        }

        //Do closings outside the synchronized block
        for (ExportDataSource source : doneSources) {
            exportLog.info("Finished processing " + source);
            VoltDB.getExportManager().onClosingSource(source.getTableName(), source.getPartitionId());
            source.closeAndDelete();
        }
    }

    /**
     * The Export Data Source reports it is drained on an unused partition.
     */
    @Override
    public void onSourceDrained(int partitionId, String tableName) {
        ExportDataSource source;
        synchronized(m_dataSourcesByPartition) {
            Map<String, ExportDataSource> sources = m_dataSourcesByPartition.get(partitionId);

            if (sources == null) {
                if (!m_removingPartitions.contains(partitionId)) {
                     exportLog.error("Could not find export data sources for partition "
                            + partitionId + ". The export cleanup stream is being discarded.");
                }
                return;
            }

            source = sources.get(tableName);
            if (source == null) {
                exportLog.warn("Could not find export data source for signature " + partitionId +
                        " name " + tableName + ". The export cleanup stream is being discarded.");
                return;
            }

            // Remove source and partition entry if empty
            exportLog.info("Drained source for " + tableName + ", partition " + partitionId);
            sources.remove(tableName);
            if (sources.isEmpty()) {
                m_dataSourcesByPartition.remove(partitionId);
                removeMailbox(partitionId);
            }
        }

        //Do closing outside the synchronized block.
        VoltDB.getExportManager().onClosingSource(tableName, partitionId);
        source.closeAndDelete();

    }


    @Override
    public void pushExportBuffer(int partitionId, String tableName,
            long startSequenceNumber, long committedSequenceNumber,
            int tupleCount, long uniqueId, BBContainer container) {

        Map<String, ExportDataSource> sources = m_dataSourcesByPartition.get(partitionId);

        if (sources == null) {
            RealVoltDB db = (RealVoltDB)VoltDB.instance();
            if (!db.isPartitionDecommissioned(partitionId)) {
                exportLog.error("PUSH Could not find export data sources for partition "
                        + partitionId + ". The export data is being discarded.");
            }
            if (container != null) {
                container.discard();
            }
            return;
        }

        ExportDataSource source = sources.get(tableName);
        if (source == null) {
            /*
             * When dropping a stream, the EE pushes the outstanding buffers: ignore them.
             */
            exportLogLimitedPush.log("PUSH on unknown export data source for partition " + partitionId +
                    " Table " + tableName + ". The export data ("
                    + "seq: " + startSequenceNumber + ", count: " + tupleCount
                    + ") is being discarded.",
                    EstTime.currentTimeMillis());

            if (container != null) {
                container.discard();
            }
            return;
        }

        source.pushExportBuffer(startSequenceNumber, committedSequenceNumber,
                tupleCount, uniqueId, container);
    }

    private void cleanup() {
        m_shutdown = true;
        //We need messenger NULL guard for tests.
        if (m_mbox != null && m_messenger != null) {
            synchronized(m_dataSourcesByPartition) {
                for (Integer partition : m_dataSourcesByPartition.keySet()) {
                    final String partitionDN =  m_mailboxesZKPath + "/" + partition;
                    String path = partitionDN + "/" + m_mbox.getHSId();
                    try {
                        m_messenger.getZK().delete(path, 0);
                    } catch (InterruptedException ex) {
                        ;
                    } catch (KeeperException ex) {
                        ;
                    }
                }
            }
            m_messenger.removeMailbox(m_mbox);
        }
    }

    @Override
    public void updateInitialExportStateToSeqNo(int partitionId, String streamName,
                                                StreamStartAction action,
                                                Map<Integer, ExportSnapshotTuple> sequenceNumberPerPartition) {
        // pre-iv2, the truncation point is the snapshot transaction id.
        // In iv2, truncation at the per-partition txn id recorded in the snapshot.
        List<ListenableFuture<?>> tasks = new ArrayList<>();
        Map<String, ExportDataSource> dataSource = m_dataSourcesByPartition.get(partitionId);
        // It is possible that for restore the partitions have changed, in which case what we are doing is silly
        if (dataSource != null) {
            ExportDataSource source = dataSource.get(streamName);
            if (source != null) {
                ExportSnapshotTuple sequences = sequenceNumberPerPartition.get(partitionId);
                if (sequences != null) {
                    ListenableFuture<?> task = source.truncateExportToSeqNo(action, sequences.getSequenceNumber(), sequences.getGenerationId());
                    tasks.add(task);
                }
            }
        }
        try {
            if (!tasks.isEmpty()) {
                Futures.allAsList(tasks).get();
            }
        } catch (Exception e) {
            VoltDB.crashLocalVoltDB("Unexpected exception truncating export data during snapshot restore. " +
                                    "You can back up export overflow data and start the " +
                                    "DB without it to get past this error", true, e);
        }
    }

    @Override
    public void updateDanglingExportStates(StreamStartAction action,
            Map<String, Map<Integer, ExportSnapshotTuple>> exportSequenceNumbers) {
        List<ListenableFuture<?>> tasks = new ArrayList<>();
        synchronized(m_dataSourcesByPartition) {
            for (Map<String, ExportDataSource> dataSources : m_dataSourcesByPartition.values()) {
                for (ExportDataSource source : dataSources.values()) {
                    if (!source.inCatalog()) {
                        Map<Integer, ExportSnapshotTuple> sequenceNumberPerPartition = exportSequenceNumbers.get(source.getTableName());
                        if (sequenceNumberPerPartition == null) {
                            exportLog.warn("Could not find export sequence number for table " + source.getTableName() +
                                    ". This warning is safe to ignore if you are loading a pre 1.3 snapshot" +
                                    " which would not contain these sequence numbers (added in 1.3)." +
                                    " If this is a post 1.3 snapshot then the restore has failed and export sequence " +
                                    " are reset to 0");
                            continue;
                        }
                        ExportSnapshotTuple sequences = sequenceNumberPerPartition.get(source.getPartitionId());
                        if (sequences != null) {
                            if (exportLog.isDebugEnabled()) {
                                exportLog.debug("Updating dangling export " + source);
                            }
                            ListenableFuture<?> task = source.truncateExportToSeqNo(action, sequences.getSequenceNumber(), sequences.getGenerationId());
                            tasks.add(task);
                        } else {
                            exportLog.warn("Could not find an export sequence number for table " + source.getTableName() +
                                    " partition " + source.getPartitionId() +
                                    ". This warning is safe to ignore if you are loading a pre 1.3 snapshot " +
                                    " which would not contain these sequence numbers (added in 1.3)." +
                                    " If this is a post 1.3 snapshot then the restore has failed and export sequence " +
                                    " are reset to 0");
                            continue;
                        }
                    }
                }
            }
        }
        try {
            if (!tasks.isEmpty()) {
                Futures.allAsList(tasks).get();
            }
        } catch (Exception e) {
            VoltDB.crashLocalVoltDB("Unexpected exception truncating export data during snapshot restore. " +
                    "You can back up export overflow data and start the " +
                    "DB without it to get past this error", true, e);
        }
    }

    @Override
    public void sync() {
        List<ListenableFuture<?>> tasks = new ArrayList<ListenableFuture<?>>();
        synchronized(m_dataSourcesByPartition) {
            for (Map<String, ExportDataSource> dataSources : m_dataSourcesByPartition.values()) {
                for (ExportDataSource source : dataSources.values()) {
                    ListenableFuture<?> syncFuture = source.sync();
                    if (syncFuture != null) {
                        tasks.add(syncFuture);
                    }
                }
            }
        }

        try {
            if (!tasks.isEmpty()) {
                Futures.allAsList(tasks).get();
            }
        } catch (Exception e) {
            exportLog.error("Unexpected exception syncing export data during snapshot save.", e);
        }
    }

    @Override
    public void shutdown() {
        List<ListenableFuture<?>> tasks = new ArrayList<ListenableFuture<?>>();
        synchronized(m_dataSourcesByPartition) {
            for (Map<String, ExportDataSource> sources : m_dataSourcesByPartition.values()) {
                for (ExportDataSource source : sources.values()) {
                    tasks.add(source.shutdown());
                }
            }
        }
        try {
            if (!tasks.isEmpty()) {
                Futures.allAsList(tasks).get();
            }
        } catch (Exception e) {
            exportLog.error("Unexpected exception shutting down export data.", e);
        }
        //Do this before so no watchers gets created.
        m_shutdown = true;
        cleanup();
    }

    /**
     * Relay processor shutdown to all EDS. This is done asynchronously.
     */
    public void onProcessorShutdown() {
        synchronized(m_dataSourcesByPartition) {
            for (Map<String, ExportDataSource> partitionDataSourceMap : m_dataSourcesByPartition.values()) {
                for (ExportDataSource source : partitionDataSourceMap.values()) {
                    source.onProcessorShutdown();
                }
            }
        }
    }

    /**
     * Indicate to all associated {@link ExportDataSource} to assume
     * leadership role for the given partition id
     * @param partitionId
     */
    @Override
    public void becomeLeader(int partitionId) {
        synchronized(m_dataSourcesByPartition) {
            Map<String, ExportDataSource> partitionDataSourceMap = m_dataSourcesByPartition.get(partitionId);

            // this case happens when there are no export tables
            if (partitionDataSourceMap == null) {
                return;
            }

            for( ExportDataSource eds: partitionDataSourceMap.values()) {
                try {
                    eds.becomeLeader();
                } catch (Exception e) {
                    exportLog.error("Unable to start exporting", e);
                }
            }
        }
    }

    @Override
    public Map<Integer, Map<String, ExportDataSource>> getDataSourceByPartition() {
        return m_dataSourcesByPartition;
    }

    public void processStreamControl(String exportSource, List<String> exportTargets, StreamControlOperation operation, VoltTable results) {
        exportLog.info("Export " + operation + " source:" + exportSource + " targets:" + exportTargets);
        TreeSet<String> targets = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        targets.addAll(exportTargets);
        synchronized (m_dataSourcesByPartition) {
            for (Map.Entry<Integer, Map<String, ExportDataSource>> partitionDataSourceMap : m_dataSourcesByPartition.entrySet()) {
                Integer partition = partitionDataSourceMap.getKey();
                for (ExportDataSource source : partitionDataSourceMap.getValue().values()) {
                    if (!source.getTableName().equalsIgnoreCase(exportSource)) {
                        continue;
                    }

                    // no target match (case insensitive)
                    if (!targets.contains(source.getTarget())) {
                        continue;
                    }

                    if (source.processStreamControl(operation)) {
                        results.addRow(source.getTableName(), source.getTarget(), partition, "SUCCESS", "");
                    }
                }
            }
        }
    }

    public void closeDataSources(List<Integer> removedPartitions) {
        synchronized (m_dataSourcesByPartition) {
            for (Map.Entry<Integer, Map<String, ExportDataSource>> dataSources : m_dataSourcesByPartition.entrySet()) {
                Integer partition = dataSources.getKey();
                if (removedPartitions.contains(partition)) {
                    for (ExportDataSource source : dataSources.getValue().values()) {
                        source.closeAndDelete();
                    }
                }
            }
            m_dataSourcesByPartition.keySet().removeAll(removedPartitions);
            removedPartitions.stream().forEach(p -> removeMailbox(p));
            if (exportLog.isDebugEnabled()) {
                exportLog.info("Remaining datasources:" + m_dataSourcesByPartition);
            }
        }
    }

    @Override
    public void updateGenerationId(long genId) {
        synchronized(m_dataSourcesByPartition) {
            for (Map<String, ExportDataSource> partitionDataSourceMap : m_dataSourcesByPartition.values()) {
                for (ExportDataSource source : partitionDataSourceMap.values()) {
                    source.updateGenerationId(genId);
                }
            }
        }
    }

    // Naming convention for export pdb file: [table name]_[partition]_[segmentId]_[prevId].pdb,
    private static String getStreamNameFromNonce(String nonce) {
        // it's possible the stream name contains underscore
        return nonce.substring(0, nonce.lastIndexOf('_'));
    }

    // Naming convention for ad file, [table name]_[partition].ad
    private static String getNonceFromAdFile(File ad) {
        return ad.getName().substring(0, ad.getName().lastIndexOf('.'));
    }

    @Override
    public String toString() {
        return "Export Generation";
    }

    @Override
    public int getCatalogVersion() {
        return m_catalogVersion;
    }
}
