/* This file is part of VoltDB.
 * Copyright (C) 2008-2018 VoltDB Inc.
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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.RejectedExecutionException;

import org.apache.zookeeper_voltpatches.AsyncCallback;
import org.apache.zookeeper_voltpatches.CreateMode;
import org.apache.zookeeper_voltpatches.KeeperException;
import org.apache.zookeeper_voltpatches.WatchedEvent;
import org.apache.zookeeper_voltpatches.Watcher;
import org.apache.zookeeper_voltpatches.ZooDefs.Ids;
import org.voltcore.logging.VoltLogger;
import org.voltcore.messaging.BinaryPayloadMessage;
import org.voltcore.messaging.HostMessenger;
import org.voltcore.messaging.Mailbox;
import org.voltcore.messaging.VoltMessage;
import org.voltcore.utils.CoreUtils;
import org.voltcore.utils.DBBPool;
import org.voltcore.utils.Pair;
import org.voltcore.zk.ZKUtil;
import org.voltdb.CatalogContext;
import org.voltdb.VoltDB;
import org.voltdb.VoltZK;
import org.voltdb.catalog.CatalogMap;
import org.voltdb.catalog.Connector;
import org.voltdb.catalog.ConnectorTableInfo;
import org.voltdb.catalog.Table;
import org.voltdb.common.Constants;
import org.voltdb.iv2.TxnEgo;
import org.voltdb.messaging.LocalMailbox;

import com.google_voltpatches.common.base.Throwables;
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

    private volatile boolean shutdown = false;

    private static final ListeningExecutorService m_childUpdatingThread =
            CoreUtils.getListeningExecutorService("Export ZK Watcher", 1);

    /**
     * Constructor to create a new generation of export data
     * @param exportOverflowDirectory
     * @throws IOException
     */
    public ExportGeneration(File exportOverflowDirectory) throws IOException {
        m_directory = exportOverflowDirectory;
        if (!m_directory.canWrite()) {
            if (!m_directory.mkdirs()) {
                throw new IOException("Could not create " + m_directory);
            }
        }

        if (exportLog.isDebugEnabled()) {
            exportLog.debug("Creating new export generation.");
        }
    }

    void initialize(HostMessenger messenger,
            int hostId,
            CatalogContext catalogContext,
            final CatalogMap<Connector> connectors,
            List<Integer> partitions,
            File exportOverflowDirectory)
    {
        List<Integer> allLocalPartitions = new ArrayList<>(partitions);
        File files[] = exportOverflowDirectory.listFiles();
        if (files != null) {
            List<Integer> onDiskPartitions = initializeGenerationFromDisk(messenger);
            // Add new unique partitions from on disk list.
            onDiskPartitions.removeAll(allLocalPartitions);
            allLocalPartitions.addAll(onDiskPartitions);
        }
        initializeGenerationFromCatalog(catalogContext, connectors, hostId, messenger, partitions);

        // One export mailbox per node, since we only keep one generation
        createAckMailboxesIfNeeded(messenger, allLocalPartitions);
    }

    List<Integer> initializeGenerationFromDisk(HostMessenger messenger) {
        List<Integer> partitions = new ArrayList<Integer>();

        /*
         * Find all the data files. Once one is found, extract the nonce
         * and check for any advertisements related to the data files. If
         * there are orphaned advertisements, delete them.
         */
        Map<String, File> dataFiles = new HashMap<>();
        File[] files = m_directory.listFiles();
        for (File data: files) {
            if (!data.getName().endsWith(".ad")) {
                String nonce = data.getName().substring(0, data.getName().length() - 3);
                dataFiles.put(nonce, data);
            }
        }
        for (File ad: files) {
            if (ad.getName().endsWith(".ad")) {
                String nonce = ad.getName().substring(0, ad.getName().length() - 3);
                File dataFile = dataFiles.get(nonce);
                if (dataFile != null) {
                    try {
                        addDataSource(ad, partitions);
                    } catch (IOException e) {
                        VoltDB.crashLocalVoltDB("Error intializing export datasource " + ad, true, e);
                    }
                } else {
                    //Delete ads that have no data
                    ad.delete();
                }
            }
        }
        return partitions;
    }

    void initializeGenerationFromCatalog(CatalogContext catalogContext,
            final CatalogMap<Connector> connectors,
            int hostId,
            HostMessenger messenger,
            List<Integer> partitions)
    {
        // Now create datasources based on the catalog
        for (Connector conn : connectors) {
            if (conn.getEnabled()) {
                for (ConnectorTableInfo ti : conn.getTableinfo()) {
                    Table table = ti.getTable();
                    addDataSources(table, hostId, partitions);
                }
            }
        }
    }

    /**
     * Create export ack mailbox during generation initialization, do nothing if generation has already initialized.
     * @param messenger  HostMessenger
     * @param localPartitions  locally covered partitions
     */
    public void createAckMailboxesIfNeeded(HostMessenger messenger, final List<Integer> localPartitions) {
        if (m_mbox != null) {
            return;
        }
        m_mailboxesZKPath = VoltZK.exportGenerations + "/" + "mailboxes";

        m_mbox = new LocalMailbox(messenger) {
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
                    String signature = new String(stringBytes, Constants.UTF8ENCODING);
                    if (partitionSources == null) {
                        exportLog.error("Received an export ack for partition " + partition +
                                " which does not exist on this node, partitions = " + m_dataSourcesByPartition);
                        return;
                    }
                    final ExportDataSource eds = partitionSources.get(signature);
                    if (eds == null) {
                        exportLog.warn("Received an export ack for partition " + partition +
                                " source signature " + signature + " which does not exist on this node, sources = " + partitionSources);
                        return;
                    }

                    if (msgType == ExportManager.RELEASE_BUFFER) {
                        final long ackUSO = buf.getLong();
                        try {
                            if (exportLog.isDebugEnabled()) {
                                exportLog.debug("Received RELEASE_BUFFER message for " + eds.toString() +
                                        " with uso: " + ackUSO +
                                        " from " + CoreUtils.hsIdToString(message.m_sourceHSId) +
                                        " to " + CoreUtils.hsIdToString(m_mbox.getHSId()));
                            }
                            eds.ack(ackUSO);
                        } catch (RejectedExecutionException ignoreIt) {
                            // ignore it: as it is already shutdown
                        }
                    } else if (msgType == ExportManager.TAKE_MASTERSHIP) {
                        final long ackUSO = buf.getLong();
                        try {
                            if (exportLog.isDebugEnabled()) {
                                exportLog.debug("Received TAKE_MASTERSHIP message for " + eds.toString() +
                                        " with uso:" + ackUSO +
                                        " from " + CoreUtils.hsIdToString(message.m_sourceHSId) +
                                        " to " + CoreUtils.hsIdToString(m_mbox.getHSId()));
                            }
                            eds.ack(ackUSO);
                        } catch (RejectedExecutionException ignoreIt) {
                            // ignore it: as it is already shutdown
                        }
                        eds.acceptMastership();
                    } else if (msgType == ExportManager.QUERY_MEMBERSHIP) {
                        if (exportLog.isDebugEnabled()) {
                            exportLog.debug("Received QUERY_MEMBERSHIP message for " + eds.toString() +
                                    " from " + CoreUtils.hsIdToString(message.m_sourceHSId) +
                                    " to " + CoreUtils.hsIdToString(m_mbox.getHSId()));
                        }
                        eds.handleQueryMessage(message.m_sourceHSId);
                    } else if (msgType == ExportManager.QUERY_RESPONSE) {
                        if (exportLog.isDebugEnabled()) {
                            exportLog.debug("Received QUERY_RESPONSE message for " + eds.toString() +
                                    " from " + CoreUtils.hsIdToString(message.m_sourceHSId) +
                                    " to " + CoreUtils.hsIdToString(m_mbox.getHSId()));
                        }
                        eds.handleQueryResponse(message);
                    } else {
                        exportLog.error("Receive unsupported message type " + message + " in export subsystem");
                    }
                } else {
                    exportLog.error("Receive unexpected message " + message + " in export subsystem");
                }
            }
        };
        messenger.createMailbox(null, m_mbox);
        // Update latest replica list to each data source.
        updateReplicaList(messenger, localPartitions);
    }

    // Access by multiple threads
    public void updateAckMailboxes(int partition, Set<Long> newHSIds) {
        synchronized (m_dataSourcesByPartition) {
            ImmutableList<Long> replicaHSIds = m_replicasHSIds.get(partition);
            for( ExportDataSource eds: m_dataSourcesByPartition.get(partition).values()) {
                if (replicaHSIds != null) {
                    eds.updateAckMailboxes(Pair.of(m_mbox, replicaHSIds));
                }
                // In case of newly joined or rejoined streams miss any RELEASE_BUFFER event,
                // master stream resend the event when the export mailbox is aware of new streams.
                if (newHSIds != null) {
                    eds.forwardAckToNewJoinedReplicas(newHSIds);
                }
            }
        }
    }

    private void updateReplicaList(HostMessenger messenger, List<Integer> localPartitions) {
        //If we have new partitions create mailbox paths.
        for (Integer partition : localPartitions) {
            final String partitionDN =  m_mailboxesZKPath + "/" + partition;
            ZKUtil.asyncMkdirs(messenger.getZK(), partitionDN);

            ZKUtil.StringCallback cb = new ZKUtil.StringCallback();
            messenger.getZK().create(
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
                for (Integer partition : localPartitions) {
                    ZKUtil.ChildrenCallback callback = new ZKUtil.ChildrenCallback();
                    messenger.getZK().getChildren(
                            m_mailboxesZKPath + "/" + partition,
                            constructMailboxChildWatcher(messenger),
                            callback,
                            null);
                    callbacks.add(Pair.of(partition, callback));
                }
                for (Pair<Integer, ZKUtil.ChildrenCallback> p : callbacks) {
                    final Integer partition = p.getFirst();
                    List<String> children = null;
                    try {
                        children = p.getSecond().getChildren();
                    } catch (InterruptedException e) {
                        Throwables.propagate(e);
                    } catch (KeeperException e) {
                        Throwables.propagate(e);
                    }
                    ImmutableList.Builder<Long> mailboxes = ImmutableList.builder();

                    for (String child : children) {
                        if (child.equals(Long.toString(m_mbox.getHSId()))) continue;
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
            Throwables.propagate(t);
        }
    }

    private Watcher constructMailboxChildWatcher(final HostMessenger messenger) {
        if (shutdown) {
            return null;
        }
        return new Watcher() {

            @Override
            public void process(final WatchedEvent event) {
                m_childUpdatingThread.submit(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            handleChildUpdate(event, messenger);
                        } catch (Throwable t) {
                            VoltDB.crashLocalVoltDB("Error in export ack handling", true, t);
                        }
                    }
                });
            }

        };
    }

    private void handleChildUpdate(final WatchedEvent event, final HostMessenger messenger) {
        if (shutdown) return;
        messenger.getZK().getChildren(event.getPath(), constructMailboxChildWatcher(messenger), constructChildRetrievalCallback(), null);
    }

    private AsyncCallback.ChildrenCallback constructChildRetrievalCallback() {
        if (shutdown) {
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
                            if (shutdown) return;
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
                            ImmutableList.Builder<Long> mailboxes = ImmutableList.builder();
                            for (String child : children) {
                                if (child.equals(Long.toString(m_mbox.getHSId()))) continue;
                                mailboxes.add(Long.valueOf(child));
                            }
                            ImmutableList<Long> mailboxHsids = mailboxes.build();
                            Set<Long> newHSIds = Sets.difference(new HashSet<Long>(mailboxHsids),
                                    new HashSet<Long>(m_replicasHSIds.get(partition)));
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
    public long getQueuedExportBytes(int partitionId, String signature) {
        Map<String, ExportDataSource> sources = m_dataSourcesByPartition.get(partitionId);

        if (sources == null) {
            /*
             * This is fine. If the table is dropped it won't have an entry in the generation created
             * after the table was dropped.
             */
            return 0;
        }

        ExportDataSource source = sources.get(signature);
        if (source == null) {
            /*
             * This is fine. If the table is dropped it won't have an entry in the generation created
             * after the table was dropped.
             */
            return 0;
        }
        return source.sizeInBytes();
    }

    @Override
    public void onSourceDone(int partitionId, String signature) {
        assert(m_dataSourcesByPartition.containsKey(partitionId));
        assert(m_dataSourcesByPartition.get(partitionId).containsKey(signature));
        ExportDataSource source;
        synchronized(m_dataSourcesByPartition) {
            Map<String, ExportDataSource> sources = m_dataSourcesByPartition.get(partitionId);

            if (sources == null) {
                exportLog.warn("Could not find export data sources for partition "
                        + partitionId + ". The export cleanup stream is being discarded.");
                return;
            }

            source = sources.get(signature);
            if (source == null) {
                exportLog.warn("Could not find export data source for signature " + partitionId +
                        " signature " + signature + ". The export cleanup stream is being discarded.");
                return;
            }

            //Remove first then do cleanup. After this is done trigger processor cleanup.
            sources.remove(signature);
        }
        //Do closing outside the synchronized block.
        exportLog.info("Finished processing " + source);
        source.closeAndDelete();
    }

    /*
     * Create a datasource based on an ad file
     */
    private void addDataSource(File adFile, List<Integer> partitions) throws IOException {
        ExportDataSource source = new ExportDataSource(this, adFile);
        partitions.add(source.getPartitionId());
        if (exportLog.isDebugEnabled()) {
            exportLog.debug("Creating ExportDataSource for " + adFile + " table " + source.getTableName() +
                    " signature " + source.getSignature() + " partition id " + source.getPartitionId() +
                    " bytes " + source.sizeInBytes());
        }
        Map<String, ExportDataSource> dataSourcesForPartition = m_dataSourcesByPartition.get(source.getPartitionId());
        if (dataSourcesForPartition == null) {
            dataSourcesForPartition = new HashMap<String, ExportDataSource>();
            m_dataSourcesByPartition.put(source.getPartitionId(), dataSourcesForPartition);
        } else {
            if (dataSourcesForPartition.get(source.getSignature()) != null) {
                exportLog.warn("On Disk generation with same table, partition already exists using known data source.");
                return;
            }
        }
        dataSourcesForPartition.put( source.getSignature(), source);
    }

    // silly helper to add datasources for a table catalog object
    private void addDataSources(Table table, int hostId, List<Integer> partitions)
    {
        for (Integer partition : partitions) {

            /*
             * IOException can occur if there is a problem
             * with the persistent aspects of the datasource storage
             */
            try {
                Map<String, ExportDataSource> dataSourcesForPartition = m_dataSourcesByPartition.get(partition);
                if (dataSourcesForPartition == null) {
                    dataSourcesForPartition = new HashMap<String, ExportDataSource>();
                    m_dataSourcesByPartition.put(partition, dataSourcesForPartition);
                }
                final String key = table.getSignature();
                if (!dataSourcesForPartition.containsKey(key)) {
                    ExportDataSource exportDataSource = new ExportDataSource(this,
                            "database",
                            table.getTypeName(),
                            partition,
                            key,
                            table.getColumns(),
                            table.getPartitioncolumn(),
                            m_directory.getPath());
                    if (exportLog.isDebugEnabled()) {
                        exportLog.debug("Creating ExportDataSource for table in catalog " + table.getTypeName() +
                                " signature " + key + " partition id " + partition);
                    }
                    dataSourcesForPartition.put(key, exportDataSource);
                } else {
                    //Since we are loading from catalog any found EDS mark it to be in catalog.
                    dataSourcesForPartition.get(key).markInCatalog();
                }
            } catch (IOException e) {
                VoltDB.crashLocalVoltDB(
                        "Error creating datasources for table " +
                        table.getTypeName() + " host id " + hostId, true, e);
            }
        }
    }

    @Override
    public void pushExportBuffer(int partitionId, String signature, long uso, ByteBuffer buffer, boolean sync) {
        Map<String, ExportDataSource> sources = m_dataSourcesByPartition.get(partitionId);

        if (sources == null) {
            exportLog.error("PUSH Could not find export data sources for partition "
                    + partitionId + ". The export data is being discarded.");
            if (buffer != null) {
                DBBPool.wrapBB(buffer).discard();
            }
            return;
        }

        ExportDataSource source = sources.get(signature);
        if (source == null) {
            exportLog.error("PUSH Could not find export data source for partition " + partitionId +
                    " Signature " + signature + ". The export data is being discarded.");
            if (buffer != null) {
                DBBPool.wrapBB(buffer).discard();
            }
            return;
        }

        source.pushExportBuffer(uso, buffer, sync);
    }

    @Override
    public void pushEndOfStream(int partitionId, String signature) {
        assert(m_dataSourcesByPartition.containsKey(partitionId));
        assert(m_dataSourcesByPartition.get(partitionId).containsKey(signature));
        Map<String, ExportDataSource> sources = m_dataSourcesByPartition.get(partitionId);

        if (sources == null) {
            exportLog.error("EOS Could not find export data sources for partition "
                    + partitionId + ". The export end of stream is being discarded.");
            return;
        }

        ExportDataSource source = sources.get(signature);
        if (source == null) {
            exportLog.error("EOS Could not find export data source for partition " + partitionId +
                    " signature " + signature + ". The export end of stream is being discarded.");
            return;
        }

        source.pushEndOfStream();
    }

    private void cleanup(final HostMessenger messenger) {
        shutdown = true;
        //We need messenger NULL guard for tests.
        if (m_mbox != null && messenger != null) {
            for (Integer partition : m_dataSourcesByPartition.keySet()) {
                final String partitionDN =  m_mailboxesZKPath + "/" + partition;
                String path = partitionDN + "/" + m_mbox.getHSId();
                try {
                    messenger.getZK().delete(path, 0);
                } catch (InterruptedException ex) {
                    ;
                } catch (KeeperException ex) {
                    ;
                }
            }
            messenger.removeMailbox(m_mbox);
        }
    }

    @Override
    public void truncateExportToTxnId(long txnId, long[] perPartitionTxnIds) {
        // create an easy partitionId:txnId lookup.
        HashMap<Integer, Long> partitionToTxnId = new HashMap<Integer, Long>();
        for (long tid : perPartitionTxnIds) {
            partitionToTxnId.put(TxnEgo.getPartitionId(tid), tid);
        }

        List<ListenableFuture<?>> tasks = new ArrayList<ListenableFuture<?>>();

        // pre-iv2, the truncation point is the snapshot transaction id.
        // In iv2, truncation at the per-partition txn id recorded in the snapshot.
        for (Map<String, ExportDataSource> dataSources : m_dataSourcesByPartition.values()) {
            for (ExportDataSource source : dataSources.values()) {
                Long truncationPoint = partitionToTxnId.get(source.getPartitionId());
                if (truncationPoint == null) {
                    exportLog.error("Snapshot " + txnId +
                            " does not include truncation point for partition " +
                            source.getPartitionId());
                }
                else {
                    //If this was drained and closed we may not have truncation point and we dont want to reopen PBDs
                    if (!source.isClosed()) {
                        tasks.add(source.truncateExportToTxnId(truncationPoint));
                    }
                }
            }
        }

        try {
            Futures.allAsList(tasks).get();
        } catch (Exception e) {
            VoltDB.crashLocalVoltDB("Unexpected exception truncating export data during snapshot restore. " +
                                    "You can back up export overflow data and start the " +
                                    "DB without it to get past this error", true, e);
        }
    }

    public void sync(final boolean nofsync) {
        List<ListenableFuture<?>> tasks = new ArrayList<ListenableFuture<?>>();
        for (Map<String, ExportDataSource> dataSources : m_dataSourcesByPartition.values()) {
            for (ExportDataSource source : dataSources.values()) {
                ListenableFuture<?> syncFuture = source.sync(nofsync);
                if (syncFuture != null)
                    tasks.add(syncFuture);
            }
        }

        try {
            if (!tasks.isEmpty())
                Futures.allAsList(tasks).get();
        } catch (Exception e) {
            exportLog.error("Unexpected exception syncing export data during snapshot save.", e);
        }
    }

    @Override
    public void close(final HostMessenger messenger) {
        List<ListenableFuture<?>> tasks = new ArrayList<ListenableFuture<?>>();
        for (Map<String, ExportDataSource> sources : m_dataSourcesByPartition.values()) {
            for (ExportDataSource source : sources.values()) {
                tasks.add(source.close());
            }
        }
        try {
            Futures.allAsList(tasks).get();
        } catch (Exception e) {
            //Logging of errors  is done inside the tasks so nothing to do here
            //intentionally not failing if there is an issue with close
            exportLog.error("Error closing export data sources", e);
        }
        //Do this before so no watchers gets created.
        shutdown = true;
        cleanup(messenger);
    }

    public void unacceptMastership() {
        for (Map<String, ExportDataSource> partitionDataSourceMap : m_dataSourcesByPartition.values()) {
            for (ExportDataSource source : partitionDataSourceMap.values()) {
                source.unacceptMastership();
            }
        }
    }

    /**
     * Indicate to all associated {@link ExportDataSource}to PREPARE give up
     * mastership role for the given partition id
     * @param partitionId
     */
    public void prepareTransferMastership(int partitionId, int hostId) {
        Map<String, ExportDataSource> partitionDataSourceMap = m_dataSourcesByPartition.get(partitionId);

        // this case happens when there are no export tables
        if (partitionDataSourceMap == null) {
            return;
        }
        for (ExportDataSource eds : partitionDataSourceMap.values()) {
            eds.prepareTransferMastership(hostId);
        }
    }

    /**
     * Indicate to all associated {@link ExportDataSource} to assume
     * mastership role for the given partition id
     * @param partitionId
     */
    @Override
    public void acceptMastership(int partitionId) {
        Map<String, ExportDataSource> partitionDataSourceMap = m_dataSourcesByPartition.get(partitionId);

        // this case happens when there are no export tables
        if (partitionDataSourceMap == null) {
            return;
        }

        for( ExportDataSource eds: partitionDataSourceMap.values()) {
            try {
                eds.acceptMastership();
            } catch (Exception e) {
                exportLog.error("Unable to start exporting", e);
            }
        }
    }

    /**
     * Indicate to all associated {@link ExportDataSource}to QUERY
     * mastership role for the given partition id
     * @param partitionId
     */
    void reassignExportStreamMaster(int partitionId) {
        Map<String, ExportDataSource> partitionDataSourceMap = m_dataSourcesByPartition.get(partitionId);

        // this case happens when there are no export tables
        if (partitionDataSourceMap == null) {
            return;
        }

        for( ExportDataSource eds: partitionDataSourceMap.values()) {
            eds.reassignExportStreamMaster();
        }
    }

    @Override
    public Map<Integer, Map<String, ExportDataSource>> getDataSourceByPartition() {
        return m_dataSourcesByPartition;
    }

    @Override
    public String toString() {
        return "Export Generation";
    }
}
