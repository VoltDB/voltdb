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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.zookeeper_voltpatches.AsyncCallback;
import org.apache.zookeeper_voltpatches.CreateMode;
import org.apache.zookeeper_voltpatches.KeeperException;
import org.apache.zookeeper_voltpatches.WatchedEvent;
import org.apache.zookeeper_voltpatches.Watcher;
import org.apache.zookeeper_voltpatches.ZooDefs.Ids;
import org.apache.zookeeper_voltpatches.ZooKeeper;
import org.voltcore.logging.VoltLogger;
import org.voltcore.messaging.BinaryPayloadMessage;
import org.voltcore.messaging.HostMessenger;
import org.voltcore.messaging.Mailbox;
import org.voltcore.messaging.VoltMessage;
import org.voltcore.utils.CoreUtils;
import org.voltcore.utils.DBBPool;
import org.voltcore.utils.Pair;
import org.voltcore.zk.ZKUtil;
import org.voltdb.VoltDB;
import org.voltdb.VoltZK;
import org.voltdb.catalog.CatalogMap;
import org.voltdb.catalog.Connector;
import org.voltdb.catalog.ConnectorTableInfo;
import org.voltdb.catalog.Table;
import org.voltdb.common.Constants;
import org.voltdb.iv2.TxnEgo;
import org.voltdb.messaging.LocalMailbox;
import org.voltdb.utils.VoltFile;

import com.google_voltpatches.common.base.Throwables;
import com.google_voltpatches.common.collect.ImmutableList;
import com.google_voltpatches.common.util.concurrent.Futures;
import com.google_voltpatches.common.util.concurrent.ListenableFuture;
import com.google_voltpatches.common.util.concurrent.ListeningExecutorService;
import com.google_voltpatches.common.util.concurrent.MoreExecutors;
import org.voltdb.catalog.Column;

/**
 * Export data from a single catalog version and database instance.
 *
 */
public class ExportGeneration {
    /**
     * Processors also log using this facility.
     */
    private static final VoltLogger exportLog = new VoltLogger("EXPORT");

    public Long m_timestamp;
    public final File m_directory;

    private String m_leadersZKPath;
    private String m_mailboxesZKPath;

    /**
     * Data sources, one per table per site, provide the interface to
     * poll() and ack() Export data from the execution engines. Data sources
     * are configured by the Export manager at initialization time.
     * partitionid : <tableid : datasource>.
     */
    public final HashMap<Integer, Map<String, ExportDataSource>> m_dataSourcesByPartition
            = new HashMap<Integer, Map<String, ExportDataSource>>();

    private int m_numSources = 0;
    private final AtomicInteger m_drainedSources = new AtomicInteger(0);

    private Runnable m_onAllSourcesDrained = null;

    private final Runnable m_onSourceDrained = new Runnable() {
        @Override
        public void run() {
            if (m_onAllSourcesDrained == null) {
                VoltDB.crashLocalVoltDB("No export generation roller found.", true, null);
                return;
            }
            int numSourcesDrained = m_drainedSources.incrementAndGet();
            exportLog.info("Drained source in generation " + m_timestamp + " with " + numSourcesDrained + " of " + m_numSources + " drained");
            if (numSourcesDrained == m_numSources) {
                if (m_partitionLeaderZKName.isEmpty()) {
                    m_onAllSourcesDrained.run();
                } else {
                    ListenableFuture<?> removeLeadership = m_childUpdatingThread.submit(new Runnable() {
                        @Override
                        public void run() {
                            for (Map.Entry<Integer, String> entry : m_partitionLeaderZKName.entrySet()) {
                                m_zk.delete(
                                        m_leadersZKPath + "/" + entry.getKey() + "/" + entry.getValue(),
                                        -1,
                                        new AsyncCallback.VoidCallback() {

                                            @Override
                                            public void processResult(int rc,
                                                    String path, Object ctx) {
                                                KeeperException.Code code = KeeperException.Code.get(rc);
                                                if (code != KeeperException.Code.OK) {
                                                    VoltDB.crashLocalVoltDB(
                                                            "Error in export leader election giving up leadership of "
                                                            + path,
                                                            true,
                                                            KeeperException.create(code));
                                                }
                                            }},
                                        null);
                            }
                        }
                    }, null);
                    removeLeadership.addListener(
                            m_onAllSourcesDrained,
                            MoreExecutors.sameThreadExecutor());
                }

                ;
            }
        }
    };

    private Mailbox m_mbox;

    private ZooKeeper m_zk;
    private volatile boolean shutdown = false;

    private static final ListeningExecutorService m_childUpdatingThread =
            CoreUtils.getListeningExecutorService("Export ZK Watcher", 1);

    private final Map<Integer, String> m_partitionLeaderZKName = new HashMap<Integer, String>();
    private final Set<Integer> m_partitionsIKnowIAmTheLeader = new HashSet<Integer>();

    //This is maintained to detect if this is a continueing generation or not
    private final boolean m_isContinueingGeneration;

    /**
     * Constructor to create a new generation of export data
     * @param exportOverflowDirectory
     * @throws IOException
     */
    public ExportGeneration(long txnId, File exportOverflowDirectory, boolean isRejoin) throws IOException {
        m_timestamp = txnId;
        m_directory = new File(exportOverflowDirectory, Long.toString(txnId));
        if (!isRejoin) {
            if (!m_directory.mkdirs()) {
                throw new IOException("Could not create " + m_directory);
            }
        } else {
            if (!m_directory.canWrite()) {
                if (!m_directory.mkdirs()) {
                    throw new IOException("Could not create " + m_directory);
                }
            }
        }
        m_isContinueingGeneration = true;
        exportLog.info("Creating new export generation " + m_timestamp);
    }

    /**
     * Constructor to create a generation based on one that has been persisted to disk
     * @param generationDirectory
     * @param catalogGen Generation from catalog.
     * @throws IOException
     */
    public ExportGeneration(File generationDirectory, long catalogGen) throws IOException {
        m_directory = generationDirectory;
        try {
            m_timestamp = Long.parseLong(generationDirectory.getName());
        } catch (NumberFormatException ex) {
            throw new IOException("Invalid Generation directory, directory name must be a number.");
        }
        m_isContinueingGeneration = (catalogGen == m_timestamp);
    }

    //This checks if the on disk generation is a catalog generation.
    public boolean isContinueingGeneration() {
        return m_isContinueingGeneration;
    }

    boolean initializeGenerationFromDisk(final CatalogMap<Connector> connectors, HostMessenger messenger) {
        Set<Integer> partitions = new HashSet<Integer>();

        /*
         * Find all the advertisements. Once one is found, extract the nonce
         * and check for any data files related to the advertisement. If no data files
         * exist ignore the advertisement.
         */
        boolean hadValidAd = false;
        for (File f : m_directory.listFiles()) {
            if (f.getName().endsWith(".ad")) {
                boolean haveDataFiles = false;
                String nonce = f.getName().substring(0, f.getName().length() - 3);
                for (File dataFile : m_directory.listFiles()) {
                    if (dataFile.getName().startsWith(nonce) && !dataFile.getName().equals(f.getName())) {
                        haveDataFiles = true;
                        break;
                    }
                }

                if (haveDataFiles) {
                    try {
                        addDataSource(f, partitions);
                        hadValidAd = true;
                    } catch (IOException e) {
                        VoltDB.crashLocalVoltDB("Error intializing export datasource " + f, true, e);
                    }
                } else {
                    //Delete ads that have no data
                    f.delete();
                }
            }
        }
        createAndRegisterAckMailboxes(partitions, messenger);
        return hadValidAd;
    }


    /*
     * Run a leader election for every partition to determine who will
     * start consuming the export data.
     *
     */
    public void kickOffLeaderElection() {
        m_childUpdatingThread.submit(new Runnable() {
            @Override
            public void run() {
                try {
                    /*
                     * The path where leaders will register for this generation
                     */
                    m_leadersZKPath = VoltZK.exportGenerations + "/" + m_timestamp + "/" + "leaders";

                    /*
                     * Create a directory for each partition
                     */
                    for (Integer partition : m_dataSourcesByPartition.keySet()) {
                        ZKUtil.asyncMkdirs(m_zk, m_leadersZKPath + "/" + partition);
                    }

                    /*
                     * Queue the creation of our ephemeral sequential and then queue
                     * a task to retrieve the children to find the result of the election
                     */
                    List<ZKUtil.ChildrenCallback> callbacks = new ArrayList<ZKUtil.ChildrenCallback>();
                    for (final Integer partition : m_dataSourcesByPartition.keySet()) {
                        m_zk.create(
                                m_leadersZKPath + "/" + partition + "/leader",
                                null,
                                Ids.OPEN_ACL_UNSAFE,
                                CreateMode.EPHEMERAL_SEQUENTIAL,
                                new org.apache.zookeeper_voltpatches.AsyncCallback.StringCallback() {
                                    @Override
                                    public void processResult(int rc, String path,
                                            Object ctx, String name) {
                                        KeeperException.Code code = KeeperException.Code.get(rc);
                                        if (code != KeeperException.Code.OK) {
                                            VoltDB.crashLocalVoltDB(
                                                    "Error in export leader election",
                                                    true,
                                                    KeeperException.create(code));
                                        }
                                        String splitName[] = name.split("/");
                                        m_partitionLeaderZKName.put(partition,  splitName[splitName.length - 1]);
                                    }

                                },
                                null);
                        ZKUtil.ChildrenCallback cb = new ZKUtil.ChildrenCallback();
                        callbacks.add(cb);
                        m_zk.getChildren(
                                m_leadersZKPath + "/" + partition,
                                constructLeaderChildWatcher(partition),
                                cb,
                                null);
                    }

                    /*
                     * Process the result of the per partition elections.
                     * No worries about ordering with the watcher because the watcher tasks
                     * all get funneled through this thread
                     */
                    Iterator<ZKUtil.ChildrenCallback> iter = callbacks.iterator();
                    for (Integer partition : m_dataSourcesByPartition.keySet()) {
                        ZKUtil.ChildrenCallback cb = iter.next();
                        handleLeaderChildrenUpdate(partition,  cb.getChildren());
                    }
                } catch (Throwable t) {
                    VoltDB.crashLocalVoltDB("Error in export leader election", true, t);
                }
            }
        });
    }

    private Watcher constructLeaderChildWatcher(final Integer partition) {
        return new Watcher() {
            @Override
            public void process(final WatchedEvent event) {
                final Runnable processRunnable = new Runnable() {
                    @Override
                    public void run() {
                        if (m_drainedSources.get() == m_numSources) {
                            return;
                        }
                        final AsyncCallback.ChildrenCallback childrenCallback =
                                new org.apache.zookeeper_voltpatches.AsyncCallback.ChildrenCallback() {
                            @Override
                            public void processResult(final int rc, final String path, Object ctx,
                                                      final List<String> children) {
                                KeeperException.Code code = KeeperException.Code.get(rc);
                                if (code != KeeperException.Code.OK) {
                                    VoltDB.crashLocalVoltDB(
                                            "Error in export leader election",
                                            true,
                                            KeeperException.create(code));
                                }
                                m_childUpdatingThread.execute(new Runnable() {
                                    @Override
                                    public void run() {
                                        try {
                                            handleLeaderChildrenUpdate(partition, children);
                                        } catch (Throwable t) {
                                            VoltDB.crashLocalVoltDB("Error in export leader election", true, t);
                                        }
                                    }
                                });
                            }
                        };
                        m_zk.getChildren(
                                m_leadersZKPath + "/" + partition,
                                constructLeaderChildWatcher(partition),
                                childrenCallback, null);
                    }
                };
                m_childUpdatingThread.execute(processRunnable);
            }
        };
    }

    private void handleLeaderChildrenUpdate(Integer partition, List<String> children) {
        if (m_drainedSources.get() == m_numSources || children.isEmpty()) {
            return;
        }

        String leader = Collections.min(children);
        if (m_partitionLeaderZKName.get(partition).equals(leader)) {
            if (m_partitionsIKnowIAmTheLeader.add(partition)) {
                for (ExportDataSource eds : m_dataSourcesByPartition.get(partition).values()) {
                    try {
                        eds.acceptMastership();
                    } catch (Exception e) {
                        exportLog.error("Unable to start exporting", e);
                    }
                }
            }
        }
    }

    void initializeGenerationFromCatalog(
            final CatalogMap<Connector> connectors,
            int hostId,
            HostMessenger messenger,
            List<Integer> partitions)
    {
        //Only populate partitions in use if export is actually happening
        Set<Integer> partitionsInUse = new HashSet<Integer>();

        /*
         * Now create datasources based on the catalog
         */
        for (Connector conn : connectors) {
            if (conn.getEnabled()) {
                for (ConnectorTableInfo ti : conn.getTableinfo()) {
                    Table table = ti.getTable();
                    addDataSources(table, hostId, partitions);

                    partitionsInUse.addAll(partitions);
                }
            }
        }

        createAndRegisterAckMailboxes(partitionsInUse, messenger);
    }

    void initializeMissingPartitionsFromCatalog(
            final CatalogMap<Connector> connectors,
            int hostId,
            HostMessenger messenger,
            List<Integer> partitions) {
        Set<Integer> missingPartitions = new HashSet<Integer>();
        findMissingDataSources(partitions, missingPartitions);
        if (missingPartitions.size() > 0) {
            exportLog.info("Found Missing partitions for continueing generation: " + missingPartitions);
            initializeGenerationFromCatalog(connectors, hostId, messenger, new ArrayList(missingPartitions));
        }
    }

    private void createAndRegisterAckMailboxes(final Set<Integer> localPartitions, HostMessenger messenger) {
        m_zk = messenger.getZK();
        m_mailboxesZKPath = VoltZK.exportGenerations + "/" + m_timestamp + "/" + "mailboxes";

        m_mbox = new LocalMailbox(messenger) {
            @Override
            public void deliver(VoltMessage message) {
                if (message instanceof BinaryPayloadMessage) {
                    BinaryPayloadMessage bpm = (BinaryPayloadMessage)message;
                    ByteBuffer buf = ByteBuffer.wrap(bpm.m_payload);
                    final int partition = buf.getInt();
                    final int length = buf.getInt();
                    byte stringBytes[] = new byte[length];
                    buf.get(stringBytes);
                    String signature = new String(stringBytes, Constants.UTF8ENCODING);
                    final long ackUSO = buf.getLong();

                    final Map<String, ExportDataSource> partitionSources = m_dataSourcesByPartition.get(partition);
                    if (partitionSources == null) {
                        exportLog.error("Received an export ack for partition " + partition +
                                " which does not exist on this node");
                        return;
                    }

                    final ExportDataSource eds = partitionSources.get(signature);
                    if (eds == null) {
                        exportLog.error("Received an export ack for partition " + partition +
                                " source signature " + signature + " which does not exist on this node");
                        return;
                    }

                    try {
                        eds.ack(ackUSO);
                    } catch (RejectedExecutionException ignoreIt) {
                        // ignore it: as it is already shutdown
                    }
                } else {
                    exportLog.error("Receive unexpected message " + message + " in export subsystem");
                }
            }
        };
        messenger.createMailbox(null, m_mbox);

        for (Integer partition : localPartitions) {
            final String partitionDN =  m_mailboxesZKPath + "/" + partition;
            ZKUtil.asyncMkdirs(m_zk, partitionDN);

            ZKUtil.StringCallback cb = new ZKUtil.StringCallback();
            m_zk.create(
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
                    m_zk.getChildren(
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

                    for( ExportDataSource eds:
                        m_dataSourcesByPartition.get( partition).values()) {
                        eds.updateAckMailboxes(Pair.of(m_mbox, mailboxHsids));
                    }
                }
            }
        });
        try {
            fut.get();
        } catch (Throwable t) {
            Throwables.propagate(t);
        }

    }

    private Watcher constructMailboxChildWatcher() {
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
        m_zk.getChildren(event.getPath(), constructMailboxChildWatcher(), constructChildRetrievalCallback(), null);
    }

    private AsyncCallback.ChildrenCallback constructChildRetrievalCallback() {
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
                            if (code != KeeperException.Code.OK) {
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
                            for( ExportDataSource eds: m_dataSourcesByPartition.get( partition).values()) {
                                eds.updateAckMailboxes(Pair.of(m_mbox, mailboxHsids));
                            }
                        } catch (Throwable t) {
                            VoltDB.crashLocalVoltDB("Error in export ack handling", true, t);
                        }
                    }
                });
            }

        };
    }

    public long getQueuedExportBytes(int partitionId, String signature) {
        //assert(m_dataSourcesByPartition.containsKey(partitionId));
        //assert(m_dataSourcesByPartition.get(partitionId).containsKey(delegateId));
        Map<String, ExportDataSource> sources = m_dataSourcesByPartition.get(partitionId);

        if (sources == null) {
            /*
             * This is fine. If the table is dropped it won't have an entry in the generation created
             * after the table was dropped.
             */
            //            exportLog.error("Could not find export data sources for generation " + m_timestamp + " partition "
            //                    + partitionId);
            return 0;
        }

        ExportDataSource source = sources.get(signature);
        if (source == null) {
            /*
             * This is fine. If the table is dropped it won't have an entry in the generation created
             * after the table was dropped.
             */
            //exportLog.error("Could not find export data source for generation " + m_timestamp + " partition " + partitionId +
            //        " signature " + signature);
            return 0;
        }
        return source.sizeInBytes();
    }

    /*
     * Create a datasource based on an ad file
     */
    private void addDataSource(File adFile, Set<Integer> partitions) throws IOException {
        ExportDataSource source = new ExportDataSource(m_onSourceDrained, adFile, isContinueingGeneration());
        partitions.add(source.getPartitionId());
        if (source.getGeneration() != this.m_timestamp) {
            throw new IOException("Failed to load generation from disk invalid data source generation found.");
        }
        exportLog.info("Creating ExportDataSource for " + adFile + " table " + source.getTableName() +
                " signature " + source.getSignature() + " partition id " + source.getPartitionId() +
                " bytes " + source.sizeInBytes());
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
        m_numSources++;
    }

    /*
     * An unfortunate test only method for supplying a mock source
     */
    public void addDataSource(ExportDataSource source) {
        Map<String, ExportDataSource> dataSourcesForPartition = m_dataSourcesByPartition.get(source.getPartitionId());
        if (dataSourcesForPartition == null) {
            dataSourcesForPartition = new HashMap<String, ExportDataSource>();
            m_dataSourcesByPartition.put(source.getPartitionId(), dataSourcesForPartition);
        }
        dataSourcesForPartition.put(source.getSignature(), source);
    }

    // silly helper to add datasources for a table catalog object
    private void addDataSources(
            Table table, int hostId, List<Integer> partitions)
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
                Column partColumn = table.getPartitioncolumn();
                ExportDataSource exportDataSource = new ExportDataSource(
                        m_onSourceDrained,
                        "database",
                        table.getTypeName(),
                        partition,
                        table.getSignature(),
                        m_timestamp,
                        table.getColumns(),
                        partColumn,
                        m_directory.getPath());
                m_numSources++;
                exportLog.info("Creating ExportDataSource for table " + table.getTypeName() +
                        " signature " + table.getSignature() + " partition id " + partition);
                dataSourcesForPartition.put(table.getSignature(), exportDataSource);
            } catch (IOException e) {
                VoltDB.crashLocalVoltDB(
                        "Error creating datasources for table " +
                        table.getTypeName() + " host id " + hostId, true, e);
            }
        }
    }

    //Find missing partitions from this generation typicaally called for current generation to fill in missing partitions
    private void findMissingDataSources(List<Integer> partitions, Set<Integer> missingPartitions) {
        for (Integer partition : partitions) {
            Map<String, ExportDataSource> dataSourcesForPartition = m_dataSourcesByPartition.get(partition);
            if (dataSourcesForPartition == null) {
                missingPartitions.add(partition);
            }
        }
    }

    public void pushExportBuffer(int partitionId, String signature, long uso, ByteBuffer buffer, boolean sync, boolean endOfStream) {
        //        System.out.println("In generation " + m_timestamp + " partition " + partitionId + " signature " + signature + (buffer == null ? " null buffer " : (" buffer length " + buffer.remaining())));
        //        for (Integer i : m_dataSourcesByPartition.keySet()) {
        //            System.out.println("Have partition " + i);
        //        }
        assert(m_dataSourcesByPartition.containsKey(partitionId));
        assert(m_dataSourcesByPartition.get(partitionId).containsKey(signature));
        Map<String, ExportDataSource> sources = m_dataSourcesByPartition.get(partitionId);

        if (sources == null) {
            exportLog.error("Could not find export data sources for partition "
                    + partitionId + " generation " + m_timestamp + " the export data is being discarded");
            if (buffer != null) {
                DBBPool.wrapBB(buffer).discard();
            }
            return;
        }

        ExportDataSource source = sources.get(signature);
        if (source == null) {
            exportLog.error("Could not find export data source for partition " + partitionId +
                    " signature " + signature + " generation " +
                    m_timestamp + " the export data is being discarded");
            if (buffer != null) {
                DBBPool.wrapBB(buffer).discard();
            }
            return;
        }

        source.pushExportBuffer(uso, buffer, sync, endOfStream);
    }

    public void closeAndDelete() throws IOException {
        List<ListenableFuture<?>> tasks = new ArrayList<ListenableFuture<?>>();
        for (Map<String, ExportDataSource> map : m_dataSourcesByPartition.values()) {
            for (ExportDataSource source : map.values()) {
                tasks.add(source.closeAndDelete());
            }
        }
        try {
            Futures.allAsList(tasks).get();
        } catch (Exception e) {
            Throwables.propagateIfPossible(e, IOException.class);
        }
        shutdown = true;
        VoltFile.recursivelyDelete(m_directory);

    }

    /*
     * Returns true if the generatino was completely truncated away
     */
    public boolean truncateExportToTxnId(long txnId, long[] perPartitionTxnIds) {
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
                    tasks.add(source.truncateExportToTxnId(truncationPoint));
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

        return m_drainedSources.get() == m_numSources;
    }

    public void close() {
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
        shutdown = true;
    }

    /**
     * Indicate to all associated {@link ExportDataSource}to assume
     * mastership role for the given partition id
     * @param partitionId
     */
    public void acceptMastershipTask( int partitionId) {
        Map<String, ExportDataSource> partitionDataSourceMap = m_dataSourcesByPartition.get(partitionId);

        // this case happens when there are no export tables
        if (partitionDataSourceMap == null) {
            return;
        }

        exportLog.info("Export generation " + m_timestamp + " accepting mastership for partition " + partitionId);
        for( ExportDataSource eds: partitionDataSourceMap.values()) {
            try {
                eds.acceptMastership();
            } catch (Exception e) {
                exportLog.error("Unable to start exporting", e);
            }
        }
    }

    @Override
    public String toString() {
        return "Export Generation - " + m_timestamp.toString();
    }

    public void setGenerationDrainRunnable(Runnable onGenerationDrained) {
        m_onAllSourcesDrained = onGenerationDrained;
    }

}
