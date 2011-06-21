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

package org.voltdb;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooKeeper.States;
import org.voltdb.SystemProcedureCatalog.Config;
import org.voltdb.catalog.Partition;
import org.voltdb.catalog.Procedure;
import org.voltdb.client.ClientResponse;
import org.voltdb.dtxn.TransactionInitiator;
import org.voltdb.logging.VoltLogger;
import org.voltdb.messaging.FastSerializable;
import org.voltdb.network.Connection;
import org.voltdb.network.NIOReadStream;
import org.voltdb.network.WriteStream;
import org.voltdb.sysprocs.saverestore.SnapshotUtil;
import org.voltdb.sysprocs.saverestore.SnapshotUtil.Snapshot;
import org.voltdb.sysprocs.saverestore.SnapshotUtil.TableFiles;
import org.voltdb.utils.DBBPool.BBContainer;
import org.voltdb.utils.DeferredSerialization;
import org.voltdb.utils.EstTime;
import org.voltdb.utils.LogReader;
import org.voltdb.utils.MiscUtils;
import org.voltdb.utils.Pair;

/**
 * An agent responsible for the whole restore process when the cluster starts
 * up. It performs the following tasks in order,
 *
 * - Try to restore the last snapshot
 * - Try to replay all command logs
 * - Take a snapshot if command logs were replayed to truncate them
 *
 * Once all of these tasks have finished successfully, it will call RealVoltDB
 * to resume normal operation.
 */
public class RestoreAgent implements CommandLogReinitiator.Callback, Watcher {
    private final static VoltLogger LOG = new VoltLogger("RESTORE");
    // TODO: Nonce for command-log snapshots, TBD
    public final static String CL_NONCE_PREFIX = "command_log";

    // ZK stuff
    private final static String RESTORE = "/restore";
    private final static String RESTORE_BARRIER = "/restore_barrier";
    private final static String SNAPSHOT_ID = "/restore/snapshot_id";
    private final String zkBarrierNode;

    private final Integer m_hostId;
    private final CatalogContext m_context;
    private final TransactionInitiator m_initiator;

    private final ZooKeeper m_zk;
    private final Semaphore m_flag = new Semaphore(0);
    private final SimpleDateFormat m_dateFormat = new SimpleDateFormat("'_'yyyy.MM.dd.HH.mm.ss");

    private final int[] m_allPartitions;

    // Different states the restore process can be in
    private enum State { RESTORE, REPLAY, TRUNCATE };

    // State of the restore agent
    private volatile State m_state = State.RESTORE;
    private final RestoreAdapter m_restoreAdapter = new RestoreAdapter();

    private CommandLogReinitiator m_replayAgent = new CommandLogReinitiator() {
        private Callback m_callback;
        @Override
        public void setCallback(Callback callback) {
            m_callback = callback;
        }
        @Override
        public void replay() {
            if (m_callback != null) {
                m_callback.onReplayCompletion();
            }
        }
        @Override
        public void join() throws InterruptedException {}
        @Override
        public boolean hasReplayed() {
            return false;
        }
        @Override
        public long getMinLastSeenTxn() {
            return 0;
        }
        @Override
        public boolean started() {
            return true;
        }
        @Override
        public void setSnapshotTxnId(long txnId) {}
    };

    private Runnable m_restorePlanner = new Runnable() {
        @Override
        public void run() {
            /*
             * Wait for at most 3 seconds for the ZK client to establish a
             * connection
             */
            try {
                m_flag.tryAcquire(3, TimeUnit.SECONDS);
            } catch (InterruptedException ignore) {}

            if (m_zk.getState() != States.CONNECTED) {
                LOG.fatal("Failed to establish a ZooKeeper connection");
                try {
                    m_zk.close();
                } catch (InterruptedException e1) {}
                VoltDB.crashVoltDB();
            }

            createZKDirectory(RESTORE);
            createZKDirectory(RESTORE_BARRIER);

            enterRestore();

            // Transaction ID of the restore sysproc
            final long txnId = 1l;
            TreeMap<Long, Snapshot> snapshots = getSnapshots();
            Set<SnapshotInfo> snapshotInfos = new HashSet<SnapshotInfo>();

            final long minLastSeenTxn = m_replayAgent.getMinLastSeenTxn();
            for (Entry<Long, Snapshot> e : snapshots.entrySet()) {
                /*
                 * If the txn of the snapshot is before the earliest txn
                 * among the last seen txns across all initiators when the
                 * log starts, there is a gap in between the snapshot was
                 * taken and the beginning of the log. So the snapshot is
                 * not viable for replay.
                 */
                if (e.getKey() < minLastSeenTxn) {
                    continue;
                }

                Snapshot s = e.getValue();
                File digest = s.m_digests.get(0);
                int partitionCount = -1;
                boolean skip = false;
                for (TableFiles tf : s.m_tableFiles.values()) {
                    if (tf.m_isReplicated) {
                        continue;
                    }

                    if (skip) {
                        break;
                    }

                    for (boolean completed : tf.m_completed) {
                        if (!completed) {
                            skip = true;
                            break;
                        }
                    }

                    // Everyone has to agree on the total partition count
                    for (int count : tf.m_totalPartitionCounts) {
                        if (partitionCount == -1) {
                            partitionCount = count;
                        } else if (count != partitionCount) {
                            skip = true;
                            break;
                        }
                    }
                }

                if (skip) {
                    continue;
                }

                SnapshotInfo info =
                    new SnapshotInfo(e.getKey(), digest.getParent(),
                                     parseDigestFilename(digest.getName()),
                                     partitionCount);
                for (Entry<String, TableFiles> te : s.m_tableFiles.entrySet()) {
                    TableFiles tableFile = te.getValue();
                    HashSet<Integer> ids = new HashSet<Integer>();
                    for (Set<Integer> idSet : tableFile.m_validPartitionIds) {
                        ids.addAll(idSet);
                    }
                    if (!tableFile.m_isReplicated) {
                        info.partitions.put(te.getKey(), ids);
                    }
                }
                snapshotInfos.add(info);
            }
            LOG.debug("Gathered " + snapshotInfos.size() + " snaphost information");

            sendLocalRestoreInformation(minLastSeenTxn, snapshotInfos);

            // Negotiate with other hosts about which snapshot to restore
            String restorePath = null;
            String restoreNonce = null;
            long lastSnapshotTxnId = 0;
            Entry<Long, Set<SnapshotInfo>> lastSnapshot = getRestorePlan();
            if (lastSnapshot != null) {
                LOG.debug("Snapshot to restore: " + lastSnapshot.getKey());
                lastSnapshotTxnId = lastSnapshot.getKey();
                Iterator<SnapshotInfo> i = lastSnapshot.getValue().iterator();
                while (i.hasNext()) {
                    SnapshotInfo next = i.next();
                    restorePath = next.path;
                    restoreNonce = next.nonce;
                    break;
                }
                assert(restorePath != null && restoreNonce != null);
            }

            /*
             * If this has the lowest host ID, initiate the snapshot restore
             */
            if (isLowestHost()) {
                sendSnapshotTxnId(lastSnapshotTxnId);
                if (restorePath != null && restoreNonce != null) {
                    LOG.debug("Initiating snapshot " + restoreNonce +
                              " in " + restorePath);
                    initSnapshotWork(txnId,
                                     Pair.of("@SnapshotRestore",
                                             new Object[] {restorePath, restoreNonce}));
                }
            }

            /*
             * A thread to keep on sending fake heartbeats until the restore is
             * complete, or otherwise the RPQ is gonna be clogged.
             */
            new Thread(new Runnable() {
                @Override
                public void run() {
                    while (m_state == State.RESTORE ||
                           (m_state == State.REPLAY && !m_replayAgent.started())) {
                        m_initiator.sendHeartbeat(txnId + 1);

                        try {
                            Thread.sleep(500);
                        } catch (InterruptedException e) {}
                    }
                }
            }).start();

            if (!isLowestHost() || restorePath == null || restoreNonce == null) {
                /*
                 * Hosts that are not initiating the restore should change
                 * state immediately
                 */
                changeState();
            }
        }
    };

    /**
     * A dummy connection to provide to the DTXN. It routes ClientResponses back
     * to the restore agent.
     */
    private class RestoreAdapter implements Connection, WriteStream {
        @Override
        public boolean hadBackPressure() {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean enqueue(BBContainer c) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean enqueue(FastSerializable f) {
            handleResponse((ClientResponse) f);
            return true;
        }

        @Override
        public boolean enqueue(FastSerializable f, int expectedSize) {
            return enqueue(f);
        }

        @Override
        public boolean enqueue(DeferredSerialization ds) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean enqueue(ByteBuffer b) {
            throw new UnsupportedOperationException();
        }

        @Override
        public int calculatePendingWriteDelta(long now) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isEmpty() {
            throw new UnsupportedOperationException();
        }

        @Override
        public int getOutstandingMessageCount() {
            throw new UnsupportedOperationException();
        }

        @Override
        public WriteStream writeStream() {
            return this;
        }

        @Override
        public NIOReadStream readStream() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void disableReadSelection() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void enableReadSelection() {
            throw new UnsupportedOperationException();
        }

        @Override
        public String getHostname() {
            return "";
        }

        @Override
        public long connectionId() {
            return -1;
        }

        @Override
        public void scheduleRunnable(Runnable r) {
        }

        @Override
        public void unregister() {
        }
    }

    /**
     * Information about the local files of a specific snapshot that will be
     * used to generate a restore plan
     */
    private static class SnapshotInfo {
        public final long txnId;
        public final String path;
        public final String nonce;
        public final int partitionCount;
        // All the partitions for partitioned tables in the local snapshot file
        public final Map<String, Set<Integer>> partitions = new TreeMap<String, Set<Integer>>();

        public SnapshotInfo(long txnId, String path, String nonce, int partitions) {
            this.txnId = txnId;
            this.path = path;
            this.nonce = nonce;
            this.partitionCount = partitions;
        }

        public int size() {
            // txnId + pathLen + nonceLen + partCount + path + nonce
            int size = 8 + 4 + 4 + 8 + 4 + path.length() + nonce.length();
            for (Entry<String, Set<Integer>> e : partitions.entrySet()) {
                size += 4 + 4 + e.getKey().length() + 4 * e.getValue().size();
            }
            return size;
        }
    }

    /**
     * Creates a ZooKeeper directory if it doesn't exist. Crashes VoltDB if the
     * creation fails.
     *
     * @param path
     */
    private void createZKDirectory(String path) {
        try {
            try {
                m_zk.create(path, new byte[0],
                            Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            } catch (KeeperException e) {
                if (e.code() != Code.NODEEXISTS) {
                    throw e;
                }
            }
        } catch (Exception e) {
            LOG.fatal("Failed to create Zookeeper node: " + e.getMessage());
            VoltDB.crashVoltDB();
        }
    }

    public RestoreAgent(CatalogContext context, TransactionInitiator initiator,
                        int hostId)
    throws IOException {
        m_hostId = hostId;
        m_context = context;
        m_initiator = initiator;

        m_zk = new ZooKeeper("ning", 3000, this);
        zkBarrierNode = RESTORE_BARRIER + "/" + m_hostId;

        m_allPartitions = new int[m_context.numberOfPartitions];
        int i = 0;
        for (Partition p : m_context.cluster.getPartitions()) {
            m_allPartitions[i++] = Integer.parseInt(p.getTypeName());
        }

        initialize();
    }

    private void initialize() {
        // Load command log reader
        LogReader reader = null;
        try {
            String logpath = m_context.cluster.getLogconfig().get("log").getLogpath();
            File path = new File(logpath);

            Class<?> readerClass = MiscUtils.loadProClass("org.voltdb.utils.LogReaderImpl",
                                                          null, true);
            if (readerClass != null) {
                Constructor<?> constructor = readerClass.getConstructor(File.class);
                reader = (LogReader) constructor.newInstance(path);
            }
        } catch (Exception e) {
            LOG.fatal("Unable to instantiate command log reader", e);
            VoltDB.crashVoltDB();
        }

        // Load command log reinitiator
        try {
            Class<?> replayClass = MiscUtils.loadProClass("org.voltdb.CommandLogReinitiatorImpl",
                                                          "Command log replay", false);
            if (replayClass != null) {
                Constructor<?> constructor =
                    replayClass.getConstructor(LogReader.class,
                                               int.class,
                                               TransactionInitiator.class,
                                               CatalogContext.class,
                                               ZooKeeper.class);

                if (reader != null) {
                    m_replayAgent =
                        (CommandLogReinitiator) constructor.newInstance(reader,
                                                                        m_hostId,
                                                                        m_initiator,
                                                                        m_context,
                                                                        m_zk);
                } else {
                    LOG.info("No command log to replay");
                }
            }
        } catch (Exception e) {
            LOG.fatal("Unable to instantiate command log reinitiator", e);
            VoltDB.crashVoltDB();
        }
        m_replayAgent.setCallback(this);
    }

    /**
     * Enters the restore process. Creates ZooKeeper barrier node for this host.
     */
    private void enterRestore() {
        try {
            m_zk.create(zkBarrierNode, new byte[0],
                        Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        } catch (Exception e) {
            LOG.fatal("Failed to create Zookeeper node: " + e.getMessage());
            VoltDB.crashVoltDB();
        }
    }

    /**
     * Exists the restore process. Waits for all other hosts to complete first.
     * This method blocks.
     */
    private void exitRestore() {
        try {
            m_zk.delete(zkBarrierNode, -1);
        } catch (Exception ignore) {}

        LOG.debug("Waiting for all hosts to complete restore");
        List<String> children = null;
        while (true) {
            try {
                children = m_zk.getChildren(RESTORE_BARRIER, false);
            } catch (KeeperException e2) {
                LOG.fatal(e2.getMessage());
                VoltDB.crashVoltDB();
            } catch (InterruptedException e2) {
                continue;
            }

            if (children.size() > 0) {
                try {
                    Thread.sleep(500);
                } catch (InterruptedException e) {}
            } else {
                break;
            }
        }
    }

    /**
     * Start the restore process. It will first try to restore from the last
     * snapshot, then replay the logs, followed by a snapshot to truncate the
     * logs. This method won't block.
     */
    public void restore() {
        new Thread(m_restorePlanner, "restore-planner").start();
    }

    /**
     * Send the txnId of the snapshot that was picked to restore from to the
     * other hosts. If there was no snapshot to restore from, send 0.
     *
     * @param txnId
     */
    private void sendSnapshotTxnId(long txnId) {
        ByteBuffer buf = ByteBuffer.allocate(8);
        buf.putLong(txnId);

        LOG.debug("Sending snapshot ID " + txnId);
        try {
            m_zk.create(SNAPSHOT_ID, buf.array(),
                        Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        } catch (Exception e) {
            LOG.fatal("Failed to create Zookeeper node: " + e.getMessage());
            VoltDB.crashVoltDB();
        }
    }

    /**
     * Wait for the specified host to send the txnId of the snapshot the cluster
     * is restoring from.
     */
    private void waitForSnapshotTxnId() {
        long txnId = 0;
        while (true) {
            LOG.debug("Waiting for the initiator to send the snapshot txnid");
            try {
                if (m_zk.exists(SNAPSHOT_ID, false) == null) {
                    Thread.sleep(500);
                    continue;
                } else {
                    byte[] data = m_zk.getData(SNAPSHOT_ID, false, null);
                    txnId = ByteBuffer.wrap(data).getLong();
                    break;
                }
            } catch (KeeperException e2) {
                LOG.fatal(e2.getMessage());
                VoltDB.crashVoltDB();
            } catch (InterruptedException e2) {
                continue;
            }
        }

        m_replayAgent.setSnapshotTxnId(txnId);
    }

    /**
     * Send the information about the local snapshot files to the other hosts to
     * generate restore plan.
     *
     * @param min
     *            The minimum txnId of the last txn across all initiators in the
     *            local command log.
     * @param snapshots
     *            The information of the local snapshot files.
     */
    private void sendLocalRestoreInformation(long min, Set<SnapshotInfo> snapshots) {
        ByteBuffer buf = serializeRestoreInformation(min, snapshots);

        String zkNode = RESTORE + "/" + m_hostId;
        try {
            m_zk.create(zkNode, buf.array(),
                        Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        } catch (Exception e) {
            LOG.fatal("Failed to create Zookeeper node: " + e.getMessage());
            VoltDB.crashVoltDB();
        }
    }

    /**
     * Pick the snapshot to restore from based on the global snapshot
     * information.
     *
     * @return The snapshot to restore from, null if none.
     */
    private Entry<Long, Set<SnapshotInfo>> getRestorePlan() {
        /*
         * Only let the first host do the rest, so we don't end up having
         * multiple hosts trying to initiate a snapshot restore
         */
        if (!isLowestHost()) {
            return null;
        }

        LOG.debug("Waiting for all hosts to send their snapshot information");
        List<String> children = null;
        while (true) {
            try {
                children = m_zk.getChildren(RESTORE, false);
            } catch (KeeperException e2) {
                LOG.fatal(e2.getMessage());
                VoltDB.crashVoltDB();
            } catch (InterruptedException e2) {
                continue;
            }

            Set<Integer> liveHosts = m_context.siteTracker.getAllLiveHosts();
            if (children.size() < liveHosts.size()) {
                try {
                    Thread.sleep(500);
                } catch (InterruptedException e1) {}
            } else {
                break;
            }
        }

        if (children == null) {
            LOG.fatal("Unable to read agreement messages from other hosts for" +
                      " restore plan");
            VoltDB.crashVoltDB();
        }

        TreeMap<Long, Set<SnapshotInfo>> snapshotFragments = new TreeMap<Long, Set<SnapshotInfo>>();
        long clStartTxnId = deserializeRestoreInformation(children, snapshotFragments);

        // Filter all snapshots that are not viable
        Iterator<Long> iter = snapshotFragments.keySet().iterator();
        while (iter.hasNext()) {
            Long txnId = iter.next();
            if (txnId < clStartTxnId) {
                iter.remove();
            }
        }

        LOG.debug("There are " + snapshotFragments.size() +
                  " snapshots available in the cluster");
        if (clStartTxnId > 0 && snapshotFragments.size() == 0) {
            LOG.fatal("No viable snapshots to restore");
            VoltDB.crashVoltDB();
        }

        // Find the last complete snapshot and use it
        HashMap<Long, Map<String, Set<Integer>>> snapshotTablePartitions =
            new HashMap<Long, Map<String,Set<Integer>>>();
        Iterator<Entry<Long, Set<SnapshotInfo>>> it = snapshotFragments.entrySet().iterator();
        while (it.hasNext()) {
            Entry<Long, Set<SnapshotInfo>> e = it.next();
            long txnId = e.getKey();
            Map<String, Set<Integer>> tablePartitions = snapshotTablePartitions.get(txnId);
            if (tablePartitions == null) {
                tablePartitions = new HashMap<String, Set<Integer>>();
                snapshotTablePartitions.put(txnId, tablePartitions);
            }

            int totalPartitions = -1;
            boolean inconsistent = false;
            Set<SnapshotInfo> fragments = e.getValue();
            for (SnapshotInfo s : fragments) {
                if (totalPartitions == -1) {
                    totalPartitions = s.partitionCount;
                } else if (totalPartitions != s.partitionCount) {
                    inconsistent = true;
                    break;
                }

                for (Entry<String, Set<Integer>> entry : s.partitions.entrySet()) {
                    Set<Integer> partitions = tablePartitions.get(entry.getKey());
                    if (partitions == null) {
                        tablePartitions.put(entry.getKey(), entry.getValue());
                    } else {
                        partitions.addAll(entry.getValue());
                    }
                }

                if (inconsistent) {
                    break;
                }
            }

            // Check if we have all the partitions
            for (Set<Integer> partitions : tablePartitions.values()) {
                if (partitions.size() != totalPartitions) {
                    inconsistent = true;
                    break;
                }
            }

            if (inconsistent) {
                it.remove();
            }
        }

        if (clStartTxnId > 0 && snapshotFragments.size() == 0) {
            LOG.fatal("No viable snapshots to restore");
            VoltDB.crashVoltDB();
        }

        return snapshotFragments.lastEntry();
    }

    /**
     * @param children
     * @param snapshotFragments
     * @return
     */
    private long deserializeRestoreInformation(List<String> children,
                                               Map<Long, Set<SnapshotInfo>> snapshotFragments) {
        long clStartTxnId = 0;
        ByteBuffer buf;
        for (String node : children) {
            byte[] data = null;
            try {
                data = m_zk.getData(RESTORE + "/" + node, false, null);
            } catch (Exception e) {
                LOG.fatal(e.getMessage());
                VoltDB.crashVoltDB();
            }

            buf = ByteBuffer.wrap(data);
            long minTxnId = buf.getLong();
            if (minTxnId > clStartTxnId) {
                clStartTxnId = minTxnId;
            }

            int count = buf.getInt();
            for (int i = 0; i < count; i++) {
                long txnId = buf.getLong();
                Set<SnapshotInfo> fragments = snapshotFragments.get(txnId);
                if (fragments == null) {
                    fragments = new HashSet<SnapshotInfo>();
                    snapshotFragments.put(txnId, fragments);
                }

                int len = buf.getInt();
                byte[] nonceBytes = new byte[len];
                buf.get(nonceBytes);

                len = buf.getInt();
                byte[] pathBytes = new byte[len];
                buf.get(pathBytes);

                int totalPartitionCount = buf.getInt();

                SnapshotInfo info = new SnapshotInfo(txnId, new String(pathBytes),
                                                     new String(nonceBytes),
                                                     totalPartitionCount);
                fragments.add(info);

                int tableCount = buf.getInt();
                for (int j = 0; j < tableCount; j++) {
                    len = buf.getInt();
                    byte[] tableNameBytes = new byte[len];
                    buf.get(tableNameBytes);

                    int partitionCount = buf.getInt();
                    HashSet<Integer> partitions = new HashSet<Integer>(partitionCount);
                    info.partitions.put(new String(tableNameBytes), partitions);
                    for (int k = 0; k < partitionCount; k++) {
                        partitions.add(buf.getInt());
                    }
                }
            }
        }
        return clStartTxnId;
    }

    /**
     * @param min
     * @param snapshots
     * @return
     */
    private ByteBuffer serializeRestoreInformation(long min, Set<SnapshotInfo> snapshots) {
        int size = 8 + 4;
        for (SnapshotInfo i : snapshots) {
            size += i.size();
        }

        ByteBuffer buf = ByteBuffer.allocate(size);
        buf.putLong(min);
        buf.putInt(snapshots.size());
        for (SnapshotInfo snapshot : snapshots) {
            buf.putLong(snapshot.txnId);
            buf.putInt(snapshot.nonce.length());
            buf.put(snapshot.nonce.getBytes());
            buf.putInt(snapshot.path.length());
            buf.put(snapshot.path.getBytes());
            buf.putInt(snapshot.partitionCount);
            buf.putInt(snapshot.partitions.size());
            for (Entry<String, Set<Integer>> p : snapshot.partitions.entrySet()) {
                buf.putInt(p.getKey().length());
                buf.put(p.getKey().getBytes());
                buf.putInt(p.getValue().size());
                for (int id : p.getValue()) {
                    buf.putInt(id);
                }
            }
        }
        return buf;
    }

    /**
     * Initiate a snapshot action to truncate the logs. This should only be
     * called by one initiator.
     *
     * @param txnId
     *            The transaction ID of the SPI to generate
     * @param invocation
     *            The invocation used to create the SPI
     */
    private void initSnapshotWork(Long txnId, final Pair<String, Object[]> invocation) {
        Config restore = SystemProcedureCatalog.listing.get(invocation.getFirst());
        Procedure restoreProc = restore.asCatalogProcedure();
        StoredProcedureInvocation spi = new StoredProcedureInvocation();
        spi.procName = invocation.getFirst();
        spi.params = new FutureTask<ParameterSet>(new Callable<ParameterSet>() {
            @Override
            public ParameterSet call() throws Exception {
                ParameterSet params = new ParameterSet();
                params.setParameters(invocation.getSecond());
                return params;
            }
        });

        if (txnId == null) {
            m_initiator.createTransaction(-1, "CommandLog", true, spi,
                                          restoreProc.getReadonly(),
                                          restoreProc.getSinglepartition(),
                                          restoreProc.getEverysite(),
                                          m_allPartitions, m_allPartitions.length,
                                          m_restoreAdapter, 0,
                                          EstTime.currentTimeMillis());
        } else {
            m_initiator.createTransaction(-1, "CommandLog", true,
                                          txnId, spi,
                                          restoreProc.getReadonly(),
                                          restoreProc.getSinglepartition(),
                                          restoreProc.getEverysite(),
                                          m_allPartitions, m_allPartitions.length,
                                          m_restoreAdapter, 0,
                                          EstTime.currentTimeMillis());
        }
    }

    private void handleResponse(ClientResponse res) {
        boolean failure = false;
        if (res.getStatus() != ClientResponse.SUCCESS) {
            failure = true;
        }

        VoltTable[] results = res.getResults();
        if (results == null || results.length != 1) {
            failure = true;
        }
        while (!failure && results[0].advanceRow()) {
            String resultStatus = results[0].getString("RESULT");
            if (!resultStatus.equalsIgnoreCase("success")) {
                failure = true;
            }
        }

        if (failure) {
            if (m_state == State.RESTORE) {
                LOG.fatal("Failed to restore from snapshot: " +
                          res.getStatusString());
            } else {
                LOG.fatal("Failed to truncate command logs by snapshot: " +
                          res.getStatusString());
            }
            VoltDB.crashVoltDB();
        } else {
            changeState();
        }
    }

    /**
     * Change the state of the restore agent based on the current state.
     */
    private void changeState() {
        if (m_state == State.RESTORE) {
            waitForSnapshotTxnId();
            exitRestore();
            m_replayAgent.replay();
            m_state = State.REPLAY;
        } else if (m_state == State.REPLAY) {
            m_state = State.TRUNCATE;
        } else if (m_state == State.TRUNCATE) {
            try {
                m_zk.close();
            } catch (InterruptedException ignore) {}
            VoltDB.instance().onRestoreCompletion();
        }
    }

    @Override
    public void onReplayCompletion() {
        if (m_replayAgent.hasReplayed()) {
            /*
             * If this has the lowest host ID, initiate the snapshot that
             * will truncate the logs
             */
            if (isLowestHost()) {
                String path =
                    m_context.cluster.getLogconfig().get("log").getInternalsnapshotpath();
                Date now = new Date(System.currentTimeMillis());
                String nonce = CL_NONCE_PREFIX + m_dateFormat.format(now);
                initSnapshotWork(null, Pair.of("@SnapshotSave",
                                               new Object[] {path, nonce, 1}));
            } else {
                m_state = State.TRUNCATE;
            }
        } else {
            m_state = State.TRUNCATE;
        }

        changeState();
    }

    private boolean isLowestHost() {
        // If this is null, it must be running test
        if (m_hostId != null) {
            int lowestSite = m_context.siteTracker.getLowestLiveNonExecSiteId();
            int lowestHost = m_context.siteTracker.getHostForSite(lowestSite);
            return m_hostId == lowestHost;
        } else {
            return false;
        }
    }

    /**
     * Finds all the snapshots in all the places we know of which could possibly
     * store snapshots, like command log snapshots, auto snapshots, etc.
     *
     * @return All snapshots
     */
    private TreeMap<Long, Snapshot> getSnapshots() {
        /*
         * Use the individual snapshot directories instead of voltroot, because
         * they can be set individually
         */
        List<String> paths = new ArrayList<String>();
        paths.add(m_context.cluster.getLogconfig().get("log").getInternalsnapshotpath());
        if (m_context.cluster.getDatabases().get("database").getSnapshotschedule().get("default") != null) {
            paths.add(m_context.cluster.getDatabases().get("database").getSnapshotschedule().get("default").getPath());
        }
        if (m_context.cluster.getFaultsnapshots().get("CLUSTER_PARTITION") != null) {
            paths.add(m_context.cluster.getFaultsnapshots().get("CLUSTER_PARTITION").getPath());
        }
        TreeMap<Long, Snapshot> snapshots = new TreeMap<Long, Snapshot>();
        FileFilter filter = new SnapshotUtil.SnapshotFilter();

        for (String path : paths) {
            SnapshotUtil.retrieveSnapshotFiles(new File(path), snapshots, filter, 0, true);
        }

        return snapshots;
    }

    /**
     * Get the nonce from the filename of the digest file.
     * @param filename The filename of the digest file
     * @return The nonce
     */
    private static String parseDigestFilename(String filename) {
        if (filename == null || !filename.endsWith(".digest")) {
            throw new IllegalArgumentException();
        }

        String nonce = filename.substring(0, filename.indexOf(".digest"));
        if (nonce.contains("-host_")) {
            nonce = nonce.substring(0, nonce.indexOf("-host_"));
        }

        return nonce;
    }

    @Override
    public void process(WatchedEvent event) {
        m_flag.release();
    }
}
