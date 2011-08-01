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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.FutureTask;
import org.apache.zookeeper_voltpatches.CreateMode;
import org.apache.zookeeper_voltpatches.KeeperException;
import org.apache.zookeeper_voltpatches.KeeperException.Code;
import org.apache.zookeeper_voltpatches.ZooDefs.Ids;
import org.apache.zookeeper_voltpatches.ZooKeeper;
import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;
import org.voltdb.SystemProcedureCatalog.Config;
import org.voltdb.VoltDB.START_ACTION;
import org.voltdb.catalog.CommandLog;
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
public class RestoreAgent implements CommandLogReinitiator.Callback,
SnapshotCompletionInterest {
    // Implement this callback to get notified when restore finishes.
    public interface Callback {
        /**
         * Callback function executed when restore finishes.
         *
         * @param txnId
         *            The txnId of the truncation snapshot at the end of the
         *            restore, or Long.MIN if there is none.
         * @param initCommandLog
         *            Whether or not to initialize the command log module
         */
        public void onRestoreCompletion(long txnId, boolean initCommandLog);
    }

    private final static VoltLogger LOG = new VoltLogger("HOST");

    // ZK stuff
    private final static String RESTORE = "/restore";
    private final static String RESTORE_BARRIER = "/restore_barrier";
    private final static String SNAPSHOT_ID = "/restore/snapshot_id";
    private final String zkBarrierNode;

    // Transaction ID of the restore sysproc
    private final static long RESTORE_TXNID = 1l;

    private final Integer m_hostId;
    private final CatalogContext m_context;
    private final TransactionInitiator m_initiator;
    private final SnapshotCompletionMonitor m_snapshotMonitor;
    private final Callback m_callback;
    private final START_ACTION m_action;

    // The txnId of the truncation snapshot generated at the end.
    private long m_truncationSnapshot = Long.MIN_VALUE;

    private final ZooKeeper m_zk;

    private final int[] m_allPartitions;

    // Different states the restore process can be in
    private enum State { RESTORE, REPLAY, TRUNCATE };

    // State of the restore agent
    private volatile State m_state = State.RESTORE;
    private final RestoreAdapter m_restoreAdapter = new RestoreAdapter();
    // Whether or not we have a snapshot to restore
    private boolean m_hasRestored = false;

    private CommandLogReinitiator m_replayAgent = new CommandLogReinitiator() {
        private Callback m_callback;
        @Override
        public void setCallback(Callback callback) {
            m_callback = callback;
        }
        @Override
        public void replay() {
            new Thread(new Runnable() {
                @Override
                public void run() {
                    if (m_callback != null) {
                        m_callback.onReplayCompletion();
                    }
                }
            }).start();
        }
        @Override
        public void join() throws InterruptedException {}
        @Override
        public boolean hasReplayed() {
            return false;
        }
        @Override
        public Long getMinLastSeenTxn() {
            return null;
        }
        @Override
        public boolean started() {
            return true;
        }
        @Override
        public void setSnapshotTxnId(long txnId) {}
        @Override
        public void returnAllSegments() {}
    };

    /*
     * A thread to keep on sending fake heartbeats until the restore is
     * complete, or otherwise the RPQ is gonna be clogged.
     */
    private Thread m_restoreHeartbeatThread = new Thread(new Runnable() {
        @Override
        public void run() {
            while (m_state == State.RESTORE) {
                m_initiator.sendHeartbeat(RESTORE_TXNID + 1);
                try {
                    Thread.sleep(500);
                } catch (InterruptedException e) {}
            }
        }
    });

    private Runnable m_restorePlanner = new Runnable() {
        @Override
        public void run() {
            createZKDirectory(RESTORE);
            createZKDirectory(RESTORE_BARRIER);

            enterRestore();

            TreeMap<Long, Snapshot> snapshots = new TreeMap<Long, SnapshotUtil.Snapshot>();
            /*
             * If the user wants to create a new database, don't scan the
             * snapshots.
             */
            if (m_action != START_ACTION.CREATE) {
                snapshots = getSnapshots();
            }

            final Long minLastSeenTxn = m_replayAgent.getMinLastSeenTxn();
            Set<SnapshotInfo> snapshotInfos = new HashSet<SnapshotInfo>();
            for (Entry<Long, Snapshot> e : snapshots.entrySet()) {
                /*
                 * If the txn of the snapshot is before the earliest txn
                 * among the last seen txns across all initiators when the
                 * log starts, there is a gap in between the snapshot was
                 * taken and the beginning of the log. So the snapshot is
                 * not viable for replay.
                 */
                if (minLastSeenTxn != null && e.getKey() < minLastSeenTxn) {
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

                Long catalog_crc = null;
                try
                {
                    JSONObject digest_detail = SnapshotUtil.CRCCheck(digest);
                    catalog_crc = digest_detail.getLong("catalogCRC");
                }
                catch (IOException ioe)
                {
                    LOG.info("Unable to read digest file: " +
                             digest.getAbsolutePath() + " due to: " + ioe.getMessage());
                    skip = true;
                }
                catch (JSONException je)
                {
                    LOG.info("Unable to extract catalog CRC from digest: " +
                             digest.getAbsolutePath() + " due to: " + je.getMessage());
                    skip = true;
                }

                if (skip) {
                    continue;
                }

                SnapshotInfo info =
                    new SnapshotInfo(e.getKey(), digest.getParent(),
                                     parseDigestFilename(digest.getName()),
                                     partitionCount, catalog_crc);
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
            LOG.debug("Gathered " + snapshotInfos.size() + " snapshot information");

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
                    initSnapshotWork(RESTORE_TXNID,
                                     Pair.of("@SnapshotRestore",
                                             new Object[] {restorePath, restoreNonce}));
                }
            }

            m_restoreHeartbeatThread.start();

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
    static class SnapshotInfo {
        public final long txnId;
        public final String path;
        public final String nonce;
        public final int partitionCount;
        public final long catalogCrc;
        // All the partitions for partitioned tables in the local snapshot file
        public final Map<String, Set<Integer>> partitions = new TreeMap<String, Set<Integer>>();

        public SnapshotInfo(long txnId, String path, String nonce, int partitions,
                            long catalogCrc)
        {
            this.txnId = txnId;
            this.path = path;
            this.nonce = nonce;
            this.partitionCount = partitions;
            this.catalogCrc = catalogCrc;
        }

        public int size() {
            // I can't make this add up --izzy
            // txnId + pathLen + nonceLen + partCount + catalogCrc + path + nonce
            int size = 8 + 4 + 4 + 8 + 8 + 4 + path.length() + nonce.length();
            for (Entry<String, Set<Integer>> e : partitions.entrySet()) {
                size += 4 + 4 + e.getKey().length() + 4 * e.getValue().size();
            }
            return size;
        }
    }

    /**
     * Creates a ZooKeeper directory if it doesn't exist. Crashes VoltDB if the
     * creation fails for any reason other then the path already existing.
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
                        ZooKeeper zk, SnapshotCompletionMonitor snapshotMonitor,
                        Callback callback, int hostId, START_ACTION action)
    throws IOException {
        m_hostId = hostId;
        m_context = context;
        m_initiator = initiator;
        m_snapshotMonitor = snapshotMonitor;
        m_callback = callback;
        m_action = action;
        m_zk = zk;

        zkBarrierNode = RESTORE_BARRIER + "/" + m_hostId;

        m_allPartitions = new int[m_context.numberOfPartitions];
        int i = 0;
        for (Partition p : m_context.cluster.getPartitions()) {
            m_allPartitions[i++] = Integer.parseInt(p.getTypeName());
        }

        initialize();
    }

    private void initialize() {
        // Load command log reinitiator
        try {
            Class<?> replayClass = MiscUtils.loadProClass("org.voltdb.CommandLogReinitiatorImpl",
                                                          "Command log replay", false);
            if (replayClass != null) {
                Constructor<?> constructor =
                    replayClass.getConstructor(int.class,
                                               START_ACTION.class,
                                               TransactionInitiator.class,
                                               CatalogContext.class,
                                               ZooKeeper.class,
                                               long.class);

                m_replayAgent =
                    (CommandLogReinitiator) constructor.newInstance(m_hostId,
                                                                    m_action,
                                                                    m_initiator,
                                                                    m_context,
                                                                    m_zk,
                                                                    RESTORE_TXNID + 1);
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

        LOG.debug("Sending snapshot ID " + txnId + " for restore to other nodes");
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
                    Thread.sleep(200);
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

        // If the txnId is not 0, it means we restored a snapshot
        if (txnId != 0) {
            m_hasRestored = true;
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
    private void sendLocalRestoreInformation(Long min, Set<SnapshotInfo> snapshots) {
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
     * @param clStartTxnId  The TXN ID at which the command log starts.
     *                      NULL means no command log exists.
     *                      Long.MIN_VALUE means the command log has no
     *                      associated snapshot.
     * @param snapshotFragments  A map of the TXNID/SnapshotInfo reported by
     *                           the other hosts in the cluster.  Will be
     *                           modified as snapshots are removed because
     *                           they are not compatible with the command log
     * @return Whether or not any of the snapshots matched the current
     *         catalog's CRC check when there was a command log to replay
     */
    static boolean currySnapshotInfo(Long clStartTxnId, long currentCatalogCrc,
                              Map<Long, Set<SnapshotInfo>> snapshotFragments)
    {
        boolean crc_catalog_match = false;
        if (clStartTxnId != null && clStartTxnId == Long.MIN_VALUE)
        {
            // command log has no snapshot requirement.  Just clear out the
            // fragment set directly
            snapshotFragments.clear();
            // Quick hack.  replace with command log catalog crc check
            crc_catalog_match = true;
        }
        else
        {
            // Filter all snapshots that are not viable
            Iterator<Long> iter = snapshotFragments.keySet().iterator();
            while (iter.hasNext()) {
                Long txnId = iter.next();
                long this_crc = snapshotFragments.get(txnId).iterator().next().catalogCrc;
                if (this_crc == currentCatalogCrc)
                {
                    crc_catalog_match = true;
                }
                if (clStartTxnId != null &&
                    (this_crc != currentCatalogCrc ||
                     txnId < clStartTxnId))
                {
                    iter.remove();
                }
            }
        }

        return crc_catalog_match;
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
        Long clStartTxnId = deserializeRestoreInformation(children, snapshotFragments);

        // If we're not recovering, skip the rest
        if (m_action == START_ACTION.CREATE) {
            return null;
        }

        boolean crc_catalog_match = false;
        // Eliminate any snapshot fragments that don't match our command log (if we have one)
        crc_catalog_match = currySnapshotInfo(clStartTxnId, m_context.catalogCRC,
                                              snapshotFragments);
        // If we have a command log and it requires a snapshot, then bail if
        // there's no good snapshot
        if ((clStartTxnId != null && clStartTxnId != Long.MIN_VALUE) &&
            snapshotFragments.size() == 0)
        {
            if (!crc_catalog_match)
            {
                LOG.fatal("No snapshot present that matches the loaded catalog.");
            }
            LOG.fatal("No viable snapshots to restore");
            VoltDB.crashVoltDB();
        }
        LOG.debug("There are " + snapshotFragments.size() +
                  " snapshots available in the cluster");

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

        if (clStartTxnId != null && clStartTxnId > 0 &&
            snapshotFragments.size() == 0) {
            LOG.fatal("No viable snapshots to restore");
            VoltDB.crashVoltDB();
        }

        return snapshotFragments.lastEntry();
    }

    /**
     * @param children
     * @param snapshotFragments
     * @return null if there is no log to replay in the whole cluster
     */
    private Long deserializeRestoreInformation(List<String> children,
                                               Map<Long, Set<SnapshotInfo>> snapshotFragments) {
        byte recover = (byte) m_action.ordinal();
        Long clStartTxnId = null;
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
            // Check if there is log to replay
            boolean hasLog = buf.get() == 1;
            if (hasLog) {
                long minTxnId = buf.getLong();
                if (clStartTxnId == null || minTxnId > clStartTxnId) {
                    clStartTxnId = minTxnId;
                }
            }

            byte recoverByte = buf.get();
            if (recoverByte != recover) {
                LOG.fatal("Database actions are not consistent, please enter " +
                          "the same database action on the command-line.");
                VoltDB.crashVoltDB();
            }

            int count = buf.getInt();
            for (int i = 0; i < count; i++) {
                long txnId = buf.getLong();
                Set<SnapshotInfo> fragments = snapshotFragments.get(txnId);
                if (fragments == null) {
                    fragments = new HashSet<SnapshotInfo>();
                    snapshotFragments.put(txnId, fragments);
                }
                long catalogCrc = buf.getLong();

                int len = buf.getInt();
                byte[] nonceBytes = new byte[len];
                buf.get(nonceBytes);

                len = buf.getInt();
                byte[] pathBytes = new byte[len];
                buf.get(pathBytes);

                int totalPartitionCount = buf.getInt();

                SnapshotInfo info = new SnapshotInfo(txnId, new String(pathBytes),
                                                     new String(nonceBytes),
                                                     totalPartitionCount,
                                                     catalogCrc);
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
    private ByteBuffer serializeRestoreInformation(Long min, Set<SnapshotInfo> snapshots) {
        // hasLog + recover + snapshotCount
        int size = 1 + 1 + 4;
        if (min != null) {
            // we need to add the size of the min number to the total size
            size += 8;
        }
        for (SnapshotInfo i : snapshots) {
            size += i.size();
        }

        ByteBuffer buf = ByteBuffer.allocate(size);
        if (min == null) {
            buf.put((byte) 0);
        } else {
            buf.put((byte) 1);
            buf.putLong(min);
        }
        // 1 means recover, 0 means to create new DB
        buf.put((byte) m_action.ordinal());

        buf.putInt(snapshots.size());
        for (SnapshotInfo snapshot : snapshots) {
            buf.putLong(snapshot.txnId);
            buf.putLong(snapshot.catalogCrc);
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
            LOG.fatal("Failed to restore from snapshot: " +
                      res.getStatusString());
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
            m_state = State.REPLAY;

            try {
                m_restoreHeartbeatThread.join();
            } catch (InterruptedException e) {}

            /*
             * Add the interest here so that we can use the barriers in replay
             * agent to synchronize.
             */
            m_snapshotMonitor.addInterest(this);
            m_replayAgent.replay();
        } else if (m_state == State.REPLAY) {
            m_state = State.TRUNCATE;
        } else if (m_state == State.TRUNCATE) {
            m_snapshotMonitor.removeInterest(this);
            if (m_callback != null) {
                m_callback.onRestoreCompletion(m_truncationSnapshot, true);
            }
        }
    }

    @Override
    public void onReplayCompletion() {
        if (!m_hasRestored && !m_replayAgent.hasReplayed() &&
            m_action == START_ACTION.RECOVER) {
            /*
             * This means we didn't restore any snapshot, and there's no command
             * log to replay. But the user asked for recover
             */
            LOG.fatal("Nothing to recover from");
            VoltDB.crashVoltDB();
        } else if (!m_replayAgent.hasReplayed()) {
            // Nothing was replayed, so no need to initiate truncation snapshot
            m_state = State.TRUNCATE;
        }

        changeState();

        if (m_replayAgent.hasReplayed()) {
            /*
             * If this has the lowest host ID, initiate the snapshot that
             * will truncate the logs
             */
            CommandLog commandLogElement = m_context.cluster.getLogconfig().get("log");
            if (isLowestHost() && commandLogElement != null) {
                try {
                    try {
                        m_zk.create("/truncation_snapshot_path",
                                commandLogElement.getInternalsnapshotpath().getBytes(),
                                Ids.OPEN_ACL_UNSAFE,
                                CreateMode.PERSISTENT);
                    } catch (KeeperException.NodeExistsException e) {}
                    m_zk.create("/request_truncation_snapshot", null,
                                Ids.OPEN_ACL_UNSAFE,
                                CreateMode.PERSISTENT);
                } catch (Exception e) {
                    LOG.fatal("Requesting a truncation snapshot via ZK should always succeed", e);
                    VoltDB.crashVoltDB();
                }
            }
        }
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
        CommandLog commandLogElement = m_context.cluster.getLogconfig().get("log");
        if (commandLogElement != null) {
            paths.add(commandLogElement.getInternalsnapshotpath());
        }
        if (m_context.cluster.getDatabases().get("database").getSnapshotschedule().get("default") != null) {
            paths.add(m_context.cluster.getDatabases().get("database").getSnapshotschedule().get("default").getPath());
        }
        TreeMap<Long, Snapshot> snapshots = new TreeMap<Long, Snapshot>();
        FileFilter filter = new SnapshotUtil.SnapshotFilter();

        for (String path : paths) {
            SnapshotUtil.retrieveSnapshotFiles(new File(path), snapshots, filter, 0, false);
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

    /**
     * All nodes will be notified about the completion of the truncation
     * snapshot.
     */
    @Override
    public CountDownLatch snapshotCompleted(long txnId,
                                            boolean truncationSnapshot) {
        if (!truncationSnapshot) {
            LOG.fatal("Failed to truncate command logs by snapshot");
            VoltDB.crashVoltDB();
        } else {
            m_truncationSnapshot = txnId;
            m_replayAgent.returnAllSegments();
            changeState();
        }
        return new CountDownLatch(0);
    }
}
