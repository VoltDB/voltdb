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

package org.voltdb;

import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;

import org.apache.zookeeper_voltpatches.CreateMode;
import org.apache.zookeeper_voltpatches.KeeperException;
import org.apache.zookeeper_voltpatches.KeeperException.Code;
import org.apache.zookeeper_voltpatches.ZooDefs.Ids;
import org.apache.zookeeper_voltpatches.ZooKeeper;
import org.json_voltpatches.JSONArray;
import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;
import org.json_voltpatches.JSONStringer;
import org.voltcore.logging.VoltLogger;
import org.voltcore.messaging.HostMessenger;
import org.voltcore.utils.InstanceId;
import org.voltcore.utils.Pair;
import org.voltdb.SystemProcedureCatalog.Config;
import org.voltdb.catalog.Procedure;
import org.voltdb.client.ClientResponse;
import org.voltdb.common.Constants;
import org.voltdb.dtxn.TransactionCreator;
import org.voltdb.sysprocs.saverestore.SnapshotUtil;
import org.voltdb.sysprocs.saverestore.SnapshotUtil.Snapshot;
import org.voltdb.sysprocs.saverestore.SnapshotUtil.TableFiles;
import org.voltdb.utils.InMemoryJarfile;
import org.voltdb.utils.MiscUtils;

import com.google_voltpatches.common.collect.ImmutableSet;

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
 *
 * Making it a Promotable and elected by the GlobalServiceElector so that it's
 * colocated with the MPI.
 */
public class RestoreAgent implements CommandLogReinitiator.Callback,
SnapshotCompletionInterest, Promotable
{
    // Implement this callback to get notified when restore finishes.
    public interface Callback {
        /**
         * Callback function executed when restore finishes.
         *
         * @param txnId
         *            The txnId of the truncation snapshot at the end of the
         *            restore, or Long.MIN if there is none.
         */
        public void onRestoreCompletion(long txnId, Map<Integer, Long> perPartitionTxnIds);
    }

    private final static VoltLogger LOG = new VoltLogger("HOST");

    private String m_generatedRestoreBarrier2;

    // Different states the restore process can be in
    private enum State { RESTORE, REPLAY, TRUNCATE }

    // Current state of the restore agent
    private volatile State m_state = State.RESTORE;

    // Restore adapter needs a completion functor.
    // Runnable here preferable to exposing all of RestoreAgent to RestoreAdapater.
    private final Runnable m_changeStateFunctor = new Runnable() {
        @Override
        public void run() {
            //After restore it is safe to initialize partition tracking
            changeState();
        }
    };

    private final SimpleClientResponseAdapter m_restoreAdapter =
        new SimpleClientResponseAdapter(ClientInterface.RESTORE_AGENT_CID, "RestoreAgentAdapter");

    private final ZooKeeper m_zk;
    private final SnapshotCompletionMonitor m_snapshotMonitor;
    private final Callback m_callback;
    private final Integer m_hostId;
    private final StartAction m_action;
    private final boolean m_clEnabled;
    private final String m_clPath;
    private final String m_clSnapshotPath;
    private final String m_snapshotPath;
    private final String m_voltdbrootPath;
    private final Set<Integer> m_liveHosts;

    private boolean m_planned = false;

    private boolean m_isLeader = false;

    private TransactionCreator m_initiator;

    // The snapshot to restore
    private SnapshotInfo m_snapshotToRestore = null;

    // The txnId of the truncation snapshot generated at the end.
    private long m_truncationSnapshot = Long.MIN_VALUE;
    private Map<Integer, Long> m_truncationSnapshotPerPartition = new HashMap<Integer, Long>();

    // Whether or not we have a snapshot to restore
    private boolean m_hasRestored = false;

    // A string builder to hold all snapshot validation errors, gets printed when no viable snapshot is found
    private final StringBuilder m_snapshotErrLogStr =
            new StringBuilder("The restore process can not find a viable snapshot. "
                              + "Restore requires a complete, uncorrupted "
                              + "snapshot that is paired with the command log. "
                              + "All identified errors are listed next."
                              + "This is an unexpected condition. "
                              + "Please contact support@voltdb.com\n");

    private CommandLogReinitiator m_replayAgent = new DefaultCommandLogReinitiator();

    private final Runnable m_restorePlanner = new Runnable() {
        @Override
        public void run() {
            if (!m_planned) {
                // TestRestoreAgent doesn't follow the same plan-making code as
                // Inits and may require the runnable to initialize its own restore
                // catalog plan.
                findRestoreCatalog();
            }

            try {
                if (!m_isLeader) {
                    // wait on the leader's barrier.
                    while (m_zk.exists(VoltZK.restore_snapshot_id, null) == null) {
                        Thread.sleep(200);
                    }
                    changeState();
                }
                else {
                    // will release the non-leaders waiting on VoltZK.restore_snapshot_id.
                    sendSnapshotTxnId(m_snapshotToRestore);

                    if (m_snapshotToRestore != null) {
                        LOG.debug("Initiating snapshot " + m_snapshotToRestore.nonce +
                                " in " + m_snapshotToRestore.path);
                        JSONObject jsObj = new JSONObject();
                        jsObj.put(SnapshotUtil.JSON_PATH, m_snapshotToRestore.path);
                        jsObj.put(SnapshotUtil.JSON_NONCE, m_snapshotToRestore.nonce);
                        if (m_action == StartAction.SAFE_RECOVER) {
                            jsObj.put(SnapshotUtil.JSON_DUPLICATES_PATH, m_voltdbrootPath);
                        }
                        if (m_replayAgent.hasReplayedSegments() &&
                            TheHashinator.getConfiguredHashinatorType() == TheHashinator.HashinatorType.ELASTIC) {
                            // Restore the hashinator if there's command log to replay and we're running elastic
                            jsObj.put(SnapshotUtil.JSON_HASHINATOR, true);
                        }
                        Object[] params = new Object[] { jsObj.toString() };
                        initSnapshotWork(params);
                    }

                    // if no snapshot to restore, transition immediately.
                    if (m_snapshotToRestore == null) {
                        changeState();
                    }
                }
            }
            catch (Exception e)
            {
                VoltDB.crashGlobalVoltDB("Failed to safely enter recovery: " + e.getMessage(),
                                         true, e);
            }
        }
    };

    /**
     * Information about the local files of a specific snapshot that will be
     * used to generate a restore plan
     */
    public static class SnapshotInfo {
        public final long txnId;
        public final String path;
        public final String nonce;
        public final int partitionCount;
        // If this is a truncation snapshot that is on the boundary of partition count change
        // newPartitionCount will record the partition count after the topology change,
        // otherwise it's the same as partitionCount
        public final int newPartitionCount;
        public final long catalogCrc;
        // All the partitions for partitioned tables in the local snapshot file
        public final Map<String, Set<Integer>> partitions = new TreeMap<String, Set<Integer>>();
        public final Map<Integer, Long> partitionToTxnId = new TreeMap<Integer, Long>();
        // This is not serialized, the name of the ZK node already has the host ID embedded.
        public final int hostId;
        public final InstanceId instanceId;
        // Track the tables contained in the snapshot digest file
        public final Set<String> digestTables = new HashSet<String>();
        // Track the tables for which we found files on the node reporting this SnapshotInfo
        public final Set<String> fileTables = new HashSet<String>();

        public void setPidToTxnIdMap(Map<Integer,Long> map) {
            partitionToTxnId.putAll(map);
        }

        public SnapshotInfo(long txnId, String path, String nonce,
                            int partitions, int newPartitionCount,
                            long catalogCrc, int hostId, InstanceId instanceId,
                            Set<String> digestTables)
        {
            this.txnId = txnId;
            this.path = path;
            this.nonce = nonce;
            this.partitionCount = partitions;
            this.newPartitionCount = newPartitionCount;
            this.catalogCrc = catalogCrc;
            this.hostId = hostId;
            this.instanceId = instanceId;
            this.digestTables.addAll(digestTables);
        }

        public SnapshotInfo(JSONObject jo) throws JSONException
        {
            txnId = jo.getLong("txnId");
            path = jo.getString("path");
            nonce = jo.getString("nonce");
            partitionCount = jo.getInt("partitionCount");
            newPartitionCount = jo.getInt("newPartitionCount");
            catalogCrc = jo.getLong("catalogCrc");
            hostId = jo.getInt("hostId");
            instanceId = new InstanceId(jo.getJSONObject("instanceId"));

            JSONArray tables = jo.getJSONArray("tables");
            int cnt = tables.length();
            for (int i=0; i < cnt; i++) {
                JSONObject tableEntry = tables.getJSONObject(i);
                String name = tableEntry.getString("name");
                JSONArray jsonPartitions = tableEntry.getJSONArray("partitions");
                Set<Integer> partSet = new HashSet<Integer>();
                int partCnt = jsonPartitions.length();
                for (int j=0; j < partCnt; j++) {
                    int p = jsonPartitions.getInt(j);
                    partSet.add(p);
                }
                partitions.put(name, partSet);
            }
            JSONObject jsonPtoTxnId = jo.getJSONObject("partitionToTxnId");
            @SuppressWarnings("unchecked")
            Iterator<String> it = jsonPtoTxnId.keys();
            while (it.hasNext()) {
                 String key = it.next();
                 Long val = jsonPtoTxnId.getLong(key);
                 partitionToTxnId.put(Integer.valueOf(key), val);
            }
            JSONArray jdt = jo.getJSONArray("digestTables");
            for (int i = 0; i < jdt.length(); i++) {
                digestTables.add(jdt.getString(i));
            }
            JSONArray ft = jo.getJSONArray("fileTables");
            for (int i = 0; i < ft.length(); i++) {
                fileTables.add(ft.getString(i));
            }
        }

        public JSONObject toJSONObject()
        {
            JSONStringer stringer = new JSONStringer();
            try {
                stringer.object();
                stringer.key("txnId").value(txnId);
                stringer.key("path").value(path);
                stringer.key("nonce").value(nonce);
                stringer.key("partitionCount").value(partitionCount);
                stringer.key("newPartitionCount").value(newPartitionCount);
                stringer.key("catalogCrc").value(catalogCrc);
                stringer.key("hostId").value(hostId);
                stringer.key("tables").array();
                for (Entry<String, Set<Integer>> p : partitions.entrySet()) {
                    stringer.object();
                    stringer.key("name").value(p.getKey());
                    stringer.key("partitions").array();
                    for (int pid : p.getValue()) {
                        stringer.value(pid);
                    }
                    stringer.endArray();
                    stringer.endObject();
                }
                stringer.endArray(); // tables
                stringer.key("partitionToTxnId").object();
                for (Entry<Integer,Long> e : partitionToTxnId.entrySet()) {
                    stringer.key(e.getKey().toString()).value(e.getValue());
                }
                stringer.endObject(); // partitionToTxnId
                stringer.key("instanceId").value(instanceId.serializeToJSONObject());
                stringer.key("digestTables").array();
                for (String digestTable : digestTables) {
                    stringer.value(digestTable);
                }
                stringer.endArray();
                stringer.key("fileTables").array();
                for (String fileTable : fileTables) {
                    stringer.value(fileTable);
                }
                stringer.endArray();
                stringer.endObject();
                return new JSONObject(stringer.toString());
            } catch (JSONException e) {
                VoltDB.crashLocalVoltDB("Invalid JSON communicate snapshot info.", true, e);
            }
            throw new RuntimeException("impossible.");
        }

        public boolean isNewerThan(SnapshotInfo other)
        {
            if (other.instanceId.getTimestamp() > instanceId.getTimestamp()) {
                return false;
            }
            else if (other.instanceId.getTimestamp() < instanceId.getTimestamp()) {
                return true;
            }
            // Same instance ID timestamps, compare all the partition txn IDs until we find
            // some hint that one ran longer than the other
            if (other.txnId > txnId) {
                return false;
            }
            else if (other.txnId < txnId) {
                return true;
            }
            for (Entry<Integer, Long> e : other.partitionToTxnId.entrySet()) {
                if (partitionToTxnId.containsKey(e.getKey())) {
                    if (e.getValue() > partitionToTxnId.get(e.getKey())) {
                        return false;
                    }
                    else if (e.getValue() < partitionToTxnId.get(e.getKey())) {
                        return true;
                    }
                }
                // When we start adding/removing partitions on the fly this
                // missing else will need to do something
            }
            return false;
        }
    }

    /**
     * Creates a ZooKeeper directory if it doesn't exist. Crashes VoltDB if the
     * creation fails for any reason other then the path already existing.
     *
     * @param path
     */
    void createZKDirectory(String path) {
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
            VoltDB.crashGlobalVoltDB("Failed to create Zookeeper node: " + e.getMessage(),
                                     false, e);
        }
    }

    /*
     * A reusable callback for the incoming responses
     */
    private SimpleClientResponseAdapter.Callback m_clientAdapterCallback =
            new SimpleClientResponseAdapter.Callback() {
                @Override
                public void handleResponse(ClientResponse res)
                {
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
                        for (VoltTable result : results) {
                            LOG.fatal(result);
                        }
                        VoltDB.crashGlobalVoltDB("Failed to restore from snapshot: " +
                                res.getStatusString(), false, null);
                    } else {
                        Thread networkHandoff = new Thread() {
                            @Override
                            public void run() {
                                m_changeStateFunctor.run();
                            }
                        };
                        networkHandoff.start();
                    }
                }
            };

    public RestoreAgent(HostMessenger hostMessenger, SnapshotCompletionMonitor snapshotMonitor,
                        Callback callback, StartAction action, boolean clEnabled,
                        String clPath, String clSnapshotPath,
                        String snapshotPath, int[] allPartitions,
                        String voltdbrootPath)
    throws IOException {
        m_hostId = hostMessenger.getHostId();
        m_initiator = null;
        m_snapshotMonitor = snapshotMonitor;
        m_callback = callback;
        m_action = action;
        m_zk = hostMessenger.getZK();
        m_clEnabled = VoltDB.instance().getConfig().m_isEnterprise ? clEnabled : false;
        m_clPath = clPath;
        m_clSnapshotPath = clSnapshotPath;
        m_snapshotPath = snapshotPath;
        m_liveHosts = ImmutableSet.copyOf(hostMessenger.getLiveHostIds());
        m_voltdbrootPath = voltdbrootPath;

        initialize(hostMessenger);
    }

    private void initialize(HostMessenger hostMessenger) {
        // Load command log reinitiator
        try {
            Class<?> replayClass = MiscUtils.loadProClass("org.voltdb.CommandLogReinitiatorImpl",
                                                          "Command log replay", true);
            if (replayClass != null) {
                Constructor<?> constructor =
                    replayClass.getConstructor(int.class,
                                               StartAction.class,
                                               HostMessenger.class,
                                               String.class,
                                               Set.class);

                m_replayAgent =
                    (CommandLogReinitiator) constructor.newInstance(m_hostId,
                                                                    m_action,
                                                                    hostMessenger,
                                                                    m_clPath,
                                                                    m_liveHosts);
            }
        } catch (Exception e) {
            VoltDB.crashGlobalVoltDB("Unable to instantiate command log reinitiator",
                                     true, e);
        }
        m_replayAgent.setCallback(this);
    }

    public void setInitiator(TransactionCreator initiator) {
        m_initiator = initiator;
        m_initiator.bindAdapter(m_restoreAdapter);
        if (m_replayAgent != null) {
            m_replayAgent.setInitiator(initiator);
        }
    }

    /**
     * Generate restore and replay plans and return the catalog associated with
     * the snapshot to restore if there is anything to restore.
     *
     * @return The (host ID, catalog path) pair, or null if there is no snapshot
     *         to restore.
     */
    public Pair<Integer, String> findRestoreCatalog() {
        createZKDirectory(VoltZK.restore);
        createZKDirectory(VoltZK.restore_barrier);
        createZKDirectory(VoltZK.restore_barrier2);

        enterRestore();

        try {
            m_snapshotToRestore = generatePlans();
        } catch (Exception e) {
            VoltDB.crashGlobalVoltDB(e.getMessage(), true, e);
        }

        if (m_snapshotToRestore != null) {
            int hostId = m_snapshotToRestore.hostId;
            File file = new File(m_snapshotToRestore.path,
                                 m_snapshotToRestore.nonce + ".jar");
            String path = file.getPath();
            return Pair.of(hostId, path);
        }

        return null;
    }

    /**
     * Enters the restore process. Creates ZooKeeper barrier node for this host.
     */
    void enterRestore() {
        try {
            m_generatedRestoreBarrier2 = m_zk.create(VoltZK.restore_barrier2 + "/counter", null,
                        Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        } catch (Exception e) {
            VoltDB.crashGlobalVoltDB("Failed to create Zookeeper node: " + e.getMessage(),
                                     false, e);
        }
    }

    /**
     * Exists the restore process. Waits for all other hosts to complete first.
     * This method blocks.
     */
    void exitRestore() {
        try {
            m_zk.delete(m_generatedRestoreBarrier2, -1);
        } catch (Exception e) {
            VoltDB.crashLocalVoltDB("Unable to delete zk node " + m_generatedRestoreBarrier2, false, e);
        }

        LOG.debug("Waiting for all hosts to complete restore");
        List<String> children = null;
        while (true) {
            try {
                children = m_zk.getChildren(VoltZK.restore_barrier2, false);
            } catch (KeeperException e2) {
                VoltDB.crashGlobalVoltDB(e2.getMessage(), false, e2);
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

        // Clean up the ZK snapshot ID node so that we're good for next time.
        try
        {
            m_zk.delete(VoltZK.restore_snapshot_id, -1);
        }
        catch (Exception ignore) {}
    }

    /**
     * Start the restore process. It will first try to restore from the last
     * snapshot, then replay the logs, followed by a snapshot to truncate the
     * logs. This method won't block.
     */
    public void restore() {
        new Thread(m_restorePlanner, "restore-planner-host-" + m_hostId).start();
    }

    /**
     * Generate restore and replay plans.
     *
     * @return The snapshot to restore, or null if there is none.
     * @throws Exception
     *             If any exception is thrown, it means that the plan generation
     *             has failed. Should crash the cluster.
     */
    SnapshotInfo generatePlans() throws Exception {
        Map<String, Snapshot> snapshots = new HashMap<String, SnapshotUtil.Snapshot>();

        // Only scan if startup might require a snapshot restore.
        if (m_action.doesRecover()) {
            snapshots = getSnapshots();
        }

        final Long maxLastSeenTxn = m_replayAgent.getMaxLastSeenTxn();
        Set<SnapshotInfo> snapshotInfos = new HashSet<SnapshotInfo>();
        for (Snapshot e : snapshots.values()) {
            SnapshotInfo info = checkSnapshotIsComplete(e.getTxnId(), e);
            // if the cluster instance IDs in the snapshot and command log don't match, just move along
            if (m_replayAgent.getInstanceId() != null && info != null &&
                !m_replayAgent.getInstanceId().equals(info.instanceId)) {
                m_snapshotErrLogStr.append("\nRejected snapshot ")
                                .append(info.nonce)
                                .append(" due to mismatching instance IDs.")
                                .append(" Command log ID: ")
                                .append(m_replayAgent.getInstanceId().serializeToJSONObject().toString())
                                .append(" Snapshot ID: ")
                                .append(info.instanceId.serializeToJSONObject().toString());
                continue;
            }
            if (info != null) {
                final Map<Integer, Long> cmdlogmap = m_replayAgent.getMaxLastSeenTxnByPartition();
                final Map<Integer, Long> snapmap = info.partitionToTxnId;
                // If cmdlogmap is null, there were no command log segments, so all snapshots are potentially valid,
                // don't do any TXN ID consistency checking between command log and snapshot
                if (cmdlogmap != null) {
                    for (Integer cmdpart : cmdlogmap.keySet()) {
                        Long snaptxnId = snapmap.get(cmdpart);
                        if (snaptxnId == null) {
                            m_snapshotErrLogStr.append("\nRejected snapshot ")
                                            .append(info.nonce)
                                            .append(" due to missing partition: ")
                                            .append(cmdpart);
                            info = null;
                            break;
                        }
                        else if (snaptxnId < cmdlogmap.get(cmdpart)) {
                            m_snapshotErrLogStr.append("\nRejected snapshot ")
                                            .append(info.nonce)
                                            .append(" because it does not overlap the command log")
                                            .append(" for partition: ")
                                            .append(cmdpart)
                                            .append(" command log txn ID: ")
                                            .append(cmdlogmap.get(cmdpart))
                                            .append(" snapshot txn ID: ")
                                            .append(snaptxnId);
                            info = null;
                            break;
                        }
                    }
                }
            }


            if (info != null) {
                snapshotInfos.add(info);
            }
        }
        LOG.debug("Gathered " + snapshotInfos.size() + " snapshot information");
        sendLocalRestoreInformation(maxLastSeenTxn, snapshotInfos);

        // Negotiate with other hosts about which snapshot to restore
        SnapshotInfo infoWithMinHostId = getRestorePlan();

        /*
         * Generate the replay plan here so that we don't have to wait until the
         * snapshot restore finishes.
         */
        if (m_action.doesRecover()) {
            if (infoWithMinHostId != null) {
                // The expected partition count could be determined by the new partition count recorded
                // in the truncation snapshot. Truncation snapshot taken at the end of the join process
                // actually records the new partition count in the digest.
                m_replayAgent.generateReplayPlan(infoWithMinHostId.newPartitionCount, m_isLeader);
            }
        }

        m_planned = true;
        return infoWithMinHostId;
    }

    private SnapshotInfo checkSnapshotIsComplete(Long key, Snapshot s)
    {
        int partitionCount = -1;
        for (TableFiles tf : s.m_tableFiles.values()) {
            if (tf.m_isReplicated) {
                continue;
            }

            for (boolean completed : tf.m_completed) {
                if (!completed) {
                    m_snapshotErrLogStr.append("\nRejected snapshot ")
                                    .append(s.getNonce())
                                    .append(" because it was not completed.");
                    return null;
                }
            }

            // Everyone has to agree on the total partition count
            for (int count : tf.m_totalPartitionCounts) {
                if (partitionCount == -1) {
                    partitionCount = count;
                } else if (count != partitionCount) {
                    m_snapshotErrLogStr.append("\nRejected snapshot ")
                                    .append(s.getNonce())
                                    .append(" because it had the wrong partition count ")
                                    .append(count)
                                    .append(", expecting ")
                                    .append(partitionCount);
                    return null;
                }
            }
        }

        if (s.m_digests.isEmpty()) {
            m_snapshotErrLogStr.append("\nRejected snapshot ")
                            .append(s.getNonce())
                            .append(" because it had no valid digest file.");
            return null;
        }
        File digest = s.m_digests.get(0);
        Long catalog_crc = null;
        Map<Integer,Long> pidToTxnMap = new TreeMap<Integer,Long>();
        Set<String> digestTableNames = new HashSet<String>();
        // Create a valid but meaningless InstanceId to support pre-instanceId checking versions
        InstanceId instanceId = new InstanceId(0, 0);
        int newParitionCount = -1;
        try
        {
            JSONObject digest_detail = SnapshotUtil.CRCCheck(digest, LOG);
            if (digest_detail == null) throw new IOException();
            catalog_crc = digest_detail.getLong("catalogCRC");

            if (digest_detail.has("partitionTransactionIds")) {
                JSONObject pidToTxnId = digest_detail.getJSONObject("partitionTransactionIds");
                @SuppressWarnings("unchecked")
                Iterator<String> it = pidToTxnId.keys();
                while (it.hasNext()) {
                    String pidkey = it.next();
                    Long txnidval = pidToTxnId.getLong(pidkey);
                    pidToTxnMap.put(Integer.valueOf(pidkey), txnidval);
                }
            }

            if (digest_detail.has("instanceId")) {
                instanceId = new InstanceId(digest_detail.getJSONObject("instanceId"));
            }

            if (digest_detail.has("newPartitionCount")) {
                newParitionCount = digest_detail.getInt("newPartitionCount");
            }

            if (digest_detail.has("tables")) {
                JSONArray tableObj = digest_detail.getJSONArray("tables");
                for (int i = 0; i < tableObj.length(); i++) {
                    digestTableNames.add(tableObj.getString(i));
                }
            }
        }
        catch (IOException ioe)
        {
            m_snapshotErrLogStr.append("\nUnable to read digest file: ")
                            .append(digest.getAbsolutePath())
                            .append(" due to: ")
                            .append(ioe.getMessage());
            return null;
        }
        catch (JSONException je)
        {
            m_snapshotErrLogStr.append("\nUnable to extract catalog CRC from digest: ")
                            .append(digest.getAbsolutePath())
                            .append(" due to: ")
                            .append(je.getMessage());
            return null;
        }

        if (s.m_catalogFile == null) {
            m_snapshotErrLogStr.append("\nRejected snapshot ")
                            .append(s.getNonce())
                            .append(" because it had no catalog.");
            return null;
        }

        FileInputStream fin = null;
        try {
            fin = new FileInputStream(s.m_catalogFile);
            byte[] buffer = new byte[(int)s.m_catalogFile.length() + 1000];
            int readBytes = 0;
            int totalBytes = 0;
            try {
                while (readBytes >= 0) {
                    totalBytes += readBytes;
                    readBytes = fin.read(buffer, totalBytes, buffer.length - totalBytes - 1);
                }
            } finally {
                fin.close();
                fin = null;
            }
            byte[] catalogBytes = Arrays.copyOf(buffer, totalBytes);
            InMemoryJarfile jarfile = new InMemoryJarfile(catalogBytes);
            if (jarfile.getCRC() != catalog_crc) {
                m_snapshotErrLogStr.append("\nRejected snapshot ")
                                .append(s.getNonce())
                                .append(" because catalog CRC did not match digest.");
                return null;
            }
        }
        catch (IOException ioe) {
            m_snapshotErrLogStr.append("\nRejected snapshot ")
                            .append(s.getNonce())
                            .append(" because catalog CRC could not be validated");
            return null;
        }
        finally {
            if (fin != null) {
                try {
                    fin.close();
                }
                catch (Exception e) {}
            }
        }

        SnapshotInfo info =
            new SnapshotInfo(key, digest.getParent(),
                    SnapshotUtil.parseNonceFromDigestFilename(digest.getName()),
                    partitionCount, newParitionCount, catalog_crc, m_hostId, instanceId,
                    digestTableNames);
        // populate table to partition map.
        for (Entry<String, TableFiles> te : s.m_tableFiles.entrySet()) {
            TableFiles tableFile = te.getValue();
            HashSet<Integer> ids = new HashSet<Integer>();
            for (Set<Integer> idSet : tableFile.m_validPartitionIds) {
                ids.addAll(idSet);
            }
            if (!tableFile.m_isReplicated) {
                info.partitions.put(te.getKey(), ids);
            }
            // keep track of tables for which we've seen files while we're here
            info.fileTables.add(te.getKey());
        }
        info.setPidToTxnIdMap(pidToTxnMap);
        return info;
    }

    /**
     * Picks a snapshot info for restore. A single snapshot might have different
     * files scattered across multiple machines. All nodes must pick the same
     * SnapshotInfo or different nodes will pick different catalogs to restore.
     * Pick one SnapshotInfo and consolidate the per-node state into it.
     */
    static SnapshotInfo consolidateSnapshotInfos(Collection<SnapshotInfo> lastSnapshot)
    {
        SnapshotInfo chosen = null;
        if (lastSnapshot != null) {
            Iterator<SnapshotInfo> i = lastSnapshot.iterator();
            while (i.hasNext()) {
                SnapshotInfo next = i.next();
                if (chosen == null) {
                    chosen = next;
                } else if (next.hostId < chosen.hostId) {
                    next.partitionToTxnId.putAll(chosen.partitionToTxnId);
                    chosen = next;
                }
                else {
                    // create a full mapping of txn ids to partition ids.
                    chosen.partitionToTxnId.putAll(next.partitionToTxnId);
                }
            }
        }
        return chosen;
    }

    /**
     * Send the txnId of the snapshot that was picked to restore from to the
     * other hosts. If there was no snapshot to restore from, send 0.
     *
     * @param txnId
     */
    private void sendSnapshotTxnId(SnapshotInfo toRestore) {
        long txnId = toRestore != null ? toRestore.txnId : 0;
        String jsonData = toRestore != null ? toRestore.toJSONObject().toString() : "{}";
        LOG.debug("Sending snapshot ID " + txnId + " for restore to other nodes");
        try {
            m_zk.create(VoltZK.restore_snapshot_id, jsonData.getBytes(Constants.UTF8ENCODING),
                        Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        } catch (Exception e) {
            VoltDB.crashGlobalVoltDB("Failed to create Zookeeper node: " + e.getMessage(),
                                     false, e);
        }
    }

    /**
     * Get the txnId of the snapshot the cluster is restoring from from ZK.
     * NOTE that the barrier for this is now completely contained
     * in run() in the restorePlanner thread; nobody gets out of there until
     * someone wins the leader election and successfully writes the VoltZK.restore_snapshot_id
     * node, so we just need to read it here.
     */
    private void fetchSnapshotTxnId() {
        try {
            byte[] data = m_zk.getData(VoltZK.restore_snapshot_id, false, null);

            String jsonData = new String(data, Constants.UTF8ENCODING);
            if (!jsonData.equals("{}")) {
                m_hasRestored = true;
                JSONObject jo = new JSONObject(jsonData);
                SnapshotInfo info = new SnapshotInfo(jo);
                m_replayAgent.setSnapshotTxnId(info);
            }
            else {
                m_hasRestored = false;
                m_replayAgent.setSnapshotTxnId(null);
            }
        } catch (KeeperException e2) {
            VoltDB.crashGlobalVoltDB(e2.getMessage(), false, e2);
        } catch (InterruptedException e2) {
        } catch (JSONException je) {
            VoltDB.crashLocalVoltDB(je.getMessage(), true, je);
        }
    }

    /**
     * Send the information about the local snapshot files to the other hosts to
     * generate restore plan.
     *
     * @param max
     *            The maximum txnId of the last txn across all initiators in the
     *            local command log.
     * @param snapshots
     *            The information of the local snapshot files.
     */
    private void sendLocalRestoreInformation(Long max, Set<SnapshotInfo> snapshots) {
        String jsonData = serializeRestoreInformation(max, snapshots);
        String zkNode = VoltZK.restore + "/" + m_hostId;
        try {
            m_zk.create(zkNode, jsonData.getBytes(Constants.UTF8ENCODING),
                        Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        } catch (Exception e) {
            throw new RuntimeException("Failed to create Zookeeper node: " +
                                       e.getMessage(), e);
        }
    }

    private List<String> waitOnVoltZK_restore() throws KeeperException
    {
        LOG.debug("Waiting for all hosts to send their snapshot information");
        List<String> children = null;
        while (true) {
            try {
                children = m_zk.getChildren(VoltZK.restore, false);
            } catch (KeeperException e2) {
                throw e2;
            } catch (InterruptedException e2) {
                continue;
            }

            if (children.size() < m_liveHosts.size()) {
                try {
                    Thread.sleep(500);
                } catch (InterruptedException e1) {}
            } else {
                break;
            }
        }
        if (children == null) {
            throw new RuntimeException("Unable to read agreement messages from" +
                                       " other hosts for restore plan");
        }
        return children;
    }

    /**
     * Pick the snapshot to restore from based on the global snapshot
     * information.
     *
     * @return The snapshot to restore from, null if none.
     * @throws Exception
     */
    private SnapshotInfo getRestorePlan() throws Exception {
        // Wait for ZK to publish the snapshot fragment/info structures.
        List<String> children = waitOnVoltZK_restore();

        // If not recovering, nothing to do.
        if (m_action == StartAction.CREATE) {
            return null;
        }

        Map<String, Set<SnapshotInfo>> snapshotFragments = new HashMap<String, Set<SnapshotInfo>>();
        Long clStartTxnId = deserializeRestoreInformation(children, snapshotFragments);

        // If command log has no snapshot requirement clear the fragment set directly
        if (clStartTxnId != null && clStartTxnId == Long.MIN_VALUE) {
            snapshotFragments.clear();
        }

        LOG.debug("There are " + snapshotFragments.size() + " restore candidate snapshots available in the cluster");

        // Find the last complete snapshot and use it
        HashMap<String, Map<String, Set<Integer>>> snapshotTablePartitions =
            new HashMap<String, Map<String,Set<Integer>>>();
        Iterator<Entry<String, Set<SnapshotInfo>>> it = snapshotFragments.entrySet().iterator();
        SnapshotInfo newest = null;
        while (it.hasNext()) {
            Entry<String, Set<SnapshotInfo>> e = it.next();
            Set<String> fileTables = new HashSet<String>();
            Set<String> digestTables = null;
            String nonce = e.getKey();
            Map<String, Set<Integer>> tablePartitions = snapshotTablePartitions.get(nonce);
            if (tablePartitions == null) {
                tablePartitions = new HashMap<String, Set<Integer>>();
                snapshotTablePartitions.put(nonce, tablePartitions);
            }

            int totalPartitions = -1;
            boolean inconsistent = false;
            Set<SnapshotInfo> fragments = e.getValue();
            for (SnapshotInfo s : fragments) {
                LOG.debug("SnapshotInfo " + s.nonce + " claims digest tables: " + s.digestTables);
                LOG.debug("SnapshotInfo " + s.nonce + " claims files for tables: " + s.fileTables);
                if (digestTables == null) {
                    digestTables = new HashSet<String>(s.digestTables);
                }
                else if (!digestTables.equals(s.digestTables)) {
                    m_snapshotErrLogStr.append("\nRejected snapshot ")
                                    .append(s.nonce)
                                    .append(" due to disagreement in digest table list.  Got ")
                                    .append(s.digestTables)
                                    .append(", expecting ")
                                    .append(digestTables);
                    inconsistent = true;
                    break;
                }
                fileTables.addAll(s.fileTables);
                if (totalPartitions == -1) {
                    totalPartitions = s.partitionCount;
                } else if (totalPartitions != s.partitionCount) {
                    m_snapshotErrLogStr.append("\nRejected snapshot ")
                                    .append(s.nonce)
                                    .append(" due to partition count mismatch. Got ")
                                    .append(s.partitionCount)
                                    .append(", expecting ")
                                    .append(totalPartitions);
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

            // Check that we have files for all the tables listed in the digest
            if (!fileTables.equals(digestTables)) {
                m_snapshotErrLogStr.append("\nRejected snapshot ")
                    .append(nonce)
                    .append(" because there were not table files for all tables in the digest.  Expected ")
                    .append(digestTables)
                    .append(", but only found table data for ")
                    .append(fileTables);
                inconsistent = true;
            }

            // Check if we have all the partitions
            for (Set<Integer> partitions : tablePartitions.values()) {
                if (partitions.size() != totalPartitions) {
                    m_snapshotErrLogStr.append("\nRejected snapshot ")
                                    .append(nonce)
                                    .append(" due to missing partitions. Got ")
                                    .append(partitions.size())
                                    .append(", expecting ")
                                    .append(totalPartitions);
                    inconsistent = true;
                }
            }

            if (inconsistent) {
                it.remove();
            }
            else {
                SnapshotInfo current = consolidateSnapshotInfos(fragments);
                if (newest == null || current.isNewerThan(newest)) {
                    newest = current;
                }
            }
        }

        // If we have a command log and it requires a snapshot but no snapshot
        // fragments were found, simply bail (or, if we didn't find a viable
        // snapshot from which to recover)
        // Reluctant to muck with the first predicate here, even though
        // the newest == null should cover all of the 'no viable snapshot' possiblities
        if ((clStartTxnId != null && clStartTxnId != Long.MIN_VALUE &&
            snapshotFragments.size() == 0) || newest == null) {
            if (m_snapshotErrLogStr.length() > 0) {
                LOG.error(m_snapshotErrLogStr.toString());
            }
            throw new RuntimeException("No viable snapshots to restore");
        }

        if (snapshotFragments.isEmpty()) {
            return null;
        } else {
            return newest;
        }
    }

    /**
     * This function, like all good functions, does three things.
     * It produces the command log start transaction Id.
     * It produces a map of SnapshotInfo objects.
     * And, it errors if the remote start action does not match the local action.
     */
    private Long deserializeRestoreInformation(List<String> children,
            Map<String, Set<SnapshotInfo>> snapshotFragments) throws Exception
    {
        try {
            int recover = m_action.ordinal();
            Long clStartTxnId = null;
            for (String node : children) {
                //This might be created before we are done fetching the restore info
                if (node.equals("snapshot_id")) continue;

                byte[] data = null;
                data = m_zk.getData(VoltZK.restore + "/" + node, false, null);
                String jsonData = new String(data, "UTF8");
                JSONObject json = new JSONObject(jsonData);

                long maxTxnId = json.optLong("max", Long.MIN_VALUE);
                if (maxTxnId != Long.MIN_VALUE) {
                    if (clStartTxnId == null || maxTxnId > clStartTxnId) {
                        clStartTxnId = maxTxnId;
                    }
                }
                int remoteRecover = json.getInt("action");
                if (remoteRecover != recover) {
                    String msg = "Database actions are not consistent, please enter " +
                        "the same database action on the command-line.";
                    throw new RuntimeException(msg);
                }

                JSONArray snapInfos = json.getJSONArray("snapInfos");
                int snapInfoCnt = snapInfos.length();
                for (int i=0; i < snapInfoCnt; i++) {
                    JSONObject jsonInfo = snapInfos.getJSONObject(i);
                    SnapshotInfo info = new SnapshotInfo(jsonInfo);
                    Set<SnapshotInfo> fragments = snapshotFragments.get(info.nonce);
                    if (fragments == null) {
                        fragments = new HashSet<SnapshotInfo>();
                        snapshotFragments.put(info.nonce, fragments);
                    }
                    fragments.add(info);
                }
            }
            return clStartTxnId;
        } catch (JSONException je) {
            VoltDB.crashLocalVoltDB("Error exchanging snapshot information", true, je);
        }
        throw new RuntimeException("impossible");
    }

    /**
     * @param max
     * @param snapshots
     * @return
     */
    private String serializeRestoreInformation(Long max, Set<SnapshotInfo> snapshots)
    {
        try {
            JSONStringer stringer = new JSONStringer();
            stringer.object();
            // optional max value.
            if (max != null) {
                stringer.key("max").value(max);
            }
            // 1 means recover, 0 means to create new DB
            stringer.key("action").value(m_action.ordinal());
            stringer.key("snapInfos").array();
            for (SnapshotInfo snapshot : snapshots) {
                stringer.value(snapshot.toJSONObject());
            }
            stringer.endArray();
            stringer.endObject();
            return stringer.toString();
        } catch (JSONException je) {
            VoltDB.crashLocalVoltDB("Error exchanging snapshot info", true, je);
        }
        throw new RuntimeException("impossible codepath.");
    }

    /**
     * Restore a snapshot. An arbitrarily early transaction is provided if command
     * log replay follows to maintain txnid sequence constraints (with simple dtxn).
     */
    private void initSnapshotWork(final Object[] procParams) {
        final String procedureName = "@SnapshotRestore";
        Config restore = SystemProcedureCatalog.listing.get(procedureName);
        Procedure restoreProc = restore.asCatalogProcedure();
        StoredProcedureInvocation spi = new StoredProcedureInvocation();
        spi.procName = procedureName;
        spi.params = new FutureTask<ParameterSet>(new Callable<ParameterSet>() {
            @Override
            public ParameterSet call() throws Exception {
                ParameterSet params = ParameterSet.fromArrayWithCopy(procParams);
                return params;
            }
        });
        spi.setClientHandle(m_restoreAdapter.registerCallback(m_clientAdapterCallback));

        m_initiator.createTransaction(m_restoreAdapter.connectionId(), spi,
                                      restoreProc.getReadonly(),
                                      restoreProc.getSinglepartition(),
                                      restoreProc.getEverysite(),
                                      0,//Can provide anything for multi-part
                                      0,
                                      System.nanoTime());
    }

    /**
     * Change the state of the restore agent based on the current state.
     */
    private void changeState() {
        if (m_state == State.RESTORE) {
            fetchSnapshotTxnId();

            exitRestore();
            m_state = State.REPLAY;

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
                m_callback.onRestoreCompletion(m_truncationSnapshot, m_truncationSnapshotPerPartition);
            }

            // Call balance partitions after enabling transactions on the node to shorten the recovery time
            if (m_isLeader) {
                if (!m_replayAgent.checkAndBalancePartitions()) {
                    VoltDB.crashLocalVoltDB("Failed to finish balancing partitions", false, null);
                }
            }
        }
    }

    @Override
    public void onReplayCompletion() {
        if (!m_hasRestored && !m_replayAgent.hasReplayedSegments() &&
            m_action.doesRecover()) {
            /*
             * This means we didn't restore any snapshot, and there's no command
             * log to replay. But the user asked for recover
             */
            VoltDB.crashGlobalVoltDB("Nothing to recover from", false, null);
        } else if (!m_clEnabled && !m_replayAgent.hasReplayedTxns()) {
            // Nothing was replayed, so no need to initiate truncation snapshot
            m_state = State.TRUNCATE;
        }

        changeState();

        /*
         * ENG-1516: Use truncation snapshot to save the catalog if CL is
         * enabled.
         */
        if (m_clEnabled || m_replayAgent.hasReplayedTxns()) {
            /*
             * If this has the lowest host ID, initiate the snapshot that
             * will truncate the logs
             */
            if (m_isLeader) {
                try {
                    try {
                        m_zk.create(VoltZK.truncation_snapshot_path,
                                    m_clSnapshotPath.getBytes(),
                                    Ids.OPEN_ACL_UNSAFE,
                                    CreateMode.PERSISTENT);
                    } catch (KeeperException.NodeExistsException e) {}
                    m_zk.create(VoltZK.request_truncation_snapshot_node, null,
                            Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
                } catch (Exception e) {
                    VoltDB.crashGlobalVoltDB("Requesting a truncation snapshot " +
                                             "via ZK should always succeed",
                                             false, e);
                }
            }
        }
    }

    /**
     * Finds all the snapshots in all the places we know of which could possibly
     * store snapshots, like command log snapshots, auto snapshots, etc.
     *
     * @return All snapshots
     */
    private Map<String, Snapshot> getSnapshots() {
        /*
         * Use the individual snapshot directories instead of voltroot, because
         * they can be set individually
         */
        List<String> paths = new ArrayList<String>();
        if (VoltDB.instance().getConfig().m_isEnterprise) {
            if (m_clSnapshotPath != null) {
                paths.add(m_clSnapshotPath);
            }
        }
        if (m_snapshotPath != null) {
            paths.add(m_snapshotPath);
        }
        HashMap<String, Snapshot> snapshots = new HashMap<String, Snapshot>();
        FileFilter filter = new SnapshotUtil.SnapshotFilter();

        for (String path : paths) {
            SnapshotUtil.retrieveSnapshotFiles(new File(path), snapshots, filter, false, LOG);
        }

        return snapshots;
    }

    /**
     * All nodes will be notified about the completion of the truncation
     * snapshot.
     */
    @Override
    public CountDownLatch snapshotCompleted(SnapshotCompletionEvent event) {
        if (!event.truncationSnapshot || !event.didSucceed) {
            VoltDB.crashGlobalVoltDB("Failed to truncate command logs by snapshot",
                                     false, null);
        } else {
            m_truncationSnapshot = event.multipartTxnId;
            m_truncationSnapshotPerPartition = event.partitionTxnIds;
            m_replayAgent.returnAllSegments();
            changeState();
        }
        return new CountDownLatch(0);
    }

    @Override
    public void acceptPromotion() throws InterruptedException, ExecutionException, KeeperException
    {
        m_isLeader = true;
    }
}
