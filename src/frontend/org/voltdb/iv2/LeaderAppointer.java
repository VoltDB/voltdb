/* This file is part of VoltDB.
 * Copyright (C) 2008-2012 VoltDB Inc.
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

package org.voltdb.iv2;

import java.util.concurrent.atomic.AtomicReference;

import java.util.concurrent.ExecutionException;

import java.util.HashSet;
import java.util.List;
import java.util.Map;

import java.util.Map.Entry;
import java.util.Set;

import org.apache.zookeeper_voltpatches.CreateMode;
import org.apache.zookeeper_voltpatches.KeeperException;
import org.apache.zookeeper_voltpatches.ZooDefs.Ids;
import org.apache.zookeeper_voltpatches.ZooKeeper;

import org.json_voltpatches.JSONArray;
import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;

import org.voltcore.logging.VoltLogger;

import org.voltcore.utils.CoreUtils;
import org.voltcore.utils.Pair;

import org.voltcore.zk.BabySitter;
import org.voltcore.zk.LeaderElector;

import org.voltdb.Promotable;
import org.voltdb.VoltDB;
import org.voltdb.VoltZK;

import com.google.common.collect.ImmutableMap;

public class LeaderAppointer implements Promotable
{
    private static final VoltLogger tmLog = new VoltLogger("TM");

    private enum AppointerState {
        INIT,           // Initial start state, used to inhibit ZK callback actions
        CLUSTER_START,  // indicates that we're doing the initial cluster startup
        CLUSTER_REPAIR, // indicates that we're been promoted within a running cluster and handling repair
        DONE            // indicates normal running conditions
    }

    private final ZooKeeper m_zk;
    private final int m_partitionCount;
    private final BabySitter[] m_partitionWatchers;
    private final LeaderCache m_iv2appointees;
    private final LeaderCache m_iv2masters;
    private final PartitionCallback[] m_callbacks;
    private final int m_kfactor;
    private final JSONObject m_topo;
    private final MpInitiator m_MPI;
    private AtomicReference<AppointerState> m_state =
        new AtomicReference<AppointerState>(AppointerState.INIT);
    private Set<Long> m_currentLeaders = new HashSet<Long>();

    private class PartitionCallback extends BabySitter.Callback
    {
        final int m_partitionId;
        final Set<Long> m_replicas;
        // bit of a hack, but no real HSId should ever be this value
        long m_currentLeader;

        PartitionCallback(int partitionId, long currentLeader)
        {
            this(partitionId);
            // Try to be clever for repair.  Create ourselves with the current leader set to
            // whatever is in the LeaderCache, and claim that replica exists, then let the
            // first run() call fix the world.
            m_currentLeader = currentLeader;
            m_replicas.add(currentLeader);
        }

        PartitionCallback(int partitionId)
        {
            m_partitionId = partitionId;
            m_currentLeader = Long.MAX_VALUE;
            m_replicas = new HashSet<Long>();
        }

        @Override
        public void run(List<String> children)
        {
            List<Long> updatedHSIds = VoltZK.childrenToReplicaHSIds(children);
            // compute previously unseen HSId set in the callback list
            Set<Long> newHSIds = new HashSet<Long>(updatedHSIds);
            newHSIds.removeAll(m_replicas);
            tmLog.info("Newly seen replicas: " + CoreUtils.hsIdCollectionToString(newHSIds));
            // compute previously seen but now vanished from the callback list HSId set
            Set<Long> missingHSIds = new HashSet<Long>(m_replicas);
            missingHSIds.removeAll(updatedHSIds);
            tmLog.info("Newly dead replicas: " + CoreUtils.hsIdCollectionToString(missingHSIds));

            tmLog.debug("Handling babysitter callback for partition " + m_partitionId + ": children: " +
                    CoreUtils.hsIdCollectionToString(updatedHSIds));
            if (m_state.get() == AppointerState.CLUSTER_START) {
                // We can't yet tolerate a host failure during startup.  Crash it all
                if (missingHSIds.size() > 0) {
                    VoltDB.crashGlobalVoltDB("Node failure detected during startup.", false, null);
                }
                if (children.size() == (m_kfactor + 1)) {
                    m_currentLeader = assignLeader(m_partitionId, updatedHSIds);
                }
                else {
                    tmLog.info("Waiting on " + ((m_kfactor + 1) - children.size()) + " more nodes " +
                            "for k-safety before startup");
                }
            }
            else {
                // Check for k-safety
                if (children.size() == 0) {
                    VoltDB.crashGlobalVoltDB("Cluster has become unviable: No remaining replicas for partition "
                            + m_partitionId + ", shutting down.", false, null);
                }
                else if (missingHSIds.contains(m_currentLeader)) {
                    m_currentLeader = assignLeader(m_partitionId, updatedHSIds);
                }
            }
            m_replicas.clear();
            m_replicas.addAll(updatedHSIds);
        }
    }

    // In CLUSTER_START, no LeaderCache watches should fire that aren't under our control
    // In CLUSTER_REPAIR, it's possible that some appointee is still completing its promotion
    // and could cause a LeaderCache watch to fire.
    LeaderCache.Callback m_masterCallback = new LeaderCache.Callback()
    {
        @Override
        public void run(ImmutableMap<Integer, Long> cache) {
            m_currentLeaders = new HashSet<Long>(cache.values());
            tmLog.info("Updated leaders: " + m_currentLeaders);
            if (m_state.get() == AppointerState.CLUSTER_START) {
                if (m_currentLeaders.size() == m_partitionCount) {
                    m_state.set(AppointerState.DONE);
                    m_MPI.acceptPromotion();
                }
            }
        }
    };

    public LeaderAppointer(ZooKeeper zk, int numberOfPartitions,
            int kfactor, JSONObject topology, MpInitiator mpi)
    {
        m_zk = zk;
        m_kfactor = kfactor;
        m_topo = topology;
        m_MPI = mpi;
        m_partitionCount = numberOfPartitions;
        m_callbacks = new PartitionCallback[m_partitionCount];
        m_partitionWatchers = new BabySitter[m_partitionCount];
        m_iv2appointees = new LeaderCache(m_zk, VoltZK.iv2appointees);
        m_iv2masters = new LeaderCache(m_zk, VoltZK.iv2masters, m_masterCallback);
    }

    @Override
    public void acceptPromotion() throws InterruptedException, ExecutionException, KeeperException
    {
        m_iv2appointees.start(true);
        m_iv2masters.start(true);
        // Figure out what conditions we assumed leadership under.
        if (m_iv2appointees.pointInTimeCache().size() == 0)
        {
            tmLog.info("LeaderAppointer in startup");
            m_state.set(AppointerState.CLUSTER_START);
        }
        else if ((m_iv2appointees.pointInTimeCache().size() != m_partitionCount) ||
                 (m_iv2masters.pointInTimeCache().size() != m_partitionCount)) {
            // If we are promoted and the appointees or masters set is partial, the previous appointer failed
            // during startup (at least for now, until we add add/remove a partition on the fly).
            VoltDB.crashGlobalVoltDB("Detected failure during startup, unable to start", false, null);
        }
        else {
            tmLog.info("LeaderAppointer in repair");
            m_state.set(AppointerState.DONE);
        }

        if (m_state.get() == AppointerState.CLUSTER_START) {
            for (int i = 0; i < m_partitionCount; i++) {
                String dir = LeaderElector.electionDirForPartition(i);
                try {
                    m_zk.create(dir, null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                } catch (KeeperException.NodeExistsException e) {
                    // expected on all nodes that don't start() first.
                }
                m_callbacks[i] = new PartitionCallback(i);
                Pair<BabySitter, List<String>> sitterstuff = BabySitter.blockingFactory(m_zk, dir, m_callbacks[i]);
                m_partitionWatchers[i] = sitterstuff.getFirst();
            }
        }
        else {
            // figure out the current state of the world.
            Map<Integer, Long> masters = m_iv2masters.pointInTimeCache();
            tmLog.info("LeaderAppointer repairing with master set: " + masters);
            for (Entry<Integer, Long> master : masters.entrySet()) {
                int partId = master.getKey();
                String dir = LeaderElector.electionDirForPartition(partId);
                m_callbacks[partId] = new PartitionCallback(partId, master.getValue());
                Pair<BabySitter, List<String>> sitterstuff =
                    BabySitter.blockingFactory(m_zk, dir, m_callbacks[partId]);
                m_partitionWatchers[partId] = sitterstuff.getFirst();
            }
            // just go ahead and promote our MPI
            m_MPI.acceptPromotion();
        }
    }

    private long assignLeader(int partitionId, List<Long> children)
    {
        int masterHostId = -1;
        if (m_state.get() == AppointerState.CLUSTER_START) {
            try {
                // find master in topo
                JSONArray parts = m_topo.getJSONArray("partitions");
                for (int p = 0; p < parts.length(); p++) {
                    JSONObject aPartition = parts.getJSONObject(p);
                    int pid = aPartition.getInt("partition_id");
                    if (pid == partitionId) {
                        masterHostId = aPartition.getInt("master");
                    }
                }
            }
            catch (JSONException jse) {
                tmLog.error("Failed to find master for partition " + partitionId + ", defaulting to 0");
                jse.printStackTrace();
                masterHostId = -1; // stupid default
            }
        }
        else {
            masterHostId = -1;
        }

        long masterHSId = children.get(0);
        for (Long child : children) {
            if (CoreUtils.getHostIdFromHSId(child) == masterHostId) {
                masterHSId = child;
                break;
            }
        }
        tmLog.info("Appointing HSId " + CoreUtils.hsIdToString(masterHSId) + " as leader for partition " +
                partitionId);
        try {
            m_iv2appointees.put(partitionId, masterHSId);
        }
        catch (Exception e) {
            VoltDB.crashLocalVoltDB("Unable to appoint new master for partition " + partitionId, true, e);
        }
        return masterHSId;
    }
}
