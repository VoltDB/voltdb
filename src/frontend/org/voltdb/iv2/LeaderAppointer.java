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

import java.util.concurrent.CountDownLatch;
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

/**
 * LeaderAppointer handles centralized appointment of partition leaders across
 * the partition.  This is primarily so that the leaders can be evenly
 * distributed throughout the cluster, reducing bottlenecks (at least at
 * startup).  As a side-effect, this service also controls the initial startup
 * of the cluster, blocking operation until each partition has a k-safe set of
 * replicas, each partition has a leader, and the MPI has started.
 */
public class LeaderAppointer implements Promotable
{
    private static final VoltLogger tmLog = new VoltLogger("TM");

    private enum AppointerState {
        INIT,           // Initial start state, used to inhibit ZK callback actions
        CLUSTER_START,  // indicates that we're doing the initial cluster startup
        DONE            // indicates normal running conditions, including repair
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
    private CountDownLatch m_startupLatch = null;

    private class PartitionCallback extends BabySitter.Callback
    {
        final int m_partitionId;
        final Set<Long> m_replicas;
        long m_currentLeader;

        /** Constructor used when we know (or think we know) who the leader for this partition is */
        PartitionCallback(int partitionId, long currentLeader)
        {
            this(partitionId);
            // Try to be clever for repair.  Create ourselves with the current leader set to
            // whatever is in the LeaderCache, and claim that replica exists, then let the
            // first run() call fix the world.
            m_currentLeader = currentLeader;
            m_replicas.add(currentLeader);
        }

        /** Constructor used at startup when there is no leader */
        PartitionCallback(int partitionId)
        {
            m_partitionId = partitionId;
            // A bit of a hack, but we should never end up with an HSID as Long.MAX_VALUE
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
            tmLog.debug("Newly seen replicas: " + CoreUtils.hsIdCollectionToString(newHSIds));
            // compute previously seen but now vanished from the callback list HSId set
            Set<Long> missingHSIds = new HashSet<Long>(m_replicas);
            missingHSIds.removeAll(updatedHSIds);
            tmLog.debug("Newly dead replicas: " + CoreUtils.hsIdCollectionToString(missingHSIds));

            tmLog.debug("Handling babysitter callback for partition " + m_partitionId + ": children: " +
                    CoreUtils.hsIdCollectionToString(updatedHSIds));
            if (m_state.get() == AppointerState.CLUSTER_START) {
                // We can't yet tolerate a host failure during startup.  Crash it all
                if (missingHSIds.size() > 0) {
                    VoltDB.crashGlobalVoltDB("Node failure detected during startup.", false, null);
                }
                // ENG-3166: Eventually we would like to get rid of the extra replicas beyond k_factor,
                // but for now we just look to see how many replicas of this partition we actually expect
                // and gate leader assignment on that many copies showing up.
                int replicaCount = m_kfactor + 1;
                JSONArray parts;
                try {
                    parts = m_topo.getJSONArray("partitions");
                    for (int p = 0; p < parts.length(); p++) {
                        JSONObject aPartition = parts.getJSONObject(p);
                        int pid = aPartition.getInt("partition_id");
                        if (pid == m_partitionId) {
                            replicaCount = aPartition.getJSONArray("replicas").length();
                        }
                    }
                } catch (JSONException e) {
                    // Ignore and just assume the normal number of replicas
                }
                if (children.size() == replicaCount) {
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

    /* We'll use this callback purely for startup so we can discover when all
     * the leaders we have appointed have completed their promotions and
     * published themselves to Zookeeper */
    LeaderCache.Callback m_masterCallback = new LeaderCache.Callback()
    {
        @Override
        public void run(ImmutableMap<Integer, Long> cache) {
            Set<Long> currentLeaders = new HashSet<Long>(cache.values());
            tmLog.debug("Updated leaders: " + currentLeaders);
            if (m_state.get() == AppointerState.CLUSTER_START) {
                if (currentLeaders.size() == m_partitionCount) {
                    tmLog.info("Leader appointment complete, promoting MPI and unblocking.");
                    m_state.set(AppointerState.DONE);
                    m_MPI.acceptPromotion();
                    m_startupLatch.countDown();
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
        // Crank up the leader caches.  Use blocking startup so that we'll have valid point-in-time caches later.
        m_iv2appointees.start(true);
        m_iv2masters.start(true);
        // Figure out what conditions we assumed leadership under.
        if (m_iv2appointees.pointInTimeCache().size() == 0)
        {
            tmLog.debug("LeaderAppointer in startup");
            m_state.set(AppointerState.CLUSTER_START);
        }
        else if ((m_iv2appointees.pointInTimeCache().size() != m_partitionCount) ||
                 (m_iv2masters.pointInTimeCache().size() != m_partitionCount)) {
            // If we are promoted and the appointees or masters set is partial, the previous appointer failed
            // during startup (at least for now, until we add add/remove a partition on the fly).
            VoltDB.crashGlobalVoltDB("Detected failure during startup, unable to start", false, null);
        }
        else {
            tmLog.debug("LeaderAppointer in repair");
            m_state.set(AppointerState.DONE);
        }

        if (m_state.get() == AppointerState.CLUSTER_START) {
            // Need to block the return of acceptPromotion until after the MPI is promoted.  Wait for this latch
            // to countdown after appointing all the partition leaders.  The
            // LeaderCache callback will count it down once it has seen all the
            // appointed leaders publish themselves as the actual leaders.
            m_startupLatch = new CountDownLatch(1);
            for (int i = 0; i < m_partitionCount; i++) {
                String dir = LeaderElector.electionDirForPartition(i);
                // Race along with all of the replicas for this partition to create the ZK parent node
                try {
                    m_zk.create(dir, null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                } catch (KeeperException.NodeExistsException e) {
                    // expected on all nodes that don't start() first.
                }
                m_callbacks[i] = new PartitionCallback(i);
                Pair<BabySitter, List<String>> sitterstuff = BabySitter.blockingFactory(m_zk, dir, m_callbacks[i]);
                m_partitionWatchers[i] = sitterstuff.getFirst();
            }
            m_startupLatch.await();
        }
        else {
            // If we're taking over for a failed LeaderAppointer, we know when
            // we get here that every partition had a leader at some point in
            // time.  We'll seed each of the PartitionCallbacks for each
            // partition with the HSID of the last published leader.  The
            // blocking startup of the BabySitter watching that partition will
            // call our callback, get the current full set of replicas, and
            // appoint a new leader if the seeded one has actually failed
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
        // We used masterHostId = -1 as a way to force the leader choice to be
        // the first replica in the list, if we don't have some other mechanism
        // which has successfully overridden it.
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
            // For now, if we're appointing a new leader as a result of a
            // failure, just pick the first replica in the children list.
            // Could eventually do something more complex here to try to keep a
            // semi-balance, but it's unclear that this has much utility until
            // we add rebalancing on rejoin as well.
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

    public void shutdown()
    {
        try {
            m_iv2appointees.shutdown();
            m_iv2masters.shutdown();
            for (BabySitter watcher : m_partitionWatchers) {
                watcher.shutdown();
            }
        }
        catch (Exception e) {
            // don't care, we're going down
        }
    }
}
