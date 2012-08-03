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

import java.util.concurrent.atomic.AtomicBoolean;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;

import java.util.HashSet;
import java.util.List;
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
import org.voltcore.zk.MapCache;

import org.voltdb.Promotable;
import org.voltdb.VoltDB;
import org.voltdb.VoltZK;

import com.google.common.collect.ImmutableMap;

public class LeaderAppointer implements Promotable
{
    private static final VoltLogger tmLog = new VoltLogger("TM");
    private final ZooKeeper m_zk;
    private final CountDownLatch m_partitionCountLatch;
    private final int m_partitionCount;
    private final BabySitter[] m_partitionWatchers;
    private final MapCache m_iv2appointees;
    private final MapCache m_iv2masters;
    private final PartitionCallback[] m_callbacks;
    private final int m_kfactor;
    private final JSONObject m_topo;
    private final MpInitiator m_MPI;
    private AtomicBoolean m_inStartup = new AtomicBoolean(false);

    private class PartitionCallback extends BabySitter.Callback
    {
        final int m_partitionId;

        PartitionCallback(int partitionId)
        {
            m_partitionId = partitionId;
        }

        @Override
        public void run(List<String> children)
        {
            tmLog.debug("Handling babysitter callback for partition " + m_partitionId + ": children: " +
                    CoreUtils.hsIdCollectionToString(VoltZK.childrenToReplicaHSIds(children)));
            if (m_inStartup.get()) {
                if (children.size() == (m_kfactor + 1)) {
                    assignLeader(m_partitionId, VoltZK.childrenToReplicaHSIds(children));
                    m_partitionCountLatch.countDown();
                }
            }
        }
    }

    MapCache.Callback m_masterCallback = new MapCache.Callback()
    {
        @Override
        public void run(ImmutableMap<String, JSONObject> cache) {
            Set<Long> updatedLeaders = new HashSet<Long>();
            for (JSONObject thing : cache.values()) {
                try {
                    updatedLeaders.add(Long.valueOf(thing.getLong("hsid")));
                } catch (JSONException e) {
                    VoltDB.crashLocalVoltDB("Corrupt ZK MapCache data.", true, e);
                }
            }
            tmLog.info("Updated leaders: " + updatedLeaders);
            if (m_inStartup.get()) {
                if (updatedLeaders.size() == m_partitionCount) {
                    m_inStartup.set(true);
                    m_MPI.acceptPromotion();
                }
            }
        }
    };

    public LeaderAppointer(ZooKeeper zk, CountDownLatch numberOfPartitions,
            int kfactor, JSONObject topology, MpInitiator mpi)
    {
        m_zk = zk;
        m_kfactor = kfactor;
        m_topo = topology;
        m_MPI = mpi;
        m_partitionCountLatch = numberOfPartitions;
        m_partitionCount = (int)numberOfPartitions.getCount();
        m_callbacks = new PartitionCallback[(int)numberOfPartitions.getCount()];
        m_partitionWatchers = new BabySitter[(int)numberOfPartitions.getCount()];
        m_iv2appointees = new MapCache(m_zk, VoltZK.iv2appointees);
        m_iv2masters = new MapCache(m_zk, VoltZK.iv2masters, m_masterCallback);
    }

    @Override
    public void acceptPromotion() throws InterruptedException, ExecutionException, KeeperException
    {
        m_iv2appointees.start(true);
        m_iv2masters.start(true);
        if (m_iv2appointees.pointInTimeCache().size() == 0)
        {
            tmLog.info("LeaderAppointer in startup");
            m_inStartup.set(true);
        }
        else if (m_iv2appointees.pointInTimeCache().size() != m_partitionCount) {
            VoltDB.crashGlobalVoltDB("Detected failure during startup, unable to start", false, null);
        }
        if (m_inStartup.get()) {
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
    }

    private void assignLeader(int partitionId, List<Long> children)
    {
        int masterHostId = -1;
        if (m_inStartup.get()) {
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
            m_iv2appointees.put(Integer.toString(partitionId), new JSONObject("{appointee:" + masterHSId + "}"));
        }
        catch (Exception e) {
            VoltDB.crashLocalVoltDB("Unable to appoint new master for partition " + partitionId, true, e);
        }
    }
}
