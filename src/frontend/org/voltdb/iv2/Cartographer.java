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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.zookeeper_voltpatches.KeeperException;
import org.apache.zookeeper_voltpatches.ZooKeeper;

import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;

import org.voltcore.logging.VoltLogger;

import org.voltcore.utils.CoreUtils;

import org.voltcore.zk.LeaderElector;

import org.voltdb.compiler.ClusterConfig;

import org.voltdb.MailboxNodeContent;
import org.voltdb.StatsSource;
import org.voltdb.VoltDB;
import org.voltdb.VoltTable.ColumnInfo;
import org.voltdb.VoltType;
import org.voltdb.VoltZK;
import org.voltdb.VoltZK.MailboxType;

import com.google.common.collect.UnmodifiableIterator;

/**
 * Cartographer provides answers to queries about the components in a cluster.
 * It provides the StatsSource interface for the TOPO statistics selector, but
 * can be called directly as long as the caller is careful about not calling
 * from a network thread (need to avoid ZK deadlocks).
 */
public class Cartographer extends StatsSource
{
    private static final VoltLogger hostLog = new VoltLogger("HOST");
    private final LeaderCacheReader m_iv2Masters;
    private final LeaderCacheReader m_iv2Mpi;
    private final ZooKeeper m_zk;
    private final int m_numberOfPartitions;

    /**
     * A dummy iterator that wraps an UnmodifiableIterator<Integer> and provides the
     * Iterator<Object>
     */
    private class DummyIterator implements Iterator<Object> {
        private final UnmodifiableIterator<Integer> i;

        private DummyIterator(UnmodifiableIterator<Integer> i) {
            this.i = i;
        }

        @Override
        public boolean hasNext() {
            return i.hasNext();
        }

        @Override
        public Object next() {
            return i.next();
        }

        @Override
        public void remove() {
            i.remove();
        }
    }

    public Cartographer(ZooKeeper zk, int numberOfPartitions)
    {
        super(false);
        m_numberOfPartitions = numberOfPartitions;
        m_zk = zk;
        m_iv2Masters = new LeaderCache(m_zk, VoltZK.iv2masters);
        m_iv2Mpi = new LeaderCache(m_zk, VoltZK.iv2mpi);
        try {
            m_iv2Masters.start(true);
            m_iv2Mpi.start(true);
        } catch (Exception e) {
            VoltDB.crashLocalVoltDB("Screwed", true, e);
        }
    }

    @Override
    protected void populateColumnSchema(ArrayList<ColumnInfo> columns)
    {
        columns.add(new ColumnInfo("Partition", VoltType.INTEGER));
        columns.add(new ColumnInfo("Sites", VoltType.STRING));
        columns.add(new ColumnInfo("Leader", VoltType.STRING));

    }

    @Override
    protected Iterator<Object> getStatsRowKeyIterator(boolean interval)
    {
        return new DummyIterator(m_iv2Masters.pointInTimeCache().keySet().iterator());
    }

    @Override
    protected void updateStatsRow(Object rowKey, Object[] rowValues) {
        long leader = m_iv2Masters.pointInTimeCache().get((Integer)rowKey);
        List<Long> sites = getReplicasForPartition((Integer)rowKey);

        rowValues[columnNameToIndex.get("Partition")] = rowKey;
        rowValues[columnNameToIndex.get("Sites")] = CoreUtils.hsIdCollectionToString(sites);
        rowValues[columnNameToIndex.get("Leader")] = CoreUtils.hsIdToString(leader);
    }

    // This used to be the method to get this on SiteTracker
    public long getHSIdForMultiPartitionInitiator()
    {
        return m_iv2Mpi.get(MpInitiator.MP_INIT_PID);
    }

    public long getBuddySiteForMPI(long hsid)
    {
        int host = CoreUtils.getHostIdFromHSId(hsid);
        // We'll be lazy and get the map we'd feed to SiteTracker's
        // constructor, then go looking for a matching host ID.
        List<MailboxNodeContent> sitesList = getMailboxNodeContentList();
        for (MailboxNodeContent site : sitesList) {
            if (site.partitionId != MpInitiator.MP_INIT_PID && host == CoreUtils.getHostIdFromHSId(site.HSId)) {
                return site.HSId;
            }
        }
        throw new RuntimeException("Unable to find a buddy initiator for MPI with HSID: " +
                                   CoreUtils.hsIdToString(hsid));
    }

    /**
     * Given a partition ID, return a list of HSIDs of all the sites with copies of that partition
     */
    public List<Long> getReplicasForPartition(int partition) {
        String zkpath = LeaderElector.electionDirForPartition(partition);
        List<Long> retval = new ArrayList<Long>();
        try {
            List<String> children = m_zk.getChildren(zkpath, null);
            for (String child : children) {
                retval.add(Long.valueOf(child.split("_")[0]));
            }
        }
        catch (KeeperException ke) {
            org.voltdb.VoltDB.crashLocalVoltDB("KeeperException getting replicas for partition: " + partition,
                    true, ke);
        }
        catch (InterruptedException ie) {
            org.voltdb.VoltDB.crashLocalVoltDB("InterruptedException getting replicas for partition: " +
                    partition, true, ie);
        }
        return retval;
    }

    /**
     * Convenience method to return the immediate count of replicas for the given partition
     */
    public int getReplicaCountForPartition(int partition) {
        return getReplicasForPartition(partition).size();
    }

    public List<Integer> getIv2PartitionsToReplace(JSONObject topology) throws JSONException
    {
        ClusterConfig clusterConfig = new ClusterConfig(topology);
        hostLog.info("Computing partitions to replace.  Total partitions: " + m_numberOfPartitions);
        List<Integer> repsPerPart = new ArrayList<Integer>(m_numberOfPartitions);
        for (int i = 0; i < m_numberOfPartitions; i++) {
            repsPerPart.add(i, getReplicaCountForPartition(i));
        }
        List<Integer> partitions = new ArrayList<Integer>();
        int freeSites = clusterConfig.getSitesPerHost();
        for (int i = 0; i < m_numberOfPartitions; i++) {
            if (repsPerPart.get(i) < clusterConfig.getReplicationFactor() + 1) {
                partitions.add(i);
                // pretend to be fully replicated so we don't put two copies of a
                // partition on this host.
                repsPerPart.set(i, clusterConfig.getReplicationFactor() + 1);
                freeSites--;
                if (freeSites == 0) {
                    break;
                }
            }
        }
        if (freeSites > 0) {
            // double check fully replicated?
            for (int i = 0; i < m_numberOfPartitions; i++) {
                if (repsPerPart.get(i) < clusterConfig.getReplicationFactor() + 1) {
                    hostLog.error("Partition " + i + " should have been replicated but wasn't");
                }
            }
        }
        hostLog.info("IV2 Sites will replicate the following partitions: " + partitions);
        return partitions;
    }

    private List<MailboxNodeContent> getMailboxNodeContentList()
    {
        List<MailboxNodeContent> sitesList = new ArrayList<MailboxNodeContent>();
        for (Integer partId : m_iv2Masters.pointInTimeCache().keySet()) {
            List<Long> hsidsForPart = getReplicasForPartition(partId);
            for (long hsid : hsidsForPart) {
                MailboxNodeContent mnc = new MailboxNodeContent(hsid, partId);
                sitesList.add(mnc);
            }
        }
        return sitesList;
    }

    public Map<MailboxType, List<MailboxNodeContent>> getSiteTrackerMailboxMap()
    {
        HashMap<MailboxType, List<MailboxNodeContent>> result =
            new HashMap<MailboxType, List<MailboxNodeContent>>();
        List<MailboxNodeContent> sitesList = getMailboxNodeContentList();
        result.put(MailboxType.ExecutionSite, sitesList);
        return result;
    }

    public void shutdown() throws InterruptedException
    {
        m_iv2Masters.shutdown();
    }
}
