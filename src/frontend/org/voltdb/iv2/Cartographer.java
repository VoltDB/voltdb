/* This file is part of VoltDB.
 * Copyright (C) 2008-2013 VoltDB Inc.
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

package org.voltdb.iv2;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.zookeeper_voltpatches.KeeperException;
import org.apache.zookeeper_voltpatches.ZooKeeper;
import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;
import org.voltcore.logging.VoltLogger;
import org.voltcore.utils.CoreUtils;
import org.voltcore.utils.Pair;
import org.voltcore.zk.LeaderElector;
import org.voltcore.zk.ZKUtil;
import org.voltdb.MailboxNodeContent;
import org.voltdb.StatsSource;
import org.voltdb.VoltDB;
import org.voltdb.VoltTable.ColumnInfo;
import org.voltdb.VoltType;
import org.voltdb.VoltZK;
import org.voltdb.VoltZK.MailboxType;
import org.voltdb.compiler.ClusterConfig;

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
    private final Set<Integer> m_allMasters = new HashSet<Integer>();

    /**
     * A dummy iterator that wraps an UnmodifiableIterator<Integer> and provides the
     * Iterator<Object>
     */
    private static class DummyIterator implements Iterator<Object> {
        private final Iterator<Integer> i;

        private DummyIterator(Iterator<Integer> i) {
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
        m_allMasters.clear();
        m_allMasters.addAll(m_iv2Masters.pointInTimeCache().keySet());
        m_allMasters.add(MpInitiator.MP_INIT_PID);
        return new DummyIterator(m_allMasters.iterator());
    }

    @Override
    protected void updateStatsRow(Object rowKey, Object[] rowValues) {
        long leader;
        List<Long> sites = new ArrayList<Long>();
        if (rowKey.equals(MpInitiator.MP_INIT_PID)) {
            leader = getHSIdForMultiPartitionInitiator();
            sites.add(leader);
        }
        else {
            leader = m_iv2Masters.pointInTimeCache().get((Integer)rowKey);
            sites.addAll(getReplicasForPartition((Integer)rowKey));
        }

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
     * Given a set of partition IDs, return a map of partition to a list of HSIDs of all the sites with copies of each partition
     */
    public Map<Integer, List<Long>> getReplicasForPartitions(Set<Integer> partitions) {
        Map<Integer, List<Long>> retval = new HashMap<Integer, List<Long>>();
        List<Pair<Integer,ZKUtil.ChildrenCallback>> callbacks = new ArrayList<Pair<Integer, ZKUtil.ChildrenCallback>>();

        for (Integer partition : partitions) {
            String zkpath = LeaderElector.electionDirForPartition(partition);
            ZKUtil.ChildrenCallback cb = new ZKUtil.ChildrenCallback();
            callbacks.add(Pair.of(partition, cb));
            m_zk.getChildren(zkpath, false, cb, null);
        }

        for (Pair<Integer, ZKUtil.ChildrenCallback> p : callbacks ) {
            final Integer partition = p.getFirst();
            try {
                List<String> children = p.getSecond().getChildren();
                List<Long> sites = new ArrayList<Long>();
                for (String child : children) {
                    sites.add(Long.valueOf(child.split("_")[0]));
                }
                retval.put(partition, sites);
            } catch (KeeperException ke) {
                org.voltdb.VoltDB.crashLocalVoltDB("KeeperException getting replicas for partition: " + partition,
                        true, ke);
            }
            catch (InterruptedException ie) {
                org.voltdb.VoltDB.crashLocalVoltDB("InterruptedException getting replicas for partition: " +
                        partition, true, ie);
            }
        }
        return retval;
    }

    /**
     * Convenience method to return the immediate count of replicas for the given partition
     */
    public int getReplicaCountForPartition(int partition) {
        return getReplicasForPartition(partition).size();
    }

    /**
     * Utility method to sort the keys of a map by their value.  public for testing.
     */
    static public List<Integer> sortKeysByValue(Map<Integer, Integer> map)
    {
        List<Entry<Integer, Integer>> entries = new ArrayList<Entry<Integer, Integer>>(map.entrySet());
        Collections.sort(entries, new Comparator<Entry<Integer, Integer>>() {
            @Override
            public int compare(Entry<Integer, Integer> o1, Entry<Integer, Integer> o2) {
                if (!o1.getValue().equals(o2.getValue())) {
                    return (o1.getValue()).compareTo(o2.getValue());
                }
                return o1.getKey().compareTo(o2.getKey());
            }
        } );
        List<Integer> keys = new ArrayList<Integer>();
        for (Entry<Integer, Integer> entry : entries) {
            keys.add(entry.getKey());
        }
        return keys;
    }

    /**
     * Given the current state of the cluster, compute the partitions which should be replicated on a single new host.
     * Break this method out to be static and testable independent of ZK, JSON, other ugh.
     */
    static public List<Integer> computeReplacementPartitions(Map<Integer, Integer> repsPerPart, int kfactor,
            int sitesPerHost, int numberOfPartitions)
    {
        List<Integer> partitions = new ArrayList<Integer>();
        List<Integer> partSortedByRep = sortKeysByValue(repsPerPart);
        for (int i = 0; i < partSortedByRep.size(); i++) {
            int leastReplicatedPart = partSortedByRep.get(i);
            if (repsPerPart.get(leastReplicatedPart) < kfactor + 1) {
                partitions.add(leastReplicatedPart);
                if (partitions.size() == sitesPerHost) {
                    break;
                }
            }
        }
        return partitions;
    }

    public List<Integer> getIv2PartitionsToReplace(JSONObject topology) throws JSONException
    {
        ClusterConfig clusterConfig = new ClusterConfig(topology);
        hostLog.info("Computing partitions to replace.  Total partitions: " + m_numberOfPartitions);
        Map<Integer, Integer> repsPerPart = new HashMap<Integer, Integer>();
        for (int i = 0; i < m_numberOfPartitions; i++) {
            repsPerPart.put(i, getReplicaCountForPartition(i));
        }
        List<Integer> partitions = computeReplacementPartitions(repsPerPart, clusterConfig.getReplicationFactor(),
                clusterConfig.getSitesPerHost(), m_numberOfPartitions);
        hostLog.info("IV2 Sites will replicate the following partitions: " + partitions);
        return partitions;
    }

    private List<MailboxNodeContent> getMailboxNodeContentList()
    {
        List<MailboxNodeContent> sitesList = new ArrayList<MailboxNodeContent>();
        final Set<Integer> iv2MastersKeySet = m_iv2Masters.pointInTimeCache().keySet();
        Map<Integer, List<Long>> hsidsForPartMap = getReplicasForPartitions(iv2MastersKeySet);
        for (Map.Entry<Integer, List<Long>> entry : hsidsForPartMap.entrySet()) {
            Integer partId = entry.getKey();
            List<Long> hsidsForPart = entry.getValue();
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
        m_iv2Mpi.shutdown();
    }
}
