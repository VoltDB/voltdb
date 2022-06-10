/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
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

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.function.BiConsumer;

import org.apache.zookeeper_voltpatches.CreateMode;
import org.apache.zookeeper_voltpatches.KeeperException;
import org.apache.zookeeper_voltpatches.ZooKeeper;
import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONStringer;
import org.voltcore.logging.VoltLogger;
import org.voltcore.messaging.BinaryPayloadMessage;
import org.voltcore.messaging.ForeignHost;
import org.voltcore.messaging.HostMessenger;
import org.voltcore.utils.CoreUtils;
import org.voltcore.utils.Pair;
import org.voltcore.zk.CoreZK;
import org.voltcore.zk.LeaderElector;
import org.voltcore.zk.ZKUtil;
import org.voltdb.AbstractTopology;
import org.voltdb.MailboxNodeContent;
import org.voltdb.RealVoltDB;
import org.voltdb.StatsSource;
import org.voltdb.VoltDB;
import org.voltdb.VoltTable;
import org.voltdb.VoltTable.ColumnInfo;
import org.voltdb.VoltType;
import org.voltdb.VoltZK;
import org.voltdb.VoltZK.MailboxType;
import org.voltdb.iv2.LeaderCache.LeaderCallBackInfo;

import com.google_voltpatches.common.base.Preconditions;
import com.google_voltpatches.common.collect.ArrayListMultimap;
import com.google_voltpatches.common.collect.ImmutableMap;
import com.google_voltpatches.common.collect.Lists;
import com.google_voltpatches.common.collect.Maps;
import com.google_voltpatches.common.collect.Multimap;
import com.google_voltpatches.common.collect.Multimaps;
import com.google_voltpatches.common.collect.Sets;

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
    private final Set<Long> m_currentSPMasters = new HashSet<Long>();
    private final HostMessenger m_hostMessenger;
    private final ZooKeeper m_zk;
    private final Set<Integer> m_allMasters = new HashSet<Integer>();

    public static final String JSON_PARTITION_ID = "partitionId";
    public static final String JSON_INITIATOR_HSID = "initiatorHSId";
    public static final String JSON_LEADER_MIGRATION = "leaderMigration";

    private final int m_configuredReplicationFactor;
    //partition masters by host
    private final Map<Integer, Set<Long>> m_currentMastersByHost = Maps.newTreeMap();

    private final ExecutorService m_es
            = CoreUtils.getCachedSingleThreadExecutor("Cartographer", 15000);

    public enum TOPO {
        Partition               (VoltType.INTEGER),
        Sites                   (VoltType.STRING),
        Leader                  (VoltType.STRING);

        public final VoltType m_type;
        TOPO(VoltType type) { m_type = type; }
    }

    /**
     * Retrieve the list of partitions in the system. Since each partition information is being populated individually
     * asynchronously some partitions may throw exceptions or block when accessing data and other may not.
     * <p>
     * The arguments passed to {@code errorHandler} will be the zookeeper path which was being accessed at the time of
     * the exception and the exception which was thrown.
     *
     * @param zk           {@link ZooKeeper} instance to use to retrieve partition information
     * @param skipMp       If {@code true} the MP partition will not be included in the result
     * @param errorHandler {@link BiConsumer} that will be invoked when exception is encountered
     * @return {@link List} of {@link AsyncPartition}s
     */
    public static List<AsyncPartition> getPartitionsAsync(ZooKeeper zk, boolean skipMp,
            BiConsumer<String, Exception> errorHandler) {
        List<String> partitionDirs = null;
        try {
            partitionDirs = zk.getChildren(VoltZK.leaders_initiators, null);
        } catch (Exception e) {
            errorHandler.accept(VoltZK.leaders_initiators, e);
            return null;
        }

        // Don't fetch the values serially do it asynchronously
        List<AsyncPartition> partitions = new ArrayList<>(partitionDirs.size());
        for (String partitionDir : partitionDirs) {
            try {
                int pid = LeaderElector.getPartitionFromElectionDir(partitionDir);
                if (skipMp && pid == MpInitiator.MP_INIT_PID) {
                    continue;
                }
                partitions.add(new AsyncPartition(pid, partitionDir, zk));
            } catch (Exception e) {
                errorHandler.accept(ZKUtil.joinZKPath(VoltZK.leaders_initiators, partitionDir), e);
                return null;
            }
        }

        return partitions;
    }

    public static long getHsidFromPartitionChild(String child) {
        return Long.parseLong(child.substring(0, child.indexOf('_')));
    }

    public static int getHostIdFromPartitionChild(String child) {
        return CoreUtils.getHostIdFromHSId(getHsidFromPartitionChild(child));
    }

    // This message used to be sent by the SP or MP initiator when they accepted a promotion.
    // For dev speed, we'll detect mastership changes here and construct and send this message to the
    // local client interface so we can keep the CIs implementation
    private void sendLeaderChangeNotify(long hsId, int partitionId, boolean migratePartitionLeader)
    {
        hostLog.info("[Cartographer] Sending leader change notification with new leader:" +
                CoreUtils.hsIdToString(hsId) + " for partition:" + partitionId);

        try {
            JSONStringer stringer = new JSONStringer();
            stringer.object();
            stringer.keySymbolValuePair(JSON_PARTITION_ID, partitionId);
            stringer.keySymbolValuePair(JSON_INITIATOR_HSID, hsId);
            stringer.keySymbolValuePair(JSON_LEADER_MIGRATION, migratePartitionLeader);
            stringer.endObject();
            BinaryPayloadMessage bpm = new BinaryPayloadMessage(new byte[0], stringer.toString().getBytes("UTF-8"));
            int hostId = m_hostMessenger.getHostId();
            m_hostMessenger.send(CoreUtils.getHSIdFromHostAndSite(hostId,
                        HostMessenger.CLIENT_INTERFACE_SITE_ID),
                    bpm);
        }
        catch (Exception e) {
            VoltDB.crashLocalVoltDB("Unable to propogate leader promotion to client interface.", true, e);
        }
    }

    LeaderCache.Callback m_MPICallback = new LeaderCache.Callback()
    {
        @Override
        public void run(ImmutableMap<Integer, LeaderCallBackInfo> cache) {
            // Every MPI change means a new single MPI.  Just do the right thing here
            int pid = MpInitiator.MP_INIT_PID;
            // Can be zero-length at startup
            if (cache.size() > 0) {
                hostLog.info("[Cartographer MP] Sending leader change notification with new leader:");
                sendLeaderChangeNotify(cache.get(pid).m_HSId, pid, false);
            }
        }
    };

    LeaderCache.Callback m_SPIMasterCallback = new LeaderCache.Callback()
    {
        @Override
        public void run(ImmutableMap<Integer, LeaderCallBackInfo> cache) {
            // We know there's a 1:1 mapping between partitions and HSIds in this map.
            // let's flip it
            // Map<Long, Integer> hsIdToPart = new HashMap<Long, Integer>();
            Set<LeaderCallBackInfo> newMasters = new HashSet<LeaderCallBackInfo>();
            Set<Long> newHSIDs = Sets.newHashSet();
            Map<Integer, Set<Long>> newMastersByHost = Maps.newTreeMap();
            for (Entry<Integer, LeaderCallBackInfo> e : cache.entrySet()) {
                LeaderCallBackInfo newMasterInfo = e.getValue();
                Long hsid = newMasterInfo.m_HSId;
                int partitionId = e.getKey();
                newHSIDs.add(hsid);
                // hsIdToPart.put(hsid, partitionId);
                int hostId = CoreUtils.getHostIdFromHSId(hsid);
                Set<Long> masters = newMastersByHost.get(hostId);
                if (masters == null) {
                    masters = Sets.newHashSet();
                    newMastersByHost.put(hostId, masters);
                }
                masters.add(hsid);
                if (!m_currentSPMasters.contains(hsid)) {
                    // we want to see items which are present in the new map but not in the old,
                    // these are newly promoted SPIs
                    newMasters.add(newMasterInfo);
                    // send the messages indicating promotion from here for each new master
                    sendLeaderChangeNotify(hsid, partitionId, newMasterInfo.m_isMigratePartitionLeaderRequested);
                }
            }

            if (hostLog.isDebugEnabled()) {
                Set<String> masters = Sets.newHashSet();
                m_currentSPMasters.forEach((k) -> {
                    masters.add(CoreUtils.hsIdToString(k));
                });
                hostLog.debug("[Cartographer] SP masters:" + masters);
                masters.clear();
                cache.values().forEach((k) -> {
                    masters.add(CoreUtils.hsIdToString(k.m_HSId));
                });
                hostLog.debug("[Cartographer]Updated SP masters:" + masters + ". New masters:" + newMasters);
            }

            m_currentSPMasters.clear();
            m_currentSPMasters.addAll(newHSIDs);
            m_currentMastersByHost.clear();
            m_currentMastersByHost.putAll(newMastersByHost);
        }
    };

    public Cartographer(HostMessenger hostMessenger, int configuredReplicationFactor, boolean partitionDetectionEnabled) {
        super(false);
        m_hostMessenger = hostMessenger;
        m_zk = hostMessenger.getZK();
        m_iv2Masters = new LeaderCache(m_zk, "Cartographer-iv2Masters-" + hostMessenger.getHostId(),
                VoltZK.iv2masters, m_SPIMasterCallback);
        m_iv2Mpi = new LeaderCache(m_zk, "Cartographer-iv2Mpi-" + hostMessenger.getHostId(),
                VoltZK.iv2mpi, m_MPICallback);
        m_configuredReplicationFactor = configuredReplicationFactor;
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
        for (TOPO col : TOPO.values()) {
            columns.add(new VoltTable.ColumnInfo(col.name(), col.m_type));
        };
    }

    @Override
    protected Iterator<Object> getStatsRowKeyIterator(boolean interval)
    {
        m_allMasters.clear();
        m_allMasters.addAll(m_iv2Masters.pointInTimeCache().keySet());
        m_allMasters.add(MpInitiator.MP_INIT_PID);

        //make a copy of the master list for the topology statistics to avoid any concurrent modification
        //since the master list may be updated while the topology statistics is being built.
        Set<Object> masters = new HashSet<>(m_allMasters);
        return masters.iterator();
    }

    @Override
    protected int updateStatsRow(Object rowKey, Object[] rowValues) {
        long leader;
        List<Long> sites = new ArrayList<Long>();
        if (rowKey.equals(MpInitiator.MP_INIT_PID)) {
            leader = getHSIdForMultiPartitionInitiator();
            sites.add(leader);
        }
        else {
            //sanity check. The master list may be updated while the statistics is calculated.
            Long leaderInCache = m_iv2Masters.pointInTimeCache().get(rowKey);
            if (leaderInCache == null) {
                return 0;
            }

            leader = leaderInCache;
            sites.addAll(getReplicasForPartition((Integer)rowKey));
        }

        rowValues[TOPO.Partition.ordinal()] = rowKey;
        rowValues[TOPO.Sites.ordinal()] = CoreUtils.hsIdCollectionToString(sites);
        rowValues[TOPO.Leader.ordinal()] = CoreUtils.hsIdToString(leader);
        return TOPO.values().length;
    }

    /**
     * Convenience method: Get the HSID of the master for the specified partition ID, SP or MP
     */
    public Long getHSIdForMaster(int partitionId)
    {
        if (partitionId == MpInitiator.MP_INIT_PID) {
            return getHSIdForMultiPartitionInitiator();
        }
        else {
            return getHSIdForSinglePartitionMaster(partitionId);
        }
    }

    /**
     * Checks whether this host is partition 0 'zero' leader
     *
     * @return result of the test
     */
    public boolean isPartitionZeroLeader()
    {
        return CoreUtils.getHostIdFromHSId(m_iv2Masters.get(0)) == m_hostMessenger.getHostId();
    }

    /**
     * Checks whether this host is partition 0 'zero' leader
     *
     * @return result of the test
     */
    public boolean isHostIdLocal(int hostId)
    {
        return hostId == m_hostMessenger.getHostId();
    }

    /**
     * Get the HSID of the single partition master for the specified partition ID
     */
    public Long getHSIdForSinglePartitionMaster(int partitionId)
    {
        return m_iv2Masters.get(partitionId);
    }

    /**
     * validate partition id
     * @param partitionId  The partition id
     * @return return true if the partition id is valid
     */
    public boolean hasPartition(int partitionId) {
        return ((LeaderCache)m_iv2Masters).contain(partitionId);
    }

    // This used to be the method to get this on SiteTracker
    public Long getHSIdForMultiPartitionInitiator()
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
     * Returns the IDs of the partitions currently in the cluster.
     * @return A list of partition IDs
     */
    public static List<Integer> getPartitions(ZooKeeper zk) {
        List<Integer> partitions = new ArrayList<Integer>();
        try {
            List<String> children = zk.getChildren(VoltZK.leaders_initiators, null);
            for (String child : children) {
                partitions.add(LeaderElector.getPartitionFromElectionDir(child));
            }
        } catch (KeeperException e) {
            VoltDB.crashLocalVoltDB("Failed to get partition IDs from ZK", true, e);
        } catch (InterruptedException e) {
            VoltDB.crashLocalVoltDB("Failed to get partition IDs from ZK", true, e);
        }
        return partitions;
    }

    public List<Integer> getPartitions() {
        return Cartographer.getPartitions(m_zk);
    }

    public List<Integer> getPartitionIdsForLocalhost() {
        return (List<Integer>) getHostToPartitionMap().get(m_hostMessenger.getHostId());
    }

    public int getPartitionCount()
    {
        // The list returned by getPartitions includes the MP PID.  Need to remove that for the
        // true partition count.
        return Cartographer.getPartitions(m_zk).size() - 1;
    }

    private Multimap<Integer, Integer> getHostToPartitionMap() {
        Multimap<Integer, Integer> hostToPartitions = ArrayListMultimap.create();
        for (int pId : getPartitions()) {
            if (pId == MpInitiator.MP_INIT_PID) {
                continue;
            }
            List<Long> hsIDs = getReplicasForPartition(pId);
            hsIDs.forEach(hsId -> hostToPartitions.put(CoreUtils.getHostIdFromHSId(hsId), pId));
        }
        return hostToPartitions;
    }

    /**
     * Convenient method, given a hostId, return the hostId of its buddies (including itself) which both
     * belong to the same partition group.
     * @param hostId
     * @return A set of host IDs (includes the given hostId) that both belong to the same partition group
     */
    public Set<Integer> getHostIdsWithinPartitionGroup(int hostId) {
        Set<Integer> hostIds = Sets.newHashSet();

        Multimap<Integer, Integer> hostToPartitions = getHostToPartitionMap();
        if (!hostToPartitions.containsKey(hostId)) {
            // during the reduced k safety mode
            // host with no partition leader contains no effective partitions
            return hostIds;
        }
        Multimap<Integer, Integer> partitionByIds = ArrayListMultimap.create();
        Multimaps.invertFrom(hostToPartitions, partitionByIds);
        for (int partition : hostToPartitions.asMap().get(hostId)) {
            hostIds.addAll(partitionByIds.get(partition));
        }
        return hostIds;
    }

    /**
     * Convenient method, given a set of partitionIds, return the hostId of their partition group buddies.
     *
     * @param partitions A list of partitions that to be assigned to the newly rejoined host
     * @return A set of host IDs that both belong to the same partition group
     */
    public Set<Integer> findPartitionGroupPeers(List<Integer> partitions) {
        Set<Integer> hostIds = Sets.newHashSet();
        Multimap<Integer, Integer> hostToPartitions = getHostToPartitionMap();
        Multimap<Integer, Integer> partitionByIds = ArrayListMultimap.create();
        Multimaps.invertFrom(hostToPartitions, partitionByIds);
        for (int p : partitions) {
            hostIds.addAll(partitionByIds.get(p));
        }
        return hostIds;
    }

    /**
     * @return a multi map of a pair of Partition to HSIDs to all Hosts
     */
    public Multimap<Integer, Entry<Integer,Long>> getHostToPartition2HSIdMap() {
        Multimap<Integer, Entry<Integer,Long>> hostToHSId = ArrayListMultimap.create();
        for (int pId : getPartitions()) {
            if (pId == MpInitiator.MP_INIT_PID) {
                continue;
            }
            List<Long> hsIDs = getReplicasForPartition(pId);
            hsIDs.forEach(hsId -> hostToHSId.put(CoreUtils.getHostIdFromHSId(hsId),  new AbstractMap.SimpleEntry<>(pId, hsId)));
        }
        return hostToHSId;
    }

    /**
     * Given a partition ID, return a list of HSIDs of all the sites with copies of that partition
     */
    public List<Long> getReplicasForPartition(int partition) {
        String zkpath = LeaderElector.electionDirForPartition(VoltZK.leaders_initiators, partition);
        List<Long> retval = new ArrayList<Long>();
        try {
            List<String> children = m_zk.getChildren(zkpath, null);
            for (String child : children) {
                retval.add(getHsidFromPartitionChild(child));
            }
        }
        catch (KeeperException.NoNodeException e) {
            //Can happen when partitions are being removed
        } catch (KeeperException | InterruptedException e) {
            org.voltdb.VoltDB.crashLocalVoltDB("Exception getting replicas for partition: " + partition,
                    true, e);
        }

        return retval;
    }

    /**
     * return host site id with a given host id and a partition id
     * @param hostId  The host id
     * @param partition  The partition id
     * @return  a site id or null if there is no such a site
     */
    public Long getHSIDForPartitionHost(int hostId, int partition) {
        List<Long> hsids = getReplicasForPartition(partition);
        for (Long hsid : hsids) {
           if (hostId == CoreUtils.getHostIdFromHSId(hsid)){
               return hsid;
           }
        }
        return null;
    }

    /**
     * Given a set of partition IDs, return a map of partition to a list of HSIDs of all the sites with copies of each partition
     */
    public Map<Integer, List<Long>> getReplicasForPartitions(Collection<Integer> partitions) {
        Map<Integer, List<Long>> retval = new HashMap<Integer, List<Long>>();
        List<Pair<Integer,ZKUtil.ChildrenCallback>> callbacks = new ArrayList<Pair<Integer, ZKUtil.ChildrenCallback>>();

        for (Integer partition : partitions) {
            String zkpath = LeaderElector.electionDirForPartition(VoltZK.leaders_initiators, partition);
            ZKUtil.ChildrenCallback cb = new ZKUtil.ChildrenCallback();
            callbacks.add(Pair.of(partition, cb));
            m_zk.getChildren(zkpath, false, cb, null);
        }

        for (Pair<Integer, ZKUtil.ChildrenCallback> p : callbacks ) {
            final Integer partition = p.getFirst();
            try {
                List<String> children = p.getSecond().get();
                List<Long> sites = new ArrayList<Long>();
                for (String child : children) {
                    sites.add(getHsidFromPartitionChild(child));
                }
                retval.put(partition, sites);
            } catch (KeeperException.NoNodeException e) {
                //This can happen when a partition is being removed from the system
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
        List<Entry<Integer, Integer>> entries = CoreUtils.sortKeyValuePairByValue(map);
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
    static public void computeReplacementPartitions(Map<Integer, Integer> repsPerPart, int kfactor,
                                                             int sitesPerHost, List<Integer> partitions)
    {
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
    }

    public List<Integer> getIv2PartitionsToReplace(int kfactor, int sitesPerHost, int localHostId, Map<Integer, String> hostGroups)
        throws JSONException
    {
        Preconditions.checkArgument(sitesPerHost != VoltDB.UNDEFINED);
        List<Integer> partitionsToReplace = new ArrayList<Integer>();
        Map<Integer, Integer> repsPerPart = new HashMap<Integer, Integer>();
        List<Collection<Integer>> sortedHosts = AbstractTopology.sortHostIdByHGDistance(localHostId, hostGroups);

        // attach partitions to each hosts
        Multimap<Integer, Integer> hostToPartitions = getHostToPartitionMap();
        for (Collection<Integer> subgroup : sortedHosts) {
            Set<Integer> partitions = new HashSet<Integer>();
            for (Integer hid : subgroup) {
                partitions.addAll(hostToPartitions.get(hid));
            }
            hostLog.info("Computing partitions to replace.  Qualified partitions: " + partitions);
            // sort the partitions by replicas number
            for (Integer pid : partitions) {
                repsPerPart.put(pid, getReplicaCountForPartition(pid));
            }
            computeReplacementPartitions(repsPerPart, kfactor, sitesPerHost, partitionsToReplace);
            if (partitionsToReplace.size() == sitesPerHost) {
                hostLog.info("IV2 Sites will replicate the following partitions: " + partitionsToReplace);
                break;
            }
        }
        return partitionsToReplace;
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
        m_es.shutdown();
    }

    //Check partition replicas.
    public synchronized String stopNodeIfClusterIsSafe(final Set<Integer> liveHids, final int ihid) {
        try {
            return m_es.submit(new Callable<String>() {
                @Override
                public String call() throws Exception {
                    if (m_configuredReplicationFactor == 0) {
                        //Dont die in k=0 cluster
                        return "Stopping individual nodes is only allowed on a K-safe cluster";
                    }
                    // check if any node still in rejoin status
                    try {
                        if (m_zk.exists(CoreZK.rejoin_node_blocker, false) != null) {
                            return "All rejoin nodes must be completed";
                        }
                    } catch (KeeperException.NoNodeException ignore) {} // shouldn't happen
                    //Otherwise we do check replicas for host
                    Set<Integer> otherStoppedHids = new HashSet<Integer>();
                    // Write the id of node to be stopped to ZK, partition detection will bypass this node.
                    ZKUtil.addIfMissing(m_zk, ZKUtil.joinZKPath(VoltZK.host_ids_be_stopped, Integer.toString(ihid)), CreateMode.PERSISTENT, null);
                    try {
                        List<String> children = m_zk.getChildren(VoltZK.host_ids_be_stopped, false);
                        for (String child : children) {
                            int hostId = Integer.parseInt(child);
                            otherStoppedHids.add(hostId);
                        }
                        otherStoppedHids.remove(ihid); /* don't count self */
                    } catch (KeeperException.NoNodeException ignore) {}
                    String reason = doPartitionsHaveReplicas(ihid, otherStoppedHids);
                    if (reason == null) {
                        // Safe to stop
                        m_hostMessenger.sendStopNodeNotice(ihid);
                        // Shutdown or send poison pill
                        int hid = m_hostMessenger.getHostId();
                        if (hid == ihid) {
                            //Killing myself no pill needs to be sent
                            VoltDB.instance().halt();
                        } else {
                            //Send poison pill with target to kill
                            m_hostMessenger.sendPoisonPill(ihid, "@StopNode", ForeignHost.CRASH_ME);
                        }
                    } else {
                        // unsafe, clear the indicator
                        ZKUtil.deleteRecursively(m_zk, ZKUtil.joinZKPath(VoltZK.host_ids_be_stopped, Integer.toString(ihid)));
                    }
                    return reason;
                }
            }).get();
        } catch (InterruptedException | ExecutionException t) {
            hostLog.debug("LeaderAppointer: Error in isClusterSafeIfIDie.", t);
            return "Internal error: " + t.getMessage();
        }
    }

    public String verifyPartitionLeaderMigrationForStopNode(final int ihid) {
        if (m_configuredReplicationFactor == 0) {
            return "Stopping individual nodes is only allowed on a K-safe cluster";
        }
        try {
            return m_es.submit(new Callable<String>() {
                @Override
                public String call() throws Exception {
                    // Check rejoining status
                    try {
                        if (m_zk.exists(CoreZK.rejoin_node_blocker, false) != null) {
                            return "All rejoin nodes must be completed";
                        }
                    } catch (KeeperException.NoNodeException ignore) {}
                    // Check replicas
                    Set<Integer> otherStoppedHids = new HashSet<Integer>();
                    try {
                        List<String> children = m_zk.getChildren(VoltZK.host_ids_be_stopped, false);
                        for (String child : children) {
                            int hostId = Integer.parseInt(child);
                            otherStoppedHids.add(hostId);
                        }
                    } catch (KeeperException.NoNodeException ignore) {}
                    otherStoppedHids.remove(ihid);
                    if (!otherStoppedHids.isEmpty()) {
                        return "Can't move partition leaders while other nodes are being shut down.";
                    }
                    String message = doPartitionsHaveReplicas(ihid, otherStoppedHids);
                    if (message != null) {
                        return message;
                    }
                    // Check existence of background partition leader migration request, and fail early if it exists.
                    // Background leader migration interferes PrepareStopNode check.
                    // Partition leader distribution must be balanced, otherwise leader might be ping-ponged
                    // from one request to another.
                    try {
                        final String errMsg = "Can't move partition leaders while another leader migration is in progress";
                        final String logForm = "[Host id %d] Can't stop host id %d: %s";
                        int thisHost = VoltDB.instance().getMyHostId();
                        if (m_zk.exists(VoltZK.migratePartitionLeaderBlocker, false) != null) {
                            hostLog.rateLimitedWarn(60, logForm, thisHost, ihid, "ZooKeeper reports partition leadership migration in progress");
                            return errMsg;
                        }
                        else if (!isPartitionLeadersBalanced()) {
                            hostLog.rateLimitedWarn(60, logForm, thisHost, ihid, "partition leadership is not in balance");
                            return errMsg;
                        }
                        hostLog.infoFmt("[Host id %d] OK to stop host id %d, no other leader migration in progress", thisHost, ihid);
                    } catch (KeeperException.NoNodeException ignore) {}
                    return null;
                }}).get();
        } catch (InterruptedException | ExecutionException t) {
            return "Internal error: " + t.getMessage();
        }
    }

    private boolean isPartitionLeadersBalanced() {
        // Is partition leader migration on?
        final boolean disableSpiTask = "true".equals(System.getProperty("DISABLE_MIGRATE_PARTITION_LEADER", "false"));
        if (disableSpiTask) {
            return true;
        }
        RealVoltDB db = (RealVoltDB) VoltDB.instance();
        if(db.isClusterComplete()) {
            Pair<Integer, Integer> pair = getPartitionLeaderMigrationTarget(db.getHostCount(),Integer.MIN_VALUE, true);
            // Partition leader migration may be in progress. don't mess up
            return (pair == null || pair.getFirst() == -1 ) ;
        }
        return true;
    }

    // Check if partitions on host hid will have enough replicas after losing host hid.
    // If nodesBeingStopped was set, it means there are concurrent @StopNode running,
    // don't count replicas on those to-be-stopped nodes.
    private String doPartitionsHaveReplicas(int hid, Set<Integer> nodesBeingStopped) {
        hostLog.debug("Cartographer: Reloading partition information.");
        assert (!nodesBeingStopped.contains(hid));

        String[] error = { null };
        List<AsyncPartition> partitions = getPartitionsAsync(m_zk, false,
                (d, e) -> error[0] = "Failed to read ZooKeeper node " + d + ": " + e.getMessage());

        if (error[0] != null) {
            return error[0];
        }

        //Assume that we are ksafe
        for (AsyncPartition partition : partitions) {
            try {
                //Dont let anyone die if someone is in INITIALIZING state
                if (partition.isInitializing()) {
                    return "StopNode is disallowed in initialization phase";
                }

                if (partition.getPid() == MpInitiator.MP_INIT_PID) {
                    continue;
                }
                //Get Hosts for replicas
                final List<Integer> replicaHost = new ArrayList<>();
                final List<Integer> replicaOnStoppingHost = new ArrayList<>();
                boolean hostHasReplicas = false;
                for (String replica : partition.getReplicas()) {
                    final String split[] = replica.split("/");
                    final int hostId = getHostIdFromPartitionChild(split[split.length - 1]);
                    if (hostId == hid) {
                        hostHasReplicas = true;
                    }
                    if (nodesBeingStopped.contains(hostId)) {
                        replicaOnStoppingHost.add(hostId);
                    }
                    replicaHost.add(hostId);
                }
                if (hostLog.isDebugEnabled()) {
                    hostLog.debug("Replica Host for Partition " + partition.getPid() + " " + replicaHost);
                }
                if (hostHasReplicas && replicaHost.size() <= 1) {
                    return "Cluster doesn't have enough replicas";
                }
                if (hostHasReplicas && !replicaOnStoppingHost.isEmpty() && replicaHost.size() <= replicaOnStoppingHost.size() + 1) {
                    return "Cluster doesn't have enough replicas. There are concurrent stop node requests, retry the command later";
                }
            } catch (InterruptedException | KeeperException | NumberFormatException e) {
                return "Failed to stop node:" + e.getMessage();
            }
        }
        return null;
    }

    /**
     * used to calculate the partition candidate for migration
     */
    private static class Host implements Comparable<Host> {

        final int m_hostId;

        //the master partition ids on the host
        List<Integer> m_masterPartitionIDs = Lists.newArrayList();

        //the replica partition ids on the host
        List<Integer> m_replicaPartitionIDs = Lists.newArrayList();

        public Host(int hostId) {
            m_hostId = hostId;
        }

        @Override
        public int compareTo(Host other){
            int diff = (other.m_masterPartitionIDs.size() - m_masterPartitionIDs.size());
            if (diff != 0) {
                return diff;
            }
            return (m_hostId - other.m_hostId);
        }

        @Override
        public int hashCode() {
            return m_hostId;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj instanceof Host) {
                Host info = (Host)obj;
                return m_hostId == info.m_hostId;
            }
            return false;
        }

        public void addPartition(Integer partitionId, boolean master) {
            if (master) {
                m_masterPartitionIDs.add(partitionId);
            } else {
                m_replicaPartitionIDs.add(partitionId);
            }
        }

        public int getPartitionLeaderCount() {
            return m_masterPartitionIDs.size();
        }

        @Override
        public String toString() {
            StringBuilder builder = new StringBuilder();
            builder.append("Host:" + m_hostId);
            builder.append(", master:" + m_masterPartitionIDs);
            builder.append(", replica:" + m_replicaPartitionIDs);
            return builder.toString();
        }
    }

    /**
     * find a partition and its target host for MigratePartitionLeader
     * SPI is migrated from the host with most partition leaders to a host which has the partition replica and
     * the least number of partition. if the host with @localHostId is not the host which has the most partition
     * leaders, return null. Eventually the host with most partition leaders will initiate MigratePartitionLeader.
     * @param hostCount  The number of hosts in the cluster
     * @param localHostId the host id
     * @return  a pair of partition id and destination host id
     */
    public Pair<Integer, Integer> getPartitionLeaderMigrationTarget(int hostCount, int localHostId, boolean prepareStopNode) {

        final int maxMastersPerHost = (int)Math.ceil(((double)getPartitionCount()) / hostCount);
        final int minMastersPerHost = (getPartitionCount() / hostCount);

        // Sort the hosts by partition leader count, descending
        List<Host> hostList = getHostsByPartionMasterCount();
        if (hostList == null) {
            return Pair.of(-1, -1);
        }

        // only move SPI from the one with most partition leaders
        // The local ClientInterface will pick it up and start @MigratePartitionLeader
        Host srcHost = hostList.get(0);

        // @MigratePartitionLeader is initiated on the host with the old leader to facilitate DR integration
        // If current host does not have the most partition leaders, give it up.
        // Let the host with the most partition leaders to migrate
        if (srcHost.m_hostId != localHostId && !prepareStopNode) {
            return null;
        }

        // The new host is the one with least number of partition leaders and the partition replica
        for (ListIterator<Host> reverseIt = hostList.listIterator(hostList.size()); reverseIt.hasPrevious();) {
            Host targetHost = reverseIt.previous();
            int partitionCandidate = findNewHostForPartitionLeader(srcHost,targetHost, maxMastersPerHost, minMastersPerHost);
            if (partitionCandidate > -1){
                return Pair.of(partitionCandidate, targetHost.m_hostId);
            }
        }

        // indicate that the cluster is balanced.
        return Pair.of(-1, -1);
    }

    private List<Host> getHostsByPartionMasterCount() {
        Set<Integer> liveHosts = m_hostMessenger.getLiveHostIds();
        if (liveHosts.size() == 1) {
            return null;
        }
        //collect host and partition info
        Map<Integer, Host> hostsMap = Maps.newHashMap();
        Set<Integer> allMasters = new HashSet<Integer>();
        allMasters.addAll(m_iv2Masters.pointInTimeCache().keySet());
        for (Integer partitionId : allMasters) {
            int leaderHostId = CoreUtils.getHostIdFromHSId(m_iv2Masters.pointInTimeCache().get(partitionId));

            //sanity check to make sure that the topology is not in the middle of leader promotion
            if (!liveHosts.contains(leaderHostId)) {
                return null;
            }
            Host leaderHost = hostsMap.get(leaderHostId);
            if (leaderHost == null) {
                leaderHost = new Host(leaderHostId);
                hostsMap.put(leaderHostId, leaderHost);
            }
            List<Long> sites = getReplicasForPartition(partitionId);
            for (long site : sites) {
                int hostId = CoreUtils.getHostIdFromHSId(site);
                if (!liveHosts.contains(hostId)) {
                    return null;
                }
                Host host = hostsMap.get(hostId);
                if (host == null) {
                    host = new Host(hostId);
                    hostsMap.put(hostId, host);
                }
                host.addPartition(partitionId, (leaderHostId == hostId));
            }
        }

        //Sort the hosts by partition leader count, descending
        List<Host> hostList = new ArrayList<>(hostsMap.values());
        Collections.sort(hostList);
        return hostList;
    }

    private int findNewHostForPartitionLeader(Host src, Host target, int maxMastersPerHost, int minMastersPerHost) {
        // can't move onto itself
        if (src.equals(target)) {
            return -1;
        }

        // still have more leaders than max?
        if (src.getPartitionLeaderCount() > maxMastersPerHost) {
            for (Integer partition : src.m_masterPartitionIDs) {
                if (target.m_replicaPartitionIDs.contains(partition) && target.getPartitionLeaderCount() < maxMastersPerHost) {
                    return partition;
                }
            }
        } else {
            if (target.getPartitionLeaderCount() >= minMastersPerHost || src.getPartitionLeaderCount() <= minMastersPerHost) {
                return -1;
            } else {
                for (Integer partition : src.m_masterPartitionIDs) {
                    if (target.m_replicaPartitionIDs.contains(partition)) {
                        return partition;
                    }
                }
            }
        }
        return -1;
    }

    // find a new host for a partition leader on this host
    public Pair<Integer, Integer> getPartitionLeaderMigrationTargetForStopNode(int localHostId) {

        // Sort the hosts by partition leader count, descending
        List<Host> hostList = getHostsByPartionMasterCount();
        if (hostList == null) {
            return Pair.of(-1, -1);
        }
        Host srcHost = null;
        for (Host host : hostList) {
            if (host.m_hostId == localHostId) {
                if (!host.m_masterPartitionIDs.isEmpty()) {
                    srcHost = host;
                }
                break;
            }
        }

        if (srcHost == null) {
            return Pair.of(-1, -1);
        }

        // The new host is the one with least number of partition leaders and the partition replica
        for (ListIterator<Host> reverseIt = hostList.listIterator(hostList.size()); reverseIt.hasPrevious();) {
            Host targetHost = reverseIt.previous();
            if (!targetHost.equals(srcHost)) {
                for (Integer partition : srcHost.m_masterPartitionIDs) {
                    if (targetHost.m_replicaPartitionIDs.contains(partition)) {
                        return Pair.of(partition, targetHost.m_hostId);
                    }
                }
            }
        }

        // Indicate that all the partition leaders have been relocated.
        return Pair.of(-1, -1);
    }

    //return the number of masters on a host
    public int getMasterCount(int hostId) {
        Set<Long> masters = m_currentMastersByHost.get(hostId);
        if (masters == null) {
            return 0;
        }
        return masters.size();
    }

    //Utility method to peek the topology
    public static VoltTable peekTopology(Cartographer cart) {
        ArrayList<ColumnInfo> columns = new ArrayList<>();
        for (TOPO col : TOPO.values()) {
            columns.add(new VoltTable.ColumnInfo(col.name(), col.m_type));
        };
        VoltTable t = new VoltTable(columns.toArray(new ColumnInfo[columns.size()]));

        Iterator<Object> i = cart.getStatsRowKeyIterator(false);
        while (i.hasNext()) {
            Object rowKey = i.next();
            long leader;
            List<Long> sites = new ArrayList<Long>();
            if (rowKey.equals(MpInitiator.MP_INIT_PID)) {
                leader = cart.getHSIdForMultiPartitionInitiator();
                sites.add(leader);
            } else {
                leader = cart.m_iv2Masters.pointInTimeCache().get(rowKey);
                sites.addAll(cart.getReplicasForPartition((Integer)rowKey));
            }
            t.addRow(rowKey, CoreUtils.hsIdCollectionToString(sites), CoreUtils.hsIdToString(leader));
        }
        return t;
    }

    public boolean hasPartitionMastersOnHosts(Set<Integer> hosts) {
        for (Integer host : hosts) {
            if (m_currentMastersByHost.containsKey(host)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Simple class to retrieve a partition data and replicas through async zookeeper callbacks
     */
    public static final class AsyncPartition {
        private final int m_pid;
        private final String m_path;
        private final ZKUtil.ByteArrayCallback m_stateCallback = new ZKUtil.ByteArrayCallback();
        private final ZKUtil.ChildrenCallback m_childrenCallback = new ZKUtil.ChildrenCallback();

        public AsyncPartition(String partitionDir, ZooKeeper zooKeeper) {
            this(LeaderElector.getPartitionFromElectionDir(partitionDir), partitionDir, zooKeeper);
        }

        AsyncPartition(int pid, String partitionDir, ZooKeeper zooKeeper) {
            m_pid = pid;
            m_path = ZKUtil.joinZKPath(VoltZK.leaders_initiators, partitionDir);
            zooKeeper.getData(m_path, false, m_stateCallback, null);
            zooKeeper.getChildren(m_path, false, m_childrenCallback, null);
        }

        /**
         * @return the pid of the partition
         */
        public int getPid() {
            return m_pid;
        }

        /**
         * @return The absolute zookeeper path to the partition
         */
        public String getPath() {
            return m_path;
        }

        /**
         * This call blocks until the data is populated by zookeeper or an exception occurs
         *
         * @return {@code true} if the state is exactly {@link LeaderElector#INITIALIZING}
         * @throws InterruptedException If this thread is interrupted while waiting for data
         * @throws KeeperException      If there was an error retrieving the state from zookeeper
         */
        public boolean isInitializing() throws InterruptedException, KeeperException {
            byte[] state = m_stateCallback.get();
            return state != null && state.length == 1 && state[0] == LeaderElector.INITIALIZING;
        }

        /**
         * This call blocks until the data is populated by zookeeper or an exception occurs
         *
         * @return {@code true} if the state is exactly {@link LeaderElector#INITIALIZED}
         * @throws InterruptedException If this thread is interrupted while waiting for data
         * @throws KeeperException      If there was an error retrieving the state from zookeeper
         */
        public boolean isInitialized() throws InterruptedException, KeeperException {
            byte[] state = m_stateCallback.get();
            assert state != null && state.length == 1;
            return state != null && state.length == 1 && state[0] == LeaderElector.INITIALIZED;
        }

        /**
         * This call blocks until the data is populated by zookeeper or an exception occurs
         *
         * @return list of replicas for partition
         * @throws InterruptedException If this thread is interrupted while waiting for data
         * @throws KeeperException      If there was an error retrieving the state from zookeeper
         */
        public List<String> getReplicas() throws InterruptedException, KeeperException {
            return m_childrenCallback.get();
        }
    }
}
