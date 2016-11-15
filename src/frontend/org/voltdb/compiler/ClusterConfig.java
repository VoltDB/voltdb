/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
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
package org.voltdb.compiler;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.json_voltpatches.JSONArray;
import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;
import org.json_voltpatches.JSONStringer;
import org.voltcore.logging.VoltLogger;
import org.voltdb.VoltDB;
import org.voltdb.utils.MiscUtils;

import com.google_voltpatches.common.collect.Lists;
import com.google_voltpatches.common.collect.Maps;
import com.google_voltpatches.common.collect.Multimap;
import com.google_voltpatches.common.collect.Sets;

public class ClusterConfig
{
    private static final VoltLogger hostLog = new VoltLogger("HOST");

    public static List<Integer> partitionsForHost(JSONObject topo, int hostId) throws JSONException
    {
        return partitionsForHost(topo, hostId, false);
    }

    public static List<Integer> partitionsForHost(JSONObject topo, int hostId, boolean onlyMasters) throws JSONException
    {
        List<Integer> partitions = new ArrayList<Integer>();

        JSONArray parts = topo.getJSONArray("partitions");

        for (int p = 0; p < parts.length(); p++) {
            // have an object in the partitions array
            JSONObject aPartition = parts.getJSONObject(p);
            int pid = aPartition.getInt("partition_id");
            if (onlyMasters) {
                if (aPartition.getInt("master") == hostId) {
                    partitions.add(pid);
                }
            } else {
                JSONArray replicas = aPartition.getJSONArray("replicas");
                for (int h = 0; h < replicas.length(); h++) {
                    int replica = replicas.getInt(h);
                    if (replica == hostId) {
                        partitions.add(pid);
                    }
                }
            }
        }

        return partitions;
    }

    /**
     * Add a list of hosts to the current topology.
     *
     * This method modifies the topology in place.
     *
     * @param newHosts The number of new hosts to add
     * @param topo The existing topology, which will have the host count updated in-place.
     */
    public static void addHosts(int newHosts, JSONObject topo) throws JSONException
    {
        ClusterConfig config = new ClusterConfig(topo);
        int kfactor = config.getReplicationFactor();

        if (newHosts != kfactor + 1) {
            VoltDB.crashLocalVoltDB("Only adding " + (kfactor + 1) + " nodes at a time is " +
                    "supported, currently trying to add " + newHosts, false, null);
        }

        // increase host count
        topo.put("hostcount", config.getHostCount() + newHosts);
    }

    /**
     * Add new partitions to the topology.
     * @param topo          The topology that will be added to.
     * @param partToHost    A map of new partitions to their corresponding replica host IDs.
     * @throws JSONException
     */
    public static void addPartitions(JSONObject topo, Multimap<Integer, Integer> partToHost)
        throws JSONException
    {
        JSONArray partitions = topo.getJSONArray("partitions");
        for (Map.Entry<Integer, Collection<Integer>> e : partToHost.asMap().entrySet()) {
            int partition = e.getKey();
            Collection<Integer> hosts = e.getValue();

            JSONObject partObj = new JSONObject();
            partObj.put("partition_id", partition);
            partObj.put("replicas", hosts);

            partitions.put(partObj);
        }
    }

    /**
     * Add new sitesperhost mapping to the toplogy
     * @param topo              The topology that will be added to.
     * @param joinedHostIds     Host ids that are elastically joining into cluster.
     * @param sphMap            A map of host ids to their sitesperhost setting
     * @throws JSONException
     */
    public static void addSitesPerHosts(JSONObject topo,
                                        Collection<Integer> joinedHostIds,
                                        Map<Integer, Integer> sphMap)
        throws JSONException
    {
        JSONArray hostIdToSph = topo.getJSONArray("host_id_to_sph");
        for (Integer hostId : joinedHostIds) {
            JSONObject sphObj = new JSONObject();
            sphObj.put("host_id", hostId);
            sphObj.put("sites_per_host", sphMap.get(hostId));
            hostIdToSph.put(sphObj);
        }
    }

    public ClusterConfig(int hostCount, Map<Integer, Integer> sitesPerHostMap, int replicationFactor)
    {
        m_hostCount = hostCount;
        m_sitesPerHostMap = sitesPerHostMap;
        m_replicationFactor = replicationFactor;
        m_errorMsg = "Config is unvalidated";
    }

    // Construct a ClusterConfig object from the JSON topology.  The computations
    // for this object are currently deterministic given the three values below, so
    // this all magically works.  If you change that fact, good luck Chuck.
    public ClusterConfig(JSONObject topo) throws JSONException
    {
        m_hostCount = topo.getInt("hostcount");
        m_sitesPerHostMap = Maps.newHashMap();
        JSONArray sphMap = topo.getJSONArray("host_id_to_sph");
        for (int i = 0; i < sphMap.length(); i++) {
            JSONObject entry = sphMap.getJSONObject(i);
            int hostId = entry.getInt("host_id");
            int sph = entry.getInt("sites_per_host");
            m_sitesPerHostMap.put(hostId, sph);
        }
        m_replicationFactor = topo.getInt("kfactor");
        m_errorMsg = "Config is unvalidated";
    }

    public int getHostCount()
    {
        return m_hostCount;
    }

    public Map<Integer, Integer> getSitesPerHostMap()
    {
        return m_sitesPerHostMap;
    }

    public int getReplicationFactor()
    {
        return m_replicationFactor;
    }

    public int getTotalSitesCount() {
        int totalSites = 0;
        for (Map.Entry<Integer, Integer> entry : m_sitesPerHostMap.entrySet()) {
            totalSites += entry.getValue();
        }
        return totalSites;
    }

    public int getPartitionCount()
    {
        return getTotalSitesCount() / (m_replicationFactor + 1);
    }

    public String getErrorMsg()
    {
        return m_errorMsg;
    }

    public boolean validate()
    {
        if (m_hostCount <= 0)
        {
            m_errorMsg = "The number of hosts must be > 0.";
            return false;
        }
        for (Map.Entry<Integer, Integer> entry : m_sitesPerHostMap.entrySet()) {
            if (entry.getValue() <= 0) {
                m_errorMsg = "The number of sites per host must be > 0.";
                return false;
            }
        }
        if (m_hostCount <= m_replicationFactor)
        {
            m_errorMsg = String.format("%d servers required for K-safety = %d",
                                       m_replicationFactor + 1, m_replicationFactor);
            return false;
        }
        if (getPartitionCount() == 0)
        {
            m_errorMsg = String.format("Insufficient execution site count to achieve K-safety of %d",
                                       m_replicationFactor);
            return false;
        }
        if (getTotalSitesCount() % (m_replicationFactor + 1) > 0)
        {
            m_errorMsg = "The cluster has more hosts and sites per hosts than required for the " +
                "requested k-safety value. The number of total sites must be a " +
                "whole multiple of the number of copies of the database (k-safety + 1)";
            return false;
        }
        m_errorMsg = "Cluster config contains no detected errors";
        return true;
    }

    public boolean validate(int origStartCount)
    {
        boolean isValid = validate();
        if (isValid && origStartCount < m_hostCount && origStartCount > 0)
        {
            if ((m_hostCount - origStartCount) > m_replicationFactor + 1)
            {
                m_errorMsg = String.format("You can only add %d servers at a time for k=%d",
                        m_replicationFactor + 1, m_replicationFactor);
                return false;
            }
            else if ((m_hostCount - origStartCount) % (m_replicationFactor + 1) != 0)
            {
                m_errorMsg = String.format("Must add %d servers at a time for k=%d",
                        m_replicationFactor + 1, m_replicationFactor);
                return false;
            }
        }
        return isValid;
    }

    private static class Partition {
        private Node m_master;
        private final Set<Node> m_replicas = new HashSet<Node>();
        private final Integer m_partitionId;

        private int m_neededReplicas;

        public Partition(Integer partitionId, int neededReplicas) {
            m_partitionId = partitionId;
            m_neededReplicas = neededReplicas;
        }

        @Override
        public int hashCode() {
            return m_partitionId.hashCode();
        }

        public void decrementNeededReplicas() {
            if (m_neededReplicas == 0) {
                throw new RuntimeException("ClusterConfig error: Attempted to replicate a partition too many times");
            }
            m_neededReplicas--;
        }

        @Override
        public boolean equals(Object o) {
            if (o instanceof Partition) {
                Partition p = (Partition)o;
                return m_partitionId.equals(p.m_partitionId);
            }
            return false;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("Partition " + m_partitionId + " needing replicas " + m_neededReplicas);
            sb.append(" with master " + m_master.m_hostId + " and replicas ");
            for (Node n : m_replicas) {
                sb.append(n.m_hostId).append(", ");
            }
            return sb.toString();
        }
    }

    private static class Node implements Comparable {
        Set<Partition> m_masterPartitions = new HashSet<Partition>();
        Set<Partition> m_replicaPartitions = new HashSet<Partition>();
        Map<Node, Integer> m_replicationConnections = new HashMap<Node, Integer>();
        Integer m_hostId;
        final String[] m_group;

        public Node(Integer hostId, String[] group) {
            m_hostId = hostId;
            m_group = group;
        }

        int partitionCount() {
            return m_masterPartitions.size() + m_replicaPartitions.size();
        }

        /**
         * Sum the replica count of each partition this node contains up. The
         * count does not include this node itself.
         */
        int replicationFactor() {
            int a = 0;
            for (Partition p : m_masterPartitions) {
                a += p.m_replicas.size();
            }
            for (Partition p : m_replicaPartitions) {
                a += p.m_replicas.size();
            }
            return a;
        }

        @Override
        public int hashCode() {
            return m_hostId.hashCode();
        }

        @Override
        public boolean equals(Object o) {
            if (o instanceof Node) {
                Node n = (Node)o;
                return m_hostId.equals(n.m_hostId);
            }
            return false;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("Host " + m_hostId + " master of ");
            for (Partition p : m_masterPartitions) {
                sb.append(p.m_partitionId).append(", ");
            }
            sb.append(" replica of ");
            for (Partition p : m_replicaPartitions) {
                sb.append(p.m_partitionId).append(", ");
            }
            sb.append(" connected to ");
            for (Map.Entry<Node, Integer> entry : m_replicationConnections.entrySet()) {
                sb.append(" host " + entry.getKey().m_hostId + " for " + entry.getValue() + " partitions");
            }
            return sb.toString();
        }

        @Override
        public int compareTo(Object o)
        {
            if (!(o instanceof Node)) {
                return -1;
            }
            return Integer.compare(m_hostId, ((Node) o).m_hostId);
        }
    }

    /**
     * Represents a group (rack, floor, data center, etc.). A group can have
     * subgroups, e.g. racks on a floor. If the group does not have subgroups,
     * it can contain nodes, e.g. nodes on a rack.
     */
    private static class Group {
        final Map<String, Group> m_children = Maps.newTreeMap();
        final Set<Node> m_hosts = Sets.newTreeSet();

        public void addHost(String[] group, int host) {
            addHost(group, 0, host);
        }

        private void addHost(String[] group, int i, int host) {
            Group nextGroup = m_children.get(group[i]);
            if (nextGroup == null) {
                nextGroup = new Group();
                m_children.put(group[i], nextGroup);
            }

            if (group.length == i + 1) {
                nextGroup.m_hosts.add(new Node(host, group));
            } else {
                nextGroup.addHost(group, i + 1, host);
            }
        }

        /**
         * Get the nodes in the groups that are sibling to the given group,
         * e.g. if the given group is a rack, the returned value is the servers
         * in the neighboring racks.
         *
         * @return A list of nodes in their groups. The list is ordered by the
         * reverse distance to the given group. So the first group in the list
         * is farthest away from the given group, and the last group in the list
         * is closest to the given group.
         */
        public List<Deque<Node>> getGroupSiblingsOf(String[] group)
        {
            List<Deque<Node>> results = Lists.newArrayList();
            getGroupSiblingsOf(group, 0, results);
            return results;
        }

        private void getGroupSiblingsOf(String[] group, int i, List<Deque<Node>> results) {
            if (m_children.isEmpty()) {
                // base condition. It works without this check, adding it here for readability.
                return;
            }

            for (Map.Entry<String, Group> e : m_children.entrySet()) {
                if (!e.getKey().equals(group[i])) {
                    getHosts(e.getValue(), results);
                }
            }

            for (Map.Entry<String, Group> e : m_children.entrySet()) {
                if (e.getKey().equals(group[i])) {
                    e.getValue().getGroupSiblingsOf(group, i + 1, results);
                }
            }
        }

        public void removeHost(Node node) {
            if (m_children.isEmpty()) {
                m_hosts.remove(node);
                return;
            }
            for (Group l : m_children.values()) {
                l.removeHost(node);
            }
        }

        private static void getHosts(Group group, List<Deque<Node>> hosts) {
            if (group.m_children.isEmpty() && !group.m_hosts.isEmpty()) {
                final Deque<Node> hostsInGroup = new ArrayDeque<>();
                hosts.add(hostsInGroup);
                hostsInGroup.addAll(group.m_hosts);
                return;
            }
            for (Group l : group.m_children.values()) {
                getHosts(l, hosts);
            }
        }

        @Override
        public String toString()
        {
            return "Level{" +
            "m_children=" + m_children +
            ", m_hosts=" + m_hosts +
            '}';
        }
    }

    /**
     * Represents the physical topology of the cluster. Nodes are always
     * organized in groups, a group can have subgroups, nodes only exist in the
     * leaf groups.
     */
    private static class PhysicalTopology {
        final Group m_root = new Group();

        public PhysicalTopology(Map<Integer, String> hostGroups) {
            for (Map.Entry<Integer, String> e : hostGroups.entrySet()) {
                m_root.addHost(parseGroup(e.getValue()), e.getKey());
            }
        }

        /**
         * Parse the group into components. A group is represented by dot
         * seperated subgroups. A subgroup is just a string. For example,
         * "rack1.server1" is a valid group with two subgroups, "192.168.0.1" is
         * also a valid group with four subgroups.
         */
        private static String[] parseGroup(String group) {
            final String[] components = group.trim().split("\\.");

            for (int i = 0; i < components.length; i++) {
                if (components[i].trim().isEmpty()) {
                    throw new IllegalArgumentException("Group component cannot be empty: " + group);
                }
                components[i] = components[i].trim();
            }

            return components;
        }

        /**
         * Get all nodes reachable from the root group. Nodes that have enough
         * partitions may be removed from the physical topology so that they no
         * longer appear in the list of subsequent calculations. This does not
         * mean that they are no longer part of the cluster.
         */
        public List<Deque<Node>> getAllHosts() {
            return m_root.getGroupSiblingsOf(new String[]{null});
        }
    }

    /*
     * Original placement strategy that doesn't get very good performance
     */
    JSONObject fallbackPlacementStrategy(
            List<Integer> hostIds,
            int hostCount,
            int partitionCount,
            Map<Integer, Integer> sitesPerHostMap) throws JSONException{
        // add all the sites
        int partitionCounter = -1;

        // build the assignment map, each entry has the lower bound of partition range as key,
        // the host id as value.
        TreeMap<Integer, Integer> sitesToHostId = Maps.newTreeMap();
        int sitesCounter = 0;
        for (Map.Entry<Integer, Integer> entry : sitesPerHostMap.entrySet()) {
            sitesToHostId.put(sitesCounter, entry.getKey());
            sitesCounter += entry.getValue();
        }

        HashMap<Integer, ArrayList<Integer>> partToHosts =
            new HashMap<Integer, ArrayList<Integer>>();
        for (int i = 0; i < partitionCount; i++)
        {
            ArrayList<Integer> hosts = new ArrayList<Integer>();
            partToHosts.put(i, hosts);
        }
        int totalSitesCount = getTotalSitesCount();
        for (int i = 0; i < totalSitesCount; i++) {
            // serially assign partitions to execution sites.
            int partition = (++partitionCounter) % partitionCount;
            int hostId = sitesToHostId.get(sitesToHostId.floorKey(i));
            partToHosts.get(partition).add(hostId);
        }

        // We need to sort the hostID lists for each partition so that
        // the leader assignment magic in the loop below will work.
        for (Map.Entry<Integer, ArrayList<Integer>> e : partToHosts.entrySet()) {
            Collections.sort(e.getValue());
        }

        if (!checkKSafetyConstraint(partToHosts, m_replicationFactor  + 1)) {
            VoltDB.crashLocalVoltDB("Unable to find feasible partition replica assignment for the specified configuration");
        }

        JSONStringer stringer = new JSONStringer();
        stringer.object();
        stringer.keySymbolValuePair("hostcount", m_hostCount);
        stringer.keySymbolValuePair("kfactor", getReplicationFactor());
        stringer.key("host_id_to_sph").array();
        for (Map.Entry<Integer, Integer> entry : sitesPerHostMap.entrySet()) {
            stringer.object();
            stringer.keySymbolValuePair("host_id", entry.getKey());
            stringer.keySymbolValuePair("sites_per_host", entry.getValue());
            stringer.endObject();
        }
        stringer.endArray();
        stringer.key("partitions").array();
        for (int part = 0; part < partitionCount; part++)
        {
            stringer.object();
            stringer.keySymbolValuePair("partition_id", part);
            // This two-line magic deterministically spreads the partition leaders
            // evenly across the cluster at startup.
            int index = part % (getReplicationFactor() + 1);
            int master = partToHosts.get(part).get(index);
            stringer.keySymbolValuePair("master", master);
            stringer.key("replicas").array();
            for (int host_pos : partToHosts.get(part)) {
                stringer.value(host_pos);
            }
            stringer.endArray();
            stringer.endObject();
        }
        stringer.endArray();
        stringer.endObject();
        JSONObject topo = new JSONObject(stringer.toString());
        return topo;
    }

    /**
     * Each partition should has K+1 replicas
     */
    public static boolean checkKSafetyConstraint(
            HashMap<Integer, ArrayList<Integer>> partToHosts,
            int replicasCount)
    {
        return partToHosts.entrySet().stream().allMatch(
                v -> replicasCount == Sets.newHashSet(v.getValue()).size());
    }

    /**
     * Placement strategy that attempts to distribute replicas across different
     * groups and also involve multiple nodes in replication so that the socket
     * between nodes is not a bottleneck.
     *
     * This algorithm has three steps.
     * 1. Partition master assignment,
     * 2. Group partition replica assignment.
     * 3. Remaining partition replica assignment.
     */
    JSONObject groupAwarePlacementStrategy(
            Map<Integer, String> hostGroups,
            int partitionCount,
            Map<Integer, Integer> sitesPerHostMap) throws JSONException {
        final PhysicalTopology phys = new PhysicalTopology(hostGroups);
        final List<Node> allNodes = MiscUtils.zip(phys.getAllHosts());

        List<Partition> partitions = new ArrayList<Partition>();
        for (int ii = 0; ii < partitionCount; ii++) {
            partitions.add(new Partition(ii, getReplicationFactor() + 1));
        }

        // Step 1. Distribute mastership by round-robining across all the
        // nodes. This balances the masters among the nodes.
        Iterator<Node> iter = allNodes.iterator();
        for (Partition p : partitions) {
            if (!iter.hasNext()) {
                iter = allNodes.iterator();
                assert iter.hasNext();
            }
            p.m_master = iter.next();
            p.decrementNeededReplicas();
            p.m_master.m_masterPartitions.add(p);
        }

        if (getReplicationFactor() > 0) {
            // Step 2. For each partition, assign a replica to each group other
            // than the group of the partition master. This guarantees that the
            // replicas are spread out as much as possible. There may be more
            // replicas than groups available, which will be handled in step 3.
            for (Partition p : partitions) {
                final List<Deque<Node>> nodesInOtherGroups =
                    sortByConnectionsToNode(p.m_master, phys.m_root.getGroupSiblingsOf(p.m_master.m_group));

                List<Node> firstReplicaNodes = new ArrayList<>();
                final Iterator<Deque<Node>> groupIter = nodesInOtherGroups.iterator();
                int needed = p.m_neededReplicas;

                // Pick one node from each other group to be the replica
                while (needed-- > 0 && groupIter.hasNext()) {
                    final Deque<Node> nextGroup = groupIter.next();
                    for (Node nextNode : nextGroup) {
                        if (nextNode.partitionCount() < sitesPerHostMap.get(nextNode.m_hostId)) {
                            assert p.m_master != nextNode;
                            assert !nextNode.m_replicaPartitions.contains(p);
                            firstReplicaNodes.add(nextNode);
                            break;
                        }
                    }
                }

                assignReplicas(sitesPerHostMap, phys, p, firstReplicaNodes);
            }

            // Step 3. Assign the remaining replicas. Make sure each partition
            // is fully replicated.
            for (Partition p : partitions) {
                if (p.m_neededReplicas > 0) {
                    final List<Node> nodesLeft = MiscUtils.zip(phys.getAllHosts());
                    nodesLeft.remove(p.m_master);
                    final List<Node> sortedNodesLeft =
                        MiscUtils.zip(sortByConnectionsToNode(p.m_master, Collections.singletonList(nodesLeft)));
                    assignReplicas(sitesPerHostMap, phys, p, sortedNodesLeft);
                }
            }
        }

        // Sanity check to make sure each node has enough partitions and each
        // partition has enough replicas.
        for (Node n : allNodes) {
            if (n.partitionCount() != sitesPerHostMap.get(n.m_hostId)) {
                throw new RuntimeException("Unable to assign partitions using the new placement algorithm");
            }
        }
        for (Partition p : partitions) {
            if (p.m_neededReplicas != 0) {
                throw new RuntimeException("Unable to assign partitions using the new placement algorithm");
            }
        }

        JSONStringer stringer = new JSONStringer();
        stringer.object();
        stringer.keySymbolValuePair("hostcount", m_hostCount);
        stringer.keySymbolValuePair("kfactor", getReplicationFactor());
        stringer.key("host_id_to_sph").array();
        for (Map.Entry<Integer, Integer> entry : sitesPerHostMap.entrySet()) {
            stringer.object();
            stringer.keySymbolValuePair("host_id", entry.getKey());
            stringer.keySymbolValuePair("sites_per_host", entry.getValue());
            stringer.endObject();
        }
        stringer.endArray();
        stringer.key("partitions").array();
        for (int part = 0; part < partitionCount; part++)
        {
            stringer.object();
            stringer.keySymbolValuePair("partition_id", part);
            stringer.keySymbolValuePair("master", partitions.get(part).m_master.m_hostId);
            stringer.key("replicas").array();
            for (Node n : partitions.get(part).m_replicas) {
                stringer.value(n.m_hostId);
            }
            stringer.value(partitions.get(part).m_master.m_hostId);
            stringer.endArray();
            stringer.endObject();
        }
        stringer.endArray();
        stringer.endObject();

        return new JSONObject(stringer.toString());
    }

    /**
     * Given a collection of candidate nodes, try to assign the remaining
     * replicas of the given partition to these nodes in the given order.
     *
     * If the given candidate nodes are more than needed, it will only use the
     * first few. If there are less, the given partition will not be fully
     * replicated.
     */
    private static void assignReplicas(Map<Integer, Integer> sitesPerHostMap, PhysicalTopology phys, Partition p, Collection<Node> nodes) {
        final Iterator<Node> nodeIter = nodes.iterator();
        while (p.m_neededReplicas > 0 && nodeIter.hasNext()) {
            final Node replica = nodeIter.next();

            if (replica.partitionCount() == sitesPerHostMap.get(replica.m_hostId)) {
                phys.m_root.removeHost(replica);
                continue;
            }
            if (p.m_master == replica || p.m_replicas.contains(replica)) {
                continue;
            }

            p.m_replicas.add(replica);
            p.decrementNeededReplicas();
            replica.m_replicaPartitions.add(p);

            if (p.m_master.m_replicationConnections.containsKey(replica)) {
                final int connections = p.m_master.m_replicationConnections.get(replica);
                p.m_master.m_replicationConnections.put(replica, connections + 1);
                replica.m_replicationConnections.put(p.m_master, connections + 1);
            } else {
                p.m_master.m_replicationConnections.put(replica, 1);
                replica.m_replicationConnections.put(p.m_master, 1);
            }
        }
    }

    /**
     * Sort the given groups of nodes based on their connections to the master
     * node, their replication factors, and their master partition counts, in
     * that order. The sorting does not change the grouping of the nodes. It
     * favors nodes with less connections to the master node. If the connection
     * count is the same, it favors nodes with fewer replications. If the
     * replication count is the same, it favors nodes with more master
     * partitions.
     */
    private static List<Deque<Node>> sortByConnectionsToNode(final Node master, List<? extends Collection<Node>> nodes) {
        List<Deque<Node>> result = Lists.newArrayList();

        for (Collection<Node> deque : nodes) {
            final LinkedList<Node> toBeSorted = Lists.newLinkedList(deque);

            Collections.sort(toBeSorted, new Comparator<Node>() {
                @Override
                public int compare(Node o1, Node o2)
                {
                    final Integer o1Connections = master.m_replicationConnections.get(o1);
                    final Integer o2Connections = master.m_replicationConnections.get(o2);
                    final int connComp = Integer.compare(o1Connections == null ? 0 : o1Connections,
                                                         o2Connections == null ? 0 : o2Connections);
                    if (connComp == 0) {
                        final int repFactorComp = Integer.compare(o1.replicationFactor(), o2.replicationFactor());
                        if (repFactorComp == 0) {
                            return -Integer.compare(o1.m_masterPartitions.size(), o2.m_masterPartitions.size());
                        } else {
                            return repFactorComp;
                        }
                    } else {
                        return connComp;
                    }
                }
            });

            result.add(toBeSorted);
        }

        return result;
    }

    // Statically build a topology. This only runs at startup;
    // rejoin clones this from an existing server.
    public JSONObject getTopology(Map<Integer, String> hostGroups) throws JSONException
    {
        int hostCount = getHostCount();
        int partitionCount = getPartitionCount();
        Map<Integer, Integer> sitesPerHostMap = getSitesPerHostMap();

        if (hostCount != hostGroups.size()) {
            throw new RuntimeException("Provided " + hostGroups.size() + " host ids when host count is " + hostCount);
        }

        JSONObject topo;
        if (Boolean.valueOf(System.getenv("VOLT_REPLICA_FALLBACK"))) {
            topo = fallbackPlacementStrategy(Lists.newArrayList(hostGroups.keySet()),
                                             hostCount, partitionCount, sitesPerHostMap);
        } else {
            try {
                topo = groupAwarePlacementStrategy(hostGroups, partitionCount, sitesPerHostMap);
            } catch (Exception e) {
                hostLog.error("Unable to use optimal replica placement strategy. " +
                              "Falling back to a less optimal strategy that may result in worse performance");
                topo = fallbackPlacementStrategy(Lists.newArrayList(hostGroups.keySet()),
                                                 hostCount, partitionCount, sitesPerHostMap);
            }
        }

        hostLog.debug("TOPO: " + topo.toString(2));
        return topo;
    }

    private final int m_hostCount;
    private final Map<Integer, Integer> m_sitesPerHostMap;
    private final int m_replicationFactor;

    private String m_errorMsg;
}
