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
import java.util.Arrays;
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
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Stream;

import org.apache.zookeeper_voltpatches.KeeperException;
import org.json_voltpatches.JSONArray;
import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;
import org.json_voltpatches.JSONStringer;
import org.voltcore.logging.VoltLogger;
import org.voltcore.utils.CoreUtils;
import org.voltdb.VoltDB;
import org.voltdb.utils.MiscUtils;

import com.google_voltpatches.common.base.Preconditions;
import com.google_voltpatches.common.collect.HashMultimap;
import com.google_voltpatches.common.collect.Lists;
import com.google_voltpatches.common.collect.Maps;
import com.google_voltpatches.common.collect.Multimap;
import com.google_voltpatches.common.collect.Multimaps;
import com.google_voltpatches.common.collect.Sets;
import com.google_voltpatches.common.collect.TreeMultimap;

public class ClusterConfig
{
    private static final VoltLogger hostLog = new VoltLogger("HOST");

    public static List<Integer> partitionsForHost(JSONObject topo, int hostId) throws JSONException
    {
        return partitionsForHost(topo, hostId, false);
    }

    public static List<Integer> partitionsForHost(JSONObject topo, int hostId, boolean onlyMasters) throws JSONException
    {
        List<Integer> partitions = new ArrayList<>();

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

    public ClusterConfig(int hostCount, int sitesPerHost, int replicationFactor)
    {
        m_hostCount = hostCount;
        m_sitesPerHost = sitesPerHost;
        m_replicationFactor = replicationFactor;
        m_errorMsg = "Config is unvalidated";
    }

    // Construct a ClusterConfig object from the JSON topology.  The computations
    // for this object are currently deterministic given the three values below, so
    // this all magically works.  If you change that fact, good luck Chuck.
    public ClusterConfig(JSONObject topo) throws JSONException
    {
        m_hostCount = topo.getInt("hostcount");
        m_sitesPerHost = topo.getInt("sites_per_host");
        m_replicationFactor = topo.getInt("kfactor");
        m_errorMsg = "Config is unvalidated";
    }

    public int getHostCount()
    {
        return m_hostCount;
    }

    public int getSitesPerHost()
    {
        return m_sitesPerHost;
    }

    public int getReplicationFactor()
    {
        return m_replicationFactor;
    }

    public int getPartitionCount()
    {
        return (m_hostCount * m_sitesPerHost) / (m_replicationFactor + 1);
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
        if (m_sitesPerHost <= 0)
        {
            m_errorMsg = "The number of sites per host must be > 0.";
            return false;
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
        if ((m_hostCount * m_sitesPerHost) % (m_replicationFactor + 1) > 0)
        {
            m_errorMsg = "The cluster has more hosts and sites per hosts than required for the " +
                "requested k-safety value. The number of total sites (sitesPerHost * hostCount) must be a " +
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


    /**
     * Extend the group concept to support multiple tags for single node.
     */
    public static class ExtensibleGroupTag {
        public final String m_rackAwarenessGroup;
        public final String m_buddyGroup;

        public ExtensibleGroupTag(String raGroup, String buddyGroup) {
            m_rackAwarenessGroup = raGroup;
            m_buddyGroup = buddyGroup;
        }
    }

    private static class Partition implements Comparable {
        private Node m_master;
        private final Set<Node> m_replicas = new HashSet<>();
        private final Integer m_partitionId;
        private int m_neededReplicas; // include master partitions
        private int m_kfactor;

        public Partition(Integer partitionId, int neededReplicas) {
            m_partitionId = partitionId;
            m_neededReplicas = neededReplicas;
            m_kfactor = m_neededReplicas - 1;
        }

        @Override
        public int hashCode() {
            return m_partitionId.hashCode();
        }

        // doesn't include the master partition
        int assignedReplicas() {
            return m_kfactor - m_neededReplicas;
        }

        public void decrementNeededReplicas() {
            if (m_neededReplicas == 0) {
                throw new RuntimeException("ClusterConfig error: Attempted to replicate a partition too many times");
            }
            m_neededReplicas--;
        }

        public void incrementNeededReplicas() {
            m_neededReplicas++;
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
            sb.append("\nP").append(m_partitionId).append(" (").append(m_neededReplicas).append(")");
            sb.append(" [");
            if (m_master != null) {
                sb.append(m_master.m_hostId).append("*,");
            }
            for (Node n : m_replicas) {
                sb.append(n.m_hostId).append(",");
            }
            sb.append("]");
            return sb.toString();
        }

        @Override
        public int compareTo(Object o)
        {
            if (!(o instanceof Partition)) {
                return -1;
            }
            return Integer.compare(m_partitionId, ((Partition) o).m_partitionId);
        }
    }

    private static class Node implements Comparable {
        Set<Partition> m_masterPartitions = new HashSet<>();
        Set<Partition> m_replicaPartitions = new HashSet<>();
        Map<Node, Integer> m_replicationConnections = Maps.newHashMap();
        Integer m_hostId;
        int m_sitesPerHost;
        final String[] m_group;

        public Node(Integer hostId, int sitesPerHost, String[] group) {
            m_hostId = hostId;
            m_sitesPerHost = sitesPerHost;
            m_group = group;
        }

        int partitionCount() {
            return m_masterPartitions.size() + m_replicaPartitions.size();
        }

        int unassignedSlot() {
            return m_sitesPerHost - partitionCount();
        }

        boolean contains(Partition p) {
            return m_masterPartitions.contains(p) || m_replicaPartitions.contains(p);
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
            sb.append("\nH").append(m_hostId).append(" [");
            for (Partition p : m_masterPartitions) {
                sb.append(p.m_partitionId).append("*,");
            }
            for (Partition p : m_replicaPartitions) {
                sb.append(p.m_partitionId).append(",");
            }
            sb.append("] -> [");
            for (Map.Entry<Node, Integer> entry : m_replicationConnections.entrySet()) {
                sb.append(entry.getKey().m_hostId).append(",");
            }
            sb.append("]");
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

        public void createHost(String[] group, int hostId, int sitesPerHost) {
            createHost(group, 0, hostId, sitesPerHost);
        }

        private void createHost(String[] group, int i, int hostId, int sitesPerHost) {
            Group nextGroup = m_children.get(group[i]);
            if (nextGroup == null) {
                nextGroup = new Group();
                m_children.put(group[i], nextGroup);
            }

            if (group.length == i + 1) {
                nextGroup.m_hosts.add(new Node(hostId, sitesPerHost, group));
            } else {
                nextGroup.createHost(group, i + 1, hostId, sitesPerHost);
            }
        }

        /**
         * Get all nodes sorted in reverse distance to the given group.
         *
         * @return Lists of nodes in their groups. The list is ordered by the
         * reverse distance to the given group. So the first group in the list
         * is farthest away from the given group, and the last group in the list
         * is the given group itself.
         */
        public List<Deque<Node>> sortNodesByDistance(String[] group)
        {
            List<Deque<Node>> results = Lists.newArrayList();
            getGroupSiblingsOf(group, 0, results);
            if (group[0] != null) {
                getHosts(findGroup(group), results);
            }
            return results;
        }

        public List<Deque<Node>> getDescendentsInGroup(String[] group)
        {
            List<Deque<Node>> results = Lists.newArrayList();
            if (group[0] != null) {
                getHosts(findGroup(group), results);
            }
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

        private Group findGroup(String[] group) {
            Group found = this;
            for (String level : group) {
                found = found.m_children.get(level);
            }
            return found;
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

        private void addHost(Node node, int i) {
            Group nextGroup = m_children.get(node.m_group[i]);
            if (nextGroup == null) {
                nextGroup = new Group();
                m_children.put(node.m_group[i], nextGroup);
            }

            if (node.m_group.length == i + 1) {
                nextGroup.m_hosts.add(node);
            } else {
                nextGroup.addHost(node, i + 1);
            }
        }

        public void addHost(Node node) {
            addHost(node, 0);
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

        public Stream<Group> flattened() {
            return Stream.concat(Stream.of(this),
                                 m_children.values().stream().flatMap(Group::flattened));
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

        public PhysicalTopology(Map<Integer, String> hostGroups, int sitesPerHost) {
            for (Map.Entry<Integer, String> e : hostGroups.entrySet()) {
                m_root.createHost(parseGroup(e.getValue()), e.getKey(), sitesPerHost);
            }
        }

        /**
         * @return The total number of distinct groups.
         */
        public int groupCount () {
            return (int) m_root.flattened().filter(n -> n.m_children.isEmpty()).count();
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
        public List<Deque<Node>> getAllHosts(String[] group) {
            return m_root.sortNodesByDistance(group);
        }
    }

    private static class BuddyGroupConfiguration {
        Map<Integer, String> m_rackAwarenessGroup;
        Map<Integer, String> m_buddyGroup;
        Multimap<Integer, Long> m_partitionReplicas;
        Map<Integer, Long> m_partitionMasters;
        List<Partition> m_partitions;
        int m_sitesPerHost;

        public BuddyGroupConfiguration(
                Map<Integer, String> rackGroups,
                Map<Integer, String> buddyGroups,
                Multimap<Integer, Long> partitionReplicas,
                Map<Integer, Long> partitionMasters,
                List<Partition> partitions,
                int sitesPerHost)
        {
            m_rackAwarenessGroup = rackGroups;
            m_buddyGroup = buddyGroups;
            m_partitionReplicas = partitionReplicas;
            m_partitionMasters = partitionMasters;
            m_partitions = partitions;
            m_sitesPerHost = sitesPerHost;
        }
    }

    /*
     * Original placement strategy that doesn't get very good performance
     */
    JSONObject fallbackPlacementStrategy(
            List<Integer> hostIds,
            int hostCount,
            int partitionCount,
            int sitesPerHost) throws JSONException{
        // add all the sites
        int partitionCounter = -1;

        HashMap<Integer, ArrayList<Integer>> partToHosts =
            new HashMap<>();
        for (int i = 0; i < partitionCount; i++)
        {
            ArrayList<Integer> hosts = new ArrayList<>();
            partToHosts.put(i, hosts);
        }
        for (int i = 0; i < sitesPerHost * hostCount; i++) {

            // serially assign partitions to execution sites.
            int partition = (++partitionCounter) % partitionCount;
            int hostForSite = hostIds.get(i / sitesPerHost);
            partToHosts.get(partition).add(hostForSite);
        }

        // We need to sort the hostID lists for each partition so that
        // the leader assignment magic in the loop below will work.
        for (Map.Entry<Integer, ArrayList<Integer>> e : partToHosts.entrySet()) {
            Collections.sort(e.getValue());
        }

        JSONStringer stringer = new JSONStringer();
        stringer.object();
        stringer.key("hostcount").value(m_hostCount);
        stringer.key("kfactor").value(getReplicationFactor());
        stringer.key("sites_per_host").value(sitesPerHost);
        stringer.key("partitions").array();
        for (int part = 0; part < partitionCount; part++)
        {
            stringer.object();
            stringer.key("partition_id").value(part);
            // This two-line magic deterministically spreads the partition leaders
            // evenly across the cluster at startup.
            int index = part % (getReplicationFactor() + 1);
            int master = partToHosts.get(part).get(index);
            stringer.key("master").value(master);
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
     * Buddy groups can be transparent to user, for any given cluster
     * it can be divided into N buddies ( N = hostCount / (K + 1) ),
     * each buddy contains a subset of master partitions and their replicas
     * proportion to the size of buddy group.
     *
     * However user can overrides this by providing buddy group tag in command line
     */
    private List<BuddyGroupConfiguration> assignNodeToBuddyGroup(
            Map<Integer, String> rackAwareGroups,
            Map<Integer, String> buddyGroups,
            Multimap<Integer, Long> partitionReplicas,
            Map<Integer, Long> partitionMasters,
            int hostCount,
            int sitesPerHost,
            int partitionCount)
    {
        Map<String, Integer> sizingPlan = Maps.newTreeMap();
        // see what's the best sizing of buddy group
        int optimalBuddyGroups = hostCount / (m_replicationFactor + 1);
        int remainder = hostCount % optimalBuddyGroups;
        for (int groupId = 0; groupId < optimalBuddyGroups; groupId++) {
            int size = (hostCount / optimalBuddyGroups) + (groupId < remainder ? 1 : 0);
            sizingPlan.put(String.valueOf(groupId), size);
        }

        Map<String, Set<Integer>> buddyGroupToHostIds = Maps.newTreeMap();
        // buddy these nodes up based on rack aware topology
        boolean singleRackGroup = rackAwareGroups.values().stream().distinct().count() == 1;
        if (singleRackGroup) {
            // Single rack group, round-robin nodes into buddy group
            Iterator<String> groupIter = sizingPlan.keySet().iterator();
            for (Map.Entry<Integer, String> e : buddyGroups.entrySet()) {
                if (!groupIter.hasNext()) {
                    groupIter = sizingPlan.keySet().iterator();
                }
                String groupTag = groupIter.next();
                buddyGroupToHostIds.putIfAbsent(groupTag, Sets.newTreeSet());
                Set<Integer> hostIds = buddyGroupToHostIds.get(groupTag);
                hostIds.add(e.getKey());
            }
        } else {
            // Multiple rack groups, make sure nodes within a buddy group
            // don't belong to same rack group
            final PhysicalTopology phys = new PhysicalTopology(rackAwareGroups, sitesPerHost);
            List<Node> allNodes = Lists.newArrayList();
            List<Deque<Node>> deques = phys.getAllHosts(new String[]{null});
            deques.stream().forEach(n -> allNodes.addAll(n));
            Iterator<String> groupIter = sizingPlan.keySet().iterator();
            for (Node n : allNodes) {
                if (!groupIter.hasNext()) {
                    groupIter = sizingPlan.keySet().iterator();
                }
                String groupTag = groupIter.next();
                buddyGroupToHostIds.putIfAbsent(groupTag, Sets.newTreeSet());
                Set<Integer> hostIds = buddyGroupToHostIds.get(groupTag);
                hostIds.add(n.m_hostId);
            }
        }

        // Additional minimum nodes check
        boolean meetNodesRequirement = buddyGroupToHostIds.values().stream().allMatch(
                n -> n.size() >= (m_replicationFactor + 1)
                );
        if (!meetNodesRequirement) {
            hostLog.warn("Current grouping cannot meet the minimum buddy nodes requirement."
                    + " Try to reduce the number of buddy groups.");
        }

        // assign partitions proportion to the size of buddy group
        List<BuddyGroupConfiguration> configs = Lists.newArrayList();
        int start = 0;
        for (Entry<String, Set<Integer>> e : buddyGroupToHostIds.entrySet()) {
            int size = sizingPlan.get(e.getKey());
            List<Partition> partitions = new ArrayList<>();
            Set<Integer> partitionIds = Sets.newHashSet();
            int end = start + (partitionCount * size) / hostCount;
            for (int partitionId = start; partitionId < end; partitionId++) {
                partitions.add(new Partition(partitionId, getReplicationFactor() + 1));
                partitionIds.add(partitionId);
            }
            Map<Integer, String> subRAGroup = Maps.newHashMap();
            Map<Integer, String> subBuddyGroup = Maps.newHashMap();
            for (Integer hostId : e.getValue()) {
                subRAGroup.put(hostId, rackAwareGroups.get(hostId));
                subBuddyGroup.put(hostId, e.getKey());
            }

            configs.add(new BuddyGroupConfiguration(subRAGroup,
                    subBuddyGroup, partitionReplicas,
                    partitionMasters, partitions, sitesPerHost));
            start = end;
        }

        return configs;
    }

    /**
     * In rejoin process, rejoined node will add back to the first found buddy
     * group that missing node(s) and all members not belong to same rack group
     * (if more than one).
     *
     * WARNING! Rejoined node is not guaranteed to add back to its original buddy
     * group.
     */
    private List<BuddyGroupConfiguration> rejoinNodeToBuddyGroup(
            Map<Integer, String> rackAwareGroups,
            Map<Integer, String> buddyGroups,
            Multimap<Integer, Long> partitionReplicas,
            Map<Integer, Long> partitionMasters,
            List<Integer> partitionsToAdd,
            int hostCount,
            int partitionCount,
            int sitesPerHost)
    {
        // Find rejoin node
        Set<Integer> liveHostIds = buddyGroups.keySet();
        Set<Integer> nonRejoinHostIds = Sets.newHashSet();
        Multimap<Long, Integer> HSIdToReplicas = HashMultimap.create();
        Multimaps.invertFrom(partitionReplicas, HSIdToReplicas);
        HSIdToReplicas.keySet().stream().forEach(
                HSId -> nonRejoinHostIds.add(CoreUtils.getHostIdFromHSId(HSId)));
        Set<Integer> rejoinHostIds = Sets.difference(liveHostIds, nonRejoinHostIds);

        // Find the current buddy topology (exclude the rejoined node)
        Map<String, Set<Integer>> buddyGroupToHostIds = Maps.newHashMap();
        for (Map.Entry<Integer, String> e : buddyGroups.entrySet()) {
            if (nonRejoinHostIds.contains(e.getKey())) {
                buddyGroupToHostIds.putIfAbsent(e.getValue(), Sets.newTreeSet());
                Set<Integer> hostIds = buddyGroupToHostIds.get(e.getValue());
                hostIds.add(e.getKey());
            }
        }

        if (!partitionsToAdd.isEmpty()) {
            //elasticJoin
            String newTag = String.valueOf(buddyGroupToHostIds.size());
            buddyGroupToHostIds.put(newTag, Sets.newTreeSet());
            Set<Integer> hostIds = buddyGroupToHostIds.get(newTag);
            hostIds.addAll(rejoinHostIds);
        } else {
            // Invert the rack aware group
            Map<String, Set<Integer>> rackGroupToHostIds = Maps.newHashMap();
            for (Entry<Integer, String> e : rackAwareGroups.entrySet()) {
                rackGroupToHostIds.putIfAbsent(e.getValue(), Sets.newTreeSet());
                Set<Integer> hostIds = rackGroupToHostIds.get(e.getValue());
                hostIds.add(e.getKey());
            }
            // Add rejoined node to the first candidate group so that they
            // doesn't belong to same rack group (if there are more than two rack groups)
            boolean found = false;
            for (Integer rejoinHostId : rejoinHostIds) {
                String rackGroup = rackAwareGroups.get(rejoinHostId);
                Set<Integer> members = rackGroupToHostIds.get(rackGroup);
                // prefer the group with least nodes
                for (Set<Integer> candidate : sortBuddyGroupsBySize(buddyGroupToHostIds)) {
                    if (rackGroupToHostIds.size() == 1 || !members.containsAll(candidate)) {
                        candidate.add(rejoinHostId);
                        found = true;
                        break;
                    }
                }
                if (!found) {
                    throw new RuntimeException("Failed to rejoin the node " + rejoinHostId + " due to the provided placement groups");
                }
            }
        }
        // Assign partitions based on current buddy topology
        List<BuddyGroupConfiguration> configs = Lists.newArrayList();
        for (Entry<String, Set<Integer>> e : buddyGroupToHostIds.entrySet()) {
            Set<Integer> buddyHostIds = e.getValue();
            Map<Integer, String> subRAGroup = Maps.newHashMap();
            Map<Integer, String> subBuddyGroup = Maps.newHashMap();
            for (Integer hostId : buddyHostIds) {
                subRAGroup.put(hostId, rackAwareGroups.get(hostId));
                subBuddyGroup.put(hostId, e.getKey());
            }
            Multimap<Integer, Long> subPartitionReps =
                    Multimaps.filterValues(partitionReplicas,
                            HSId -> buddyHostIds.contains(CoreUtils.getHostIdFromHSId(HSId)));
            Map<Integer, Long> subPartitionMaster =
                    Maps.filterValues(partitionMasters,
                            HSId -> buddyHostIds.contains(CoreUtils.getHostIdFromHSId(HSId)));
            List<Partition> subPartitions = Lists.newArrayList();
            if (subPartitionMaster.isEmpty()) {
                //elasticJoin
                partitionsToAdd.forEach(
                        k -> subPartitions.add(new Partition(k, getReplicationFactor() + 1)));
            } else {
                subPartitionMaster.forEach(
                        (k, v) -> subPartitions.add(new Partition(k, getReplicationFactor() + 1)));
            }
            configs.add(new BuddyGroupConfiguration(subRAGroup, subBuddyGroup, subPartitionReps,
                    subPartitionMaster, subPartitions, sitesPerHost));
        }
        return configs;
    }

    private List<Set<Integer>> sortBuddyGroupsBySize(Map<String, Set<Integer>> buddyGroupToHostIds)
    {
        List<Set<Integer>> buddyGroupSize = Lists.newArrayList();
        buddyGroupToHostIds.forEach((k, v) -> buddyGroupSize.add(v));
        Collections.sort(buddyGroupSize, new Comparator<Set<Integer>>() {

            @Override
            public int compare(Set<Integer> o1, Set<Integer> o2) {
                return Integer.compare(o1.size(), o2.size());
            }

        });
        return buddyGroupSize;
    }

    /**
     * Buddy represents a subset of nodes in a K-safety cluster that meets
     * K-safety constraint by itself, meaning for every partition within a
     * buddy group, its members have all the copies of this partition.
     *
     * Get the buddy group topology upon the given rack awareness topology
     * (could be only one rack group). For any given node, groups this node
     * with other nodes that are as far as possible
     */
    private List<BuddyGroupConfiguration> getBuddyGroupTopology(
            Map<Integer, ExtensibleGroupTag> hostGroups,
            Multimap<Integer, Long> partitionReplicas,
            Map<Integer, Long> partitionMasters,
            List<Integer> partitionsToAdd,
            int hostCount,
            int partitionCount,
            int sitesPerHost) throws JSONException {
        Map<Integer, String> rackAwareGroups = Maps.newHashMap();
        Map<Integer, String> buddyGroups = Maps.newHashMap();
        for (Map.Entry<Integer, ExtensibleGroupTag> e : hostGroups.entrySet()) {
            rackAwareGroups.put(e.getKey(), e.getValue().m_rackAwarenessGroup);
            buddyGroups.put(e.getKey(), e.getValue().m_buddyGroup);
        }
        if (!partitionReplicas.isEmpty()) {
            return rejoinNodeToBuddyGroup(rackAwareGroups,
                    buddyGroups, partitionReplicas, partitionMasters,
                    partitionsToAdd, hostCount, partitionCount, sitesPerHost);
        } else {
            return assignNodeToBuddyGroup(rackAwareGroups,
                    buddyGroups, partitionReplicas, partitionMasters,
                    hostCount, sitesPerHost, partitionCount);
        }
    }

    private void printClusterTopology(final List<Node> allNodes) {
        hostLog.info("Buddy Group: ");
        for (Node n : allNodes) {
            StringBuilder groups = new StringBuilder();
            for (int ii = 0; ii < n.m_group.length; ii++) {
                groups.append(n.m_group[ii]);
                if (ii != (n.m_group.length - 1)) {
                    groups.append(".");
                }
            }
            hostLog.info(String.format("%s(rack group:%s)", n.toString(), groups.toString()));
        }
    }

    private void assignPartitions(BuddyGroupConfiguration config) {
        int sitesPerHost = config.m_sitesPerHost;
        List<Partition> partitions = config.m_partitions;
        Multimap<Integer, Long> partitionReplicas = config.m_partitionReplicas;
        Map<Integer, Long> partitionMasters = config.m_partitionMasters;

        final PhysicalTopology phys = new PhysicalTopology(config.m_rackAwarenessGroup, sitesPerHost);
        final List<Node> allNodes = MiscUtils.zip(phys.getAllHosts(new String[]{null}));
        final Map<Integer, Node> hostIdToNode = toHostIdNodeMap(allNodes);

        // Step 1. Distribute mastership by round-robining across all the
        // nodes. This balances the masters among the nodes.
        Iterator<Node> iter = allNodes.iterator();
        for (Partition p : partitions) {
            if (!iter.hasNext()) {
                iter = allNodes.iterator();
                assert iter.hasNext();
            }

            final Long hsId = partitionMasters.get(p.m_partitionId);
            if (hsId != null) {
                p.m_master = hostIdToNode.get(CoreUtils.getHostIdFromHSId(hsId));
            } else {
                p.m_master = iter.next();
            }
            p.m_master.m_masterPartitions.add(p);
            p.decrementNeededReplicas();
        }

        // If there is any existing partition replicas, assign them first.
        // e.g. rejoin would have existing nodes.
        for (Map.Entry<Integer, Long> e : partitionReplicas.entries()) {
            for (Partition p : partitions) {
                if (p.m_partitionId == e.getKey()) {
                    assignReplica(sitesPerHost, phys, p,
                              hostIdToNode.get(CoreUtils.getHostIdFromHSId(e.getValue())));
                }
            }
        }

        if (getReplicationFactor() > 0) {
            Map<Integer, List<Node>> sortedCandidatesForPartitions = new HashMap<>();
            for (Partition p : partitions) {
                sortedCandidatesForPartitions.put(p.m_partitionId,
                                                  MiscUtils.zip(sortByConnectionsToNode(p.m_master,
                                                                                        phys.m_root.sortNodesByDistance(p.m_master.m_group))));
            }

            if (!partitionMasters.isEmpty()) {
                partitions = sortPartitions(phys, sitesPerHost, allNodes, partitions);
                if (partitions == null) {
                    printClusterTopology(allNodes);
                    // all replicas must have be assigned already
                    return;
                }
            }

            // Step 2. For each partition, assign a replica to each group other
            // than the group of the partition master. This recursively goes
            // through permutations to try to find a feasible assignment for all
            // partitions. For large deployments, it may take a while.
            if (!recursivelyAssignReplicas(!partitionMasters.isEmpty(),
                                           phys.groupCount(),
                                           sitesPerHost,
                                           phys,
                                           partitions,
                                           sortedCandidatesForPartitions,
                                           0)) {
                throw new RuntimeException("Unable to find feasible partition replica assignment for the specified grouping");
            }

        }

        // Sanity check to make sure each node has enough partitions and each
        // partition has enough replicas.
        for (Node n : allNodes) {
            if (n.partitionCount() != sitesPerHost) {
                throw new RuntimeException("Some nodes are missing partition replicas: " + allNodes);
            }
        }
        for (Partition p : partitions) {
            if (p.m_neededReplicas != 0 && partitionReplicas.isEmpty() && partitionMasters.isEmpty()) {
                throw new RuntimeException("Some partitions are missing replicas: " + partitions);
            }
        }
        printClusterTopology(allNodes);
    }

    /**
     * Placement strategy that attempts to distribute replicas across different
     * groups and also involve multiple nodes in replication so that the socket
     * between nodes is not a bottleneck.
     *
     * This algorithm has two steps.
     * 1. Partition master assignment,
     * 2. Group partition replica assignment.
     * @throws InterruptedException
     * @throws KeeperException
     */
    JSONObject rackAwarePlacement(
            Map<Integer, ExtensibleGroupTag> hostGroups,
            Multimap<Integer, Long> partitionReplicas,
            Map<Integer, Long> partitionMasters,
            List<Integer> partitionsToAdd,
            int hostCount,
            int partitionCount,
            int sitesPerHost) throws JSONException, KeeperException, InterruptedException {

        List<Partition> partitions = new ArrayList<>();
        List<BuddyGroupConfiguration> buddyConfigs = getBuddyGroupTopology(
                hostGroups, partitionReplicas, partitionMasters,
                partitionsToAdd, hostCount, partitionCount,
                sitesPerHost);
        for (BuddyGroupConfiguration config : buddyConfigs) {
            assignPartitions(config);
            partitions.addAll(config.m_partitions);
        }

        JSONStringer stringer = new JSONStringer();
        stringer.object();
        stringer.key("hostcount").value(m_hostCount);
        stringer.key("kfactor").value(getReplicationFactor());
        stringer.key("sites_per_host").value(sitesPerHost);
        stringer.key("partitions").array();
        for (Partition p : partitions)
        {
            stringer.object();
            stringer.key("partition_id").value(p.m_partitionId);
            stringer.key("master").value(p.m_master.m_hostId);
            stringer.key("replicas").array();
            for (Node n : p.m_replicas) {
                stringer.value(n.m_hostId);
            }
            stringer.value(p.m_master.m_hostId);
            stringer.endArray();
            stringer.endObject();
        }
        stringer.endArray();
        stringer.endObject();

        // Rewrite buddy topology into host groups
        for (BuddyGroupConfiguration config : buddyConfigs) {
            for (Map.Entry<Integer, String> e : config.m_buddyGroup.entrySet()) {
                if (hostGroups.containsKey(e.getKey())) {
                    hostGroups.compute(e.getKey(),
                            (k, v) -> new ExtensibleGroupTag(v.m_rackAwarenessGroup, e.getValue()));
                }
            }
        }

        return new JSONObject(stringer.toString());
    }

    /**
     * Each partition should have at least one copy at different group
     */
    public static boolean checkRackAwarenessConstraint(
            List<Partition> partitions,
            List<Deque<Node>> nodesPerGroup)
    {
        if (nodesPerGroup.size() > 1) {
            for (Partition p : partitions) {
                Set<Node> partitionNodes = Sets.newHashSet();
                partitionNodes.add(p.m_master);
                partitionNodes.addAll(p.m_replicas);
                for (Deque<Node> deque : nodesPerGroup) {
                    if (deque.containsAll(partitionNodes)) {
                        return false;
                    }
                }
            }
        }
        return true;
    }

    /**
     * Each partition should has K different replica
     */
    public static boolean checkKSafetyConstraint(
            List<Partition> partitions,
            int allowedCopies)
    {
        return partitions.stream().allMatch(n -> n.m_replicas.size() == allowedCopies);
    }

    private static List<Partition> sortPartitions(PhysicalTopology phys, int sitesPerHost,
                                                  Collection<Node> allNodes, List<Partition> partitions)
    {
        Node rejoinNode = null;
        // Find the rejoining node first, assumes one rejoin node
        for (Node n : allNodes) {
            if (n.partitionCount() == 0) {
                rejoinNode = n;
                break;
            } else {
                assert n.partitionCount() == sitesPerHost;
            }
        }

        if (rejoinNode == null) {
            return null;
        }

        HashMap<Partition, Integer> partitionRepCount = new HashMap<>();
        final List<Deque<Node>> siblingsWithSelf = phys.m_root.getDescendentsInGroup(new String[]{rejoinNode.m_group[0]});

        // Prime the partition replication count map with all partitions set to 0
        partitions.forEach(p -> partitionRepCount.put(p, 0));

        // Count how many replicas each partition has in the same top-level group
        for (Deque<Node> nodes : siblingsWithSelf) {
            for (Node n : nodes) {
                for (Partition p : n.m_masterPartitions) {
                    partitionRepCount.compute(p, (k, v) -> v + 1);
                }
                for (Partition p : n.m_replicaPartitions) {
                    partitionRepCount.compute(p, (k, v) -> v + 1);
                }
            }
        }

        // Sort candidate partitions in replica count order
        Multimap<Integer, Partition> inverse = Multimaps.invertFrom(Multimaps.forMap(partitionRepCount),
                                                                  TreeMultimap.create());
        return new ArrayList<>(inverse.values());
    }

    /**
     * For each partition that needs more replicas, find a feasible candidate
     * node and assign it. Recursively call this function until all partitions
     * have enough replicas. If there is no feasible assignment, back up one
     * step and try a different candidate node.
     * @return true if a feasible global assignment has been found, false otherwise.
     */
    private static boolean recursivelyAssignReplicas(boolean isRejoin,
                                                     int groupCount,
                                                     int sitesPerHost,
                                                     PhysicalTopology phys,
                                                     List<Partition> partitions,
                                                     Map<Integer, List<Node>> candidates,
                                                     int level)
    {
        StringBuilder indent = null;
        if (hostLog.isDebugEnabled()) {
            indent = new StringBuilder();
            for (int i = 0; i < level; i++) {
                indent.append("    ");
            }
        }

        for (Partition p : partitions) {
            if (p.m_neededReplicas == 0) {
                continue;
            }
            Collection<Node> bestNodes = pickBestCandidates(sitesPerHost, groupCount, p, candidates.get(p.m_partitionId));
            for (Node candidate : bestNodes) {
                assignReplica(sitesPerHost, phys, p, candidate);

                if (hostLog.isDebugEnabled()) {
                    StringBuilder sb = new StringBuilder();
                    for (Node n : bestNodes) {
                        sb.append("H" + n.m_hostId). append(" ");
                    }
                    hostLog.debug(String.format("%sP%d: (H%d)%s",
                            indent.toString(), p.m_partitionId,
                            candidate.m_hostId, sb.toString()));
                }
                if (recursivelyAssignReplicas(isRejoin, groupCount, sitesPerHost, phys, partitions, candidates, level + 1)) {
                    break;
                } else {
                    // No feasible assignment with this candidate, try a different one.
                    removeReplica(sitesPerHost, phys, p, candidate);
                }
            }

            // If we can't find enough nodes in all the candidates to satisfy
            // this partition, there's no feasible assignment for this partition
            // with the current configuration, back up and try a different
            // configuration.
            if (!isRejoin && p.m_neededReplicas > 0) {
                return false;
            }
        }

        return true;
    }

    private static Collection<Node> pickBestCandidates(int sitesPerHost, int groupCount, Partition p, List<Node> candidates) {
        List<Node> bestCandidates = new ArrayList<>();
        List<Node> qualifiedCandidates = new ArrayList<>();

        for (Node candidate : candidates) {
            // The candidate has to satisfy the following,
            // - have available sites
            // - doesn't already contain the partition
            // - in a different group if there are more than one group in total and there's no replicas yet
            if (candidate.partitionCount() == sitesPerHost ||
                candidate.contains(p) ||
                (groupCount > 1 && Arrays.equals(candidate.m_group, p.m_master.m_group) && p.m_replicas.isEmpty())) {
                continue;
            }

            qualifiedCandidates.add(candidate);

            // Soft requirements
            // - If more than one group and there are replicas, pick a candidate in a group different from replicas'
            if (groupCount == 1 ||
                (!Arrays.equals(candidate.m_group, p.m_master.m_group) && p.m_replicas.stream().noneMatch(n -> Arrays.equals(candidate.m_group, n.m_group)))) {
                bestCandidates.add(candidate);
            }
        }

        return bestCandidates.isEmpty() ? qualifiedCandidates : bestCandidates;
    }

    private static Map<Integer, Node> toHostIdNodeMap(Collection<Node> nodes) {
        Map<Integer, Node> nodeMap = new HashMap<>();
        for (Node n : nodes) {
            Preconditions.checkArgument(nodeMap.put(n.m_hostId, n) == null);
        }
        return nodeMap;
    }

    private static void assignReplica(int sitesPerHost, PhysicalTopology phys, Partition p, Node replica)
    {
        if (replica.partitionCount() == sitesPerHost) {
            phys.m_root.removeHost(replica);
            return;
        }
        if (p.m_master == replica || p.m_replicas.contains(replica)) {
            return;
        }

        p.m_replicas.add(replica);
        p.decrementNeededReplicas();
        replica.m_replicaPartitions.add(p);

        p.m_master.m_replicationConnections.put(replica, p.m_partitionId);
        replica.m_replicationConnections.put(p.m_master, p.m_partitionId);
    }

    private static void removeReplica(int sitesPerHost, PhysicalTopology phys, Partition p, Node replica)
    {
        if (p.m_master == replica || !p.m_replicas.contains(replica)) {
            return;
        }

        replica.m_replicationConnections.remove(p.m_master, p.m_partitionId);
        p.m_master.m_replicationConnections.remove(replica, p.m_partitionId);
        replica.m_replicaPartitions.remove(p);
        p.m_replicas.remove(replica);
        p.incrementNeededReplicas();

        if (replica.partitionCount() < sitesPerHost) {
            phys.m_root.addHost(replica);
        }
    }

    /**
     * Sort the given groups of nodes based on their connections to the master
     * node, their replication factors, and their master partition counts, in
     * that order. The sorting does not change the grouping of the nodes. It
     * favors nodes with less connections to the master node. If the connection
     * count is the same, it favors nodes with fewer replications. If the
     * replication count is the same, it favors nodes with less master
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
                            return Integer.compare(o1.m_masterPartitions.size(), o2.m_masterPartitions.size());
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

    /**
     * There are two kinds of group, and they can be overlapped with each other
     * Rack-Aware : round-robining master partitions and placing their replicas to racks other than
     *              the rack where master resides as far as possible.
     *              (distance is defined as length between two nodes in the rack group tree hierarchy)
     *              If this layout is possible, then we may lose up to K(factor) racks without crashing
     *              the cluster.
     * Buddy Group: divides cluster into N buddies ( N = hostCount / (K + 1) ), each buddy contains
     *              a subset of master partitions and their replicas. Each buddy can lose up to K(factor)
     *              node(s) without crashing the cluster.
     *              When N = 1, it generates the same layout as rack-aware algorithm with only 1 rack.
     */
    JSONObject groupAwarePlacementStrategy(
            Map<Integer, ExtensibleGroupTag> hostGroups,
            Multimap<Integer, Long> partitionReplicas,
            Map<Integer, Long> partitionMasters,
            List<Integer> partitionsToAdd,
            int hostCount,
            int partitionCount,
            int sitesPerHost ) throws JSONException {
        JSONObject topo;
        try {
            topo = rackAwarePlacement(hostGroups, partitionReplicas,
                                      partitionMasters, partitionsToAdd,
                                      hostCount, partitionCount, sitesPerHost);
        } catch (Exception e) {
            e.printStackTrace();
            hostLog.error("Unable to use optimal replica placement strategy. " +
                    "Falling back to a less optimal strategy that may result in worse performance. " +
                    "Original error was " + e.getMessage());
            topo = fallbackPlacementStrategy(Lists.newArrayList(hostGroups.keySet()),
                    hostCount, partitionCount, sitesPerHost);
        }
        return topo;
    }

    // Statically build a topology. This only runs at startup;
    // rejoin clones this from an existing server.
    public JSONObject getTopology(Map<Integer, ExtensibleGroupTag> hostGroups,
                                  Multimap<Integer, Long> partitionReplicas,
                                  Map<Integer, Long> partitionMasters,
                                  List<Integer> partitionsToAdd) throws JSONException
    {
        int hostCount = getHostCount();
        int partitionCount = partitionMasters.isEmpty() ? getPartitionCount() : partitionMasters.size();
        if (!partitionsToAdd.isEmpty()) {
            partitionCount += partitionsToAdd.size();
        }
        int sitesPerHost = getSitesPerHost();

        if (hostCount != hostGroups.size() && partitionReplicas.isEmpty() && partitionMasters.isEmpty()) {
            throw new RuntimeException("Provided " + hostGroups.size() + " host ids when host count is " + hostCount);
        }

        JSONObject topo;
        if (Boolean.valueOf(System.getenv("VOLT_REPLICA_FALLBACK"))) {
            topo = fallbackPlacementStrategy(Lists.newArrayList(hostGroups.keySet()),
                                             hostCount, partitionCount, sitesPerHost);
        } else {
            topo = groupAwarePlacementStrategy(hostGroups, partitionReplicas,
                    partitionMasters, partitionsToAdd, hostCount, partitionCount, sitesPerHost);
        }

        if (hostLog.isDebugEnabled()) {
            hostLog.debug("TOPO: " + topo.toString(2));
        }
        return topo;
    }

    private final int m_hostCount;
    private final int m_sitesPerHost;
    private final int m_replicationFactor;

    private String m_errorMsg;
}
