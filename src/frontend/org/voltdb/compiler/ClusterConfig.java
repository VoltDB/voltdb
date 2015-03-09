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
package org.voltdb.compiler;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.json_voltpatches.JSONArray;
import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;
import org.json_voltpatches.JSONStringer;
import org.voltcore.logging.VoltLogger;
import org.voltdb.VoltDB;

import com.google_voltpatches.common.collect.Multimap;

public class ClusterConfig
{
    private static final VoltLogger hostLog = new VoltLogger("HOST");

    public static List<Integer> partitionsForHost(JSONObject topo, int hostId) throws JSONException
    {
        List<Integer> partitions = new ArrayList<Integer>();

        JSONArray parts = topo.getJSONArray("partitions");

        for (int p = 0; p < parts.length(); p++) {
            // have an object in the partitions array
            JSONObject aPartition = parts.getJSONObject(p);
            int pid = aPartition.getInt("partition_id");
            JSONArray replicas = aPartition.getJSONArray("replicas");
            for (int h = 0; h < replicas.length(); h++)
            {
                int replica = replicas.getInt(h);
                if (replica == hostId)
                {
                    partitions.add(pid);
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
                m_errorMsg = String.format("You can only add %d servers at a time for k=&d",
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

        boolean needsReplicas() {
            return m_neededReplicas > 0;
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

        public boolean canUseAsReplica(Node n) {
            return needsReplicas() && m_master != n && !m_replicas.contains(n);
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

    private static class Node {
        Set<Partition> m_masterPartitions = new HashSet<Partition>();
        Set<Partition> m_replicaPartitions = new HashSet<Partition>();
        Map<Node, Set<Partition>> m_replicationConnections = new HashMap<Node, Set<Partition>>();
        Integer m_hostId;

        public Node(Integer hostId) {
            m_hostId = hostId;
        }

        int partitionCount() {
            return m_masterPartitions.size() + m_replicaPartitions.size();
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
            for (Map.Entry<Node, Set<Partition>> entry : m_replicationConnections.entrySet()) {
                sb.append(" host " + entry.getKey().m_hostId + " for partitions ");
                for (Partition p : entry.getValue()) {
                    sb.append(p.m_partitionId).append(", ");
                }
                sb.append(";");
            }
            return sb.toString();
        }
    }

    /*
     * Are there any partitions that are not fully replicated
     */
    private static boolean needReplication(List<Partition> partitions) {
        for (Partition p : partitions) {
            if (p.needsReplicas()) {
                return true;
            }
        }
        return false;
    }

    /*
     * Find the node that can take more replicas,
     * has the least number of replication connections,
     * and where the number of replication connections are equal,
     * where the number of replicated partitions is smallest
     */
    Node nextNotFullNode(List<Node> nodes, int sitesPerNode) {
        ArrayList<Node> notFullList = new ArrayList<Node>();
        for (Node n : nodes) {
            if (n.partitionCount() < sitesPerNode) {
                notFullList.add(n);
            }
        }

        Node leastConnectedNode = null;
        for (Node n : notFullList) {
            if (leastConnectedNode == null) {
                leastConnectedNode = n;
                continue;
            }

            /*
             * Pick the one with the fewest connections, and for those that have the same number
             * of connections, pick the one that is replicating the fewest partitions
             */
            if (n.m_replicationConnections.size() <= leastConnectedNode.m_replicationConnections.size()) {
                int sumA = 0;
                for (Set<Partition> replicas : n.m_replicationConnections.values()) {
                    sumA += replicas.size();
                }
                int sumB = 0;
                for (Set<Partition> replicas : leastConnectedNode.m_replicationConnections.values()) {
                    sumB += replicas.size();
                }
                if (sumA < sumB) {
                    leastConnectedNode = n;
                }
            }
        }
        return leastConnectedNode;
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
            new HashMap<Integer, ArrayList<Integer>>();
        for (int i = 0; i < partitionCount; i++)
        {
            ArrayList<Integer> hosts = new ArrayList<Integer>();
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

    /*
     * Placement strategy that attempts to involve multiple nodes in replication
     * so that the socket between nodes is not a bottleneck.
     */
    JSONObject newPlacementStrategy(
            List<Integer> hostIds,
            int hostCount,
            int partitionCount,
            int sitesPerHost) throws JSONException {
        Collections.sort(hostIds);
        List<Partition> partitions = new ArrayList<Partition>();
        for (int ii = 0; ii < partitionCount; ii++) {
            partitions.add(new Partition(ii, getReplicationFactor() + 1));
        }

        List<Node> nodes = new ArrayList<Node>();
        for (Integer hostId : hostIds) {
            nodes.add(new Node(hostId));
        }

        /*
         * Distribute master ship
         */
        for(int ii = 0; ii < partitions.size(); ii++) {
            Partition p = partitions.get(ii);
            Node n = nodes.get(ii % hostCount);
            p.m_master = n;
            p.decrementNeededReplicas();
            n.m_masterPartitions.add(p);
        }

        while (needReplication(partitions)) {
            Node n = nextNotFullNode(nodes, sitesPerHost);

            //Find a partition that will increase number of hosts inter-replicating
            boolean foundUsefulPartition = false;
            Partition partitionToUse = null;
            for (Partition p : partitions) {
                if (p.canUseAsReplica(n)) {
                    if (!p.m_master.m_replicationConnections.containsKey(n)) {
                        foundUsefulPartition = true;
                        partitionToUse = p;
                    }
                }
            }

            if (!foundUsefulPartition) {
                //Fall back to finding any old thing to replicate
                for (Partition p : partitions) {
                    if (p.canUseAsReplica(n)) {
                        partitionToUse = p;
                        break;
                    }
                }
                Set<Partition> replicatedPartitions = partitionToUse.m_master.m_replicationConnections.get(n);
                replicatedPartitions.add(partitionToUse);
                partitionToUse.m_replicas.add(n);
                n.m_replicaPartitions.add(partitionToUse);
                partitionToUse.decrementNeededReplicas();
            } else {
                //Connect the partition, node, and master together
                Set<Partition> replicatedPartitions = new HashSet<Partition>();
                replicatedPartitions.add(partitionToUse);
                partitionToUse.m_master.m_replicationConnections.put(n, replicatedPartitions);
                n.m_replicationConnections.put(partitionToUse.m_master, replicatedPartitions);
                n.m_replicaPartitions.add(partitionToUse);
                partitionToUse.decrementNeededReplicas();
                partitionToUse.m_replicas.add(n);
            }
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
            stringer.key("master").value(partitions.get(part).m_master.m_hostId);
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

        JSONObject topo = new JSONObject(stringer.toString());
        return topo;
    }

    // Statically build a topology. This only runs at startup;
    // rejoin clones this from an existing server.
    public JSONObject getTopology(List<Integer> hostIds) throws JSONException
    {
        int hostCount = getHostCount();
        int partitionCount = getPartitionCount();
        int sitesPerHost = getSitesPerHost();

        if (hostCount != hostIds.size()) {
            throw new RuntimeException("Provided " + hostIds.size() + " host ids when host count is " + hostCount);
        }

        boolean useFallbackStrategy = Boolean.valueOf(System.getenv("VOLT_REPLICA_FALLBACK"));
        if ((sitesPerHost * hostCount) % (getReplicationFactor() + 1) > 0) {
            VoltDB.crashGlobalVoltDB("The cluster has more hosts and sites per hosts than required for the " +
                    "requested k-safety value.  The number of total sites (sitesPerHost * hostCount) must be a " +
                    "whole multiple of the number of copies of the database (k-safety + 1)", false, null);
        }
        if (sitesPerHost * hostCount % partitionCount > 0 || partitionCount < hostCount) {
            hostLog.warn("Unable to use optimal replica placement strategy with this configuration. " +
                    " Falling back to a less optimal strategy that may result in worse performance. " +
                    " Try using an even number of sites per host.");
            useFallbackStrategy = true;
        }

        JSONObject topo = null;
        if (useFallbackStrategy) {
            topo = fallbackPlacementStrategy(hostIds, hostCount, partitionCount, sitesPerHost);
        } else {
            try {
                topo = newPlacementStrategy(hostIds, hostCount, partitionCount, sitesPerHost);
            } catch (Exception e) {
                hostLog.error("Unable to use optimal replica placement strategy. " +
                              "Falling back to a less optimal strategy that may result in worse performance");
                topo = fallbackPlacementStrategy(hostIds, hostCount, partitionCount, sitesPerHost);
            }
        }

        hostLog.debug("TOPO: " + topo.toString(2));
        return topo;
    }

    private final int m_hostCount;
    private final int m_sitesPerHost;
    private final int m_replicationFactor;

    private String m_errorMsg;
}
