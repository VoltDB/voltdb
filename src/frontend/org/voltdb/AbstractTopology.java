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

package org.voltdb;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.OptionalInt;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;

import org.apache.commons.lang3.ArrayUtils;
import org.json_voltpatches.JSONArray;
import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;
import org.json_voltpatches.JSONStringer;

import com.google_voltpatches.common.collect.ImmutableMap;
import com.google_voltpatches.common.collect.ImmutableSet;

public class AbstractTopology {

    public final long version;
    public final ImmutableMap<Integer, Host> hostsById;
    public final ImmutableMap<Integer, Partition> partitionsById;

    public static final AbstractTopology EMPTY_TOPOLOGY = new AbstractTopology(0, new TreeSet<>());

    /////////////////////////////////////
    //
    // PUBLIC CLASSES
    //
    /////////////////////////////////////

    public static class Partition {
        public final int id;
        public final int k;
        public final int leaderHostId;
        public final ImmutableSet<Integer> hostIds;

        private Partition(int id, int k, int leaderHostId, Collection<Integer> hostIds) {
            this.id = id;
            this.k = k;
            this.leaderHostId = leaderHostId;
            this.hostIds = ImmutableSet.copyOf(hostIds);
            assert(k >= 0);
        }

        @Override
        public String toString() {
            String[] hostIdStrings = hostIds.stream().map(id -> String.valueOf(id)).toArray(String[]::new);
            return String.format("Partition %d (leader %d, hosts %)", id, leaderHostId, String.join(",", hostIdStrings));
        }

        private void toJSON(JSONStringer stringer) throws JSONException {
            stringer.object();
            stringer.key("id").value(id);
            stringer.key("k").value(k);
            stringer.key("leaderHostId").value(leaderHostId);
            stringer.key("hostIds").array();
            for (Integer hostId : hostIds) {
                stringer.value(hostId);
            }
            stringer.endArray();
            stringer.endObject();
        }

        private static Partition fromJSON(JSONObject json) throws JSONException {
            int id = json.getInt("id");
            int k = json.getInt("k");
            int leaderHostId = json.getInt("leaderHostId");

            List<Integer> mutableHostIds = new ArrayList<>();
            JSONArray jsonHostIds = json.getJSONArray("hostIds");
            for (int i = 0; i < jsonHostIds.length(); i++) {
                mutableHostIds.add(jsonHostIds.getInt(i));
            }
            return new Partition(id, k, leaderHostId, mutableHostIds);
        }
    }

    public static class HAGroup {
        public final String token;
        public final ImmutableSet<Integer> hostIds;

        private HAGroup(String token, int[] hostIds) {
            this.token = token;
            Integer[] hostIdsInteger = ArrayUtils.toObject(hostIds);
            this.hostIds = ImmutableSet.copyOf(hostIdsInteger);
        }

        @Override
        public String toString() {
            String[] hostIdStrings = hostIds.stream().map(id -> id.toString()).toArray(String[]::new);
            return String.format("HAGroup %s (Hosts %s)", token, String.join(",", hostIdStrings));
        }

        private void toJSON(JSONStringer stringer) throws JSONException {
            stringer.object();
            stringer.key("token").value(token);
            stringer.key("hostIds").array();
            for (int hostId : hostIds) {
                stringer.value(hostId);
            }
            stringer.endArray();
            stringer.endObject();
        }

        private static HAGroup fromJSON(JSONObject json) throws JSONException {
            String token = json.getString("token");
            JSONArray jsonHosts = json.getJSONArray("hostIds");
            int[] hostIds = new int[jsonHosts.length()];
            for (int i = 0; i < jsonHosts.length(); i++) {
                hostIds[i] = jsonHosts.getInt(i);
            }
            return new HAGroup(token, hostIds);
        }
    }

    public static class Host {
        public final int id;
        public final int targetSiteCount;
        public final HAGroup haGroup;
        public final ImmutableSet<Partition> partitions;

        private Host(int id, int targetSiteCount, HAGroup haGroup, Collection<Partition> partitions) {
            assert(id >= 0);
            assert(targetSiteCount >= 0);
            assert(haGroup != null);
            assert(partitions != null);
            assert(partitions.size() >= 0);

            this.id = id;
            this.targetSiteCount = targetSiteCount;
            this.haGroup = haGroup;
            this.partitions = ImmutableSet.copyOf(partitions);
        }

        public List<Integer> getSortedPartitionIdList() {
            return partitions.stream()
                    .map(p -> p.id)
                    .sorted()
                    .collect(Collectors.toList());
        }

        @Override
        public String toString() {
            String[] partitionIdStrings = partitions.stream().map(p -> String.valueOf(p.id)).toArray(String[]::new);
            return String.format("Host %d tsph:%d ha:%s (Partitions %s)",
                    id, targetSiteCount, haGroup.token, String.join(",", partitionIdStrings));
        }

        private void toJSON(JSONStringer stringer) throws JSONException {
            stringer.object();
            stringer.key("id").value(id);
            stringer.key("targetSiteCount").value(targetSiteCount);
            stringer.key("haGroup").value(haGroup.token);
            stringer.key("partitions").array();
            for (Partition partition : partitions) {
                stringer.value(partition.id);
            }
            stringer.endArray();
            stringer.endObject();
        }

        private static Host fromJSON(
                JSONObject json,
                final Map<String, HAGroup> haGroupsByToken,
                final Map<Integer, Partition> partitionsById)
                        throws JSONException
        {
            int id = json.getInt("id");
            int targetSiteCount = json.getInt("targetSiteCount");
            String haGroupToken = json.getString("haGroup");
            HAGroup haGroup = haGroupsByToken.get(haGroupToken);
            JSONArray jsonPartitions = json.getJSONArray("partitions");
            ArrayList<Partition> partitions = new ArrayList<>();
            for (int i = 0; i < jsonPartitions.length(); i++) {
                int partitionId = jsonPartitions.getInt(i);
                partitions.add(partitionsById.get(partitionId));
            }
            return new Host(id, targetSiteCount, haGroup, partitions);
        }
    }

    public static class KSafetyViolationException extends Exception {
        private static final long serialVersionUID = 1L;
        public final int failedHostId;
        public final ImmutableSet<Integer> missingPartitionIds;

        public KSafetyViolationException(int failedHostId, Set<Integer> missingPartitionIds) {
            assert(missingPartitionIds != null);
            this.failedHostId = failedHostId;
            this.missingPartitionIds = ImmutableSet.copyOf(missingPartitionIds);
        }

        @Override
        public String getMessage() {
            // convert set of ints to array of strings
            String[] strIds = missingPartitionIds.stream().map(i -> String.valueOf(i)).toArray(String[]::new);
            return String.format("After Host %d failure, non-viable cluster due to k-safety violation. "
                               + "Missing partitions: %s", failedHostId, String.join(",", strIds));
        }
    }

    /////////////////////////////////////
    //
    // PRIVATE BUILDER CLASSES
    //
    /////////////////////////////////////

    private static class MutablePartition {
        final int id;
        final int k;
        final Set<MutableHost> hosts = new HashSet<>();
        MutableHost leader = null;

        MutablePartition(int id, int k) {
            this.id = id;
            this.k = k;
        }
    }

    private static class MutableHost {
        final int id;
        int targetSiteCount;
        HAGroup haGroup;
        Set<MutablePartition> partitions = new HashSet<MutablePartition>();

        MutableHost(int id, int targetSiteCount, HAGroup haGroup) {
            this.id = id;
            this.targetSiteCount = targetSiteCount;
            this.haGroup = haGroup;
        }

        int freeSpace() {
            return Math.max(targetSiteCount - partitions.size(), 0);
        }

        /** Count the number of partitions that consider this host a leader */
        int leaderCount() {
            return (int) partitions.stream().filter(p -> p.leader == this).count();
        }
    }

    /////////////////////////////////////
    //
    // PUBLIC STATIC API
    //
    /////////////////////////////////////

    public static class HostDescription {
        public final int hostId;
        public final int targetSiteCount;
        public final String haGroupToken;

        public HostDescription(int hostId, int targetSiteCount, String haGroupToken) {
            this.hostId = hostId;
            this.targetSiteCount = targetSiteCount;
            this.haGroupToken = haGroupToken;
        }
    }

    public static class PartitionDescription {
        public final int k;

        public PartitionDescription(int k) {
            this.k = k;
        }
    }

    /**
     *
     *
     * @param hostCount
     * @param sitesPerHost
     * @param k
     * @return
     */
    public static String validateLegacyClusterConfig(int hostCount, int sitesPerHost, int k) {
        int totalSites = sitesPerHost * hostCount;

        if (hostCount <= k) {
            return "Not enough nodes to ensure K-Safety.";
        }
        if (totalSites % (k + 1) != 0) {
            return "Total number of sites is not divisable by the number of partitions.";
        }

        // valid config
        return null;
    }

    public static AbstractTopology mutateAddHosts(AbstractTopology currentTopology,
                                                  HostDescription[] hostDescriptions)
    {
        // validate input
        assert(currentTopology != null);
        Arrays.stream(hostDescriptions).forEach(hd -> {
            assert(hd != null);
            assert(hd.targetSiteCount >= 0);
            assert(hd.haGroupToken != null);
        });

        // validate no duplicate host ids
        Set<Integer> hostIds = new HashSet<>(currentTopology.hostsById.keySet());
        for (HostDescription hostDescription : hostDescriptions) {
            if (hostIds.contains(hostDescription.hostId)) {
                throw new RuntimeException("New host descriptions must contain unique and unused hostid.");
            }
            hostIds.add(hostDescription.hostId);
        }

        // for now, just add empty nodes to the topology -- not much else to do here

        // get immutable HAGroups - these are fixed by user command line
        final HAGroup[] haGroups = getHAGroupsForHosts(currentTopology, hostDescriptions);

        // get a map of hostid => hostdescription for new hosts
        Map<Integer, HostDescription> hostDescriptionsById = Arrays.stream(hostDescriptions)
                .collect(Collectors.toMap(hd -> hd.hostId, hd -> hd));

        // build the full set of immutable hosts, using the HAGroups
        Set<Host> fullHostSet = new HashSet<>();
        for (HAGroup haGroup : haGroups) {
            for (int hostId : haGroup.hostIds) {
                Host currentHost = currentTopology.hostsById.get(hostId);
                Host newHost = null;
                if (currentHost != null) {
                    newHost = new Host(hostId, currentHost.targetSiteCount, haGroup, currentHost.partitions);
                }
                else {
                    HostDescription hostDescription = hostDescriptionsById.get(hostId);
                    assert(hostDescription != null);
                    newHost = new Host(hostId, hostDescription.targetSiteCount, haGroup, new TreeSet<>());
                }
                fullHostSet.add(newHost);
            }
        }

        return new AbstractTopology(currentTopology.version + 1, fullHostSet);
    }

    public static AbstractTopology mutateAddPartitionsToEmptyHosts(AbstractTopology currentTopology,
                                                                   PartitionDescription[] partitionDescriptions)
    {
        // validate input
        assert(currentTopology != null);
        Arrays.stream(partitionDescriptions).forEach(pd -> {
            assert(pd != null);
            assert(pd.k >= 0);
        });

        /////////////////////////////////
        // convert all hosts to mutable hosts to add partitions and sites
        /////////////////////////////////
        final Map<Integer, MutableHost> mutableHostMap = new TreeMap<>();
        final Map<Integer, MutablePartition> mutablePartitionMap = new TreeMap<>();
        convertTopologyToMutables(currentTopology, mutableHostMap, mutablePartitionMap);

        // get max used site and partition ids so new ones will be unique
        int largestPartitionId = getNextFreePartitionId(currentTopology);

        /////////////////////////////////
        // find eligible mutable hosts (those without any partitions and with sph > 0)
        /////////////////////////////////
        Map<Integer, MutableHost> eligibleHosts = mutableHostMap.values().stream()
                .filter(h -> h.partitions.size() == 0)
                .filter(h -> h.targetSiteCount > 0)
                .collect(Collectors.toMap(h -> h.id, h -> h));

        /////////////////////////////////
        // generate partitions
        /////////////////////////////////
        Map<Integer, MutablePartition> partitionsToAdd = new TreeMap<>();
        for (PartitionDescription partitionDescription : partitionDescriptions) {
            MutablePartition partition = new MutablePartition(largestPartitionId++, partitionDescription.k);
            partitionsToAdd.put(partition.id, partition);
            mutablePartitionMap.put(partition.id, partition);
        }

        // group partitions by k
        Map<Integer, List<MutablePartition>> newPartitionsByK = partitionsToAdd.values().stream()
                .collect(Collectors.groupingBy(mp -> mp.k));
        // sort partitions by k
        newPartitionsByK = new TreeMap<>(newPartitionsByK);

        /////////////////////////////////
        // validate eligible hosts have enough space for partitions
        /////////////////////////////////
        int totalFreeSpace = mutableHostMap.values().stream()
                .mapToInt(h -> h.freeSpace())
                .sum();
        int totalReplicasToPlace = partitionsToAdd.values().stream()
                .mapToInt(p -> p.k + 1)
                .sum();
        if (totalFreeSpace < totalReplicasToPlace) {
            throw new RuntimeException("Hosts have inadequate space to hold all partition replicas.");
        }

        /////////////////////////////////
        // compute HAGroup distances
        /////////////////////////////////
        List<HAGroup> haGroups = mutableHostMap.values().stream()
                .map(h -> h.haGroup)
                .distinct()
                .collect(Collectors.toList());

        Map<HAGroup, Map<HAGroup, Integer>> haGroupDistances = new HashMap<>();
        for (HAGroup haGroup1 : haGroups) {
            Map<HAGroup, Integer> distances = new HashMap<>();
            haGroupDistances.put(haGroup1, distances);
            for (HAGroup haGroup2 : haGroups) {
                int distance = computeHADistance(haGroup1.token, haGroup2.token);
                distances.put(haGroup2, distance);
            }
        }

        /////////////////////////////////
        // place partitions with hosts
        /////////////////////////////////
        while (partitionsToAdd.size() > 0) {
            // PLAN:
            // 1. Start with the largest k, over all partitions
            // 2. Find k+1 eligible hosts, starting with ha groups that have low max distance to other ha groups
            Entry<Integer, List<MutablePartition>> partitionsWithLargestK = //newPartitionsByK.//findMostCommonK(newPartitionsByK);
                    newPartitionsByK.entrySet().stream()
                            .filter(e -> e.getValue().size() > 0) // ignore k with empty partition lists
                            .max((e1, e2) -> e1.getKey() - e2.getKey()).get();

            // goal is to find a set k + 1 hosts that contain the starter host that
            //  a) have space for at least one partition
            //  b) are reasonably distributed w.r.t. ha groups
            int targetReplicaCount = partitionsWithLargestK.getKey() + 1;

            // verify enough hosts exist
            if (eligibleHosts.size() < targetReplicaCount) {
                throw new RuntimeException(String.format(
                        "Partition requesting %d replicas " +
                        "but there are only %d eligable hosts on which to place them. " +
                        "Topology request invalid.",
                        targetReplicaCount, eligibleHosts.size()));
            }

            // if there isn't space for a partition, shift partitions around until there is
            // or give up if shifting can't free up enough space
            while (countHostsWithFreeSpace(eligibleHosts) < targetReplicaCount) {
                // if there aren't k + 1 good nodes, then move around some partition replicas until there are
                if (!shiftAPartition(eligibleHosts, haGroupDistances)) {
                    throw new RuntimeException(String.format(
                            "Partition requesting %d replicas " +
                            "but unable to find more than %d hosts with free space to place them. " +
                            "Topology request invalid.",
                            targetReplicaCount, countHostsWithFreeSpace(eligibleHosts)));
                }
            }

            // pick one host to be part of a partition group
            MutableHost starterHost = findBestStarterHost(eligibleHosts, haGroupDistances);
            // find k + 1 peers for starter host
            Set<MutableHost> peerHostsForPartition = findBestPeerHosts(starterHost, targetReplicaCount, eligibleHosts, haGroupDistances, false);
            assert(peerHostsForPartition.size() == targetReplicaCount);

            // determine how many partitions this group of hosts can handle
            int minAvailableSitesForSet =  peerHostsForPartition.stream()
                    .mapToInt(h -> h.freeSpace())
                    .min().getAsInt();
            // determine how many partitions we have with this k value
            int availablePartitionCount = partitionsWithLargestK.getValue().size();

            // assign the partitions
            for (int i = 0; i < Math.min(minAvailableSitesForSet, availablePartitionCount); i++) {
                // pop a partition of the list
                MutablePartition partition = partitionsWithLargestK.getValue().remove(0);
                // assign it to the host set
                for (MutableHost host : peerHostsForPartition) {
                    host.partitions.add(partition);
                    partition.hosts.add(host);
                }
                // remove the partition from the tracking
                partitionsToAdd.remove(partition.id);
            }
        }

        /////////////////////////////////
        // pick leaders for partitions that need them
        /////////////////////////////////
        assignLeadersToPartitionsThatNeedThem(mutableHostMap, mutablePartitionMap);

        /////////////////////////////////
        // convert mutable hosts to hosts to prep a return value
        /////////////////////////////////
        return convertMutablesToTopology(
                currentTopology.version + 1,
                mutableHostMap,
                mutablePartitionMap);
    }

    public static AbstractTopology mutateRemoveHost(AbstractTopology currentTopology, int hostId)
        throws KSafetyViolationException
    {
        /////////////////////////////////
        // convert all hosts to mutable hosts to add partitions and sites
        /////////////////////////////////
        final Map<Integer, MutableHost> mutableHostMap = new TreeMap<>();
        final Map<Integer, MutablePartition> mutablePartitionMap = new TreeMap<>();
        convertTopologyToMutables(currentTopology, mutableHostMap, mutablePartitionMap);

        Set<Integer> missingPartitionIds = new HashSet<>();

        MutableHost hostToRemove = mutableHostMap.remove(hostId);
        if (hostToRemove == null) {
            throw new RuntimeException("Can't remove host; host id not present in current topology.");
        }
        for (MutablePartition partition : hostToRemove.partitions) {
            partition.hosts.remove(hostToRemove);
            if (partition.hosts.size() == 0) {
                missingPartitionIds.add(partition.id);
            }
        }

        // check for k-safety violation
        if (missingPartitionIds.size() > 0) {
            throw new KSafetyViolationException(hostId, missingPartitionIds);
        }

        Set<Integer> mutableHostIds = new HashSet<>(hostToRemove.haGroup.hostIds);
        mutableHostIds.remove(hostId);
        int[] hostIdArray = hostToRemove.haGroup.hostIds.stream()
                .filter(candidateId -> candidateId != hostId)
                .mapToInt(id -> id)
                .toArray();

        HAGroup newHaGroup = new HAGroup(hostToRemove.haGroup.token, hostIdArray);
        mutableHostMap.values().forEach(h -> {
            if (h.haGroup.token.equals(newHaGroup.token)) {
                h.haGroup = newHaGroup;
            }
        });

        /////////////////////////////////
        // pick leaders for partitions that need them (naive)
        /////////////////////////////////
        assignLeadersToPartitionsThatNeedThem(mutableHostMap, mutablePartitionMap);

        /////////////////////////////////
        // convert mutable hosts to hosts to prep a return value
        /////////////////////////////////
        return convertMutablesToTopology(
                currentTopology.version + 1,
                mutableHostMap,
                mutablePartitionMap);
    }

    /**
     * Get the total number of missing replicas across all partitions.
     * Note this doesn't say how many partitions are under-represented.
     *
     * If this returns > 0, you should rejoin, rather than elastic join.
     *
     * @param topology Current topology
     * @return The number of missing replicas.
     */
    public int countMissingPartitionReplicas() {
        // sum up, for all partitions, the diff between k+1 and replica count
        return partitionsById.values().stream()
                .mapToInt(p -> (p.k + 1) - p.hostIds.size())
                .sum();
    }

    /**
     *
     * @param currentTopology
     * @param hostDescription
     * @return
     */
    public static AbstractTopology mutateRejoinHost(AbstractTopology currentTopology, HostDescription hostDescription) {
        // add the node
        currentTopology = AbstractTopology.mutateAddHosts(currentTopology, new HostDescription[] { hostDescription });

        /////////////////////////////////
        // convert all hosts to mutable hosts to add partitions and sites
        /////////////////////////////////
        final Map<Integer, MutableHost> mutableHostMap = new TreeMap<>();
        final Map<Integer, MutablePartition> mutablePartitionMap = new TreeMap<>();
        convertTopologyToMutables(currentTopology, mutableHostMap, mutablePartitionMap);

        // collect under-replicated partitions by hostid
        List<MutablePartition> underReplicatedPartitions = mutablePartitionMap.values().stream()
                .filter(p -> (p.k + 1) > p.hosts.size())
                .collect(Collectors.toList());

        // find hosts with under-replicated partitions
        Map<Integer, List<MutablePartition>> urPartitionsByHostId = new TreeMap<>();
        underReplicatedPartitions.forEach(urPartition -> {
            urPartition.hosts.forEach(host -> {
                List<MutablePartition> partitionsForHost = urPartitionsByHostId.get(host.id);
                if (partitionsForHost == null) {
                    partitionsForHost = new ArrayList<>();
                    urPartitionsByHostId.put(host.id, partitionsForHost);
                }
                partitionsForHost.add(urPartition);
            });
        });

        // divide partitions into groups
        Set<MutablePartition> partitionsToScan = new HashSet<>(underReplicatedPartitions);
        List<List<MutablePartition>> partitionGroups = new ArrayList<>();
        while (!partitionsToScan.isEmpty()) {
            List<MutablePartition> partitionGroup = new ArrayList<MutablePartition>();
            partitionGroups.add(partitionGroup);
            // get any partition from the set to scan
            MutablePartition starter = partitionsToScan.iterator().next();
            scanPeerParititions(partitionsToScan, partitionGroup, starter);
        }

        // sort partition groups from largest to smallest
        partitionGroups = partitionGroups.stream()
                .sorted((l1, l2) -> l2.size() - l1.size())
                .collect(Collectors.toList());

        // look for a group with the right number of partitions
        List<MutablePartition> match = null;
        // look for a fallback where host covers exactly 1/X (for some X) of a partition group
        List<MutablePartition> altMatch1 = null;
        // look for a second fallback where host covers some of a group, but not perfectly 1/X
        List<MutablePartition> altMatch2 = null;
        // look for a third fallback where host joins two groups
        List<MutablePartition> altMatch3 = null;
        for (List<MutablePartition> partitionGroup : partitionGroups) {
            if (partitionGroup.size() == hostDescription.targetSiteCount) {
                match = partitionGroup;
                break;
            }
            if ((partitionGroup.size() % hostDescription.targetSiteCount) == 0) {
                altMatch1 = partitionGroup;
                continue;
            }
            if (partitionGroup.size() > hostDescription.targetSiteCount) {
                altMatch2 = partitionGroup;
                continue;
            }
            for (List<MutablePartition> altPartitionGroup : partitionGroups) {
                if (altPartitionGroup == partitionGroup) {
                    continue;
                }

            }
        }

        // collapse the alternates to pick the best one we can
        if (match == null) match = altMatch1;
        if (match == null) match = altMatch2;
        if (match == null) match = altMatch3;

        // if no match or alternates, combine groups until you get a fit
        if (match == null) {
            match = new ArrayList<>();
            // remember: list of partition groups are sorted by size
            for (List<MutablePartition> partitionGroup : partitionGroups) {
                match.addAll(partitionGroup);
                // break when we have enough partitions
                if (match.size() >= hostDescription.targetSiteCount) {
                    break;
                }
                // though we might add all of them if target SPH > under-replicated partitions
            }
        }

        // now we can assume match is correct!
        MutableHost rejoiningHost = mutableHostMap.get(hostDescription.hostId);
        assert(rejoiningHost.targetSiteCount == hostDescription.targetSiteCount);
        assert(rejoiningHost.id == hostDescription.hostId);
        assert(rejoiningHost.partitions.isEmpty());

        rejoiningHost.partitions.addAll(match);
        match.forEach(p -> p.hosts.add(rejoiningHost));
        mutableHostMap.put(rejoiningHost.id, rejoiningHost);

        /////////////////////////////////
        // convert mutable hosts to hosts to prep a return value
        /////////////////////////////////
        return convertMutablesToTopology(
                currentTopology.version + 1,
                mutableHostMap,
                mutablePartitionMap);
    }

    /////////////////////////////////////
    //
    // SERIALIZATION API
    //
    /////////////////////////////////////

    public String topologyToJSON() throws JSONException {
        JSONStringer stringer = new JSONStringer();
        stringer.object();

        stringer.key("version").value(version);

        stringer.key("haGroups").array();
        List<HAGroup> haGroups = hostsById.values().stream()
                .map(h -> h.haGroup)
                .distinct()
                .collect(Collectors.toList());
        for (HAGroup haGroup : haGroups) {
            haGroup.toJSON(stringer);
        }
        stringer.endArray();

        stringer.key("partitions").array();
        for (Partition partition : partitionsById.values()) {
            partition.toJSON(stringer);
        }
        stringer.endArray();

        stringer.key("hosts").array();
        for (Host host : hostsById.values()) {
            host.toJSON(stringer);
        }
        stringer.endArray();

        stringer.endObject();

        return new JSONObject(stringer.toString()).toString(4);
    }

    public static AbstractTopology topologyFromJSON(String jsonTopology) throws JSONException {
        JSONObject jsonObj = new JSONObject(jsonTopology);
        return topologyFromJSON(jsonObj);
    }

    public static AbstractTopology topologyFromJSON(JSONObject jsonTopology) throws JSONException {
        Map<Integer, Partition> partitionsById = new TreeMap<>();
        Map<String, HAGroup> haGroupsByToken = new TreeMap<>();
        List<Host> hosts = new ArrayList<>();

        long version = jsonTopology.getLong("version");

        JSONArray haGroupsJSON = jsonTopology.getJSONArray("haGroups");
        for (int i = 0; i < haGroupsJSON.length(); i++) {
            HAGroup haGroup = HAGroup.fromJSON(haGroupsJSON.getJSONObject(i));
            haGroupsByToken.put(haGroup.token, haGroup);
        }

        JSONArray partitionsJSON = jsonTopology.getJSONArray("partitions");
        for (int i = 0; i < partitionsJSON.length(); i++) {
            Partition partition = Partition.fromJSON(partitionsJSON.getJSONObject(i));
            partitionsById.put(partition.id, partition);
        }

        JSONArray hostsJSON = jsonTopology.getJSONArray("hosts");
        for (int i = 0; i < hostsJSON.length(); i++) {
            Host host = Host.fromJSON(hostsJSON.getJSONObject(i), haGroupsByToken, partitionsById);
            hosts.add(host);
        }

        return new AbstractTopology(version, hosts);
    }

    /////////////////////////////////////
    //
    // PRIVATE TOPOLOGY CONSTRUCTOR
    //
    /////////////////////////////////////

    private AbstractTopology(long version, Collection<Host> hosts) {
        assert(hosts != null);
        assert(version >= 0);

        this.version = version;

        // get a sorted map of hosts across the cluster by id
        Map<Integer, Host> hostsByIdTemp = new TreeMap<>();
        for (Host host : hosts) {
            hostsByIdTemp.put(host.id, host);
        }
        this.hostsById = ImmutableMap.copyOf(hostsByIdTemp);

        // get a sorted map of unique partitions across the cluster by id
        Map<Integer, Partition> paritionsByIdTemp = new TreeMap<>();
        for (Host host : hosts) {
            for (Partition partition : host.partitions) {
                paritionsByIdTemp.put(partition.id, partition);
            }
        }
        this.partitionsById = ImmutableMap.copyOf(paritionsByIdTemp);
    }

    /////////////////////////////////////
    //
    // PRIVATE STATIC HELPER METHODS
    //
    /////////////////////////////////////

    private static void convertTopologyToMutables(
            final AbstractTopology topology,
            final Map<Integer, MutableHost> mutableHostMap,
            final Map<Integer, MutablePartition> mutablePartitionMap)
    {
        // create mutable hosts without partitions
        for (Host host : topology.hostsById.values()) {
            final MutableHost mutableHost = new MutableHost(
                    host.id, host.targetSiteCount, host.haGroup);
            mutableHostMap.put(host.id, mutableHost);
        }

        // create partitions
        for (Partition partition : topology.partitionsById.values()) {
            MutablePartition mp = new MutablePartition(partition.id, partition.k);
            mutablePartitionMap.put(mp.id, mp);
            for (Integer hostId : partition.hostIds) {
                mp.hosts.add(mutableHostMap.get(hostId));
            }
            mp.leader = mutableHostMap.get(partition.leaderHostId);
        }

        // link partitions and hosts
        for (Host host : topology.hostsById.values()) {
            final MutableHost mutableHost = mutableHostMap.get(host.id);
            host.partitions.stream().forEach(p -> {
                MutablePartition mp = mutablePartitionMap.get(p.id);
                mutableHost.partitions.add(mp);
            });
        }
    }

    private static AbstractTopology convertMutablesToTopology(
            final long currentVersion,
            final Map<Integer, MutableHost> mutableHostMap,
            final Map<Integer, MutablePartition> mutablePartitionMap)
    {
        final Map<Integer, Partition> partitionsById = new TreeMap<>();
        mutablePartitionMap.values().stream().forEach(mp -> {
            assert(mp.leader != null);

            List<Integer> hostIds = mp.hosts.stream()
                    .map(h -> h.id)
                    .collect(Collectors.toList());

            Partition p = new Partition(mp.id, mp.k, mp.leader.id, hostIds);
            partitionsById.put(p.id, p);
        });

        Set<Host> fullHostSet = new HashSet<>();
        for (MutableHost mutableHost : mutableHostMap.values()) {
            List<Partition> hostPartitions = mutableHost.partitions.stream()
                    .map(mp -> partitionsById.get(mp.id))
                    .collect(Collectors.toList());
            Host newHost = new Host(mutableHost.id, mutableHost.targetSiteCount, mutableHost.haGroup, hostPartitions);
            fullHostSet.add(newHost);
        }

        return new AbstractTopology(currentVersion + 1, fullHostSet);
    }

    private static int getNextFreePartitionId(AbstractTopology topology) {
        OptionalInt maxPartitionIdOptional = topology.partitionsById.values().stream()
                .mapToInt(p -> p.id)
                .max();
        if (maxPartitionIdOptional.isPresent()) {
            return maxPartitionIdOptional.getAsInt() + 1;
        }
        else {
            return 0;
        }
    }

    private static HAGroup[] getHAGroupsForHosts(AbstractTopology currentTopology, HostDescription[] hostDescriptions) {

        class MutableHAGroup {
            String token = "";
            List<Integer> hostsIds;

            private MutableHAGroup(String token) {
                this.token = token;
                this.hostsIds = new ArrayList<>();
            }
        }

        final Map<String, MutableHAGroup> groupsByToken = new TreeMap<>();

        // deal with all pre-existing hosts
        for (Host host : currentTopology.hostsById.values()) {
            MutableHAGroup haGroup = groupsByToken.get(host.haGroup.token);
            if (haGroup == null) {
                haGroup = new MutableHAGroup(host.haGroup.token);
                groupsByToken.put(host.haGroup.token, haGroup);
            }
            haGroup.hostsIds.add(host.id);
        }

        // deal with all new hosts
        for (HostDescription host : hostDescriptions) {
            MutableHAGroup haGroup = groupsByToken.get(host.haGroupToken);
            if (haGroup == null) {
                haGroup = new MutableHAGroup(host.haGroupToken);
                groupsByToken.put(host.haGroupToken, haGroup);
            }
            haGroup.hostsIds.add(host.hostId);
        }

        // convert mutable to immutable
        return groupsByToken.values().stream()
                .map(g -> new HAGroup(g.token, g.hostsIds.stream().mapToInt(i -> i).toArray()))
                .toArray(HAGroup[]::new);
    }

    /**
     * Compute the tree-edge distance between any two ha group tokens
     * Not the most efficient way to do this, but even n^2 for 100 nodes is computable
     */
    private static int computeHADistance(/*final Map<String, Integer> distanceCache,*/ String token1, String token2) {
        // ensure token1 < token2 (swap if need be)
        if (token1.compareTo(token2) > 0) {
            String temp = token1;
            token1 = token2;
            token2 = temp;
        }

        // break into arrays of graph edges
        String[] token1parts = token1.split(".");
        String[] token2parts = token1.split(".");

        // trim shared path prefix
        while (token1parts.length > 0) {
            if (token2parts.length == 0) break;
            if (token1parts.equals(token2parts)) {
                token1parts = Arrays.copyOfRange(token1parts, 1, token1parts.length);
                token2parts = Arrays.copyOfRange(token2parts, 1, token2parts.length);
            }
        }

        // distance is now the sum of the two path lengths
        return token1parts.length + token2parts.length;
    }

    /**
     * First, find hosts in the ha group that has the lowest max distance to another ha group
     * Second, find hosts with the most free sites per host target within that ha group
     */
    private static MutableHost findBestStarterHost(
            Map<Integer, MutableHost> eligableHosts,
            Map<HAGroup, Map<HAGroup, Integer>> haGroupDistances)
    {
        // special case the one node setup
        if (eligableHosts.size() == 1) {
            MutableHost host = eligableHosts.values().iterator().next();
            // we assume if there was one host, and it was full, we wouldn't be here
            assert(host.partitions.size() < host.targetSiteCount);
            return host;
        }

        Map<HAGroup, Integer> distances = new HashMap<>();
        for (Entry<HAGroup, Map<HAGroup, Integer>> e : haGroupDistances.entrySet()) {
            int distance = 0;
            if (e.getValue().size() > 0) {
                distance = e.getValue().values().stream()
                        .mapToInt(i -> i)
                        .max().getAsInt();
            }
            distances.put(e.getKey(), distance);
        }

        List<HAGroup> haGroupsByMinimalMaxHADistance = distances.entrySet().stream()
                .sorted((e1,e2) -> e1.getValue() - e2.getValue())
                .map(e -> e.getKey())
                .collect(Collectors.toList());

        for (HAGroup haGroup : haGroupsByMinimalMaxHADistance) {
            List<MutableHost> hostsByAvailability = haGroup.hostIds.stream()
                    .map(id -> eligableHosts.get(id))
                    .filter(h -> h != null)
                    .filter(h -> h.targetSiteCount > h.partitions.size())
                    .sorted((h1, h2) -> h2.freeSpace() - h1.freeSpace())
                    .collect(Collectors.toList());
            if (!hostsByAvailability.isEmpty()) {
                return hostsByAvailability.get(0);
            }
        }

        assert(false);
        return null;
    }

    private static MutableHost findNextPeerHost(
            final Set<MutableHost> peers,
            final Map<Integer, MutableHost> eligibleHosts,
            final Map<HAGroup, Map<HAGroup, Integer>> haGroupDistances,
            boolean findFullHosts)
    {
        List<MutableHost> hostsInOrder = null;

        MutableHost anyHost = peers.iterator().next();

        Set<HAGroup> undesireableHAGroups = peers.stream()
                .map(h -> h.haGroup)
                .collect(Collectors.toSet());

        Map<HAGroup, Integer> hasByDistance = haGroupDistances.get(anyHost.haGroup);
        List<HAGroup> haGroupsByDistance = hasByDistance.entrySet().stream()
                .sorted((e1,e2) -> e1.getValue() - e2.getValue())
                .map(e -> e.getKey())
                .filter(hag -> undesireableHAGroups.contains(hag) == false)
                .collect(Collectors.toList());

        for (HAGroup haGroup : haGroupsByDistance) {
            List<MutableHost> validHosts = haGroup.hostIds.stream()
                    .map(id -> eligibleHosts.get(id))
                    .filter(h -> h != null)
                    .collect(Collectors.toList());

            if (findFullHosts) {
                // find full and non-full hosts sorted by availability (less -> more)
                hostsInOrder = validHosts.stream()
                        .sorted((h1, h2) -> h1.freeSpace() - h2.freeSpace())
                        .collect(Collectors.toList());
            }
            else {
                // find non-full hosts sorted by availability (more -> less)
                hostsInOrder = validHosts.stream()
                        .filter(h -> h.targetSiteCount > h.partitions.size())
                        .sorted((h1, h2) -> h2.freeSpace() - h1.freeSpace())
                        .collect(Collectors.toList());
            }
            if (!hostsInOrder.isEmpty()) {
                return hostsInOrder.get(0);
            }
        }

        // at this point, give up on using distinct ha groups and just find a host

        if (findFullHosts) {
            hostsInOrder = eligibleHosts.values().stream()
                    .filter(h -> h.targetSiteCount == h.partitions.size()) // is full
                    .filter(h -> peers.contains(h) == false) // not chosen yet
                    .collect(Collectors.toList());
        }
        else {
            // sort candidate hosts by free space
            hostsInOrder = eligibleHosts.values().stream()
                    .filter(h -> h.freeSpace() > 0) // has space
                    .filter(h -> peers.contains(h) == false) // not chosen yet
                    .sorted((h1, h2) -> h2.freeSpace() - h1.freeSpace()) // pick most free space
                    .collect(Collectors.toList());
        }

        if (hostsInOrder.isEmpty()) {
            return null;
        }

        // pick the most empty hosts that haven't been selected
        return hostsInOrder.get(0);
    }

    private static Set<MutableHost> findBestPeerHosts(
            MutableHost starterHost,
            int peerCount,
            Map<Integer, MutableHost> eligibleHosts,
            Map<HAGroup, Map<HAGroup, Integer>> haGroupDistances,
            boolean findFullHosts)
    {
        final Set<MutableHost> peers = new HashSet<>();
        peers.add(starterHost);
        // special case k = 0
        if (peerCount == 1) {
            return peers;
        }

        while (peers.size() < peerCount) {
            MutableHost nextPeer = findNextPeerHost(peers, eligibleHosts, haGroupDistances, findFullHosts);
            if (nextPeer == null) {
                return peers;
            }
            peers.add(nextPeer);
        }

        return peers;
    }

    /**
     * Find (peerCount - 1) nodes that are full, and move one partition from them
     * to the given host. Obviously, make sure the moved partitions aren't the same one.
     */
    private static boolean shiftAPartition(
            Map<Integer, MutableHost> eligibleHosts,
            Map<HAGroup, Map<HAGroup, Integer>> haGroupDistances)
    {
        // find a host that has at least two open slots to move a partition to
        List<MutableHost> hostsWithSpaceInOrder = eligibleHosts.values().stream()
                .filter(h -> h.freeSpace() >= 2) // need at least two open slots
                .sorted((h1, h2) -> h2.freeSpace() - h1.freeSpace()) // sorted by emptiness
                .collect(Collectors.toList());

        // iterate over all hosts with space free
        for (MutableHost starterHost : hostsWithSpaceInOrder) {
            // get candidate hosts to donate a partition to a starter host
            List<MutableHost> fullHostsInOrder = eligibleHosts.values().stream()
                    .filter(h -> h.freeSpace() == 0) // full hosts
                    .filter(h -> h.targetSiteCount > 0) // not slotless hosts
                    .sorted((h1, h2) ->
                        computeHADistance(starterHost.haGroup.token, h1.haGroup.token) -
                        computeHADistance(starterHost.haGroup.token, h2.haGroup.token)
                    ) // by distance from starter host
                    .collect(Collectors.toList());

            // walk through all candidate hosts, swapping at most one partition

            for (MutableHost fullHost : fullHostsInOrder) {
                assert(fullHost.freeSpace() == 0);

                for (MutablePartition partition : fullHost.partitions) {
                    // skip moving this one if we're already a replica
                    if (starterHost.partitions.contains(partition)) continue;

                    // move it!
                    starterHost.partitions.add(partition);
                    partition.hosts.add(starterHost);
                    fullHost.partitions.remove(partition);
                    partition.hosts.remove(fullHost);

                    assert(starterHost.partitions.size() <= starterHost.targetSiteCount);
                    assert(starterHost.partitions.size() <= fullHost.targetSiteCount);

                    return true;
                }
            }
        }

        // didn't shift anything
        return false;
    }

    private static int countHostsWithFreeSpace(Map<Integer, MutableHost> eligibleHosts) {
        int freeSpaceHostCount = eligibleHosts.values().stream()
                .mapToInt(h -> (h.targetSiteCount > h.partitions.size()) ? 1 : 0)
                .sum();
        return freeSpaceHostCount;
    }

    private static void scanPeerParititions(Set<MutablePartition> partitionsToScan,
            List<MutablePartition> partitionGroup,
            MutablePartition partition)
    {
        partitionsToScan.remove(partition);
        partitionGroup.add(partition);
        for (MutableHost host : partition.hosts) {
            for (MutablePartition peer : host.partitions) {
                if (partitionsToScan.contains(peer)) {
                    scanPeerParititions(partitionsToScan, partitionGroup, peer);
                }
            }
        }
    }

    private static void assignLeadersToPartitionsThatNeedThem(
            Map<Integer, MutableHost> mutableHostMap,
            Map<Integer, MutablePartition> mutablePartitionMap)
    {
        // clean up any partitions with leaders that don't exist
        // (this is used by remove node, not during new cluster forming)
        mutablePartitionMap.values().stream()
                .filter(p -> p.leader != null)
                // if a leader isn't in the current set of hosts, set to null
                .filter(p -> mutableHostMap.containsKey(p.leader.id) == false)
                .forEach(p -> p.leader = null);

        // sort partitions by small k, so we can assign less flexible partitions first
        List<MutablePartition> leaderlessPartitionsSortedByK = mutablePartitionMap.values().stream()
                .filter(p -> p.leader == null)
                .sorted((p1, p2) -> p1.k - p2.k)
                .collect(Collectors.toList());

        // pick a leader for each partition based on the host that is least full of leaders
        for (MutablePartition partition : leaderlessPartitionsSortedByK) {
            // find host with fewest leaders
            MutableHost leaderHost = partition.hosts.stream()
                    .min((h1, h2) -> h1.leaderCount() - h2.leaderCount()).get();
            partition.leader = leaderHost;
            assert(partition.hosts.contains(leaderHost));
        }

        // run through and shift leaders from hosts with high partition counts to those with low ones
        // iterate until it's not possible to shift things this way
        //
        // There might be better ways to do this that invovle mutli-swaps, but this is probably decent
        boolean foundAMove;
        do {
            foundAMove = false;

            for (MutablePartition partition : leaderlessPartitionsSortedByK) {
                int loadOfLeadHost = partition.leader.leaderCount();
                MutableHost loadWithMinLeaders = partition.hosts.stream()
                        .min((h1, h2) -> h1.leaderCount() - h2.leaderCount()).get();
                if ((loadOfLeadHost - loadWithMinLeaders.leaderCount()) >= 2) {
                    foundAMove = true;
                    partition.leader = loadWithMinLeaders;
                    break;
                }
            }

        } while (foundAMove);
    }
}
