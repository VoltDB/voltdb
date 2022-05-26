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

package org.voltdb;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.OptionalInt;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;

import org.json_voltpatches.JSONArray;
import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;
import org.json_voltpatches.JSONStringer;
import org.voltcore.messaging.HostMessenger.HostInfo;
import org.voltcore.utils.Pair;

import com.google_voltpatches.common.base.Preconditions;
import com.google_voltpatches.common.base.Splitter;
import com.google_voltpatches.common.collect.Collections2;
import com.google_voltpatches.common.collect.ComparisonChain;
import com.google_voltpatches.common.collect.ImmutableList;
import com.google_voltpatches.common.collect.ImmutableMap;
import com.google_voltpatches.common.collect.ImmutableSortedSet;
import com.google_voltpatches.common.collect.Iterables;
import com.google_voltpatches.common.collect.Lists;
import com.google_voltpatches.common.collect.Maps;
import com.google_voltpatches.common.collect.Multimap;
import com.google_voltpatches.common.collect.MultimapBuilder;
import com.google_voltpatches.common.collect.Range;
import com.google_voltpatches.common.collect.RangeMap;
import com.google_voltpatches.common.collect.Sets;
import com.google_voltpatches.common.collect.TreeRangeMap;

public class AbstractTopology {

    //Topology JSON keys
    final static String TOPO_PARTITIONS = "partitions";
    final static String TOPO_PARTITION_ID = "partition_id";
    final static String TOPO_MASTER = "master";
    final static String TOPO_REPLICA = "replicas";
    final static String TOPO_HOST_ID = "host_id";
    final static String TOPO_VERSION = "version";
    final static String TOPO_HAGROUP = "haGroup";
    final static String TOPO_HOSTS = "hosts";
    final static String TOPO_HOST_MISSING = "missing";
    final static String TOPO_SITES_PER_HOST = "sitesPerHost";
    final static String TOPO_REPLICATION_FACTOR = "replicationFactor";
    final static String TOPO_UNBALANCED_PART_COUNT = "unbalancedPartitionCount";

    public final static String PLACEMENT_GROUP_DEFAULT="0";

    public final long version;
    public final int m_replicationFactor;
    public final int m_sitesPerHost;
    public final int m_unbalancedPartitionCount;
    public final ImmutableMap<Integer, Host> hostsById;
    public final ImmutableMap<Integer, Partition> partitionsById;

    public static final AbstractTopology EMPTY_TOPOLOGY = new AbstractTopology(0, ImmutableMap.of(), ImmutableMap.of(),
            -1, -1, 0);

    /////////////////////////////////////
    //
    // PUBLIC CLASSES
    //
    /////////////////////////////////////

    public static class Partition implements Comparable<Partition>{
        public final int id;
        public final int leaderHostId;
        public final ImmutableSortedSet<Integer> hostIds;

        private Partition(int id, int leaderHostId, Collection<Integer> hostIds) {
            this.id = id;
            this.leaderHostId = leaderHostId;
            this.hostIds = ImmutableSortedSet.copyOf(hostIds);
        }

        public ImmutableSortedSet<Integer> getHostIds(){
            return hostIds;
        }

        public int getLeaderHostId() {
            return leaderHostId;
        }

        @Override
        public String toString() {
            String[] hostIdStrings = hostIds.stream().map(id -> String.valueOf(id)).toArray(String[]::new);
            return String.format("Partition %d (leader %d, hosts %s)", id, leaderHostId, String.join(",", hostIdStrings));
        }

        private void toJSON(JSONStringer stringer) throws JSONException {
            stringer.object();
            stringer.key(TOPO_PARTITION_ID).value(id);
            stringer.key(TOPO_MASTER).value(leaderHostId);
            stringer.key(TOPO_REPLICA).array();
            for (Integer hostId : hostIds) {
                stringer.value(hostId);
            }
            stringer.endArray();
            stringer.endObject();
        }

        private static Partition fromJSON(JSONObject json) throws JSONException {
            int id = json.getInt(TOPO_PARTITION_ID);
            int leaderHostId = json.getInt(TOPO_MASTER);

            ImmutableSortedSet.Builder<Integer> builder = ImmutableSortedSet.naturalOrder();
            JSONArray jsonHostIds = json.getJSONArray(TOPO_REPLICA);
            for (int i = 0; i < jsonHostIds.length(); i++) {
                builder.add(jsonHostIds.getInt(i));
            }
            return new Partition(id, leaderHostId, builder.build());
        }

        @Override
        public int compareTo(Partition o) {
            return (this.id - o.id);
        }
    }

    public static class Host implements Comparable<Host> {
        public final int id;
        public final String haGroup;
        public final ImmutableSortedSet<Partition> partitions;

        public final boolean isMissing;

        private Host(Host oldHost, int newId, Map<Integer, Partition> allPartitions, boolean isMissing) {
            ImmutableSortedSet.Builder<Partition> builder = ImmutableSortedSet.naturalOrder();
            for (Partition p : oldHost.partitions) {
                builder.add(allPartitions.get(p.id));
            }
            id = newId;
            haGroup = oldHost.haGroup;
            partitions = builder.build();
            this.isMissing = isMissing;
        }

        private Host(Host oldHost, int newId, Map<Integer, Partition> allPartitions) {
           this(oldHost, newId, allPartitions, false);
        }

        private Host(HostBuilder hostBuilder, Map<Integer, Partition> allPartitions) {
            ImmutableSortedSet.Builder<Partition> builder = ImmutableSortedSet.naturalOrder();
            for (PartitionBuilder pb : hostBuilder.m_partitions) {
                builder.add(allPartitions.get(pb.m_id));
            }
            id = hostBuilder.m_id;
            haGroup = hostBuilder.m_haGroup;
            partitions = builder.build();
            isMissing = hostBuilder.m_missing;
        }

        private Host(int id, String haGroup, ImmutableSortedSet<Partition> partitions, boolean missing) {
            assert(id >= 0);
            assert(haGroup != null);
            assert(partitions != null);
            assert(partitions.size() >= 0);

            this.id = id;
            this.haGroup = haGroup;
            this.partitions = partitions;
            this.isMissing = missing;
        }

        public List<Integer> getPartitionIdList() {

            // return as list to ensure the order as inserted
            return partitions.stream()
                    .map(p -> p.id)
                    .collect(Collectors.toList());
        }

        public ImmutableSortedSet<Partition> getPartitions() {
            return partitions;
        }

        public int getleaderCount() {
            int leaders = 0;
            for( Partition p : partitions) {
                if (p.leaderHostId == id) {
                    leaders++;
                }
            }
            return leaders;
        }

        @Override
        public String toString() {
            String[] partitionIdStrings = partitions.stream().map(p -> String.valueOf(p.id)).toArray(String[]::new);
            return String.format("Host %d ha:%s (Partitions %s)",
                    id, haGroup, String.join(",", partitionIdStrings));
        }

        private void toJSON(JSONStringer stringer) throws JSONException {
            stringer.object();
            stringer.key(TOPO_HOST_ID).value(id);
            stringer.key(TOPO_HAGROUP).value(haGroup);
            stringer.key(TOPO_HOST_MISSING).value(isMissing);
            stringer.key(TOPO_PARTITIONS).array();
            for (Partition partition : partitions) {
                stringer.value(partition.id);
            }
            stringer.endArray();
            stringer.endObject();
        }

        private static Host fromJSON(JSONObject json, final Map<Integer, Partition> partitionsById)
                throws JSONException {
            int id = json.getInt(TOPO_HOST_ID);
            String haGroupToken = json.getString(TOPO_HAGROUP);
            JSONArray jsonPartitions = json.getJSONArray(TOPO_PARTITIONS);
            ImmutableSortedSet.Builder<Partition> partitionBuilder = ImmutableSortedSet.naturalOrder();
            for (int i = 0; i < jsonPartitions.length(); i++) {
                int partitionId = jsonPartitions.getInt(i);
                partitionBuilder.add(partitionsById.get(partitionId));
            }
            Host host = new Host(id, haGroupToken, partitionBuilder.build(), json.getBoolean(TOPO_HOST_MISSING));
            return host;
        }

        @Override
        public int compareTo(Host o) {
            return (this.id - o.id);
        }
    }

    /////////////////////////////////////
    //
    // PRIVATE BUILDER CLASSES
    //
    /////////////////////////////////////

    /**
     * Checks whether or not a partition is considered to be balanced in relation to the ha groups in which it resides.
     */
    private interface BalancedPartitionChecker {
        /**
         * @param partition {@link PartitionBuilder} to test
         * @return {@code true} if the partition is properly balanced across ha groups
         */
        boolean isBalanced(PartitionBuilder partition);
    }

    /**
     * Selects the hosts to be part of a partition group. Taking in consideration the ha group layout for the cluster.
     */
    private interface PartitionGroupSelector {
        /**
         * Request the next partition group from the selector with {@code size} nodes in the group
         *
         * @param size number of nodes that should be in the next partition group
         * @return an array of {@link HostBuilder}
         */
        HostBuilder[] getNextPartitionGroup(int size, int maxMissing);

        /**
         * @return a {@link BalancedPartitionChecker} implementation which works with this selector implementation
         */
        BalancedPartitionChecker getBalancedPartitionChecker();
    }

    /**
     * Implementation of {@link BalancedPartitionChecker} which checks that the hosts which on which a partition reside
     * are in a minimum number of unique host groups
     */
    private static final class MinimumGroupBalancedPartitionChecker implements BalancedPartitionChecker {
        final int m_minimumGroupCount;

        public MinimumGroupBalancedPartitionChecker(int numberOfGroups, int replicaCount) {
            super();
            this.m_minimumGroupCount = numberOfGroups >= replicaCount ? replicaCount : 2;
        }

        @Override
        public boolean isBalanced(PartitionBuilder partition) {
            return partition.m_hosts.stream().map(h -> h.m_haGroup).distinct().count() >= m_minimumGroupCount;
        }
    }

    /**
     * A simple implementation of {@link PartitionGroupSelector} where there is only one ha group. With only one group
     * the hosts can just be pulled from the group one after the other. Also, because there is only one group the
     * partitions are considered to be balanced.
     */
    private static class SingleGroupPartitionGroupSelector implements PartitionGroupSelector {
        final HAGroup m_haGroup;

        SingleGroupPartitionGroupSelector(HAGroup group) {
            super();
            this.m_haGroup = group;
        }

        @Override
        public HostBuilder[] getNextPartitionGroup(int size, int maxMissing) {
            HostBuilder[] hostBuilders = new HostBuilder[size];
            for (int i = 0; i < size; ++i) {
                hostBuilders[i] = m_haGroup.pollHost(maxMissing > 0);
                if (hostBuilders[i].m_missing) {
                    --maxMissing;
                }
            }
            return hostBuilders;
        }

        @Override
        public BalancedPartitionChecker getBalancedPartitionChecker() {
            return p -> true;
        }
    }

    /**
     * An implementation of {@link PartitionGroupSelector} which handles the case that there are multple ha groups but
     * they are all the same distance away from each other. This means that all groups are equal and there is no reason
     * to favor selection from one group over another.
     * <p>
     * Groups are stored in a queue and removed from the queue until {@code size} of group is met or queue is empty.
     * When the queue is empty the already used groups are put back in the queue to be selected again.
     */
    private static class EqualDistantHaGroupsPartitionGroupSelector implements PartitionGroupSelector {
        final NavigableSet<HAGroup> m_groups = new TreeSet<>();
        final BalancedPartitionChecker m_balancedPartitionChecker;

        EqualDistantHaGroupsPartitionGroupSelector(Collection<HAGroup> groups, int replicaCount) {
            m_groups.addAll(groups);
            m_balancedPartitionChecker = new MinimumGroupBalancedPartitionChecker(groups.size(), replicaCount);
        }

        @Override
        public HostBuilder[] getNextPartitionGroup(int size, int maxMissing) {
            HostBuilder[] hostBuilders = new HostBuilder[size];
            List<HAGroup> usedGroups = new ArrayList<>(size);

            for (int i = 0; i < size; ++i) {
                do {
                    if (m_groups.isEmpty()) {
                        returnGroups(usedGroups);
                    }
                    HAGroup group = m_groups.pollFirst();
                    usedGroups.add(group);
                    hostBuilders[i] = group.pollHost(maxMissing > 0);
                } while (hostBuilders[i] == null);
                if (hostBuilders[i].m_missing) {
                    --maxMissing;
                }
            }

            returnGroups(usedGroups);

            return hostBuilders;
        }

        @Override
        public BalancedPartitionChecker getBalancedPartitionChecker() {
            return m_balancedPartitionChecker;
        }

        private void returnGroups(List<HAGroup> usedGroups) {
            for (HAGroup group : usedGroups) {
                if (!group.isEmpty()) {
                    m_groups.add(group);
                }
            }
            usedGroups.clear();
        }
    }

    /**
     * A complex {@link PartitionGroupSelector} which handles multiple ha groups with varying distances and overlapping
     * ancestry.
     * <p>
     * The distance and shared ancestry is calculated for every pair of groups and then the groups are sorted by the
     * distance to the farthest group. The group with the smallest max is then selected to provide the first node. This
     * makes it so that the group in the "middle" is chosen.
     * <p>
     * Each time a group is chosen the distance from it to other groups and the ancestry overlap is accumulated and the
     * group that has the highest distance and the lowest ancestry overlap is chosen as the next group. This is repeated
     * until {@code size} groups have been selected.
     */
    private static class ComplexHaGroupsPartitionGroupSelector implements PartitionGroupSelector {
        final NavigableSet<HAGroupWithRelationships> m_groupRelationships = new TreeSet<>();
        final Map<String, HAGroupRelationship> m_eligibleGroupsByToken = new HashMap<>();
        final BalancedPartitionChecker m_balancedPartitionChecker;

        ComplexHaGroupsPartitionGroupSelector(Collection<HAGroup> groups, int replicaCount) {
            List<HAGroupWithRelationships> tempGroups = new ArrayList<>(groups.size());
            for (HAGroup newGroup : groups) {
                HAGroupWithRelationships newGroupDistances = new HAGroupWithRelationships(newGroup);
                for (HAGroupWithRelationships otherGroup : tempGroups) {
                    HAGroupWithRelationships.associateGroups(newGroupDistances, otherGroup);
                }
                tempGroups.add(newGroupDistances);
                m_eligibleGroupsByToken.put(newGroup.m_token,
                        new HAGroupRelationship(newGroupDistances, new RelationshipDescription(0, 0)));
            }
            m_groupRelationships.addAll(tempGroups);
            m_balancedPartitionChecker = new MinimumGroupBalancedPartitionChecker(groups.size(), replicaCount);
        }

        @Override
        public HostBuilder[] getNextPartitionGroup(int size, int maxMissing) {
            HostBuilder[] hostBuilders = new HostBuilder[size];

            do {
                hostBuilders[0] = getHostFromGroup(m_groupRelationships.first(), false);
            } while (hostBuilders[0] == null);

            for (int i = 1; i < size; ++i) {
                do {
                    HAGroupWithRelationships group = m_eligibleGroupsByToken.values().stream()
                            .max(Comparator.naturalOrder()).get().m_groupWithRelationships;
                    hostBuilders[i] = getHostFromGroup(group, maxMissing > 0);
                } while (hostBuilders[i] == null);

                if (hostBuilders[i].m_missing) {
                    --maxMissing;
                }
            }

            for (HAGroupRelationship group : m_eligibleGroupsByToken.values()) {
                group.m_relationship.m_distance = 0;
                group.m_relationship.m_sharedAncestry = 0;
            }

            return hostBuilders;
        }

        @Override
        public BalancedPartitionChecker getBalancedPartitionChecker() {
            return m_balancedPartitionChecker;
        }

        /**
         * Retrieves the next {@link HostBuilder} from {@code group} which is available for partitions. Also, handles
         * when groups run out of hosts.
         * <p>
         * This lazily cleans up empty groups by first removing them from {@link #m_eligibleGroupsByToken} and
         * {@link #m_groupRelationships} and then the removal is detected by other groups when they are selected and the
         * distances are to be added to the groups in {@link #m_eligibleGroupsByToken}
         *
         * @param group {@link HAGroupWithRelationships} from which to retrieve a host
         * @return next {@link HostBuilder} from {@code group}
         */
        private HostBuilder getHostFromGroup(HAGroupWithRelationships group, boolean allowMissing) {
            Iterator<HAGroupRelationship> distanceIter = group.m_distances.iterator();

            m_groupRelationships.remove(group);

            while (distanceIter.hasNext()) {
                HAGroupRelationship distance = distanceIter.next();
                HAGroupRelationship accumulator = m_eligibleGroupsByToken.get(distance.getGroupToken());
                if (accumulator == null) {
                    distanceIter.remove();
                } else {
                    accumulator.m_relationship.m_distance += distance.m_relationship.m_distance;
                    accumulator.m_relationship.m_sharedAncestry += distance.m_relationship.m_sharedAncestry;
                }
            }

            HostBuilder host = group.m_group.pollHost(allowMissing);
            if (group.m_group.isEmpty()) {
                m_eligibleGroupsByToken.remove(group.m_group.m_token);
            } else {
                m_groupRelationships.add(group);
            }
            return host;
        }

        /**
         * Helper class which associates a {@link HAGroupWithRelationships} with the distance and shared parents count
         * relative to some other group or used for running accumulation.
         */
        static final class HAGroupRelationship implements Comparable<HAGroupRelationship> {
            final HAGroupWithRelationships m_groupWithRelationships;
            final RelationshipDescription m_relationship;

            HAGroupRelationship(HAGroupWithRelationships group, RelationshipDescription relationship) {
                m_groupWithRelationships = group;
                m_relationship = relationship;
            }

            String getGroupToken() {
                return m_groupWithRelationships.m_group.m_token;
            }

            @Override
            public int compareTo(HAGroupRelationship o) {
                return o == this ? 0
                        : ComparisonChain.start().compare(m_relationship, o.m_relationship)
                                .compare(m_groupWithRelationships.m_group, o.m_groupWithRelationships.m_group).result();
            }
        }

        /**
         * Tracks the {@link HAGroupRelationship} between a given group and all of the other eligible groups in the system.
         */
        static final class HAGroupWithRelationships implements Comparable<HAGroupWithRelationships> {
            final HAGroup m_group;
            final NavigableSet<HAGroupRelationship> m_distances = new TreeSet<>();

            static void associateGroups(HAGroupWithRelationships group1, HAGroupWithRelationships group2) {
                RelationshipDescription relationship = group1.m_group.getRelationshipTo(group2.m_group);

                group1.addRelationship(group2, relationship);
                group2.addRelationship(group1, relationship);
            }

            HAGroupWithRelationships(HAGroup m_group) {
                this.m_group = m_group;

                // Add this so that the accumulator sets shared ancestry high for this group to avoid being selected
                addRelationship(this, new RelationshipDescription(0, 1024));
            }

            void addRelationship(HAGroupWithRelationships group, RelationshipDescription relationship) {
                m_distances.add(new HAGroupRelationship(group, relationship));
            }

            @Override
            public int compareTo(HAGroupWithRelationships o) {
                return o == this ? 0
                        : ComparisonChain.start()
                                .compare(m_distances.last().m_relationship, o.m_distances.last().m_relationship)
                                .compare(m_group, o.m_group).result();
            }
        }
    }

    /**
     * Builder class for {@link Partition}
     */
    private static final class PartitionBuilder implements Comparable<PartitionBuilder> {
        final Integer m_id;
        final Set<HostBuilder> m_hosts = new HashSet<>();
        HostBuilder m_leader = null;

        PartitionBuilder(Integer id) {
            this.m_id = id;
        }

        Partition build() {
            return new Partition(m_id, m_leader.m_id, Collections2.transform(m_hosts, h -> h.m_id));
        }

        @Override
        public int compareTo(PartitionBuilder o) {
            return m_id.compareTo(o.m_id);
        }

        @Override
        public String toString() {
            String[] hostIdStrings = m_hosts.stream().map(h -> String.valueOf(h.m_id)).toArray(String[]::new);
            return String.format("Partition %d (leader %d, hosts %s)", m_id, m_leader == null ? -1 : m_leader.m_id,
                    String.join(",", hostIdStrings));
        }
    }

    /**
     * Builder class for {@link Host}
     */
    private static class HostBuilder implements Comparable<HostBuilder> {
        final Integer m_id;
        // a flag indicating if the host is missing or not
        final boolean m_missing;
        final String m_haGroup;
        final String m_ipAddress;
        final Set<PartitionBuilder> m_partitions = new TreeSet<>();
        int m_ledPartitionCount = 0;

        HostBuilder(Integer id, HostInfo info, boolean missing) {
            this.m_id = id;
            this.m_haGroup = info.m_group;
            this.m_ipAddress = info.m_hostIp;
            this.m_missing = missing;
        }

        @Override
        public int compareTo(HostBuilder o) {
            return m_ipAddress.compareTo(o.m_ipAddress);
        }

        @Override
        public String toString() {
            String[] partitionIdStrings = m_partitions.stream().map(p -> String.valueOf(p.m_id)).toArray(String[]::new);
            return String.format("Host %d ha:%s (Partitions %s)", m_id, m_haGroup, String.join(",", partitionIdStrings));
        }
    }

    private static final class RelationshipDescription implements Comparable<RelationshipDescription> {
        int m_distance;
        int m_sharedAncestry;

        public RelationshipDescription(int distance, int sharedAncestry) {
            super();
            m_distance = distance;
            m_sharedAncestry = sharedAncestry;
        }

        @Override
        public int compareTo(RelationshipDescription o) {
            return ComparisonChain.start().compare(m_distance, o.m_distance)
                    .compare(o.m_sharedAncestry, m_sharedAncestry).result();
        }
    }

    /**
     * Class to represent an ha group. Used by {@link TopologyBuilder} to construct a {@link AbstractTopology}. Keeps
     * track of which hosts are eligible for partition selection within the group.
     */
    private static class HAGroup implements Comparable<HAGroup> {
        final static Splitter GROUP_SPLITTER = Splitter.on('.');

        final String m_token;
        final List<String> m_tokenParts;
        private final Queue<HostBuilder> m_hosts = new PriorityQueue<>();
        private final Queue<HostBuilder> m_missingHosts = new ArrayDeque<>();

        HAGroup(String token) {
            super();
            this.m_token = token;
            this.m_tokenParts = GROUP_SPLITTER.splitToList(token);
        }

        RelationshipDescription getRelationshipTo(HAGroup other) {
            return getRelationshipTo(other.m_tokenParts);
        }

        RelationshipDescription getRelationshipTo(String otherGroupToken) {
            return getRelationshipTo(GROUP_SPLITTER.splitToList(otherGroupToken));
        }

        private RelationshipDescription getRelationshipTo(List<String> otherGroupTokenParts) {
            int size = Math.min(m_tokenParts.size(), otherGroupTokenParts.size());
            int sharedAncestry = 0;
            while (sharedAncestry < size) {
                if (!m_tokenParts.get(sharedAncestry).equals(otherGroupTokenParts.get(sharedAncestry))) {
                    break;
                }
                sharedAncestry++;
            }

            // distance is the sum of the two diverting path lengths
            int distance = m_tokenParts.size() + otherGroupTokenParts.size() - 2 * sharedAncestry;

            return new RelationshipDescription(distance, sharedAncestry);
        }

        void addHost(HostBuilder host) {
            if (host.m_missing) {
                m_missingHosts.offer(host);
            } else {
                m_hosts.offer(host);
            }
        }

        HostBuilder pollHost(boolean allowMissing) {
            return allowMissing && !m_missingHosts.isEmpty() ? m_missingHosts.poll() : m_hosts.poll();
        }

        boolean isEmpty() {
            return m_hosts.isEmpty() && m_missingHosts.isEmpty();
        }

        int size() {
            return m_hosts.size() + m_missingHosts.size();
        }

        @Override
        public int hashCode() {
            return m_token.hashCode();
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            HAGroup other = (HAGroup) obj;
            return m_token.equals(other.m_token);
        }

        @Override
        public int compareTo(HAGroup o) {
            return ComparisonChain.start().compare(size(), o.size()).compare(m_token, o.m_token)
                    .result();
        }
    }

    /**
     * Builder class for {@link AbstractTopology}. Determines which partitions should be placed on which hosts. Hosts
     * are grouped together into partition groups. Each group is as small as possible with the larger groups attempted
     * to be selected first and contain the partitions with the lowest IDs.
     */
    private static class TopologyBuilder {
        final List<HostBuilder> m_hosts;
        final List<PartitionBuilder> m_partitions;
        final int m_replicaCount;
        final int m_sitesPerHost;
        final PartitionGroupSelector m_selector;
        int m_missingCount;
        int m_unbalancedPartitionCount = 0;

        TopologyBuilder(Map<Integer, HostInfo> hosts, Set<Integer> missingHosts, int firstPartitionId,
                int replicaCount) {
            m_hosts = new ArrayList<>(hosts.size());
            m_replicaCount = replicaCount;
            m_missingCount = missingHosts.size();
            int sitesPerHost = -1;

            // Convert hosts passed in to HostBuilder and build the ha groups
            boolean equalDistance = true;
            int lastDistance = -1;
            Map<String, HAGroup> groups = new HashMap<>();
            for (Map.Entry<Integer, HostInfo> entry : hosts.entrySet()) {
                HostInfo hostInfo = entry.getValue();
                if (sitesPerHost == -1) {
                    sitesPerHost = hostInfo.m_localSitesCount;
                } else if (sitesPerHost != hostInfo.m_localSitesCount) {
                    throw new RuntimeException("Not all hosts have the same site count: " + hosts);
                }
                HAGroup group = groups.get(hostInfo.m_group);
                if (group == null) {
                    group = new HAGroup(hostInfo.m_group);
                    if (equalDistance) {
                        for (HAGroup neighbor : groups.values()) {
                            int distance = group.getRelationshipTo(neighbor).m_distance;
                            if (lastDistance == -1) {
                                lastDistance = distance;
                            } else if (lastDistance != distance) {
                                equalDistance = false;
                                break;
                            }
                        }
                    }
                    groups.put(hostInfo.m_group, group);
                }
                Integer hostId = entry.getKey();
                HostBuilder host = new HostBuilder(hostId, hostInfo, missingHosts.contains(hostId));
                group.addHost(host);
                m_hosts.add(host);
            }

            m_sitesPerHost = sitesPerHost;

            if (hosts.size() < replicaCount) {
                throw new RuntimeException(String.format("System requesting %d replicas "
                        + "but there are only %d eligable hosts on which to place them. " + "Topology request invalid.",
                        replicaCount, hosts.size()));
            }

            final int partitionsCount = sitesPerHost * hosts.size() / replicaCount;
            m_partitions = new ArrayList<>(partitionsCount);
            for (int i = firstPartitionId; i < firstPartitionId + partitionsCount; ++i) {
                m_partitions.add(new PartitionBuilder(i));
            }

            // Select the PartitionGroupSelector based on the ha groups
            if (groups.size() == 1) {
                m_selector = new SingleGroupPartitionGroupSelector(groups.values().iterator().next());
            } else if (equalDistance) {
                m_selector = new EqualDistantHaGroupsPartitionGroupSelector(groups.values(), replicaCount);
            } else {
                m_selector = new ComplexHaGroupsPartitionGroupSelector(groups.values(), replicaCount);
            }
        }

        /**
         * Distribute the partitions out to the hosts in this topology builder.
         *
         * @return {@code this}
         */
        TopologyBuilder addPartitionsToHosts() {
            int hostsNotInAGroup = m_hosts.size();
            BalancedPartitionChecker bpc = m_selector.getBalancedPartitionChecker();

            List<HostBuilder[]> partitionGroups = new ArrayList<>(hostsNotInAGroup / m_replicaCount);

            do {
                int partitionGroupCount = (hostsNotInAGroup / m_replicaCount);
                /*
                 * Calculate how many hosts should be in each group. Ideally this is replica count but with some host
                 * counts, sites per host and replica count values that is not possible. When it isn't possible try to
                 * make the groups as small as possible with the largest groups selected last to have the partitions
                 * with the lowest IDs
                 */
                int hostsInGroup = m_replicaCount + ((hostsNotInAGroup % m_replicaCount) / partitionGroupCount);

                // Keep adding hosts to group until the number of sites it can hold is a multiple of m_replicaCount
                while (hostsInGroup < hostsNotInAGroup && hostsInGroup * m_sitesPerHost % m_replicaCount != 0) {
                    ++hostsInGroup;
                }

                /*
                 * If there are not enough hosts left to make a new group toss them in this group.
                 */
                hostsNotInAGroup -= hostsInGroup;
                if (hostsNotInAGroup > 0 && hostsNotInAGroup < m_replicaCount) {
                    hostsInGroup += hostsNotInAGroup;
                    hostsNotInAGroup = 0;
                }

                // Accidentally created a partition group which is a multiple of m_replicaCount break it up
                if (hostsInGroup >= (m_replicaCount << 1) && hostsInGroup % m_replicaCount == 0) {
                    hostsNotInAGroup += (hostsInGroup - m_replicaCount);
                    hostsInGroup = m_replicaCount;
                }

                int maxMissing = m_missingCount / partitionGroupCount;
                if (maxMissing >= hostsInGroup) {
                    throw new RuntimeException("Too many missing hosts for configuration");
                }

                HostBuilder[] partitionGroup = m_selector.getNextPartitionGroup(hostsInGroup, maxMissing);
                partitionGroups.add(partitionGroup);

                if (maxMissing > 0) {
                    for (HostBuilder host : partitionGroup) {
                        if (host.m_missing) {
                            --m_missingCount;
                        }
                    }
                }
            } while (hostsNotInAGroup > 0);

            // Sort the partition groups biggest to smallest
            Collections.sort(partitionGroups, (pg1, pg2) -> Integer.compare(pg2.length, pg1.length));

            Iterator<PartitionBuilder> partitionIter = m_partitions.iterator();
            for (HostBuilder[] partitionGroup : partitionGroups) {
                int partitionsInGroup = partitionGroup.length * m_sitesPerHost / m_replicaCount;

                int counter = 0;
                List<HostBuilder> eligibleLeaders = new ArrayList<>(m_replicaCount);
                for (int i = 0; i < partitionsInGroup; ++i) {
                    PartitionBuilder partition = partitionIter.next();
                    for (int j = 0; j < m_replicaCount; ++j) {
                        HostBuilder host = partitionGroup[counter++ % partitionGroup.length];
                        assert (host.m_partitions.size() < m_sitesPerHost) : "host " + host + " already has at least "
                                + m_sitesPerHost + " partitions";
                        host.m_partitions.add(partition);
                        partition.m_hosts.add(host);
                        if (!host.m_missing) {
                            eligibleLeaders.add(host);
                        }
                    }

                    if (!bpc.isBalanced(partition)) {
                        ++m_unbalancedPartitionCount;
                    }

                    partition.m_leader = eligibleLeaders.stream()
                            .min((h1, h2) -> Integer.compare(h1.m_ledPartitionCount, h2.m_ledPartitionCount)).get();
                    ++partition.m_leader.m_ledPartitionCount;
                    eligibleLeaders.clear();
                }
            }

            assert !partitionIter.hasNext();

            return this;
        }

        int getReplicationFactor() {
            return m_replicaCount - 1;
        }
    }

    private static TopologyBuilder addPartitionsToHosts(Map<Integer, HostInfo> hostInfos, Set<Integer> missingHosts,
            int kfactor, int firstPartitionId) {
        return new TopologyBuilder(hostInfos, missingHosts, firstPartitionId, kfactor + 1).addPartitionsToHosts();
    }
    /////////////////////////////////////
    //
    // PUBLIC STATIC API
    //
    /////////////////////////////////////

    /**
     * Add new hosts to an existing topology and layout partitions on those hosts
     *
     * @param currentTopology to extend
     * @param newHostInfos    new hosts to add to topology
     * @return update {@link AbstractTopology} with new hosts and {@link ImmutableList} of new partition IDs
     * @throws RuntimeException if hosts are not valid for topology
     */
    public static Pair<AbstractTopology, ImmutableList<Integer>> mutateAddNewHosts(AbstractTopology currentTopology,
            Map<Integer, HostInfo> newHostInfos) {
        int startingPartitionId = getNextFreePartitionId(currentTopology);

        TopologyBuilder topologyBuilder = addPartitionsToHosts(newHostInfos, Collections.emptySet(),
                currentTopology.getReplicationFactor(), startingPartitionId);

        ImmutableList.Builder<Integer> newPartitions = ImmutableList.builder();
        for (PartitionBuilder pb : topologyBuilder.m_partitions) {
            newPartitions.add(pb.m_id);
        }

        return Pair.of(new AbstractTopology(currentTopology, topologyBuilder), newPartitions.build());
    }

    /**
     * Remove hosts from an existing topology
     *
     * @param currentTopology to extend
     * @param removalHostInfos   hosts to be removed from topology
     * @return update {@link AbstractTopology} with remaining hosts and removed partition IDs
     * @throws RuntimeException if hosts are not valid for topology
     */
    public static Pair<AbstractTopology, Set<Integer>> mutateRemoveHosts(AbstractTopology currentTopology,
                                                                         Set<Integer>  removalHosts) {
        Set<Integer> removalPartitionIds = getPartitionIdsForHosts(currentTopology, removalHosts);
        return Pair.of(new AbstractTopology(currentTopology, removalHosts, removalPartitionIds), removalPartitionIds);
    }

    /**
     * Create a new topology using {@code hosts}
     *
     * @param hostInfos    hosts to put in topology
     * @param missingHosts set of missing host IDs
     * @param kfactor      for cluster
     * @return {@link AbstractTopology} for cluster
     * @throws RuntimeException if hosts are not valid for topology
     */
    public static AbstractTopology getTopology(Map<Integer, HostInfo> hostInfos, Set<Integer> missingHosts,
            int kfactor) {
        return getTopology(hostInfos, missingHosts, kfactor, false);
    }

    /**
     * Create a new topology using {@code hosts}
     *
     * @param hostInfos    hosts to put in topology
     * @param missingHosts set of missing host IDs
     * @param kfactor      for cluster
     * @return {@link AbstractTopology} for cluster
     * @throws RuntimeException if hosts are not valid for topology
     */
    public static AbstractTopology getTopology(Map<Integer, HostInfo> hostInfos, Set<Integer> missingHosts,
            int kfactor, boolean restorePartition ) {
        TopologyBuilder builder = addPartitionsToHosts(hostInfos, missingHosts, kfactor, 0);
        AbstractTopology topo = new AbstractTopology(EMPTY_TOPOLOGY, builder);
        if (restorePartition && hostInfos.size() == topo.getHostCount()) {
            topo = mutateRestorePartitionsForRecovery(topo, hostInfos, missingHosts);
        }
        return topo;
    }

    /**
     * Best effort to find the matching host on the existing topology from ZK Use the placement group of the recovering
     * host to match a lost node in the topology
     *
     * @param topology       The topology
     * @param liveHosts      The live host ids
     * @param localHostId    The rejoining host id
     * @param placementGroup The rejoining placement group
     * @return recovered topology if a matching node is found
     */
    public static AbstractTopology mutateRecoverTopology(AbstractTopology topology, Set<Integer> liveHosts,
            int localHostId, String placementGroup, Set<Integer> recoverPartitions) {

         Host replaceHost = null;
         for (Host h : topology.hostsById.values()) {

             // filter out
             if (liveHosts.contains(h.id) || !h.haGroup.equalsIgnoreCase(placementGroup)) {
                 continue;
             }
             if (replaceHost == null) {
                 replaceHost = h;
             }

             // use the matched host. if no match found, use any one available.
             if (recoverPartitions != null && !recoverPartitions.isEmpty()) {
                 if (h.getPartitionIdList().equals(recoverPartitions)) {
                     replaceHost = h;
                     break;
                 }
             }
         }

         if (replaceHost == null) {
            return null;
         }

         Integer replaceHostId = replaceHost.id;

         ImmutableMap.Builder<Integer, Partition> partitionsByIdBuilder = ImmutableMap.builder();
         ArrayList<Integer> hostIds = new ArrayList<>();

         for (Map.Entry<Integer, Partition> entry : topology.partitionsById.entrySet()) {
             Partition partition = entry.getValue();
             boolean modified = false;
             for (Integer id : partition.hostIds) {
                 if (id.equals(replaceHostId)) {
                     modified = true;
                     hostIds.add(localHostId);
                 } else {
                     hostIds.add(id);
                 }
             }

             if (modified) {
                 partition = new Partition(partition.id,
                         replaceHostId.equals(partition.leaderHostId) ? localHostId : partition.leaderHostId, hostIds);
             }

             partitionsByIdBuilder.put(entry.getKey(), partition);
             hostIds.clear();
         }

         ImmutableMap<Integer, Partition> partitionsById = partitionsByIdBuilder.build();
         liveHosts.add(Integer.valueOf(localHostId));
         ImmutableMap.Builder<Integer, Host> hostsByIdBuilder = ImmutableMap.builder();
         for (Map.Entry<Integer, Host> entry : topology.hostsById.entrySet()) {
             Integer id = entry.getKey().intValue() == replaceHostId ? localHostId : entry.getKey();
             hostsByIdBuilder.put(id, new Host(entry.getValue(), id, partitionsById,
                             !liveHosts.contains(id)));
         }

         return new AbstractTopology(topology, hostsByIdBuilder.build(), partitionsById);
     }
    /////////////////////////////////////
    //
    // SERIALIZATION API
    //
    /////////////////////////////////////

    public JSONObject topologyToJSON() throws JSONException {
        JSONStringer stringer = new JSONStringer();
        stringer.object();

        stringer.keySymbolValuePair(TOPO_VERSION, version);

        stringer.key(TOPO_PARTITIONS).array();
        for (Partition partition : partitionsById.values()) {
            partition.toJSON(stringer);
        }
        stringer.endArray();

        stringer.key(TOPO_HOSTS).array();
        for (Host host : hostsById.values()) {
            host.toJSON(stringer);
        }
        stringer.endArray();
        stringer.keySymbolValuePair(TOPO_SITES_PER_HOST, getSitesPerHost());
        stringer.keySymbolValuePair(TOPO_REPLICATION_FACTOR, getReplicationFactor());
        stringer.keySymbolValuePair(TOPO_UNBALANCED_PART_COUNT, m_unbalancedPartitionCount);

        stringer.endObject();

        return new JSONObject(stringer.toString());
    }

    /**
     * Best effort to find the matching host on the existing topology from ZK Use the placement group of the recovering
     * host to match a lost node in the topology
     *
     * @param topo       The topology
     * @return recovered topology if a matching node is found
     */
    public static RangeMap<Integer, Set<Integer>> getPartitionGroupsFromTopology(AbstractTopology topo) {
        Set<Integer> visited = new HashSet<>();
        RangeMap<Integer, Set<Integer>> protectionGroups = TreeRangeMap.create();
        for (AbstractTopology.Host host : topo.hostsById.values()) {
            if (visited.contains(host.id) || host.partitions.isEmpty()) {
                continue;
            }
            Set<Integer> hosts = new HashSet<>();
            @SuppressWarnings("unchecked")
            Range<Integer>[] partitionRange = new Range[1];
            buildProtectionGroup(topo, visited, host, partitionRange, hosts);
            assert hosts.size() * topo.getSitesPerHost() / (topo.getReplicationFactor() + 1) == partitionRange[0].upperEndpoint() - partitionRange[0].lowerEndpoint() + 1;
            protectionGroups.put(partitionRange[0], hosts);
        }
        return protectionGroups;
    }

    private static void buildProtectionGroup(AbstractTopology topo, Set<Integer> visited, AbstractTopology.Host host,
                                             Range<Integer>[] partitionRange, Set<Integer> hosts) {
        if (!visited.add(host.id) || host.partitions.isEmpty()) {
            return;
        }

        Range<Integer> hostRange = Range.closed(host.partitions.first().id, host.partitions.last().id);
        partitionRange[0] = partitionRange[0] == null ? hostRange : partitionRange[0].span(hostRange);

        for (AbstractTopology.Partition partition : host.partitions) {
            for (Integer hostId : partition.hostIds) {
                if (hosts.add(hostId)) {
                    buildProtectionGroup(topo, visited, topo.hostsById.get(hostId), partitionRange, hosts);
                }
            }
        }
    }

    private static AbstractTopology mutateRestorePartitionsForRecovery(AbstractTopology topology,
            Map<Integer, HostInfo> hostInfos, Set<Integer> missingHosts) {
        Map<Set<Integer>, List<Integer>> restoredPartitionsByHosts = Maps.newHashMap();
        List<Integer> partitionsRestored = Lists.newArrayList();
        Boolean[] sphValid = {true};
        hostInfos.forEach((k, v) -> {
            Set<Integer> partitions = v.getRecoveredPartitions();
            if (partitions.size() != topology.m_sitesPerHost) {
                sphValid[0] = false;
            } else {
                restoredPartitionsByHosts.computeIfAbsent(partitions, p -> new ArrayList<>()).add(k);
                partitionsRestored.addAll(partitions);
            }
        });

        // Do not restore partition assignments if there are nodes without or kfactor + 1 restored partitions
        if (restoredPartitionsByHosts.isEmpty() || !sphValid[0]) {
            return topology;
        }

        //All partitions must have exact kfactor + 1 replicas
        boolean kfactorCheck = topology.partitionsById.values().stream().anyMatch(p->
                Collections.frequency(partitionsRestored, p.id) != (topology.m_replicationFactor + 1));
        if (kfactorCheck) {
            return topology;
        }

        Map<Integer, Partition> allPartitions = Maps.newHashMap(topology.partitionsById);
        Map<Integer, Host> hostsById = new TreeMap<>(topology.hostsById);
        for (Map.Entry<Set<Integer>, List<Integer>> entry : restoredPartitionsByHosts.entrySet()) {
            List<Integer> restoredHosts = entry.getValue();
            List<Integer> matchedHosts = new ArrayList<>();
            for (Iterator<Map.Entry<Integer, Host>> it = hostsById.entrySet().iterator(); it.hasNext();) {
                Host host = it.next().getValue();

                // host the same list of partitions but different host id, a candidate for swap
                Set<Integer> partitionIds = Sets.newHashSet(host.getPartitionIdList());
                if (partitionIds.equals(entry.getKey())) {
                    if (!restoredHosts.remove(Integer.valueOf(host.id))) {
                        matchedHosts.add(host.id);
                    } else {
                        it.remove();
                    }
                }
            }

            // all the partitions are on the right hosts or no matching layout found
            if (!restoredHosts.isEmpty() && !matchedHosts.isEmpty()) {

                // exchange partition assignments, one to one.
                if (restoredHosts.size() != matchedHosts.size()) {
                    return topology;
                }
                // found matching hosts, let us swap
                for(int i = 0; i < restoredHosts.size(); i++) {
                    Host restoredHost = hostsById.get(restoredHosts.get(i));
                    Host matchedHost = hostsById.get(matchedHosts.get(i));
                    allPartitions = relocatePartitions(allPartitions, restoredHost, matchedHost);
                    hostsById.remove(Integer.valueOf(restoredHost.id));
                }
            }
        }

        Map<Integer, List<Partition>> partitionsByHost = Maps.newHashMap();
        allPartitions.forEach((k, v) -> {
            for (Integer hostId : v.hostIds){
                partitionsByHost.computeIfAbsent(hostId, p -> new ArrayList<>()).add(v);
            }
        });
        ImmutableMap.Builder<Integer, Host> hostsByIdBuilder = ImmutableMap.builder();
        partitionsByHost.forEach((k, v) -> {
            hostsByIdBuilder.put(k, new Host(k, topology.hostsById.get(k).haGroup, ImmutableSortedSet.copyOf(v), missingHosts.contains(k)));
        });
        return new AbstractTopology(topology, hostsByIdBuilder.build(), ImmutableMap.copyOf(allPartitions));
    }

    private static Map<Integer, Partition> relocatePartitions(Map<Integer, Partition> allPartitions, Host restoredHost, Host matchedHost) {
        Map<Integer, Partition> partitions = Maps.newHashMap();
        for(Partition p : allPartitions.values()) {
            List<Integer> hostIds = Lists.newArrayList(p.hostIds);
            boolean relocated = false;
            int leaderHostId = p.leaderHostId;
            if (hostIds.contains(restoredHost.id) && !hostIds.contains(matchedHost.id)) {
                hostIds.remove(Integer.valueOf(restoredHost.id));
                hostIds.add(matchedHost.id);
                relocated = true;
                leaderHostId = (leaderHostId == restoredHost.id) ? matchedHost.id : leaderHostId;
            } else if (!hostIds.contains(restoredHost.id) && hostIds.contains(matchedHost.id)) {
                hostIds.remove(Integer.valueOf(matchedHost.id));
                hostIds.add(restoredHost.id);
                relocated = true;
                leaderHostId = (leaderHostId == matchedHost.id) ? restoredHost.id : leaderHostId;
            }
            if (relocated) {
                p = new Partition(p.id, leaderHostId, hostIds);
            }
            partitions.put(p.id, p);
        }
        return partitions;
    }

    public static AbstractTopology topologyFromJSON(String jsonTopology) throws JSONException {
        JSONObject jsonObj = new JSONObject(jsonTopology);
        return topologyFromJSON(jsonObj);
    }

    public static AbstractTopology topologyFromJSON(JSONObject jsonTopology) throws JSONException {
        ImmutableMap.Builder<Integer, Partition> partitionsByIdBuilder = ImmutableMap.builder();
        ImmutableMap.Builder<Integer, Host> hostsByIdBuilder = ImmutableMap.builder();

        long version = jsonTopology.getLong(TOPO_VERSION);

        JSONArray partitionsJSON = jsonTopology.getJSONArray(TOPO_PARTITIONS);
        for (int i = 0; i < partitionsJSON.length(); i++) {
            Partition partition = Partition.fromJSON(partitionsJSON.getJSONObject(i));
            partitionsByIdBuilder.put(partition.id, partition);
        }
        ImmutableMap<Integer, Partition> partitionsById = partitionsByIdBuilder.build();

        JSONArray hostsJSON = jsonTopology.getJSONArray(TOPO_HOSTS);
        for (int i = 0; i < hostsJSON.length(); i++) {
            Host host = Host.fromJSON(hostsJSON.getJSONObject(i), partitionsById);
            hostsByIdBuilder.put(host.id, host);
        }

        int sitesPerHost = jsonTopology.getInt(TOPO_SITES_PER_HOST);

        int replicationFactor = jsonTopology.getInt(TOPO_REPLICATION_FACTOR);

        int unbalancedPartitionCount = jsonTopology.getInt(TOPO_UNBALANCED_PART_COUNT);

        return new AbstractTopology(version, hostsByIdBuilder.build(), partitionsById, sitesPerHost, replicationFactor,
                unbalancedPartitionCount);
    }

    /////////////////////////////////////
    //
    // PRIVATE TOPOLOGY CONSTRUCTOR
    //
    /////////////////////////////////////
    // private construct for creating new or add new hosts
    private AbstractTopology(AbstractTopology existingTopology, TopologyBuilder topologyBuilder) {
        Preconditions.checkArgument(
                (existingTopology.getSitesPerHost() == -1 && topologyBuilder.m_sitesPerHost != -1)
                        || existingTopology.getSitesPerHost() == topologyBuilder.m_sitesPerHost,
                "Sites per host is not the same for all hosts: " + existingTopology.getSitesPerHost() + " vs "
                        + topologyBuilder.m_sitesPerHost);
        assert ((existingTopology.getReplicationFactor() == -1 && topologyBuilder.getReplicationFactor() != -1)
                || existingTopology.getReplicationFactor() == topologyBuilder.getReplicationFactor());
        version = existingTopology.version + 1;

        ImmutableMap.Builder<Integer, Partition> partitionBuilder = ImmutableMap.builder();
        for (Partition partition : Iterables.concat(existingTopology.partitionsById.values(),
                Collections2.transform(topologyBuilder.m_partitions, PartitionBuilder::build))) {
            partitionBuilder.put(partition.id, partition);
        }

        partitionsById = partitionBuilder.build();

        ImmutableMap.Builder<Integer, Host> hostsBuilder = ImmutableMap.builder();
        for (Host host : Iterables.concat(existingTopology.hostsById.values(),
                Collections2.transform(topologyBuilder.m_hosts, h -> new Host(h, partitionsById)))) {
            hostsBuilder.put(host.id, host);
        }
        try {
            hostsById = hostsBuilder.build();
        } catch (IllegalArgumentException e) {
            throw new RuntimeException("must contain unique and unused hostid", e);
        }
        this.m_sitesPerHost = topologyBuilder.m_sitesPerHost;
        m_replicationFactor = topologyBuilder.getReplicationFactor();
        m_unbalancedPartitionCount = existingTopology.m_unbalancedPartitionCount
                + topologyBuilder.m_unbalancedPartitionCount;
    }

    // private construct for removing host from existing valid Topology
    private AbstractTopology(AbstractTopology existingTopology, Set<Integer> removalHosts, Set<Integer> removalPartitionIds) {
        Preconditions.checkArgument(!removalHosts.isEmpty(),"Given remove host set is empty.");
        Preconditions.checkArgument(existingTopology.getSitesPerHost() != -1,"Trying to remove from uninitialized Topology.");

        assert(!removalHosts.isEmpty() && existingTopology.getSitesPerHost() != -1 );
        version = existingTopology.version + 1;

        ImmutableMap.Builder<Integer, Partition> partitionBuilder = ImmutableMap.builder();
        partitionBuilder.putAll(
                Iterables.filter(existingTopology.partitionsById.entrySet(), item -> !removalPartitionIds.contains(item.getKey())));
        partitionsById = partitionBuilder.build();

        ImmutableMap.Builder<Integer, Host> hostsBuilder = ImmutableMap.builder();
        hostsBuilder.putAll(
                Iterables.filter(existingTopology.hostsById.entrySet(), item -> !removalHosts.contains(item.getKey())));
        try {
            hostsById = hostsBuilder.build();
        } catch (IllegalArgumentException e) {
            // shouldn't happen for the remove case
            throw new RuntimeException("must contain unique and unused hostid", e);
        }
        this.m_sitesPerHost = existingTopology.m_sitesPerHost;
        m_replicationFactor = existingTopology.getReplicationFactor();
        m_unbalancedPartitionCount = existingTopology.m_unbalancedPartitionCount;
    }

    // private construct for recovery
    private AbstractTopology(AbstractTopology existingTopology, ImmutableMap<Integer, Host> hostsById,
            ImmutableMap<Integer, Partition> partitionsById) {
        this(existingTopology.version + 1, hostsById, partitionsById, existingTopology.getSitesPerHost(),
                existingTopology.getReplicationFactor(), existingTopology.m_unbalancedPartitionCount);
    }

    private AbstractTopology(long version, ImmutableMap<Integer, Host> hostsById,
            ImmutableMap<Integer, Partition> partitionsById, int sitesPerHost, int replicationFactor,
            int unbalancedPartitionCount) {
        assert (version >= 0);

        this.version = version;
        this.hostsById = hostsById;
        this.partitionsById = partitionsById;

        this.m_sitesPerHost = sitesPerHost;
        this.m_replicationFactor = replicationFactor;
        this.m_unbalancedPartitionCount = unbalancedPartitionCount;
    }

    /////////////////////////////////////
    //
    // PRIVATE STATIC HELPER METHODS
    //
    /////////////////////////////////////
    private static Set<Integer> getPartitionIdsForHosts(AbstractTopology topology, Set<Integer> hostIds) {
        Set<Integer> partitionIds = new HashSet<>();
        for (int hostId : hostIds) {
            partitionIds.addAll(topology.getPartitionIdList(hostId));
        }
        return partitionIds;
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

    public int getHostCount() {
        return hostsById.size();
    }

    public int getPartitionCount() {
        return partitionsById.size();
    }

    /**
     * get all the hostIds in the partition group
     * contain the host(s) that have highest partition id
     * @return all the hostIds in the partition group
     */
    public Set<Integer> getPartitionGroupPeersContainHighestPid() {
        // find highest partition
        int hPid = getPartitionCount() -1;

        // find the host that contains the highest partition
        Collection<Integer> hHostIds = getHostIdList(hPid);
        if (hHostIds == null || hHostIds.isEmpty()) {
            return Collections.emptySet();
        }
        int hHostId = hHostIds.iterator().next();
        return getPartitionGroupPeers(hHostId);
    }

    /**
     * get all the hostIds in the partition group where the host with the given host id belongs
     * @param hostId the given hostId
     * @return all the hostIds in the partition group
     */
    public Set<Integer> getPartitionGroupPeers(int hostId) {
        Set<Integer> peers = Sets.newHashSet();
        for (Partition p : hostsById.get(hostId).partitions) {
            peers.addAll(p.hostIds);
        }
        return peers;
    }

    public List<Integer> getPartitionIdList(int hostId) {
        Host h = hostsById.get(hostId);
        return (h != null) ? h.getPartitionIdList() : null;
    }

    public Collection<Integer> getHostIdList(int partitionId) {
        Partition p = partitionsById.get(partitionId);
        return (p != null) ? p.hostIds : null;
    }

    public int getSitesPerHost() {
        return m_sitesPerHost;
    }

    public int getReplicationFactor() {
        return m_replicationFactor;
    }

    public boolean hasMissingPartitions() {
        Set<Partition> partitions = Sets.newHashSet();
        for (Host host : hostsById.values()) {
            if (!host.isMissing) {
                partitions.addAll(host.partitions);
            }
        }
        return getPartitionCount() > partitions.size();
    }

    /**
     * Sort all nodes in reverse hostGroup distance order, then group by rack-aware group, local host id is excluded.
     * @param hostId the local host id
     * @param hostGroups a host id to group map
     * @return sorted grouped host ids from farthest to nearest
     */
    public static List<Collection<Integer>> sortHostIdByHGDistance(int hostId, Map<Integer, String> hostGroups) {
        String localHostGroup = hostGroups.get(hostId);
        Preconditions.checkArgument(localHostGroup != null);

        HAGroup localHaGroup = new HAGroup(localHostGroup);

        // Memorize the distance, map the distance to host ids.
        Multimap<Integer, Integer> distanceMap = MultimapBuilder.treeKeys(Comparator.<Integer>naturalOrder().reversed())
                .arrayListValues().build();
        for (Map.Entry<Integer, String> entry : hostGroups.entrySet()) {
            if (hostId == entry.getKey()) {
                continue;
            }
            distanceMap.put(localHaGroup.getRelationshipTo(entry.getValue()).m_distance, entry.getKey());
        }

        return new ArrayList<>(distanceMap.asMap().values());
    }

    /**
     * The default placement group (a.k.a. rack-aware group) is "0", if user override the setting
     * in command line configuration, we need to check whether the partition layout meets the
     * requirement (tolerate entire rack loss without shutdown the cluster). And also because we
     * support partition group by default, if we can meet both requirements ( we prefer partition
     * group because online upgrade with minimum hardware option needs it), at least we need tell
     * user the fact.
     *
     * @return null if the topology is balanced, otherwise return the error message
     */
    public String validateLayout(Set<Integer> liveHosts) {
        if (m_unbalancedPartitionCount > 0) {
            return String.format("%d out of %d partitions are unbalanced across placement groups.",
                    m_unbalancedPartitionCount, partitionsById.size());
        }

        if (liveHosts == null) {
            return null;
        }

        // verify the partition leaders on live hosts
        for (Host host : hostsById.values()) {
            if (liveHosts.contains(Integer.valueOf(host.id))) {
                for (Partition p : host.partitions) {
                    if (!liveHosts.contains(Integer.valueOf(p.leaderHostId))) {
                        return String.format("The leader host %d of partition %d is not on live host.",
                                p.leaderHostId, p.id);
                    }
                }
            }
        }
        return null;
    }
}
