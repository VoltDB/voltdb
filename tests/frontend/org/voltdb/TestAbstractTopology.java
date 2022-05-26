/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

package org.voltdb;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.voltcore.messaging.HostMessenger.HostInfo;
import org.voltcore.utils.Pair;
import org.voltdb.AbstractTopology.Host;
import org.voltdb.AbstractTopology.Partition;
import org.voltdb.test.utils.RandomTestRule;

import com.google.common.collect.Iterables;
import com.google_voltpatches.common.base.Joiner;
import com.google_voltpatches.common.collect.ContiguousSet;
import com.google_voltpatches.common.collect.DiscreteDomain;
import com.google_voltpatches.common.collect.HashMultimap;
import com.google_voltpatches.common.collect.ImmutableList;
import com.google_voltpatches.common.collect.ImmutableMap;
import com.google_voltpatches.common.collect.Iterators;
import com.google_voltpatches.common.collect.Lists;
import com.google_voltpatches.common.collect.Maps;
import com.google_voltpatches.common.collect.Multimap;
import com.google_voltpatches.common.collect.Range;
import com.google_voltpatches.common.collect.RangeMap;

public class TestAbstractTopology {

    @Rule
    public RandomTestRule random = new RandomTestRule();

    static class TestDescription {
        Map<Integer, HostInfo> hosts;
        int kfactor;
        int expectedPartitionGroups = -1;
        int expectedMaxReplicationPerHAGroup = -1;

        @Override
        public String toString() {
            // group hosts by sph
            String[] bySPH = hosts.values().stream().collect(Collectors.groupingBy(h -> h.m_localSitesCount)).entrySet()
                    .stream().map(e -> String.format("sph=%d (%d)", e.getKey(), e.getValue().size()))
                    .toArray(String[]::new);
           String hostString = String.join(", ", bySPH);

           // hagroups
            String[] haGroups = hosts.values().stream().collect(Collectors.groupingBy(h -> h.m_group)).entrySet()
                    .stream().map(e -> String.format("%s (%d)", e.getKey(), e.getValue().size()))
                    .toArray(String[]::new);
           String haGroupsString = String.join(", ", haGroups);

           return "TEST:\n" +
                   "  Hosts                     " + hostString + "\n" +
                   "  K-Factor                  " + kfactor + '\n' +
                   "  HAGroups                  " + haGroupsString + "\n" +
                   "  Expected Partition Groups " + String.valueOf(expectedPartitionGroups) + "\n" +
                   "  Expected Max Repl Groups  " + String.valueOf(expectedMaxReplicationPerHAGroup);
        }
    }

    static class Metrics {
        long testTimeMS = 0;
        long topoTomeMS = 0;
        int distinctPeerGroups = 0;
        int partitionsThatDontSpanHAGroups = 0;
        int haGroupCount = 0;
        double avgReplicationPerHAGroup = 0;
        int maxReplicationPerHAGroup = 0;
        int maxLeaderCount = 0;
        int minLeaderCount = 0;

        @Override
        public String toString() {
            return "METRICS:\n" +
                   "  Test Time MS              " + String.valueOf(testTimeMS) + "\n" +
                   "  Topo Time MS              " + String.valueOf(testTimeMS) + "\n" +
                   "  Distinct Peer Groups      " + String.valueOf(distinctPeerGroups) + "\n" +
                   "  Non-Spanning Partitions   " + String.valueOf(partitionsThatDontSpanHAGroups) + "\n" +
                   "  HA Group Count            " + String.valueOf(haGroupCount) + "\n" +
                   "  AVG Replication Per Group " + String.valueOf(avgReplicationPerHAGroup) + "\n" +
                   "  MAX Replication Per Group " + String.valueOf(maxReplicationPerHAGroup) + "\n" +
                   "  MAX Leadership Impalance  " + String.format("%d (%d-%d)", maxLeaderCount - minLeaderCount,
                                                                  minLeaderCount, maxLeaderCount);
        }
    }

    Metrics validate(AbstractTopology topo) throws JSONException {
        return validate(topo, Iterators.singletonIterator(0));
    }

    Metrics validate(AbstractTopology topo, Iterator<Integer> firstPartitionsOnNewHosts) throws JSONException {
        Metrics metrics = new Metrics();

        // technically valid
        if (topo.hostsById.size() == 0) {
            return metrics;
        }

        // check partitions and hosts are mirrored
        topo.partitionsById.values().forEach(p -> {
            p.getHostIds().stream().forEach(hid -> {
                Host h = topo.hostsById.get(hid);
                assertNotNull(h);
                assertTrue(h.getPartitions().contains(p));
                // check hosts the other direction
                h.getPartitions().forEach(p2 -> {
                    Partition p3 = topo.partitionsById.get(p2.id);
                    assertNotNull(p3);
                    assertEquals(p2, p3);
                });
                assertFalse("Leader host is missing", p.getLeaderHostId() == h.id && h.isMissing);
            });
            // check k+1 copies of partition
            assertEquals(topo.getReplicationFactor() + 1, p.getHostIds().size());
        });

        // examine ha group placement
        Multimap<String, Host> haGroupToHosts = HashMultimap.create();
        for (Host host : topo.hostsById.values()) {
            haGroupToHosts.put(host.haGroup, host);
        }
        metrics.haGroupCount = haGroupToHosts.size();
        int sumOfReplicaCounts = 0;

        for (Map.Entry<String, Collection<Host>> entry : haGroupToHosts.asMap().entrySet()) {
            // get all partitions in ha group, possibly more than once
            List<Partition> partitions = entry.getValue().stream().flatMap(h -> h.getPartitions().stream())
                    .collect(Collectors.toList());

            // skip ha groups with no partitions
            // but this will be counted in the average metrics (so be careful)
            if (partitions.size() == 0) {
                continue;
            }

            // count up how many copies of each partition
            Map<Partition, Long> counts = partitions.stream()
                    .collect(
                            Collectors.groupingBy(
                                    Function.identity(), Collectors.counting()));
            // only care about max count for each ha group
            int replicaCount = counts.values().stream()
                    .mapToInt(l -> l.intValue())
                    .max()
                    .getAsInt();
            sumOfReplicaCounts += replicaCount;
            if (replicaCount > metrics.maxReplicationPerHAGroup) {
                metrics.maxReplicationPerHAGroup = replicaCount;
            }
        }
        metrics.avgReplicationPerHAGroup = sumOfReplicaCounts / (double) metrics.haGroupCount;

        // find min and max leaders
        if (topo.hostsById.size() > 0) {
            metrics.maxLeaderCount = topo.hostsById.values().stream()
                    .mapToInt(host -> (int) host.getPartitions().stream().filter(p -> p.getLeaderHostId() == host.id).count())
                    .max().getAsInt();
            metrics.minLeaderCount = topo.hostsById.values().stream()
                    .mapToInt(host -> (int) host.getPartitions().stream().filter(p -> p.getLeaderHostId() == host.id).count())
                    .min().getAsInt();
        }

        // collect distinct peer groups
        RangeMap<Integer, Set<Integer>> protectionGroups = AbstractTopology.getPartitionGroupsFromTopology(topo);
        metrics.distinctPeerGroups = protectionGroups.asMapOfRanges().size();

        // Validate that the protection groups get smaller or stay the same size as partitions IDs get higher
        int groupSize = 0;
        for (Map.Entry<Range<Integer>, Set<Integer>> entry : protectionGroups.asMapOfRanges().entrySet()) {
            Set<Integer> hosts = entry.getValue();
            if (groupSize < hosts.size()) {
                Integer firstPartition;
                do {
                    assertTrue("groupSize decreased from " + groupSize + " to " + hosts.size(),
                            firstPartitionsOnNewHosts.hasNext());
                    firstPartition = firstPartitionsOnNewHosts.next();
                } while (firstPartition.compareTo(entry.getKey().lowerEndpoint()) < 0);

                assertTrue("groupSize decreased from " + groupSize + " to " + hosts.size(),
                        entry.getKey().lowerEndpoint().equals(firstPartition));
            }
            groupSize = hosts.size();
        }

        JSONObject json = topo.topologyToJSON();
        AbstractTopology deserialized = AbstractTopology.topologyFromJSON(json);
        assertEquals(topo.getHostCount(), deserialized.getHostCount());
        assertEquals(topo.getPartitionCount(), deserialized.getPartitionCount());
        assertEquals(topo.getReplicationFactor(), deserialized.getReplicationFactor());
        assertEquals(topo.getSitesPerHost(), deserialized.getSitesPerHost());
        assertEquals(topo.hasMissingPartitions(), deserialized.hasMissingPartitions());

        return metrics;
    }

    @Test
    public void testOneNode() throws JSONException {
        TestDescription td = getBoringDescription(1, 5, 0, 1, 1);
        subTestDescription(td, false);
    }

    @Test
    public void testTwoNodesTwoRacks() throws JSONException {
        TestDescription td = getBoringDescription(2, 7, 1, 2, 2);
        AbstractTopology topo = subTestDescription(td, false);
        validate(topo);
    }

    @Test
    public void testThreeNodeOneRack() throws JSONException {
        TestDescription td = getBoringDescription(3, 4, 1, 1, 1);
        subTestDescription(td, false);
    }

    @Test
    public void testFiveNodeK1OneRack() throws JSONException {
        TestDescription td = getBoringDescription(5, 2, 1, 1, 1);
        subTestDescription(td, true);
    }

    @Test
    public void testFiveNodeK1TwoRacks() throws JSONException {
        TestDescription td = getBoringDescription(5, 6, 1, 1, 2);
        subTestDescription(td, false);
    }

    @Test
    public void testKTooLarge() {
        Map<Integer, HostInfo> hosts = ImmutableMap.of(0, new HostInfo("", "0", 2), 1, new HostInfo("", "0", 2), 2,
                new HostInfo("", "0", 2));

        try {
            AbstractTopology.getTopology(hosts, Collections.emptySet(), 3);
            fail();
        }
        catch (Exception e) {
            if (e.getMessage() == null || !e.getMessage().contains("Topology request invalid")) {
                throw e;
            }
        }
    }

    @Test
    public void testNonUniqueHostIds() {
        // now try adding on from existing topo
        TestDescription td = getBoringDescription(5, 6, 1, 2, 1);
        AbstractTopology topo = AbstractTopology.getTopology(td.hosts, Collections.emptySet(), td.kfactor);

        int lastHostId = td.hosts.keySet().stream().mapToInt(Integer::intValue).max().getAsInt();
        Map<Integer, HostInfo> hosts = ImmutableMap.of(lastHostId, new HostInfo("", "0", 6), lastHostId + 1,
                new HostInfo("", "0", 6));
        try {
            AbstractTopology.mutateAddNewHosts(topo, hosts);
            fail();
        }
        catch (Exception e) {
            assertTrue(e.getMessage().contains("must contain unique and unused hostid"));
        }
    }

    @Test
    public void testSortByDistance() {
        Map<Integer, String> hostGroups = new HashMap<Integer, String>();
        hostGroups.put(0, "rack1.node1");
        hostGroups.put(1, "rack1.node2");
        hostGroups.put(2, "rack1.node3");
        hostGroups.put(3, "rack2.node1");
        hostGroups.put(4, "rack2.node2");
        hostGroups.put(5, "rack2.node3");
        hostGroups.put(6, "rack1.node1.blade1");
        hostGroups.put(7, "rack1.node1.blade2");

        // Remember some prior knowledge
        Map<Integer, List<Integer>> distanceVector = Maps.newHashMap();
        distanceVector.put(0, new ArrayList<Integer>(Arrays.asList(0, 2, 2, 4, 4, 4, 1, 1)));
        distanceVector.put(1, new ArrayList<Integer>(Arrays.asList(2, 0, 2, 4, 4, 4, 3, 3)));
        distanceVector.put(2, new ArrayList<Integer>(Arrays.asList(2, 2, 0, 4, 4, 4, 3, 3)));
        distanceVector.put(3, new ArrayList<Integer>(Arrays.asList(4, 4, 4, 0, 2, 2, 5, 5)));
        distanceVector.put(4, new ArrayList<Integer>(Arrays.asList(4, 4, 4, 2, 0, 2, 5, 5)));
        distanceVector.put(5, new ArrayList<Integer>(Arrays.asList(4, 4, 4, 2, 2, 0, 5, 5)));
        distanceVector.put(6, new ArrayList<Integer>(Arrays.asList(1, 3, 3, 5, 5, 5, 0, 2)));
        distanceVector.put(7, new ArrayList<Integer>(Arrays.asList(1, 3, 3, 5, 5, 5, 2, 0)));
        // Verify the order is correct
        for (int i = 0; i < hostGroups.size(); i++) {
            List<Collection<Integer>> sortedList = AbstractTopology.sortHostIdByHGDistance(i, hostGroups);
            TreeMap<Integer, List<Integer>> sortedMap = Maps.newTreeMap();
            int index = -1;
            for (Integer distance : distanceVector.get(i)) {
                index++;
                if (distance == 0) {
                    continue;
                }
                List<Integer> hids = sortedMap.get(distance);
                if (hids == null) {
                    hids = com.google_voltpatches.common.collect.Lists.newArrayList();
                    sortedMap.put(distance, hids);
                }
                hids.add(index);
            }
            assertTrue(sortedList.size() == sortedMap.size());
            Deque<Integer> zippedList = Lists.newLinkedList();
            sortedList.forEach(l -> zippedList.addAll(l));
            Map.Entry<Integer, List<Integer>> entry;
            while ((entry = sortedMap.pollLastEntry()) != null) {
                for (Integer host : entry.getValue()) {
                    assertEquals(host, zippedList.pollFirst());
                }
            }
        }
    }

    ////////////////////////////////////////////////////////////////////////
    // Run this ignored unit test to manually test specific configuration.
    ////////////////////////////////////////////////////////////////////////
    @Ignore
    @Test
    public void testSpecificConfiguration() throws JSONException {
        //////////////////////////////////////////////////////////////////////////
        // Change the configuration here
        int totalNodeCount = 0;
        int sph = 0;
        int k = 0;
        int rackCount = 0;
        //////////////////////////////////////////////////////////////////////////
        // treeWidth > 1 means the group contains subgroup
        List<String> haGroups = getHAGroupTagTree(1, rackCount);
        TestDescription td = new TestDescription();
        td.hosts = new HashMap<>();
        for (int i = 0; i < totalNodeCount; i++) {
            td.hosts.put(i, new HostInfo("", haGroups.get(i % haGroups.size()), sph));
        }
        td.kfactor = k;

        td.expectedPartitionGroups = sph == 0 ? 0 : totalNodeCount / (k + 1);
        AbstractTopology topo = subTestDescription(td, true);

        // see if partition layout is balanced
        String err;
        if ((err = topo.validateLayout(null)) != null) {
            System.out.println(err);
            System.out.println(topo.topologyToJSON());
        }
        assertTrue(err == null); // expect balanced layout
    }

    @Test
    public void testRandomHAGroups() throws JSONException {
        for (int i = 0; i < 200; i++) {
            runRandomHAGroupsTest();
        }
    }

    // Generate random but valid configurations feature both partition group
    // and rack-aware group attributes.
    // A valid configuration means:
    // 1) each rack contains same number of nodes,
    // 2) number of replica copies is divisible by number of racks.
    private void runRandomHAGroupsTest() throws JSONException {
        final int MAX_RACKS = 5;
        final int MAX_RACK_NODES = 10;
        final int MAX_K = 10;
        final int MAX_PARTITIONS = 20;
        int rackCount = random.nextInt(MAX_RACKS) + 1; // [1-5]
        int rackNodeCount = random.nextInt(MAX_RACK_NODES) + 1; // [1-10]
        int totalNodeCount = rackNodeCount * rackCount;
        int k;
        do {
            k = random.nextInt(rackCount - 1, Math.min(totalNodeCount, MAX_K)); // [rackCount - 1, 10]
        } while ((k + 1) % rackCount != 0);
        int sph;
        do {
            sph = random.nextInt(MAX_PARTITIONS) + 1; // [1-20]
        } while ((totalNodeCount * sph) % (k + 1) != 0);
        //////////////////////////////////////////////////////////////////////////
        System.out.println(
                String.format("Node count: %d, kfactor: %d, SPH: %d, # of racks: %d",
                              totalNodeCount, k, sph, rackCount));
        //////////////////////////////////////////////////////////////////////////
        // treeWidth > 1 means the group contains subgroup
        List<String> haGroups = getHAGroupTagTree(1, rackCount);
        TestDescription td = new TestDescription();
        td.hosts = new HashMap<>();
        for (int i = 0; i < totalNodeCount; i++) {
            td.hosts.put(i, new HostInfo("", haGroups.get(i % haGroups.size()), sph, ""));
        }
        td.kfactor = k;
        td.expectedPartitionGroups = sph == 0 ? 0 : totalNodeCount / (k + 1);
        AbstractTopology topo = subTestDescription(td, false);

        // see if partition layout is balanced
        String err;
        if ((err = topo.validateLayout(null)) != null) {
            System.out.println(err);
            System.out.println(topo.topologyToJSON());
        }
        assertTrue(err == null); // expect balanced layout
    }

    private List<String> getHAGroupTagTree(int treeWidth, int leafCount) {
        // create a set of ha group paths
        List<String> haGroupTags = new ArrayList<>();
        int height = treeWidth == 1 ? 1 : (int) Math.ceil(Math.log(leafCount) / Math.log(treeWidth));
        for (int i = 0; i < leafCount; i++) {
            String tag = String.valueOf(i);
            int nextWidth = leafCount;
            for (int depth = 1; depth < height; depth++) {
                nextWidth /= treeWidth;
                int nextTag = i % nextWidth;
                tag = String.valueOf(nextTag) + "." + tag;
            }
            haGroupTags.add(tag);
        }

        /*for (String tag : haGroupTags) {
            System.out.println(tag);
        }*/

        return haGroupTags;
    }

    private TestDescription getBoringDescription(int nodeCount, int sph, int k, int treeWidth, int leafCount) {
        return getBoringDescription(nodeCount, sph, k, treeWidth, leafCount, 0);
    }

    private TestDescription getBoringDescription(int nodeCount, int sph, int k, int treeWidth, int leafCount, int hostIdOffset) {
        TestDescription td = new TestDescription();

        List<String> haGroupTags = getHAGroupTagTree(treeWidth, leafCount);

        td.hosts = new HashMap<>();
        for (int i = 0; i < nodeCount; i++) {
            td.hosts.put(i + hostIdOffset, new HostInfo("", haGroupTags.get(i % haGroupTags.size()), sph));
        }

        td.kfactor = k;

        td.expectedMaxReplicationPerHAGroup = sph == 0 ? 0 : (int) Math.ceil((nodeCount / (double) (k + 1)) / leafCount);
        td.expectedPartitionGroups = sph == 0 ? 0 : nodeCount / (k + 1);

        return td;
    }

    private TestDescription getChaoticDescription() {
        final int MAX_NODE_COUNT = 120;
        final int MAX_K = 10;

        TestDescription td = new TestDescription();

        // for now, forget about ha groups
        List<String> haGroupTags = new ArrayList<>();
        haGroupTags.add("0");

        // start with a random node count
        int nodeCount = random.nextInt(MAX_NODE_COUNT) + 1; // 1 .. MAX_NODE_COUNT
        assert(nodeCount > 0);

        int sph = random.nextInt(MAX_K) + 1;
        td.kfactor = random.nextInt(Math.min(MAX_K + 1, nodeCount));

        td.hosts = new HashMap<>();
        for (int i = 0; i < nodeCount; i++) {
            int haIndex = random.nextInt(haGroupTags.size());
            td.hosts.put(i, new HostInfo("", haGroupTags.get(haIndex), sph));
        }

        td.expectedMaxReplicationPerHAGroup = 0;
        td.expectedPartitionGroups = Integer.MAX_VALUE;

        return td;
    }

    private AbstractTopology subTestDescription(TestDescription td, boolean print) throws JSONException {
        System.out.println(td.toString());

        long start = System.currentTimeMillis();

        AbstractTopology topo;
        //System.out.println(topo.topologyToJSON());

        topo = AbstractTopology.getTopology(td.hosts, Collections.emptySet(), td.kfactor);

        if (print) {
            System.out.println(topo.topologyToJSON());
        }
        long subEnd = System.currentTimeMillis();

        Metrics metrics = validate(topo);
        long end = System.currentTimeMillis();
        metrics.testTimeMS = end - start;
        metrics.topoTomeMS = subEnd - start;

        if (print) {
            System.out.println(metrics.toString());
        }

        //assertEquals(td.expectedMaxReplicationPerHAGroup, metrics.maxReplicationPerHAGroup);
        assertTrue(metrics.distinctPeerGroups <= td.expectedPartitionGroups);

        return topo;
    }

    private TestDescription getRandomBoringTestDescription() {
        return getRandomBoringTestDescription(0);
    }

    private TestDescription getRandomBoringTestDescription(int hostIdOffset) {
        final int MAX_NODE_COUNT = 120;
        final int MAX_SPH = 60;
        final int MAX_K = 10;

        int hostCount, k, sph;
        hostCount = random.nextInt(MAX_NODE_COUNT) + 1;

        k = random.nextInt(Math.min(hostCount - 1, MAX_K) + 1);
        do {
            sph = random.nextInt(MAX_SPH) + 1;
        }
        while ((sph * hostCount) % (k + 1) != 0);
        return getRandomBoringTestDescription(hostCount, sph, k, hostIdOffset);
    }

    private TestDescription getRandomBoringTestDescription(int sph, int k, int hostIdOffset) {
        final int MAX_NODE_COUNT = 120;

        int hostCount;
        do {
            hostCount = random.nextInt(MAX_NODE_COUNT) + 1;
        } while (hostCount < k + 1 || (sph * hostCount) % (k + 1) != 0);
        return getRandomBoringTestDescription(hostCount, sph, k, hostIdOffset);
    }

    private TestDescription getRandomBoringTestDescription(int hostCount, int sph, int k, int hostIdOffset) {
        int leafCount, treeWidth;

        ArrayList<Integer> leafOptions = new ArrayList<>();
        for (int leafCountOption = k + 1;
            leafCountOption <= hostCount;
            leafCountOption = Math.max(leafCountOption *= (k + 1), leafCountOption + 1)) {
            leafOptions.add(leafCountOption);
        }
        leafCount = hostCount == 0 ? 0 : leafOptions.get(random.nextInt(leafOptions.size()));

        treeWidth = hostCount == 0 ? 0 : random.nextInt(leafCount) + 1;

        return getBoringDescription(hostCount, sph, k, treeWidth, leafCount, hostIdOffset);
    }

    @Test
    public void testManyBoringClusters() throws JSONException {
        for (int i = 0; i < 1000; i++) {
            TestDescription td = getRandomBoringTestDescription();
            AbstractTopology topo = subTestDescription(td, false);

            // kill and rejoin a host
            if (td.hosts.isEmpty()) {
                continue;
            }
            validate(topo);
        }
    }

    void mutateSwapHAGroups(TestDescription td) {
        if (td.hosts.isEmpty()) {
            return;
        }

        int consolidationCount = random.nextInt(td.hosts.size()) + 1;

        for (int i = 0; i < consolidationCount; i++) {
            int hostIndexToCopyTo = random.nextInt(td.hosts.size());
            int hostIndedToCopyFrom = random.nextInt(td.hosts.size());

            HostInfo orig = td.hosts.get(hostIndexToCopyTo);

            td.hosts.put(hostIndexToCopyTo,
                    new HostInfo(orig.m_hostIp, td.hosts.get(hostIndedToCopyFrom).m_group, orig.m_localSitesCount));
        }
    }

    @Test
    public void testManySlightlyImperfectCluster() throws JSONException {
        for (int i = 0; i < 1000; i++) {
            TestDescription td = getRandomBoringTestDescription();
            if (random.nextDouble() < .3) {
                mutateSwapHAGroups(td);
            }
            subTestDescription(td, false);
        }
    }

    @Test
    public void testTotalChaos() throws JSONException {
        for (int i = 0; i < 200; i++) {
            TestDescription td = getChaoticDescription();
            subTestDescription(td, false);
        }
    }

    @Test
    public void testManyExpandingBoringWithBoring() throws JSONException {
        for (int i = 0; i < 200; i++) {
            TestDescription td1 = getRandomBoringTestDescription();
            AbstractTopology topo = subTestDescription(td1, false);
            // get another random topology that offsets hostids so they don't collide
            TestDescription td2 = getRandomBoringTestDescription(td1.hosts.values().iterator().next().m_localSitesCount,
                    td1.kfactor, td1.hosts.size());
            Pair<AbstractTopology, ImmutableList<Integer>> result = AbstractTopology.mutateAddNewHosts(topo, td2.hosts);
            validate(result.getFirst(), Iterators.forArray(0, result.getSecond().get(0)));
            List<Integer> exptectedNewPartitions = ContiguousSet.create(
                    Range.closedOpen(topo.getPartitionCount(),
                            topo.getPartitionCount() + td2.hosts.size() * topo.getSitesPerHost() / (td2.kfactor + 1)),
                    DiscreteDomain.integers()).asList();
            assertEquals(exptectedNewPartitions, result.getSecond());
        }
    }

    /**
     * generate topology multiple times and validate the same topology
     * @throws InterruptedException
     */
    @Test
    public void testTopologyStatibility() throws InterruptedException {
        Map<Integer, HostInfo> hostInfos = new HashMap<>();
        hostInfos.put(0, new HostInfo("", "g0", 6));
        hostInfos.put(1, new HostInfo("", "g0", 6));
        hostInfos.put(2, new HostInfo("", "g0", 6));
        hostInfos.put(3, new HostInfo("", "g0", 6));
        //test 1
        doStabilityTest(hostInfos, 2);

        hostInfos.put(4, new HostInfo("", "g1", 6));
        hostInfos.put(5, new HostInfo("", "g1", 6));
        hostInfos.put(6, new HostInfo("", "g1", 6));
        hostInfos.put(7, new HostInfo("", "g1", 6));
        //test 2
        doStabilityTest(hostInfos, 3);
    }

    @Test
    public void testMutateRecoverTopology() throws Exception {
        TestDescription td1 = getRandomBoringTestDescription();
        AbstractTopology topo = subTestDescription(td1, false);

        Host hostToReplace = Iterables.get(topo.hostsById.values(), random.nextInt(topo.getHostCount()));
        Set<Integer> liveHosts = new HashSet<>(topo.hostsById.keySet());
        liveHosts.remove(hostToReplace.id);

        int newId = topo.getHostCount() + 1;
        AbstractTopology replaced = AbstractTopology.mutateRecoverTopology(topo, liveHosts, newId,
                hostToReplace.haGroup, null);
        Host newHost = replaced.hostsById.get(newId);
        Set<Integer> origPartitionIds = hostToReplace.getPartitions().stream().map(p -> p.id).collect(Collectors.toSet());
        Set<Integer> newPartitionIds = newHost.getPartitions().stream().map(p -> p.id).collect(Collectors.toSet());
        assertEquals(origPartitionIds, newPartitionIds);

        for (Partition p : newHost.getPartitions()) {
            assertTrue(p.getHostIds().contains(newId));
            assertFalse(p.getHostIds().contains(hostToReplace.id));
        }

        validate(replaced);

        assertNull(AbstractTopology.mutateRecoverTopology(topo, liveHosts, newId, "NotReallyAGroup", null));
    }

    private void doStabilityTest(Map<Integer, HostInfo> hostInfos, int kfactor) throws InterruptedException {
        int count = 200;
        CountDownLatch latch = new CountDownLatch(count);
        Set<String> topos = new HashSet<>();
        List<StabilityTestTask> tasks = new ArrayList<> ();
        while (count > 0) {
            tasks.add(new StabilityTestTask(latch, topos, hostInfos, kfactor));
            count--;
        }
        for (StabilityTestTask task :tasks) {
            task.run();
        }
        latch.await();
        assertEquals(1, topos.size());
    }
    class StabilityTestTask implements Runnable {
        final CountDownLatch latch;
        Set<String> topos;
        final Map<Integer, HostInfo> hostInfos;
        final int kfactor;

        public StabilityTestTask(CountDownLatch latch, Set<String> topos, Map<Integer, HostInfo> hostInfos,
                int kfactor) {
            this.latch = latch;
            this.topos = topos;
            this.hostInfos = hostInfos;
            this.kfactor = kfactor;
        }
        @Override
        public void run() {
            AbstractTopology topology = AbstractTopology.getTopology(hostInfos, Collections.emptySet(), kfactor);
            try {
                topos.add(topology.topologyToJSON().toString());
            } catch (JSONException e) {
                e.printStackTrace();
            }
            latch.countDown();
        }
    }

    @Test
    public void testRestorePlacementOnRecovery() throws Exception {
        Joiner joiner = Joiner.on(',');
        ImmutableMap<Integer, List<Integer>> hostPartitions = ImmutableMap.of(0, ImmutableList.of(0, 1, 2), 1,
                ImmutableList.of(6, 7, 8), 2, ImmutableList.of(9, 10, 11), 3, ImmutableList.of(3, 4, 5));
        Map<Integer, HostInfo> hostInfos = new HashMap<>();
        for (Map.Entry<Integer, List<Integer>> entry : hostPartitions.entrySet()) {
            hostInfos.put(entry.getKey(), new HostInfo("", "g0", 3, joiner.join(entry.getValue())));
        }

        AbstractTopology topology = AbstractTopology.getTopology(hostInfos, Collections.emptySet(), 0, true);

        for (Map.Entry<Integer, List<Integer>> entry : hostPartitions.entrySet()) {
            assertEquals(entry.getValue(), topology.getPartitionIdList(entry.getKey()));
        }

        topology = AbstractTopology.getTopology(hostInfos, Collections.emptySet(), 0, false);

        for (Map.Entry<Integer, List<Integer>> entry : hostPartitions.entrySet()) {
            if (!entry.getValue().equals(topology.getPartitionIdList(entry.getKey()))) {
                return;
            }
        }
        fail("Partitions restored when they shouldn't have been");
    }
}
