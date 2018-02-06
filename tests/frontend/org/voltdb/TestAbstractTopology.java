/* This file is part of VoltDB.
 * Copyright (C) 2008-2018 VoltDB Inc.
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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.json_voltpatches.JSONException;
import org.junit.Ignore;
import org.voltdb.AbstractTopology.HAGroup;
import org.voltdb.AbstractTopology.Host;
import org.voltdb.AbstractTopology.HostDescription;
import org.voltdb.AbstractTopology.KSafetyViolationException;
import org.voltdb.AbstractTopology.Partition;
import org.voltdb.AbstractTopology.PartitionDescription;

import com.google_voltpatches.common.collect.Lists;
import com.google_voltpatches.common.collect.Maps;

import junit.framework.TestCase;

public class TestAbstractTopology extends TestCase {

    static class TestDescription {
        HostDescription[] hosts;
        PartitionDescription[] partitions;
        int expectedPartitionGroups = -1;
        int expectedMaxReplicationPerHAGroup = -1;

        @Override
        public String toString() {
            // group hosts by sph
           String[] bySPH = Arrays.stream(hosts)
                   .collect(Collectors.groupingBy(h -> h.targetSiteCount)).entrySet().stream()
                           .map(e -> String.format("sph=%d (%d)", e.getKey(), e.getValue().size()))
                           .toArray(String[]::new);
           String hostString = String.join(", ", bySPH);
           // group partitions by k
           String[] byK = Arrays.stream(partitions)
                   .collect(Collectors.groupingBy(p -> p.k)).entrySet().stream()
                           .map(e -> String.format("k=%d (%d)", e.getKey(), e.getValue().size()))
                           .toArray(String[]::new);
           String partitionString = String.join(", ", byK);

           // hagroups
           String[] haGroups = Arrays.stream(hosts)
                   .collect(Collectors.groupingBy(h -> h.haGroupToken)).entrySet().stream()
                           .map(e -> String.format("%s (%d)", e.getKey(), e.getValue().size()))
                           .toArray(String[]::new);
           String haGroupsString = String.join(", ", haGroups);

           return "TEST:\n" +
                   "  Hosts                     " + hostString + "\n" +
                   "  Partitions                " + partitionString + "\n" +
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

    Metrics validate(AbstractTopology topo) {
        Metrics metrics = new Metrics();

        // technically valid
        if (topo.hostsById.size() == 0) return metrics;

        // check partitions and hosts are mirrored
        topo.partitionsById.values().forEach(p -> {
            p.hostIds.stream().forEach(hid -> {
                Host h = topo.hostsById.get(hid);
                assert(h != null);
                assert(h.partitions.contains(p));
                // check hosts the other direction
                h.partitions.forEach(p2 -> {
                    Partition p3 = topo.partitionsById.get(p2.id);
                    assert(p3 != null);
                    assert(p2 == p3);
                });
            });
            // check k+1 copies of partition
            assert(p.hostIds.size() == p.k + 1);
        });

        // check ha groups and hosts are mirrored
        topo.hostsById.values().forEach(h -> {
            HAGroup haGroup = h.haGroup;
            assert(haGroup.hostIds.contains(h.id));
            // check all hosts in the hagroup exist
            haGroup.hostIds.forEach(hid -> {
                Host h2 = topo.hostsById.get(hid);
                assert(h2 != null);
            });
        });

        // collect distinct peer groups
        Set<Host> unseenHosts = new HashSet<>(topo.hostsById.values());
        Set<Partition> unseenPartitions = new HashSet<>(topo.partitionsById.values());
        while (unseenPartitions.size() > 0) {
            Partition starterPartition = unseenPartitions.iterator().next();
            unseenPartitions.remove(starterPartition);
            collectConnectedPartitions(starterPartition, unseenHosts, unseenPartitions, topo.hostsById);
            metrics.distinctPeerGroups++;
        }

        // examine ha group placement
        Set<HAGroup> haGroups = topo.hostsById.values().stream()
                .map(h -> h.haGroup)
                .distinct()
                .collect(Collectors.toSet());
        metrics.haGroupCount = haGroups.size();
        int sumOfReplicaCounts = 0;

        for (HAGroup haGroup : haGroups) {
            // get all partitions in ha group, possibly more than once
            List<Partition> partitions = haGroup.hostIds.stream()
                .map(hid -> topo.hostsById.get(hid))
                .flatMap(h -> h.partitions.stream())
                .collect(Collectors.toList());

            // skip ha groups with no partitions
            // but this will be counted in the average metrics (so be careful)
            if (partitions.size() == 0) continue;

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
                    .mapToInt(host -> (int) host.partitions.stream().filter(p -> p.leaderHostId == host.id).count())
                    .max().getAsInt();
            metrics.minLeaderCount = topo.hostsById.values().stream()
                    .mapToInt(host -> (int) host.partitions.stream().filter(p -> p.leaderHostId == host.id).count())
                    .min().getAsInt();
        }

        return metrics;
    }

    private static void collectConnectedPartitions(Partition starterPartition, Set<Host> unseenHosts, Set<Partition> unseenPartitions, Map<Integer, Host> hostsById) {
        for (int hostId : starterPartition.hostIds) {
            Host host = hostsById.get(hostId);
            if (!unseenHosts.contains(host)) continue;
            unseenHosts.remove(host);
            for (Partition partition : host.partitions) {
                if (!unseenPartitions.contains(partition)) continue;
                unseenPartitions.remove(partition);
                collectConnectedPartitions(partition, unseenHosts, unseenPartitions, hostsById);
            }
        }
    }

    public void testOneNode() throws JSONException {
        TestDescription td = getBoringDescription(1, 5, 0, 1, 1);
        subTestDescription(td, false);
    }

    public void testTwoNodesTwoRacks() throws JSONException {
        TestDescription td = getBoringDescription(2, 7, 1, 2, 2);
        AbstractTopology topo = subTestDescription(td, false);
        //System.out.printf("TOPO PRE: %s\n", topo.topologyToJSON());

        // kill and rejoin a host
        HostDescription hostDescription = td.hosts[0];
        try {
            topo = AbstractTopology.mutateRemoveHost(topo, hostDescription.hostId);
        } catch (KSafetyViolationException e) {
            e.printStackTrace();
            fail();
        }
        //System.out.printf("TOPO MID: %s\n", topo.topologyToJSON());
        topo = AbstractTopology.mutateRejoinHost(topo, hostDescription);
        //System.out.printf("TOPO END: %s\n", topo.topologyToJSON());
        validate(topo);
    }

    public void testThreeNodeOneRack() throws JSONException {
        TestDescription td = getBoringDescription(3, 4, 1, 1, 1);
        subTestDescription(td, false);
    }

    public void testFiveNodeK1OneRack() throws JSONException {
        TestDescription td = getBoringDescription(5, 2, 1, 1, 1);
        subTestDescription(td, true);
    }

    public void testFiveNodeK1TwoRacks() throws JSONException {
        TestDescription td = getBoringDescription(5, 6, 1, 1, 2);
        subTestDescription(td, false);
    }

    public void testTooManyPartitions() {
        TestDescription td = getBoringDescription(5, 6, 1, 2, 1);
        td.partitions[0] = new PartitionDescription(td.partitions[0].k + 1);
        AbstractTopology topo = AbstractTopology.mutateAddHosts(AbstractTopology.EMPTY_TOPOLOGY, td.hosts);
        try {
            AbstractTopology.mutateAddPartitionsToEmptyHosts(topo, new HashSet<Integer>(), td.partitions);
            fail();
        }
        catch (Exception e) {
            assertTrue(e.getMessage().contains("Hosts have inadequate space"));
        }
    }

    public void testKTooLarge() {
        HostDescription[] hds = new HostDescription[3];
        hds[0] = new HostDescription(0, 2, "0");
        hds[1] = new HostDescription(1, 2, "0");
        hds[2] = new HostDescription(2, 2, "0");
        PartitionDescription[] pds = new PartitionDescription[2];
        pds[0] = new PartitionDescription(3);
        pds[1] = new PartitionDescription(1);

        AbstractTopology topo = AbstractTopology.mutateAddHosts(AbstractTopology.EMPTY_TOPOLOGY, hds);
        try {
            AbstractTopology.mutateAddPartitionsToEmptyHosts(topo, new HashSet<Integer>(), pds);
            fail();
        }
        catch (Exception e) {
            assertTrue(e.getMessage().contains("Topology request invalid"));
        }
    }

    public void testSubtleKTooLarge() {
        HostDescription[] hds = new HostDescription[3];
        hds[0] = new HostDescription(0, 4, "0");
        hds[1] = new HostDescription(1, 1, "0");
        hds[2] = new HostDescription(2, 1, "0");
        PartitionDescription[] pds = new PartitionDescription[2];
        pds[0] = new PartitionDescription(2);
        pds[1] = new PartitionDescription(2);

        AbstractTopology topo = AbstractTopology.mutateAddHosts(AbstractTopology.EMPTY_TOPOLOGY, hds);
        try {
            AbstractTopology.mutateAddPartitionsToEmptyHosts(topo, new HashSet<Integer>(), pds);
            fail();
        }
        catch (Exception e) {
            assertTrue(e.getMessage().contains("Topology request invalid"));
        }
    }

    public void testNonUniqueHostIds() {
        HostDescription[] hds = new HostDescription[3];
        hds[0] = new HostDescription(201, 1, "0");
        hds[1] = new HostDescription(202, 1, "0");
        hds[2] = new HostDescription(201, 1, "0");
        PartitionDescription[] pds = new PartitionDescription[2];
        pds[0] = new PartitionDescription(0);
        pds[1] = new PartitionDescription(1);

        try {
            AbstractTopology.mutateAddHosts(AbstractTopology.EMPTY_TOPOLOGY, hds);
            fail();
        }
        catch (Exception e) {
            assertTrue(e.getMessage().contains("must contain unique and unused hostid"));
        }

        // now try adding on from existing topo
        TestDescription td = getBoringDescription(5, 6, 1, 2, 1);
        AbstractTopology topo = AbstractTopology.mutateAddHosts(AbstractTopology.EMPTY_TOPOLOGY, td.hosts);
        topo = AbstractTopology.mutateAddPartitionsToEmptyHosts(topo, new HashSet<Integer>(), td.partitions);

        try {
            AbstractTopology.mutateAddHosts(topo, hds);
            fail();
        }
        catch (Exception e) {
            assertTrue(e.getMessage().contains("must contain unique and unused hostid"));
        }
    }

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
                if (distance == 0) continue;
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
        td.hosts = new HostDescription[totalNodeCount];
        for (int i = 0; i < totalNodeCount; i++) {
            td.hosts[i] = new HostDescription(i, sph, haGroups.get(i % haGroups.size()));
        }
        int partitionCount = sph * totalNodeCount / (k + 1);
        td.partitions = new PartitionDescription[partitionCount];
        for (int i = 0; i < partitionCount; i++) {
            td.partitions[i] = new PartitionDescription(k);
        }
        td.expectedPartitionGroups = sph == 0 ? 0 : totalNodeCount / (k + 1);
        AbstractTopology topo = subTestDescription(td, true);

        // see if partition layout is balanced
        String err;
        if ((err = topo.validateLayout()) != null) {
            System.out.println(err);
            System.out.println(topo.topologyToJSON());
        }
        assertTrue(err == null); // expect balanced layout
    }

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
        ThreadLocalRandom r = ThreadLocalRandom.current();
        final int MAX_RACKS = 5;
        final int MAX_RACK_NODES = 10;
        final int MAX_K = 10;
        final int MAX_PARTITIONS = 20;
        int rackCount = r.nextInt(MAX_RACKS) + 1; // [1-5]
        int rackNodeCount = r.nextInt(MAX_RACK_NODES) + 1; // [1-10]
        int totalNodeCount = rackNodeCount * rackCount;
        int k;
        do {
            k = r.nextInt(rackCount - 1, Math.min(totalNodeCount, MAX_K));  // [rackCount - 1, 10]
        } while ((k + 1) % rackCount != 0);
        int sph;
        do {
            sph = r.nextInt(MAX_PARTITIONS) + 1; // [1-20]
        } while ((totalNodeCount * sph) % (k + 1) != 0);
        //////////////////////////////////////////////////////////////////////////
        System.out.println(
                String.format("Node count: %d, kfactor: %d, SPH: %d, # of racks: %d",
                              totalNodeCount, k, sph, rackCount));
        //////////////////////////////////////////////////////////////////////////
        // treeWidth > 1 means the group contains subgroup
        List<String> haGroups = getHAGroupTagTree(1, rackCount);
        TestDescription td = new TestDescription();
        td.hosts = new HostDescription[totalNodeCount];
        for (int i = 0; i < totalNodeCount; i++) {
            td.hosts[i] = new HostDescription(i, sph, haGroups.get(i % haGroups.size()));
        }
        int partitionCount = sph * totalNodeCount / (k + 1);
        td.partitions = new PartitionDescription[partitionCount];
        for (int i = 0; i < partitionCount; i++) {
            td.partitions[i] = new PartitionDescription(k);
        }
        td.expectedPartitionGroups = sph == 0 ? 0 : totalNodeCount / (k + 1);
        AbstractTopology topo = subTestDescription(td, false);

        // see if partition layout is balanced
        String err;
        if ((err = topo.validateLayout()) != null) {
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

        td.hosts = new HostDescription[nodeCount];
        for (int i = 0; i < nodeCount; i++) {
            td.hosts[i] = new HostDescription(i + hostIdOffset, sph, haGroupTags.get(i % haGroupTags.size()));
        }

        int partitionCount = sph * nodeCount / (k + 1);
        td.partitions = new PartitionDescription[partitionCount];
        for (int i = 0; i < partitionCount; i++) {
            td.partitions[i] = new PartitionDescription(k);
        }

        td.expectedMaxReplicationPerHAGroup = sph == 0 ? 0 : (int) Math.ceil((nodeCount / (double) (k + 1)) / leafCount);
        td.expectedPartitionGroups = sph == 0 ? 0 : nodeCount / (k + 1);

        return td;
    }

    private TestDescription getChaoticDescription(Random rand) {
        final int MAX_NODE_COUNT = 120;
        final int MAX_K = 10;

        TestDescription td = new TestDescription();

        // for now, forget about ha groups
        List<String> haGroupTags = new ArrayList<>();
        haGroupTags.add("0");

        // start with a random node count
        int nodeCount = rand.nextInt(MAX_NODE_COUNT) + 1; // 1 .. MAX_NODE_COUNT
        assert(nodeCount > 0);

        // as we go, compute SPH for each node based on random partition needs
        int[] sph = new int[nodeCount];
        for (int i = 0; i < nodeCount; i++) sph[i] = 0;

        List<PartitionDescription> partitions = new ArrayList<PartitionDescription>();

        List<Integer> allHostIndexes = IntStream.range(0, nodeCount).boxed().collect(Collectors.toList());

        for (int i = 0; i < nodeCount; i++) {
            int k = rand.nextInt(Math.min(MAX_K + 1, nodeCount));
            Collections.shuffle(allHostIndexes);

            // find K + 1 nodes to increase sph by one
            for (int j = 0; j < k + 1; j++) {
                sph[allHostIndexes.get(j)] = sph[allHostIndexes.get(j)] + 1;
            }
            partitions.add(new PartitionDescription(k));
        }

        td.hosts = new HostDescription[nodeCount];
        for (int i = 0; i < nodeCount; i++) {
            int haIndex = rand.nextInt(haGroupTags.size());
            td.hosts[i] = new HostDescription(i, sph[i], haGroupTags.get(haIndex));
        }

        td.partitions = partitions.toArray(new PartitionDescription[0]);

        td.expectedMaxReplicationPerHAGroup = 0;
        td.expectedPartitionGroups = Integer.MAX_VALUE;

        return td;
    }

    private AbstractTopology subTestDescription(TestDescription td, boolean print) throws JSONException {
        System.out.println(td.toString());

        long start = System.currentTimeMillis();

        AbstractTopology topo = AbstractTopology.mutateAddHosts(
                AbstractTopology.EMPTY_TOPOLOGY, td.hosts);
        //System.out.println(topo.topologyToJSON());

        try {
        topo = AbstractTopology.mutateAddPartitionsToEmptyHosts(
                topo, new HashSet<Integer>(), td.partitions);
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        if (print) System.out.println(topo.topologyToJSON());
        long subEnd = System.currentTimeMillis();

        Metrics metrics = validate(topo);
        long end = System.currentTimeMillis();
        metrics.testTimeMS = end - start;
        metrics.topoTomeMS = subEnd - start;

        if (print) System.out.println(metrics.toString());

        //assertEquals(td.expectedMaxReplicationPerHAGroup, metrics.maxReplicationPerHAGroup);
        assertTrue(metrics.distinctPeerGroups <= td.expectedPartitionGroups);

        return topo;
    }

    private TestDescription getRandomBoringTestDescription(Random rand) {
        return getRandomBoringTestDescription(rand, 0);
    }

    private TestDescription getRandomBoringTestDescription(Random rand, int hostIdOffset) {
        final int MAX_NODE_COUNT = 120;
        final int MAX_SPH = 60;
        final int MAX_K = 10;

        int hostCount, k, sph, leafCount, treeWidth;


        hostCount = rand.nextInt(MAX_NODE_COUNT) + 1;
        k = rand.nextInt(Math.min(hostCount - 1, MAX_K) + 1);

        do { sph = rand.nextInt(MAX_SPH) + 1; }
        while ((sph * hostCount) % (k + 1) != 0);

        ArrayList<Integer> leafOptions = new ArrayList<>();
        for (int leafCountOption = k + 1;
            leafCountOption <= hostCount;
            leafCountOption = Math.max(leafCountOption *= (k + 1), leafCountOption + 1)) {
            leafOptions.add(leafCountOption);
        }
        leafCount = hostCount == 0 ? 0 : leafOptions.get(rand.nextInt(leafOptions.size()));

        treeWidth = hostCount == 0 ? 0 : rand.nextInt(leafCount) + 1;

        return getBoringDescription(hostCount, sph, k, treeWidth, leafCount, hostIdOffset);
    }

    public void testManyBoringClusters() throws JSONException {
        Random rand = new Random();

        for (int i = 0; i < 1000; i++) {
            TestDescription td = getRandomBoringTestDescription(rand);
            AbstractTopology topo = subTestDescription(td, false);

            // kill and rejoin a host
            if (td.hosts.length == 0) {
                continue;
            }
            HostDescription hostDescription = td.hosts[rand.nextInt(td.hosts.length)];
            try {
                topo = AbstractTopology.mutateRemoveHost(topo, hostDescription.hostId);
            } catch (KSafetyViolationException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
                return;
            }
            topo = AbstractTopology.mutateRejoinHost(topo, hostDescription);
            validate(topo);
        }
    }

    void mutateSwapPartitionCounts(Random rand, TestDescription td) {
        int totalSitesBefore = Arrays.stream(td.partitions)
                .mapToInt(p -> p.k + 1)
                .sum();


        if (td.partitions.length < 2) return;
        int portionToSwap = rand.nextInt(td.partitions.length / 2);
        // increment and decrement pairs of k
        for (int i = 0; i < portionToSwap; i++) {
            // if k is already maxed, skip it
            if (td.partitions[i].k >= (td.hosts.length - 1)) {
                continue;
            }
            td.partitions[i] = new PartitionDescription(td.partitions[i].k + 1);
            td.partitions[i + portionToSwap] = new PartitionDescription(td.partitions[i + portionToSwap].k - 1);
        }
        // remove any where k is now -1
        td.partitions = Arrays.stream(td.partitions)
                .filter(p -> p.k >= 0)
                .toArray(PartitionDescription[]::new);
        // making no promises for non-boring clusters yet
        td.expectedPartitionGroups = Integer.MAX_VALUE;

        int totalSitesAfter = Arrays.stream(td.partitions)
                .mapToInt(p -> p.k + 1)
                .sum();

        int totalHostSites = Arrays.stream(td.hosts)
                .mapToInt(h -> h.targetSiteCount)
                .sum();

        assertEquals(totalSitesBefore, totalSitesAfter);
        assertEquals(totalSitesAfter, totalHostSites);
    }

    void mutateSwapHAGroups(Random rand, TestDescription td) {
        if (td.hosts.length == 0) {
            return;
        }

        int consolidationCount = rand.nextInt(td.hosts.length) + 1;

        for (int i = 0; i < consolidationCount; i++) {
            int hostIndexToCopyTo = rand.nextInt(td.hosts.length);
            int hostIndedToCopyFrom = rand.nextInt(td.hosts.length);

            HostDescription hd = new HostDescription(
                    td.hosts[hostIndexToCopyTo].hostId,
                    td.hosts[hostIndexToCopyTo].targetSiteCount,
                    td.hosts[hostIndedToCopyFrom].haGroupToken);

            td.hosts[hostIndexToCopyTo] = hd;
        }
    }

    void mutateChangeSPH(Random rand, TestDescription td) {
        if (td.hosts.length == 0) {
            return;
        }

        int sphBumpCount = rand.nextInt(td.hosts.length) + 1;

        for (int i = 0; i < sphBumpCount; i++) {
            int hostIndex = rand.nextInt(td.hosts.length);

            HostDescription hd = new HostDescription(
                    td.hosts[hostIndex].hostId,
                    td.hosts[hostIndex].targetSiteCount + 1,
                    td.hosts[hostIndex].haGroupToken);

            td.hosts[hostIndex] = hd;
        }

        // making no promises for non-boring clusters yet
        td.expectedPartitionGroups = Integer.MAX_VALUE;
    }

    public void testManySlightlyImperfectCluster() throws JSONException {
        Random rand = new Random();

        for (int i = 0; i < 1000; i++) {
            TestDescription td = getRandomBoringTestDescription(rand);
            if (rand.nextDouble() < .3) {
                mutateSwapPartitionCounts(rand, td);
            }
            if (rand.nextDouble() < .3) {
                mutateSwapHAGroups(rand, td);
            }
            if (rand.nextDouble() < .3) {
                mutateChangeSPH(rand, td);
            }
            subTestDescription(td, false);
        }
    }

    public void testTotalChaos() throws JSONException {
        Random rand = new Random();

        for (int i = 0; i < 200; i++) {
            TestDescription td = getChaoticDescription(rand);
            subTestDescription(td, false);
        }
    }

    public void testManyExpandingBoringWithBoring() throws JSONException {
        Random rand = new Random();

        for (int i = 0; i < 200; i++) {
            TestDescription td1 = getRandomBoringTestDescription(rand);
            AbstractTopology topo = subTestDescription(td1, false);
            // get another random topology that offsets hostids so they don't collide
            TestDescription td2 = getRandomBoringTestDescription(rand, td1.hosts.length);
            topo = AbstractTopology.mutateAddHosts(topo, td2.hosts);
            topo = AbstractTopology.mutateAddPartitionsToEmptyHosts(topo, new HashSet<Integer>(), td2.partitions);
            validate(topo);
        }
    }

    /**
     * generate topology multiple times and validate the same topology
     * @throws InterruptedException
     */
    public void testTopologyStatibility() throws InterruptedException {
        Map<Integer, String> hostGroups = new HashMap<>();
        hostGroups.put(0, "g0");
        hostGroups.put(1, "g0");
        hostGroups.put(2, "g0");
        hostGroups.put(3, "g0");
        Map<Integer, Integer> sitesPerHostMap = new HashMap<>();
        sitesPerHostMap.put(0, 6);
        sitesPerHostMap.put(1, 6);
        sitesPerHostMap.put(2, 6);
        sitesPerHostMap.put(3, 6);
        //test 1
        doStabilityTest(hostGroups, sitesPerHostMap, 2);

        hostGroups.put(4, "g1");
        hostGroups.put(5, "g1");
        hostGroups.put(6, "g1");
        hostGroups.put(7, "g1");

        sitesPerHostMap.put(4, 6);
        sitesPerHostMap.put(5, 6);
        sitesPerHostMap.put(6, 6);
        sitesPerHostMap.put(7, 6);
        //test 2
        doStabilityTest(hostGroups, sitesPerHostMap, 3);
    }

    private void doStabilityTest( Map<Integer, String> hostGroups,
            Map<Integer, Integer> sitesPerHostMap, int kfactor) throws InterruptedException {
        int count = 200;
        CountDownLatch latch = new CountDownLatch(count);
        Set<String> topos = new HashSet<>();
        List<StabilityTestTask> tasks = new ArrayList<> ();
        while (count > 0) {
            tasks.add(new StabilityTestTask(latch, topos,sitesPerHostMap, hostGroups, kfactor));
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
        final Map<Integer, Integer>  sitesPerHostMap;
        final Map<Integer, String> hostGroups;
        final int kfactor;
        public StabilityTestTask (CountDownLatch latch, Set<String> topos,
                Map<Integer, Integer>  sitesPerHostMap, Map<Integer, String> hostGroups, int kfactor) {
            this.latch = latch;
            this.topos = topos;
            this.hostGroups = hostGroups;
            this.kfactor = kfactor;
            this.sitesPerHostMap = sitesPerHostMap;
        }
        @Override
        public void run() {
            AbstractTopology topology = AbstractTopology.getTopology(sitesPerHostMap, new HashSet<Integer>(), hostGroups, kfactor);
            try {
                topos.add(topology.topologyToJSON().toString());
            } catch (JSONException e) {
                e.printStackTrace();
            }
            latch.countDown();
        }
    }
}
