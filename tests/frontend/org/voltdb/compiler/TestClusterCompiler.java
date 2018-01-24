/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
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
package org.voltdb.compiler;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

import org.json_voltpatches.JSONArray;
import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;
import org.voltcore.utils.CoreUtils;
import org.voltdb.VoltDB;

import com.google_voltpatches.common.collect.HashMultimap;
import com.google_voltpatches.common.collect.ImmutableMap;
import com.google_voltpatches.common.collect.Maps;
import com.google_voltpatches.common.collect.Multimap;
import com.google_voltpatches.common.collect.Multimaps;
import com.google_voltpatches.common.collect.Sets;

import junit.framework.TestCase;

public class TestClusterCompiler extends TestCase
{
    public void testNonZeroReplicationFactor() throws Exception
    {
        ClusterConfig config = new ClusterConfig(3, 1, 2);
        Map<Integer, String> topology = Maps.newHashMap();
        topology.put(0, "0");
        topology.put(1, "0");
        topology.put(2, "0");
        JSONObject obj = config.getTopology(topology, HashMultimap.create(), new HashMap<>());
        config.validate();
        System.out.println(obj.toString(4));
        JSONArray partitions = obj.getJSONArray("partitions");

        // despite 3 hosts, should only have 1 partition with k-safety of 2
        assertEquals(1, partitions.length());

        // All the execution sites should have the same relative index
        for (int ii = 0; ii < partitions.length(); ii++) {
            JSONObject partition = partitions.getJSONObject(ii);
            assertEquals(0, partition.getInt("partition_id"));
            JSONArray replicas = partition.getJSONArray("replicas");
            assertEquals(3, replicas.length());
            HashSet<Integer> replicasContents = new HashSet<Integer>();
            for (int zz = 0; zz < replicas.length(); zz++) {
                replicasContents.add(replicas.getInt(zz));
            }
            assertTrue(replicasContents.contains(0));
            assertTrue(replicasContents.contains(1));
            assertTrue(replicasContents.contains(2));


        }
    }

    public void testInSufficientHostsToReplicate()
    {
        // 2 hosts, 6 sites per host, 2 copies of each partition.
        // there are sufficient execution sites, but insufficient hosts
        ClusterConfig config = new ClusterConfig(2, 6, 2);
        try
        {
            if (!config.validate()) {
                throw new RuntimeException(config.getErrorMsg());
            }
        }
        catch (RuntimeException e)
        {
            if (e.getMessage().contains("servers required"))
            {
                return;
            }
        }
        fail();
    }

    public void testAddHostToNonKsafe() throws JSONException
    {
        ClusterConfig config = new ClusterConfig(1, 6, 0);
        Map<Integer, String> topology = Maps.newHashMap();
        topology.put(0, "0");
        JSONObject topo = config.getTopology(topology, HashMultimap.create(), new HashMap<>());
        assertEquals(1, topo.getInt("hostcount"));
        assertEquals(6, topo.getInt("sites_per_host"));
        assertEquals(0, topo.getInt("kfactor"));
        assertEquals(6, topo.getJSONArray("partitions").length());

        ClusterConfig.addHosts(1, topo);
        assertEquals(2, topo.getInt("hostcount"));
        assertEquals(0, topo.getInt("kfactor"));
    }

    public void testAddHostsToNonKsafe() throws JSONException
    {
        ClusterConfig config = new ClusterConfig(2, 6, 0);
        Map<Integer, String> topology = Maps.newHashMap();
        topology.put(0, "0");
        topology.put(1, "0");
        JSONObject topo = config.getTopology(topology, HashMultimap.create(), new HashMap<>());
        assertEquals(2, topo.getInt("hostcount"));
        assertEquals(6, topo.getInt("sites_per_host"));
        assertEquals(0, topo.getInt("kfactor"));
        assertEquals(12, topo.getJSONArray("partitions").length());

        VoltDB.ignoreCrash = true;
        try {
            ClusterConfig.addHosts(2, topo);
            fail("Shouldn't allow adding more than one node");
        } catch (AssertionError e) {}
    }

    public void testAddHostsToKsafe() throws JSONException
    {
        ClusterConfig config = new ClusterConfig(2, 6, 1);
        Map<Integer, String> topology = Maps.newHashMap();
        topology.put(0, "0");
        topology.put(1, "0");
        JSONObject topo = config.getTopology(topology, HashMultimap.create(), new HashMap<>());
        assertEquals(2, topo.getInt("hostcount"));
        assertEquals(6, topo.getInt("sites_per_host"));
        assertEquals(1, topo.getInt("kfactor"));
        assertEquals(6, topo.getJSONArray("partitions").length());

        ClusterConfig.addHosts(2, topo);
        assertEquals(4, topo.getInt("hostcount"));
        assertEquals(1, topo.getInt("kfactor"));
    }

    public void testAddMoreThanKsafeHosts() throws JSONException
    {
        ClusterConfig config = new ClusterConfig(2, 6, 1);
        Map<Integer, String> topology = Maps.newHashMap();
        topology.put(0, "0");
        topology.put(1, "0");
        JSONObject topo = config.getTopology(topology, HashMultimap.create(), new HashMap<>());
        assertEquals(2, topo.getInt("hostcount"));
        assertEquals(6, topo.getInt("sites_per_host"));
        assertEquals(1, topo.getInt("kfactor"));
        assertEquals(6, topo.getJSONArray("partitions").length());

        VoltDB.ignoreCrash = true;
        try {
            ClusterConfig.addHosts(3, topo);
            fail("Shouldn't allow adding more than ksafe + 1 node");
        } catch (AssertionError e) {}
    }

    public void testEightNodesTwoLevelsOfGroups() throws JSONException
    {
        Map<Integer, String> hostGroups = Maps.newHashMap();
        hostGroups.put(0, "0.0");
        hostGroups.put(1, "0.0");
        hostGroups.put(2, "0.1");
        hostGroups.put(3, "0.1");
        hostGroups.put(4, "1.0");
        hostGroups.put(5, "1.0");
        hostGroups.put(6, "1.1");
        hostGroups.put(7, "1.1");
        int kfactor = 3;
        for (int sph = 1; sph <= 20; sph++) {
            runConfiguration(true, hostGroups, sph, kfactor, true);
        }
    }

    public void testTenNodes_ENG13435() throws JSONException
    {
        Map<Integer, String> hostGroups = Maps.newHashMap();
        hostGroups.put(0, "FC1");
        hostGroups.put(1, "FC2");
        hostGroups.put(2, "FC2");
        hostGroups.put(3, "FC2");
        hostGroups.put(4, "FC2");
        hostGroups.put(5, "FC1");
        hostGroups.put(6, "FC2");
        hostGroups.put(7, "FC1");
        hostGroups.put(8, "FC1");
        hostGroups.put(9, "FC1");
        int kfactor = 3;
        for (int sph = 1; sph <= 20; sph++) {
            runConfiguration(true, hostGroups, sph, kfactor, false);
        }
    }

    public void testSixNodes_ENG13435() throws JSONException
    {
        Map<Integer, String> hostGroups = Maps.newHashMap();
        hostGroups.put(0, "FC1");
        hostGroups.put(1, "FC2");
        hostGroups.put(2, "FC2");
        hostGroups.put(3, "FC2");
        hostGroups.put(4, "FC1");
        hostGroups.put(5, "FC1");
        int kfactor = 3;
        for (int sph = 1; sph <= 20; sph++) {
            runConfiguration(true, hostGroups, sph, kfactor, false);
        }
    }

    // For non-optimal but legitimate configuration, only verify that
    // all the nodes have right number of partitions and k-safety constrain
    // is satisfied.
    public void testRandomNonOptimalConfig() throws JSONException
    {
        for (int i = 0; i < 200; i++) {
            runRandomHAGroupTest(false);
        }
        System.out.println("Hooooray!!!!!!!!!!!");
    }

    public void testRandomOptimalConfig() throws JSONException
    {
        for (int i = 0; i < 200; i++) {
            runRandomHAGroupTest(true);
        }
    }

    public void testRejoinOneNode() throws JSONException
    {
        ImmutableMap<Integer, String> topology =
        ImmutableMap.of(0, "0",
                        1, "0");
        killAndRejoinNodes(topology, 1, 1);
    }

    public void testRejoinTwoNodes() throws JSONException
    {
        ImmutableMap<Integer, String> topology =
        ImmutableMap.of(0, "0",
                        1, "1",
                        2, "2");
        killAndRejoinNodes(topology, 2, 2);
    }

    public void testRejoinTwoNodesToTwo() throws JSONException
    {
        ImmutableMap<Integer, String> topology = new ImmutableMap.Builder<Integer, String>()
                                                 .put(0, "0")
                                                 .put(1, "0")
                                                 .put(2, "1")
                                                 .put(3, "1")
                                                 .put(4, "2")
                                                 .put(5, "2")
                                                 .build();
        killAndRejoinNodes(topology, 2, 2);
    }

    public void testRejoinThreeNodesToFour() throws JSONException
    {
        ImmutableMap<Integer, String> topology = new ImmutableMap.Builder<Integer, String>()
                                                 .put(0, "0")
                                                 .put(1, "0")
                                                 .put(2, "0")
                                                 .put(3, "0")
                                                 .put(4, "1")
                                                 .put(5, "1")
                                                 .put(6, "1")
                                                 .put(7, "1")
                                                 .build();
        killAndRejoinNodes(topology, 3, 3);
    }

    // Kill entire rack plus one more node on the other rack, then rejoin them back
    public void testRejoinSixNodes() throws JSONException
    {
        ImmutableMap<Integer, String> topology = new ImmutableMap.Builder<Integer, String>()
                                                 .put(0, "0")
                                                 .put(1, "1")
                                                 .put(2, "1")
                                                 .put(3, "1")
                                                 .put(4, "1")
                                                 .put(5, "0")
                                                 .put(6, "0")
                                                 .put(7, "1")
                                                 .put(8, "0")
                                                 .put(9, "0")
                                                 .build();
        killAndRejoinNodes(topology, 3, 6);
    }

    private static void killAndRejoinNodes(ImmutableMap<Integer, String> fullHostGroup, int kfactor, int nodesToKill)
    throws JSONException
    {
        final int maxSph = 20;
        for (int sph = 1; sph < maxSph; sph++) {
            final Map<Integer, String> rejoinHostGroup = new HashMap<>(fullHostGroup);
            final ClusterConfig initialConfig = new ClusterConfig(rejoinHostGroup.size(), sph, kfactor);
            if (!initialConfig.validate()) {
                // Invalid config, skip
                continue;
            }
            final JSONObject initialTopo = initialConfig.getTopology(rejoinHostGroup, HashMultimap.create(), new HashMap<>());

            final Multimap<Integer, Integer> partitionToHosts = HashMultimap.create();
            final Multimap<Integer, Integer> hostPartitions = HashMultimap.create();
            final Multimap<Integer, Integer> hostMasters = HashMultimap.create();
            for (int hostId : rejoinHostGroup.keySet()) {
                for (int pid : ClusterConfig.partitionsForHost(initialTopo, hostId)) {
                    partitionToHosts.put(pid, hostId);
                }
                hostPartitions.putAll(hostId, ClusterConfig.partitionsForHost(initialTopo, hostId));
                hostMasters.putAll(hostId, ClusterConfig.partitionsForHost(initialTopo, hostId, true));
            }

            // Pick nodes to kill from different groups. if this is
            // not a valid topology for multiple rejoins in different
            // groups, the returned set will be empty.
            Multimap<String, Integer> groupToHosts = Multimaps.invertFrom(Multimaps.forMap(rejoinHostGroup), HashMultimap.create());
            final Set<Integer> hostIdsToKill = pickNodesInDiffGroupsToKill(fullHostGroup, groupToHosts, hostPartitions, kfactor, nodesToKill);
            if (hostIdsToKill.isEmpty()) {
                System.out.println("Not a valid topology for multi node rejoin, skipping. Sites per host " + sph);
                continue;
            }
            System.out.println("Running config: sites " + sph + ", k-factor " + kfactor
                    + ", host partitions: " + hostPartitions +
                               ", nodes to kill: " + hostIdsToKill);

            // Remove all partitions and masters from the failed nodes, migrate masters to remaining nodes
            final Multimap<Integer, Integer> expectedRejoinHostPartitions = HashMultimap.create();
            final HashSet<Integer> liveHosts = new HashSet<>(hostPartitions.keySet());
            final Map<Integer, String> rejoinHostGroups = new HashMap<>();
            for (int toKill : hostIdsToKill) {
                liveHosts.remove(toKill);
                for (int masterToRedistribute : hostMasters.get(toKill)) {
                    for (int hostCandidate : partitionToHosts.get(masterToRedistribute)) {
                        if (hostCandidate != toKill && liveHosts.contains(hostCandidate)) {
                            hostMasters.put(hostCandidate, masterToRedistribute);
                            break;
                        }
                    }
                }
                expectedRejoinHostPartitions.putAll(toKill, hostPartitions.removeAll(toKill));
                hostMasters.removeAll(toKill);
                rejoinHostGroups.put(toKill, rejoinHostGroup.remove(toKill));
            }

            // Fake partition to HSID mappings
            Multimap<Integer, Long> replicas = HashMultimap.create();
            for (Map.Entry<Integer, Integer> e : hostPartitions.entries()) {
                replicas.put(e.getValue(), CoreUtils.getHSIdFromHostAndSite(e.getKey(), e.getValue()));
            }
            Map<Integer, Long> masters = new HashMap<>();
            for (Map.Entry<Integer, Integer> e : hostMasters.entries()) {
                masters.put(e.getValue(), CoreUtils.getHSIdFromHostAndSite(e.getKey(), e.getValue()));
            }

            // Rejoin one node at a time and verify that they have the correct partitions
            JSONObject rejoinTopo = null;
            for (Map.Entry<Integer, String> rejoin : rejoinHostGroups.entrySet()) {
                final int rejoinHostId = rejoin.getKey() + fullHostGroup.size();
                System.out.println("Rejoining " + rejoin.getKey() + " as " + rejoinHostId + " in group " + rejoin.getValue());
                rejoinHostGroup.put(rejoinHostId, rejoin.getValue());
                rejoinTopo = initialConfig.getTopology(rejoinHostGroup, replicas, masters);
                final List<Integer> partitionsOnRejoinHost = ClusterConfig.partitionsForHost(rejoinTopo, rejoinHostId);

                // No master on rejoined host
                assertTrue(ClusterConfig.partitionsForHost(rejoinTopo, rejoinHostId, true).isEmpty());

                // Verify existing hosts remain the same
                for (Map.Entry<Integer, Collection<Integer>> e : hostPartitions.asMap().entrySet()) {
                    int hostId = e.getKey();
                    if (rejoinHostGroups.containsKey(e.getKey())) {
                        hostId = e.getKey() + fullHostGroup.size();
                    }
                    assertEquals("host " + hostId + " key " + e.getKey(), e.getValue(), new HashSet<>(ClusterConfig.partitionsForHost(rejoinTopo, hostId)));
                    assertEquals(hostMasters.get(e.getKey()), new HashSet<>(ClusterConfig.partitionsForHost(rejoinTopo, hostId, true)));
                }

                // Now add the rejoined host to the live host maps
                hostPartitions.putAll(rejoin.getKey(), partitionsOnRejoinHost);
                for (int pid : partitionsOnRejoinHost) {
                    replicas.put(pid, CoreUtils.getHSIdFromHostAndSite(rejoinHostId, pid));
                }
            }
            assert (rejoinTopo != null);
            // Verify partitions are well balanced across groups
            verifyTopology(true, true, rejoinHostGroup, rejoinTopo, kfactor);
        }
    }

    private static Set<Integer> pickNodesInDiffGroupsToKill(ImmutableMap<Integer, String> topo,
                                                            Multimap<String, Integer> groupToHosts,
                                                            Multimap<Integer, Integer> hostPartitions,
                                                            int kfactor,
                                                            int nodesToKill)
    {
        // Find minimum number of nodes in placement groups
        int minNode = groupToHosts.asMap().values().stream()
                .mapToInt(c -> c.size())
                .min()
                .orElse(Integer.MAX_VALUE);
        for (Set<Integer> perm : Sets.powerSet(topo.keySet())) {
            // Skip permutation size not equal to the node count to kill
            if (perm.size() != nodesToKill) {
                continue;
            }

            if (nodesToKill > minNode) {
                boolean killRack = false;
                for (Map.Entry<String, Collection<Integer>> group : groupToHosts.asMap().entrySet()) {
                    // Then kill entire rack
                    if (perm.containsAll(group.getValue())) {
                        killRack = true;
                        break;
                    }
                }
                if (!killRack) {
                    continue;
                }
            }

            // Count the occurrences of each partition in the candidate nodes
            Map<Integer, Integer> partitionOccurrances = new HashMap<>();
            perm.stream().map(hostPartitions::get).forEach(s -> s.forEach(a -> {
                if (partitionOccurrances.containsKey(a)) {
                    partitionOccurrances.put(a, partitionOccurrances.get(a) + 1);
                } else {
                    partitionOccurrances.put(a, 1);
                }
            }));

            // If a partition appears k+1 times in the candidate nodes, killing
            // them will take the cluster down, so skip this set of candidate nodes
            boolean skip = false;
            for (int count : partitionOccurrances.values()) {
                if (count > kfactor) {
                    skip = true;
                }
            }
            if (skip) {
                continue;
            }

            return new HashSet<>(perm);
        }
        return new HashSet<>();
    }

    // optimal means only generate configurations that
    // 1) each placement group has same number of nodes
    // 2) number of placement groups is divisible to (kfactor + 1)
    private static void runRandomHAGroupTest(boolean optimal) throws JSONException
    {
        ThreadLocalRandom r = ThreadLocalRandom.current();
        final int MAX_RACKS = 5;
        final int MAX_RACK_NODES = 10;
        final int MAX_K = 10;
        final int MAX_PARTITIONS = 20;
        int rackCount = optimal ? (r.nextInt(MAX_RACKS) + 1 ): r.nextInt(2, MAX_RACKS + 1); // [1-5] or [2-5]
        int rackNodeCount = r.nextInt(MAX_RACK_NODES) + 1; // [1-10]
        int totalNodeCount = rackNodeCount * rackCount;
        int k, sph;
        do {
            int lowerBound = optimal ? (rackCount - 1) : 0;
            k = r.nextInt(lowerBound, Math.min(totalNodeCount, MAX_K));  // [rackCount - 1, 10]
        } while (optimal ?
                (k + 1) % rackCount != 0 : (k + 1) % rackCount == 0);
        do {
            sph = r.nextInt(MAX_PARTITIONS) + 1; // [1-20]
        } while ((totalNodeCount * sph) % (k + 1) != 0 );
        //////////////////////////////////////////////////////////////////////////
        System.out.println(
                String.format("Optimal=%s Node count: %d, kfactor: %d, SPH: %d, # of racks: %d",
                              optimal ? "true" : "false", totalNodeCount, k, sph, rackCount));
        //////////////////////////////////////////////////////////////////////////
        Map<Integer, String> hostGroups = Maps.newHashMap();
        for (int i = 0; i < totalNodeCount; i++) {
            hostGroups.put(i, String.valueOf(i % rackCount));
        }

        runConfiguration(optimal, hostGroups, sph, k, true);
    }

    private static void runConfiguration(
            boolean optimal,
            Map<Integer, String> hostGroups,
            int sph,
            int kfactor,
            boolean print) throws JSONException
    {
        final ClusterConfig config = new ClusterConfig(hostGroups.size(), sph, kfactor);
        System.out.println("Running config " + hostGroups.size() + " hosts, " + sph + " sitesperhost, k=" + kfactor);
        if (config.validate()) {
            long start = System.currentTimeMillis();
            final JSONObject topo = config.getTopology(hostGroups, HashMultimap.create(), new HashMap<>());
            if (print) {
                System.out.println("It takes " + (System.currentTimeMillis() - start) + " ms to compute the topo.");
                System.out.println(topo);
            }
            verifyTopology(optimal, false, hostGroups, topo, kfactor);
        } else {
            System.out.println(config.getErrorMsg());
        }
    }

    private static void verifyTopology(
            boolean optimal,
            boolean isRejoin,
            Map<Integer, String> hostGroups,
            JSONObject topo,
            int kfactor) throws JSONException
    {
        final int hostCount = new ClusterConfig(topo).getHostCount();
        final int sitesPerHost = new ClusterConfig(topo).getSitesPerHost();
        final int partitionCount = new ClusterConfig(topo).getPartitionCount();
        final int minMasterCount = partitionCount / hostCount;
        final int maxMasterCount = minMasterCount + 1;
        final int nodesWithMaxMasterCount = partitionCount % hostCount;

        Multimap<Integer, Integer> masterCountToHost = HashMultimap.create();
        Multimap<String, Integer> groupHosts = HashMultimap.create();
        Map<String, Map<Integer, Integer>> groupPartitions = new HashMap<>();   // <groupId, <pId, count>>
        for (Map.Entry<Integer, String> entry : hostGroups.entrySet()) {
            final List<Integer> hostPartitions = ClusterConfig.partitionsForHost(topo, entry.getKey());
            final List<Integer> hostMasters = ClusterConfig.partitionsForHost(topo, entry.getKey(), true);
            // Make sure a host has exactly sitesPerHost number of partitions
            assertEquals(sitesPerHost, hostPartitions.size());
            // Make sure a host only has unique partitions
            assertEquals(hostPartitions.size(), Sets.newHashSet(hostPartitions).size());

            masterCountToHost.put(hostMasters.size(), entry.getKey());
            groupHosts.put(entry.getValue(), entry.getKey());
            Map<Integer, Integer> pidCountMap = groupPartitions.get(entry.getValue());
            if (pidCountMap != null) {
                for (Integer pid : hostPartitions) {
                    if (pidCountMap.containsKey(pid)) {
                        int count = pidCountMap.get(pid);
                        pidCountMap.put(pid, count + 1);
                    } else {
                        pidCountMap.put(pid, 1);
                    }
                }
            } else {
                pidCountMap = new HashMap<Integer, Integer>();
                groupPartitions.put(entry.getValue(), pidCountMap);
                for (Integer pid : hostPartitions) {
                    pidCountMap.put(pid, 1);
                }
            }
        }

        if (optimal) {
            if (!isRejoin) {
                // Make sure master partitions are spread out
                if (nodesWithMaxMasterCount == 0) {
                    assertEquals(1, masterCountToHost.keySet().size());
                    assertTrue(masterCountToHost.containsKey(minMasterCount));
                } else {
                    assertEquals(2, masterCountToHost.keySet().size());
                    assertTrue(masterCountToHost.containsKey(minMasterCount));
                    assertEquals(nodesWithMaxMasterCount, masterCountToHost.get(maxMasterCount).size());
                }
            }
            int expectedReplicasPerGroup = (kfactor + 1) / groupPartitions.size();

            // Each group should have equal number of copies of all the partitions
            for (Map.Entry<String, Map<Integer, Integer>> groupAndPids : groupPartitions.entrySet()) {
                assertTrue( groupPartitions.toString(),
                        groupAndPids.getValue().values().stream().allMatch(c -> c == expectedReplicasPerGroup));
            }
        }
    }
}
