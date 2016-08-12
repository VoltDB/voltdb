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

import com.google_voltpatches.common.collect.ImmutableMap;
import com.google_voltpatches.common.collect.HashMultimap;
import com.google_voltpatches.common.collect.Maps;
import com.google_voltpatches.common.collect.Multimap;
import com.google_voltpatches.common.collect.Sets;
import org.json_voltpatches.JSONArray;
import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;

import junit.framework.TestCase;
import org.voltcore.utils.CoreUtils;
import org.voltdb.VoltDB;

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
        killAndRejoinNodes(topology, 1, 2);
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
                                                 .build();
        killAndRejoinNodes(topology, 2, 3);
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
            final Set<Integer> hostIdsToKill = pickNodesInDiffGroupsToKill(fullHostGroup, hostPartitions, kfactor, nodesToKill);
            if (hostIdsToKill.isEmpty()) {
                System.out.println("Not a valid topology for multi node rejoin, skipping. Sites per host " + sph);
                continue;
            }
            System.out.println("Running config: sites " + sph + ", host partitions: " + hostPartitions +
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
            for (Map.Entry<Integer, String> rejoin : rejoinHostGroups.entrySet()) {
                System.out.println("Rejoining " + rejoin.getKey() + " in group " + rejoin.getValue());
                rejoinHostGroup.put(rejoin.getKey(), rejoin.getValue());
                final JSONObject rejoinTopo = initialConfig.getTopology(rejoinHostGroup, replicas, masters);
                final List<Integer> partitionsOnRejoinHost = ClusterConfig.partitionsForHost(rejoinTopo, rejoin.getKey());

                // Verify rejoined host first. Partitions on rejoined host should have
                // at least one replica in a different group.
                if (fullHostGroup.values().stream().distinct().count() > 1) {
                    for (int partitionOnRejoined : partitionsOnRejoinHost) {
                        final String rejoinedHostGroup = rejoin.getValue();
                        boolean foundHostInOtherGroup = false;
                        for (long hsId : replicas.get(partitionOnRejoined)) {
                            if (!rejoinHostGroup.get(CoreUtils.getHostIdFromHSId(hsId)).equals(rejoinedHostGroup)) {
                                foundHostInOtherGroup = true;
                                break;
                            }
                        }
                        assertTrue("Host " + rejoin.getKey() + " partition " + partitionOnRejoined + " rejoined: " +
                                   partitionsOnRejoinHost + " current " + replicas,
                                   foundHostInOtherGroup);
                    }
                }
                assertTrue(ClusterConfig.partitionsForHost(rejoinTopo, rejoin.getKey(), true).isEmpty()); // No master on rejoined host

                // Verify existing hosts remain the same
                for (Map.Entry<Integer, Collection<Integer>> e : hostPartitions.asMap().entrySet()) {
                    assertEquals(e.getValue(), new HashSet<>(ClusterConfig.partitionsForHost(rejoinTopo, e.getKey())));
                    assertEquals(hostMasters.get(e.getKey()), new HashSet<>(ClusterConfig.partitionsForHost(rejoinTopo, e.getKey(), true)));
                }

                // Now add the rejoined host to the live host maps
                hostPartitions.putAll(rejoin.getKey(), partitionsOnRejoinHost);
                for (int pid : partitionsOnRejoinHost) {
                    replicas.put(pid, CoreUtils.getHSIdFromHostAndSite(rejoin.getKey(), pid));
                }
            }
        }
    }

    private static Set<Integer> pickNodesInDiffGroupsToKill(ImmutableMap<Integer, String> topo,
                                                            Multimap<Integer, Integer> hostPartitions,
                                                            int kfactor,
                                                            int nodesToKill)
    {
        for (Set<Integer> perm : Sets.powerSet(topo.keySet())) {
            // Skip permutation size not equal to the node count to kill
            if (perm.size() != nodesToKill) {
                continue;
            }
            // If the nodes in the list share the group, skip
            if (perm.stream().map(topo::get).distinct().count() != nodesToKill) {
                continue;
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

    public void testFourNodesOneGroups() throws JSONException
    {
        Map<Integer, String> hostGroups = Maps.newHashMap();
        hostGroups.put(0, "0");
        hostGroups.put(1, "0");
        hostGroups.put(2, "0");
        hostGroups.put(3, "0");
        runConfigAndVerifyTopology(hostGroups, 1);
    }

    public void testFourNodesTwoGroups() throws JSONException
    {
        Map<Integer, String> hostGroups = Maps.newHashMap();
        hostGroups.put(0, "0");
        hostGroups.put(1, "0");
        hostGroups.put(2, "1");
        hostGroups.put(3, "1");
        runConfigAndVerifyTopology(hostGroups, 1);
    }

    public void testFourNodesTwoGroupsNonoptimal() throws JSONException
    {
        Map<Integer, String> hostGroups = Maps.newHashMap();
        hostGroups.put(0, "0");
        hostGroups.put(1, "0");
        hostGroups.put(2, "0");
        hostGroups.put(3, "1");
        runConfigAndVerifyTopology(hostGroups, 1);
    }

    public void testThreeNodesThreeGroups() throws JSONException
    {
        Map<Integer, String> hostGroups = Maps.newHashMap();
        hostGroups.put(0, "0");
        hostGroups.put(1, "1");
        hostGroups.put(2, "2");
        runConfigAndVerifyTopology(hostGroups, 2);
    }

    public void testSixNodesThreeGroups() throws JSONException
    {
        Map<Integer, String> hostGroups = Maps.newHashMap();
        hostGroups.put(0, "0");
        hostGroups.put(1, "0");
        hostGroups.put(2, "1");
        hostGroups.put(3, "1");
        hostGroups.put(4, "2");
        hostGroups.put(5, "2");
        runConfigAndVerifyTopology(hostGroups, 1);
    }

    public void testFiveNodesTwoGroups() throws JSONException
    {
        Map<Integer, String> hostGroups = Maps.newHashMap();
        hostGroups.put(0, "0");
        hostGroups.put(1, "0");
        hostGroups.put(2, "1");
        hostGroups.put(3, "1");
        hostGroups.put(4, "1");
        runConfigAndVerifyTopology(hostGroups, 2);
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
        runConfigAndVerifyTopology(hostGroups, 3);
    }

    public void testFifteenNodesTwoGroups() throws JSONException
    {
        Map<Integer, String> hostGroups = Maps.newHashMap();
        hostGroups.put(0, "0");
        hostGroups.put(1, "0");
        hostGroups.put(2, "0");
        hostGroups.put(3, "0");
        hostGroups.put(4, "0");
        hostGroups.put(5, "0");
        hostGroups.put(6, "0");
        hostGroups.put(7, "0");
        hostGroups.put(8, "1");
        hostGroups.put(9, "1");
        hostGroups.put(10, "1");
        hostGroups.put(11, "1");
        hostGroups.put(12, "1");
        hostGroups.put(13, "1");
        hostGroups.put(14, "1");
        runConfigAndVerifyTopology(hostGroups, 2);
    }

    public void testFiftNodesInThreeGroups() throws JSONException
    {
        Map<Integer, String> hostGroups = Maps.newHashMap();
        for (int i = 0; i < 50; i++) {
            hostGroups.put(i, String.valueOf(i % 3));
        }
        runConfigAndVerifyTopology(hostGroups, 2);
    }

    private static void runConfigAndVerifyTopology(Map<Integer, String> hostGroups, int kfactor) throws JSONException
    {
        final int maxSites = 20;
        int invalidConfigCount = 0;
        int expectedInvalidConfigs = 0;
        if (hostGroups.size() % (kfactor + 1) != 0) {
            expectedInvalidConfigs = maxSites - (maxSites / (kfactor + 1));
        }

        for (int sites = 1; sites <= maxSites; sites++) {
            final ClusterConfig config = new ClusterConfig(hostGroups.size(), sites, kfactor);
            System.out.println("Running config " + hostGroups.size() + " hosts, " + sites + " sitesperhost, k=" + kfactor);
            if (config.validate()) {
                final JSONObject topo = config.getTopology(hostGroups, HashMultimap.create(), new HashMap<>());
                verifyTopology(hostGroups, topo);
            } else {
                System.out.println(config.getErrorMsg());
                invalidConfigCount++;
            }
        }

        assertEquals(expectedInvalidConfigs, invalidConfigCount);
    }

    private static void verifyTopology(Map<Integer, String> hostGroups, JSONObject topo) throws JSONException
    {
        final int hostCount = new ClusterConfig(topo).getHostCount();
        final int sitesPerHost = new ClusterConfig(topo).getSitesPerHost();
        final int partitionCount = new ClusterConfig(topo).getPartitionCount();
        final int minMasterCount = partitionCount / hostCount;
        final int maxMasterCount = minMasterCount + 1;
        final int nodesWithMaxMasterCount = partitionCount % hostCount;

        Multimap<Integer, Integer> masterCountToHost = HashMultimap.create();
        Multimap<String, Integer> groupHosts = HashMultimap.create();
        Multimap<String, Integer> groupPartitions = HashMultimap.create();
        for (Map.Entry<Integer, String> entry : hostGroups.entrySet()) {
            final List<Integer> hostPartitions = ClusterConfig.partitionsForHost(topo, entry.getKey());
            final List<Integer> hostMasters = ClusterConfig.partitionsForHost(topo, entry.getKey(), true);
            assertEquals(sitesPerHost, hostPartitions.size());
            // Make sure a host only has unique partitions
            assertEquals(hostPartitions.size(), Sets.newHashSet(hostPartitions).size());

            masterCountToHost.put(hostMasters.size(), entry.getKey());
            groupHosts.put(entry.getValue(), entry.getKey());
            groupPartitions.putAll(entry.getValue(), hostPartitions);
        }

        // Make sure master partitions are spread out
        if (nodesWithMaxMasterCount == 0) {
            assertEquals(1, masterCountToHost.keySet().size());
            assertTrue(masterCountToHost.containsKey(minMasterCount));
        } else {
            assertEquals(2, masterCountToHost.keySet().size());
            assertTrue(masterCountToHost.containsKey(minMasterCount));
            assertEquals(nodesWithMaxMasterCount, masterCountToHost.get(maxMasterCount).size());
        }

        // Each group should have at least one copy of all the partitions
        for (Map.Entry<String, Collection<Integer>> groupAndPids : groupPartitions.asMap().entrySet()) {
            assertEquals(groupPartitions.toString(),
                         Math.min(partitionCount, groupHosts.get(groupAndPids.getKey()).size() * sitesPerHost),
                         Sets.newHashSet(groupAndPids.getValue()).size());
        }
    }
}
