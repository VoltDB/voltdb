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
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.json_voltpatches.JSONArray;
import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;
import org.voltdb.VoltDB;

import com.google_voltpatches.common.collect.HashMultimap;
import com.google_voltpatches.common.collect.Maps;
import com.google_voltpatches.common.collect.Multimap;
import com.google_voltpatches.common.collect.Sets;

import junit.framework.TestCase;

public class TestClusterCompiler extends TestCase
{
    private static Map<Integer, Integer> generateSameSphMap(int hostCount, int sitesperhost) {
        Map<Integer, Integer> sphMap = Maps.newHashMap();
        for (int hostId = 0; hostId < hostCount; hostId++) {
            sphMap.put(hostId, sitesperhost);
        }
        return sphMap;
    }

    public void testNonZeroReplicationFactor() throws Exception
    {
        final int hostCount = 3;
        final int Kfactor = 2;
        final int sitesperhost = 1;
        ClusterConfig config = new ClusterConfig(hostCount,
                                                 generateSameSphMap(hostCount, sitesperhost),
                                                 Kfactor);
        Map<Integer, String> topology = Maps.newHashMap();
        topology.put(0, "0");
        topology.put(1, "0");
        topology.put(2, "0");
        JSONObject obj = config.getTopology(topology);
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
        final int hostCount = 2;
        final int Kfactor = 2;
        final int sitesperhost = 6;
        ClusterConfig config = new ClusterConfig(hostCount,
                                                 generateSameSphMap(hostCount, sitesperhost),
                                                 Kfactor);
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
        final int hostCount = 1;
        final int Kfactor = 0;
        final int sitesperhost = 6;
        final int partitionNum = (hostCount * sitesperhost) / (Kfactor + 1);
        Map<Integer, Integer> sphMap = generateSameSphMap(hostCount, sitesperhost);
        ClusterConfig config = new ClusterConfig(hostCount, sphMap, Kfactor);
        Map<Integer, String> topology = Maps.newHashMap();
        topology.put(0, "0");
        JSONObject topo = config.getTopology(topology);
        assertEquals(hostCount, topo.getInt("hostcount"));

        int size = topo.getJSONArray("host_id_to_sph").length();
        assertEquals(sphMap.size(), size);
        for (int ii = 0; ii < size; ii++) {
            JSONObject sphObj = topo.getJSONArray("host_id_to_sph").getJSONObject(ii);
            int hostId = sphObj.getInt("host_id");
            int sph = sphObj.getInt("sites_per_host");
            assertTrue(sphMap.containsKey(hostId));
            assertTrue(sphMap.get(hostId) == sph);
        }
        assertEquals(Kfactor, topo.getInt("kfactor"));
        assertEquals(partitionNum, topo.getJSONArray("partitions").length());

        // add one host
        ClusterConfig.addHosts(1, topo);
        assertEquals(hostCount + 1, topo.getInt("hostcount"));
        assertEquals(Kfactor, topo.getInt("kfactor"));
    }

    public void testAddHostsToNonKsafe() throws JSONException
    {
        final int hostCount = 2;
        final int Kfactor = 0;
        final int sitesperhost = 6;
        final int partitionNum = (hostCount * sitesperhost) / (Kfactor + 1);
        Map<Integer, Integer> sphMap = generateSameSphMap(hostCount, sitesperhost);
        ClusterConfig config = new ClusterConfig(hostCount, sphMap, Kfactor);
        Map<Integer, String> topology = Maps.newHashMap();
        topology.put(0, "0");
        topology.put(1, "0");
        JSONObject topo = config.getTopology(topology);
        assertEquals(hostCount, topo.getInt("hostcount"));

        int size = topo.getJSONArray("host_id_to_sph").length();
        assertEquals(sphMap.size(), size);
        for (int ii = 0; ii < size; ii++) {
            JSONObject sphObj = topo.getJSONArray("host_id_to_sph").getJSONObject(ii);
            int hostId = sphObj.getInt("host_id");
            int sph = sphObj.getInt("sites_per_host");
            assertTrue(sphMap.containsKey(hostId));
            assertTrue(sphMap.get(hostId) == sph);
        }
        assertEquals(Kfactor, topo.getInt("kfactor"));
        assertEquals(partitionNum, topo.getJSONArray("partitions").length());

        // add two hosts then fail
        VoltDB.ignoreCrash = true;
        try {
            ClusterConfig.addHosts(2, topo);
            fail("Shouldn't allow adding more than one node");
        } catch (AssertionError e) {}
    }

    public void testAddHostsToKsafe() throws JSONException
    {
        final int hostCount = 2;
        final int Kfactor = 1;
        final int sitesperhost = 6;
        final int partitionNum = (hostCount * sitesperhost) / (Kfactor + 1);
        Map<Integer, Integer> sphMap = generateSameSphMap(hostCount, sitesperhost);
        ClusterConfig config = new ClusterConfig(hostCount, sphMap, Kfactor);
        Map<Integer, String> topology = Maps.newHashMap();
        topology.put(0, "0");
        topology.put(1, "0");
        JSONObject topo = config.getTopology(topology);
        assertEquals(hostCount, topo.getInt("hostcount"));
        int size = topo.getJSONArray("host_id_to_sph").length();
        assertEquals(sphMap.size(), size);
        for (int ii = 0; ii < size; ii++) {
            JSONObject sphObj = topo.getJSONArray("host_id_to_sph").getJSONObject(ii);
            int hostId = sphObj.getInt("host_id");
            int sph = sphObj.getInt("sites_per_host");
            assertTrue(sphMap.containsKey(hostId));
            assertTrue(sphMap.get(hostId) == sph);
        }
        assertEquals(Kfactor, topo.getInt("kfactor"));
        assertEquals(partitionNum, topo.getJSONArray("partitions").length());

        // add two hosts
        ClusterConfig.addHosts(2, topo);
        assertEquals(hostCount + 2, topo.getInt("hostcount"));
        assertEquals(Kfactor, topo.getInt("kfactor"));
    }

    public void testAddMoreThanKsafeHosts() throws JSONException
    {
        final int hostCount = 2;
        final int Kfactor = 1;
        final int sitesperhost = 6;
        final int partitionNum = (hostCount * sitesperhost) / (Kfactor + 1);
        Map<Integer, Integer> sphMap = generateSameSphMap(hostCount, sitesperhost);
        ClusterConfig config = new ClusterConfig(hostCount, sphMap, Kfactor);
        Map<Integer, String> topology = Maps.newHashMap();
        topology.put(0, "0");
        topology.put(1, "0");
        JSONObject topo = config.getTopology(topology);
        assertEquals(hostCount, topo.getInt("hostcount"));
        int size = topo.getJSONArray("host_id_to_sph").length();
        assertEquals(sphMap.size(), size);
        for (int ii = 0; ii < size; ii++) {
            JSONObject sphObj = topo.getJSONArray("host_id_to_sph").getJSONObject(ii);
            int hostId = sphObj.getInt("host_id");
            int sph = sphObj.getInt("sites_per_host");
            assertTrue(sphMap.containsKey(hostId));
            assertTrue(sphMap.get(hostId) == sph);
        }
        assertEquals(Kfactor, topo.getInt("kfactor"));
        assertEquals(partitionNum, topo.getJSONArray("partitions").length());

        // add three host then fail
        VoltDB.ignoreCrash = true;
        try {
            ClusterConfig.addHosts(3, topo);
            fail("Shouldn't allow adding more than ksafe + 1 node");
        } catch (AssertionError e) {}
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
        runConfigAndVerifyTopology(hostGroups, 2);
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

    private static void runConfigAndVerifyTopology(Map<Integer, String> hostGroups, int kfactor) throws JSONException
    {
        final int maxSites = 20;
        int invalidConfigCount = 0;
        int expectedInvalidConfigs = 0;
        if (hostGroups.size() % (kfactor + 1) != 0) {
            expectedInvalidConfigs = maxSites - (maxSites / (kfactor + 1));
        }

        for (int sites = 1; sites <= maxSites; sites++) {
            Map<Integer, Integer> sphMap = Maps.newHashMap();
            for (int host = 0; host < hostGroups.size(); host++) {
                sphMap.put(host, sites);
            }
            final ClusterConfig config = new ClusterConfig(hostGroups.size(), sphMap, kfactor);
            System.out.println("Running config " + sites + " sitesperhost, k=" + kfactor);
            if (config.validate()) {
                final JSONObject topo = config.getTopology(hostGroups);
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
        final Map<Integer, Integer> sitesPerHostMap = new ClusterConfig(topo).getSitesPerHostMap();
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
            assertNotNull(sitesPerHostMap.get(entry.getKey()));
            int sitesPerHost = sitesPerHostMap.get(entry.getKey());
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

        for (Map.Entry<String, Collection<Integer>> ghEntry : groupHosts.asMap().entrySet()) {
            int sitesCount = 0;
            for(Integer hostId : ghEntry.getValue()) {
                sitesCount += sitesPerHostMap.get(hostId);
            }
            if (sitesCount < partitionCount) {
                // Non-optimal topology, no need to verify the rest
                System.out.println("Group " + ghEntry.getKey() + " only has " + ghEntry.getValue().size() + " hosts");
                return;
            }
        }

        // Each group should have at least one copy of all the partitions
        for (Collection<Integer> partitions : groupPartitions.asMap().values()) {
            assertEquals(partitionCount, Sets.newHashSet(partitions).size());
        }
    }
}
