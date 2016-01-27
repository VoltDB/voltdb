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

import com.google_voltpatches.common.collect.HashMultimap;
import com.google_voltpatches.common.collect.Maps;
import com.google_voltpatches.common.collect.Multimap;
import com.google_voltpatches.common.collect.Sets;
import org.json_voltpatches.JSONArray;
import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;

import junit.framework.TestCase;
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
        JSONObject topo = config.getTopology(topology);
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
        JSONObject topo = config.getTopology(topology);
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
        JSONObject topo = config.getTopology(topology);
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
        JSONObject topo = config.getTopology(topology);
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
            final ClusterConfig config = new ClusterConfig(hostGroups.size(), sites, kfactor);
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

        for (Map.Entry<String, Collection<Integer>> ghEntry : groupHosts.asMap().entrySet()) {
            if (ghEntry.getValue().size() * sitesPerHost < partitionCount) {
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
