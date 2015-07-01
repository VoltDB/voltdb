/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

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
        List<Integer> topology = Arrays.asList(new Integer[] { 0, 1, 2 });
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

    public void testSufficientHostsToReplicate()
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
        JSONObject topo = config.getTopology(Arrays.asList(0));
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
        JSONObject topo = config.getTopology(Arrays.asList(0, 1));
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
        JSONObject topo = config.getTopology(Arrays.asList(0, 1));
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
        JSONObject topo = config.getTopology(Arrays.asList(0, 1));
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
}
