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

package org.voltdb.iv2;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.zookeeper_voltpatches.ZooKeeper;

import org.json_voltpatches.JSONObject;

import org.mockito.ArgumentCaptor;

import org.voltcore.messaging.BinaryPayloadMessage;
import org.voltcore.messaging.HostMessenger;
import org.voltcore.messaging.VoltMessage;

import org.voltcore.zk.ZKTestBase;

import static org.junit.Assert.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.mockito.Mockito.*;

import org.voltdb.VoltZK;

public class TestCartographer extends ZKTestBase {

    @Before
    public void setUp() throws Exception
    {
        setUpZK(1);
    }

    @After
    public void tearDown() throws Exception
    {
        tearDownZK();
    }

    @Test
    public void testSortByValue()
    {
        Map<Integer, Integer> unsorted = new HashMap<Integer, Integer>();
        unsorted.put(0, 5);
        unsorted.put(1, 0);
        unsorted.put(2, 3);
        unsorted.put(3, 0);
        unsorted.put(4, 2);
        List<Integer> sorted = Cartographer.sortKeysByValue(unsorted);
        assertEquals((Integer)1, sorted.get(0));
        assertEquals((Integer)3, sorted.get(1));
        assertEquals((Integer)4, sorted.get(2));
        assertEquals((Integer)2, sorted.get(3));
        assertEquals((Integer)0, sorted.get(4));
    }

    @Test
    public void testReplicateLeastReplicated()
    {
        // Gin up our fake replication counts to leave the last partition needing two replicas
        // 5 hosts, 3 sites/host, k=2 is 15 sites, 5 partitions.
        // Need 2 hosts dead, so 6 missing replicas, last one needs to miss 2
        Map<Integer, Integer> repsPerPart = new HashMap<Integer, Integer>();
        repsPerPart.put(0, 2);
        repsPerPart.put(1, 2);
        repsPerPart.put(2, 2);
        repsPerPart.put(3, 2);
        repsPerPart.put(4, 1);
        int kfactor = 2;
        int numberOfPartitions = 5;
        int sitesPerHost = 3;

        List<Integer> firstRejoin =
            Cartographer.computeReplacementPartitions(repsPerPart, kfactor, sitesPerHost);
        assertEquals(3, firstRejoin.size());
        assertEquals((Integer)4, firstRejoin.get(0));
        assertEquals((Integer)0, firstRejoin.get(1));
        assertEquals((Integer)1, firstRejoin.get(2));
        // add to each of the repsPerPart from the firstRejoin results
        for (int partition : firstRejoin) {
            repsPerPart.put(partition, repsPerPart.get(partition) + 1);
        }

        // now do it again and make sure we fully replicate
        List<Integer> secondRejoin =
            Cartographer.computeReplacementPartitions(repsPerPart, kfactor, sitesPerHost);
        assertEquals(3, secondRejoin.size());
        assertEquals((Integer)2, secondRejoin.get(0));
        assertEquals((Integer)3, secondRejoin.get(1));
        assertEquals((Integer)4, secondRejoin.get(2));
    }

    @Test
    public void testNoOverReplication()
    {
        // We'll set things up to only require one more replica and then offer up a host with more sites
        // than we need.  This particular case could never happen but should test the logic correctly.
        // Also, test that we don't replicate the same partition twice on the same host again.
        int kfactor = 2;
        int numberOfPartitions = 5;
        int sitesPerHost = 3;

        Map<Integer, Integer> repsPerPart = new HashMap<Integer, Integer>();
        repsPerPart.put(0, 3);
        repsPerPart.put(1, 3);
        repsPerPart.put(2, 3);
        repsPerPart.put(3, 3);
        repsPerPart.put(4, 1);

        List<Integer> firstRejoin =
            Cartographer.computeReplacementPartitions(repsPerPart, kfactor, sitesPerHost);
        assertEquals(1, firstRejoin.size());
        assertEquals((Integer)4, firstRejoin.get(0));
    }

    @Test
    public void testSPMasterChange() throws Exception
    {
        ZooKeeper zk = getClient(0);
        VoltZK.createPersistentZKNodes(zk);
        LeaderCache spwriter = new LeaderCache(zk, VoltZK.iv2masters);
        HostMessenger hm = mock(HostMessenger.class);
        when(hm.getZK()).thenReturn(m_messengers.get(0).getZK());
        Cartographer dut = new Cartographer(hm, 0, false);
        // Startup partitions
        spwriter.start(true);
        spwriter.put(0, 0l);
        verify(hm, timeout(10000)).send(anyLong(), any(VoltMessage.class));
        reset(hm);
        spwriter.put(1, 1l);
        verify(hm, timeout(10000)).send(anyLong(), any(VoltMessage.class));
        reset(hm);
        spwriter.put(2, 2l);
        verify(hm, timeout(10000)).send(anyLong(), any(VoltMessage.class));
        reset(hm);

        // now change master for part 0
        spwriter.put(0, 3l);
        ArgumentCaptor<Long> hsIdCaptor = ArgumentCaptor.forClass(Long.class);
        ArgumentCaptor<BinaryPayloadMessage> bpmCaptor = ArgumentCaptor.forClass(BinaryPayloadMessage.class);
        verify(hm, timeout(10000)).send(hsIdCaptor.capture(), bpmCaptor.capture());
        JSONObject jsObj = new JSONObject(new String(bpmCaptor.getValue().m_payload, "UTF-8"));
        System.out.println("BPM: " + jsObj.toString());
        final int partitionId = jsObj.getInt(Cartographer.JSON_PARTITION_ID);
        final long initiatorHSId = jsObj.getLong(Cartographer.JSON_INITIATOR_HSID);
        assertEquals(0, partitionId);
        assertEquals(3, initiatorHSId);
        spwriter.shutdown();
    }

    @Test
    public void testMPIChange() throws Exception
    {
        ZooKeeper zk = getClient(0);
        VoltZK.createPersistentZKNodes(zk);
        LeaderCache mpwriter = new LeaderCache(zk, VoltZK.iv2mpi);
        HostMessenger hm = mock(HostMessenger.class);
        when(hm.getZK()).thenReturn(m_messengers.get(0).getZK());
        Cartographer dut = new Cartographer(hm, 0, false);
        mpwriter.start(true);
        // initial master
        mpwriter.put(MpInitiator.MP_INIT_PID, 0l);
        verify(hm, timeout(10000)).send(anyLong(), any(VoltMessage.class));
        reset(hm);

        // Now change the master
        mpwriter.put(MpInitiator.MP_INIT_PID, 3l);
        ArgumentCaptor<Long> hsIdCaptor = ArgumentCaptor.forClass(Long.class);
        ArgumentCaptor<BinaryPayloadMessage> bpmCaptor = ArgumentCaptor.forClass(BinaryPayloadMessage.class);
        verify(hm, timeout(10000)).send(hsIdCaptor.capture(), bpmCaptor.capture());
        JSONObject jsObj = new JSONObject(new String(bpmCaptor.getValue().m_payload, "UTF-8"));
        final int partitionId = jsObj.getInt(Cartographer.JSON_PARTITION_ID);
        final long initiatorHSId = jsObj.getLong(Cartographer.JSON_INITIATOR_HSID);
        assertEquals(MpInitiator.MP_INIT_PID, partitionId);
        assertEquals(3, initiatorHSId);
        mpwriter.shutdown();
    }
}
