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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.zookeeper_voltpatches.KeeperException;
import org.apache.zookeeper_voltpatches.ZooKeeper;
import org.json_voltpatches.JSONException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.voltcore.messaging.HostMessenger;
import org.voltcore.zk.LeaderElector;
import org.voltcore.zk.ZKTestBase;
import org.voltcore.zk.ZKUtil;
import org.voltdb.TheHashinator;
import org.voltdb.VoltDB;
import org.voltdb.VoltZK;
import org.voltdb.compiler.ClusterConfig;
import org.voltdb.compiler.ClusterConfig.ExtensibleGroupTag;

import com.google_voltpatches.common.collect.HashMultimap;
import com.google_voltpatches.common.collect.ImmutableMap;
import com.google_voltpatches.common.collect.Maps;
import com.google_voltpatches.common.collect.Sets;

public class TestLeaderAppointer extends ZKTestBase {

    private final int NUM_AGREEMENT_SITES = 1;
    private ClusterConfig m_config = null;
    private Set<Integer> m_hostIds;
    private Map<Integer, ExtensibleGroupTag> m_hostGroups;
    private MpInitiator m_mpi = null;
    private HostMessenger m_hm = null;
    private ZooKeeper m_zk = null;
    private LeaderCache m_cache = null;
    private final AtomicBoolean m_newAppointee = new AtomicBoolean(false);

    private LeaderAppointer m_dut = null;

    LeaderCache.Callback m_changeCallback = new LeaderCache.Callback()
    {
        @Override
        public void run(ImmutableMap<Integer, Long> cache) {
            m_newAppointee.set(true);
        }
    };

    @Before
    public void setUp() throws Exception
    {
        VoltDB.ignoreCrash = false;
        VoltDB.wasCrashCalled = false;
        setUpZK(NUM_AGREEMENT_SITES);
    }

    @After
    public void tearDown() throws Exception
    {
        if (m_dut != null) {
            m_dut.shutdown();
        }
        if (m_cache != null) {
            m_cache.shutdown();
        }
        if (m_zk != null) {
            m_zk.close();
        }
        tearDownZK();
    }

    void configure(int hostCount, int sitesPerHost, int replicationFactor,
            boolean enablePPD) throws JSONException, Exception
    {
        m_hm = mock(HostMessenger.class);
        m_zk = getClient(0);
        when(m_hm.getZK()).thenReturn(m_zk);
        VoltZK.createPersistentZKNodes(m_zk);

        m_config = new ClusterConfig(hostCount, sitesPerHost, replicationFactor);
        TheHashinator.initialize(TheHashinator.getConfiguredHashinatorClass(), TheHashinator.getConfigureBytes(m_config.getPartitionCount()));
        m_hostIds = Sets.newTreeSet();
        m_hostGroups = Maps.newHashMap();
        for (int i = 0; i < hostCount; i++) {
            m_hostIds.add(i);
            m_hostGroups.put(i, new ExtensibleGroupTag("0", "0"));
        }
        when(m_hm.getLiveHostIds()).thenReturn(m_hostIds);
        m_mpi = mock(MpInitiator.class);
        createAppointer(enablePPD);

        m_cache = new LeaderCache(m_zk, VoltZK.iv2appointees, m_changeCallback);
        m_cache.start(true);
    }

    void createAppointer(boolean enablePPD) throws JSONException
    {
        KSafetyStats stats = new KSafetyStats();
        m_dut = new LeaderAppointer(m_hm, m_config.getPartitionCount(),
                                    m_config.getReplicationFactor(),
                                    null, m_config.getTopology(m_hostGroups, HashMultimap.create(), new HashMap<>()), m_mpi, stats, false);
        m_dut.onReplayCompletion();
    }

    void addReplica(int partitionId, long HSId) throws KeeperException, InterruptedException, Exception
    {
        LeaderElector.createParticipantNode(m_zk,
                LeaderElector.electionDirForPartition(VoltZK.leaders_initiators, partitionId),
                Long.toString(HSId), null);
    }

    void deleteReplica(int partitionId, long HSId) throws KeeperException, InterruptedException
    {
        String dir = LeaderElector.electionDirForPartition(VoltZK.leaders_initiators, partitionId);
        List<String> children = m_zk.getChildren(dir, false);
        for (String child : children) {
            if (LeaderElector.getPrefixFromChildName(child).equals(Long.toString(HSId))) {
                String victim = ZKUtil.joinZKPath(dir, child);
                m_zk.delete(victim, -1);
            }
        }
    }

    void registerLeader(int partitionId, long HSId) throws KeeperException, InterruptedException
    {
        LeaderCacheWriter iv2masters = new LeaderCache(m_zk, VoltZK.iv2masters);
        iv2masters.put(partitionId, HSId);
    }

    void waitForAppointee(int partitionId) throws InterruptedException
    {
        while (m_cache.pointInTimeCache().get(partitionId) == null) {
            Thread.sleep(0);
        }
    }

    @Test
    public void testBasicStartup() throws Exception
    {
        configure(2, 2, 0, false);

        Thread dutthread = new Thread() {
            @Override
            public void run() {
                try {
                    m_dut.acceptPromotion();
                } catch (Exception e) {
                }
            }
        };

        dutthread.start();

        addReplica(0, 0L);
        addReplica(1, 1L);
        addReplica(2, 2L);
        addReplica(3, 3L);

        waitForAppointee(3);
        assertEquals(0L, (long)m_cache.pointInTimeCache().get(0));
        assertEquals(1L, (long)m_cache.pointInTimeCache().get(1));
        assertEquals(2L, (long)m_cache.pointInTimeCache().get(2));
        assertEquals(3L, (long)m_cache.pointInTimeCache().get(3));

        registerLeader(0, 0L);
        registerLeader(1, 1L);
        registerLeader(2, 2L);
        registerLeader(3, 3L);

        dutthread.join();

        verify(m_mpi, times(1)).acceptPromotion();
    }

    @Test
    public void testStartupFailure() throws Exception
    {
        configure(2, 2, 1, false);
        // Write an appointee before we start to simulate a failure during startup
        m_cache.put(0, 0L);
        VoltDB.ignoreCrash = true;
        boolean threw = false;
        try {
            m_dut.acceptPromotion();
        }
        catch (ExecutionException e) {
            if (e.getCause() instanceof AssertionError) {
                threw = true;
            }
        }
        assertTrue(threw);
        assertTrue(VoltDB.wasCrashCalled);
    }

    @Test
    public void testStartupFailure2() throws Exception
    {
        configure(2, 2, 1, false);
        // Write the appointees and one master before we start to simulate a failure during startup
        m_cache.put(0, 0L);
        m_cache.put(1, 1L);
        waitForAppointee(1);
        registerLeader(0, 0L);
        VoltDB.ignoreCrash = true;
        boolean threw = false;
        try {
            m_dut.acceptPromotion();
        }
        catch (ExecutionException e) {
            if (e.getCause() instanceof AssertionError) {
                threw = true;
            }
        }
        assertTrue(threw);
        assertTrue(VoltDB.wasCrashCalled);
    }

    @Test
    public void testFailureDuringReplay() throws Exception
    {
        configure(2, 2, 1, false);
        Thread dutthread = new Thread() {
            @Override
            public void run() {
                try {
                    m_dut.acceptPromotion();
                } catch (Exception e) {
                }
            }
        };
        dutthread.start();
        // Need to sleep so we don't write to ZK before the LeaderAppointer appears or we'll crash
        Thread.sleep(1000);
        addReplica(0, 0L);
        addReplica(0, 1L);
        addReplica(1, 2L);
        addReplica(1, 3L);
        waitForAppointee(1);
        registerLeader(0, m_cache.pointInTimeCache().get(0));
        registerLeader(1, m_cache.pointInTimeCache().get(1));
        dutthread.join();
        // kill the appointer and delete one of the leaders
        m_dut.shutdown();
        deleteReplica(0, m_cache.pointInTimeCache().get(0));
        // create a new appointer and start it up in the replay state
        m_dut = new LeaderAppointer(m_hm,
                                    m_config.getPartitionCount(),
                                    m_config.getReplicationFactor(),
                                    null,
                                    m_config.getTopology(m_hostGroups, HashMultimap.create(), new HashMap<>()),
                                    m_mpi,
                                    new KSafetyStats(),
                                    false);
        m_newAppointee.set(false);
        VoltDB.ignoreCrash = true;
        boolean threw = false;
        try {
            m_dut.acceptPromotion();
        } catch (ExecutionException e) {
            threw = true;
        }
        assertTrue(threw);
        assertTrue(VoltDB.wasCrashCalled);
    }

    @Test
    public void testWaitsForAllReplicasAndLeaders() throws Exception
    {
        configure(2, 2, 1, false);
        Thread dutthread = new Thread() {
            @Override
            public void run() {
                try {
                    m_dut.acceptPromotion();
                } catch (Exception e) {
                }
            }
        };
        dutthread.start();
        // Need to sleep so we don't write to ZK before the LeaderAppointer appears or we'll crash
        Thread.sleep(1000);

        // Write the appointees and one master before we start to simulate a failure during startup
        addReplica(0, 0L);
        // Sleep just a little to give time for the above put to hopefully
        // trickle into ZK.  Not really a better condition to wait on here.
        Thread.sleep(500);
        System.out.println("pitc: " + m_cache.pointInTimeCache());
        assertTrue(m_cache.pointInTimeCache().get(0) == null);
        addReplica(0, 1L);
        waitForAppointee(0);
        addReplica(1, 2L);
        addReplica(1, 3L);
        waitForAppointee(1);
        // NOTE: these might not be the real masters, but they'll make progress for now,
        // unless we choose to add sanity checking to the LeaderAppointer
        registerLeader(0, m_cache.pointInTimeCache().get(0));
        // Bleh, still need to sleep here, since we're basically waiting on no change to state
        Thread.sleep(1000);
        verify(m_mpi, times(0)).acceptPromotion();
        registerLeader(1, m_cache.pointInTimeCache().get(1));
        dutthread.join();
        verify(m_mpi, times(1)).acceptPromotion();
    }

    @Test
    public void testPromotesOnReplicaDeathAndDiesOnKSafety() throws Exception
    {
        // run once to get to a startup state
        configure(2, 2, 1, false);
        VoltDB.ignoreCrash = true;
        Thread dutthread = new Thread() {
            @Override
            public void run() {
                try {
                    m_dut.acceptPromotion();
                } catch (Exception e) {
                }
            }
        };
        dutthread.start();
        // Need to sleep so we don't write to ZK before the LeaderAppointer appears or we'll crash
        Thread.sleep(1000);
        addReplica(0, 0L);
        addReplica(0, 1L);
        addReplica(1, 2L);
        addReplica(1, 3L);
        waitForAppointee(1);
        registerLeader(0, m_cache.pointInTimeCache().get(0));
        registerLeader(1, m_cache.pointInTimeCache().get(1));
        dutthread.join();
        // Now, delete the leader of partition 0 from ZK
        m_newAppointee.set(false);
        deleteReplica(0, m_cache.pointInTimeCache().get(0));
        while (!m_newAppointee.get()) {
            Thread.sleep(0);
        }
        assertEquals(1L, (long)m_cache.pointInTimeCache().get(0));
        // now, kill the other replica and watch everything BURN
        deleteReplica(0, m_cache.pointInTimeCache().get(0));
        while (!VoltDB.wasCrashCalled) {
            Thread.sleep(0);
        }
    }

    @Test
    public void testAppointerPromotion() throws Exception
    {
        // run once to get to a startup state
        configure(2, 2, 1, false);
        VoltDB.ignoreCrash = true;
        Thread dutthread = new Thread() {
            @Override
            public void run() {
                try {
                    m_dut.acceptPromotion();
                } catch (Exception e) {
                }
            }
        };
        dutthread.start();
        // Need to sleep so we don't write to ZK before the LeaderAppointer appears or we'll crash
        Thread.sleep(1000);
        addReplica(0, 0L);
        addReplica(0, 1L);
        addReplica(1, 2L);
        addReplica(1, 3L);
        waitForAppointee(1);
        registerLeader(0, m_cache.pointInTimeCache().get(0));
        registerLeader(1, m_cache.pointInTimeCache().get(1));
        dutthread.join();
        // kill the appointer and delete one of the leaders
        m_dut.shutdown();
        deleteReplica(0, m_cache.pointInTimeCache().get(0));
        // create a new appointer and start it up
        createAppointer(false);
        m_newAppointee.set(false);
        m_dut.acceptPromotion();
        while (!m_newAppointee.get()) {
            Thread.sleep(0);
        }
        assertEquals(1L, (long)m_cache.pointInTimeCache().get(0));

        // Add a new partition with two replicas, see if the newly elected leader appointer picks up the new
        // partition and elects a new leader
        addReplica(2, 4L);
        addReplica(2, 5L);
        waitForAppointee(2);
    }

    @Test
    public void testAddPartition() throws Exception
    {
        // run once to get to a startup state
        configure(2, 2, 1, false);
        VoltDB.ignoreCrash = true;
        Thread dutthread = new Thread() {
            @Override
            public void run() {
                try {
                    m_dut.acceptPromotion();
                } catch (Exception e) {
                }
            }
        };
        dutthread.start();
        // Need to sleep so we don't write to ZK before the LeaderAppointer appears or we'll crash
        Thread.sleep(1000);
        addReplica(0, 0L);
        addReplica(0, 1L);
        addReplica(1, 2L);
        addReplica(1, 3L);
        waitForAppointee(1);
        registerLeader(0, m_cache.pointInTimeCache().get(0));
        registerLeader(1, m_cache.pointInTimeCache().get(1));
        dutthread.join();
        assertFalse(VoltDB.wasCrashCalled);

        // Create a partition dir
        LeaderElector.createRootIfNotExist(m_zk, LeaderElector.electionDirForPartition(VoltZK.leaders_initiators, 2));
        Thread.sleep(500); // I'm evil
        assertFalse(VoltDB.wasCrashCalled);

        // Now, add a replica for partition 2, should be promoted
        m_newAppointee.set(false);
        addReplica(2, 4L);
        while (!m_newAppointee.get()) {
            Thread.sleep(0);
        }
        assertEquals(4L, (long)m_cache.pointInTimeCache().get(2));


        // Now deleting the only replica for partition 2 shouldn't crash because it isn't on the ring
        // for elastic, but for legacy it should crash immediately
        VoltDB.wasCrashCalled = false;
        deleteReplica(2, m_cache.pointInTimeCache().get(2));

        if (TheHashinator.getConfiguredHashinatorType() == TheHashinator.HashinatorType.LEGACY) {
            while (!VoltDB.wasCrashCalled) {
                Thread.yield();
            }
            return;
        }

        //For elastic hashinator do more testing
        Thread.sleep(1000);
        assertFalse(VoltDB.wasCrashCalled);

        // Now, add a replica for partition 2, should be promoted
        m_newAppointee.set(false);
        addReplica(2, 4L);
        while (!m_newAppointee.get()) {
            Thread.sleep(0);
        }
        assertEquals(4L, (long)m_cache.pointInTimeCache().get(2));

        TheHashinator.initialize(TheHashinator.getConfiguredHashinatorClass(), TheHashinator.getConfigureBytes(4));
        //Deleting it now should cause a crash, now that the partition is on the ring
        deleteReplica(2, m_cache.pointInTimeCache().get(2));
        while (!VoltDB.wasCrashCalled) {
            Thread.yield();
        }
    }

    @Test
    public void testFailureDuringSyncSnapshot() throws Exception
    {
        configure(2, 2, 1, false);
        Thread dutthread = new Thread() {
            @Override
            public void run() {
                try {
                    m_dut.acceptPromotion();
                } catch (Exception e) {
                }
            }
        };
        dutthread.start();
        // Need to sleep so we don't write to ZK before the LeaderAppointer appears or we'll crash
        Thread.sleep(1000);
        addReplica(0, 0L);
        addReplica(0, 1L);
        addReplica(1, 2L);
        addReplica(1, 3L);
        waitForAppointee(1);
        registerLeader(0, m_cache.pointInTimeCache().get(0));
        registerLeader(1, m_cache.pointInTimeCache().get(1));
        dutthread.join();
        // kill the appointer and delete one of the leaders
        m_dut.shutdown();
        deleteReplica(0, m_cache.pointInTimeCache().get(0));
        // create a new appointer and start it up with expectSyncSnapshot=true
        m_dut = new LeaderAppointer(m_hm,
                                    m_config.getPartitionCount(),
                                    m_config.getReplicationFactor(),
                                    null,
                                    m_config.getTopology(m_hostGroups, HashMultimap.create(), new HashMap<>()),
                                    m_mpi,
                                    new KSafetyStats(),
                                    true);
        m_dut.onReplayCompletion();
        m_newAppointee.set(false);
        VoltDB.ignoreCrash = true;
        boolean threw = false;
        try {
            m_dut.acceptPromotion();
        } catch (ExecutionException e) {
            threw = true;
        }
        assertTrue(threw);
        assertTrue(VoltDB.wasCrashCalled);
    }
}
