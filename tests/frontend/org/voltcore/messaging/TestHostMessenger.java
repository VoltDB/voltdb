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

package org.voltcore.messaging;

import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertNotSame;
import static junit.framework.Assert.assertNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.entry;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.google_voltpatches.common.collect.ImmutableMap;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.zookeeper_voltpatches.ZooKeeper;
import org.assertj.core.util.Sets;
import org.json_voltpatches.JSONObject;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.voltcore.common.Constants;
import org.voltcore.utils.Pair;
import org.voltcore.zk.CoreZK;
import org.voltdb.StartAction;
import org.voltdb.VoltDB;
import org.voltdb.common.NodeState;
import org.voltdb.probe.MeshProber;
import org.voltdb.probe.MeshProber.Determination;

import com.google_voltpatches.common.base.Supplier;

public class TestHostMessenger {

    private static final ArrayList<HostMessenger> createdMessengers = new ArrayList<HostMessenger>();

    static class NodeStateRef extends AtomicReference<NodeState> implements Supplier<NodeState> {
        private static final long serialVersionUID = 1L;
        public NodeStateRef() {
        }
        public NodeStateRef(NodeState initialValue) {
            super(initialValue);
        }
    }

    @BeforeClass
    public static void setUpClass() throws Exception {
        System.setProperty("MESH_JOIN_RETRY_INTERVAL", "0");
        System.setProperty("MESH_JOIN_RETRY_INTERVAL_SALT", "1");
    }

    @Before
    public void setUp() throws Exception {
    }

    @After
    public void tearDown() throws Exception {
        VoltDB.crashMessage = null;
        VoltDB.ignoreCrash  = false;
        VoltDB.wasCrashCalled = false;

        for (HostMessenger hm : createdMessengers) {
            hm.shutdown();
        }
        createdMessengers.clear();
    }

    private HostMessenger createHostMessenger(int index, int hostcount) throws Exception {
        return createHostMessenger(index, true, hostcount);
    }

    private HostMessenger createHostMessenger(int index, boolean start, int hostcount) throws Exception {

        assertTrue("index is bigger than hostcount", index < hostcount);
        final HostMessenger.Config config = new HostMessenger.Config(false);
        String [] coordinators = IntStream.range(0, hostcount)
                .mapToObj(i -> ":" + (i+config.internalPort))
                .toArray(s -> new String[s]);
        config.acceptor = MeshProber.builder()
                .coordinators(coordinators)
                .build();
        config.internalPort = config.internalPort + index;
        config.zkInterface = "127.0.0.1";
        config.zkPort = 7181 + index;
        HostMessenger hm = new HostMessenger(config, null, randomHostDisplayName());
        createdMessengers.add(hm);
        if (start) {
            hm.start();
        }
        return hm;
    }

    private MeshProber prober(HostMessenger hm) {
        return MeshProber.prober(hm);
    }

    private String [] coordinators(int hostCount) {
        return IntStream.range(0, hostCount)
                .mapToObj(i -> ":" + (i+Constants.DEFAULT_INTERNAL_PORT))
                .toArray(s -> new String[s]);
    }

    private HostMessenger createHostMessenger(int index, JoinAcceptor jc,
            boolean start)  throws Exception {
        return createHostMessengerWithDisplayName(index, jc, start, randomHostDisplayName());
    }

    private HostMessenger createHostMessengerWithDisplayName(int index,
                                                             JoinAcceptor jc,
                                                             boolean start,
                                                             String hostDisplayName) throws Exception {
        if (jc instanceof MeshProber) {
            assertTrue("index is bigger than hostcount", index < ((MeshProber) jc).getHostCount());
        }
        final HostMessenger.Config config;
        if (jc instanceof MeshProber) {
            config = new HostMessenger.Config(((MeshProber) jc).isPaused());
        } else {
            config = new HostMessenger.Config(false);
        }
        config.internalPort = config.internalPort + index;
        config.zkInterface = "127.0.0.1";
        config.zkPort = 7181 + index;
        config.acceptor = jc;
        HostMessenger hm = new HostMessenger(config, null, hostDisplayName);
        createdMessengers.add(hm);
        if (start) {
            hm.start();
        }
        return hm;
    }

    private String randomHostDisplayName() {
        return RandomStringUtils.random(20);
    }

    static class HostMessengerThread extends Thread {
        final HostMessenger hm;
        final AtomicReference<Exception> exception;
        public HostMessengerThread(HostMessenger hm) {

            this.hm = hm;
            this.exception = new AtomicReference<Exception>(null);
        }
        public HostMessengerThread(HostMessenger hm, AtomicReference<Exception> exception) {
            this.hm = hm;
            this.exception = exception;
        }
        @Override
        public void run() {
            try {
                hm.start();
            } catch (Exception e) {
                e.printStackTrace();
                exception.compareAndSet(null, e);
            }
        }
    }

    @Test
    public void testSingleHost() throws Exception {
        HostMessenger hm = createHostMessenger(0,1);

        Mailbox m1 = hm.createMailbox();

        SiteMailbox sm = new SiteMailbox(hm, (-2L << 32));

        hm.createMailbox(sm.getHSId(), sm);

        sm.send(m1.getHSId(), new LocalObjectMessage(null));
        m1.send(sm.getHSId(), new LocalObjectMessage(null));

        LocalObjectMessage lom = (LocalObjectMessage)m1.recv();
        assertEquals(lom.m_sourceHSId, sm.getHSId());

        lom =  (LocalObjectMessage)sm.recv();
        assertEquals(lom.m_sourceHSId, m1.getHSId());
    }

    @Test
    public void testSingleProbedCreateHost() throws Exception {
        MeshProber jc = MeshProber.builder()
                .coordinators(coordinators(1))
                .startAction(StartAction.PROBE)
                .nodeState(NodeState.INITIALIZING)
                .bare(true)
                .build();

        HostMessenger hm = createHostMessenger(0, jc, true);

        Determination dtm =  prober(hm).waitForDetermination();
        assertEquals(StartAction.CREATE, dtm.startAction);
        assertEquals(1, dtm.hostCount);

        Mailbox m1 = hm.createMailbox();

        SiteMailbox sm = new SiteMailbox(hm, (-2L << 32));

        hm.createMailbox(sm.getHSId(), sm);

        sm.send(m1.getHSId(), new LocalObjectMessage(null));
        m1.send(sm.getHSId(), new LocalObjectMessage(null));

        LocalObjectMessage lom = (LocalObjectMessage)m1.recv();
        assertEquals(lom.m_sourceHSId, sm.getHSId());

        lom =  (LocalObjectMessage)sm.recv();
        assertEquals(lom.m_sourceHSId, m1.getHSId());
    }

    @Test
    public void testSingleProbedRecoverHost() throws Exception {
        MeshProber jc = MeshProber.builder()
                .coordinators(coordinators(1))
                .startAction(StartAction.PROBE)
                .nodeState(NodeState.INITIALIZING)
                .bare(false)
                .build();

        HostMessenger hm = createHostMessenger(0, jc, true);

        Determination dtm =  prober(hm).waitForDetermination();
        assertEquals(StartAction.RECOVER, dtm.startAction);
        assertEquals(1, dtm.hostCount);

        Mailbox m1 = hm.createMailbox();

        SiteMailbox sm = new SiteMailbox(hm, (-2L << 32));

        hm.createMailbox(sm.getHSId(), sm);

        sm.send(m1.getHSId(), new LocalObjectMessage(null));
        m1.send(sm.getHSId(), new LocalObjectMessage(null));

        LocalObjectMessage lom = (LocalObjectMessage)m1.recv();
        assertEquals(lom.m_sourceHSId, sm.getHSId());

        lom =  (LocalObjectMessage)sm.recv();
        assertEquals(lom.m_sourceHSId, m1.getHSId());
    }

    @Test
    public void testMultiHostProbedCreate() throws Exception {
        MeshProber.Builder jc = MeshProber.builder()
                .coordinators(coordinators(3))
                .startAction(StartAction.PROBE)
                .nodeState(NodeState.INITIALIZING)
                .bare(true);

        HostMessenger hm1 = createHostMessenger(0, jc.build(), true);
        HostMessenger hm2 = createHostMessenger(1, jc.build(), false);
        HostMessenger hm3 = createHostMessenger(2, jc.build(), false);

        final AtomicReference<Exception> exception = new AtomicReference<Exception>();
        HostMessengerThread hm2Start = new HostMessengerThread(hm2, exception);
        HostMessengerThread hm3Start = new HostMessengerThread(hm3, exception);

        hm2Start.start();
        hm3Start.start();

        hm2Start.join();
        hm3Start.join();

        if (exception.get() != null) {
            fail(exception.get().toString());
        }

        Determination dtm = prober(hm1).waitForDetermination();
        assertEquals(StartAction.CREATE, dtm.startAction);
        assertEquals(3, dtm.hostCount);

        assertEquals(dtm, prober(hm2).waitForDetermination());
        assertEquals(dtm, prober(hm3).waitForDetermination());

        List<String> root1 = hm1.getZK().getChildren("/", false );
        List<String> root2 = hm2.getZK().getChildren("/", false );
        List<String> root3 = hm3.getZK().getChildren("/", false );

        assertTrue(root1.equals(root2));
        assertTrue(root2.equals(root3));

        List<String> hostids1 = hm1.getZK().getChildren(CoreZK.hostids, false );
        List<String> hostids2 = hm2.getZK().getChildren(CoreZK.hostids, false );
        List<String> hostids3 = hm3.getZK().getChildren(CoreZK.hostids, false );

        assertTrue(hostids1.equals(hostids2));
        assertTrue(hostids2.equals(hostids3));
    }

    @Test
    public void testShouldCreateAdditionalConnectionToPartitionGroupPeer() throws Exception {
        MeshProber.Builder jc = MeshProber.builder()
                .coordinators(coordinators(2))
                .startAction(StartAction.PROBE)
                .nodeState(NodeState.INITIALIZING)
                .bare(true);

        HostMessenger hm1 = createHostMessenger(0, jc.build(), true);
        HostMessenger hm2 = createHostMessenger(1, jc.build(), false);

        final AtomicReference<Exception> exception = new AtomicReference<Exception>();
        HostMessengerThread hm2Start = new HostMessengerThread(hm2, exception);

        hm2Start.start();
        hm2Start.join();

        if (exception.get() != null) {
            fail(exception.get().toString());
        }

        Determination dtm = prober(hm1).waitForDetermination();
        assertEquals(StartAction.CREATE, dtm.startAction);
        assertEquals(2, dtm.hostCount);

        assertEquals(dtm, prober(hm2).waitForDetermination());

        assertFalse(hm1.m_foreignHosts.get(hm2.getHostId()).getHasMultiConnections());

        hm1.setPartitionGroupPeers(Sets.set(hm2.getHostId()), 5);
        hm1.createAuxiliaryConnections(false);

        assertTrue(hm1.m_foreignHosts.get(hm2.getHostId()).getHasMultiConnections());
    }

    @Test
    public void testMultiHostProbedRecover() throws Exception {
        MeshProber.Builder jc = MeshProber.builder()
                .coordinators(coordinators(3))
                .startAction(StartAction.PROBE)
                .nodeState(NodeState.INITIALIZING)
                .bare(false);

        HostMessenger hm1 = createHostMessenger(0, jc.build(), true);
        HostMessenger hm2 = createHostMessenger(1, jc.build(), false);
        HostMessenger hm3 = createHostMessenger(2, jc.build(), false);

        final AtomicReference<Exception> exception = new AtomicReference<Exception>();
        HostMessengerThread hm2Start = new HostMessengerThread(hm2, exception);
        HostMessengerThread hm3Start = new HostMessengerThread(hm3, exception);

        hm2Start.start();
        hm3Start.start();

        hm2Start.join();
        hm3Start.join();

        if (exception.get() != null) {
            fail(exception.get().toString());
        }

        Determination dtm = prober(hm1).waitForDetermination();
        assertEquals(StartAction.RECOVER, dtm.startAction);
        assertEquals(3, dtm.hostCount);

        assertEquals(dtm, prober(hm2).waitForDetermination());
        assertEquals(dtm, prober(hm3).waitForDetermination());

        List<String> root1 = hm1.getZK().getChildren("/", false );
        List<String> root2 = hm2.getZK().getChildren("/", false );
        List<String> root3 = hm3.getZK().getChildren("/", false );

        assertTrue(root1.equals(root2));
        assertTrue(root2.equals(root3));

        List<String> hostids1 = hm1.getZK().getChildren(CoreZK.hostids, false );
        List<String> hostids2 = hm2.getZK().getChildren(CoreZK.hostids, false );
        List<String> hostids3 = hm3.getZK().getChildren(CoreZK.hostids, false );

        assertTrue(hostids1.equals(hostids2));
        assertTrue(hostids2.equals(hostids3));
    }

    @Test
    public void testMultiHostProbedRecoverWithOneBare() throws Exception {
        MeshProber.Builder jcb = MeshProber.builder()
                .coordinators(coordinators(3))
                .startAction(StartAction.PROBE)
                .nodeState(NodeState.INITIALIZING)
                .kfactor(1);

        MeshProber bare  = jcb.bare(true).build();
        MeshProber dressed = jcb.bare(false).build();

        HostMessenger hm1 = createHostMessenger(0, bare, true);
        HostMessenger hm2 = createHostMessenger(1, jcb.prober(dressed).build(), false);
        HostMessenger hm3 = createHostMessenger(2, jcb.prober(dressed).build(), false);

        final AtomicReference<Exception> exception = new AtomicReference<Exception>();
        HostMessengerThread hm2Start = new HostMessengerThread(hm2, exception);
        HostMessengerThread hm3Start = new HostMessengerThread(hm3, exception);

        hm2Start.start();
        hm3Start.start();

        hm2Start.join();
        hm3Start.join();

        if (exception.get() != null) {
            fail(exception.get().toString());
        }

        Determination dtm = prober(hm1).waitForDetermination();
        assertEquals(StartAction.RECOVER, dtm.startAction);
        assertEquals(3, dtm.hostCount);

        assertEquals(dtm, prober(hm2).waitForDetermination());
        assertEquals(dtm, prober(hm3).waitForDetermination());

        List<String> root1 = hm1.getZK().getChildren("/", false );
        List<String> root2 = hm2.getZK().getChildren("/", false );
        List<String> root3 = hm3.getZK().getChildren("/", false );

        assertTrue(root1.equals(root2));
        assertTrue(root2.equals(root3));

        List<String> hostids1 = hm1.getZK().getChildren(CoreZK.hostids, false );
        List<String> hostids2 = hm2.getZK().getChildren(CoreZK.hostids, false );
        List<String> hostids3 = hm3.getZK().getChildren(CoreZK.hostids, false );

        assertTrue(hostids1.equals(hostids2));
        assertTrue(hostids2.equals(hostids3));
    }

    @Test
    public void testPauseModePropagation() throws Exception {
        MeshProber.Builder jcb = MeshProber.builder()
                .coordinators(coordinators(3))
                .startAction(StartAction.PROBE)
                .nodeState(NodeState.INITIALIZING)
                .bare(true)
                .kfactor(1);

        MeshProber paused  = jcb.paused(true).build();
        MeshProber unpaused = jcb.paused(false).build();

        HostMessenger hm1 = createHostMessenger(0, unpaused, true);
        HostMessenger hm2 = createHostMessenger(1, paused, false);
        HostMessenger hm3 = createHostMessenger(2, unpaused, false);

        final AtomicReference<Exception> exception = new AtomicReference<Exception>();
        HostMessengerThread hm2Start = new HostMessengerThread(hm2, exception);
        HostMessengerThread hm3Start = new HostMessengerThread(hm3, exception);

        hm2Start.start();
        hm3Start.start();

        hm2Start.join();
        hm3Start.join();

        if (exception.get() != null) {
            fail(exception.get().toString());
        }

        Determination dtm = prober(hm1).waitForDetermination();
        System.out.println("1 " + dtm);
        assertEquals(StartAction.CREATE, dtm.startAction);
        assertEquals(3, dtm.hostCount);
        Determination dtm2 = prober(hm2).waitForDetermination();
        System.out.println("2 " + dtm2);
        assertEquals(dtm, dtm2);
        Determination dtm3 = prober(hm3).waitForDetermination();
        System.out.println("2 " + dtm3);
        assertEquals(dtm, dtm3);

        assertTrue(dtm.paused);

        List<String> root1 = hm1.getZK().getChildren("/", false );
        List<String> root2 = hm2.getZK().getChildren("/", false );
        List<String> root3 = hm3.getZK().getChildren("/", false );

        assertTrue(root1.equals(root2));
        assertTrue(root2.equals(root3));

        List<String> hostids1 = hm1.getZK().getChildren(CoreZK.hostids, false );
        List<String> hostids2 = hm2.getZK().getChildren(CoreZK.hostids, false );
        List<String> hostids3 = hm3.getZK().getChildren(CoreZK.hostids, false );

        assertTrue(hostids1.equals(hostids2));
        assertTrue(hostids2.equals(hostids3));
    }

    @Test
    public void testNotEnoughTerminusPropagation() throws Exception {
        MeshProber.Builder jcb = MeshProber.builder()
                .coordinators(coordinators(3))
                .startAction(StartAction.PROBE)
                .nodeState(NodeState.INITIALIZING)
                .bare(true)
                .kfactor(1);

        MeshProber without = jcb.build();
        MeshProber with  = jcb.terminusNonce("nonce-uno").build();

        HostMessenger hm1 = createHostMessenger(0, jcb.prober(without).build(), true);
        HostMessenger hm2 = createHostMessenger(1, with, false);
        HostMessenger hm3 = createHostMessenger(2, jcb.prober(without).build(), false);

        final AtomicReference<Exception> exception = new AtomicReference<Exception>();
        HostMessengerThread hm2Start = new HostMessengerThread(hm2, exception);
        HostMessengerThread hm3Start = new HostMessengerThread(hm3, exception);

        hm2Start.start();
        hm3Start.start();

        hm2Start.join();
        hm3Start.join();

        if (exception.get() != null) {
            fail(exception.get().toString());
        }

        Determination dtm = prober(hm1).waitForDetermination();
        assertNull(dtm.terminusNonce);
        assertEquals(3, dtm.hostCount);

        assertEquals(dtm, prober(hm2).waitForDetermination());
        assertEquals(dtm, prober(hm3).waitForDetermination());

        List<String> root1 = hm1.getZK().getChildren("/", false );
        List<String> root2 = hm2.getZK().getChildren("/", false );
        List<String> root3 = hm3.getZK().getChildren("/", false );

        assertTrue(root1.equals(root2));
        assertTrue(root2.equals(root3));

        List<String> hostids1 = hm1.getZK().getChildren(CoreZK.hostids, false );
        List<String> hostids2 = hm2.getZK().getChildren(CoreZK.hostids, false );
        List<String> hostids3 = hm3.getZK().getChildren(CoreZK.hostids, false );

        assertTrue(hostids1.equals(hostids2));
        assertTrue(hostids2.equals(hostids3));
    }

    @Test
    public void testTerminusPropagation() throws Exception {
        MeshProber.Builder jcb = MeshProber.builder()
                .coordinators(coordinators(3))
                .startAction(StartAction.PROBE)
                .nodeState(NodeState.INITIALIZING)
                .bare(true)
                .kfactor(1);

        MeshProber without = jcb.build();
        MeshProber with  = jcb.terminusNonce("nonce-uno").build();

        HostMessenger hm1 = createHostMessenger(0, with, true);
        HostMessenger hm2 = createHostMessenger(1, with, false);
        HostMessenger hm3 = createHostMessenger(2, jcb.prober(without).build(), false);

        final AtomicReference<Exception> exception = new AtomicReference<Exception>();
        HostMessengerThread hm2Start = new HostMessengerThread(hm2, exception);
        HostMessengerThread hm3Start = new HostMessengerThread(hm3, exception);

        hm2Start.start();
        hm3Start.start();

        hm2Start.join();
        hm3Start.join();

        if (exception.get() != null) {
            fail(exception.get().toString());
        }

        Determination dtm = prober(hm1).waitForDetermination();
        assertEquals("nonce-uno", dtm.terminusNonce);
        assertEquals(3, dtm.hostCount);

        assertEquals(dtm, prober(hm2).waitForDetermination());
        assertEquals(dtm, prober(hm3).waitForDetermination());

        List<String> root1 = hm1.getZK().getChildren("/", false );
        List<String> root2 = hm2.getZK().getChildren("/", false );
        List<String> root3 = hm3.getZK().getChildren("/", false );

        assertTrue(root1.equals(root2));
        assertTrue(root2.equals(root3));

        List<String> hostids1 = hm1.getZK().getChildren(CoreZK.hostids, false );
        List<String> hostids2 = hm2.getZK().getChildren(CoreZK.hostids, false );
        List<String> hostids3 = hm3.getZK().getChildren(CoreZK.hostids, false );

        assertTrue(hostids1.equals(hostids2));
        assertTrue(hostids2.equals(hostids3));
    }

    @Test
    public void testSafeModePropagation() throws Exception {
        MeshProber.Builder jcb = MeshProber.builder()
                .coordinators(coordinators(3))
                .startAction(StartAction.PROBE)
                .nodeState(NodeState.INITIALIZING)
                .bare(false)
                .kfactor(1);

        MeshProber safe  = jcb.safeMode(true).build();
        MeshProber unsafe = jcb.safeMode(false).build();

        HostMessenger hm1 = createHostMessenger(0, jcb.prober(unsafe).build(), true);
        HostMessenger hm2 = createHostMessenger(1, safe, false);
        HostMessenger hm3 = createHostMessenger(2, jcb.prober(unsafe).build(), false);

        final AtomicReference<Exception> exception = new AtomicReference<Exception>();
        HostMessengerThread hm2Start = new HostMessengerThread(hm2, exception);
        HostMessengerThread hm3Start = new HostMessengerThread(hm3, exception);

        hm2Start.start();
        hm3Start.start();

        hm2Start.join();
        hm3Start.join();

        if (exception.get() != null) {
            fail(exception.get().toString());
        }

        Determination dtm = prober(hm1).waitForDetermination();
        assertEquals(StartAction.SAFE_RECOVER, dtm.startAction);
        assertEquals(3, dtm.hostCount);

        assertEquals(dtm, prober(hm2).waitForDetermination());
        assertEquals(dtm, prober(hm3).waitForDetermination());

        List<String> root1 = hm1.getZK().getChildren("/", false );
        List<String> root2 = hm2.getZK().getChildren("/", false );
        List<String> root3 = hm3.getZK().getChildren("/", false );

        assertTrue(root1.equals(root2));
        assertTrue(root2.equals(root3));

        List<String> hostids1 = hm1.getZK().getChildren(CoreZK.hostids, false );
        List<String> hostids2 = hm2.getZK().getChildren(CoreZK.hostids, false );
        List<String> hostids3 = hm3.getZK().getChildren(CoreZK.hostids, false );

        assertTrue(hostids1.equals(hostids2));
        assertTrue(hostids2.equals(hostids3));
    }

    @Test
    public void testStaggeredStartsWithFewerCoordinators() throws Exception {
        MeshProber.Builder jcb = MeshProber.builder()
                .coordinators(coordinators(2))
                .hostCount(3)
                .startAction(StartAction.PROBE)
                .nodeState(NodeState.INITIALIZING)
                .kfactor(1);


        HostMessenger hm1 = createHostMessenger(0, jcb.bare(true).build(), false);
        HostMessenger hm2 = createHostMessenger(1, jcb.bare(true).build(), false);
        HostMessenger hm3 = createHostMessenger(2, jcb.bare(true).build(), false);

        final AtomicReference<Exception> exception = new AtomicReference<Exception>();
        HostMessengerThread hm2Start = new HostMessengerThread(hm2, exception);
        HostMessengerThread hm3Start = new HostMessengerThread(hm3, exception);

        hm2Start.start();
        Thread.sleep(50);
        hm3Start.start();
        Thread.sleep(150);
        hm1.start();

        hm2Start.join();
        hm3Start.join();

        if (exception.get() != null) {
            fail(exception.get().toString());
        }

        Determination dtm = prober(hm1).waitForDetermination();
        assertEquals(StartAction.CREATE, dtm.startAction);
        assertEquals(3, dtm.hostCount);

        assertEquals(dtm, prober(hm2).waitForDetermination());
        assertEquals(dtm, prober(hm3).waitForDetermination());

        List<String> root1 = hm1.getZK().getChildren("/", false );
        List<String> root2 = hm2.getZK().getChildren("/", false );
        List<String> root3 = hm3.getZK().getChildren("/", false );

        assertTrue(root1.equals(root2));
        assertTrue(root2.equals(root3));

        List<String> hostids1 = hm1.getZK().getChildren(CoreZK.hostids, false );
        List<String> hostids2 = hm2.getZK().getChildren(CoreZK.hostids, false );
        List<String> hostids3 = hm3.getZK().getChildren(CoreZK.hostids, false );

        assertTrue(hostids1.equals(hostids2));
        assertTrue(hostids2.equals(hostids3));
    }

    @Test
    public void testProbedRejoinForPreviousLeader() throws Exception {
        NodeStateRef upNodesState = new NodeStateRef(NodeState.INITIALIZING);
        MeshProber.Builder jcb = MeshProber.builder()
                .coordinators(coordinators(2))
                .hostCount(3)
                .startAction(StartAction.PROBE)
                .nodeStateSupplier(upNodesState)
                .kfactor(1)
                .paused(false)
                .bare(true);

        HostMessenger hm1 = createHostMessenger(0, jcb.build(), true);
        HostMessenger hm2 = createHostMessenger(1, jcb.build(), false);
        HostMessenger hm3 = createHostMessenger(2, jcb.build(), false);

        final AtomicReference<Exception> exception = new AtomicReference<Exception>();
        HostMessengerThread hm2Start = new HostMessengerThread(hm2, exception);
        HostMessengerThread hm3Start = new HostMessengerThread(hm3, exception);

        hm2Start.start();
        hm3Start.start();

        hm2Start.join();
        hm3Start.join();

        if (exception.get() != null) {
            fail(exception.get().toString());
        }

        Determination dtm = prober(hm1).waitForDetermination();
        assertEquals(StartAction.CREATE, dtm.startAction);
        assertEquals(3, dtm.hostCount);

        assertEquals(dtm, prober(hm2).waitForDetermination());
        assertEquals(dtm, prober(hm3).waitForDetermination());

        assertTrue(upNodesState.compareAndSet(NodeState.INITIALIZING, NodeState.UP));
        hm1.shutdown();

        // rejoining node cannot propagate its safe mode or paused demand
        jcb.nodeState(NodeState.INITIALIZING)
                .bare(false)
                .paused(true)
                .safeMode(true);
        hm1 = createHostMessenger(0, jcb.build(), true);

        dtm = prober(hm1).waitForDetermination();
        assertEquals(StartAction.LIVE_REJOIN, dtm.startAction);
        assertEquals(3, dtm.hostCount);
        //Dont depend on HM paused status it gets set by RealVoltDB in this test rely on Determination.
        assertFalse(dtm.paused);

        List<String> root1 = hm1.getZK().getChildren("/", false );
        List<String> root2 = hm2.getZK().getChildren("/", false );
        List<String> root3 = hm3.getZK().getChildren("/", false );

        assertTrue(root1.equals(root2));
        assertTrue(root2.equals(root3));

        List<String> hostids1 = hm1.getZK().getChildren(CoreZK.hostids, false );
        List<String> hostids2 = hm2.getZK().getChildren(CoreZK.hostids, false );
        List<String> hostids3 = hm3.getZK().getChildren(CoreZK.hostids, false );

        assertTrue(hostids1.equals(hostids2));
        assertTrue(hostids2.equals(hostids3));
    }

    @Test
    public void testProbedRejoinForNonLeader() throws Exception {
        NodeStateRef upNodesState = new NodeStateRef(NodeState.INITIALIZING);
        MeshProber.Builder jcb = MeshProber.builder()
                .coordinators(coordinators(2))
                .hostCount(3)
                .startAction(StartAction.PROBE)
                .nodeStateSupplier(upNodesState)
                .kfactor(1)
                .paused(false)
                .bare(true);

        HostMessenger hm1 = createHostMessenger(0, jcb.build(), false);
        HostMessenger hm2 = createHostMessenger(1, jcb.build(), false);
        HostMessenger hm3 = createHostMessenger(2, jcb.build(), false);

        final AtomicReference<Exception> exception = new AtomicReference<Exception>();
        HostMessengerThread hm1Start = new HostMessengerThread(hm1, exception);
        HostMessengerThread hm3Start = new HostMessengerThread(hm3, exception);

        hm1Start.start();
        hm3Start.start();
        hm2.start();

        hm1Start.join();
        hm3Start.join();

        if (exception.get() != null) {
            fail(exception.get().toString());
        }

        Determination dtm = prober(hm1).waitForDetermination();
        assertEquals(StartAction.CREATE, dtm.startAction);
        assertEquals(3, dtm.hostCount);

        assertEquals(dtm, prober(hm2).waitForDetermination());
        assertEquals(dtm, prober(hm3).waitForDetermination());

        assertTrue(upNodesState.compareAndSet(NodeState.INITIALIZING, NodeState.UP));
        hm2.shutdown();

        jcb.nodeState(NodeState.INITIALIZING)
                .bare(false)
                .paused(true);
        hm2 = createHostMessenger(1, jcb.build(), true);

        dtm = prober(hm2).waitForDetermination();
        assertEquals(StartAction.LIVE_REJOIN, dtm.startAction);
        assertEquals(3, dtm.hostCount);
        //Dont depend on HM paused status it gets set by RealVoltDB in this test rely on Determination.
        assertFalse(dtm.paused);

        List<String> root1 = hm1.getZK().getChildren("/", false );
        List<String> root2 = hm2.getZK().getChildren("/", false );
        List<String> root3 = hm3.getZK().getChildren("/", false );

        assertTrue(root1.equals(root2));
        assertTrue(root2.equals(root3));

        List<String> hostids1 = hm1.getZK().getChildren(CoreZK.hostids, false );
        List<String> hostids2 = hm2.getZK().getChildren(CoreZK.hostids, false );
        List<String> hostids3 = hm3.getZK().getChildren(CoreZK.hostids, false );

        assertTrue(hostids1.equals(hostids2));
        assertTrue(hostids2.equals(hostids3));
    }

    @Test
    public void testProbedRejoinOnWholeCluster() throws Exception {
        VoltDB.ignoreCrash = true;

        NodeStateRef upNodesState = new NodeStateRef(NodeState.INITIALIZING);
        MeshProber.Builder jcb = MeshProber.builder()
                .coordinators(coordinators(2))
                .hostCount(3)
                .startAction(StartAction.PROBE)
                .nodeStateSupplier(upNodesState)
                .kfactor(1)
                .paused(false)
                .bare(true);

        HostMessenger hm1 = createHostMessenger(0, jcb.build(), false);
        HostMessenger hm2 = createHostMessenger(1, jcb.build(), false);
        HostMessenger hm3 = createHostMessenger(2, jcb.build(), false);

        final AtomicReference<Exception> exception = new AtomicReference<Exception>();
        HostMessengerThread hm1Start = new HostMessengerThread(hm1, exception);
        HostMessengerThread hm2Start = new HostMessengerThread(hm2, exception);
        HostMessengerThread hm3Start = new HostMessengerThread(hm3, exception);

        hm1Start.start();
        hm2Start.start();
        hm3Start.start();

        hm1Start.join();
        hm2Start.join();
        hm3Start.join();

        if (exception.get() != null) {
            fail(exception.get().toString());
        }

        Determination dtm = prober(hm1).waitForDetermination();
        assertEquals(StartAction.CREATE, dtm.startAction);
        assertEquals(3, dtm.hostCount);

        assertEquals(dtm, prober(hm2).waitForDetermination());
        assertEquals(dtm, prober(hm3).waitForDetermination());

        assertTrue(upNodesState.compareAndSet(NodeState.INITIALIZING, NodeState.UP));

        jcb.nodeState(NodeState.INITIALIZING)
            .bare(true);
        HostMessenger hm4 = createHostMessenger(2, jcb.build(), false);

        try {
            hm4.start();
            fail("did not crash on whole cluster rejoin attempt");
        } catch (AssertionError pass) {
            assertTrue(VoltDB.wasCrashCalled);
        }
    }

    @Test
    public void testProbedRejoinWithMismatchedStartAction() throws Exception {
        VoltDB.ignoreCrash = true;

        NodeStateRef upNodesState = new NodeStateRef(NodeState.INITIALIZING);
        MeshProber.Builder jcb = MeshProber.builder()
                .coordinators(coordinators(2))
                .hostCount(3)
                .startAction(StartAction.PROBE)
                .nodeStateSupplier(upNodesState)
                .kfactor(1)
                .paused(false)
                .bare(true);

        HostMessenger hm1 = createHostMessenger(0, jcb.build(), false);
        HostMessenger hm2 = createHostMessenger(1, jcb.build(), false);
        HostMessenger hm3 = createHostMessenger(2, jcb.build(), false);

        final AtomicReference<Exception> exception = new AtomicReference<Exception>();
        HostMessengerThread hm1Start = new HostMessengerThread(hm1, exception);
        HostMessengerThread hm2Start = new HostMessengerThread(hm2, exception);
        HostMessengerThread hm3Start = new HostMessengerThread(hm3, exception);

        hm1Start.start();
        hm2Start.start();
        hm3Start.start();

        hm1Start.join();
        hm2Start.join();
        hm3Start.join();

        if (exception.get() != null) {
            fail(exception.get().toString());
        }

        Determination dtm = prober(hm1).waitForDetermination();
        assertEquals(StartAction.CREATE, dtm.startAction);
        assertEquals(3, dtm.hostCount);

        assertEquals(dtm, prober(hm2).waitForDetermination());
        assertEquals(dtm, prober(hm3).waitForDetermination());

        assertTrue(upNodesState.compareAndSet(NodeState.INITIALIZING, NodeState.UP));

        hm2.shutdown();

        jcb.nodeState(NodeState.INITIALIZING)
            .startAction(StartAction.REJOIN)
            .bare(true);
        HostMessenger hm4 = createHostMessenger(1, jcb.build(), false);

        try {
            hm4.start();
            fail("did not crash on mismatched start actions");
        } catch (AssertionError pass) {
            assertTrue(VoltDB.wasCrashCalled);
            assertTrue(VoltDB.crashMessage.contains("use init and start to join this cluster"));
        }
    }

    @Test
    public void testProbedJoinOnWholeCluster() throws Exception {
        NodeStateRef upNodesState = new NodeStateRef(NodeState.INITIALIZING);

        MeshProber.Builder jcb = MeshProber.builder()
                .coordinators(coordinators(2))
                .hostCount(3)
                .startAction(StartAction.PROBE)
                .nodeStateSupplier(upNodesState)
                .kfactor(1)
                .paused(false)
                .bare(true);

        HostMessenger hm1 = createHostMessenger(0, jcb.build(), false);
        HostMessenger hm2 = createHostMessenger(1, jcb.build(), false);
        HostMessenger hm3 = createHostMessenger(2, jcb.build(), false);

        final AtomicReference<Exception> exception = new AtomicReference<Exception>();
        HostMessengerThread hm1Start = new HostMessengerThread(hm1, exception);
        HostMessengerThread hm2Start = new HostMessengerThread(hm2, exception);
        HostMessengerThread hm3Start = new HostMessengerThread(hm3, exception);

        hm1Start.start();
        hm2Start.start();
        hm3Start.start();

        hm1Start.join();
        hm2Start.join();
        hm3Start.join();

        if (exception.get() != null) {
            fail(exception.get().toString());
        }

        Determination dtm = prober(hm1).waitForDetermination();
        assertEquals(StartAction.CREATE, dtm.startAction);
        assertEquals(3, dtm.hostCount);

        assertEquals(dtm, prober(hm2).waitForDetermination());
        assertEquals(dtm, prober(hm3).waitForDetermination());

        assertTrue(upNodesState.compareAndSet(NodeState.INITIALIZING, NodeState.UP));

        jcb.nodeState(NodeState.INITIALIZING)
                .bare(true)
                .addAllowed(true)
                .hostCount(5);
        HostMessenger hm4 = createHostMessenger(3, jcb.build(), false);

        hm4.start();
        dtm = prober(hm4).waitForDetermination();
        assertEquals(StartAction.JOIN, dtm.startAction);
        assertEquals(5, dtm.hostCount);

        List<String> root1 = hm1.getZK().getChildren("/", false );
        List<String> root2 = hm2.getZK().getChildren("/", false );
        List<String> root3 = hm3.getZK().getChildren("/", false );
        List<String> root4 = hm4.getZK().getChildren("/", false );

        assertTrue(root1.equals(root2));
        assertTrue(root2.equals(root3));
        assertTrue(root3.equals(root4));

        List<String> hostids1 = hm1.getZK().getChildren(CoreZK.hostids, false );
        List<String> hostids2 = hm2.getZK().getChildren(CoreZK.hostids, false );
        List<String> hostids3 = hm3.getZK().getChildren(CoreZK.hostids, false );
        List<String> hostids4 = hm4.getZK().getChildren(CoreZK.hostids, false );

        assertTrue(hostids1.equals(hostids2));
        assertTrue(hostids2.equals(hostids3));
        assertTrue(hostids3.equals(hostids4));
    }

    @Test
    public void testTwoProbedConcuncurrentRejoins() throws Exception {

        NodeStateRef hmns1 = new NodeStateRef(NodeState.INITIALIZING);
        MeshProber.Builder jcb = MeshProber.builder()
                .coordinators(coordinators(2))
                .hostCount(5)
                .startAction(StartAction.PROBE)
                .nodeStateSupplier(hmns1)
                .kfactor(2)
                .paused(false)
                .bare(true);

        HostMessenger hm1 = createHostMessenger(0, jcb.build(), false);
        HostMessenger hm2 = createHostMessenger(1, jcb.build(), false);
        HostMessenger hm3 = createHostMessenger(2, jcb.build(), false);
        HostMessenger hm4 = createHostMessenger(3, jcb.build(), false);
        HostMessenger hm5 = createHostMessenger(4, jcb.build(), false);

        final AtomicReference<Exception> exception = new AtomicReference<Exception>();
        HostMessengerThread hm1Start = new HostMessengerThread(hm1, exception);
        HostMessengerThread hm2Start = new HostMessengerThread(hm2, exception);
        HostMessengerThread hm3Start = new HostMessengerThread(hm3, exception);
        HostMessengerThread hm4Start = new HostMessengerThread(hm4, exception);
        HostMessengerThread hm5Start = new HostMessengerThread(hm5, exception);

        hm1Start.start();
        hm2Start.start();
        hm3Start.start();
        hm4Start.start();
        hm5Start.start();

        hm1Start.join();
        hm2Start.join();
        hm3Start.join();
        hm4Start.join();
        hm5Start.join();

        if (exception.get() != null) {
            fail(exception.get().toString());
        }

        Determination dtm = prober(hm1).waitForDetermination();
        assertEquals(StartAction.CREATE, dtm.startAction);
        assertEquals(5, dtm.hostCount);

        assertEquals(dtm, prober(hm2).waitForDetermination());
        assertEquals(dtm, prober(hm3).waitForDetermination());
        assertEquals(dtm, prober(hm4).waitForDetermination());
        assertEquals(dtm, prober(hm5).waitForDetermination());

        assertTrue(hmns1.compareAndSet(NodeState.INITIALIZING, NodeState.UP));

        hm1.shutdown();
        hm3.shutdown();

        jcb.nodeState(NodeState.INITIALIZING)
                .bare(true);
        HostMessenger hm6 = createHostMessenger(0, jcb.build(), false);
        HostMessenger hm7 = createHostMessenger(2, jcb.build(), false);

        HostMessengerThread hm6Start = new HostMessengerThread(hm6, exception);
        HostMessengerThread hm7Start = new HostMessengerThread(hm7, exception);

        hm6Start.start();
        hm7Start.start();

        Thread.sleep(2_000);
        CoreZK.removeRejoinNodeIndicatorForHost(hm2.getZK(), 5);

        hm6Start.join();
        hm7Start.join();

        if (exception.get() != null) {
            fail(exception.get().toString());
        }

        dtm = prober(hm6).waitForDetermination();
        assertEquals(StartAction.LIVE_REJOIN, dtm.startAction);
        assertEquals(5, dtm.hostCount);
        assertEquals(dtm, prober(hm6).waitForDetermination());

        assertTrue(Math.max(hm6.getHostId(), hm7.getHostId()) > 6);

        List<String> root1 = hm6.getZK().getChildren("/", false );
        List<String> root2 = hm2.getZK().getChildren("/", false );
        List<String> root3 = hm7.getZK().getChildren("/", false );
        List<String> root4 = hm4.getZK().getChildren("/", false );
        List<String> root5 = hm5.getZK().getChildren("/", false );

        assertTrue(root1.equals(root2));
        assertTrue(root2.equals(root3));
        assertTrue(root3.equals(root4));
        assertTrue(root4.equals(root5));

        List<String> hostids1 = hm6.getZK().getChildren(CoreZK.hostids, false );
        List<String> hostids2 = hm2.getZK().getChildren(CoreZK.hostids, false );
        List<String> hostids3 = hm7.getZK().getChildren(CoreZK.hostids, false );
        List<String> hostids4 = hm4.getZK().getChildren(CoreZK.hostids, false );
        List<String> hostids5 = hm5.getZK().getChildren(CoreZK.hostids, false );

        assertTrue(hostids1.equals(hostids2));
        assertTrue(hostids2.equals(hostids3));
        assertTrue(hostids3.equals(hostids4));
        assertTrue(hostids4.equals(hostids5));
    }

    @Test
    public void testProbedConfigMismatchCrash() throws Exception {
        VoltDB.ignoreCrash = true;

        MeshProber.Builder jcb = MeshProber.builder()
                .coordinators(coordinators(2))
                .hostCount(3)
                .startAction(StartAction.PROBE)
                .nodeState(NodeState.INITIALIZING)
                .kfactor(1)
                .paused(false)
                .bare(true);

        MeshProber jc1 = jcb.build();
        MeshProber jc2 = jcb.configHash(new UUID(-2L, -2L)).build();

        assertNotSame(jc1.getConfigHash(), jc2.getConfigHash());

        HostMessenger hm1 = createHostMessenger(0, jcb.prober(jc1).build(), false);
        HostMessenger hm2 = createHostMessenger(1, jcb.prober(jc1).build(), false);
        HostMessenger hm3 = createHostMessenger(2, jcb.prober(jc2).build(), false);

        final AtomicReference<Exception> exception = new AtomicReference<Exception>();
        HostMessengerThread hm1Start = new HostMessengerThread(hm1, exception);
        HostMessengerThread hm2Start = new HostMessengerThread(hm2, exception);

        hm1Start.start();
        hm2Start.start();

        hm1Start.join();
        hm2Start.join();

        if (exception.get() != null) {
            fail(exception.get().toString());
        }

        try {
            hm3.start();
            fail("did not crash on whole cluster rejoin attempt");
        } catch (AssertionError pass) {
            assertTrue(VoltDB.wasCrashCalled);
            assertTrue(VoltDB.crashMessage.contains("deployment options that do not match"));
        }
    }

    @Test
    public void testProbedMeshMismatchCrash() throws Exception {
        VoltDB.ignoreCrash = true;

        MeshProber.Builder jcb = MeshProber.builder()
                .coordinators(coordinators(2))
                .hostCount(3)
                .startAction(StartAction.PROBE)
                .nodeState(NodeState.INITIALIZING)
                .kfactor(1)
                .paused(false)
                .bare(true);

        MeshProber jc1 = jcb.build();
        MeshProber jc2 = jcb.coordinators(coordinators(3)).build();

        assertNotSame(jc1.getMeshHash(), jc2.getMeshHash());

        HostMessenger hm1 = createHostMessenger(0, jcb.prober(jc1).build(), false);
        HostMessenger hm2 = createHostMessenger(1, jcb.prober(jc1).build(), false);
        HostMessenger hm3 = createHostMessenger(2, jcb.prober(jc2).build(), false);

        final AtomicReference<Exception> exception = new AtomicReference<Exception>();
        HostMessengerThread hm1Start = new HostMessengerThread(hm1, exception);
        HostMessengerThread hm2Start = new HostMessengerThread(hm2, exception);

        hm1Start.start();
        hm2Start.start();

        hm1Start.join();
        hm2Start.join();

        if (exception.get() != null) {
            fail(exception.get().toString());
        }

        try {
            hm3.start();
            fail("did not crash on whole cluster rejoin attempt");
        } catch (AssertionError pass) {
            assertTrue(VoltDB.wasCrashCalled);
            assertTrue(VoltDB.crashMessage.contains("Mismatched list of hosts"));
        }
    }

    @Test
    public void testProbedTerminusNonceMismatchCrash() throws Exception {
        VoltDB.ignoreCrash = true;

        MeshProber.Builder jcb = MeshProber.builder()
                .coordinators(coordinators(2))
                .hostCount(3)
                .startAction(StartAction.PROBE)
                .nodeState(NodeState.INITIALIZING)
                .kfactor(1)
                .paused(false)
                .bare(true)
                .terminusNonce("uno");

        MeshProber jc1 = jcb.build();
        MeshProber jc2 = jcb.terminusNonce("due").build();

        assertNotSame(jc1.getTerminusNonce(), jc2.getTerminusNonce());

        HostMessenger hm1 = createHostMessenger(0, jcb.prober(jc1).build(), false);
        HostMessenger hm2 = createHostMessenger(1, jcb.prober(jc1).build(), false);
        HostMessenger hm3 = createHostMessenger(2, jcb.prober(jc2).build(), false);

        final AtomicReference<Exception> exception = new AtomicReference<Exception>();
        HostMessengerThread hm1Start = new HostMessengerThread(hm1, exception);
        HostMessengerThread hm2Start = new HostMessengerThread(hm2, exception);

        hm1Start.start();
        hm2Start.start();

        hm1Start.join();
        hm2Start.join();

        if (exception.get() != null) {
            fail(exception.get().toString());
        }

        try {
            hm3.start();
            fail("did not crash on whole cluster rejoin attempt");
        } catch (AssertionError pass) {
            assertTrue(VoltDB.wasCrashCalled);
            assertTrue(VoltDB.crashMessage.contains("have different startup snapshot nonces"));
        }
    }

    @Test
    public void testProbedHostCountMismatchCrash() throws Exception {
        VoltDB.ignoreCrash = true;

        MeshProber.Builder jcb = MeshProber.builder()
                .coordinators(coordinators(2))
                .hostCount(3)
                .startAction(StartAction.PROBE)
                .nodeState(NodeState.INITIALIZING)
                .kfactor(1)
                .paused(false)
                .bare(true);

        MeshProber jc1 = jcb.build();
        MeshProber jc2 = jcb.hostCount(4).build();

        assertNotSame(jc1.getHostCount(), jc2.getHostCount());

        HostMessenger hm1 = createHostMessenger(0, jcb.prober(jc1).build(), false);
        HostMessenger hm2 = createHostMessenger(1, jcb.prober(jc1).build(), false);
        HostMessenger hm3 = createHostMessenger(2, jcb.prober(jc2).build(), false);

        final AtomicReference<Exception> exception = new AtomicReference<Exception>();
        HostMessengerThread hm1Start = new HostMessengerThread(hm1, exception);
        HostMessengerThread hm2Start = new HostMessengerThread(hm2, exception);

        hm1Start.start();
        hm2Start.start();

        hm1Start.join();
        hm2Start.join();

        if (exception.get() != null) {
            fail(exception.get().toString());
        }

        try {
            hm3.start();
            fail("did not crash on whole cluster rejoin attempt");
        } catch (AssertionError pass) {
            assertTrue(VoltDB.wasCrashCalled);
            assertTrue(VoltDB.crashMessage.contains("Mismatched host count"));
        }
    }

    @Test
    public void testProbedJoinerAcceptorMismatchCrash() throws Exception {
        VoltDB.ignoreCrash = true;

        MeshProber.Builder jcb = MeshProber.builder()
                .coordinators(coordinators(2))
                .hostCount(3)
                .startAction(StartAction.PROBE)
                .nodeState(NodeState.INITIALIZING)
                .kfactor(1)
                .paused(false)
                .bare(true);

        MeshProber jc1 = jcb.build();
        JoinAcceptor jc2 = new JoinAcceptor() {
            @Override
            public void detract(Set<Integer> hostIds) {
            }

            @Override
            public void detract(ZooKeeper zooKeeper, int hostId) {
            }

            @Override
            public void accrue(Map<Integer, JSONObject> jos) {
            }

            @Override
            public void accrue(int hostId, JSONObject jo) {
            }
        };

        HostMessenger hm1 = createHostMessenger(0, jcb.prober(jc1).build(), false);
        HostMessenger hm2 = createHostMessenger(1, jcb.prober(jc1).build(), false);
        HostMessenger hm3 = createHostMessenger(2, jc2, false);

        final AtomicReference<Exception> exception = new AtomicReference<Exception>();
        HostMessengerThread hm1Start = new HostMessengerThread(hm1, exception);
        HostMessengerThread hm2Start = new HostMessengerThread(hm2, exception);

        hm1Start.start();
        hm2Start.start();

        hm1Start.join();
        hm2Start.join();

        if (exception.get() != null) {
            fail(exception.get().toString());
        }

        try {
            hm3.start();
            fail("did not crash on whole cluster rejoin attempt");
        } catch (AssertionError pass) {
            assertTrue(VoltDB.wasCrashCalled);
            assertTrue(VoltDB.crashMessage.contains("is incompatible with this node version"));
        }
    }

    @Test
    public void testProbedEditionMismatchCrash() throws Exception {
        VoltDB.ignoreCrash = true;

        MeshProber.Builder jcb = MeshProber.builder()
                .coordinators(coordinators(2))
                .hostCount(3)
                .startAction(StartAction.PROBE)
                .nodeState(NodeState.INITIALIZING)
                .kfactor(1)
                .paused(false)
                .enterprise(true)
                .bare(true);

        MeshProber jc1 = jcb.build();
        MeshProber jc2 = jcb.enterprise(false).build();

        assertNotSame(jc1.isEnterprise(), jc2.isEnterprise());

        HostMessenger hm1 = createHostMessenger(0, jcb.prober(jc1).build(), false);
        HostMessenger hm2 = createHostMessenger(1, jcb.prober(jc1).build(), false);
        HostMessenger hm3 = createHostMessenger(2, jcb.prober(jc2).build(), false);

        final AtomicReference<Exception> exception = new AtomicReference<Exception>();
        HostMessengerThread hm1Start = new HostMessengerThread(hm1, exception);
        HostMessengerThread hm2Start = new HostMessengerThread(hm2, exception);

        hm1Start.start();
        hm2Start.start();

        hm1Start.join();
        hm2Start.join();

        if (exception.get() != null) {
            fail(exception.get().toString());
        }

        try {
            hm3.start();
            fail("did not crash on whole cluster rejoin attempt");
        } catch (AssertionError pass) {
            assertTrue(VoltDB.wasCrashCalled);
            assertTrue(VoltDB.crashMessage.contains("cannot contain both enterprise and community editions"));
        }
    }

    @Test
    public void testStartActionMismatchCrash() throws Exception {
        VoltDB.ignoreCrash = true;

        MeshProber.Builder jcb = MeshProber.builder()
                .coordinators(coordinators(2))
                .hostCount(3)
                .startAction(StartAction.PROBE)
                .nodeState(NodeState.INITIALIZING)
                .kfactor(1)
                .paused(false)
                .bare(true);

        MeshProber jc1 = jcb.build();
        MeshProber jc2 = jcb.startAction(StartAction.CREATE).build();

        assertNotSame(jc1.getStartAction(), jc2.getStartAction());

        HostMessenger hm1 = createHostMessenger(0, jcb.prober(jc1).build(), false);
        HostMessenger hm2 = createHostMessenger(1, jcb.prober(jc1).build(), false);
        HostMessenger hm3 = createHostMessenger(2, jcb.prober(jc2).build(), false);

        final AtomicReference<Exception> exception = new AtomicReference<Exception>();
        HostMessengerThread hm1Start = new HostMessengerThread(hm1, exception);
        HostMessengerThread hm2Start = new HostMessengerThread(hm2, exception);

        hm1Start.start();
        hm2Start.start();

        hm1Start.join();
        hm2Start.join();

        if (exception.get() != null) {
            fail(exception.get().toString());
        }

        try {
            hm3.start();
            fail("did not crash on whole cluster rejoin attempt");
        } catch (AssertionError pass) {
            assertTrue(VoltDB.wasCrashCalled);
            assertTrue(VoltDB.crashMessage.contains("Start action CREATE does not match PROBE"));
        }
    }

    @Test
    public void testMultipleMismatchesCrash() throws Exception {
        VoltDB.ignoreCrash = true;

        MeshProber.Builder jcb = MeshProber.builder()
                .coordinators(coordinators(2))
                .hostCount(3)
                .startAction(StartAction.PROBE)
                .nodeState(NodeState.INITIALIZING)
                .kfactor(1)
                .paused(false)
                .bare(true);

        MeshProber jc1 = jcb.build();
        MeshProber jc2 = jcb
                .startAction(StartAction.CREATE)
                .configHash(new UUID(-2L, -2L))
                .hostCount(4)
                .build();

        assertNotSame(jc1.getStartAction(), jc2.getStartAction());
        assertNotSame(jc1.getHostCount(), jc2.getHostCount());
        assertNotSame(jc1.getConfigHash(), jc2.getConfigHash());

        HostMessenger hm1 = createHostMessenger(0, jcb.prober(jc1).build(), false);
        HostMessenger hm2 = createHostMessenger(1, jcb.prober(jc1).build(), false);
        HostMessenger hm3 = createHostMessenger(2, jcb.prober(jc2).build(), false);

        final AtomicReference<Exception> exception = new AtomicReference<Exception>();
        HostMessengerThread hm1Start = new HostMessengerThread(hm1, exception);
        HostMessengerThread hm2Start = new HostMessengerThread(hm2, exception);

        hm1Start.start();
        hm2Start.start();

        hm1Start.join();
        hm2Start.join();

        if (exception.get() != null) {
            fail(exception.get().toString());
        }

        try {
            hm3.start();
            fail("did not crash on whole cluster rejoin attempt");
        } catch (AssertionError pass) {
            assertTrue(VoltDB.wasCrashCalled);
            assertTrue(VoltDB.crashMessage.contains("Start action CREATE does not match PROBE"));
            assertTrue(VoltDB.crashMessage.contains("Mismatched host count"));
            assertTrue(VoltDB.crashMessage.contains("deployment options that do not match"));
        }
    }

    @Test
    public void testMultiHost() throws Exception {
        HostMessenger hm1 = createHostMessenger(0, 3);

        final HostMessenger hm2 = createHostMessenger(1, false, 3);

        final HostMessenger hm3 = createHostMessenger(2, false, 3);

        final AtomicReference<Exception> exception = new AtomicReference<Exception>();
        HostMessengerThread hm2Start = new HostMessengerThread(hm2, exception);
        HostMessengerThread hm3Start = new HostMessengerThread(hm3, exception);

        hm2Start.start();
        hm3Start.start();
        hm2Start.join();
        System.out.println(hm2.getZK().getChildren(CoreZK.hostids, false ));
        hm3Start.join();

        if (exception.get() != null) {
            fail(exception.get().toString());
        }

        List<String> root1 = hm1.getZK().getChildren("/", false );
        List<String> root2 = hm2.getZK().getChildren("/", false );
        List<String> root3 = hm3.getZK().getChildren("/", false );
        System.out.println(root1);
        System.out.println(root2);
        System.out.println(root3);
        assertTrue(root1.equals(root2));
        assertTrue(root2.equals(root3));

        List<String> hostids1 = hm1.getZK().getChildren(CoreZK.hostids, false );
        List<String> hostids2 = hm2.getZK().getChildren(CoreZK.hostids, false );
        List<String> hostids3 = hm3.getZK().getChildren(CoreZK.hostids, false );
        System.out.println(hostids1);
        System.out.println(hostids2);
        System.out.println(hostids3);
        assertTrue(hostids1.equals(hostids2));
        assertTrue(hostids2.equals(hostids3));

        List<String> hosts3;
        List<String> hosts1;
        hm2.shutdown();
        boolean success = false;
        for (int ii = 0; ii < (200 / 5); ii++) {
            hosts3 = hm3.getZK().getChildren(CoreZK.hosts, false );
            hosts1 = hm1.getZK().getChildren(CoreZK.hosts, false );
            if (hosts3.size() == 2 && hosts1.size() == 2 && hosts1.equals(hosts3)) {
                success = true;
                break;
            }
            Thread.sleep(5);
        }
        assertTrue(success);

        hm1.waitForGroupJoin(2);
        hm3.waitForGroupJoin(2);
    }

    @Test
    public void testPartitionDetectionMinoritySet() throws Exception
    {
        Set<Integer> previous = new HashSet<Integer>();
        Set<Integer> current = new HashSet<Integer>();

        // current cluster has 2 hosts
        current.add(0);
        current.add(1);
        // the pre-fail cluster had 5 hosts.
        previous.addAll(current);
        previous.add(2);
        previous.add(3);
        previous.add(4);
        // this should trip partition detection
        assertTrue(HostMessenger.makePPDDecision(-1, previous, current, true));
    }

    @Test
    public void testPartitionDetection5050KillBlessed() throws Exception
    {
        Set<Integer> previous = new HashSet<Integer>();
        Set<Integer> current = new HashSet<Integer>();

        // current cluster has 2 hosts
        current.add(2);
        current.add(3);
        // the pre-fail cluster had 4 hosts and the lowest host ID
        previous.addAll(current);
        previous.add(0);
        previous.add(1);
        // this should trip partition detection
        assertTrue(HostMessenger.makePPDDecision(-1, previous, current, true));
    }

    @Test
    public void testPartitionDetection5050KillNonBlessed() throws Exception
    {
        Set<Integer> previous = new HashSet<Integer>();
        Set<Integer> current = new HashSet<Integer>();

        // current cluster has 2 hosts
        current.add(0);
        current.add(1);
        // the pre-fail cluster had 4 hosts but not the lowest host ID
        previous.addAll(current);
        previous.add(2);
        previous.add(3);
        // this should not trip partition detection
        assertFalse(HostMessenger.makePPDDecision(-1, previous, current, true));
    }

    @Test
    public void testShouldCorrectlyPropagateHostDisplayNames() throws Exception {
        MeshProber.Builder jc = MeshProber.builder()
                .coordinators(coordinators(3))
                .startAction(StartAction.PROBE)
                .nodeState(NodeState.INITIALIZING)
                .bare(true);

        int hostId1 = 0;
        int hostId2 = 1;
        int hostId3 = 2;

        String hostDisplayName1 = "host-0";
        String hostDisplayName2 = "host-1";
        String hostDisplayName3 = "host-2";

        HostMessenger hm1 = createHostMessengerWithDisplayName(hostId1, jc.build(), true, hostDisplayName1);
        HostMessenger hm2 = createHostMessengerWithDisplayName(hostId2, jc.build(), false, hostDisplayName2);
        HostMessenger hm3 = createHostMessengerWithDisplayName(hostId3, jc.build(), false, hostDisplayName3);

        final AtomicReference<Exception> exception = new AtomicReference<Exception>();
        HostMessengerThread hm2Start = new HostMessengerThread(hm2, exception);
        HostMessengerThread hm3Start = new HostMessengerThread(hm3, exception);

        hm2Start.start();
        hm2Start.join();

        hm3Start.start();
        hm3Start.join();

        if (exception.get() != null) {
            fail(exception.get().toString());
        }

        Determination dtm = prober(hm1).waitForDetermination();
        assertEquals(StartAction.CREATE, dtm.startAction);
        assertEquals(3, dtm.hostCount);

        assertEquals(dtm, prober(hm2).waitForDetermination());
        assertEquals(dtm, prober(hm3).waitForDetermination());

        Map<Integer, String> hostIdToHostDisplayName1 = toHostIdToHostDisplayNameMap(hm1.m_foreignHosts);
        Map<Integer, String> hostIdToHostDisplayName2 = toHostIdToHostDisplayNameMap(hm2.m_foreignHosts);
        Map<Integer, String> hostIdToHostDisplayName3 = toHostIdToHostDisplayNameMap(hm3.m_foreignHosts);

        assertThat(hostIdToHostDisplayName1).containsOnly(
                entry(hostId2, hostDisplayName2),
                entry(hostId3, hostDisplayName3)
        );
        assertThat(hostIdToHostDisplayName2).containsOnly(
                entry(hostId1, hostDisplayName1),
                entry(hostId3, hostDisplayName3)
        );
        assertThat(hostIdToHostDisplayName3).containsOnly(
                entry(hostId1, hostDisplayName1),
                entry(hostId2, hostDisplayName2)
        );
    }

    @Test
    public void testShouldCorrectlyPropagateHostDisplayNamesInIoStats() throws Exception {
        MeshProber.Builder jc = MeshProber.builder()
                .coordinators(coordinators(3))
                .startAction(StartAction.PROBE)
                .nodeState(NodeState.INITIALIZING)
                .bare(true);

        int hostId1 = 0;
        int hostId2 = 1;
        int hostId3 = 2;

        String hostDisplayName1 = "host-0";
        String hostDisplayName2 = "host-1";
        String hostDisplayName3 = "host-2";

        HostMessenger hm1 = createHostMessengerWithDisplayName(hostId1, jc.build(), true, hostDisplayName1);
        HostMessenger hm2 = createHostMessengerWithDisplayName(hostId2, jc.build(), false, hostDisplayName2);
        HostMessenger hm3 = createHostMessengerWithDisplayName(hostId3, jc.build(), false, hostDisplayName3);

        final AtomicReference<Exception> exception = new AtomicReference<Exception>();
        HostMessengerThread hm2Start = new HostMessengerThread(hm2, exception);
        HostMessengerThread hm3Start = new HostMessengerThread(hm3, exception);

        hm2Start.start();
        hm2Start.join();

        hm3Start.start();
        hm3Start.join();

        if (exception.get() != null) {
            fail(exception.get().toString());
        }

        Determination dtm = prober(hm1).waitForDetermination();
        assertEquals(StartAction.CREATE, dtm.startAction);
        assertEquals(3, dtm.hostCount);

        assertEquals(dtm, prober(hm2).waitForDetermination());
        assertEquals(dtm, prober(hm3).waitForDetermination());

        List<String> ioStatsConnectionNames1 = toConnectionNames(hm1.getIOStats(false));
        List<String> ioStatsConnectionNames2 = toConnectionNames(hm2.getIOStats(false));
        List<String> ioStatsConnectionNames3 = toConnectionNames(hm3.getIOStats(false));

        assertThat(ioStatsConnectionNames1).contains(
                hostDisplayName2,
                hostDisplayName3
        );
        assertThat(ioStatsConnectionNames2).contains(
                hostDisplayName1,
                hostDisplayName3
        );
        assertThat(ioStatsConnectionNames3).contains(
                hostDisplayName1,
                hostDisplayName2
        );
    }

    private Map<Integer, String> toHostIdToHostDisplayNameMap(ImmutableMap<Integer, ForeignHost> m_foreignHosts) {
        return m_foreignHosts.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().hostDisplayName()));
    }

    private List<String> toConnectionNames(Map<Long, Pair<String, long[]>> ioStats) {
        return ioStats.values().stream().map(Pair::getFirst).collect(Collectors.toList());
    }
}
