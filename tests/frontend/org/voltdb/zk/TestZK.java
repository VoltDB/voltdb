/* This file is part of VoltDB.
 * Copyright (C) 2008-2011 VoltDB Inc.
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
package org.voltdb.zk;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.voltdb.VoltDB;
import org.voltdb.agreement.AgreementSite;
import org.voltdb.fault.FaultHandler;
import org.voltdb.fault.NodeFailureFault;
import org.voltdb.fault.VoltFault;
import org.voltdb.fault.VoltFault.FaultType;
import org.voltdb.messaging.Subject;
import org.voltdb.messaging.VoltMessage;
import org.apache.zookeeper_voltpatches.*;
import org.apache.zookeeper_voltpatches.KeeperException.NoNodeException;
import org.apache.zookeeper_voltpatches.Watcher.Event.EventType;
import org.apache.zookeeper_voltpatches.Watcher.Event.KeeperState;
import org.apache.zookeeper_voltpatches.ZooDefs.Ids;
import org.apache.zookeeper_voltpatches.data.Stat;
import org.voltdb.messaging.*;
import org.voltdb.utils.DBBPool;

import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

public class TestZK {

    private final int NUM_AGREEMENT_SITES = 8;

    private ArrayList<AgreementSite> m_agreementSites;
    private ArrayList<MockMailbox> m_mailboxes;
    private FaultDistributor m_faultDistributor;
    private ArrayList<ZooKeeper> m_clients;


    private class MockMailbox implements Mailbox {
        final ArrayList<Deque<VoltMessage>> m_messages = new ArrayList<Deque<VoltMessage>>();

        public MockMailbox() {
            for (Subject s : Subject.values()) {
                m_messages.add( s.getId(), new ArrayDeque<VoltMessage>());
            }
        }

        private final Subject m_defaultSubjects[] = new Subject[] { Subject.FAILURE, Subject.DEFAULT };

        @Override
        public void send(int siteId, int mailboxId, VoltMessage message)
                throws MessagingException {
            try {
                message = VoltMessage.createMessageFromBuffer(message.getBufferForMessaging(new DBBPool(true, false)).b, true);
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            assertTrue(mailboxId == VoltDB.AGREEMENT_MAILBOX_ID);
            try {
                Mailbox mailbox = m_mailboxes.get(siteId);
                if (mailbox != null) {
                    m_mailboxes.get(siteId).deliver(
                            VoltMessage.createMessageFromBuffer(
                                    message.getBufferForMessaging(
                                            new DBBPool(true, false)).b,
                                            true));
                }
            } catch (IOException e) {
                // TODO Auto-generated catch block
                throw new MessagingException(e);
            }
        }

        @Override
        public void send(int[] siteIds, int mailboxId, VoltMessage message)
                throws MessagingException {
            for (int siteId : siteIds) {
                send(siteId, mailboxId, message);
            }
        }

        @Override
        public void deliver(VoltMessage message) {
            deliver(message, false);
        }

        @Override
        public void deliverFront(VoltMessage message) {
            deliver(message, true);
        }

        public void deliver(VoltMessage message, final boolean toFront) {
            final Deque<VoltMessage> dq = m_messages.get(message.getSubject());
            synchronized (this) {
                if (toFront){
                    dq.push(message);
                } else {
                    dq.offer(message);
                }
                this.notify();
            }
        }

        @Override
        public VoltMessage recv() {
            return recv(m_defaultSubjects);
        }

        @Override
        public VoltMessage recvBlocking() {
            return recvBlocking(m_defaultSubjects);
        }

        @Override
        public VoltMessage recvBlocking(long timeout) {
            return recvBlocking(m_defaultSubjects, timeout);
        }

        @Override
        public synchronized VoltMessage recv(Subject[] subjects) {
            for (Subject s : subjects) {
                final Deque<VoltMessage> dq = m_messages.get(s.getId());
                assert(dq != null);
                VoltMessage m = dq.poll();
                if (m != null) {
                    return m;
                }
            }
            return null;
        }

        @Override
        public synchronized VoltMessage recvBlocking(Subject[] subjects) {
            VoltMessage message = null;
            while (message == null) {
                for (Subject s : subjects) {
                    final Deque<VoltMessage> dq = m_messages.get(s.getId());
                    message = dq.poll();
                    if (message != null) {
                        return message;
                    }
                }
                try {
                    this.wait();
                } catch (InterruptedException e) {
                    return null;
                }
            }
            return null;
        }

        @Override
        public synchronized VoltMessage recvBlocking(Subject[] subjects, long timeout) {
            VoltMessage message = null;
            for (Subject s : subjects) {
                final Deque<VoltMessage> dq = m_messages.get(s.getId());
                message = dq.poll();
                if (message != null) {
                    return message;
                }
            }
            try {
                this.wait(timeout);
            } catch (InterruptedException e) {
                return null;
            }
            for (Subject s : subjects) {
                final Deque<VoltMessage> dq = m_messages.get(s.getId());
                message = dq.poll();
                if (message != null) {
                    return message;
                }
            }
            return null;
        }

        @Override
        public int getSiteId() {
            fail();
            return 0;
        }
    }

    private class FaultDistributor implements org.voltdb.fault.FaultDistributorInterface {
        private final HashMap<FaultHandler, HashSet<VoltFault>> m_faults = new HashMap<FaultHandler, HashSet<VoltFault>>();

        private final ArrayList<FaultHandler> m_faultHandlers = new ArrayList<FaultHandler>();

        private Integer m_expectedHandler = null;
        @Override
        public void registerFaultHandler(int order, FaultHandler handler,
                FaultType... types) {
            m_faults.put(handler, new HashSet<VoltFault>());
            if (m_expectedHandler == null) {
                m_faultHandlers.add(handler);
            } else {
                m_faultHandlers.set(m_expectedHandler, handler);
                m_expectedHandler = null;
            }
        }

        @Override
        public void reportFault(VoltFault fault) {
            for (Entry<FaultHandler, HashSet<VoltFault>> entry : m_faults.entrySet()) {
                if (entry.getValue().contains(fault)) {
                    return;
                }
                entry.getValue().add(fault);
                entry.getKey().faultOccured(entry.getValue());
            }
        }

        @Override
        public void reportFaultHandled(FaultHandler handler, VoltFault fault) {
           assertTrue(m_faults.get(handler).remove(fault));
        }

        @Override
        public void reportFaultCleared(VoltFault fault) {
            HashSet<VoltFault> clearedFaults = new HashSet<VoltFault>();
            clearedFaults.add(fault);
            for (FaultHandler handler : m_faults.keySet()) {
                handler.faultCleared(clearedFaults);
            }
        }

        @Override
        public void shutDown() throws InterruptedException {
            // TODO Auto-generated method stub

        }

        @Override
        public PPDPolicyDecision makePPDPolicyDecisions(
                HashSet<Integer> newFailedSiteIds) {
            // TODO Auto-generated method stub
            return null;
        }

    }

    @Before
    public void setUp() throws Exception {
        m_clients = new ArrayList<ZooKeeper>();
        m_mailboxes = new ArrayList<MockMailbox>();
        m_faultDistributor = new FaultDistributor();
        m_agreementSites = new ArrayList<AgreementSite>();
        Set<Integer> agreementSiteIds = new HashSet<Integer>();
        for (int ii = 0; ii < NUM_AGREEMENT_SITES; ii++) agreementSiteIds.add(ii);
        for (int ii = 0; ii < NUM_AGREEMENT_SITES; ii++) {
            m_mailboxes.add(new MockMailbox());
            m_agreementSites.add( new AgreementSite(
                    ii,
                    agreementSiteIds,
                    ii,
                    new HashSet<Integer>(),
                    m_mailboxes.get(ii),
                    new InetSocketAddress(2181 + ii),
                    m_faultDistributor,
                    false));
        }
        for (AgreementSite site : m_agreementSites) {
            site.start();
        }
    }

    @After
    public void tearDown() throws Exception {
        for (ZooKeeper keeper : m_clients) {
            keeper.close();
        }
        m_clients.clear();
        for (AgreementSite site : m_agreementSites) {
            if (site != null) {
                site.shutdown();
            }
        }
    }

    public void failSite(int site) throws Exception {
        m_agreementSites.get(site).shutdown();
        m_agreementSites.set(site, null);
        m_mailboxes.set(site, null);
        m_faultDistributor.m_faults.remove(m_faultDistributor.m_faultHandlers.get(site));
        m_faultDistributor.m_faultHandlers.remove(site);
        m_faultDistributor.reportFault(
                new NodeFailureFault(
                        site,
                        new HashSet<Integer>(Arrays.asList(site)),
                        Integer.toString(site)));
    }

    public void recoverSite(int site) throws Exception {
        HashSet<Integer> failedSites = new HashSet<Integer>();
        HashSet<Integer> agreementSiteIds = new HashSet<Integer>();
        int zz = 0;
        for (MockMailbox mailbox : m_mailboxes) {
            agreementSiteIds.add(zz);
            if (mailbox == null && zz != site) {
                failedSites.add(zz);
            }
            zz++;
        }
        m_mailboxes.set( site, new MockMailbox());
        m_faultDistributor.reportFaultCleared(
                new NodeFailureFault(
                        site,
                        new HashSet<Integer>(Arrays.asList(site)),
                        Integer.toString(site)));
        m_faultDistributor.m_expectedHandler = site;
        m_agreementSites.set(site, new AgreementSite(
                    site,
                    agreementSiteIds,
                    site,
                    failedSites,
                    m_mailboxes.get(site),
                    new InetSocketAddress(2181 + site),
                    m_faultDistributor,
                    true));
        m_agreementSites.get(site).start();
        for (int ii = 0; ii < m_agreementSites.size(); ii++) {
            if (ii == site) {
                continue;
            }
            m_agreementSites.get(ii).clearFault(site);
        }
    }

    public ZooKeeper getClient(int site) throws Exception {
        final Semaphore permit = new Semaphore(0);
        ZooKeeper keeper = new ZooKeeper("localhost:" + Integer.toString(2181 + site), 4000, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                if (event.getState() == KeeperState.SyncConnected) {
                    permit.release();
                }
                System.out.println(event);
            }});
        m_clients.add(keeper);
        permit.acquire();
        return keeper;
    }

    @Test
    public void testBasic() throws Exception {
        ZooKeeper zk = getClient(0);
        zk.create("/foo", new byte[1], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

        ZooKeeper zk2 = getClient(1);
        Stat stat = new Stat();
        assertEquals( 1, zk2.getData("/foo", false, stat).length);
        zk2.setData("/foo", new byte[4], stat.getVersion());

        assertEquals( 4, zk.getData("/foo", false, stat).length);
        zk.delete("/foo", -1);

        zk2.create("/bar", new byte[6], Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        zk.create("/bar", new byte[7], Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);

        ZooKeeper zk3 = getClient(2);
        List<String> children = zk3.getChildren("/", false);
        System.out.println(children);
        assertEquals(3, children.size());
        assertTrue(children.contains("zookeeper"));
        assertTrue(children.contains("bar0000000002"));
        assertTrue(children.contains("bar0000000003"));

        zk.close();
        zk2.close();
        m_clients.clear(); m_clients.add(zk3);

        children = zk3.getChildren("/", false);
        assertEquals(1, children.size());
        assertTrue(children.contains("zookeeper"));
        System.out.println(children);
    }

    @Test
    public void testFailure() throws Exception {
        ZooKeeper zk = getClient(2);
        zk.create("/foo", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        assertEquals(zk.getData("/foo", false, null).length, 0);
        System.out.println("Created node");
        failSite(0);
        assertEquals(zk.getData("/foo", false, null).length, 0);
    }

    @Test
    public void testFailureKillsEphemeral() throws Exception {
        ZooKeeper zk = getClient(0);
        zk.create("/foo", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        assertEquals(zk.getData("/foo", false, null).length, 0);
        failSite(0);
        zk = getClient(1);
        try {
            zk.getData("/foo", false, null);
        } catch (NoNodeException e) {
            return;
        }
        fail();
    }

    @Test
    public void testRecovery() throws Exception {
        ZooKeeper zk = getClient(0);
        zk.create("/foo", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        failSite(0);
        zk.close();
        zk = getClient(1);
        assertEquals(zk.getData("/foo", false, null).length, 0);
        recoverSite(0);
        assertEquals(zk.getData("/foo", false, null).length, 0);
        zk = getClient(0);
        assertEquals(zk.getData("/foo", false, null).length, 0);
    }

    @Test
    public void testWatches() throws Exception {
        ZooKeeper zk = getClient(0);
        ZooKeeper zk2 = getClient(1);
        final Semaphore sem = new Semaphore(0);
        zk.exists("/foo", new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                if (event.getType() == EventType.NodeCreated) {
                    sem.release();
                    System.out.println(event);
                }
            }
        });
        zk2.create("/foo", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        sem.tryAcquire(5, TimeUnit.SECONDS);

        zk.create("/foo2", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

        zk2.exists("/foo2", new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                if (event.getType() == EventType.NodeDeleted) {
                    sem.release();
                    System.out.println(event);
                }
            }
        });

        zk.delete("/foo2", -1);
        sem.acquire();
    }

    @Test
    public void testNullVsZeroLengthData() throws Exception {
        ZooKeeper zk = getClient(0);
        zk.create("/foo", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        zk.create("/bar", null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        assertEquals(null, zk.getData("/bar", false, null));
        assertTrue(zk.getData("/foo", false, null).length == 0);
    }
}
