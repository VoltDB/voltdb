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

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.Map.Entry;
import java.util.concurrent.Semaphore;

import org.apache.zookeeper_voltpatches.WatchedEvent;
import org.apache.zookeeper_voltpatches.Watcher;
import org.apache.zookeeper_voltpatches.ZooKeeper;
import org.apache.zookeeper_voltpatches.Watcher.Event.KeeperState;
import org.voltdb.VoltDB;
import org.voltdb.agreement.AgreementSite;
import org.voltdb.fault.FaultHandler;
import org.voltdb.fault.VoltFault;
import org.voltdb.fault.VoltFault.FaultType;
import org.voltdb.messaging.Mailbox;
import org.voltdb.messaging.MessagingException;
import org.voltdb.messaging.Subject;
import org.voltdb.messaging.VoltMessage;
import org.voltdb.utils.DBBPool;

/**
 *
 */
public class ZKTestBase {
    protected ArrayList<AgreementSite> m_agreementSites;
    protected ArrayList<MockMailbox> m_mailboxes;
    protected FaultDistributor m_faultDistributor;
    protected ArrayList<ZooKeeper> m_clients;

    protected class MockMailbox implements Mailbox {
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

    protected class FaultDistributor implements org.voltdb.fault.FaultDistributorInterface {
        protected final HashMap<FaultHandler, HashSet<VoltFault>> m_faults = new HashMap<FaultHandler, HashSet<VoltFault>>();

        protected final ArrayList<FaultHandler> m_faultHandlers = new ArrayList<FaultHandler>();

        protected Integer m_expectedHandler = null;
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

    protected void setUpZK(int sites) throws Exception {
        m_clients = new ArrayList<ZooKeeper>();
        m_mailboxes = new ArrayList<MockMailbox>();
        m_faultDistributor = new FaultDistributor();
        m_agreementSites = new ArrayList<AgreementSite>();
        Set<Integer> agreementSiteIds = new HashSet<Integer>();
        for (int ii = 0; ii < sites; ii++) agreementSiteIds.add(ii);
        for (int ii = 0; ii < sites; ii++) {
            m_mailboxes.add(new MockMailbox());
            m_agreementSites.add( new AgreementSite(
                    ii,
                    agreementSiteIds,
                    ii,
                    new HashSet<Integer>(),
                    m_mailboxes.get(ii),
                    new InetSocketAddress(2182 + ii),
                    m_faultDistributor,
                    false));
        }
        for (AgreementSite site : m_agreementSites) {
            site.start();
        }
    }

    protected void tearDownZK() throws Exception {
        for (ZooKeeper keeper : m_clients) {
            keeper.close();
        }
        m_clients.clear();
        for (AgreementSite site : m_agreementSites) {
            if (site != null) {
                site.shutdown();
            }
        }
        m_agreementSites.clear();
    }

    protected ZooKeeper getClient(int site) throws Exception {
        final Semaphore permit = new Semaphore(0);
        ZooKeeper keeper = new ZooKeeper("localhost:" + Integer.toString(2182 + site), 4000, new Watcher() {
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
}
