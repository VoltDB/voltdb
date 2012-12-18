/* This file is part of VoltDB.
 * Copyright (C) 2008-2012 VoltDB Inc.
 *
 * VoltDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * VoltDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.voltcore.messaging;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.Set;
import java.util.concurrent.Semaphore;

import org.voltdb.CatalogContext;
import org.voltdb.CommandLog;
import org.voltdb.messaging.InitiateTaskMessage;
import org.voltdb.messaging.Iv2InitiateTaskMessage;

public class SiteMailbox implements Mailbox {

    private CommandLog m_commandLog = new CommandLog() {
        @Override
        public void init(CatalogContext context, long txnId, long perPartitionTxnId[], String affinity) {}
        @Override
        public void initForRejoin(CatalogContext context, long txnId, long perPartitionTxnId[], boolean isRejoin, String affinity) {}
        @Override
        public boolean needsInitialization() {
            return false;
        }
        @Override
        public void log(InitiateTaskMessage message) {}
        @Override
        public void shutdown() throws InterruptedException {}
        @Override
        public Semaphore logFault(Set<Long> failedSites, Set<Long> faultedTxns) {
            return null;
        }
        @Override
        public void logHeartbeat(long txnId) {}
        @Override
        public long getFaultSequenceNumber() {
            return 0;
        }
        @Override
        public boolean log(
                Iv2InitiateTaskMessage message,
                long spHandle,
                DurabilityListener l,
                Object handle) {
            return false;
        }
        @Override
        public void logIv2Fault(long writerHSId, Set<Long> survivorHSId,
                int partitionId, long spHandle) {
        }
        };
    final HostMessenger m_hostMessenger;
    final ArrayList<Deque<VoltMessage>> m_messages = new ArrayList<Deque<VoltMessage>>();
    final long m_hsId;

    SiteMailbox(HostMessenger hostMessenger, long hsId) {
        this.m_hostMessenger = hostMessenger;
        this.m_hsId = hsId;
        for (Subject s : Subject.values()) {
            m_messages.add( s.getId(), new ArrayDeque<VoltMessage>());
        }
    }

    public void setCommandLog(CommandLog log) {
        m_commandLog = log;
    }

    @Override
    public void deliverFront(VoltMessage message) {
        deliver(message, true);
    }

    @Override
    public void deliver(VoltMessage message) {
        deliver(message, false);
    }

    public void deliver(VoltMessage message, final boolean toFront) {
        assert(message != null);

        /*
         * Doing delivery here so that the delivery thread is the one interacting with the
         * log instead of the receiver. This way only the network threads contend for the log.
         */
        if (message instanceof InitiateTaskMessage) {
            InitiateTaskMessage msg = (InitiateTaskMessage)message;
            if (!msg.isReadOnly()) {
                m_commandLog.log(msg);
            }
        } else if (message instanceof HeartbeatMessage) {
            HeartbeatMessage msg = (HeartbeatMessage)message;
            m_commandLog.logHeartbeat(msg.getTxnId());
        }

        final Deque<VoltMessage> dq = m_messages.get(message.getSubject());
        synchronized (this) {
            if (toFront) {
                dq.offerFirst(message);
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

    private static final Subject m_defaultSubjects[] = new Subject[] { Subject.FAILURE, Subject.DEFAULT };
    @Override
    public VoltMessage recvBlocking(long timeout) {
        return recvBlocking(m_defaultSubjects, timeout);
    }

    @Override
    public void send(long hsId, VoltMessage message)
    {
        assert(message != null);
        message.m_sourceHSId = m_hsId;
        m_hostMessenger.send(hsId, message);
    }

    @Override
    public void send(long[] hsIds, VoltMessage message)
    {
        assert(message != null);
        assert(hsIds != null);
        message.m_sourceHSId = m_hsId;
        m_hostMessenger.send(hsIds, message);
    }

    @Override
    public synchronized VoltMessage recv(Subject subjects[]) {
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
    public synchronized VoltMessage recvBlocking(Subject subjects[]) {
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
    public synchronized VoltMessage recvBlocking(Subject subjects[], long timeout) {
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

    /**
     * Get the number of messages waiting to be delivered for this mailbox.
     *
     * @return An integer representing the number of waiting messages.
     */
    public int getWaitingCount() {
        return m_messages.get(Subject.DEFAULT.getId()).size();
    }

    @Override
    public long getHSId() {
        return m_hsId;
    }

    @Override
    public void setHSId(long hsId) {
        throw new UnsupportedOperationException();
    }
}
