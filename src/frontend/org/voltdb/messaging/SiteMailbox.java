/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB L.L.C.
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

package org.voltdb.messaging;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Queue;

import org.voltdb.debugstate.MailboxHistory;
import org.voltdb.debugstate.MailboxHistory.MessageState;

/**
 *
 *
 */
public class SiteMailbox implements Mailbox {

    final HostMessenger m_hostMessenger;
    final ArrayList<Queue<VoltMessage>> m_messages = new ArrayList<Queue<VoltMessage>>();
    final int m_siteId;

    // deques to store recent inter-site messages
    private final int MESSAGE_HISTORY_SIZE;
    ArrayDeque<VoltMessage> m_lastTenSentMessages = new ArrayDeque<VoltMessage>();
    ArrayDeque<VoltMessage> m_lastTenReceivedMessages = new ArrayDeque<VoltMessage>();
    ArrayDeque<VoltMessage> m_lastTenMembershipNotices = new ArrayDeque<VoltMessage>();
    ArrayDeque<VoltMessage> m_lastTenHeartbeats = new ArrayDeque<VoltMessage>();

    SiteMailbox(HostMessenger hostMessenger, int siteId, int mailboxId, Queue<VoltMessage> queue) {
        this.m_hostMessenger = hostMessenger;
        this.m_siteId = siteId;
        MESSAGE_HISTORY_SIZE = 0;
        for (Subject s : Subject.values()) {
            if (s.equals(Subject.DEFAULT)) {
                if (queue == null) {
                    m_messages.add( s.getId(), new ArrayDeque<VoltMessage>());
                } else {
                    m_messages.add( s.getId(), queue);
                }
            } else {
                m_messages.add( s.getId(), new ArrayDeque<VoltMessage>());
            }
        }
    }

    @Override
    public void deliver(VoltMessage message) {
        assert(message != null);
        final Queue<VoltMessage> dq = m_messages.get(message.getSubject());
        assert(dq != null);
        synchronized (dq) {
            dq.offer(message);
            dq.notify();
        }

        // tag some extra transient data for debugging
        message.receivedFromSiteId = m_siteId;

        // this code keeps track of last 10 messages received in various buckets

        if (MESSAGE_HISTORY_SIZE > 0) {
            if (message instanceof DebugMessage)
                return;
            else if (message instanceof TransactionInfoBaseMessage) {
                TransactionInfoBaseMessage mn = (TransactionInfoBaseMessage) message;
                if (mn instanceof HeartbeatMessage)
                    synchronized(m_lastTenHeartbeats) {
                        m_lastTenHeartbeats.addLast(message);
                        if (m_lastTenHeartbeats.size() > MESSAGE_HISTORY_SIZE)
                            m_lastTenHeartbeats.pollFirst();
                    }
                else
                    synchronized(m_lastTenMembershipNotices) {
                        m_lastTenMembershipNotices.addLast(message);
                        if (m_lastTenMembershipNotices.size() > MESSAGE_HISTORY_SIZE)
                            m_lastTenMembershipNotices.pollFirst();
                    }
            }
            else synchronized(m_lastTenReceivedMessages) {
                m_lastTenReceivedMessages.addLast(message);
                if (m_lastTenReceivedMessages.size() > MESSAGE_HISTORY_SIZE)
                    m_lastTenReceivedMessages.pollFirst();
            }
        }
    }

    @Override
    public int getWaitingCount() {
        return m_messages.get(Subject.DEFAULT.getId()).size();
    }

    @Override
    public VoltMessage recv() {
        return recv(Subject.DEFAULT);
    }

    @Override
    public VoltMessage recvBlocking() {
        return recvBlocking(Subject.DEFAULT);
    }

    @Override
    public VoltMessage recvBlocking(long timeout) {
        return recvBlocking(Subject.DEFAULT, timeout);
    }

    @Override
    public void send(int siteId, int mailboxId, VoltMessage message)
            throws MessagingException {
        assert(message != null);
        m_hostMessenger.send(siteId, mailboxId, message);

        // this code keeps track of last 10 non-heartbeat messages sent
        if (message instanceof HeartbeatMessage)
            return;
        synchronized(m_lastTenSentMessages) {
            m_lastTenSentMessages.addLast(message);
            if (m_lastTenSentMessages.size() > MESSAGE_HISTORY_SIZE)
                m_lastTenSentMessages.pollFirst();
        }
    }

    @Override
    public void send(int[] siteIds, int mailboxId, VoltMessage message)
            throws MessagingException {
        assert(message != null);
        assert(siteIds != null);
        m_hostMessenger.send(siteIds, mailboxId, message);

        // this code keeps track of last 10 non-heartbeat messages sent
        if (message instanceof HeartbeatMessage)
            return;
        synchronized(m_lastTenSentMessages) {
            m_lastTenSentMessages.addLast(message);
            if (m_lastTenSentMessages.size() > MESSAGE_HISTORY_SIZE)
                m_lastTenSentMessages.pollFirst();
        }
    }

    public MailboxHistory getHistory() {
        MailboxHistory retval = new MailboxHistory();

        synchronized(m_lastTenSentMessages) {
            retval.messagesSent = new MessageState[m_lastTenSentMessages.size()];
            int i = 0;
            for (VoltMessage message : m_lastTenSentMessages) {
                assert (message != null);
                retval.messagesSent[i++] = message.getDumpContents();
            }
        }

        synchronized(m_lastTenReceivedMessages) {
            retval.messagesReceived = new MessageState[m_lastTenReceivedMessages.size()];
            int i = 0;
            for (VoltMessage message : m_lastTenReceivedMessages) {
                assert (message != null);
                retval.messagesReceived[i++] = message.getDumpContents();
            }
        }

        synchronized(m_lastTenHeartbeats) {
            retval.heartbeatsReceived = new MessageState[m_lastTenHeartbeats.size()];
            int i = 0;
            for (VoltMessage message : m_lastTenHeartbeats) {
                assert (message != null);
                retval.heartbeatsReceived[i++] = message.getDumpContents();
            }
        }

        synchronized(m_lastTenMembershipNotices) {
            retval.noticesReceived = new MessageState[m_lastTenMembershipNotices.size()];
            int i = 0;
            for (VoltMessage message : m_lastTenMembershipNotices) {
                assert (message != null);
                retval.noticesReceived[i++] = message.getDumpContents();
            }
        }

        return retval;
    }

    @Override
    public VoltMessage recv(Subject s) {
        final Queue<VoltMessage> dq = m_messages.get(s.getId());
        assert(dq != null);
        synchronized (dq) {
            return dq.poll();
        }
    }

    @Override
    public VoltMessage recvBlocking(Subject s) {
        try {
            final Queue<VoltMessage> dq = m_messages.get(s.getId());
            assert(dq != null);
            synchronized (dq) {
                while (dq.isEmpty()) {
                    dq.wait();
                }
                final VoltMessage message = dq.poll();
                assert(message != null);
                return message;
            }
        } catch (InterruptedException e) {
        }
        return null;
    }

    @Override
    public VoltMessage recvBlocking(Subject s, long timeout) {
        try {
            final Queue<VoltMessage> dq = m_messages.get(s.getId());
            assert(dq != null);
            synchronized (dq) {
                if (dq.isEmpty()) {
                    dq.wait(timeout);
                }
                return dq.poll();
            }
        } catch (InterruptedException e) {
        }
        return null;
    }
}
