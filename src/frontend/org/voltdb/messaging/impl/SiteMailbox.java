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

package org.voltdb.messaging.impl;

import java.util.ArrayDeque;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.BlockingQueue;
import org.voltdb.debugstate.MailboxHistory;
import org.voltdb.debugstate.MailboxHistory.MessageState;
import org.voltdb.messages.DebugMessage;
import org.voltdb.messages.MembershipNotice;
import org.voltdb.messaging.*;

/**
 *
 *
 */
public class SiteMailbox implements Mailbox {

    final HostMessenger m_hostMessenger;
    final BlockingQueue<VoltMessage> m_messages;
    final int m_siteId;

    // deques to store recent inter-site messages
    static final int MESSAGE_HISTORY_SIZE = 0;
    ArrayDeque<VoltMessage> m_lastTenSentMessages = new ArrayDeque<VoltMessage>();
    ArrayDeque<VoltMessage> m_lastTenReceivedMessages = new ArrayDeque<VoltMessage>();
    ArrayDeque<VoltMessage> m_lastTenMembershipNotices = new ArrayDeque<VoltMessage>();
    ArrayDeque<VoltMessage> m_lastTenHeartbeats = new ArrayDeque<VoltMessage>();

    SiteMailbox(HostMessenger hostMessenger, int siteId, int mailboxId, BlockingQueue<VoltMessage> queue) {
        this.m_hostMessenger = hostMessenger;
        this.m_siteId = siteId;
        if (queue == null) {
            m_messages = new LinkedBlockingQueue<VoltMessage>();
        } else {
            m_messages = queue;
        }
    }

    void deliver(VoltMessage message) {
        assert(message != null);
        boolean ok = m_messages.offer(message);
        assert(ok);

        // tag some extra transient data for debugging
        message.receivedFromSiteId = m_siteId;

        // this code keeps track of last 10 messages received in various buckets
        if (message instanceof DebugMessage)
            return;
        else if (message instanceof MembershipNotice) {
            MembershipNotice mn = (MembershipNotice) message;
            if (mn.isHeartBeat())
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

    @Override
    public int getWaitingCount() {
        return m_messages.size();
    }

    @Override
    public VoltMessage recv() {
        return m_messages.poll();
    }

    @Override
    public VoltMessage recvBlocking() {
        try {
            return m_messages.take();
        } catch (InterruptedException e) {
        }
        return null;
    }

    @Override
    public VoltMessage recvBlocking(long timeout) {
        try {
            return m_messages.poll( timeout, java.util.concurrent.TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
        }
        return null;
    }

    @Override
    public void send(int siteId, int mailboxId, VoltMessage message)
            throws MessagingException {
        assert(message != null);
        m_hostMessenger.send(siteId, mailboxId, message);

        // this code keeps track of last 10 non-heartbeat messages sent
        if ((message instanceof MembershipNotice) && (((MembershipNotice)message).isHeartBeat()))
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
        if ((message instanceof MembershipNotice) && (((MembershipNotice)message).isHeartBeat()))
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
}
