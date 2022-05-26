/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.voltcore.messaging;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;

public class SiteMailbox implements Mailbox {

    final HostMessenger m_hostMessenger;
    final ArrayList<Deque<VoltMessage>> m_messages = new ArrayList<Deque<VoltMessage>>();
    final long m_hsId;

    public SiteMailbox(HostMessenger hostMessenger, long hsId) {
        this.m_hostMessenger = hostMessenger;
        this.m_hsId = hsId;
        for (Subject s : Subject.values()) {
            m_messages.add( s.getId(), new ArrayDeque<VoltMessage>());
        }
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
