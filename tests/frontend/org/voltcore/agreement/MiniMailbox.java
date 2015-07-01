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

package org.voltcore.agreement;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.Queue;

import org.voltcore.agreement.FakeMesh.Message;
import org.voltcore.messaging.Mailbox;
import org.voltcore.messaging.Subject;
import org.voltcore.messaging.VoltMessage;

public class MiniMailbox implements Mailbox {

    private long m_HSId;
    final private Queue<Message> m_sendQ;
    final ArrayList<Deque<VoltMessage>> m_messages = new ArrayList<Deque<VoltMessage>>();
    private static final Subject m_defaultSubjects[] = new Subject[] { Subject.FAILURE, Subject.DEFAULT };

    MiniMailbox(long HSId, Queue<Message> sendQ)
    {
        m_HSId = HSId;
        m_sendQ = sendQ;
        for (Subject s : Subject.values()) {
            m_messages.add(s.getId(), new ArrayDeque<VoltMessage>());
        }
    }

    @Override
    public void send(long hsId, VoltMessage message) {
        message.m_sourceHSId = m_HSId;
        m_sendQ.offer(new Message(m_HSId, hsId, message));
    }

    @Override
    public void send(long[] hsIds, VoltMessage message) {
        for (long hsId : hsIds) {
            send(hsId, message);
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
    public long getHSId() {
        return m_HSId;
    }

    @Override
    public void setHSId(long hsId) {
        m_HSId = hsId;
    }
}
