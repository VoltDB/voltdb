/* This file is part of VoltDB.
 * Copyright (C) 2008-2011 VoltDB Inc.
 *
 * This file contains original code and/or modifications of original code.
 * Any modifications made by VoltDB Inc. are licensed under the following
 * terms and conditions:
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
/* Copyright (C) 2008
 * Evan Jones
 * Massachusetts Institute of Technology
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
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

package org.voltdb.messaging;

import java.util.*;

import org.voltdb.messaging.Mailbox;
import org.voltdb.messaging.MessagingException;

public class MockMailbox implements Mailbox {

    private static HashMap<Integer, MockMailbox> postoffice =
        new HashMap<Integer, MockMailbox>();

    public static void registerMailbox(int siteId, MockMailbox mbox) {
        postoffice.put(siteId, mbox);
    }

    public MockMailbox() {
        for (Subject s : Subject.values()) {
            m_messages.add( s.getId(), new ArrayDeque<VoltMessage>());
        }
    }

    @Override
    public void send(int siteId, int mailboxId, VoltMessage message) throws MessagingException {
        outgoingMessages.add(new Message(siteId, mailboxId, message));

        MockMailbox dest = postoffice.get(siteId);
        if (dest != null) {
            dest.deliver(message);
        }
    }

    @Override
    public void send(int[] siteIds, int mailboxId, VoltMessage message) throws MessagingException {
        for (int i=0; siteIds != null && i < siteIds.length; ++i) {
            MockMailbox dest = postoffice.get(siteIds[i]);
            if (dest != null) {
                dest.deliver(message);
            }
        }
    }

    public int getWaitingCount() {
        throw new UnsupportedOperationException();
    }

    private static final Subject m_defaultSubjects[] = new Subject[] { Subject.FAILURE_SITE_UPDATE, Subject.DEFAULT };

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
    public synchronized VoltMessage recv(Subject subjects[]) {
        for (Subject s : subjects) {
            final Queue<VoltMessage> dq = m_messages.get(s.getId());
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
                final Queue<VoltMessage> dq = m_messages.get(s.getId());
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
            final Queue<VoltMessage> dq = m_messages.get(s.getId());
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
            final Queue<VoltMessage> dq = m_messages.get(s.getId());
            message = dq.poll();
            if (message != null) {
                return message;
            }
        }
        return null;
    }

    public VoltMessage popLastMessage() {
        return outgoingMessages.pollLast().contents;
    }

    public boolean noSentMessages() {
        return outgoingMessages.isEmpty();
    }

    public boolean lastEquals(int siteId, int mailboxId) {
        Message last = outgoingMessages.peekLast();
        return last.siteId == siteId && last.mailboxId == mailboxId;
    }
    public boolean lastEquals(int siteId, int mailboxId, Object contents) {
        return lastEquals(siteId, mailboxId) && outgoingMessages.peekLast().contents == contents;
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
            if (toFront) {
                dq.push(message);
            } else {
                dq.offer(message);
            }
            this.notify();
        }
    }

    public VoltMessage next;

    private static class Message {
        public Message(int siteId, int mailboxId, VoltMessage contents) {
            this.siteId = siteId;
            this.mailboxId = mailboxId;
            this.contents = contents;
        }

        public final int siteId;
        public final int mailboxId;
        public final VoltMessage contents;
    }

    final ArrayList<Deque<VoltMessage>> m_messages = new ArrayList<Deque<VoltMessage>>();

    private final ArrayDeque<Message> outgoingMessages = new ArrayDeque<Message>();

    @Override
    public int getSiteId() {
        // TODO Auto-generated method stub
        return 0;
    }
}
