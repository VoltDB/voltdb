/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB L.L.C.
 *
 * This file contains original code and/or modifications of original code.
 * Any modifications made by VoltDB L.L.C. are licensed under the following
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
import java.util.concurrent.LinkedBlockingQueue;

import org.voltdb.messaging.Mailbox;
import org.voltdb.messaging.MessagingException;

public class MockMailbox implements Mailbox {

    private static HashMap<Integer, MockMailbox> postoffice =
        new HashMap<Integer, MockMailbox>();

    public static void registerMailbox(int siteId, MockMailbox mbox) {
        postoffice.put(siteId, mbox);
    }

    public MockMailbox(LinkedBlockingQueue<VoltMessage> queue) {
        incomingMessages = queue;
        m_failureNoticeMessages = new LinkedBlockingQueue<VoltMessage>();
    }

    public void send(int siteId, int mailboxId, VoltMessage message) throws MessagingException {
        outgoingMessages.add(new Message(siteId, mailboxId, message));

        MockMailbox dest = postoffice.get(siteId);
        if (dest != null) {
            dest.deliver(message);
        }
    }

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

    public VoltMessage recv() {
        return recv(Subject.DEFAULT);
    }

    public VoltMessage recvBlocking() {
        return recvBlocking(Subject.DEFAULT);
    }

    @Override
    public VoltMessage recvBlocking(long timeout) {
        return recvBlocking(Subject.DEFAULT, timeout);
    }

    @Override
    public VoltMessage recv(Subject s) {
        throw new UnsupportedOperationException();
    }

    @Override
    public VoltMessage recvBlocking(Subject s) {
        try {
            if (s == Subject.DEFAULT)
                return incomingMessages.take();

            else
                return m_failureNoticeMessages.take();
        }
        catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

    }

    @Override
    public VoltMessage recvBlocking(Subject s, long timeout) {
        do {
            try {
                if (s == Subject.DEFAULT)
                    return incomingMessages.take();
                else
                    return m_failureNoticeMessages.take();
            }
            catch (InterruptedException e) {
                e.printStackTrace();
            }
        } while (true);
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
        if (message.getSubject() == Subject.DEFAULT.getId())
            incomingMessages.offer(message);
        else
            m_failureNoticeMessages.offer(message);
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

    // Queue for DEFAULT Messages
    private final LinkedBlockingQueue<VoltMessage> incomingMessages;

    // Queue for FAILURE_SITE_UPDATE Messages
    private final LinkedBlockingQueue<VoltMessage> m_failureNoticeMessages;

    private final ArrayDeque<Message> outgoingMessages = new ArrayDeque<Message>();
}
