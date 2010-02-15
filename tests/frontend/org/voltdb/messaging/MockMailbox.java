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

import java.util.ArrayDeque;

import org.voltdb.dtxn.SimpleDtxnInitiator;
import org.voltdb.messaging.Mailbox;
import org.voltdb.messaging.MessagingException;

public class MockMailbox implements Mailbox {

    public MockMailbox(SimpleDtxnInitiator.DummyBlockingQueue queue) {
        incomingMessages = queue;
    }
    public void send(int siteId, int mailboxId, VoltMessage message) throws MessagingException {
        outgoingMessages.add(new Message(siteId, mailboxId, message));
    }

    public void send(int[] siteIds, int mailboxId, VoltMessage message) throws MessagingException {
        throw new UnsupportedOperationException();
    }

    public int getWaitingCount() {
        throw new UnsupportedOperationException();
    }

    public VoltMessage recv() {
        throw new UnsupportedOperationException();
    }

    public VoltMessage recvBlocking() {
        throw new UnsupportedOperationException();
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

    public void deliver(VoltMessage message) {
        incomingMessages.offer(message);
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

    private final SimpleDtxnInitiator.DummyBlockingQueue incomingMessages;
    private final ArrayDeque<Message> outgoingMessages = new ArrayDeque<Message>();
    @Override
    public VoltMessage recvBlocking(long timeout) {
        throw new UnsupportedOperationException();
    }
}
