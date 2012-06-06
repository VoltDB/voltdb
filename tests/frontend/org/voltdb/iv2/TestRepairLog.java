/* This file is part of VoltDB.
 * Copyright (C) 2008-2012 VoltDB Inc.
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

package org.voltdb.iv2;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import static org.mockito.Mockito.*;

import org.voltcore.messaging.VoltMessage;

import org.voltdb.messaging.Iv2InitiateTaskMessage;
import junit.framework.TestCase;
import org.junit.Test;

public class TestRepairLog extends TestCase
{
    VoltMessage truncInitMsg(long truncPt, long handle)
    {
        Iv2InitiateTaskMessage msg = mock(Iv2InitiateTaskMessage.class);
        when(msg.getTruncationHandle()).thenReturn(truncPt);
        when(msg.getSpHandle()).thenReturn(handle);
        return msg;
    }

    VoltMessage nonTruncInitMsg()
    {
        return truncInitMsg(Long.MIN_VALUE, 0);
    }

    private static class FooMessage extends VoltMessage
    {
        @Override
        protected void initFromBuffer(ByteBuffer buf) throws IOException {
        }

        @Override
        public void flattenToBuffer(ByteBuffer buf) throws IOException {
        }
    }

    @Test
    public void testOffer()
    {
        // offer some various messages to log and check
        // that it keeps the expected ones.
        RepairLog rl = new RepairLog();
        VoltMessage m1 = nonTruncInitMsg();
        VoltMessage m2 = nonTruncInitMsg();

        rl.deliver(m1);
        rl.deliver(m2);

        List<RepairLog.Item> contents = rl.contents();
        assertEquals(2, contents.size());
        assertEquals(m1, contents.get(0).getMessage());
        assertEquals(m2, contents.get(1).getMessage());
    }

    @Test
    public void testOfferWithTruncation()
    {
        RepairLog rl = new RepairLog();

        // add m1
        VoltMessage m1 = truncInitMsg(0L, 1L);
        rl.deliver(m1);
        assertEquals(1, rl.contents().size());

        // add m2
        VoltMessage m2 = truncInitMsg(0L, 2L);
        rl.deliver(m2);
        assertEquals(2, rl.contents().size());

        // trim m1. add m3
        VoltMessage m3 = truncInitMsg(1L, 3L);
        rl.deliver(m3);
        assertEquals(2, rl.contents().size());
        assertEquals(m2, rl.contents().get(0).getMessage());
        assertEquals(2L, rl.contents().get(0).getSpHandle());
        assertEquals(m3, rl.contents().get(1).getMessage());
        assertEquals(3L, rl.contents().get(1).getSpHandle());

    }

    @Test
    public void testOfferUneededMessage()
    {
        RepairLog rl = new RepairLog();
        VoltMessage m1 = truncInitMsg(0L, 1L);
        rl.deliver(m1);
        // deliver a non-logged message (this is the test).
        rl.deliver(new FooMessage());
        VoltMessage m2 = truncInitMsg(0L, 2L);
        rl.deliver(m2);
        assertEquals(2, rl.contents().size());
        assertEquals(m1, rl.contents().get(0).getMessage());
        assertEquals(m2, rl.contents().get(1).getMessage());
    }

}
