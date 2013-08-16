/* This file is part of VoltDB.
 * Copyright (C) 2008-2013 VoltDB Inc.
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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import junit.framework.TestCase;

import org.junit.Test;
import org.voltcore.messaging.TransactionInfoBaseMessage;
import org.voltcore.messaging.VoltMessage;
import org.voltdb.messaging.CompleteTransactionMessage;
import org.voltdb.messaging.FragmentTaskMessage;
import org.voltdb.messaging.Iv2InitiateTaskMessage;
import org.voltdb.messaging.Iv2RepairLogResponseMessage;

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

    VoltMessage truncFragMsg(long truncPt, long mpTxnId)
    {
        FragmentTaskMessage msg = mock(FragmentTaskMessage.class);
        when(msg.getTxnId()).thenReturn(mpTxnId);
        when(msg.getTruncationHandle()).thenReturn(truncPt);
        return msg;
    }

    VoltMessage truncCompleteMsg(long truncPt, long mpTxnId)
    {
        CompleteTransactionMessage msg = mock(CompleteTransactionMessage.class);
        when(msg.getTxnId()).thenReturn(mpTxnId);
        when(msg.getTruncationHandle()).thenReturn(truncPt);
        return msg;
    }

    // a message that should never be logged.
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

        List<Iv2RepairLogResponseMessage> contents = rl.contents(1l, false);
        assertEquals(3, contents.size());
        assertEquals(m1, contents.get(1).getPayload());
        assertEquals(m2, contents.get(2).getPayload());
    }

    @Test
    public void testOfferWithTruncation()
    {
        RepairLog rl = new RepairLog();

        // add m1
        VoltMessage m1 = truncInitMsg(0L, 1L);
        rl.deliver(m1);
        assertEquals(2, rl.contents(1L, false).size());

        // add m2
        VoltMessage m2 = truncInitMsg(0L, 2L);
        rl.deliver(m2);
        assertEquals(3, rl.contents(1L, false).size());

        // trim m1. add m3
        VoltMessage m3 = truncInitMsg(1L, 3L);
        rl.deliver(m3);
        assertEquals(3, rl.contents(1L, false).size());
        assertEquals(m2, rl.contents(1L, false).get(1).getPayload());
        assertEquals(2L, rl.contents(1L, false).get(1).getHandle());
        assertEquals(m3, rl.contents(1L, false).get(2).getPayload());
        assertEquals(3L, rl.contents(1L, false).get(2).getHandle());

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
        assertEquals(3, rl.contents(1L, false).size());
        assertEquals(m1, rl.contents(1L, false).get(1).getPayload());
        assertEquals(m2, rl.contents(1L, false).get(2).getPayload());
    }

    @Test
    public void testOfferFragmentTaskMessage()
    {
        RepairLog rl = new RepairLog();

        // trunc(trunc point, txnId).
        VoltMessage m1 = truncFragMsg(0L, 1L);
        rl.deliver(m1);
        assertEquals(2, rl.contents(1L, false).size());

        VoltMessage m2 = truncFragMsg(0L, 2L);
        rl.deliver(m2);
        assertEquals(3, rl.contents(1L, false).size());

        // only the first message for a transaction is logged.
        VoltMessage m2b = truncFragMsg(0L, 2L);
        rl.deliver(m2b);
        assertEquals(3, rl.contents(1L, false).size());

        // trim m1. add m3
        VoltMessage m3 = truncFragMsg(1L, 3L);
        rl.deliver(m3);
        assertEquals(3, rl.contents(1L, false).size());
        assertEquals(m2, rl.contents(1L, false).get(1).getPayload());
        assertEquals(2L, rl.contents(1L, false).get(1).getTxnId());
        assertEquals(m3, rl.contents(1L, false).get(2).getPayload());
        assertEquals(3L, rl.contents(1L, false).get(2).getTxnId());
    }

    @Test
    public void testOfferCompleteMessage()
    {
        RepairLog rl = new RepairLog();

        // trunc(trunc point, txnId).
        VoltMessage m1 = truncCompleteMsg(0L, 1L);
        rl.deliver(m1);
        assertEquals(2, rl.contents(1L, false).size());

        VoltMessage m2 = truncCompleteMsg(0L, 2L);
        rl.deliver(m2);
        assertEquals(3, rl.contents(1L, false).size());

        // trim m1. add m3
        VoltMessage m3 = truncCompleteMsg(1L, 3L);
        rl.deliver(m3);
        assertEquals(3, rl.contents(1L, false).size());
        assertEquals(m2, rl.contents(1L, false).get(1).getPayload());
        assertEquals(2L, rl.contents(1L, false).get(1).getTxnId());
        assertEquals(m3, rl.contents(1L, false).get(2).getPayload());
        assertEquals(3L, rl.contents(1L, false).get(2).getTxnId());
    }


    @Test
    public void testTruncationAfterPromotion()
    {
        RepairLog rl = new RepairLog();
        VoltMessage m1 = truncInitMsg(0L, 1L);
        rl.deliver(m1);
        VoltMessage m2 = truncInitMsg(0L, 2L);
        rl.deliver(m2);
        assertEquals(3, rl.contents(1L, false).size());
        rl.setLeaderState(true);
        assertEquals(1, rl.contents(1L, false).size());
    }

    // validate the invariants on the RepairLog contents:
    // Every entry in the log should have a unique, constantly increasing SP handle.
    // There should be only one FragmentTaskMessage per MP TxnID
    // There should be at most one FragmentTaskMessage uncovered by a CompleteTransactionMessage
    // There should be no CompleteTransactionMessages indicating restart
    private void validateRepairLog(List<Iv2RepairLogResponseMessage> stuff)
    {
        long prevHandle = Long.MIN_VALUE;
        Long mpTxnId = null;
        for (Iv2RepairLogResponseMessage imsg : stuff) {
            if (imsg.getSequence() > 0) {
                assertTrue(imsg.getHandle() > prevHandle);
                prevHandle = imsg.getHandle();
                if (imsg.getPayload() instanceof FragmentTaskMessage) {
                    assertEquals(null, mpTxnId);
                    mpTxnId = imsg.getTxnId();
                } else if (imsg.getPayload() instanceof CompleteTransactionMessage) {
                    // can see bare CompleteTransactionMessage, but if we've got an MP
                    // in progress this should close it
                    assertFalse(((CompleteTransactionMessage)imsg.getPayload()).isRestart());
                    if (mpTxnId != null) {
                        assertEquals((long)mpTxnId, imsg.getTxnId());
                    }
                    mpTxnId = null;
                }
            }
        }
    }

    @Test
    public void testFuzz()
    {
        TxnEgo sphandle = TxnEgo.makeZero(0);
        sphandle = sphandle.makeNext();
        RandomMsgGenerator msgGen = new RandomMsgGenerator();
        RepairLog dut = new RepairLog();
        for (int i = 0; i < 4000; i++) {
            // get next message, update the sphandle according to SpScheduler rules,
            // but only submit messages that would have been forwarded by the master
            // to the repair log.
            TransactionInfoBaseMessage msg = msgGen.generateRandomMessageInStream();
            msg.setSpHandle(sphandle.getTxnId());
            sphandle = sphandle.makeNext();
            if (!msg.isReadOnly() || msg instanceof CompleteTransactionMessage) {
                dut.deliver(msg);
            }
        }
        List<Iv2RepairLogResponseMessage> stuff = dut.contents(1l, false);
        validateRepairLog(stuff);
        // Also check the MP version
        stuff = dut.contents(1l, true);
        validateRepairLog(stuff);
    }
}
