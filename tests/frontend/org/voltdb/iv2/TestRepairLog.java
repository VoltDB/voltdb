/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
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

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.BeforeClass;
import org.junit.Test;
import org.voltcore.messaging.TransactionInfoBaseMessage;
import org.voltcore.messaging.VoltMessage;
import org.voltcore.utils.Pair;
import org.voltdb.ElasticHashinator;
import org.voltdb.ParameterSet;
import org.voltdb.StoredProcedureInvocation;
import org.voltdb.TheHashinator;
import org.voltdb.messaging.CompleteTransactionMessage;
import org.voltdb.messaging.FragmentTaskMessage;
import org.voltdb.messaging.Iv2InitiateTaskMessage;
import org.voltdb.messaging.Iv2RepairLogResponseMessage;
import org.voltdb.messaging.RepairLogTruncationMessage;

public class TestRepairLog
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

    VoltMessage truncMsg(long spHandle)
    {
        return new RepairLogTruncationMessage(spHandle);
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

    @BeforeClass
    static public void initializeHashinator() {
        TheHashinator.initialize(ElasticHashinator.class, TheHashinator.getConfigureBytes(8));
    }

    @Test
    public void testOffer()
    {
        final AtomicLong lastCommitted = new AtomicLong(Long.MIN_VALUE);
        final TransactionCommitInterest interest = lastCommitted::set;

        // offer some various messages to log and check
        // that it keeps the expected ones.
        RepairLog rl = new RepairLog();
        rl.registerTransactionCommitInterest(interest);
        VoltMessage m1 = nonTruncInitMsg();
        VoltMessage m2 = nonTruncInitMsg();

        rl.deliver(m1);
        rl.deliver(m2);

        List<Iv2RepairLogResponseMessage> contents = rl.contents(1l, false);
        assertEquals(3, contents.size());
        assertEquals(m1, contents.get(1).getPayload());
        assertEquals(m2, contents.get(2).getPayload());
        assertEquals(Long.MIN_VALUE, lastCommitted.get());
    }

    @Test
    public void testOfferWithTruncation()
    {
        final AtomicLong lastCommitted = new AtomicLong(Long.MIN_VALUE);
        final TransactionCommitInterest interest = lastCommitted::set;
        RepairLog rl = new RepairLog();
        rl.registerTransactionCommitInterest(interest);

        // add m1
        VoltMessage m1 = truncInitMsg(0L, 1L);
        rl.deliver(m1);
        assertEquals(2, rl.contents(1L, false).size());
        assertEquals(0, lastCommitted.get());

        // add m2
        VoltMessage m2 = truncInitMsg(0L, 2L);
        rl.deliver(m2);
        assertEquals(3, rl.contents(1L, false).size());
        assertEquals(0, lastCommitted.get());

        // trim m1. add m3
        VoltMessage m3 = truncInitMsg(1L, 3L);
        rl.deliver(m3);
        assertEquals(3, rl.contents(1L, false).size());
        assertEquals(m2, rl.contents(1L, false).get(1).getPayload());
        assertEquals(2L, rl.contents(1L, false).get(1).getHandle());
        assertEquals(m3, rl.contents(1L, false).get(2).getPayload());
        assertEquals(3L, rl.contents(1L, false).get(2).getHandle());
        assertEquals(1, lastCommitted.get());

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
        final AtomicLong lastCommitted = new AtomicLong(Long.MIN_VALUE);
        final TransactionCommitInterest interest = lastCommitted::set;
        RepairLog rl = new RepairLog();
        rl.registerTransactionCommitInterest(interest);

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
        assertEquals(Long.MIN_VALUE, lastCommitted.get());
    }

    @Test
    public void testOfferCompleteMessage()
    {
        final AtomicLong lastCommitted = new AtomicLong(Long.MIN_VALUE);
        final TransactionCommitInterest interest = lastCommitted::set;
        RepairLog rl = new RepairLog();
        rl.registerTransactionCommitInterest(interest);

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
        assertEquals(Long.MIN_VALUE, lastCommitted.get());
    }


    @Test
    public void testTruncationAfterPromotion()
    {
        final AtomicLong lastCommitted = new AtomicLong(Long.MIN_VALUE);
        final TransactionCommitInterest interest = lastCommitted::set;
        RepairLog rl = new RepairLog();
        rl.registerTransactionCommitInterest(interest);
        VoltMessage m1 = truncInitMsg(0L, 1L);
        rl.deliver(m1);
        VoltMessage m2 = truncInitMsg(0L, 2L);
        rl.deliver(m2);
        assertEquals(3, rl.contents(1L, false).size());
        rl.setLeaderState(true);
        assertEquals(1, rl.contents(1L, false).size());
        assertEquals(2, lastCommitted.get());
    }

    // validate the invariants on the RepairLog contents:
    // Every entry in the log should have a unique, constantly increasing SP handle.
    // There should be only one FragmentTaskMessage per MP TxnID
    // There should be at most one FragmentTaskMessage uncovered by a CompleteTransactionMessage
    // There should be no CompleteTransactionMessages indicating restart
    private void validateRepairLog(List<Iv2RepairLogResponseMessage> stuff, long binaryLogSpUniqueId, long binaryLogMpUniqueId)
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
            } else {
                assertTrue(imsg.hasHashinatorConfig());
            }
        }
    }

    public static Pair<Long, Long> setBinaryLogUniqueId(TransactionInfoBaseMessage msg, UniqueIdGenerator spbuig, UniqueIdGenerator mpbuig) {
        Iv2InitiateTaskMessage taskMsg = null;
        if (msg instanceof Iv2InitiateTaskMessage) {
            taskMsg = (Iv2InitiateTaskMessage) msg;
        } else if (msg instanceof FragmentTaskMessage) {
            taskMsg = ((FragmentTaskMessage) msg).getInitiateTask();
        }

        if (taskMsg != null && taskMsg.getStoredProcedureName().startsWith("@ApplyBinaryLog")) {
            ParameterSet params = taskMsg.getStoredProcedureInvocation().getParams();
            long spuid = spbuig == null?0:spbuig.getNextUniqueId();
            long mpuid = mpbuig.getNextUniqueId();
            when(params.toArray()).thenReturn(new Object[] {null, 0l, 0l, spuid, mpuid, null});
            return Pair.of(spuid, mpuid);
        }

        return Pair.of(Long.MIN_VALUE, Long.MIN_VALUE);
    }

    @Test
    public void testFuzz()
    {
        TxnEgo sphandle = TxnEgo.makeZero(0);
        UniqueIdGenerator spbuig = new UniqueIdGenerator(0, 0);
        UniqueIdGenerator mpbuig = new UniqueIdGenerator(0, 0);
        sphandle = sphandle.makeNext();
        RandomMsgGenerator msgGen = new RandomMsgGenerator();
        RepairLog dut = new RepairLog();
        long spBinaryLogSpUniqueId = Long.MIN_VALUE;
        long spBinaryLogMpUniqueId = Long.MIN_VALUE;
        long mpBinaryLogMpUniqueId = Long.MIN_VALUE;
        for (int i = 0; i < 4000; i++) {
            // get next message, update the sphandle according to SpScheduler rules,
            // but only submit messages that would have been forwarded by the master
            // to the repair log.
            TransactionInfoBaseMessage msg = msgGen.generateRandomMessageInStream();
            msg.setSpHandle(sphandle.getTxnId());
            if (msg instanceof Iv2InitiateTaskMessage) {
                Pair<Long, Long> uids = setBinaryLogUniqueId(msg, spbuig, mpbuig);
                spBinaryLogSpUniqueId = Math.max(spBinaryLogSpUniqueId, uids.getFirst());
                spBinaryLogMpUniqueId = Math.max(spBinaryLogMpUniqueId, uids.getSecond());
            } else if (msg instanceof FragmentTaskMessage) {
                mpBinaryLogMpUniqueId = Math.max(mpBinaryLogMpUniqueId, setBinaryLogUniqueId(msg, null, mpbuig).getSecond());
            }
            sphandle = sphandle.makeNext();
            if (!msg.isReadOnly() || msg instanceof CompleteTransactionMessage) {
                dut.deliver(msg);
            }
        }
        List<Iv2RepairLogResponseMessage> stuff = dut.contents(1l, false);
        validateRepairLog(stuff, spBinaryLogSpUniqueId, spBinaryLogMpUniqueId);
        // Also check the MP version
        stuff = dut.contents(1l, true);
        validateRepairLog(stuff, Long.MIN_VALUE, mpBinaryLogMpUniqueId);
    }

    @Test
    public void testComparator()
    {
        RepairLog dut = new RepairLog();
        Random rand = new Random();
        List<RepairLog.Item> items = new ArrayList<RepairLog.Item>();
        for (int i = 0; i < 1000000; i++) {
            RepairLog.Item item = new RepairLog.Item(true, null, rand.nextInt(), i);
            items.add(item);
        }
        Collections.sort(items, dut.m_handleComparator);
    }

    @Test
    public void testTrackBinaryLogUniqueId() {
        // The end unique id for an @ApplyBinaryLogSP invocation is recorded
        // as its fifth parameter. Create a realistic invocation, deliver it
        // to the repair log, and see what we get
        final long endSpUniqueId = 42;
        final long endMpUniqueId = 25;
        StoredProcedureInvocation spi = new StoredProcedureInvocation();
        spi.setProcName("@ApplyBinaryLogSP");
        spi.setParams(0, endSpUniqueId - 10, endSpUniqueId, endSpUniqueId, endMpUniqueId, new byte[]{0});

        Iv2InitiateTaskMessage msg =
                new Iv2InitiateTaskMessage(0l, 0l, 0l, Long.MIN_VALUE, 0l, false, true, false,
                        spi, 0l, 0l, false);
        msg.setSpHandle(900l);
        RepairLog log = new RepairLog();
        log.deliver(msg);
        validateRepairLog(log.contents(1l, false), endSpUniqueId, endMpUniqueId);
    }

    @Test
    public void testTruncationWithInterest()
    {
        final AtomicLong lastCommitted = new AtomicLong(Long.MIN_VALUE);
        final TransactionCommitInterest interest = lastCommitted::set;
        final RepairLog dut = new RepairLog();
        dut.registerTransactionCommitInterest(interest);
        assertEquals(Long.MIN_VALUE, lastCommitted.get());

        for (int i = 0; i < 10; i++) {
            dut.deliver(truncMsg(i));
            assertEquals(i, lastCommitted.get());
        }
    }
}
