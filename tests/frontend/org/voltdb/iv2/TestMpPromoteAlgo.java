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

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import junit.framework.TestCase;

import org.junit.Test;
import org.mockito.InOrder;
import org.voltcore.utils.Pair;
import org.voltdb.messaging.CompleteTransactionMessage;
import org.voltdb.messaging.FragmentTaskMessage;
import org.voltdb.messaging.Iv2RepairLogRequestMessage;
import org.voltdb.messaging.Iv2RepairLogResponseMessage;

public class TestMpPromoteAlgo extends TestCase
{
    long txnEgo(long handle)
    {
        // repair log uses long.max_value in the header response
        // to signify an empty repair log. this is fragile and
        // should be improved.
        if (handle == Long.MAX_VALUE) {
            return handle;
        } else {
            int partitionId = 100;
            long sequence = TxnEgo.SEQUENCE_ZERO + handle;
            return new TxnEgo(sequence, partitionId).getTxnId();
        }
    }

    Iv2RepairLogResponseMessage makeFragResponse(long handle)
    {
        FragmentTaskMessage frag = mock(FragmentTaskMessage.class);
        Iv2RepairLogResponseMessage m = mock(Iv2RepairLogResponseMessage.class);
        when(m.getPayload()).thenReturn(frag);
        when(m.getHandle()).thenReturn(-1L);
        when(m.getTxnId()).thenReturn(handle);
        return m;
    }


    Iv2RepairLogResponseMessage makeCompleteResponse(long handle)
    {
        CompleteTransactionMessage complete = mock(CompleteTransactionMessage.class);
        Iv2RepairLogResponseMessage m = mock(Iv2RepairLogResponseMessage.class);
        when(m.getPayload()).thenReturn(complete);
        when(m.getHandle()).thenReturn(-1L);
        when(m.getTxnId()).thenReturn(handle);
        return m;
    }

    Iv2RepairLogResponseMessage makeRealCompleteResponse(long requestId,
            long sourceHSId, int sequence, int ofTotal, long handle)
    {
        CompleteTransactionMessage complete = mock(CompleteTransactionMessage.class);
        Iv2RepairLogResponseMessage m = new Iv2RepairLogResponseMessage(requestId, sequence,
            ofTotal, handle, handle, complete);
        m.m_sourceHSId = sourceHSId;
        return m;
    }


    Iv2RepairLogResponseMessage makeRealAckResponse(long requestId,
            long sourceHSId, int sequence, int ofTotal, long handle)
    {
        assertEquals(0, sequence);
        Iv2RepairLogResponseMessage m = new Iv2RepairLogResponseMessage(requestId, sequence,
            ofTotal, handle, handle, null);
        m.m_sourceHSId = sourceHSId;
        return m;
    }


    Iv2RepairLogResponseMessage makeRealFragResponse(long requestId,
            long sourceHSId, int sequence, int ofTotal, long handle)
    {
        FragmentTaskMessage frag = mock(FragmentTaskMessage.class);
        Iv2RepairLogResponseMessage m = new Iv2RepairLogResponseMessage(requestId, sequence,
            ofTotal, handle, handle, frag);
        m.m_sourceHSId = sourceHSId;
        return m;
    }


//    Iv2RepairLogResponseMessage makeStaleResponse(long handle, long requestId)
//    {
//        Iv2RepairLogResponseMessage m = makeFragResponse(handle);
//        when(m.getRequestId()).thenReturn(requestId);
//        return m;
//    }

    // verify that responses are correctly unioned and ordered.
    @Test
    public void testUnion() throws Exception
    {
        MpPromoteAlgo algo = new MpPromoteAlgo(new ArrayList<Long>(), null, "Test");

        // returned handles in a non-trivial order, with duplicates.
        // txns 1-5 are complete. 6 is not complete.
        // txn 5 returns frag(s) and complete(s).
        final Boolean t = true; final Boolean f = false;
        long returnedHandles[]  = new long[]{txnEgo(1L), txnEgo(5L), txnEgo(2L), txnEgo(5L), txnEgo(6L), txnEgo(3L), txnEgo(5L), txnEgo(1L)};
        boolean isComplete[] = new boolean[]{t,  f,  t,  t,  f,  t,  f,  t};

        long expectedUnion[] = new long[]{txnEgo(1L), txnEgo(2L), txnEgo(3L), txnEgo(5L), txnEgo(6L)};
        boolean expectComp[] = new boolean[]{t, t, t, t, f};

        Iv2RepairLogResponseMessage makeCompleteResponse = makeCompleteResponse(returnedHandles[0]);
        System.out.println("txnEgo: " + returnedHandles[0] + " m.handle(): " + makeCompleteResponse.getHandle());

        for (int ii=0; ii < isComplete.length; ii++) {
            if (isComplete[ii]) {
                algo.addToRepairLog(makeCompleteResponse(returnedHandles[ii]));
            }
            else {
                algo.addToRepairLog(makeFragResponse(returnedHandles[ii]));
            }
        }

        // assert one log entry per transaction and that complete trumped frag.
        assertEquals(expectedUnion.length, algo.m_repairLogUnion.size());
        int i = 0;
        for (Iv2RepairLogResponseMessage li : algo.m_repairLogUnion) {
            System.out.println("Comparing " + li.getHandle() + " to expected " + expectedUnion[i] + "SEQ 0 is: " + TxnEgo.makeZero(0).getTxnId() + " shifted zero: " + (TxnEgo.makeZero(0).getTxnId() << 14));
            assertEquals(li.getTxnId(), expectedUnion[i]);
            if (expectComp[i]) {
                assertTrue(li.getPayload() instanceof CompleteTransactionMessage);
            }
            else {
                assertTrue(li.getPayload() instanceof FragmentTaskMessage);
            }
            i++;
        }
    }


    // verify that algo asks initMailbox to send the expected repair messages.
    @Test
    public void testRepairSurvivors() throws InterruptedException, ExecutionException
    {
        InitiatorMailbox mailbox = mock(InitiatorMailbox.class);
        ArrayList<Long> masters = new ArrayList<Long>();
        masters.add(1L);
        masters.add(2L);
        masters.add(3L);

        MpPromoteAlgo algo = new MpPromoteAlgo(masters, mailbox, "Test");
        long requestId = algo.getRequestId();
        Future<Pair<Boolean, Long>> result = algo.start();
        verify(mailbox, times(1)).send(any(long[].class), any(Iv2RepairLogRequestMessage.class));

        // has a frag for txn 1000. MP handle is 1000L
        algo.deliver(makeRealAckResponse(requestId, 1L, 0, 2, txnEgo(1000L)));
        algo.deliver(makeRealFragResponse(requestId, 1L, 1, 2, txnEgo(1000L)));

        // has only the normal ack. Never saw an MP transaction.
        algo.deliver(makeRealAckResponse(requestId, 2L, 0, 1, Long.MAX_VALUE));

        // also has a complete. MP handle is 1000L
        algo.deliver(makeRealAckResponse(requestId, 3L, 0, 3, txnEgo(1000L)));
        algo.deliver(makeRealFragResponse(requestId, 3L, 1, 3, txnEgo(1000L)));
        algo.deliver(makeRealCompleteResponse(requestId, 3L, 2, 3, txnEgo(1000L)));

        // Verify that we send a complete to every site.
        List<Long> needsRepair = new ArrayList<Long>();
        needsRepair.add(1L);
        needsRepair.add(2L);
        needsRepair.add(3L);
        verify(mailbox, times(1)).repairReplicasWith(eq(needsRepair), any(Iv2RepairLogResponseMessage.class));
        Pair<Boolean, Long> real_result = result.get();
        assertEquals(txnEgo(1000L), (long)real_result.getSecond());
    }

    @Test
    public void testSlowDieOff() throws InterruptedException, ExecutionException
    {
        InitiatorMailbox mailbox = mock(InitiatorMailbox.class);
        InOrder inOrder = inOrder(mailbox);
        ArrayList<Long> masters = new ArrayList<Long>();
        masters.add(1L);
        masters.add(2L);
        masters.add(3L);

        MpPromoteAlgo algo = new MpPromoteAlgo(masters, mailbox, "Test");
        long requestId = algo.getRequestId();
        Future<Pair<Boolean, Long>> result = algo.start();

        // Master 1
        // First, everyone completed
        // has a frag for txn 1000. MP handle is 1000L
        algo.deliver(makeRealAckResponse(requestId,      1L, 0, 8, txnEgo(1000L)));
        algo.deliver(makeRealFragResponse(requestId,     1L, 1, 8, txnEgo(1000L)));
        algo.deliver(makeRealCompleteResponse(requestId, 1L, 2, 8, txnEgo(1000L)));
        // Second, 3 will lose complete
        algo.deliver(makeRealFragResponse(requestId,     1L, 3, 8, txnEgo(1001L)));
        algo.deliver(makeRealCompleteResponse(requestId, 1L, 4, 8, txnEgo(1001L)));
        // Third, 2 will lose complete and 3 has nothing
        algo.deliver(makeRealFragResponse(requestId,     1L, 5, 8, txnEgo(1002L)));
        algo.deliver(makeRealCompleteResponse(requestId, 1L, 6, 8, txnEgo(1002L)));
        // Fourth, 1 just has a fragment, the other two are gone.
        algo.deliver(makeRealFragResponse(requestId,     1L, 7, 8, txnEgo(1003L)));

        // Master 2
        // has only the normal ack. Never saw an MP transaction.
        algo.deliver(makeRealAckResponse(requestId,      2L, 0, 6, txnEgo(1000L)));
        algo.deliver(makeRealFragResponse(requestId,     2L, 1, 6, txnEgo(1000L)));
        algo.deliver(makeRealCompleteResponse(requestId, 2L, 2, 6, txnEgo(1000L)));
        // second, 3 loses complete
        algo.deliver(makeRealFragResponse(requestId,     2L, 3, 6, txnEgo(1001L)));
        algo.deliver(makeRealCompleteResponse(requestId, 2L, 4, 6, txnEgo(1001L)));
        // third, 2 (us) loses complete
        algo.deliver(makeRealFragResponse(requestId,     2L, 5, 6, txnEgo(1002L)));

        // Master 3
        // also has a complete. MP handle is 1000L
        algo.deliver(makeRealAckResponse(requestId,      3L, 0, 4, txnEgo(1000L)));
        algo.deliver(makeRealFragResponse(requestId,     3L, 1, 4, txnEgo(1000L)));
        algo.deliver(makeRealCompleteResponse(requestId, 3L, 2, 4, txnEgo(1000L)));
        // 3 loses complete
        algo.deliver(makeRealFragResponse(requestId,     3L, 3, 4, txnEgo(1001L)));

        // We should send to all hosts in all cases for all non-truncated MP txns now
        List<Long> needsRepair = new ArrayList<Long>();
        needsRepair.add(1L);
        needsRepair.add(2L);
        needsRepair.add(3L);
        inOrder.verify(mailbox, times(4)).repairReplicasWith(eq(needsRepair), any(Iv2RepairLogResponseMessage.class));

        Pair<Boolean, Long> real_result = result.get();
        assertEquals(txnEgo(1003L), (long)real_result.getSecond());
    }

    // verify correct txnID when no MP has ever been done
    @Test
    public void testSaneWithNoMP() throws InterruptedException, ExecutionException
    {
        InitiatorMailbox mailbox = mock(InitiatorMailbox.class);
        ArrayList<Long> masters = new ArrayList<Long>();
        masters.add(1L);
        masters.add(2L);
        masters.add(3L);

        MpPromoteAlgo algo = new MpPromoteAlgo(masters, mailbox, "Test");
        long requestId = algo.getRequestId();
        Future<Pair<Boolean, Long>> result = algo.start();
        verify(mailbox, times(1)).send(any(long[].class), any(Iv2RepairLogRequestMessage.class));

        // has only the normal ack. Never saw an MP transaction.
        algo.deliver(makeRealAckResponse(requestId, 1L, 0, 1, Long.MAX_VALUE));

        // has only the normal ack. Never saw an MP transaction.
        algo.deliver(makeRealAckResponse(requestId, 2L, 0, 1, Long.MAX_VALUE));

        // has only the normal ack. Never saw an MP transaction.
        algo.deliver(makeRealAckResponse(requestId, 3L, 0, 1, Long.MAX_VALUE));

        // verify that the discovered txn id is 0 (the correct starting txnid).
        Pair<Boolean, Long> real_result = result.get();
        assertEquals(TxnEgo.makeZero(MpInitiator.MP_INIT_PID).getTxnId(), (long)real_result.getSecond());
    }
}

