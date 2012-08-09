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
            return new TxnEgo(sequence, partitionId).getSequence();
        }
    }

    Iv2RepairLogResponseMessage makeFragResponse(long handle)
    {
        FragmentTaskMessage frag = mock(FragmentTaskMessage.class);
        Iv2RepairLogResponseMessage m = mock(Iv2RepairLogResponseMessage.class);
        when(m.getPayload()).thenReturn(frag);
        when(m.getHandle()).thenReturn(handle);
        return m;
    }


    Iv2RepairLogResponseMessage makeCompleteResponse(long handle)
    {
        CompleteTransactionMessage complete = mock(CompleteTransactionMessage.class);
        Iv2RepairLogResponseMessage m = mock(Iv2RepairLogResponseMessage.class);
        when(m.getPayload()).thenReturn(complete);
        when(m.getHandle()).thenReturn(handle);
        return m;
    }

    Iv2RepairLogResponseMessage makeRealCompleteResponse(long requestId,
            long sourceHSId, int sequence, int ofTotal, long handle)
    {
        CompleteTransactionMessage complete = mock(CompleteTransactionMessage.class);
        Iv2RepairLogResponseMessage m = new Iv2RepairLogResponseMessage(requestId, sequence,
            ofTotal, handle, complete);
        m.m_sourceHSId = sourceHSId;
        return m;
    }


    Iv2RepairLogResponseMessage makeRealAckResponse(long requestId,
            long sourceHSId, int sequence, int ofTotal, long handle)
    {
        assertEquals(0, sequence);
        Iv2RepairLogResponseMessage m = new Iv2RepairLogResponseMessage(requestId, sequence,
            ofTotal, handle, null);
        m.m_sourceHSId = sourceHSId;
        return m;
    }


    Iv2RepairLogResponseMessage makeRealFragResponse(long requestId,
            long sourceHSId, int sequence, int ofTotal, long handle)
    {
        FragmentTaskMessage frag = mock(FragmentTaskMessage.class);
        Iv2RepairLogResponseMessage m = new Iv2RepairLogResponseMessage(requestId, sequence,
            ofTotal, handle, frag);
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
            System.out.println("Comparing " + li.getHandle() + " to expected " + expectedUnion[i] + "SEQ 0 is: " + TxnEgo.SEQUENCE_ZERO + " shifted zero: " + (TxnEgo.SEQUENCE_ZERO << 14));
            assertEquals(li.getHandle(), expectedUnion[i]);
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

        // verify exactly 1 repair happened.
        List<Long> needsRepair = new ArrayList<Long>();
        needsRepair.add(1L);
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

        // First, repair 3
        List<Long> needsRepair = new ArrayList<Long>();
        needsRepair.add(3L);
        inOrder.verify(mailbox).repairReplicasWith(eq(needsRepair), any(Iv2RepairLogResponseMessage.class));
        needsRepair.clear();
        needsRepair.add(2L);
        needsRepair.add(3L);
        inOrder.verify(mailbox).repairReplicasWith(eq(needsRepair), any(Iv2RepairLogResponseMessage.class));
        needsRepair.clear();
        needsRepair.add(1L);
        needsRepair.add(2L);
        needsRepair.add(3L);
        inOrder.verify(mailbox).repairReplicasWith(eq(needsRepair), any(Iv2RepairLogResponseMessage.class));

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
        assertEquals(txnEgo(0L), (long)real_result.getSecond());
    }






/*
    // verify that bad request ids are not submitted to the log.
    @Test
    public void testStaleResponse() throws Exception
    {
        MpPromoteAlgo term = new SpPromoteAlgo(null, null, 0, null, VoltZK.iv2masters, "Test");
        term.deliver(makeStaleResponse(1L, term.getRequestId() + 1));
        assertEquals(0L, term.m_repairLogUnion.size());
    }



    // verify that the all-done logic works on replica repair structs
    @Test
    public void testRepairLogsAreComplete()
    {
        MpPromoteAlgo term = new SpPromoteAlgo(null, null, 0, null, VoltZK.iv2masters, "Test");
        MpPromoteAlgo.ReplicaRepairStruct notDone1 = new SpPromoteAlgo.ReplicaRepairStruct();
        notDone1.m_receivedResponses = 1;
        notDone1.m_expectedResponses = 2;
        assertFalse(notDone1.logsComplete());

        MpPromoteAlgo.ReplicaRepairStruct notDone2 = new SpPromoteAlgo.ReplicaRepairStruct();
        notDone2.m_receivedResponses = 0;
        notDone2.m_expectedResponses = 10;

        MpPromoteAlgo.ReplicaRepairStruct done1 = new SpPromoteAlgo.ReplicaRepairStruct();
        done1.m_receivedResponses = 5;
        done1.m_expectedResponses = 5;
        assertTrue(done1.logsComplete());

        MpPromoteAlgo.ReplicaRepairStruct done2 = new SpPromoteAlgo.ReplicaRepairStruct();
        done2.m_receivedResponses = 10;
        done2.m_expectedResponses = 10;

        term.m_replicaRepairStructs.put(0L, notDone1);
        term.m_replicaRepairStructs.put(1L, done1);
        term.m_replicaRepairStructs.put(2L, notDone2);
        term.m_replicaRepairStructs.put(3L, done2);
        assertFalse(term.areRepairLogsComplete());

        term.m_replicaRepairStructs.clear();
        term.m_replicaRepairStructs.put(2L, notDone2);
        assertFalse(term.areRepairLogsComplete());

        term.m_replicaRepairStructs.clear();
        term.m_replicaRepairStructs.put(1L, done1);
        term.m_replicaRepairStructs.put(3L, done2);
        assertTrue(term.areRepairLogsComplete());

        term.m_replicaRepairStructs.clear();
        term.m_replicaRepairStructs.put(3L, done2);
        assertTrue(term.areRepairLogsComplete());
    }


    // be a little a paranoid about order. note that the union test also verifies
    // order; unsure this is interesting... This test is on one replica because
    // I'm not sure that we guarentee the orer of iteration across replicas.
    @SuppressWarnings("unchecked")
    @Test
    public void testRepairSurvivorOrder()
    {
        InitiatorMailbox mailbox = mock(InitiatorMailbox.class);
        InOrder inOrder = inOrder(mailbox);

        MpPromoteAlgo term = new SpPromoteAlgo(null, mock(ZooKeeper.class), 0, mailbox, VoltZK.iv2masters, "Test");

        // missing 3, 4, 5
        MpPromoteAlgo.ReplicaRepairStruct r3 = new SpPromoteAlgo.ReplicaRepairStruct();
        r3.m_maxSpHandleSeen = 2L;

        term.m_replicaRepairStructs.put(3L, r3);

        // build the log
        long spHandles[] = new long[]{0L, 1L, 2L, 3L, 4L, 5L};
        Iv2RepairLogResponseMessage msgs[] = new Iv2RepairLogResponseMessage[6];
        for (int i=1; i < spHandles.length; ++i) {
            msgs[i] = makeResponse(spHandles[i]);
            term.m_repairLogUnion.add(msgs[i]);
        }

        // call the function being tested...
        term.repairSurvivors();

        List<Long> repair3 = new ArrayList<Long>();
        repair3.add(3L);

        // verify that r3 saw 3, 4, 5
        inOrder.verify(mailbox).repairReplicasWith(repair3, msgs[3]);
        inOrder.verify(mailbox).repairReplicasWith(repair3, msgs[4]);
        inOrder.verify(mailbox).repairReplicasWith(repair3, msgs[5]);

        // verify exactly 3 repairs happened.
        verify(mailbox, times(3)).repairReplicasWith(any(repair3.getClass()), any(Iv2RepairLogResponseMessage.class));
    }

    // Verify that a babysitter update causes the term to be cancelled.
    @Test
    public void testMidPromotionReplicaUpdate() throws Exception
    {
        final AtomicBoolean promotionResult = new AtomicBoolean(true);
        final InitiatorMailbox mailbox = mock(InitiatorMailbox.class);

        // Stub some portions of a concrete Term instance - this is the
        // object being tested.
        final MpPromoteAlgo term = new SpPromoteAlgo(null, mock(ZooKeeper.class), 0, mailbox, VoltZK.iv2masters, "Test") {
            // there aren't replicas to ask for repair logs
            @Override
            void prepareForFaultRecovery() {
            }

        };

        Thread promotionThread = new Thread() {
            @Override
            public void run() {
                try {
                    promotionResult.set(term.start().get().getFirst());
                } catch (Exception e) {
                    System.out.println("Promotion thread threw: " + e);
                    throw new RuntimeException(e);
                }
            }
        };
        promotionThread.start();

        // cancel the term as if updateReplica() triggered.
        term.cancel();
        promotionThread.join();

        // promotion success must be false after cancel.
        assertFalse(promotionResult.get());
    }
*/

}

