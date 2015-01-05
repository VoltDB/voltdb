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

package org.voltdb.iv2;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.InOrder;
import org.voltcore.messaging.TransactionInfoBaseMessage;
import org.voltcore.messaging.VoltMessage;
import org.voltcore.utils.Pair;
import org.voltdb.TheHashinator;
import org.voltdb.TheHashinator.HashinatorType;
import org.voltdb.messaging.CompleteTransactionMessage;
import org.voltdb.messaging.FragmentTaskMessage;
import org.voltdb.messaging.Iv2InitiateTaskMessage;
import org.voltdb.messaging.Iv2RepairLogRequestMessage;
import org.voltdb.messaging.Iv2RepairLogResponseMessage;

public class TestMpPromoteAlgo
{
    Pair<Long,byte[]> m_hashinatorConfig;

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
            long sourceHSId, int sequence, int ofTotal, long handle,
            Pair<Long,byte[]> versionedHashinatorConfig)
    {
        assertEquals(0, sequence);
        Iv2RepairLogResponseMessage m = new Iv2RepairLogResponseMessage(requestId,
            ofTotal, handle, handle, versionedHashinatorConfig);
        m.m_sourceHSId = sourceHSId;
        return m;
    }


    Iv2RepairLogResponseMessage makeRealFragResponse(long requestId,
            long sourceHSId, int sequence, int ofTotal, long handle)
    {
        FragmentTaskMessage frag = mock(FragmentTaskMessage.class);
        when(frag.getInitiateTask()).thenReturn(mock(Iv2InitiateTaskMessage.class));
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

    @BeforeClass
    static public void initializeHashinator() {
        TheHashinator.setConfiguredHashinatorType(HashinatorType.ELASTIC);
        TheHashinator.initialize(TheHashinator.getConfiguredHashinatorClass(), TheHashinator.getConfigureBytes(8));
    }

    @Before
    public void setUp() {
        m_hashinatorConfig = TheHashinator.getCurrentVersionedConfigCooked();
    }

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
        InitiatorMailbox mailbox = mock(MpInitiatorMailbox.class);
        doReturn(4L).when(mailbox).getHSId();
        ArrayList<Long> masters = new ArrayList<Long>();
        masters.add(1L);
        masters.add(2L);
        masters.add(3L);

        MpPromoteAlgo algo = new MpPromoteAlgo(masters, mailbox, "Test");
        long requestId = algo.getRequestId();
        Future<Long> result = algo.start();
        verify(mailbox, times(1)).send(any(long[].class), any(Iv2RepairLogRequestMessage.class));

        // has a frag for txn 1000. MP handle is 1000L
        algo.deliver(makeRealAckResponse(requestId, 1L, 0, 2, txnEgo(1000L), m_hashinatorConfig));
        algo.deliver(makeRealFragResponse(requestId, 1L, 1, 2, txnEgo(1000L)));

        // has only the normal ack. Never saw an MP transaction.
        algo.deliver(makeRealAckResponse(requestId, 2L, 0, 1, Long.MAX_VALUE, m_hashinatorConfig));

        // also has a complete. MP handle is 1000L
        // and deliver a newer version of the hashinator config
        Pair<Long,byte[]> torv3 = Pair.of(
                m_hashinatorConfig.getFirst()+1,
                m_hashinatorConfig.getSecond()
                );
        algo.deliver(makeRealAckResponse(requestId, 3L, 0, 3, txnEgo(1000L), torv3));
        algo.deliver(makeRealFragResponse(requestId, 3L, 1, 3, txnEgo(1000L)));
        algo.deliver(makeRealCompleteResponse(requestId, 3L, 2, 3, txnEgo(1000L)));

        // deliver the same complete from the MPI's repair log
        algo.deliver(makeRealAckResponse(requestId, 4L, 0, 2, txnEgo(1000L), m_hashinatorConfig));
        algo.deliver(makeRealCompleteResponse(requestId, 4L, 1, 2, txnEgo(1000L)));

        // Verify that we send a complete to every site.
        List<Long> needsRepair = new ArrayList<Long>();
        needsRepair.add(1L);
        needsRepair.add(2L);
        needsRepair.add(3L);
        verify(mailbox, times(1)).repairReplicasWith(eq(needsRepair), any(Iv2RepairLogResponseMessage.class));
        Long real_result = result.get();
        assertEquals(txnEgo(1000L), (long)real_result);

        // check if the hashinator was updated to the newer version
        assertEquals(torv3.getFirst(), TheHashinator.getCurrentVersionedConfig().getFirst());
    }

    @Test
    public void testSlowDieOff() throws InterruptedException, ExecutionException
    {
        InitiatorMailbox mailbox = mock(MpInitiatorMailbox.class);
        doReturn(4L).when(mailbox).getHSId();
        InOrder inOrder = inOrder(mailbox);
        ArrayList<Long> masters = new ArrayList<Long>();
        masters.add(1L);
        masters.add(2L);
        masters.add(3L);

        MpPromoteAlgo algo = new MpPromoteAlgo(masters, mailbox, "Test");
        long requestId = algo.getRequestId();
        Future<Long> result = algo.start();

        // Master 1
        // First, everyone completed
        // has a frag for txn 1000. MP handle is 1000L
        algo.deliver(makeRealAckResponse(requestId,      1L, 0, 8, txnEgo(1000L), m_hashinatorConfig));
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
        algo.deliver(makeRealAckResponse(requestId,      2L, 0, 6, txnEgo(1000L), m_hashinatorConfig));
        algo.deliver(makeRealFragResponse(requestId,     2L, 1, 6, txnEgo(1000L)));
        algo.deliver(makeRealCompleteResponse(requestId, 2L, 2, 6, txnEgo(1000L)));
        // second, 3 loses complete
        algo.deliver(makeRealFragResponse(requestId,     2L, 3, 6, txnEgo(1001L)));
        algo.deliver(makeRealCompleteResponse(requestId, 2L, 4, 6, txnEgo(1001L)));
        // third, 2 (us) loses complete
        algo.deliver(makeRealFragResponse(requestId,     2L, 5, 6, txnEgo(1002L)));

        // Master 3
        // also has a complete. MP handle is 1000L
        algo.deliver(makeRealAckResponse(requestId,      3L, 0, 4, txnEgo(1000L), m_hashinatorConfig));
        algo.deliver(makeRealFragResponse(requestId,     3L, 1, 4, txnEgo(1000L)));
        algo.deliver(makeRealCompleteResponse(requestId, 3L, 2, 4, txnEgo(1000L)));
        // 3 loses complete
        algo.deliver(makeRealFragResponse(requestId,     3L, 3, 4, txnEgo(1001L)));

        // MPI
        // Deliver the last complete
        algo.deliver(makeRealAckResponse(requestId, 4L, 0, 2, txnEgo(1002L), m_hashinatorConfig));
        algo.deliver(makeRealCompleteResponse(requestId, 4L, 1, 2, txnEgo(1002L)));

        // We should send to all hosts in all cases for all non-truncated MP txns now
        List<Long> needsRepair = new ArrayList<Long>();
        needsRepair.add(1L);
        needsRepair.add(2L);
        needsRepair.add(3L);
        inOrder.verify(mailbox, times(4)).repairReplicasWith(eq(needsRepair), any(Iv2RepairLogResponseMessage.class));

        Long real_result = result.get();
        assertEquals(txnEgo(1003L), (long)real_result);
    }

    // verify correct txnID when no MP has ever been done
    @Test
    public void testSaneWithNoMP() throws InterruptedException, ExecutionException
    {
        InitiatorMailbox mailbox = mock(MpInitiatorMailbox.class);
        doReturn(4L).when(mailbox).getHSId();
        ArrayList<Long> masters = new ArrayList<Long>();
        masters.add(1L);
        masters.add(2L);
        masters.add(3L);

        MpPromoteAlgo algo = new MpPromoteAlgo(masters, mailbox, "Test");
        long requestId = algo.getRequestId();
        Future<Long> result = algo.start();
        verify(mailbox, times(1)).send(any(long[].class), any(Iv2RepairLogRequestMessage.class));

        // has only the normal ack. Never saw an MP transaction.
        algo.deliver(makeRealAckResponse(requestId, 1L, 0, 1, Long.MAX_VALUE, m_hashinatorConfig));

        // has only the normal ack. Never saw an MP transaction.
        algo.deliver(makeRealAckResponse(requestId, 2L, 0, 1, Long.MAX_VALUE, m_hashinatorConfig));

        // has only the normal ack. Never saw an MP transaction.
        algo.deliver(makeRealAckResponse(requestId, 3L, 0, 1, Long.MAX_VALUE, m_hashinatorConfig));

        // has only the normal ack. Never saw an MP transaction.
        algo.deliver(makeRealAckResponse(requestId, 4L, 0, 1, Long.MAX_VALUE, m_hashinatorConfig));

        // verify that the discovered txn id is 0 (the correct starting txnid).
        Long real_result = result.get();
        assertEquals(TxnEgo.makeZero(MpInitiator.MP_INIT_PID).getTxnId(), (long)real_result);
    }

    // Verify that if the MPI is the only person with a complete, that we
    // complete the transaction to the new replicas
    @Test
    public void testRepairSurvivorsFromJustMPI() throws InterruptedException, ExecutionException
    {
        InitiatorMailbox mailbox = mock(MpInitiatorMailbox.class);
        doReturn(4L).when(mailbox).getHSId();
        ArrayList<Long> masters = new ArrayList<Long>();
        // This should only possible with one master, but we'll make sure multiple work
        masters.add(1L);
        masters.add(2L);

        MpPromoteAlgo algo = new MpPromoteAlgo(masters, mailbox, "Test");
        long requestId = algo.getRequestId();
        Future<Long> result = algo.start();
        verify(mailbox, times(1)).send(any(long[].class), any(Iv2RepairLogRequestMessage.class));

        // has a frag for txn 1000. MP handle is 1000L
        algo.deliver(makeRealAckResponse(requestId, 1L, 0, 2, txnEgo(1000L), m_hashinatorConfig));
        algo.deliver(makeRealFragResponse(requestId, 1L, 1, 2, txnEgo(1000L)));

        // has only the normal ack. Never saw an MP transaction.
        algo.deliver(makeRealAckResponse(requestId, 2L, 0, 1, Long.MAX_VALUE, m_hashinatorConfig));

        // deliver the same complete from the MPI's repair log
        algo.deliver(makeRealAckResponse(requestId, 4L, 0, 2, txnEgo(1000L), m_hashinatorConfig));
        algo.deliver(makeRealCompleteResponse(requestId, 4L, 1, 2, txnEgo(1000L)));

        // Verify that we send a complete to every site.
        List<Long> needsRepair = new ArrayList<Long>();
        needsRepair.add(1L);
        needsRepair.add(2L);
        verify(mailbox, times(1)).repairReplicasWith(eq(needsRepair), any(Iv2RepairLogResponseMessage.class));
        Long real_result = result.get();
        assertEquals(txnEgo(1000L), (long)real_result);
    }

    @Test
    public void testFuzz() throws Exception
    {
        InitiatorMailbox mbox = mock(InitiatorMailbox.class);
        Random rand = new Random(System.currentTimeMillis());
        // Generate a random message stream to several "replicas", interrupted
        // at random points to all but one.  Validate that promotion repair
        // results in identical, correct, repair streams to all replicas.
        TxnEgo sphandle = TxnEgo.makeZero(0);
        sphandle = sphandle.makeNext();
        RandomMsgGenerator msgGen = new RandomMsgGenerator();
        boolean[] stops = new boolean[3];
        RepairLog[] logs = new RepairLog[3];
        for (int i = 0; i < 3; i++) {
            logs[i] = new RepairLog();
            stops[i] = false;
        }
        for (int i = 0; i < 4000; i++) {
            // get next message, update the sphandle according to SpScheduler rules,
            // but only submit messages that would have been forwarded by the master
            // to the repair log.
            TransactionInfoBaseMessage msg = msgGen.generateRandomMessageInStream();
            msg.setSpHandle(sphandle.getTxnId());
            sphandle = sphandle.makeNext();
            if (!msg.isReadOnly() || msg instanceof CompleteTransactionMessage) {
                if (!stops[0]) {
                    logs[0].deliver(msg);
                }
                if (!stops[1]) {
                    logs[1].deliver(msg);
                }
                logs[2].deliver(msg);
                // Putting this inside this loop
                // guarantees at least one message in everyone's repair log,
                // which avoids having to check for the special case where a node
                // has an empty repair log on account of rejoin and shouldn't
                // be fed any transactions
                for (int j = 0; j < 2; j++) {
                    // Hacky way to get spaced failures
                    if (rand.nextDouble() < (.01 / ((j + 1) * 5))) {
                        stops[j] = true;
                    }
                }
            }
        }

        List<Long> survivors = new ArrayList<Long>();
        survivors.add(0l);
        survivors.add(1l);
        survivors.add(2l);
        MpPromoteAlgo dut = new MpPromoteAlgo(survivors, mbox, "bleh ");
        Future<Long> result = dut.start();
        for (int i = 0; i < 3; i++) {
            List<Iv2RepairLogResponseMessage> stuff = logs[i].contents(dut.getRequestId(), true);
            System.out.println("Repair log size from: " + i + ": " + stuff.size());
            for (Iv2RepairLogResponseMessage msg : stuff) {
                msg.m_sourceHSId = (long)i;
                dut.deliver(msg);
            }
        }
        result.get();
        assertFalse(result.isCancelled());
        assertTrue(result.isDone());
        // Unfortunately, it's painful to try to stub things to make repairSurvivors() work, so we'll
        // go and inspect the guts of SpPromoteAlgo instead.  This iteration is largely a copy of the inner loop
        // of repairSurvivors()
        List<TransactionInfoBaseMessage> finalStream = new ArrayList<TransactionInfoBaseMessage>();
        for (Iv2RepairLogResponseMessage li : dut.m_repairLogUnion) {
            VoltMessage msg = dut.createRepairMessage(li);
            finalStream.add((TransactionInfoBaseMessage)msg);
        }
        // Check the sanity of the repair stream generated by the MPI.
        long lastTxnId = Long.MIN_VALUE;
        boolean seenFrag = false;
        for (TransactionInfoBaseMessage msg : finalStream) {
            if (lastTxnId == Long.MIN_VALUE) {
                lastTxnId = msg.getTxnId();
            }
            else {
                assertTrue(msg.getTxnId() > lastTxnId);
                lastTxnId = msg.getTxnId();
            }
            if (msg instanceof FragmentTaskMessage) {
                assertFalse(seenFrag);
                seenFrag = true;
            }
        }
    }
}

