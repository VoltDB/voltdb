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

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertTrue;
import static junit.framework.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.concurrent.CancellationException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.InOrder;
import org.voltcore.messaging.TransactionInfoBaseMessage;
import org.voltcore.messaging.VoltMessage;
import org.voltcore.utils.Pair;
import org.voltdb.TheHashinator;
import org.voltdb.TheHashinator.HashinatorType;
import org.voltdb.messaging.CompleteTransactionMessage;
import org.voltdb.messaging.Iv2InitiateTaskMessage;
import org.voltdb.messaging.Iv2RepairLogResponseMessage;

public class TestSpPromoteAlgo
{
    Iv2RepairLogResponseMessage makeResponse(long spHandle)
    {
        Iv2RepairLogResponseMessage m = mock(Iv2RepairLogResponseMessage.class);
        when(m.getHandle()).thenReturn(spHandle);
        Iv2InitiateTaskMessage im = mock(Iv2InitiateTaskMessage.class);
        when(m.getPayload()).thenReturn(im);
        return m;
    }

    Iv2RepairLogResponseMessage makeStaleResponse(long spHandle, long requestId)
    {
        Iv2RepairLogResponseMessage m = makeResponse(spHandle);
        when(m.getRequestId()).thenReturn(requestId);
        Iv2InitiateTaskMessage im = mock(Iv2InitiateTaskMessage.class);
        when(m.getPayload()).thenReturn(im);
        return m;
    }

    @BeforeClass
    static public void initializeHashinator() {
        TheHashinator.setConfiguredHashinatorType(HashinatorType.ELASTIC);
        TheHashinator.initialize(TheHashinator.getConfiguredHashinatorClass(), TheHashinator.getConfigureBytes(8));
    }

    // verify that responses are correctly unioned and ordered.
    @Test
    public void testUnion() throws Exception
    {
        SpPromoteAlgo term = new SpPromoteAlgo(null, null, "Test", 1);

        // returned sphandles in a non-trivial order, with duplicates.
        long returnedSpHandles[] = new long[]{1L, 5L, 2L, 5L, 6L, 3L, 5L, 1L};
        long expectedUnion[] = new long[]{1L, 2L, 3L, 5L, 6L};

        for (long sp : returnedSpHandles) {
            term.m_repairLogUnion.add(makeResponse(sp));
        }

        assertEquals(expectedUnion.length, term.m_repairLogUnion.size());

        int i = 0;
        for (Iv2RepairLogResponseMessage li : term.m_repairLogUnion) {
            assertEquals(li.getHandle(), expectedUnion[i++]);
        }
    }

    // verify that bad request ids are not submitted to the log.
    @Test
    public void testStaleResponse() throws Exception
    {
        SpPromoteAlgo term = new SpPromoteAlgo(null, null, "Test", 1);
        term.deliver(makeStaleResponse(1L, term.getRequestId() + 1));
        assertEquals(0L, term.m_repairLogUnion.size());
    }



    // verify that the all-done logic works on replica repair structs
    @Test
    public void testRepairLogsAreComplete()
    {
        SpPromoteAlgo term = new SpPromoteAlgo(null, null, "Test", 1);
        SpPromoteAlgo.ReplicaRepairStruct notDone1 = new SpPromoteAlgo.ReplicaRepairStruct();
        notDone1.m_receivedResponses = 1;
        notDone1.m_expectedResponses = 2;
        assertFalse(notDone1.logsComplete());

        SpPromoteAlgo.ReplicaRepairStruct notDone2 = new SpPromoteAlgo.ReplicaRepairStruct();
        notDone2.m_receivedResponses = 0;
        notDone2.m_expectedResponses = 10;

        SpPromoteAlgo.ReplicaRepairStruct done1 = new SpPromoteAlgo.ReplicaRepairStruct();
        done1.m_receivedResponses = 5;
        done1.m_expectedResponses = 5;
        assertTrue(done1.logsComplete());

        SpPromoteAlgo.ReplicaRepairStruct done2 = new SpPromoteAlgo.ReplicaRepairStruct();
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

    // verify that Term asks initMailbox to send the expected repair messages.
    @SuppressWarnings("unchecked")
    @Test
    public void testRepairSurvivors()
    {
        InitiatorMailbox mailbox = mock(InitiatorMailbox.class);
        SpPromoteAlgo term = new SpPromoteAlgo(null, mailbox, "Test", 1);

        // missing 4, 5
        SpPromoteAlgo.ReplicaRepairStruct r1 = new SpPromoteAlgo.ReplicaRepairStruct();
        r1.m_maxSpHandleSeen = 3L;

        // complete
        SpPromoteAlgo.ReplicaRepairStruct r2 = new SpPromoteAlgo.ReplicaRepairStruct();
        r2.m_maxSpHandleSeen = 5L;

        // missing 3, 4, 5
        SpPromoteAlgo.ReplicaRepairStruct r3 = new SpPromoteAlgo.ReplicaRepairStruct();
        r3.m_maxSpHandleSeen = 2L;

        term.m_replicaRepairStructs.put(1L, r1);
        term.m_replicaRepairStructs.put(2L, r2);
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
        verify(mailbox).repairReplicasWith(repair3, msgs[3].getPayload());

        List<Long> repair4And5 = new ArrayList<Long>();
        repair4And5.add(1L);
        repair4And5.add(3L);
        verify(mailbox).repairReplicasWith(repair4And5, msgs[4].getPayload());
        verify(mailbox).repairReplicasWith(repair4And5, msgs[5].getPayload());

        // verify exactly 3 repairs happened.
        verify(mailbox, times(3)).repairReplicasWith(any(repair3.getClass()), any(VoltMessage.class));
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

        SpPromoteAlgo term = new SpPromoteAlgo(null, mailbox, "Test", 1);

        // missing 3, 4, 5
        SpPromoteAlgo.ReplicaRepairStruct r3 = new SpPromoteAlgo.ReplicaRepairStruct();
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
        inOrder.verify(mailbox).repairReplicasWith(repair3, msgs[3].getPayload());
        inOrder.verify(mailbox).repairReplicasWith(repair3, msgs[4].getPayload());
        inOrder.verify(mailbox).repairReplicasWith(repair3, msgs[5].getPayload());

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
        final SpPromoteAlgo term = new SpPromoteAlgo(null, mailbox, "Test", 1) {
            // there aren't replicas to ask for repair logs
            @Override
            void prepareForFaultRecovery() {
            }

        };

        Thread promotionThread = new Thread() {
            @Override
            public void run() {
                try {
                    boolean success = false;
                    try {
                        term.start().get();
                        success = true;
                    } catch (CancellationException e) {}
                    promotionResult.set(success);
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

    @Test
    public void testFuzz() throws Exception
    {
        InitiatorMailbox mbox = mock(InitiatorMailbox.class);
        Map<Long, List<TransactionInfoBaseMessage>> finalStreams =
            new HashMap<Long, List<TransactionInfoBaseMessage>>();
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
            finalStreams.put((long)i, new ArrayList<TransactionInfoBaseMessage>());
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
        SpPromoteAlgo dut = new SpPromoteAlgo(survivors, mbox, "bleh ", 0);
        Future<Long> result = dut.start();
        for (int i = 0; i < 3; i++) {
            List<Iv2RepairLogResponseMessage> stuff = logs[i].contents(dut.getRequestId(), false);
            System.out.println("Repair log size from: " + i + ": " + stuff.size());
            for (Iv2RepairLogResponseMessage msg : stuff) {
                msg.m_sourceHSId = i;
                dut.deliver(msg);
                // First message is metadata only, skip it in validation stream
                if (msg.getSequence() > 0) {
                    //System.out.println("finalstreams: " + finalStreams);
                    //System.out.println("get(i): " + i + ": " + finalStreams.get((long)i));
                    //System.out.println("msg: " + msg);
                    finalStreams.get((long)i).add((TransactionInfoBaseMessage)msg.getPayload());
                }
            }
        }
        result.get();
        assertFalse(result.isCancelled());
        assertTrue(result.isDone());
        // Unfortunately, it's painful to try to stub things to make repairSurvivors() work, so we'll
        // go and inspect the guts of SpPromoteAlgo instead.  This iteration is largely a copy of the inner loop
        // of repairSurvivors()
        for (Iv2RepairLogResponseMessage li : dut.m_repairLogUnion) {
            for (Entry<Long, SpPromoteAlgo.ReplicaRepairStruct> entry : dut.m_replicaRepairStructs.entrySet()) {
                if (entry.getValue().needs(li.getHandle())) {
                    // append the missing message for this 'node' to the list of messages that node has seen
                    finalStreams.get(entry.getKey()).add((TransactionInfoBaseMessage)li.getPayload());
                }
            }
        }
        // check that all the lists for all the nodes are identical after repair
        int longest = Integer.MIN_VALUE;
        for (Entry<Long, List<TransactionInfoBaseMessage>> entry : finalStreams.entrySet()) {
            System.out.println("SIZE: " + entry.getValue().size());
            if (entry.getValue().size() > longest) {
                if (longest == Integer.MIN_VALUE) {
                    longest = entry.getValue().size();
                }
                else {
                    fail("Mismatch in repair stream size!");
                }
            }
        }
        for (int i = 0; i < longest; i++) {
            TransactionInfoBaseMessage current = null;
            for (Entry<Long, List<TransactionInfoBaseMessage>> entry : finalStreams.entrySet()) {
                TransactionInfoBaseMessage msg = entry.getValue().get(i);
                if (current == null) {
                    current = msg;
                }
                else {
                    assertEquals(current.getSpHandle(), msg.getSpHandle());
                    assertEquals(current.getClass(), msg.getClass());
                }
            }
        }
    }
}

