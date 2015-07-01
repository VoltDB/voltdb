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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayDeque;
import java.util.Deque;

import junit.framework.TestCase;

import org.junit.Test;
import org.voltdb.StarvationTracker;
import org.voltdb.dtxn.TransactionState;
import org.voltdb.messaging.CompleteTransactionMessage;
import org.voltdb.messaging.FragmentTaskMessage;
import org.voltdb.messaging.Iv2InitiateTaskMessage;

public class TestTransactionTaskQueue extends TestCase
{

    private static SiteTaskerQueue getSiteTaskerQueue() {
        SiteTaskerQueue queue = new SiteTaskerQueue();
        queue.setStarvationTracker(new StarvationTracker(0));
        return queue;
    }

    // Cases to test:
    // several single part txns

    private SpProcedureTask createSpProc(long localTxnId,
                                         TransactionTaskQueue queue)
    {
        // Mock an initiate message; override its txnid to return
        // the default SP value (usually set by ClientInterface).
        Iv2InitiateTaskMessage init = mock(Iv2InitiateTaskMessage.class);
        when(init.getTxnId()).thenReturn(Iv2InitiateTaskMessage.UNUSED_MP_TXNID);
        when(init.getSpHandle()).thenReturn(localTxnId);

        InitiatorMailbox mbox = mock(InitiatorMailbox.class);
        when(mbox.getHSId()).thenReturn(1337l);

        SpProcedureTask task =
            new SpProcedureTask(mbox, "TestProc", queue, init, null);
        return task;
    }

    private FragmentTask createFrag(long localTxnId, long mpTxnId,
            TransactionTaskQueue queue) {
        return createFrag(localTxnId, mpTxnId, queue, false);
    }
    // Create the first fragment of a MP txn
    private FragmentTask createFrag(long localTxnId, long mpTxnId,
                                    TransactionTaskQueue queue,
                                    boolean forReplay)
    {
        FragmentTaskMessage msg = mock(FragmentTaskMessage.class);
        when(msg.getTxnId()).thenReturn(mpTxnId);
        when(msg.isForReplay()).thenReturn(forReplay);
        InitiatorMailbox mbox = mock(InitiatorMailbox.class);
        when(mbox.getHSId()).thenReturn(1337l);
        ParticipantTransactionState pft =
            new ParticipantTransactionState(localTxnId, msg);
        FragmentTask task =
            new FragmentTask(mbox, pft, queue, msg, null);
        return task;
    }

    // Create follow-on fragments of an MP txn
    private FragmentTask createFrag(TransactionState txn, long mpTxnId,
                                    TransactionTaskQueue queue)
    {
        FragmentTaskMessage msg = mock(FragmentTaskMessage.class);
        when(msg.getTxnId()).thenReturn(mpTxnId);
        InitiatorMailbox mbox = mock(InitiatorMailbox.class);
        when(mbox.getHSId()).thenReturn(1337l);
        FragmentTask task =
            new FragmentTask(mbox, (ParticipantTransactionState)txn, queue, msg, null);
        return task;
    }

    private CompleteTransactionTask createComplete(TransactionState txn,
                                                   long mpTxnId,
                                                   TransactionTaskQueue queue)
    {
        CompleteTransactionMessage msg = mock(CompleteTransactionMessage.class);
        when(msg.getTxnId()).thenReturn(mpTxnId);
        CompleteTransactionTask task =
            new CompleteTransactionTask(txn, queue, msg, null);
        return task;
    }

    private void addTask(TransactionTask task, TransactionTaskQueue dut,
                         Deque<TransactionTask> teststorage)
    {
        if (teststorage != null) {
            teststorage.addLast(task);
        }
        dut.offer(task);
        dut.flush(task.getTxnId());
    }

    @Test
    public void testBasicParticipantOps() throws InterruptedException
    {
        long localTxnId = 0;
        long mpTxnId = 0;
        SiteTaskerQueue task_queue = getSiteTaskerQueue();
        TransactionTaskQueue dut = new TransactionTaskQueue(task_queue,localTxnId);
        Deque<TransactionTask> expected_order =
            new ArrayDeque<TransactionTask>();

        // add a few SP procs
        TransactionTask next = createSpProc(localTxnId++, dut);
        addTask(next, dut, expected_order);
        next = createSpProc(localTxnId++, dut);
        addTask(next, dut, expected_order);
        next = createSpProc(localTxnId++, dut);
        addTask(next, dut, expected_order);
        // Should squirt on through the queue
        assertEquals(0, dut.size());

        // Now a fragment task to block things
        long blocking_mp_txnid = mpTxnId;
        next = createFrag(localTxnId++, mpTxnId++, dut);
        TransactionTask block = next;
        addTask(next, dut, expected_order);
        assertEquals(1, dut.size());

        // Add some tasks that are going to be blocked
        // Manually track the should-be-blocked procedures
        // for comparison later.
        ArrayDeque<TransactionTask> blocked = new ArrayDeque<TransactionTask>();
        next = createSpProc(localTxnId++, dut);
        addTask(next, dut, blocked);
        next = createSpProc(localTxnId++, dut);
        addTask(next, dut, blocked);

        // here's our next blocker
        next = createFrag(localTxnId++, mpTxnId++, dut);
        addTask(next, dut, blocked);
        assertEquals(blocked.size() + 1, dut.size());

        // Add a completion for the next blocker, too.  Simulates rollback causing
        // an additional task for this TXN ID to appear before it's blocking the queue
        next = createComplete(next.getTransactionState(), next.getTxnId(), dut);
        addTask(next, dut, blocked);
        assertEquals(blocked.size() + 1, dut.size());
        System.out.println("blocked: " + blocked);

        // now, do more work on the blocked task
        next = createFrag(block.getTransactionState(), blocking_mp_txnid, dut);
        addTask(next, dut, expected_order);
        // Should have passed through and not be in the queue
        assertEquals(blocked.size() + 1, dut.size());

        // now, complete the blocked task
        next = createComplete(block.getTransactionState(), blocking_mp_txnid, dut);
        addTask(next, dut, expected_order);
        // Should have passed through and not be in the queue
        assertEquals(blocked.size() + 1, dut.size());
        // DONE!  Should flush everything to the next blocker
        block.getTransactionState().setDone();
        int offered = dut.flush(block.getTxnId());
        assertEquals(blocked.size(), offered);
        assertEquals(1, dut.size());
        expected_order.addAll(blocked);

        while (!expected_order.isEmpty())
        {
            TransactionTask next_poll = (TransactionTask)task_queue.take();
            TransactionTask expected = expected_order.removeFirst();
            assertEquals(expected.getSpHandle(), next_poll.getSpHandle());
            assertEquals(expected.getTxnId(), next_poll.getTxnId());
        }
    }
}
