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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;

import org.junit.Test;
import org.voltdb.StarvationTracker;
import org.voltdb.dtxn.TransactionState;
import org.voltdb.messaging.CompleteTransactionMessage;
import org.voltdb.messaging.FragmentTaskMessage;
import org.voltdb.messaging.Iv2InitiateTaskMessage;

import junit.framework.TestCase;

public class TestTransactionTaskQueue extends TestCase
{
    private static final int SITE_COUNT = 2;
    private List<TransactionTaskQueue> m_txnTaskQueues = new ArrayList<>();
    private List<SiteTaskerQueue> m_siteTaskQueues = new ArrayList<>();
    List<Deque<TransactionTask>> m_expectedOrders = new ArrayList<>();
    private MpRestartSequenceGenerator m_repairGenerator;
    private MpRestartSequenceGenerator m_restartGenerator;


    TxnEgo[] m_localTxnEgo = new TxnEgo[SITE_COUNT]; // for sp txn, txnId is spHandle
    TxnEgo m_mpTxnEgo;

    private static SiteTaskerQueue getSiteTaskerQueue() {
        SiteTaskerQueue queue = new SiteTaskerQueue(0);
        queue.setStarvationTracker(new StarvationTracker(0));
        queue.setupQueueDepthTracker(0);
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
                SpProcedureTask.create(mbox, "TestProc", queue, init);
        return task;
    }

    private FragmentTask createFrag(long localTxnId, long mpTxnId,
            TransactionTaskQueue queue) {
        return createFrag(localTxnId, mpTxnId, queue, CompleteTransactionMessage.INITIAL_TIMESTAMP, false);
    }
    // Create the first fragment of a MP txn
    private FragmentTask createFrag(long localTxnId, long mpTxnId,
                                    TransactionTaskQueue queue,
                                    long restartTimestamp,
                                    boolean forReplay)
    {
        FragmentTaskMessage msg = mock(FragmentTaskMessage.class);
        when(msg.isNPartTxn()).thenReturn(false);
        when(msg.getTxnId()).thenReturn(mpTxnId);
        when(msg.isForReplay()).thenReturn(forReplay);
        when(msg.getTimestamp()).thenReturn(restartTimestamp);
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
                                    TransactionTaskQueue queue, long restartTimestamp)
    {
        FragmentTaskMessage msg = mock(FragmentTaskMessage.class);
        when(msg.getTxnId()).thenReturn(mpTxnId);
        when(msg.getTimestamp()).thenReturn(restartTimestamp);
        InitiatorMailbox mbox = mock(InitiatorMailbox.class);
        when(mbox.getHSId()).thenReturn(1337l);
        FragmentTask task =
            new FragmentTask(mbox, (ParticipantTransactionState)txn, queue, msg, null);
        return task;
    }

    private CompleteTransactionTask createCompletion(TransactionState txn,
            long mpTxnId,
            TransactionTaskQueue queue)
    {
        CompleteTransactionMessage msg = mock(CompleteTransactionMessage.class);
        when(msg.getTxnId()).thenReturn(mpTxnId);
        when(msg.getTimestamp()).thenReturn(CompleteTransactionMessage.INITIAL_TIMESTAMP);
        when(msg.needsCoordination()).thenReturn(true);
        when(msg.isAbortDuringRepair()).thenReturn(false);
        CompleteTransactionTask task =
                new CompleteTransactionTask(mock(InitiatorMailbox.class), txn, queue, msg);
        return task;
    }
    private CompleteTransactionTask createRepairCompletion(TransactionState txn,
                                                       long mpTxnId,
                                                       TransactionTaskQueue queue,
                                                       long seq,
                                                       boolean isAbortDuringRepair)
    {
        CompleteTransactionMessage msg = mock(CompleteTransactionMessage.class);
        when(msg.getTxnId()).thenReturn(mpTxnId);
        when(msg.getTimestamp()).thenReturn(seq);
        when(msg.needsCoordination()).thenReturn(true);
        when(msg.isAbortDuringRepair()).thenReturn(txn.isDone()?false:isAbortDuringRepair);
        CompleteTransactionTask task =
            new CompleteTransactionTask(mock(InitiatorMailbox.class), txn, queue, msg);
        return task;
    }

    private CompleteTransactionTask createRestartComplete(TransactionState txn,
                                                         long mpTxnId,
                                                         TransactionTaskQueue queue,
                                                         long seq)
    {
        CompleteTransactionMessage msg = mock(CompleteTransactionMessage.class);
        when(msg.getTxnId()).thenReturn(mpTxnId);
        when(msg.getTimestamp()).thenReturn(seq);
        when(msg.needsCoordination()).thenReturn(true);
        when(msg.isAbortDuringRepair()).thenReturn(false);
        CompleteTransactionTask task =
                new CompleteTransactionTask(mock(InitiatorMailbox.class), txn, queue, msg);
        return task;
    }

    private void addTask(TransactionTask task, TransactionTaskQueue dut,
                         Deque<TransactionTask> teststorage)
    {
        if (teststorage != null) {
            teststorage.addLast(task);
        }
        dut.offer(task);
    }

    private void addMissingCompletionTask(TransactionTask task, TransactionTaskQueue dut,
                                          Deque<TransactionTask> teststorage) {
        if (teststorage != null) {
            teststorage.addLast(task);
        }
        dut.handleCompletionForMissingTxn((CompleteTransactionTask) task);
    }

    private CompleteTransactionTask[] repairOnly(TransactionTask[] repairTask, Boolean[] missingCompletions) {
        long repairSeq = m_repairGenerator.getNextSeqNum();
        CompleteTransactionTask completions[] = new CompleteTransactionTask[SITE_COUNT];
        for (int i = 0; i < SITE_COUNT; i++) {
            CompleteTransactionTask comp = createRepairCompletion(repairTask[i].getTransactionState(), repairTask[i].getTxnId(), m_txnTaskQueues.get(i), repairSeq, true);
            completions[i] = comp;
            if (missingCompletions[i]) {
                addMissingCompletionTask(comp, m_txnTaskQueues.get(i), m_expectedOrders.get(i));
            } else {
                addTask(comp, m_txnTaskQueues.get(i), m_expectedOrders.get(i));
            }
        }
        return completions;
    }

    private void repairThenRestart(TransactionTask[] repairTask, TransactionTask restartTask, boolean[] missingCompletions) {
        long restartSeq = m_restartGenerator.getNextSeqNum();
        long repairSeq = m_repairGenerator.getNextSeqNum();
        for (int i = 0; i < SITE_COUNT; i++) {
            TransactionTask comp = createRepairCompletion(repairTask[i].getTransactionState(),
                    repairTask[i].getTxnId(), m_txnTaskQueues.get(i), repairSeq, false);
            if (missingCompletions[i]) {
                addMissingCompletionTask(comp, m_txnTaskQueues.get(i), m_expectedOrders.get(i));
            } else {
                addTask(comp, m_txnTaskQueues.get(i), m_expectedOrders.get(i));
            }

            comp = createRestartComplete(restartTask.getTransactionState(), restartTask.getTxnId(), m_txnTaskQueues.get(i), restartSeq);
            addTask(comp, m_txnTaskQueues.get(i), m_expectedOrders.get(i));

            TransactionTask restartFrag = createFrag(m_localTxnEgo[i].getTxnId(), restartTask.getTxnId(), m_txnTaskQueues.get(i));
            addTask(restartFrag, m_txnTaskQueues.get(i), m_expectedOrders.get(i));
            m_localTxnEgo[i] = m_localTxnEgo[i].makeNext();
        }
    }

    private void flushBacklog(TransactionTask[] task) {
        // Finish the transaction, flush the backlog
        for (int i = 0; i < SITE_COUNT; i++) {
            task[i].getTransactionState().setDone();
            m_txnTaskQueues.get(i).flush(task[i].getTxnId());
        }
    }

    private void execCompletions(CompleteTransactionTask[] task) {
        for (int i = 0; i < SITE_COUNT; i++) {
            task[i].run(null);
        }
    }

    private void verify() throws InterruptedException {
        for (int i = 0; i < SITE_COUNT; i++) {
            assertEquals(m_expectedOrders.get(i).size(), m_siteTaskQueues.get(i).size());
            while (!m_expectedOrders.get(i).isEmpty()) {
                TransactionTask next_poll = (TransactionTask)m_siteTaskQueues.get(i).take();
                TransactionTask expected = m_expectedOrders.get(i).removeFirst();
                assertEquals(expected.getSpHandle(), next_poll.getSpHandle());
                assertEquals(expected.getTxnId(), next_poll.getTxnId());
            }
        }
    }

    @Override
    public void setUp() {
        TransactionTaskQueue.resetScoreboards(0, SITE_COUNT);
        m_mpTxnEgo = TxnEgo.makeZero(MpInitiator.MP_INIT_PID);
        m_repairGenerator = new MpRestartSequenceGenerator(0, false);
        m_restartGenerator = new MpRestartSequenceGenerator(0, true);
        for (int i = 0; i < SITE_COUNT; i++) {
            SiteTaskerQueue siteTaskQueue = getSiteTaskerQueue();
            m_siteTaskQueues.add(siteTaskQueue);
            TransactionTaskQueue txnTaskQueue = new TransactionTaskQueue(siteTaskQueue, true);
            txnTaskQueue.initializeScoreboard(i);
            m_txnTaskQueues.add(txnTaskQueue);
            Deque<TransactionTask> expectedOrder = new ArrayDeque<>();
            m_expectedOrders.add(expectedOrder);
            m_localTxnEgo[i] = TxnEgo.makeZero(i);
        }
    }

    @Override
    public void tearDown() {
        m_siteTaskQueues.clear();
        m_txnTaskQueues.clear();
        m_expectedOrders.clear();
        for (int i = 0; i < SITE_COUNT; i++) {
            m_localTxnEgo[i] = null;
        }
        m_mpTxnEgo = null;
    }

    // This is the most common case
    @Test
    public void testBasicMultiFragmentsMp() throws InterruptedException {
        // Every site receives first fragment
        long txnId = m_mpTxnEgo.getTxnId();
        TransactionTask firstFrag = null;
        for (int i = 0; i < SITE_COUNT; i++) {
            firstFrag = createFrag(m_localTxnEgo[i].getTxnId(), txnId, m_txnTaskQueues.get(i));
            addTask(firstFrag, m_txnTaskQueues.get(i), m_expectedOrders.get(i));
        }

        // Every site receives second fragment
        for (int i = 0; i < SITE_COUNT; i++) {
            TransactionTask next = createFrag(firstFrag.getTransactionState(), txnId, m_txnTaskQueues.get(i), CompleteTransactionMessage.INITIAL_TIMESTAMP);
            addTask(next, m_txnTaskQueues.get(i), m_expectedOrders.get(i));
        }

        // Every site receives a completion
        TransactionTask comp = null;
        for (int i = 0; i < SITE_COUNT; i++) {
            comp = createCompletion(firstFrag.getTransactionState(), firstFrag.getTxnId(), m_txnTaskQueues.get(i));
            addTask(comp, m_txnTaskQueues.get(i), m_expectedOrders.get(i));
        }

        verify();
    }

    // In case MpProc doesn't generate any fragment, e.g. run() method is empty
    @Test
    public void testNoFragmentMp() throws InterruptedException {
        long txnId = m_mpTxnEgo.getTxnId();
        // Every site receives a completion
        TransactionTask comp = null;
        for (int i = 0; i < SITE_COUNT; i++) {
            MpTransactionState txnState = mock(MpTransactionState.class);
            comp = createCompletion(txnState, txnId, m_txnTaskQueues.get(i));
            addTask(comp, m_txnTaskQueues.get(i), m_expectedOrders.get(i));
        }

        verify();
    }

    // MpProc is in progress, a node failure cause MPI to repair previous transaction and restart current transaction
    @Test
    public void testMpRepair() throws InterruptedException {
        // Every site receives first fragment
        long txnId = m_mpTxnEgo.getTxnId();
        TransactionTask[] firstFrag = new TransactionTask[SITE_COUNT];
        for (int i = 0; i < SITE_COUNT; i++) {
            firstFrag[i] = createFrag(m_localTxnEgo[i].getTxnId(), txnId, m_txnTaskQueues.get(i));
            addTask(firstFrag[i], m_txnTaskQueues.get(i), m_expectedOrders.get(i));
        }

        // Not all sites receive completion
        TransactionTask comp = null;
        for (int i = 0; i < SITE_COUNT - 1; i++) {
            comp = createCompletion(firstFrag[i].getTransactionState(), firstFrag[i].getTxnId(), m_txnTaskQueues.get(i));
            addTask(comp, m_txnTaskQueues.get(i), null);
        }

        m_mpTxnEgo = m_mpTxnEgo.makeNext();
        txnId = m_mpTxnEgo.getTxnId();
        // it will stay at backlog
        TransactionTask firstFragOfNextTxn = null;
        for (int i = 0; i < SITE_COUNT - 1; i++) {
            m_localTxnEgo[i] = m_localTxnEgo[i].makeNext();
            firstFragOfNextTxn = createFrag(m_localTxnEgo[i].getTxnId(), txnId, m_txnTaskQueues.get(i));
            addTask(firstFragOfNextTxn, m_txnTaskQueues.get(i), null);
        }

        // failure occurs, MPI checks repair logs from everybody, decide to repair previous transaction,
        // restart current transaction

        repairThenRestart(firstFrag, firstFragOfNextTxn, new boolean[]{false, false});
        flushBacklog(firstFrag);

        for (int i = 0; i < SITE_COUNT; i++) {
            comp = createCompletion(firstFragOfNextTxn.getTransactionState(),
                    firstFragOfNextTxn.getTxnId(), m_txnTaskQueues.get(i));
            addTask(comp, m_txnTaskQueues.get(i), m_expectedOrders.get(i));
        }

        verify();
    }

    // MpProc is in progress, a node failure cause MPI to repair non-restartable transaction followed by SP txn
    @Test
    public void testNonRestartableMpRepair() throws InterruptedException {
        // Even sites receives first fragment
        long txnId = m_mpTxnEgo.getTxnId();
        TransactionTask[] firstFrag = new TransactionTask[SITE_COUNT];
        for (int i = 0; i < SITE_COUNT; i++) {
            firstFrag[i] = createFrag(m_localTxnEgo[i].getTxnId(), txnId, m_txnTaskQueues.get(i));
            if (i % 2 == 0) {
                // This won't get past the scoreboard
                addTask(firstFrag[i], m_txnTaskQueues.get(i), null);
            }
        }

        m_mpTxnEgo = m_mpTxnEgo.makeNext();
        txnId = m_mpTxnEgo.getTxnId();
        m_localTxnEgo[0] = m_localTxnEgo[0].makeNext();

        // it will stay at backlog
        TransactionTask next = createSpProc(m_localTxnEgo[0].getTxnId(), m_txnTaskQueues.get(0));
        addTask(next, m_txnTaskQueues.get(0), null);
        m_localTxnEgo[0] = m_localTxnEgo[0].makeNext();

        // failure occurs, MPI checks repair logs from everybody, decide to repair previous transaction,
        // restart current transaction

        CompleteTransactionTask[] completions = repairOnly(firstFrag, new Boolean[]{false, true});

        // pending (backlogged) SP Txn in even partitions will be released after the completion
        m_expectedOrders.get(0).add(next);

        // Push SPs to 2 sites to verify that the backlog is clear
        next = createSpProc(m_localTxnEgo[0].getTxnId(), m_txnTaskQueues.get(0));
        addTask(next, m_txnTaskQueues.get(0), m_expectedOrders.get(0));
        m_localTxnEgo[0] = m_localTxnEgo[0].makeNext();

        next = createSpProc(m_localTxnEgo[1].getTxnId(), m_txnTaskQueues.get(1));
        addTask(next, m_txnTaskQueues.get(1), m_expectedOrders.get(1));
        m_localTxnEgo[1] = m_localTxnEgo[1].makeNext();

        // since the completions (even for frags that were never processed) still execute on the sites,
        // the completion tasks actually perform the flush
        execCompletions(completions);

        verify();
    }

    @Test
    public void testStaledFragment() throws InterruptedException {
        MpRestartSequenceGenerator generator = new MpRestartSequenceGenerator(0, false);
        long nextSeq = generator.getNextSeqNum();
        long txnId = m_mpTxnEgo.getTxnId();
        for (int i = 0; i < SITE_COUNT; i++) {
            TransactionTask firstFrag = createFrag(m_localTxnEgo[i].getTxnId(), txnId, m_txnTaskQueues.get(i));
            addTask(firstFrag, m_txnTaskQueues.get(i), null);

            m_localTxnEgo[i] = m_localTxnEgo[i].makeNext();
            TransactionTask staleFrag = createFrag(m_localTxnEgo[i].getTxnId(), txnId, m_txnTaskQueues.get(i), nextSeq, false);
            addTask(staleFrag, m_txnTaskQueues.get(i), m_expectedOrders.get(i));
        }

        verify();
    }

    // Sometimes, especially in MPI failover, site may gets staled completion because of slow network.
    @Test
    public void testStaledCompletion() throws InterruptedException {
        // Every site receives first fragment
        long txnId = m_mpTxnEgo.getTxnId();
        TransactionTask[] firstFrag = new TransactionTask[SITE_COUNT];
        for (int i = 0; i < SITE_COUNT; i++) {
            firstFrag[i] = createFrag(m_localTxnEgo[i].getTxnId(), txnId, m_txnTaskQueues.get(i));
            addTask(firstFrag[i], m_txnTaskQueues.get(i), m_expectedOrders.get(i));
        }

        // Not all sites receive completion
        TransactionTask comp = null;
        for (int i = 0; i < SITE_COUNT - 1; i++) {
            comp = createCompletion(firstFrag[i].getTransactionState(), firstFrag[i].getTxnId(), m_txnTaskQueues.get(i));
            addTask(comp, m_txnTaskQueues.get(i), null);
        }

        // failure occurs, MPI repair current transaction

        // Every site gets the repair completion message
        long seq = m_repairGenerator.getNextSeqNum();
        for (int i = 0; i < SITE_COUNT; i++) {
            comp = createRepairCompletion(firstFrag[i].getTransactionState(),
                    firstFrag[i].getTxnId(), m_txnTaskQueues.get(i), seq, false);
            addTask(comp, m_txnTaskQueues.get(i), m_expectedOrders.get(i));
        }
        flushBacklog(firstFrag);

        // But on one site, a staled completion arrives, it should be discarded.
        for (int i = SITE_COUNT - 1; i < SITE_COUNT; i++) {
            comp = createCompletion(firstFrag[i].getTransactionState(), firstFrag[i].getTxnId(), m_txnTaskQueues.get(i));
            addTask(comp, m_txnTaskQueues.get(i), null);
        }

        verify();
    }

    // MPI may send repair messages multiple times because every SPI promotion interrupts repair.
    @Test
    public void testMultipleFailures() throws InterruptedException {
        // Every site receives first fragment
        long txnId = m_mpTxnEgo.getTxnId();
        TransactionTask[] firstFrag = new TransactionTask[SITE_COUNT];
        for (int i = 0; i < SITE_COUNT; i++) {
            firstFrag[i] = createFrag(m_localTxnEgo[i].getTxnId(), txnId, m_txnTaskQueues.get(i));
            addTask(firstFrag[i], m_txnTaskQueues.get(i), m_expectedOrders.get(i));
        }

        // Every site receives completion
        TransactionTask comp = null;
        for (int i = 0; i < SITE_COUNT; i++) {
            comp = createCompletion(firstFrag[i].getTransactionState(), firstFrag[i].getTxnId(), m_txnTaskQueues.get(i));
            // Finish the transaction
            firstFrag[i].getTransactionState().setDone();
            addTask(comp, m_txnTaskQueues.get(i), m_expectedOrders.get(i));
        }

        // Every site's completion response flushes the TaskQueue
        for (int i = 0; i < SITE_COUNT; i++) {
            m_txnTaskQueues.get(i).flush(txnId);
        }


        // Run next mp transaction
        m_mpTxnEgo = m_mpTxnEgo.makeNext();
        txnId = m_mpTxnEgo.getTxnId();
        TransactionTask[] firstFragOfNextTxn = new TransactionTask[SITE_COUNT];
        for (int i = 0; i < SITE_COUNT ; i++) {
            firstFragOfNextTxn[i] = createFrag(m_localTxnEgo[i].getTxnId(), txnId, m_txnTaskQueues.get(i));
            addTask(firstFragOfNextTxn[i], m_txnTaskQueues.get(i), m_expectedOrders.get(i));
        }

        // failure occurs, MPI repair current transaction

        // Every site gets the repair completion to close previous transaction,
        // and restart completion to restart the current one.
        repairThenRestart(firstFrag, firstFragOfNextTxn[0], new boolean[]{true, true});
        flushBacklog(firstFrag);

        // failure occurs, MPI repair current transaction
        repairThenRestart(firstFrag, firstFragOfNextTxn[0], new boolean[]{true, true});
        flushBacklog(firstFrag);

        // failure occurs, MPI repair current transaction
        repairThenRestart(firstFrag, firstFragOfNextTxn[0], new boolean[]{true, true});

        // Transaction complete
        for (int i = 0; i < SITE_COUNT; i++) {
            comp = createCompletion(firstFragOfNextTxn[i].getTransactionState(), firstFragOfNextTxn[i].getTxnId(), m_txnTaskQueues.get(i));
            // Finish the transaction
            firstFragOfNextTxn[i].getTransactionState().setDone();
            addTask(comp, m_txnTaskQueues.get(i), m_expectedOrders.get(i));
        }

        verify();
    }

    // As part of repair, MPI sends completion message of prior MP transaction to transaction queue
    // backlog should let the message go through.
    @Test
    public void testLetPriorRepairCompletionGoThrough() throws InterruptedException {

        // Every site receives first fragment
        long prevTxnId = m_mpTxnEgo.getTxnId();
        TransactionTask[] firstFrag = new TransactionTask[SITE_COUNT];
        for (int i = 0; i < SITE_COUNT; i++) {
            firstFrag[i] = createFrag(m_localTxnEgo[i].getTxnId(), prevTxnId, m_txnTaskQueues.get(i));
            addTask(firstFrag[i], m_txnTaskQueues.get(i), m_expectedOrders.get(i));
        }

        // Assume site receives fragment for next transaction (won't happen in real world).
        m_mpTxnEgo = m_mpTxnEgo.makeNext();
        long txnId = m_mpTxnEgo.getTxnId();
        TransactionTask[] nextFrag = new TransactionTask[SITE_COUNT];
        for (int i = 0; i < SITE_COUNT; i++) {
            m_localTxnEgo[i] = m_localTxnEgo[i].makeNext();
            nextFrag[i] = createFrag(m_localTxnEgo[i].getTxnId(), txnId, m_txnTaskQueues.get(i));
            addTask(nextFrag[i], m_txnTaskQueues.get(i), null);
        }

        // failure happens, MPI starts to repair

        // Every site gets the repair completion to close previous transaction,
        TransactionTask comp = null;
        long seq = m_repairGenerator.getNextSeqNum();
        for (int i = 0; i < SITE_COUNT; i++) {
            comp = createRepairCompletion(firstFrag[i].getTransactionState(), prevTxnId, m_txnTaskQueues.get(i), seq, false);
            // Finish the transaction
            firstFrag[i].getTransactionState().setDone();
            addTask(comp, m_txnTaskQueues.get(i), m_expectedOrders.get(i));
            // Add the frag after the completion because that is the order it will actually be handled
            m_expectedOrders.get(i).add(nextFrag[i]);
        }
        for (int i = 0; i < SITE_COUNT; i++) {
            m_txnTaskQueues.get(i).flush(prevTxnId);
        }

        verify();
    }

    @Test
    public void testBasicParticipantOps() throws InterruptedException
    {
        // add a few SP procs to site 0
        TransactionTask next = createSpProc(m_localTxnEgo[0].getTxnId(), m_txnTaskQueues.get(0));
        addTask(next, m_txnTaskQueues.get(0), m_expectedOrders.get(0));
        m_localTxnEgo[0] = m_localTxnEgo[0].makeNext();
        next = createSpProc(m_localTxnEgo[0].getTxnId(), m_txnTaskQueues.get(0));
        addTask(next, m_txnTaskQueues.get(0), m_expectedOrders.get(0));
        m_localTxnEgo[0] = m_localTxnEgo[0].makeNext();
        next = createSpProc(m_localTxnEgo[0].getTxnId(), m_txnTaskQueues.get(0));
        addTask(next, m_txnTaskQueues.get(0), m_expectedOrders.get(0));
        m_localTxnEgo[0] = m_localTxnEgo[0].makeNext();
        // Should squirt on through the queue
        assertEquals(0, m_txnTaskQueues.get(0).size());

        // Now a fragment task to block things
        long blocking_mp_txnid = m_mpTxnEgo.getTxnId();
        TransactionTask[] block = new TransactionTask[SITE_COUNT];
        for (int i = 0 ; i < SITE_COUNT; i++) {
            block[i] = createFrag(m_localTxnEgo[i].getTxnId(), blocking_mp_txnid, m_txnTaskQueues.get(i));
            addTask(block[i], m_txnTaskQueues.get(i), m_expectedOrders.get(i));
            m_localTxnEgo[i] = m_localTxnEgo[i].makeNext();
            assertEquals(1, m_txnTaskQueues.get(i).size());
        }

        m_mpTxnEgo = m_mpTxnEgo.makeNext();
        long another_blocking_mp_txnid = m_mpTxnEgo.getTxnId();
        List<ArrayDeque<TransactionTask>> blocked = new ArrayList<>();
        TransactionTask[] nextBlock = new TransactionTask[SITE_COUNT];
        for (int i = 0 ; i < SITE_COUNT; i++) {
            // Add some tasks that are going to be blocked
            // Manually track the should-be-blocked procedures
            // for comparison later.
            ArrayDeque<TransactionTask> queue = new ArrayDeque<>();
            blocked.add(queue);
            next = createSpProc(m_localTxnEgo[i].getTxnId(), m_txnTaskQueues.get(i));
            addTask(next, m_txnTaskQueues.get(i), queue);
            m_localTxnEgo[i] = m_localTxnEgo[i].makeNext();
            next = createSpProc(m_localTxnEgo[i].getTxnId(), m_txnTaskQueues.get(i));
            addTask(next, m_txnTaskQueues.get(i), queue);
            m_localTxnEgo[i] = m_localTxnEgo[i].makeNext();

            // here's our next blocker
            nextBlock[i] = createFrag(m_localTxnEgo[i].getTxnId(), another_blocking_mp_txnid, m_txnTaskQueues.get(i));
            addTask(nextBlock[i], m_txnTaskQueues.get(i), queue);
            m_localTxnEgo[i] = m_localTxnEgo[i].makeNext();
            assertEquals(queue.size() + 1, m_txnTaskQueues.get(i).size());
        }

        // Add a completion for the next blocker, too.  Simulates rollback causing
        // an additional task for this TXN ID to appear before it's blocking the queue
        for (int i = 0 ; i < SITE_COUNT; i++) {
            next = createCompletion(nextBlock[i].getTransactionState(), nextBlock[i].getTxnId(), m_txnTaskQueues.get(i));
            addTask(next, m_txnTaskQueues.get(i), blocked.get(i));
            assertEquals(blocked.get(i).size() + 1, m_txnTaskQueues.get(i).size());
            System.out.println("blocked: " + blocked.get(i));
        }

        // now, do more work on the blocked task
        for (int i = 0 ; i < SITE_COUNT; i++) {
            next = createCompletion(block[i].getTransactionState(), blocking_mp_txnid, m_txnTaskQueues.get(i));
            addTask(next, m_txnTaskQueues.get(i), m_expectedOrders.get(i));
            // Should have passed through and not be in the queue
            assertEquals(blocked.get(i).size() + 1, m_txnTaskQueues.get(i).size());
        }

        // now, complete the blocked task
        for (int i = 0 ; i < SITE_COUNT; i++) {
            // DONE!  Should flush everything to the next blocker
            block[i].getTransactionState().setDone();
            int offered = m_txnTaskQueues.get(i).flush(block[i].getTxnId());
            assertEquals(blocked.get(i).size(), offered);
            assertEquals(1, m_txnTaskQueues.get(i).size());
            m_expectedOrders.get(i).addAll(blocked.get(i));
        }

        verify();
    }
}
