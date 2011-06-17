/* This file is part of VoltDB.
 * Copyright (C) 2008-2011 VoltDB Inc.
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

package org.voltdb.dtxn;

import java.util.Iterator;
import java.util.Vector;

import junit.framework.TestCase;

import org.voltdb.StoredProcedureInvocation;
import org.voltdb.TransactionIdManager;
import org.voltdb.VoltDB;
import org.voltdb.messaging.InitiateTaskMessage;

public class TestRestrictedPriorityQueue extends TestCase{
    int[] m_initiators;
    RestrictedPriorityQueue m_queue;
    TransactionIdManager m_idManager;
    StoredProcedureInvocation m_proc;
    InitiateTaskMessage m_task;
    TransactionState m_state1;
    TransactionState m_state2;
    TransactionState m_state3;
    TransactionState m_state4;
    TransactionState m_state5;
    TransactionState m_state6;
    Vector<Long> m_txnIds;

    TransactionState m_states[] = new TransactionState[6];

    @Override
    public void setUp() throws InterruptedException {
        m_initiators = new int[2];
        m_initiators[0] = 0;
        m_initiators[1] = 1;
        m_queue = new RestrictedPriorityQueue(m_initiators, 0, null, VoltDB.DTXN_MAILBOX_ID);
        m_idManager = new TransactionIdManager(0, 0);
        m_txnIds = new Vector<Long>();
        m_proc = new StoredProcedureInvocation();

        long txnId = m_idManager.getNextUniqueTransactionId();
        m_task = new InitiateTaskMessage(0, 0, txnId, true, true, m_proc, txnId);
        m_txnIds.add(m_task.getTxnId());
        m_state1 = new SinglePartitionTxnState(null, null, m_task);
        m_states[0] = m_state1;

        txnId = m_idManager.getNextUniqueTransactionId();
        m_task = new InitiateTaskMessage(1, 1, txnId, true, true, m_proc, txnId);
        assertTrue(m_txnIds.lastElement() < m_task.getTxnId());
        m_txnIds.add(m_task.getTxnId());
        m_state2 = new SinglePartitionTxnState(null, null, m_task);
        m_states[1] = m_state2;

        txnId = m_idManager.getNextUniqueTransactionId();
        m_task = new InitiateTaskMessage(0, 0, txnId, true, true, m_proc, txnId);
        assertTrue(m_txnIds.lastElement() < m_task.getTxnId());
        m_txnIds.add(m_task.getTxnId());
        m_state3 = new SinglePartitionTxnState(null, null, m_task);
        m_states[2] = m_state3;

        txnId = m_idManager.getNextUniqueTransactionId();
        m_task = new InitiateTaskMessage(1, 1, txnId, true, true, m_proc, txnId);
        assertTrue(m_txnIds.lastElement() < m_task.getTxnId());
        m_txnIds.add(m_task.getTxnId());
        m_state4 = new SinglePartitionTxnState(null, null, m_task);
        m_states[3] = m_state4;

        txnId = m_idManager.getNextUniqueTransactionId();
        m_task = new InitiateTaskMessage(0, 0, txnId, true, true, m_proc, txnId);
        assertTrue(m_txnIds.lastElement() < m_task.getTxnId());
        m_txnIds.add(m_task.getTxnId());
        m_state5 = new SinglePartitionTxnState(null, null, m_task);
        m_states[4] = m_state5;

        // Create an additional transaction to add to the priority queue but
        // don't add it to m_txnIds.  This additional transaction allows us
        // to iterate through all of m_txnIds safely.
        txnId = m_idManager.getNextUniqueTransactionId();
        m_task = new InitiateTaskMessage(1, 1, txnId, true, true, m_proc, txnId);
        assertTrue(m_txnIds.lastElement() < m_task.getTxnId());
        m_state6 = new SinglePartitionTxnState(null, null, m_task);
        m_states[5] = m_state6;

        m_queue.shutdown();
    }

    private void addTxnToQueue(TransactionState state)
    {
        m_queue.noteTransactionRecievedAndReturnLastSeen(state.initiatorSiteId, state.txnId, false, state.txnId);
        m_queue.add(state);
    }

    private void checkNextStateNull()
    {
        TransactionState state = (TransactionState)m_queue.poll();
        assertNull(state);
    }

    private void checkNextStateValid(long expectedTxnId)
    {
        TransactionState state = (TransactionState)m_queue.poll();
        assertNotNull(state);
        assertEquals(state.txnId, expectedTxnId);
    }

    // Test that interleaved transaction generated by two ids work if they
    // all arrive in the order in which they were generated
    public void testIdealOrder()
    {
        for (int i=0; i < m_states.length; ++i) {
            addTxnToQueue(m_states[i]);
        }
        assertEquals(m_queue.size(), 6);

        long safeTxnId = m_idManager.getLastTxnId();
        m_queue.noteTransactionRecievedAndReturnLastSeen(0, m_idManager.getNextUniqueTransactionId(), true, safeTxnId);
        m_queue.noteTransactionRecievedAndReturnLastSeen(1, m_idManager.getNextUniqueTransactionId(), true, safeTxnId);

        for (long txnId : m_txnIds) {
            checkNextStateValid(txnId);
        }
    }

    // Test that all the arrivals from one ID are blocked if we get nothing
    // from the other ID.  A bit hacky; we 'know' that the odd states are ID 0
    // and the evens are ID 1
    public void testBatchedIdOrder()
    {
        addTxnToQueue(m_state1);
        addTxnToQueue(m_state3);
        addTxnToQueue(m_state5);

        // ID 1 should still be blocking us
        checkNextStateNull();

        // This test is totally broken by what's added for the new release stuff :(

        // now trickle in ID 1's txns and make sure that the right things
        // happen
        addTxnToQueue(m_state2);
        m_queue.noteTransactionRecievedAndReturnLastSeen(0, m_state2.txnId, true, m_state2.txnId);
        m_queue.noteTransactionRecievedAndReturnLastSeen(1, m_state2.txnId, true, m_state2.txnId);
        checkNextStateValid(m_state1.txnId);
        checkNextStateValid(m_state2.txnId);
        checkNextStateNull();
        addTxnToQueue(m_state4);
        m_queue.noteTransactionRecievedAndReturnLastSeen(0, m_state4.txnId, true, m_state4.txnId);
        m_queue.noteTransactionRecievedAndReturnLastSeen(1, m_state4.txnId, true, m_state4.txnId);
        checkNextStateValid(m_state3.txnId);
        checkNextStateValid(m_state4.txnId);
        checkNextStateNull();
        addTxnToQueue(m_state6);
        m_queue.noteTransactionRecievedAndReturnLastSeen(0, m_state5.txnId, true, m_state5.txnId);
        m_queue.noteTransactionRecievedAndReturnLastSeen(1, m_state5.txnId, true, m_state5.txnId);
        checkNextStateValid(m_state5.txnId);
        checkNextStateNull();
    }

    /**
     * Provide a failure and then prune based on txnids.
     * @param initiatorId
     */
    public void simulateInitiatorFault(int initiatorId, long globalInitiationPoint) {
        m_queue.gotFaultForInitiator(1);
        Iterator<OrderableTransaction> iterator = m_queue.iterator();
        while (iterator.hasNext()) {
            TransactionState next = (TransactionState)iterator.next();

            // Execution site does something along these lines
            if (next.txnId > globalInitiationPoint &&
                next.initiatorSiteId == initiatorId)
            {
                iterator.remove();
            }
        }
    }

    // Setup the interleaved transactions from different initiators
    // Then remove initiator and see that the next states are valid
    public void testInitiatorRemoval1() {
        for (int i=0; i < m_states.length; ++i) {
            addTxnToQueue(m_states[i]);
        }

        assertEquals(m_queue.size(), 6);
        checkNextStateValid(m_txnIds.get(0));

        // the interesting part of the test
        simulateInitiatorFault(1, Long.MIN_VALUE);
        assertEquals(m_queue.size(), 2);
        checkNextStateValid(m_txnIds.get(2));
        checkNextStateValid(m_txnIds.get(4));
        checkNextStateNull();
    }

    // same as above but removing other initiator
    public void testInitiatorRemoval0() {
        for (int i=0; i < m_states.length; ++i) {
            addTxnToQueue(m_states[i]);
        }
        assertEquals(m_queue.size(), 6);
        checkNextStateValid(m_states[0].txnId);

        // the interesting part of the test
        simulateInitiatorFault(0, Long.MIN_VALUE);
        assertEquals(m_queue.size(), 3);
        checkNextStateValid(m_states[1].txnId);
        checkNextStateValid(m_states[3].txnId);
    }

    // setup the rpq. remove the first item's initiator.
    // verify that the next item in line is for a new
    // initiator and that its txnid is gt than the first
    // peek'ed result.
    public void testInitiatorRemovalFixesMinTxnId() {
        for (int i=0; i < m_states.length; ++i) {
            addTxnToQueue(m_states[i]);
        }
        assertEquals(m_queue.size(), 6);

        TransactionState peek = (TransactionState)m_queue.peek();
        simulateInitiatorFault(peek.initiatorSiteId, Long.MIN_VALUE);
        TransactionState peek2 = (TransactionState)m_queue.peek();
        assertTrue(peek != peek2);
        assertTrue(peek2.txnId > peek.txnId);

        assertEquals(m_queue.size(), 3);
    }

    // Check that adding an old message from the dead initiator is ok/ignored
    public void testMessageAddFromDeadInitiator() {
        m_queue.gotFaultForInitiator(0);
        addTxnToQueue(m_states[0]);
        addTxnToQueue(m_states[1]);
        assertEquals(1, m_queue.size());
        checkNextStateValid(m_states[1].txnId);
    }
}
