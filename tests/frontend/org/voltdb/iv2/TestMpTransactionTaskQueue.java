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

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;

import org.junit.Test;

import junit.framework.TestCase;

public class TestMpTransactionTaskQueue extends TestCase
{
    MpProcedureTask makeTransactionTask(long txnid, boolean readOnly)
    {
        MpTransactionState state = mock(MpTransactionState.class);
        when(state.isReadOnly()).thenReturn(readOnly);
        MpProcedureTask task = mock(MpProcedureTask.class);
        when(task.getTransactionState()).thenReturn(state);
        when(task.getTxnId()).thenReturn(txnid);
        return task;
    }

    SiteTaskerQueue m_writeQueue;
    MpRoSitePool m_MPpool;
    MpTransactionTaskQueue m_dut;

    @Override
    public void setUp()
    {
        m_writeQueue = mock(SiteTaskerQueue.class);
        m_MPpool = mock(MpRoSitePool.class);
        // Accept work for a while
        when(m_MPpool.canAcceptWork()).thenReturn(true);
        m_dut = new MpTransactionTaskQueue(m_writeQueue);
        m_dut.setMpRoSitePool(m_MPpool);
    }

    // Test cases:

    // Reads will continue to emit until the MpRoSite pool says stop
    @Test
    public void testMultiReads()
    {
        // We'll keep handing reads to the pool until it tells us to stop
        TxnEgo txnId = TxnEgo.makeZero(MpInitiator.MP_INIT_PID);
        List<Long> activeTxns = new ArrayList<Long>();
        for (int i = 0; i < 100; i++) {
            txnId = txnId.makeNext();
            activeTxns.add(txnId.getTxnId());
            m_dut.offer(makeTransactionTask(txnId.getTxnId(), true));
            verify(m_MPpool).doWork(eq(txnId.getTxnId()), any(TransactionTask.class));
        }
        verify(m_MPpool, times(100)).doWork(anyLong(), any(TransactionTask.class));
        // Pool says no mas
        when(m_MPpool.canAcceptWork()).thenReturn(false);
        List<Long> delayedTxns = new ArrayList<Long>();
        for (int i = 0; i < 10; i++) {
            txnId = txnId.makeNext();
            m_dut.offer(makeTransactionTask(txnId.getTxnId(), true));
            delayedTxns.add(txnId.getTxnId());
            verify(m_MPpool, never()).doWork(eq(txnId.getTxnId()), any(TransactionTask.class));
        }
        // flush something and watch the delayed ones come out
        for (int i = 0; i < 10; i++) {
            // flush will cause the pool to be able to accept work before it
            // attempts to give it more, we'll have to fake it by telling the mock
            // to accept more before flush, then we'll test MpRoSitePool separately
            when(m_MPpool.canAcceptWork()).thenReturn(true);
            m_dut.flush(activeTxns.get(i));
            verify(m_MPpool).completeWork(activeTxns.get(i));
            verify(m_MPpool).doWork(eq(delayedTxns.get(i)), any(TransactionTask.class));
        }

    }

    // Single write completes before any more reads are executed but after pending reads finish
    @Test
    public void testReadWriteBlocking()
    {
        TxnEgo txnId = TxnEgo.makeZero(MpInitiator.MP_INIT_PID);
        // Offer a bunch of reads and see that they're sent to the pool
        Deque<Long> offeredReads = new ArrayDeque<Long>();
        for (int i = 0; i < 10; i++) {
            txnId = txnId.makeNext();
            offeredReads.push(txnId.getTxnId());
            m_dut.offer(makeTransactionTask(txnId.getTxnId(), true));
            verify(m_MPpool).doWork(eq(txnId.getTxnId()), any(TransactionTask.class));
        }
        // Offer a write and verify that it's not yet sent to the write queue
        txnId = txnId.makeNext();
        long writetxnid = txnId.getTxnId();
        m_dut.offer(makeTransactionTask(writetxnid, false));
        verify(m_writeQueue, never()).offer(any(TransactionTask.class));
        // Offer another read and verify that it's not sent to the pool
        txnId = txnId.makeNext();
        long readtxnid = txnId.getTxnId();
        m_dut.offer(makeTransactionTask(readtxnid, true));
        verify(m_MPpool, never()).doWork(eq(readtxnid), any(TransactionTask.class));
        // Now flush the first set of reads, make sure we don't offer the write or read early
        for (int i = 0; i < 10; i++) {
            verify(m_writeQueue, never()).offer(any(TransactionTask.class));
            verify(m_MPpool, never()).doWork(eq(readtxnid), any(TransactionTask.class));
            long txnid = offeredReads.pop();
            m_dut.flush(txnid);
            verify(m_MPpool).completeWork(txnid);
        }
        // the write should come out now, but not the read
        verify(m_writeQueue).offer(any(TransactionTask.class));
        verify(m_MPpool, never()).doWork(eq(readtxnid), any(TransactionTask.class));
        // add another read, see that it doesn't come out either
        txnId = txnId.makeNext();
        long readtxnid2 = txnId.getTxnId();
        m_dut.offer(makeTransactionTask(readtxnid2, true));
        verify(m_MPpool, never()).doWork(eq(readtxnid2), any(TransactionTask.class));
        // now flush the write and verify that the reads emerge
        m_dut.flush(writetxnid);
        verify(m_MPpool).doWork(eq(readtxnid), any(TransactionTask.class));
        verify(m_MPpool).doWork(eq(readtxnid2), any(TransactionTask.class));
    }
}
