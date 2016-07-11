/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import junit.framework.TestCase;

import org.junit.Test;

public class TestMpTransactionTaskQueue extends TestCase
{
    MpProcedureTask makeTransactionTask(long txnid, boolean readOnly, Map<Integer, Long> sitemap)
    {
        MpTransactionState state = mock(MpTransactionState.class);
        when(state.isReadOnly()).thenReturn(readOnly);
        when(state.getMasterHSIds()).thenReturn(sitemap);
        MpProcedureTask task = mock(MpProcedureTask.class);
        when(task.getTransactionState()).thenReturn(state);
        when(task.getTxnId()).thenReturn(txnid);
        return task;
    }

    Map<Integer, Long> generateSiteMap(Integer [] sites, Long id) {
        Map<Integer, Long> map = new HashMap<Integer, Long>();
        for(int i=0; i < sites.length; i++) {
            map.put(sites[i], id);
        }
        return map;
    }

    SiteTaskerQueue m_writeQueue;
    MpSitePool m_MPpool;
    MpTransactionTaskQueue m_dut;
    TxnEgo txnId;

    @Override
    public void setUp()
    {
        m_writeQueue = mock(SiteTaskerQueue.class);
        m_MPpool = mock(MpSitePool.class);
        // Accept work for a while
        when(m_MPpool.canAcceptWork()).thenReturn(true);
        m_dut = new MpTransactionTaskQueue(m_writeQueue,TxnEgo.makeZero(MpInitiator.MP_INIT_PID).getTxnId());
        m_dut.setMpRoSitePool(m_MPpool);
        txnId = TxnEgo.makeZero(MpInitiator.MP_INIT_PID);
    }

    private long offerTransaction(boolean read, Integer [] sites) {
        txnId = txnId.makeNext();
        long m_txnid = txnId.getTxnId();
        m_dut.offer(makeTransactionTask(m_txnid, read, generateSiteMap(sites, m_txnid)));
        System.out.println("Offered " + m_txnid + " " + read);
        return m_txnid;
    }

    // Test cases:

    // Reads will continue to emit until the MpRoSite pool says stop
    @Test
    public void testMultiReads()
    {
        // We'll keep handing reads to the pool until it tells us to stop
        List<Long> activeTxns = new ArrayList<Long>();
        for (int i = 0; i < 100; i++) {
            txnId = txnId.makeNext();
            activeTxns.add(txnId.getTxnId());
            m_dut.offer(makeTransactionTask(txnId.getTxnId(), true, generateSiteMap(new Integer [] {1,2}, txnId.getTxnId())));
            verify(m_MPpool).doWork(eq(txnId.getTxnId()), any(TransactionTask.class));
        }
        verify(m_MPpool, times(100)).doWork(anyLong(), any(TransactionTask.class));
        // Pool says no mas
        when(m_MPpool.canAcceptWork()).thenReturn(false);
        List<Long> delayedTxns = new ArrayList<Long>();
        for (int i = 0; i < 10; i++) {
            txnId = txnId.makeNext();
            m_dut.offer(makeTransactionTask(txnId.getTxnId(), true, generateSiteMap(new Integer [] {1,2,3}, txnId.getTxnId())));
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
        // Offer a bunch of reads and see that they're sent to the pool
        Deque<Long> offeredReads = new ArrayDeque<Long>();
        Integer [] sites = new Integer [] {1, 2};
        for (int i = 0; i < 10; i++) {
            long m_txnid = offerTransaction(true,sites);
            offeredReads.push(m_txnid);
            verify(m_MPpool).doWork(eq(m_txnid), any(TransactionTask.class));
        }
        // Offer a write and verify that it's not yet sent to the write queue
        long writetxnid = offerTransaction(false,sites);
        verify(m_writeQueue, never()).offer(any(TransactionTask.class));
        // Offer another read and verify that it's not sent to the pool
        long readtxnid = offerTransaction(true,sites);
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
        long readtxnid2 = offerTransaction(true,sites);
        verify(m_MPpool, never()).doWork(eq(readtxnid2), any(TransactionTask.class));
        // now flush the write and verify that the reads emerge
        m_dut.flush(writetxnid);
        verify(m_MPpool).doWork(eq(readtxnid), any(TransactionTask.class));
        verify(m_MPpool).doWork(eq(readtxnid2), any(TransactionTask.class));
    }

    // Reads and Writes can execute concurrently, as long as their sites don't intersect
    @Test
    public void testReadWriteNoBlocking()
    {
        // Offer a bunch of reads and see that they're sent to the pool
        Deque<Long> offeredReads = new ArrayDeque<Long>();
        Integer [] sites = new Integer [] {1, 2};
        for (int i = 0; i < 3; i++) {
            long m_txnid = offerTransaction(true,sites);
            offeredReads.push(m_txnid);
            verify(m_MPpool).doWork(eq(m_txnid), any(TransactionTask.class));
        }
        // Offer a write and verify that it's not yet sent to the write queue
        long writetxnid = offerTransaction(false,sites);
        verify(m_writeQueue, never()).offer(any(TransactionTask.class));
        // Offer another read and verify that it's not sent to the pool
        long readtxnid = offerTransaction(true,sites);
        verify(m_MPpool, never()).doWork(eq(readtxnid), any(TransactionTask.class));
        // Offer a write and verify that it's not yet sent to the write queue (FCFS ordering!)
        long writetxnid2 = offerTransaction(false,sites);
        verify(m_writeQueue, never()).offer(any(TransactionTask.class));
        // Now flush the first set of reads, make sure we don't offer the writes or read early
        for (int i = 0; i < 3; i++) {
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
        long readtxnid2 = offerTransaction(true,sites);
        verify(m_MPpool, never()).doWork(eq(readtxnid2), any(TransactionTask.class));
        // now flush the write and verify that the reads emerge
        m_dut.flush(writetxnid);
        verify(m_MPpool).doWork(eq(readtxnid), any(TransactionTask.class));
        verify(m_MPpool, never()).doWork(eq(readtxnid2), any(TransactionTask.class));
        m_dut.flush(readtxnid);
        m_dut.flush(writetxnid2);
        verify(m_MPpool).doWork(eq(readtxnid2), any(TransactionTask.class));
    }

    // Reads and Writes can execute concurrently, as long as their sites don't intersect
    @Test
    public void testConcurrentReadWrite()
    {
        Deque<Long> offeredReads = new ArrayDeque<Long>();
        Deque<Long> offeredWrites = new ArrayDeque<Long>();
        Integer [] sitesA = new Integer [] {1, 3};
        Integer [] sitesB = new Integer [] {2, 4};
        Integer [] sitesAB = new Integer [] {1, 2, 3, 4};
        long m_txnid;

        // R1 Read from SitesA, W1 Write to SitesB, both should succeed
        m_txnid = offerTransaction(true, sitesA);
        offeredReads.addLast(m_txnid);
        verify(m_MPpool).doWork(eq(m_txnid), any(TransactionTask.class));

        m_txnid = offerTransaction(false, sitesB);
        offeredWrites.addLast(m_txnid);
        verify(m_writeQueue).offer(any(TransactionTask.class));

        // R2 Read from SitesA (should succeed), R3 Read from SitesB (should wait)
        m_txnid = offerTransaction(true, sitesA);
        offeredReads.addLast(m_txnid);
        verify(m_MPpool).doWork(eq(m_txnid), any(TransactionTask.class));

        m_txnid = offerTransaction(true, sitesB);
        offeredReads.addLast(m_txnid);
        verify(m_MPpool, never()).doWork(eq(m_txnid), any(TransactionTask.class));

        // W2 Write to SitesA, W3 Write to Sites B (should wait)
        m_txnid = offerTransaction(false, sitesA);
        offeredWrites.addLast(m_txnid);
        verify(m_writeQueue).offer(any(TransactionTask.class));

        m_txnid = offerTransaction(false, sitesB);
        offeredWrites.addLast(m_txnid);
        verify(m_writeQueue).offer(any(TransactionTask.class));

        // R4 Read from SitesA (should wait)
        m_txnid = offerTransaction(true, sitesA);
        offeredReads.addLast(m_txnid);
        verify(m_MPpool, never()).doWork(eq(m_txnid), any(TransactionTask.class));

        // R5 Read SitesAB (should wait)
        m_txnid = offerTransaction(true, sitesAB);
        offeredReads.addLast(m_txnid);
        verify(m_MPpool, never()).doWork(eq(m_txnid), any(TransactionTask.class));

        // W4 Write SitesB (should wait)
        m_txnid = offerTransaction(false, sitesB);
        offeredWrites.addLast(m_txnid);
        verify(m_writeQueue).offer(any(TransactionTask.class));

        // R6 Read SitesA (should wait)
        m_txnid = offerTransaction(true, sitesA);
        offeredReads.addLast(m_txnid);
        verify(m_MPpool, never()).doWork(eq(m_txnid), any(TransactionTask.class));

        // Flush R1, W1, R2: R3, W2 should continue, all others should wait
        m_txnid = offeredReads.removeFirst();
        System.out.println("Flushing " + m_txnid);
        m_dut.flush(m_txnid);
        verify(m_MPpool).completeWork(m_txnid);

        m_txnid = offeredReads.removeFirst();
        m_dut.flush(m_txnid);
        verify(m_MPpool).completeWork(m_txnid);

        m_txnid = offeredWrites.removeFirst();
        System.out.println("Flushing " + m_txnid);
        m_dut.flush(m_txnid);

        verify(m_MPpool).doWork(eq(offeredReads.peekFirst()), any(TransactionTask.class));
        verify(m_writeQueue, times(2)).offer(any(TransactionTask.class));

        // Flush R3: W3 should continue, others should wait

        m_txnid = offeredReads.removeFirst();
        m_dut.flush(m_txnid);
        verify(m_MPpool).completeWork(m_txnid);

        verify(m_writeQueue, times(3)).offer(any(TransactionTask.class));

        // Flush W2: R4 should continue

        m_txnid = offeredWrites.removeFirst();
        m_dut.flush(m_txnid);

        verify(m_MPpool).doWork(eq(offeredReads.peekFirst()), any(TransactionTask.class));

        // Flush W3: R5 should continue

        m_txnid = offeredWrites.removeFirst();
        m_dut.flush(m_txnid);

        verify(m_MPpool).doWork(eq(offeredReads.peekFirst()), any(TransactionTask.class));

        // Flush R4, R5: W4, R6 should continue

        m_txnid = offeredReads.removeFirst();
        m_dut.flush(m_txnid);
        verify(m_MPpool).completeWork(m_txnid);

        verify(m_MPpool).doWork(eq(offeredReads.peekFirst()), any(TransactionTask.class));

        m_txnid = offeredReads.removeFirst();
        m_dut.flush(m_txnid);
        verify(m_MPpool).completeWork(m_txnid);

        verify(m_MPpool).doWork(eq(offeredReads.peekFirst()), any(TransactionTask.class));
        verify(m_writeQueue, times(4)).offer(any(TransactionTask.class));

    }
}
