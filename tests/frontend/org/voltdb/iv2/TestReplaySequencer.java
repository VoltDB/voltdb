/* This file is part of VoltDB.
 * Copyright (C) 2008-2014 VoltDB Inc.
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

import org.voltcore.messaging.TransactionInfoBaseMessage;

import org.voltdb.StoredProcedureInvocation;
import org.voltdb.messaging.MultiPartitionParticipantMessage;

import junit.framework.TestCase;

import org.junit.Test;
import org.voltdb.messaging.CompleteTransactionMessage;
import org.voltdb.messaging.FragmentTaskMessage;
import org.voltdb.messaging.Iv2EndOfLogMessage;
import org.voltdb.messaging.Iv2InitiateTaskMessage;

public class TestReplaySequencer extends TestCase
{

    TransactionInfoBaseMessage makeIv2InitTask(long unused)
    {
        return makeIv2InitTask(unused, "Proc");
    }

    TransactionInfoBaseMessage makeIv2InitTask(long unused, String procName)
    {
        Iv2InitiateTaskMessage m = mock(Iv2InitiateTaskMessage.class);
        StoredProcedureInvocation invocation = mock(StoredProcedureInvocation.class);
        when(invocation.getProcName()).thenReturn(procName);
        when(m.getStoredProcedureInvocation()).thenReturn(invocation);
        when(m.isForReplay()).thenReturn(true);
        return m;
    }

    TransactionInfoBaseMessage makeSentinel(long unused)
    {
        MultiPartitionParticipantMessage m = mock(MultiPartitionParticipantMessage.class);
        when(m.isForReplay()).thenReturn(true);
        return m;
    }

    TransactionInfoBaseMessage makeFragment(long unused)
    {
        FragmentTaskMessage m = mock(FragmentTaskMessage.class);
        when(m.isForReplay()).thenReturn(true);
        return m;
    }

    TransactionInfoBaseMessage makeCompleteTxn(long unused)
    {
        CompleteTransactionMessage m = mock(CompleteTransactionMessage.class);
        when(m.isForReplay()).thenReturn(true);
        return m;
    }

    Iv2EndOfLogMessage makeEOL(boolean isMP)
    {
        Iv2EndOfLogMessage m = mock(Iv2EndOfLogMessage.class);
        when(m.isMP()).thenReturn(isMP);
        return m;
    }

    @Test
    public void testOfferSentinelThenFragment()
    {
        boolean result;
        ReplaySequencer dut = new ReplaySequencer();

        TransactionInfoBaseMessage sntl = makeSentinel(1L);
        TransactionInfoBaseMessage frag = makeFragment(1L);

        result = dut.offer(1L, sntl);
        assertEquals(true, result);
        assertEquals(null, dut.poll());

        result = dut.offer(1L, frag);
        assertEquals(true, result);
        assertEquals(frag, dut.poll());
        assertEquals(null, dut.poll());
        assertNull(dut.drain());
    }

    @Test
    public void testOfferSentinelThenFragments()
    {
        boolean result;
        ReplaySequencer dut = new ReplaySequencer();

        TransactionInfoBaseMessage sntl = makeSentinel(1L);
        TransactionInfoBaseMessage frag = makeFragment(1L);
        TransactionInfoBaseMessage frag2 = makeFragment(1L);

        result = dut.offer(1L, sntl);
        result = dut.offer(1L, frag);
        assertEquals(true, result);
        assertEquals(frag, dut.poll());
        assertEquals(null, dut.poll());

        // subsequent fragments won't block the queue.
        result = dut.offer(1L, frag2);
        assertEquals(false, result);
        assertEquals(null, dut.poll());
        assertNull(dut.drain());
    }

    @Test
    public void testOfferFragmentThenSentinel()
    {
        boolean result;
        ReplaySequencer dut = new ReplaySequencer();

        TransactionInfoBaseMessage sntl = makeSentinel(1L);
        TransactionInfoBaseMessage frag = makeFragment(1L);

        result = dut.offer(1L, frag);
        assertEquals(true, result);
        assertEquals(null, dut.poll());

        result = dut.offer(1L, sntl);
        assertEquals(true, result);
        assertEquals(frag, dut.poll());
        assertEquals(null, dut.poll());
        assertNull(dut.drain());
    }

    @Test
    public void testRejectedSP()
    {
        boolean result;
        ReplaySequencer dut = new ReplaySequencer();
        TransactionInfoBaseMessage m = makeIv2InitTask(2L);
        result = dut.offer(2L, m);
        assertEquals(false, result);
    }

    @Test
    public void testPollsInOrder()
    {
        TransactionInfoBaseMessage sntl1 = makeSentinel(1L);
        TransactionInfoBaseMessage frag1 = makeFragment(1L);
        TransactionInfoBaseMessage sp1a = makeIv2InitTask(100L);
        TransactionInfoBaseMessage sp1b = makeIv2InitTask(101L);
        TransactionInfoBaseMessage sp1c = makeIv2InitTask(102L);
        TransactionInfoBaseMessage sp1d = makeIv2InitTask(103L);

        TransactionInfoBaseMessage sntl2 = makeSentinel(2L);
        TransactionInfoBaseMessage frag2 = makeFragment(2L);
        TransactionInfoBaseMessage sp2a = makeIv2InitTask(104L);
        TransactionInfoBaseMessage sp2b = makeIv2InitTask(105L);
        TransactionInfoBaseMessage sp2c = makeIv2InitTask(106L);
        TransactionInfoBaseMessage sp2d = makeIv2InitTask(107L);

        ReplaySequencer dut = new ReplaySequencer();

        // offer all non-fragment work first..
        dut.offer(1L, sntl1);
        dut.offer(100L, sp1a);
        dut.offer(101L, sp1b);
        dut.offer(102L, sp1c);
        dut.offer(103L, sp1d);
        dut.offer(2L, sntl2);
        dut.offer(104L, sp2a);
        dut.offer(105L, sp2b);
        dut.offer(106L, sp2c);
        dut.offer(107L, sp2d);

        // Nothing satisified.
        assertEquals(null, dut.poll());
        // Nothing drained
        assertNull(dut.drain());

        // Offer the first fragment to free up the first half.
        dut.offer(1L, frag1);
        assertEquals(frag1, dut.poll());
        assertEquals(sp1a, dut.poll());
        assertEquals(sp1b, dut.poll());
        assertEquals(sp1c, dut.poll());
        assertEquals(sp1d, dut.poll());
        assertEquals(null, dut.poll());
        assertNull(dut.drain());

        // Offer the second fragment to free up the second half
        dut.offer(2L, frag2);
        assertEquals(frag2, dut.poll());
        assertEquals(sp2a, dut.poll());
        assertEquals(sp2b, dut.poll());
        assertEquals(sp2c, dut.poll());
        assertEquals(sp2d, dut.poll());
        assertEquals(null, dut.poll());
        assertNull(dut.drain());
    }

    @Test
    public void testPollsInOrder2()
    {
        TransactionInfoBaseMessage sntl1 = makeSentinel(1L);
        TransactionInfoBaseMessage frag1 = makeFragment(1L);

        TransactionInfoBaseMessage sntl2 = makeSentinel(2L);
        TransactionInfoBaseMessage frag2 = makeFragment(2L);
        TransactionInfoBaseMessage sp2a = makeIv2InitTask(104L);
        TransactionInfoBaseMessage sp2b = makeIv2InitTask(105L);

        ReplaySequencer dut = new ReplaySequencer();

        dut.offer(1L, sntl1);
        dut.offer(2L, sntl2);
        dut.offer(104L, sp2a);
        dut.offer(105L, sp2b);

        // Nothing satisified.
        assertEquals(null, dut.poll());
        assertNull(dut.drain());

        // Offer the first fragment to free up the first half.
        dut.offer(1L, frag1);
        assertEquals(frag1, dut.poll());
        assertEquals(null, dut.poll());
        assertNull(dut.drain());

        // Offer the second fragment to free up the second half
        dut.offer(2L, frag2);
        assertEquals(frag2, dut.poll());
        assertEquals(sp2a, dut.poll());
        assertEquals(sp2b, dut.poll());
        assertEquals(null, dut.poll());
        assertNull(dut.drain());
    }

    @Test
    public void testPollsInOrder3()
    {
        TransactionInfoBaseMessage frag1 = makeFragment(1L);
        TransactionInfoBaseMessage sp2a = makeIv2InitTask(104L);
        TransactionInfoBaseMessage sp2b = makeIv2InitTask(105L);
        TransactionInfoBaseMessage sntl1 = makeSentinel(1L);

        ReplaySequencer dut = new ReplaySequencer();

        // Offer fragment first, should be sequenced
        assertTrue(dut.offer(1L, frag1));
        // Offer SPs, should not be sequenced
        assertFalse(dut.offer(104L, sp2a));
        assertFalse(dut.offer(105L, sp2b));
        // Offer sentinel to free up the first fragment
        assertTrue(dut.offer(1L, sntl1));

        assertEquals(frag1, dut.poll());
        assertEquals(null, dut.poll());
        assertNull(dut.drain());
    }

    @Test
    public void testFragmentThenSetEOL()
    {
        boolean result;
        ReplaySequencer dut = new ReplaySequencer();

        TransactionInfoBaseMessage frag = makeFragment(1L);
        TransactionInfoBaseMessage frag2 = makeFragment(1L);
        TransactionInfoBaseMessage complete = makeCompleteTxn(1L);

        TransactionInfoBaseMessage frag3 = makeFragment(2L);

        result = dut.offer(1L, frag);
        assertTrue(result);
        assertNull(dut.poll());
        assertNull(dut.drain());

        assertTrue(dut.offer(0L, makeEOL(false)));
        assertEquals(frag, dut.poll());
        assertNull(dut.poll());
        assertNull(dut.drain());

        result = dut.offer(1L, frag2);
        assertFalse(result);
        assertNull(dut.poll());
        assertNull(dut.drain());

        result = dut.offer(1L, complete);
        assertFalse(result);
        assertNull(dut.poll());
        assertNull(dut.drain());

        // another txn
        result = dut.offer(2L, frag3);
        assertFalse(result);
        assertNull(dut.poll());
        assertNull(dut.drain());
    }

    @Test
    public void testSetEOLThanFragment()
    {
        boolean result;
        ReplaySequencer dut = new ReplaySequencer();

        TransactionInfoBaseMessage frag = makeFragment(1L);
        TransactionInfoBaseMessage frag2 = makeFragment(1L);
        TransactionInfoBaseMessage complete = makeCompleteTxn(1L);

        TransactionInfoBaseMessage frag3 = makeFragment(2L);

        assertTrue(dut.offer(0L, makeEOL(false)));

        result = dut.offer(1L, frag);
        assertFalse(result);
        assertNull(dut.poll());
        assertNull(dut.drain());

        result = dut.offer(1L, frag2);
        assertFalse(result);
        assertNull(dut.poll());
        assertNull(dut.drain());

        result = dut.offer(1L, complete);
        assertFalse(result);
        assertNull(dut.poll());
        assertNull(dut.drain());

        result = dut.offer(2L, frag3);
        assertFalse(result);
        assertNull(dut.poll());
        assertNull(dut.drain());
    }

    @Test
    public void testSentinelThenEOLThenFragment()
    {
        boolean result;
        ReplaySequencer dut = new ReplaySequencer();

        TransactionInfoBaseMessage sntl = makeSentinel(1L);
        TransactionInfoBaseMessage frag = makeFragment(1L);
        TransactionInfoBaseMessage frag2 = makeFragment(1L);
        TransactionInfoBaseMessage complete = makeCompleteTxn(1L);

        dut.offer(1L, sntl);

        assertTrue(dut.offer(0L, makeEOL(false)));

        result = dut.offer(1L, frag);
        assertTrue(result);
        assertEquals(frag, dut.poll());
        assertNull(dut.poll());
        assertNull(dut.drain());

        result = dut.offer(1L, frag2);
        assertFalse(result);
        assertNull(dut.poll());
        assertNull(dut.drain());

        result = dut.offer(1L, complete);
        assertFalse(result);
        assertNull(dut.poll());
        assertNull(dut.drain());
    }

    /**
     * If the first MP txn rolled back before this partition has executed the
     * first fragment, and the MPI sends out the first fragment of the next txn,
     * the replay sequencer should hold on to both txns until a sentinel or the
     * EOL arrives to release them in order.
     */
    @Test
    public void testTwoTxnsThenSetEOL()
    {
        boolean result;
        ReplaySequencer dut = new ReplaySequencer();

        TransactionInfoBaseMessage frag = makeFragment(1L);
        TransactionInfoBaseMessage complete = makeCompleteTxn(1L);

        TransactionInfoBaseMessage frag3 = makeFragment(2L);

        result = dut.offer(1L, frag);
        assertTrue(result);
        assertNull(dut.poll());
        assertNull(dut.drain());

        result = dut.offer(1L, complete);
        assertTrue(result);
        assertNull(dut.poll());
        assertNull(dut.drain());

        result = dut.offer(2L, frag3);
        assertTrue(result);
        assertNull(dut.poll());
        assertNull(dut.drain());

        assertTrue(dut.offer(0L, makeEOL(false)));

        assertEquals(frag, dut.poll());
        assertEquals(complete, dut.poll());
        assertEquals(frag3, dut.poll());
        assertNull(dut.poll());
        assertNull(dut.drain());
    }

    @Test
    public void testMPIEOLWithSentinels()
    {
        ReplaySequencer dut = new ReplaySequencer();

        TransactionInfoBaseMessage init1 = makeIv2InitTask(101L);
        TransactionInfoBaseMessage sentinel1 = makeSentinel(1L);
        TransactionInfoBaseMessage init2 = makeIv2InitTask(102L);
        TransactionInfoBaseMessage init3 = makeIv2InitTask(103L);

        assertFalse(dut.offer(101L, init1));
        assertNull(dut.poll());

        assertTrue(dut.offer(1L, sentinel1));
        assertNull(dut.poll());

        // SPs blocked by the sentinel
        assertTrue(dut.offer(102L, init2));
        assertTrue(dut.offer(103L, init3));
        assertNull(dut.poll());
        assertNull(dut.drain());

        // The outstanding sentinel and the MP EOL should
        // move us to the draining state
        assertTrue(dut.offer(0L, makeEOL(true)));

        // poll() should shut up and drain should start just
        // giving us everything
        assertNull(dut.poll());
        assertEquals(init2, dut.drain());
        assertEquals(init3, dut.drain());

        TransactionInfoBaseMessage init4 = makeIv2InitTask(104L);
        TransactionInfoBaseMessage sentinel2 = makeSentinel(2L);
        TransactionInfoBaseMessage init5 = makeIv2InitTask(105L);

        // These SPIs should be offered after sentinel1
        assertTrue(dut.offer(104L, init4));
        assertTrue(dut.offer(2L, sentinel2));
        assertTrue(dut.offer(105L, init5));
        assertNull(dut.poll());
        assertEquals(init4, dut.drain());
        assertEquals(init5, dut.drain());
        assertNull(dut.drain());
    }

    @Test
    public void testMPIEOLWithoutSentinels()
    {
        ReplaySequencer dut = new ReplaySequencer();

        TransactionInfoBaseMessage init1 = makeIv2InitTask(101L);
        TransactionInfoBaseMessage init2 = makeIv2InitTask(102L);
        TransactionInfoBaseMessage init3 = makeIv2InitTask(103L);

        assertFalse(dut.offer(101L, init1));
        assertFalse(dut.offer(102L, init2));
        assertFalse(dut.offer(103L, init3));
        assertNull(dut.poll());
        assertNull(dut.drain());

        assertTrue(dut.offer(0L, makeEOL(true)));

        TransactionInfoBaseMessage init4 = makeIv2InitTask(104L);
        TransactionInfoBaseMessage sentinel2 = makeSentinel(2L);
        TransactionInfoBaseMessage init5 = makeIv2InitTask(105L);

        assertFalse(dut.offer(104L, init4));
        // This will re-block us and should induce drain()
        assertTrue(dut.offer(2L, sentinel2));
        assertTrue(dut.offer(105L, init5));
        assertNull(dut.poll());
        assertEquals(init5, dut.drain());
        assertNull(dut.drain());
    }

    @Test
    public void testSPIEOLThenMPIEOL()
    {
        ReplaySequencer dut = new ReplaySequencer();

        TransactionInfoBaseMessage init1 = makeIv2InitTask(101L);
        TransactionInfoBaseMessage sentinel1 = makeSentinel(1L);
        TransactionInfoBaseMessage init2 = makeIv2InitTask(102L);
        TransactionInfoBaseMessage init3 = makeIv2InitTask(103L);
        TransactionInfoBaseMessage sentinel2 = makeSentinel(2L);
        TransactionInfoBaseMessage init4 = makeIv2InitTask(104L);
        TransactionInfoBaseMessage init5 = makeIv2InitTask(105L);

        assertFalse(dut.offer(101L, init1));
        assertNull(dut.poll());

        assertTrue(dut.offer(1L, sentinel1));
        assertNull(dut.poll());

        // SPs blocked by the sentinel
        assertTrue(dut.offer(102L, init2));
        assertTrue(dut.offer(103L, init3));
        assertNull(dut.poll());

        // SPs blocked by second sentinel
        assertTrue(dut.offer(2L, sentinel2));
        assertTrue(dut.offer(104L, init4));
        assertTrue(dut.offer(105L, init5));
        assertNull(dut.poll());
        assertNull(dut.drain());

        assertTrue(dut.offer(0L, makeEOL(false)));

        // SPI EOL shouldn't release blocked SPs
        assertNull(dut.poll());
        assertNull(dut.drain());

        TransactionInfoBaseMessage frag1 = makeFragment(1L);
        TransactionInfoBaseMessage complete1 = makeCompleteTxn(1L);

        // Offering the fragment and the complete releases init2 and init3
        assertTrue(dut.offer(1L, frag1));
        assertEquals(frag1, dut.poll());
        assertEquals(init2, dut.poll());
        assertEquals(init3, dut.poll());
        assertNull(dut.poll());
        assertNull(dut.drain());

        assertFalse(dut.offer(1L, complete1));
        assertNull(dut.poll());
        assertNull(dut.drain());

        // Move us to drain() mode
        assertTrue(dut.offer(0L, makeEOL(true)));

        // All blocked SPs should be released
        assertNull(dut.poll());
        assertEquals(init4, dut.drain());
        assertEquals(init5, dut.drain());
        assertNull(dut.drain());
    }

    /**
     * This tests multiple @LoadMultipartitionTable transactions with the same
     * txnId as DR does.
     */
    @Test
    public void testSentinelsAndFragsWithSameTxnId()
    {
        ReplaySequencer dut = new ReplaySequencer();

        TransactionInfoBaseMessage sntl = makeSentinel(1L);
        TransactionInfoBaseMessage frag = makeFragment(1L);
        TransactionInfoBaseMessage frag2 = makeFragment(1L);

        assertTrue(dut.offer(1L, sntl));
        assertTrue(dut.offer(1L, frag));
        assertEquals(frag, dut.poll());
        assertEquals(null, dut.poll());
        assertNull(dut.drain());

        // subsequent fragments won't block the queue.
        assertFalse(dut.offer(1L, frag2));
        assertEquals(null, dut.poll());
        assertNull(dut.drain());

        // Dupe sentinels and fragments
        assertTrue(dut.offer(1L, sntl));
        assertFalse(dut.offer(1L, frag));
        assertFalse(dut.offer(1L, frag2));
        assertEquals(null, dut.poll());
        assertNull(dut.drain());
    }

    @Test
    public void testDrain()
    {
        ReplaySequencer dut = new ReplaySequencer();

        // need
        // sp1
        // mp1
        // sp2
        // mp2
        // sp3
        // mp3
        // sp4

        TransactionInfoBaseMessage init1 = makeIv2InitTask(101L);
        TransactionInfoBaseMessage sentinel1 = makeSentinel(1L);
        TransactionInfoBaseMessage init2 = makeIv2InitTask(102L);
        TransactionInfoBaseMessage sentinel2 = makeSentinel(2L);
        TransactionInfoBaseMessage init3 = makeIv2InitTask(103L);
        TransactionInfoBaseMessage sentinel3 = makeSentinel(3L);
        TransactionInfoBaseMessage init4 = makeIv2InitTask(104L);

        assertFalse(dut.offer(101L, init1));
        assertTrue(dut.offer(1L, sentinel1));
        assertTrue(dut.offer(102L, init2));
        assertTrue(dut.offer(2L, sentinel2));
        assertTrue(dut.offer(103L, init3));
        assertTrue(dut.offer(3L, sentinel3));
        assertTrue(dut.offer(104L, init4));
        assertNull(dut.drain());
        // SPI EOL
        assertTrue(dut.offer(0L, makeEOL(false)));
        assertNull(dut.drain());
        // MPI EOL
        assertTrue(dut.offer(0L, makeEOL(true)));
        // Now, we need to be able to drain all of the SP inits out in order to respond IGNORING
        assertNull(dut.poll());
        assertEquals(init2, dut.drain());
        assertEquals(init3, dut.drain());
        assertEquals(init4, dut.drain());
        assertNull(dut.drain());
    }

    @Test
    public void testDrain2()
    {
        ReplaySequencer dut = new ReplaySequencer();

        // A bunch of sentinels before some SP transactions
        // We need drain to skip the 3 sentinels and get to the SPs.
        // need
        // mp1
        // mp2
        // mp3
        // sp1
        // sp2
        // sp3
        // sp4

        TransactionInfoBaseMessage init1 = makeIv2InitTask(101L);
        TransactionInfoBaseMessage sentinel1 = makeSentinel(1L);
        TransactionInfoBaseMessage init2 = makeIv2InitTask(102L);
        TransactionInfoBaseMessage sentinel2 = makeSentinel(2L);
        TransactionInfoBaseMessage init3 = makeIv2InitTask(103L);
        TransactionInfoBaseMessage sentinel3 = makeSentinel(3L);
        TransactionInfoBaseMessage init4 = makeIv2InitTask(104L);

        assertTrue(dut.offer(1L, sentinel1));
        assertTrue(dut.offer(2L, sentinel2));
        assertTrue(dut.offer(3L, sentinel3));
        assertTrue(dut.offer(101L, init1));
        assertTrue(dut.offer(102L, init2));
        assertTrue(dut.offer(103L, init3));
        assertTrue(dut.offer(104L, init4));
        assertNull(dut.drain());
        // SPI EOL
        assertTrue(dut.offer(0L, makeEOL(false)));
        assertNull(dut.drain());
        // MPI EOL
        assertTrue(dut.offer(0L, makeEOL(true)));
        // Now, we need to be able to drain all of the SP inits out in order to respond IGNORING
        assertNull(dut.poll());
        assertEquals(init1, dut.drain());
        assertEquals(init2, dut.drain());
        assertEquals(init3, dut.drain());
        assertEquals(init4, dut.drain());
        assertNull(dut.drain());
    }
    @Test
    public void testEarlyMPIEOL()
    {
        ReplaySequencer dut = new ReplaySequencer();

        // need
        // Fragment 1
        // Fragment 2
        // MPI EOL
        // Sentinel 1
        // Sentinel 2
        TransactionInfoBaseMessage frag1 = makeFragment(1L);
        TransactionInfoBaseMessage sentinel1 = makeSentinel(1L);
        TransactionInfoBaseMessage complete1 = makeCompleteTxn(1L);
        TransactionInfoBaseMessage frag2 = makeFragment(2L);
        TransactionInfoBaseMessage sentinel2 = makeSentinel(2L);
        TransactionInfoBaseMessage complete2 = makeCompleteTxn(2L);

        assertTrue(dut.offer(1L, frag1));
        assertTrue(dut.offer(1L, complete1));
        assertTrue(dut.offer(2L, frag2));
        assertTrue(dut.offer(2L, complete2));
        // We get a really early MPI EOL before we have any of our partition's sentinels
        assertTrue(dut.offer(0L, makeEOL(true)));
        assertTrue(dut.offer(1L, sentinel1));
        assertEquals(frag1, dut.poll());
        assertEquals(complete1, dut.poll());
        assertTrue(dut.offer(2L, sentinel2));
        assertEquals(frag2, dut.poll());
        assertEquals(complete2, dut.poll());
        assertNull(dut.poll());
        assertNull(dut.drain());
    }

    @Test
    public void testDupInitMsg()
    {
        ReplaySequencer dut = new ReplaySequencer();

        // simple deduping
        TransactionInfoBaseMessage init1 = makeIv2InitTask(1L);

        assertFalse(dut.offer(1L, init1));
        assertNotNull(dut.dedupe(1L, init1));
        assertTrue(dut.offer(1L, init1));
        assertNull(dut.poll());

        // dedupe with sentinels mixed
        TransactionInfoBaseMessage sntl1 = makeSentinel(2L);
        TransactionInfoBaseMessage init2 = makeIv2InitTask(3L);

        assertTrue(dut.offer(2L, sntl1));
        assertTrue(dut.offer(3L, init2));
        assertNotNull(dut.dedupe(3L, init2));
        assertTrue(dut.offer(3L, init2));
        assertNull(dut.poll());

        TransactionInfoBaseMessage frag1 = makeFragment(2L);

        assertTrue(dut.offer(2L, frag1));
        assertEquals(frag1, dut.poll());
        assertEquals(init2, dut.poll());
        assertNull(dut.poll());

        // dedupe with already polled
        assertNotNull(dut.dedupe(1L, init1));
        assertTrue(dut.offer(1L, init1));
        assertNull(dut.poll());
    }

    /**
     * No harm in sending duplicate sentinels
     */
    @Test
    public void testDupSentinels()
    {
        ReplaySequencer dut = new ReplaySequencer();

        TransactionInfoBaseMessage sntl1 = makeSentinel(1L);
        TransactionInfoBaseMessage frag1 = makeFragment(1L);
        TransactionInfoBaseMessage cmpl1 = makeCompleteTxn(1L);
        TransactionInfoBaseMessage init1 = makeIv2InitTask(2L);
        TransactionInfoBaseMessage init2 = makeIv2InitTask(3L);

        assertTrue(dut.offer(1L, sntl1));
        assertTrue(dut.offer(1L, frag1));
        assertTrue(dut.offer(2L, init1));
        assertEquals(frag1, dut.poll());
        assertEquals(init1, dut.poll());
        assertFalse(dut.offer(1L, cmpl1));
        assertNull(dut.poll());

        assertNull(dut.dedupe(1L, sntl1)); // don't care about sentinels
        assertTrue(dut.offer(1L, sntl1));
        assertFalse(dut.offer(3L, init2));
        assertNull(dut.poll());
    }

    @Test
    public void testAllowLoadTableWithSameTxnId()
    {
        ReplaySequencer dut = new ReplaySequencer();

        TransactionInfoBaseMessage init1 = makeIv2InitTask(1L, "@LoadSinglepartitionTable");
        TransactionInfoBaseMessage init2 = makeIv2InitTask(2L, "@LoadMultipartitionTable");

        assertFalse(dut.offer(1L, init1));
        assertNull(dut.dedupe(1L, init1));
        assertFalse(dut.offer(1L, init1));
        assertNull(dut.poll());

        assertFalse(dut.offer(2L, init2));
        assertNull(dut.dedupe(2L, init2));
        assertFalse(dut.offer(2L, init2));
        assertNull(dut.poll());
    }

    @Test
    public void testCompleteWithoutFirstFrag()
    {
        ReplaySequencer dut = new ReplaySequencer();

        TransactionInfoBaseMessage sntl = makeSentinel(1L);
        TransactionInfoBaseMessage frag = makeFragment(1L);
        TransactionInfoBaseMessage cmpl = makeCompleteTxn(1L);

        assertTrue(dut.offer(1L, sntl));
        // a restart complete arrives before the first fragment
        assertFalse(dut.offer(1L, cmpl));
        assertTrue(dut.offer(1L, frag));
        // this one should be queued
        assertTrue(dut.offer(1L, cmpl));

        assertEquals(frag, dut.poll());
        assertEquals(cmpl, dut.poll());
        assertNull(dut.poll());
    }
}

