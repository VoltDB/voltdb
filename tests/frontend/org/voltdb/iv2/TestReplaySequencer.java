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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.voltcore.messaging.TransactionInfoBaseMessage;

import org.voltdb.messaging.MultiPartitionParticipantMessage;

import junit.framework.TestCase;

import org.junit.Test;
import org.voltdb.messaging.CompleteTransactionMessage;
import org.voltdb.messaging.FragmentTaskMessage;
import org.voltdb.messaging.Iv2InitiateTaskMessage;

public class TestReplaySequencer extends TestCase
{

    TransactionInfoBaseMessage makeIv2InitTask(long unused)
    {
        Iv2InitiateTaskMessage m = mock(Iv2InitiateTaskMessage.class);
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

        // Offer the first fragment to free up the first half.
        dut.offer(1L, frag1);
        assertEquals(frag1, dut.poll());
        assertEquals(sp1a, dut.poll());
        assertEquals(sp1b, dut.poll());
        assertEquals(sp1c, dut.poll());
        assertEquals(sp1d, dut.poll());
        assertEquals(null, dut.poll());

        // Offer the second fragment to free up the second half
        dut.offer(2L, frag2);
        assertEquals(frag2, dut.poll());
        assertEquals(sp2a, dut.poll());
        assertEquals(sp2b, dut.poll());
        assertEquals(sp2c, dut.poll());
        assertEquals(sp2d, dut.poll());
        assertEquals(null, dut.poll());
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

        // Offer the first fragment to free up the first half.
        dut.offer(1L, frag1);
        assertEquals(frag1, dut.poll());
        assertEquals(null, dut.poll());

        // Offer the second fragment to free up the second half
        dut.offer(2L, frag2);
        assertEquals(frag2, dut.poll());
        assertEquals(sp2a, dut.poll());
        assertEquals(sp2b, dut.poll());
        assertEquals(null, dut.poll());
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

        dut.setEOLReached();
        assertEquals(frag, dut.poll());
        assertNull(dut.poll());

        result = dut.offer(1L, frag2);
        assertFalse(result);
        assertNull(dut.poll());

        result = dut.offer(1L, complete);
        assertFalse(result);
        assertNull(dut.poll());

        // another txn
        result = dut.offer(2L, frag3);
        assertFalse(result);
        assertNull(dut.poll());
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

        dut.setEOLReached();

        result = dut.offer(1L, frag);
        assertFalse(result);
        assertNull(dut.poll());

        result = dut.offer(1L, frag2);
        assertFalse(result);
        assertNull(dut.poll());

        result = dut.offer(1L, complete);
        assertFalse(result);
        assertNull(dut.poll());

        result = dut.offer(2L, frag3);
        assertFalse(result);
        assertNull(dut.poll());
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

        dut.setEOLReached();

        result = dut.offer(1L, frag);
        assertTrue(result);
        assertEquals(frag, dut.poll());
        assertNull(dut.poll());

        result = dut.offer(1L, frag2);
        assertFalse(result);
        assertNull(dut.poll());

        result = dut.offer(1L, complete);
        assertFalse(result);
        assertNull(dut.poll());
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

        result = dut.offer(1L, complete);
        assertTrue(result);
        assertNull(dut.poll());

        result = dut.offer(2L, frag3);
        assertTrue(result);
        assertNull(dut.poll());

        dut.setEOLReached();

        assertEquals(frag, dut.poll());
        assertEquals(complete, dut.poll());
        assertEquals(frag3, dut.poll());
        assertNull(dut.poll());
    }
}

