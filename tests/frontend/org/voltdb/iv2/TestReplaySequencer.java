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

import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.Assert;
import org.junit.Test;
import org.voltcore.messaging.TransactionInfoBaseMessage;
import org.voltdb.StoredProcedureInvocation;
import org.voltdb.messaging.CompleteTransactionMessage;
import org.voltdb.messaging.FragmentTaskMessage;
import org.voltdb.messaging.Iv2EndOfLogMessage;
import org.voltdb.messaging.Iv2InitiateTaskMessage;
import org.voltdb.messaging.MultiPartitionParticipantMessage;

public class TestReplaySequencer {

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

    Iv2EndOfLogMessage makeMPIEOL()
    {
        Iv2EndOfLogMessage m = mock(Iv2EndOfLogMessage.class);
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
        Assert.assertEquals(true, result);
        Assert.assertEquals(null, dut.poll());

        result = dut.offer(1L, frag);
        Assert.assertEquals(true, result);
        Assert.assertEquals(frag, dut.poll());
        Assert.assertEquals(null, dut.poll());
        Assert.assertNull(dut.drain());
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
        try { dut.dump(1); } catch (Exception e) { fail(e.getMessage()); } // toString should not throw
        result = dut.offer(1L, frag);
        Assert.assertEquals(true, result);
        Assert.assertEquals(frag, dut.poll());
        Assert.assertEquals(null, dut.poll());

        // subsequent fragments won't block the queue.
        result = dut.offer(1L, frag2);
        Assert.assertEquals(false, result);
        Assert.assertEquals(null, dut.poll());
        Assert.assertNull(dut.drain());
    }

    @Test
    public void testOfferFragmentThenSentinel()
    {
        boolean result;
        ReplaySequencer dut = new ReplaySequencer();

        TransactionInfoBaseMessage sntl = makeSentinel(1L);
        TransactionInfoBaseMessage frag = makeFragment(1L);

        result = dut.offer(1L, frag);
        Assert.assertEquals(true, result);
        Assert.assertEquals(null, dut.poll());
        try { dut.dump(1); } catch (Exception e) { fail(e.getMessage()); } // toString should not throw

        result = dut.offer(1L, sntl);
        Assert.assertEquals(true, result);
        Assert.assertEquals(frag, dut.poll());
        Assert.assertEquals(null, dut.poll());
        Assert.assertNull(dut.drain());
    }

    @Test
    public void testRejectedSP()
    {
        boolean result;
        ReplaySequencer dut = new ReplaySequencer();
        TransactionInfoBaseMessage m = makeIv2InitTask(2L);
        result = dut.offer(2L, m);
        Assert.assertEquals(false, result);
    }

    @Test
    public void testPollsInOrder()
    {
        TransactionInfoBaseMessage sntl1 = makeSentinel(1L);
        TransactionInfoBaseMessage frag1 = makeFragment(1L);
        TransactionInfoBaseMessage cmpl1 = makeCompleteTxn(1L);

        TransactionInfoBaseMessage sp1a = makeIv2InitTask(100L);
        TransactionInfoBaseMessage sp1b = makeIv2InitTask(101L);
        TransactionInfoBaseMessage sp1c = makeIv2InitTask(102L);
        TransactionInfoBaseMessage sp1d = makeIv2InitTask(103L);

        TransactionInfoBaseMessage sntl2 = makeSentinel(2L);
        TransactionInfoBaseMessage frag2 = makeFragment(2L);
        TransactionInfoBaseMessage cmpl2 = makeCompleteTxn(2L);

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
        Assert.assertEquals(null, dut.poll());
        // Nothing drained
        Assert.assertNull(dut.drain());

        // Offer the first fragment to free up the first half.
        dut.offer(1L, frag1);
        dut.offer(1L, cmpl1);
        Assert.assertEquals(frag1, dut.poll());
        Assert.assertEquals(cmpl1, dut.poll());
        Assert.assertEquals(sp1a, dut.poll());
        Assert.assertEquals(sp1b, dut.poll());
        Assert.assertEquals(sp1c, dut.poll());
        Assert.assertEquals(sp1d, dut.poll());
        Assert.assertEquals(null, dut.poll());
        Assert.assertNull(dut.drain());

        // Offer the second fragment to free up the second half
        dut.offer(2L, frag2);
        dut.offer(2L, cmpl2);
        Assert.assertEquals(frag2, dut.poll());
        Assert.assertEquals(cmpl2, dut.poll());
        Assert.assertEquals(sp2a, dut.poll());
        Assert.assertEquals(sp2b, dut.poll());
        Assert.assertEquals(sp2c, dut.poll());
        Assert.assertEquals(sp2d, dut.poll());
        Assert.assertEquals(null, dut.poll());
        Assert.assertNull(dut.drain());
    }

    @Test
    public void testPollsInOrder2()
    {
        TransactionInfoBaseMessage sntl1 = makeSentinel(1L);
        TransactionInfoBaseMessage frag1 = makeFragment(1L);
        TransactionInfoBaseMessage cmpl1 = makeCompleteTxn(1L);

        TransactionInfoBaseMessage sntl2 = makeSentinel(2L);
        TransactionInfoBaseMessage frag2 = makeFragment(2L);
        TransactionInfoBaseMessage cmpl2 = makeCompleteTxn(2L);

        TransactionInfoBaseMessage sp2a = makeIv2InitTask(104L);
        TransactionInfoBaseMessage sp2b = makeIv2InitTask(105L);

        ReplaySequencer dut = new ReplaySequencer();

        dut.offer(1L, sntl1);
        dut.offer(2L, sntl2);
        dut.offer(104L, sp2a);
        dut.offer(105L, sp2b);

        // Nothing satisified.
        Assert.assertEquals(null, dut.poll());
        Assert.assertNull(dut.drain());

        // Offer the first fragment to free up the first half.
        dut.offer(1L, frag1);
        dut.offer(1L, cmpl1);
        Assert.assertEquals(frag1, dut.poll());
        Assert.assertEquals(cmpl1, dut.poll());
        Assert.assertEquals(null, dut.poll());
        Assert.assertNull(dut.drain());

        // Offer the second fragment to free up the second half
        dut.offer(2L, frag2);
        Assert.assertEquals(frag2, dut.poll());
        Assert.assertEquals(null, dut.poll());

        // Completed the second mp to free up the rests
        dut.offer(2L, cmpl2);

        Assert.assertEquals(sp2a, dut.poll());
        Assert.assertEquals(sp2b, dut.poll());
        Assert.assertEquals(null, dut.poll());
        Assert.assertNull(dut.drain());
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
        Assert.assertTrue(dut.offer(1L, frag1));
        // Offer SPs, should not be sequenced
        Assert.assertFalse(dut.offer(104L, sp2a));
        Assert.assertFalse(dut.offer(105L, sp2b));
        // Offer sentinel to free up the first fragment
        Assert.assertTrue(dut.offer(1L, sntl1));

        Assert.assertEquals(frag1, dut.poll());
        Assert.assertEquals(null, dut.poll());
        Assert.assertNull(dut.drain());
    }

    @Test
    public void testSentinelThenFragment()
    {
        boolean result;
        ReplaySequencer dut = new ReplaySequencer();

        TransactionInfoBaseMessage sntl = makeSentinel(1L);
        TransactionInfoBaseMessage frag = makeFragment(1L);
        TransactionInfoBaseMessage frag2 = makeFragment(1L);
        TransactionInfoBaseMessage complete = makeCompleteTxn(1L);

        dut.offer(1L, sntl);

        result = dut.offer(1L, frag);
        Assert.assertTrue(result);
        Assert.assertEquals(frag, dut.poll());
        Assert.assertNull(dut.poll());
        Assert.assertNull(dut.drain());

        result = dut.offer(1L, frag2);
        Assert.assertFalse(result);
        Assert.assertNull(dut.poll());
        Assert.assertNull(dut.drain());

        result = dut.offer(1L, complete);
        Assert.assertFalse(result);
        Assert.assertNull(dut.poll());
        Assert.assertNull(dut.drain());
    }

    /**
     * If the first MP txn rolled back before this partition has executed the
     * first fragment, and the MPI sends out the first fragment of the next txn,
     * the replay sequencer should hold on to both txns until a sentinel arrives
     * to release them in order.
     */
    @Test
    public void testTwoTxnsThenSentinels()
    {
        boolean result;
        ReplaySequencer dut = new ReplaySequencer();

        TransactionInfoBaseMessage frag = makeFragment(1L);
        TransactionInfoBaseMessage complete = makeCompleteTxn(1L);

        TransactionInfoBaseMessage frag3 = makeFragment(2L);

        result = dut.offer(1L, frag);
        Assert.assertTrue(result);
        Assert.assertNull(dut.poll());
        Assert.assertNull(dut.drain());

        result = dut.offer(1L, complete);
        Assert.assertTrue(result);
        Assert.assertNull(dut.poll());
        Assert.assertNull(dut.drain());

        result = dut.offer(2L, frag3);
        Assert.assertTrue(result);
        Assert.assertNull(dut.poll());
        Assert.assertNull(dut.drain());

        Assert.assertTrue(dut.offer(1L, makeSentinel(1L)));

        Assert.assertEquals(frag, dut.poll());
        Assert.assertEquals(complete, dut.poll());
        Assert.assertNull(dut.poll());

        Assert.assertTrue(dut.offer(2L, makeSentinel(2L)));
        Assert.assertEquals(frag3, dut.poll());
        Assert.assertNull(dut.poll());
        Assert.assertNull(dut.drain());
    }

    @Test
    public void testMPIEOLWithSentinels()
    {
        ReplaySequencer dut = new ReplaySequencer();

        TransactionInfoBaseMessage init1 = makeIv2InitTask(101L);
        TransactionInfoBaseMessage sentinel1 = makeSentinel(1L);
        TransactionInfoBaseMessage init2 = makeIv2InitTask(102L);
        TransactionInfoBaseMessage init3 = makeIv2InitTask(103L);

        Assert.assertFalse(dut.offer(101L, init1));
        Assert.assertNull(dut.poll());

        Assert.assertTrue(dut.offer(1L, sentinel1));
        Assert.assertNull(dut.poll());

        // SPs blocked by the sentinel
        Assert.assertTrue(dut.offer(102L, init2));
        Assert.assertTrue(dut.offer(103L, init3));
        Assert.assertNull(dut.poll());
        Assert.assertNull(dut.drain());

        // The outstanding sentinel and the MP EOL should
        // move us to the draining state
        Assert.assertTrue(dut.offer(0L, makeMPIEOL()));

        // poll() should shut up and drain should start just
        // giving us everything
        Assert.assertNull(dut.poll());
        Assert.assertEquals(init2, dut.drain());
        Assert.assertEquals(init3, dut.drain());

        TransactionInfoBaseMessage init4 = makeIv2InitTask(104L);
        TransactionInfoBaseMessage sentinel2 = makeSentinel(2L);
        TransactionInfoBaseMessage init5 = makeIv2InitTask(105L);

        // These SPIs should be offered after sentinel1
        Assert.assertTrue(dut.offer(104L, init4));
        Assert.assertTrue(dut.offer(2L, sentinel2));
        Assert.assertTrue(dut.offer(105L, init5));
        Assert.assertNull(dut.poll());
        Assert.assertEquals(init4, dut.drain());
        Assert.assertEquals(init5, dut.drain());
        Assert.assertNull(dut.drain());
    }

    @Test
    public void testMPIEOLWithoutSentinels()
    {
        ReplaySequencer dut = new ReplaySequencer();

        TransactionInfoBaseMessage init1 = makeIv2InitTask(101L);
        TransactionInfoBaseMessage init2 = makeIv2InitTask(102L);
        TransactionInfoBaseMessage init3 = makeIv2InitTask(103L);

        Assert.assertFalse(dut.offer(101L, init1));
        Assert.assertFalse(dut.offer(102L, init2));
        Assert.assertFalse(dut.offer(103L, init3));
        Assert.assertNull(dut.poll());
        Assert.assertNull(dut.drain());

        Assert.assertTrue(dut.offer(0L, makeMPIEOL()));

        TransactionInfoBaseMessage init4 = makeIv2InitTask(104L);
        TransactionInfoBaseMessage sentinel2 = makeSentinel(2L);
        TransactionInfoBaseMessage init5 = makeIv2InitTask(105L);

        Assert.assertFalse(dut.offer(104L, init4));
        // This will re-block us and should induce drain()
        Assert.assertTrue(dut.offer(2L, sentinel2));
        Assert.assertTrue(dut.offer(105L, init5));
        Assert.assertNull(dut.poll());
        Assert.assertEquals(init5, dut.drain());
        Assert.assertNull(dut.drain());
    }

    @Test
    public void testMPIEOL()
    {
        ReplaySequencer dut = new ReplaySequencer();

        TransactionInfoBaseMessage init1 = makeIv2InitTask(101L);
        TransactionInfoBaseMessage sentinel1 = makeSentinel(1L);
        TransactionInfoBaseMessage init2 = makeIv2InitTask(102L);
        TransactionInfoBaseMessage init3 = makeIv2InitTask(103L);
        TransactionInfoBaseMessage sentinel2 = makeSentinel(2L);
        TransactionInfoBaseMessage init4 = makeIv2InitTask(104L);
        TransactionInfoBaseMessage init5 = makeIv2InitTask(105L);

        Assert.assertFalse(dut.offer(101L, init1));
        Assert.assertNull(dut.poll());

        Assert.assertTrue(dut.offer(1L, sentinel1));
        Assert.assertNull(dut.poll());

        // SPs blocked by the sentinel
        Assert.assertTrue(dut.offer(102L, init2));
        Assert.assertTrue(dut.offer(103L, init3));
        Assert.assertNull(dut.poll());

        // SPs blocked by second sentinel
        Assert.assertTrue(dut.offer(2L, sentinel2));
        Assert.assertTrue(dut.offer(104L, init4));
        Assert.assertTrue(dut.offer(105L, init5));
        Assert.assertNull(dut.poll());
        Assert.assertNull(dut.drain());

        TransactionInfoBaseMessage frag1 = makeFragment(1L);
        TransactionInfoBaseMessage complete1 = makeCompleteTxn(1L);

        // Offering the fragment and the complete releases init2 and init3
        Assert.assertTrue(dut.offer(1L, frag1));
        Assert.assertEquals(frag1, dut.poll());
        Assert.assertNull(dut.poll());
        Assert.assertFalse(dut.offer(1L, complete1));
        Assert.assertEquals(init2, dut.poll());
        Assert.assertEquals(init3, dut.poll());
        Assert.assertNull(dut.poll());
        Assert.assertNull(dut.drain());

        // Move us to drain() mode
        Assert.assertTrue(dut.offer(0L, makeMPIEOL()));

        // All blocked SPs should be released
        Assert.assertNull(dut.poll());
        Assert.assertEquals(init4, dut.drain());
        Assert.assertEquals(init5, dut.drain());
        Assert.assertNull(dut.drain());
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

        Assert.assertTrue(dut.offer(1L, sntl));
        Assert.assertTrue(dut.offer(1L, frag));
        Assert.assertEquals(frag, dut.poll());
        Assert.assertEquals(null, dut.poll());
        Assert.assertNull(dut.drain());

        // subsequent fragments won't block the queue.
        Assert.assertFalse(dut.offer(1L, frag2));
        Assert.assertEquals(null, dut.poll());
        Assert.assertNull(dut.drain());

        // Dupe sentinels and fragments
        Assert.assertTrue(dut.offer(1L, sntl));
        Assert.assertFalse(dut.offer(1L, frag));
        Assert.assertFalse(dut.offer(1L, frag2));
        Assert.assertEquals(null, dut.poll());
        Assert.assertNull(dut.drain());
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

        Assert.assertFalse(dut.offer(101L, init1));
        Assert.assertTrue(dut.offer(1L, sentinel1));
        Assert.assertTrue(dut.offer(102L, init2));
        Assert.assertTrue(dut.offer(2L, sentinel2));
        Assert.assertTrue(dut.offer(103L, init3));
        Assert.assertTrue(dut.offer(3L, sentinel3));
        Assert.assertTrue(dut.offer(104L, init4));
        Assert.assertNull(dut.drain());
        // MPI EOL
        Assert.assertTrue(dut.offer(0L, makeMPIEOL()));
        // Now, we need to be able to drain all of the SP inits out in order to respond IGNORING
        Assert.assertNull(dut.poll());
        Assert.assertEquals(init2, dut.drain());
        Assert.assertEquals(init3, dut.drain());
        Assert.assertEquals(init4, dut.drain());
        Assert.assertNull(dut.drain());
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

        Assert.assertTrue(dut.offer(1L, sentinel1));
        Assert.assertTrue(dut.offer(2L, sentinel2));
        Assert.assertTrue(dut.offer(3L, sentinel3));
        Assert.assertTrue(dut.offer(101L, init1));
        Assert.assertTrue(dut.offer(102L, init2));
        Assert.assertTrue(dut.offer(103L, init3));
        Assert.assertTrue(dut.offer(104L, init4));
        Assert.assertNull(dut.drain());
        // MPI EOL
        Assert.assertTrue(dut.offer(0L, makeMPIEOL()));
        // Now, we need to be able to drain all of the SP inits out in order to respond IGNORING
        Assert.assertNull(dut.poll());
        Assert.assertEquals(init1, dut.drain());
        Assert.assertEquals(init2, dut.drain());
        Assert.assertEquals(init3, dut.drain());
        Assert.assertEquals(init4, dut.drain());
        Assert.assertNull(dut.drain());
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

        Assert.assertTrue(dut.offer(1L, frag1));
        Assert.assertTrue(dut.offer(1L, complete1));
        Assert.assertTrue(dut.offer(2L, frag2));
        Assert.assertTrue(dut.offer(2L, complete2));
        // We get a really early MPI EOL before we have any of our partition's sentinels
        Assert.assertTrue(dut.offer(0L, makeMPIEOL()));
        Assert.assertTrue(dut.offer(1L, sentinel1));
        Assert.assertEquals(frag1, dut.poll());
        Assert.assertEquals(complete1, dut.poll());
        Assert.assertTrue(dut.offer(2L, sentinel2));
        Assert.assertEquals(frag2, dut.poll());
        Assert.assertEquals(complete2, dut.poll());
        Assert.assertNull(dut.poll());
        Assert.assertNull(dut.drain());
    }

    @Test
    public void testDupInitMsg()
    {
        ReplaySequencer dut = new ReplaySequencer();

        // simple deduping
        TransactionInfoBaseMessage init1 = makeIv2InitTask(1L);

        Assert.assertFalse(dut.offer(1L, init1));
        Assert.assertNotNull(dut.dedupe(1L, init1));
        Assert.assertTrue(dut.offer(1L, init1));
        Assert.assertNull(dut.poll());

        // dedupe with sentinels mixed
        TransactionInfoBaseMessage sntl1 = makeSentinel(2L);
        TransactionInfoBaseMessage init2 = makeIv2InitTask(3L);

        Assert.assertTrue(dut.offer(2L, sntl1));
        Assert.assertTrue(dut.offer(3L, init2));
        Assert.assertNotNull(dut.dedupe(3L, init2));
        Assert.assertTrue(dut.offer(3L, init2));
        Assert.assertNull(dut.poll());

        TransactionInfoBaseMessage frag1 = makeFragment(2L);
        TransactionInfoBaseMessage cmpl1 = makeCompleteTxn(2L);

        Assert.assertTrue(dut.offer(2L, frag1));
        Assert.assertEquals(frag1, dut.poll());
        Assert.assertFalse(dut.offer(2L,cmpl1));
        Assert.assertEquals(init2, dut.poll());
        Assert.assertNull(dut.poll());

        // dedupe with already polled
        Assert.assertNotNull(dut.dedupe(1L, init1));
        Assert.assertTrue(dut.offer(1L, init1));
        Assert.assertNull(dut.poll());
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

        Assert.assertTrue(dut.offer(1L, sntl1));
        Assert.assertTrue(dut.offer(1L, frag1));
        Assert.assertTrue(dut.offer(2L, init1));
        Assert.assertEquals(frag1, dut.poll());
        Assert.assertFalse(dut.offer(1L, cmpl1));
        Assert.assertEquals(init1, dut.poll());
        Assert.assertNull(dut.poll());

        Assert.assertNull(dut.dedupe(1L, sntl1)); // don't care about sentinels
        Assert.assertTrue(dut.offer(1L, sntl1));
        Assert.assertFalse(dut.offer(3L, init2));
        Assert.assertNull(dut.poll());
    }

    @Test
    public void testAllowLoadTableWithSameTxnId()
    {
        ReplaySequencer dut = new ReplaySequencer();

        TransactionInfoBaseMessage init1 = makeIv2InitTask(1L, "@LoadSinglepartitionTable");
        TransactionInfoBaseMessage init2 = makeIv2InitTask(2L, "@LoadMultipartitionTable");

        Assert.assertFalse(dut.offer(1L, init1));
        Assert.assertNull(dut.dedupe(1L, init1));
        Assert.assertFalse(dut.offer(1L, init1));
        Assert.assertNull(dut.poll());

        Assert.assertFalse(dut.offer(2L, init2));
        Assert.assertNull(dut.dedupe(2L, init2));
        Assert.assertFalse(dut.offer(2L, init2));
        Assert.assertNull(dut.poll());
    }

    @Test
    public void testCompleteWithoutFirstFrag()
    {
        ReplaySequencer dut = new ReplaySequencer();

        TransactionInfoBaseMessage sntl = makeSentinel(1L);
        TransactionInfoBaseMessage frag = makeFragment(1L);
        TransactionInfoBaseMessage cmpl = makeCompleteTxn(1L);

        Assert.assertTrue(dut.offer(1L, sntl));
        // a restart complete arrives before the first fragment
        Assert.assertFalse(dut.offer(1L, cmpl));
        Assert.assertTrue(dut.offer(1L, frag));
        // this one should be queued
        Assert.assertTrue(dut.offer(1L, cmpl));

        Assert.assertEquals(frag, dut.poll());
        Assert.assertEquals(cmpl, dut.poll());
        Assert.assertNull(dut.poll());
    }
}

