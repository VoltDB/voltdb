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

import java.util.HashSet;
import java.util.concurrent.atomic.AtomicLong;

import junit.framework.TestCase;

public class TestUniqueIdGenerator extends TestCase {

    private UniqueIdGenerator tim;

    @Override
    public void setUp() {
        tim = new UniqueIdGenerator(MpInitiator.MP_INIT_PID, 0);
    }

    public void testGetNextUniqueId() {
        long numIds = 100000;
        long numBusyWork = 0;
        HashSet<Long> generatedIds = new HashSet<Long>();
        long start = System.nanoTime();
        for (int ii = 0; ii < numIds; ii++) {
            Long id = tim.getNextUniqueId();
            assertEquals( false, generatedIds.contains(id));
            generatedIds.add(id);
            //make some busy work
            for (int zz = 0; zz < numBusyWork; zz++) {
                long foo1 = zz;
                Long foo2 = foo1;
                long foo3 = foo2;
                Long foo4 = foo3;
                foo4++;
            }
        }
        generatedIds.clear();
        long end = System.nanoTime();
        double nanosPerId = (end - start) / numIds;
        System.out.println("Finished in " + (end - start) + " nanoseconds with " + nanosPerId + " nanoseconds per generated id");
    }

    /** make a good faith effort to wrap the intra-ms counter bits. */
    public void testSpinForNextUniqueId() {
        long lastid = 0;
        long id = 0;
        long iters = UniqueIdGenerator.COUNTER_MAX_VALUE * 3;
        for  (int i = 0; i < iters; ++i) {
            id = tim.getNextUniqueId();
            assertTrue(id > lastid);
            lastid = id;
        }
    }

    public void testSiteIdFromTransactionId() {
        long siteid = UniqueIdGenerator.getPartitionIdFromUniqueId(tim.getNextUniqueId());
        assertEquals(siteid, MpInitiator.MP_INIT_PID);

        UniqueIdGenerator tim2 = new UniqueIdGenerator(5, -1);
        siteid = UniqueIdGenerator.getPartitionIdFromUniqueId(tim2.getNextUniqueId());
        assertEquals(5, siteid);
        siteid = UniqueIdGenerator.getPartitionIdFromUniqueId(tim2.getNextUniqueId());
        assertEquals(5, siteid);
    }

    public void testLastTxnId() {
        for (int i = 0; i < 1000; i++) {
            long id = tim.getNextUniqueId();
            assertEquals(id, tim.getLastUniqueId());

            long id2 = tim.getNextUniqueId();
            assertEquals(id2, tim.getLastUniqueId());

            assertTrue(id2 > id);
        }
    }

    public void testTimestampFromId() {
        long then = System.currentTimeMillis();
        long tid = tim.getNextUniqueId();
        long now = System.currentTimeMillis();

        assertTrue(then <= UniqueIdGenerator.getDateFromUniqueId(tid).getTime());
        assertTrue(now >= UniqueIdGenerator.getDateFromUniqueId(tid).getTime());
    }

    public void testInAndOut() {
        long ts1 = 1267732596224L;
        //long ts1 = TransactionIdManager.VOLT_EPOCH;
        long seq1 = 2;
        long init1 = 6;
        long txnId1 = UniqueIdGenerator.makeIdFromComponents(ts1, seq1, init1);
        System.out.printf("%20d : %s\n", txnId1, UniqueIdGenerator.toBitString(txnId1));

        assertEquals(ts1, UniqueIdGenerator.getTimestampFromUniqueId(txnId1));
        assertEquals(seq1, UniqueIdGenerator.getSequenceNumberFromUniqueId(txnId1));
        assertEquals(init1, UniqueIdGenerator.getPartitionIdFromUniqueId(txnId1));

        long ts2 = 1267732596224L;
        //long ts2 = TransactionIdManager.VOLT_EPOCH + 1;
        long seq2 = 4;
        long init2 = 6;
        long txnId2 = UniqueIdGenerator.makeIdFromComponents(ts2, seq2, init2);
        System.out.printf("%20d : %s\n", txnId2, UniqueIdGenerator.toBitString(txnId2));

        assertEquals(ts2, UniqueIdGenerator.getTimestampFromUniqueId(txnId2));
        assertEquals(seq2, UniqueIdGenerator.getSequenceNumberFromUniqueId(txnId2));
        assertEquals(init2, UniqueIdGenerator.getPartitionIdFromUniqueId(txnId2));

        assertTrue(txnId2 > txnId1);
        System.out.printf("%d > %d\n", txnId1, txnId2);
    }

    /*
     * Going back less than 3 seconds exercises a different code path where we block waiting
     * for time to move forwards far enough to catch up.
     */
    public void testTimeMovesBackwards() {
        final long goBackTime = 10000;
        final long goBackQuantity = 2000;
        UniqueIdGenerator.Clock fakeClock = new UniqueIdGenerator.Clock() {
            public long currentTime = 2000;
            public boolean haveGoneBack = false;

            @Override
            public long get() {
                currentTime++;
                if (currentTime == goBackTime && !haveGoneBack) {
                    haveGoneBack = true;
                    currentTime -= goBackQuantity;
                }
                return currentTime;
            }

            @Override
            public void sleep(long millis) throws InterruptedException {
                if (millis < 0) {
                    throw new IllegalArgumentException();
                }
                currentTime += millis;
            }
        };
        tim = new UniqueIdGenerator(0, 0, fakeClock);
        long last = 0;
        for (int  ii = 0; ii < 60000; ii++) {
            last = tim.getNextUniqueId();
        }
        assertTrue(last > goBackTime);
    }

    /*
     * If the clock jumps back more then we are willing to block for
     * it should still generate IDs by adding an offset to the clock
     */
    public void testTimeMovesBackwardsALot() {
        final long goBackTime = 400000 + 10000;
        final long goBackQuantity = 200000;
        final AtomicLong currentTime = new AtomicLong(400000);
        UniqueIdGenerator.Clock fakeClock = new UniqueIdGenerator.Clock() {
            public boolean haveGoneBack = false;

            @Override
            public long get() {
                currentTime.incrementAndGet();
                if (currentTime.get() == goBackTime && !haveGoneBack) {
                    haveGoneBack = true;
                    currentTime.addAndGet(-goBackQuantity);
                }
                return currentTime.get();
            }

            @Override
            public void sleep(long millis) throws InterruptedException {
                if (millis < 0) {
                    throw new IllegalArgumentException();
                }
                currentTime.addAndGet(millis);
            }
        };
        tim = new UniqueIdGenerator(0, 0, fakeClock);
        long last = 0;
        for (int  ii = 0; ii < 60000; ii++) {
            last = tim.getNextUniqueId();
        }
        assertTrue(UniqueIdGenerator.getTimestampFromUniqueId(last) > goBackTime);

        /*
         * Check that if the clock extracts it's head from it's arse it doesn't leap forward like a madman
         * and removes the offset
         */
        currentTime.set(UniqueIdGenerator.getTimestampFromUniqueId(last) + 1000);
        assertTrue(
                UniqueIdGenerator.getTimestampFromUniqueId(tim.getNextUniqueId())
                < UniqueIdGenerator.getTimestampFromUniqueId(last) + 2000);
    }

    /*
     * If the time is coming from another master
     * and we transition to generating IDs, we have to be sure the IDs are
     * > then whatever came from the master
     */
    public void testIsGreaterThanMaster() {
        UniqueIdGenerator.Clock fakeClock = new UniqueIdGenerator.Clock() {
            long currentTime = 2000;
            public boolean haveGoneBack = false;

            @Override
            public long get() {
                currentTime++;
                return currentTime;
            }

            @Override
            public void sleep(long millis) throws InterruptedException {
                if (millis < 0) {
                    throw new IllegalArgumentException();
                }
                currentTime += millis;
            }
        };
        tim = new UniqueIdGenerator(0, 0, fakeClock);
        long greatestSeen = UniqueIdGenerator.makeIdFromComponents(500000, 21, 0);
        tim.updateMostRecentlyGeneratedUniqueId(greatestSeen);
        assertTrue(tim.getNextUniqueId() > greatestSeen);
        long last = 0;
        for (int  ii = 0; ii < 60000; ii++) {
            last = tim.getNextUniqueId();
        }
        assertTrue(last > greatestSeen);
    }

}
