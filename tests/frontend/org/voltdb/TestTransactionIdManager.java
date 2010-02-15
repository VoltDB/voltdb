/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB L.L.C.
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

package org.voltdb;

import junit.framework.TestCase;

import org.voltdb.TransactionIdManager;
import java.util.HashSet;

public class TestTransactionIdManager extends TestCase {

    private TransactionIdManager tim;

    public void setUp() {
        tim = new TransactionIdManager(VoltDB.INITIATOR_SITE_ID);
    }

    public void testGetNextUniqueId() {
        long numIds = 100000;
        long numBusyWork = 0;
        HashSet<Long> generatedIds = new HashSet<Long>();
        long start = System.nanoTime();
        for (int ii = 0; ii < numIds; ii++) {
            Long id = tim.getNextUniqueTransactionId();
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
        long iters = TransactionIdManager.COUNTER_MAX_VALUE * 3;
        for  (int i = 0; i < iters; ++i) {
            id = tim.getNextUniqueTransactionId();
            assertTrue(id > lastid);
            lastid = id;
        }
    }

    public void testSiteIdFromTransactionId() {
        long siteid = TransactionIdManager.getSiteIdFromTransactionId(tim.getNextUniqueTransactionId());
        assertEquals(siteid, VoltDB.INITIATOR_SITE_ID);

        TransactionIdManager tim2 = new TransactionIdManager(5);
        siteid = TransactionIdManager.getSiteIdFromTransactionId(tim2.getNextUniqueTransactionId());
        assertEquals(5, siteid);
        siteid = TransactionIdManager.getSiteIdFromTransactionId(tim2.getNextUniqueTransactionId());
        assertEquals(5, siteid);
    }

    public void testLastTxnId() {
        long id = tim.getNextUniqueTransactionId();
        assertEquals(id, tim.getLastTxnId());

        long id2 = tim.getNextUniqueTransactionId();
        assertEquals(id2, tim.getLastTxnId());

        assertTrue(id2 != id);
    }

    public void testTimestampFromId() {
        long then = System.currentTimeMillis();
        long tid = tim.getNextUniqueTransactionId();
        long now = System.currentTimeMillis();

        assertTrue(then <= TransactionIdManager.getDateFromTransactionId(tid).getTime());
        assertTrue(now >= TransactionIdManager.getDateFromTransactionId(tid).getTime());
    }

}
