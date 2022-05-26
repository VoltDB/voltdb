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
package org.voltdb;

import junit.framework.TestCase;

public class TestAdmissionControlGroup extends TestCase {


    private static class ACGMember implements AdmissionControlGroup.ACGMember {
        private int onBackpressure = 0;
        private int offBackpressure = 0;

        @Override
        public void onBackpressure() {
            onBackpressure++;
        }

        @Override
        public void offBackpressure() {
            offBackpressure++;
        }

        @Override
        public long connectionId() {
            return 32;
        }

    }

    private AdmissionControlGroup acg;
    private ACGMember member;

    @Override
    public void setUp() {
        acg = new AdmissionControlGroup(1024 * 1024 * 8, 1000);
        member = new ACGMember();
        acg.addMember(member);
    }

    public void testBackpressureViaQueue() {
        acg.queue(1024 * 1024 * 8 + 1);
        assertEquals(1, member.onBackpressure);
        assertTrue(acg.hasBackPressure());

        //For the condition to end it has to go to less .8 of max
        acg.queue(-10);
        assertEquals(0, member.offBackpressure);
        assertTrue(acg.hasBackPressure());
        acg.queue(-1024 * 1024 * 2);
        assertEquals(1, member.offBackpressure);
        assertFalse(acg.hasBackPressure());
    }

    public void testBackpressureViaQueueAndIncrease() {
        acg.queue(1024 * 1024 * 4 + 1);
        assertEquals(0, member.onBackpressure);
        assertFalse(acg.hasBackPressure());
        acg.increaseBackpressure(1024 * 1024 * 4);
        assertEquals(1, member.onBackpressure);
        assertTrue(acg.hasBackPressure());
        acg.increaseBackpressure(1024 * 1024 * 1);

        //Shouldn't signal backpressure twice, it's edge triggered
        assertEquals(1, member.onBackpressure);

        acg.reduceBackpressure(1024 * 1024 * 5);
        assertEquals(1, member.offBackpressure);
        assertFalse(acg.hasBackPressure());

        //Again, it's edge triggered, should still be one
        acg.reduceBackpressure(1);
        assertEquals(1, member.offBackpressure);

        //Should cause backpressure again
        acg.queue(1024 * 1024 * 5);
        assertTrue(acg.hasBackPressure());
        assertEquals(2, member.onBackpressure);

        //Remove the member, shouldn't get backpressure notification
        acg.removeMember(member);
        acg.queue(-1024 * 1024 * 5);
        assertFalse(acg.hasBackPressure());
        assertEquals(1, member.offBackpressure);
    }

    public void testNegativeTxnBytes() {
        acg.queue(1024 * 1024 * -9);
        assertEquals(0, member.offBackpressure);
        assertEquals(0, member.onBackpressure);

        //The negative thing should have normalized to 0
        acg.queue(1024 * 1024 * 8 + 1);
        assertEquals(1, member.onBackpressure);
    }

    public void testNegativeTxnCount() {
        acg.reduceBackpressure(1);
        acg.reduceBackpressure(1);
        acg.reduceBackpressure(1);
        acg.reduceBackpressure(1);
        for (int ii = 0; ii < 1001; ii++) {
            acg.increaseBackpressure(1);
        }
        assertTrue(acg.hasBackPressure());
        assertEquals(1, member.onBackpressure);
        assertEquals(0, member.offBackpressure);
        for (int ii = 0; ii < 1001; ii++) {
            acg.reduceBackpressure(1);
        }
        assertFalse(acg.hasBackPressure());
        assertEquals(1, member.offBackpressure);

    }

    public void testArgs() {
        try {
            acg.increaseBackpressure(-1);
            fail();
        } catch (IllegalArgumentException e) {}
        try {
            acg.reduceBackpressure(-1);
        } catch (IllegalArgumentException e) {}
    }
}
