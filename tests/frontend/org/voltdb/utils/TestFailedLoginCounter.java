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

package org.voltdb.utils;

import org.junit.Test;

import junit.framework.TestCase;

public class TestFailedLoginCounter extends TestCase {

    @Test
    public void testLogMessage() {
        FailedLoginCounter flc = new FailedLoginCounter();
        long ts = System.currentTimeMillis();
        flc.logMessage(ts, "voltdbuser", "10.0.0.1");

        assertEquals(flc.m_totalFailedAttempts, 1);
        assertEquals(flc.m_timeBucketQueue.peek().m_ts, ts/1000);
    }

    @Test
    public void testCheckCounter() {
        FailedLoginCounter flc = new FailedLoginCounter();
        long ts = System.currentTimeMillis();
        flc.logMessage(ts, "voltdbuser1", "10.0.0.1");
        flc.logMessage(ts + 1, "voltdbuser2", "10.0.0.1");
        flc.logMessage(ts + 2, "voltdbuser3", "10.0.0.1");
        System.out.println(flc.m_totalFailedAttempts);
        assertEquals(flc.m_totalFailedAttempts, 3);
        flc.checkCounter(ts + 70000);
        System.out.println(flc.m_totalFailedAttempts);
        assertEquals(flc.m_totalFailedAttempts, 0);
    }

    @Test
    public void testUserCounter() {
        FailedLoginCounter flc = new FailedLoginCounter();
        long ts = System.currentTimeMillis();
        flc.logMessage(ts, "voltdbuser1", "10.0.0.1");
        flc.logMessage(ts + 1, "voltdbuser2", "10.0.0.1");
        flc.logMessage(ts + 2, "voltdbuser3", "10.0.0.1");

        assertTrue(flc.getUserFailedAttempts().get("voltdbuser1") == 1);
        assertTrue(flc.getUserFailedAttempts().get("voltdbuser2") == 1);
        assertTrue(flc.getUserFailedAttempts().get("voltdbuser3") == 1);
        flc.checkCounter(ts + 70000);
        assertTrue(flc.getUserFailedAttempts().get("voltdbuser1") == 0);
        assertTrue(flc.getUserFailedAttempts().get("voltdbuser2") == 0);
        assertTrue(flc.getUserFailedAttempts().get("voltdbuser3") == 0);
    }

    @Test
    public void testIPCounter() {
        FailedLoginCounter flc = new FailedLoginCounter();
        long ts = System.currentTimeMillis();
        flc.logMessage(ts, "voltdbuser1", "10.0.0.1");
        flc.logMessage(ts + 1, "voltdbuser1", "10.0.0.2");
        flc.logMessage(ts + 2, "voltdbuser1", "10.0.0.3");

        assertTrue(flc.getIPFailedAttempts().get("10.0.0.1") == 1);
        assertTrue(flc.getIPFailedAttempts().get("10.0.0.2") == 1);
        assertTrue(flc.getIPFailedAttempts().get("10.0.0.3") == 1);
        flc.checkCounter(ts + 70000);
        assertTrue(flc.getIPFailedAttempts().get("10.0.0.1") == 0);
        assertTrue(flc.getIPFailedAttempts().get("10.0.0.2") == 0);
        assertTrue(flc.getIPFailedAttempts().get("10.0.0.3") == 0);
    }
}
