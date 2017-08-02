/* This file is part of VoltDB.
 * Copyright (C) 2008-2017 VoltDB Inc.
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

    public void testHexEncoderWithString() {
        String someText = "This is some text\nwith a newline.";
        String hexText = Encoder.hexEncode(someText);
        String result = Encoder.hexDecodeToString(hexText);

        assertEquals(someText, result);
    }

    @Test
    public void testLogMessage() {
    	FailedLoginCounter flc = new FailedLoginCounter();
    	long ts = System.currentTimeMillis();
    	flc.logMessage(ts, "voltdbuser");

    	assertEquals(flc.m_totalFailedAttempts, 1);
    	assertEquals(flc.m_timeBucketQueue.peek().m_ts, ts/1000);
    }

    @Test
    public void testCheckCounter() {
    	FailedLoginCounter flc = new FailedLoginCounter();
    	long ts = System.currentTimeMillis();
    	flc.logMessage(ts, "voltdbuser1");
    	flc.logMessage(ts + 1, "voltdbuser2");
    	flc.logMessage(ts + 2, "voltdbuser3");

    	assertEquals(flc.m_totalFailedAttempts, 3);
    	flc.checkCounter(ts + 70000);
    	//System.out.println(ts);
    	System.out.println(flc.m_totalFailedAttempts);
    	assertEquals(flc.m_totalFailedAttempts, 0);
    }
}
