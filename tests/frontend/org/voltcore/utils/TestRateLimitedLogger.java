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

package org.voltcore.utils;

import junit.framework.TestCase;

import org.junit.Test;
import org.mockito.Mockito;
import org.voltcore.logging.Level;
import org.voltcore.logging.VoltLogger;

public class TestRateLimitedLogger extends TestCase {

    @Test
    public void testRateLimit() {
        VoltLogger vlogger = Mockito.mock(VoltLogger.class);
        RateLimitedLogger logger = new RateLimitedLogger(20, vlogger, Level.DEBUG);

        long startTime = System.currentTimeMillis();
        while (true) {
            logger.log("foo", System.currentTimeMillis());
            if (System.currentTimeMillis() - startTime > 100) {
                break;
            }
        }
        // Rate limited to every 20 ms, we should get this logged
        // no more than 5 times.  Add an extra possible count just
        // to be safe; the real goal is that we not see an infinite
        // number of these.
        Mockito.verify(vlogger, Mockito.atMost(6)).debug("foo");
    }
}
