/* This file is part of VoltDB.
 * Copyright (C) 2022 Volt Active Data Inc.
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

public class TestTimeUtils {

    @Rule
    public final TestName testname = new TestName();

    @Before
    public void before() {
        System.out.printf("Test: %s\n", testname.getMethodName());
    }

    private static final TimeUnit T = TimeUnit.MILLISECONDS;
    private static final TimeUnit S = TimeUnit.SECONDS;
    private static final TimeUnit M = TimeUnit.MINUTES;
    private static final TimeUnit H = TimeUnit.HOURS;
    private static final TimeUnit D = TimeUnit.DAYS;

    @Test
    public void testExplicitUnit() {
        String val;

        val = "123s";
        assertEquals(123L * 1000, TimeUtils.convertTimeAndUnit(val, T, null));
        assertEquals(123L, TimeUtils.convertTimeAndUnit(val, S, null));

        val = "123m";
        assertEquals(123L * 1000 * 60, TimeUtils.convertTimeAndUnit(val, T, null));
        assertEquals(123L * 60, TimeUtils.convertTimeAndUnit(val, S, null));
        assertEquals(123L, TimeUtils.convertTimeAndUnit(val, M, null));

        val = "123h";
        assertEquals(123L * 1000 * 60 * 60, TimeUtils.convertTimeAndUnit(val, T, null));
        assertEquals(123L * 60 * 60, TimeUtils.convertTimeAndUnit(val, S, null));
        assertEquals(123L * 60, TimeUtils.convertTimeAndUnit(val, M, null));
        assertEquals(123L, TimeUtils.convertTimeAndUnit(val, H, null));

        val = "123d";
        assertEquals(123L * 1000 * 60 * 60 * 24, TimeUtils.convertTimeAndUnit(val, T, null));
        assertEquals(123L * 60 * 60 * 24, TimeUtils.convertTimeAndUnit(val, S, null));
        assertEquals(123L * 60 * 24, TimeUtils.convertTimeAndUnit(val, M, null));
        assertEquals(123L * 24, TimeUtils.convertTimeAndUnit(val, H, null));
        assertEquals(123L, TimeUtils.convertTimeAndUnit(val, D, null));
    }

    @Test
    public void testImplicitUnit() {
        String val = "123"; // no unit

        assertEquals(123L * 1000, TimeUtils.convertTimeAndUnit(val, T, S));
        assertEquals(123L, TimeUtils.convertTimeAndUnit(val, S, S));

        assertEquals(123L * 1000 * 60, TimeUtils.convertTimeAndUnit(val, T, M));
        assertEquals(123L * 60, TimeUtils.convertTimeAndUnit(val, S, M));
        assertEquals(123L, TimeUtils.convertTimeAndUnit(val, M, M));

        assertEquals(123L * 1000 * 60 * 60, TimeUtils.convertTimeAndUnit(val, T, H));
        assertEquals(123L * 60 * 60, TimeUtils.convertTimeAndUnit(val, S, H));
        assertEquals(123L * 60, TimeUtils.convertTimeAndUnit(val, M, H));
        assertEquals(123L, TimeUtils.convertTimeAndUnit(val, H, H));

        assertEquals(123L * 1000 * 60 * 60 * 24, TimeUtils.convertTimeAndUnit(val, T, D));
        assertEquals(123L * 60 * 60 * 24, TimeUtils.convertTimeAndUnit(val, S, D));
        assertEquals(123L * 60 * 24, TimeUtils.convertTimeAndUnit(val, M, D));
        assertEquals(123L * 24, TimeUtils.convertTimeAndUnit(val, H, D));
        assertEquals(123L, TimeUtils.convertTimeAndUnit(val, D, D));
    }

    @Test
    public void testTwoCharUnit() {
        String val;

        val = "123ss";
        assertEquals(123L * 1000, TimeUtils.convertTimeAndUnit(val, T, null));

        val = "123mn";
        assertEquals(123L * 1000 * 60, TimeUtils.convertTimeAndUnit(val, T, null));

        val = "123hr";
        assertEquals(123L * 1000 * 60 * 60, TimeUtils.convertTimeAndUnit(val, T, null));

        val = "123dy";
        assertEquals(123L * 1000 * 60 * 60 * 24, TimeUtils.convertTimeAndUnit(val, T, null));
    }

    @Test
    public void testWhitespace() {
        String val;

        val = "  123  s  ";
        assertEquals(123L * 1000, TimeUtils.convertTimeAndUnit(val, T, null));

        val = "\t\t123s\t";
        assertEquals(123L * 1000, TimeUtils.convertTimeAndUnit(val, T, null));

        val = "123\r\n ss"; // silly but handled
        assertEquals(123L * 1000, TimeUtils.convertTimeAndUnit(val, T, null));
    }

    void shouldFail(String val, TimeUnit ret, TimeUnit def) {
        try {
            long n = TimeUtils.convertTimeAndUnit(val, ret, def);
            fail("call did not fail as expected, returned " + n);
        }
        catch (IllegalArgumentException ex) {
            System.out.printf("rejected as expected: %s\n", ex.getMessage());
        }
        catch (Exception ex) {
            fail("call failed as expected but with wrong exception: " + ex);
        }
    }

    @Test
    public void testErrorDetection() {
        shouldFail("s", S, null);                 // no integer
        shouldFail("123", S, null);               // no unit, no  default
        shouldFail("123x", S, null);              // bad unit
        shouldFail("123s123", S, null);           // complete junk
        shouldFail("123s", M, null);              // wants seconds, precision is minutes
        shouldFail("123", M, S);                  // bug, default seconds, precision is minutes
        shouldFail("12345678901234567890", T, T); // numeric overflow
    }

    @Test
    public void testSeparateStrings() {
        String val = "123"; // no unit

        assertEquals(123L * 1000, TimeUtils.convertSeparateTimeAndUnit(val, "s", T));
        assertEquals(123L, TimeUtils.convertSeparateTimeAndUnit(val, "s", S));

        assertEquals(123L * 1000 * 60, TimeUtils.convertSeparateTimeAndUnit(val, "m", T));
        assertEquals(123L * 60, TimeUtils.convertSeparateTimeAndUnit(val, "m", S));
        assertEquals(123L, TimeUtils.convertSeparateTimeAndUnit(val, "m", M));

        assertEquals(123L * 1000 * 60 * 60, TimeUtils.convertSeparateTimeAndUnit(val, "h", T));
        assertEquals(123L * 60 * 60, TimeUtils.convertSeparateTimeAndUnit(val, "h", S));
        assertEquals(123L * 60, TimeUtils.convertSeparateTimeAndUnit(val, "h", M));
        assertEquals(123L, TimeUtils.convertSeparateTimeAndUnit(val, "h", H));

        assertEquals(123L * 1000 * 60 * 60 * 24, TimeUtils.convertSeparateTimeAndUnit(val, "d", T));
        assertEquals(123L * 60 * 60 * 24, TimeUtils.convertSeparateTimeAndUnit(val, "d", S));
        assertEquals(123L * 60 * 24, TimeUtils.convertSeparateTimeAndUnit(val, "d", M));
        assertEquals(123L * 24, TimeUtils.convertSeparateTimeAndUnit(val, "d", H));
        assertEquals(123L, TimeUtils.convertSeparateTimeAndUnit(val, "d", D));
    }

    void shouldFailInt(String val, TimeUnit ret, TimeUnit def) {
        try {
            int n = TimeUtils.convertIntTimeAndUnit(val, ret, def);
            fail("call did not fail as expected, returned " + n);
        }
        catch (IllegalArgumentException ex) {
            System.out.printf("rejected as expected: %s\n", ex.getMessage());
        }
        catch (Exception ex) {
            fail("call failed as expected but with wrong exception: " + ex);
        }
    }

    @Test
    public void test32BitInt() {
        String str68d = String.valueOf(68 * 365) + " D"; // 68 years in days
        String str68h = String.valueOf(68 * 365 * 24) + " H"; // 68 years in hours
        String str68m = String.valueOf(68 * 365 * 24 * 60) + " M"; // 68 years in minutes
        int secsIn68yr = 68 * 365 * 24 * 60 * 60; // as seconds, just fits

        assertEquals(secsIn68yr, TimeUtils.convertIntTimeAndUnit(str68d, S, null));
        shouldFailInt(str68d, T, null);

        assertEquals(secsIn68yr, TimeUtils.convertIntTimeAndUnit(str68h, S, null));
        shouldFailInt(str68h, T, null);

        assertEquals(secsIn68yr, TimeUtils.convertIntTimeAndUnit(str68m, S, null));
        shouldFailInt(str68m, T, null);
    }

}
