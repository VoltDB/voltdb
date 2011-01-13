/* This file is part of VoltDB.
 * Copyright (C) 2008-2011 VoltDB Inc.
 *
 * This file contains original code and/or modifications of original code.
 * Any modifications made by VoltDB Inc. are licensed under the following
 * terms and conditions:
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
/* Copyright (C) 2008
 * Evan Jones
 * Massachusetts Institute of Technology
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
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

package org.voltdb.benchmark.tpcc;

import org.voltdb.benchmark.tpcc.MockRandomGenerator;
import org.voltdb.benchmark.tpcc.Constants;
import junit.framework.TestCase;

public class RandomGeneratorTest extends TestCase {
    MockRandomGenerator generator;
    public void setUp() {
        generator = new MockRandomGenerator();
    }

    public void testMakeLastNameBadNumber() {
        try {
            generator.makeLastName(-1);
            fail("expected assertion");
        } catch (AssertionError e) {}

        try {
            generator.makeLastName(1000);
            fail("expected assertion");
        } catch (AssertionError e) {}
    }

    public void testMakeLastNameSuccess() {
        assertEquals("PRICALLYOUGHT", generator.makeLastName(371));
        assertEquals("OUGHTCALLYATION", generator.makeLastName(178));
    }

    public void testNURandBadA() {
        try {
            generator.NURand(1, 33, 72);
            fail("expected exception");
        } catch (IllegalArgumentException e) {}
    }

    public void testNURand() {
        assertEquals(10, generator.NURand(255, 5, 100));
        assertEquals(0, generator.NURand(1023, 0, 100));
        generator.minimum = false;
        assertEquals(10, generator.NURand(8191, 0, 100));
    }

    public void testMakeRandomLastName() {
        assertEquals("BARBARBAR", generator.makeRandomLastName(Constants.CUSTOMERS_PER_DISTRICT));
        generator.minimum = false;
        assertEquals("BARABLEPRI", generator.makeRandomLastName(Constants.CUSTOMERS_PER_DISTRICT));
    }

    public void testMakeRandomLastNameLimited() {
        generator.minimum = false;
        assertEquals("BARESEESE", generator.makeRandomLastName(100));
    }

    public void testNumberExcludingBad() {
        try {
            generator.numberExcluding(1, 1, 1);
            fail("expected assertion");
        } catch (AssertionError e) {}
    }

    public void testNumberExcludingSuccess() {
        assertEquals(1, generator.numberExcluding(0, 1, 0));
        assertEquals(0, generator.numberExcluding(0, 1, 1));

        generator.minimum = false;
        assertEquals(2, generator.numberExcluding(0, 2, 1));
    }

    public void testFixedPoint() {
        assertEquals(0.0001, generator.fixedPoint(4, 0.0001, 1.0001));
        generator.minimum = false;
        assertEquals(1.0001, generator.fixedPoint(4, 0.0001, 1.0001));
    }

    public void testAString() {
        assertEquals("aaaaa", generator.astring(5, 10));
        generator.minimum = false;
        assertEquals("zzzzzzzzzz", generator.astring(5, 10));
    }

    public void testNString() {
        assertEquals("0", generator.nstring(1, 5));
        generator.minimum = false;
        assertEquals("99999", generator.nstring(1, 5));
    }
}
