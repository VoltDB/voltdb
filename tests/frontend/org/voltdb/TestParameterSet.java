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

package org.voltdb;

import java.io.IOException;
import java.math.BigDecimal;

import junit.framework.TestCase;

import org.json_voltpatches.JSONException;
import org.voltdb.messaging.FastSerializableTestUtil;
import org.voltdb.types.TimestampType;

public class TestParameterSet extends TestCase {
    ParameterSet params;
    @Override
    public void setUp() {
        params = new ParameterSet();
    }

    public void testNull() {
        params.setParameters(new Object[]{null});

        ParameterSet out = FastSerializableTestUtil.roundTrip(params);
        assertEquals(1, out.toArray().length);
        assertNull(out.toArray()[0]);
    }

    public void testStrings() {
        params.setParameters(new Object[]{"foo"});
        ParameterSet out = FastSerializableTestUtil.roundTrip(params);
        assertEquals(1, out.toArray().length);
        assertEquals("foo", out.toArray()[0]);
    }

    public void testStringsAsByteArray() {
        params = new ParameterSet(true);
        params.setParameters(new Object[]{new byte[]{'f', 'o', 'o'}});
        ParameterSet out = FastSerializableTestUtil.roundTrip(params);
        assertEquals(1, out.toArray().length);

        byte[] bin = (byte[]) out.toArray()[0];
        assertEquals(bin[0], 'f'); assertEquals(bin[1], 'o'); assertEquals(bin[2], 'o');
    }

    public void testFloatsInsteadOfDouble() {
        params = new ParameterSet(true);
        params.setParameters(5.5f);
        ParameterSet out = FastSerializableTestUtil.roundTrip(params);
        Object value = out.toArray()[0];
        assertTrue(value instanceof Double);
        assertTrue((5.5f - ((Double) value).doubleValue()) < 0.01);
    }

    public void testJSONEncodesBinary() throws JSONException, IOException {
        params = new ParameterSet(true);
        params.setParameters(new Object[]{ 123,
                                           12345,
                                           1234567,
                                           12345678901L,
                                           1.234567,
                                           "aabbcc",
                                           new byte[] { 10, 26, 10 },
                                           new TimestampType(System.currentTimeMillis()),
                                           new BigDecimal("123.45") } );

        String json = params.toJSONString();
        ParameterSet p2 = ParameterSet.fromJSONString(json);

        assertEquals(p2.toJSONString(), json);

        // this tests that param sets deal with hex-encoded binary stuff right
        json = json.replace("[10,26,10]", "\"0a1A0A\"");
        p2 = ParameterSet.fromJSONString(json);

        assertEquals("0a1A0A", p2.m_params[6]);
    }
}
