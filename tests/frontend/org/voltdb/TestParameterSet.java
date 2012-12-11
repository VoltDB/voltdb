/* This file is part of VoltDB.
 * Copyright (C) 2008-2012 VoltDB Inc.
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
import java.nio.ByteBuffer;
import java.util.Date;
import java.util.zip.CRC32;

import junit.framework.TestCase;

import org.apache.commons.lang3.ArrayUtils;
import org.json_voltpatches.JSONException;
import org.voltdb.messaging.FastDeserializer;
import org.voltdb.types.TimestampType;

public class TestParameterSet extends TestCase {
    ParameterSet params;

    @Override
    public void setUp() {
        params = new ParameterSet();
    }

    public void testNull() throws IOException {
        params.setParameters(new Object[]{null, null, null});
        ByteBuffer buf = ByteBuffer.allocate(params.getSerializedSize());
        params.flattenToBuffer(buf);
        buf.rewind();

        ParameterSet out = new ParameterSet();
        out.readExternal(new FastDeserializer(buf));

        assertEquals(3, out.toArray().length);
        assertNull(out.toArray()[0]);
    }

    public void testStrings() throws IOException {
        params.setParameters(new Object[]{"foo"});
        ByteBuffer buf = ByteBuffer.allocate(params.getSerializedSize());
        params.flattenToBuffer(buf);
        buf.rewind();

        ParameterSet out = new ParameterSet();
        out.readExternal(new FastDeserializer(buf));
        assertEquals(1, out.toArray().length);
        assertEquals("foo", out.toArray()[0]);
    }

    public void testStringsAsByteArray() throws IOException {
        params = new ParameterSet();
        params.setParameters(new Object[]{new byte[]{'f', 'o', 'o'}});
        ByteBuffer buf = ByteBuffer.allocate(params.getSerializedSize());
        params.flattenToBuffer(buf);
        buf.rewind();

        ParameterSet out = new ParameterSet();
        out.readExternal(new FastDeserializer(buf));
        assertEquals(1, out.toArray().length);

        byte[] bin = (byte[]) out.toArray()[0];
        assertEquals(bin[0], 'f'); assertEquals(bin[1], 'o'); assertEquals(bin[2], 'o');
    }

    private boolean arrayLengthTester(Object[] objs)
    {
        params = new ParameterSet();
        params.setParameters(objs);
        ByteBuffer buf = ByteBuffer.allocate(params.getSerializedSize());
        boolean threw = false;
        try
        {
            params.flattenToBuffer(buf);
        }
        catch (IOException ioe)
        {
            threw = true;
        }
        return threw;
    }

    public void testArraysTooLong() throws IOException {
        assertTrue("Array longer than Short.MAX_VALUE didn't fail to serialize",
                   arrayLengthTester(new Object[]{new short[Short.MAX_VALUE + 1]}));
        assertTrue("Array longer than Short.MAX_VALUE didn't fail to serialize",
                   arrayLengthTester(new Object[]{new int[Short.MAX_VALUE + 1]}));
        assertTrue("Array longer than Short.MAX_VALUE didn't fail to serialize",
                   arrayLengthTester(new Object[]{new long[Short.MAX_VALUE + 1]}));
        assertTrue("Array longer than Short.MAX_VALUE didn't fail to serialize",
                   arrayLengthTester(new Object[]{new double[Short.MAX_VALUE + 1]}));
        assertTrue("Array longer than Short.MAX_VALUE didn't fail to serialize",
                   arrayLengthTester(new Object[]{new String[Short.MAX_VALUE + 1]}));
        assertTrue("Array longer than Short.MAX_VALUE didn't fail to serialize",
                   arrayLengthTester(new Object[]{new TimestampType[Short.MAX_VALUE + 1]}));
        assertTrue("Array longer than Short.MAX_VALUE didn't fail to serialize",
                   arrayLengthTester(new Object[]{new BigDecimal[Short.MAX_VALUE + 1]}));
    }

    public void testFloatsInsteadOfDouble() throws IOException {
        params = new ParameterSet();
        params.setParameters(5.5f);
        ByteBuffer buf = ByteBuffer.allocate(params.getSerializedSize());
        params.flattenToBuffer(buf);
        buf.rewind();

        ParameterSet out = new ParameterSet();
        out.readExternal(new FastDeserializer(buf));
        Object value = out.toArray()[0];
        assertTrue(value instanceof Double);
        assertTrue((5.5f - ((Double) value).doubleValue()) < 0.01);
    }

    public void testJSONEncodesBinary() throws JSONException, IOException {
        params = new ParameterSet();
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

        assertEquals("0a1A0A", p2.toArray()[6]);
    }

    public void testGetCRCWithoutCrash() throws IOException {
        ParameterSet pset;
        CRC32 crc;

        Object[] psetObjs = new Object[] {
                null, VoltType.INTEGER.getNullValue(), VoltType.DECIMAL.getNullValue(), // null values
                (byte)1, (short)2, (int)3, (long)4, 1.2f, 3.6d, // numbers
                "This is spinal tap", "", // strings
                "ABCDF012", new byte[] { 1, 3, 5 }, new byte[0], // binary
                new BigDecimal(5.5), // decimal
                new TimestampType(new Date()) // timestamp
        };

        pset = new ParameterSet();
        pset.setParameters(psetObjs);
        crc = new CRC32();
        pset.addToCRC(crc);
        long crc1 = crc.getValue();

        ArrayUtils.reverse(psetObjs);

        pset = new ParameterSet();
        pset.setParameters(psetObjs);
        crc = new CRC32();
        pset.addToCRC(crc);
        long crc2 = crc.getValue();

        pset = new ParameterSet();
        pset.setParameters(new Object[0]);
        crc = new CRC32();
        pset.addToCRC(crc);
        long crc3 = crc.getValue();

        pset = new ParameterSet();
        pset.setParameters(new Object[] { 1 });
        crc = new CRC32();
        pset.addToCRC(crc);
        long crc4 = crc.getValue();

        assertNotSame(crc1, crc2);
        assertNotSame(crc1, crc3);
        assertNotSame(crc1, crc4);
        assertNotSame(crc2, crc3);
        assertNotSame(crc2, crc4);
        assertNotSame(crc3, crc4);
    }
}
