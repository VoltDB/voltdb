/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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
import java.util.Arrays;
import java.util.Date;

import junit.framework.TestCase;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.hadoop_voltpatches.util.PureJavaCrc32C;
import org.json_voltpatches.JSONException;
import org.voltdb.types.TimestampType;
import org.voltdb.types.VoltDecimalHelper;

public class TestParameterSet extends TestCase {
    ParameterSet params;

    public void testNull() throws IOException {
        params = ParameterSet.fromArrayNoCopy(new Object[]{null, null, null});
        ByteBuffer buf = ByteBuffer.allocate(params.getSerializedSize());
        params.flattenToBuffer(buf);
        buf.rewind();

        ParameterSet out = ParameterSet.fromByteBuffer(buf);

        buf = ByteBuffer.allocate(out.getSerializedSize());
        out.flattenToBuffer(buf);
        buf.rewind();

        ParameterSet out2 = ParameterSet.fromByteBuffer(buf);

        assertEquals(3, out2.toArray().length);
        assertNull(out.toArray()[0]);
    }

    public void testStrings() throws IOException {
        params = ParameterSet.fromArrayNoCopy(new Object[]{"foo"});
        ByteBuffer buf = ByteBuffer.allocate(params.getSerializedSize());
        params.flattenToBuffer(buf);
        buf.rewind();

        ParameterSet out = ParameterSet.fromByteBuffer(buf);
        assertEquals(1, out.toArray().length);
        assertEquals("foo", out.toArray()[0]);
    }

    public void testStringsAsByteArray() throws IOException {
        params = ParameterSet.fromArrayNoCopy(new Object[]{new byte[]{'f', 'o', 'o'}});
        ByteBuffer buf = ByteBuffer.allocate(params.getSerializedSize());
        params.flattenToBuffer(buf);
        buf.rewind();

        ParameterSet out = ParameterSet.fromByteBuffer(buf);
        assertEquals(1, out.toArray().length);

        byte[] bin = (byte[]) out.toArray()[0];
        assertEquals(bin[0], 'f'); assertEquals(bin[1], 'o'); assertEquals(bin[2], 'o');
    }

    public void testNullSigils() throws IOException {
        params = ParameterSet.fromArrayNoCopy(VoltType.NULL_STRING_OR_VARBINARY, VoltType.NULL_DECIMAL, VoltType.NULL_INTEGER);
        ByteBuffer buf = ByteBuffer.allocate(params.getSerializedSize());
        params.flattenToBuffer(buf);
        buf.rewind();

        ParameterSet out = ParameterSet.fromByteBuffer(buf);
        assertEquals(3, out.toArray().length);

        buf = ByteBuffer.allocate(out.getSerializedSize());
        out.flattenToBuffer(buf);
        buf.rewind();

        ParameterSet out2 = ParameterSet.fromByteBuffer(buf);
        assertEquals(3, out2.toArray().length);

        System.out.println(out2.toJSONString());
    }

    public void testNullInObjectArray() throws IOException {
        // This test passes nulls and VoltType nulls in Object[] arrays where the arrays contain
        // all supported datatypes (with the exception that we currently don't support Timestamp or varbinary in Object arrays).
        // Each Object[] type passes in "null" and the VoltType Null equivalent.  Note that Object[] containing Strings will
        // support null and VoltType nulls as array elements.  But any other Sigil type class (big decimal, timestamp, varbinary)
        // DO NOT support nulls or VoltType nulls in Object[] arrays.

        Object o_array[] = null;
        ParameterSet p1 = null;
        Object first_param[] = null;
        boolean failed = false;

        // SHOULD FAIL: Object array of nulls
        o_array = new Object[]{null, null, null};
        failed = false;
        try
        {
            p1 = ParameterSet.fromArrayNoCopy(o_array, 1);
        }
        catch (Exception ex)
        {
            failed = true;
        }
        assert(failed);

        // SHOULD SUCCEED: Empty Object array of null
        o_array = new Object[]{};
        p1 = ParameterSet.fromArrayNoCopy(new Object[]{});
        first_param = p1.toArray();
        assertEquals(first_param.length, p1.toArray().length);

        // SHOULD SUCCEED: Object array of Strings - pass null
        o_array = new Object[]{"Null", null, "not null"};
        p1 = ParameterSet.fromArrayNoCopy(o_array, 1);
        first_param = p1.toArray();
        assertEquals(first_param.length, p1.toArray().length);

        // SHOULD SUCCEED: Object array of Strings - pass VoltType null
        o_array = new Object[]{"Null", VoltType.NULL_STRING_OR_VARBINARY, "not null" };
        p1 = ParameterSet.fromArrayNoCopy(o_array, 1);
        first_param = p1.toArray();
        assertEquals(first_param.length, p1.toArray().length);

        // SHOULD FAIL: Object array of BigDecimal - pass both null
        o_array = new Object[]{ BigDecimal.ONE, null, BigDecimal.TEN };
        failed = false;
        try
        {
            p1 = ParameterSet.fromArrayNoCopy(o_array, 1);
        }
        catch (Exception ex)
        {
            failed = true;
        }
        assert(failed);

        // SHOULD FAIL: Object array of BigDecimal - pass both VoltType null
        o_array = new Object[]{ BigDecimal.ONE, VoltType.NULL_DECIMAL, BigDecimal.TEN };
        failed = false;
        try
        {
            p1 = ParameterSet.fromArrayNoCopy(o_array, 1);
        }
        catch (Exception ex)
        {
            failed = true;
        }
        assert(failed);


        // SHOULD FAIL: Object array of Byte - pass null
        o_array = new Object[]{new Byte((byte)3), null, new Byte((byte)15) };
        failed = false;
        try
        {
            p1 = ParameterSet.fromArrayNoCopy(o_array, 1);
        }
        catch (Exception ex)
        {
            failed = true;
        }
        assert(failed);

        // SHOULD SUCCEED: Object array of Byte - pass VoltType null
        o_array = new Object[]{new Byte((byte)3), VoltType.NULL_TINYINT, new Byte((byte)15) };
        p1 = ParameterSet.fromArrayNoCopy(o_array, 1);
        first_param = p1.toArray();
        assertEquals(first_param.length, p1.toArray().length);

        // SHOULD FAIL: Object array of Short - pass null
        o_array = new Object[]{new Short((short)3), null, new Short((short)15) };
        failed = false;
        try
        {
            p1 = ParameterSet.fromArrayNoCopy(o_array, 1);
        }
        catch (Exception ex)
        {
            failed = true;
        }
        assert(failed);

        // SHOULD SUCCEED: Object array of Short - pass VoltType null
        o_array = new Object[]{new Short((short)3), VoltType.NULL_SMALLINT, new Short((short)15) };
        p1 = ParameterSet.fromArrayNoCopy(o_array, 1);
        first_param = p1.toArray();
        assertEquals(first_param.length, p1.toArray().length);

        // SHOULD FAIL: Object array of Integer - pass null
        o_array = new Object[]{new Integer(3), null, new Integer(15) };
        failed = false;
        try
        {
            p1 = ParameterSet.fromArrayNoCopy(o_array, 1);
        }
        catch (Exception ex)
        {
            failed = true;
        }
        assert(failed);

        // SHOULD SUCCEED: Integer array, pass VoltType null
        o_array = new Object[]{new Integer(3), VoltType.NULL_SMALLINT, new Integer(15) };
        p1 = ParameterSet.fromArrayNoCopy(o_array, 1);
        first_param = p1.toArray();
        assertEquals(first_param.length, p1.toArray().length);

        // SHOULD FAIL: Object array of Long - pass null
        o_array = new Object[]{new Long(3), null };
        failed = false;
        try
        {
            p1 = ParameterSet.fromArrayNoCopy(o_array, 1);
        }
        catch (Exception ex)
        {
            failed = true;
        }
        assert(failed);

        // SHOULD SUCCEED: Object array of Long - pass VoltType null
        o_array = new Object[]{VoltType.NULL_BIGINT, new Long(15) };
        p1 = ParameterSet.fromArrayNoCopy(o_array, 1);
        first_param = p1.toArray();
        assertEquals(first_param.length, p1.toArray().length);

        // SHOULD FAIL: Object array of Float - pass null
        o_array = new Object[]{null, new Double(3.1415) };
        failed = false;
        try
        {
            p1 = ParameterSet.fromArrayNoCopy(o_array, 1);
        }
        catch (Exception ex)
        {
            failed = true;
        }
        assert(failed);

        // SHOULD SUCCEED: Object array of Float - pass VoltType null
        o_array = new Object[]{new Double(3.1415), VoltType.NULL_FLOAT};
        p1 = ParameterSet.fromArrayNoCopy(o_array, 1);
        first_param = p1.toArray();
        assertEquals(first_param.length, p1.toArray().length);

        // SHOULD FAIL: Object array of Decimal - pass null
        o_array = new Object[]{new Double(3.1415), null, new Double(3.1415) };
        failed = false;
        try
        {
            p1 = ParameterSet.fromArrayNoCopy(o_array, 1);
        }
        catch (Exception ex)
        {
            failed = true;
        }
        assert(failed);

        // Object array of Timestamp - pass both null and VoltType null
        // Currently not supported
        o_array = new Object[]{new org.voltdb.types.TimestampType(123432), null, new org.voltdb.types.TimestampType(1233) };
        failed = false;
        try
        {
            p1 = ParameterSet.fromArrayNoCopy(o_array, 1);
        }
        catch (Exception ex)
        {
            failed = true;
        }
        assert(failed);

        // SHOULD FAIL: not supported
        o_array = new Object[]{new org.voltdb.types.TimestampType(123432), VoltType.NULL_TIMESTAMP, new org.voltdb.types.TimestampType(1233)};
        failed = false;
        try
        {
            p1 = ParameterSet.fromArrayNoCopy(o_array, 1);
        }
        catch (Exception ex)
        {
            failed = true;
        }
        assert(failed);


        // Object array of varbinary (byte array) - pass both null and VoltType null

        // Currently not supported
    }

    private boolean arrayLengthTester(Object[] objs)
    {
        params = ParameterSet.fromArrayNoCopy(objs);
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
        params = ParameterSet.fromArrayNoCopy(5.5f);
        ByteBuffer buf = ByteBuffer.allocate(params.getSerializedSize());
        params.flattenToBuffer(buf);
        buf.rewind();

        ParameterSet out = ParameterSet.fromByteBuffer(buf);
        Object value = out.toArray()[0];
        assertTrue(value instanceof Double);
        assertTrue((5.5f - ((Double) value).doubleValue()) < 0.01);
    }

    public void testJSONEncodesBinary() throws JSONException, IOException {
        params = ParameterSet.fromArrayNoCopy(new Object[]{ 123,
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
        PureJavaCrc32C crc;
        ByteBuffer buf;

        Object[] psetObjs = new Object[] {
                null, VoltType.INTEGER.getNullValue(), VoltType.DECIMAL.getNullValue(), // null values
                (byte)1, (short)2, (int)3, (long)4, 1.2f, 3.6d, // numbers
                "This is spinal tap", "", // strings
                "ABCDF012", new byte[] { 1, 3, 5 }, new byte[0], // binary
                new BigDecimal(5.5), // decimal
                new TimestampType(new Date()) // timestamp
        };

        pset = ParameterSet.fromArrayNoCopy(psetObjs);
        crc = new PureJavaCrc32C();
        buf = ByteBuffer.allocate(pset.getSerializedSize());
        pset.flattenToBuffer(buf);
        crc.update(buf.array());
        long crc1 = crc.getValue();

        ArrayUtils.reverse(psetObjs);

        pset = ParameterSet.fromArrayNoCopy(psetObjs);
        crc = new PureJavaCrc32C();
        buf = ByteBuffer.allocate(pset.getSerializedSize());
        pset.flattenToBuffer(buf);
        crc.update(buf.array());
        long crc2 = crc.getValue();

        pset = ParameterSet.fromArrayNoCopy(new Object[0]);
        crc = new PureJavaCrc32C();
        buf = ByteBuffer.allocate(pset.getSerializedSize());
        pset.flattenToBuffer(buf);
        crc.update(buf.array());
        long crc3 = crc.getValue();

        pset = ParameterSet.fromArrayNoCopy(new Object[] { 1 });
        crc = new PureJavaCrc32C();
        buf = ByteBuffer.allocate(pset.getSerializedSize());
        pset.flattenToBuffer(buf);
        crc.update(buf.array());
        long crc4 = crc.getValue();

        assertNotSame(crc1, crc2);
        assertNotSame(crc1, crc3);
        assertNotSame(crc1, crc4);
        assertNotSame(crc2, crc3);
        assertNotSame(crc2, crc4);
        assertNotSame(crc3, crc4);
    }

    public void testRoundtrip() throws IOException {
        Byte byteparam = new Byte((byte) 2);
        Short shortparam = new Short(Short.MAX_VALUE);
        Integer intparam = new Integer(Integer.MIN_VALUE);
        Long longparam = new Long(Long.MAX_VALUE -1);
        Double doubleparam = new Double(Double.MAX_VALUE -1);
        String stringparam = new String("ABCDE");
        TimestampType dateparam = new TimestampType(); // current time
        BigDecimal bigdecimalparam = new BigDecimal(7654321).setScale(VoltDecimalHelper.kDefaultScale);
        VoltTable volttableparam = new VoltTable(new VoltTable.ColumnInfo("foo", VoltType.INTEGER));
        volttableparam.addRow(Integer.MAX_VALUE);

        byte[] bytearray = new byte[] {(byte)'f', (byte)'o', (byte)'o'};
        short[] shortarray = new short[] {Short.MAX_VALUE, Short.MIN_VALUE, (short)5};
        int[] intarray = new int[] {Integer.MAX_VALUE, Integer.MIN_VALUE, 5};
        double[] doublearray = new double[] {Double.MAX_VALUE, Double.MIN_VALUE, 5.5};
        String[] stringarray = new String[] {"ABC", "DEF", "HIJ"};
        TimestampType[] datearray = new TimestampType[] {new TimestampType(), new TimestampType(), new TimestampType()};

        BigDecimal bdtmp1 = new BigDecimal(7654321).setScale(VoltDecimalHelper.kDefaultScale);
        BigDecimal bdtmp2 = new BigDecimal(654321).setScale(VoltDecimalHelper.kDefaultScale);
        BigDecimal bdtmp3 = new BigDecimal(54321).setScale(VoltDecimalHelper.kDefaultScale);
        BigDecimal[] bigdecimalarray = new BigDecimal[] {bdtmp1, bdtmp2, bdtmp3};

        VoltTable vttmp1 = new VoltTable(new VoltTable.ColumnInfo("foo", VoltType.INTEGER),
                new VoltTable.ColumnInfo("bar", VoltType.STRING));
        vttmp1.addRow(Integer.MAX_VALUE, "ry@nlikestheyankees");
        VoltTable vttmp2 = new VoltTable(new VoltTable.ColumnInfo("bar", VoltType.INTEGER),
                new VoltTable.ColumnInfo("bar", VoltType.STRING));
        vttmp2.addRow(Integer.MIN_VALUE, null);
        VoltTable vttmp3 = new VoltTable(new VoltTable.ColumnInfo("far", VoltType.INTEGER),
                new VoltTable.ColumnInfo("bar", VoltType.STRING));
        vttmp3.addRow(new Integer(5), "");
        VoltTable[] volttablearray = new VoltTable[] { vttmp1, vttmp2, vttmp3 };

        assertTrue(bigdecimalparam.scale() == VoltDecimalHelper.kDefaultScale);
        assertTrue(bdtmp1.scale() == VoltDecimalHelper.kDefaultScale);
        assertTrue(bdtmp2.scale() == VoltDecimalHelper.kDefaultScale);
        assertTrue(bdtmp3.scale() == VoltDecimalHelper.kDefaultScale);

        ParameterSet pset = ParameterSet.fromArrayNoCopy(byteparam,
                                                         shortparam,
                                                         intparam,
                                                         longparam,
                                                         doubleparam,
                                                         stringparam,
                                                         dateparam,
                                                         bigdecimalparam,
                                                         volttableparam,
                                                         bytearray,
                                                         shortarray,
                                                         intarray,
                                                         doublearray,
                                                         stringarray,
                                                         datearray,
                                                         bigdecimalarray,
                                                         volttablearray);

        ByteBuffer buf = ByteBuffer.allocate(pset.getSerializedSize());
        pset.flattenToBuffer(buf);
        buf.flip();
        ParameterSet pset2 = ParameterSet.fromByteBuffer(buf);

        Object[] pset1array = pset.toArray();
        Object[] pset2array = pset2.toArray();

        assertTrue(Arrays.deepEquals(pset1array, pset2array));
    }
}
