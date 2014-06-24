/* This file is part of VoltDB.
 * Copyright (C) 2008-2014 VoltDB Inc.
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

import org.voltdb.types.TimestampType;
import org.voltdb.utils.Encoder;


public class TestParameterConverter extends TestCase
{
    // Tests use naming convention:
    // (invocation deserialized type) To (stored procedure parameter type)

    public void testIntegerToInt() throws Exception
    {
        Integer input = new Integer(1);
        Object generic = input;
        Object r = ParameterConverter.
                makeCompatible(StoredProcParamType.INTEGER, generic, StoredProcParamType.typeFromClass(generic.getClass()));
        assertTrue("expect integer", r.getClass() == Integer.class);
        assertEquals(1, ((Integer)r).intValue());
    }

    public void testIntToInt() throws Exception
    {
        int input = 2;
        Object generic = input;
        Object r = ParameterConverter.
            makeCompatible(StoredProcParamType.INTEGER, generic, StoredProcParamType.typeFromClass(generic.getClass()));
        assertTrue("expect integer", r.getClass() == Integer.class);
        assertEquals(2, ((Integer)r).intValue());
    }

    public void testIntToLong() throws Exception
    {
        int input = -1000;
        Object generic = input;
        Object r = ParameterConverter.
            makeCompatible(StoredProcParamType.BIGINT, generic, StoredProcParamType.typeFromClass(generic.getClass()));
        assertTrue("expect long", r instanceof Number);
        assertEquals(-1000L, ((Number)r).longValue());
    }

    public void testStringToInt() throws Exception
    {
        String input = "1000";
        Object generic = input;
        Object r = ParameterConverter.
            makeCompatible(StoredProcParamType.INTEGER, generic, StoredProcParamType.typeFromClass(generic.getClass()));
        assertTrue("expect integer", r.getClass() == Integer.class);
        assertEquals(1000, ((Integer)r).intValue());
    }

    public void testStringToDouble() throws Exception
    {
        String input = "34.56";
        Object generic = input;
        Object r = ParameterConverter.
            makeCompatible(StoredProcParamType.FLOAT, generic, StoredProcParamType.typeFromClass(generic.getClass()));
        assertTrue("expect double", r.getClass() == Double.class);
        assertEquals(new Double(34.56), ((Double)r).doubleValue());
    }

    // Add more test unit cases
    public void testStringWithWhitespaceToDouble() throws Exception
    {
        String input = "  34.56  ";
        Object generic = input;
        Object r = ParameterConverter.
            makeCompatible(StoredProcParamType.FLOAT, generic, StoredProcParamType.typeFromClass(generic.getClass()));
        assertTrue("expect double", r.getClass() == Double.class);
        assertEquals(new Double(34.56), ((Double)r).doubleValue());
    }

    public void testCommasStringIntegerToInt() throws Exception
    {
        String input = "1,100";
        Object generic = input;
        Object r = ParameterConverter.
            makeCompatible(StoredProcParamType.INTEGER, generic, StoredProcParamType.typeFromClass(generic.getClass()));
        assertTrue("expect integer", r.getClass() == Integer.class);
        assertEquals(1100, ((Integer)r).intValue());
    }

    public void testCommasStringIntegerToDouble() throws Exception
    {
        String input = "2,301,100.23";
        Object generic = input;
        Object r = ParameterConverter.
            makeCompatible(StoredProcParamType.FLOAT, generic, StoredProcParamType.typeFromClass(generic.getClass()));
        assertTrue("expect integer", r.getClass() == Double.class);
        assertEquals(new Double(2301100.23), ((Double)r).doubleValue());
    }

//    public void testNULLToInt() throws Exception
//    {
//        Object generic = null;
//        Object r = ParameterConverter.
//            makeCompatible(StoredProcParamType.INTEGER, null);
//        assertTrue("expect null integer", r.getClass() == Integer.class);
//        assertEquals(VoltType.NULL_INTEGER, r);
//    }

    public void testStringToTimestamp() throws Exception
    {
        TimestampType t = new TimestampType();
        Object r = ParameterConverter.
            makeCompatible(StoredProcParamType.VOLTTIMESTAMP, t, StoredProcParamType.typeFromClass(t.getClass()));
        assertTrue("expect timestamp", r.getClass() == TimestampType.class);
        assertEquals(t, r);
    }

    public void testStringToVarBinary() throws Exception
    {
        String t = "1E3A";
        Object r = ParameterConverter.
            makeCompatible(StoredProcParamType.VARBINARY, t, StoredProcParamType.typeFromClass(t.getClass()));
        assertTrue("expect varbinary", r.getClass() == byte[].class);
        assertEquals(t, Encoder.hexEncode((byte[])r));
    }

    public void testNulls()
    {
        assertEquals(VoltType.NULL_TINYINT, ParameterConverter.makeCompatible(StoredProcParamType.TINYINT, VoltType.NULL_TINYINT, StoredProcParamType.TINYINT));
        assertEquals(VoltType.NULL_SMALLINT, ParameterConverter.makeCompatible(StoredProcParamType.SMALLINT, VoltType.NULL_SMALLINT, StoredProcParamType.SMALLINT));
        assertEquals(VoltType.NULL_INTEGER, ParameterConverter.makeCompatible(StoredProcParamType.INTEGER, VoltType.NULL_INTEGER, StoredProcParamType.INTEGER));
        assertEquals(VoltType.NULL_BIGINT, ParameterConverter.makeCompatible(StoredProcParamType.BIGINT, VoltType.NULL_BIGINT, StoredProcParamType.BIGINT));
        assertEquals(VoltType.NULL_FLOAT, ParameterConverter.makeCompatible(StoredProcParamType.FLOAT, VoltType.NULL_FLOAT, StoredProcParamType.FLOAT));
        assertEquals(null, ParameterConverter.makeCompatible(StoredProcParamType.VOLTTIMESTAMP, VoltType.NULL_TIMESTAMP, StoredProcParamType.VOLTTIMESTAMP));
        assertEquals(null, ParameterConverter.makeCompatible(StoredProcParamType.SQLTIMESTAMP, VoltType.NULL_TIMESTAMP, StoredProcParamType.SQLTIMESTAMP));
        assertEquals(null, ParameterConverter.makeCompatible(StoredProcParamType.JAVADATESTAMP, VoltType.NULL_TIMESTAMP, StoredProcParamType.JAVADATESTAMP));
        assertEquals(null, ParameterConverter.makeCompatible(StoredProcParamType.SQLDATESTAMP, VoltType.NULL_TIMESTAMP, StoredProcParamType.SQLDATESTAMP));
        assertEquals(null, ParameterConverter.makeCompatible(StoredProcParamType.STRING, VoltType.NULL_STRING_OR_VARBINARY, StoredProcParamType.STRING));
        assertEquals(null, ParameterConverter.makeCompatible(StoredProcParamType.DECIMAL, VoltType.NULL_DECIMAL, StoredProcParamType.DECIMAL));
    }
}
