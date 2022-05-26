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

import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

import org.voltdb.types.TimestampType;

import junit.framework.TestCase;

public class TestVoltType extends TestCase {

    public void testGet() {
        boolean caught = false;
        try {
            VoltType.get(Byte.MAX_VALUE);
        } catch (AssertionError ex) {
            caught = true;
        }
        assertTrue(caught);

        VoltType vt = VoltType.get((byte)3);
        assertTrue(vt.getValue() == VoltType.TINYINT.getValue());
    }

    public void testTypeFromString() {
        assertEquals(VoltType.TINYINT, VoltType.typeFromString("TINYINT"));
        assertEquals(VoltType.SMALLINT, VoltType.typeFromString("SMALLINT"));
        assertEquals(VoltType.INTEGER, VoltType.typeFromString("INTEGER"));
        assertEquals(VoltType.BIGINT, VoltType.typeFromString("BIGINT"));
        assertEquals(VoltType.FLOAT, VoltType.typeFromString("FLOAT"));
        assertEquals(VoltType.FLOAT, VoltType.typeFromString("DOUBLE"));      // also floats
        assertEquals(VoltType.TIMESTAMP, VoltType.typeFromString("TIMESTAMP"));
        assertEquals(VoltType.STRING, VoltType.typeFromString("STRING"));
        assertEquals(VoltType.VOLTTABLE, VoltType.typeFromString("VOLTTABLE"));
        assertEquals(VoltType.STRING, VoltType.typeFromString("VARCHAR"));
        // Need to be able to turn CHARACTER SqlStmt parameters into varchars.
        assertEquals(VoltType.STRING, VoltType.typeFromString("CHARACTER"));
        assertEquals(VoltType.TIMESTAMP, VoltType.typeFromString("TIMESTAMP"));
        assertEquals(VoltType.DECIMAL, VoltType.typeFromString("DECIMAL"));
        assertEquals(VoltType.VARBINARY, VoltType.typeFromString("VARBINARY"));

        // test with classname prefix
        assertEquals(VoltType.VARBINARY, VoltType.typeFromString("VoltType.VARBINARY"));
        assertEquals(VoltType.STRING, VoltType.typeFromString("VoltType.STRING"));
        assertEquals(VoltType.STRING, VoltType.typeFromString("VoltType.VARCHAR"));
        assertEquals(VoltType.FLOAT, VoltType.typeFromString("VoltType.DOUBLE"));

        boolean caught = false;
        try {
            VoltType.typeFromString("Muhahaha");
        } catch (RuntimeException ex) {
           caught = true;
        }
        assertTrue(caught);
    }

    public void testGetLengthInBytesForFixedTypes() {
        assertEquals(1, VoltType.TINYINT.getLengthInBytesForFixedTypes());
        assertEquals(2, VoltType.SMALLINT.getLengthInBytesForFixedTypes());
        assertEquals(4, VoltType.INTEGER.getLengthInBytesForFixedTypes());
        assertEquals(8, VoltType.BIGINT.getLengthInBytesForFixedTypes());
        assertEquals(8, VoltType.FLOAT.getLengthInBytesForFixedTypes());
        assertEquals(8, VoltType.TIMESTAMP.getLengthInBytesForFixedTypes());
        assertEquals(16, VoltType.DECIMAL.getLengthInBytesForFixedTypes());
        boolean caught = false;
        try {
            VoltType.STRING.getLengthInBytesForFixedTypes();
        } catch (RuntimeException ex) {
            caught = true;
        }
        assertTrue(caught);
    }

    public void testToSQLString() {
        assertEquals("tinyint", VoltType.TINYINT.toSQLString());
        assertEquals("smallint", VoltType.SMALLINT.toSQLString());
        assertEquals("integer", VoltType.INTEGER.toSQLString());
        assertEquals("bigint", VoltType.BIGINT.toSQLString());
        assertEquals("float", VoltType.FLOAT.toSQLString());
        assertEquals("timestamp", VoltType.TIMESTAMP.toSQLString());
        assertEquals("decimal", VoltType.DECIMAL.toSQLString());
        assertEquals("varchar", VoltType.STRING.toSQLString());
        assertEquals("varbinary", VoltType.VARBINARY.toSQLString());
        assertNull(VoltType.VOLTTABLE.toSQLString());
    }

    public void testTypeFromObject() {
        VoltType vt;
        vt = VoltType.typeFromObject(new Byte((byte) 0));
        assertTrue(vt.getValue() == VoltType.TINYINT.getValue());

        vt = VoltType.typeFromObject(new Short((short) 0));
        assertTrue(vt.getValue() == VoltType.SMALLINT.getValue());

        boolean caught = false;
        try {
            VoltType.typeFromClass(Class.class);
        } catch (RuntimeException ex) {
            caught = true;
        }
        assertTrue(caught);
    }

    public void testEquivalences() {

        assertEquals(VoltType.typeFromString("TINYINT"), VoltType.typeFromClass(Byte.class));
        assertEquals(VoltType.typeFromString("SMALLINT"), VoltType.typeFromClass(Short.class));
        assertEquals(VoltType.typeFromString("INTEGER"), VoltType.typeFromClass(Integer.class));
        assertEquals(VoltType.typeFromString("BIGINT"), VoltType.typeFromClass(Long.class));
        assertEquals(VoltType.typeFromString("FLOAT"), VoltType.typeFromClass(Float.class));
        assertEquals(VoltType.typeFromString("DOUBLE"), VoltType.typeFromClass(Double.class));
        assertEquals(VoltType.typeFromString("TIMESTAMP"), VoltType.typeFromClass(TimestampType.class));
        assertEquals(VoltType.typeFromString("STRING"), VoltType.typeFromClass(String.class));
        assertEquals(VoltType.typeFromString("CHARACTER"), VoltType.typeFromClass(String.class));
        assertEquals(VoltType.typeFromString("VOLTTABLE"), VoltType.typeFromClass(VoltTable.class));
        assertEquals(VoltType.typeFromString("DECIMAL"), VoltType.typeFromClass(BigDecimal.class));
        assertEquals(VoltType.typeFromString("VARBINARY"), VoltType.typeFromClass(byte[].class));
    }

    /* round trip the constructors */
    public void testTimestampCreation() {
        long usec = 999999999;
        TimestampType ts1 = new TimestampType(usec);
        assertEquals(usec, ts1.getTime());
        String ts1string = ts1.toString();
        TimestampType ts1prime = new TimestampType(ts1string);
        assertEquals(usec, ts1prime.getTime());

        usec = 999999000;
        ts1 = new TimestampType(usec);
        assertEquals(usec, ts1.getTime());

        // the 0 at the start of microseconds is important
        // for round-trip string/string correctness test
        String date = "2011-06-24 10:30:26.123012";
        TimestampType ts3 = new TimestampType(date);
        assertEquals(date, ts3.toString());

        boolean caught = false;

        caught = false;
        try {
            // Date string inputs interpreted as Dates should not have sub-millisecond granularity.
            // This is the utility function that does this validation.
            TimestampType.millisFromJDBCformat(date);
        } catch (IllegalArgumentException ex) {
            caught = true;
        }
        assertTrue(caught);

        caught = false;
        try {
            // Date string inputs interpreted as TimestampType should not have sub-microsecond granularity.
            String nanoNoNos = "2011-06-24 10:30:26.123012001";
            new TimestampType(nanoNoNos);
        } catch (IllegalArgumentException ex) {
            caught = true;
        }
        assertTrue(caught);

        // Test timestamp before epoch
        usec = -923299922232L;
        ts1 = new TimestampType(usec);
        assertEquals(usec, ts1.getTime());
        ts1string = ts1.toString();
        ts1prime = new TimestampType(ts1string);
        assertEquals(usec, ts1prime.getTime());


        date = "1966-06-24 10:30:26.123012";
        ts3 = new TimestampType(date);
        assertEquals(date, ts3.toString());

    }

    /* Compare some values that differ by microseconds and by full millis */
    public void testTimestampEquality() {
        TimestampType ts1 = new TimestampType(150000);
        TimestampType ts2 = new TimestampType(150000);
        TimestampType ts3 = new TimestampType(150001);
        TimestampType ts4 = new TimestampType(160000);

        assertTrue(ts1.equals(ts2));
        assertTrue(ts2.equals(ts1));
        assertFalse(ts1.equals(ts3));
        assertFalse(ts3.equals(ts1));
        assertFalse(ts1.equals(ts4));
        assertFalse(ts4.equals(ts1));

        assertTrue(ts1.compareTo(ts2) == 0);
        assertTrue(ts2.compareTo(ts1) == 0);
        assertTrue(ts1.compareTo(ts3) < 0);
        assertTrue(ts3.compareTo(ts1) > 0);
        assertTrue(ts1.compareTo(ts4) < 0);
        assertTrue(ts4.compareTo(ts1) > 0);

        String micro = "2011-06-24 10:30:26.123012";
        String milli = "2011-06-24 10:30:26.123";
        TimestampType microTs = new TimestampType(micro);
        TimestampType milliTs = new TimestampType(milli);

        Date fromString = new Date(TimestampType.millisFromJDBCformat(milli.toString()));
        Date fromMillis = new Date(milliTs.getTime()/1000);
        assertEquals(fromString, fromMillis);

        java.sql.Date sqlFromString = new java.sql.Date(TimestampType.millisFromJDBCformat(milliTs.toString()));
        java.sql.Date sqlFromMillis = new java.sql.Date(milliTs.getTime()/1000);
        assertEquals(sqlFromString, sqlFromMillis);

        boolean caught = false;

        caught = false;
        try {
            // Date string inputs converted from TimestampType should not have sub-millisecond granularity.
            new Date(TimestampType.millisFromJDBCformat(microTs.toString()));
        } catch (IllegalArgumentException ex) {
            caught = true;
        }
        assertTrue(caught);

        caught = false;
        try {
            // Date string inputs converted from TimestampType should not have sub-millisecond granularity.
            new java.sql.Date(TimestampType.millisFromJDBCformat(microTs.toString()));
        } catch (IllegalArgumentException ex) {
            caught = true;
        }
        assertTrue(caught);
}

    public void testTimestampToString() {
        // I suppose these could fall across minute boundaries and fail the
        // test.. but that would seem exceedingly unlikely? Do this a few times
        // to try to avoid the false negative.
        for (int ii=0; ii < 5; ++ii) {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            TimestampType now = new TimestampType();
            Date date = new Date();
            if (now.toString().startsWith(sdf.format(date))) {
                assertTrue(now.toString().startsWith(sdf.format(date)));
                return;
            }
        }
        fail();
    }

    public void testTimestampToStringBeforeEpoch() {
        long micros = -48932323284323L;
        TimeZone tz = TimeZone.getTimeZone("America/New_York");
        TimestampType beforeEpoch = new TimestampType(micros);
        String answer = beforeEpoch.toString(tz);
        assertEquals("1968-06-13 11:41:16.715677", answer);
        assertEquals(micros, beforeEpoch.getTime());

        // test Long.MIN as NULL_TimestampType
        // NULL_TimestampType is translated to VoltType.NULL_BIGINT in
        // @see org.voltdb.ParameterSet#flattenToBuffer()
        beforeEpoch = new TimestampType(VoltType.NULL_BIGINT);
        answer = beforeEpoch.toString(tz);
        assertEquals("290303-12-10 14:59:05.224192", answer);
        assertEquals(VoltType.NULL_BIGINT, beforeEpoch.getTime());
    }

    public void testTimestampStringRoundTrip() {
        String[] date_str = {"1900-01-01",
                             "2000-02-03",
                             "2100-04-05",
                             "2012-12-31",
                             "2001-10-25",
        };
        for (int ii=0; ii<date_str.length; ++ii) {
            try{
                String str = new TimestampType(date_str[ii]).toString();
                assertEquals(date_str[ii]+" 00:00:00.000000", str);
            } catch (Exception e){
                e.printStackTrace();
                fail();
            }
        }
    }

}
