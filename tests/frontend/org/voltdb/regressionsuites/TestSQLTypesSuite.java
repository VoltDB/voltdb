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

package org.voltdb.regressionsuites;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import org.voltdb.BackendTarget;
import org.voltdb.VoltTable;
import org.voltdb.VoltTableRow;
import org.voltdb.VoltType;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcCallException;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.types.GeographyPointValue;
import org.voltdb.types.GeographyValue;
import org.voltdb.types.TimestampType;
import org.voltdb.types.VoltDecimalHelper;
import org.voltdb.utils.Encoder;
import org.voltdb.utils.VoltTypeUtil;
import org.voltdb_testprocs.regressionsuites.sqltypesprocs.Delete;
import org.voltdb_testprocs.regressionsuites.sqltypesprocs.Insert;
import org.voltdb_testprocs.regressionsuites.sqltypesprocs.InsertBase;
import org.voltdb_testprocs.regressionsuites.sqltypesprocs.InsertBoxed;
import org.voltdb_testprocs.regressionsuites.sqltypesprocs.InsertMulti;
import org.voltdb_testprocs.regressionsuites.sqltypesprocs.ParamSetArrays;
import org.voltdb_testprocs.regressionsuites.sqltypesprocs.Select;
import org.voltdb_testprocs.regressionsuites.sqltypesprocs.Update;
import org.voltdb_testprocs.regressionsuites.sqltypesprocs.UpdateDecimal;

import com.google_voltpatches.common.base.Charsets;

public class TestSQLTypesSuite extends RegressionSuite {

    // used to generate unique pkeys
    public static final AtomicInteger pkey = new AtomicInteger(0);

    // constant for 0x00
    private static final byte OO = (byte) 0x00; // font test?

    // 1500 character string
    private static final String ReallyLongString;

    /** MP Procedures used by this suite */
    static final Class<?>[] MP_PROCEDURES = {
            InsertBase.class, InsertBoxed.class, InsertMulti.class,
            UpdateDecimal.class, ParamSetArrays.class };

    /** Utility to create an array of bytes with value "b" of length "length" */
    public static byte[] byteArray(final int length, final byte b) {
        final byte[] arr = new byte[length];
        for (int i = 0; i < length; ++i) {
            arr[i] = b;
        }
        return arr;
    }

    /** Utility to compare two instances of a VoltType for equality */
    private boolean comparisonHelper(final Object lhs, final Object rhs,
            final VoltType vt) {
        switch (vt) {
        case TINYINT:
            final Byte b1 = (Byte) lhs;
            final Byte b2 = (Byte) rhs;
            // System.out.println("\tComparing " + b1 + " == " + b2);
            return b1.byteValue() == b2.byteValue();
        case SMALLINT:
            final Short s1 = (Short) lhs;
            final Short s2 = (Short) rhs;
            // System.out.println("\tComparing " + s1 + " == " + s2);
            return s1.shortValue() == s2.shortValue();
        case INTEGER:
            final Integer i1 = (Integer) lhs;
            final Integer i2 = (Integer) rhs;
            // System.out.println("\tComparing " + i1 + " == " + i2);
            return i1.intValue() == i2.intValue();
        case BIGINT:
            final Long l1 = (Long) lhs;
            final Long l2 = (Long) rhs;
            // System.out.println("\tComparing " + l1 + " == " + l2);
            return l1.longValue() == l2.longValue();
        case FLOAT:
            final Double d1 = (Double) lhs;
            final Double d2 = (Double) rhs;
            // System.out.println("\tComparing " + d1 + " == " + d2);
            // Handle the screwy null double value (isn't quite min double)
            if (((d1 == VoltType.NULL_FLOAT) && (d2 <= d1))
                    || ((d2 == VoltType.NULL_FLOAT) && (d1 <= d2))) {
                return true;
            }
            return (Math.abs(d1 - d2) < 0.0000000001);
        case STRING:
            // System.out.println("\tComparing " + lhs + " == " + rhs);
            if ((lhs == null || lhs == VoltType.NULL_STRING_OR_VARBINARY)
                    && (rhs == null || rhs == VoltType.NULL_STRING_OR_VARBINARY)) {
                return true;
            }
            return ((String) lhs).equals(rhs);
        case VARBINARY:
            boolean lhsnull = (lhs == null || lhs == VoltType.NULL_STRING_OR_VARBINARY);
            boolean rhsnull = (rhs == null || rhs == VoltType.NULL_STRING_OR_VARBINARY);
            if (lhsnull && rhsnull)
                return true;
            if (lhsnull != rhsnull)
                return false;

            // assume neither is null from here

            String lhs2 = null;
            String rhs2 = null;

            if (lhs instanceof byte[])
                lhs2 = Encoder.hexEncode((byte[]) lhs);
            else
                lhs2 = (String) lhs;

            if (rhs instanceof byte[])
                rhs2 = Encoder.hexEncode((byte[]) rhs);
            else
                rhs2 = (String) rhs;

            return lhs2.equalsIgnoreCase(rhs2);
        case TIMESTAMP:
            // System.out.println("\tComparing " + lhs + " == " + rhs);
            if ((lhs == null || lhs == VoltType.NULL_TIMESTAMP)
                    && (rhs == null || rhs == VoltType.NULL_TIMESTAMP)) {
                return true;
            }
            return ((TimestampType) lhs).equals(rhs);
        case DECIMAL:
            // System.out.println("\tComparing " + lhs + " == " + rhs);
            if ((lhs == null || lhs == VoltType.NULL_DECIMAL)
                    && (rhs == null || rhs == VoltType.NULL_DECIMAL)) {
                return true;
            }
            return ((BigDecimal) lhs).equals(rhs);
        case GEOGRAPHY_POINT: {
            if ((lhs == VoltType.NULL_POINT || lhs == null)
                    && (rhs == VoltType.NULL_POINT || rhs == null)) {
                return true;
            }

            GeographyPointValue gpvLhs = (GeographyPointValue)lhs;
            GeographyPointValue gpvRhs = (GeographyPointValue)rhs;
            return gpvLhs.equals(gpvRhs);
        }
        case GEOGRAPHY: {
            if ((lhs == VoltType.NULL_GEOGRAPHY || lhs == null)
                    && (rhs == VoltType.NULL_GEOGRAPHY || rhs == null)) {
                return true;
            }

            GeographyValue gvLhs = (GeographyValue)lhs;
            GeographyValue gvRhs = (GeographyValue)rhs;
            return gvLhs.equals(gvRhs);
        }
        default:
            throw new IllegalArgumentException("Unknown type in comparisonHelper");
        }
    }

    //
    // UPDATE THE COLUMN LIST WHEN ADDING NEW TYPE
    //

    // Class to hold column test information.
    private static class Column {
        final String m_columnName;
        final VoltType m_type;
        final boolean m_supportsMath;
        final Object m_nullValue;
        final Object m_defaultValue;
        final Object m_minValue;
        final Object m_midValue;
        Object m_maxValue;

        Column(String columnName,
               VoltType type,
               boolean supportsMath,
               Object nullValue,
               Object defaultValue,
               Object minValue,
               Object midValue,
               Object maxValue) {
            m_type = type;
            m_supportsMath = supportsMath;
            m_columnName = columnName;
            m_nullValue = nullValue;
            m_defaultValue = defaultValue;
            m_minValue = minValue;
            m_midValue = midValue;
            m_maxValue = maxValue;
        }
    }

    // Non-PKEY column information, including interesting sets of values for the various types.
    // Tests rely on this ordering of the string varchar widths.
    // APPEND HERE WHEN ADDING NEW TYPE/COLUMN
    private static Column[] m_columns = {
        new Column("A_TINYINT", VoltType.TINYINT, true ,
                   VoltType.NULL_TINYINT,
                   new Byte((byte) (1)),
                   new Byte((byte) (Byte.MIN_VALUE + 1)), // MIN is NULL
                   new Byte((byte) 10),
                   Byte.MAX_VALUE),
        new Column("A_SMALLINT", VoltType.SMALLINT, true,
                   VoltType.NULL_SMALLINT,
                   new Short((short) (2)),
                   new Short((short) (Short.MIN_VALUE + 1)), // MIN is NULL
                   new Short((short) 11),
                   Short.MAX_VALUE),
        new Column("A_INTEGER", VoltType.INTEGER, true ,
                   VoltType.NULL_INTEGER,
                   3,
                   Integer.MIN_VALUE + 1, // MIN is NULL
                   new Integer(12),
                   Integer.MAX_VALUE),
        new Column("A_BIGINT", VoltType.BIGINT, true,
                   VoltType.NULL_BIGINT,
                   4L,
                   Long.MIN_VALUE + 1, // MIN is NULL
                   new Long(13),
                   Long.MAX_VALUE),
        new Column("A_FLOAT", VoltType.FLOAT, true,
                   VoltType.NULL_FLOAT,
                   5.1,
                   Math.nextAfter(VoltType.NULL_FLOAT, 0), // NULL is -1.7E308.
                   new Double(14.5),
                   Double.MAX_VALUE),
        new Column("A_TIMESTAMP", VoltType.TIMESTAMP, false,
                   VoltType.NULL_TIMESTAMP,
                   new TimestampType(600000),
                   new TimestampType(Long.MIN_VALUE + 1),
                   new TimestampType(),
                   new TimestampType(Long.MAX_VALUE)),
        new Column("A_INLINE_S1", VoltType.STRING, false,
                   VoltType.NULL_STRING_OR_VARBINARY,
                   new String("abcd"),
                   new String(byteArray(1, OO)),
                   new String("xyz"),
                   new String("ZZZZ")),
        new Column("A_INLINE_S2", VoltType.STRING, false,
                   VoltType.NULL_STRING_OR_VARBINARY,
                   new String("abcdefghij"),
                   new String(byteArray(1, OO)),
                   new String("xyzab"),
                   new String("ZZZZZZZZZZ" + // 10
                              "ZZZZZZZZZZ" + // 20
                              "ZZZZZZZZZZ" + // 30
                              "ZZZZZZZZZZ" + // 40
                              "ZZZZZZZZZZ" + // 50
                              "ZZZZZZZZZZ" + // 60
                              "ZZZ")), // 63
        new Column("A_POOL_S", VoltType.STRING, false,
                   VoltType.NULL_STRING_OR_VARBINARY,
                   new String("abcdefghijklmnopqrstuvwxyz"),
                   new String(byteArray(1, OO)),
                   new String("xyzabcdefghijklmnopqrstuvw"),
                   ""),
        new Column("A_POOL_MAX_S", VoltType.STRING, false,
                   VoltType.NULL_STRING_OR_VARBINARY,
                   new String("abcdefghijklmnopqrstuvwxyz"),
                   new String(byteArray(1, OO)),
                   new String("xyzabcdefghijklmnopqrstuvw"),
                   ""),
        new Column("A_INLINE_B", VoltType.VARBINARY, false,
                   VoltType.NULL_STRING_OR_VARBINARY,
                   new String("ABCDEFABCDEF0123"),
                   new byte[] { 0 },
                   new byte[] { 'a', 'b', 'c' },
                   new String("ZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZ").getBytes(Charsets.UTF_8)),
        new Column("A_POOL_B", VoltType.VARBINARY, false,
                   VoltType.NULL_STRING_OR_VARBINARY,
                   new String("ABCDEFABCDEF0123456789"),
                   new byte[] { 0 },
                   new byte[] { 'a', 'b', 'c' },
                   new byte[] {}),
        new Column("A_DECIMAL", VoltType.DECIMAL, true,
                   VoltType.NULL_DECIMAL,
                   new BigDecimal(new BigInteger("6000000000000"))
                       .scaleByPowerOfTen(-1 * VoltDecimalHelper.kDefaultScale),
                   new BigDecimal(new BigInteger(
                       "-99999999999999999999999999999999999999"))
                       .scaleByPowerOfTen(-1 * VoltDecimalHelper.kDefaultScale),
                   new BigDecimal(new BigInteger("5115101010101010345634"))
                       .scaleByPowerOfTen(-1 * VoltDecimalHelper.kDefaultScale),
                   new BigDecimal(new BigInteger(
                       "99999999999999999999999999999999999999"))
                       .scaleByPowerOfTen(-1 * VoltDecimalHelper.kDefaultScale)),
        new Column("A_GEOGRAPHY_POINT",
                VoltType.GEOGRAPHY_POINT,
                false, // supports math
                VoltType.NULL_POINT, // null value
                new GeographyPointValue(-122.0, 37.0),
                new GeographyPointValue(-180.0, -90.0),
                new GeographyPointValue(0.0, 0.0),
                new GeographyPointValue(180.0, 90.0)),
        new Column("A_GEOGRAPHY",
                VoltType.GEOGRAPHY,
                false, // supports math
                VoltType.NULL_GEOGRAPHY, // null value
                GeographyValue.fromWKT("polygon((-122 37, -122 39, -120 39, -122 37))"),
                GeographyValue.fromWKT("polygon((-142 37, -142 39, -140 39, -142 37))"),
                GeographyValue.fromWKT("polygon((-152 37, -152 39, -150 39, -152 37))"),
                GeographyValue.fromWKT("polygon((-162 37, -162 39, -160 39, -162 37))"))
    };

    // Generate additional m_maxValue data and ReallyLongString..
    static {
        StringBuilder sb = new StringBuilder(1048576);
        int ii = 0;
        for (; ii < 65536; ii++) {
            sb.append('Z');
        }
        m_columns[8].m_maxValue = sb.toString();
        for (; ii < 1048576; ii++) {
            sb.append('Z');
        }
        m_columns[9].m_maxValue = sb.toString();
        sb = new StringBuilder(102400);
        for (ii = 0; ii < 102400; ii++) {
            sb.append('a');
        }
        ReallyLongString = sb.toString();
    }

    // Populate these members from m_columns for backward compatibility.
    // Changing all the references to use m_columns would be much more painful.
    public static int COLS = m_columns.length;
    public static VoltType[] m_types = new VoltType[m_columns.length];
    public static boolean[] m_supportsMath = new boolean[m_columns.length];
    public static String[] m_columnNames = new String[m_columns.length];
    public static Object[] m_nullValues = new Object[m_columns.length];
    public static Object[] m_defaultValues = new Object[m_columns.length];
    public static Object[] m_minValues = new Object[m_columns.length];
    public static Object[] m_midValues = new Object[m_columns.length];
    public static Object[] m_maxValues = new Object[m_columns.length];
    static {
        for (int i = 0; i < m_columns.length; i++) {
            m_types[i] = m_columns[i].m_type;
            m_supportsMath[i] = m_columns[i].m_supportsMath;
            m_columnNames[i] = m_columns[i].m_columnName;
            m_nullValues[i] = m_columns[i].m_nullValue;
            m_defaultValues[i] = m_columns[i].m_defaultValue;
            m_minValues[i] = m_columns[i].m_minValue;
            m_midValues[i] = m_columns[i].m_midValue;
            m_maxValues[i] = m_columns[i].m_maxValue;
        }
    }

    public void testPassingNullObjectToSingleStmtProcedure() throws Exception {
        final Client client = this.getClient();

        client.callProcedure("PassObjectNull", 0, 0, 0, 0, 0, 0.0, null, null,
                null, null, null, null, null, null, null, null);
    }

    public void testPassingDateAndTimeObjectsToStatements() throws Exception {
        final Client client = this.getClient();

        // Capture the same value within the supported millisecond granularity
        // in each of the supported time formats to demonstrate that they are interchangeable.
        long millisecondsSinceEpoch = 1001001001L;
        TimestampType tst = new TimestampType(millisecondsSinceEpoch * 1000);
        java.util.Date utild = new java.util.Date(millisecondsSinceEpoch);
        java.sql.Date sqld = new java.sql.Date(millisecondsSinceEpoch);
        java.sql.Timestamp ts = new java.sql.Timestamp(millisecondsSinceEpoch);

        int lowerBound = pkey.incrementAndGet();
        // system-defined CRUD inputs
        client.callProcedure("ALLOW_NULLS.insert", pkey.incrementAndGet(), 0, 0, 0, 0, 0.0, tst,
                null, null, null, null, null, null, null, null, null);
        client.callProcedure("ALLOW_NULLS.insert", pkey.incrementAndGet(), 0, 0, 0, 0, 0.0, utild,
                null, null, null, null, null, null, null, null, null);
        client.callProcedure("ALLOW_NULLS.insert", pkey.incrementAndGet(), 0, 0, 0, 0, 0.0, sqld,
                null, null, null, null, null, null, null, null, null);
        client.callProcedure("ALLOW_NULLS.insert", pkey.incrementAndGet(), 0, 0, 0, 0, 0.0, ts,
                null, null, null, null, null, null, null, null, null);

        // user-defined statement inputs
        client.callProcedure("PassObjectNull", pkey.incrementAndGet(), 0, 0, 0, 0, 0.0, tst,
                null, null, null, null, null, null, null, null, null);
        client.callProcedure("PassObjectNull", pkey.incrementAndGet(), 0, 0, 0, 0, 0.0, utild,
                null, null, null, null, null, null, null, null, null);
        client.callProcedure("PassObjectNull", pkey.incrementAndGet(), 0, 0, 0, 0, 0.0, sqld,
                null, null, null, null, null, null, null, null, null);
        client.callProcedure("PassObjectNull", pkey.incrementAndGet(), 0, 0, 0, 0, 0.0, ts,
                null, null, null, null, null, null, null, null, null);

        // stored procedure inputs into queued statement
        // -- this doesn't exercise passing the java types into the stored procedure
        // -- that's covered by TestSQLFeaturesSuite's testPassAllArgTypes
        client.callProcedure("Insert", "ALLOW_NULLS", pkey.incrementAndGet(), 0, 0, 0, 0, 0.0, tst,
                null, null, null, null, null, null, null, null, null);
        client.callProcedure("Insert", "ALLOW_NULLS and use sql.Timestamp", pkey.incrementAndGet(), 0, 0, 0, 0, 0.0, tst,
                null, null, null, null, null, null, null, null, null);
        client.callProcedure("Insert", "ALLOW_NULLS and use sql.Date", pkey.incrementAndGet(), 0, 0, 0, 0, 0.0, tst,
                null, null, null, null, null, null, null, null, null);
        client.callProcedure("Insert", "ALLOW_NULLS and use util.Date", pkey.incrementAndGet(), 0, 0, 0, 0, 0.0, tst,
                null, null, null, null, null, null, null, null, null);

        ClientResponse cr;
        VoltTable[] result;
        VoltTable vt;
        cr = client.callProcedure("@AdHoc", "SELECT A_TIMESTAMP from ALLOW_NULLS where PKEY > " + lowerBound + ";");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        result = cr.getResults();
        assertEquals(12, result[0].getRowCount());
        vt = result[0];
        while (vt.advanceRow()) {
            // Within the millisecond granularity all formats should encapsulate the same value.
            assertEquals(tst, vt.getTimestampAsTimestamp(0));
            assertEquals(ts, vt.getTimestampAsSqlTimestamp(0));
            assertEquals(sqld, vt.getTimestampAsSqlTimestamp(0));
            assertEquals(utild, vt.getTimestampAsSqlTimestamp(0));
        }

        // Demonstrate that TimestampType and java.sql.Timestamp support microseconds while Dates truncate to milliseconds.
        // Capture the same value within the supported millisecond granularity
        // in each of the supported time formats to demonstrate that they are interchangeable.
        long microsecondsSinceEpoch = 1001001001001L;
        TimestampType tst_micro = new TimestampType(microsecondsSinceEpoch);
        java.sql.Timestamp ts_micro = new java.sql.Timestamp(microsecondsSinceEpoch/1000);
        // At this point, the additional 1 microsecond was truncated in the division, and so is still not reflected in ts_micro.
        assertEquals(ts, ts_micro);
        // Extract the 1000000 nanos (doubly-counted milliseconds)
        assertEquals(1000000, ts_micro.getNanos());
        // and explicitly add in the truncated 1000 nanos (1 microsecond)
        ts_micro.setNanos(ts_micro.getNanos()+1000);

        assertNotSame(tst, tst_micro);
        assertNotSame(ts, ts_micro);

        // A new round of inserts, just using the more accurate formats.
        lowerBound = pkey.incrementAndGet();
        // system-defined CRUD inputs
        client.callProcedure("ALLOW_NULLS.insert", pkey.incrementAndGet(), 0, 0, 0, 0, 0.0, tst_micro,
                null, null, null, null, null, null, null, null, null);
        client.callProcedure("ALLOW_NULLS.insert", pkey.incrementAndGet(), 0, 0, 0, 0, 0.0, ts_micro,
                null, null, null, null, null, null, null, null, null);

        // user-defined statement inputs
        client.callProcedure("PassObjectNull", pkey.incrementAndGet(), 0, 0, 0, 0, 0.0, tst_micro,
                null, null, null, null, null, null, null, null, null);
        client.callProcedure("PassObjectNull", pkey.incrementAndGet(), 0, 0, 0, 0, 0.0, ts_micro,
                null, null, null, null, null, null, null, null, null);

        // stored procedure inputs into queued statement
        client.callProcedure("Insert", "ALLOW_NULLS", pkey.incrementAndGet(), 0, 0, 0, 0, 0.0, tst_micro,
                null, null, null, null, null, null, null, null, null);
        client.callProcedure("Insert", "ALLOW_NULLS and use sql.Timestamp", pkey.incrementAndGet(), 0, 0, 0, 0, 0.0, tst_micro,
                null, null, null, null, null, null, null, null, null);

        cr = client.callProcedure("@AdHoc", "SELECT A_TIMESTAMP from ALLOW_NULLS where PKEY > " + lowerBound + ";");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        result = cr.getResults();
        assertEquals(6, result[0].getRowCount());
        vt = result[0];
        while (vt.advanceRow()) {
            // Within the microsecond granularity only the detailed formats preserve the "full" accuracy.
            assertNotSame(tst, vt.getTimestampAsTimestamp(0));
            assertNotSame(ts, vt.getTimestampAsSqlTimestamp(0));
            assertEquals(tst_micro, vt.getTimestampAsTimestamp(0));
            assertEquals(ts_micro, vt.getTimestampAsSqlTimestamp(0));
            assertEquals(sqld, vt.getTimestampAsSqlTimestamp(0));
            assertEquals(utild, vt.getTimestampAsSqlTimestamp(0));
        }

        // Now, go overboard, trying to preserve nano accuracy.
        // XXX: The following tests are a little controversial.
        // Some would prefer a gentler response -- just truncating/rounding to the nearest microsecond.
        // When these voices of reason prevail, this test should be replaced by a test that nano-noise
        // gets filtered out but the result is still correct to microsecond granularity.
        java.sql.Timestamp ts_nano = new java.sql.Timestamp(millisecondsSinceEpoch);
        assertEquals(ts, ts_nano);
        // Extract the 1000000 nanos (doubly-counted milliseconds)
        assertEquals(1000000, ts_nano.getNanos());
        // and explicitly add in 1001 nanos (1 microsecond + 1 nanosecond)
        ts_nano.setNanos(ts_nano.getNanos()+1001);

        // Should be off by 1 nano.
        assertNotSame(ts_micro, ts_nano);

        // A new round of inserts, trying to use the too accurate format.
        lowerBound = pkey.incrementAndGet();

        boolean caught;
        try {
            caught = false;
            // system-defined CRUD inputs
            cr = client.callProcedure("ALLOW_NULLS.insert", pkey.incrementAndGet(), 0, 0, 0, 0, 0.0, ts_nano,
                    null, null, null, null, null, null, null, null, null);
        } catch (RuntimeException e) {
            assertEquals("Can't serialize TIMESTAMP value with fractional microseconds", e.getMessage());
            caught = true;
        }
        assert(caught);

        try {
            caught = false;
            // user-defined statement inputs
            cr = client.callProcedure("PassObjectNull", pkey.incrementAndGet(), 0, 0, 0, 0, 0.0, ts_nano,
                    null, null, null, null, null, null, null, null, null);
        } catch (RuntimeException e) {
            assertEquals("Can't serialize TIMESTAMP value with fractional microseconds", e.getMessage());
            caught = true;
        }
        assert(caught);

        // Smuggling nanos into a stored procedure is also already covered by TestSQLFeaturesSuite's testPassAllArgTypes

        // Exceptions above should have pre-empted execution (and not just come after successful writes).
        cr = client.callProcedure("@AdHoc", "SELECT A_TIMESTAMP from ALLOW_NULLS where PKEY > " + lowerBound + ";");
        result = cr.getResults();
        assertEquals(0, result[0].getRowCount());
    }

    public void testPassingDateAndTimeObjectsBeforeEpochToStatements() throws Exception {
        final Client client = this.getClient();

        Random rn = new Random();

        for (int i = 0; i < 100; ++i) {
            long ts = rn.nextLong();
            long[] timestampValues = {
                    ts, // random
                    ts / 1000 * 1000, // even milliseconds
                    ts / 1000000 * 1000000, // even seconds
                    ts / 1000000 * -1000000 // even seconds
            };

            for (long microsecondsSinceEpoch: timestampValues) {
                TimestampType tst_micro = new TimestampType(microsecondsSinceEpoch);

                // Add 1 more millis from epoch
                java.sql.Timestamp ts_micro = VoltTypeUtil.getSqlTimestampFromMicrosSinceEpoch(microsecondsSinceEpoch);

                client.callProcedure("Insert", "ALLOW_NULLS", 0, 0, 0, 0, 0,
                                     null, tst_micro, null,
                                     null, null, null, null, null, null, null, null);

                VoltTable vt;
                vt = client.callProcedure("@AdHoc", "Select A_TIMESTAMP from allow_nulls where pkey = 0").getResults()[0];
                assertTrue(vt.advanceRow());
                assertEquals(microsecondsSinceEpoch, vt.getTimestampAsLong(0));
                assertEquals(tst_micro, vt.getTimestampAsTimestamp(0));
                assertEquals(ts_micro, vt.getTimestampAsSqlTimestamp(0));

                client.callProcedure("@AdHoc", "truncate table allow_nulls;");
            }
        }
    }

    // ENG-1276
    public void testPassingFloatToDoubleArg() throws Exception {
        final Client client = this.getClient();

        client.callProcedure("Insert", "ALLOW_NULLS", 0, 0, 0, 0, 0,
                             new Float(0.0), null, null,
                             null, null, null, null, null, null, null, null);
    }

    //
    // Insert strings that violate the VARCHAR size limit.
    //
    public void testInsertViolatesStringLength() throws IOException,
            ProcCallException {
        final Client client = this.getClient();
        boolean caught = false;

        // perform this test on the NULLS and NO_NULLS tables
        // by looping twice and setting params[0] differently each time.
        for (int i = 0; i < 2; ++i) {
            final Object params[] = new Object[COLS + 2];
            params[0] = (i == 0) ? "NO_NULLS" : "ALLOW_NULLS";

            // insert a string that violates the varchar size.
            // there are three strings in the schema with sizes
            // that can be violated. test each.
            // loop three times and set a different
            // varchar to the too-big value each time.
            for (int stringcount = 0; stringcount < 3; ++stringcount) {
                int curr_string = 0;
                params[1] = pkey.incrementAndGet();
                for (int k = 0; k < COLS; ++k) {
                    if ((m_types[k] == VoltType.STRING)
                            && (stringcount == curr_string)) {
                        params[k + 2] = ReallyLongString;
                    } else {
                        params[k + 2] = m_midValues[k];
                    }
                    if (m_types[k] == VoltType.STRING)
                        curr_string++;
                }
                try {
                    caught = false;
                    client.callProcedure("Insert", params);
                } catch (final ProcCallException e) {
                    caught = true;
                }

                assertTrue(caught);
            }
        }
    }

    //
    // Test that the max serializable string length is correctly handled.
    // It must be rejected always since it is greater than the max varchar size.
    //
    public void testMaxSerializeStringSize() throws IOException,
            ProcCallException {
        final Client client = getClient();
        boolean caught = false;
        final Object params[] = new Object[COLS + 2];
        params[0] = "NO_NULLS";

        // array to build the Big String.
        final char blob[] = new char[VoltType.MAX_VALUE_LENGTH + 4];
        for (int i = 0; i < blob.length; i++) {
            blob[i] = 'a';
        }

        // try to insert a max length string blob into each of the string fields
        // this string *is* fastserializable.
        for (int stringcount = 0; stringcount < 4; ++stringcount) {
            int curr_string = 0;
            params[1] = pkey.incrementAndGet();
            for (int k = 0; k < COLS; ++k) {
                if ((m_types[k] == VoltType.STRING)
                        && (stringcount == curr_string)) {
                    params[k + 2] = new String(blob);
                } else {
                    params[k + 2] = m_midValues[k];
                }
                if (m_types[k] == VoltType.STRING)
                    curr_string++;
            }
            try {
                caught = false;
                client.callProcedure("Insert", params);
            }
            catch (final ProcCallException e) {
                System.err.println(e.getMessage());
                assertTrue(e.toString().contains("exceeds the size of the VARCHAR"));
                caught = true;
            }
            assertTrue(caught);
        }
    }

    //
    // Test that the max supported varchar can be inserted.
    //
    public void testMaxValidStringSize() throws IOException, ProcCallException {
        final Client client = getClient();
        boolean caught = false;
        final Object params[] = new Object[COLS + 2];
        params[0] = "NO_NULLS";

        // array to build the Big String.
        final char blob[] = new char[VoltType.MAX_VALUE_LENGTH];
        for (int i = 0; i < blob.length; i++) {
            blob[i] = 'a';
        }

        // try to insert a max length string blob into each of the string fields
        // this string *is* fastserializable.
        for (int stringcount = 0; stringcount < 4; ++stringcount) {
            int curr_string = 0;
            params[1] = pkey.incrementAndGet();
            for (int k = 0; k < COLS; ++k) {
                if ((m_types[k] == VoltType.STRING)
                        && (stringcount == curr_string)) {
                    params[k + 2] = new String(blob);
                } else {
                    params[k + 2] = m_midValues[k];
                }
                if (m_types[k] == VoltType.STRING)
                    curr_string++;
            }
            try {
                caught = false;
                client.callProcedure("Insert", params);
            } catch (final ProcCallException e) {
                caught = true;
            }
            // the last (1048576) string should be fine here.
            if (stringcount != 3) {
                assertTrue(caught);
            } else {
                assertFalse(caught);
            }
        }
    }

    //
    // Verify that NULLS are rejected in in NOT NULL columns
    //
    public void testInsertNulls_No_Nulls() throws IOException {
        final Client client = this.getClient();

        // Insert a NULL value for each column. For the first
        // row, insert null in the first column, for the 5th row
        // in the 5 column, etc.

        final Object params[] = new Object[COLS + 2];

        for (int k = 0; k < COLS; ++k) {
            boolean caught = false;

            // build the parameter list as described above
            params[0] = "NO_NULLS";
            params[1] = pkey.incrementAndGet();
            for (int i = 0; i < COLS; i++) {
                params[i + 2] = (i == k) ? m_nullValues[i] : m_midValues[i];
                assert (params[i + 2] != null);
            }

            // Each insert into the NO_NULLS table must fail with a
            // constraint failure. Verify this.

            System.out.println("testNullsRejected: :" + k + " " + m_types[k]);
            try {
                client.callProcedure("Insert", params);
            } catch (final ProcCallException e) {
                if (e.getMessage().contains("CONSTRAINT VIOLATION"))
                    caught = true;
                else {
                    e.printStackTrace();
                    fail();
                }
            } catch (final NoConnectionsException e) {
                e.printStackTrace();
                fail();
            }
            assertTrue(caught);
        }
    }

    //
    // Verify that NULLS are allowed in non-NOT NULL columns
    //
    public void testInsertNulls_Nulls_Allowed() throws IOException {
        final Client client = this.getClient();

        // Insert a NULL value for each column. For the first
        // row, insert null in the first column, for the 5th row
        // in the 5 column, etc.

        final Object params[] = new Object[COLS + 2];

        for (int k = 0; k < COLS; ++k) {

            // build the parameter list as described above
            params[0] = "";
            params[1] = pkey.incrementAndGet();
            for (int i = 0; i < COLS; i++) {
                params[i + 2] = (i == k) ? m_nullValues[i] : m_midValues[i];
                assert (params[i + 2] != null);
            }

            // Each insert in to the ALLOW_NULLS table must succeed.
            // Perform the inserts and execute selects, verifying the
            // content of the select matches the parameters passed to
            // insert

            System.out.println("testNullsAllowed: " + k + " NULL type is "
                    + m_types[k]);

            try {
                params[0] = "ALLOW_NULLS";
                // We'll use the multi-partition insert for this test. Between
                // this and testInsertNull_No_Nulls we should cover both
                // cases in ticket 306
                client.callProcedure("InsertMulti", params);
            } catch (final ProcCallException e) {
                e.printStackTrace();
                fail();
            } catch (final NoConnectionsException e) {
                e.printStackTrace();
                fail();
            }

            // verify that the row was inserted
            try {
                final VoltTable[] result = client.callProcedure("Select",
                        "ALLOW_NULLS", pkey.get()).getResults();
                final VoltTableRow row = result[0].fetchRow(0);
                for (int i = 0; i < COLS; ++i) {
                    final Object obj = row.get(i + 1, m_types[i]);
                    if (i == k) {
                        assertTrue(row.wasNull());
                        System.out.println("Row " + i + " verifed as NULL");
                    } else {
                        assertTrue(comparisonHelper(obj, params[i + 2],
                                m_types[i]));
                    }
                }
            } catch (final Exception ex) {
                ex.printStackTrace();
                fail();
            }
        }
    }

    public void testUpdateToNull() throws IOException, ProcCallException {
        final Client client = this.getClient();

        final Object params[] = new Object[COLS + 2];

        for (int k = 0; k < COLS; ++k) {

            // build the parameter list as described above
            // Fill the row with non-null data and insert
            params[0] = "";
            params[1] = pkey.incrementAndGet();
            for (int i = 0; i < COLS; i++) {
                params[i + 2] = m_midValues[i];
                assert (params[i + 2] != null);
            }
            params[0] = "ALLOW_NULLS";
            client.callProcedure("Insert", params);

            for (int i = 0; i < COLS; i++) {
                params[i + 2] = (i == k) ? m_nullValues[i] : m_midValues[i];
                assert (params[i + 2] != null);
            }

            try {
                client.callProcedure("Update", params);
            } catch (final ProcCallException e) {
                e.printStackTrace();
                fail(e.getMessage());
            } catch (final NoConnectionsException e) {
                e.printStackTrace();
                fail(e.getMessage());
            }

            // verify that the row was updated
            final VoltTable[] result = client.callProcedure("Select",
                    "ALLOW_NULLS", pkey.get()).getResults();
            final VoltTableRow row = result[0].fetchRow(0);
            for (int i = 0; i < COLS; ++i) {
                final Object obj = row.get(i + 1, m_types[i]);
                if (i == k) {
                    assertTrue(row.wasNull());
                } else {
                    assertTrue(comparisonHelper(obj, params[i + 2], m_types[i]));
                }
            }
        }
    }

    public void testUpdateFromNull() throws NoConnectionsException,
            ProcCallException, IOException {
        final Client client = this.getClient();

        final Object params[] = new Object[COLS + 2];

        for (int k = 0; k < COLS; ++k) {

            // build the parameter list as described above
            // Fill the row with diagonal null data and insert
            params[0] = "";
            params[1] = pkey.incrementAndGet();
            for (int i = 0; i < COLS; i++) {
                params[i + 2] = (i == k) ? m_nullValues[i] : m_midValues[i];
                assert (params[i + 2] != null);
            }
            params[0] = "ALLOW_NULLS";
            client.callProcedure("Insert", params);

            for (int i = 0; i < COLS; i++) {
                params[i + 2] = m_midValues[i];
                assert (params[i + 2] != null);
            }

            try {
                client.callProcedure("Update", params);
            } catch (final ProcCallException e) {
                e.printStackTrace();
                fail();
            } catch (final NoConnectionsException e) {
                e.printStackTrace();
                fail();
            }

            // verify that the row was updated
            final VoltTable[] result = client.callProcedure("Select",
                    "ALLOW_NULLS", pkey.get()).getResults();
            final VoltTableRow row = result[0].fetchRow(0);
            for (int i = 0; i < COLS; ++i) {
                final Object obj = row.get(i + 1, m_types[i]);
                assertTrue(comparisonHelper(obj, params[i + 2], m_types[i]));
            }
        }
    }

    public void testDeleteNulls() throws NoConnectionsException,
            ProcCallException, IOException {
        final Client client = this.getClient();

        // Insert a NULL value for each column. For the first
        // row, insert null in the first column, for the 5th row
        // in the 5 column, etc.

        final Object params[] = new Object[COLS + 2];

        for (int k = 0; k < COLS; ++k) {

            // build the parameter list as described above
            // Fill the row with diagonal null data and insert
            params[0] = "ALLOW_NULLS";
            params[1] = pkey.incrementAndGet();
            for (int i = 0; i < COLS; i++) {
                params[i + 2] = (i == k) ? m_nullValues[i] : m_midValues[i];
                assert (params[i + 2] != null);
            }
            client.callProcedure("Insert", params);
            VoltTable[] result = client.callProcedure("Select", "ALLOW_NULLS",
                    pkey.get()).getResults();
            System.out.println(result[0]);

            try {
                client.callProcedure("Delete", "ALLOW_NULLS", pkey.get());
            } catch (final ProcCallException e) {
                e.printStackTrace();
                fail();
            } catch (final NoConnectionsException e) {
                e.printStackTrace();
                fail();
            }

            // verify that the row was deleted
            result = client.callProcedure("Select", "ALLOW_NULLS", pkey.get())
                    .getResults();
            assertEquals(0, result[0].getRowCount());
        }
    }

    public void testInsertNullBoxed() throws IOException, ProcCallException {
        Client client = this.getClient();

        Integer p_key = pkey.incrementAndGet();
        VoltTable[] results = client.callProcedure("InsertBoxed", p_key,
                new Byte( (byte) -128), new Short( (short) -32768),
                new Integer(-2147483648), new Long(-9223372036854775808L) ).getResults();

        System.out.println("testInsertBoxedNulls" + results[1]);

        results[1].advanceRow();
        assertEquals(VoltType.NULL_TINYINT, results[1].get("A_TINYINT", VoltType.TINYINT));
        assertEquals(VoltType.NULL_SMALLINT, results[1].get("A_SMALLINT", VoltType.SMALLINT));
        assertEquals(VoltType.NULL_INTEGER, results[1].get("A_INTEGER", VoltType.INTEGER));
        assertEquals(VoltType.NULL_BIGINT, results[1].get("A_BIGINT", VoltType.BIGINT));

        results = client.callProcedure("@AdHoc", "SELECT * FROM WITH_DEFAULTS WHERE A_TINYINT IS NULL").getResults();
        results[0].advanceRow();
        assertEquals(p_key, results[0].get("PKEY", VoltType.INTEGER));
    }

    public void testInsertNullValues() throws IOException, ProcCallException {
        Client client = this.getClient();

        Integer p_key = pkey.incrementAndGet();
        VoltTable[] results = client.callProcedure("InsertBoxed", p_key,
                null, null, null, null).getResults();

        System.out.println("testInsertNullValues" + results[1]);

        results[1].advanceRow();
        assertEquals(VoltType.NULL_TINYINT, results[1].get("A_TINYINT", VoltType.TINYINT));
        assertEquals(VoltType.NULL_SMALLINT, results[1].get("A_SMALLINT", VoltType.SMALLINT));
        assertEquals(VoltType.NULL_INTEGER, results[1].get("A_INTEGER", VoltType.INTEGER));
        assertEquals(VoltType.NULL_BIGINT, results[1].get("A_BIGINT", VoltType.BIGINT));

        results = client.callProcedure("@AdHoc", "SELECT * FROM WITH_DEFAULTS WHERE A_TINYINT IS NULL").getResults();
        results[0].advanceRow();
        assertEquals(p_key, results[0].get("PKEY", VoltType.INTEGER));
    }

    public void testMissingAttributeInsert_With_Defaults()
            throws NoConnectionsException, ProcCallException, IOException {
        Client client = this.getClient();

        Object params[] = new Object[COLS + 2];

        params[0] = "WITH_DEFAULTS";
        params[1] = pkey.incrementAndGet();
        for (int i = 0; i < COLS; i++) {
            params[i + 2] = m_defaultValues[i];
            assert (params[i + 2] != null);
        }

        try {
            client.callProcedure("Insert", params);
        } catch (ProcCallException e) {
            e.printStackTrace();
            fail();
        } catch (NoConnectionsException e) {
            e.printStackTrace();
            fail();
        }

        VoltTable[] result = client.callProcedure("Select", "WITH_DEFAULTS",
                pkey.get()).getResults();
        VoltTableRow row = result[0].fetchRow(0);
        for (int i = 0; i < COLS; ++i) {
            Object obj = row.get(i + 1, m_types[i]);
            if (m_types[i] == VoltType.GEOGRAPHY || m_types[i] == VoltType.GEOGRAPHY_POINT) {
                // Default values are not supported for these types (yet?)
                assertNull(obj);
            }
            else {
                assertTrue("Expected to be equal: (" + obj + ", " + params[i + 2] + ")",
                        comparisonHelper(obj, params[i + 2], m_types[i]));
            }
        }
    }

    public void testMissingAttributeInsert_With_Null_Defaults()
            throws NoConnectionsException, ProcCallException, IOException {
        Client client = this.getClient();

        Object params[] = new Object[COLS + 2];

        params[0] = "WITH_NULL_DEFAULTS";
        params[1] = pkey.incrementAndGet();
        for (int i = 0; i < COLS; i++) {
            params[i + 2] = m_nullValues[i];
            assert (params[i + 2] != null);
        }

        try {
            client.callProcedure("Insert", params);
        } catch (ProcCallException e) {
            e.printStackTrace();
            fail();
        } catch (NoConnectionsException e) {
            e.printStackTrace();
            fail();
        }

        VoltTable[] result = client.callProcedure("Select",
                "WITH_NULL_DEFAULTS", pkey.get()).getResults();
        VoltTableRow row = result[0].fetchRow(0);
        for (int i = 0; i < COLS; ++i) {
            Object obj = row.get(i + 1, m_types[i]);
            assertTrue("Expected to be equal: (" + obj + ", " + params[i + 2] + ")",
                    comparisonHelper(obj, params[i + 2], m_types[i]));
        }
    }

    //
    // Round trip the maximum value
    //
    public void testInsertMaxValues_No_Nulls() throws NoConnectionsException,
            ProcCallException, IOException {
        final Client client = this.getClient();

        // Insert a MAX value for each column. For the first
        // row, insert MAX in the first column, for the 5th row
        // in the 5 column, etc.

        final Object params[] = new Object[COLS + 2];

        for (int k = 0; k < COLS; ++k) {

            // build the parameter list as described above
            params[0] = "";
            params[1] = pkey.incrementAndGet();
            for (int i = 0; i < COLS; i++) {
                params[i + 2] = (i == k) ? m_maxValues[i] : m_midValues[i];
                assert (params[i + 2] != null);
            }

            // Perform the inserts and execute selects, verifying the
            // content of the select matches the parameters passed to
            // insert

            System.out.println("testInsertMaxValues: " + k + " MAX type is "
                    + m_types[k]);
            params[0] = "NO_NULLS";
            client.callProcedure("Insert", params);
            // verify that the row was updated
            final VoltTable[] result = client.callProcedure("Select",
                    "NO_NULLS", pkey.get()).getResults();
            final VoltTableRow row = result[0].fetchRow(0);
            for (int i = 0; i < COLS; ++i) {
                final Object obj = row.get(i + 1, m_types[i]);
                assertTrue(!row.wasNull());
                assertTrue(comparisonHelper(obj, params[i + 2], m_types[i]));
            }
        }
    }

    //
    // Round trip the minimum value.
    //
    public void testInsertMinValues_No_Nulls() throws NoConnectionsException,
            ProcCallException, IOException {
        final Client client = this.getClient();

        // Insert a MIN value for each column. For the first
        // row, insert null in the first column, for the 5th row
        // in the 5 column, etc.

        final Object params[] = new Object[COLS + 2];

        for (int k = 0; k < COLS; ++k) {

            // build the parameter list as described above
            params[0] = "";
            params[1] = pkey.incrementAndGet();
            for (int i = 0; i < COLS; i++) {
                params[i + 2] = (i == k) ? m_minValues[i] : m_midValues[i];
                assert (params[i + 2] != null);
            }

            // Perform the inserts and execute selects, verifying the
            // content of the select matches the parameters passed to
            // insert

            System.out.println("testInsertMinValues: " + k + " MIN type is "
                    + m_types[k]);
            params[0] = "NO_NULLS";
            client.callProcedure("Insert", params);
            final VoltTable[] result = client.callProcedure("Select",
                    "NO_NULLS", pkey.get()).getResults();
            final VoltTableRow row = result[0].fetchRow(0);
            for (int i = 0; i < COLS; ++i) {
                final Object obj = row.get(i + 1, m_types[i]);
                assertTrue(!row.wasNull());
                assertTrue(comparisonHelper(obj, params[i + 2], m_types[i]));
            }
        }
    }

    //
    // Apply a simple expression to each type that supports math.
    //
    public void testSimpleExpressions() throws NoConnectionsException,
            ProcCallException, IOException {
        final Client client = this.getClient();

        // Build a simple expression to do addition and select one column at
        // a time, using that expression in a trivial projection.

        // insert one row with the mid values
        final Object params[] = new Object[COLS + 2];
        params[0] = "NO_NULLS";
        params[1] = pkey.incrementAndGet();
        for (int i = 0; i < COLS; i++) {
            params[i + 2] = m_midValues[i];
        }
        client.callProcedure("Insert", params);

        // insert one row with the max values
        params[0] = "NO_NULLS";
        params[1] = pkey.incrementAndGet();
        for (int i = 0; i < COLS; i++) {
            params[i + 2] = m_maxValues[i];
        }
        client.callProcedure("Insert", params);

        // select A + 11 from no_nulls where A = mid_value
        for (int i = 0; i < COLS; i++) {
            if (!m_supportsMath[i])
                continue;

            // TODO see trac 236.
            // Would be better here to select where the column under test
            // equals its mid value - but decimals can't do that.
            final String sql = "SELECT (" + m_columnNames[i]
                    + " + 11) from NO_NULLS where " + m_columnNames[3] + " = "
                    + m_midValues[3];
            System.out.println("testsimpleexpression: " + sql);
            final VoltTable[] result = client.callProcedure("@AdHoc", sql)
                    .getResults();
            final VoltTableRow row = result[0].fetchRow(0);
            final Object obj = row.get(0, m_types[i]);

            final double expect = ((Number) m_midValues[i]).doubleValue() + 11;
            final double got = ((Number) obj).doubleValue();
            System.out.println("Expect: " + expect + " got: " + got);
            assertEquals(expect, got);
        }
    }

    public void testJumboRow() throws Exception {
        final Client client = getClient();
        byte firstStringBytes[] = new byte[1048576];
        java.util.Arrays.fill(firstStringBytes, (byte) 'c');
        String firstString = new String(firstStringBytes, "UTF-8");
        byte secondStringBytes[] = new byte[1048564];
        java.util.Arrays.fill(secondStringBytes, (byte) 'a');
        String secondString = new String(secondStringBytes, "UTF-8");

        Object params[] = new Object[] { "JUMBO_ROW", 0, 0, 0, 0, 0, 0.0,
                new TimestampType(0), firstString, secondString, "", "",
                new byte[0], new byte[0], VoltType.NULL_DECIMAL, null, null };
        VoltTable results[] = client.callProcedure("Insert", params)
                .getResults();
        params = null;
        firstString = null;
        secondString = null;

        assertEquals(results.length, 1);
        assertEquals(1, results[0].asScalarLong());

        results = client.callProcedure("Select", "JUMBO_ROW", 0).getResults();
        assertEquals(results.length, 1);
        assertTrue(results[0].advanceRow());
        assertTrue(java.util.Arrays.equals(results[0].getStringAsBytes(1),
                firstStringBytes));
        assertTrue(java.util.Arrays.equals(results[0].getStringAsBytes(2),
                secondStringBytes));

        java.util.Arrays.fill(firstStringBytes, (byte) 'q');
        firstString = new String(firstStringBytes, "UTF-8");
        java.util.Arrays.fill(secondStringBytes, (byte) 'r');
        secondString = new String(secondStringBytes, "UTF-8");

        params = new Object[] { "JUMBO_ROW", 0, 0, 0, 0, 0, 0.0,
                new TimestampType(0), firstString, secondString, "", "",
                new byte[0], new byte[0], VoltType.NULL_DECIMAL, null, null };

        results = client.callProcedure("Update", params).getResults();
        params = null;
        firstString = null;
        secondString = null;

        assertEquals(results.length, 1);
        assertEquals(1, results[0].asScalarLong());

        results = client.callProcedure("Select", "JUMBO_ROW", 0).getResults();
        assertEquals(results.length, 1);
        assertTrue(results[0].advanceRow());
        assertTrue(java.util.Arrays.equals(results[0].getStringAsBytes(1),
                firstStringBytes));
        assertTrue(java.util.Arrays.equals(results[0].getStringAsBytes(2),
                secondStringBytes));
    }

    public void testUpdateDecimalWithPVE() throws NoConnectionsException,
            ProcCallException, IOException {
        // insert a couple of rows.
        final Client client = this.getClient();
        final Object params[] = new Object[COLS + 2];
        params[0] = "ALLOW_NULLS";
        params[1] = 0;
        for (int i = 0; i < COLS; i++) {
            params[i + 2] = m_midValues[i];
        }
        client.callProcedure("Insert", params);

        // insert one row with the max values
        params[0] = "ALLOW_NULLS";
        params[1] = 1;
        for (int i = 0; i < COLS; i++) {
            params[i + 2] = m_maxValues[i];
        }
        client.callProcedure("Insert", params);

        // update the mid value to the minimum decimal value
        VoltTable[] result = client.callProcedure("UpdateDecimal",
                m_minValues[12], m_midValues[12]).getResults();

        // select that same row again by primary key
        result = client.callProcedure("Select", "ALLOW_NULLS", 0).getResults();

        // and verify the row
        final VoltTableRow row = result[0].fetchRow(0);
        final BigDecimal bd = (row.getDecimalAsBigDecimal(13));
        assertTrue(comparisonHelper(m_minValues[12], bd, m_types[12]));
    }

    private void helper_testInvalidParameterSerializations(Client client,
            Object[] params) throws NoConnectionsException, IOException,
            ProcCallException {
        try {
            client.callProcedure("ParamSetArrays", params);
        } catch (RuntimeException e) {
            assertTrue(e.getCause() instanceof IOException);
        }
    }

    public void testInvalidParameterSerializations()
            throws NoConnectionsException, ProcCallException, IOException {
        final Client client = this.getClient();
        final Object params[] = new Object[8];

        params[0] = new short[1];
        params[1] = new int[1];
        params[2] = new long[1];
        params[3] = new double[1];
        params[4] = new String[1];
        params[5] = new TimestampType[1];
        params[6] = new BigDecimal[1];
        params[7] = new byte[1];

        // make sure the procedure CAN work.
        client.callProcedure("ParamSetArrays", params);

        // now cycle through invalid array lengths
        // these should fail in client serialization to the server
        params[0] = new short[Short.MAX_VALUE + 1];
        helper_testInvalidParameterSerializations(client, params);

        params[0] = new short[1];
        params[1] = new int[Short.MAX_VALUE + 1];
        helper_testInvalidParameterSerializations(client, params);

        params[1] = new int[1];
        params[2] = new long[Short.MAX_VALUE + 1];
        helper_testInvalidParameterSerializations(client, params);

        params[2] = new long[1];
        params[3] = new double[Short.MAX_VALUE + 1];
        helper_testInvalidParameterSerializations(client, params);

        params[3] = new double[1];
        params[4] = new String[Short.MAX_VALUE + 1];
        helper_testInvalidParameterSerializations(client, params);

        params[4] = new String[1];
        params[5] = new TimestampType[Short.MAX_VALUE + 1];
        helper_testInvalidParameterSerializations(client, params);

        params[5] = new TimestampType[1];
        params[6] = new BigDecimal[Short.MAX_VALUE + 1];
        helper_testInvalidParameterSerializations(client, params);

        params[6] = new BigDecimal[1];
        helper_testInvalidParameterSerializations(client, params);
    }

    public void testEng5013() throws NoConnectionsException, ProcCallException, IOException {
        Client client = this.getClient();

        client.callProcedure("InsertDecimal", 1, 3.4f);
        client.callProcedure("InsertDecimal", 2, 3.4d);
        client.callProcedure("InsertDecimal", 3, 1f);
        client.callProcedure("InsertDecimal", 4, 1d);
        client.callProcedure("InsertDecimal", 5, 0.25f);
        client.callProcedure("InsertDecimal", 6, 0.25d);
        client.callProcedure("InsertDecimal", 7, 3.3f);
        client.callProcedure("InsertDecimal", 8, 3.3d);

        try {
            client.callProcedure("InsertDecimal", 9, Double.MAX_VALUE);
            fail();
        } catch (ProcCallException e) {
            // should give out of precision range error
            assertTrue(e.getMessage().contains("has more than 38 digits of precision"));
        } catch (Exception e) {
            fail();
        }
        try {
            client.callProcedure("InsertDecimal", 9, -Double.MAX_VALUE);
            fail();
        } catch (ProcCallException e) {
            // should give out of precision range error
            assertTrue(e.getMessage().contains("has more than 38 digits of precision"));
        } catch (Exception e) {
            fail();
        }
        try {
            client.callProcedure("InsertDecimal", 9, Float.MAX_VALUE);
            fail();
        } catch (ProcCallException e) {
            // should give out of precision range error
            assertTrue(e.getMessage().contains("has more than 38 digits of precision"));
        } catch (Exception e) {
            fail();
        }
        try {
            client.callProcedure("InsertDecimal", 9, -Float.MAX_VALUE);
            fail();
        } catch (ProcCallException e) {
            // should give out of precision range error
            assertTrue(e.getMessage().contains("has more than 38 digits of precision"));
        } catch (Exception e) {
            fail();
        }
        double nand = 0.0d / 0.0d;
        float nanf = 0.0f / 0.0f;
        try {
            client.callProcedure("InsertDecimal", 9, nand);
        } catch (ProcCallException e) {
            // passing a NaN value will cause NumberFormatException, and fail the proceudre call
            assertTrue(e.getMessage().contains("NumberFormatException"));
        } catch (Exception e) {
            fail();
        }
        try {
            client.callProcedure("InsertDecimal", 9, nanf);
            fail();
        } catch (ProcCallException e) {
            // passing a NaN value will cause NumberFormatException, and fail the proceudre call
            assertTrue(e.getMessage().contains("NumberFormatException"));
        } catch (Exception e) {
            fail();
        }

        client.callProcedure("InsertDecimal", 9, Double.MIN_VALUE);
        client.callProcedure("InsertDecimal", 10, Float.MIN_VALUE);

        // will lose some precision by truncated to .12f
        client.callProcedure("InsertDecimal", 11, 123456789.01234567890123456789f);
        VoltTable table;
        table = client.callProcedure("@AdHoc", "SELECT A_DECIMAL FROM WITH_DEFAULTS WHERE PKEY = 11").getResults()[0];
        assertTrue(table.getRowCount() == 1);
        table.advanceRow();
        float f = table.getDecimalAsBigDecimal(0).floatValue();
        assertEquals(123456789.01234567890123456789f, f, 0.000000000001);
        // will lose some precision by truncated to .12f
        client.callProcedure("InsertDecimal", 12, 123456789.01234567890123456789d);
        table = client.callProcedure("@AdHoc", "SELECT A_DECIMAL FROM WITH_DEFAULTS WHERE PKEY = 12").getResults()[0];
        assertTrue(table.getRowCount() == 1);
        table.advanceRow();
        double d = table.getDecimalAsBigDecimal(0).doubleValue();
        assertEquals(123456789.01234567890123456789d, d, 0.000000000001);
    }

    //
    // JUnit / RegressionSuite boilerplate
    //
    public TestSQLTypesSuite(final String name) {
        super(name);
    }

    static public junit.framework.Test suite() {

        VoltServerConfig config = null;
        final MultiConfigSuiteBuilder builder = new MultiConfigSuiteBuilder(
                TestSQLTypesSuite.class);

        final VoltProjectBuilder project = new VoltProjectBuilder();
        project.addSchema(TestSQLTypesSuite.class
                .getResource("sqltypessuite-ddl.sql"));
        project.addSchema(TestSQLTypesSuite.class
                .getResource("sqltypessuite-nonulls-ddl.sql"));
        project.addPartitionInfo("NO_NULLS", "PKEY");
        project.addPartitionInfo("ALLOW_NULLS", "PKEY");
        project.addPartitionInfo("WITH_DEFAULTS", "PKEY");
        project.addPartitionInfo("WITH_NULL_DEFAULTS", "PKEY");
        project.addPartitionInfo("EXPRESSIONS_WITH_NULLS", "PKEY");
        project.addPartitionInfo("EXPRESSIONS_NO_NULLS", "PKEY");
        project.addPartitionInfo("JUMBO_ROW", "PKEY");
        project.addMultiPartitionProcedures(MP_PROCEDURES);

        project.addProcedure(Delete.class, "ALLOW_NULLS.PKEY: 1");
        project.addProcedure(Insert.class, "NO_NULLS.PKEY: 1");
        project.addProcedure(Select.class, "NO_NULLS.PKEY: 1");
        project.addProcedure(Update.class, "NO_NULLS.PKEY: 1");


        project.addStmtProcedure(
                "PassObjectNull",
                "insert into ALLOW_NULLS values ( ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);",
                "NO_NULLS.PKEY: 0");
        project.addStmtProcedure("InsertDecimal", "INSERT INTO WITH_DEFAULTS (PKEY, A_DECIMAL) VALUES (?, ?);");

        boolean success;

        // JNI
        config = new LocalCluster("sqltypes-onesite.jar", 1, 1, 0, BackendTarget.NATIVE_EE_JNI);
        success = config.compile(project);
        assertTrue(success);
        builder.addServerConfig(config);

        // CLUSTER?
        config = new LocalCluster("sqltypes-cluster.jar", 2, 2, 1, BackendTarget.NATIVE_EE_JNI);
        success = config.compile(project);
        assertTrue(success);
        builder.addServerConfig(config);

        return builder;
    }
}
