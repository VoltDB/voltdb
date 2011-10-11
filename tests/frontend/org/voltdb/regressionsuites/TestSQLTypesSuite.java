/* This file is part of VoltDB.
 * Copyright (C) 2008-2011 VoltDB Inc.
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
import java.util.concurrent.atomic.AtomicInteger;

import org.voltdb.BackendTarget;
import org.voltdb.VoltTable;
import org.voltdb.VoltTableRow;
import org.voltdb.VoltType;
import org.voltdb.client.Client;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcCallException;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.types.TimestampType;
import org.voltdb.types.VoltDecimalHelper;
import org.voltdb.utils.Encoder;
import org.voltdb_testprocs.regressionsuites.sqltypesprocs.Delete;
import org.voltdb_testprocs.regressionsuites.sqltypesprocs.Insert;
import org.voltdb_testprocs.regressionsuites.sqltypesprocs.InsertBase;
import org.voltdb_testprocs.regressionsuites.sqltypesprocs.InsertMulti;
import org.voltdb_testprocs.regressionsuites.sqltypesprocs.ParamSetArrays;
import org.voltdb_testprocs.regressionsuites.sqltypesprocs.Select;
import org.voltdb_testprocs.regressionsuites.sqltypesprocs.Update;
import org.voltdb_testprocs.regressionsuites.sqltypesprocs.UpdateDecimal;

public class TestSQLTypesSuite extends RegressionSuite {

    // used to generate unique pkeys
    public static final AtomicInteger pkey = new AtomicInteger(0);

    // constant for 0x00
    private static final byte OO = (byte) 0x00; // font test?

    // 1500 character string
    private static final String ReallyLongString;

    /** Procedures used by this suite */
    static final Class<?>[] PROCEDURES = { Delete.class, Insert.class,
            InsertBase.class, InsertMulti.class, Select.class, Update.class,
            UpdateDecimal.class, ParamSetArrays.class };

    /** Utility to create an array of bytes with value "b" of length "length" */
    public byte[] byteArray(final int length, final byte b) {
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
            return (d1.compareTo(d2) == 0);
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
        }

        return false;
    }

    //
    // UPDATE WHEN ADDING NEW TYPE
    //

    // Column count in each sqltypessuite-ddl.sql table NOT including PKEY
    public static int COLS = 13;

    // Interesting sets of values for the various types

    // tests rely on this ordering of the string varchar widths
    public static VoltType[] m_types = { VoltType.TINYINT, VoltType.SMALLINT,
            VoltType.INTEGER, VoltType.BIGINT, VoltType.FLOAT,
            VoltType.TIMESTAMP, VoltType.STRING, // varchar(4)
            VoltType.STRING, // varchar(63)
            VoltType.STRING, // varchar(1024)
            VoltType.STRING, // varchar(42000)
            VoltType.VARBINARY, // varbinary(32)
            VoltType.VARBINARY, // varbinary(256)
            VoltType.DECIMAL
    // UPDATE WHEN ADDING NEW TYPE
    };

    // used to filter types that don't support arithmetic expressions
    public boolean[] m_supportsMath = { true, // tinyint
            true, // smallint
            true, // integer
            true, // bigint
            true, // float
            false, // timestamp
            false, // string
            false, // string
            false, // string
            false, // string
            false, // varbinary
            false, // varbinary
            true // decimal
    // UPDATE WHEN ADDING NEW TYPE
    };

    // the column names from the DDL used to dynamically create
    // sql select lists.
    public static String[] m_columnNames = { "A_TINYINT", "A_SMALLINT",
            "A_INTEGER", "A_BIGINT", "A_FLOAT", "A_TIMESTAMP", "A_INLINE_S1",
            "A_INLINE_S2", "A_POOL_S", "A_POOL_MAX_S", "A_INLINE_B", "A_POOL_B",
            "A_DECIMAL"
    // UPDATE WHEN ADDING NEW TYPE
    };

    // sql null representation for each type
    public Object[] m_nullValues = { VoltType.NULL_TINYINT,
            VoltType.NULL_SMALLINT, VoltType.NULL_INTEGER,
            VoltType.NULL_BIGINT, VoltType.NULL_FLOAT,
            VoltType.NULL_TIMESTAMP,
            VoltType.NULL_STRING_OR_VARBINARY, // inlined LT ptr size
            VoltType.NULL_STRING_OR_VARBINARY, // inlined GT ptr size
            VoltType.NULL_STRING_OR_VARBINARY, // not inlined (1024)
            VoltType.NULL_STRING_OR_VARBINARY, // not inlined (max length)
            VoltType.NULL_STRING_OR_VARBINARY,
            VoltType.NULL_STRING_OR_VARBINARY, VoltType.NULL_DECIMAL
    // UPDATE WHEN ADDING NEW TYPE
    };

    // maximum value for each type
    public static Object[] m_maxValues = { Byte.MAX_VALUE,
            Short.MAX_VALUE,
            Integer.MAX_VALUE,
            Long.MAX_VALUE,
            Double.MAX_VALUE,
            new TimestampType(Long.MAX_VALUE),
            new String("ZZZZ"),
            new String("ZZZZZZZZZZ" + // 10
                    "ZZZZZZZZZZ" + // 20
                    "ZZZZZZZZZZ" + // 30
                    "ZZZZZZZZZZ" + // 40
                    "ZZZZZZZZZZ" + // 50
                    "ZZZZZZZZZZ" + // 60
                    "ZZZ"), // 63
            "",
            "",
            new String("ZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZ").getBytes(),
            new byte[] {},
            new BigDecimal(new BigInteger(
                    "99999999999999999999999999999999999999"))
                    .scaleByPowerOfTen(-1 * VoltDecimalHelper.kDefaultScale)
    // UPDATE WHEN ADDING NEW TYPE
    };

    static {
        StringBuilder sb = new StringBuilder(1048576);
        int ii = 0;
        for (; ii < 65536; ii++) {
            sb.append('Z');
        }
        m_maxValues[8] = sb.toString();
        for (; ii < 1048576; ii++) {
            sb.append('Z');
        }
        m_maxValues[9] = sb.toString();
        sb = new StringBuilder(102400);
        for (ii = 0; ii < 102400; ii++) {
            sb.append('a');
        }
        ReallyLongString = sb.toString();
    }

    // a non-max, non-min value for each type
    public static Object[] m_midValues = {
            new Byte((byte) 10),
            new Short((short) 11),
            new Integer(12),
            new Long(13),
            new Double(14.5),
            new TimestampType(),
            new String("xyz"),
            new String("xyzab"),
            new String("xyzabcdefghijklmnopqrstuvw"),
            new String("xyzabcdefghijklmnopqrstuvw"),
            new byte[] { 'a', 'b', 'c' },
            new byte[] { 'a', 'b', 'c' },
            new BigDecimal(new BigInteger("5115101010101010345634"))
                    .scaleByPowerOfTen(-1 * VoltDecimalHelper.kDefaultScale)
    // UPDATE WHEN ADDING NEW TYPE
    };

    // minimum value for each type
    public Object[] m_minValues = {
            new Byte((byte) (Byte.MIN_VALUE + 1)), // MIN is NULL
            new Short((short) (Short.MIN_VALUE + 1)), // MIN is NULL
            Integer.MIN_VALUE + 1, // MIN is NULL
            Long.MIN_VALUE + 1, // MIN is NULL
            Double.MIN_VALUE, // NULL is -1.7E308.
            new TimestampType(Long.MIN_VALUE + 1),
            new String(byteArray(1, OO)),
            new String(byteArray(1, OO)),
            new String(byteArray(1, OO)),
            new String(byteArray(1, OO)),
            new byte[] { 0 },
            new byte[] { 0 },
            new BigDecimal(new BigInteger(
                    "-99999999999999999999999999999999999999"))
                    .scaleByPowerOfTen(-1 * VoltDecimalHelper.kDefaultScale)
    // UPDATE WHEN ADDING NEW TYPE
    };

    // default (defined in DDL) value for each type
    public static Object[] m_defaultValues = {
            new Byte((byte) (1)),
            new Short((short) (2)),
            3,
            4L,
            5.1,
            new TimestampType(600000),
            new String("abcd"),
            new String("abcdefghij"),
            new String("abcdefghijklmnopqrstuvwxyz"),
            new String("abcdefghijklmnopqrstuvwxyz"),
            new String("ABCDEFABCDEF0123"),
            new String("ABCDEFABCDEF0123456789"),
            new BigDecimal(new BigInteger("6000000000000"))
                    .scaleByPowerOfTen(-1 * VoltDecimalHelper.kDefaultScale)
    // UPDATE WHEN ADDING NEW TYPE
    };

    public void testPassingNullObjectToSingleStmtProcedure() throws Exception {
        final Client client = this.getClient();

        client.callProcedure("PassObjectNull", 0, 0, 0, 0, 0, 0.0, null, null,
                null, null, null, null, null, null);
    }

    // ENG-1276
    public void testPassingFloatToDoubleArg() throws Exception {
        final Client client = this.getClient();

        client.callProcedure("Insert", "ALLOW_NULLS", 0, 0, 0, 0, 0,
                             new Float(0.0), null, null,
                             null, null, null, null, null, null);
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
            } catch (final RuntimeException e) {
                assertTrue(e.getCause() instanceof java.io.IOException);
                assertTrue(e.toString().contains(
                        "String exceeds maximum length of"));
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
            assertTrue(comparisonHelper(obj, params[i + 2], m_types[i]));
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
            assertTrue(comparisonHelper(obj, params[i + 2], m_types[i]));
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
                new byte[0], new byte[0], VoltType.NULL_DECIMAL };
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
                new byte[0], new byte[0], VoltType.NULL_DECIMAL };

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
        project.addProcedures(PROCEDURES);
        project.addStmtProcedure(
                "PassObjectNull",
                "insert into ALLOW_NULLS values ( ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);",
                "NO_NULLS.PKEY: 0");

        boolean success;

        /*
         * // CONFIG #1: Local Site/Partitions running on IPC backend config =
         * new LocalSingleProcessServer("sqltypes-onesite.jar", 1,
         * BackendTarget.NATIVE_EE_IPC); success = config.compile(project);
         * assertTrue(success); builder.addServerConfig(config); // CONFIG #2:
         * HSQL config = new LocalSingleProcessServer("sqltypes-hsql.jar", 1,
         * BackendTarget.HSQLDB_BACKEND); success = config.compile(project);
         * assertTrue(success); builder.addServerConfig(config);
         */
        // JNI
        config = new LocalSingleProcessServer("sqltypes-onesite.jar", 1,
                BackendTarget.NATIVE_EE_JNI);
        success = config.compile(project);
        assertTrue(success);
        builder.addServerConfig(config);

        // CLUSTER?
        config = new LocalCluster("sqltypes-cluster.jar", 2, 2, 1,
                BackendTarget.NATIVE_EE_JNI);
        success = config.compile(project);
        assertTrue(success);
        builder.addServerConfig(config);

        return builder;
    }

}
