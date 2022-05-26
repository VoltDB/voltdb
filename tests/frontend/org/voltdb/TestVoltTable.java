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

import static org.junit.Assert.assertNotEquals;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import org.apache.commons.lang3.RandomStringUtils;
import org.json_voltpatches.JSONException;
import org.voltdb.TableHelper.RandomTable;
import org.voltdb.VoltTable.ColumnInfo;
import org.voltdb.test.utils.RandomTestRule;
import org.voltdb.types.GeographyPointValue;
import org.voltdb.types.GeographyValue;
import org.voltdb.types.TimestampType;
import org.voltdb.types.VoltDecimalHelper;
import org.voltdb.utils.CompressionService;
import org.voltdb.utils.VoltTableUtil;

import com.google_voltpatches.common.collect.ImmutableList;

import junit.framework.TestCase;

public class TestVoltTable extends TestCase {
    private VoltTable LONG_FIVE;
    private VoltTable t;
    private VoltTable t2;

    private static final GeographyValue GEOG_VALUE = GeographyValue.fromWKT("POLYGON((0 0, 0 1, -1 1, -1 0, 0 0))");
    private static final GeographyPointValue GEOG_PT_VALUE = GeographyPointValue.fromWKT("POINT(-122.0264 36.9719)");
    private final RandomTestRule m_random = new RandomTestRule();

    @Override
    public void setUp() {
        LONG_FIVE = new VoltTable(new VoltTable.ColumnInfo("Test",
                VoltType.BIGINT));
        LONG_FIVE.addRow(5L);
        t = new VoltTable();
        t2 = new VoltTable();
        m_random.setMethodName(getName());
    }

    VoltTable roundTrip(VoltTable t) {
        ByteBuffer buf = ByteBuffer.allocate(t.getSerializedSize());
        t.flattenToBuffer(buf);
        buf.flip();
        buf.getInt(); // ignore length prefix here
        return PrivateVoltTableFactory.createVoltTableFromBuffer(buf.slice(), false);
    }

    void addAllPrimitives(VoltTable table, Class<?>[] permittedTypes) {
        Object[] primitives = {
                null,
                (byte) 0,
                (short) 1,
                (short) 2,
                3,
                4L,
                5.01f,
                6.02,
                "string",
                new byte[] { 'b', 'y', 't', 'e', 's' },
                new TimestampType(99),
                new BigDecimal(7654321)
                        .setScale(VoltDecimalHelper.kDefaultScale),
                GEOG_VALUE,
                GEOG_PT_VALUE,
                new Object(), };

        for (Object o : primitives) {
            try {
                table.addRow(o);
                if (o != null && !contains(permittedTypes, o.getClass())) {
                    fail(o.getClass()
                            + " is not permitted but addRow succeeded");
                }
            } catch (VoltTypeException e) {
                if (contains(permittedTypes, o.getClass())) {
                    fail(o.getClass() + " is permitted by addRow failed");
                }
            }
        }
    }

    // VoltTable initially sizes itself to fit exactly, so 2 longs should be
    // sufficient to get
    // resizing. Use 16 to be on the safe side.
    private static final int LONGS_TO_RESIZE = 16;

    // return an VoltTable that needed to grow once.
    private VoltTable makeResizedTable() {
        VoltTable temp = new VoltTable(new ColumnInfo("Foo", VoltType.BIGINT));
        for (int i = 0; i < LONGS_TO_RESIZE; ++i) {
            temp.addRow((long) i);
        }
        return temp;
    }

    private static boolean contains(Class<?>[] types, Class<?> type) {
        for (Class<?> c : types) {
            if (type == c) {
                return true;
            }
        }
        return false;
    }

    private boolean comparisonHelper(Object lhs, Object rhs, VoltType vt) {
        switch (vt) {
        case TINYINT:
            Byte b1 = (Byte) lhs;
            Byte b2 = (Byte) rhs;
            return b1.byteValue() == b2.byteValue();
        case SMALLINT:
            Short s1 = (Short) lhs;
            Short s2 = (Short) rhs;
            return s1.shortValue() == s2.shortValue();
        case INTEGER:
            Integer i1 = (Integer) lhs;
            Integer i2 = (Integer) rhs;
            return i1.intValue() == i2.intValue();
        case BIGINT:
            Long l1 = (Long) lhs;
            Long l2 = (Long) rhs;
            return l1.longValue() == l2.longValue();
        case FLOAT:
            Double d1 = (Double) lhs;
            Double d2 = (Double) rhs;
            return (d1.compareTo(d2) == 0);
        case STRING:
            if (lhs == null && rhs == null) {
                return true;
            }
            if (lhs == VoltType.NULL_STRING_OR_VARBINARY && rhs == null) {
                return true;
            }
            return ((String) lhs).equals(rhs);
        case VARBINARY:
            if (lhs == null && rhs == null) {
                return true;
            }
            if (lhs == VoltType.NULL_STRING_OR_VARBINARY && rhs == null) {
                return true;
            }
            return Arrays.equals((byte[]) lhs, (byte[]) rhs);
        case TIMESTAMP:
            if (lhs == null && rhs == null) {
                return true;
            }
            if (lhs == VoltType.NULL_TIMESTAMP && rhs == null) {
                return true;
            }
            return ((TimestampType) lhs).equals(rhs);
        case DECIMAL:
            if (lhs == null && rhs == null) {
                return true;
            }
            if (lhs == VoltType.NULL_DECIMAL && rhs == null) {
                return true;
            }
            if (lhs == null || rhs == null) {
                return false;
            }
            return ((BigDecimal) lhs).equals(rhs);
        }

        return false;
    }

    public void testMakeFromScalar() {
        assertEquals(1, LONG_FIVE.getColumnCount());
        assertEquals(1, LONG_FIVE.getRowCount());
        assertEquals("Test", LONG_FIVE.getColumnName(0));
    }

    public void testAsScalarLong() {
        assertEquals(5L, LONG_FIVE.asScalarLong());
    }

    public void testAddColumnNullName() {
        try {
            t = new VoltTable(new ColumnInfo(null, VoltType.BIGINT));
            fail("expected exception");
        } catch (IllegalArgumentException e) {
        }
    }

    public void testLongSchemaOver1K() {
        // this test would crash before r531
        StringBuilder columnname = new StringBuilder();
        while (columnname.length() < 8192) {
            columnname.append("ryanlikestheyankees");
        }
        t = new VoltTable(
                new ColumnInfo(columnname.toString(), VoltType.BIGINT));
        assertTrue(t.getColumnName(0).compareTo(columnname.toString()) == 0);
    }

    public void testColumnIndexBounds() {
        try {
            LONG_FIVE.fetchRow(0).getLong(-1);
            fail("expected exception");
        } catch (IndexOutOfBoundsException e) {
        }

        try {
            LONG_FIVE.fetchRow(0).getLong(1);
            fail("expected exception");
        } catch (IndexOutOfBoundsException e) {
        }
    }

    public void testColumnTypeMismatch() {
        try {
            LONG_FIVE.fetchRow(0).getString(0);
            fail("expected exception");
        } catch (IllegalArgumentException e) {
        }
    }

    public void testColumnByName() {
        t = new VoltTable(new ColumnInfo("foo", VoltType.STRING),
                new ColumnInfo("twofoo", VoltType.INTEGER));
        t.addRow("bar", 5);

        assertEquals(0, t.getColumnIndex("foo"));
        assertEquals(1, t.getColumnIndex("twofoo"));
        assertEquals(t.getColumnName(0).equals("foo"), true);
        assertEquals(t.getColumnName(1).equals("twofoo"), true);
        assertEquals(t.getColumnName(1).compareTo("twofoo"), 0);
        System.out.println(t.toString());

        VoltTableRow r = t.fetchRow(0);
        assertEquals("bar", r.getString("foo"));

        try {
            t.getColumnIndex("bar");
            fail("expected exception");
        } catch (IllegalArgumentException e) {
        }

        try {
            r.getString("bar");
            fail("expected exception");
        } catch (IllegalArgumentException e) {
        }
    }

    public void testAddRow() {
        t = new VoltTable(new ColumnInfo("foo", VoltType.BIGINT));
        try {
            t.addRow(42L, 47L);
            fail("expected exception (1)");
        } catch (IllegalArgumentException e) {
        }
        try {
            Object[] objs = new Object[] { new Long(50), new Long(51) };
            t.addRow(objs);
            fail("expected exception (2)");
        } catch (IllegalArgumentException e) {
        }
    }

    public void testResizedTable() {
        // Create a table big enough to require resizing
        t = makeResizedTable();

        assertEquals(LONGS_TO_RESIZE, t.getRowCount());
        for (int i = 0; i < LONGS_TO_RESIZE; ++i) {
            assertEquals(i, t.fetchRow(i).getLong(0));
        }
    }

    /*
     * Use a heap buffer with an array offset to simulate a result set
     * from the EE
     */
    public void testCompressionWithArrayOffset() throws Exception {
        testResizedTable();
        ByteBuffer buf = t.getBuffer();
        byte bytes[] = new byte[buf.remaining() + 12];
        byte subBytes[] = new byte[buf.remaining()];
        buf.get(subBytes);
        System.arraycopy(subBytes, 0, bytes, 12, subBytes.length);

        buf = ByteBuffer.wrap(bytes);
        buf.position(12);
        buf = buf.slice();

        VoltTable vt = PrivateVoltTableFactory.createVoltTableFromBuffer(buf, true);

        ByteBuffer tempBuf = ByteBuffer.allocate(t.getSerializedSize());
        t.flattenToBuffer(tempBuf);
        byte uncompressedBytes[] = tempBuf.array().clone();

        /*
         * Test the sync method
         */
        byte decompressedBytes[] = CompressionService
                .decompressBytes(TableCompressor.getCompressedTableBytes(vt));
        vt = PrivateVoltTableFactory.createVoltTableFromBuffer(
                ByteBuffer.wrap(decompressedBytes), true);
        tempBuf = ByteBuffer.allocate(vt.getSerializedSize());
        vt.flattenToBuffer(tempBuf);
        byte bytesForComparison[] = tempBuf.array().clone();
        assertTrue(java.util.Arrays.equals(bytesForComparison,
                uncompressedBytes));

        /*
         * Now test async
         */
        vt = PrivateVoltTableFactory.createVoltTableFromBuffer(buf, true);
        decompressedBytes = CompressionService
                .decompressBytes(TableCompressor.getCompressedTableBytes(vt));
        vt = PrivateVoltTableFactory.createVoltTableFromBuffer(
                ByteBuffer.wrap(decompressedBytes), true);
        tempBuf = ByteBuffer.allocate(vt.getSerializedSize());
        vt.flattenToBuffer(tempBuf);
        bytesForComparison = tempBuf.array().clone();
        assertTrue(java.util.Arrays.equals(bytesForComparison,
                uncompressedBytes));
    }

    public void testCompression() throws Exception {
        testResizedTable();
        byte compressedBytes[] = TableCompressor.getCompressedTableBytes(t);

        ByteBuffer tempBuf = ByteBuffer.allocate(t.getSerializedSize());
        t.flattenToBuffer(tempBuf);
        byte uncompressedBytes[] = tempBuf.array().clone();

        assertTrue(uncompressedBytes.length > compressedBytes.length);

        compressedBytes = TableCompressor.getCompressedTableBytesAsync(t).get();
        assertTrue(uncompressedBytes.length > compressedBytes.length);

        byte decompressedBytes[] = CompressionService
                .decompressBytes(compressedBytes);
        VoltTable vt = PrivateVoltTableFactory.createVoltTableFromBuffer(
                ByteBuffer.wrap(decompressedBytes), true);

        tempBuf = ByteBuffer.allocate(vt.getSerializedSize());
        vt.flattenToBuffer(tempBuf);
        byte[] bytesForComparison = tempBuf.array().clone();
        assertTrue(java.util.Arrays.equals(bytesForComparison,
                uncompressedBytes));
    }

    public void testCompressionDirect() throws Exception {
        testResizedTable();
        ByteBuffer view = t.getBuffer();
        int position = view.position();
        view.position(0);
        ByteBuffer copy = ByteBuffer.allocateDirect(view.limit());
        copy.put(view);
        copy.position(position);
        VoltTable tDirect = PrivateVoltTableFactory.createVoltTableFromBuffer(
                copy, true);

        byte compressedBytes[] = TableCompressor.getCompressedTableBytes(tDirect);

        ByteBuffer tempBuf = ByteBuffer.allocate(t.getSerializedSize());
        t.flattenToBuffer(tempBuf);
        byte uncompressedBytes[] = tempBuf.array().clone();
        assertTrue(uncompressedBytes.length > compressedBytes.length);

        compressedBytes = TableCompressor.getCompressedTableBytesAsync(tDirect).get();
        assertTrue(uncompressedBytes.length > compressedBytes.length);

        byte decompressedBytes[] = CompressionService
                .decompressBytes(compressedBytes);
        VoltTable vt = PrivateVoltTableFactory.createVoltTableFromBuffer(
                ByteBuffer.wrap(decompressedBytes), true);

        tempBuf = ByteBuffer.allocate(vt.getSerializedSize());
        vt.flattenToBuffer(tempBuf);
        byte[] bytesForComparison = tempBuf.array().clone();
        assertTrue(java.util.Arrays.equals(bytesForComparison,
                uncompressedBytes));
    }

    @SuppressWarnings("deprecation")
    public void testEquals() {
        assertFalse(LONG_FIVE.equals(null));
        assertFalse(LONG_FIVE.equals(new Object()));

        // Different column name
        t = new VoltTable(new VoltTable.ColumnInfo("Test2", VoltType.BIGINT));
        t.addRow(5L);
        assertFalse(LONG_FIVE.equals(t));

        // Different number of columns
        t = new VoltTable(new ColumnInfo("Test", VoltType.BIGINT),
                new ColumnInfo("Test2", VoltType.BIGINT));
        assertFalse(LONG_FIVE.equals(t));
        t.addRow(5L, 10L);
        assertFalse(LONG_FIVE.equals(t));

        // These are the same table
        t = new VoltTable(new ColumnInfo("Test", VoltType.BIGINT));
        t.addRow(5L);
        assertEquals(LONG_FIVE, t);

        // Test two tables with strings
        t = new VoltTable(new VoltTable.ColumnInfo("Foo", VoltType.STRING));
        t.addRow("Bar");
        VoltTable t2 = new VoltTable(new VoltTable.ColumnInfo("Foo",
                VoltType.STRING));
        t2.addRow("Baz");
        assertFalse(t.equals(t2));
        t2 = new VoltTable(new VoltTable.ColumnInfo("Foo", VoltType.STRING));
        t2.addRow("Bar");
        assertEquals(t, t2);
    }

    @SuppressWarnings("deprecation")
    public void testEqualsDeserialized() {
        t = makeResizedTable();
        t2 = roundTrip(t);
        boolean equal = t.equals(t2);
        assertTrue(equal);
    }

    public void testStrings() {
        t = new VoltTable(new ColumnInfo("", VoltType.STRING));
        addAllPrimitives(t, new Class[] { String.class, byte[].class });
        t.addRow("");
        assertEquals("string", t.fetchRow(1).getString(0));

        t2 = roundTrip(t);
        assertEquals("", t2.getColumnName(0));
        assertEquals(4, t2.getRowCount());
        VoltTableRow r = t2.fetchRow(0);
        assertNull(r.getString(0));
        assertTrue(r.wasNull());

        assertEquals("string", t2.fetchRow(1).getString(0));
        assertEquals("bytes", t2.fetchRow(2).getString(0));
        assertEquals("", t2.fetchRow(3).getString(0));

        t2.clearRowData();
        assertTrue(t2.getRowCount() == 0);
    }

    public void testStringsAsBytes() {
        t = new VoltTable(new ColumnInfo("", VoltType.STRING));
        t.addRow(new byte[0]);
        final byte[] FOO = new byte[] { 'f', 'o', 'o' };
        t.addRow(FOO);

        t2 = roundTrip(t);
        assertEquals(2, t2.getRowCount());
        assertEquals("", t2.fetchRow(0).getString(0));
        assertEquals(0, t2.fetchRow(0).getStringAsBytes(0).length);
        assertTrue(Arrays.equals(FOO, t2.fetchRow(1).getStringAsBytes(0)));
        assertEquals("foo", t2.fetchRow(1).getString(0));

        t2.clearRowData();
        assertTrue(t2.getRowCount() == 0);
    }

    public void testVarbinary() {
        t = new VoltTable(new ColumnInfo("", VoltType.VARBINARY));
        final byte[] empty = new byte[0];
        t.addRow(empty);
        final byte[] FOO = new byte[] { 'f', 'o', 'o' };
        t.addRow(FOO);

        t2 = roundTrip(t);
        assertEquals(2, t2.getRowCount());
        assertTrue(Arrays.equals(empty, t2.fetchRow(0).getVarbinary(0)));
        assertEquals(0, t2.fetchRow(0).getVarbinary(0).length);
        assertTrue(Arrays.equals(FOO, t2.fetchRow(1).getVarbinary(0)));

        t2.clearRowData();
        assertTrue(t2.getRowCount() == 0);
    }

    public void testIntegers() {
        t = new VoltTable(new ColumnInfo("foo", VoltType.BIGINT));
        addAllPrimitives(t, new Class[] { Long.class, Integer.class, Short.class,
                Byte.class, Double.class, Float.class });

        t2 = roundTrip(t);
        assertEquals(8, t2.getRowCount());
        assertEquals(0, t2.fetchRow(1).getLong(0));

        VoltTableRow r = t2.fetchRow(0);
        assertEquals(VoltType.NULL_BIGINT, r.getLong(0));
        assertTrue(r.wasNull());

        t2.clearRowData();
        assertTrue(t2.getRowCount() == 0);
    }

    public void testExactTypes() {
        VoltTable basecase = new VoltTable(new ColumnInfo("foo",
                VoltType.DECIMAL));
        basecase.addRow(new BigDecimal(7654321)
                .setScale(VoltDecimalHelper.kDefaultScale));
        VoltTableRow basecaserow = basecase.fetchRow(0);
        BigDecimal bd = basecaserow.getDecimalAsBigDecimal(0);
        assertEquals(bd,
                new BigDecimal(7654321)
                        .setScale(VoltDecimalHelper.kDefaultScale));

        t = new VoltTable(new ColumnInfo("foo", VoltType.DECIMAL));
        addAllPrimitives(t, new Class[] { BigDecimal.class });

        t2 = roundTrip(t);
        assertEquals(2, t2.getRowCount());

        // row 0 contains NULL
        VoltTableRow r = t2.fetchRow(0);
        r.getDecimalAsBigDecimal(0);
        assertTrue(r.wasNull());

        // row 1 contains a known value
        r = t2.fetchRow(1);
        assertTrue(new BigDecimal(7654321).setScale(
                VoltDecimalHelper.kDefaultScale).equals(
                r.getDecimalAsBigDecimal(0)));

        t2.clearRowData();
        assertTrue(t2.getRowCount() == 0);
    }

    public void testFloats() {
        t = new VoltTable(new ColumnInfo("foo", VoltType.FLOAT));
        addAllPrimitives(t, new Class[] { Long.class, Integer.class, Short.class,
                Byte.class, Double.class, Float.class });

        t2 = roundTrip(t);
        assertEquals(8, t2.getRowCount());
        VoltTableRow r = t2.fetchRow(0);
        assertEquals(VoltType.NULL_FLOAT, r.getDouble(0));
        assertTrue(r.wasNull());

        assertEquals(0.0, t2.fetchRow(1).getDouble(0), .000001);
        assertEquals(1.0, t2.fetchRow(2).getDouble(0), .000001);
        assertEquals(2.0, t2.fetchRow(3).getDouble(0), .000001);
        assertEquals(3.0, t2.fetchRow(4).getDouble(0), .000001);
        assertEquals(4.0, t2.fetchRow(5).getDouble(0), .000001);
        assertEquals(5.01, t2.fetchRow(6).getDouble(0), .000001);
        assertEquals(6.02, t2.fetchRow(7).getDouble(0), .000001);

        t2.clearRowData();
        assertTrue(t2.getRowCount() == 0);
    }

    public void testGeographies() {
        VoltTable vt = new VoltTable(new ColumnInfo("gg", VoltType.GEOGRAPHY));
        addAllPrimitives(vt, new Class[] {GeographyValue.class});

        VoltTable vtCopy = roundTrip(vt);

        assertEquals(2, vt.getRowCount());
        assertEquals(2, vtCopy.getRowCount());

        assertTrue(vt.advanceRow());
        assertTrue(vtCopy.advanceRow());

        assertNull(vt.getGeographyValue(0));
        assertNull(vtCopy.getGeographyValue(0));
        assertNull(vt.get(0, VoltType.GEOGRAPHY));
        assertTrue(vt.wasNull());
        assertTrue(vtCopy.wasNull());

        assertTrue(vt.advanceRow());
        assertTrue(vtCopy.advanceRow());

        String wkt = GEOG_VALUE.toString();
        assertEquals(wkt, vt.getGeographyValue(0).toString());
        assertEquals(wkt, vtCopy.getGeographyValue(0).toString());
        assertEquals(wkt, vt.getGeographyValue("gg").toString());
        assertEquals(wkt, vt.get(0, VoltType.GEOGRAPHY).toString());

        byte[] raw = vt.getRaw(0);
        // Raw bytes does not include the length prefix
        assertEquals(GEOG_VALUE.getLengthInBytes(), raw.length - 4);

        byte[] rawCopy = vtCopy.getRaw(0);
        assertEquals(raw.length, rawCopy.length);
        for (int i = 0; i < rawCopy.length; ++i) {
            assertEquals("raw geography not equal to copy at byte " + i,
                    raw[i], rawCopy[i]);
        }

        assertFalse(vt.advanceRow());
        assertFalse(vtCopy.advanceRow());
    }

    public void testGeographyPoints() {
        VoltTable vt = new VoltTable(new ColumnInfo("pt", VoltType.GEOGRAPHY_POINT));
        addAllPrimitives(vt, new Class[] {GeographyPointValue.class});

        VoltTable vtCopy = roundTrip(vt);

        assertEquals(2, vt.getRowCount());
        assertEquals(2, vtCopy.getRowCount());

        assertTrue(vt.advanceRow());
        assertTrue(vtCopy.advanceRow());

        assertNull(vt.getGeographyPointValue(0));
        assertNull(vtCopy.getGeographyPointValue(0));
        assertNull(vt.get(0, VoltType.GEOGRAPHY_POINT));
        assertTrue(vt.wasNull());

        assertTrue(vt.advanceRow());
        assertTrue(vtCopy.advanceRow());

        String wkt = GEOG_PT_VALUE.toString();
        assertEquals(wkt, vt.getGeographyPointValue(0).toString());
        assertEquals(wkt, vtCopy.getGeographyPointValue(0).toString());
        assertEquals(wkt, vt.getGeographyPointValue("pt").toString());
        assertEquals(wkt, vt.get(0, VoltType.GEOGRAPHY_POINT).toString());

        byte[] raw = vt.getRaw(0);
        assertEquals(GeographyPointValue.getLengthInBytes(), raw.length);

        byte[] rawCopy = vtCopy.getRaw(0);
        assertEquals(raw.length, rawCopy.length);
        for (int i = 0; i < rawCopy.length; ++i) {
            assertEquals("raw geography not equal to copy at byte " + i,
                    raw[i], rawCopy[i]);
        }

        assertFalse(vt.advanceRow());
        assertFalse(vtCopy.advanceRow());
    }

    // At least check that NULL_VALUEs of one type get interpreted as NULL
    // if we attempt to put them into a column of a different type
    public void testNulls() {
        VoltType[] types = { VoltType.TINYINT, VoltType.SMALLINT,
                VoltType.INTEGER, VoltType.BIGINT, VoltType.FLOAT,
                VoltType.DECIMAL, VoltType.TIMESTAMP, VoltType.STRING };

        for (int i = 0; i < types.length; ++i) {
            for (int j = 0; j < types.length; ++j) {
                VoltTable table = new VoltTable(new ColumnInfo("test_table",
                        types[i]));
                table.addRow(types[j].getNullValue());
                VoltTableRow row = table.fetchRow(0);
                row.get(0, types[i]);
                assertTrue("Value wasn't null", row.wasNull());
            }
        }
    }

    public void testTruncatingCasts() {
        VoltType[] test_types = { VoltType.TINYINT, VoltType.SMALLINT,
                VoltType.INTEGER };

        Object[][] test_vals = {
                { (long) Byte.MAX_VALUE, ((long) Byte.MAX_VALUE) + 1,
                        ((long) Byte.MIN_VALUE) - 1 },
                { (long) Short.MAX_VALUE, ((long) Short.MAX_VALUE) + 1,
                        ((long) Short.MIN_VALUE) - 1 },
                { (long) Integer.MAX_VALUE, ((long) Integer.MAX_VALUE) + 1,
                        ((long) Integer.MIN_VALUE) - 1 } };

        for (int i = 0; i < test_types.length; ++i) {
            t = new VoltTable(new ColumnInfo("test_table", test_types[i]));
            t.addRow(test_vals[i][0]);
            boolean caught = false;
            try {
                t.addRow(test_vals[i][1]);
            } catch (VoltTypeException e) {
                caught = true;
            }
            assertTrue("Failed on: " + test_types[i].toString(), caught);

            caught = false;
            try {
                t.addRow(test_vals[i][2]);
            } catch (VoltTypeException e) {
                caught = true;
            }
            assertTrue("Failed on: " + test_types[i].toString(), caught);
        }
    }

    public void testTimestamps() {
        t = new VoltTable(new ColumnInfo("foo", VoltType.TIMESTAMP));
        addAllPrimitives(t, new Class[] { Byte.class, Short.class, Integer.class,
                Long.class, Float.class, Double.class, TimestampType.class });

        t2 = roundTrip(t);
        assertEquals(9, t2.getRowCount());
        assertEquals(0L, t2.fetchRow(1).getTimestampAsTimestamp(0).getTime());
        assertEquals(0L, t2.fetchRow(1).getTimestampAsLong(0));
        assertEquals(1L, t2.fetchRow(2).getTimestampAsTimestamp(0).getTime());
        assertEquals(1L, t2.fetchRow(2).getTimestampAsLong(0));

        VoltTableRow r = t2.fetchRow(0);
        assertNull(r.getTimestampAsTimestamp(0));
        assertTrue(r.wasNull());

        r = t2.fetchRow(0);
        assertEquals(VoltType.NULL_BIGINT, r.getTimestampAsLong(0));
        assertTrue(r.wasNull());

        t2.clearRowData();
        assertTrue(t2.getRowCount() == 0);
    }

    public void testAddRowExceptionSafe() {
        t = new VoltTable(new ColumnInfo("foo", VoltType.BIGINT),
                new ColumnInfo("bar", VoltType.STRING), new ColumnInfo("baz",
                        VoltType.BIGINT));

        t.addRow(0L, "a", 1L);
        try {
            t.addRow(42L, "", "bad");
            fail("expected exception");
        } catch (VoltTypeException e) {
        }
        t.addRow(2L, "b", 3L);

        // the contents of the table should not be corrupted
        assertEquals(2, t.getRowCount());
        assertEquals(0L, t.fetchRow(0).getLong(0));
        assertEquals("a", t.fetchRow(0).getString(1));
        assertEquals(1L, t.fetchRow(0).getLong(2));
        assertEquals(2L, t.fetchRow(1).getLong(0));
        assertEquals("b", t.fetchRow(1).getString(1));
        assertEquals(3L, t.fetchRow(1).getLong(2));
    }

    public void testClone() {
        VoltTable item_data_template = new VoltTable(new ColumnInfo("i_name",
                VoltType.STRING),
                new ColumnInfo("s_quantity", VoltType.BIGINT), new ColumnInfo(
                        "brand_generic", VoltType.STRING), new ColumnInfo(
                        "i_price", VoltType.FLOAT), new ColumnInfo("ol_amount",
                        VoltType.FLOAT));
        VoltTable item_data = item_data_template.clone(1024);
        assertEquals(5, item_data.getColumnCount());
        assertEquals("i_name", item_data.getColumnName(0));
        assertEquals("s_quantity", item_data.getColumnName(1));
        assertEquals("brand_generic", item_data.getColumnName(2));
        assertEquals("i_price", item_data.getColumnName(3));
        assertEquals("ol_amount", item_data.getColumnName(4));
        assertEquals(VoltType.STRING, item_data.getColumnType(0));
        assertEquals(VoltType.BIGINT, item_data.getColumnType(1));
        assertEquals(VoltType.STRING, item_data.getColumnType(2));
        assertEquals(VoltType.FLOAT, item_data.getColumnType(3));
        assertEquals(VoltType.FLOAT, item_data.getColumnType(4));
        item_data.addRow("asdfsdgfsdg", 123L, "a", 45.0d, 656.2d);
    }

    public void testGetSchema() {
        VoltTable item_data_template = new VoltTable(new ColumnInfo("i_name",
                VoltType.STRING),
                new ColumnInfo("s_quantity", VoltType.BIGINT), new ColumnInfo(
                        "brand_generic", VoltType.STRING), new ColumnInfo(
                        "i_price", VoltType.FLOAT), new ColumnInfo("ol_amount",
                        VoltType.FLOAT));
        ColumnInfo[] cols = item_data_template.getTableSchema();
        assertEquals(5, cols.length);
        assertEquals("i_name", cols[0].name);
        assertEquals("s_quantity", cols[1].name);
        assertEquals("brand_generic", cols[2].name);
        assertEquals("i_price", cols[3].name);
        assertEquals("ol_amount", cols[4].name);
        assertEquals(VoltType.STRING, cols[0].type);
        assertEquals(VoltType.BIGINT, cols[1].type);
        assertEquals(VoltType.STRING, cols[2].type);
        assertEquals(VoltType.FLOAT, cols[3].type);
        assertEquals(VoltType.FLOAT, cols[4].type);
    }

    public void testRowIterator() {

        // Test iteration of empty table
        VoltTable empty = new VoltTable();

        // make sure it craps out
        try {
            empty.getLong(1);
            fail();
        } catch (Exception e) {
        }
        ;

        assertEquals(-1, empty.getActiveRowIndex());
        assertFalse(empty.advanceRow());

        // Make a table with info to iterate

        t = new VoltTable(new ColumnInfo("foo", VoltType.BIGINT),
                new ColumnInfo("bar", VoltType.STRING));
        for (int i = 0; i < 10; i++) {
            t.addRow(i, String.valueOf(i));
        }

        int rowcount = 0;
        VoltTableRow copy = null;
        while (t.advanceRow()) {
            assertEquals(rowcount, t.getLong(0));
            assertTrue(String.valueOf(rowcount).equals(t.getString(1)));
            if (rowcount == 4) {
                copy = t.cloneRow();
            }
            rowcount++;
        }
        assertEquals(10, rowcount);

        rowcount = 5;
        while (copy.advanceRow()) {
            assertEquals(rowcount, copy.getLong(0));
            assertTrue(String.valueOf(rowcount).equals(copy.getString(1)));
            rowcount++;
        }
        assertEquals(10, rowcount);
    }

    public void testStupidAdvanceRowUse() {
        VoltTable table = new VoltTable(new ColumnInfo("foo", VoltType.BIGINT));
        table.addRow(5);
        // try to access value without calling advanceRow
        try {
            table.getLong(0);
            fail();
        } catch (RuntimeException e) {
            assertTrue(e.getMessage().startsWith("VoltTableRow.advanceRow"));
        }
    }

    public void testAdvanceToRow() {
        // Test iteration of empty table
        VoltTable empty = new VoltTable();

        // make sure it craps out on an empty table. Don't reset row position in
        // between.
        assertFalse(empty.advanceToRow(1));
        assertFalse(empty.advanceToRow(0));
        assertFalse(empty.advanceToRow(-1));

        // make sure it craps out on an empty table. Reset row position in
        // between.
        empty.resetRowPosition();
        assertFalse(empty.advanceToRow(1));
        empty.resetRowPosition();
        assertFalse(empty.advanceToRow(0));
        empty.resetRowPosition();
        assertFalse(empty.advanceToRow(-1));
        empty.resetRowPosition();

        // Make a table with rows to navigate
        t = new VoltTable(new ColumnInfo("foo", VoltType.BIGINT),
                new ColumnInfo("bar", VoltType.STRING));
        for (int i = 0; i < 10; i++) {
            t.addRow(i, String.valueOf(i));
        }

        // Advance to each row one at a time
        t.resetRowPosition();
        for (int i = 0; i < 10; i++) {
            t.advanceToRow(i);
            assertTrue(String.valueOf(i).equals(t.getString(1)));
        }

        // Now jump around randomly.
        t.resetRowPosition();
        t.advanceRow();
        assertTrue(t.advanceToRow(3));
        assertTrue("3".equals(t.getString(1)));
        assertTrue(t.advanceToRow(9));
        assertTrue("9".equals(t.getString(1)));

        // Jump off the end of the list
        assertFalse(t.advanceToRow(99));

        t.resetRowPosition();
        t.advanceRow();
        assertFalse(t.advanceToRow(-99));

        // End on a happy note
        t.resetRowPosition();
        t.advanceRow();
        assertTrue(t.advanceToRow(5));
        assertTrue("5".equals(t.getString(1)));
    }

    public void testRowGet() {

        byte b1 = (byte) 1;
        short s1 = (short) 2;
        int i1 = 3;
        long l1 = Long.MIN_VALUE + 1;
        double f1 = 3.5;
        String S1 = "A";
        TimestampType d1 = new TimestampType(0);
        BigDecimal B1 = new BigDecimal(7654321)
                .setScale(VoltDecimalHelper.kDefaultScale);

        // create a table with one column per supported type with NULLS on the
        // left to right diagonal.
        // tinyint is intentionally first AND last to test that wasNull is
        // correctly cleared by the
        // the next-to-last instance and re-initialized by tinyint.

        Object content[] = { b1, S1, i1, l1, f1, s1, d1, B1, b1 };

        Object nulls[] = { VoltType.NULL_TINYINT,
                VoltType.NULL_STRING_OR_VARBINARY, VoltType.NULL_INTEGER,
                VoltType.NULL_BIGINT, VoltType.NULL_FLOAT,
                VoltType.NULL_SMALLINT, VoltType.NULL_TIMESTAMP,
                VoltType.NULL_DECIMAL, VoltType.NULL_TINYINT };

        VoltType types[] = { VoltType.TINYINT, VoltType.STRING,
                VoltType.INTEGER, VoltType.BIGINT, VoltType.FLOAT,
                VoltType.SMALLINT, VoltType.TIMESTAMP, VoltType.DECIMAL,
                VoltType.TINYINT };

        VoltTable tt = new VoltTable(new ColumnInfo("tinyint", types[0]),
                new ColumnInfo("string", types[1]), new ColumnInfo("integer",
                        types[2]), new ColumnInfo("bigint", types[3]),
                new ColumnInfo("float", types[4]), new ColumnInfo("smallint",
                        types[5]), new ColumnInfo("timestamp", types[6]),
                new ColumnInfo("decimal", types[7]), new ColumnInfo("tinyint",
                        types[0]));

        for (int i = 0; i < content.length; ++i) {
            Object[] vals = new Object[content.length];
            ;
            for (int k = 0; k < content.length; k++) {
                if (i == k) {
                    vals[k] = nulls[k];
                } else {
                    vals[k] = content[k];
                }
            }
            System.out.println("Adding row: " + i);
            tt.addRow(vals);
        }

        // now iterate all the fields in the table and verify that row.get(idx,
        // type)
        // works and that the wasNull state is correctly set and cleared.
        System.out.println(tt);
        int rowcounter = 0;
        while (tt.advanceRow()) {
            for (int k = 0; k < content.length; k++) {
                System.out.println("verifying row:" + rowcounter + " col:" + k);

                if (rowcounter == k) {
                    boolean result = comparisonHelper(nulls[k],
                            tt.get(k, types[k]), types[k]);
                    assertTrue(result);
                    assertTrue(tt.wasNull());
                } else {
                    Object got = tt.get(k, types[k]);
                    System.out.println("Type: " + types[k]);
                    System.out.println("Expecting: " + content[k]);
                    System.out.println("Got: " + got);
                    assertTrue(comparisonHelper(content[k], got, types[k]));
                    assertFalse(tt.wasNull());
                }
            }
            rowcounter++;
        }
        assertEquals(rowcounter, content.length);
    }

    private void assertExpectedOutput(String expected, String actual) {
        if (!actual.equals(expected))
        {
            System.out.println("Received formatted output:");
            System.out.println(actual);
            System.out.println("Expected output:");
            System.out.println(expected);
        }
        assertEquals(expected, actual);
    }

    public void testFormattedString() throws JSONException, IOException {
        // Set the default timezone since we're using a timestamp type.  Eliminate test flakeyness.
        VoltDB.setDefaultTimezone();

        VoltTable table = new VoltTable(new ColumnInfo("tinyint",   VoltType.TINYINT),
                                        new ColumnInfo("smallint",  VoltType.SMALLINT),
                                        new ColumnInfo("integer",   VoltType.INTEGER),
                                        new ColumnInfo("bigint",    VoltType.BIGINT),
                                        new ColumnInfo("float",     VoltType.FLOAT),
                                        new ColumnInfo("string",    VoltType.STRING),
                                        new ColumnInfo("varbinary", VoltType.VARBINARY),
                                        new ColumnInfo("timestamp", VoltType.TIMESTAMP),
                                        new ColumnInfo("decimal",   VoltType.DECIMAL));

        // add a row of nulls the hard way
        table.addRow(VoltType.NULL_TINYINT,
                     VoltType.NULL_SMALLINT,
                     VoltType.NULL_INTEGER,
                     VoltType.NULL_BIGINT,
                     VoltType.NULL_FLOAT,
                     VoltType.NULL_STRING_OR_VARBINARY,
                     VoltType.NULL_STRING_OR_VARBINARY,
                     VoltType.NULL_TIMESTAMP,
                     VoltType.NULL_DECIMAL);

        // add a row of nulls the easy way
        table.addRow(null, null, null, null, null, null, null, null, null);

        // add a row of actual data.  Hard-code the timestamp so that we can compare deterministically
        table.addRow(123, 12345, 1234567, 12345678901L, 1.234567, "aabbcc",
                     new byte[] { 10, 26, 10 },
                     new TimestampType(99),
                     new BigDecimal("123.45"));
        String formattedOutput = table.toFormattedString();
        String expectedOutput =
"tinyint  smallint  integer  bigint       float     string  varbinary  timestamp                   decimal          \n" +
"-------- --------- -------- ------------ --------- ------- ---------- --------------------------- -----------------\n" +
"    NULL      NULL     NULL         NULL      NULL NULL    NULL       NULL                                     NULL\n" +
"    NULL      NULL     NULL         NULL      NULL NULL    NULL       NULL                                     NULL\n" +
"     123     12345  1234567  12345678901  1.234567 aabbcc  0A1A0A     1970-01-01 00:00:00.000099   123.450000000000\n";

        assertExpectedOutput(expectedOutput, formattedOutput);

        // fetch formatted output without column names
        formattedOutput = table.toFormattedString(false);
        expectedOutput =
"    NULL      NULL     NULL         NULL      NULL NULL    NULL       NULL                                     NULL\n" +
"    NULL      NULL     NULL         NULL      NULL NULL    NULL       NULL                                     NULL\n" +
"     123     12345  1234567  12345678901  1.234567 aabbcc  0A1A0A     1970-01-01 00:00:00.000099   123.450000000000\n";

        assertExpectedOutput(expectedOutput, formattedOutput);

        table = new VoltTable(new ColumnInfo("bigint",          VoltType.BIGINT),
                              new ColumnInfo("geography",       VoltType.GEOGRAPHY),
                              new ColumnInfo("geography_point", VoltType.GEOGRAPHY_POINT),
                              new ColumnInfo("timestamp", VoltType.TIMESTAMP));
        table.addRow(VoltType.NULL_BIGINT, VoltType.NULL_GEOGRAPHY, VoltType.NULL_POINT, VoltType.NULL_TIMESTAMP);
        table.addRow(null, null, null, null);
        table.addRow(123456789,
                     new GeographyValue("POLYGON (( 1.1  9.9, " +
                                                  "-9.1  9.9, " +
                                                  "-9.1 -9.9, " +
                                                  " 9.1 -9.9, " +
                                                  " 1.1  9.9))"),
                     new GeographyPointValue(-179.0, -89.9),
                     new TimestampType(-1));
        formattedOutput = table.toFormattedString();
        expectedOutput =
"bigint     geography                                                    geography_point       timestamp                  \n" +
"---------- ------------------------------------------------------------ --------------------- ---------------------------\n" +
"      NULL NULL                                                         NULL                  NULL                       \n" +
"      NULL NULL                                                         NULL                  NULL                       \n" +
" 123456789 POLYGON ((1.1 9.9, -9.1 9.9, -9.1 -9.9, 9.1 -9.9, 1.1 9.9))  POINT (-179.0 -89.9)  1969-12-31 23:59:59.999999 \n";

        assertExpectedOutput(expectedOutput, formattedOutput);

        // row with a polygon that max's output column width for geopgraphy column
        table.addRow(1234567890,
                     new GeographyValue("POLYGON (( 179.1  89.9, " +
                                                 "-179.1  89.9, " +
                                                 "-179.1 -89.9, " +
                                                 " 179.1 -89.9, " +
                                                 " 179.1  89.9))"),
                     new GeographyPointValue(-179.0, -89.9),
                     new TimestampType(0));
        formattedOutput = table.toFormattedString();
        expectedOutput =
"bigint      geography                                                                   geography_point       timestamp                  \n" +
"----------- --------------------------------------------------------------------------- --------------------- ---------------------------\n" +
"       NULL NULL                                                                        NULL                  NULL                       \n" +
"       NULL NULL                                                                        NULL                  NULL                       \n" +
"  123456789 POLYGON ((1.1 9.9, -9.1 9.9, -9.1 -9.9, 9.1 -9.9, 1.1 9.9))                 POINT (-179.0 -89.9)  1969-12-31 23:59:59.999999 \n" +
" 1234567890 POLYGON ((179.1 89.9, -179.1 89.9, -179.1 -89.9, 179.1 -89.9, 179.1 89.9))  POINT (-179.0 -89.9)  1970-01-01 00:00:00.000000 \n";

        assertExpectedOutput(expectedOutput, formattedOutput);

        // row with a polygon that goes beyond max aligned display limit for polygon. This will result in
        // other columns following to appear further away from their original column
        table.addRow(12345678901L,
                new GeographyValue("POLYGON (( 179.12  89.9, " +
                                             "-179.12  89.9, " +
                                             "-179.1  -89.9, " +
                                             " 179.1  -89.9, " +
                                             "   0     0," +
                                             "  1.123  1.11," +
                                             " 179.12  89.9))"),
                new GeographyPointValue(0, 0),
                new TimestampType(99));
        formattedOutput = table.toFormattedString();
        expectedOutput =
"bigint       geography                                                                   geography_point       timestamp                  \n" +
"------------ --------------------------------------------------------------------------- --------------------- ---------------------------\n" +
"        NULL NULL                                                                        NULL                  NULL                       \n" +
"        NULL NULL                                                                        NULL                  NULL                       \n" +
"   123456789 POLYGON ((1.1 9.9, -9.1 9.9, -9.1 -9.9, 9.1 -9.9, 1.1 9.9))                 POINT (-179.0 -89.9)  1969-12-31 23:59:59.999999 \n" +
"  1234567890 POLYGON ((179.1 89.9, -179.1 89.9, -179.1 -89.9, 179.1 -89.9, 179.1 89.9))  POINT (-179.0 -89.9)  1970-01-01 00:00:00.000000 \n" +
" 12345678901 POLYGON ((179.12 89.9, -179.12 89.9, -179.1 -89.9, 179.1 -89.9, 0.0 0.0, 1.123 1.11, 179.12 89.9)) POINT (0.0 0.0)       1970-01-01 00:00:00.000099 \n";

        assertExpectedOutput(expectedOutput, formattedOutput);

        // test the final one without column names
        formattedOutput = table.toFormattedString(false);
        expectedOutput =
"        NULL NULL                                                                        NULL                  NULL                       \n" +
"        NULL NULL                                                                        NULL                  NULL                       \n" +
"   123456789 POLYGON ((1.1 9.9, -9.1 9.9, -9.1 -9.9, 9.1 -9.9, 1.1 9.9))                 POINT (-179.0 -89.9)  1969-12-31 23:59:59.999999 \n" +
"  1234567890 POLYGON ((179.1 89.9, -179.1 89.9, -179.1 -89.9, 179.1 -89.9, 179.1 89.9))  POINT (-179.0 -89.9)  1970-01-01 00:00:00.000000 \n" +
" 12345678901 POLYGON ((179.12 89.9, -179.12 89.9, -179.1 -89.9, 179.1 -89.9, 0.0 0.0, 1.123 1.11, 179.12 89.9)) POINT (0.0 0.0)       1970-01-01 00:00:00.000099 \n";

        assertExpectedOutput(expectedOutput, formattedOutput);
    }

    @SuppressWarnings("deprecation")
    public void testJSONRoundTrip() throws JSONException, IOException {
        VoltTable t1 = new VoltTable(
                new ColumnInfo("tinyint", VoltType.TINYINT), new ColumnInfo(
                        "smallint", VoltType.SMALLINT), new ColumnInfo(
                        "integer", VoltType.INTEGER), new ColumnInfo("bigint",
                        VoltType.BIGINT), new ColumnInfo("float",
                        VoltType.FLOAT), new ColumnInfo("string",
                        VoltType.STRING), new ColumnInfo("varbinary",
                        VoltType.VARBINARY), new ColumnInfo("timestamp",
                        VoltType.TIMESTAMP), new ColumnInfo("decimal",
                        VoltType.DECIMAL));

        // add a row of nulls the hard way
        t1.addRow(VoltType.NULL_TINYINT, VoltType.NULL_SMALLINT,
                VoltType.NULL_INTEGER, VoltType.NULL_BIGINT,
                VoltType.NULL_FLOAT, VoltType.NULL_STRING_OR_VARBINARY,
                VoltType.NULL_STRING_OR_VARBINARY, VoltType.NULL_TIMESTAMP,
                VoltType.NULL_DECIMAL);

        // add a row of nulls the easy way
        t1.addRow(null, null, null, null, null, null, null, null, null);

        // add a row with NaN
        t1.addRow(null, null, null, null, Double.NaN, null, null, null, null);

        // add a row with +inf
        t1.addRow(null, null, null, null, Double.POSITIVE_INFINITY, null, null, null, null);

        // add a row with all defaults
        t1.addRow(VoltType.NULL_TINYINT, VoltType.NULL_SMALLINT,
                VoltType.NULL_INTEGER, VoltType.NULL_BIGINT,
                VoltType.NULL_FLOAT, VoltType.NULL_STRING_OR_VARBINARY,
                VoltType.NULL_STRING_OR_VARBINARY, VoltType.NULL_TIMESTAMP,
                VoltType.NULL_DECIMAL);

        // add a row of actual data
        t1.addRow(123, 12345, 1234567, 12345678901L, 1.234567, "aabbcc",
                new byte[] { 10, 26, 10 },
                new TimestampType(System.currentTimeMillis()), new BigDecimal(
                        "123.45"));

        String json = t1.toJSONString();

        VoltTable t2 = VoltTable.fromJSONString(json);

        assertTrue(t1.equals(t2));
    }

    /**
     * Java won't let you pass >255 args to a method. Verify it's possible to
     * make a big table using vararg methods and arrays.
     */
    public void test300Columns() {
        VoltTable.ColumnInfo[] schema = new VoltTable.ColumnInfo[300];
        for (int i = 0; i < schema.length; ++i) {
            schema[i] = new VoltTable.ColumnInfo(String.valueOf(i),
                    VoltType.BIGINT);
        }
        VoltTable t = new VoltTable(schema);
        assertNotNull(t);
        assertEquals(300, t.getColumnCount());

        Object[] row = new Object[schema.length];
        for (int i = 0; i < schema.length; ++i) {
            row[i] = new Long(i);
        }

        t.addRow(row);
        assertEquals(1, t.getRowCount());
        assertEquals(schema.length - 1, t.fetchRow(0)
                .getLong(schema.length - 1));
    }

    @SuppressWarnings("deprecation")
    public void testBlittingAddRowSuccess() throws IOException {
        int rowCount = 100;
        int numberOfPartitions = 20;
        Random r = new Random(0);

        for (int j = 0; j < 10; j++) {
            TableHelper th = new TableHelper();
            VoltTable t = th.getTotallyRandomTable("foo", true).table;
            th.randomFill(t, rowCount, 500);

            t.resetRowPosition();
            // create a table for each partition
            VoltTable[] partitioned_tables = new VoltTable[numberOfPartitions];
            for (int i = 0; i < partitioned_tables.length; i++) {
                partitioned_tables[i] =
                        t.clone((int) ((PrivateVoltTableFactory.getUnderlyingBufferSize(t) /
                                numberOfPartitions) * 1.5));
            }

            // split the input table into per-partition units
            while (t.advanceRow()) {
                int partition = 0;
                partition = r.nextInt(numberOfPartitions);
                // this adds the active row of loadedTable
                //partitioned_tables[partition].addRowIdenticalSchema(t);
                partitioned_tables[partition].add(t);
            }

            // merge the tables
            VoltTable t2 = t.clone(100);
            for (VoltTable pt : partitioned_tables) {
                pt.resetRowPosition();
                while (pt.advanceRow()) {
                    t2.add(pt);
                }
            }

            assertTrue(t2.equals(t));
        }
    }

    public void testSchemaChangeAddRow() {
        int rowCount = 100;
        Random r = new Random(0);

        for (int j = 0; j < 100; j++) {
            TableHelper th = new TableHelper();
            VoltTable t1 = th.getTotallyRandomTable("foo", true).table;
            th.randomFill(t1, rowCount, 500);

            // get the schema of the first table
            VoltTable.ColumnInfo[] columns = new VoltTable.ColumnInfo[t1.getColumnCount()];
            for (int i = 0; i < columns.length; i++) {
                columns[i] = new VoltTable.ColumnInfo(t1.getColumnName(i), t1.getColumnType(i));
            }

            // make a few random column changes that are compatible (numbers only)
            for (int i = 0; i < 5; i++) {
                int randCol = r.nextInt(columns.length);
                VoltType ct1 = t1.getColumnType(randCol);
                if ((ct1 == VoltType.TINYINT) || (ct1 == VoltType.INTEGER) || (ct1 == VoltType.SMALLINT)) {
                    columns[randCol] = new VoltTable.ColumnInfo(columns[randCol].name, VoltType.BIGINT);
                }
            }

            // make a second empty table with the incompatible schema
            VoltTable t2 = new VoltTable(columns);

            t1.resetRowPosition();
            while (t1.advanceRow()) {
                t2.add(t1);
            }

            // compare formatted strings (imperfect)
            String t1s = t1.toFormattedString();
            String t2s = t2.toFormattedString();
            assertTrue(t1s.equals(t2s));
        }
    }

    public void testBadAddRow() {
        int rowCount = 10;
        Random r = new Random(0);

        for (int j = 0; j < 75; j++) {
            TableHelper th = new TableHelper();
            VoltTable t1 = th.getTotallyRandomTable("foo", true).table;
            th.randomFill(t1, rowCount, 500);

            // get the schema of the first table
            VoltTable.ColumnInfo[] columns = new VoltTable.ColumnInfo[t1.getColumnCount()];
            for (int i = 0; i < columns.length; i++) {
                columns[i] = new VoltTable.ColumnInfo(t1.getColumnName(i), t1.getColumnType(i));
            }

            // make a random column incompatible
            int randCol = r.nextInt(columns.length);
            VoltType ct1 = t1.getColumnType(randCol);
            if ((ct1 == VoltType.VARBINARY) || (ct1 == VoltType.STRING)) {
                columns[randCol] = new VoltTable.ColumnInfo(columns[randCol].name, VoltType.TINYINT);
            }
            else {
                columns[randCol] = new VoltTable.ColumnInfo(columns[randCol].name, VoltType.VARBINARY);
            }

            // make a second empty table with the incompatible schema
            VoltTable t2 = new VoltTable(columns);

            t1.resetRowPosition();
            while (t1.advanceRow()) {
                try {
                    // add a random row successfully every other row
                    th.randomFill(t2, 1, 10);
                    // - except small chance of overflow too due to large string
                    if (r.nextInt(20) == 0) {
                        th.randomFill(t2, 1, 2048 * 1024);
                    }
                    // add a bad row from t1
                    t2.add(t1);

                    // only check if the twiddled column was non-null
                    Object valueOfInterest = t1.get(randCol, t1.getColumnType(randCol));
                    if ((valueOfInterest != null) && (t1.wasNull() == false)) {
                        fail();
                    }
                }
                catch (Exception e) {
                    // two cases where we might fail and it's ok
                    boolean tooBig = e instanceof VoltOverflowException;
                    boolean nonNull = t1.get(randCol, t1.getColumnType(randCol)) != null;
                    assertTrue(tooBig || nonNull);
                }
            }

            // this should bomb if the data is invalid because it scans every value
            t2.toJSONString();
        }
    }

    public void testFetchRowPerformance() {
        final int ROW_COUNT = 100000;

        TableHelper th = new TableHelper();
        RandomTable rt = th.getTotallyRandomTable("FOO");
        th.randomFill(rt.table, ROW_COUNT, 128);
        assertTrue(rt.table.getRowCount() == ROW_COUNT);
        VoltTable t = rt.table;

        System.out.println("Iterating with fetchRow()...");
        long t0 = System.nanoTime();

        t.resetRowPosition();
        for (int i=0; i < ROW_COUNT; i++) {
            t.fetchRow(i);
        }

        System.out.println("Iterating with advanceRow()...");

        long t1 = System.nanoTime();

        t.resetRowPosition();
        while (t.advanceRow()) {
            t.advanceRow();
        }

        long t2 = System.nanoTime();

        long fetchRowTime = t1-t0;
        System.out.println("Took "+ fetchRowTime + " nanos to call fetchRow() for " + ROW_COUNT + " rows");

        long advanceRowTime = t2-t1;
        System.out.println("Took "+ advanceRowTime + " nanos to call advanceRow() for " + ROW_COUNT + " rows");

        // this is a super loose bound just to check that the time isn't n^2
        assertTrue(fetchRowTime < (advanceRowTime * 20));
    }

    public void testFetchRowAccuracy() {
        final int ROW_COUNT = 10000;

        TableHelper th = new TableHelper();
        RandomTable rt = th.getTotallyRandomTable("FOO");
        th.randomFill(rt.table, ROW_COUNT, 128);
        assertTrue(rt.table.getRowCount() == ROW_COUNT);
        VoltTable t = rt.table;

        Object[] results = new Object[ROW_COUNT];

        // cache all the answers and check that incrementing fetchrow and advancerow agree
        t.resetRowPosition();
        int i = 0;
        while (t.advanceRow()) {
            VoltTableRow r = t.fetchRow(i);

            Object lhs = r.get(0, r.getColumnType(0));
            Object rhs = t.get(0, t.getColumnType(0));
            if (lhs == null) {
                assert(rhs == null);
            }
            else {
                assertTrue(lhs.equals(rhs));
            }

            results[i++] = lhs;
        }

        // match cached answers to random fetchrow requests
        Random rand = new Random();
        for (i = 0; i < 1000; i++) {
            int randIndex = rand.nextInt(ROW_COUNT);
            VoltTableRow r = t.fetchRow(randIndex);

            Object lhs = r.get(0, r.getColumnType(0));
            Object rhs = results[randIndex];
            if (lhs == null) {
                assert(rhs == null);
            }
            else {
                assertTrue(lhs.equals(rhs));
            }
        }

        // verify nothing gets hosed if you work out of bounds
        for (i = 0; i < 1000; i++) {
            // add some range on either end so we fetch rows out of bounds
            int randIndex = rand.nextInt(ROW_COUNT * 3 / 2) - (ROW_COUNT / 4);

            try {
                VoltTableRow r = t.fetchRow(randIndex);
                assertTrue(randIndex >= 0);
                assertTrue(randIndex < ROW_COUNT);

                Object lhs = r.get(0, r.getColumnType(0));
                Object rhs = results[randIndex];
                if (lhs == null) {
                    assert(rhs == null);
                }
                else {
                    assertTrue(lhs.equals(rhs));
                }
            }
            catch (Exception e) {
                assertTrue((randIndex < 0) || (randIndex >= ROW_COUNT));
            }
        }
    }

    public void testTableJava8Streams() {
        Random rand = new Random(0);

        for (int i = 0; i < 100; i++) {
            int rowCount = rand.nextInt(2000);
            TableHelper th = new TableHelper();
            VoltTable t1 = th.getTotallyRandomTable("foo", true).table;
            th.randomFill(t1, rowCount, 100);

            assertEquals(rowCount, VoltTableUtil.stream(t1).count());

            VoltTableUtil.stream(t1)
                .map(r -> r.get(0, r.getColumnType(0)))
                .map(x -> String.valueOf(x))
                .map(s -> "Hello:" + s)
                .forEach(s -> System.out.println(s));
        }
    }

    public void testAddTable() {
        t = new VoltTable(new ColumnInfo[] { new ColumnInfo("C1", VoltType.BIGINT),
                new ColumnInfo("C2", VoltType.STRING), new ColumnInfo("C3", VoltType.INTEGER) });

        // Test different type for one column
        try {
            t.addTable(new VoltTable(new ColumnInfo[] { new ColumnInfo("C1", VoltType.BIGINT),
                    new ColumnInfo("C2", VoltType.STRING), new ColumnInfo("C3", VoltType.FLOAT) }));
            fail("Should have thrown IllegalArgumentException");
        } catch (IllegalArgumentException e) {}

        // Test different column name for one column
        try {
            t.addTable(new VoltTable(new ColumnInfo[] { new ColumnInfo("C1", VoltType.BIGINT),
                    new ColumnInfo("C2", VoltType.STRING), new ColumnInfo("C4", VoltType.INTEGER) }));
            fail("Should have thrown IllegalArgumentException");
        } catch (IllegalArgumentException e) {}

        t2 = new VoltTable(t.getTableSchema());

        t.addTable(t2);
        assertEquals(0, t.getRowCount());

        for (int i = 0; i < 1000; ++i) {
            t2.addRow(i, RandomStringUtils.random(847), i);
        }

        for (int i = 0; i < 5; ++i) {
            t.addTable(t2);
            assertEquals(t2.getRowCount() * (i + 1), t.getRowCount());

            t.resetRowPosition();
            for (int j = 0; j < i; ++j) {
                t2.resetRowPosition();
                while (t2.advanceRow()) {
                    assertTrue(t.advanceRow());
                    for (int k = 0; k < t2.getColumnCount(); ++k) {
                        VoltType type = t2.getColumnType(k);
                        assertEquals(t2.get(k, type), t.get(k, type));
                    }
                }
            }
        }
    }

    public void testAddTables() {
        t = new VoltTable(new ColumnInfo[] { new ColumnInfo("C1", VoltType.BIGINT),
                new ColumnInfo("C2", VoltType.STRING), new ColumnInfo("C3", VoltType.INTEGER) });

        t2 = new VoltTable(t.getTableSchema());

        // Test different type for one column
        try {
            t.addTables(
                    ImmutableList.of(t2,
                            new VoltTable(new ColumnInfo[] { new ColumnInfo("C1", VoltType.BIGINT),
                                    new ColumnInfo("C2", VoltType.STRING), new ColumnInfo("C3", VoltType.FLOAT) }),
                    t2, t2));
            fail("Should have thrown IllegalArgumentException");
        } catch (IllegalArgumentException e) {}

        // Test different column name for one column
        try {
            t.addTables(
                    ImmutableList.of(t2, t2,
                            new VoltTable(new ColumnInfo[] { new ColumnInfo("C1", VoltType.BIGINT),
                                    new ColumnInfo("C2", VoltType.STRING), new ColumnInfo("C4", VoltType.INTEGER) }),
                    t2));
            fail("Should have thrown IllegalArgumentException");
        } catch (IllegalArgumentException e) {}

        t.addTables(ImmutableList.of(t2, t2, t2, t2));
        assertEquals(0, t.getRowCount());

        for (int i = 0; i < 1000; ++i) {
            t2.addRow(i, RandomStringUtils.random(847), i);
        }

        t.addTables(ImmutableList.of(t2, t2, t2, t2, t2));
        assertEquals(t2.getRowCount() * 5, t.getRowCount());
        t.resetRowPosition();
        for (int i = 0; i < 5; ++i) {
            t2.resetRowPosition();
            while (t2.advanceRow()) {
                assertTrue(t.advanceRow());
                for (int j = 0; j < t2.getColumnCount(); ++j) {
                    VoltType type = t2.getColumnType(j);
                    assertEquals(t2.get(j, type), t.get(j, type));
                }
            }
        }
    }

    /*
     * Test that checksum and same contents methods generate the expected results for different tables
     */
    public void testTableChecksumAndSameContents() {

        List<VoltType> types = Arrays.asList(VoltType.TINYINT, VoltType.SMALLINT, VoltType.INTEGER, VoltType.BIGINT,
                VoltType.FLOAT, VoltType.STRING, VoltType.VARBINARY);
        t = new VoltTable(types.stream().map(t -> new ColumnInfo(t.getName(), t)).toArray(ColumnInfo[]::new));
        List<Object[]> rows = new ArrayList<>(1000);
        for (int i =0;i<1000;++i) {
            Object[] row = m_random
                    .nextValues(types);
            t.addRow(row);
            rows.add(row);
        }

        // Hashing the same volt table should be equal
        assertEquals(t.getTableCheckSum(true), t.getTableCheckSum(true));
        assertEquals(t.getTableCheckSum(false), t.getTableCheckSum(false));
        assertTrue(t.hasSameContents(t, true));
        assertTrue(t.hasSameContents(t, false));

        // With header should not equal without header
        assertNotEquals(t.getTableCheckSum(true), t.getTableCheckSum(false));

        t2 = new VoltTable(t.getTableSchema());
        t2.addTable(t);

        // Hashing two identical volt tables should be equal
        assertEquals(t.getTableCheckSum(true), t2.getTableCheckSum(true));
        assertEquals(t.getTableCheckSum(false), t2.getTableCheckSum(false));
        assertTrue(t.hasSameContents(t2, true));
        assertTrue(t.hasSameContents(t2, false));

        t2 = new VoltTable(t.getTableSchema());
        rows.forEach(t2::addRow);

        // Hashing two identical volt tables should be equal
        assertEquals(t.getTableCheckSum(true), t2.getTableCheckSum(true));
        assertEquals(t.getTableCheckSum(false), t2.getTableCheckSum(false));
        assertTrue(t.hasSameContents(t2, true));
        assertTrue(t.hasSameContents(t2, false));

        Collections.shuffle(rows);

        t2 = new VoltTable(t.getTableSchema());
        rows.forEach(t2::addRow);

        // Hashing two tables with different row order should be equal
        assertEquals(t.getTableCheckSum(true), t2.getTableCheckSum(true));
        assertEquals(t.getTableCheckSum(false), t2.getTableCheckSum(false));
        assertTrue(t.hasSameContents(t2, true));
        assertFalse(t.hasSameContents(t2, false));

        // Add hashes of multiple tables together and make sure it is the same the whole table when headers are not
        // included
        VoltTable[] tables = new VoltTable[m_random.nextInt(10) + 2];
        for (int i = 0; i < tables.length; ++i) {
            tables[i] = new VoltTable(t.getTableSchema());
        }
        Iterator<Object[]> iter = rows.iterator();

        while (iter.hasNext()) {
            for (int i = 0; i < tables.length && iter.hasNext(); ++i) {
                tables[i].addRow(iter.next());
            }
        }

        assertNotEquals(t.getTableCheckSum(true), getTablesChecksum(tables, true));
        assertEquals(t.getTableCheckSum(false), getTablesChecksum(tables, false));
        for (int i = 0; i < tables.length; ++i) {
            assertFalse(t.hasSameContents(tables[i], true));
            assertFalse(t.hasSameContents(tables[i], false));
        }
    }

    private long getTablesChecksum(VoltTable[] tables, boolean includeHeader) {
        long result = 0;
        for (VoltTable table : tables) {
            result += table.getTableCheckSum(includeHeader);
        }
        return result;
    }
}
