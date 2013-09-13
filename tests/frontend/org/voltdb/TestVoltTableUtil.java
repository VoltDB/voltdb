/* This file is part of VoltDB.
 * Copyright (C) 2008-2013 VoltDB Inc.
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

import static org.junit.Assert.*;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.util.ArrayList;

import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.voltdb.VoltTable.ColumnInfo;
import org.voltdb.types.TimestampType;
import org.voltdb.utils.VoltTableUtil;

import au.com.bytecode.opencsv_voltpatches.CSVWriter;
import java.math.BigDecimal;
import java.text.ParseException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.mockito.Mockito;

public class TestVoltTableUtil extends Mockito {
    @Test
    public void testCSVNullConversion() throws IOException {
        CSVWriter writer = mock(CSVWriter.class);
        ColumnInfo[] columns = new ColumnInfo[] {new ColumnInfo("", VoltType.BIGINT),
                                                 new ColumnInfo("", VoltType.FLOAT),
                                                 new ColumnInfo("", VoltType.DECIMAL),
                                                 new ColumnInfo("", VoltType.STRING),
                                                 new ColumnInfo("", VoltType.TIMESTAMP),
                                                 new ColumnInfo("", VoltType.VARBINARY)};
        ArrayList<VoltType> columnTypes = new ArrayList<VoltType>();
        VoltTable vt = new VoltTable(columns);
        vt.addRow(VoltType.NULL_BIGINT,
                  VoltType.NULL_FLOAT,
                  VoltType.NULL_DECIMAL,
                  VoltType.NULL_STRING_OR_VARBINARY,
                  VoltType.NULL_TIMESTAMP,
                  VoltType.NULL_STRING_OR_VARBINARY);

        for (ColumnInfo ci : columns) {
            columnTypes.add(ci.type);
        }

        VoltTableUtil.toCSVWriter(writer, vt, columnTypes);

        ArgumentCaptor<String[]> captor = ArgumentCaptor.forClass(String[].class);
        verify(writer).writeNext(captor.capture());
        String[] values = captor.getValue();
        assertEquals(6, values.length);
        for (String v : values) {
            assertEquals(VoltTable.CSV_NULL, v);
        }
    }

    /**
     * Round-trip the time, see if it's still the same.
     * @throws IOException
     */
    @Test
    public void testCSVTimestamp() throws IOException {
        CSVWriter writer = mock(CSVWriter.class);
        ColumnInfo[] columns = new ColumnInfo[] {new ColumnInfo("", VoltType.TIMESTAMP)};
        ArrayList<VoltType> columnTypes = new ArrayList<VoltType>();
        // To make sure we have microseconds we get millisecond from current timestamp
        // and add remainder to ensure we have micros in timestamp.
        TimestampType ts = new TimestampType((System.currentTimeMillis() * 1000) + System.currentTimeMillis() % 1000);
        VoltTable vt = new VoltTable(columns);
        vt.addRow(ts);

        for (ColumnInfo ci : columns) {
            columnTypes.add(ci.type);
        }

        VoltTableUtil.toCSVWriter(writer, vt, columnTypes);

        ArgumentCaptor<String[]> captor = ArgumentCaptor.forClass(String[].class);
        verify(writer).writeNext(captor.capture());
        String[] values = captor.getValue();
        assertEquals(1, values.length);
        TimestampType newTs = new TimestampType(values[0]);
        assertEquals(ts, newTs);
    }

    @Test
    public void testUnionTables()
    {
        VoltTable.ColumnInfo[] columns =
                new VoltTable.ColumnInfo[] {new VoltTable.ColumnInfo("ID", VoltType.BIGINT)};
        VoltTable table1 = new VoltTable(columns);
        table1.setStatusCode((byte) -1);
        table1.addRow(1);
        VoltTable table2 = new VoltTable(columns);
        table2.setStatusCode((byte) -1);
        table2.addRow(2);

        VoltTable result = VoltTableUtil.unionTables(Arrays.asList(null, table1,
                null, table2));
        assertNotNull(result);
        assertEquals(-1, result.getStatusCode());

        Set<Long> numbers = new HashSet<Long>();
        result.resetRowPosition();
        while (result.advanceRow()) {
            long i = result.getLong(0);
            numbers.add(i);
        }

        assertEquals(2, numbers.size());
        assertEquals(numbers.contains(1l), true);
        assertEquals(numbers.contains(2l), true);
    }

    @Test
    public void testaddRowToVoltTableFromLine() {
        ColumnInfo[] columns = new ColumnInfo[]{
            new ColumnInfo("", VoltType.TINYINT),
            new ColumnInfo("", VoltType.SMALLINT),
            new ColumnInfo("", VoltType.INTEGER),
            new ColumnInfo("", VoltType.BIGINT),
            new ColumnInfo("", VoltType.DECIMAL),
            new ColumnInfo("", VoltType.FLOAT),
            new ColumnInfo("", VoltType.STRING),
            new ColumnInfo("", VoltType.VARBINARY),
            new ColumnInfo("", VoltType.TIMESTAMP)
        };
        Map<Integer, VoltType> columnTypes = new HashMap<Integer, VoltType>();
        columnTypes.put(0, VoltType.TINYINT);
        columnTypes.put(1, VoltType.SMALLINT);
        columnTypes.put(2, VoltType.INTEGER);
        columnTypes.put(3, VoltType.BIGINT);
        columnTypes.put(4, VoltType.DECIMAL);
        columnTypes.put(5, VoltType.FLOAT);
        columnTypes.put(6, VoltType.STRING);
        columnTypes.put(7, VoltType.VARBINARY);
        columnTypes.put(8, VoltType.TIMESTAMP);

        VoltTable vt = new VoltTable(columns);
        String fields[] = {"1", "11", "1,110", "1212121212110",
            "1.00", "12091212.33", "This is a String", "BytesArray", "2010-07-01 12:30:21.0678"};
        try {
            VoltTableUtil.addRowToVoltTableFromLine(vt, fields, columnTypes);
            assertEquals(1, vt.getRowCount());
            vt.advanceRow();
            long l = vt.getLong(0);
            assertEquals(l, 1);
            l = vt.getLong(1);
            assertEquals(l, 11);
            l = vt.getLong(2);
            assertEquals(l, 1110);
            l = vt.getLong(3);
            assertEquals(l, 1212121212110L);
            BigDecimal bd = vt.getDecimalAsBigDecimal(4);
            assertEquals(bd.doubleValue(), 1.0, 0);
            double d = vt.getDouble(5);
            assertEquals(d, 12091212.33, 0);
            String s = vt.getString(6);
            assertEquals(s, "This is a String");

            byte ba[] = vt.getVarbinary(7);
            assertEquals(new String(ba), "BytesArray");

            TimestampType ts = new TimestampType(vt.getTimestampAsLong(8));
            assertEquals(ts.toString(), "2010-07-01 12:30:21.067800");

            VoltTableUtil.addRowToVoltTableFromLine(vt, fields, columnTypes);
            assertEquals(2, vt.getRowCount());
        } catch (ParseException ex) {
            fail(ex.toString());
        } catch (IOException ex) {
            fail(ex.toString());
        }
    }
}
