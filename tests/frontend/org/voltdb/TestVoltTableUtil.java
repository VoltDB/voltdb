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

import au.com.bytecode.opencsv_voltpatches.CSVReader;
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
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
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
    public void testCSVReader() {
        try {
            // To make sure we have microseconds we get millisecond from current timestamp
            // and add remainder to ensure we have micros in timestamp.
            TimestampType ts = new TimestampType((System.currentTimeMillis() * 1000) + System.currentTimeMillis() % 1000);

            ColumnInfo[] columns = new ColumnInfo[]{
                new ColumnInfo("TINYINT", VoltType.TINYINT),
                new ColumnInfo("SMALLINT", VoltType.SMALLINT),
                new ColumnInfo("INTEGER", VoltType.INTEGER),
                new ColumnInfo("BIGINT", VoltType.BIGINT),
                new ColumnInfo("FLOAT", VoltType.FLOAT),
                new ColumnInfo("DECIMAL", VoltType.DECIMAL),
                new ColumnInfo("VARCHAR", VoltType.STRING),
                new ColumnInfo("VARBINARY", VoltType.VARBINARY),
                new ColumnInfo("TIMESTAMP", VoltType.TIMESTAMP)
            };
            ArrayList<VoltType> columnTypes = new ArrayList<VoltType>();
            for (ColumnInfo ci : columns) {
                columnTypes.add(ci.type);
            }
            CSVReader reader = mock(CSVReader.class);
            String line[] = {String.valueOf(1),
                String.valueOf(10),
                String.valueOf(100000),
                String.valueOf(100000000),
                String.valueOf(3.712),
                (new BigDecimal("718877.11888")).toString(),
                "test-varchar",
                "test-varbinary",
                ts.toString(),};
            stub(reader.readNext()).toReturn(line);

            ArrayList<String> colNames = new ArrayList<String>();
            colNames.add("TINYINT");
            colNames.add("SMALLINT");
            colNames.add("INTEGER");
            colNames.add("BIGINT");
            colNames.add("FLOAT");
            colNames.add("DECIMAL");
            colNames.add("VARCHAR");
            colNames.add("VARBINARY");
            colNames.add("TIMESTAMP");
            VoltTable tab = VoltTableUtil.toVoltTableFromCSV(reader, colNames, columnTypes);
            assertEquals(tab.getRowCount(), 1);
            assert (tab.advanceRow());
            assertEquals(tab.getColumnCount(), 9);
            assertEquals(tab.getLong(0), 1);
            assertEquals(tab.getLong(1), 10);
            assertEquals(tab.getLong(2), 100000);
            assertEquals(tab.getLong(3), 100000000);
            assertEquals(tab.getDouble(4), 3.712, 0);
            BigDecimal bd1 = tab.getDecimalAsBigDecimal(5);

            assertTrue((bd1.compareTo(new BigDecimal("718877.11888"))) == 0);
            assertEquals(tab.getString(6), "test-varchar");
            assertTrue(Arrays.equals(tab.getVarbinary(7), ("test-varbinary").getBytes()));
            assertEquals(tab.getTimestampAsTimestamp(8), ts);

        } catch (IOException ex) {
            Logger.getLogger(TestVoltTableUtil.class.getName()).log(Level.SEVERE, null, ex);
        }
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

}
