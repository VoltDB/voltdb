/* This file is part of VoltDB.
 * Copyright (C) 2008-2019 VoltDB Inc.
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

package org.voltdb.exportclient;


import java.io.IOException;
import java.io.StringWriter;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.TimeZone;

import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.common.Constants;
import org.voltdb.export.AdvertisedDataSource;
import org.voltdb.export.AdvertisedDataSource.ExportFormat;
import org.voltdb.exportclient.ExportDecoderBase.ExportRowData;
import org.voltdb.messaging.FastDeserializer;
import org.voltdb.types.GeographyPointValue;
import org.voltdb.types.GeographyValue;
import org.voltdb.types.TimestampType;
import org.voltdb.types.VoltDecimalHelper;

import au.com.bytecode.opencsv_voltpatches.CSVWriter;
import junit.framework.TestCase;

public class TestExportDecoderBaseLegacy extends TestCase
{
    class StubExportDecoder extends ExportDecoderBase
    {
        public StubExportDecoder(AdvertisedDataSource source)
        {
            super(source);
        }

        @Override
        public boolean processRow(int rowSize, byte[] rowData)
        {
            return false;
        }

        @Override
        public void sourceNoLongerAdvertised(AdvertisedDataSource source) {
            // TODO Auto-generated method stub

        }
    }

    static final SimpleDateFormat SIMPLE_DATE_FORMAT;
    static {
        // Explicitly format dates for the East Coast so these tests passes when
        // run in other time zones.
        SIMPLE_DATE_FORMAT = new SimpleDateFormat(Constants.ODBC_DATE_FORMAT_STRING);
        SIMPLE_DATE_FORMAT.setTimeZone(TimeZone.getTimeZone("America/New_York"));
    }

    static final String[] COLUMN_NAMES = {"tid", "ts", "sq", "pid", "site", "op",
        "tinyint", "smallint", "integer", "bigint", "float", "timestamp", "string", "decimal",
        "geog_point", "geog"};

    static final VoltType[] COLUMN_TYPES
            = {VoltType.BIGINT, VoltType.BIGINT, VoltType.BIGINT, VoltType.BIGINT, VoltType.BIGINT, VoltType.BIGINT,
                VoltType.TINYINT, VoltType.SMALLINT, VoltType.INTEGER,
                VoltType.BIGINT, VoltType.FLOAT, VoltType.TIMESTAMP,VoltType.STRING, VoltType.DECIMAL,
                VoltType.GEOGRAPHY_POINT, VoltType.GEOGRAPHY};

    static final Integer COLUMN_LENGTHS[] = {
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 64, 0, 0, 2048
    };

    static final GeographyPointValue GEOG_POINT = GeographyPointValue.fromWKT("point(-122 37)");
    static final GeographyValue GEOG = GeographyValue.fromWKT("polygon((0 0, 1 1, 0 1, 0 0))");

    static VoltTable vtable = new VoltTable(
            new VoltTable.ColumnInfo("VOLT_TRANSACTION_ID", VoltType.BIGINT),
            new VoltTable.ColumnInfo("VOLT_EXPORT_TIMESTAMP", VoltType.BIGINT),
            new VoltTable.ColumnInfo("VOLT_EXPORT_SEQUENCE_NUMBER", VoltType.BIGINT),
            new VoltTable.ColumnInfo("VOLT_PARTITION_ID", VoltType.BIGINT),
            new VoltTable.ColumnInfo("VOLT_TRANSACTION_ID", VoltType.BIGINT),
            new VoltTable.ColumnInfo("VOLT_SITE_ID", VoltType.BIGINT),
            new VoltTable.ColumnInfo("tinyint", VoltType.TINYINT),
            new VoltTable.ColumnInfo("smallint", VoltType.SMALLINT),
            new VoltTable.ColumnInfo("integer", VoltType.INTEGER),
            new VoltTable.ColumnInfo("bigint", VoltType.BIGINT),
            new VoltTable.ColumnInfo("float", VoltType.FLOAT),
            new VoltTable.ColumnInfo("timestamp", VoltType.TIMESTAMP),
            new VoltTable.ColumnInfo("string", VoltType.STRING),
            new VoltTable.ColumnInfo("decimal", VoltType.DECIMAL),
            new VoltTable.ColumnInfo("geog_point", VoltType.GEOGRAPHY_POINT),
            new VoltTable.ColumnInfo("geog", VoltType.GEOGRAPHY)
    );

    static AdvertisedDataSource constructTestSource() {
        return constructTestSource(0, "smallint");
    }

    static AdvertisedDataSource constructTestSource(int partition, String partitionColumn)    {
        ArrayList<String> col_names = new ArrayList<String>();
        ArrayList<VoltType> col_types = new ArrayList<VoltType>();
        for (int i = 0; i < COLUMN_TYPES.length; i++)
        {
            col_names.add(COLUMN_NAMES[i]);
            col_types.add(COLUMN_TYPES[i]);
        }
        //clear the table
        vtable.clearRowData();
        AdvertisedDataSource source = new AdvertisedDataSource(partition, "yankeelover", partitionColumn, 0, 32,
                col_names, col_types, Arrays.asList(COLUMN_LENGTHS), ExportFormat.SEVENDOTX);
        return source;
    }

    public void testNullFlags() throws IOException
    {
        StubExportDecoder dut =
            new StubExportDecoder(constructTestSource());
        for (int i = 0; i < COLUMN_TYPES.length; i++)
        {
            byte[] flag = new byte[2];
            flag[0] = (byte) (1 << 8 - i - 1);
            flag[1] = (byte) (1 << 16 - i - 1);
            FastDeserializer fds = new FastDeserializer(flag, ByteOrder.LITTLE_ENDIAN);
            boolean[] nulls = dut.extractNullFlags(fds);
            for (int j = 0; j < COLUMN_TYPES.length; j++)
            {
                if (j == i)
                {
                    assertTrue(nulls[j]);
                }
                else
                {
                    assertFalse(nulls[j]);
                }
            }
        }
    }

    public void testExportWriter() throws IOException {
        StubExportDecoder dut
                = new StubExportDecoder(constructTestSource());

        long l = System.currentTimeMillis();
        vtable.addRow(l, l, l, 0, l, l, (byte) 1, (short) 2, 3, 4, 5.5, 6, "xx", new BigDecimal(88), GEOG_POINT, GEOG);
        vtable.advanceRow();
        byte[] rowBytes = ExportEncoder.encodeRow(vtable, "mytable", 7, 1L);
        ByteBuffer bb = ByteBuffer.wrap(rowBytes);
        bb.order(ByteOrder.LITTLE_ENDIAN);
        int schemaSize = bb.getInt();
        ExportRow schemaRow = ExportRow.decodeBufferSchema(bb, schemaSize, 1, 0);
        int size = bb.getInt(); // row size
        byte[] rawRow = new byte[size];
        bb.get(rawRow);
        ExportRowData rowdata = dut.decodeRow(rawRow);
        Object[] rd = rowdata.values;
        assertEquals(rd[0], l);
        assertEquals(rd[1], l);
        assertEquals(rd[2], l);
        assertEquals(rd[3], 0L); //partition id
        assertEquals(rd[4], l);
        assertEquals(rd[5], l);

        //By default partitionValue will be based on partition column which is 2
        assertEquals(rowdata.partitionId, 0);
        assertEquals(rowdata.partitionValue, (short) 2);

        //Verify data
        assertEquals(rd[6], (byte) 1);
        assertEquals(rd[7], (short) 2);
        assertEquals(rd[8], 3);
        assertEquals(rd[9], 4L);
        assertEquals(rd[10], 5.5);
        assertEquals(rd[11], new TimestampType(6));
        assertEquals(rd[12], "xx");
        BigDecimal bd = (BigDecimal) rd[13];
        assertEquals(bd.compareTo(new BigDecimal(88)), 0);
        assertEquals(rd[14].toString(), GEOG_POINT.toString());
        assertEquals(rd[15].toString(), GEOG.toString());

        //Now Test write with skipinternal true
        StringWriter stringer = new StringWriter();
        String out1 = "\"1\",\"2\",\"3\",\"4\",\"5.5\",\"1969-12-31 19:00:00.000\",\"xx\",\"88.000000000000\","
                + "\"" + GEOG_POINT.toWKT() + "\",\"" + GEOG.toWKT() + "\"";
        CSVWriter csv = new CSVWriter(stringer);
        dut.writeRow(rd, csv, true, ExportDecoderBase.BinaryEncoding.BASE64, SIMPLE_DATE_FORMAT);
        csv.flush();
        assertEquals(stringer.getBuffer().toString().trim(), out1);
        System.out.println(stringer.getBuffer().toString().trim());

        //Now Test write with skipinternal false
        stringer = new StringWriter();
        String out2 = "\"" + l + "\"," + "\"" + l + "\"," + "\"" + l + "\"," + "\"0\"," + "\"" + l + "\"," + "\"" + l + "\","
                + "\"1\",\"2\",\"3\",\"4\",\"5.5\",\"1969-12-31 19:00:00.000\",\"xx\",\"88.000000000000\","
                + "\"" + GEOG_POINT.toWKT() + "\",\"" + GEOG.toWKT() + "\"";
        csv = new CSVWriter(stringer);
        dut.writeRow(rd, csv, false, ExportDecoderBase.BinaryEncoding.BASE64, SIMPLE_DATE_FORMAT);
        csv.flush();
        assertEquals(stringer.getBuffer().toString().trim(), out2);
        System.out.println(stringer.getBuffer().toString().trim());
    }

    public void testExportDecoderPartitioning() throws IOException {
        AdvertisedDataSource source = constructTestSource();
        StubExportDecoder dut
                = new StubExportDecoder(source);

        String pnames[] = {"tinyint", "smallint", "integer", "bigint", "float", "timestamp", "string", "decimal",
                "geog_point", "geog"};
        Object evalues[] = {(byte) 1, (short) 2, 3, (long) 4, 5.5,
            new TimestampType(6L), "xx", VoltDecimalHelper.setDefaultScale(new BigDecimal(88)),
            GEOG_POINT, GEOG};
        for (int i = 0; i < 8; i++) {
            vtable.clearRowData();
            long l = System.currentTimeMillis();
            vtable.addRow(l, l, l, 0, l, l, (byte) 1, (short) 2, 3, 4, 5.5, 6, "xx", new BigDecimal(88), GEOG_POINT, GEOG);
            vtable.advanceRow();
            //Check for bad values.
            dut.setPartitionColumnName("unknown");
            byte[] rowBytes = ExportEncoder.encodeRow(vtable, "mytable", -1, 1L);
            ByteBuffer bb = ByteBuffer.wrap(rowBytes);
            bb.order(ByteOrder.LITTLE_ENDIAN);
            int schemaSize = bb.getInt();
            ExportRow.decodeBufferSchema(bb, schemaSize, 1, 0);
            int size = bb.getInt(); // row size
            byte[] rawRow = new byte[size];
            bb.get(rawRow);
            ExportRowData rowdata = dut.decodeRow(rawRow);
            Object[] rd = rowdata.values;
            assertEquals(rd[0], l);
            assertEquals(rd[1], l);
            assertEquals(rd[2], l);
            assertEquals(rd[3], 0L); //partition id
            assertEquals(rd[4], l);
            assertEquals(rd[5], l);

            //With bad partition column name partitionValue should be partition id
            assertEquals(rowdata.partitionId, 0);
            assertEquals(rowdata.partitionValue, 0);

            //After this I should get correct col set for partition value.
            int pidx = dut.setPartitionColumnName(pnames[i]);
            rowBytes = ExportEncoder.encodeRow(vtable, "mytable", pidx, 1L);
            bb = ByteBuffer.wrap(rowBytes);
            bb.order(ByteOrder.LITTLE_ENDIAN);
            schemaSize = bb.getInt();
            ExportRow.decodeBufferSchema(bb, schemaSize, 1, 0);
            size = bb.getInt(); // row size
            rawRow = new byte[size];
            bb.get(rawRow);
            rowdata = dut.decodeRow(rawRow);
            rd = rowdata.values;
            assertEquals(rd[0], l);
            assertEquals(rd[1], l);
            assertEquals(rd[2], l);
            assertEquals(rd[3], 0L); //partition id
            assertEquals(rd[4], l);
            assertEquals(rd[5], l);

            assertEquals(rowdata.partitionId, 0);
            assertEquals(rowdata.partitionValue, evalues[i]);

            //Verify data
            assertEquals(rd[6], (byte) 1);
            assertEquals(rd[7], (short) 2);
            assertEquals(rd[8], 3);
            assertEquals(rd[9], 4L);
            assertEquals(rd[10], 5.5);
            assertEquals(rd[11], new TimestampType(6));
            assertEquals(rd[12], "xx");
            BigDecimal bd = (BigDecimal) rd[13];
            assertEquals(bd.compareTo(new BigDecimal(88)), 0);
            assertEquals(rd[14].toString(), GEOG_POINT.toString());
            assertEquals(rd[15].toString(), GEOG.toString());

            //Now Test write with skipinternal true
            StringWriter stringer = new StringWriter();
            String out1 = "\"1\",\"2\",\"3\",\"4\",\"5.5\",\"1969-12-31 19:00:00.000\",\"xx\",\"88.000000000000\","
                    + "\"" + GEOG_POINT.toWKT() + "\",\"" + GEOG.toWKT() + "\"";
            CSVWriter csv = new CSVWriter(stringer);
            dut.writeRow(rd, csv, true, ExportDecoderBase.BinaryEncoding.BASE64, SIMPLE_DATE_FORMAT);
            csv.flush();
            assertEquals(stringer.getBuffer().toString().trim(), out1);
            System.out.println(stringer.getBuffer().toString().trim());

            //Now Test write with skipinternal false
            stringer = new StringWriter();
            String out2 = "\"" + l + "\"," + "\"" + l + "\"," + "\"" + l + "\"," + "\"0\"," + "\"" + l + "\"," + "\"" + l + "\","
                    + "\"1\",\"2\",\"3\",\"4\",\"5.5\",\"1969-12-31 19:00:00.000\",\"xx\",\"88.000000000000\","
                    + "\"" + GEOG_POINT.toWKT() + "\",\"" + GEOG.toWKT() + "\"";
            csv = new CSVWriter(stringer);
            dut.writeRow(rd, csv, false, ExportDecoderBase.BinaryEncoding.BASE64, SIMPLE_DATE_FORMAT);
            csv.flush();
            assertEquals(stringer.getBuffer().toString().trim(), out2);
            System.out.println(stringer.getBuffer().toString().trim());
        }
    }

}