/* This file is part of VoltDB.
 * Copyright (C) 2008-2018 VoltDB Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.voltdb.exportclient;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.messaging.FastSerializer;
import org.voltdb.types.GeographyPointValue;
import org.voltdb.types.GeographyValue;
import org.voltdb.types.TimestampType;
import org.voltdb.types.VoltDecimalHelper;

/**
 * Makes export blocks from VoltTables.
 * Note: this is not fast...
 * @see ExportDecoderBase
 */
public class ExportEncoder {

    public static ByteBuffer getEncodedTable(VoltTable table)
    throws IOException {

        // get the table
        FastSerializer fs = new FastSerializer(false, true);
        table.resetRowPosition();
        while (table.advanceRow()) {
            byte[] rowData = encodeRow(table);
            fs.writeInt(rowData.length);
            fs.write(rowData);
        }
        byte[] data = fs.getBytes();

        // write the table with a length prefix
        fs = new FastSerializer(false, false);
        fs.writeInt(data.length);
        fs.write(data);

        return fs.getBuffer();
    }

    static byte[] encodeRow(VoltTable table)
    throws IOException {

        FastSerializer fs = new FastSerializer(false, true);
        int colCount = table.getColumnCount();

        // pack the null flags
        int nullArrayLen = ((colCount + 7) & -8) >> 3;
        boolean[] nullArray = new boolean[colCount];
        byte[] nullBits = new byte[nullArrayLen];
        for (int i = 0; i < colCount; i++) {
            nullArray[i] = isColumnNull(i, table);
            if (nullArray[i]) {
                int index = i >> 3;
                int bit = i % 8;
                byte mask = (byte) (0x80 >>> bit);
                nullBits[index] = (byte) (nullBits[index] | mask);
            }
        }
        fs.write(nullBits);

        // write the non-null columns
        for (int i = 0; i < colCount; i++) {
            if (!nullArray[i]) {
                encodeColumn(fs, i, table);
            }
        }

        final byte[] bytes = fs.getBytes();
        fs.discard();
        return bytes;
    }

    static boolean isColumnNull(int index, VoltTable table) {
        table.get(index, table.getColumnType(index));
        return table.wasNull();
    }

    static void encodeColumn(FastSerializer fs, int index, VoltTable table)
    throws IOException {
        VoltType columnType = table.getColumnType(index);
        switch (columnType) {
            case TINYINT:
                encodeTinyInt(fs, table.getLong(index));
                break;
            case SMALLINT:
                encodeSmallInt(fs, table.getLong(index));
                break;
            case INTEGER:
                encodeInteger(fs, table.getLong(index));
                break;
            case BIGINT:
                encodeBigInt(fs, table.getLong(index));
                break;
            case FLOAT:
                encodeFloat(fs, table.getDouble(index));
                break;
            case TIMESTAMP:
                encodeTimestamp(fs, table.getTimestampAsTimestamp(index));
                break;
            case STRING:
                encodeString(fs, table.getString(index));
                break;
            case DECIMAL:
                encodeDecimal(fs, table.getDecimalAsBigDecimal(index));
                break;
            case GEOGRAPHY_POINT:
                encodeGeographyPoint(fs, table.getGeographyPointValue(index));
                break;
            case GEOGRAPHY:
                encodeGeography(fs, table.getGeographyValue(index));
                break;
            default:
                throw new IOException("Invalid column type: " + columnType);
        }
    }

    /**
    * Read a decimal according to the Export encoding specification.
    *
    * @param fds
    *            Fastdeserializer containing Export stream data
    * @return decoded BigDecimal value
    * @throws IOException
    */
    static public void encodeDecimal(final FastSerializer fs, BigDecimal value)
    throws IOException {
        fs.write((byte)VoltDecimalHelper.kDefaultScale);
        fs.write((byte)16);
        fs.write(VoltDecimalHelper.serializeBigDecimal(value));
    }

    /**
    * Read a string according to the Export encoding specification
    *
    * @param fds
    * @throws IOException
    */
    static public void encodeString(final FastSerializer fs, String value)
    throws IOException {
        final byte[] strdata = value.getBytes("UTF-8");
        fs.writeInt(strdata.length);
        fs.write(strdata);
    }

    /**
    * Read a timestamp according to the Export encoding specification.
    *
    * @param fds
    * @throws IOException
    */
    static public void encodeTimestamp(final FastSerializer fs, TimestampType value)
    throws IOException {
        final long lvalue = value.getTime();
        fs.writeLong(lvalue);
    }

    /**
    * Read a float according to the Export encoding specification
    *
    * @param fds
    * @throws IOException
    */
    static public void encodeFloat(final FastSerializer fs, double value)
    throws IOException {
        fs.writeDouble(value);
    }

    /**
    * Read a bigint according to the Export encoding specification.
    *
    * @param fds
    * @throws IOException
    */
    static public void encodeBigInt(final FastSerializer fs, long value)
    throws IOException {
        fs.writeLong(value);
    }

    /**
    * Read an integer according to the Export encoding specification.
    *
    * @param fds
    * @throws IOException
    */
    static public void encodeInteger(final FastSerializer fs, long value)
    throws IOException {
        fs.writeInt((int)value);
    }

    /**
    * Read a small int according to the Export encoding specification.
    *
    * @param fds
    * @throws IOException
    */
    static public void encodeSmallInt(final FastSerializer fs, long value)
    throws IOException {
        fs.writeShort((short)value);
    }

    /**
    * Read a tiny int according to the Export encoding specification.
    *
    * @param fds
    * @throws IOException
    */
    static public void encodeTinyInt(final FastSerializer fs, long value)
    throws IOException {
        fs.write((byte)value);
    }

    /**
    * Encode a GEOGRAPHY_POINT according to the Export encoding specification.
    *
    * @param fs  The serializer to serialize to
    * @throws IOException
    */
    static public void encodeGeographyPoint(final FastSerializer fs, GeographyPointValue value)
    throws IOException {
        final int length = GeographyPointValue.getLengthInBytes();
        ByteBuffer bb = ByteBuffer.allocate(length);
        bb.order(ByteOrder.nativeOrder());
        value.flattenToBuffer(bb);
        byte[] array = bb.array();

        assert(array.length == length);

        fs.write(array);
    }

    /**
    * Encode a GEOGRAPHY according to the Export encoding specification.
    *
    * @param fs  The serializer to serialize to
    * @throws IOException
    */
    static public void encodeGeography(final FastSerializer fs, GeographyValue value)
    throws IOException {
        ByteBuffer bb = ByteBuffer.allocate(value.getLengthInBytes());
        bb.order(ByteOrder.nativeOrder());
        value.flattenToBuffer(bb);
        byte[] array = bb.array();

        fs.writeInt(array.length);
        fs.write(array);
    }
}
