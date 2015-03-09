/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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

package org.voltdb.utils;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;

import org.voltdb.PrivateVoltTableFactory;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.common.Constants;
import org.voltdb.types.TimestampType;
import org.voltdb.types.VoltDecimalHelper;

public class SerializationHelper {
    byte[] memoizedStringBytes;
    String memoizedString;

    int getSerializedSize(String value) {
        if (memoizedString == value) {
            assert(memoizedStringBytes != null);
        }
        else {
            memoizedString = value;
            memoizedStringBytes = value.getBytes(Constants.UTF8ENCODING);
        }
        return memoizedStringBytes.length + 4; // int length prefix
    }

    void flattenToBuffer(ByteBuffer buf, String value) {
        if (memoizedString == value) {
            assert(memoizedStringBytes != null);
        }
        else {
            memoizedString = value;
            memoizedStringBytes = value.getBytes(Constants.UTF8ENCODING);
        }
        buf.putInt(memoizedStringBytes.length);
        buf.put(memoizedStringBytes);
    }

    public static String getString(ByteBuffer buf) throws IOException {
        final int len = buf.getInt();

        // check for null string
        if (len == VoltType.NULL_STRING_LENGTH) {
            return null;
        }
        assert len >= 0;

        if (len < VoltType.NULL_STRING_LENGTH) {
            throw new IOException("String length is negative " + len);
        }

        // now assume not null
        final byte[] strbytes = new byte[len];
        buf.get(strbytes);
        return new String(strbytes, Constants.UTF8ENCODING);
    }

    public static byte[] getVarbinary(ByteBuffer buf) throws IOException {
        final int len = buf.getInt();

        // check for null string
        if (len == VoltType.NULL_STRING_LENGTH) {
            return null;
        }
        assert len >= 0;

        if (len < VoltType.NULL_STRING_LENGTH) {
            throw new IOException("Varbinary length is negative " + len);
        }

        // now assume not null
        final byte[] retval = new byte[len];
        buf.get(retval);
        return retval;
    }

    public static BigDecimal getBigDecimal(ByteBuffer buf) {
        return VoltDecimalHelper.deserializeBigDecimal(buf);
    }

    public static Object readArray(final Class<?> type, ByteBuffer buf) throws IOException {
        final int count = type == byte.class ? buf.getInt() : buf.getShort();
        if (count < 0) {
            throw new IOException("Array length is negative " + count);
        }
        else if (type == byte.class) {
            if (count > (VoltType.MAX_VALUE_LENGTH)) {
                throw new IOException("Array length is greater then the max of 1 megabyte " + count);
            }
            final byte[] retval = new byte[count];
            buf.get(retval);
            return retval;
        }
        else if (type == byte[].class) {
            final byte[][] retval = new byte[count][];
            for (int i = 0; i < count; i++) {
                int size = buf.getInt();
                if (size == -1) { // null length prefix
                    retval[i] = null;
                }
                else {
                    retval[i] = new byte[size];
                    buf.get(retval[i]);
                }
            }
            return retval;
        }
        else if (type == short.class) {
            final short[] retval = new short[count];
            for (int i = 0; i < count; i++) {
                retval[i] = buf.getShort();
            }
            return retval;
        }
        else if (type == int.class) {
            final int[] retval = new int[count];
            for (int i = 0; i < count; i++) {
                retval[i] = buf.getInt();
            }
            return retval;
        }
        else if (type == long.class) {
            final long[] retval = new long[count];
            for (int i = 0; i < count; i++) {
                retval[i] = buf.getLong();
            }
            return retval;
        }
        else if (type == Long.class) {
            final Long[] retval = new Long[count];
            for (int i = 0; i < count; i++) {
                retval[i] = buf.getLong();
            }
            return retval;
        }
        else if (type == String.class) {
            final String[] retval = new String[count];
            for (int i = 0; i < count; i++) {
                retval[i] = getString(buf);
            }
            return retval;
        }
        else if (type == double.class) {
            final double[] retval = new double[count];
            for (int i = 0; i < count; i++) {
                retval[i] = buf.getDouble();
            }
            return retval;
        }
        else if (type == Double.class) {
            final Double[] retval = new Double[count];
            for (int i = 0; i < count; i++) {
                retval[i] = buf.getDouble();
            }
            return retval;
        }
        else if (type == TimestampType.class) {
            final TimestampType[] retval = new TimestampType[count];
            for (int i = 0; i < count; i++) {
                retval[i] = new TimestampType(buf.getLong());
            }
            return retval;
        }
        else if (type == BigDecimal.class) {
            final BigDecimal[] retval = new BigDecimal[count];
            for (int i = 0; i < count; ++i) {
                retval[i] = getBigDecimal(buf);
            }
            return retval;
        }
        else if (type == VoltTable.class) {
            final VoltTable[] retval = new VoltTable[count];
            for (int i = 0; i < count; ++i) {
                retval[i] = PrivateVoltTableFactory.createVoltTableFromSharedBuffer(buf.slice());
                buf.position(buf.position() + retval[i].getSerializedSize());
            }
            return retval;
        }
        else {
            throw new RuntimeException("SerializationHelper.readArray called with unhandled type: " + type.getName());
        }
    }

    /**
     * Write a string in the standard VoltDB way
     */
    public static void writeString(String value, ByteBuffer buf) {
        if (value == null) {
            buf.putInt(VoltType.NULL_STRING_LENGTH);
            return;
        }

        byte[] strbytes = value.getBytes(Constants.UTF8ENCODING);
        int len = strbytes.length;

        buf.putInt(len);
        buf.put(strbytes);
    }

    /**
     * Write a set of bytes in the standard VoltDB way
     */
    public static void writeVarbinary(byte[] bytes, ByteBuffer buf) throws IOException {
        if (bytes == null) {
            buf.putInt(VoltType.NULL_STRING_LENGTH);
            return;
        }

        buf.putInt(bytes.length);
        buf.put(bytes);
    }

    public static void writeArray(byte[] values, ByteBuffer buf) throws IOException {
        buf.putInt(values.length);
        buf.put(values);
    }

    public static void writeArray(short[] values, ByteBuffer buf) throws IOException {
        if (values.length > Short.MAX_VALUE) {
            throw new IOException("Array exceeds maximum length of "
                                  + Short.MAX_VALUE + " bytes");
        }
        buf.putShort((short)values.length);
        for (int i = 0; i < values.length; ++i) {
            buf.putShort(values[i]);
        }
    }

    public static void writeArray(int[] values, ByteBuffer buf) throws IOException {
        if (values.length > Short.MAX_VALUE) {
            throw new IOException("Array exceeds maximum length of "
                                  + Short.MAX_VALUE + " bytes");
        }
        buf.putShort((short)values.length);
        for (int i = 0; i < values.length; ++i) {
            buf.putInt(values[i]);
        }
    }

    public static void writeArray(long[] values, ByteBuffer buf) throws IOException {
        if (values.length > Short.MAX_VALUE) {
            throw new IOException("Array exceeds maximum length of "
                                  + Short.MAX_VALUE + " bytes");
        }
        buf.putShort((short)values.length);
        for (int i = 0; i < values.length; ++i) {
            buf.putLong(values[i]);
        }
    }

    public static void writeArray(double[] values, ByteBuffer buf) throws IOException {
        if (values.length > Short.MAX_VALUE) {
            throw new IOException("Array exceeds maximum length of "
                                  + Short.MAX_VALUE + " bytes");
        }
        buf.putShort((short)values.length);
        for (int i = 0; i < values.length; ++i) {
            buf.putDouble(values[i]);
        }
    }

    public static void writeArray(TimestampType[] values, ByteBuffer buf) throws IOException {
        if (values.length > Short.MAX_VALUE) {
            throw new IOException("Array exceeds maximum length of "
                                  + Short.MAX_VALUE + " bytes");
        }
        buf.putShort((short)values.length);
        for (int i = 0; i < values.length; ++i) {
            if (values[i] == null) buf.putLong(Long.MIN_VALUE);
            else buf.putLong(values[i].getTime());
        }
    }

    public static void writeArray(BigDecimal[] values, ByteBuffer buf) throws IOException {
        if (values.length > Short.MAX_VALUE) {
            throw new IOException("Array exceeds maximum length of "
                                  + Short.MAX_VALUE + " bytes");
        }
        buf.putShort((short)values.length);
        for (int i = 0; i < values.length; ++i) {
            if (values[i] == null) {
                VoltDecimalHelper.serializeNull(buf);
            }
            else {
                VoltDecimalHelper.serializeBigDecimal(values[i], buf);
            }
        }
    }

    public static void writeArray(VoltTable[] values, ByteBuffer buf) throws IOException {
        if (values.length > Short.MAX_VALUE) {
            throw new IOException("Array exceeds maximum length of "
                                  + Short.MAX_VALUE + " bytes");
        }
        buf.putShort((short)values.length);
        for (int i = 0; i < values.length; ++i) {
            if (values[i] == null)
                throw new IOException("Array being fastserialized can't contain null values (position " + i + ")");
            values[i].flattenToBuffer(buf);
        }
    }

    public static void writeArray(byte[][] values, ByteBuffer buf) throws IOException {
        if (values.length > VoltType.MAX_VALUE_LENGTH) {
            throw new IOException("Array exceeds maximum length of "
                                  + VoltType.MAX_VALUE_LENGTH + " bytes");
        }
        buf.putShort((short) values.length);
        for (int i = 0; i < values.length; ++i) {
            if (values[i] == null) {
                buf.putInt(VoltType.NULL_STRING_LENGTH);
            }
            else {
                writeArray(values[i], buf);
            }
        }
    }

}
