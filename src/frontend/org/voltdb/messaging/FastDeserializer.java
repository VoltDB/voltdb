/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
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

package org.voltdb.messaging;

import java.io.DataInput;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import org.voltdb.VoltType;
import org.voltdb.common.Constants;
import org.voltdb.types.TimestampType;
import org.voltdb.types.VoltDecimalHelper;

/**
 * <code>DataInputStream</code> subclass to read objects that implement
 * the FastSerializable interface.
 *
 */
public class FastDeserializer implements DataInput {
    /**
     * Interface to monitor metrics and other information about the deserialization process
     *
     */
    public interface DeserializationMonitor {
        public void deserializedBytes(int numBytes);
    }

    private final ByteBuffer buffer;

    /**
     * Create a <code>FastDeserializer</code> from an array of bytes.
     *
     * @param in A byte array to wrap.
     */
    public FastDeserializer(final byte[] in) {
        buffer = ByteBuffer.wrap(in);
        assert(buffer.order() == ByteOrder.BIG_ENDIAN);
    }

    public FastDeserializer(final byte[] in, ByteOrder order) {
        buffer = ByteBuffer.wrap(in);
        buffer.order(order);
    }

    /** Create a <code>FastDeserializer</code> from a ByteBuffer.
     * @param in The ByteBuffer that will be part of this FastDeserializer. */
    public FastDeserializer(final ByteBuffer in) {
        buffer = in;
        assert(buffer.order() == ByteOrder.BIG_ENDIAN);
    }

    /**
     *  Reset this FastDeserializer and make it ready for more reads. This will set the first 4 bytes to 0
     *  so that you can differentiate between no results and the last set of results that were placed
     *  in the buffer used by this FastDeserializer
     **/
    public void clear() {
        //Don't rely on the EE to set the value to 0 when there are no results
        buffer.clear();
        buffer.putInt(0, 0);
    }

    /** @return The byte buffer contained in this object. */
    public ByteBuffer buffer() { return buffer; }

    /** @return the unread bytes from the contained byte buffer. */
    public ByteBuffer remainder() { return buffer.slice(); }

    /**
     * Read an object from its byte array representation. This is a shortcut
     * utility method useful when only a single object needs to be deserialized.
     *
     * @return The byte array representation for <code>object</code>.
     */
    public final static <T extends FastSerializable> T deserialize(
            final byte[] data, final Class<T> expectedType) throws IOException {
        final FastDeserializer in = new FastDeserializer(data);
        return in.readObject(expectedType);
    }

    /**
     * Read an object from a a byte array stream assuming you know the expected type.
     *
     * @param expectedType The class of the type to be deserialized.
     * @return A derserialized object.
     * @throws IOException Rethrows any IOExceptions thrown.
     */
    public <T extends FastSerializable> T readObject(final Class<T> expectedType) throws IOException {
        assert(expectedType != null);
        T obj = null;
        try {
            obj = expectedType.newInstance();
            obj.readExternal(this);
        } catch (final InstantiationException e) {
            e.printStackTrace();
        } catch (final IllegalAccessException e) {
            e.printStackTrace();
        }
        return obj;
    }

    /**
     * Read an object from a a byte array stream into th provied instance. Takes in a
     * deserialization monitor which is notified of how many bytes were deserialized.
     *
     * @param obj Instance of the class of the type to be deserialized.
     * @param monitor Monitor that will be notified of how many bytes are deserialized
     * @return A deserialized object.
     * @throws IOException Rethrows any IOExceptions thrown.
     */
    public FastSerializable readObject(final FastSerializable obj, final DeserializationMonitor monitor) throws IOException {
        final int startPosition = buffer.position();
        obj.readExternal(this);
        final int endPosition = buffer.position();
        if (monitor != null) {
            monitor.deserializedBytes(endPosition - startPosition);
        }
        return obj;
    }

    /**
     * Read a string in the standard VoltDB way without
     * wrapping the byte buffer[
     */
    public static String readString(ByteBuffer buffer) throws IOException {
        final int NULL_STRING_INDICATOR = -1;

        final int len = buffer.getInt();

        // check for null string
        if (len == NULL_STRING_INDICATOR)
            return null;
        assert len >= 0;

        if (len > VoltType.MAX_VALUE_LENGTH) {
            throw new IOException("Serializable strings cannot be longer then "
                    + VoltType.MAX_VALUE_LENGTH + " bytes");
        }
        if (len < NULL_STRING_INDICATOR) {
            throw new IOException("String length is negative " + len);
        }

        // now assume not null
        final byte[] strbytes = new byte[len];
        buffer.get(strbytes);
        String retval = null;
        try {
            retval = new String(strbytes, "UTF-8");
        } catch (final UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        return retval;
    }

    /**
     * Read a string in the standard VoltDB way. That is, four
     * bytes of length info followed by the bytes of characters
     * encoded in UTF-8.
     *
     * @return The String value read from the stream.
     * @throws IOException Rethrows any IOExceptions.
     */
    public String readString() throws IOException {
        final int len = readInt();

        // check for null string
        if (len == VoltType.NULL_STRING_LENGTH) {
            return null;
        }

        if (len < VoltType.NULL_STRING_LENGTH) {
            throw new IOException("String length is negative " + len);
        }
        if (len > buffer.remaining()) {
            throw new IOException("String length is bigger than total buffer " + len);
        }

        // now assume not null
        final byte[] strbytes = new byte[len];
        readFully(strbytes);
        return new String(strbytes, Constants.UTF8ENCODING);
    }

    /**
     * Read a varbinary, serialized the same way VoltDB serializes
     * strings, but returned as byte[].
     *
     * @return The byte[] value read from the stream.
     * @throws IOException Rethrows any IOExceptions.
     */
    public byte[] readVarbinary() throws IOException {
        final int len = readInt();

        // check for null string
        if (len == VoltType.NULL_STRING_LENGTH) {
            return null;
        }
        assert len >= 0;

        if (len < VoltType.NULL_STRING_LENGTH) {
            throw new IOException("Varbinary length is negative " + len);
        }
        if (len > buffer.remaining()) {
            throw new IOException("Varbinary length is bigger than total buffer " + len);
        }

        // now assume not null
        final byte[] retval = new byte[len];
        readFully(retval);
        return retval;
    }

    /**
     * Read the VoltDB BigDecimal serialization.
     * @return BigDecimal
     * @throws IOException
     */
    public BigDecimal readBigDecimal() throws IOException {
        return VoltDecimalHelper.deserializeBigDecimal(buffer);
    }

    /**
     * Read the VoltDB BigDecimal serialization.
     * @return BigDecimal
     * @throws IOException
     */
    public BigDecimal readBigDecimalFromString() throws IOException {
        return VoltDecimalHelper.deserializeBigDecimalFromString(this.readString());
    }

    public Object readArray(final Class<?> type) throws IOException {
        final int count = type == byte.class ? readInt() : readShort();
        if (count < 0) {
            throw new IOException("Array length is negative " + count);
        }
        if (type == byte.class) {
            if (count > (VoltType.MAX_VALUE_LENGTH)) {
                throw new IOException("Array length is greater then the max of 1 megabyte " + count);
            }
        }
        if (type == byte.class) {
            final byte[] retval = new byte[count];
            readFully(retval);
            return retval;
        }
        if (type == byte[].class) {
            final byte[][] retval = new byte[count][];
            for (int i = 0; i < count; i++) {
                int size = readInt();
                if (size == -1) { // null length prefix
                    retval[i] = null;
                }
                else {
                    retval[i] = new byte[size];
                    readFully(retval[i]);
                }
            }
            return retval;
        }
        if (type == short.class) {
            final short[] retval = new short[count];
            for (int i = 0; i < count; i++) {
                retval[i] = readShort();
            }
            return retval;
        }
        if (type == int.class) {
            final int[] retval = new int[count];
            for (int i = 0; i < count; i++)
                retval[i] = readInt();
            return retval;
        }
        if (type == long.class) {
            final long[] retval = new long[count];
            for (int i = 0; i < count; i++)
                retval[i] = readLong();
            return retval;
        }
        if (type == Long.class) {
            final Long[] retval = new Long[count];
            for (int i = 0; i < count; i++)
                retval[i] = readLong();
            return retval;
        }
        if (type == String.class) {
            final String[] retval = new String[count];
            for (int i = 0; i < count; i++)
                retval[i] = readString();
            return retval;
        }
        if (type == double.class) {
            final double[] retval = new double[count];
            for (int i = 0; i < count; i++)
                retval[i] = readDouble();
            return retval;
        }
        if (type == Double.class) {
            final Double[] retval = new Double[count];
            for (int i = 0; i < count; i++)
                retval[i] = readDouble();
            return retval;
        }
        if (type == TimestampType.class) {
            final TimestampType[] retval = new TimestampType[count];
            for (int i = 0; i < count; i++)
                retval[i] = new TimestampType(readLong());
            return retval;
        }
        if (type == BigDecimal.class) {
            final BigDecimal[] retval = new BigDecimal[count];
            for (int i = 0; i < count; ++i) {
                retval[i] = readBigDecimal();
            }
            return retval;
        }

        // probably throws an exception if type is not fastserializable
        @SuppressWarnings("unchecked")
        final
        Class<FastSerializable> cls = (Class<FastSerializable>) type;

        final FastSerializable[] retval = (FastSerializable[])Array.newInstance(type, count);
        for (int i = 0; i < count; i++)
            retval[i] = readObject(cls);
        return retval;
    }

    /**
     * Create a copy of the first byteLen bytes of the underlying buffer.
     * @param byteLen Number of bytes to copy
     * @return ByteBuffer wrapping the copied data
     */
    public ByteBuffer readBuffer(final int byteLen) {
        final byte[] data = new byte[byteLen];
        buffer.get(data);
        return ByteBuffer.wrap(data);
    }

    @Override
    public boolean readBoolean() throws IOException {
        return buffer.get() > 0;
    }

    @Override
    public byte readByte() throws IOException {
        return buffer.get();
    }

    @Override
    public char readChar() throws IOException {
        return buffer.getChar();
    }

    @Override
    public double readDouble() throws IOException {
        return buffer.getDouble();
    }

    @Override
    public float readFloat() throws IOException {
        return buffer.getFloat();
    }

    @Override
    public void readFully(final byte[] b) throws IOException {
        buffer.get(b);
    }

    @Override
    public void readFully(final byte[] b, final int off, final int len) throws IOException {
        buffer.get(b, off, len);
    }

    @Override
    public int readInt() throws IOException {
        return buffer.getInt();
    }

    @Override
    public String readLine() throws IOException {
        throw new RuntimeException("FastDeserializer.readLine() not supported.");
    }

    @Override
    public long readLong() throws IOException {
        return buffer.getLong();
    }

    @Override
    public short readShort() throws IOException {
        return buffer.getShort();
    }

    @Override
    public String readUTF() throws IOException {
        throw new RuntimeException("FastDeserializer.readUTF() not supported.");
    }

    @Override
    public int readUnsignedByte() throws IOException {
        throw new RuntimeException("FastDeserializer.readUnsignedByte() not supported.");
    }

    @Override
    public int readUnsignedShort() throws IOException {
        throw new RuntimeException("FastDeserializer.readUnsignedShort() not supported.");
    }

    @Override
    public int skipBytes(final int n) throws IOException {
        for (int i=0; i < n; i++)
            readByte();
        return n;
    }

    /**
     * return Current position within the underlying buffer, for self-comparison only.
     */
    public int getPosition() {
        return buffer.position();
    }

    /**
     * Set current position of underlying buffer. Useful only in concert with getPosition()
     * @param pos The position to set to.
     */
    public void setPosition(int pos) {
        buffer.position(pos);
    }
}
