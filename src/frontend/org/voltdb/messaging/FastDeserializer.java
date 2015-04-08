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

package org.voltdb.messaging;

import java.io.IOException;
import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import org.voltdb.VoltType;
import org.voltdb.types.TimestampType;
import org.voltdb.types.VoltDecimalHelper;
import org.voltdb.utils.ByteBufferUtil;

/**
 * Yet another ByteBuffer wrapper class,
 * this one to read objects that implement the FastSerializable interface.
 * TODO: They'll come. You'll see.
 *
 * TODO: None of the use cases to date conform the the DataInput interface.
 * If that changes, maybe this class could be subtyped for the new use case.
 * Implementing the interface tends to add complexity not needed in the
 * current use cases, like implementing/stubbing unused calls and forcing
 * callers to pretend to be catching IOExceptions that are never actually
 * thrown.
 */
public class FastDeserializer {
    /**
     * Interface to monitor metrics and other information about the deserialization process
     *
     */
    public interface DeserializationMonitor {
        public void deserializedBytes(int numBytes);
    }

    private final ByteBuffer m_buffer;

    /**
     * Create a <code>FastDeserializer</code> from an array of bytes.
     *
     * @param in A byte array to wrap.
     */
    public FastDeserializer(final byte[] in) {
        this(ByteBuffer.wrap(in));
    }

    public FastDeserializer(final byte[] in, ByteOrder order) {
        m_buffer = ByteBuffer.wrap(in);
        m_buffer.order(order);
    }

    /** Create a <code>FastDeserializer</code> from a ByteBuffer.
     * @param in The ByteBuffer that will be part of this FastDeserializer. */
    public FastDeserializer(final ByteBuffer in) {
        m_buffer = in;
        assert(m_buffer.order() == ByteOrder.BIG_ENDIAN);
    }

    /**
     *  Reset this FastDeserializer and make it ready for more reads. This will set the first 4 bytes to 0
     *  so that you can differentiate between no results and the last set of results that were placed
     *  in the buffer used by this FastDeserializer
     **/
    public void clear() {
        //Don't rely on the EE to set the value to 0 when there are no results
        m_buffer.clear();
        m_buffer.putInt(0, 0);
    }

    /** @return The byte buffer contained in this object. */
    public ByteBuffer buffer() { return m_buffer; }

    /** @return the unread bytes from the contained byte buffer. */
    public ByteBuffer remainder() { return m_buffer.slice(); }

    /**
     * Read an object from its byte array representation. This is a shortcut
     * utility method useful when only a single object needs to be deserialized.
     *
     * @return The byte array representation for <code>object</code>.
     * @throws IOException
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
     * @return A deserialized object.
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
     * Read an object from a a byte array stream into the provided instance. Takes in a
     * deserialization monitor which is notified of how many bytes were deserialized.
     *
     * @param obj Instance of the class of the type to be deserialized.
     * @param monitor Monitor that will be notified of how many bytes are deserialized
     * @return A deserialized object.
     * @throws IOException Rethrows any IOExceptions thrown.
     */
    public FastSerializable readObject(final FastSerializable obj, final DeserializationMonitor monitor) throws IOException {
        final int startPosition = m_buffer.position();
        obj.readExternal(this);
        final int endPosition = m_buffer.position();
        if (monitor != null) {
            monitor.deserializedBytes(endPosition - startPosition);
        }
        return obj;
    }

    /**
     * Read a string in the standard VoltDB way.
     * @return The String value read from the internal buffer.
     * @throws IOException
     */
    public String readString() throws IOException {
        return ByteBufferUtil.readArbitraryString(m_buffer);
    }

    /**
     * Read a string in the standard VoltDB way AND
     * check that it is a reasonable non-null symbol value.
     * @return The String value read from the internal buffer.
     * @throws IOException
     */
    public String readNonNullSymbolString() throws IOException {
        return ByteBufferUtil.readNonNullSymbolString(m_buffer);
    }

    /**
     * Read a string in the standard VoltDB way AND
     * check that it is a reasonable String column value.
     * @return The String value read from the internal buffer.
     * @throws IOException
     */
    public String readVarchar() throws IOException {
        return ByteBufferUtil.readVarchar(m_buffer);
    }
    /**
     * Read a varbinary, serialized the same way VoltDB serializes
     * strings, but returned as byte[].
     *
     * @return The byte[] value read from the internal buffer.
     * @throws IOException Rethrows any IOExceptions.
     */
    public byte[] readVarbinary() throws IOException {
        int len = readInt();

        // check for null string
        if (len == VoltType.NULL_STRING_LENGTH) {
            return null;
        }

        if (len < VoltType.NULL_STRING_LENGTH) {
            throw new IOException("Varbinary length is negative " + len);
        }
        if (len > (VoltType.MAX_VALUE_LENGTH)) {
            throw new IOException("Varbinary length is greater then the max of 1 megabyte " + len);
        }
        if (len > m_buffer.remaining()) {
            throw new IOException("Varbinary length is bigger than total buffer " + len);
        }

        // now assume not null
        byte[] retval = new byte[len];
        m_buffer.get(retval);
        return retval;
    }

    /**
     * Read the VoltDB BigDecimal serialization.
     * @return BigDecimal
     * @throws IOException
     */
    public BigDecimal readBigDecimal() {
        return VoltDecimalHelper.deserializeBigDecimal(m_buffer);
    }

    /**
     * Read the VoltDB BigDecimal serialization.
     * @return BigDecimal
     * @throws IOException
     */
    public BigDecimal readBigDecimalFromString() throws IOException {
        String digits = ByteBufferUtil.readDecimalString(m_buffer);
        return VoltDecimalHelper.deserializeBigDecimalFromString(digits);
    }

    public byte[] readByteArray() throws IOException {
        final int count = readInt();
        if (count < 0) {
            throw new IOException("Serialized byte array length is negative " + count);
        }
        if (count > m_buffer.remaining()) {
            throw new IOException("Serialized byte array length " + count +
                    " is greater than total buffer length " + m_buffer.remaining());
        }

        final byte[] retval = new byte[count];
        readFully(retval);
        return retval;
    }

    public Object readArray(final Class<?> type) throws IOException {
        if (type == byte.class) {
            return readByteArray();
        }
        final int count = readShort();
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
        m_buffer.get(data);
        return ByteBuffer.wrap(data);
    }

    public boolean readBoolean() { return m_buffer.get() > 0; }

    public byte readByte() { return m_buffer.get(); }

    public double readDouble() { return m_buffer.getDouble(); }

    public float readFloat() { return m_buffer.getFloat(); }

    public void readFully(final byte[] b) { m_buffer.get(b); }

    public int readInt() { return m_buffer.getInt(); }

    public long readLong() { return m_buffer.getLong(); }

    public short readShort() { return m_buffer.getShort(); }

    public int skipBytes(final int n) {
        for (int i=0; i < n; i++)
            m_buffer.get();
        return n;
    }

    /**
     * return Current position within the underlying buffer, for self-comparison only.
     */
    public int getPosition() { return m_buffer.position(); }

    /**
     * Set current position of underlying buffer. Useful only in concert with getPosition()
     * @param pos The position to set to.
     */
    public void setPosition(int pos) { m_buffer.position(pos); }

}
