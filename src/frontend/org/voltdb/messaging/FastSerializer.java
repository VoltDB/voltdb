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

import java.io.DataOutput;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import org.voltcore.utils.DBBPool;
import org.voltcore.utils.DBBPool.BBContainer;
import org.voltdb.ParameterSet;
import org.voltdb.StoredProcedureInvocation;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.common.Constants;
import org.voltdb.types.TimestampType;
import org.voltdb.types.VoltDecimalHelper;
import org.voltdb.utils.Encoder;
import org.voltdb.utils.SerializationHelper;

/**
 * <code>DataInputStream</code> subclass to write objects that implement
 * the FastSerializable interface.
 *
 */
public class FastSerializer implements DataOutput {
    /** callbacked when the internal buffer was grown. */
    public interface BufferGrowCallback {
        void onBufferGrow(FastSerializer obj);
    }

    public static final int INITIAL_ALLOCATION = 2048;
    private BBContainer buffer;
    private final BufferGrowCallback callback;
    private final boolean isDirect;

    /**
     * Create a <code>FastSerializer</code> that is BigEndian and uses a HeapByteBuffer
     */
    public FastSerializer() {
        this(true, false);
    }

    /**
     * Create a FastSerializer that will use the provided pool for all its allocations
     * with an initial allocation of the specified size
     * @param pool
     * @param initialAllocation
     */
    public FastSerializer(int initialAllocation) {
        this(true, false, null, initialAllocation);
    }


    /** Warning: use direct ByteBuffers with caution, as they are generally slower. */
    public FastSerializer(boolean bigEndian, boolean isDirect) {
        this(bigEndian, isDirect, null, INITIAL_ALLOCATION);
    }

    /**
     * Create an FS that will use the provided pool for direct allocations or
     * will do its own direct allocations if the pool is null. The provided BufferGrowCallback will
     * be invoked every time the buffer grows.
     * @param bigEndian
     * @param callback
     * @param pool
     */
    public FastSerializer(boolean bigEndian, BufferGrowCallback callback) {
        this(bigEndian, true, callback, INITIAL_ALLOCATION);
    }

    /**
     * Caveat. A pool won't always give back a direct byte buffer. If a direct byte buffer
     * is absolutely necessary for the serialized result then use isDirect and a null pool
     */
    /** constructor that sets callback object. */
    public FastSerializer(boolean bigEndian, boolean isDirect, BufferGrowCallback callback, int initialAllocation) {
        assert(initialAllocation > 0);
        this.isDirect = isDirect;
        if (isDirect) {
           buffer = DBBPool.allocateDirect(initialAllocation);
        } else {
           buffer = DBBPool.wrapBB(ByteBuffer.allocate(initialAllocation));
        }
        this.callback = callback;
        buffer.b().order(bigEndian ? ByteOrder.BIG_ENDIAN : ByteOrder.LITTLE_ENDIAN);
    }

    public int size() {
        return buffer.b().position();
    }

    /** Clears the contents of the underlying buffer, making iteady for more writes. */
    public void clear() {
        buffer.b().clear();
    }

    /** Resizes the internal byte buffer with a simple doubling policy, if needed. */
    private final void growIfNeeded(int minimumDesired) {
        if (buffer.b().remaining() < minimumDesired) {
            // Compute the size of the new buffer
            int newCapacity = buffer.b().capacity();
            int newRemaining = newCapacity - buffer.b().position();
            while (newRemaining < minimumDesired) {
                newRemaining += newCapacity;
                newCapacity *= 2;
            }

            // Allocate and copy
            BBContainer next;
            if (isDirect) {
                next = DBBPool.allocateDirect(newCapacity);
            } else {
                next = DBBPool.wrapBB(ByteBuffer.allocate(newCapacity));
            }
            buffer.b().flip();
            next.b().put(buffer.b());
            assert next.b().remaining() == newRemaining;
            buffer.discard();
            buffer = next;
            if (callback != null) callback.onBufferGrow(this);
            assert(buffer.b().order() == ByteOrder.BIG_ENDIAN);
        }
    }

    /**
     * Get the byte version of object. This is a shortcut utility method when
     * you only need to serialize a single object.
     *
     * @return The byte array representation for <code>object</code>.
     */
    public static byte[] serialize(FastSerializable object) throws IOException {
        FastSerializer out = new FastSerializer();
        object.writeExternal(out);
        return out.getBBContainer().b().array();
    }

    /** @return a reference to the underlying ByteBuffer. */
    public BBContainer getBBContainer() {
        buffer.b().flip();
        return buffer;
    }

    /**
     * This method is slow and horrible. It entails an extra copy. Don't use it! Ever! Not even for test!
     * Just say no to test only code. It will also leak the BBContainer if this FS is being used with a pool.
     */
    public byte[] getBytes() {
        byte[] retval = new byte[buffer.b().position()];
        int position = buffer.b().position();
        buffer.b().rewind();
        buffer.b().get(retval);
        assert position == buffer.b().position();
        return retval;
    }

    /**
     * Return a readOnly slice of this buffer. Flips the internal buffer.
     * May not be, usefully, invoked multiple times on the same internal
     * state.
     *
     * Only use this if using a non-direct ByteBuffer!
     */
    public ByteBuffer getBuffer() {
        assert(isDirect == false);
        assert(buffer.b().hasArray());
        assert(!buffer.b().isDirect());
        buffer.b().flip();
        return buffer.b().asReadOnlyBuffer();
    }

    /**
     * When a fast serializer is shared between Java and native
     * this is called to retrieve a reference to the to buffer without
     * flipping it. OnBufferGrowCallback needs this to update the pointer
     * to the shared buffer when the parameter buffer grows.
     */
    public BBContainer getContainerNoFlip() {
        assert(isDirect == true);
        assert(buffer.b().isDirect());
        return buffer;
    }

    /**
     * Get a ascii-string-safe version of the binary value using a
     * hex encoding.
     *
     * @return A hex-encoded string value representing the serialized
     * objects.
     */
    public String getHexEncodedBytes() {
        buffer.b().flip();
        byte bytes[] = new byte[buffer.b().remaining()];
        buffer.b().get(bytes);
        String hex = Encoder.hexEncode(bytes);
        buffer.discard();
        return hex;
    }

    /**
     * Write an object to the byte stream. Note: you need to know the
     * type to read it back out again.
     *
     * @param obj The <code>FastSerializable</code> object to be written.
     * @throws IOException Rethrows any IOExceptions thrown.
     */
    public void writeObject(FastSerializable obj) throws IOException {
        obj.writeExternal(this);
    }

    /**
     * Write a string in the standard VoltDB way without
     * wrapping the byte buffer.
     */
    public static void writeString(String string, ByteBuffer buffer) throws IOException {
        if (string == null) {
            buffer.putInt(VoltType.NULL_STRING_LENGTH);
            return;
        }

        byte[] strbytes = string.getBytes(Constants.UTF8ENCODING);
        int len = strbytes.length;

        buffer.putInt(len);
        buffer.put(strbytes);
    }

    /**
     * Write a string in the standard VoltDB way. That is, two
     * bytes of length info followed by the bytes of characters
     * encoded in UTF-8.
     *
     * @param string The string value to be serialized.
     * @throws IOException Rethrows any IOExceptions thrown.
     */
    public void writeString(String string) throws IOException {
        if (string == null) {
            writeInt(VoltType.NULL_STRING_LENGTH);
            return;
        }

        byte[]  strbytes = string.getBytes(Constants.UTF8ENCODING);
        int len = strbytes.length;

        writeInt(len);
        write(strbytes);
    }

    /**
     * Write a varbinary in the standard VoltDB way. That is, four
     * bytes of length info followed by the bytes.
     *
     * @param bin The byte array value to be serialized.
     * @throws IOException Rethrows any IOExceptions thrown.
     */
    public void writeVarbinary(byte[] bin) throws IOException {
        if (bin == null) {
            writeInt(VoltType.NULL_STRING_LENGTH);
            return;
        }

        if (bin.length > VoltType.MAX_VALUE_LENGTH) {
            throw new IOException("Varbinary exceeds maximum length of "
                                  + VoltType.MAX_VALUE_LENGTH + " bytes.");
        }
        writeInt(bin.length);
        write(bin);
    }

    /**
     * Write a table using it's ByteBuffer serialization code.
     */
    public void writeTable(VoltTable table) throws IOException {
        int len = table.getSerializedSize();
        growIfNeeded(len);
        table.flattenToBuffer(buffer.b());
    }

    /**
     * Write an SPI using it's ByteBuffer serialization code.
     */
    public void writeInvocation(StoredProcedureInvocation invocation) throws IOException {
        int len = invocation.getSerializedSize();
        growIfNeeded(len);
        invocation.flattenToBuffer(buffer.b());
    }

    /**
     * Write a ParameterSet using it's ByteBuffer serialization code.
     */
    public void writeParameterSet(ParameterSet params) throws IOException {
        int len = params.getSerializedSize();
        growIfNeeded(len);
        params.flattenToBuffer(buffer.b());
    }

    // These writeArray() methods are tested in TestSQLTypesSuite.
    // If changing the max limits, please update testInvalidParameterSerializations.

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
        if (values.length > Short.MAX_VALUE) {
            throw new IOException("Array exceeds maximum length of "
                                  + Short.MAX_VALUE + " bytes");
        }
        buf.putShort((short)values.length);
        for (int i = 0; i < values.length; ++i) {
            if (values[i] == null) {
                buf.putInt(-1); // null length prefix
            }
            else {
                SerializationHelper.writeArray(values[i], buf);
            }
        }
    }

    public void writeArray(FastSerializable[] values) throws IOException {
        if (values.length > Short.MAX_VALUE) {
            throw new IOException("Array exceeds maximum length of "
                                  + Short.MAX_VALUE + " bytes");
        }
        writeShort(values.length);
        for (int i = 0; i < values.length; ++i) {
            if (values[i] == null)
                throw new IOException("Array being fastserialized can't contain null values (position " + i + ")");
            writeObject(values[i]);
        }
    }

    public void writeArray(byte[][] values) throws IOException {
        if (values.length > VoltType.MAX_VALUE_LENGTH) {
            throw new IOException("Array exceeds maximum length of "
                                  + VoltType.MAX_VALUE_LENGTH + " bytes");
        }
        writeShort(values.length);
        for (int i = 0; i < values.length; ++i) {
            if (values[i] == null) {
                writeInt(VoltType.NULL_STRING_LENGTH);
            }
            else {
                writeArray(values[i]);
            }
        }
    }

    public void writeArray(byte[] values) throws IOException {
        if (values.length > VoltType.MAX_VALUE_LENGTH) {
            throw new IOException("Array exceeds maximum length of "
                                  + VoltType.MAX_VALUE_LENGTH + " bytes");
        }
        writeInt(values.length);
        write(values);
    }

    public void writeArray(short[] values) throws IOException {
        if (values.length > Short.MAX_VALUE) {
            throw new IOException("Array exceeds maximum length of "
                                  + Short.MAX_VALUE + " bytes");
        }
        writeShort(values.length);
        for (int i = 0; i < values.length; ++i) {
            writeShort(values[i]);
        }
    }

    public void writeArray(int[] values) throws IOException {
        if (values.length > Short.MAX_VALUE) {
            throw new IOException("Array exceeds maximum length of "
                                  + Short.MAX_VALUE + " bytes");
        }
        writeShort(values.length);
        for (int i = 0; i < values.length; ++i) {
            writeInt(values[i]);
        }
    }

    public void writeArray(long[] values) throws IOException {
        if (values.length > Short.MAX_VALUE) {
            throw new IOException("Array exceeds maximum length of "
                                  + Short.MAX_VALUE + " bytes");
        }
        writeShort(values.length);
        for (int i = 0; i < values.length; ++i) {
            writeLong(values[i]);
        }
    }

    public void writeArray(double[] values) throws IOException {
        if (values.length > Short.MAX_VALUE) {
            throw new IOException("Array exceeds maximum length of "
                                  + Short.MAX_VALUE + " bytes");
        }
        writeShort(values.length);
        for (int i = 0; i < values.length; ++i) {
            writeDouble(values[i]);
        }
    }

    public void writeArray(String[] values) throws IOException {
        if (values.length > Short.MAX_VALUE) {
            throw new IOException("Array exceeds maximum length of "
                                  + Short.MAX_VALUE + " bytes");
        }
        writeShort(values.length);
        for (int i = 0; i < values.length; ++i) {
            writeString(values[i]);
        }
    }

    public void writeArray(TimestampType[] values) throws IOException {
        if (values.length > Short.MAX_VALUE) {
            throw new IOException("Array exceeds maximum length of "
                                  + Short.MAX_VALUE + " bytes");
        }
        writeShort(values.length);
        for (int i = 0; i < values.length; ++i) {
            if (values[i] == null) writeLong(Long.MIN_VALUE);
            else writeLong(values[i].getTime());
        }
    }

    public void writeArray(BigDecimal[] values) throws IOException {
        if (values.length > Short.MAX_VALUE) {
            throw new IOException("Array exceeds maximum length of "
                                  + Short.MAX_VALUE + " bytes");
        }
        writeShort(values.length);
        growIfNeeded(16); // sizeof bigdecimal
        for (int i = 0; i < values.length; ++i) {
            if (values[i] == null) {
                VoltDecimalHelper.serializeNull(buffer.b());
            }
            else {
                VoltDecimalHelper.serializeBigDecimal(values[i], buffer.b());
            }
        }
    }

    @Override
    public void write(int b) throws IOException {
        writeByte((byte) b);
    }

    @Override
    public void write(byte[] b) throws IOException {
        growIfNeeded(b.length);
        buffer.b().put(b);
    }

    public void write(ByteBuffer b) throws IOException {
        growIfNeeded(b.limit() - b.position());
        buffer.b().put(b);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        growIfNeeded(len);
        buffer.b().put(b, off, len);
    }

    @Override
    public void writeBoolean(boolean v) throws IOException {
        writeByte((byte) (v ? 1 : 0));
    }

    @Override
    public void writeByte(int v) throws IOException {
        growIfNeeded(Byte.SIZE/8);
        buffer.b().put((byte) v);
    }

    @Override
    public void writeBytes(String s) throws IOException {
        throw new UnsupportedOperationException("FastSerializer.writeBytes() not supported.");
    }

    @Override
    public void writeChar(int v) throws IOException {
        growIfNeeded(Character.SIZE/8);
        buffer.b().putChar((char) v);
    }

    @Override
    public void writeChars(String s) throws IOException {
        throw new UnsupportedOperationException("FastSerializer.writeChars() not supported.");
    }

    @Override
    public void writeDouble(double v) throws IOException {
        growIfNeeded(Double.SIZE/8);
        buffer.b().putDouble(v);
    }

    @Override
    public void writeFloat(float v) throws IOException {
        growIfNeeded(Float.SIZE/8);
        buffer.b().putFloat(v);
    }

    @Override
    public void writeInt(int v) throws IOException {
        growIfNeeded(Integer.SIZE/8);
        buffer.b().putInt(v);
    }

    @Override
    public void writeLong(long v) throws IOException {
        growIfNeeded(Long.SIZE/8);
        buffer.b().putLong(v);
    }

    @Override
    public void writeShort(int v) throws IOException {
        growIfNeeded(Short.SIZE/8);
        buffer.b().putShort((short) v);
    }

    @Override
    public void writeUTF(String str) throws IOException {
        throw new UnsupportedOperationException("FastSerializer.writeChars() not supported.");
    }

    /**
     * return Current position within the underlying buffer, for self-comparison only.
     */
    public int getPosition() {
        return buffer.b().position();
    }

    /**
     * Set current position of underlying buffer. Useful only in concert with getPosition()
     * @param pos The position to set to.
     */
    public void setPosition(int pos) {
        buffer.b().position(pos);
    }

    public static void writeString(byte[] m_procNameBytes, ByteBuffer buf) throws IOException {
        if (m_procNameBytes == null) {
            buf.putInt(VoltType.NULL_STRING_LENGTH);
            return;
        }
        buf.putInt(m_procNameBytes.length);
        buf.put(m_procNameBytes);
    }

    public void discard() {
        buffer.discard();
    }
}
