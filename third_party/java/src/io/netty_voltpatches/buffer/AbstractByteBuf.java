/*
 * Copyright 2012 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package io.netty_voltpatches.buffer;

import io.netty_voltpatches.util.IllegalReferenceCountException;
import io.netty_voltpatches.util.ResourceLeakDetector;
import io.netty_voltpatches.util.ResourceLeakDetectorFactory;
import io.netty_voltpatches.util.internal.PlatformDependent;
import io.netty_voltpatches.util.internal.StringUtil;
import io.netty_voltpatches.util.internal.SystemPropertyUtil;
import io.netty_voltpatches.util.internal.logging.InternalLogger;
import io.netty_voltpatches.util.internal.logging.InternalLoggerFactory;

import static io.netty_voltpatches.util.internal.MathUtil.isOutOfBounds;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.GatheringByteChannel;
import java.nio.channels.ScatteringByteChannel;
import java.nio.charset.Charset;

/**
 * A skeletal implementation of a buffer.
 */
public abstract class AbstractByteBuf extends ByteBuf {
    private static final InternalLogger logger = InternalLoggerFactory.getInstance(AbstractByteBuf.class);
    private static final String PROP_MODE = "io.netty.buffer.bytebuf.checkAccessible";
    private static final boolean checkAccessible;

    static {
        checkAccessible = SystemPropertyUtil.getBoolean(PROP_MODE, true);
        if (logger.isDebugEnabled()) {
            logger.debug("-D{}: {}", PROP_MODE, checkAccessible);
        }
    }

    static final ResourceLeakDetector<ByteBuf> leakDetector =
            ResourceLeakDetectorFactory.instance().newResourceLeakDetector(ByteBuf.class);

    int readerIndex;
    int writerIndex;
    private int markedReaderIndex;
    private int markedWriterIndex;

    private int maxCapacity;

    private SwappedByteBuf swappedBuf;

    protected AbstractByteBuf(int maxCapacity) {
        if (maxCapacity < 0) {
            throw new IllegalArgumentException("maxCapacity: " + maxCapacity + " (expected: >= 0)");
        }
        this.maxCapacity = maxCapacity;
    }

    @Override
    public int maxCapacity() {
        return maxCapacity;
    }

    protected final void maxCapacity(int maxCapacity) {
        this.maxCapacity = maxCapacity;
    }

    @Override
    public int readerIndex() {
        return readerIndex;
    }

    @Override
    public ByteBuf readerIndex(int readerIndex) {
        if (readerIndex < 0 || readerIndex > writerIndex) {
            throw new IndexOutOfBoundsException(String.format(
                    "readerIndex: %d (expected: 0 <= readerIndex <= writerIndex(%d))", readerIndex, writerIndex));
        }
        this.readerIndex = readerIndex;
        return this;
    }

    @Override
    public int writerIndex() {
        return writerIndex;
    }

    @Override
    public ByteBuf writerIndex(int writerIndex) {
        if (writerIndex < readerIndex || writerIndex > capacity()) {
            throw new IndexOutOfBoundsException(String.format(
                    "writerIndex: %d (expected: readerIndex(%d) <= writerIndex <= capacity(%d))",
                    writerIndex, readerIndex, capacity()));
        }
        this.writerIndex = writerIndex;
        return this;
    }

    @Override
    public ByteBuf setIndex(int readerIndex, int writerIndex) {
        if (readerIndex < 0 || readerIndex > writerIndex || writerIndex > capacity()) {
            throw new IndexOutOfBoundsException(String.format(
                    "readerIndex: %d, writerIndex: %d (expected: 0 <= readerIndex <= writerIndex <= capacity(%d))",
                    readerIndex, writerIndex, capacity()));
        }
        setIndex0(readerIndex, writerIndex);
        return this;
    }

    @Override
    public ByteBuf clear() {
        readerIndex = writerIndex = 0;
        return this;
    }

    @Override
    public boolean isReadable() {
        return writerIndex > readerIndex;
    }

    @Override
    public boolean isReadable(int numBytes) {
        return writerIndex - readerIndex >= numBytes;
    }

    @Override
    public boolean isWritable() {
        return capacity() > writerIndex;
    }

    @Override
    public boolean isWritable(int numBytes) {
        return capacity() - writerIndex >= numBytes;
    }

    @Override
    public int readableBytes() {
        return writerIndex - readerIndex;
    }

    @Override
    public int writableBytes() {
        return capacity() - writerIndex;
    }

    @Override
    public int maxWritableBytes() {
        return maxCapacity() - writerIndex;
    }

    @Override
    public ByteBuf markReaderIndex() {
        markedReaderIndex = readerIndex;
        return this;
    }

    @Override
    public ByteBuf resetReaderIndex() {
        readerIndex(markedReaderIndex);
        return this;
    }

    @Override
    public ByteBuf markWriterIndex() {
        markedWriterIndex = writerIndex;
        return this;
    }

    @Override
    public ByteBuf resetWriterIndex() {
        writerIndex = markedWriterIndex;
        return this;
    }

    @Override
    public ByteBuf discardReadBytes() {
        ensureAccessible();
        if (readerIndex == 0) {
            return this;
        }

        if (readerIndex != writerIndex) {
            setBytes(0, this, readerIndex, writerIndex - readerIndex);
            writerIndex -= readerIndex;
            adjustMarkers(readerIndex);
            readerIndex = 0;
        } else {
            adjustMarkers(readerIndex);
            writerIndex = readerIndex = 0;
        }
        return this;
    }

    @Override
    public ByteBuf discardSomeReadBytes() {
        ensureAccessible();
        if (readerIndex == 0) {
            return this;
        }

        if (readerIndex == writerIndex) {
            adjustMarkers(readerIndex);
            writerIndex = readerIndex = 0;
            return this;
        }

        if (readerIndex >= capacity() >>> 1) {
            setBytes(0, this, readerIndex, writerIndex - readerIndex);
            writerIndex -= readerIndex;
            adjustMarkers(readerIndex);
            readerIndex = 0;
        }
        return this;
    }

    protected final void adjustMarkers(int decrement) {
        int markedReaderIndex = this.markedReaderIndex;
        if (markedReaderIndex <= decrement) {
            this.markedReaderIndex = 0;
            int markedWriterIndex = this.markedWriterIndex;
            if (markedWriterIndex <= decrement) {
                this.markedWriterIndex = 0;
            } else {
                this.markedWriterIndex = markedWriterIndex - decrement;
            }
        } else {
            this.markedReaderIndex = markedReaderIndex - decrement;
            markedWriterIndex -= decrement;
        }
    }

    @Override
    public ByteBuf ensureWritable(int minWritableBytes) {
        if (minWritableBytes < 0) {
            throw new IllegalArgumentException(String.format(
                    "minWritableBytes: %d (expected: >= 0)", minWritableBytes));
        }
        ensureWritable0(minWritableBytes);
        return this;
    }

    private void ensureWritable0(int minWritableBytes) {
        if (minWritableBytes <= writableBytes()) {
            return;
        }

        if (minWritableBytes > maxCapacity - writerIndex) {
            throw new IndexOutOfBoundsException(String.format(
                    "writerIndex(%d) + minWritableBytes(%d) exceeds maxCapacity(%d): %s",
                    writerIndex, minWritableBytes, maxCapacity, this));
        }

        // Normalize the current capacity to the power of 2.
        int newCapacity = calculateNewCapacity(writerIndex + minWritableBytes);

        // Adjust to the new capacity.
        capacity(newCapacity);
    }

    @Override
    public int ensureWritable(int minWritableBytes, boolean force) {
        if (minWritableBytes < 0) {
            throw new IllegalArgumentException(String.format(
                    "minWritableBytes: %d (expected: >= 0)", minWritableBytes));
        }

        if (minWritableBytes <= writableBytes()) {
            return 0;
        }

        if (minWritableBytes > maxCapacity - writerIndex) {
            if (force) {
                if (capacity() == maxCapacity()) {
                    return 1;
                }

                capacity(maxCapacity());
                return 3;
            }
        }

        // Normalize the current capacity to the power of 2.
        int newCapacity = calculateNewCapacity(writerIndex + minWritableBytes);

        // Adjust to the new capacity.
        capacity(newCapacity);
        return 2;
    }

    private int calculateNewCapacity(int minNewCapacity) {
        final int maxCapacity = this.maxCapacity;
        final int threshold = 1048576 * 4; // 4 MiB page

        if (minNewCapacity == threshold) {
            return threshold;
        }

        // If over threshold, do not double but just increase by threshold.
        if (minNewCapacity > threshold) {
            int newCapacity = minNewCapacity / threshold * threshold;
            if (newCapacity > maxCapacity - threshold) {
                newCapacity = maxCapacity;
            } else {
                newCapacity += threshold;
            }
            return newCapacity;
        }

        // Not over threshold. Double up to 4 MiB, starting from 64.
        int newCapacity = 64;
        while (newCapacity < minNewCapacity) {
            newCapacity <<= 1;
        }

        return Math.min(newCapacity, maxCapacity);
    }

    @Override
    public ByteBuf order(ByteOrder endianness) {
        if (endianness == null) {
            throw new NullPointerException("endianness");
        }
        if (endianness == order()) {
            return this;
        }

        SwappedByteBuf swappedBuf = this.swappedBuf;
        if (swappedBuf == null) {
            this.swappedBuf = swappedBuf = newSwappedByteBuf();
        }
        return swappedBuf;
    }

    /**
     * Creates a new {@link SwappedByteBuf} for this {@link ByteBuf} instance.
     */
    protected SwappedByteBuf newSwappedByteBuf() {
        return new SwappedByteBuf(this);
    }

    @Override
    public byte getByte(int index) {
        checkIndex(index);
        return _getByte(index);
    }

    protected abstract byte _getByte(int index);

    @Override
    public boolean getBoolean(int index) {
        return getByte(index) != 0;
    }

    @Override
    public short getUnsignedByte(int index) {
        return (short) (getByte(index) & 0xFF);
    }

    @Override
    public short getShort(int index) {
        checkIndex(index, 2);
        return _getShort(index);
    }

    protected abstract short _getShort(int index);

    @Override
    public int getUnsignedShort(int index) {
        return getShort(index) & 0xFFFF;
    }

    @Override
    public int getUnsignedMedium(int index) {
        checkIndex(index, 3);
        return _getUnsignedMedium(index);
    }

    protected abstract int _getUnsignedMedium(int index);

    @Override
    public int getMedium(int index) {
        int value = getUnsignedMedium(index);
        if ((value & 0x800000) != 0) {
            value |= 0xff000000;
        }
        return value;
    }

    @Override
    public int getInt(int index) {
        checkIndex(index, 4);
        return _getInt(index);
    }

    protected abstract int _getInt(int index);

    @Override
    public long getUnsignedInt(int index) {
        return getInt(index) & 0xFFFFFFFFL;
    }

    @Override
    public long getLong(int index) {
        checkIndex(index, 8);
        return _getLong(index);
    }

    protected abstract long _getLong(int index);

    @Override
    public char getChar(int index) {
        return (char) getShort(index);
    }

    @Override
    public float getFloat(int index) {
        return Float.intBitsToFloat(getInt(index));
    }

    @Override
    public double getDouble(int index) {
        return Double.longBitsToDouble(getLong(index));
    }

    @Override
    public ByteBuf getBytes(int index, byte[] dst) {
        getBytes(index, dst, 0, dst.length);
        return this;
    }

    @Override
    public ByteBuf getBytes(int index, ByteBuf dst) {
        getBytes(index, dst, dst.writableBytes());
        return this;
    }

    @Override
    public ByteBuf getBytes(int index, ByteBuf dst, int length) {
        getBytes(index, dst, dst.writerIndex(), length);
        dst.writerIndex(dst.writerIndex() + length);
        return this;
    }

    @Override
    public ByteBuf setByte(int index, int value) {
        checkIndex(index);
        _setByte(index, value);
        return this;
    }

    protected abstract void _setByte(int index, int value);

    @Override
    public ByteBuf setBoolean(int index, boolean value) {
        setByte(index, value? 1 : 0);
        return this;
    }

    @Override
    public ByteBuf setShort(int index, int value) {
        checkIndex(index, 2);
        _setShort(index, value);
        return this;
    }

    protected abstract void _setShort(int index, int value);

    @Override
    public ByteBuf setChar(int index, int value) {
        setShort(index, value);
        return this;
    }

    @Override
    public ByteBuf setMedium(int index, int value) {
        checkIndex(index, 3);
        _setMedium(index, value);
        return this;
    }

    protected abstract void _setMedium(int index, int value);

    @Override
    public ByteBuf setInt(int index, int value) {
        checkIndex(index, 4);
        _setInt(index, value);
        return this;
    }

    protected abstract void _setInt(int index, int value);

    @Override
    public ByteBuf setFloat(int index, float value) {
        setInt(index, Float.floatToRawIntBits(value));
        return this;
    }

    @Override
    public ByteBuf setLong(int index, long value) {
        checkIndex(index, 8);
        _setLong(index, value);
        return this;
    }

    protected abstract void _setLong(int index, long value);

    @Override
    public ByteBuf setDouble(int index, double value) {
        setLong(index, Double.doubleToRawLongBits(value));
        return this;
    }

    @Override
    public ByteBuf setBytes(int index, byte[] src) {
        setBytes(index, src, 0, src.length);
        return this;
    }

    @Override
    public ByteBuf setBytes(int index, ByteBuf src) {
        setBytes(index, src, src.readableBytes());
        return this;
    }

    @Override
    public ByteBuf setBytes(int index, ByteBuf src, int length) {
        checkIndex(index, length);
        if (src == null) {
            throw new NullPointerException("src");
        }
        if (length > src.readableBytes()) {
            throw new IndexOutOfBoundsException(String.format(
                    "length(%d) exceeds src.readableBytes(%d) where src is: %s", length, src.readableBytes(), src));
        }

        setBytes(index, src, src.readerIndex(), length);
        src.readerIndex(src.readerIndex() + length);
        return this;
    }

    @Override
    public ByteBuf setZero(int index, int length) {
        if (length == 0) {
            return this;
        }

        checkIndex(index, length);

        int nLong = length >>> 3;
        int nBytes = length & 7;
        for (int i = nLong; i > 0; i --) {
            _setLong(index, 0);
            index += 8;
        }
        if (nBytes == 4) {
            _setInt(index, 0);
            // Not need to update the index as we not will use it after this.
        } else if (nBytes < 4) {
            for (int i = nBytes; i > 0; i --) {
                _setByte(index, (byte) 0);
                index ++;
            }
        } else {
            _setInt(index, 0);
            index += 4;
            for (int i = nBytes - 4; i > 0; i --) {
                _setByte(index, (byte) 0);
                index ++;
            }
        }
        return this;
    }

    @Override
    public byte readByte() {
        checkReadableBytes0(1);
        int i = readerIndex;
        byte b = _getByte(i);
        readerIndex = i + 1;
        return b;
    }

    @Override
    public boolean readBoolean() {
        return readByte() != 0;
    }

    @Override
    public short readUnsignedByte() {
        return (short) (readByte() & 0xFF);
    }

    @Override
    public short readShort() {
        checkReadableBytes0(2);
        short v = _getShort(readerIndex);
        readerIndex += 2;
        return v;
    }

    @Override
    public int readUnsignedShort() {
        return readShort() & 0xFFFF;
    }

    @Override
    public int readMedium() {
        int value = readUnsignedMedium();
        if ((value & 0x800000) != 0) {
            value |= 0xff000000;
        }
        return value;
    }

    @Override
    public int readUnsignedMedium() {
        checkReadableBytes0(3);
        int v = _getUnsignedMedium(readerIndex);
        readerIndex += 3;
        return v;
    }

    @Override
    public int readInt() {
        checkReadableBytes0(4);
        int v = _getInt(readerIndex);
        readerIndex += 4;
        return v;
    }

    @Override
    public long readUnsignedInt() {
        return readInt() & 0xFFFFFFFFL;
    }

    @Override
    public long readLong() {
        checkReadableBytes0(8);
        long v = _getLong(readerIndex);
        readerIndex += 8;
        return v;
    }

    @Override
    public char readChar() {
        return (char) readShort();
    }

    @Override
    public float readFloat() {
        return Float.intBitsToFloat(readInt());
    }

    @Override
    public double readDouble() {
        return Double.longBitsToDouble(readLong());
    }

    @Override
    public ByteBuf readBytes(int length) {
        checkReadableBytes(length);
        if (length == 0) {
            return Unpooled.EMPTY_BUFFER;
        }

        ByteBuf buf = alloc().buffer(length, maxCapacity);
        buf.writeBytes(this, readerIndex, length);
        readerIndex += length;
        return buf;
    }

    @Override
    public ByteBuf readSlice(int length) {
        ByteBuf slice = slice(readerIndex, length);
        readerIndex += length;
        return slice;
    }

    @Override
    public ByteBuf readBytes(byte[] dst, int dstIndex, int length) {
        checkReadableBytes(length);
        getBytes(readerIndex, dst, dstIndex, length);
        readerIndex += length;
        return this;
    }

    @Override
    public ByteBuf readBytes(byte[] dst) {
        readBytes(dst, 0, dst.length);
        return this;
    }

    @Override
    public ByteBuf readBytes(ByteBuf dst) {
        readBytes(dst, dst.writableBytes());
        return this;
    }

    @Override
    public ByteBuf readBytes(ByteBuf dst, int length) {
        if (length > dst.writableBytes()) {
            throw new IndexOutOfBoundsException(String.format(
                    "length(%d) exceeds dst.writableBytes(%d) where dst is: %s", length, dst.writableBytes(), dst));
        }
        readBytes(dst, dst.writerIndex(), length);
        dst.writerIndex(dst.writerIndex() + length);
        return this;
    }

    @Override
    public ByteBuf readBytes(ByteBuf dst, int dstIndex, int length) {
        checkReadableBytes(length);
        getBytes(readerIndex, dst, dstIndex, length);
        readerIndex += length;
        return this;
    }

    @Override
    public ByteBuf readBytes(ByteBuffer dst) {
        int length = dst.remaining();
        checkReadableBytes(length);
        getBytes(readerIndex, dst);
        readerIndex += length;
        return this;
    }

    @Override
    public int readBytes(GatheringByteChannel out, int length)
            throws IOException {
        checkReadableBytes(length);
        int readBytes = getBytes(readerIndex, out, length);
        readerIndex += readBytes;
        return readBytes;
    }

    @Override
    public ByteBuf readBytes(OutputStream out, int length) throws IOException {
        checkReadableBytes(length);
        getBytes(readerIndex, out, length);
        readerIndex += length;
        return this;
    }

    @Override
    public ByteBuf skipBytes(int length) {
        checkReadableBytes(length);
        readerIndex += length;
        return this;
    }

    @Override
    public ByteBuf writeBoolean(boolean value) {
        writeByte(value ? 1 : 0);
        return this;
    }

    @Override
    public ByteBuf writeByte(int value) {
        ensureAccessible();
        ensureWritable0(1);
        _setByte(writerIndex++, value);
        return this;
    }

    @Override
    public ByteBuf writeShort(int value) {
        ensureAccessible();
        ensureWritable0(2);
        _setShort(writerIndex, value);
        writerIndex += 2;
        return this;
    }

    @Override
    public ByteBuf writeMedium(int value) {
        ensureAccessible();
        ensureWritable0(3);
        _setMedium(writerIndex, value);
        writerIndex += 3;
        return this;
    }

    @Override
    public ByteBuf writeInt(int value) {
        ensureAccessible();
        ensureWritable0(4);
        _setInt(writerIndex, value);
        writerIndex += 4;
        return this;
    }

    @Override
    public ByteBuf writeLong(long value) {
        ensureAccessible();
        ensureWritable0(8);
        _setLong(writerIndex, value);
        writerIndex += 8;
        return this;
    }

    @Override
    public ByteBuf writeChar(int value) {
        writeShort(value);
        return this;
    }

    @Override
    public ByteBuf writeFloat(float value) {
        writeInt(Float.floatToRawIntBits(value));
        return this;
    }

    @Override
    public ByteBuf writeDouble(double value) {
        writeLong(Double.doubleToRawLongBits(value));
        return this;
    }

    @Override
    public ByteBuf writeBytes(byte[] src, int srcIndex, int length) {
        ensureAccessible();
        ensureWritable(length);
        setBytes(writerIndex, src, srcIndex, length);
        writerIndex += length;
        return this;
    }

    @Override
    public ByteBuf writeBytes(byte[] src) {
        writeBytes(src, 0, src.length);
        return this;
    }

    @Override
    public ByteBuf writeBytes(ByteBuf src) {
        writeBytes(src, src.readableBytes());
        return this;
    }

    @Override
    public ByteBuf writeBytes(ByteBuf src, int length) {
        if (length > src.readableBytes()) {
            throw new IndexOutOfBoundsException(String.format(
                    "length(%d) exceeds src.readableBytes(%d) where src is: %s", length, src.readableBytes(), src));
        }
        writeBytes(src, src.readerIndex(), length);
        src.readerIndex(src.readerIndex() + length);
        return this;
    }

    @Override
    public ByteBuf writeBytes(ByteBuf src, int srcIndex, int length) {
        ensureAccessible();
        ensureWritable(length);
        setBytes(writerIndex, src, srcIndex, length);
        writerIndex += length;
        return this;
    }

    @Override
    public ByteBuf writeBytes(ByteBuffer src) {
        ensureAccessible();
        int length = src.remaining();
        ensureWritable(length);
        setBytes(writerIndex, src);
        writerIndex += length;
        return this;
    }

    @Override
    public int writeBytes(InputStream in, int length)
            throws IOException {
        ensureAccessible();
        ensureWritable(length);
        int writtenBytes = setBytes(writerIndex, in, length);
        if (writtenBytes > 0) {
            writerIndex += writtenBytes;
        }
        return writtenBytes;
    }

    @Override
    public int writeBytes(ScatteringByteChannel in, int length) throws IOException {
        ensureAccessible();
        ensureWritable(length);
        int writtenBytes = setBytes(writerIndex, in, length);
        if (writtenBytes > 0) {
            writerIndex += writtenBytes;
        }
        return writtenBytes;
    }

    @Override
    public ByteBuf writeZero(int length) {
        if (length == 0) {
            return this;
        }

        ensureWritable(length);
        int wIndex = writerIndex;
        checkIndex(wIndex, length);

        int nLong = length >>> 3;
        int nBytes = length & 7;
        for (int i = nLong; i > 0; i --) {
            _setLong(wIndex, 0);
            wIndex += 8;
        }
        if (nBytes == 4) {
            _setInt(wIndex, 0);
            wIndex += 4;
        } else if (nBytes < 4) {
            for (int i = nBytes; i > 0; i --) {
                _setByte(wIndex, (byte) 0);
                wIndex++;
            }
        } else {
            _setInt(wIndex, 0);
            wIndex += 4;
            for (int i = nBytes - 4; i > 0; i --) {
                _setByte(wIndex, (byte) 0);
                wIndex++;
            }
        }
        writerIndex = wIndex;
        return this;
    }

    @Override
    public ByteBuf copy() {
        return copy(readerIndex, readableBytes());
    }

    @Override
    public ByteBuf duplicate() {
        return new DuplicatedAbstractByteBuf(this);
    }

    @Override
    public ByteBuf slice() {
        return slice(readerIndex, readableBytes());
    }

    @Override
    public ByteBuf slice(int index, int length) {
        return new SlicedAbstractByteBuf(this, index, length);
    }

    @Override
    public ByteBuffer nioBuffer() {
        return nioBuffer(readerIndex, readableBytes());
    }

    @Override
    public ByteBuffer[] nioBuffers() {
        return nioBuffers(readerIndex, readableBytes());
    }

    @Override
    public String toString(Charset charset) {
        return toString(readerIndex, readableBytes(), charset);
    }

    @Override
    public String toString(int index, int length, Charset charset) {
        return ByteBufUtil.decodeString(this, index, length, charset);
    }

    @Override
    public int indexOf(int fromIndex, int toIndex, byte value) {
        return ByteBufUtil.indexOf(this, fromIndex, toIndex, value);
    }

    @Override
    public int bytesBefore(byte value) {
        return bytesBefore(readerIndex(), readableBytes(), value);
    }

    @Override
    public int bytesBefore(int length, byte value) {
        checkReadableBytes(length);
        return bytesBefore(readerIndex(), length, value);
    }

    @Override
    public int bytesBefore(int index, int length, byte value) {
        int endIndex = indexOf(index, index + length, value);
        if (endIndex < 0) {
            return -1;
        }
        return endIndex - index;
    }

    @Override
    public int forEachByte(ByteBufProcessor processor) {
        ensureAccessible();
        try {
            return forEachByteAsc0(readerIndex, writerIndex, processor);
        } catch (Exception e) {
            PlatformDependent.throwException(e);
            return -1;
        }
    }

    @Override
    public int forEachByte(int index, int length, ByteBufProcessor processor) {
        checkIndex(index, length);
        try {
            return forEachByteAsc0(index, index + length, processor);
        } catch (Exception e) {
            PlatformDependent.throwException(e);
            return -1;
        }
    }

    private int forEachByteAsc0(int start, int end, ByteBufProcessor processor) throws Exception {
        for (; start < end; ++start) {
            if (!processor.process(_getByte(start))) {
                return start;
            }
        }

        return -1;
    }

    @Override
    public int forEachByteDesc(ByteBufProcessor processor) {
        ensureAccessible();
        try {
            return forEachByteDesc0(writerIndex - 1, readerIndex, processor);
        } catch (Exception e) {
            PlatformDependent.throwException(e);
            return -1;
        }
    }

    @Override
    public int forEachByteDesc(int index, int length, ByteBufProcessor processor) {
        checkIndex(index, length);
        try {
            return forEachByteDesc0(index + length - 1, index, processor);
        } catch (Exception e) {
            PlatformDependent.throwException(e);
            return -1;
        }
    }

    private int forEachByteDesc0(int rStart, final int rEnd, ByteBufProcessor processor) throws Exception {
        for (; rStart >= rEnd; --rStart) {
            if (!processor.process(_getByte(rStart))) {
                return rStart;
            }
        }
        return -1;
    }

    @Override
    public int hashCode() {
        return ByteBufUtil.hashCode(this);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o instanceof ByteBuf) {
            return ByteBufUtil.equals(this, (ByteBuf) o);
        }
        return false;
    }

    @Override
    public int compareTo(ByteBuf that) {
        return ByteBufUtil.compare(this, that);
    }

    @Override
    public String toString() {
        if (refCnt() == 0) {
            return StringUtil.simpleClassName(this) + "(freed)";
        }

        StringBuilder buf = new StringBuilder()
            .append(StringUtil.simpleClassName(this))
            .append("(ridx: ").append(readerIndex)
            .append(", widx: ").append(writerIndex)
            .append(", cap: ").append(capacity());
        if (maxCapacity != Integer.MAX_VALUE) {
            buf.append('/').append(maxCapacity);
        }

        ByteBuf unwrapped = unwrap();
        if (unwrapped != null) {
            buf.append(", unwrapped: ").append(unwrapped);
        }
        buf.append(')');
        return buf.toString();
    }

    protected final void checkIndex(int index) {
        checkIndex(index, 1);
    }

    protected final void checkIndex(int index, int fieldLength) {
        ensureAccessible();
        checkIndex0(index, fieldLength);
    }

    final void checkIndex0(int index, int fieldLength) {
        if (isOutOfBounds(index, fieldLength, capacity())) {
            throw new IndexOutOfBoundsException(String.format(
                    "index: %d, length: %d (expected: range(0, %d))", index, fieldLength, capacity()));
        }
    }

    protected final void checkSrcIndex(int index, int length, int srcIndex, int srcCapacity) {
        checkIndex(index, length);
        if (isOutOfBounds(srcIndex, length, srcCapacity)) {
            throw new IndexOutOfBoundsException(String.format(
                    "srcIndex: %d, length: %d (expected: range(0, %d))", srcIndex, length, srcCapacity));
        }
    }

    protected final void checkDstIndex(int index, int length, int dstIndex, int dstCapacity) {
        checkIndex(index, length);
        if (isOutOfBounds(dstIndex, length, dstCapacity)) {
            throw new IndexOutOfBoundsException(String.format(
                    "dstIndex: %d, length: %d (expected: range(0, %d))", dstIndex, length, dstCapacity));
        }
    }

    /**
     * Throws an {@link IndexOutOfBoundsException} if the current
     * {@linkplain #readableBytes() readable bytes} of this buffer is less
     * than the specified value.
     */
    protected final void checkReadableBytes(int minimumReadableBytes) {
        if (minimumReadableBytes < 0) {
            throw new IllegalArgumentException("minimumReadableBytes: " + minimumReadableBytes + " (expected: >= 0)");
        }
        checkReadableBytes0(minimumReadableBytes);
    }

    private void checkReadableBytes0(int minimumReadableBytes) {
        ensureAccessible();
        if (readerIndex > writerIndex - minimumReadableBytes) {
            throw new IndexOutOfBoundsException(String.format(
                    "readerIndex(%d) + length(%d) exceeds writerIndex(%d): %s",
                    readerIndex, minimumReadableBytes, writerIndex, this));
        }
    }

    /**
     * Should be called by every method that tries to access the buffers content to check
     * if the buffer was released before.
     */
    protected final void ensureAccessible() {
        if (checkAccessible && refCnt() == 0) {
            throw new IllegalReferenceCountException(0);
        }
    }

    final void setIndex0(int readerIndex, int writerIndex) {
        this.readerIndex = readerIndex;
        this.writerIndex = writerIndex;
    }

    final void discardMarks() {
        markedReaderIndex = markedWriterIndex = 0;
    }
}
