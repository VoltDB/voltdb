/*
 * Copyright 2015 The Netty Project
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

import io.netty_voltpatches.util.internal.PlatformDependent;

final class UnpooledUnsafeHeapByteBuf extends UnpooledHeapByteBuf {

    /**
     * Creates a new heap buffer with a newly allocated byte array.
     *
     * @param initialCapacity the initial capacity of the underlying byte array
     * @param maxCapacity the max capacity of the underlying byte array
     */
    UnpooledUnsafeHeapByteBuf(ByteBufAllocator alloc, int initialCapacity, int maxCapacity) {
        super(alloc, initialCapacity, maxCapacity);
    }

    @Override
    public byte getByte(int index) {
        checkIndex(index);
        return _getByte(index);
    }

    @Override
    protected byte _getByte(int index) {
        return UnsafeByteBufUtil.getByte(array, index);
    }

    @Override
    public short getShort(int index) {
        checkIndex(index, 2);
        return _getShort(index);
    }

    @Override
    protected short _getShort(int index) {
        return UnsafeByteBufUtil.getShort(array, index);
    }

    @Override
    public int getUnsignedMedium(int index) {
        checkIndex(index, 3);
        return _getUnsignedMedium(index);
    }

    @Override
    protected int _getUnsignedMedium(int index) {
        return UnsafeByteBufUtil.getUnsignedMedium(array, index);
    }

    @Override
    public int getInt(int index) {
        checkIndex(index, 4);
        return _getInt(index);
    }

    @Override
    protected int _getInt(int index) {
        return UnsafeByteBufUtil.getInt(array, index);
    }

    @Override
    public long getLong(int index) {
        checkIndex(index, 8);
        return _getLong(index);
    }

    @Override
    protected long _getLong(int index) {
        return UnsafeByteBufUtil.getLong(array, index);
    }

    @Override
    public ByteBuf setByte(int index, int value) {
        checkIndex(index);
        _setByte(index, value);
        return this;
    }

    @Override
    protected void _setByte(int index, int value) {
        UnsafeByteBufUtil.setByte(array, index, value);
    }

    @Override
    public ByteBuf setShort(int index, int value) {
        checkIndex(index, 2);
        _setShort(index, value);
        return this;
    }

    @Override
    protected void _setShort(int index, int value) {
        UnsafeByteBufUtil.setShort(array, index, value);
    }

    @Override
    public ByteBuf setMedium(int index, int   value) {
        checkIndex(index, 3);
        _setMedium(index, value);
        return this;
    }

    @Override
    protected void _setMedium(int index, int value) {
        UnsafeByteBufUtil.setMedium(array, index, value);
    }

    @Override
    public ByteBuf setInt(int index, int   value) {
        checkIndex(index, 4);
        _setInt(index, value);
        return this;
    }

    @Override
    protected void _setInt(int index, int value) {
        UnsafeByteBufUtil.setInt(array, index, value);
    }

    @Override
    public ByteBuf setLong(int index, long  value) {
        checkIndex(index, 8);
        _setLong(index, value);
        return this;
    }

    @Override
    protected void _setLong(int index, long value) {
        UnsafeByteBufUtil.setLong(array, index, value);
    }

    @Override
    public ByteBuf setZero(int index, int length) {
        if (PlatformDependent.javaVersion() >= 7) {
            // Only do on java7+ as the needed Unsafe call was only added there.
            _setZero(index, length);
            return this;
        }
        return super.setZero(index, length);
    }

    @Override
    public ByteBuf writeZero(int length) {
        if (PlatformDependent.javaVersion() >= 7) {
            // Only do on java7+ as the needed Unsafe call was only added there.
            ensureWritable(length);
            int wIndex = writerIndex;
            _setZero(wIndex, length);
            writerIndex = wIndex + length;
            return this;
        }
        return super.writeZero(length);
    }

    private void _setZero(int index, int length) {
        checkIndex(index, length);
        UnsafeByteBufUtil.setZero(array, index, length);
    }

    @Override
    @Deprecated
    protected SwappedByteBuf newSwappedByteBuf() {
        if (PlatformDependent.isUnaligned()) {
            // Only use if unaligned access is supported otherwise there is no gain.
            return new UnsafeHeapSwappedByteBuf(this);
        }
        return super.newSwappedByteBuf();
    }
}
