/* This file is part of VoltDB.
 * Copyright (C) 2008-2020 VoltDB Inc.
 *
 * This file contains original code and/or modifications of original code.
 * Any modifications made by VoltDB Inc. are licensed under the following
 * terms and conditions:
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
/* Copyright (C) 2008 by H-Store Project
 * Brown University
 * Massachusetts Institute of Technology
 * Yale University
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
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

#ifndef NATIVEEXPORTSERIALIZEIO_H
#define NATIVEEXPORTSERIALIZEIO_H

#include <common/debuglog.h>
#include <cstring>
#include <limits>
#include <stdint.h>
#include <string>

#include "common/SerializableEEException.h"

namespace voltdb {

/*
 * This file defines a crude Export serialization interface.
 * The idea is that other code could implement these method
 * names and duck-type their way to a different Export
 * serialization .. maybe doing some dynamic symbol finding
 * for a pluggable Export serializer. It's a work in progress.
 *
 * This doesn't derive from serializeio to avoid making the
 * the serialize IO baseclass functions all virtual.
 */

class ExportSerializeInput {
  public:

    ExportSerializeInput(const void* data, size_t length)
    {
        current_ = reinterpret_cast<const char*>(data);
        end_ = current_ + length;
    }


    virtual ~ExportSerializeInput() {};

    inline char readChar() {
        return readPrimitive<char>();
    }

    inline int8_t readByte() {
        return readPrimitive<int8_t>();
    }

    inline int16_t readShort() {
        return readPrimitive<int16_t>();
    }

    inline int32_t readInt() {
        return readPrimitive<int32_t>();
    }

    inline bool readBool() {
        return readByte();
    }

    inline char readEnumInSingleByte() {
        return readByte();
    }

    inline int64_t readLong() {
        return readPrimitive<int64_t>();
    }

    /**
     * Read an int encoded as a variable length value
     */
    inline int32_t readVarInt() {
        return readVarInt<int32_t>();
    }

    /**
     * Read a long encoded as a variable length value
     */
    inline int64_t readVarLong() {
        return readVarInt<int64_t>();
    }

    inline float readFloat() {
        int32_t value = readPrimitive<int32_t>();
        float retval;
        memcpy(&retval, &value, sizeof(retval));
        return retval;
    }

    inline double readDouble() {
        int64_t value = readPrimitive<int64_t>();
        double retval;
        memcpy(&retval, &value, sizeof(retval));
        return retval;
    }

    /** Returns a pointer to the internal data buffer, advancing the read position by length. */
    const void* getRawPointer(size_t length) {
        const void* result = current_;
        current_ += length;
        vassert(current_ <= end_);
        return result;
    }

    /** Copy a string from the buffer. */
    inline std::string readTextString() {
        int32_t stringLength = readInt();
        vassert(stringLength >= 0);
        return std::string(reinterpret_cast<const char*>(getRawPointer(stringLength)),
                stringLength);
    };

    /** Copy the next length bytes from the buffer to destination. */
    inline void readBytes(void* destination, size_t length) {
        ::memcpy(destination, getRawPointer(length), length);
    };

    /** Move the read position back by bytes. Warning: this method is
    currently unverified and could result in reading before the
    beginning of the buffer. */
    // TODO(evanj): Change the implementation to validate this?
    void unread(size_t bytes) {
        current_ -= bytes;
    }

private:
    /**
     * Read an integer type encoded as a variable length value which was encoded with zigzag encoding
     *
     * https://en.wikipedia.org/wiki/Variable-length_quantity#Zigzag_encoding
     */
    template <typename type>
    inline type readVarInt() {
        // How many bits to increment the shift by for each byte in the value
        const int32_t shiftIncrement = 7;
        // The maximum shifts allowed for this type
        const int32_t maxShift = (sizeof(type) * CHAR_BIT / shiftIncrement) * shiftIncrement;

        type value = 0;
        int shift = 0;
        long b;
        while (((b = readByte()) & 0x80) != 0) {
            value |= (b & 0x7f) << shift;
            shift += shiftIncrement;
            if (shift > maxShift) {
                throw SerializableEEException("Variable length integer value too large");
            }
        }
        value |= b << shift;
        return static_cast<type>(((value >> 1) ^ -(value & 1)));
    }

    template <typename T>
    T readPrimitive() {
        T value;
        ::memcpy(&value, current_, sizeof(value));
        current_ += sizeof(value);
        return value;
    }

    // Current read position.
    const char* current_;

    // End of the buffer. Valid byte range: current_ <= validPointer < end_.
    const char* end_;

    // No implicit copies
    ExportSerializeInput(const ExportSerializeInput&);
    ExportSerializeInput& operator=(const ExportSerializeInput&);
};

class ExportSerializeOutput {
  public:
    ExportSerializeOutput(void *buffer, size_t capacity) :
        buffer_(NULL), position_(0), capacity_(0)
    {
        buffer_ = reinterpret_cast<char*>(buffer);
        vassert(position_ <= capacity);
        capacity_ = capacity;
    }

    virtual ~ExportSerializeOutput() {
        // the serialization wrapper never owns its data buffer
    };

    /** Returns a pointer to the beginning of the buffer, for reading the serialized data. */
    const char* data() const { return buffer_; }

    /** Returns the number of bytes written in to the buffer. */
    size_t size() const { return position_; }

    // functions for serialization
    inline size_t writeChar(char value) {
        return writePrimitive(value);
    }

    inline size_t writeByte(int8_t value) {
        return writePrimitive(value);
    }

    inline size_t writeShort(int16_t value) {
        return writePrimitive(static_cast<uint16_t>(value));
    }

    inline size_t writeInt(int32_t value) {
        return writePrimitive(value);
    }

    inline size_t writeBool(bool value) {
        return writeByte(value ? int8_t(1) : int8_t(0));
    };

    inline size_t writeLong(int64_t value) {
        return writePrimitive(value);
    }

    inline size_t writeFloat(float value) {
        int32_t data;
        memcpy(&data, &value, sizeof(data));
        return writePrimitive(data);
    }

    inline size_t writeDouble(double value) {
        int64_t data;
        memcpy(&data, &value, sizeof(data));
        return writePrimitive(data);
    }

    /**
     * Write a long as a variable length value using zigzag encoding
     *
     * https://en.wikipedia.org/wiki/Variable-length_quantity#Zigzag_encoding
     */
    inline size_t writeVarLong(int64_t value) {
        int64_t v = ((value << 1) ^ (value >> 63));

        const int mask  = 0x7F;
        buffer_[position_] = v & mask;
        size_t written = 1;

        while (v >>= 7) {
            buffer_[position_ + written - 1] |= 0x80;
            buffer_[position_ + written++] = (v & mask);
        }
        position_ += written;

        return written;
    }

    /**
     * Calculate the size in bytes to serialize value as a variable length value
     */
    inline static size_t sizeOfVarLong(int64_t value) {
        int64_t v = ((value << 1) ^ (value >> 63));
        size_t bytes = 1;
        while (v >>= 7) {
            ++bytes;
        }
        return bytes;
    }

    inline size_t writeEnumInSingleByte(int value) {
        vassert(std::numeric_limits<int8_t>::min() <= value &&
                value <= std::numeric_limits<int8_t>::max());
        return writeByte(static_cast<int8_t>(value));
    }

    // this explicitly accepts char* and length (or ByteArray)
    // as std::string's implicit construction is unsafe!
    inline size_t writeBinaryString(const void* value, size_t length) {
        int32_t stringLength = static_cast<int32_t>(length);
        assureExpand(length + sizeof(stringLength));

        char* current = buffer_ + position_;
        memcpy(current, &stringLength, sizeof(stringLength));
        current += sizeof(stringLength);
        memcpy(current, value, length);
        position_ += sizeof(stringLength) + length;
        return sizeof(stringLength) + length;
    }

    inline size_t writeTextString(const std::string &value) {
        return writeBinaryString(value.data(), value.size());
    }

    inline size_t writeBytes(const void *value, size_t length) {
        assureExpand(length);
        memcpy(buffer_ + position_, value, length);
        position_ += length;
        return length;
    }

    inline size_t writeZeros(size_t length) {
        assureExpand(length);
        memset(buffer_ + position_, 0, length);
        position_ += length;
        return length;
    }

    /** Reserves length bytes of space for writing. Returns the offset to the bytes. */
    size_t reserveBytes(size_t length) {
        assureExpand(length);
        size_t offset = position_;
        position_ += length;
        return offset;
    }

    std::size_t position() {
        return position_;
    }

    void position(std::size_t pos) {
        position_ = pos;
    }

private:
    template <typename T>
    size_t writePrimitive(T value) {
        assureExpand(sizeof(value));
        memcpy(buffer_ + position_, &value, sizeof(value));
        position_ += sizeof(value);
        return sizeof(value);
    }

    template <typename T>
    size_t writePrimitiveAt(size_t position, T value) {
        return writeBytesAt(position, &value, sizeof(value));
    }

    inline void assureExpand(size_t next_write) {
        size_t minimum_desired = position_ + next_write;
        if (minimum_desired > capacity_) {
            // TODO: die
        }
        vassert(capacity_ >= minimum_desired);
    }

    // Beginning of the buffer.
    char* buffer_;

    // No implicit copies
    ExportSerializeOutput(const ExportSerializeOutput&);
    ExportSerializeOutput& operator=(const ExportSerializeOutput&);

protected:
    // Current write position in the buffer.
    size_t position_;
    // Total bytes this buffer can contain.
    size_t capacity_;
};

}
#endif
