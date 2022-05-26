/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
 *
 * This file contains original code and/or modifications of original code.
 * Any modifications made by Volt Active Data Inc. are licensed under the following
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

#pragma once

#include <limits>
#include <vector>
#include <string>
#include <sstream>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <exception>
#include <arpa/inet.h>
#include <common/debuglog.h>
#include <boost/ptr_container/ptr_vector.hpp>

#include "bytearray.h"
#include "debuglog.h"
#include "common/SQLException.h"
#include "common/types.h"

namespace voltdb {


#ifdef __DARWIN_OSSwapInt64 // for darwin/macosx

#undef htonll
#undef ntohll
#define htonll(x) __DARWIN_OSSwapInt64(x)
#define ntohll(x) __DARWIN_OSSwapInt64(x)

#else // unix in general

#undef htons
#undef ntohs
#define htons(x) static_cast<uint16_t>((((x) >> 8) & 0xff) | (((x) & 0xff) << 8))
#define ntohs(x) static_cast<uint16_t>((((x) >> 8) & 0xff) | (((x) & 0xff) << 8))

#ifdef __bswap_64 // recent linux

#undef htonll
#undef ntohll
#define htonll(x) static_cast<uint64_t>(__bswap_constant_64(x))
#define ntohll(x) static_cast<uint64_t>(__bswap_constant_64(x))

#else // unix in general again

#undef htonll
#undef ntohll
#define htonll(x) (((int64_t)(ntohl((int32_t)((x << 32) >> 32))) << 32) | (uint32_t)ntohl(((int32_t)(x >> 32))))
#define ntohll(x) (((int64_t)(ntohl((int32_t)((x << 32) >> 32))) << 32) | (uint32_t)ntohl(((int32_t)(x >> 32))))

#endif // __bswap_64

#endif // unix or mac



/** Abstract class for reading from memory buffers. */
template <Endianess E> class SerializeInput {
protected:
    /** Does no initialization. Subclasses must call initialize. */
    SerializeInput() = default;

    void initialize(const void* data, size_t length) {
        m_current = reinterpret_cast<const char*>(data);
        m_end = m_current + length;
    }

public:
    // No implicit copies
    SerializeInput(const SerializeInput&) = delete;
    SerializeInput& operator=(const SerializeInput&) = delete;
    virtual ~SerializeInput() {};

    // functions for deserialization
    inline char readChar() {
        return readPrimitive<char>();
    }

    inline int8_t readByte() {
        return readPrimitive<int8_t>();
    }

    inline int16_t readShort() {
        int16_t value = readPrimitive<int16_t>();
        if (E == BYTE_ORDER_BIG_ENDIAN) {
            return ntohs(value);
        } else {
            return value;
        }
    }

    inline int32_t readInt() {
        int32_t value = readPrimitive<int32_t>();
        if (E == BYTE_ORDER_BIG_ENDIAN) {
            return ntohl(value);
        } else {
            return value;
        }
    }

    inline bool readBool() {
        return readByte();
    }

    inline char readEnumInSingleByte() {
        return readByte();
    }

    inline int64_t readLong() {
        int64_t value = readPrimitive<int64_t>();
        if (E == BYTE_ORDER_BIG_ENDIAN) {
            return ntohll(value);
        } else {
            return value;
        }
    }

    inline float readFloat() {
        int32_t value = readPrimitive<int32_t>();
        if (E == BYTE_ORDER_BIG_ENDIAN) {
            value = ntohl(value);
        }
        float retval;
        memcpy(&retval, &value, sizeof(retval));
        return retval;
    }

    inline double readDouble() {
        int64_t value = readPrimitive<int64_t>();
        if (E == BYTE_ORDER_BIG_ENDIAN) {
            value = ntohll(value);
        }
        double retval;
        memcpy(&retval, &value, sizeof(retval));
        return retval;
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

    /** Returns a pointer to the internal data buffer, advancing the read position by length. */
    const char* getRawPointer(size_t length) {
        const char* result = m_current;
        m_current += length;
        // TODO: Make this a non-optional check?
        vassert(m_current <= m_end);
        return result;
    }

    const char* getRawPointer() {
        return m_current;
    }

    /** Copy a string from the buffer. */
    inline std::string readTextString() {
        int32_t stringLength = readInt();
        vassert(stringLength >= 0);
        return std::string(reinterpret_cast<const char*>(getRawPointer(stringLength)),
                stringLength);
    };

    /** Copy a ByteArray from the buffer. */
    inline ByteArray readBinaryString() {
        int32_t stringLength = readInt();
        vassert(stringLength >= 0);
        return ByteArray(reinterpret_cast<const char*>(getRawPointer(stringLength)),
                stringLength);
    };

    /** Copy the next length bytes from the buffer to destination. */
    inline void readBytes(void* destination, size_t length) {
        ::memcpy(destination, getRawPointer(length), length);
    };

    /** Write the buffer as hex bytes for debugging */
    std::string fullBufferStringRep();

    /** Move the read position back by bytes. Warning: this method is
    currently unverified and could result in reading before the
    beginning of the buffer. */
    // TODO(evanj): Change the implementation to validate this?
    void unread(size_t bytes) {
        m_current -= bytes;
    }

    bool hasRemaining() {
        return m_current < m_end;
    }

    int64_t remaining() {
        return m_end - m_current;
    }

    // Reduce the size of this input down to limit. Limit must be less then current end
    void limit(size_t limit) {
        vassert(m_current + limit <= m_end);
        m_end = m_current + limit;
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

        uint64_t value = 0;
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
        ::memcpy(&value, m_current, sizeof(value));
        m_current += sizeof(value);
        return value;
    }

    // Current read position.
    const char* m_current = nullptr;
    // End of the buffer. Valid byte range: m_current <= validPointer < m_end.
    const char* m_end = nullptr;
};

/** Abstract class for writing to memory buffers. Subclasses may optionally support resizing. */
class SerializeOutput {
    template <typename T>
    void writePrimitive(T value) {
        assureExpand(sizeof(value));
        memcpy(m_buffer + m_position, &value, sizeof(value));
        m_position += sizeof(value);
    }

    template <typename T>
    size_t writePrimitiveAt(size_t position, T value) {
        return writeBytesAt(position, &value, sizeof(value));
    }

    inline void assureExpand(size_t next_write) {
        size_t minimum_desired = m_position + next_write;
        if (minimum_desired > m_capacity) {
            expand(minimum_desired);
        }
        vassert(m_capacity >= minimum_desired);
    }

    /**
     * Convert a 64bit value into a zigzag encoding.
     * https://en.wikipedia.org/wiki/Variable-length_quantity#Zigzag_encoding
     */
    inline static uint64_t zigZagLong(int64_t value) {
        return (value << 1) ^ (value >> 63);
    }

    /**
     * Calculate the number of bytes required to serialize a zig zag encoded value
     */
    inline static int sizeOfZigZaggedLong(uint64_t zigZagValue) {
        int bytes = 1;
        while (zigZagValue >>= 7) {
            ++bytes;
        }
        return bytes;
    }

    // Beginning of the buffer.
    char* m_buffer = nullptr;
protected:
    // Current write position in the buffer.
    size_t m_position = 0;
    // Total bytes this buffer can contain.
    size_t m_capacity = 0;

    SerializeOutput() = default;

    /** Set the buffer to buffer with capacity. Note this does not change the position. */
    void initialize(void* buffer, size_t capacity) {
        m_buffer = reinterpret_cast<char*>(buffer);
        vassert(m_position <= capacity);
        m_capacity = capacity;
    }
    void setPosition(size_t position) {
        m_position = position;
    }
    /** Called when trying to write past the end of the
    buffer. Subclasses can optionally resize the buffer by calling
    initialize. If this function returns and size() < minimum_desired,
    the program will crash.  @param minimum_desired the minimum length
    the resized buffer needs to have. */
    virtual void expand(size_t minimum_desired) = 0;
public:
    // No implicit copies
    SerializeOutput(const SerializeOutput&) = delete;
    SerializeOutput& operator=(const SerializeOutput&) = delete;

    virtual ~SerializeOutput() {};

    /** Returns a pointer to the beginning of the buffer, for reading the serialized data. */
    const char* data() const {
        return m_buffer;
    }

    /** Returns the number of bytes written in to the buffer. */
    size_t size() const {
        return m_position;
    }

    // functions for serialization
    void writeChar(char value) {
        writePrimitive(value);
    }

    void writeByte(int8_t value) {
        writePrimitive(value);
    }

    void writeShort(int16_t value) {
        writePrimitive(static_cast<uint16_t>(htons(value)));
    }

    void writeInt(int32_t value) {
        writePrimitive(htonl(value));
    }

    void writeBool(bool value) {
        writeByte(value ? int8_t(1) : int8_t(0));
    };

    void writeLong(int64_t value) {
        writePrimitive(htonll(value));
    }

    void writeFloat(float value) {
        int32_t data;
        memcpy(&data, &value, sizeof(data));
        writePrimitive(htonl(data));
    }

    void writeDouble(double value) {
        int64_t data;
        memcpy(&data, &value, sizeof(data));
        writePrimitive(htonll(data));
    }

    void writeEnumInSingleByte(int value) {
        vassert(std::numeric_limits<int8_t>::min() <= value &&
                value <= std::numeric_limits<int8_t>::max());
        writeByte(static_cast<int8_t>(value));
    }

    size_t writeCharAt(size_t position, char value) {
        return writePrimitiveAt(position, value);
    }

    size_t writeByteAt(size_t position, int8_t value) {
        return writePrimitiveAt(position, value);
    }

    size_t writeShortAt(size_t position, int16_t value) {
        return writePrimitiveAt(position, htons(value));
    }

    size_t writeIntAt(size_t position, int32_t value) {
        return writePrimitiveAt(position, htonl(value));
    }

    size_t writeBoolAt(size_t position, bool value) {
        return writePrimitiveAt(position, value ? int8_t(1) : int8_t(0));
    }

    size_t writeLongAt(size_t position, int64_t value) {
        return writePrimitiveAt(position, htonll(value));
    }

    size_t writeFloatAt(size_t position, float value) {
        int32_t data;
        memcpy(&data, &value, sizeof(data));
        return writePrimitiveAt(position, htonl(data));
    }

    size_t writeDoubleAt(size_t position, double value) {
        int64_t data;
        memcpy(&data, &value, sizeof(data));
        return writePrimitiveAt(position, htonll(data));
    }

    /**
     * Write a long as a variable length value using zigzag encoding
     *
     * https://en.wikipedia.org/wiki/Variable-length_quantity#Zigzag_encoding
     */
    inline size_t writeVarLong(int64_t value) {
        uint64_t v = zigZagLong(value);
        assureExpand(sizeOfZigZaggedLong(v));

        const int mask  = 0x7F;
        m_buffer[m_position] = v & mask;
        size_t written = 1;

        while (v >>= 7) {
            m_buffer[m_position + written - 1] |= 0x80;
            m_buffer[m_position + written++] = (v & mask);
        }
        m_position += written;

        return written;
    }

    /**
     * Calculate the size in bytes to serialize value as a variable length value
     */
    inline static size_t sizeOfVarLong(int64_t value) {
        return sizeOfZigZaggedLong(zigZagLong(value));
    }

    // this explicitly accepts char* and length (or ByteArray)
    // as std::string's implicit construction is unsafe!
    void writeBinaryString(const void* value, size_t length) {
        int32_t stringLength = static_cast<int32_t>(length);
        assureExpand(length + sizeof(stringLength));

        // do a network order conversion
        int32_t networkOrderLen = htonl(stringLength);

        char* current = m_buffer + m_position;
        memcpy(current, &networkOrderLen, sizeof(networkOrderLen));
        current += sizeof(stringLength);
        memcpy(current, value, length);
        m_position += sizeof(stringLength) + length;
    }

    void writeBinaryString(const ByteArray &value) {
        writeBinaryString(value.data(), value.length());
    }

    void writeTextString(const std::string &value) {
        writeBinaryString(value.data(), value.size());
    }

    void writeBytes(const void *value, size_t length) {
        assureExpand(length);
        memcpy(m_buffer + m_position, value, length);
        m_position += length;
    }

    void writeZeros(size_t length) {
        assureExpand(length);
        memset(m_buffer + m_position, 0, length);
        m_position += length;
    }

    void writeVarBinary(std::function<void(SerializeOutput&)> writer) {
        int pos = reserveBytes(sizeof(int32_t));
        writer(*this);
        writeIntAt(pos, position() - pos - sizeof(int32_t));
    }

    /** Reserves length bytes of space for writing. Returns the offset to the bytes. */
    size_t reserveBytes(size_t length) {
        assureExpand(length);
        size_t offset = m_position;
        m_position += length;
        return offset;
    }

    /** Copies length bytes from value to this buffer, starting at
    offset. Offset should have been obtained from reserveBytes. This
    does not affect the current write position.  * @return offset +
    length */
    size_t writeBytesAt(size_t offset, const void *value, size_t length) {
        vassert(offset + length <= m_position);
        memcpy(m_buffer + offset, value, length);
        return offset + length;
    }

    static bool isLittleEndian() {
        static const uint16_t s = 0x0001;
        uint8_t byte;
        memcpy(&byte, &s, 1);
        return byte != 0;
    }

    std::size_t position() const {
        return m_position;
    }
};

/** Implementation of SerializeInput that references an existing buffer. */
template <Endianess E>
class ReferenceSerializeInput : public SerializeInput<E> {
public:
    ReferenceSerializeInput(const void* data, size_t length) {
        SerializeInput<E>::initialize(data, length);
    }

    // Destructor does nothing: nothing to clean up!
    virtual ~ReferenceSerializeInput() {}
};

/** Implementation of SerializeInput that makes a copy of the buffer. */
template <Endianess E>
class CopySerializeInput : public SerializeInput<E> {
public:
    CopySerializeInput(const void* data, size_t length) :
            m_bytes(reinterpret_cast<const char*>(data), static_cast<int>(length)) {
        SerializeInput<E>::initialize(m_bytes.data(), static_cast<int>(length));
    }

    // Destructor frees the ByteArray.
    virtual ~CopySerializeInput() {}

private:
    ByteArray m_bytes;
};

#ifndef SERIALIZE_IO_DECLARATIONS
#define SERIALIZE_IO_DECLARATIONS
typedef SerializeInput<BYTE_ORDER_BIG_ENDIAN> SerializeInputBE;
typedef SerializeInput<BYTE_ORDER_LITTLE_ENDIAN> SerializeInputLE;

typedef ReferenceSerializeInput<BYTE_ORDER_BIG_ENDIAN> ReferenceSerializeInputBE;
typedef ReferenceSerializeInput<BYTE_ORDER_LITTLE_ENDIAN> ReferenceSerializeInputLE;

typedef CopySerializeInput<BYTE_ORDER_BIG_ENDIAN> CopySerializeInputBE;
typedef CopySerializeInput<BYTE_ORDER_LITTLE_ENDIAN> CopySerializeInputLE;
#endif

/** Implementation of SerializeOutput that references an existing buffer. */
class ReferenceSerializeOutput : public SerializeOutput {
public:
    ReferenceSerializeOutput() = default;
    ReferenceSerializeOutput(void* data, size_t length) : SerializeOutput() {
        initialize(data, length);
    }

    /** Set the buffer to buffer with capacity and sets the position. */
    virtual void initializeWithPosition(void* buffer, size_t capacity, size_t position) {
        setPosition(position);
        initialize(buffer, capacity);
    }

    size_t remaining() const {
        return m_capacity - m_position;
    }

    // Destructor does nothing: nothing to clean up!
    virtual ~ReferenceSerializeOutput() {}

protected:
    /** Reference output can't resize the buffer: Frowny-Face. */
    virtual void expand(size_t minimum_desired) {
        throwSQLException(SQLException::volt_output_buffer_overflow,
            "Output from SQL stmt overflowed output/network buffer (%lu > %lu bytes). "
            "Try a \"limit\" clause or a stronger predicate.", minimum_desired, m_capacity);
    }
};

/*
 * A serialize output class that falls back to allocating a 50 meg buffer
 * if the regular allocation runs out of space. The topend is notified when this occurs.
 */
class FallbackSerializeOutput : public ReferenceSerializeOutput {
public:
    FallbackSerializeOutput() = default;

    /** Set the buffer to buffer with capacity and sets the position. */
    void initializeWithPosition(void* buffer, size_t capacity, size_t position) {
        if (m_fallbackBuffer != NULL) {
            char *temp = m_fallbackBuffer;
            m_fallbackBuffer = NULL;
            delete []temp;
        }
        setPosition(position);
        initialize(buffer, capacity);
    }

    // Destructor frees the fallback buffer if it is allocated
    virtual ~FallbackSerializeOutput() {
        delete []m_fallbackBuffer;
    }

    /** Expand once to a fallback size, and if that doesn't work abort */
    void expand(size_t minimum_desired);
private:
    char *m_fallbackBuffer = nullptr;
};

/** Implementation of SerializeOutput that makes a copy of the buffer. */
class CopySerializeOutput : public SerializeOutput {
public:
    // Start with something sizeable so we avoid a ton of initial
    // allocations.
    static const int INITIAL_SIZE = 8388608;

    CopySerializeOutput() : m_bytes(INITIAL_SIZE) {
        initialize(m_bytes.data(), INITIAL_SIZE);
    }

    // Destructor frees the ByteArray.
    virtual ~CopySerializeOutput() {}

    void reset() {
        setPosition(0);
    }

    size_t remaining() const {
        return m_bytes.length() - static_cast<int>(position());
    }

protected:
    /** Resize this buffer to contain twice the amount desired or add 32MB to prevent constant doubling */
    virtual void expand(size_t minimum_desired) {
        size_t next_capacity;
        if (minimum_desired > INITIAL_SIZE * 4) {
            next_capacity = m_bytes.length() + INITIAL_SIZE * 4;
        } else {
            next_capacity = minimum_desired * 2;
        }
        vassert(next_capacity < static_cast<size_t>(std::numeric_limits<int>::max()));
        m_bytes.copyAndExpand(static_cast<int>(next_capacity));
        initialize(m_bytes.data(), next_capacity);
    }

private:
    ByteArray m_bytes;
};
}

