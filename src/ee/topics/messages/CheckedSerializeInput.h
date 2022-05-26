/* This file is part of VoltDB.
 * Copyright (C) 2022 Volt Active Data Inc.
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


#pragma once

#include "common/NValue.hpp"
#include "common/SerializableEEException.h"
#include "common/serializeio.h"
#include "common/ValueFactory.hpp"

namespace voltdb {
namespace topics {

/*
 * Class to wrap a SerializeInputBE and assert that all reads of data from this buffer do not go beyond the bounds of
 * this buffer
 */
class CheckedSerializeInput {

public:
    CheckedSerializeInput(SerializeInputBE& delegate) : m_delegate(delegate) {}

    inline void checkBounds(size_t size) {
        if (m_delegate.remaining() < size) {
            std::ostringstream message;
            message << "Unable to read " << size << " only " << m_delegate.remaining() << " remaining: "
                    << typeid(this).name();
            message.flush();
            throw SerializableEEException(VoltEEExceptionType::VOLT_EE_EXCEPTION_TYPE_INVALID_MESSAGE,
                    message.str());
        }
    }

    // Wrappers around read methods from SerializeInput that always perform bounds checking
    inline int8_t readByte() {
        checkBounds(sizeof(int8_t));
        return m_delegate.readByte();
    }

    inline int16_t readShort() {
        checkBounds(sizeof(int16_t));
        return m_delegate.readShort();
    }

    inline int32_t readInt() {
        checkBounds(sizeof(int32_t));
        return m_delegate.readInt();
    }

    inline int64_t readLong() {
        checkBounds(sizeof(int64_t));
        return m_delegate.readLong();
    }

    /**
     * Read a string out of in. This does not copy the data but just refers to the data from in
     */
    inline NValue readString() {
        int16_t length = readShort();
        if (length < 0) {
            ValueFactory::getNullStringValue();
        }

        checkBounds(length);
        return ValueFactory::getTempStringValue(m_delegate.getRawPointer(length), length);
    }

    /***
     * Read a byte array from in. This does not copy the data but just refers to the data from in
     */
    inline NValue readBytes() {
        int32_t length = readInt();
        if (length < 0) {
            ValueFactory::getNullBinaryValue();
        }

        checkBounds(length);
        return ValueFactory::getTempBinaryValue(m_delegate.getRawPointer(length), length);
    }

    /**
     * Read a variable number of components and store them in a vector
     */
    template <class Component>
    inline void readComponents(const int16_t version, std::vector<Component>& vector) {
        const int32_t count = readInt();
        for (int i = 0; i < count; ++i) {
            vector.push_back(Component(version, *this));
        }
    }

private:
    SerializeInputBE& m_delegate;
};

}
}
