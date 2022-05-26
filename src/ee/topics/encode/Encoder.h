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

#include "common/MiscUtil.h"
#include "common/serializeio.h"
#include "common/tabletuple.h"

namespace voltdb { namespace topics {

using  TopicProperties = std::unordered_map<std::string, std::string>;

/**
 * Interface for serializing a TableTuple into an SerializeOutput
 */
class TupleEncoder {
public:
    virtual ~TupleEncoder() {}

    /**
     * @param tuple that will be serialized
     * @return the exact size that the tuple will consume when serialized or -1 if tuple to be encoded is null
     */
    virtual int32_t sizeOf(const TableTuple& tuple) = 0;

    /**
     * Serialize tuple into out
     *
     * @param out to write serialized data
     * @param tuple to serialize to out
     * @return amount of data written  or -1 if the value encoded was null
     */
    virtual int32_t encode(SerializeOutput& out, const TableTuple& tuple) = 0;

protected:
    static bool parseBoolProperty(const TopicProperties& props,
            const std::string& property, const bool defBool) {
        auto prop = props.find(property);
        if (prop != props.end()) {
            return MiscUtil::parseBool(&prop->second);
        }
        return defBool;
    }

    static char parseCharProperty(const TopicProperties& props,
            const std::string& property, const char defChar) {
        auto prop = props.find(property);
        if (prop != props.end() && prop->second.length() > 0) {
            return prop->second[0];
        }
        return defChar;
    }

    static const std::string& parseStringProperty(const TopicProperties& props,
            const std::string& property, const std::string& defStr) {
        auto prop = props.find(property);
        if (prop != props.end()) {
            return prop->second;
        }
        return defStr;
    }
};

/**
 * Interface for serializing a NValue into an SerializeOutput
 */
class NValueEncoder {
public:
    virtual ~NValueEncoder() {}
    /**
     * @param value that will be serialized
     * @return the exact size that the value will consume when serialized or -1 if value to be encoded is null
     */
    virtual int32_t sizeOf(const NValue& value) = 0;

    /**
     * Serialize tuple into out
     *
     * @param out to write serialized data
     * @param value to serialize to out
     * @return amount of data written or -1 if value to be encoded is null
     */
    virtual int32_t encode(SerializeOutput& out, const NValue& value) = 0;

protected:
    /**
     * Utility method for calculating the serialized size of an integer value written in a zigzag variable length encoding
     *
     * https://en.wikipedia.org/wiki/Variable-length_quantity#Zigzag_encoding
     */
    static int32_t serializedSizeOfVarInt(int64_t value) {
        return SerializeOutput::sizeOfVarLong(value);
    }
};

/**
 * TupleEncoder which does not encode anything and always returns -1 to indicate value is null
 */
class NullEncoder : public TupleEncoder {
public:
    NullEncoder() = default;

    int32_t sizeOf(const TableTuple& tuple) override {
        return -1;
    }

    int32_t encode(SerializeOutput& out, const TableTuple& tuple) override {
        return -1;
    }
};

/**
 * Simple tuple encoder which uses a single NValueEncoder to encode one value at a constant index from the tuple
 */
template <class ENCODER>
class SingleValueEncoder : public TupleEncoder {
public:
    SingleValueEncoder(int32_t index) : m_index(index) {};

    int32_t sizeOf(const TableTuple& tuple) override {
        const NValue value = tuple.getNValue(m_index);
        if (value.isNull()) {
            return -1;
        }
        return m_encoder.sizeOf(value);
    }

    int32_t encode(SerializeOutput& out, const TableTuple& tuple) override {
        const NValue value = tuple.getNValue(m_index);
        if (value.isNull()) {
            return -1;
        }
        return m_encoder.encode(out, value);
    }

private:
    ENCODER m_encoder;
    const int32_t m_index;
};

/**
 * NValueEncoder for encoding ValueType::tINTEGER
 */
class IntEncoder : public NValueEncoder {
public:
    IntEncoder() = default;

    int32_t sizeOf(const NValue& value) override {
        vassert(ValuePeeker::peekValueType(value) == ValueType::tINTEGER);
        return sizeof(int32_t);
    }

    int32_t encode(SerializeOutput& out, const NValue& value) override {
        out.writeInt(ValuePeeker::peekInteger(value));
        return sizeof(int32_t);
    }
};

/**
 * NValueEncoder for encoding ValueType::tBIGINT
 */
class BigIntEncoder : public NValueEncoder {
public:
    BigIntEncoder() = default;

    int32_t sizeOf(const NValue& value) override {
        vassert(ValuePeeker::peekValueType(value) == ValueType::tBIGINT);
        return sizeof(int64_t);
    }

    int32_t encode(SerializeOutput& out, const NValue& value) override {
        out.writeLong(ValuePeeker::peekBigInt(value));
        return sizeof(int64_t);
    }
};

/**
 * NValueEncoder for encoding ValueType::tDOUBLE
 */
class DoubleEncoder : public NValueEncoder {
public:
    DoubleEncoder() = default;

    int32_t sizeOf(const NValue& value) override {
        vassert(ValuePeeker::peekValueType(value) == ValueType::tDOUBLE);
        return sizeof(double);
    }

    int32_t encode(SerializeOutput& out, const NValue& value) override {
        out.writeDouble(ValuePeeker::peekDouble(value));
        return sizeof(double);
    }
};

/**
 * NValueEncoder for encoding ValueType::tVARCHAR or ValueType::tVARBINARY without a preceding length value
 */
class PlainVarLenEncoder : public NValueEncoder {
public:
    PlainVarLenEncoder() = default;

    int32_t sizeOf(const NValue& value) override {
        int32_t length;
        ValuePeeker::peekObject_withoutNull(value, &length);
        return length;
    }

    int32_t encode(SerializeOutput& out, const NValue& value) override {
        int32_t length;
        const char* string = ValuePeeker::peekObject_withoutNull(value, &length);
        out.writeBytes(string, length);
        return length;
    }
};

/**
 * A generic NValue encoder which converts the nvalue to a string and then serializes that string.
 * Uses a caching mechanism so that the string does not have to be generated twice
 */
class ToStringEncoder : public NValueEncoder {
public:
    ToStringEncoder() = default;

    int32_t sizeOf(const NValue& value) override {
        m_nvalue = value;
        m_value = value.toString();
        return m_value.length();
    }

    int32_t encode(SerializeOutput& out, const NValue& value) override {
        if (m_nvalue != value) {
            m_value = value.toString();
        }

        int32_t length = m_value.length();
        out.writeBytes(m_value.c_str(), length);

        m_nvalue = NValue::getNullValue(ValueType::tNULL);
        m_value.clear();

        return length;
    }

private:
    // The cache of last encoded Tuple
    NValue m_nvalue = NValue::getNullValue(ValueType::tNULL);
    std::string m_value;
};

} }
