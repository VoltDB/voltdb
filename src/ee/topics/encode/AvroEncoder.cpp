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

#include <vector>

#include "topics/encode/AvroEncoder.h"

namespace voltdb { namespace topics {

// Encoders used for encoding avro values. None of these handle NULL because that is handled by the AvroValueEncoder wrapper

/**
 * Value encoder for encoding integer types in a variable length format
 */
template<typename type>
class VarIntEncoder: public NValueEncoder {
public:
    VarIntEncoder() = default;

    int32_t sizeOf(const NValue& value) override {
        return serializedSizeOfVarInt(ValuePeeker::peekAsBigInt(value));
    }

    virtual int32_t encode(SerializeOutput& out, const NValue& value) override {
        return static_cast<int32_t>(out.writeVarLong(ValuePeeker::peekAsBigInt(value)));
    }
};

/**
 * Value encoder for timestamps in micro seconds
 */
class MicroTimestampEncoder: public VarIntEncoder<int64_t> {
public:
    MicroTimestampEncoder() = default;

    int32_t encode(SerializeOutput& out, const NValue& value) override {
        return out.writeVarLong(ValuePeeker::peekTimestamp(value));
    }
};

/**
 * Value encoder for timestamps in milli seconds
 */
class MilliTimestampEncoder: public VarIntEncoder<int64_t> {
public:
    MilliTimestampEncoder() = default;

    int32_t encode(SerializeOutput& out, const NValue& value) override {
        return out.writeVarLong(ValuePeeker::peekTimestamp(value) / 1000);
    }
};

/**
 * Value encoder for encoding doubles using little endian order, which is required by avro serialization
 */
class DoubleLEEncoder: public NValueEncoder {
public:
    DoubleLEEncoder() = default;

    int32_t sizeOf(const NValue& value) override {
        vassert(ValuePeeker::peekValueType(value) == ValueType::tDOUBLE);
        return sizeof(double);
    }

    int32_t encode(SerializeOutput& out, const NValue& value) override {
        vassert(out.isLittleEndian());
        double rawValue = ValuePeeker::peekDouble(value);
        out.writeBytes(reinterpret_cast<char*>(&rawValue), sizeof(rawValue));
        return sizeof(rawValue);
    }
};

/**
 * Value encoder for wrapping other value encoders that write variable length data such as strings or char[]
 */
template <class ENCODER>
class VarLenEncoder: public NValueEncoder {
public:
    VarLenEncoder() = default;

    int32_t sizeOf(const NValue& value) override {
        int32_t size = m_encoder.sizeOf(value);
        size += serializedSizeOfVarInt(size);
        return size;
    }

    int32_t encode(SerializeOutput& out, const NValue& value) override {
        int32_t len = m_encoder.sizeOf(value);
        len += out.writeVarLong(len);
        m_encoder.encode(out, value);
        return len;
    }

private:
    ENCODER m_encoder;
};

/**
 * Value encoder for decimal types with a fixed size of 16 bytes, which is the only type currently supported
 */
class DecimalEncoder: public NValueEncoder {
public:
    DecimalEncoder() = default;

    int32_t sizeOf(const NValue& value) override {
        return sizeof(int64_t) * 2;
    }

    int32_t encode(SerializeOutput& out, const NValue& value) override {
        TTInt decimal = ValuePeeker::peekDecimal(value);
        out.writeLong(htonll(decimal.table[1]));
        out.writeLong(htonll(decimal.table[0]));
        return sizeOf(value);
    }
};

/**
 * Value encoder to encode GeographyPointValues in a binary format
 */
class BinaryPointEncoder: public NValueEncoder {
public:
    BinaryPointEncoder() = default;

    int32_t sizeOf(const NValue& value) override {
        return sizeof(double) * 2;
    }

    int32_t encode(SerializeOutput& out, const NValue& value) override {
        ValuePeeker::peekGeographyPointValue(value).serializeTo(out);
        return sizeof(double) * 2;
    }
};

/**
 * Value encoder to encode GeographyPointValues in a string format
 */
class StringPointEncoder: public NValueEncoder {
public:
    StringPointEncoder() = default;

    int32_t sizeOf(const NValue& value) override {
        m_valueCache = &value;
        m_stringCache = ValuePeeker::peekGeographyPointValue(value).toWKT();
        return m_stringCache.length();
    }

    int32_t encode(SerializeOutput& out, const NValue& value) override {
        if (m_valueCache != &value) {
            m_stringCache = ValuePeeker::peekGeographyPointValue(value).toWKT();
        }
        int32_t len = m_stringCache.length();
        out.writeBytes(m_stringCache.c_str(), len);

        // clear the cache on the assumption that a value will only be written once
        m_valueCache = nullptr;
        m_stringCache.clear();
        return len;
    }

private:
    const NValue* m_valueCache = nullptr;
    std::string m_stringCache;
};

/**
 * Value encoder to encode GeographyValues in a binary format
 */
class BinaryGeographyEncoder : public NValueEncoder {
public:
    BinaryGeographyEncoder() = default;

    int32_t sizeOf(const NValue& value) override {
        return ValuePeeker::peekGeographyValue(value).length();
    }

    int32_t encode(SerializeOutput& out, const NValue& value) override {
        GeographyValue gv = ValuePeeker::peekGeographyValue(value);
        out.writeBytes(gv.data(), gv.length());
        return gv.length();
    }
};

/**
 * Value encoder to encode GeographyValues in a string format
 */
class StringGeographyEncoder: public NValueEncoder {
public:
    StringGeographyEncoder() = default;

    int32_t sizeOf(const NValue& value) override {
        m_valueCache = &value;
        m_stringCache = ValuePeeker::peekGeographyValue(value).toWKT();
        return m_stringCache.length();
    }

    int32_t encode(SerializeOutput& out, const NValue& value) override {
        if (m_valueCache != &value) {
            m_stringCache = ValuePeeker::peekGeographyValue(value).toWKT();
        }
        int32_t len = m_stringCache.length();
        out.writeBytes(m_stringCache.c_str(), len);

        // clear the cache on the assumption that a value will only be written once
        m_valueCache = nullptr;
        m_stringCache.clear();
        return len;
    }

private:
    const NValue* m_valueCache = nullptr;
    std::string m_stringCache;
};

// Properties keys which correspond to those defined for TopicProperties in java
const std::string AvroEncoder::PROP_TIMESTAMP_ENCODING = "config.avro.timestamp";
const std::string AvroEncoder::PROP_POINT_ENCODING = "config.avro.geographyPoint";
const std::string AvroEncoder::PROP_GEOGRAPHY_ENCODING = "config.avro.geography";

AvroEncoder::AvroEncoder(int32_t schemaId, const TupleSchema& schema, const std::vector<int32_t>& indexes,
        const std::unordered_map<std::string, std::string>& props) :
                m_schemaId(schemaId), m_encoders() {
    m_encoders.reserve(indexes.size());

    for (int32_t index : indexes) {
        const TupleSchema::ColumnInfo* info = schema.getColumnInfo(index);

        NValueEncoder* encoder = nullptr;

        switch (info->getVoltType()) {
        case ValueType::tTINYINT:
            encoder = new VarIntEncoder<int8_t>();
            break;
        case ValueType::tSMALLINT:
            encoder = new VarIntEncoder<int16_t>();
            break;
        case ValueType::tINTEGER:
            encoder = new VarIntEncoder<int32_t>();
            break;
        case ValueType::tBIGINT:
            encoder = new VarIntEncoder<int64_t>();
            break;
        case ValueType::tDOUBLE:
            encoder = new DoubleLEEncoder();
            break;
        case ValueType::tTIMESTAMP: {
                auto prop = props.find(PROP_TIMESTAMP_ENCODING);
                if (prop == props.end() || prop->second == "MICROSECONDS") {
                    encoder = new MicroTimestampEncoder();
                } else if (prop->second == "MILLISECONDS") {
                    encoder = new MilliTimestampEncoder();
                } else {
                    vassert(false);
                }
                break;
            }
        case ValueType::tDECIMAL:
            // TODO maybe make a fixed length version of this to save 8 bytes but that has to be added to schema generation
            encoder = new VarLenEncoder<DecimalEncoder>();
            break;
        case ValueType::tVARCHAR:
        case ValueType::tVARBINARY:
            encoder = new VarLenEncoder<PlainVarLenEncoder>();
            break;
        case ValueType::tPOINT: {
            auto prop = props.find(PROP_POINT_ENCODING);
            if (prop == props.end() || prop->second == "FIXED_BINARY") {
                encoder = new BinaryPointEncoder();
            } else if (prop->second == "BINARY") {
                encoder = new VarLenEncoder<BinaryPointEncoder>();
            } else if (prop->second == "STRING") {
                encoder = new VarLenEncoder<StringPointEncoder>();
            } else {
                vassert(false);
            }
            break;
        }
        case ValueType::tGEOGRAPHY: {
            auto prop = props.find(PROP_GEOGRAPHY_ENCODING);
            if (prop == props.end() || prop->second == "BINARY") {
                encoder = new VarLenEncoder<BinaryGeographyEncoder>();
            } else if (prop->second == "STRING") {
                encoder = new VarLenEncoder<StringGeographyEncoder>();
            } else {
                vassert(false);
            }
            break;
        }
        default:
            throw new SerializableEEException("Unsupported column type");
        }

        vassert(encoder);
        m_encoders.emplace_back(encoder, index, info->allowNull);
    }
}

} }
