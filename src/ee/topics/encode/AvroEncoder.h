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

#include <numeric>
#include <unordered_map>
#include "topics/encode/Encoder.h"

namespace voltdb { namespace topics {

/**
 * A simplified avro encoder for encoding a single NValue from a TableTuple. This encoder supports the value being
 * nullable or not
 */
class AvroValueEncoder : public TupleEncoder {
public:
    AvroValueEncoder() : m_encoder(nullptr), m_index(-1), m_nullable(true) {}

    AvroValueEncoder(NValueEncoder *encoder, int32_t index, bool nullable) :
        m_encoder(encoder), m_index(index), m_nullable(nullable) {}

    AvroValueEncoder(const AvroValueEncoder& other) :
        m_encoder(other.m_encoder.get()), m_index(other.m_index), m_nullable(other.m_nullable) {}

    int32_t sizeOf(const TableTuple& tuple) override {
        const NValue value = tuple.getNValue(m_index);
        int32_t length = m_nullable ? 1 : 0;
        if (!value.isNull()) {
            length += m_encoder->sizeOf(value);
        } else {
            vassert(m_nullable);
        }
        return length;
    }

    /**
     * NOTE: this method expects nulls to be written with index 0 and non-null values written
     * with index 1: this is a convention with the Java schema generation in AvroSerde.java,
     * in order to avoid having to download and interpret schemas in the EE.
     */
    int32_t encode(SerializeOutput& out, const TableTuple& tuple) override {
        const NValue value = tuple.getNValue(m_index);
        int32_t length = 0;
        if (value.isNull()) {
            vassert(m_nullable);
            length = out.writeVarLong(0);
        } else {
            if (m_nullable) {
                length = out.writeVarLong(1);
            }
            length += m_encoder->encode(out, value);
        }
        return length;
    }

private:
    const std::unique_ptr<NValueEncoder> m_encoder;
    const int32_t m_index;
    const bool m_nullable;
};

/**
 * Implementation of TupleEncoder which encodes specific columns from a tuple using the avro serialization format
 */
class AvroEncoder : public TupleEncoder {
public:
    /**
     * @param schemaId which identifies the schema in a registry
     * @param schema of topic stream
     * @param index vector of indexes which are to be serialized
     * @param props map with properties which affect how values are serialized
     */
    AvroEncoder(int32_t schemaId, const TupleSchema& schema, const std::vector<int32_t>& indexes,
            const std::unordered_map<std::string, std::string>& props);

    int32_t sizeOf(const TableTuple& tuple) override {
        return std::accumulate(m_encoders.begin(), m_encoders.end(), s_headerSize,
                [&tuple](int32_t sum, AvroValueEncoder& encoder) { return sum + encoder.sizeOf(tuple); });
    }

    int32_t encode(SerializeOutput& out, const TableTuple& tuple) override {
        int32_t length = s_headerSize;

        // Write out header to indicate which schema from the registry this is using
        out.writeByte(s_magicValue);
        out.writeInt(m_schemaId);

        // Encode all of the selected columns
        for (AvroValueEncoder& encoder : m_encoders) {
            length += encoder.encode(out, tuple);
        }

        return length;
    }

    // Property keys used to configure avro encoding
    static const std::string PROP_TIMESTAMP_ENCODING;
    static const std::string PROP_POINT_ENCODING;
    static const std::string PROP_GEOGRAPHY_ENCODING;

private:
    const int32_t m_schemaId;
    std::vector<AvroValueEncoder> m_encoders;

    static const int8_t s_magicValue = 0;
    static const int32_t s_headerSize = sizeof(s_magicValue) + sizeof(m_schemaId);
};

} }
