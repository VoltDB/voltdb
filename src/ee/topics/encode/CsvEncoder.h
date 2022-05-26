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
#include <sstream>

#include "topics/encode/Encoder.h"

namespace voltdb { namespace topics {

class CsvEncoder : public TupleEncoder {
public:
    /*
     * Constructor
     *
     * @param index vector of indexes which are to be serialized
     * @param props map with properties which affect how values are serialized
     */
    CsvEncoder(const std::vector<int32_t>& indexes,
            const TopicProperties& props);

    int32_t sizeOf(const TableTuple& tuple) override {
        // Fill cache with encoded Tuple and return length
        return encode(tuple);
    }

    int32_t encode(SerializeOutput& out, const TableTuple& tuple) override {
        if (m_tuple == nullptr || !m_tuple->equals(tuple)) {
            // Cache miss, encode this tuple
            encode(tuple);
        }

        // Extract and write out cached encoding
        std::string cachedString = m_oss.str();
        int32_t len = cachedString.length();
        out.writeBytes(cachedString.c_str(), len);

        // Reset cache and return encoded length
        m_tuple = nullptr;
        m_oss.str("");
        return len;
    }

    // Property keys used to configure csv encoding
    static const std::string PROP_CSV_SEPARATOR;
    static const std::string PROP_CSV_QUOTE;
    static const std::string PROP_CSV_ESCAPE;
    static const std::string PROP_CSV_NULL;
    static const std::string PROP_CSV_QUOTE_ALL;

    static const char DEFAULT_CSV_SEPARATOR;
    static const char DEFAULT_CSV_QUOTE;
    static const char DEFAULT_CSV_ESCAPE;
    static const std::string DEFAULT_CSV_NULL;

private:
    // Cache tuple and its encoded value
    int32_t encode(const TableTuple& tuple);

    bool containsEscapableCharacters(const std::string& value);
    void insertEscapedValue(const std::string& value);

    const std::vector<int32_t> m_indexes;

    const char m_separator;
    const char m_quote;
    const char m_escape;
    const std::string m_null;
    const bool m_quoteAll;

    std::string m_quotables;
    std::string m_escapables;

    // The cache of last encoded Tuple
    const TableTuple* m_tuple;
    std::ostringstream m_oss;
};

} }
