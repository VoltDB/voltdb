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

#include "common/MiscUtil.h"
#include "topics/encode/CsvEncoder.h"

namespace voltdb { namespace topics {

const std::string CsvEncoder::PROP_CSV_SEPARATOR = "config.csv.separator";
const std::string CsvEncoder::PROP_CSV_QUOTE = "config.csv.quote";
const std::string CsvEncoder::PROP_CSV_ESCAPE = "config.csv.escape";
const std::string CsvEncoder::PROP_CSV_NULL = "config.csv.null";
const std::string CsvEncoder::PROP_CSV_QUOTE_ALL = "config.csv.quoteAll";

const char CsvEncoder::DEFAULT_CSV_SEPARATOR = ',';
const char CsvEncoder::DEFAULT_CSV_QUOTE = '"';
const char CsvEncoder::DEFAULT_CSV_ESCAPE = '\\';
const std::string CsvEncoder::DEFAULT_CSV_NULL = "\\N";

CsvEncoder::CsvEncoder(const std::vector<int32_t>& indexes,
        const TopicProperties& props)
    : m_indexes(indexes),
      m_separator(parseCharProperty(props, PROP_CSV_SEPARATOR, DEFAULT_CSV_SEPARATOR)),
      m_quote(parseCharProperty(props, PROP_CSV_QUOTE, DEFAULT_CSV_QUOTE)),
      m_escape(parseCharProperty(props, PROP_CSV_ESCAPE, DEFAULT_CSV_ESCAPE)),
      m_null(parseStringProperty(props, PROP_CSV_NULL, DEFAULT_CSV_NULL)),
      m_quoteAll(parseBoolProperty(props, PROP_CSV_QUOTE_ALL, false)),
      m_tuple(nullptr),
      m_oss()
{
    // We must quote any string with separator
    m_quotables = {m_separator, m_quote, '\n', '\r'};
    m_escapables = {m_quote, m_escape};
}

int32_t CsvEncoder::encode(const TableTuple& tuple) {
    m_tuple = &tuple;
    m_oss.str("");

    // Encode tuple values referred to by index vector
    bool first = true;
    for (int32_t index : m_indexes) {
        if (!first) {
            m_oss << m_separator;
        }
        else {
            first = false;
        }

        const NValue value = tuple.getNValue(index);
        if (value.isNull()) {
            if (m_quoteAll) {
                m_oss << m_quote;
            }
            m_oss << m_null;
            if (m_quoteAll) {
                m_oss << m_quote;
            }
            continue;
        }
        std::string strValue = value.toCsvString();

        bool hasEscapes = containsEscapableCharacters(strValue);
        bool mustQuote = m_quoteAll || strValue.find_first_of(m_quotables) != std::string::npos;

        if (mustQuote) {
            m_oss << m_quote;
        }

        if (hasEscapes) {
            insertEscapedValue(strValue);
        }
        else {
            m_oss << strValue;
        }

        if (mustQuote) {
            m_oss << m_quote;
        }
    }
    // Note: Volt CSV encoding does not encode a newline at the end of each record
    return m_oss.str().length();
}

bool CsvEncoder::containsEscapableCharacters(const std::string& value) {
    return value.find_first_of(m_escapables) != std::string::npos;
}

void CsvEncoder::insertEscapedValue(const std::string& value) {
    for (const char c : value) {
        if (m_escapables.find(c) != std::string::npos) {
            m_oss << m_escape;
        }
        m_oss << c;
    }
}

} }
