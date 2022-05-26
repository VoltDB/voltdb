/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
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

#include "common/SerializableEEException.h"

#define throwSQLException(type, ...) do {                       \
   char msg[8192];                                              \
   snprintf(msg, sizeof msg, __VA_ARGS__);                      \
   msg[sizeof msg - 1] = '\0';                                  \
   throw voltdb::SQLException(type, msg);            \
} while (false)

#define throwDynamicSQLException(...) {                                   \
    char message[8192];                                                   \
    snprintf(message, sizeof message, __VA_ARGS__);                       \
    message[sizeof message - 1] = '\0';                                   \
    throw voltdb::SQLException(SQLException::dynamic_sql_error, message); \
}

namespace voltdb {
class ReferenceSerializeOutput;

class SQLException : public SerializableEEException {
public:

    // Please keep these ordered alphabetically.
    // Names and codes are standardized.
    static const char* data_exception_division_by_zero;
    static const char* data_exception_invalid_character_value_for_cast;
    static const char* data_exception_invalid_parameter;
    static const char* data_exception_most_specific_type_mismatch;
    static const char* data_exception_numeric_value_out_of_range;
    static const char* data_exception_string_data_length_mismatch;
    static const char* dynamic_sql_error;
    static const char* integrity_constraint_violation;

    // This is non-standard -- keep it unique.
    static const char* nonspecific_error_code_for_error_forced_by_user;
    static const char* specific_error_specified_by_user;

    // These are ordered by error code. Names and codes are volt
    // specific - must find merge conflicts on duplicate codes.
    static const char* volt_output_buffer_overflow;
    static const char* volt_temp_table_memory_overflow;
    static const char* volt_decimal_serialization_error;
    static const char* volt_user_defined_function_error;

    SQLException(std::string const& sqlState, std::string const& message);
    SQLException(std::string const& sqlState, int error_no, std::string const& message);
    SQLException(std::string const& sqlState, std::string const& message, VoltEEExceptionType type);
    SQLException(std::string const& sqlState, std::string const& message, int internalFlags);
    virtual ~SQLException() throw() {}

    const std::string& getSqlState() const { return m_sqlState; }

    // internal flags that are not serialized to java
    static const int TYPE_UNDERFLOW             = 1;
    static const int TYPE_OVERFLOW              = 2;
    static const int TYPE_VAR_LENGTH_MISMATCH   = 4;
    int getInternalFlags() const { return m_internalFlags; }

protected:
    void p_serialize(ReferenceSerializeOutput *output) const;
private:
    std::string const m_sqlState;

    // internal and not sent to java
    const int m_internalFlags;
};
}

