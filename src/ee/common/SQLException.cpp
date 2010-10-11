/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB Inc.
 *
 * VoltDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * VoltDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */
#include "common/SQLException.h"
#include "common/SerializableEEException.h"
#include "common/serializeio.h"
#include <iostream>
#include <cassert>

using namespace voltdb;

// Please keep these ordered alphabetically.
// Names and codes are standardized.
const char* SQLException::data_exception_division_by_zero = "22012";
const char* SQLException::data_exception_invalid_parameter = "22023";
const char* SQLException::data_exception_most_specific_type_mismatch = "2200G";
const char* SQLException::data_exception_numeric_value_out_of_range = "22003";
const char* SQLException::data_exception_string_data_length_mismatch = "22026";
const char* SQLException::integrity_constraint_violation = "23000";

// These are ordered by error code. Names and codes are volt
// specific - must find merge conflicts on duplicate codes.
const char* SQLException::volt_output_buffer_overflow = "V0001";
const char* SQLException::volt_temp_table_memory_overflow = "V0002";
const char* SQLException::volt_decimal_serialization_error = "V0003";

SQLException::SQLException(const char* sqlState, std::string message) :
    SerializableEEException(VOLT_EE_EXCEPTION_TYPE_SQL, message),
    m_sqlState(sqlState) {
    assert(m_sqlState[5] == '\0');
}

SQLException::SQLException(const char* sqlState, std::string message, VoltEEExceptionType type) :
    SerializableEEException(type, message),
    m_sqlState(sqlState) {}

void SQLException::p_serialize(ReferenceSerializeOutput *output) {
    for (int ii = 0; m_sqlState != NULL && ii < 5; ii++) {
        output->writeByte(m_sqlState[ii]);
    }
}
