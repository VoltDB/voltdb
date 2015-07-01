/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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
#include "common/SQLException.h"
#include "common/SerializableEEException.h"
#include "common/serializeio.h"
#include <iostream>
#include <cassert>

using namespace voltdb;

// Please keep these ordered alphabetically.
// Names and codes are standardized.
const char* SQLException::data_exception_division_by_zero = "22012";
const char* SQLException::data_exception_invalid_character_value_for_cast = "22018";
const char* SQLException::data_exception_invalid_parameter = "22023";
const char* SQLException::data_exception_most_specific_type_mismatch = "2200G";
const char* SQLException::data_exception_numeric_value_out_of_range = "22003";
const char* SQLException::data_exception_string_data_length_mismatch = "22026";
const char* SQLException::dynamic_sql_error = "07000";
const char* SQLException::integrity_constraint_violation = "23000";

// This is non-standard -- keep it unique.
const char* SQLException::nonspecific_error_code_for_error_forced_by_user = "99999";
const char* SQLException::specific_error_specified_by_user = "Specific error code specified by user invocation of SQL_ERROR";


// These are ordered by error code. Names and codes are volt
// specific - must find merge conflicts on duplicate codes.
const char* SQLException::volt_output_buffer_overflow = "V0001";
const char* SQLException::volt_temp_table_memory_overflow = "V0002";
const char* SQLException::volt_decimal_serialization_error = "V0003";

SQLException::SQLException(std::string sqlState, std::string message) :
    SerializableEEException(VOLT_EE_EXCEPTION_TYPE_SQL, message),
    m_sqlState(sqlState), m_internalFlags(0)
{
    assert(m_sqlState.length() == 5);
}

SQLException::SQLException(std::string sqlState, std::string message, VoltEEExceptionType type) :
    SerializableEEException(type, message),
    m_sqlState(sqlState), m_internalFlags(0)
{
    assert(m_sqlState.length() == 5);
}

SQLException::SQLException(std::string sqlState, std::string message, int internalFlags) :
    SerializableEEException(VOLT_EE_EXCEPTION_TYPE_SQL, message),
    m_sqlState(sqlState), m_internalFlags(internalFlags)
{
    assert(m_sqlState.length() == 5);
}

void SQLException::p_serialize(ReferenceSerializeOutput *output) const {
    const char* sqlState = m_sqlState.c_str();
    for (int ii = 0; ii < 5; ii++) {
        output->writeByte(sqlState[ii]);
    }
}
