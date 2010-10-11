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

#ifndef SQLEXCEPTION_H_
#define SQLEXCEPTION_H_

#include "common/SerializableEEException.h"

namespace voltdb {
class ReferenceSerializeOutput;

class SQLException : public SerializableEEException {
public:

    // Please keep these ordered alphabetically.
    // Names and codes are standardized.
    static const char* data_exception_division_by_zero;
    static const char* data_exception_invalid_parameter;
    static const char* data_exception_most_specific_type_mismatch;
    static const char* data_exception_numeric_value_out_of_range;
    static const char* data_exception_string_data_length_mismatch;
    static const char* integrity_constraint_violation;

    // These are ordered by error code. Names and codes are volt
    // specific - must find merge conflicts on duplicate codes.
    static const char* volt_output_buffer_overflow;
    static const char* volt_temp_table_memory_overflow;
    static const char* volt_decimal_serialization_error;

    SQLException(const char* sqlState, std::string message);
    SQLException(const char* sqlState, std::string message, VoltEEExceptionType type);
    virtual ~SQLException() {}
protected:
    void p_serialize(ReferenceSerializeOutput *output);
private:
    const char* m_sqlState;
};
}

#endif /* SQLEXCEPTION_H_ */
