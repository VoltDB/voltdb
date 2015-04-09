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

#include "common/NValue.hpp"

namespace voltdb {

template<> inline NValue NValue::call<FUNC_BITAND>(const std::vector<NValue>& arguments) {
    assert(arguments.size() == 2);
    const NValue& lval = arguments[0];
    const NValue& rval = arguments[1];
    if (lval.getValueType() != VALUE_TYPE_BIGINT || rval.getValueType() != VALUE_TYPE_BIGINT) {
        throw SQLException(SQLException::dynamic_sql_error, "unsupported non-BigInt type for SQL BITAND function");
    }

    if (lval.isNull() || rval.isNull()) {
        return getNullValue(VALUE_TYPE_BIGINT);
    }

    int64_t lv = lval.getBigInt();
    int64_t rv = rval.getBigInt();

    int64_t result = lv & rv;
    if (result == INT64_NULL) {
        throw SQLException(SQLException::data_exception_numeric_value_out_of_range,
                "Application of bitwise function BITAND would produce INT64_MIN, "
                "which is reserved for SQL NULL values.");
    }
    return getBigIntValue(result);
}


template<> inline NValue NValue::call<FUNC_BITOR>(const std::vector<NValue>& arguments) {
    assert(arguments.size() == 2);
    const NValue& lval = arguments[0];
    const NValue& rval = arguments[1];
    if (lval.getValueType() != VALUE_TYPE_BIGINT || rval.getValueType() != VALUE_TYPE_BIGINT) {
        throw SQLException(SQLException::dynamic_sql_error, "unsupported non-BigInt type for SQL BITOR function");
    }

    if (lval.isNull() || rval.isNull()) {
        return getNullValue(VALUE_TYPE_BIGINT);
    }

    int64_t lv = lval.getBigInt();
    int64_t rv = rval.getBigInt();

    int64_t result = lv | rv;
    if (result == INT64_NULL) {
        throw SQLException(SQLException::data_exception_numeric_value_out_of_range,
                "Application of bitwise function BITOR would produce INT64_MIN, "
                "which is reserved for SQL NULL values.");
    }
    return getBigIntValue(result);
}


template<> inline NValue NValue::call<FUNC_BITXOR>(const std::vector<NValue>& arguments) {
    assert(arguments.size() == 2);
    const NValue& lval = arguments[0];
    const NValue& rval = arguments[1];
    if (lval.getValueType() != VALUE_TYPE_BIGINT || rval.getValueType() != VALUE_TYPE_BIGINT) {
        throw SQLException(SQLException::dynamic_sql_error, "unsupported non-BigInt type for SQL BITXOR function");
    }

    int64_t lv = lval.getBigInt();
    int64_t rv = rval.getBigInt();

    if (lval.isNull() || rval.isNull()) {
        return getNullValue(VALUE_TYPE_BIGINT);
    }

    int64_t result = lv ^ rv;
    if (result == INT64_NULL) {
        throw SQLException(SQLException::data_exception_numeric_value_out_of_range,
                "Application of bitwise function BITXOR would produce INT64_MIN, "
                "which is reserved for SQL NULL values.");
    }
    return getBigIntValue(result);
}


}
