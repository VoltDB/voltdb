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

template<> inline NValue NValue::callUnary<FUNC_VOLT_BITNOT>() const {

    if (getValueType() != VALUE_TYPE_BIGINT) {
        // The parser should enforce this for us,
        // but just in case...
        throw SQLException(SQLException::dynamic_sql_error, "unsupported non-BigInt type for SQL BITNOT function");
    }

    if (isNull()) {
        return getNullValue(VALUE_TYPE_BIGINT);
    }

    int64_t result = ~(getBigInt());
    if (result == INT64_NULL) {
        throw SQLException(SQLException::data_exception_numeric_value_out_of_range,
                           "Application of bitwise function BITNOT would produce INT64_MIN, "
                           "which is reserved for SQL NULL values.");
    }

    return getBigIntValue(result);
}

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

    if (lval.isNull() || rval.isNull()) {
        return getNullValue(VALUE_TYPE_BIGINT);
    }

    int64_t lv = lval.getBigInt();
    int64_t rv = rval.getBigInt();

    int64_t result = lv ^ rv;
    if (result == INT64_NULL) {
        throw SQLException(SQLException::data_exception_numeric_value_out_of_range,
                "Application of bitwise function BITXOR would produce INT64_MIN, "
                "which is reserved for SQL NULL values.");
    }
    return getBigIntValue(result);
}


template<> inline NValue NValue::call<FUNC_VOLT_BIT_SHIFT_LEFT>(const std::vector<NValue>& arguments) {
    assert(arguments.size() == 2);
    const NValue& lval = arguments[0];
    if (lval.getValueType() != VALUE_TYPE_BIGINT) {
        throw SQLException(SQLException::dynamic_sql_error, "unsupported non-BigInt type for SQL BIT_SHIFT_LEFT function");
    }

    const NValue& rval = arguments[1];

    if (lval.isNull() || rval.isNull()) {
        return getNullValue(VALUE_TYPE_BIGINT);
    }

    int64_t lv = lval.getBigInt();
    int64_t shifts = rval.castAsBigIntAndGetValue();
    if (shifts < 0) {
        throw SQLException(SQLException::data_exception_numeric_value_out_of_range,
                "unsupported negative value for bit shifting");
    }
    // shifting by more than 63 bits is undefined behavior
    if (shifts > 63) {
        return getBigIntValue(0);
    }

    int64_t result = lv << shifts;
    if (result == INT64_NULL) {
        throw SQLException(SQLException::data_exception_numeric_value_out_of_range,
                "Application of bitwise function BIT_SHIFT_LEFT would produce INT64_MIN, "
                "which is reserved for SQL NULL values.");
    }

    return getBigIntValue(result);
}

template<> inline NValue NValue::call<FUNC_VOLT_BIT_SHIFT_RIGHT>(const std::vector<NValue>& arguments) {
    assert(arguments.size() == 2);
    const NValue& lval = arguments[0];
    if (lval.getValueType() != VALUE_TYPE_BIGINT) {
        throw SQLException(SQLException::dynamic_sql_error, "unsupported non-BigInt type for SQL BIT_SHIFT_RIGHT function");
    }

    const NValue& rval = arguments[1];

    if (lval.isNull() || rval.isNull()) {
        return getNullValue(VALUE_TYPE_BIGINT);
    }

    int64_t lv = lval.getBigInt();
    int64_t shifts = rval.castAsBigIntAndGetValue();
    if (shifts < 0) {
        throw SQLException(SQLException::data_exception_numeric_value_out_of_range,
                "unsupported negative value for bit shifting");
    }
    // shifting by more than 63 bits is undefined behavior
    if (shifts > 63) {
        return getBigIntValue(0);
    }

    // right logical shift, padding 0s without taking care of sign bits
    int64_t result = (int64_t)(((uint64_t) lv) >> shifts);
    if (result == INT64_NULL) {
        throw SQLException(SQLException::data_exception_numeric_value_out_of_range,
                "Application of bitwise function BIT_SHIFT_RIGHT would produce INT64_MIN, "
                "which is reserved for SQL NULL values.");
    }

    return getBigIntValue(result);
}


}
