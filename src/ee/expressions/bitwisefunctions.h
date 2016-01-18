/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
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
#include <sstream>
#include <string>
#include <limits.h>
#include "common/NValue.hpp"

namespace voltdb {

template<> inline NValue NValue::callUnary<FUNC_VOLT_BITNOT>() const {
    if (getValueType() != VALUE_TYPE_BIGINT) {
        // The parser should enforce this for us, but just in case...
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

template<> inline NValue NValue::callUnary<FUNC_VOLT_HEX>() const {
    if (getValueType() != VALUE_TYPE_BIGINT) {
        // The parser should enforce this for us, but just in case...
        throw SQLException(SQLException::dynamic_sql_error, "unsupported non-BigInt type for SQL HEX function");
    }

    if (isNull()) {
        return getNullStringValue();
    }
    int64_t inputDecimal = getBigInt();

    std::stringstream ss;
    ss << std::hex << std::uppercase << inputDecimal; // decimal_value
    std::string res (ss.str());
    return getTempStringValue(res.c_str(),res.length());
}

template<> inline NValue NValue::callUnary<FUNC_VOLT_BIN>() const {
    if (getValueType() != VALUE_TYPE_BIGINT) {
        // The parser should enforce this for us, but just in case...
        throw SQLException(SQLException::dynamic_sql_error, "unsupported non-BigInt type for SQL BIN function");
    }

    if (isNull()) {
        return getNullStringValue();
    }
    uint64_t inputDecimal = uint64_t(getBigInt());

    std::stringstream ss;
    const size_t uint64_size = sizeof(inputDecimal)*CHAR_BIT;
    uint64_t mask = 0x1ULL << (uint64_size - 1);
    int idx = int(uint64_size - 1);
    for (;0 <= idx && (inputDecimal & mask) == 0; idx -= 1) {
        mask >>= 1;
    }
    for (; 0 <= idx; idx -= 1) {
        ss << ((inputDecimal & mask) ? '1' : '0');
        mask >>= 1;
    }
    std::string res (ss.str());
    if (res.size() == 0) {
        res = std::string("0");
    }
    return getTempStringValue(res.c_str(),res.length());
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
