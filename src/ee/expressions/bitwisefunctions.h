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

    // We're treating LONG_MIN as a normal bit pattern, so *don't*
    // check for null here.

    int64_t operand = getBigInt();
    return getBigIntValue(~operand);
}

template<> inline NValue NValue::call<FUNC_BITAND>(const std::vector<NValue>& arguments) {
    assert(arguments.size() == 2);
    const NValue& lval = arguments[0];
    const NValue& rval = arguments[1];
    if (lval.getValueType() != VALUE_TYPE_BIGINT || rval.getValueType() != VALUE_TYPE_BIGINT) {
        throw SQLException(SQLException::dynamic_sql_error, "unsupported non-BigInt type for SQL BITAND function");
    }

    int64_t lv = lval.getBigInt();
    int64_t rv = rval.getBigInt();

    int64_t res = lv & rv;
    return getBigIntValue(res);
}


template<> inline NValue NValue::call<FUNC_BITOR>(const std::vector<NValue>& arguments) {
    assert(arguments.size() == 2);
    const NValue& lval = arguments[0];
    const NValue& rval = arguments[1];
    if (lval.getValueType() != VALUE_TYPE_BIGINT || rval.getValueType() != VALUE_TYPE_BIGINT) {
        throw SQLException(SQLException::dynamic_sql_error, "unsupported non-BigInt type for SQL BITAND function");
    }

    int64_t lv = lval.getBigInt();
    int64_t rv = rval.getBigInt();

    int64_t res = lv | rv;
    return getBigIntValue(res);
}


template<> inline NValue NValue::call<FUNC_BITXOR>(const std::vector<NValue>& arguments) {
    assert(arguments.size() == 2);
    const NValue& lval = arguments[0];
    const NValue& rval = arguments[1];
    if (lval.getValueType() != VALUE_TYPE_BIGINT || rval.getValueType() != VALUE_TYPE_BIGINT) {
        throw SQLException(SQLException::dynamic_sql_error, "unsupported non-BigInt type for SQL BITAND function");
    }

    int64_t lv = lval.getBigInt();
    int64_t rv = rval.getBigInt();

    int64_t res = lv ^ rv;
    return getBigIntValue(res);
}


template<> inline NValue NValue::call<FUNC_VOLT_BIT_SHIFT_LEFT>(const std::vector<NValue>& arguments) {
    assert(arguments.size() == 2);
    const NValue& lval = arguments[0];
    int64_t lv = lval.castAsBigIntAndGetValue();

    const NValue& rval = arguments[1];
    int64_t shifts = rval.castAsBigIntAndGetValue();
    if (shifts < 0) {
        throw SQLException(SQLException::dynamic_sql_error, "unsupported negative value for bit shifting");
    }
    if (shifts > 64) {
        return getBigIntValue(0);
    }

    return getBigIntValue(lv << shifts);
}

template<> inline NValue NValue::call<FUNC_VOLT_BIT_SHIFT_RIGHT>(const std::vector<NValue>& arguments) {
    assert(arguments.size() == 2);
    const NValue& lval = arguments[0];
    int64_t lv = lval.castAsBigIntAndGetValue();

    const NValue& rval = arguments[1];
    int64_t shifts = rval.castAsBigIntAndGetValue();
    if (shifts < 0) {
        throw SQLException(SQLException::dynamic_sql_error, "unsupported negative value for bit shifting");
    }
    if (shifts > 64) {
        return getBigIntValue(0);
    }

    return getBigIntValue(lv >> shifts);
}


}
