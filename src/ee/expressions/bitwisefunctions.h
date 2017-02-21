/* This file is part of VoltDB.
 * Copyright (C) 2008-2017 VoltDB Inc.
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

#include <arpa/inet.h>
/**
 * Given a host order IPv4 or IPv6 address, return the presentation form of
 * the address.  The single argument must have one of two types.
 * <ul>
 *   <li>If the argument has type BIGINT, then the lower 32 bits
 *       of the value are interpreted as an IPv4 internet address in
 *       host byte order.  This address is converted to presentation
 *       format, which is the usual numbers and dots format.</li>
 *   <li>If the argument has type VARBINARY, then the value is a
 *       string of hexadecimal digits whose value is an IP address.
 *       If the argument is 4 bytes long the digit string is an
 *       IPv4 address.  If the argument is 32 bytes long, the digit
 *       string is an IPv6 address.  Note that these lengths are twice
 *       the size of the binary representation, since they are encoded
 *       as hexadecimal digits.</li>
 * </ul>
 */
template<> inline NValue NValue::callUnary<FUNC_INET_NTOA>() const {
    if (getValueType() != VALUE_TYPE_BIGINT &&
        getValueType() != VALUE_TYPE_VARBINARY) {
        // The parser should enforce this for us, but just in case...
        throw SQLException(SQLException::dynamic_sql_error, "unsupported non-BigInt/VarBinary type for SQL INET_NTOA function");
    }

    if (isNull()) {
        return getNullStringValue();
    }
    if(getValueType() == VALUE_TYPE_BIGINT){
        uint32_t v = static_cast<uint32_t> (getBigInt());
        std::stringstream ss;
        ss << ((v&0xff000000)>>24) << "." << ((v&0xff0000)>>16) << "."
           << ((v&0xff00)>>8) << "." << (v&0xff) ;
        std::string res (ss.str());
        return getTempStringValue(res.c_str(), res.length());
    }
    if(getValueType() == VALUE_TYPE_VARBINARY){
        std::string token = toString();
        std::size_t sz = token.length();
        if(sz==8){
            uint32_t v;
            catalog::Catalog::hexDecodeString(token, (char *) &v);
            std::stringstream ss;
            ss << ((v&0xff000000)>>24) << "." << ((v&0xff0000)>>16) << "."
               << ((v&0xff00)>>8) << "." << (v&0xff) ;
            std::string res (ss.str());
            return getTempStringValue(res.c_str(),res.length());
        }else if(sz==32){
            const size_t IPV6BINLEN = 16;
            char ipv6bin[IPV6BINLEN];
            char str[INET6_ADDRSTRLEN];

            catalog::Catalog::hexDecodeString(toString(),ipv6bin);
            inet_ntop(AF_INET6, ipv6bin, str, INET6_ADDRSTRLEN);

            std::string res(str);
            return getTempStringValue(res.c_str(), res.length());
        } else {
            throw SQLException(SQLException::dynamic_sql_error, "SQL INET_NTOA function requires 4 or 16 bytes with VARBINARY");
        }
    }
    return getNullStringValue();
}

/**
 * Given a string representing an IPv4 address, return the
 * address as a BIGINT value in host byte order.  If the string
 * cannot be parsed, throw a SQLException.  Note that this is
 * the inverse of FUNC_MY_INET_NTOA when the argument type is
 * BIGINT.
 */
template<> inline NValue NValue::callUnary<FUNC_INET4_ATON>() const {
    if (getValueType() != VALUE_TYPE_VARCHAR) {
        throw SQLException(SQLException::dynamic_sql_error, "unsupported non-VARCHAR type for SQL INET_ATON4 function");
    }

    std::string token = toString();
    uint32_t addr;
    // Let inet_pton do the validation.
    if(inet_pton(AF_INET, token.c_str(), (void*) &(addr))==1){
        return NValue::getBigIntValue(static_cast<int64_t>(ntohl(addr)));
    }
    throw SQLException(SQLException::dynamic_sql_error, "unrecognized ipv4 address format string");
}

/**
 * Given a string representing an IPv6 address, return the
 * address as a VARBINARY.  The address will be represented as
 * a 128 bit number.  More significant bits will appear before
 * less significant bits in the output.
 */
template<> inline NValue NValue::callUnary<FUNC_INET6_ATON>() const {
    if (getValueType() != VALUE_TYPE_VARCHAR) {
        throw SQLException(SQLException::dynamic_sql_error, "unsupported non-VARCHAR type for SQL INET_ATON6 function");
    }

    std::string token = toString();
    const size_t IPV6BINLEN = 16;
    unsigned char addr[IPV6BINLEN];
    // Let inet_pton do the validation.
    if(inet_pton(AF_INET6, token.c_str(), (void*) addr)==1){
        return NValue::getAllocatedValue(VALUE_TYPE_VARBINARY,
                (const char*) addr, IPV6BINLEN, getTempStringPool());
    }
    throw SQLException(SQLException::dynamic_sql_error, "unrecognized ipv6 address format string");
}

}
