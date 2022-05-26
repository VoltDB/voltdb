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
#include <arpa/inet.h>
#include <sstream>
#include <string>
#include <limits.h>
#include "common/NValue.hpp"
#include "common/StackTrace.h"

namespace voltdb {

template<> inline NValue NValue::callUnary<FUNC_VOLT_BITNOT>() const {
    if (getValueType() != ValueType::tBIGINT) {
        // The parser should enforce this for us, but just in case...
        throw SQLException(SQLException::dynamic_sql_error, "unsupported non-BigInt type for SQL BITNOT function");
    }

    if (isNull()) {
        return getNullValue(ValueType::tBIGINT);
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
    if (getValueType() != ValueType::tBIGINT) {
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
    if (getValueType() != ValueType::tBIGINT) {
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
    vassert(arguments.size() == 2);
    const NValue& lval = arguments[0];
    const NValue& rval = arguments[1];
    if (lval.getValueType() != ValueType::tBIGINT || rval.getValueType() != ValueType::tBIGINT) {
        throw SQLException(SQLException::dynamic_sql_error, "unsupported non-BigInt type for SQL BITAND function");
    }

    if (lval.isNull() || rval.isNull()) {
        return getNullValue(ValueType::tBIGINT);
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
    vassert(arguments.size() == 2);
    const NValue& lval = arguments[0];
    const NValue& rval = arguments[1];
    if (lval.getValueType() != ValueType::tBIGINT || rval.getValueType() != ValueType::tBIGINT) {
        throw SQLException(SQLException::dynamic_sql_error, "unsupported non-BigInt type for SQL BITOR function");
    }

    if (lval.isNull() || rval.isNull()) {
        return getNullValue(ValueType::tBIGINT);
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
    vassert(arguments.size() == 2);
    const NValue& lval = arguments[0];
    const NValue& rval = arguments[1];
    if (lval.getValueType() != ValueType::tBIGINT || rval.getValueType() != ValueType::tBIGINT) {
        throw SQLException(SQLException::dynamic_sql_error, "unsupported non-BigInt type for SQL BITXOR function");
    }

    if (lval.isNull() || rval.isNull()) {
        return getNullValue(ValueType::tBIGINT);
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
    vassert(arguments.size() == 2);
    const NValue& lval = arguments[0];
    if (lval.getValueType() != ValueType::tBIGINT) {
        throw SQLException(SQLException::dynamic_sql_error, "unsupported non-BigInt type for SQL BIT_SHIFT_LEFT function");
    }

    const NValue& rval = arguments[1];

    if (lval.isNull() || rval.isNull()) {
        return getNullValue(ValueType::tBIGINT);
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
    vassert(arguments.size() == 2);
    const NValue& lval = arguments[0];
    if (lval.getValueType() != ValueType::tBIGINT) {
        throw SQLException(SQLException::dynamic_sql_error, "unsupported non-BigInt type for SQL BIT_SHIFT_RIGHT function");
    }

    const NValue& rval = arguments[1];

    if (lval.isNull() || rval.isNull()) {
        return getNullValue(ValueType::tBIGINT);
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

/**
 * Given a 64-bit BIGINT value, translate the lower 32 bits of this
 * value into the presentation format of an IPv4 address.  The upper
 * 32 bits must be zero.  The lower 32 bits must be in the host byte
 * order.  The translation is done with the library
 * function inet_ntop.
 */
template<> inline NValue NValue::callUnary<FUNC_INET_NTOA>() const {
    if (getValueType() != ValueType::tBIGINT) {
        // The parser should enforce this for us, but just in case...
        throw SQLException(SQLException::dynamic_sql_error, "Unsupported non-BIGINT type for SQL INET_NTOA function");
    }

    if (isNull()) {
        return getNullStringValue();
    }
    // Get the bigint value and mask off the
    // top and bottom half.
    const uint64_t mask32 = (1L << 32)-1;
    uint64_t v = getBigInt();
    struct in_addr addr;
    // We've got a number in our host byte format.
    // We want to store in an address structure so
    // that inet_ntop can work on it.  This needs to be
    // in the network byte order.  So we translate it
    // using htonl.
    addr.s_addr = htonl(v & mask32);
    // Save space for the address string,
    // plus one more for a null terminator.
    char answer[INET_ADDRSTRLEN + 1];
    answer[sizeof(answer)-1] = 0;
    if (inet_ntop(AF_INET, &addr, answer, sizeof(answer)) == 0) {
        throw SQLException(SQLException::dynamic_sql_error, errno, "INET_NTOA Conversion error");
    }
    return getTempStringValue(answer, strlen(answer));
}

/**
 * Given a string containing the presentation format of an IPv4 address, return the
 * address as a BIGINT value.  The address is in the lower 32 bit of
 * the 64 bit BIGINT value, and the upper 32 bits will be zero.  The
 * number will be in the host byte order.  If the string
 * cannot be parsed, throw a SQLException.  Note that this is
 * the inverse of FUNC_INET_NTOA.  The translation is done
 * using inet_pton.  See the manual page for inet_pton(3) for
 * restrictions on the function.
 */
template<> inline NValue NValue::callUnary<FUNC_INET_ATON>() const {
    if (getValueType() != ValueType::tVARCHAR) {
        throw SQLException(SQLException::dynamic_sql_error, "Unsupported non-VARCHAR type for SQL INET_ATON function");
    }
    if (isNull()) {
        return getNullValue(ValueType::tBIGINT);
    }

    in_addr addr;
    int32_t addrlen;
    const char *addr_str = getObject_withoutNull(addrlen);
    if (INET_ADDRSTRLEN < addrlen) {
        std::stringstream sb;
        sb << "Address string for INET_ATON is too long to be an IPv4 address: "
           << addrlen;
        throw SQLException(SQLException::dynamic_sql_error, sb.str().c_str());
    }
    // We have to copy this to null terminate it.
    char addrbuf[INET_ADDRSTRLEN + 1];
    memcpy(addrbuf, addr_str, addrlen);
    addrbuf[addrlen] = 0;
    // Let inet_pton do the validation.
    if (inet_pton(AF_INET, (const char *)addrbuf, (void *) &(addr)) == 0) {
        std::stringstream sb;
        sb << "Unrecognized format for IPv4 address string: <"
           << addrbuf
           << ">";
        throw SQLException(SQLException::dynamic_sql_error, sb.str().c_str());
    }
    return NValue::getBigIntValue(static_cast<int64_t>(ntohl(addr.s_addr)));
}

/**
 * Given a VARBINARY value containing an IPv6 address in network byte order,
 * convert the address to presentation form using inet_ntop.  The input should
 * be a 16 byte varbinary value.  This is the inverse of the function INET6_ATON
 * below.
 */
template<> inline NValue NValue::callUnary<FUNC_INET6_NTOA>() const {
    if (getValueType() != ValueType::tVARBINARY) {
        throw SQLException(SQLException::dynamic_sql_error, "Unsupported non-VARBINARY type for SQL INET6_NTOA function");
    }
    if (isNull()) {
        return getNullValue(ValueType::tVARCHAR);
    }
    int32_t addr_len;
    const in6_addr *addr = (const in6_addr *)getObject_withoutNull(addr_len);
    if (addr_len != sizeof(in6_addr)) {
        std::stringstream sb;
        sb << "VARBINARY value is the wrong size to hold an IPv6 address: "
           << addr_len;
        throw SQLException(SQLException::dynamic_sql_error, sb.str().c_str());
    }
    // Save enough space for the address string plus a
    // trailing null terminator.
    char dest[INET6_ADDRSTRLEN + 1];
    dest[sizeof(dest) - 1] = 0;
    if (inet_ntop(AF_INET6, (const void *)addr, dest, sizeof(dest)) == 0) {
        throw SQLException(SQLException::dynamic_sql_error, errno, "INET6_NTOA Conversion error");
    }
    return getTempStringValue(dest, strlen(dest));
}

/**
 * Given a string representing an IPv6 address, return the
 * address as a VARBINARY.  The address will be represented as
 * host format IPv6 adress.  This is a sequence of 8 bit
 * less significant bits in the output.
 */
template<> inline NValue NValue::callUnary<FUNC_INET6_ATON>() const {
    if (getValueType() != ValueType::tVARCHAR) {
        throw SQLException(SQLException::dynamic_sql_error, "Unsupported non-VARCHAR type for SQL INET6_ATON function");
    } else if (isNull()) {
        return getNullValue(ValueType::tVARBINARY);
    }

    int32_t addrlen;
    const char *addr_str = getObject_withoutNull(addrlen);
    if (INET6_ADDRSTRLEN < addrlen) {
        std::stringstream sb;
        sb << "INET6_ATON: Argument string is too long to be an IPv6 address: " << addrlen;
        throw SQLException(SQLException::dynamic_sql_error, sb.str().c_str());
    } else {
       throwDataExceptionIfInfiniteOrNaN(0, "function LN");
    }
    // Copy the string out to cbuff so we can
    // null terminate it for inet_pton.
    char cbuff[INET6_ADDRSTRLEN + 1];
    memcpy(cbuff, addr_str, addrlen);
    cbuff[addrlen] = 0;
    in6_addr addr;
    // Let inet_pton do the validation.
    if (inet_pton(AF_INET6, cbuff, (void*)&addr) == 0) {
        std::stringstream sb;
        sb << "Unrecognized format for IPv6 address string <" << cbuff << ">";
        throw SQLException(SQLException::dynamic_sql_error, sb.str().c_str());
    }
    return NValue::getAllocatedValue(ValueType::tVARBINARY, (const char*) &addr, sizeof(addr), getTempStringPool());
}

}
