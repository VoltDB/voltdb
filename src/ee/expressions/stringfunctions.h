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

#ifndef STRINGFUNCTIONS_H
#define STRINGFUNCTIONS_H

#include <boost/algorithm/string.hpp>
#include <boost/locale.hpp>
#include <boost/scoped_array.hpp>

#include <iostream>
#include <sstream>
#include <string>
#include <cstring>
#include <locale>
#include <iomanip>

namespace voltdb {

/** implement the 1-argument SQL OCTET_LENGTH function */
template<> inline NValue NValue::callUnary<FUNC_OCTET_LENGTH>() const {
    if (isNull())
        return getNullValue();

    return getIntegerValue(getObjectLength_withoutNull());
}

/** implement the 1-argument SQL CHAR function */
template<> inline NValue NValue::callUnary<FUNC_CHAR>() const {
    if (isNull())
        return getNullValue();

    unsigned int point = static_cast<unsigned int>(castAsBigIntAndGetValue());
    std::string utf8 = boost::locale::conv::utf_to_utf<char>(&point, &point + 1);

    return getTempStringValue(utf8.c_str(), utf8.length());
}

/** implement the 1-argument SQL CHAR_LENGTH function */
template<> inline NValue NValue::callUnary<FUNC_CHAR_LENGTH>() const {
    if (isNull())
        return getNullValue();

    char *valueChars = reinterpret_cast<char*>(getObjectValue_withoutNull());
    return getBigIntValue(static_cast<int64_t>(getCharLength(valueChars, getObjectLength_withoutNull())));
}

/** implement the 1-argument SQL SPACE function */
template<> inline NValue NValue::callUnary<FUNC_SPACE>() const {
    if (isNull())
        return getNullStringValue();

    int32_t count = static_cast<int32_t>(castAsBigIntAndGetValue());
    if (count < 0) {
        char msg[1024];
        snprintf(msg, 1024, "data exception: substring error");
        throw SQLException(SQLException::data_exception_string_data_length_mismatch,
            msg);
    }

    std::string spacesStr (count, ' ');
    return getTempStringValue(spacesStr.c_str(),count);
}

template<> inline NValue NValue::callUnary<FUNC_FOLD_LOWER>() const {
    if (isNull())
        return getNullStringValue();

    if (getValueType() != VALUE_TYPE_VARCHAR) {
        throwCastSQLException (getValueType(), VALUE_TYPE_VARCHAR);
    }

    const char* ptr = reinterpret_cast<const char*>(getObjectValue_withoutNull());
    int32_t objectLength = getObjectLength_withoutNull();

    std::string inputStr = std::string(ptr, objectLength);
    boost::algorithm::to_lower(inputStr);

    return getTempStringValue(inputStr.c_str(),objectLength);
}

template<> inline NValue NValue::callUnary<FUNC_FOLD_UPPER>() const {
    if (isNull())
        return getNullStringValue();

    if (getValueType() != VALUE_TYPE_VARCHAR) {
        throwCastSQLException (getValueType(), VALUE_TYPE_VARCHAR);
    }

    const char* ptr = reinterpret_cast<const char*>(getObjectValue_withoutNull());
    int32_t objectLength = getObjectLength_withoutNull();

    std::string inputStr = std::string(ptr, objectLength);
    boost::algorithm::to_upper(inputStr);

    return getTempStringValue(inputStr.c_str(),objectLength);
}

/** implement the 2-argument SQL REPEAT function */
template<> inline NValue NValue::call<FUNC_REPEAT>(const std::vector<NValue>& arguments) {
    assert(arguments.size() == 2);
    const NValue& strValue = arguments[0];
    if (strValue.isNull()) {
        return strValue;
    }
    if (strValue.getValueType() != VALUE_TYPE_VARCHAR) {
        throwCastSQLException (strValue.getValueType(), VALUE_TYPE_VARCHAR);
    }

    const NValue& countArg = arguments[1];
    if (countArg.isNull()) {
        return getNullStringValue();
    }
    int32_t count = static_cast<int32_t>(countArg.castAsBigIntAndGetValue());
    if (count < 0) {
        char msg[1024];
        snprintf(msg, 1024, "data exception: substring error");
        throw SQLException(SQLException::data_exception_string_data_length_mismatch,
                msg);
    }
    if (count == 0) {
        return getTempStringValue("", 0);
    }

    const int32_t valueUTF8Length = strValue.getObjectLength_withoutNull();
    char *repeatChars = reinterpret_cast<char*>(strValue.getObjectValue_withoutNull());

    std::string repeatStr;
    while (count-- > 0)
        repeatStr.append(repeatChars,valueUTF8Length);

    return getTempStringValue(repeatStr.c_str(),repeatStr.length());
}

/** implement the 2-argument SQL FUNC_POSITION_CHAR function */
template<> inline NValue NValue::call<FUNC_POSITION_CHAR>(const std::vector<NValue>& arguments) {
    assert(arguments.size() == 2);
    const NValue& target = arguments[0];
    if (target.isNull()) {
        return getNullValue();
    }
    if (target.getValueType() != VALUE_TYPE_VARCHAR) {
        throwCastSQLException (target.getValueType(), VALUE_TYPE_VARCHAR);
    }
    int32_t lenTarget = target.getObjectLength_withoutNull();

    const NValue& pool = arguments[1];
    if (pool.isNull()) {
        return getNullValue();
    }
    int32_t lenPool = pool.getObjectLength_withoutNull();
    char *targetChars = reinterpret_cast<char*>(target.getObjectValue_withoutNull());
    char *poolChars = reinterpret_cast<char*>(pool.getObjectValue_withoutNull());

    std::string poolStr(poolChars, lenPool);
    std::string targetStr(targetChars, lenTarget);

    size_t position = poolStr.find(targetStr);
    if (position == std::string::npos)
        position = 0;
    else {
        position = NValue::getCharLength(poolStr.substr(0,position).c_str(),position) + 1;
    }
    return getIntegerValue(static_cast<int32_t>(position));
}

/** implement the 2-argument SQL LEFT function */
template<> inline NValue NValue::call<FUNC_LEFT>(const std::vector<NValue>& arguments) {
    assert(arguments.size() == 2);
    const NValue& strValue = arguments[0];
    if (strValue.isNull()) {
        return strValue;
    }
    if (strValue.getValueType() != VALUE_TYPE_VARCHAR) {
        throwCastSQLException (strValue.getValueType(), VALUE_TYPE_VARCHAR);
    }

    const NValue& startArg = arguments[1];
    if (startArg.isNull()) {
        return getNullStringValue();
    }
    int32_t count = static_cast<int32_t>(startArg.castAsBigIntAndGetValue());
    if (count < 0) {
        char msg[1024];
        snprintf(msg, 1024, "data exception: substring error");
        throw SQLException(SQLException::data_exception_string_data_length_mismatch,
            msg);
    }
    if (count == 0) {
        return getTempStringValue("", 0);
    }

    const int32_t valueUTF8Length = strValue.getObjectLength_withoutNull();
    char *valueChars = reinterpret_cast<char*>(strValue.getObjectValue_withoutNull());

    return getTempStringValue(valueChars,(int32_t)(NValue::getIthCharPosition(valueChars,valueUTF8Length,count+1) - valueChars));
}

/** implement the 2-argument SQL RIGHT function */
template<> inline NValue NValue::call<FUNC_RIGHT>(const std::vector<NValue>& arguments) {
    assert(arguments.size() == 2);
    const NValue& strValue = arguments[0];
    if (strValue.isNull()) {
        return strValue;
    }
    if (strValue.getValueType() != VALUE_TYPE_VARCHAR) {
        throwCastSQLException (strValue.getValueType(), VALUE_TYPE_VARCHAR);
    }

    const NValue& startArg = arguments[1];
    if (startArg.isNull()) {
        return getNullStringValue();
    }

    int32_t count = static_cast<int32_t>(startArg.castAsBigIntAndGetValue());
    if (count < 0) {
        char msg[1024];
        snprintf(msg, 1024, "data exception: substring error");
        throw SQLException(SQLException::data_exception_string_data_length_mismatch,
            msg);
    }
    if (count == 0) {
        return getTempStringValue("", 0);
    }

    const int32_t valueUTF8Length = strValue.getObjectLength_withoutNull();
    char *valueChars = reinterpret_cast<char*>(strValue.getObjectValue_withoutNull());
    const char *valueEnd = valueChars+valueUTF8Length;
    int32_t charLen = getCharLength(valueChars,valueUTF8Length);
    if (count >= charLen)
        return getTempStringValue(valueChars,(int32_t)(valueEnd - valueChars));

    const char* newStartChar = NValue::getIthCharPosition(valueChars,valueUTF8Length,charLen-count+1);
    return getTempStringValue(newStartChar,(int32_t)(valueEnd - newStartChar));
}

/** implement the 2-or-more-argument SQL CONCAT function */
template<> inline NValue NValue::call<FUNC_CONCAT>(const std::vector<NValue>& arguments) {
    assert(arguments.size() >= 2);
    int64_t size = 0;
    for(std::vector<NValue>::const_iterator iter = arguments.begin(); iter !=arguments.end(); iter++) {
        if (iter->isNull()) {
            return getNullStringValue();
        }
        if (iter->getValueType() != VALUE_TYPE_VARCHAR) {
            throwCastSQLException (iter->getValueType(), VALUE_TYPE_VARCHAR);
        }
        size += (int64_t) iter->getObjectLength_withoutNull();
        if (size > (int64_t)INT32_MAX) {
            throw SQLException(SQLException::data_exception_numeric_value_out_of_range,
                               "The result of CONCAT function is out of range");
        }
    }

    if (size == 0) {
        return getNullStringValue();
    }

    size_t cur = 0;
    char *buffer = new char[size];
    boost::scoped_array<char> smart(buffer);
    for(std::vector<NValue>::const_iterator iter = arguments.begin(); iter !=arguments.end(); iter++) {
        size_t cur_size = iter->getObjectLength_withoutNull();
        char *next = reinterpret_cast<char*>(iter->getObjectValue_withoutNull());
        memcpy((void *)(buffer + cur), (void *)next, cur_size);
        cur += cur_size;
    }

    return getTempStringValue(buffer, cur);
}

/** implement the 2-argument SQL SUBSTRING function */
template<> inline NValue NValue::call<FUNC_VOLT_SUBSTRING_CHAR_FROM>(const std::vector<NValue>& arguments) {
    assert(arguments.size() == 2);
    const NValue& strValue = arguments[0];
    if (strValue.isNull()) {
        return strValue;
    }
    if (strValue.getValueType() != VALUE_TYPE_VARCHAR) {
        throwCastSQLException (strValue.getValueType(), VALUE_TYPE_VARCHAR);
    }

    const NValue& startArg = arguments[1];
    if (startArg.isNull()) {
        return getNullStringValue();
    }

    const int32_t valueUTF8Length = strValue.getObjectLength_withoutNull();
    char *valueChars = reinterpret_cast<char*>(strValue.getObjectValue_withoutNull());
    const char *valueEnd = valueChars+valueUTF8Length;

    int64_t start = std::max(startArg.castAsBigIntAndGetValue(), static_cast<int64_t>(1L));

    UTF8Iterator iter(valueChars, valueEnd);
    const char* startChar = iter.skipCodePoints(start-1);
    return getTempStringValue(startChar, (int32_t)(valueEnd - startChar));
}

static inline std::string trim_function(std::string source, const std::string match,
        bool doltrim, bool dortrim) {
    // Assuming SOURCE string and MATCH string are both valid UTF-8 strings
    size_t mlen = match.length();
    assert (mlen > 0);
    if (doltrim) {
        while (boost::starts_with(source, match)) {
            source.erase(0, mlen);
        }
    }
    if (dortrim) {
        while (boost::ends_with(source, match)) {
            source.erase(source.length() - mlen, mlen);
        }
    }

    return source;
}

/** implement the 3-argument SQL TRIM function */
template<> inline NValue NValue::call<FUNC_TRIM_CHAR>(const std::vector<NValue>& arguments) {
    assert(arguments.size() == 3);

    for (int i = 0; i < arguments.size(); i++) {
        const NValue& arg = arguments[i];
        if (arg.isNull()) {
            return getNullStringValue();
        }
    }

    const NValue& opt = arguments[0];
    int32_t optArg = static_cast<int32_t>(opt.castAsBigIntAndGetValue());

    char* ptr;
    const NValue& trimChar = arguments[1];
    if (trimChar.getValueType() != VALUE_TYPE_VARCHAR) {
        throwCastSQLException (trimChar.getValueType(), VALUE_TYPE_VARCHAR);
    }

    ptr = reinterpret_cast<char*>(trimChar.getObjectValue_withoutNull());
    int32_t length = trimChar.getObjectLength_withoutNull();

    std::string trimArg = std::string(ptr, length);

    const NValue& strVal = arguments[2];
    if (strVal.getValueType() != VALUE_TYPE_VARCHAR) {
        throwCastSQLException (trimChar.getValueType(), VALUE_TYPE_VARCHAR);
    }

    ptr = reinterpret_cast<char*>(strVal.getObjectValue_withoutNull());
    int32_t objectLength = strVal.getObjectLength_withoutNull();
    std::string inputStr = std::string(ptr, objectLength);

    // SQL03 standard only allows 1 character trim character.
    // In order to be compatible with other popular databases like MySQL,
    // our implementation also allows multiple characters, but rejects 0 character.
    if (length == 0) {
        throw SQLException( SQLException::data_exception_numeric_value_out_of_range,
                "data exception -- trim error, invalid length argument 0");
    }

    std::string result = "";
    switch (optArg) {
    case SQL_TRIM_BOTH:
        result = trim_function(inputStr, trimArg, true, true);
        break;
    case SQL_TRIM_LEADING:
        result = trim_function(inputStr, trimArg, true, false);
        break;
    case SQL_TRIM_TRAILING:
        result = trim_function(inputStr, trimArg, false, true);
        break;
    default:
        throw SQLException(SQLException::dynamic_sql_error, "unsupported SQL TRIM exception");
    }

    return getTempStringValue(result.c_str(), result.length());
}

/** implement the 3-argument SQL REPLACE function */
template<> inline NValue NValue::call<FUNC_REPLACE>(const std::vector<NValue>& arguments) {
    assert(arguments.size() == 3);

    for (int i = 0; i < arguments.size(); i++) {
        const NValue& arg = arguments[i];
        if (arg.isNull()) {
            return getNullStringValue();
        }
    }

    char* ptr;
    const NValue& str0 = arguments[0];
    if (str0.getValueType() != VALUE_TYPE_VARCHAR) {
        throwCastSQLException (str0.getValueType(), VALUE_TYPE_VARCHAR);
    }
    ptr = reinterpret_cast<char*>(str0.getObjectValue_withoutNull());
    int32_t length = str0.getObjectLength_withoutNull();
    std::string targetStr = std::string(ptr, length);

    const NValue& str1 = arguments[1];
    if (str1.getValueType() != VALUE_TYPE_VARCHAR) {
        throwCastSQLException (str1.getValueType(), VALUE_TYPE_VARCHAR);
    }
    ptr = reinterpret_cast<char*>(str1.getObjectValue_withoutNull());
    std::string matchStr = std::string(ptr, str1.getObjectLength_withoutNull());

    const NValue& str2 = arguments[2];

    if (str2.getValueType() != VALUE_TYPE_VARCHAR) {
        throwCastSQLException (str2.getValueType(), VALUE_TYPE_VARCHAR);
    }
    ptr = reinterpret_cast<char*>(str2.getObjectValue_withoutNull());
    std::string replaceStr = std::string(ptr, str2.getObjectLength_withoutNull());

    boost::algorithm::replace_all(targetStr, matchStr, replaceStr);
    return getTempStringValue(targetStr.c_str(), targetStr.length());
}

/** implement the 3-argument SQL SUBSTRING function */
template<> inline NValue NValue::call<FUNC_SUBSTRING_CHAR>(const std::vector<NValue>& arguments) {
    assert(arguments.size() == 3);
    const NValue& strValue = arguments[0];
    if (strValue.isNull()) {
        return strValue;
    }
    if (strValue.getValueType() != VALUE_TYPE_VARCHAR) {
        throwCastSQLException (strValue.getValueType(), VALUE_TYPE_VARCHAR);
    }

    const NValue& startArg = arguments[1];
    if (startArg.isNull()) {
        return getNullStringValue();
    }
    const NValue& lengthArg = arguments[2];
    if (lengthArg.isNull()) {
        return getNullStringValue();
    }
    const int32_t valueUTF8Length = strValue.getObjectLength_withoutNull();
    const char *valueChars = reinterpret_cast<char*>(strValue.getObjectValue_withoutNull());
    const char *valueEnd = valueChars+valueUTF8Length;
    int64_t start = startArg.castAsBigIntAndGetValue();
    int64_t length = lengthArg.castAsBigIntAndGetValue();
    if (length < 0) {
        char message[128];
        snprintf(message, 128, "data exception -- substring error, negative length argument %ld", (long)length);
        throw SQLException(SQLException::data_exception_numeric_value_out_of_range, message);
    }
    if (start < 1) {
        // According to the standard, START < 1 effectively moves the end point based on (LENGTH + START)
        // to the left while fixing the start point at 1.
        length += (start - 1); // This moves endChar in.
        start = 1;
        if (length < 0) {
            // The standard considers this a 0-length result -- not a substring error.
            length = 0;
        }
    }
    UTF8Iterator iter(valueChars, valueEnd);
    const char* startChar = iter.skipCodePoints(start-1);
    const char* endChar = iter.skipCodePoints(length);
    return getTempStringValue(startChar, endChar - startChar);
}

static inline std::string overlay_function(const char* ptrSource, size_t lengthSource,
        std::string insertStr, size_t start, size_t length) {
    int32_t i = NValue::getIthCharIndex(ptrSource, lengthSource, start);
    std::string result = std::string(ptrSource, i);
    result.append(insertStr);

    int32_t j = i;
    if (length > 0) {
        // the end the last character may be multiple byte character, get to the next character index
        j += NValue::getIthCharIndex(&ptrSource[i], lengthSource-i, length+1);
    }
    result.append(std::string(&ptrSource[j], lengthSource - j));

    return result;
}

/** implement the 3 or 4 argument SQL OVERLAY function */
template<> inline NValue NValue::call<FUNC_OVERLAY_CHAR>(const std::vector<NValue>& arguments) {
    assert(arguments.size() == 3 || arguments.size() == 4);

    for (int i = 0; i < arguments.size(); i++) {
        const NValue& arg = arguments[i];
        if (arg.isNull()) {
            return getNullStringValue();
        }
    }

    const NValue& str0 = arguments[0];
    if (str0.getValueType() != VALUE_TYPE_VARCHAR) {
        throwCastSQLException (str0.getValueType(), VALUE_TYPE_VARCHAR);
    }
    const char* ptrSource = reinterpret_cast<const char*>(str0.getObjectValue_withoutNull());
    size_t lengthSource = str0.getObjectLength_withoutNull();

    const NValue& str1 = arguments[1];
    if (str1.getValueType() != VALUE_TYPE_VARCHAR) {
        throwCastSQLException (str1.getValueType(), VALUE_TYPE_VARCHAR);
    }
    const char* ptrInsert = reinterpret_cast<const char*>(str1.getObjectValue_withoutNull());
    size_t lengthInsert = str1.getObjectLength_withoutNull();
    std::string insertStr = std::string(ptrInsert, lengthInsert);

    const NValue& startArg = arguments[2];

    int64_t start = startArg.castAsBigIntAndGetValue();
    if (start <= 0) {
        char message[128];
        snprintf(message, 128, "data exception -- OVERLAY error, not positive start argument %ld",(long)start);
        throw SQLException( SQLException::data_exception_numeric_value_out_of_range, message);
    }

    int64_t length = 0;
    if (arguments.size() == 4) {
        const NValue& lengthArg = arguments[3];
        length = lengthArg.castAsBigIntAndGetValue();
        if (length < 0) {
            char message[128];
            snprintf(message, 128, "data exception -- OVERLAY error, negative length argument %ld",(long)length);
            throw SQLException( SQLException::data_exception_numeric_value_out_of_range, message);
        }
    } else {
        // By default without length argument
        length = getCharLength(ptrInsert, lengthInsert);
    }

    assert(start >= 1);
    std::string resultStr = overlay_function(ptrSource, lengthSource, insertStr, start, length);

    return getTempStringValue(resultStr.c_str(), resultStr.length());
}

/** the facet used to group three digits */
struct money_numpunct : std::numpunct<char> {
    std::string do_grouping() const {return "\03";}
};

/** implement the Volt SQL Format_Currency function for decimal values */
template<> inline NValue NValue::call<FUNC_VOLT_FORMAT_CURRENCY>(const std::vector<NValue>& arguments) {
    static std::locale newloc(std::cout.getloc(), new money_numpunct);
    static std::locale nullloc(std::cout.getloc(), new std::numpunct<char>);
    static TTInt one("1");
    static TTInt five("5");

    assert(arguments.size() == 2);
    const NValue &arg1 = arguments[0];
    if (arg1.isNull()) {
        return getNullStringValue();
    }
    const ValueType type = arg1.getValueType();
    if (type != VALUE_TYPE_DECIMAL) {
        throwCastSQLException (type, VALUE_TYPE_DECIMAL);
    }

    std::ostringstream out;
    out.imbue(newloc);
    TTInt scaledValue = arg1.castAsDecimalAndGetValue();

    if (scaledValue.IsSign()) {
        out << '-';
        scaledValue.ChangeSign();
    }

    // rounding
    const NValue &arg2 = arguments[1];
    int32_t places = arg2.castAsIntegerAndGetValue();
    if (places >= 12 || places <= -26) {
        throw SQLException(SQLException::data_exception_numeric_value_out_of_range,
            "the second parameter should be < 12 and > -26");
    }

    TTInt ten(10);
    if (places <= 0) {
        ten.Pow(-places);
    }
    else {
        ten.Pow(places);
    }
    TTInt denominator = (places <= 0) ? (TTInt(kMaxScaleFactor) * ten):
                                        (TTInt(kMaxScaleFactor) / ten);
    TTInt fractional(scaledValue);
    fractional %= denominator;
    TTInt barrier = five * (denominator / 10);

    if (fractional > barrier) {
        scaledValue += denominator;
    }
    else if (fractional == barrier) {
        TTInt prev = scaledValue / denominator;
        if (prev % 2 == one) {
            scaledValue += denominator;
        }
    }
    else {
        // do nothing here
    }

    if (places <= 0) {
        scaledValue -= fractional;
        int64_t whole = narrowDecimalToBigInt(scaledValue);
        out << std::fixed << whole;
    }
    else {
        int64_t whole = narrowDecimalToBigInt(scaledValue);
        int64_t fraction = getFractionalPart(scaledValue);
        // here denominator is guarateed to be able to converted to int64_t
        fraction /= denominator.ToInt();
        out << std::fixed << whole;
        // fractional part does not need groups
        out.imbue(nullloc);
        out << '.' << std::setfill('0') << std::setw(places) << fraction;
    }
    // TODO: Although there should be only one copy of newloc (and money_numpunct),
    // we still need to test and make sure no memory leakage in this piece of code.
    std::string rv = out.str();
    return getTempStringValue(rv.c_str(), rv.length());
}

}

#endif /* STRINGFUNCTIONS_H */
