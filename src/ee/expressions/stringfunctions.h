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

#pragma once

#include "common/ThreadLocalPool.h" // for POOLED_MAX_VALUE_LENGTH

#include <boost/algorithm/string.hpp>
#include <boost/locale.hpp>
#include <boost/scoped_array.hpp>

#define PCRE2_CODE_UNIT_WIDTH 8
#include <string.h>
#include <boost/shared_ptr.hpp>
#include <pcre2.h>

#include <iostream>
#include <sstream>
#include <string>
#include <cstring>
#include <locale>
#include <iomanip>

namespace voltdb {

/** implement the 1-argument SQL OCTET_LENGTH function */
template<> inline NValue NValue::callUnary<FUNC_OCTET_LENGTH>() const {
    if (isNull()) {
        return getNullValue(ValueType::tINTEGER);
    }
    int32_t length;
    getObject_withoutNull(length);
    return getIntegerValue(length);
}

/** implement the 1-argument SQL CHAR function */
template<> inline NValue NValue::callUnary<FUNC_CHAR>() const {
    if (isNull()) {
        return getNullStringValue();
    }

    unsigned int point = static_cast<unsigned int>(castAsBigIntAndGetValue());
    std::string utf8 = boost::locale::conv::utf_to_utf<char>(&point, &point + 1);

    return getTempStringValue(utf8.c_str(), utf8.length());
}

/** implement the 1-argument SQL CHAR_LENGTH function */
template<> inline NValue NValue::callUnary<FUNC_CHAR_LENGTH>() const {
    if (isNull()) {
        return getNullValue(ValueType::tBIGINT);
    }

    int32_t lenValue;
    const char* valueChars = getObject_withoutNull(lenValue);
    return getBigIntValue(static_cast<int64_t>(getCharLength(valueChars, lenValue)));
}

/** implement the 1-argument SQL SPACE function */
template<> inline NValue NValue::callUnary<FUNC_SPACE>() const {
    if (isNull()) {
        return getNullStringValue();
    }

    int32_t count = static_cast<int32_t>(castAsBigIntAndGetValue());
    if (count < 0) {
        throw SQLException(SQLException::data_exception_string_data_length_mismatch,
                           "The argument to the SPACE function is negative");
    } else if (ThreadLocalPool::POOLED_MAX_VALUE_LENGTH < count) {
        std::ostringstream oss;
        oss << "The argument to the SPACE function is larger than the maximum size allowed for strings ("
            << ThreadLocalPool::POOLED_MAX_VALUE_LENGTH << " bytes). "
            << "Reduce either the string size or repetition count.";
        throw SQLException(SQLException::data_exception_string_data_length_mismatch,
                           oss.str().c_str());
    }

    std::string spacesStr(count, ' ');
    return getTempStringValue(spacesStr.c_str(),count);
}

template<> inline NValue NValue::callUnary<FUNC_FOLD_LOWER>() const {
    if (isNull()) {
        return getNullStringValue();
    }

    if (getValueType() != ValueType::tVARCHAR) {
        throwCastSQLException (getValueType(), ValueType::tVARCHAR);
    }

    int32_t length;
    const char* buf = getObject_withoutNull(length);
    std::string inputStr(buf, length);
    boost::algorithm::to_lower(inputStr);

    return getTempStringValue(inputStr.c_str(), length);
}

template<> inline NValue NValue::callUnary<FUNC_FOLD_UPPER>() const {
    if (isNull())
        return getNullStringValue();

    if (getValueType() != ValueType::tVARCHAR) {
        throwCastSQLException (getValueType(), ValueType::tVARCHAR);
    }

    int32_t length;
    const char* buf = getObject_withoutNull(length);
    std::string inputStr(buf, length);
    boost::algorithm::to_upper(inputStr);

    return getTempStringValue(inputStr.c_str(), length);
}

/** implement the 2-argument SQL REPEAT function */
template<> inline NValue NValue::call<FUNC_REPEAT>(const std::vector<NValue>& arguments) {
    vassert(arguments.size() == 2);
    const NValue& strValue = arguments[0];
    if (strValue.isNull()) {
        return strValue;
    }
    if (strValue.getValueType() != ValueType::tVARCHAR) {
        throwCastSQLException (strValue.getValueType(), ValueType::tVARCHAR);
    }

    const NValue& countArg = arguments[1];
    if (countArg.isNull()) {
        return getNullStringValue();
    }
    int64_t count = countArg.castAsBigIntAndGetValue();
    if (count < 0) {
        throwSQLException(SQLException::data_exception_string_data_length_mismatch,
                "data exception: substring error");
    }
    if (count == 0) {
        return getTempStringValue("", 0);
    }

    int32_t argLength32;
    const char* buf = strValue.getObject_withoutNull(argLength32);
    int64_t argLength = static_cast<int64_t>(argLength32);

    bool overflowed = false;
    int64_t outputLength = NValue::multiplyAndCheckOverflow(count, argLength, &overflowed);
    if (overflowed || outputLength > ThreadLocalPool::POOLED_MAX_VALUE_LENGTH) {
        std::ostringstream oss;
        oss << "The result of the REPEAT function is larger than the maximum size allowed for strings ("
            << ThreadLocalPool::POOLED_MAX_VALUE_LENGTH << " bytes). "
            << "Reduce either the string size or repetition count.";
        throw SQLException(SQLException::data_exception_string_data_length_mismatch,
                           oss.str().c_str());
    }

    std::string repeatStr;
    if (argLength > 0) {
        while (count-- > 0) {
            repeatStr.append(buf, argLength);
        }
    }

    return getTempStringValue(repeatStr.c_str(), repeatStr.length());
}

/** implement the 2-argument SQL FUNC_POSITION_CHAR function */
template<> inline NValue NValue::call<FUNC_POSITION_CHAR>(const std::vector<NValue>& arguments) {
    vassert(arguments.size() == 2);
    const NValue& target = arguments[0];
    if (target.isNull()) {
        return getNullValue(ValueType::tINTEGER);
    }
    if (target.getValueType() != ValueType::tVARCHAR) {
        throwCastSQLException (target.getValueType(), ValueType::tVARCHAR);
    }
    const NValue& pool = arguments[1];
    if (pool.isNull()) {
        return getNullValue(ValueType::tINTEGER);
    }
    int32_t lenTarget;
    const char* targetChars = target.getObject_withoutNull(lenTarget);

    int32_t lenPool;
    const char* poolChars = pool.getObject_withoutNull(lenPool);
    std::string poolStr(poolChars, lenPool);

    size_t position = poolStr.find(targetChars, 0, lenTarget);
    if (position == std::string::npos)
        position = 0;
    else {
        position = NValue::getCharLength(poolStr.substr(0,position).c_str(),position) + 1;
    }
    return getIntegerValue(static_cast<int32_t>(position));
}

/** implement the 2-argument SQL LEFT function */
template<> inline NValue NValue::call<FUNC_LEFT>(const std::vector<NValue>& arguments) {
    vassert(arguments.size() == 2);
    const NValue& strValue = arguments[0];
    if (strValue.isNull()) {
        return strValue;
    }
    if (strValue.getValueType() != ValueType::tVARCHAR) {
        throwCastSQLException (strValue.getValueType(), ValueType::tVARCHAR);
    }

    const NValue& startArg = arguments[1];
    if (startArg.isNull()) {
        return getNullStringValue();
    }
    int32_t count = static_cast<int32_t>(startArg.castAsBigIntAndGetValue());
    if (count < 0) {
        throw SQLException(SQLException::data_exception_string_data_length_mismatch,
                           "The argument to the LEFT function is negative");
    }
    if (count == 0) {
        return getTempStringValue("", 0);
    }

    int32_t length;
    const char* buf = strValue.getObject_withoutNull(length);

    return getTempStringValue(buf, (int32_t)(getIthCharPosition(buf, length, count+1) - buf));
}

/** implement the 2-argument SQL RIGHT function */
template<> inline NValue NValue::call<FUNC_RIGHT>(const std::vector<NValue>& arguments) {
    vassert(arguments.size() == 2);
    const NValue& strValue = arguments[0];
    if (strValue.isNull()) {
        return strValue;
    }
    if (strValue.getValueType() != ValueType::tVARCHAR) {
        throwCastSQLException (strValue.getValueType(), ValueType::tVARCHAR);
    }

    const NValue& startArg = arguments[1];
    if (startArg.isNull()) {
        return getNullStringValue();
    }

    int32_t count = static_cast<int32_t>(startArg.castAsBigIntAndGetValue());
    if (count < 0) {
        throw SQLException(SQLException::data_exception_string_data_length_mismatch,
                           "The argument to the RIGHT function is negative.");
    }
    if (count == 0) {
        return getTempStringValue("", 0);
    }

    int32_t length;
    const char* buf = strValue.getObject_withoutNull(length);
    const char *valueEnd = buf + length;
    int32_t charLen = getCharLength(buf, length);
    if (count >= charLen) {
        return getTempStringValue(buf, (int32_t)(valueEnd - buf));
    }
    const char* newStartChar = getIthCharPosition(buf, length, charLen-count+1);
    return getTempStringValue(newStartChar, (int32_t)(valueEnd - newStartChar));
}

/** implement the 2-or-more-argument SQL CONCAT function */
template<> inline NValue NValue::call<FUNC_CONCAT>(const std::vector<NValue>& arguments) {
    vassert(arguments.size() >= 2);
    int64_t size = 0;
    for (std::vector<NValue>::const_iterator iter = arguments.begin();
        iter !=arguments.end(); iter++) {
        if (iter->isNull()) {
            return getNullStringValue();
        }
        if (iter->getValueType() != ValueType::tVARCHAR) {
            throwCastSQLException (iter->getValueType(), ValueType::tVARCHAR);
        }
        int32_t length;
        iter->getObject_withoutNull(length);
        size += length;
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
    for (std::vector<NValue>::const_iterator iter = arguments.begin();
        iter !=arguments.end(); iter++) {
        int32_t length;
        const char* next = iter->getObject_withoutNull(length);
        memcpy(buffer + cur, next, length);
        cur += length;
    }

    return getTempStringValue(buffer, cur);
}

/** implement the 2-argument SQL SUBSTRING function */
template<> inline NValue NValue::call<FUNC_VOLT_SUBSTRING_CHAR_FROM>(const std::vector<NValue>& arguments) {
    vassert(arguments.size() == 2);
    const NValue& strValue = arguments[0];
    if (strValue.isNull()) {
        return strValue;
    }
    if (strValue.getValueType() != ValueType::tVARCHAR) {
        throwCastSQLException (strValue.getValueType(), ValueType::tVARCHAR);
    }

    const NValue& startArg = arguments[1];
    if (startArg.isNull()) {
        return getNullStringValue();
    }

    int32_t lenValue;
    const char* valueChars = strValue.getObject_withoutNull(lenValue);
    const char *valueEnd = valueChars + lenValue;

    int64_t start = std::max(startArg.castAsBigIntAndGetValue(), static_cast<int64_t>(1L));

    UTF8Iterator iter(valueChars, valueEnd);
    const char* startChar = iter.skipCodePoints(start-1);
    return getTempStringValue(startChar, (int32_t)(valueEnd - startChar));
}

static inline std::string trim_function(std::string source, const std::string& match,
        bool doltrim, bool dortrim) {
    // Assuming SOURCE string and MATCH string are both valid UTF-8 strings
    size_t mlen = match.length();
    vassert(mlen > 0);
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


/** implement the 2-argument SQL TRIM functions */
inline NValue NValue::trimWithOptions(const std::vector<NValue>& arguments, bool leading, bool trailing) {
    vassert(arguments.size() == 2);

    for (int i = 0; i < arguments.size(); i++) {
        const NValue& arg = arguments[i];
        if (arg.isNull()) {
            return getNullStringValue();
        }
    }

    const NValue& trimChar = arguments[0];
    if (trimChar.getValueType() != ValueType::tVARCHAR) {
        throwCastSQLException (trimChar.getValueType(), ValueType::tVARCHAR);
    }

    int32_t length;
    const char* buf = trimChar.getObject_withoutNull(length);
    // SQL03 standard only allows a 1-character trim character.
    // In order to be compatible with other popular databases like MySQL,
    // our implementation also allows multiple characters, but rejects 0 characters.
    if (length == 0) {
        throw SQLException(SQLException::data_exception_numeric_value_out_of_range,
                "data exception -- trim error, invalid trim character length 0");
    }

    std::string trimArg(buf, length);

    const NValue& strVal = arguments[1];
    if (strVal.getValueType() != ValueType::tVARCHAR) {
        throwCastSQLException (trimChar.getValueType(), ValueType::tVARCHAR);
    }

    buf = strVal.getObject_withoutNull(length);
    std::string inputStr(buf, length);

    std::string result = trim_function(inputStr, trimArg, leading, trailing);
    return getTempStringValue(result.c_str(), result.length());
}

template<> inline NValue NValue::call<FUNC_TRIM_BOTH_CHAR>(const std::vector<NValue>& arguments) {
    return trimWithOptions(arguments, true, true);
}

template<> inline NValue NValue::call<FUNC_TRIM_LEADING_CHAR>(const std::vector<NValue>& arguments) {
    return trimWithOptions(arguments, true, false);
}

template<> inline NValue NValue::call<FUNC_TRIM_TRAILING_CHAR>(const std::vector<NValue>& arguments) {
    return trimWithOptions(arguments, false, true);
}


/** implement the 3-argument SQL REPLACE function */
template<> inline NValue NValue::call<FUNC_REPLACE>(const std::vector<NValue>& arguments) {
    vassert(arguments.size() == 3);

    for (int i = 0; i < arguments.size(); i++) {
        const NValue& arg = arguments[i];
        if (arg.isNull()) {
            return getNullStringValue();
        }
    }

    const NValue& str0 = arguments[0];
    if (str0.getValueType() != ValueType::tVARCHAR) {
        throwCastSQLException (str0.getValueType(), ValueType::tVARCHAR);
    }
    int32_t length;
    const char* buf = str0.getObject_withoutNull(length);
    std::string targetStr(buf, length);

    const NValue& str1 = arguments[1];
    if (str1.getValueType() != ValueType::tVARCHAR) {
        throwCastSQLException (str1.getValueType(), ValueType::tVARCHAR);
    }
    buf = str1.getObject_withoutNull(length);
    std::string matchStr(buf, length);

    const NValue& str2 = arguments[2];

    if (str2.getValueType() != ValueType::tVARCHAR) {
        throwCastSQLException (str2.getValueType(), ValueType::tVARCHAR);
    }
    buf = str2.getObject_withoutNull(length);
    std::string replaceStr(buf, length);

    boost::algorithm::replace_all(targetStr, matchStr, replaceStr);
    return getTempStringValue(targetStr.c_str(), targetStr.length());
}

/** implement the 3-argument SQL SUBSTRING function */
template<> inline NValue NValue::call<FUNC_SUBSTRING_CHAR>(const std::vector<NValue>& arguments) {
    vassert(arguments.size() == 3);
    const NValue& strValue = arguments[0];
    if (strValue.isNull()) {
        return strValue;
    }
    if (strValue.getValueType() != ValueType::tVARCHAR) {
        throwCastSQLException (strValue.getValueType(), ValueType::tVARCHAR);
    }

    const NValue& startArg = arguments[1];
    if (startArg.isNull()) {
        return getNullStringValue();
    }
    const NValue& lengthArg = arguments[2];
    if (lengthArg.isNull()) {
        return getNullStringValue();
    }
    int32_t lenValue;
    const char* valueChars = strValue.getObject_withoutNull(lenValue);
    const char *valueEnd = valueChars + lenValue;
    int64_t start = startArg.castAsBigIntAndGetValue();
    int64_t length = lengthArg.castAsBigIntAndGetValue();
    if (length < 0) {
        throwSQLException(SQLException::data_exception_numeric_value_out_of_range,
                "data exception -- substring error, negative length argument %ld",
                static_cast<long>(length));
    }
    if (start < 1) {
        // According to the standard, START < 1 effectively
        // moves the end point based on (LENGTH + START)
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
    std::string result(ptrSource, i);
    result.append(insertStr);

    int32_t j = i;
    if (length > 0) {
        // The last character may be a multiple byte character,
        // get to the next character index.
        j += NValue::getIthCharIndex(&ptrSource[i], lengthSource-i, length+1);
    }
    result.append(std::string(&ptrSource[j], lengthSource - j));

    return result;
}

/** implement the 3 or 4 argument SQL OVERLAY function */
template<> inline NValue NValue::call<FUNC_OVERLAY_CHAR>(const std::vector<NValue>& arguments) {
    vassert(arguments.size() == 3 || arguments.size() == 4);

    for (int i = 0; i < arguments.size(); i++) {
        const NValue& arg = arguments[i];
        if (arg.isNull()) {
            return getNullStringValue();
        }
    }

    const NValue& str0 = arguments[0];
    if (str0.getValueType() != ValueType::tVARCHAR) {
        throwCastSQLException (str0.getValueType(), ValueType::tVARCHAR);
    }
    int32_t lenSrc;
    const char* srcChars = str0.getObject_withoutNull(lenSrc);

    const NValue& str1 = arguments[1];
    if (str1.getValueType() != ValueType::tVARCHAR) {
        throwCastSQLException (str1.getValueType(), ValueType::tVARCHAR);
    }
    int32_t lenInsert;
    const char* insertChars = str1.getObject_withoutNull(lenInsert);
    std::string insertStr(insertChars, lenInsert);

    const NValue& startArg = arguments[2];

    int64_t start = startArg.castAsBigIntAndGetValue();
    if (start <= 0) {
        throwSQLException(SQLException::data_exception_numeric_value_out_of_range,
                "data exception -- OVERLAY error, not positive start argument %ld",
                static_cast<long>(start));
    }

    int64_t length = 0;
    if (arguments.size() == 4) {
        const NValue& lengthArg = arguments[3];
        length = lengthArg.castAsBigIntAndGetValue();
        if (length < 0) {
            throwSQLException(SQLException::data_exception_numeric_value_out_of_range,
                    "data exception -- OVERLAY error, negative length argument %ld",
                    static_cast<long>(length));
        }
    }
    else {
        // By default without length argument
        length = getCharLength(insertChars, lenInsert);
    }

    vassert(start >= 1);
    std::string resultStr = overlay_function(srcChars, lenSrc, insertStr, start, length);

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

    vassert(arguments.size() == 2);
    const NValue &arg1 = arguments[0];
    if (arg1.isNull()) {
        return getNullStringValue();
    }
    const ValueType type = arg1.getValueType();
    if (type != ValueType::tDECIMAL) {
        throwCastSQLException (type, ValueType::tDECIMAL);
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

/** implement the Volt SQL STR function for decimal values */
template<> inline NValue NValue::callUnary<FUNC_VOLT_STR>() const {
    if (isNull()) {
       return getNullStringValue();
    }

    if (getValueType() != ValueType::tDECIMAL && getValueType() != ValueType::tDOUBLE) {
        throwCastSQLException (getValueType(), ValueType::tDECIMAL);
    }

    std::ostringstream out;
    TTInt scaledValue;
    if(getValueType() == ValueType::tDOUBLE)
    {
        scaledValue = castAsDecimal().castAsDecimalAndGetValue();
    } else {
        scaledValue = castAsDecimalAndGetValue();
    }

    if (scaledValue.IsSign()) {
        out << '-';
        scaledValue.ChangeSign();
    }

    //default scale 0
    int32_t scaleLength = 0;

    TTInt ten(10);
    if (scaleLength <= 0) {
        ten.Pow(-scaleLength);
    }
    else {
        ten.Pow(scaleLength);
    }
    TTInt denominator = (scaleLength <= 0) ? (TTInt(kMaxScaleFactor) * ten):
                                        (TTInt(kMaxScaleFactor) / ten);
    TTInt fractional(scaledValue);
    fractional %= denominator;
    TTInt barrier = CONST_FIVE * (denominator / 10);

    if (fractional > barrier) {
        scaledValue += denominator;
    }

    if (fractional == barrier) {
        TTInt prev = scaledValue / denominator;
        if (prev % 2 == CONST_ONE) {
            scaledValue += denominator;
        }
    }

    if (scaleLength <= 0) {
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
        out << '.' << std::setfill('0') << std::setw(scaleLength) << fraction;
    }

    // TODO: Although there should be only one copy of newloc (and money_numpunct),
    // we still need to test and make sure no memory leakage in this piece of code.
    std::string rv = out.str();

    //default number length 10
    int32_t numberLength = 10;

    if(rv.length() > numberLength){
        std::string res = "";
        for(int i=0; i<numberLength; i++){
            res += "*";
        }
        return getTempStringValue(res.c_str(), res.length());
    } else {
        return getTempStringValue(rv.c_str(), rv.length());
    }
}

/** implement the Volt SQL STR function for decimal values */
template<> inline NValue NValue::call<FUNC_VOLT_STR>(const std::vector<NValue>& arguments) {
    static TTInt one("1");
    static TTInt five("5");

    vassert(arguments.size() > 0 && arguments.size() < 4);
    const NValue &arg1 = arguments[0];
    if (arg1.isNull()) {
        return getNullStringValue();
    }

    const ValueType type = arg1.getValueType();
    if (type != ValueType::tDECIMAL && type != ValueType::tDOUBLE) {
        throwCastSQLException (type, ValueType::tDECIMAL);
    }

    std::ostringstream out;
    TTInt scaledValue;
    if (type == ValueType::tDOUBLE) {
        scaledValue = arg1.castAsDecimal().castAsDecimalAndGetValue();
    } else {
        scaledValue = arg1.castAsDecimalAndGetValue();
    }

    if (scaledValue.IsSign()) {
        out << '-';
        scaledValue.ChangeSign();
    }

    //default scale 0
    int32_t scaleLength = 0;
    if( arguments.size() == 3 ){
        const NValue &arg3 = arguments[2];
        if(!arg3.isNull()){
            scaleLength = arg3.castAsIntegerAndGetValue();
        }
    }

    if (scaleLength >= 12 || scaleLength < 0) {
        throw SQLException(SQLException::data_exception_numeric_value_out_of_range,
            "the third parameter should be < 12 and >= 0");
    }

    TTInt ten(10);
    if (scaleLength <= 0) {
        ten.Pow(-scaleLength);
    }
    else {
        ten.Pow(scaleLength);
    }
    TTInt denominator = (scaleLength <= 0) ? (TTInt(kMaxScaleFactor) * ten):
                                        (TTInt(kMaxScaleFactor) / ten);
    TTInt fractional(scaledValue);
    fractional %= denominator;
    TTInt barrier = five * (denominator / 10);

    if (fractional > barrier) {
        scaledValue += denominator;
    }

    if (fractional == barrier) {
        TTInt prev = scaledValue / denominator;
        if (prev % 2 == one) {
            scaledValue += denominator;
        }
    }

    if (scaleLength <= 0) {
        scaledValue -= fractional;
        int64_t whole = narrowDecimalToBigInt(scaledValue);
        out << std::fixed << whole;
    } else {
        int64_t whole = narrowDecimalToBigInt(scaledValue);
        int64_t fraction = getFractionalPart(scaledValue);
        // here denominator is guarateed to be able to converted to int64_t
        fraction /= denominator.ToInt();
        out << std::fixed << whole;
        // fractional part does not need groups
        out << '.' << std::setfill('0') << std::setw(scaleLength) << fraction;
    }
    // TODO: Although there should be only one copy of newloc (and money_numpunct),
    // we still need to test and make sure no memory leakage in this piece of code.
    std::string rv = out.str();

    //default number length 10
    int32_t numberLength = 10;
    if( arguments.size() > 1 ){
        const NValue &arg2 = arguments[1];
        if(!arg2.isNull()){
            numberLength = arg2.castAsIntegerAndGetValue();
        }
    }

    if (numberLength >= 38 || numberLength <= 0) {
        throw SQLException(SQLException::data_exception_numeric_value_out_of_range,
            "the second parameter should be <= 38 and > 0");
    }

    if(rv.length() > numberLength){
        std::string res = "";
        for(int i=0; i<numberLength; i++){
            res += "*";
        }
        return getTempStringValue(res.c_str(), res.length());
    } else {
        return getTempStringValue(rv.c_str(), rv.length());
    }
}

static std::string pcre2_error_code_message(int error_code, std::string prefix)
{
    unsigned char buffer[1024];
    /* This function really wants an unsigned char buffer, but std::string wants signed characters. */
    pcre2_get_error_message(error_code, buffer, sizeof(buffer));
    return std::string("Regular Expression Compilation Error: ") + reinterpret_cast<char *>(buffer);
}

/** Implement the VoltDB SQL function regexp_position for re-based pattern matching */
template<> inline NValue NValue::call<FUNC_VOLT_REGEXP_POSITION>(const std::vector<NValue>& arguments) {
    vassert(arguments.size() == 2 || arguments.size() == 3);

    const NValue& source = arguments[0];
    if (source.isNull()) {
        return getNullValue(ValueType::tBIGINT);
    }
    if (source.getValueType() != ValueType::tVARCHAR) {
        throwCastSQLException(source.getValueType(), ValueType::tVARCHAR);
    }

    const NValue& pat = arguments[1];
    if (pat.isNull()) {
        return getNullValue(ValueType::tBIGINT);
    }
    if (pat.getValueType() != ValueType::tVARCHAR) {
        throwCastSQLException(pat.getValueType(), ValueType::tVARCHAR);
    }

    uint32_t syntaxOpts = PCRE2_UTF;

    if (arguments.size() == 3) {
        const NValue& flags = arguments[2];
        if (!flags.isNull()) {
            if (flags.getValueType() != ValueType::tVARCHAR) {
                 throwCastSQLException(flags.getValueType(), ValueType::tVARCHAR);
            }

            int32_t lenFlags;
            const char* flagChars = reinterpret_cast<const char*>
                (flags.getObject_withoutNull(lenFlags));
            // temporary workaround to make sure the string we are operating on is null terminated
            std::string flagStr(flagChars, lenFlags);

            for(std::string::iterator it = flagStr.begin(); it != flagStr.end(); ++it) {
                switch (*it) {
                    case 'c':
                        syntaxOpts &= ~PCRE2_CASELESS;
                        break;
                    case 'i':
                        syntaxOpts |= PCRE2_CASELESS;
                        break;
                    default:
                        throw SQLException(SQLException::data_exception_invalid_parameter, "Regular Expression Compilation Error: Illegal Match Flags");
                }
            }
        }
    }

    int32_t lenSource;
    const unsigned char* sourceChars = reinterpret_cast<const unsigned char*>
        (source.getObject_withoutNull(lenSource));
    int32_t lenPat;
    const unsigned char* patChars = reinterpret_cast<const unsigned char*>
        (pat.getObject_withoutNull(lenPat));
    // Compile the pattern.

    int error_code = 0;
    PCRE2_SIZE error_offset = 0;
    /*
     * Note: We use a shared_ptr here, even though nothing is really shared.
     *       We want to make sure the deleter, pcre2_code_free, is called when
     *       this goes out of scope.  Scoped_ptr is a better choice here,
     *       but it will not allow a custom deleter.  Unique_ptr is an even
     *       better choice, but without C++11 move semantics this is not
     *       really implementable, and this is a C++03 code.  So we are
     *       stuck with shared_ptr.  The overhead is a reference count,
     *       and an increment and decrement.  This is pretty small
     *       compared with regular expression compilation and matching,
     *       so it's not likely to be expensive.
     */
    boost::shared_ptr<pcre2_code> pattern ( pcre2_compile(patChars,
                                                          lenPat,
                                                          syntaxOpts,
                                                          &error_code,
                                                          &error_offset,
                                                          NULL), pcre2_code_free );
    if (pattern.get() == NULL) {
        std::string emsg = pcre2_error_code_message(error_code, "Regular Expression Compilation Error: ");
        throw SQLException(SQLException::data_exception_invalid_parameter, emsg.c_str());
    }
    /*
     * We use a shared_ptr for the same reasons as above.
     */
    boost::shared_ptr<pcre2_match_data> match_data(pcre2_match_data_create_from_pattern(pattern.get(), NULL),
                                                   pcre2_match_data_free);
    if (match_data.get() == NULL) {
        throw SQLException(SQLException::data_exception_invalid_parameter, "Internal error: Cannot create PCRE2 match data.");
    }
    unsigned int matchFlags = 0;
    error_code = pcre2_match(pattern.get(),
                      sourceChars,
                      lenSource,
                      0ul,
                      matchFlags,
                      match_data.get(),
                      NULL);
    if (error_code < 0) {
        if (error_code == PCRE2_ERROR_NOMATCH) {
            return getBigIntValue(0);
        }
        std::string emsg = pcre2_error_code_message(error_code, "Regular Expression Matching Error: ");
        throw SQLException(SQLException::data_exception_invalid_parameter, emsg.c_str());
    }
    PCRE2_SIZE *ovector = pcre2_get_ovector_pointer(match_data.get());
    unsigned long position = ovector[0];
    return getBigIntValue(getCharLength(reinterpret_cast<const char *>(sourceChars), position) + 1);
}
}

