/* This file is part of VoltDB.
 * Copyright (C) 2008-2013 VoltDB Inc.
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

namespace voltdb {

static inline int32_t getCharLength(const char *valueChars, const size_t length) {
    // very efficient code to count characters in UTF string and ASCII string
    int32_t i = 0, j = 0;
    size_t len = length;
    while (len-- > 0) {
        if ((valueChars[i] & 0xc0) != 0x80) j++;
        i++;
    }
    return j;
}

// Return the beginning char * place of the ith char.
// Return the end char* when ith is larger than it has, NULL if ith is less and equal to zero.
static inline const char* getIthCharPosition(const char *valueChars, const size_t length, const int32_t ith) {
    // very efficient code to count characters in UTF string and ASCII string
    if (ith <= 0) return NULL;
    int32_t i = 0, j = 0;
    size_t len = length;
    while (len-- > 0) {
        if ((valueChars[i] & 0xc0) != 0x80) {
            j++;
            if (ith == j) break;
        }
        i++;
    }
    return &valueChars[i];
}

/** implement the 1-argument SQL OCTET_LENGTH function */
template<> inline NValue NValue::callUnary<FUNC_OCTET_LENGTH>() const {
    if (isNull())
        return getNullValue();

    return getIntegerValue(getObjectLength());
}

/** implement the 1-argument SQL CHAR_LENGTH function */
template<> inline NValue NValue::callUnary<FUNC_CHAR_LENGTH>() const {
    if (isNull())
        return getNullValue();

    char *valueChars = reinterpret_cast<char*>(getObjectValue());
    return getBigIntValue(static_cast<int64_t>(getCharLength(valueChars, getObjectLength())));
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

    const int32_t valueUTF8Length = strValue.getObjectLength();
    char *repeatChars = reinterpret_cast<char*>(strValue.getObjectValue());

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
    int32_t lenTarget = target.getObjectLength();

    const NValue& pool = arguments[1];
    if (pool.isNull()) {
        return getNullValue();
    }
    int32_t lenPool = pool.getObjectLength();
    char *targetChars = reinterpret_cast<char*>(target.getObjectValue());
    char *poolChars = reinterpret_cast<char*>(pool.getObjectValue());

    std::string poolStr(poolChars, lenPool);
    std::string targetStr(targetChars, lenTarget);

    size_t position = poolStr.find(targetStr);
    if (position == std::string::npos)
        position = 0;
    else {
        position = getCharLength(poolStr.substr(0,position).c_str(),position) + 1;
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

    const int32_t valueUTF8Length = strValue.getObjectLength();
    char *valueChars = reinterpret_cast<char*>(strValue.getObjectValue());

    return getTempStringValue(valueChars,(int32_t)(getIthCharPosition(valueChars,valueUTF8Length,count+1) - valueChars));
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

    const int32_t valueUTF8Length = strValue.getObjectLength();
    char *valueChars = reinterpret_cast<char*>(strValue.getObjectValue());
    const char *valueEnd = valueChars+valueUTF8Length;
    int32_t charLen = getCharLength(valueChars,valueUTF8Length);
    if (count >= charLen)
        return getTempStringValue(valueChars,(int32_t)(valueEnd - valueChars));

    const char* newStartChar = getIthCharPosition(valueChars,valueUTF8Length,charLen-count+1);
    return getTempStringValue(newStartChar,(int32_t)(valueEnd - newStartChar));
}

/** implement the 2-argument SQL CONCAT function */
template<> inline NValue NValue::call<FUNC_CONCAT>(const std::vector<NValue>& arguments) {
    assert(arguments.size() == 2);
    const NValue& left = arguments[0];
    if (left.isNull()) {
        return getNullStringValue();
    }
    if (left.getValueType() != VALUE_TYPE_VARCHAR) {
        throwCastSQLException (left.getValueType(), VALUE_TYPE_VARCHAR);
    }
    int32_t lenLeft = left.getObjectLength();

    const NValue& right = arguments[1];
    if (right.isNull()) {
        return getNullStringValue();
    }
    int32_t lenRight = right.getObjectLength();
    char *leftChars = reinterpret_cast<char*>(left.getObjectValue());
    char *rightChars = reinterpret_cast<char*>(right.getObjectValue());

    std::string leftStr(leftChars, lenLeft);
    leftStr.append(rightChars, lenRight);

    return getTempStringValue(leftStr.c_str(),lenLeft+lenRight);
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

    const int32_t valueUTF8Length = strValue.getObjectLength();
    char *valueChars = reinterpret_cast<char*>(strValue.getObjectValue());
    const char *valueEnd = valueChars+valueUTF8Length;

    int64_t start = std::max(startArg.castAsBigIntAndGetValue(), static_cast<int64_t>(1L));

    UTF8Iterator iter(valueChars, valueEnd);
    const char* startChar = iter.skipCodePoints(start-1);
    return getTempStringValue(startChar, (int32_t)(valueEnd - startChar));
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
    const int32_t valueUTF8Length = strValue.getObjectLength();
    const char *valueChars = reinterpret_cast<char*>(strValue.getObjectValue());
    const char *valueEnd = valueChars+valueUTF8Length;
    int64_t start = std::max(startArg.castAsBigIntAndGetValue(), static_cast<int64_t>(1L));
    int64_t length = lengthArg.castAsBigIntAndGetValue();
    if (length < 0) {
        char message[128];
        snprintf(message, 128, "data exception -- substring error, negative length argument %ld", (long)length);
        throw SQLException( SQLException::data_exception_numeric_value_out_of_range, message);
    }
    UTF8Iterator iter(valueChars, valueEnd);
    const char* startChar = iter.skipCodePoints(start-1);
    const char* endChar = iter.skipCodePoints(length);
    return getTempStringValue(startChar, endChar - startChar);
}

}

#endif /* STRINGFUNCTIONS_H */
