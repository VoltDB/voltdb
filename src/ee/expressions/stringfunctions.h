/* This file is part of VoltDB.
 * Copyright (C) 2008-2012 VoltDB Inc.
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

namespace voltdb {

static int32_t inline getCharLength(const char *valueChars, const size_t length) {
    // very efficient code to count characters in UTF string and ASCII string
    int32_t i = 0, j = 0;
    size_t len = length;
    while (len-- > 0) {
        if ((valueChars[i] & 0xc0) != 0x80) j++;
        i++;
    }
    return j;
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
    return getIntegerValue(getCharLength(valueChars, getObjectLength()));
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
