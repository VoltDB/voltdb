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


/** implement the 1-argument SQL CHAR_LENGTH function */
template<> inline NValue NValue::callUnary<FUNC_OCTET_LENGTH>() const {
    if (isNull()) {
        return getIntegerValue(0);
    }
    return getIntegerValue(getObjectLength());
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
