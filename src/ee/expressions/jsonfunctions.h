/* This file is part of VoltDB.
 * Copyright (C) 2008-2014 VoltDB Inc.
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

#ifndef JSONFUNCTIONS_H_
#define JSONFUNCTIONS_H_

#include <cassert>
#include <cstring>
#include <string>
#include <sstream>
#include <algorithm>

#include <jsoncpp/jsoncpp.h>
#include <jsoncpp/jsoncpp-forwards.h>

namespace voltdb {

/** implement the 2-argument SQL FIELD function */
template<> inline NValue NValue::call<FUNC_VOLT_FIELD>(const std::vector<NValue>& arguments) {
    assert(arguments.size() == 2);

    const NValue& docNVal = arguments[0];
    if (docNVal.isNull()) {
        return getNullStringValue();
    }
    if (docNVal.getValueType() != VALUE_TYPE_VARCHAR) {
        throwCastSQLException (docNVal.getValueType(), VALUE_TYPE_VARCHAR);
    }
    int32_t lenDoc = docNVal.getObjectLength_withoutNull();

    const NValue& fieldNVal = arguments[1];
    if (fieldNVal.isNull()) {
        return getNullStringValue();
    }
    if (fieldNVal.getValueType() != VALUE_TYPE_VARCHAR) {
        throwCastSQLException (fieldNVal.getValueType(), VALUE_TYPE_VARCHAR);
    }
    int32_t lenField = fieldNVal.getObjectLength_withoutNull();

    char *docChars = reinterpret_cast<char*>(docNVal.getObjectValue_withoutNull());
    char *fieldChars = reinterpret_cast<char*>(fieldNVal.getObjectValue_withoutNull());

    const std::string doc(docChars, lenDoc);
    const std::string field(fieldChars,lenField);

    Json::Value root;
    Json::Reader reader;

    if( ! reader.parse(doc, root)) {
        char msg[1024];
        // getFormatedErrorMessages returns concise message about location
        // of the error rather than the malformed document itself
        snprintf(msg, sizeof(msg), "Invalid JSON %s", reader.getFormatedErrorMessages().c_str());
        throw SQLException(SQLException::
                           data_exception_invalid_parameter,
                           msg);
    }

    // only object type contain fields. primitives, arrays do not
    if( ! root.isObject()) {
        return getNullStringValue();
    }

    // field is not present in the document
    if( ! root.isMember(field)) {
        return getNullStringValue();
    }

    Json::Value fieldValue = root[field];

    if (fieldValue.isNull()) {
        return getNullStringValue();
    }

    if (fieldValue.isConvertibleTo(Json::stringValue)) {
        std::string stringValue(fieldValue.asString());
        return getTempStringValue(stringValue.c_str(), stringValue.length());
    }

    Json::FastWriter writer;
    std::string serializedValue(writer.write(fieldValue));
    // writer always appends a trailing new line \n
    return getTempStringValue(serializedValue.c_str(), serializedValue.length() -1);
}

/** implement the 2-argument SQL ARRAY_ELEMENT function */
template<> inline NValue NValue::call<FUNC_VOLT_ARRAY_ELEMENT>(const std::vector<NValue>& arguments) {
    assert(arguments.size() == 2);

    const NValue& docNVal = arguments[0];
    if (docNVal.isNull()) {
        return getNullStringValue();
    }
    if (docNVal.getValueType() != VALUE_TYPE_VARCHAR) {
        throwCastSQLException (docNVal.getValueType(), VALUE_TYPE_VARCHAR);
    }

    const NValue& indexNVal = arguments[1];
    if (indexNVal.isNull()) {
        return getNullStringValue();
    }
    int32_t lenDoc = docNVal.getObjectLength_withoutNull();
    char *docChars = reinterpret_cast<char*>(docNVal.getObjectValue_withoutNull());
    const std::string doc(docChars, lenDoc);

    int32_t index = indexNVal.castAsIntegerAndGetValue();

    Json::Value root;
    Json::Reader reader;

    if( ! reader.parse(doc, root)) {
        char msg[1024];
        // getFormatedErrorMessages returns concise message about location
        // of the error rather than the malformed document itself
        snprintf(msg, sizeof(msg), "Invalid JSON %s", reader.getFormatedErrorMessages().c_str());
        throw SQLException(SQLException::
                           data_exception_invalid_parameter,
                           msg);
    }

    // only array type contains elements. objects, primitives do not
    if( ! root.isArray()) {
        return getNullStringValue();
    }

    // Sure, root[-1].isNull() would return true just like we want it to
    // -- but only in production with asserts turned off.
    // Turn on asserts for debugging and you'll want this guard up front.
    // Forcing the null return for a negative index seems more consistent than crashing in debug mode
    // or even throwing an SQL error in any mode. It's the same handling that a too large index gets.
    if (index < 0) {
        return getNullStringValue();
    }

    Json::Value fieldValue = root[index];

    if (fieldValue.isNull()) {
        return getNullStringValue();
    }

    if (fieldValue.isConvertibleTo(Json::stringValue)) {
        std::string stringValue(fieldValue.asString());
        return getTempStringValue(stringValue.c_str(), stringValue.length());
    }

    Json::FastWriter writer;
    std::string serializedValue(writer.write(fieldValue));
    // writer always appends a trailing new line \n
    return getTempStringValue(serializedValue.c_str(), serializedValue.length() -1);
}

/** implement the 1-argument SQL ARRAY_LENGTH function */
template<> inline NValue NValue::callUnary<FUNC_VOLT_ARRAY_LENGTH>() const {

    if (isNull()) {
        return getNullValue(VALUE_TYPE_INTEGER);
    }
    if (getValueType() != VALUE_TYPE_VARCHAR) {
        throwCastSQLException (getValueType(), VALUE_TYPE_VARCHAR);
    }

    int32_t lenDoc = getObjectLength_withoutNull();
    char *docChars = reinterpret_cast<char*>(getObjectValue_withoutNull());
    const std::string doc(docChars, lenDoc);

    Json::Value root;
    Json::Reader reader;

    if( ! reader.parse(doc, root)) {
        char msg[1024];
        // getFormatedErrorMessages returns concise message about location
        // of the error rather than the malformed document itself
        snprintf(msg, sizeof(msg), "Invalid JSON %s", reader.getFormatedErrorMessages().c_str());
        throw SQLException(SQLException::
                           data_exception_invalid_parameter,
                           msg);
    }

    // only array type contains indexed elements. objects, primitives do not
    if( ! root.isArray()) {
        return getNullValue(VALUE_TYPE_INTEGER);
    }

    NValue result(VALUE_TYPE_INTEGER);
    int32_t size = static_cast<int32_t>(root.size());
    result.getInteger() = size;
    return result;
}

}


#endif /* JSONFUNCTIONS_H_ */
