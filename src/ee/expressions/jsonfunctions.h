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
    int32_t lenDoc = docNVal.getObjectLength();

    const NValue& fieldNVal = arguments[1];
    if (fieldNVal.isNull()) {
        return getNullStringValue();
    }
    if (fieldNVal.getValueType() != VALUE_TYPE_VARCHAR) {
        throwCastSQLException (fieldNVal.getValueType(), VALUE_TYPE_VARCHAR);
    }
    int32_t lenField = fieldNVal.getObjectLength();

    char *docChars = reinterpret_cast<char*>(docNVal.getObjectValue());
    char *fieldChars = reinterpret_cast<char*>(fieldNVal.getObjectValue());

    const std::string doc(docChars, lenDoc);
    const std::string field(fieldChars,lenField);

    Json::Value root;
    Json::Reader reader;

    if( ! reader.parse(doc, root)) {
        char msg[1024];
        // get formattedErrorMessafe returns concise message about location
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

}


#endif /* JSONFUNCTIONS_H_ */
