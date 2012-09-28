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

#include "json_spirit/json_spirit.h"
#include "stringfunctions.h"

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

    json_spirit::mValue docVal;

    /*
     * if false it is an invalid JSON. It reports the full JSON doc in the error
     * message if it is 50 characters or less. If it is longer than 50, it truncates
     * the reported JSON doc to the first 50 characters, and appends ellipses '...'
     */
    if (! json_spirit::read(doc, docVal)) {

        char msg[1024];
        const char *fiftyith = getIthCharPosition(docChars, lenDoc, 50);
        const size_t elipsedLen = static_cast<size_t>( fiftyith - docChars);
        std::string elipsed( docChars, elipsedLen);

        if (*fiftyith != '\0') {
            elipsed += " ...";
        }

        snprintf(msg, sizeof(msg), "'%s' is not valid JSON", elipsed.c_str());
        throw SQLException(SQLException::
                           data_exception_invalid_parameter,
                           msg);
    }

    /* only object type contain fields. primitives, arrays do not */
    if( docVal.type() != json_spirit::obj_type) {
        return getNullStringValue();
    }

    json_spirit::mObject mObj = docVal.get_obj();
    json_spirit::mObject::const_iterator itr = mObj.find(field);

    if (itr == mObj.end() || itr->first != field) {
        return getNullStringValue();
    }

    const json_spirit::mValue& value = itr->second;

    std::stringstream ss;

    switch( value.type()) {
    case json_spirit::str_type:  ss << value.get_str(); break;
    case json_spirit::int_type:  ss << value.get_int(); break;
    case json_spirit::bool_type: ss << (value.get_bool() ? "true" : "false"); break;
    case json_spirit::real_type: ss << value.get_real(); break;
    case json_spirit::null_type: return getNullStringValue();
    default:
        json_spirit::write(value, ss);
        break;
    }

    const std::string& returnValue = ss.str();
    return getTempStringValue(returnValue.c_str(), returnValue.length());
}

}


#endif /* JSONFUNCTIONS_H_ */
