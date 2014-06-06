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

/** a path node is either a field name or an array index */
struct JsonPathNode {
    JsonPathNode(int32_t arrayIndex) : m_arrayIndex(arrayIndex) {}
    JsonPathNode(const char* field) : m_arrayIndex(-1), m_field(field) {}

    int32_t m_arrayIndex;
    std::string m_field;
};

/** representation of a JSON document that can be accessed and updated via
    our path syntax */
class JsonDocument {
public:
    JsonDocument(const char* docChars, int32_t lenDoc) : m_head(NULL), m_tail(NULL) {
        if (docChars == NULL) {
            // null documents have null everything, but they turn into objects/arrays
            // if we try to set their properties
            m_doc = Json::Value::null;
        } else if (!m_reader.parse(docChars, docChars + lenDoc, m_doc)) {
            // we have something real, but it isn't JSON
            throwJsonFormattingError();
        }
    }

    std::string value() { return m_writer.write(m_doc); }

    bool get(const char* pathChars, int32_t lenPath, std::string& serializedValue) {
        if (m_doc.isNull()) {
            return false;
        }

        // get and traverse the path
        std::vector<JsonPathNode> path = resolveJsonPath(pathChars, lenPath);
        const Json::Value* node = &m_doc;
        for (std::vector<JsonPathNode>::const_iterator cit = path.begin(); cit != path.end(); ++cit) {
            const JsonPathNode& pathNode = *cit;
            if (pathNode.m_arrayIndex != -1) {
                // can't access an array index of something that isn't an array
                if (!node->isArray()) {
                    return false;
                }
                int32_t arrayIndex = pathNode.m_arrayIndex;
                if (arrayIndex == ARRAY_TAIL) {
                    unsigned int arraySize = node->size();
                    arrayIndex = arraySize > 0 ? arraySize - 1 : 0;
                }
                node = &((*node)[arrayIndex]);
                if (node == &Json::Value::null) {
                    return false;
                }
            } else {
                // this is a field. only objects have fields
                if (!node->isObject()) {
                    return false;
                }
                node = &((*node)[pathNode.m_field]);
                if (node == &Json::Value::null) {
                    return false;
                }
            }
        }

        // return the string representation of what we have obtained
        if (node->isConvertibleTo(Json::stringValue)) {
            // 'append' is to standardize that there's something to remove. quicker
            // than substr on the other one, which incurs an extra copy
            serializedValue = node->asString().append(1, '\n');
        } else {
            serializedValue = m_writer.write(*node);
        }
        return true;
    }

    void set(const char* pathChars, int32_t lenPath, const char* valueChars, int32_t lenValue) {
        // translate database nulls into JSON nulls, because that's really all that makes
        // any semantic sense. otherwise, parse the value as JSON
        Json::Value value;
        if (lenValue <= 0) {
            value = Json::Value::null;
        } else if (!m_reader.parse(valueChars, valueChars + lenValue, value)) {
            throwJsonFormattingError();
        }

        std::vector<JsonPathNode> path = resolveJsonPath(pathChars, lenPath);
        // the non-const version of the Json::Value [] operator creates a new, null node on attempted
        // access if none already exists
        Json::Value* node = &m_doc;
        for (std::vector<JsonPathNode>::const_iterator cit = path.begin(); cit != path.end(); ++cit) {
            const JsonPathNode& pathNode = *cit;
            if (pathNode.m_arrayIndex != -1) {
                if (!node->isNull() && !node->isArray()) {
                    // no-op if the update is impossible, I guess?
                    return;
                }
                int32_t arrayIndex = pathNode.m_arrayIndex;
                if (arrayIndex == ARRAY_TAIL) {
                    arrayIndex = node->size();
                }
                // get or create the specified node
                node = &((*node)[arrayIndex]);
            } else {
                if (!node->isNull() && !node->isObject()) {
                    return;
                }
                node = &((*node)[pathNode.m_field]);
            }
        }
        *node = value;
    }

private:
    Json::Value m_doc;
    Json::Reader m_reader;
    Json::FastWriter m_writer;

    const char* m_head;
    const char* m_tail;
    int32_t m_pos;

    static const int32_t ARRAY_TAIL = -10;

    /** parse our path to its vector representation */
    std::vector<JsonPathNode> resolveJsonPath(const char* pathChars, int32_t lenPath) {
        std::vector<JsonPathNode> path;
        // NULL path refers directly to the doc root
        if (pathChars == NULL) {
            return path;
        }
        m_head = pathChars;
        m_tail = m_head + lenPath;

        m_pos = -1;
        char c;
        bool first = true;
        bool expectArrayIndex = false;
        bool expectField = false;
        char strField[lenPath];
        while (readChar(c)) {
            if (expectArrayIndex) {
                if (!readChar(c)) {
                    throwInvalidPathError("Unexpected termination of JSON path");
                }
                // -1 to refer to the tail of the array
                bool neg = false;
                if (c == '-') {
                    neg = true;
                    if (!readChar(c)) {
                        throwInvalidPathError("Unexpected termination of JSON path");
                    }
                }
                if (c < '0' || c > '9') {
                    throwInvalidPathError("Unexpected character in JSON path array index");
                }
                // atoi while advancing our pointer
                int32_t arrayIndex = c - '0';
                bool success = false;
                while (readChar(c)) {
                    if (c == ']') {
                        success = true;
                        break;
                    } else if (c < '0' || c > '9') {
                        throwInvalidPathError("Unexpected character in JSON path array index");
                    }
                    arrayIndex = 10 * arrayIndex + (c - '0');
                }
                if (neg) {
                    // other than the special '-1' case, negative indices aren't allowed
                    if (arrayIndex != 1) {
                        throwInvalidPathError("Invalid array index in JSON path");
                    }
                    arrayIndex = ARRAY_TAIL;
                }
                path.push_back(arrayIndex);
                expectArrayIndex = false;
            } else if (c == '[') {
                // handle the case of empty field names. for example, getting the first element of the array
                // in { "a": { "": [ true, false ] } } would be the path 'a.[0]'
                if (expectField) {
                    path.push_back(JsonPathNode(""));
                    expectField = false;
                }
                expectArrayIndex = true;
            } else if (c == '.') {
                // a leading '.' also involves accessing the "" property of the root...
                if (expectField || first) {
                    path.push_back(JsonPathNode(""));
                }
                expectField = true;
            } else {
                expectField = false;
                // read a literal field name
                int32_t i = 0;
                do {
                    if (c == '\\') {
                        if (!readChar(c)) {
                            throwInvalidPathError("Unexpected termination of JSON path (empty escape sequence)");
                        } else if (c != '\\' && c != '[' && c != ']' && c != '.') {
                            // need to further escape our backslashes to separate our escaped '.' and '['
                            // from things that are meant to be escaped in the JSON. ']' is only allowed for
                            // symmetry, since there's really no need to escape it
                            throwInvalidPathError("Unexpected escape sequence in JSON path");
                        }
                    } else if (c == '.') {
                        expectField = true;
                        break;
                    } else if (c == '[') {
                        expectArrayIndex = true;
                        break;
                    }
                    strField[i++] = c;
                } while (readChar(c));
                strField[i] = '\0';
                path.push_back(JsonPathNode(strField));
            }
            first = false;
        }
        // if we're either empty or ended on a trailing '.', add an empty field name
        if (expectField || first) {
            path.push_back(JsonPathNode(""));
        }

        m_head = NULL;
        m_tail = NULL;
        return path;
    }

    bool readChar(char& c) {
        assert(m_head != NULL && m_tail != NULL);
        if (m_head == m_tail) {
            return false;
        }
        c = *m_head++;
        m_pos++;
        return true;
    }

    void throwInvalidPathError(const char* err) const {
        char msg[1024];
        snprintf(msg, sizeof(msg), "Invalid JSON path: %s [position %d]", err, m_pos);
        throw SQLException(SQLException::
                           data_exception_invalid_parameter,
                           msg);
    }

    void throwJsonFormattingError() const {
        char msg[1024];
        // getFormatedErrorMessages returns concise message about location
        // of the error rather than the malformed document itself
        snprintf(msg, sizeof(msg), "Invalid JSON %s", m_reader.getFormatedErrorMessages().c_str());
        throw SQLException(SQLException::
                           data_exception_invalid_parameter,
                           msg);
    }
};

/** implement the 2-argument SQL FIELD function */
template<> inline NValue NValue::call<FUNC_VOLT_FIELD>(const std::vector<NValue>& arguments) {
    assert(arguments.size() == 2);

    const NValue& docNVal = arguments[0];
    const NValue& pathNVal = arguments[1];

    int32_t lenDoc = -1;
    const char* docChars = NULL;
    if (!docNVal.isNull()) {
        if (docNVal.getValueType() != VALUE_TYPE_VARCHAR) {
            throwCastSQLException (docNVal.getValueType(), VALUE_TYPE_VARCHAR);
        }
        lenDoc = docNVal.getObjectLength_withoutNull();
        docChars = reinterpret_cast<char*>(docNVal.getObjectValue_withoutNull());
    }

    int32_t lenPath = -1;
    const char* pathChars = NULL;
    if (!pathNVal.isNull()) {
        if (pathNVal.getValueType() != VALUE_TYPE_VARCHAR) {
            throwCastSQLException (pathNVal.getValueType(), VALUE_TYPE_VARCHAR);
        }
        lenPath = pathNVal.getObjectLength_withoutNull();
        pathChars = reinterpret_cast<char*>(pathNVal.getObjectValue_withoutNull());
    }

    JsonDocument doc(docChars, lenDoc);
    std::string value;
    if (!doc.get(pathChars, lenPath, value)) {
        return getNullStringValue();
    }
    return getTempStringValue(value.c_str(), value.length() - 1);
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

/** implement the 3-argument SQL SET_FIELD function */
template<> inline NValue NValue::call<FUNC_VOLT_SET_FIELD>(const std::vector<NValue>& arguments) {
    assert(arguments.size() == 3);

    const NValue& docNVal = arguments[0];
    const NValue& pathNVal = arguments[1];
    const NValue& valueNVal = arguments[2];

    int32_t lenDoc = -1;
    const char* docChars = NULL;
    if (!docNVal.isNull()) {
        if (docNVal.getValueType() != VALUE_TYPE_VARCHAR) {
            throwCastSQLException (docNVal.getValueType(), VALUE_TYPE_VARCHAR);
        }
        lenDoc = docNVal.getObjectLength_withoutNull();
        docChars = reinterpret_cast<char*>(docNVal.getObjectValue_withoutNull());
    }

    int32_t lenPath = -1;
    const char* pathChars = NULL;
    if (!pathNVal.isNull()) {
        if (pathNVal.getValueType() != VALUE_TYPE_VARCHAR) {
            throwCastSQLException (pathNVal.getValueType(), VALUE_TYPE_VARCHAR);
        }
        lenPath = pathNVal.getObjectLength_withoutNull();
        pathChars = reinterpret_cast<char*>(pathNVal.getObjectValue_withoutNull());
    }

    int32_t lenValue = -1;
    const char* valueChars = NULL;
    if (!valueNVal.isNull()) {
        if (valueNVal.getValueType() != VALUE_TYPE_VARCHAR) {
            throwCastSQLException (valueNVal.getValueType(), VALUE_TYPE_VARCHAR);
        }
        lenValue = valueNVal.getObjectLength_withoutNull();
        valueChars = reinterpret_cast<char*>(valueNVal.getObjectValue_withoutNull());
    }

    JsonDocument doc(docChars, lenDoc);
    doc.set(pathChars, lenPath, valueChars, lenValue);

    std::string value = doc.value();
    return getTempStringValue(value.c_str(), value.length() - 1);
}

}


#endif /* JSONFUNCTIONS_H_ */
