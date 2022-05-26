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

#include <common/debuglog.h>
#include <cstring>
#include <string>
#include <sstream>
#include <algorithm>

#include <json/json.h>
#include <json/forwards.h>

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
                if (node->isNull()) {
                    return false;
                }
            } else {
                // this is a field. only objects have fields
                if (!node->isObject()) {
                    return false;
                }
                node = &((*node)[pathNode.m_field]);
                if (node->isNull()) {
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

        std::vector<JsonPathNode> path = resolveJsonPath(pathChars, lenPath, true /*enforceArrayIndexLimitForSet*/);
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
    std::vector<JsonPathNode> resolveJsonPath(const char* pathChars, int32_t lenPath,
                                              bool enforceArrayIndexLimitForSet = false) {
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
                // -1 index to refer to the tail of the array
                bool neg = false;
                if (c == '-') {
                    neg = true;
                    if (!readChar(c)) {
                        throwInvalidPathError("Unexpected termination (unterminated array access)");
                    }
                }
                if (c < '0' || c > '9') {
                    throwInvalidPathError("Unexpected character in array index");
                }
                // atoi while advancing our pointer
                int64_t arrayIndex = c - '0';
                bool terminated = false;
                while (readChar(c)) {
                    if (c == ']') {
                        terminated = true;
                        break;
                    } else if (c < '0' || c > '9') {
                        throwInvalidPathError("Unexpected character in array index");
                    }
                    arrayIndex = 10 * arrayIndex + (c - '0');
                    if (enforceArrayIndexLimitForSet) {
                        // This 500000 is a mostly arbitrary maximum JSON array index enforced for practical
                        // purposes. We enforce this up front to avoid excessive delays, ridiculous short-term
                        // memory growth, and/or bad_alloc errors that the jsoncpp library could produce
                        // essentially for nothing since our supported JSON document columns are typically not
                        // wide enough to hold the string representations of arrays this large.
                        if (arrayIndex > 500000) {
                            if (neg) {
                                // other than the special '-1' case, negative indices aren't allowed
                                throwInvalidPathError("Array index less than -1");
                            }
                            throwInvalidPathError("Array index greater than the maximum allowed value of 500000");
                        }
                    } else {
                        if (arrayIndex > static_cast<int64_t>(INT32_MAX)) {
                            if (neg) {
                                // other than the special '-1' case, negative indices aren't allowed
                                throwInvalidPathError("Array index less than -1");
                            }
                            throwInvalidPathError("Array index greater than the maximum integer value");
                        }
                    }
                }
                if ( ! terminated ) {
                    throwInvalidPathError("Missing ']' after array index");
                }
                if (neg) {
                    // other than the special '-1' case, negative indices aren't allowed
                    if (arrayIndex != 1) {
                        throwInvalidPathError("Array index less than -1");
                    }
                    arrayIndex = ARRAY_TAIL;
                }
                path.push_back(static_cast<int32_t>(arrayIndex));
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
                        if (!readChar(c) || (c != '[' && c != ']' && c != '.' && c != '\\')) {
                            throwInvalidPathError("Unescaped backslash (double escaping required for path)");
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
        // trailing '['
        if (expectArrayIndex) {
            throwInvalidPathError("Unexpected termination (unterminated array access)");
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
        vassert(m_head != NULL && m_tail != NULL);
        if (m_head == m_tail) {
            return false;
        } else {
            c = *m_head++;
            m_pos++;
            return true;
        }
    }

    void throwInvalidPathError(const char* err) const {
        throwSQLException(SQLException::data_exception_invalid_parameter,
                "Invalid JSON path: %s [position %d]", err, m_pos);
    }

    void throwJsonFormattingError() const {
        // getFormatedErrorMessages returns concise message about location
        // of the error rather than the malformed document itself
        throwSQLException(SQLException::data_exception_invalid_parameter,
                "Invalid JSON %s", m_reader.getFormatedErrorMessages().c_str());
    }
};

/** implement the 2-argument SQL FIELD function */
template<> inline NValue NValue::call<FUNC_VOLT_FIELD>(const std::vector<NValue>& arguments) {
    vassert(arguments.size() == 2);

    const NValue& docNVal = arguments[0];
    const NValue& pathNVal = arguments[1];

    if (docNVal.isNull()) {
        return docNVal;
    } else if (pathNVal.isNull()) {
        throw SQLException(SQLException::data_exception_invalid_parameter, "Invalid FIELD path argument (SQL null)");
    } else if (docNVal.getValueType() != ValueType::tVARCHAR) {
        throwCastSQLException(docNVal.getValueType(), ValueType::tVARCHAR);
    } else if (pathNVal.getValueType() != ValueType::tVARCHAR) {
        throwCastSQLException(pathNVal.getValueType(), ValueType::tVARCHAR);
    }

    int32_t lenDoc;
    const char* docChars = docNVal.getObject_withoutNull(lenDoc);
    JsonDocument doc(docChars, lenDoc);

    int32_t lenPath;
    const char* pathChars = pathNVal.getObject_withoutNull(lenPath);
    std::string result;
    if (doc.get(pathChars, lenPath, result)) {
        return getTempStringValue(result.c_str(), result.length() - 1);
    } else {
        return getNullStringValue();
    }
}

/** implement the 2-argument SQL ARRAY_ELEMENT function */
template<> inline NValue NValue::call<FUNC_VOLT_ARRAY_ELEMENT>(const std::vector<NValue>& arguments) {
    vassert(arguments.size() == 2);

    const NValue& docNVal = arguments[0];
    if (docNVal.isNull()) {
        return getNullStringValue();
    } else if (docNVal.getValueType() != ValueType::tVARCHAR) {
        throwCastSQLException(docNVal.getValueType(), ValueType::tVARCHAR);
    }

    const NValue& indexNVal = arguments[1];
    if (indexNVal.isNull()) {
        return getNullStringValue();
    }
    int32_t lenDoc;
    const char* docChars = docNVal.getObject_withoutNull(lenDoc);
    const std::string doc(docChars, lenDoc);

    int32_t index = indexNVal.castAsIntegerAndGetValue();

    Json::Value root;
    Json::Reader reader;

    if (! reader.parse(doc, root)) {
        // getFormatedErrorMessages returns concise message about location
        // of the error rather than the malformed document itself
        throwSQLException(SQLException::data_exception_invalid_parameter,
                "Invalid JSON %s", reader.getFormatedErrorMessages().c_str());
    }

    // only array type contains elements. objects, primitives do not
    if ( ! root.isArray()) {
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
        return getNullValue(ValueType::tINTEGER);
    }
    if (getValueType() != ValueType::tVARCHAR) {
        throwCastSQLException(getValueType(), ValueType::tVARCHAR);
    }

    int32_t lenDoc;
    const char* docChars = getObject_withoutNull(lenDoc);
    const std::string doc(docChars, lenDoc);

    Json::Value root;
    Json::Reader reader;

    if (! reader.parse(doc, root)) {
        // getFormatedErrorMessages returns concise message about location
        // of the error rather than the malformed document itself
        throwSQLException(SQLException::data_exception_invalid_parameter,
                "Invalid JSON %s", reader.getFormatedErrorMessages().c_str());
    }

    // only array type contains indexed elements. objects, primitives do not
    if ( ! root.isArray()) {
        return getNullValue(ValueType::tINTEGER);
    }

    NValue result(ValueType::tINTEGER);
    int32_t size = static_cast<int32_t>(root.size());
    result.getInteger() = size;
    return result;
}

/** implement the 3-argument SQL SET_FIELD function */
template<> inline NValue NValue::call<FUNC_VOLT_SET_FIELD>(const std::vector<NValue>& arguments) {
    vassert(arguments.size() == 3);

    const NValue& docNVal = arguments[0];
    const NValue& pathNVal = arguments[1];
    const NValue& valueNVal = arguments[2];

    if (docNVal.isNull()) {
        return docNVal;
    }
    if (pathNVal.isNull()) {
        throw SQLException(SQLException::data_exception_invalid_parameter,
                           "Invalid SET_FIELD path argument (SQL null)");
    }
    if (valueNVal.isNull()) {
        throw SQLException(SQLException::data_exception_invalid_parameter,
                           "Invalid SET_FIELD value argument (SQL null)");
    }

    if (docNVal.getValueType() != ValueType::tVARCHAR) {
        throwCastSQLException(docNVal.getValueType(), ValueType::tVARCHAR);
    }

    if (pathNVal.getValueType() != ValueType::tVARCHAR) {
        throwCastSQLException(pathNVal.getValueType(), ValueType::tVARCHAR);
    }

    if (valueNVal.getValueType() != ValueType::tVARCHAR) {
        throwCastSQLException(valueNVal.getValueType(), ValueType::tVARCHAR);
    }

    int32_t lenDoc;
    const char* docChars = docNVal.getObject_withoutNull(lenDoc);
    JsonDocument doc(docChars, lenDoc);

    int32_t lenPath;
    const char* pathChars = pathNVal.getObject_withoutNull(lenPath);
    int32_t lenValue;
    const char* valueChars = valueNVal.getObject_withoutNull(lenValue);

    try {
        doc.set(pathChars, lenPath, valueChars, lenValue);
        std::string value = doc.value();
        return getTempStringValue(value.c_str(), value.length() - 1);
    } catch (std::bad_alloc& too_large) {
        std::string pathForDiagnostic(pathChars, lenPath);
        throwDynamicSQLException(
            "Insufficient memory for SET_FIELD operation with path argument: %s",
            pathForDiagnostic.c_str());
    }
}

}

