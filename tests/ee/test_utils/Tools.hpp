/* This file is part of VoltDB.
 * Copyright (C) 2008-2017 VoltDB Inc.
 *
 * This file contains original code and/or modifications of original code.
 * Any modifications made by VoltDB Inc. are licensed under the following
 * terms and conditions:
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */
/* Copyright (C) 2008 by H-Store Project
 * Brown University
 * Massachusetts Institute of Technology
 * Yale University
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

#include <vector>

#include "common/NValue.hpp"
#include "common/tabletuple.h"
#include "common/TupleSchema.h"
#include "common/ValueFactory.hpp"

#ifndef _TEST_EE_TEST_UTILS_TOOLS_HPP_
#define _TEST_EE_TEST_UTILS_TOOLS_HPP_

/** Various useful methods for working with tuples and related data
    structures */
class Tools {
public:

    /** Construct an instance of TupleSchema with the given data
        types.

        For variable length types, use a pair with the type and the
        length, e.g.:

        TupleSchema* schema = Tools::buildSchema(
            VALUE_TYPE_BIGINT,
            std::make_pair(VALUE_TYPE_VARCHAR, 15));
    */
    template<typename... Args>
    static voltdb::TupleSchema* buildSchema(Args... args);

    /** Given a tuple, populate its fields with the given native
        values, e.g.,

        Tools::setTupleValues(&tuple, 100, "foo", toDec(3.1415));
    */
    template<typename ... Args>
    static void setTupleValues(voltdb::TableTuple* tuple, Args... args);

    /** Given a native value, produce its NValue equivalent. */
    template<typename T>
    static voltdb::NValue nvalueFromNative(T val);

    /** Convert a native double to an NValue with type decimal. */
    static voltdb::NValue toDec(double val);

    /** Constructor that exists purely to help eliminate compiler
        warnings for unused functions. */
    Tools();
};


voltdb::NValue Tools::toDec(double val) {
    return voltdb::ValueFactory::getDecimalValue(val);
}

template<>
voltdb::NValue Tools::nvalueFromNative(int64_t val) {
    return voltdb::ValueFactory::getBigIntValue(val);
}

template<>
voltdb::NValue Tools::nvalueFromNative(int val) {
    return nvalueFromNative(static_cast<int64_t>(val));
}

template<>
voltdb::NValue Tools::nvalueFromNative(std::string val) {
    return voltdb::ValueFactory::getTempStringValue(val);
}

template<>
voltdb::NValue Tools::nvalueFromNative(const char* val) {
    return voltdb::ValueFactory::getTempStringValue(val, strlen(val));
}

template<>
voltdb::NValue Tools::nvalueFromNative(double val) {
    return voltdb::ValueFactory::getDoubleValue(val);
}

template<>
voltdb::NValue Tools::nvalueFromNative(voltdb::NValue nval) {
    return nval;
}

namespace {

void setTupleValuesHelper(voltdb::TableTuple* tuple, int index) {
    assert(tuple->getSchema()->columnCount() == index);
}

template<typename T, typename ... Args>
void setTupleValuesHelper(voltdb::TableTuple* tuple, int index, T arg, Args... args) {
    tuple->setNValue(index, Tools::nvalueFromNative(arg));
    setTupleValuesHelper(tuple, index + 1, args...);
}

} // end unnamed namespace

template<typename ... Args>
void Tools::setTupleValues(voltdb::TableTuple* tuple, Args... args) {
    setTupleValuesHelper(tuple, 0, args...);
}

namespace {

void buildSchemaHelper(std::vector<voltdb::ValueType>* columnTypes,
                       std::vector<int32_t>* columnSizes) {
    return;
}

template<typename... Args>
void buildSchemaHelper(std::vector<voltdb::ValueType>* columnTypes,
                       std::vector<int32_t>* columnSizes,
                       voltdb::ValueType valueType,
                       Args... args);
template<typename... Args>
void buildSchemaHelper(std::vector<voltdb::ValueType>* columnTypes,
                       std::vector<int32_t>* columnSizes,
                       std::pair<voltdb::ValueType, int> typeAndSize,
                       Args... args);

template<typename... Args>
void buildSchemaHelper(std::vector<voltdb::ValueType>* columnTypes,
                       std::vector<int32_t>* columnSizes,
                       voltdb::ValueType valueType,
                       Args... args) {
    assert(! isVariableLengthType(valueType));
    columnTypes->push_back(valueType);
    columnSizes->push_back(voltdb::NValue::getTupleStorageSize(valueType));
    buildSchemaHelper(columnTypes, columnSizes, args...);
}

template<typename... Args>
void buildSchemaHelper(std::vector<voltdb::ValueType>* columnTypes,
                       std::vector<int32_t>* columnSizes,
                       std::pair<voltdb::ValueType, int> typeAndSize,
                       Args... args) {
    assert(isVariableLengthType(typeAndSize.first));
    columnTypes->push_back(typeAndSize.first);
    columnSizes->push_back(typeAndSize.second);
    buildSchemaHelper(columnTypes, columnSizes, args...);
}

} // end unnamed namespace

template<typename... Args>
voltdb::TupleSchema* Tools::buildSchema(Args... args) {
    std::vector<voltdb::ValueType> columnTypes;
    std::vector<int32_t> columnSizes;

    buildSchemaHelper(&columnTypes, &columnSizes, args...);

    std::vector<bool> allowNull(columnTypes.size(), true);
    std::vector<bool> inBytes(columnTypes.size(), false);

    return voltdb::TupleSchema::createTupleSchema(columnTypes, columnSizes, allowNull, inBytes);
}

inline Tools::Tools() {
    buildSchemaHelper(NULL, NULL);
}

#endif // _TEST_EE_TEST_UTILS_TOOLS_HPP_
