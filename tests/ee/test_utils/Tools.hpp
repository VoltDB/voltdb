/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
 *
 * This file contains original code and/or modifications of original code.
 * Any modifications made by Volt Active Data Inc. are licensed under the following
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
#pragma once
#include <vector>

#include "boost/optional.hpp"

#include "common/NValue.hpp"
#include "common/tabletuple.h"
#include "common/TupleSchema.h"
#include "common/ValueFactory.hpp"


/** Various useful methods for working with tuples and related data
    structures */
class Tools {
public:

    /** Construct an instance of TupleSchema with the given data
        types.

        For variable length types, use a pair with the type and the
        length, e.g.:

        TupleSchema* schema = Tools::buildSchema(
            tBIGINT,
            std::make_pair(tVARCHAR, 15));
    */
    template<typename... Args>
    static voltdb::TupleSchema* buildSchema(Args... args);

    /** Produce an instance of TupleSchema using the element
        types of a std::tuple type, e.g.,

        TupleSchema* schema = Tools::buildSchema<std::tuple<int64_t, std::string>>();
    */
    template<typename Tuple>
    static voltdb::TupleSchema* buildSchema();

    /** Given a tuple, populate its fields with the given native
        values, e.g.,

        Tools::setTupleValues(&tuple, 100, "foo", toDec(3.1415));
    */
    template<typename ... Args>
    static void setTupleValues(voltdb::TableTuple* tuple, Args... args);

    /** Given a voltdb::TableTuple and an instance of std::tuple,
        populate the TableTuple. */
    template<typename Tuple>
    static void initTuple(voltdb::TableTuple* tuple, const Tuple& initValues);

    /** Given two values, convert them to NValues and compare them.
        Nulls will compare as equal, if types are equal.  */
    template<typename T, typename S>
    static int nvalueCompare(T val1, S val2);

    /** Given a native value, produce its NValue equivalent. */
    template<typename T>
    static voltdb::NValue nvalueFromNative(T val);

    /** Given an optional native value, produce its NValue equivalent,
        or a NULL value of the appropriate type. */
    template<class T>
    static voltdb::NValue nvalueFromNative(boost::optional<T> possiblyNullValue);

    template<class T>
    static T nativeFromNValue(const voltdb::NValue& nval);

    /** Convert a native double to an NValue with type decimal. */
    static voltdb::NValue toDec(double val);

    enum VarcharUnits {
        CHARS,
        BYTES
    };

    typedef std::tuple<voltdb::ValueType, int32_t, bool> VarLenTypeSpec;
    struct VarcharBuilder {
        VarLenTypeSpec operator()(int32_t count, VarcharUnits units) const {
            return VarLenTypeSpec(voltdb::ValueType::tVARCHAR, count, units == BYTES);
        }
    };

    static const VarcharBuilder VARCHAR;

    /** Constructor that exists purely to help eliminate compiler
        warnings for unused functions. */
    Tools();
};

/** A helper template to convert from a native type to the equivalent
    t* enum value */
template<class NativeType>
struct ValueTypeFor;

template<>
struct ValueTypeFor<double> {
    static const voltdb::ValueType valueType = voltdb::ValueType::tDOUBLE;
};

template<>
struct ValueTypeFor<int64_t> {
    static const voltdb::ValueType valueType = voltdb::ValueType::tBIGINT;
};

template<>
struct ValueTypeFor<int32_t> {
    static const voltdb::ValueType valueType = voltdb::ValueType::tINTEGER;
};

template<>
struct ValueTypeFor<int16_t> {
    static const voltdb::ValueType valueType = voltdb::ValueType::tSMALLINT;
};

template<>
struct ValueTypeFor<int8_t> {
    static const voltdb::ValueType valueType = voltdb::ValueType::tTINYINT;
};

template<>
struct ValueTypeFor<std::string> {
    static const voltdb::ValueType valueType = voltdb::ValueType::tVARCHAR;
};

template<>
struct ValueTypeFor<const char*> {
    static const voltdb::ValueType valueType = voltdb::ValueType::tVARCHAR;
};

template<typename R>
struct ValueTypeFor<boost::optional<R>> {
    static const voltdb::ValueType valueType = ValueTypeFor<R>::valueType;
};

template<typename T>
struct IsNullable;

template<typename R>
struct IsNullable<boost::optional<R>> {
    static const bool value = true;
};

template<typename T>
struct IsNullable {
    static const bool value = false;
};

// TODO: TTInt for decimal values?

voltdb::NValue Tools::toDec(double val) {
    return voltdb::ValueFactory::getDecimalValue(val);
}

template<>
voltdb::NValue Tools::nvalueFromNative(int64_t val) {
    return voltdb::ValueFactory::getBigIntValue(val);
}

template<>
voltdb::NValue Tools::nvalueFromNative(int32_t val) {
    return voltdb::ValueFactory::getIntegerValue(val);
}

template<>
voltdb::NValue Tools::nvalueFromNative(int16_t val) {
    return voltdb::ValueFactory::getSmallIntValue(val);
}

template<>
voltdb::NValue Tools::nvalueFromNative(int8_t val) {
    return voltdb::ValueFactory::getTinyIntValue(val);
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

template<class T>
voltdb::NValue Tools::nvalueFromNative(boost::optional<T> possiblyNullValue) {
    if (! possiblyNullValue) {
        return voltdb::NValue::getNullValue(ValueTypeFor<T>::valueType);
    }
    else {
        return nvalueFromNative(*possiblyNullValue);
    }
}

template<>
voltdb::NValue Tools::nvalueFromNative(voltdb::NValue nval) {
    return nval;
}

template<>
std::string Tools::nativeFromNValue(const voltdb::NValue& nval) {
    assert(voltdb::ValuePeeker::peekValueType(nval) == voltdb::ValueType::tVARCHAR);
    int32_t valueLen;
    const char* value = voltdb::ValuePeeker::peekObject(nval, &valueLen);
    return std::string(value, valueLen);
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

template<typename Tuple, int I>
struct InitTupleHelper {
    static void impl(voltdb::TableTuple* tuple, const Tuple& initValues) {
        tuple->setNValue(I, Tools::nvalueFromNative(std::get<I>(initValues)));
        InitTupleHelper<Tuple, I - 1>::impl(tuple, initValues);
    }
};

template<typename Tuple>
struct InitTupleHelper<Tuple, -1> {
    static void impl(voltdb::TableTuple*, const Tuple&) {
    }
};

} // end unnamed namespace

template<typename Tuple>
void Tools::initTuple(voltdb::TableTuple* tuple, const Tuple& initValues) {
    const size_t NUMVALUES = std::tuple_size<Tuple>::value;
    InitTupleHelper<Tuple, NUMVALUES - 1>::impl(tuple, initValues);
}

template<typename T, typename S>
int Tools::nvalueCompare(T val1, S val2) {
    voltdb::NValue nval1 = nvalueFromNative(val1);
    voltdb::NValue nval2 = nvalueFromNative(val2);
    voltdb::ValueType vt1 = voltdb::ValuePeeker::peekValueType(nval1);
    voltdb::ValueType vt2 = voltdb::ValuePeeker::peekValueType(nval2);

    if (vt1 != vt2) {
        return static_cast<int>(vt1) - static_cast<int>(vt2);
    }

    if (nval1.isNull() != nval2.isNull()) {
        return nval1.isNull() ? -1 : 1;
    }

    if (! nval1.isNull()) {
        return nval1.compare(nval2);
    }

    return 0;  // both nulls
}

namespace {

void buildSchemaHelper(std::vector<voltdb::ValueType>* columnTypes,
                       std::vector<int32_t>* columnSizes,
                       std::vector<bool>* inBytes) {
    return;
}

template<typename... Args>
void buildSchemaHelper(std::vector<voltdb::ValueType>* columnTypes,
                       std::vector<int32_t>* columnSizes,
                       std::vector<bool>* inBytes,
                       voltdb::ValueType valueType,
                       Args... args);
template<typename... Args>
void buildSchemaHelper(std::vector<voltdb::ValueType>* columnTypes,
                       std::vector<int32_t>* columnSizes,
                       std::vector<bool>* inBytes,
                       std::pair<voltdb::ValueType, int> typeAndSize,
                       Args... args);
template<typename... Args>
void buildSchemaHelper(std::vector<voltdb::ValueType>* columnTypes,
                       std::vector<int32_t>* columnSizes,
                       std::vector<bool>* inBytes,
                       Tools::VarLenTypeSpec varLenTypeSpec,
                       Args... args);

template<typename... Args>
void buildSchemaHelper(std::vector<voltdb::ValueType>* columnTypes,
                       std::vector<int32_t>* columnSizes,
                       std::vector<bool>* inBytes,
                       voltdb::ValueType valueType,
                       Args... args) {
    assert(! isVariableLengthType(valueType));
    columnTypes->push_back(valueType);
    columnSizes->push_back(voltdb::NValue::getTupleStorageSize(valueType));
    inBytes->push_back(false);
    buildSchemaHelper(columnTypes, columnSizes, inBytes, args...);
}

template<typename... Args>
void buildSchemaHelper(std::vector<voltdb::ValueType>* columnTypes,
                       std::vector<int32_t>* columnSizes,
                       std::vector<bool>* inBytes,
                       std::pair<voltdb::ValueType, int> typeAndSize,
                       Args... args) {
    assert(isVariableLengthType(typeAndSize.first));
    columnTypes->push_back(typeAndSize.first);
    columnSizes->push_back(typeAndSize.second);
    inBytes->push_back(false);
    buildSchemaHelper(columnTypes, columnSizes, inBytes, args...);
}

template<typename... Args>
void buildSchemaHelper(std::vector<voltdb::ValueType>* columnTypes,
                       std::vector<int32_t>* columnSizes,
                       std::vector<bool>* inBytes,
                       Tools::VarLenTypeSpec varLenTypeSpec,
                       Args... args) {
    assert(isVariableLengthType(std::get<0>(varLenTypeSpec)));
    columnTypes->push_back(std::get<0>(varLenTypeSpec));
    columnSizes->push_back(std::get<1>(varLenTypeSpec));
    inBytes->push_back(std::get<2>(varLenTypeSpec));
    buildSchemaHelper(columnTypes, columnSizes, inBytes, args...);
}

} // end unnamed namespace

template<typename... Args>
voltdb::TupleSchema* Tools::buildSchema(Args... args) {
    std::vector<voltdb::ValueType> columnTypes;
    std::vector<int32_t> columnSizes;
    std::vector<bool> inBytes;

    buildSchemaHelper(&columnTypes, &columnSizes, &inBytes, args...);

    std::vector<bool> allowNull(columnTypes.size(), true);

    return voltdb::TupleSchema::createTupleSchema(columnTypes, columnSizes, allowNull, inBytes);
}

namespace {

template<typename Tuple, std::size_t I>
struct BuildSchemaTupleHelper {
    static void impl(std::vector<voltdb::ValueType>* columnTypes,
                     std::vector<bool>* allowNulls) {
        const std::size_t INDEX = std::tuple_size<Tuple>::value - I;
        typedef typename std::tuple_element<INDEX, Tuple>::type ElemType;

        voltdb::ValueType vt = ValueTypeFor<ElemType>::valueType;
        columnTypes->push_back(vt);

        bool isNullable = IsNullable<ElemType>::value;
        allowNulls->push_back(isNullable);

        BuildSchemaTupleHelper<Tuple, I - 1>::impl(columnTypes, allowNulls);
    }
};

template<typename Tuple>
struct BuildSchemaTupleHelper<Tuple, 0> {
    static void impl(std::vector<voltdb::ValueType>*,
                     std::vector<bool>*) {
    }
};

} // end unnamed namespace

template<typename Tuple>
voltdb::TupleSchema* Tools::buildSchema() {
    const size_t NUMVALUES = std::tuple_size<Tuple>::value;
    std::vector<voltdb::ValueType> columnTypes;
    std::vector<bool> allowNulls;

    // populate columnTypes from values
    BuildSchemaTupleHelper<Tuple, NUMVALUES>::impl(&columnTypes,
                                                   &allowNulls);

    std::vector<int32_t> columnSizes;
    assert(columnTypes.size() == allowNulls.size());
    for (int i = 0; i < columnTypes.size(); ++i) {
        voltdb::ValueType vt = columnTypes[i];
        if (isVariableLengthType(columnTypes[i])) {
            columnSizes.push_back(4096); // good enough for testing
        }
        else {
            columnSizes.push_back(voltdb::NValue::getTupleStorageSize(vt));
        }
    }

    std::vector<bool> inBytes(columnSizes.size(), false);
    return voltdb::TupleSchema::createTupleSchema(columnTypes, columnSizes, allowNulls, inBytes);
}

inline Tools::Tools() {
    buildSchemaHelper(NULL, NULL, NULL);
    setTupleValuesHelper(NULL, 0);
}

