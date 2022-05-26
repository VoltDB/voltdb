/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
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

#ifndef TUPLECOMPARINGTEST_HPP
#define TUPLECOMPARINGTEST_HPP

#include <sstream>

#include "harness.h"

#include "test_utils/Tools.hpp"

#include "common/tabletuple.h"
#include "common/types.h"
#include "common/NValue.hpp"

/**
 * A helpful macro that can be used by tests that inherit from the class below:
 * Example: (for TableTuple with schema BIGINT, VARCHAR)
 *       voltdb::TableTuple tuple = ...;
 *       typedef std::tuple<int64_t, std::string> Tuple;
 *       ASSERT_TUPLES_EQ(Tuple{32, "foo"}, tuple);
 *
 * May also be used with two instances of voltdb::TableTuple.
 */
#define ASSERT_TUPLES_EQ(refTuple, voltdbTuple) \
    do { \
        bool rc = assertTuplesEqual(refTuple, voltdbTuple, __FILE__, __LINE__); \
        if (! rc) { \
            return; \
        } \
    } while (0)

/**
 * EE unit tests can inherit from this class to use the handy methods
 * below to assert that voltdb::TableTuples contain expected values.
 */
class TupleComparingTest : public Test {
protected:

    /** Given a tuple, assert that it contains the specified values,
        which may be specified as native types.

        Will call FAIL with an appropriate diagnostic, and return
        false if tuples does not contain expected values.

        Example: (for a tuple with types BIGINT and VARCHAR)
        assertTupleValuesEqual(&tuple, int64_t(1), "foo");
     */
    template<typename... Args>
    bool assertTupleValuesEqual(voltdb::TableTuple* tuple, Args... expectedVals);

    /** Given an expected tuple of type std::tuple<>, compare it with
        the given voltdb::TableTuple.

        Will call FAIL with an appropriate diagnostic, and return
        false if tuples does not contain expected values.

        Example: (for TableTuple with schema BIGINT, VARCHAR)
        typedef std::tuple<int64_t, std::string> Tuple;
        ASSERT_TUPLES_EQ(Tuple{32, "foo"}, &tuple);
    */
    template<typename Tuple>
    bool assertTuplesEqual(const Tuple& expectedTuple,
                           const voltdb::TableTuple& actualTuple,
                           const std::string& theFile,
                           int theLine);

    bool assertTuplesEqual(const voltdb::TableTuple& expectedTuple,
                           const voltdb::TableTuple& actualTuple,
                           const std::string& theFile,
                           int theLine);

private:
    inline bool assertTupleValuesEqualHelper(voltdb::TableTuple* tuple, int index);

    template<typename T, typename ...Args>
    bool assertTupleValuesEqualHelper(voltdb::TableTuple* tuple, int index, T expected, Args... args);
};



bool TupleComparingTest::assertTupleValuesEqualHelper(voltdb::TableTuple* tuple, int index) {
    int expectedColCount = tuple->getSchema()->columnCount();
    if (expectedColCount != index) {
        std::ostringstream oss;
        oss << "Wrong number of values provided: expected " << expectedColCount
            << ", actual " << index;
        FAIL(oss.str().c_str());
        return false;
    }

    return true;
}

template<typename T, typename ...Args>
bool TupleComparingTest::assertTupleValuesEqualHelper(voltdb::TableTuple* tuple,
                                                      int index,
                                                      T expected,
                                                      Args... args) {
    if (index >= tuple->getSchema()->columnCount()) {
        FAIL("More values provided than columns in tuple");
        return false;
    }

    voltdb::NValue expectedNVal = Tools::nvalueFromNative(expected);
    voltdb::NValue actualNVal = tuple->getNValue(index);

    voltdb::ValueType expectedType = voltdb::ValuePeeker::peekValueType(expectedNVal);
    voltdb::ValueType actualType = voltdb::ValuePeeker::peekValueType(actualNVal);
    if (expectedType != actualType) {
        std::ostringstream oss;
        oss << "Comparing field " << index << ", types do not match : "
            << "expected " << getTypeName(expectedType)
            << ", actual " << getTypeName(actualType);
        FAIL(oss.str().c_str());
        return false;
    }

    int cmp = expectedNVal.compare(actualNVal);
    if (cmp != 0) {
        std::ostringstream oss;
        oss << "Comparing field " << index << ", values do not match: "
            << "expected " << expectedNVal.debug()
            << ", actual " << actualNVal.debug();
        FAIL(oss.str().c_str());
        return false;
    }

    return assertTupleValuesEqualHelper(tuple, index + 1, args...);
}

template<typename... Args>
bool TupleComparingTest::assertTupleValuesEqual(voltdb::TableTuple* tuple,
                                Args... expectedVals) {
    return assertTupleValuesEqualHelper(tuple, 0, expectedVals...);
}

namespace {
template<typename Tuple, int I>
struct AssertTuplesEqualHelper {
    static bool impl(Test* theTest,
                     const Tuple& expectedTuple,
                     const voltdb::TableTuple& actualTuple,
                     const std::string& theFile,
                     int theLine) {
        int compareResult = Tools::nvalueCompare(std::get<I>(expectedTuple),
                                                 actualTuple.getNValue(I));
        if (compareResult != 0) {
            std::ostringstream oss;
            oss << "Values at column " << I << " are not equal; "
                << "expected: " << Tools::nvalueFromNative(std::get<I>(expectedTuple)).debug()
                << ", actual: " << actualTuple.getNValue(I).debug();
            theTest->fail(theFile.c_str(), theLine, oss.str().c_str());
            return false;
        }

        return AssertTuplesEqualHelper<Tuple, I - 1>::impl(theTest,
                                                           expectedTuple,
                                                           actualTuple,
                                                           theFile,
                                                           theLine);
    }
};

template<typename Tuple>
struct AssertTuplesEqualHelper<Tuple, -1> {
    static bool impl(Test*,
                     const Tuple& expectedTuple,
                     const voltdb::TableTuple& actualTuple,
                     const std::string& theFile,
                     int theLine) {
        return true;
    }
};
}

template<typename Tuple>
bool TupleComparingTest::assertTuplesEqual(const Tuple& expectedTuple,
                                           const voltdb::TableTuple& actualTuple,
                                           const std::string& theFile,
                                           int theLine) {
    const std::size_t numColumns = std::tuple_size<Tuple>::value;
    if (numColumns != actualTuple.columnCount()) {
        std::ostringstream oss;
        oss << "Column count mismatch, expected: "
            << numColumns << ", actual: " << actualTuple.columnCount();
        FAIL(oss.str().c_str());
    }

    return AssertTuplesEqualHelper<Tuple, numColumns - 1>::impl(this,
                                                                expectedTuple,
                                                                actualTuple,
                                                                theFile,
                                                                theLine);
}

bool TupleComparingTest::assertTuplesEqual(const voltdb::TableTuple& expectedTuple,
                                           const voltdb::TableTuple& actualTuple,
                                           const std::string& theFile,
                                           int theLine) {
    int expectedColumnCount = expectedTuple.columnCount();
    int actualColumnCount = actualTuple.columnCount();
    if (expectedColumnCount != actualColumnCount) {
        std::ostringstream oss;
        oss << "Tuple does not have expected number of columns; "
            << "expected: " << expectedColumnCount
            << ", actual: " << actualColumnCount;
        fail(theFile.c_str(), theLine, oss.str().c_str());
        return false;
    }

    for (int i = 0; i < expectedColumnCount; ++i) {
        voltdb::NValue expectedVal = expectedTuple.getNValue(i);
        voltdb::NValue actualVal = actualTuple.getNValue(i);
        int compareResult = Tools::nvalueCompare(expectedVal, actualVal);
        if (compareResult != 0) {
            std::ostringstream oss;
            oss << "Values at column " << i << " are not equal; "
                << "expected: " << expectedVal.debug()
                << ", actual: " << actualVal.debug();
            fail(theFile.c_str(), theLine, oss.str().c_str());
            return false;
        }
    }

    return true;
}

#endif // TUPLECOMPARINGTEST_HPP
