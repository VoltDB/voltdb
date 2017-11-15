/* This file is part of VoltDB.
 * Copyright (C) 2008-2017 VoltDB Inc.
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

class TupleComparingTest : public Test {
protected:

    bool assertTupleValuesEqualHelper(voltdb::TableTuple* tuple, int index) {
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
    bool assertTupleValuesEqualHelper(voltdb::TableTuple* tuple, int index, T expected, Args... args) {
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
    bool assertTupleValuesEqual(voltdb::TableTuple* tuple,
                                Args... expectedVals) {
        return assertTupleValuesEqualHelper(tuple, 0, expectedVals...);
    }






    template<typename Tuple, int I>
    struct AssertTuplesEqualHelper {
        static bool impl(Test* theTest,
                         const Tuple& expectedTuple,
                         voltdb::TableTuple *actualTuple) {
            int compareResult = Tools::nvalueCompare(std::get<I>(expectedTuple),
                                                     actualTuple->getNValue(I));
            if (compareResult != 0) {
                std::ostringstream oss;
                oss << "Values at column " << I << " are not equal; "
                    << "expected: " << Tools::nvalueFromNative(std::get<I>(expectedTuple)).debug()
                    << ", actual: " << actualTuple->getNValue(I).debug();
                theTest->fail(__FILE__, __LINE__, oss.str().c_str());
                return false;
            }

            return AssertTuplesEqualHelper<Tuple, I - 1>::impl(theTest, expectedTuple,
                                                               actualTuple);
        }
    };


    template<typename Tuple>
    struct AssertTuplesEqualHelper<Tuple, -1> {
        static bool impl(Test*,
                         const Tuple& expectedTuple,
                         voltdb::TableTuple *actualTuple) {
            return true;
        }
    };

    template<typename Tuple>
    bool assertTuplesEqual(const Tuple& expectedTuple,
                           voltdb::TableTuple* actualTuple) {
        const std::size_t numColumns = std::tuple_size<Tuple>::value;
        if (numColumns != actualTuple->columnCount()) {
            std::ostringstream oss;
            oss << "Column count mismatch, expected: "
                << numColumns << ", actual: " << actualTuple->columnCount();
            FAIL(oss.str().c_str());
        }

        return AssertTuplesEqualHelper<Tuple, numColumns - 1>::impl(this, expectedTuple, actualTuple);
    }
};

#endif // TUPLECOMPARINGTEST_HPP
