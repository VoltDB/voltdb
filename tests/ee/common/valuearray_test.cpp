/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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

#include <string>
#include <vector>
#include "harness.h"
#include "common/types.h"
#include "common/NValue.hpp"
#include "common/ValueFactory.hpp"
#include "common/valuevector.h"
#include "common/ValuePeeker.hpp"
#include "common/ThreadLocalPool.h"


using namespace std;
using namespace voltdb;

class ValueArrayTest : public Test {
public:
    ThreadLocalPool m_pool;
    ValueArrayTest() {};
};
TEST_F(ValueArrayTest, BasicTest) {
    vector<NValue> cachedStringValues;
    NValueArray array1(3);
    NValueArray array2(3);
    EXPECT_EQ(3, array1.size());
    array1[0] = ValueFactory::getBigIntValue(10);
    EXPECT_EQ(VALUE_TYPE_BIGINT, ValuePeeker::peekValueType(array1[0]));
    EXPECT_TRUE(ValueFactory::getBigIntValue(10).op_equals(array1[0]).isTrue());
    array2[0] = array1[0];
    EXPECT_EQ(VALUE_TYPE_BIGINT, ValuePeeker::peekValueType(array2[0]));
    EXPECT_TRUE(ValueFactory::getBigIntValue(10).op_equals(array2[0]).isTrue());
    EXPECT_TRUE(array1[0].op_equals(array2[0]).isTrue());

    cachedStringValues.push_back(ValueFactory::getStringValue("str1"));
    array1[1] = cachedStringValues.back();
    EXPECT_EQ(VALUE_TYPE_VARCHAR, ValuePeeker::peekValueType(array1[1]));
    cachedStringValues.push_back(ValueFactory::getStringValue("str1"));
    EXPECT_TRUE(cachedStringValues.back().op_equals(array1[1]).isTrue());
    cachedStringValues.push_back(ValueFactory::getStringValue("str2"));
    array2[1] = cachedStringValues.back();
    EXPECT_TRUE(array1[1].op_notEquals(array2[1]).isTrue());
    cachedStringValues.push_back(ValueFactory::getStringValue("str2"));
    EXPECT_TRUE(cachedStringValues.back().op_equals(array2[1]).isTrue());

    array1[2] = ValueFactory::getDoubleValue(0.01f);
    array2[2] = ValueFactory::getDoubleValue(0.02f);
    EXPECT_TRUE(array1[2].op_lessThan(array2[2]).isTrue());
    EXPECT_FALSE(array1[2].op_greaterThan(array2[2]).isTrue());
    EXPECT_FALSE(array1[2].op_equals(array2[2]).isTrue());

    EXPECT_TRUE(array1 < array2);
    EXPECT_FALSE(array1 > array2);
    EXPECT_FALSE(array1 == array2);

    cachedStringValues.push_back(ValueFactory::getStringValue("str1"));
    array2[1] = cachedStringValues.back();
    array2[2] = ValueFactory::getDoubleValue(0.01f);
    EXPECT_TRUE(array1 == array2);
    EXPECT_FALSE(array1 != array2);

    for (vector<NValue>::const_iterator i = cachedStringValues.begin();
         i != cachedStringValues.end();
         i++) {
        (*i).free();
    }
}
int main() {
    return TestSuite::globalInstance()->runAll();
}
