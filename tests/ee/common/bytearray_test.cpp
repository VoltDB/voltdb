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
#include "harness.h"
#include "common/bytearray.h"

using namespace std;
using namespace voltdb;

class ByteArrayTest : public Test {
public:
    ByteArrayTest() {};

// memory leak tests for passing
    ByteArray passArray(ByteArray passed) {
        return passed;
    };
    GenericArray<int> passArray2(GenericArray<int> passed) {
        GenericArray<int> ret = passed;
        for (int i = 0; i < 10; ++i) ret = passArray3(ret);
        return ret;
    };
    GenericArray<int> passArray3(GenericArray<int> passed) {
        return GenericArray<int>(passed);
    };
};
TEST_F(ByteArrayTest, BasicTest) {
    ByteArray data;
    EXPECT_EQ(true, data.isNull());

    data = ByteArray(10);
    EXPECT_EQ(false, data.isNull());
    EXPECT_EQ(10, data.length());
    data.assign("hogehoge", 0, 8);
    EXPECT_EQ(10, data.length());
    EXPECT_EQ("hogehoge", string(data.data(), 8));
    EXPECT_NE("fuga", string(data.data(), 8));

    data = passArray(data);
    EXPECT_EQ(false, data.isNull());
    EXPECT_EQ(10, data.length());
    EXPECT_EQ("hogehoge", string(data.data(), 8));
    EXPECT_NE("fuga", string(data.data(), 8));


    ByteArray data2("0123456789abcdef", 16);
    EXPECT_EQ(false, data2.isNull());
    EXPECT_EQ(16, data2.length());
    EXPECT_EQ("0123456789abcdef", string(data2.data(), 16));

    ByteArray data3("xyz", 3);
    ByteArray concated = data2 + data3;
    EXPECT_EQ(false, concated.isNull());
    EXPECT_EQ(16 + 3, concated.length());
    EXPECT_EQ("0123456789abcdefxyz", string(concated.data(), 16 + 3));
    concated[10] = 'p';
    EXPECT_EQ("0123456789pbcdefxyz", string(concated.data(), 16 + 3));

    data3.data()[1] = 'c';
    concated = data3 + data2;
    EXPECT_EQ(false, concated.isNull());
    EXPECT_EQ(16 + 3, concated.length());
    EXPECT_EQ("xcz0123456789abcdef", string(concated.data(), 16 + 3));

    data.reset();
    EXPECT_EQ(true, data.isNull());
}
TEST_F(ByteArrayTest, GenericTest) {
    int values[] = {1,4,5,10};
    int values2[] = {25, 30, 10};
    GenericArray<int> data(values, 4);
    EXPECT_EQ(false, data.isNull());
    EXPECT_EQ(4, data.length());

    GenericArray<int> data2(3);
    EXPECT_EQ(false, data2.isNull());
    EXPECT_EQ(3, data2.length());
    data2.assign(values2, 0, 3);

    GenericArray<int> data3 = data + data2;
    EXPECT_EQ(false, data3.isNull());
    EXPECT_EQ(7, data3.length());
    EXPECT_EQ(1, data3.data()[0]);
    EXPECT_EQ(4, data3.data()[1]);
    EXPECT_EQ(5, data3.data()[2]);
    EXPECT_EQ(10, data3.data()[3]);
    EXPECT_EQ(25, data3.data()[4]);
    EXPECT_EQ(30, data3.data()[5]);
    EXPECT_EQ(10, data3.data()[6]);

    GenericArray<int> data4 = passArray2(data3);
    EXPECT_EQ(false, data4.isNull());
    EXPECT_EQ(7, data4.length());
    EXPECT_EQ(1, data4[0]);
    EXPECT_EQ(4, data4[1]);
    EXPECT_EQ(5, data4[2]);
    EXPECT_EQ(10, data4[3]);
    EXPECT_EQ(25, data4[4]);
    EXPECT_EQ(30, data4[5]);
    EXPECT_EQ(10, data4[6]);
}
int main() {
    return TestSuite::globalInstance()->runAll();
}
