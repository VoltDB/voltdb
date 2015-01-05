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

#include "harness.h"

#include "common/Pool.hpp"

using namespace std;
using namespace voltdb;

class PoolTest : public Test {
public:
    PoolTest() {};
};

TEST_F(PoolTest, SimpleTest) {
    Pool testPool;
    void* space;
    space = testPool.allocate(100000);
    EXPECT_NE(space, NULL);
}

TEST_F(PoolTest, OverflowTest) {
    Pool testPool;
    void* space;
    // Size the allocations to allow some chunk packing before overflowing several chunks.
    space = testPool.allocate(100000);
    EXPECT_NE(space, NULL);
    space = testPool.allocate(100000);
    EXPECT_NE(space, NULL);
    space = testPool.allocate(100000);
    EXPECT_NE(space, NULL);
    space = testPool.allocate(100000);
    EXPECT_NE(space, NULL);
    space = testPool.allocate(100000);
    EXPECT_NE(space, NULL);
    space = testPool.allocate(100000);
    EXPECT_NE(space, NULL);
}

TEST_F(PoolTest, ChunkyOverflowTest) {
    Pool testPool;
    void* space;
    // Size the allocations to not allow chunk packing before overflowing several chunks.
    space = testPool.allocate(200000);
    EXPECT_NE(space, NULL);
    space = testPool.allocate(200000);
    EXPECT_NE(space, NULL);
    space = testPool.allocate(200000);
    EXPECT_NE(space, NULL);
}

TEST_F(PoolTest, OversizeTest) {
    Pool testPool;
    void* space;
    // Size the allocations to force oversized chunks.
    space = testPool.allocate(1000000);
    EXPECT_NE(space, NULL);
    space = testPool.allocate(1000000);
    EXPECT_NE(space, NULL);
    space = testPool.allocate(1000000);
    EXPECT_NE(space, NULL);
}

int main() {
    return TestSuite::globalInstance()->runAll();
}
