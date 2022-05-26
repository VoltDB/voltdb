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
/* Copyright (C) 2008
 * Evan Jones
 * Massachusetts Institute of Technology
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

#include "common/LargeTempTableBlockId.hpp"

#include "harness.h"

using namespace voltdb;

class LargeTempTableBlockIdTest : public Test {
public:
    LargeTempTableBlockIdTest() {}
};

TEST_F(LargeTempTableBlockIdTest, InitializeAndTest) {
    voltdb::LargeTempTableBlockId blockId(100, 0);
    voltdb::LargeTempTableBlockId newBlockId(100, 1);
    EXPECT_EQ(0, blockId.getBlockCounter());
    EXPECT_EQ(100, blockId.getSiteId());
    for (int idx = 1; idx <= 100; idx += 1) {
        newBlockId = ++blockId;
        // The blockId is incremented.
        EXPECT_EQ(idx, blockId.getBlockCounter());
        EXPECT_EQ(100, blockId.getSiteId());
        // newBlockId gets it.
        EXPECT_EQ(idx, newBlockId.getBlockCounter());
        EXPECT_EQ(100, newBlockId.getSiteId());
    }
    blockId = voltdb::LargeTempTableBlockId(100, 1);
    newBlockId = voltdb::LargeTempTableBlockId(110, 1);
    EXPECT_EQ(1, (blockId < newBlockId) ? 1 : 0);
    EXPECT_EQ(0, (blockId == newBlockId) ? 1 : 0);

    blockId = voltdb::LargeTempTableBlockId(100, 1);
    newBlockId = voltdb::LargeTempTableBlockId(100, 2);
    EXPECT_EQ(1, (blockId < newBlockId) ? 1 : 0);
    EXPECT_EQ(0, (blockId == newBlockId) ? 1 : 0);

    blockId = voltdb::LargeTempTableBlockId(100, 1);
    newBlockId = voltdb::LargeTempTableBlockId(100, 1);
    EXPECT_EQ(0, (blockId < newBlockId) ? 1 : 0);
    EXPECT_EQ(1, (blockId == newBlockId) ? 1 : 0);
}

int main() {
    return TestSuite::globalInstance()->runAll();
}
