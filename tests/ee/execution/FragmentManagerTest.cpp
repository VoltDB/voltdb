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

#include <iostream>
#include <map>
#include <algorithm>
#include <cstdlib>
#include <cmath>
#include <cstdio>
#include <sys/time.h>
#include "harness.h"
#include "execution/FragmentManager.h"

using namespace voltdb;
using namespace std;

class FragmentManagerTest : public Test {
public:
};

TEST_F(FragmentManagerTest, Basic) {
    voltdb::FragmentManager fm(3);

    char plan1[] = "hello";
    char plan2[] = "hello";
    char plan3[] = "why";
    char plan4[] = "booberry";
    char plan5[] = "whale";

    int64_t fragId = 0;
    bool cacheHit = false;

    cacheHit = fm.upsert(plan1, (int32_t)strlen(plan1), fragId);
    ASSERT_FALSE(cacheHit);
    ASSERT_TRUE(fragId == -1);

    fragId = fm.purgeNext();
    ASSERT_TRUE(fragId == 0);

    cacheHit = fm.upsert(plan3, (int32_t)strlen(plan3), fragId);
    ASSERT_FALSE(cacheHit);
    ASSERT_TRUE(fragId == -2);

    fragId = fm.purgeNext();
    ASSERT_TRUE(fragId == 0);

    cacheHit = fm.upsert(plan4, (int32_t)strlen(plan4), fragId);
    ASSERT_FALSE(cacheHit);
    ASSERT_TRUE(fragId == -3);

    fragId = fm.purgeNext();
    ASSERT_TRUE(fragId == 0);

    cacheHit = fm.upsert(plan2, (int32_t)strlen(plan2), fragId);
    ASSERT_TRUE(cacheHit);
    ASSERT_TRUE(fragId == -1);

    fragId = fm.purgeNext();
    ASSERT_TRUE(fragId == 0);

    cacheHit = fm.upsert(plan4, (int32_t)strlen(plan4), fragId);
    ASSERT_TRUE(cacheHit);
    ASSERT_TRUE(fragId == -3);

    fragId = fm.purgeNext();
    ASSERT_TRUE(fragId == 0);

    cacheHit = fm.upsert(plan5, (int32_t)strlen(plan5), fragId);
    ASSERT_FALSE(cacheHit);
    ASSERT_TRUE(fragId == -6);

    fragId = fm.purgeNext();
    ASSERT_TRUE(fragId == -2);

    plan1[0] = 'a';
    cacheHit = fm.upsert(plan1, (int32_t)strlen(plan1), fragId);
    ASSERT_FALSE(cacheHit);
    ASSERT_TRUE(fragId == -7);
}

int main() {
    assert(printf("Assertions are enabled\n"));
    return TestSuite::globalInstance()->runAll();
}
