/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
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

#include <cstdlib>
#include <set>

#include "boost/foreach.hpp"
#include "harness.h"
#include "structures/CompactingSet.h"

using voltdb::CompactingSet;

class CompactingSetTest : public Test {
public:
    CompactingSetTest() {
    }

    ~CompactingSetTest() {
    }
};

TEST_F(CompactingSetTest, Simple) {
    CompactingSet<int*> mySet;
    std::set<int*> stdSet;

    ASSERT_TRUE(mySet.empty());
    ASSERT_EQ(0, mySet.size());

    const int UB = 1000;
    for (int i = 0; i < UB; ++i) {
        int *val = new int(rand());
        auto itBoolPair = stdSet.insert(val);
        bool  b = mySet.insert(val);
        ASSERT_EQ(itBoolPair.second, b);
    }

    ASSERT_FALSE(mySet.empty());
    ASSERT_EQ(stdSet.size(), mySet.size());

    // If you try to insert a duplicate value, insert should return false.
    ASSERT_FALSE(mySet.insert(*(stdSet.begin())));

    BOOST_FOREACH(int* val, stdSet) {
        ASSERT_TRUE(mySet.exists(val));
        auto it = mySet.find(val);
        ASSERT_EQ(val, it.key());
    }

    auto it = mySet.begin();
    while (! it.isEnd()) {
        auto stdIt = stdSet.find(it.key());
        ASSERT_EQ(*stdIt, it.key());
        it.moveNext();
    }

    // Delete all the elements (this also prevents a leak in memcheck mode)
    while (mySet.size() > 0) {
        auto it = mySet.begin();
        int *v = it.key();

        ASSERT_EQ(1, stdSet.erase(v));
        ASSERT_TRUE(mySet.erase(v));

        delete v;
    }

    ASSERT_TRUE(mySet.empty());
    ASSERT_EQ(0, mySet.size());
}


int main() {
    ::srand(777);
    return TestSuite::globalInstance()->runAll();
}
