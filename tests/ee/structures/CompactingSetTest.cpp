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
#include "structures/CompactingPool.h"
#include "structures/CompactingSet.h"
#include "common/ThreadLocalPool.h"

using voltdb::CompactingSet;
using voltdb::SizePtrPair;
using voltdb::SizePtrPairComparator;

class CompactingSetTest : public Test {
public:
    CompactingSetTest() {
    }

    ~CompactingSetTest() {
    }
};

/**
 * A simple comparator that works for any kind of pointer.
 */
struct PointerComparator {
    int operator()(const void* v1, const void* v2) const {
        if (v1 < v2) {
            return -1;
        }

        if (v1 > v2) {
            return 1;
        }

        return 0;
    }
};


TEST_F(CompactingSetTest, Simple) {
    CompactingSet<int*, PointerComparator> mySet;
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

TEST_F(CompactingSetTest, RangeScan) {
    CompactingSet<SizePtrPair, SizePtrPairComparator> mySet;
    std::set<SizePtrPair> stdSet;

    std::vector<SizePtrPair> pairs = {
        {2, new int32_t(72)},
        {2, new int32_t(73)},
        {2, new int32_t(74)},
        {4, new int32_t(75)},
        {4, new int32_t(76)},
        {4, new int32_t(77)},
        {8, new int32_t(78)},
        {8, new int32_t(79)},
        {8, new int32_t(80)}
    };

    for (int i = 0; i < pairs.size(); ++i) {
        mySet.insert(pairs[i]);
        stdSet.insert(pairs[i]);
    }

    BOOST_FOREACH(int32_t size, std::vector<int32_t>({2, 4, 8})) {
        SizePtrPair lowerKey(size, static_cast<void*>(NULL));
        SizePtrPair upperKey(size + 1, static_cast<void*>(NULL));

        auto myIt = mySet.lowerBound(lowerKey);
        auto stdIt = stdSet.lower_bound(lowerKey);

        auto myUpperIt = mySet.upperBound(upperKey);
        auto stdUpperIt = stdSet.upper_bound(upperKey);

        while (stdIt != stdUpperIt) {
            SizePtrPair expected = *stdIt;
            SizePtrPair actual = myIt.key();
            ASSERT_EQ(expected, actual);
            ++stdIt;
            myIt.moveNext();
        }

        ASSERT_TRUE(myIt.equals(myUpperIt));
    }

    auto it = mySet.begin();
    while (! it.isEnd()) {
        delete static_cast<int32_t*>(it.key().second);
        mySet.erase(it.key());
        it = mySet.begin();
    }
}

int main() {
    ::srand(777);
    return TestSuite::globalInstance()->runAll();
}
