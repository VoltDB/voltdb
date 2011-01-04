/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB Inc.
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
#include <sys/time.h>
#include <boost/unordered_map.hpp>
#include "harness.h"
#include "structures/CompactingHashTable.h"

using namespace voltdb;
using namespace std;

class StringComparator {
public:
    int comparisons;
    StringComparator() : comparisons(0) {}
    ~StringComparator() { printf("Compared Strings ### %d ### times\n", comparisons); fflush(stdout); }

    inline int operator()(const string &lhs, const string &rhs) const {
        int *comp = const_cast<int*>(&comparisons);
        *comp = comparisons + 1;
        return lhs.compare(rhs);
    }
};

class IntComparator {
public:
    inline int operator()(const int &lhs, const int &rhs) const {
        if (lhs > rhs) return 1;
        else if (lhs < rhs) return -1;
        else return 0;
    }
};

class CompactingHashTest : public Test {
public:
    std::string keyFromInt(int i) {
        char buf[256];
        snprintf(buf, 256, "%010d", i);
        string val = buf;
        return val;
    }
};

TEST_F(CompactingHashTest, Benchmark) {
    const int ITERATIONS = 10000;

    boost::unordered_map<uint64_t,uint64_t> stl;
    voltdb::CompactingHashTable<uint64_t,uint64_t> volt(true);

    timeval tp;
    gettimeofday(&tp, NULL);
    printf("Time: %ld, %ld\n", (long int)tp.tv_sec, (long int)tp.tv_usec);
    double t1 = static_cast<double>(tp.tv_sec * 1000 + tp.tv_usec / 1000);
    fflush(stdout);

    map<string,string>::const_iterator iter_stl;

    for (uint64_t val = 0; val < ITERATIONS; val++) {
        stl.insert(pair<uint64_t,uint64_t>(val,val));
    }

    for (uint64_t val = 0; val < ITERATIONS; val += 2) {
        stl.erase(val);
    }

    gettimeofday(&tp, NULL);
    printf("Time: %ld, %ld\n", (long int)tp.tv_sec, (long int)tp.tv_usec);
    double t2 = static_cast<double>(tp.tv_sec * 1000 + tp.tv_usec / 1000);
    printf("Time elapsed: %.2f\n", (t2 - t1) / static_cast<double>(1000));
    fflush(stdout);

    voltdb::CompactingHashTable<uint64_t,uint64_t>::iterator iter;

    for (uint64_t val = 0; val < ITERATIONS; val++) {
        //cout << "Inserting: " << val << endl;
        volt.insert(val, val);

        ASSERT_TRUE(volt.size() == val + 1);
    }

    for (uint64_t val = 0; val < ITERATIONS; val += 2) {
        iter = volt.find(val);
        ASSERT_FALSE(iter.isEnd());
        volt.erase(iter);
        iter = volt.find(val);
        ASSERT_TRUE(iter.isEnd());
    }

    for (uint64_t val = 0; val < ITERATIONS; val++) {
        iter = volt.find(val);
        if ((val % 2) == 0) {
            ASSERT_TRUE(iter.isEnd());
        }
        else {
            ASSERT_FALSE(iter.isEnd());
            ASSERT_TRUE(iter.value() == val);
        }
    }

    gettimeofday(&tp, NULL);
    printf("Time: %ld, %ld\n", (long int)tp.tv_sec, (long int)tp.tv_usec);
    double t3 = static_cast<double>(tp.tv_sec * 1000 + tp.tv_usec / 1000);
    printf("Time elapsed: %.2f\n", (t3 - t2) / static_cast<double>(1000));
    fflush(stdout);

    ASSERT_TRUE(volt.verify());
}

TEST_F(CompactingHashTest, BenchmarkDel) {
    const int ITERATIONS = 1000;

    boost::unordered_multimap<string,int> stl;
    voltdb::CompactingHashTable<string,int> volt(false);

    boost::unordered_multimap<string,int>::const_iterator iter_stl;

    printf("Inserting into the STL Map\n");

    for (int i = 0; i < ITERATIONS; i++) {
        string val = keyFromInt(i);
        stl.insert(pair<string,int>(val,i));
    }

    for (int i = 0; i < ITERATIONS; i += 2) {
        string val = keyFromInt(i);
        stl.insert(std::pair<string,int>(val,i));
    }

    for (int i = 0; i < ITERATIONS; i += 4) {
        string val = keyFromInt(i);
        stl.insert(std::pair<string,int>(val,i));
    }

    for (int i = 0; i < ITERATIONS; i += 8) {
        string val = keyFromInt(i);
        stl.insert(std::pair<string,int>(val,i));
    }

    voltdb::CompactingHashTable<string, int>::iterator iter;

    printf("Inserting into the VoltDB Map\n");

    for (int i = 0; i < ITERATIONS; i++) {
        string val = keyFromInt(i);
        volt.insert(val,i);
    }

    for (int i = 0; i < ITERATIONS; i += 2) {
        string val = keyFromInt(i);
        volt.insert(val,i);
    }

    for (int i = 0; i < ITERATIONS; i += 4) {
        string val = keyFromInt(i);
        volt.insert(val,i);
    }

    for (int i = 0; i < ITERATIONS; i += 8) {
        string val = keyFromInt(i);
        volt.insert(val,i);
    }

    timeval tp;
    gettimeofday(&tp, NULL);
    printf("Time: %ld, %ld\n", (long int)tp.tv_sec, (long int)tp.tv_usec);
    double t1 = static_cast<double>(tp.tv_sec * 1000 + tp.tv_usec / 1000);
    fflush(stdout);

    printf("Range From STL Map\n");

    for (int i = 0; i < ITERATIONS; i += 2) {
        stl.erase(keyFromInt(i));
    }

    //for (int i = 0; i < ITERATIONS; i += 1) {
    //  stl.equal_range(keyFromInt(i));
    //}

    gettimeofday(&tp, NULL);
    printf("Time: %ld, %ld\n", (long int)tp.tv_sec, (long int)tp.tv_usec);
    double t2 = static_cast<double>(tp.tv_sec * 1000 + tp.tv_usec / 1000);
    printf("Time elapsed: %.2f\n", (t2 - t1) / static_cast<double>(1000));
    fflush(stdout);

    printf("Range From VoltDB Map\n");

    for (int i = 0; i < ITERATIONS; i += 2) {
        string val = keyFromInt(i);
        iter = volt.find(val);
        ASSERT_FALSE(iter.isEnd());
        volt.erase(iter);
    }

    /*for (int i = 0; i < ITERATIONS; i += 1) {
        volt.equalRange(keyFromInt(i));
    }*/

    gettimeofday(&tp, NULL);
    printf("Time: %ld, %ld\n", (long int)tp.tv_sec, (long int)tp.tv_usec);
    double t3 = static_cast<double>(tp.tv_sec * 1000 + tp.tv_usec / 1000);
    printf("Time elapsed: %.2f\n", (t3 - t2) / static_cast<double>(1000));
    fflush(stdout);

    ASSERT_TRUE(volt.verify());

    printf("Done!\n");
}

TEST_F(CompactingHashTest, BenchmarkMulti) {
    const int ITERATIONS = 20000;
    const int BATCH_SIZE = 5;
    const int BATCH_COUNT = 1000;

    boost::unordered_multimap<string,int> stl;
    voltdb::CompactingHashTable<string,int> volt(false);

    boost::unordered_multimap<string,int>::const_iterator iter_stl;

    printf("Inserting into the STL Map\n");

    for (int i = 0; i < BATCH_COUNT; i++) {
        for (int j = 0; j < BATCH_SIZE; j++) {
            std::string val = keyFromInt(i);
            stl.insert(std::pair<std::string,int>(val,j));
        }
    }

    voltdb::CompactingHashTable<string, int>::iterator iter;

    printf("Inserting into the VoltDB Map\n");

    for (int i = 0; i < BATCH_COUNT; i++) {
        for (int j = 0; j < BATCH_SIZE; j++) {
            std::string val = keyFromInt(i);
            volt.insert(val,j);
        }
    }

    assert(volt.verify());

    timeval tp;
    gettimeofday(&tp, NULL);
    printf("Time: %ld, %ld\n", (long int)tp.tv_sec, (long int)tp.tv_usec);
    double t1 = static_cast<double>(tp.tv_sec * 1000 + tp.tv_usec / 1000);
    fflush(stdout);

    printf("Range From STL Map\n");

    for (int i = 0; i < ITERATIONS; i += 2) {
        int k = rand() % BATCH_COUNT;
        string key = keyFromInt(k);
        pair<boost::unordered_multimap<string,int>::const_iterator,boost::unordered_multimap<string,int>::const_iterator> range;
        range = stl.equal_range(key);
        ASSERT_FALSE(range.first == stl.end());
        int count = 0;
        while (range.first != range.second) {
            count++;
            range.first++;
        }
        ASSERT_EQ(BATCH_SIZE, count);
        ASSERT_EQ(BATCH_SIZE, stl.count(key));
    }

    gettimeofday(&tp, NULL);
    printf("Time: %ld, %ld\n", (long int)tp.tv_sec, (long int)tp.tv_usec);
    double t2 = static_cast<double>(tp.tv_sec * 1000 + tp.tv_usec / 1000);
    printf("Time elapsed: %.2f\n", (t2 - t1) / static_cast<double>(1000));
    fflush(stdout);

    printf("Range From VoltDB Map\n");

    for (int i = 0; i < ITERATIONS; i += 2) {
        int k = rand() % BATCH_COUNT;
        string key = keyFromInt(k);
        iter = volt.find(key);
        ASSERT_FALSE(iter.isEnd());
        int count = 0;
        while (!iter.isEnd()) {
            count++;
            iter.moveNext();
        }
        ASSERT_EQ(BATCH_SIZE, count);
    }

    gettimeofday(&tp, NULL);
    printf("Time: %ld, %ld\n", (long int)tp.tv_sec, (long int)tp.tv_usec);
    double t3 = static_cast<double>(tp.tv_sec * 1000 + tp.tv_usec / 1000);
    printf("Time elapsed: %.2f\n", (t3 - t2) / static_cast<double>(1000));
    fflush(stdout);

    ASSERT_TRUE(volt.verify());

    printf("Done!\n");
}

TEST_F(CompactingHashTest, Trivial) {
    uint64_t one = 1;
    uint64_t two = 2;
    uint64_t three = 3;
    bool success;

    // UNIQUE MAP
    voltdb::CompactingHashTable<uint64_t,uint64_t> m(true);
    success = m.insert(two,two);
    ASSERT_TRUE(success);
    success = m.insert(one,one);
    ASSERT_TRUE(success);
    success = m.insert(three,three);
    ASSERT_TRUE(success);

    ASSERT_TRUE(m.verify());
    ASSERT_TRUE(m.size() == 3);

    voltdb::CompactingHashTable<uint64_t,uint64_t>::iterator iter;
    iter = m.find(two);
    ASSERT_FALSE(iter.isEnd());
    ASSERT_EQ(iter.key(), two);

    success = m.erase(iter);
    ASSERT_TRUE(success);
    ASSERT_TRUE(m.verify());
    ASSERT_TRUE(m.size() == 2);

    iter = m.find(two);
    ASSERT_TRUE(iter.isEnd());

    // MULTIMAP
    voltdb::CompactingHashTable<uint64_t,uint64_t> m2(false);
    success = m2.insert(one,one);
    ASSERT_TRUE(success);
    success = m2.insert(one,one);
    ASSERT_TRUE(success);
    success = m2.insert(one,one);
    ASSERT_TRUE(success);
    success = m2.insert(one,one);
    ASSERT_TRUE(success);
    success = m2.insert(one,one);
    ASSERT_TRUE(success);
    success = m2.insert(one,one);
    ASSERT_TRUE(success);
    success = m2.insert(one,one);
    ASSERT_TRUE(success);

    ASSERT_TRUE(m2.verify());
    ASSERT_TRUE(m2.size() == 7);
}

int main() {
    return TestSuite::globalInstance()->runAll();
}
