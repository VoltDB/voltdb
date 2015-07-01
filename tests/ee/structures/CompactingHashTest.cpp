/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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
#include <boost/unordered_map.hpp>
#include "harness.h"
#include "structures/CompactingHashTable.h"
#include "common/FixUnusedAssertHack.h"

using namespace voltdb;
using namespace std;

class StringComparator {
public:
    int comparisons;
    StringComparator() : comparisons(0) {}
    ~StringComparator() {}

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

bool coinFlip() {
    return rand() > (RAND_MAX / 2);
}

int64_t randomValue(int64_t absMax) {
    int64_t value = rand() % absMax;
    if (coinFlip()) value *= -1;
    return value;
}

void uniqueFuzzIteration() {
    const int ITERATIONS = 10000;

    boost::unordered_map<int64_t,int64_t> stl;
    voltdb::CompactingHashTable<int64_t,int64_t> volt(true);

    std::pair<boost::unordered_map<int64_t,int64_t>::iterator, bool> insertSTLIter;
    boost::unordered_map<int64_t,int64_t>::iterator stlIter;

    voltdb::CompactingHashTable<int64_t,int64_t>::iterator voltIter;

    double mix = rand();

    for (int i = 0; i < ITERATIONS; i++) {
        bool insert = rand() >= mix;

        int64_t value = rand();
        if (insert) {
            insertSTLIter = stl.insert(pair<int64_t,int64_t>(value,value));
            bool didInsert = volt.insert(value, value);
            assert (insertSTLIter.second == didInsert);
        }
        else {
            stlIter = stl.find(value);
            voltIter = volt.find(value);
            assert( (stlIter == stl.end()) == (voltIter.isEnd()) );
            if (stlIter != stl.end()) {
                stl.erase(stlIter);
                volt.erase(voltIter);
            }
        }

        assert( stl.size() == volt.size() );
    }

    volt.verify();
}

void multiFuzzIteration() {
    const int ITERATIONS = 10000;

    // mix of inserts and deletes
    double mix = rand();

    // whether to allow failed deletes or inserts (50%)
    bool alwaysSucceed = coinFlip();

    // how many of the same value to insert
    // how many nodes to delete for each pass
    // (picks a random power of 2 which is <= 16)
    int dups = (int)log(rand() % 65536);
    if (dups == 0) dups = 1;

    // max positive and negative value to insert
    int range = rand();
    if (alwaysSucceed) range = (int)log(range);
    else range = range % ITERATIONS * dups / 2;

    if (0) {
        printf("Running %.2f mix with %d dups, %d max and %s alwaysSucceed.\n",
           mix / (double)RAND_MAX,
           dups,
           range,
           alwaysSucceed ? "DO" : "DO NOT");
    }

    // counters
    int insertions = 0;
    int deletions = 0;

    // data structures and iterators
    boost::unordered_multimap<int64_t,int64_t> stl;
    voltdb::CompactingHashTable<int64_t,int64_t> volt(false);
    boost::unordered_multimap<int64_t,int64_t>::iterator stlIter;
    boost::unordered_multimap<int64_t,int64_t>::iterator stlIter2;
    voltdb::CompactingHashTable<int64_t,int64_t>::iterator voltIter;

    // the main loop
    for (int i = 0; i < ITERATIONS; i++) {
        // pick whether to insert or remove
        bool insert = rand() >= mix;

        if (insert) {
            int64_t value = randomValue(range);
            for (int j = 0; j < dups; j++) {
                int64_t toInsert = randomValue(100);
                stlIter = stl.insert(pair<int64_t,int64_t>(value, toInsert));
                bool didInsert = volt.insert(value, toInsert);
                insertions++;
                assert(didInsert);
            }
        }
        else {
            for (int j = 0; j < dups; j++) {
                if (volt.size() == 0)
                    break;

                int64_t value = randomValue(range);
                stlIter = stl.find(value);
                if (stlIter == stl.end()) {
                    voltIter = volt.find(value);
                    assert(voltIter.isEnd());
                    if (alwaysSucceed) j--;
                }
                else {
                    int64_t stlValue = stlIter->second;
                    if (coinFlip()) {
                        stlIter2 = stlIter;
                        stlIter2++;
                        if ((stlIter2 != stl.end()) && (stlIter2->first == value)) {
                            stlIter = stlIter2;
                            stlValue = stlIter->second;
                        }
                    }

                    voltIter = volt.find(value, stlValue);
                    assert(voltIter.isEnd() == false);

                    int64_t stlKey = stlIter->first;
                    int64_t voltKey = voltIter.key();
                    int64_t voltValue = voltIter.value();

                    assert(stlKey == voltKey);
                    assert(stlValue == voltValue);

                    // this should always succeed
                    stl.erase(stlIter);

                    // try to delete something that doesn't exist by value
                    bool erased = volt.erase(stlKey, 1000);
                    assert(!erased);

                    // now delete the real thing
                    erased = volt.erase(stlKey, stlValue);
                    assert(erased);
                    deletions++;
                }
            }
        }

        assert( stl.size() == volt.size() );
    }

    volt.verify();

    if(0) { printf("  did %d insertions and %d deletions.\n", insertions, deletions); }
}

TEST_F(CompactingHashTest, Fuzz) {
    const int ITERATIONS = 10;

    for (int i = 0; i < ITERATIONS; i++)
        uniqueFuzzIteration();

    for (int i = 0; i < ITERATIONS; i++)
        multiFuzzIteration();
}

TEST_F(CompactingHashTest, MissingByKey) {
    voltdb::CompactingHashTable<int64_t,int64_t> volt(false);
    voltdb::CompactingHashTable<int64_t,int64_t>::iterator voltIter;

    volt.insert(1,1);
    voltIter = volt.find(1,1);
    bool erased = volt.erase(1,2);
    assert(!erased);
    voltIter.setValue(2);
    erased = volt.erase(1,2);
    assert(erased);
}

TEST_F(CompactingHashTest, ShrinkAndGrowUnique) {
    const int ITERATIONS = 10000;

    voltdb::CompactingHashTable<uint64_t,uint64_t> volt(true);

    for (uint64_t i = 0; i < ITERATIONS; i++)
        ASSERT_TRUE(volt.insert(i, i));

    volt.verify();

    for (uint64_t i = 0; i < ITERATIONS; i++)
        ASSERT_TRUE(volt.erase(i, i));

    volt.verify();

    for (uint64_t i = 0; i < ITERATIONS; i++)
        ASSERT_TRUE(volt.insert(i, i));

    volt.verify();

    for (uint64_t i = 0; i < ITERATIONS; i++)
        ASSERT_TRUE(volt.erase(i, i));

    volt.verify();
}

TEST_F(CompactingHashTest, ShrinkAndGrowMulti) {
    const int ITERATIONS = 10000;

    voltdb::CompactingHashTable<uint64_t,uint64_t> volt(false);

    for (uint64_t i = 0; i < ITERATIONS; i++)
        ASSERT_TRUE(volt.insert(i, i));

    volt.verify();

    for (uint64_t i = 0; i < ITERATIONS; i++)
        ASSERT_TRUE(volt.erase(i, i));

    volt.verify();

    for (uint64_t i = 0; i < ITERATIONS; i++)
        ASSERT_TRUE(volt.insert(i, i));

    volt.verify();

    for (uint64_t i = 0; i < ITERATIONS; i++)
        ASSERT_TRUE(volt.erase(i, i));

    volt.verify();
}

TEST_F(CompactingHashTest, Benchmark) {
    const int ITERATIONS = 10000;

    boost::unordered_map<uint64_t,uint64_t> stl;
    voltdb::CompactingHashTable<uint64_t,uint64_t> volt(true);

    timeval tp;
    gettimeofday(&tp, NULL);
    // printf("Time: %ld, %ld\n", (long int)tp.tv_sec, (long int)tp.tv_usec);
    // double t1 = static_cast<double>(tp.tv_sec * 1000 + tp.tv_usec / 1000);
    // fflush(stdout);

    map<string,string>::const_iterator iter_stl;

    for (uint64_t val = 0; val < ITERATIONS; val++) {
        stl.insert(pair<uint64_t,uint64_t>(val,val));
    }

    for (uint64_t val = 0; val < ITERATIONS; val += 2) {
        stl.erase(val);
    }

    gettimeofday(&tp, NULL);
    // printf("Time: %ld, %ld\n", (long int)tp.tv_sec, (long int)tp.tv_usec);
    // double t2 = static_cast<double>(tp.tv_sec * 1000 + tp.tv_usec / 1000);
    // printf("Time elapsed: %.2f\n", (t2 - t1) / static_cast<double>(1000));
    // fflush(stdout);

    voltdb::CompactingHashTable<uint64_t,uint64_t>::iterator iter;

    for (uint64_t val = 0; val < ITERATIONS; val++) {
        //cout << "Inserting: " << val << endl;
        volt.insert(val, val);

        ASSERT_TRUE(volt.size() == val + 1);
    }
    ASSERT_TRUE(volt.verify());

    for (uint64_t val = 0; val < ITERATIONS; val += 2) {
        iter = volt.find(val);
        ASSERT_FALSE(iter.isEnd());
        volt.erase(iter);
        iter = volt.find(val);
        ASSERT_TRUE(iter.isEnd());
    }
    ASSERT_TRUE(volt.verify());

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
    ASSERT_TRUE(volt.verify());

    gettimeofday(&tp, NULL);
    // printf("Time: %ld, %ld\n", (long int)tp.tv_sec, (long int)tp.tv_usec);
    // double t3 = static_cast<double>(tp.tv_sec * 1000 + tp.tv_usec / 1000);
    // printf("Time elapsed: %.2f\n", (t3 - t2) / static_cast<double>(1000));
    // fflush(stdout);

    ASSERT_TRUE(volt.verify());
}

TEST_F(CompactingHashTest, BenchmarkDel) {
    const int ITERATIONS = 1000;

    boost::unordered_multimap<string,int> stl;
    voltdb::CompactingHashTable<string,int> volt(false);

    boost::unordered_multimap<string,int>::const_iterator iter_stl;

    // printf("Inserting into the STL Map\n");

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

    // printf("Inserting into the VoltDB Map\n");

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
    // printf("Time: %ld, %ld\n", (long int)tp.tv_sec, (long int)tp.tv_usec);
    // double t1 = static_cast<double>(tp.tv_sec * 1000 + tp.tv_usec / 1000);
    // fflush(stdout);

    // printf("Range From STL Map\n");

    for (int i = 0; i < ITERATIONS; i += 2) {
        stl.erase(keyFromInt(i));
    }

    //for (int i = 0; i < ITERATIONS; i += 1) {
    //  stl.equal_range(keyFromInt(i));
    //}

    gettimeofday(&tp, NULL);
    // printf("Time: %ld, %ld\n", (long int)tp.tv_sec, (long int)tp.tv_usec);
    // double t2 = static_cast<double>(tp.tv_sec * 1000 + tp.tv_usec / 1000);
    // printf("Time elapsed: %.2f\n", (t2 - t1) / static_cast<double>(1000));
    // fflush(stdout);

    // printf("Range From VoltDB Map\n");

    for (int i = 0; i < ITERATIONS; i += 2) {
        string val = keyFromInt(i);
        iter = volt.find(val);
        ASSERT_FALSE(iter.isEnd());
        volt.erase(iter);
    }

    gettimeofday(&tp, NULL);
    // printf("Time: %ld, %ld\n", (long int)tp.tv_sec, (long int)tp.tv_usec);
    // double t3 = static_cast<double>(tp.tv_sec * 1000 + tp.tv_usec / 1000);
    // printf("Time elapsed: %.2f\n", (t3 - t2) / static_cast<double>(1000));
    // fflush(stdout);

    ASSERT_TRUE(volt.verify());

    // printf("Done!\n");
}

TEST_F(CompactingHashTest, BenchmarkMulti) {
    const int ITERATIONS = 20000;
    const int BATCH_SIZE = 5;
    const int BATCH_COUNT = 1000;

    boost::unordered_multimap<string,int> stl;
    voltdb::CompactingHashTable<string,int> volt(false);

    boost::unordered_multimap<string,int>::const_iterator iter_stl;

    // printf("Inserting into the STL Map\n");

    for (int i = 0; i < BATCH_COUNT; i++) {
        for (int j = 0; j < BATCH_SIZE; j++) {
            std::string val = keyFromInt(i);
            stl.insert(std::pair<std::string,int>(val,j));
        }
    }

    voltdb::CompactingHashTable<string, int>::iterator iter;

    // printf("Inserting into the VoltDB Map\n");

    for (int i = 0; i < BATCH_COUNT; i++) {
        for (int j = 0; j < BATCH_SIZE; j++) {
            std::string val = keyFromInt(i);
            volt.insert(val,j);
        }
    }

    assert(volt.verify());

    timeval tp;
    gettimeofday(&tp, NULL);
    // printf("Time: %ld, %ld\n", (long int)tp.tv_sec, (long int)tp.tv_usec);
    // double t1 = static_cast<double>(tp.tv_sec * 1000 + tp.tv_usec / 1000);
    // fflush(stdout);
    // printf("Range From STL Map\n");

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
    // printf("Time: %ld, %ld\n", (long int)tp.tv_sec, (long int)tp.tv_usec);
    // double t2 = static_cast<double>(tp.tv_sec * 1000 + tp.tv_usec / 1000);
    // printf("Time elapsed: %.2f\n", (t2 - t1) / static_cast<double>(1000));
    // fflush(stdout);
    // printf("Range From VoltDB Map\n");

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
    // printf("Time: %ld, %ld\n", (long int)tp.tv_sec, (long int)tp.tv_usec);
    // double t3 = static_cast<double>(tp.tv_sec * 1000 + tp.tv_usec / 1000);
    // printf("Time elapsed: %.2f\n", (t3 - t2) / static_cast<double>(1000));
    // fflush(stdout);

    ASSERT_TRUE(volt.verify());

    // printf("Done!\n");
}

TEST_F(CompactingHashTest, Trivial) {
    uint64_t one = 1;
    uint64_t two = 2;
    uint64_t three = 3;
    bool success;
    voltdb::CompactingHashTable<uint64_t,uint64_t>::iterator iter;

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
    success = m2.insert(one,two);
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

    iter = m2.find(one, two);
    ASSERT_FALSE(iter.isEnd());
    ASSERT_EQ(iter.key(), one);
    ASSERT_EQ(iter.value(), two);
}

int main() {
    assert(printf("Assertions are enabled\n"));
    return TestSuite::globalInstance()->runAll();
}
