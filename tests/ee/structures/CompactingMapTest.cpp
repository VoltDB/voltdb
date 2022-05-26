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
#include <cstdio>
#include <sys/time.h>
#include "harness.h"
#include "structures/CompactingMap.h"
#include "common/debuglog.h"

using namespace voltdb;
using namespace std;

class StringComparator {
public:
    int comparisons;
    StringComparator() : comparisons(0) {}
    ~StringComparator() { }

    inline int operator()(const std::string &lhs, const std::string &rhs) const {
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

class CompactingMapTest : public Test {
public:
    CompactingMapTest() {
    }

    ~CompactingMapTest() {
    }

    void print(voltdb::CompactingMap<NormalKeyValuePair<int, int>, IntComparator> &m) {
        printf(" compactmap [ ");
        for (auto iter = m.begin(); !iter.isEnd(); iter.moveNext()) {
            printf("%d ", iter.key());
        }
        printf("]\n");
        fflush(stdout);
    }

    void print(std::multimap<int, int> &m) {
        printf("   multimap [ ");
        for (auto iter = m.cbegin(); iter != m.cend(); ++iter) {
            printf("%d ", iter->first);
        }
        printf("]\n");
        fflush(stdout);
    }

    void print(std::map<int, int> &m) {
        printf("       map [ ");
        for (auto iter = m.cbegin(); iter != m.cend(); ++iter) {
            printf("%d ", iter->first);
        }
        printf("]\n");
        fflush(stdout);
    }

    /*
     * Walk stli and volti as long as key = val.
     * Collect all the values, sort those values, and make sure
     * that stl and volt returned equal value sets.
     * Record cardinality of largest value set evaluated in chainCounter
     */
    void verifyIterators(std::multimap<std::string, std::string> &stl, std::multimap<std::string, std::string>::iterator &stli,
            voltdb::CompactingMap<NormalKeyValuePair<std::string, std::string>, StringComparator>::iterator &volti,
            std::string val, int *chainCounter) {
        std::vector<std::string> stlv;
        std::vector<std::string> voltv;

        for (; stli != stl.end(); stli++) {
            if (stli->first.compare(val) == 0) {
                stlv.push_back(stli->second);
            }
            else {
                break;
            }
        }
        for (;!volti.isEnd(); volti.moveNext()) {
            if (volti.key().compare(val) == 0) {
                voltv.push_back(volti.value());
            }
            else {
                break;
            }
        }

        ASSERT_TRUE(stlv.size() > 0);
        ASSERT_EQ(stlv.size(), voltv.size());
        if (chainCounter && ((int)stlv.size() > (int)*chainCounter)) {
            *chainCounter = (int)stlv.size();
        }
        std::sort(stlv.begin(), stlv.end());
        std::sort(voltv.begin(), voltv.end());
        for (int i=0; i < stlv.size(); i++) {
            ASSERT_EQ(stlv[i].compare(voltv[i]), 0);
        }
    }

    std::string keyFromInt(int i) {
        char buf[256];
        snprintf(buf, 256, "%010d", i);
        std::string val = buf;
        return val;
    }
};

TEST_F(CompactingMapTest, Benchmark) {
    const int ITERATIONS = 1000;

    std::map<std::string,std::string> stl;
    voltdb::CompactingMap<NormalKeyValuePair<std::string, std::string>, StringComparator> volt(true, StringComparator());

    timeval tp;
    gettimeofday(&tp, NULL);
    // printf("Time: %ld, %ld\n", (long int)tp.tv_sec, (long int)tp.tv_usec);
    // double t1 = static_cast<double>(tp.tv_sec * 1000 + tp.tv_usec / 1000);
    // fflush(stdout);

    std::map<std::string,std::string>::const_iterator iter_stl;

    for (int i = 0; i < ITERATIONS; i++) {
        std::string val = keyFromInt(i);
        stl.insert(std::pair<std::string,std::string>(val,val));

        iter_stl = stl.find(keyFromInt(i / 2));
        ASSERT_TRUE(iter_stl != stl.end());
        ASSERT_TRUE(iter_stl->second == (keyFromInt(i / 2)));
    }

    for (int i = 0; i < ITERATIONS; i += 2) {
        stl.erase(keyFromInt(i));
    }

    iter_stl = stl.begin();
    for (int i = 1; i < ITERATIONS; i += 2, iter_stl++) {
        ASSERT_TRUE(iter_stl != stl.end());
        ASSERT_TRUE(iter_stl->second.compare(keyFromInt(i)) == 0);
    }

    gettimeofday(&tp, NULL);
    // printf("Time: %ld, %ld\n", (long int)tp.tv_sec, (long int)tp.tv_usec);
    // double t2 = static_cast<double>(tp.tv_sec * 1000 + tp.tv_usec / 1000);
    // printf("Time elapsed: %.2f\n", (t2 - t1) / static_cast<double>(1000));
    // fflush(stdout);

    voltdb::CompactingMap<NormalKeyValuePair<std::string, std::string>, StringComparator>::iterator iter;

    // We don't have a cached last buffer yet.
    ASSERT_FALSE(volt.hasCachedLastBuffer());
    ASSERT_TRUE(volt.size() == 0);
    for (int i = 0; i < ITERATIONS; i++) {
        volt.insert(std::pair<std::string,std::string>(keyFromInt(i),keyFromInt(i)));
        // Still don't have a cached last buffer, because we never
        // had a last buffer.
        ASSERT_FALSE(volt.hasCachedLastBuffer());
        //ASSERT_TRUE(volt.size() == i + 1);

        iter = volt.find(keyFromInt(i / 2));
        ASSERT_TRUE(!iter.isEnd());
        ASSERT_TRUE(iter.value().compare(keyFromInt(i / 2)) == 0);
    }
    ASSERT_EQ(ITERATIONS, volt.size());
    for (int i = 0; i < ITERATIONS; i += 2) {
        ASSERT_EQ(ITERATIONS-i/2, volt.size());
        volt.erase(keyFromInt(i));
        ASSERT_EQ(ITERATIONS-i/2-1, volt.size());
        ASSERT_FALSE(volt.hasCachedLastBuffer());
        iter = volt.find(keyFromInt(i));
        ASSERT_TRUE(iter.isEnd());
    }

    // Insert another and test, because why not?
    volt.insert(std::pair<std::string,std::string>(keyFromInt(0),keyFromInt(0)));
    ASSERT_FALSE(volt.hasCachedLastBuffer());
    volt.erase(keyFromInt(0));

    iter = volt.begin();
    for (int i = 1; i < ITERATIONS; i += 2, iter.moveNext()) {
        ASSERT_TRUE(!iter.isEnd());
        ASSERT_TRUE(iter.value().compare(keyFromInt(i)) == 0);
    }

    for (int i = 0; i < ITERATIONS; i += 2) {
        iter = volt.find(keyFromInt(i));
        ASSERT_TRUE(iter.isEnd());
    }

    gettimeofday(&tp, NULL);
    // printf("Time: %ld, %ld\n", (long int)tp.tv_sec, (long int)tp.tv_usec);
    // double t3 = static_cast<double>(tp.tv_sec * 1000 + tp.tv_usec / 1000);
    // printf("Time elapsed: %.2f\n", (t3 - t2) / static_cast<double>(1000));
    // fflush(stdout);

    for (iter_stl = stl.begin(); iter_stl != stl.end(); iter_stl++) {
        std::string key = iter_stl->first;
        bool success = volt.erase(key);
        ASSERT_TRUE(success);
    }
    ASSERT_TRUE(volt.size() == 0);
    ASSERT_TRUE(volt.hasCachedLastBuffer());
}

TEST_F(CompactingMapTest, BenchmarkDel) {
    const int ITERATIONS = 1000;

    std::map<std::string,std::string> stl;
    voltdb::CompactingMap<NormalKeyValuePair<std::string, std::string>, StringComparator> volt(false, StringComparator());

    std::map<std::string,std::string>::const_iterator iter_stl;

    // printf("Inserting into the STL Map\n");

     for (int i = 0; i < ITERATIONS; i++) {
     std::string val = keyFromInt(i);
     stl.insert(std::pair<std::string,std::string>(val,val));
     }

     for (int i = 0; i < ITERATIONS; i += 2) {
     std::string val = keyFromInt(i);
     stl.insert(std::pair<std::string,std::string>(val,val));
     }

     for (int i = 0; i < ITERATIONS; i += 4) {
     std::string val = keyFromInt(i);
     stl.insert(std::pair<std::string,std::string>(val,val));
     }

     for (int i = 0; i < ITERATIONS; i += 8) {
     std::string val = keyFromInt(i);
     stl.insert(std::pair<std::string,std::string>(val,val));
     }

    voltdb::CompactingMap<NormalKeyValuePair<std::string, std::string>, StringComparator>::iterator iter;

    // printf("Inserting into the VoltDB Map\n");

    for (int i = 0; i < ITERATIONS; i++) {
        std::string val = keyFromInt(i);
        volt.insert(std::pair<std::string,std::string>(val,val));
    }

    for (int i = 0; i < ITERATIONS; i += 2) {
        std::string val = keyFromInt(i);
        volt.insert(std::pair<std::string,std::string>(val,val));
    }

    for (int i = 0; i < ITERATIONS; i += 4) {
        std::string val = keyFromInt(i);
        volt.insert(std::pair<std::string,std::string>(val,val));
    }

    for (int i = 0; i < ITERATIONS; i += 8) {
        std::string val = keyFromInt(i);
        volt.insert(std::pair<std::string,std::string>(val,val));
    }

    timeval tp;
    gettimeofday(&tp, NULL);
    // printf("Time: %ld, %ld\n", (long int)tp.tv_sec, (long int)tp.tv_usec);
    // double t1 = static_cast<double>(tp.tv_sec * 1000 + tp.tv_usec / 1000);
    // fflush(stdout);

    // printf("Range From STL Map\n");

    //for (int i = 0; i < ITERATIONS; i += 2) {
    //  stl.erase(keyFromInt(i));
    //}

    //for (int i = 0; i < ITERATIONS; i += 1) {
    //  stl.equal_range(keyFromInt(i));
    //}

    gettimeofday(&tp, NULL);
    // printf("Time: %ld, %ld\n", (long int)tp.tv_sec, (long int)tp.tv_usec);
    // double t2 = static_cast<double>(tp.tv_sec * 1000 + tp.tv_usec / 1000);
    // printf("Time elapsed: %.2f\n", (t2 - t1) / static_cast<double>(1000));
    // fflush(stdout);

    // printf("Range From VoltDB Map\n");

    //for (int i = 0; i < ITERATIONS; i += 2) {
    //  volt.erase(keyFromInt(i));
    //}

    for (int i = 0; i < ITERATIONS; i += 1) {
        volt.equalRange(keyFromInt(i));
    }

    gettimeofday(&tp, NULL);
    // printf("Time: %ld, %ld\n", (long int)tp.tv_sec, (long int)tp.tv_usec);
    // double t3 = static_cast<double>(tp.tv_sec * 1000 + tp.tv_usec / 1000);
    // printf("Time elapsed: %.2f\n", (t3 - t2) / static_cast<double>(1000));
    // fflush(stdout);

    // printf("Done!\n");
}

TEST_F(CompactingMapTest, Bounds) {
    voltdb::CompactingMap<NormalKeyValuePair<int, int>, IntComparator> volt(true, IntComparator());

    ASSERT_TRUE(volt.lowerBound(1).isEnd());
    ASSERT_TRUE(volt.upperBound(1).isEnd());

    volt.insert(std::pair<int,int>(1,1));

    ASSERT_TRUE(volt.lowerBound(0).key() == 1);
    ASSERT_TRUE(volt.lowerBound(1).key() == 1);
    ASSERT_TRUE(volt.lowerBound(2).isEnd());

    ASSERT_TRUE(volt.upperBound(0).key() == 1);
    ASSERT_TRUE(volt.upperBound(1).isEnd());
    ASSERT_TRUE(volt.upperBound(2).isEnd());

    for (int i = 3; i <= 99; i += 2)
        volt.insert(std::pair<int,int>(i,i));

    ASSERT_TRUE(volt.lowerBound(99).key() == 99);
    ASSERT_TRUE(volt.upperBound(99).isEnd());
    ASSERT_TRUE(volt.lowerBound(100).isEnd());
    ASSERT_TRUE(volt.upperBound(100).isEnd());

    for (int i = 0; i <= 98; i += 2) {
        ASSERT_TRUE(volt.upperBound(i).key() == i + 1);
        ASSERT_TRUE(volt.lowerBound(i).key() == i + 1);
    }
    for (int i = 1; i <= 98; i += 2) {
        ASSERT_TRUE(volt.upperBound(i).key() == i + 2);
        ASSERT_TRUE(volt.lowerBound(i).key() == i);
    }

    // test range
    voltdb::CompactingMap<NormalKeyValuePair<int, int>, IntComparator> volt2(false, IntComparator());

    volt2.insert(std::pair<int,int>(0,0));
    volt2.insert(std::pair<int,int>(1,666));
    volt2.insert(std::pair<int,int>(1,1));
    volt2.insert(std::pair<int,int>(1,777));
    volt2.insert(std::pair<int,int>(2,2));
    volt2.insert(std::pair<int,int>(3,888));
    volt2.insert(std::pair<int,int>(3,3));
    volt2.insert(std::pair<int,int>(3,3));
    volt2.insert(std::pair<int,int>(3,999));

    std::pair<voltdb::CompactingMap<NormalKeyValuePair<int, int>, IntComparator>::iterator, voltdb::CompactingMap<NormalKeyValuePair<int, int>, IntComparator>::iterator> p;

    p = volt2.equalRange(1);
    ASSERT_TRUE(p.first.value() == 666);
    ASSERT_TRUE(p.second.value() == 2);

    p = volt2.equalRange(3);
    ASSERT_TRUE(p.first.value() == 888);
    ASSERT_TRUE(p.second.isEnd());

    p = volt2.equalRange(2);
    ASSERT_TRUE(p.first.value() == 2);
    ASSERT_TRUE(p.second.value() == 888);
}

TEST_F(CompactingMapTest, BenchmarkMulti) {
    const int ITERATIONS = 2000;
    const int BATCH_SIZE = 50;
    const int BATCH_COUNT = 10;

    std::map<std::string, int> stl;
    voltdb::CompactingMap<NormalKeyValuePair<std::string, int>, StringComparator> volt(false, StringComparator());

    std::map<std::string,std::string>::const_iterator iter_stl;

    // printf("Inserting into the STL Map\n");

    for (int i = 0; i < BATCH_COUNT; i++) {
        for (int j = 0; j < BATCH_SIZE; j++) {
            std::string val = keyFromInt(i);
            stl.insert(std::pair<std::string,int>(val,j));
        }
    }

    voltdb::CompactingMap<NormalKeyValuePair<std::string, std::string>, StringComparator>::iterator iter;

    // printf("Inserting into the VoltDB Map\n");

    for (int i = 0; i < BATCH_COUNT; i++) {
        for (int j = 0; j < BATCH_SIZE; j++) {
            std::string val = keyFromInt(i);
            volt.insert(std::pair<std::string,int>(val,j));
        }
    }

    ASSERT_TRUE(volt.verify());

    timeval tp;
    gettimeofday(&tp, NULL);
    // printf("Time: %ld, %ld\n", (long int)tp.tv_sec, (long int)tp.tv_usec);
    // int64_t t1 = tp.tv_sec * 1000 + tp.tv_usec / 1000;
    // fflush(stdout);

    // printf("Range From STL Map\n");

    for (int i = 0; i < ITERATIONS; i += 2) {
        int k = rand() % BATCH_COUNT;
        stl.equal_range(keyFromInt(k));
    }

    gettimeofday(&tp, NULL);
    // printf("Time: %ld, %ld\n", (long int)tp.tv_sec, (long int)tp.tv_usec);
    // int64_t t2 = tp.tv_sec * 1000 + tp.tv_usec / 1000;
    // printf("Time elapsed: %.2f\n", static_cast<double>(t2 - t1) / static_cast<double>(1000));
    // fflush(stdout);

    // printf("Range From VoltDB Map\n");

    for (int i = 0; i < ITERATIONS; i += 2) {
        int k = rand() % BATCH_COUNT;
        volt.equalRange(keyFromInt(k));
    }

    gettimeofday(&tp, NULL);
    // printf("Time: %ld, %ld\n", (long int)tp.tv_sec, (long int)tp.tv_usec);
    // int64_t t3 = tp.tv_sec * 1000 + tp.tv_usec / 1000;
    // printf("Time elapsed: %.2f\n", static_cast<double>(t3 - t2) / static_cast<double>(1000));
    // fflush(stdout);

    // printf("Done!\n");
}

TEST_F(CompactingMapTest, Trivial) {
    voltdb::CompactingMap<NormalKeyValuePair<int, int>, IntComparator> m(true, IntComparator());
    bool success = m.insert(std::pair<int,int>(2,2));
    ASSERT_TRUE(success);
    success = m.insert(std::pair<int,int>(1,1));
    ASSERT_TRUE(success);
    success = m.insert(std::pair<int,int>(3,3));
    ASSERT_TRUE(success);

    ASSERT_TRUE(m.verify());

    voltdb::CompactingMap<NormalKeyValuePair<int, int>, IntComparator> m2(false, IntComparator());
    success = m2.insert(std::pair<int,int>(1,1));
    ASSERT_TRUE(success);
    success = m2.insert(std::pair<int,int>(1,1));
    ASSERT_TRUE(success);
    success = m2.insert(std::pair<int,int>(1,1));
    ASSERT_TRUE(success);
    success = m2.insert(std::pair<int,int>(1,1));
    ASSERT_TRUE(success);
    success = m2.insert(std::pair<int,int>(1,1));
    ASSERT_TRUE(success);
    success = m2.insert(std::pair<int,int>(1,1));
    ASSERT_TRUE(success);
    success = m2.insert(std::pair<int,int>(1,1));
    ASSERT_TRUE(success);

    ASSERT_TRUE(m.verify());
}

TEST_F(CompactingMapTest, RandomUnique) {
    const int ITERATIONS = 1001;
    const int BIGGEST_VAL = 100;

    const int INSERT = 0;
    const int DELETE = 1;

    std::map<int,int> stl;
    voltdb::CompactingMap<NormalKeyValuePair<int, int>, IntComparator> volt(true, IntComparator());
    ASSERT_TRUE(volt.verify());

    std::map<int,int>::const_iterator stli;
    voltdb::CompactingMap<NormalKeyValuePair<int, int>, IntComparator>::iterator volti;

    srand(0);

    for (int i = 0; i < ITERATIONS; i++) {
        if ((i % 1000) == 0) {
            ASSERT_TRUE(volt.verify());
        }

        int op = rand() % 2;
        int val = rand() % BIGGEST_VAL;
        if (op == INSERT) {
            stli = stl.find(val);
            volti = volt.find(val);
            if (stli == stl.end()) {
                ASSERT_TRUE(volti.isEnd());

                stl.insert(std::pair<int,int>(val, val));
                bool sucess = volt.insert(std::pair<int,int>(val, val));
                ASSERT_TRUE(sucess);
            }
            else {
                ASSERT_TRUE(!volti.isEnd());
                ASSERT_TRUE(stli->first == volti.key());
                ASSERT_TRUE(stli->first == val);

                bool sucess = volt.insert(std::pair<int,int>(val, val));
                ASSERT_TRUE(!sucess);
            }
        }
        if (op == DELETE) {
            stli = stl.find(val);
            volti = volt.find(val);
            if (stli == stl.end()) {
                ASSERT_TRUE(volti.isEnd());
                bool success = volt.erase(val);
                ASSERT_TRUE(!success);
            }
            else {
                ASSERT_TRUE(!volti.isEnd());
                ASSERT_TRUE(stli->first == volti.key());

                stl.erase(val);
                volt.erase(val);
            }
        }
    }

    ASSERT_TRUE(volt.verify());
}

TEST_F(CompactingMapTest, RandomMulti) {
    const int ITERATIONS  = 1001;
    const int BIGGEST_VAL = 100;

    const int INSERT = 0;   int countInserts = 0;
    const int ERASE = 1;    int countErases = 0;
    const int ERASE_IT = 2; int countEraseIts = 0;
    const int FIND = 3;     int countFinds = 0; int countFinds_notFound = 0; int countFinds_found = 0; int find_greatestChain = 0;
    const int SIZE = 4;     int countSizes = 0; int size_greatest = 0;
    const int LBOUND = 5;   int lowerBounds = 0; int lb_greatestChain = 0;
    const int UBOUND = 6;   int upperBounds = 0; int ub_greatestChain = 0;
    const int EQ_RANGE = 7;
    const int TOTAL_OPS = 8;

    std::multimap<std::string, std::string> stl;
    voltdb::CompactingMap<NormalKeyValuePair<std::string, std::string>, StringComparator> volt(false, StringComparator());

    std::multimap<std::string, std::string>::iterator stli;
    voltdb::CompactingMap<NormalKeyValuePair<std::string, std::string>, StringComparator>::iterator volti;

    srand(0);

    // just check that an error can be found

    for (int i = 0; i < ITERATIONS; i++) {
        if ((i % 1000) == 0) {
            ASSERT_TRUE(volt.verify());
        }

        int op = rand() % TOTAL_OPS;
        std::string val = keyFromInt(rand() % BIGGEST_VAL); // the key, really

        //
        // Insert a new <k,v>.
        //
        if (op == INSERT) {
            for (int i = 0; i < 100; i++) {
                val = keyFromInt(rand() % BIGGEST_VAL);

                countInserts++;

                stli = stl.find(val);
                volti = volt.find(val);
                if (stli == stl.end()) {
                    ASSERT_TRUE(volti.isEnd());
                }
                else {
                    ASSERT_TRUE(!volti.isEnd());
                    ASSERT_TRUE(stli->first == volti.key());

                    int stlCount = 0;
                    for (; (stli != stl.end()) && (stli->first == val); stli++)
                        stlCount++;

                    int voltCount = 0;
                    for (; (!volti.isEnd()) && (volti.key() == val); volti.moveNext())
                        voltCount++;

                    ASSERT_TRUE(stlCount == voltCount);
                    ASSERT_TRUE(stlCount == stl.count(val));
                }

                std::string val_value = keyFromInt(rand() % BIGGEST_VAL);
                stli = stl.insert(std::pair<std::string, std::string>(val, val_value));
                ASSERT_TRUE(stli != stl.end());
                bool success = volt.insert(std::pair<std::string, std::string>(val, val_value));
                ASSERT_TRUE(success);
            }
        }
        //
        // Erase a key, by equality or by iterator location.
        //
        else if (op == ERASE || op == ERASE_IT) {
            for (int i = 0; i < 100; i++) {
                val = keyFromInt(rand() % BIGGEST_VAL);

                if (op == ERASE) countErases++;
                if (op == ERASE_IT) countEraseIts++;

                stli = stl.find(val);
                volti = volt.find(val);
                if (stli == stl.end()) {
                    ASSERT_TRUE(volti.isEnd());
                    bool success = volt.erase(val);
                    ASSERT_TRUE(!success);
                }
                else {
                    ASSERT_TRUE(!volti.isEnd());
                    ASSERT_TRUE(stli->first == volti.key());

                    // don't know why this is true, but it seems to be invariant.
                    // if it is suddenly not true, will have to iterate to erase
                    // the correct value corresponding to the stl.found key.
                    ASSERT_TRUE(stli->second == volti.value());

                    stl.erase(stli);
                    if (op == ERASE) {
                        bool success = volt.erase(val);
                        ASSERT_TRUE(success);
                    }
                    else {
                        ASSERT_TRUE(op == ERASE_IT);
                        bool success = volt.erase(volti);
                        ASSERT_TRUE(success);
                    }
                }
            }
        }
        //
        // Find a key and verify that all corresponding values match
        //
        else if (op == FIND) {
            countFinds++;
            stli = stl.find(val);
            volti = volt.find(val);
            if (stli == stl.end()) {
                countFinds_notFound++;
                ASSERT_TRUE(volti.isEnd());
            }
            else {
                countFinds_found++;
                // show that the same values where found associated to val.
                verifyIterators(stl, stli, volti, val, &find_greatestChain);
            }
        }
        //
        // Verify map size (cardinality of member)
        //
        else if (op == SIZE) {
            countSizes++;
            ASSERT_TRUE(stl.size() == volt.size());
            if (stl.size() > size_greatest) {
                size_greatest = (int)stl.size();
            }
        }
        //
        // Verify lower bounds
        //
        else if (op == LBOUND) {
            lowerBounds++;
            stli = stl.lower_bound(val);
            volti = volt.lowerBound(val);

            if (stli == stl.end()) {
                ASSERT_TRUE(volti.isEnd());
            }
            else {
                // compare all the keys equal to the lowerbound
                verifyIterators(stl, stli, volti, stli->first, &lb_greatestChain);
            }
        }
        else if (op == UBOUND) {
            upperBounds++;
            stli = stl.upper_bound(val);
            volti = volt.upperBound(val);
            if (stli == stl.end()) {
                ASSERT_TRUE(volti.isEnd());
            }
            else {
                verifyIterators(stl, stli, volti, stli->first, &ub_greatestChain);
            }
        }
        //
        // Verify equal ranges. Checks that the iterator pair returned points to
        // equal keys, but does not use the returned iterators to do iteration
        //
        else if (op == EQ_RANGE) {
            std::pair<std::multimap<std::string, std::string>::iterator, std::multimap<std::string, std::string>::iterator> stli_pair;
            std::pair<voltdb::CompactingMap<NormalKeyValuePair<std::string, std::string>, StringComparator>::iterator, voltdb::CompactingMap<NormalKeyValuePair<std::string, std::string>, StringComparator>::iterator> volti_pair;
            stli_pair = stl.equal_range(val);
            volti_pair = volt.equalRange(val);

            if (stli_pair.first == stl.end()) {
                ASSERT_TRUE(volti_pair.first.isEnd());
            }
            else {
                ASSERT_TRUE(!volti_pair.first.isEnd());
                if(stli_pair.first->first != volti_pair.first.key()) {
                    //std::cout << "FIRST stl: " << stli_pair.first->first << " volt: " << volti_pair.first.key() << std::endl;
                    ASSERT_TRUE(false);
                }
            }
            if (stli_pair.second == stl.end()) {
                ASSERT_TRUE(volti_pair.second.isEnd());
            }
            else {
                ASSERT_TRUE(!volti_pair.second.isEnd());
                if(stli_pair.second->first != volti_pair.second.key()) {
                    //std::cout << "SECOND stl: " << stli_pair.second->first << " volt: " << volti_pair.second.key() << std::endl;
                    ASSERT_TRUE(false);
                }
            }
        }
        else {
            ASSERT_TRUE(!"Invalid test operation selected.");
        }
    }

    ASSERT_TRUE(volt.verify());
    // std::cout << "Inserts: " << countInserts << std::endl;
    // std::cout << "Erase: " << countErases << std::endl;
    // std::cout << "Erase(it): " << countEraseIts << std::endl;
    // std::cout << "Finds: " << countFinds << " found: " << countFinds_found << " not: " << countFinds_notFound;
    // std::cout << " find greatest chain: " <<  find_greatestChain << std::endl;
    // std::cout << "Sizes: " << countSizes << " greatest size: " << size_greatest << std::endl;
    // std::cout << "LowerBounds: " << lowerBounds << " lb greatest chain: " << lb_greatestChain << std::endl;
    // std::cout << "UpperBounds: " << upperBounds << " ub greatest chain: " << ub_greatestChain << std::endl;
}

// ENG-1057
//
// I have commented this out intentionally.  It demonstrates that the
// bytesAllocated() reported by the index doesn't overflow and become
// negative, but it runs really slowly under valgrind and I'm not
// happy checking it in, but I want evidence left around.  There's an
// argument to be made that the equivalent test in CompactingPoolTest
// covers this since the defect is actually in ContiguousAllocator.
// --izzy 3/22/2011
//
// TEST_F(CompactingMapTest, bytesAllocated) {
//     voltdb::CompactingMap<NormalKeyValuePair<int, int>, IntComparator> volt(true, IntComparator());
//
//     int64_t entry_size = 40; // magic
//     int64_t bigsize = 2L * (1024L * 1024L * 1024L) + (1024L * 1024L * 10L);
//     int64_t num_entries = (bigsize / entry_size) + 1;
//
//     for (int i = 0; i < num_entries; ++i)
//     {
//         volt.insert(std::pair<int,int>(i,i));
//         // return value of bytesAllocated() is unsigned.  However,
//         // when it overflows internally, we get a HUGE value back.
//         // Our sanity check is that the value is less than twice the
//         // giant memory we're trying to fill
//         ASSERT_TRUE(volt.bytesAllocated() < (bigsize * 2L));
//     }
//     // Make sure that we would have, in fact, overflowed an int32_t
//     EXPECT_TRUE(volt.bytesAllocated() > 0x7fffffff);
// }

int main() {
    return TestSuite::globalInstance()->runAll();
}
