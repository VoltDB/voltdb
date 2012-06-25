/* This file is part of VoltDB.
 * Copyright (C) 2008-2012 VoltDB Inc.
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

    void print(voltdb::CompactingMap<int, int, IntComparator> &m) {
        voltdb::CompactingMap<int, int, IntComparator>::iterator iter;

        printf(" compactmap [ ");
        for (iter = m.begin(); !iter.isEnd(); iter.moveNext()) {
            printf("%d ", iter.key());
        }
        printf("]\n");
        fflush(stdout);
    }

    void print(std::multimap<int, int> &m) {
        std::multimap<int, int>::iterator iter;

        printf("   multimap [ ");
        for (iter = m.begin(); iter != m.end(); iter++) {
            printf("%d ", iter->first);
        }
        printf("]\n");
        fflush(stdout);
    }

    void print(std::map<int, int> &m) {
        std::map<int, int>::iterator iter;

        printf("       map [ ");
        for (iter = m.begin(); iter != m.end(); iter++) {
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
    void verifyIterators(std::multimap<std::string, std::string> &stl,
                         std::multimap<std::string, std::string>::iterator &stli,
                         voltdb::CompactingMap<std::string, std::string, StringComparator>::iterator &volti,
                         std::string val, int *chainCounter)
    {
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
        ASSERT_TRUE(stlv.size() == voltv.size());
        if (chainCounter && ((int)stlv.size() > (int)*chainCounter)) {
            *chainCounter = (int)stlv.size();
        }
        std::sort(stlv.begin(), stlv.end());
        std::sort(voltv.begin(), voltv.end());
        for (int i=0; i < stlv.size(); i++) {
            ASSERT_TRUE(stlv[i].compare(voltv[i]) == 0);
        }
    }

    std::string keyFromInt(int i) {
        char buf[256];
        snprintf(buf, 256, "%010d", i);
        std::string val = buf;
        return val;
    }
};

TEST_F(CompactingMapTest, RandomUnique) {
    const int ITERATIONS = 1;
    const int BIGGEST_VAL = 100;

    const int INSERT = 0;
    const int DELETE = 1;

    std::map<int,int> stl;
    voltdb::CompactingMap<int, int, IntComparator> volt(true, IntComparator());
    ASSERT_TRUE(volt.verify());

    std::map<int,int>::const_iterator stli;
    voltdb::CompactingMap<int, int, IntComparator>::iterator volti;

    srand(0);

    for (int i = 0; i < ITERATIONS; i++) {
        if ((i % 1000) == 0) {
            ASSERT_TRUE(volt.verify());
        }
        int op = rand() % 2;
        int val = rand() % BIGGEST_VAL;
        op = 0;
        if (op == INSERT) {
            stli = stl.find(val);
            volti = volt.find(val);
            if (stli == stl.end()) {
                ASSERT_TRUE(volti.isEnd());

                stl.insert(std::pair<int,int>(val, val));
                bool sucess = volt.insert(std::pair<int,int>(val, val));
                ASSERT_TRUE(sucess);

                stli = stl.find(val);
                int ct = 1;
                std::map<int,int>::const_iterator it;
                for (it = stl.begin(); it != stli; it++) {
                	ct++;
                	printf("key : %d", (*it).first);
                }
                int rankasc= volt.rankAsc(val);
                printf("Rank expected %d, got %d", ct, rankasc);
                ASSERT_TRUE(rankasc == ct);
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


int main() {
    return TestSuite::globalInstance()->runAll();
}
