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

    void print(voltdb::CompactingMap<NormalKeyValuePair<int, int>, IntComparator, true> &m) {
        voltdb::CompactingMap<NormalKeyValuePair<int, int>, IntComparator, true>::iterator iter;

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
                         voltdb::CompactingMap<NormalKeyValuePair<std::string, std::string>, StringComparator, true>::iterator &volti,
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

TEST_F(CompactingMapTest, SimpleUniqueRank) {
    // Start the counting index feature
    voltdb::CompactingMap<NormalKeyValuePair<int, int>, IntComparator, true> volt(true, IntComparator());
    ASSERT_TRUE(volt.verify());

    voltdb::CompactingMap<NormalKeyValuePair<int, int>, IntComparator, true>::iterator volti;
    bool sucess;
    int64_t rankasc;

    for (int val = 1; val < 10; val++) {
        sucess = volt.insert(std::pair<int,int>(val, val));
        ASSERT_TRUE(sucess);
    }

    sucess = volt.insert(std::pair<int,int>(3, 3)); ASSERT_TRUE(!sucess);
    rankasc = volt.rankAsc(3);
    if (rankasc != 3)
        printf("false: <SimpleUniqueRank> expected %d, but got %ld\n", 3, (long)rankasc);
    ASSERT_TRUE(rankasc == 3);

    ASSERT_TRUE(volt.verify());
    ASSERT_TRUE(volt.verifyRank());
}

TEST_F(CompactingMapTest, RandomUniqueRank) {
    const int ITERATIONS = 1001;
    const int BIGGEST_VAL = 100;

    const int INSERT = 0;
    const int DELETE = 1;

    std::map<int,int> stl;
    // Start the counting index feature
    voltdb::CompactingMap<NormalKeyValuePair<int, int>, IntComparator, true> volt(true, IntComparator());
    ASSERT_TRUE(volt.verify());

    std::map<int,int>::const_iterator stli;
    voltdb::CompactingMap<NormalKeyValuePair<int, int>, IntComparator, true>::iterator volti;

    srand(0);
    for (int i = 0; i < ITERATIONS; i++) {
        if ((i % 1000) == 0) {
            ASSERT_TRUE(volt.verify());
            ASSERT_TRUE(volt.verifyRank());
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

                stli = stl.find(val);
                int64_t ct = 1;
                std::map<int,int>::const_iterator it;
                for (it = stl.begin(); it != stli; it++) {
                        ct++;
                }
                int64_t rankasc = volt.rankAsc(val);
                if (rankasc != ct)
                    printf("false: unique_rankAsc expected %ld, but got %ld\n", (long)ct, (long)rankasc);
                ASSERT_TRUE(rankasc == ct);
                volti = volt.findRank(rankasc);
                ASSERT_TRUE(volti.key() == val);

            }
            else {
                ASSERT_TRUE(!volti.isEnd());
                ASSERT_TRUE(stli->first == volti.key());
                ASSERT_TRUE(stli->first == val);

                bool sucess = volt.insert(std::pair<int,int>(val, val));
                ASSERT_TRUE(!sucess);

                int64_t ct = 1;
                std::map<int,int>::const_iterator it;
                for (it = stl.begin(); it != stli; it++) {
                        ct++;
                }
                int64_t rankasc= volt.rankAsc(val);
                if (rankasc != ct)
                    printf("false: DEPULICATE... unique_rankAsc expected %ld, but got %ld\n", (long)ct, (long)rankasc);
                ASSERT_TRUE(rankasc == ct);
                volti = volt.findRank(rankasc);
                ASSERT_TRUE(volti.key() == val);

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

                ASSERT_TRUE(stl.erase(val));
                ASSERT_TRUE(volt.erase(val));
            }
        }
    }

    ASSERT_TRUE(volt.verify());
    ASSERT_TRUE(volt.verifyRank());
}

TEST_F(CompactingMapTest, SimpleMultiRank) {
    // Start the counting index feature
    voltdb::CompactingMap<NormalKeyValuePair<int, int>, IntComparator, true> volt(false, IntComparator());
    ASSERT_TRUE(volt.verify());
    voltdb::CompactingMap<NormalKeyValuePair<int, int>, IntComparator, true>::iterator volti;
    bool sucess;
    int64_t rankasc, rankupper;

    sucess = volt.insert(std::pair<int,int>(1, 1)); ASSERT_TRUE(sucess);
    sucess = volt.insert(std::pair<int,int>(2, 2)); ASSERT_TRUE(sucess);
    sucess = volt.insert(std::pair<int,int>(3, 3)); ASSERT_TRUE(sucess);
    sucess = volt.insert(std::pair<int,int>(3, 3)); ASSERT_TRUE(sucess);
    sucess = volt.insert(std::pair<int,int>(3, 3)); ASSERT_TRUE(sucess);
    sucess = volt.insert(std::pair<int,int>(5, 5)); ASSERT_TRUE(sucess);
    sucess = volt.insert(std::pair<int,int>(6, 6)); ASSERT_TRUE(sucess);
    sucess = volt.insert(std::pair<int,int>(6, 6)); ASSERT_TRUE(sucess);
    sucess = volt.insert(std::pair<int,int>(8, 8)); ASSERT_TRUE(sucess);
    sucess = volt.insert(std::pair<int,int>(8, 8)); ASSERT_TRUE(sucess);

    rankasc = volt.rankAsc(1); ASSERT_TRUE(rankasc == 1);
    rankasc = volt.rankAsc(2); ASSERT_TRUE(rankasc == 2);
    rankasc = volt.rankAsc(3); ASSERT_TRUE(rankasc == 3);
    rankasc = volt.rankAsc(5); ASSERT_TRUE(rankasc == 6);
    rankasc = volt.rankAsc(6); ASSERT_TRUE(rankasc == 7);
    rankasc = volt.rankAsc(8); ASSERT_TRUE(rankasc == 9);
    // key is not in Map, return -1 always
    rankasc = volt.rankAsc(0); ASSERT_TRUE(rankasc == -1);
    rankasc = volt.rankAsc(7); ASSERT_TRUE(rankasc == -1);
    rankasc = volt.rankAsc(12); ASSERT_TRUE(rankasc == -1);

    rankupper = volt.rankUpper(1); ASSERT_TRUE(rankupper == 1);
    rankupper = volt.rankUpper(2); ASSERT_TRUE(rankupper == 2);
    rankupper = volt.rankUpper(3); ASSERT_TRUE(rankupper == 5);
    rankupper = volt.rankUpper(5); ASSERT_TRUE(rankupper == 6);
    rankupper = volt.rankUpper(6); ASSERT_TRUE(rankupper == 8);
    rankupper = volt.rankUpper(8); ASSERT_TRUE(rankupper == 10);
    // key is not in Map, return -1 always
    rankupper = volt.rankUpper(0); ASSERT_TRUE(rankupper == -1);
    rankupper = volt.rankUpper(7); ASSERT_TRUE(rankupper == -1);
    rankupper = volt.rankUpper(12); ASSERT_TRUE(rankupper == -1);
}

TEST_F(CompactingMapTest, RandomMultiRank) {
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
    // Start the counting index feature
    voltdb::CompactingMap<NormalKeyValuePair<std::string, std::string>, StringComparator, true> volt(false, StringComparator());

    std::multimap<std::string, std::string>::iterator stli;
    voltdb::CompactingMap<NormalKeyValuePair<std::string, std::string>, StringComparator, true>::iterator volti;

    srand(0);

    // just check that an error can be found
    // assert(false);

    for (int i = 0; i < ITERATIONS; i++) {
        if ((i % 1000) == 0) {
            ASSERT_TRUE(volt.verify());
            ASSERT_TRUE(volt.verifyRank());
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
            std::pair<voltdb::CompactingMap<NormalKeyValuePair<std::string, std::string>, StringComparator, true>::iterator, voltdb::CompactingMap<NormalKeyValuePair<std::string, std::string>, StringComparator, true>::iterator> volti_pair;
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
    ASSERT_TRUE(volt.verifyRank());
}

int main() {
    return TestSuite::globalInstance()->runAll();
}
