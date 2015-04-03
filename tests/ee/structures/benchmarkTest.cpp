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
#include <vector>
#include <boost/unordered_map.hpp>

#include "harness.h"
#include "structures/CompactingMap.h"
#include "structures/CompactingHashTable.h"
#include "structures/btree.h"


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

class IntComparator : public btree::btree_key_compare_to_tag {
public:
    inline int operator()(const int &lhs, const int &rhs) const {
        if (lhs > rhs) return 1;
        else if (lhs < rhs) return -1;
        else return 0;
    }
};

class CompactingMapBenchTest : public Test {
public:
    CompactingMapBenchTest() {
    }

    ~CompactingMapBenchTest() {
    }
};

int64_t getMicrosNow () {
    timeval tv;
    gettimeofday(&tv, NULL);
    return tv.tv_sec * 1000000 + tv.tv_usec;
}

std::vector<int> getRandomValues(int size, int max) {
    std::vector<int> vec;
    srand(getMicrosNow() % 1000000);
    for (int i = 0; i < size; i++) {
        int val = rand() % max;
        vec.push_back(val);
    }

    return vec;
}

#define VoltMap 1
#define VoltHash 2
#define STLMap 3
#define BtreeMap 4
#define BoostUnorderedMap 5
std::string mapCategoryToString(int mapCategory) {
    switch(mapCategory) {
    case VoltMap:
        return "VoltMap";
    case VoltHash:
        return "VoltHash";
    case STLMap:
        return "STLMap";
    case BtreeMap:
        return "BtreeMap";
    case BoostUnorderedMap:
        return "BoostUnorderedMap";
    default:
        return "invalid";
    }
}

class Benchmark {
public:
    Benchmark(int mapCategory) {
        m_name = mapCategory;
        m_start = 0;
        m_duration = 0;
    }

    void start() {
        m_start = getMicrosNow();
        m_duration = 0;
    }

    void stop() {
        m_duration += getMicrosNow() - m_start;
        m_start = getMicrosNow();
    }

    void print() {
        std::cout << mapCategoryToString(m_name) << " finished in " << m_duration << " microseconds\n";
        std::cout.flush();
    }
private:
    int m_name;

    int64_t m_start;
    int64_t m_duration;
};

void resultPrinter(std::string name, int scale, std::vector<Benchmark> result) {
    std::cout << "\n\nBenchmark: " << name << ", scale size " << scale << std::endl;

    for (int i = 0; i < result.size(); i++) {
        Benchmark ben = result[i];
        ben.print();
    }

    std::cout << std::endl;
    std::cout.flush();
}

void BenchmarkRun(int NUM_OF_VALUES) {
    int BIGGEST_VAL = NUM_OF_VALUES;
    int ITERATIONS = NUM_OF_VALUES / 10; // for 10% LOOK UP and DELETE

    std::vector<Benchmark> result;

    std::vector<int> input = getRandomValues(NUM_OF_VALUES, BIGGEST_VAL);

    // tree map and hash map
    voltdb::CompactingMap<NormalKeyValuePair<int, int>, IntComparator, false> voltMap(false, IntComparator());
    std::multimap<int,int> stlMap;

    std::allocator<int> alloc = std::allocator<int>();
    btree::btree<btree::btree_map_params<int, int, IntComparator, std::allocator<int>, 256> > btreeMap(IntComparator(), alloc);
    boost::unordered_multimap<int, int> boostMap;
    voltdb::CompactingHashTable<int,int> voltHash(false);

    // Iterators
    voltdb::CompactingMap<NormalKeyValuePair<int, int>, IntComparator, false>::iterator iter_volt_map;
    std::multimap<int, int>::const_iterator iter_stl;
    btree::btree<btree::btree_map_params<int, int, IntComparator, std::allocator<int>, 256> >::iterator iter_btree;
    boost::unordered_multimap<int,int>::iterator iter_boost_map;
    voltdb::CompactingHashTable<int,int>::iterator iter_volt_hash;

    // benchmark
    Benchmark benVoltMap(VoltMap);
    Benchmark benStl(STLMap);
    Benchmark benBtree(BtreeMap);
    Benchmark benBoost(BoostUnorderedMap);
    Benchmark benVoltHash(VoltHash);

    // INSERT
    benVoltMap.start();
    for (int i = 0; i < NUM_OF_VALUES; i++) {
        int val = input[i];
        voltMap.insert(std::pair<int,int>(val, val));
    }
    benVoltMap.stop();
    result.push_back(benVoltMap);

    benStl.start();
    for (int i = 0; i < NUM_OF_VALUES; i++) {
        int val = input[i];
        stlMap.insert(std::pair<int,int>(val, val));
    }
    benStl.stop();
    result.push_back(benStl);

    benBtree.start();
    for (int i = 0; i < NUM_OF_VALUES; i++) {
        int val = input[i];
        btreeMap.insert_multi(std::pair<int,int>(val, val));
    }
    benBtree.stop();
    result.push_back(benBtree);

    benBoost.start();
    for (int i = 0; i < NUM_OF_VALUES; i++) {
        int val = input[i];
        boostMap.insert(std::pair<int,int>(val, val));
    }
    benBoost.stop();
    result.push_back(benBoost);

    benVoltHash.start();
    for (int i = 0; i < NUM_OF_VALUES; i++) {
        int val = input[i];
        voltHash.insert(val, val);
    }
    benVoltHash.stop();
    result.push_back(benVoltHash);

    resultPrinter("INSERT", NUM_OF_VALUES, result);
    result.clear();

    // SCAN
    iter_volt_map = voltMap.begin();
    benVoltMap.start();
    while(! iter_volt_map.isEnd()) {
        iter_volt_map.moveNext();
    }
    benVoltMap.stop();
    result.push_back(benVoltMap);

    iter_stl = stlMap.begin();
    benStl.start();
    while(iter_stl != stlMap.end()) {
        iter_stl++;
    }
    benStl.stop();
    result.push_back(benStl);

    iter_btree = btreeMap.begin();
    benBtree.start();
    while(iter_btree != btreeMap.end()) {
        iter_btree++;
    }
    benBtree.stop();
    result.push_back(benBtree);

    resultPrinter("SCAN", NUM_OF_VALUES, result);
    result.clear();


    // LOOK UP
    std::vector<int> keys = getRandomValues(ITERATIONS, BIGGEST_VAL);

    benVoltMap.start();
    for (int i = 0; i< ITERATIONS; i++) {
        int val = keys[i];
        iter_volt_map = voltMap.find(val);
    }
    benVoltMap.stop();
    result.push_back(benVoltMap);

    benStl.start();
    for (int i = 0; i< ITERATIONS; i++) {
        int val = keys[i];
        iter_stl = stlMap.find(val);
    }
    benStl.stop();
    result.push_back(benStl);

    benBtree.start();
    for (int i = 0; i< ITERATIONS; i++) {
        int val = keys[i];
        iter_btree = btreeMap.find_multi(val);
    }
    benBtree.stop();
    result.push_back(benBtree);

    benBoost.start();
    for (int i = 0; i < NUM_OF_VALUES; i++) {
        int val = input[i];
        iter_boost_map = boostMap.find(val);
    }
    benBoost.stop();
    result.push_back(benBoost);

    benVoltHash.start();
    for (int i = 0; i < NUM_OF_VALUES; i++) {
        int val = input[i];
        iter_volt_hash = voltHash.find(val);
    }
    benVoltHash.stop();
    result.push_back(benVoltHash);

    resultPrinter("LOOK UP", ITERATIONS, result);
    result.clear();

    // DELETE
    std::vector<int> deletes = getRandomValues(ITERATIONS, BIGGEST_VAL);

    benVoltMap.start();
    for (int i = 0; i< ITERATIONS; i++) {
        int val = deletes[i];
        iter_volt_map = voltMap.find(val);
        if (!iter_volt_map.isEnd()) {
            voltMap.erase(iter_volt_map);
        }
    }
    benVoltMap.stop();
    result.push_back(benVoltMap);

    benStl.start();
    for (int i = 0; i< ITERATIONS; i++) {
        int val = deletes[i];
        iter_stl = stlMap.find(val);
        if (iter_stl != stlMap.end()) {
            stlMap.erase(iter_stl);
        }
    }
    benStl.stop();
    result.push_back(benStl);

    benBtree.start();
    for (int i = 0; i< ITERATIONS; i++) {
        int val = deletes[i];
        iter_btree = btreeMap.find_multi(val);
        if (iter_btree != btreeMap.end()) {
            btreeMap.erase(iter_btree);
        }
    }
    benBtree.stop();
    result.push_back(benBtree);

    benBoost.start();
    for (int i = 0; i < NUM_OF_VALUES; i++) {
        int val = input[i];
        iter_boost_map = boostMap.find(val);
        if (iter_boost_map != boostMap.end()) {
            boostMap.erase(iter_boost_map);
        }
    }
    benBoost.stop();
    result.push_back(benBoost);

    benVoltHash.start();
    for (int i = 0; i < NUM_OF_VALUES; i++) {
        int val = input[i];
        iter_volt_hash = voltHash.find(val);
        if (!iter_volt_hash.isEnd()) {
            voltHash.erase(iter_volt_hash);
        }
    }
    benVoltHash.stop();
    result.push_back(benVoltHash);

    resultPrinter("DELETE", ITERATIONS, result);
    result.clear();
}

TEST_F(CompactingMapBenchTest, RandomInsert) {
    // change this number to 6 to run full benchmark tests
    int length = 0;
    int scales[] = {100, 1000, 10000, 100000, 1000000, 10000000};

    for (int i = 0; i < length; i++) {
        int scale = scales[i];
        printf("=============\nBenchmark Run, Scale %d\n", scale);
        BenchmarkRun(scale);
        printf("=============\nBenchmark Finishes\n\n");
    }
}

int main() {
    return TestSuite::globalInstance()->runAll();
}
