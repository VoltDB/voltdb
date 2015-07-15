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
#include "boost/unordered_map.hpp"

#include "harness.h"
#include "structures/CompactingMap.h"
#include "structures/CompactingHashTable.h"

using namespace voltdb;
using namespace std;

class IntComparator {
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
#define BoostUnorderedMap 4
std::string mapCategoryToString(int mapCategory) {
    switch(mapCategory) {
    case VoltMap:
        return "VoltMap";
    case VoltHash:
        return "VoltHash";
    case STLMap:
        return "STLMap";
    case BoostUnorderedMap:
        return "BoostUnorderedMap";
    default:
        return "invalid";
    }
}

class BenchmarkRecorder {
public:
    BenchmarkRecorder(int mapCategory) {
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

void resultPrinter(std::string name, int scale, std::vector<BenchmarkRecorder> result) {
    std::cout << "Benchmark: " << name << ", scale size " << scale << std::endl;

    for (int i = 0; i < result.size(); i++) {
        BenchmarkRecorder ben = result[i];
        ben.print();
    }

    std::cout << std::endl;
    std::cout.flush();
}

void BenchmarkRun(int NUM_OF_VALUES,
        bool runScan=true,
        bool runScanNoEndCheck=false,
        bool runLookup=false,
        bool runDelete=false,
        bool runVoltMap = true,
        bool runStlMap=false,
        bool runBoostMap=false,
        bool runVoltHash=false
        ) {
    int BIGGEST_VAL = NUM_OF_VALUES;
    int ITERATIONS = NUM_OF_VALUES / 10; // for 10% LOOK UP and DELETE

    printf("=============\nBenchmark Run, Scale %d\n\n", NUM_OF_VALUES);

    std::vector<BenchmarkRecorder> result;
    std::vector<int> input = getRandomValues(NUM_OF_VALUES, BIGGEST_VAL);

    // tree map and hash map
    voltdb::CompactingMap<NormalKeyValuePair<int, int>, IntComparator, false> voltMap(false, IntComparator());
    std::multimap<int,int> stlMap;

    boost::unordered_multimap<int, int> boostMap;
    voltdb::CompactingHashTable<int,int> voltHash(false);

    // Iterators
    voltdb::CompactingMap<NormalKeyValuePair<int, int>, IntComparator, false>::iterator iter_volt_map;
    std::multimap<int, int>::const_iterator iter_stl;
    boost::unordered_multimap<int,int>::iterator iter_boost_map;
    voltdb::CompactingHashTable<int,int>::iterator iter_volt_hash;

    // benchmark
    BenchmarkRecorder benVoltMap(VoltMap);
    BenchmarkRecorder benStl(STLMap);
    BenchmarkRecorder benBoost(BoostUnorderedMap);
    BenchmarkRecorder benVoltHash(VoltHash);

    int SLEEP_10_SECONDS = 10;

    //
    // INSERT the data
    //
    printf("Preparing to run INSERT benchmark in %d seconds...\n", SLEEP_10_SECONDS);
    sleep(SLEEP_10_SECONDS);

    if (runVoltMap) {
        benVoltMap.start();
        for (int i = 0; i < NUM_OF_VALUES; i++) {
            int val = input[i];
            voltMap.insert(std::pair<int,int>(val, val));
        }
        benVoltMap.stop();
        result.push_back(benVoltMap);
    }

    if (runStlMap) {
        benStl.start();
        for (int i = 0; i < NUM_OF_VALUES; i++) {
            int val = input[i];
            stlMap.insert(std::pair<int,int>(val, val));
        }
        benStl.stop();
        result.push_back(benStl);
    }

    if (runBoostMap) {
        benBoost.start();
        for (int i = 0; i < NUM_OF_VALUES; i++) {
            int val = input[i];
            boostMap.insert(std::pair<int,int>(val, val));
        }
        benBoost.stop();
        result.push_back(benBoost);
    }

    if (runVoltHash) {
        benVoltHash.start();
        for (int i = 0; i < NUM_OF_VALUES; i++) {
            int val = input[i];
            voltHash.insert(val, val);
        }
        benVoltHash.stop();
        result.push_back(benVoltHash);
    }
    resultPrinter("INSERT", NUM_OF_VALUES, result);
    result.clear();

    //
    // SCAN
    //
    if (runScan) {
        printf("Preparing to run SCAN benchmark in %d seconds...\n", SLEEP_10_SECONDS);
        sleep(SLEEP_10_SECONDS);

        if (runVoltMap) {
            iter_volt_map = voltMap.begin();
            benVoltMap.start();
            while(! iter_volt_map.isEnd()) {
                iter_volt_map.moveNext();
            }
            benVoltMap.stop();
            result.push_back(benVoltMap);
        }

        if (runStlMap) {
            iter_stl = stlMap.begin();
            benStl.start();
            while(iter_stl != stlMap.end()) {
                iter_stl++;
            }
            benStl.stop();
            result.push_back(benStl);
        }
        resultPrinter("SCAN", NUM_OF_VALUES, result);
        result.clear();
    }

    if (runScanNoEndCheck) {
        printf("Preparing to run Scan benchmark without END() function call in %d seconds...\n", SLEEP_10_SECONDS);
        sleep(SLEEP_10_SECONDS);

        // SCAN without END() factor
        if (runVoltMap) {
            iter_volt_map = voltMap.begin();
            benVoltMap.start();
            for (int i = 0; i < NUM_OF_VALUES; i++) {
                iter_volt_map.moveNext();
            }
            benVoltMap.stop();
            result.push_back(benVoltMap);
        }

        if (runStlMap) {
            iter_stl = stlMap.begin();
            benStl.start();
            for (int i = 0; i < NUM_OF_VALUES; i++) {
                iter_stl++;
            }
            benStl.stop();
            result.push_back(benStl);
        }

        resultPrinter("SCAN without END() factor", NUM_OF_VALUES, result);
        result.clear();
    }


    //
    // LOOK UP
    //
    if(runLookup) {
        std::vector<int> keys = getRandomValues(ITERATIONS, BIGGEST_VAL);

        printf("Preparing to run LOOKUP benchmark in %d seconds...\n", SLEEP_10_SECONDS);
        sleep(SLEEP_10_SECONDS);

        if (runVoltMap) {
            benVoltMap.start();
            for (int i = 0; i< ITERATIONS; i++) {
                int val = keys[i];
                iter_volt_map = voltMap.find(val);
            }
            benVoltMap.stop();
            result.push_back(benVoltMap);
        }

        if (runStlMap) {
            benStl.start();
            for (int i = 0; i< ITERATIONS; i++) {
                int val = keys[i];
                iter_stl = stlMap.find(val);
            }
            benStl.stop();
            result.push_back(benStl);
        }

        if (runBoostMap) {
            benBoost.start();
            for (int i = 0; i < NUM_OF_VALUES; i++) {
                int val = input[i];
                iter_boost_map = boostMap.find(val);
            }
            benBoost.stop();
            result.push_back(benBoost);
        }

        if (runVoltHash) {
            benVoltHash.start();
            for (int i = 0; i < NUM_OF_VALUES; i++) {
                int val = input[i];
                iter_volt_hash = voltHash.find(val);
            }
            benVoltHash.stop();
            result.push_back(benVoltHash);
        }
        resultPrinter("LOOK UP", ITERATIONS, result);
        result.clear();
    }


    //
    // DELETE
    //
    if (runDelete) {
        std::vector<int> deletes = getRandomValues(ITERATIONS, BIGGEST_VAL);
        printf("Preparing to run DELETE benchmark in %d seconds...\n", SLEEP_10_SECONDS);
        sleep(SLEEP_10_SECONDS);

        if (runVoltMap) {
            benVoltMap.start();
            for (int i = 0; i< ITERATIONS; i++) {
                int val = deletes[i];
                voltMap.erase(val);
            }
            benVoltMap.stop();
            result.push_back(benVoltMap);
        }

        if (runStlMap) {
            benStl.start();
            for (int i = 0; i< ITERATIONS; i++) {
                int val = deletes[i];
                stlMap.erase(val);
            }
            benStl.stop();
            result.push_back(benStl);
        }

        if (runBoostMap) {
            benBoost.start();
            for (int i = 0; i < NUM_OF_VALUES; i++) {
                int val = input[i];
                boostMap.erase(val);
            }
            benBoost.stop();
            result.push_back(benBoost);
        }

        if (runVoltHash) {
            benVoltHash.start();
            for (int i = 0; i < NUM_OF_VALUES; i++) {
                int val = input[i];
                voltHash.erase(val);
            }
            benVoltHash.stop();
            result.push_back(benVoltHash);
        }

        resultPrinter("DELETE", ITERATIONS, result);
        result.clear();
    }

    // still holds the data before the destructor gets called
    printf("Preparing to exit this benchmark run in %d seconds...\n", SLEEP_10_SECONDS);
    sleep(SLEEP_10_SECONDS);

    printf("=============\nBenchmark Finishes\n\n");
}

void BenchmarkRunWrapper(int NUM_OF_VALUES, std::vector<bool> params) {
    int len = params.size();

    bool runScan=true, runScanNoEndCheck=false, runLookup=false, runDelete=false;
    int i = -1;
    if (len > ++i) runScan = params.at(i);
    if (len > ++i) runScanNoEndCheck = params.at(i);
    if (len > ++i) runLookup = params.at(i);
    if (len > ++i) runDelete = params.at(i);

    bool runVoltMap = true, runStlMap=false, runBoostMap=false, runVoltHash=false;
    if (len > ++i) runVoltMap = params.at(i);
    if (len > ++i) runStlMap = params.at(i);
    if (len > ++i) runBoostMap = params.at(i);
    if (len > ++i) runVoltHash = params.at(i);

    BenchmarkRun(NUM_OF_VALUES, runScan, runScanNoEndCheck, runLookup, runDelete,
            runVoltMap, runStlMap, runBoostMap, runVoltHash);
}

bool isTrue(char* arg) {
    if (strcmp(arg,"0") == 0) {
        return false;
    }
    return true;
}

int main(int argc, char *argv[]) {
    std::vector<bool> params;
    int scale = 10000;

    printf("Input parameters requested: (data_size_number_of_integers, "
            "runScan, runScanNoEndCheck, runLookup, runDelete, "
            "runVoltMap, runStlMap, runBoostMap, runVoltHash)\n");
    // 0 is FALSE, others are TRUE

    if (argc > 1) {
        scale = std::atoi(argv[1]);

        for (int i = 2; i < argc; i++) {
            params.push_back(isTrue(argv[i]));
        }
        // ONLY test with input parameters
        BenchmarkRunWrapper(scale, params);
    }


    return 0;
}
