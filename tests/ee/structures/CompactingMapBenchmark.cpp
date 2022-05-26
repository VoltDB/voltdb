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
#define PRINT_FREQUENCY 100
#define WARM_UP 50
#define MAXSCALE 10000000

int VEC[MAXSCALE] = {};

int* getRandomValues(int size, int max) {
    srand(static_cast<unsigned int>(getMicrosNow() % 1000000));
    for (int i = 0; i < size; i++) {
        int val = rand() % max;
        VEC[i] = val;
    }
    return VEC;
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
        reset();
    }

    void start() {
        m_start = getMicrosNow();
    }

    void stop() {
        m_duration += getMicrosNow() - m_start;
        m_count++;
    }

    void print() {
        if (m_count < 1) {
            return;
        }
        std::cout << mapCategoryToString(m_name) << " finished in " << m_duration
                << " microseconds for " << m_count << " runs, AVG "
                << m_duration / m_count << " microseconds" << std::endl;
    }

    void reset() {
        m_start = 0;
        m_duration = 0;
        m_count = 0;
    }

private:
    int m_name;

    long m_start;
    long m_duration;
    int m_count;
};

void resultPrinter(std::string name, int scale,
        BenchmarkRecorder benVoltMap, BenchmarkRecorder benStl,
        BenchmarkRecorder benBoost, BenchmarkRecorder benVoltHash) {
    std::cout << "Benchmark: " << name << ", scale size " << scale << "\n";

    std::vector<BenchmarkRecorder> result;
    result.push_back(benVoltMap);
    result.push_back(benStl);
    result.push_back(benBoost);
    result.push_back(benVoltHash);

    for (int i = 0; i < result.size(); i++) {
        BenchmarkRecorder ben = result[i];
        ben.print();
    }
}

const char* interpret(bool flag) {
    if (flag) {
        return "enabled";
    }
    return "disabled";
}

void BenchmarkRun(
        int DATA_SCALE,
        int SLEEP_IN_SECONDS,
        int READ_OPS_REPEAT,
        bool runScan,
        bool runScanNoEndCheck,
        bool runLookup,
        bool runDelete,
        bool runVoltMap,
        bool runStlMap,
        bool runBoostMap,
        bool runVoltHash) {
    int BIGGEST_VAL = DATA_SCALE;
    int ITERATIONS = DATA_SCALE / 10; // for 10% LOOK UP and DELETE

    printf("=============\n"
            "Benchmark starts with parameters as\n"
            "DATA_SCALE %d\n"
            "SLEEP_IN_SECONDS %d\n"
            "READ_OPS_REPEAT %d\n"
            "runScan = %s\n"
            "runScanNoEndCheck = %s\n"
            "runLookup = %s\n"
            "runDelete = %s\n"
            "runVoltMap = %s\n"
            "runStlMap = %s\n"
            "runBoostMap = %s\n"
            "runVoltHash = %s\n"
            "=============\n",
            DATA_SCALE,
            SLEEP_IN_SECONDS,
            READ_OPS_REPEAT,
            interpret(runScan),
            interpret(runScanNoEndCheck),
            interpret(runLookup),
            interpret(runDelete),
            interpret(runVoltMap),
            interpret(runStlMap),
            interpret(runBoostMap),
            interpret(runVoltHash)
    );

    string str;

    int* input = getRandomValues(DATA_SCALE, BIGGEST_VAL);

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

    //
    // INSERT the data
    //
    printf("Preparing to run INSERT benchmark in %d seconds...\n", SLEEP_IN_SECONDS);
    sleep(SLEEP_IN_SECONDS);

    {
        BenchmarkRecorder benVoltMap(VoltMap), benStl(STLMap), benBoost(BoostUnorderedMap), benVoltHash(VoltHash);
        if (runVoltMap) {
            benVoltMap.start();
            for (int i = 0; i < DATA_SCALE; i++) {
                int val = input[i];
                voltMap.insert(std::pair<int,int>(val, val));
            }
            benVoltMap.stop();
        }

        if (runStlMap) {
            benStl.start();
            for (int i = 0; i < DATA_SCALE; i++) {
                int val = input[i];
                stlMap.insert(std::pair<int,int>(val, val));
            }
            benStl.stop();
        }

        if (runBoostMap) {
            benBoost.start();
            for (int i = 0; i < DATA_SCALE; i++) {
                int val = input[i];
                boostMap.insert(std::pair<int,int>(val, val));
            }
            benBoost.stop();
        }

        if (runVoltHash) {
            benVoltHash.start();
            for (int i = 0; i < DATA_SCALE; i++) {
                int val = input[i];
                voltHash.insert(val, val);
            }
            benVoltHash.stop();
        }

        resultPrinter("INSERT", DATA_SCALE, benVoltMap, benStl, benBoost, benVoltHash);
    }

    //
    // SCAN
    //
    if (runScan) {
        BenchmarkRecorder benVoltMap(VoltMap), benStl(STLMap), benBoost(BoostUnorderedMap), benVoltHash(VoltHash);

        printf("Preparing to run SCAN benchmark in %d seconds...\n", SLEEP_IN_SECONDS);
        sleep(SLEEP_IN_SECONDS);

        //
        // Read only operations has a scale factor to repeat the tests
        //
        for (int i = 0; i < READ_OPS_REPEAT; i++) {
            // clean up
            if (i == WARM_UP) {
                benVoltMap.reset();
                benStl.reset();
                printf("Finish warm up...\n");
            }

            if (runVoltMap) {
                iter_volt_map = voltMap.begin();
                benVoltMap.start();
                while(! iter_volt_map.isEnd()) {
                    iter_volt_map.moveNext();
                }
                benVoltMap.stop();
            }

            if (runStlMap) {
                iter_stl = stlMap.begin();
                benStl.start();
                while(iter_stl != stlMap.end()) {
                    iter_stl++;
                }
                benStl.stop();
            }
        }
        resultPrinter("SCAN", DATA_SCALE, benVoltMap, benStl, benBoost, benVoltHash);
    }

    //
    // SCAN WITHOUT END CHECK
    //
    if (runScanNoEndCheck) {
        BenchmarkRecorder benVoltMap(VoltMap), benStl(STLMap), benBoost(BoostUnorderedMap), benVoltHash(VoltHash);
        printf("Preparing to run Scan benchmark without END() function call in %d seconds...\n", SLEEP_IN_SECONDS);
        sleep(SLEEP_IN_SECONDS);

        for (int i = 0; i < READ_OPS_REPEAT; i++) {
            // clean up
            if (i == WARM_UP) {
                benVoltMap.reset();
                benStl.reset();
                printf("Finish warm up...\n");
            }

            // SCAN without END() factor
            if (runVoltMap) {
                iter_volt_map = voltMap.begin();
                benVoltMap.start();
                for (int i = 0; i < DATA_SCALE; i++) {
                    iter_volt_map.moveNext();
                }
                benVoltMap.stop();
            }

            if (runStlMap) {
                iter_stl = stlMap.begin();
                benStl.start();
                for (int i = 0; i < DATA_SCALE; i++) {
                    iter_stl++;
                }
                benStl.stop();
            }
        }
        resultPrinter("SCAN without END() factor", DATA_SCALE, benVoltMap, benStl, benBoost, benVoltHash);
    }


    //
    // LOOKUP
    //
    if (runLookup) {
        BenchmarkRecorder benVoltMap(VoltMap), benStl(STLMap), benBoost(BoostUnorderedMap), benVoltHash(VoltHash);
        int* keys = getRandomValues(ITERATIONS, BIGGEST_VAL);

        printf("Preparing to run LOOKUP benchmark in %d seconds...\n", SLEEP_IN_SECONDS);
        sleep(SLEEP_IN_SECONDS);

        for (int i = 0; i < READ_OPS_REPEAT; i++) {
            // clean up
            if (i == WARM_UP) {
                benVoltMap.reset();
                benStl.reset();
                benBoost.reset();
                benVoltHash.reset();
                printf("Finish warm up...\n");
            }

            if (runVoltMap) {
                benVoltMap.start();
                for (int i = 0; i< ITERATIONS; i++) {
                    int val = keys[i];
                    iter_volt_map = voltMap.find(val);
                }
                benVoltMap.stop();
            }

            if (runStlMap) {
                benStl.start();
                for (int i = 0; i< ITERATIONS; i++) {
                    int val = keys[i];
                    iter_stl = stlMap.find(val);
                }
                benStl.stop();
            }

            if (runBoostMap) {
                benBoost.start();
                for (int i = 0; i < DATA_SCALE; i++) {
                    int val = input[i];
                    iter_boost_map = boostMap.find(val);
                }
                benBoost.stop();
            }

            if (runVoltHash) {
                benVoltHash.start();
                for (int i = 0; i < DATA_SCALE; i++) {
                    int val = input[i];
                    iter_volt_hash = voltHash.find(val);
                }
                benVoltHash.stop();
            }
        }
        resultPrinter("LOOKUP", ITERATIONS, benVoltMap, benStl, benBoost, benVoltHash);
    }

    //
    // DELETE
    //
    if (runDelete) {
        BenchmarkRecorder benVoltMap(VoltMap), benStl(STLMap), benBoost(BoostUnorderedMap), benVoltHash(VoltHash);
        int* deletes = getRandomValues(ITERATIONS, BIGGEST_VAL);
        printf("Preparing to run DELETE benchmark in %d seconds...\n", SLEEP_IN_SECONDS);
        sleep(SLEEP_IN_SECONDS);

        if (runVoltMap) {
            benVoltMap.start();
            for (int i = 0; i< ITERATIONS; i++) {
                int val = deletes[i];
                voltMap.erase(val);
            }
            benVoltMap.stop();
        }

        if (runStlMap) {
            benStl.start();
            for (int i = 0; i< ITERATIONS; i++) {
                int val = deletes[i];
                stlMap.erase(val);
            }
            benStl.stop();
        }

        if (runBoostMap) {
            benBoost.start();
            for (int i = 0; i < DATA_SCALE; i++) {
                int val = input[i];
                boostMap.erase(val);
            }
            benBoost.stop();
        }

        if (runVoltHash) {
            benVoltHash.start();
            for (int i = 0; i < DATA_SCALE; i++) {
                int val = input[i];
                voltHash.erase(val);
            }
            benVoltHash.stop();
        }

        resultPrinter("DELETE", ITERATIONS, benVoltMap, benStl, benBoost, benVoltHash);
    }

    // still holds the data before the destructor gets called
    printf("Preparing to exit this benchmark run in %d seconds...\n", SLEEP_IN_SECONDS);
    sleep(SLEEP_IN_SECONDS);

    printf("=============\n"
            "Benchmark Finishes\n\n");
}

void BenchmarkRunWrapper(int DATA_SCALE, int SLEEP_IN_SECONDS, int READON_OPS_REPEAT, std::vector<bool> params) {
    size_t len = params.size();

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

    BenchmarkRun(DATA_SCALE, SLEEP_IN_SECONDS, READON_OPS_REPEAT,
            runScan, runScanNoEndCheck, runLookup, runDelete,
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
    int data_scale = 10000;
    int sleep_in_seconds = 5;
    int read_ops_repeat = 1;

    if ((argc > 1 && *argv[1] == '-') || argc <= 3) {
        printf("To run a benchmark, execute %s with command line arguments. "
                "The first 3 are required: ("
                "data_scale<int>, "
                "sleep_in_seconds<int>, "
                "read_ops_repeat<int>, "
                "runScan<0 or 1>, "
                "runScanNoEndCheck<0, 1>, "
                "runLookup<0, 1>, "
                "runDelete<0, 1>, "
                "runVoltMap<0, 1>, "
                "runStlMap<0, 1>, "
                "runBoostMap<0, 1>, "
                "runVoltHash<0, 1>)\n",
                argv[0]);
        return 0;
    }
    data_scale = std::atoi(argv[1]);
    if (data_scale > MAXSCALE) {
        printf("data scale larger than %d is not supported\n", MAXSCALE);
        return 0;
    }

    sleep_in_seconds = std::atoi(argv[2]);
    read_ops_repeat = std::atoi(argv[3]) + WARM_UP;

    // 0 is FALSE, others are TRUE
    for (int i = 3; i < argc; i++) {
        params.push_back(isTrue(argv[i]));
    }

    // ONLY test with input parameters
    BenchmarkRunWrapper(data_scale,sleep_in_seconds,read_ops_repeat,params);

    return 0;
}
