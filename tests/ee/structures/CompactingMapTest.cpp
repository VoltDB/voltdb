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
#include <cstdlib>
#include <sys/time.h>
#include "harness.h"
#include "structures/CompactingMap.h"

using namespace voltdb;

class CompactingMapTest : public Test {
public:
    CompactingMapTest() {
    }

    ~CompactingMapTest() {
    }
    
    void printCompactingMap(voltdb::CompactingMap<int, int> &m) {
    	voltdb::CompactingMap<int, int>::iterator iter;
    
    	printf(" compactmap [ ");
		for (iter = m.begin(); !iter.isEnd(); iter.moveNext()) {
			printf("%d ", iter.key());
		}
		printf("]\n");
		fflush(stdout);
	}
	
	void printMultiMap(std::multimap<int, int> &m) {
		std::multimap<int, int>::iterator iter;
		
		printf("   multimap [ ");
		for (iter = m.begin(); iter != m.end(); iter++) {
			printf("%d ", iter->first);
		}
		printf("]\n");
		fflush(stdout);
	}
	
	void printMap(std::map<int, int> &m) {
		std::map<int, int>::iterator iter;
		
		printf("       map [ ");
		for (iter = m.begin(); iter != m.end(); iter++) {
			printf("%d ", iter->first);
		}
		printf("]\n");
		fflush(stdout);
	}
};

TEST_F(CompactingMapTest, Benchmark) {
    const int ITERATIONS = 100000;
    
    std::map<int,int> stl;
	voltdb::CompactingMap<int, int> volt(true, std::less<int>());
	
    timeval tp;
    gettimeofday(&tp, NULL);
    printf("Time: %ld, %ld\n", (long int)tp.tv_sec, (long int)tp.tv_usec);
    int64_t t1 = tp.tv_sec * 1000 + tp.tv_usec / 1000;
    fflush(stdout);

    std::map<int,int>::const_iterator iter_stl;

    for (int i = 0; i < ITERATIONS; i++) {
        stl.insert(std::pair<int,int>(i,i));
        iter_stl = stl.find(i / 2);
        assert(iter_stl != stl.end());
        assert(iter_stl->second == (i / 2));
    }

    for (int i = 0; i < ITERATIONS; i += 2) {
        stl.erase(i);
    }

    iter_stl = stl.begin();
        for (int i = 1; i < ITERATIONS; i += 2, iter_stl++) {
            assert(iter_stl != stl.end());
            assert(iter_stl->second == i);
    }

    gettimeofday(&tp, NULL);
    printf("Time: %ld, %ld\n", (long int)tp.tv_sec, (long int)tp.tv_usec);
    int64_t t2 = tp.tv_sec * 1000 + tp.tv_usec / 1000;
    printf("Time elapsed: %.2f\n", (t2 - t1) / (double) 1000);
    fflush(stdout);
    
    voltdb::CompactingMap<int, int>::iterator iter;
    
    for (int i = 0; i < ITERATIONS; i++) {
		volt.insert(std::pair<int,int>(i,i));
		//assert(volt.size() == i + 1);
        
        iter = volt.find(i / 2);
        assert(!iter.isEnd());
        assert(iter.value() == (i / 2));
	}
		
	for (int i = 0; i < ITERATIONS; i += 2) {
		volt.erase(i);
        iter = volt.find(i);
        assert(iter.isEnd());
	}
    
    iter = volt.begin();
    for (int i = 1; i < ITERATIONS; i += 2, iter.moveNext()) {
        assert(!iter.isEnd());
        assert(iter.value() == i);
    }
    
    for (int i = 0; i < ITERATIONS; i += 2) {
        iter = volt.find(i);
        assert(iter.isEnd());
    }
    
    gettimeofday(&tp, NULL);
    printf("Time: %ld, %ld\n", (long int)tp.tv_sec, (long int)tp.tv_usec);
    int64_t t3 = tp.tv_sec * 1000 + tp.tv_usec / 1000;
    printf("Time elapsed: %.2f\n", (t3 - t2) / (double) 1000);
    fflush(stdout);    
}

TEST_F(CompactingMapTest, Trivial) {
	voltdb::CompactingMap<int, int> m(true, std::less<int>());
    bool success = m.insert(std::pair<int,int>(2,2));
    assert(success);
    success = m.insert(std::pair<int,int>(1,1));
    assert(success);
    success = m.insert(std::pair<int,int>(3,3));
    assert(success);
    m.verify();
    
    voltdb::CompactingMap<int, int> m2(false, std::less<int>());
    success = m2.insert(std::pair<int,int>(1,1));
    assert(success);
    success = m2.insert(std::pair<int,int>(1,1));
    assert(success);
    success = m2.insert(std::pair<int,int>(1,1));
    assert(success);
    success = m2.insert(std::pair<int,int>(1,1));
    assert(success);
    success = m2.insert(std::pair<int,int>(1,1));
    assert(success);
    success = m2.insert(std::pair<int,int>(1,1));
    assert(success);
    success = m2.insert(std::pair<int,int>(1,1));
    assert(success);
    m.verify();
}

TEST_F(CompactingMapTest, RandomUnique) {
    const int ITERATIONS = 10000;
    const int BIGGEST_VAL = 100;
    
    const int INSERT = 0;
    const int DELETE = 1;
    
    std::map<int,int> stl;
	voltdb::CompactingMap<int, int> volt(true, std::less<int>());
    
    std::map<int,int>::const_iterator stli;
    voltdb::CompactingMap<int, int>::iterator volti;
    
    srand(0);
    
    for (int i = 0; i < ITERATIONS; i++) {
        if ((i % 1000) == 0)
            volt.verify();
        
        int op = rand() % 2;
        int val = rand() % BIGGEST_VAL;
        if (op == INSERT) {
            stli = stl.find(val);
            volti = volt.find(val);
            if (stli == stl.end()) {
                assert(volti.isEnd());
                
                stl.insert(std::pair<int,int>(val, val));
                bool sucess = volt.insert(std::pair<int,int>(val, val));
                assert(sucess);
            }
            else {
                assert(!volti.isEnd());
                assert(stli->first == volti.key());
                assert(stli->first == val);
                
                bool sucess = volt.insert(std::pair<int,int>(val, val));
                assert(!sucess);
            }
        }
        if (op == DELETE) {
            stli = stl.find(val);
            volti = volt.find(val);
            if (stli == stl.end()) {
                assert(volti.isEnd());
                bool success = volt.erase(val);
                assert(!success);
            }
            else {
                assert(!volti.isEnd());
                assert(stli->first == volti.key());
                
                stl.erase(val);
                volt.erase(val);
            }
        }
    }
    
    volt.verify();
}

TEST_F(CompactingMapTest, RandomMulti) {
    const int ITERATIONS = 10000;
    const int BIGGEST_VAL = 100;
    
    const int INSERT = 0;
    const int DELETE = 1;
    
    std::multimap<int,int> stl;
	voltdb::CompactingMap<int, int> volt(false, std::less<int>());
    
    std::multimap<int,int>::iterator stli;
    voltdb::CompactingMap<int, int>::iterator volti;
    
    srand(0);
    
    for (int i = 0; i < ITERATIONS; i++) {
        if ((i % 1000) == 0)
            volt.verify();
        
        int op = rand() % 2;
        int val = rand() % BIGGEST_VAL;
        if (op == INSERT) {
            
            stli = stl.find(val);
            volti = volt.find(val);
            if (stli == stl.end()) {
                assert(volti.isEnd());
            }
            else {
                assert(!volti.isEnd());
                assert(stli->first == volti.key());
                
                int stlCount = 0;
                for (; (stli != stl.end()) && (stli->first == val); stli++)
                    stlCount++;
                
                int voltCount = 0;
                for (; (!volti.isEnd()) && (volti.key() == val); volti.moveNext())
                    voltCount++;
                
                assert(stlCount == voltCount);
                assert(stlCount == stl.count(val));
            }
            
            stli = stl.insert(std::pair<int,int>(val, val));
            assert(stli != stl.end());
            bool success = volt.insert(std::pair<int,int>(val, val));
            assert(success);
        }
        if (op == DELETE) {            
            stli = stl.find(val);
            volti = volt.find(val);
            if (stli == stl.end()) {
                assert(volti.isEnd());
                bool success = volt.erase(val);
                assert(!success);
            }
            else {
                assert(!volti.isEnd());
                assert(stli->first == volti.key());
                
                stl.erase(stli);
                bool success = volt.erase(val);
                assert(success);
            }
        }
    }
    
    volt.verify();
}

TEST_F(CompactingMapTest, Bounds) {
	voltdb::CompactingMap<int, int> volt(true, std::less<int>());
    
    assert(volt.lowerBound(1).isEnd());
    assert(volt.upperBound(1).isEnd());
    
    volt.insert(std::pair<int,int>(1,1));
    
    assert(volt.lowerBound(0).key() == 1);
    assert(volt.lowerBound(1).key() == 1);
    assert(volt.lowerBound(2).isEnd());
    
    assert(volt.upperBound(0).key() == 1);
    assert(volt.upperBound(1).isEnd());
    assert(volt.upperBound(2).isEnd());
    
    for (int i = 3; i <= 99; i += 2)
        volt.insert(std::pair<int,int>(i,i));
    
    assert(volt.lowerBound(99).key() == 99);
    assert(volt.upperBound(99).isEnd());
    assert(volt.lowerBound(100).isEnd());
    assert(volt.upperBound(100).isEnd());
    
    for (int i = 0; i <= 98; i += 2) {
        assert(volt.upperBound(i).key() == i + 1);
        assert(volt.lowerBound(i).key() == i + 1);
    }
    for (int i = 1; i <= 98; i += 2) {
        assert(volt.upperBound(i).key() == i + 2);
        assert(volt.lowerBound(i).key() == i);
    }
    
    // test range
    voltdb::CompactingMap<int, int> volt2(false, std::less<int>());

    volt2.insert(std::pair<int,int>(0,0));
    volt2.insert(std::pair<int,int>(1,666));
    volt2.insert(std::pair<int,int>(1,1));
    volt2.insert(std::pair<int,int>(1,777));
    volt2.insert(std::pair<int,int>(2,2));
    volt2.insert(std::pair<int,int>(3,888));
    volt2.insert(std::pair<int,int>(3,3));
    volt2.insert(std::pair<int,int>(3,3));
    volt2.insert(std::pair<int,int>(3,999));
    
    std::pair<voltdb::CompactingMap<int, int>::iterator, voltdb::CompactingMap<int, int>::iterator> p;
    
    p = volt2.equalRange(1);
    assert(p.first.value() == 666);
    assert(p.second.value() == 2);
    
    p = volt2.equalRange(3);
    assert(p.first.value() == 888);
    assert(p.second.isEnd());
    
    p = volt2.equalRange(2);
    assert(p.first.value() == 2);
    assert(p.second.value() == 888);
}

int main() {
    return TestSuite::globalInstance()->runAll();
}
