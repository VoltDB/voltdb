/* This file is part of VoltDB.
 * Copyright (C) 2008-2019 VoltDB Inc.
 *
 * This file contains original code and/or modifications of original code.
 * Any modifications made by VoltDB Inc. are licensed under the following
 * terms and conditions:
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
/* Copyright (C) 2008 by H-Store Project
 * Brown University
 * Massachusetts Institute of Technology
 * Yale University
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
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

#include "harness.h"

#include "common/Pool.hpp"

using namespace std;
using namespace voltdb;

class PoolTest : public Test {
public:
    PoolTest() {};
};

TEST_F(PoolTest, SimpleTest) {
    Pool testPool;
    void* space;
    space = testPool.allocate(100000);
    EXPECT_NE(space, NULL);
}

TEST_F(PoolTest, OverflowTest) {
    Pool testPool;
    void* space;
    // Size the allocations to allow some chunk packing before overflowing several chunks.
    space = testPool.allocate(100000);
    EXPECT_NE(space, NULL);
    space = testPool.allocate(100000);
    EXPECT_NE(space, NULL);
    space = testPool.allocate(100000);
    EXPECT_NE(space, NULL);
    space = testPool.allocate(100000);
    EXPECT_NE(space, NULL);
    space = testPool.allocate(100000);
    EXPECT_NE(space, NULL);
    space = testPool.allocate(100000);
    EXPECT_NE(space, NULL);
}

TEST_F(PoolTest, ChunkyOverflowTest) {
    Pool testPool;
    void* space;
    // Size the allocations to not allow chunk packing before overflowing several chunks.
    space = testPool.allocate(200000);
    EXPECT_NE(space, NULL);
    space = testPool.allocate(200000);
    EXPECT_NE(space, NULL);
    space = testPool.allocate(200000);
    EXPECT_NE(space, NULL);
}

TEST_F(PoolTest, OversizeTest) {
    Pool testPool;
    void* space;
    // Size the allocations to force oversized chunks.
    space = testPool.allocate(1000000);
    EXPECT_NE(space, NULL);
    space = testPool.allocate(1000000);
    EXPECT_NE(space, NULL);
    space = testPool.allocate(1000000);
    EXPECT_NE(space, NULL);
}

// Tests voltdb::allocator with multiple threads.
#include <algorithm>
#include <cassert>
#include <deque>
#include <list>
#include <random>
#include <thread>

struct S1 {
   static std::mt19937 s_gen;
   static std::uniform_int_distribution<> s_dist;     // hole allowed in string
   static void genRandString(char* dst, size_t len) {
      for(size_t i = 0; i < len; ++i) {
         dst[i] = S1::s_dist(S1::s_gen);
      }
      dst[len - 1] = 0;     // last byte set to sentinel
   }
   static std::string genRandString(size_t len) {
      std::string s(len, 0);
      genRandString(&s[0], len);
      return s;
   }
public:
   const size_t m_stringLen;
   char m_char;
   std::string m_str;
   S1(const std::string& s): m_stringLen(s.size()), m_char(s[0]), m_str(s) {}
   S1() : S1(genRandString(255)) {}
   virtual ~S1() { }
};
std::random_device rd;
std::mt19937 S1::s_gen(rd());
std::uniform_int_distribution<> S1::s_dist(0, 255);     // hole allowed in string

struct S2 {
   S1 m_s1;
   std::string m_str;
   double m_doubleVal = -1.243;
   char m_stringOnStack[512];
   S2() : m_s1(), m_str(S1::genRandString(S1::s_dist(S1::s_gen))) { }
   virtual ~S2() {}
};

struct S3 : public S2 {
   char m_char;
   S1 m_S1Array[32];
   S3() : S2(), m_char(S1::s_dist(S1::s_gen)) {
   }
};

struct S4 {
   std::vector<S1> m_s1;
   std::deque<S2> m_s2;
   std::list<S3> m_s3;
   std::string m_str;
   S4() {
      for(int i = 0; i < 6; ++i) {
         m_s1.emplace_back();
         m_s2.emplace_back();
         m_s3.emplace_back();
      }
   }
};

struct S5 {
   char m_holder[16385];                                // oversized struct that does not fit in normal chunk
   std::string m_str;
};

template<typename T, typename Alloc, template<typename, typename> class Cont>
void updateContainer(Cont<T, Alloc>& cont) {
   // flip a coin to determine using emplace_back() with 80% of likelihood, or pop_back() with 20% of likelihood.
   static std::bernoulli_distribution dist(.8);
   if (cont.empty() || dist(S1::s_gen)) {
      cont.emplace_back();
   } else {
      cont.pop_back();
   }
   // and with 20% likelihood mutating 1st and last elem's std::string field
   if (!cont.empty() && !dist(S1::s_gen)) {
      cont.front().m_str.append("- foo");
      cont.back().m_str.append("- bar");
   }
}

template<typename T> using Alloc = voltdb::allocator<T>;
/**
 * Test logic for using multiple containers from an allocator at
 * the same time. Note that each container shall have at most 1
 * thread on it, as operations on STL container is not
 * thread-safe.
 */
TEST_F(PoolTest, AllocatorTest) {
   using Cont1 = std::vector<S1, Alloc<S1>>;
#if defined __GNUC__ && __GNUC__ > 5
   using Cont2 = std::deque<S2, Alloc<S2>>;
   using Cont3 = std::list<S3, Alloc<S3>>;
   using Cont4 = std::list<S4, Alloc<S4>>;
#else
   using Cont2 = std::vector<S4, Alloc<S4>>;
   using Cont3 = std::vector<S4, Alloc<S4>>;
   using Cont4 = std::vector<S4, Alloc<S4>>;
#endif
   using Cont5 = std::vector<S5, Alloc<S5>>;
   Cont1 cont1;
   Cont2 cont2;
   Cont3 cont3;
   Cont4 cont4;
   Cont5 cont5;
   // Creating thread pool to freely contend with each other for
   // allocator resources
   std::vector<std::thread> threads;
   for(int i = 0; i < 5; ++i) {                         // shall have only 3 threads, each working on one container
      threads.emplace_back([&cont1, &cont2, &cont3, &cont4, &cont5, i]() {
            for(size_t iter = 0; iter < 8000lu; ++iter) {
               switch(i) {
                  case 0:
                     updateContainer(cont1);
                     break;
                  case 1:
                     updateContainer(cont2);
                     break;
                  case 2:
                     updateContainer(cont3);
                     break;
                  case 3:
                     updateContainer(cont4);
                     break;
                  case 4:
                     updateContainer(cont5);
                     break;
                  default:
                     assert(!"Each container can only have 1 thread associated");
                  }
               }
            });
   }
   std::for_each(threads.begin(), threads.end(), [](std::thread& thrd) { thrd.join(); });
}

int main() {
    return TestSuite::globalInstance()->runAll();
}
