/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
 *
 * This file contains original code and/or modifications of original code.
 * Any modifications made by Volt Active Data Inc. are licensed under the following
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

#include <iostream>

#include "harness.h"


#include "common/ThreadLocalPool.h"
#include "common/FatalException.hpp"

using namespace voltdb;

#ifdef VOLT_POOL_CHECKING
static const bool POOL_CHECKING_ENABLED = true;
#else
static const bool POOL_CHECKING_ENABLED = false;
#endif

/**
 * This test ensures that, when build with VOLT_POOL_CHECKING set,
 * extra checks are performed on our memory pools to help
 * ensure their correct operation.
 */
class PoolCheckingTest : public Test {

};

TEST_F(PoolCheckingTest, ExactSize) {
    if (! POOL_CHECKING_ENABLED) {
        std::cout << "  [test not run because pool checking not enabled]  ";
        std::cout.flush();
        return;
    }

    ThreadLocalPool tlPool;
    void *addr = ThreadLocalPool::allocateExactSizedObject(10);
    ASSERT_NE(NULL, addr);

    try {
        // Wrong size
        ThreadLocalPool::freeExactSizedObject(50, addr);
        FAIL("Expected exception");
    }
    catch (const FatalException& exc) {
        ASSERT_NE(std::string::npos, exc.m_reason.find("Attempt to deallocate exact-sized object of unknown size"));
    }

    // Object is now freed
    ThreadLocalPool::freeExactSizedObject(10, addr);

    try {
        // Attempt to de-allocate it again
        ThreadLocalPool::freeExactSizedObject(10, addr);
        FAIL("Expected exception");
    }
    catch (const FatalException& exc) {
        ASSERT_NE(std::string::npos, exc.m_reason.find("Attempt to deallocate unknown exact-sized object"));
    }

    std::cout << " *** *** Above errors are expected and are okay as long as test is passing *** *** --> ";
    std::cout.flush();
}

namespace {
    ThreadLocalPool::Sized *asSizedObject(void *stringPtr) {
        return reinterpret_cast<ThreadLocalPool::Sized *>(stringPtr);
    }
}

TEST_F(PoolCheckingTest, Relocatable) {
    if (! POOL_CHECKING_ENABLED) {
        std::cout << "  [test not run because pool checking not enabled]  ";
        std::cout.flush();
        return;
    }

    ThreadLocalPool tlPool;

    char *referrer = NULL;
    void *addr = ThreadLocalPool::allocateRelocatable(&referrer, 128);
    ASSERT_NE(NULL, addr);


    ThreadLocalPool::freeRelocatable(asSizedObject(addr));

    try {
        ThreadLocalPool::freeRelocatable(asSizedObject(addr));
        FAIL("Expected exception");
    }
    catch (const FatalException &exc) {
        ASSERT_NE(std::string::npos, exc.m_reason.find("Deallocation of unknown pointer to relocatable object"));
    }

    std::cout << " *** *** Above errors are expected and are okay as long as test is passing *** *** --> ";
    std::cout.flush();
}

int main() {
    return TestSuite::globalInstance()->runAll();
}
