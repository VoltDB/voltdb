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

#include "harness.h"

#include <iostream>
#include <cstring>
#include <boost/scoped_ptr.hpp>

#include "common/ThreadLocalPool.h"

#include "structures/CompactingPool.h"

using namespace voltdb;

class CompactingPoolTest : public Test
{
    // When run in pool-checking mode,
    // we need an instance of ThreadLocalPool
    // to initialize thread partition ID.
    ThreadLocalPool m_tlPool;
};

TEST_F(CompactingPoolTest, basic_ops)
{
    const int32_t ELEMENT_SIZE = 17;
    const int32_t ELEMENTS_PER_BUFFER = 7;
    CompactingPool dut(ELEMENT_SIZE, ELEMENTS_PER_BUFFER);

    // test freeing with just one element is happy
    char* elem = reinterpret_cast<char*>(dut.malloc(&elem));
    EXPECT_EQ((ELEMENT_SIZE + CompactingPool::FIXED_OVERHEAD_PER_ENTRY())* ELEMENTS_PER_BUFFER,
            dut.getBytesAllocated());
    dut.free(elem);
    EXPECT_EQ(0, dut.getBytesAllocated());

    // fill up a buffer + 1, then free something in the middle and
    // verify that we shrink appropriately
    char* elems[ELEMENTS_PER_BUFFER + 2];
    for (int i = 0; i <= ELEMENTS_PER_BUFFER; i++) {
        elems[i] = reinterpret_cast<char*>(dut.malloc(&(elems[i])));
        memset(elems[i], i, ELEMENT_SIZE);
    }
    EXPECT_EQ(2, *reinterpret_cast<int8_t*>(elems[2]));
    EXPECT_EQ((ELEMENT_SIZE + CompactingPool::FIXED_OVERHEAD_PER_ENTRY()) * ELEMENTS_PER_BUFFER * 2,
            dut.getBytesAllocated());
    dut.free(elems[2]);
    // 2 should now have the last element, filled with num_elements
    EXPECT_EQ(ELEMENTS_PER_BUFFER, *reinterpret_cast<int8_t*>(elems[2]));
    // and we should have shrunk back to 1 buffer
    EXPECT_EQ((ELEMENT_SIZE + CompactingPool::FIXED_OVERHEAD_PER_ENTRY()) * ELEMENTS_PER_BUFFER,
            dut.getBytesAllocated());

    // add an element and free it and verify that we don't mutate anything else
    elems[ELEMENTS_PER_BUFFER + 1] = reinterpret_cast<char*>(dut.malloc(&(elems[ELEMENTS_PER_BUFFER + 1])));
    EXPECT_EQ((ELEMENT_SIZE + CompactingPool::FIXED_OVERHEAD_PER_ENTRY()) * ELEMENTS_PER_BUFFER * 2,
            dut.getBytesAllocated());
    dut.free(elems[ELEMENTS_PER_BUFFER + 1]);
    EXPECT_EQ((ELEMENT_SIZE + CompactingPool::FIXED_OVERHEAD_PER_ENTRY()) * ELEMENTS_PER_BUFFER,
            dut.getBytesAllocated());

    while (dut.getBytesAllocated() > 0) {
        dut.free(elems[0]);
    }
}

TEST_F(CompactingPoolTest, bytes_allocated_test)
{
    int32_t size = 1024 * 512; // half a meg object
    int32_t num_elements = ((2 * 1024 * 1024) / size) + 1;

    // need to top 2GB to overflow
    int64_t bigsize = 2L * (1024L * 1024L * 1024L) + (1024L * 1024L * 10L);
    int64_t elems_needed = bigsize / size + 1;
    char* elems[elems_needed];

    CompactingPool dut(size, num_elements);
    for (int i = 0; i < elems_needed; ++i) {
        elems[i] = reinterpret_cast<char*>(dut.malloc(&(elems[i])));
        // return value of getBytesAllocated() is unsigned.  However,
        // when it overflows internally, we get a HUGE value back.
        // Our sanity check is that the value is less than twice the
        // giant memory we're trying to fill
        EXPECT_TRUE(dut.getBytesAllocated() < (bigsize * 2L));
    }
    // Ghetto way to get INT_MAX
    // Make sure that we would have, in fact, overflowed an int32_t
    EXPECT_TRUE(dut.getBytesAllocated() > 0x7fffffff);

    for (int i = 0; i < elems_needed; ++i) {
        // bonus extra hack test.  If we keep freeing the first
        // element, it should get compacted into and we can free it
        // again!
        dut.free(elems[0]);
    }
}

int main() {
    return TestSuite::globalInstance()->runAll();
}
