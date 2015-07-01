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

#include "structures/CompactingPool.h"

#include "harness.h"
#include <iostream>
#include <cstring>
#include <boost/scoped_ptr.hpp>

using namespace voltdb;
using namespace std;

class CompactingPoolTest : public Test
{
public:
    CompactingPoolTest()
    {
    }

    ~CompactingPoolTest()
    {
    }
};

TEST_F(CompactingPoolTest, basic_ops)
{
    int32_t size = 17;
    int32_t num_elements = 7;
    boost::scoped_ptr<CompactingPool> dut(new CompactingPool(size, num_elements));

    // test freeing with just one element is happy
    void* elem = dut->malloc();
    EXPECT_EQ(size * num_elements, dut->getBytesAllocated());
    bool mutated = dut->free(elem);
    EXPECT_FALSE(mutated);
    EXPECT_EQ(0, dut->getBytesAllocated());

    // fill up a buffer + 1, then free something in the middle and
    // verify that we shrink appropriately
    void* elems[num_elements + 1];
    for (int i = 0; i <= num_elements; i++)
    {
        elems[i] = dut->malloc();
        memset(elems[i], i, size);
    }
    EXPECT_EQ(2, *reinterpret_cast<int8_t*>(elems[2]));
    EXPECT_EQ(size * num_elements * 2, dut->getBytesAllocated());
    mutated = dut->free(elems[2]);
    EXPECT_TRUE(mutated);
    // 2 should now have the last element, filled with num_elements
    EXPECT_EQ(num_elements, *reinterpret_cast<int8_t*>(elems[2]));
    // and we should have shrunk back to 1 buffer
    EXPECT_EQ(size * num_elements, dut->getBytesAllocated());

    // add an element and free it and verify that we don't mutate anything else
    elems[num_elements + 1] = dut->malloc();
    EXPECT_EQ(size * num_elements * 2, dut->getBytesAllocated());
    mutated = dut->free(elems[num_elements + 1]);
    EXPECT_FALSE(mutated);
    EXPECT_EQ(size * num_elements, dut->getBytesAllocated());
}

TEST_F(CompactingPoolTest, bytes_allocated_test)
{
    int32_t size = 1024 * 512; // half a meg object
    int32_t num_elements = ((2 * 1024 * 1024) / size) + 1;

    // need to top 2GB to overflow
    int64_t bigsize = 2L * (1024L * 1024L * 1024L) + (1024L * 1024L * 10L);
    int64_t elems_needed = bigsize / size + 1;
    void* elems[elems_needed];

    CompactingPool dut(size, num_elements);
    for (int i = 0; i < elems_needed; ++i)
    {
        elems[i] = dut.malloc();
        // return value of getBytesAllocated() is unsigned.  However,
        // when it overflows internally, we get a HUGE value back.
        // Our sanity check is that the value is less than twice the
        // giant memory we're trying to fill
        EXPECT_TRUE(dut.getBytesAllocated() < (bigsize * 2L));
    }
    // Ghetto way to get INT_MAX
    // Make sure that we would have, in fact, overflowed an int32_t
    EXPECT_TRUE(dut.getBytesAllocated() > 0x7fffffff);

    for (int i = 0; i < elems_needed; ++i)
    {
        // bonus extra hack test.  If we keep freeing the first
        // element, it should get compacted into and we can free it
        // again!
        dut.free(elems[0]);
    }
}

int main() {
    return TestSuite::globalInstance()->runAll();
}
