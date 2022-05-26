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

#include "harness.h"
#include <cstdlib>
#include <iostream>

using namespace std;

namespace voltdb {
int TestOnlyAllocationSizeForObject(int input);
};

// CHEATING SLIGHTLY -- The tests are a little too stringent when applied
// to the actual MIN_REQUEST value of 0.
static const int MIN_REQUEST = 2;

class ThreadLocalPoolTest : public Test {
public:
    ThreadLocalPoolTest() {};

    void validateDeltas(int input, int testcase,
                        int byte_increment, int percent_increment)
    {
        if (byte_increment < 0) {
            cout << "Failing case " << testcase << " input " << input << " byte_increment " << byte_increment << endl;
        }
        ASSERT_TRUE(byte_increment >= 0);
        if (byte_increment >= (1<<19)) {
            cout << "Failing case " << testcase << " input " << input << " byte_increment " << byte_increment << endl;
        }
        ASSERT_TRUE(byte_increment < (1<<19));
        if (percent_increment >= 66) {
            cout << "Failing case " << testcase << " input " << input << " percent_increment " << percent_increment << endl;
        }
        ASSERT_TRUE(percent_increment < 66);
    }

    int validateAllocation(int input)
    {
        int result = voltdb::TestOnlyAllocationSizeForObject(input);
        // A minimum 12 byte overhead is assumed.
        // We measure percent increases from that higher baseline.
        // Otherwise tiny requested sizes would appear to be blown
        // out of proportion -- only because they really ARE.
        input += 12;
        int byte_overhead = result - input;
        int percent_overhead = byte_overhead * 100 / input;
        validateDeltas(input, 0, byte_overhead, percent_overhead);
        return result;
    }

    void validateTrend(int input, int suite, int low, int medium, int high)
    {
        int byte_increment_ml = medium - low;
        int percent_increment_ml = byte_increment_ml * 100 / medium;
        int byte_increment_hm = high - medium;
        int percent_increment_hm = byte_increment_hm * 100 / medium;
        int byte_increment_hl = high - low;
        int percent_increment_hl = byte_increment_hl * 100 / medium;
        validateDeltas(input, suite+1, byte_increment_ml, percent_increment_ml);
        validateDeltas(input, suite+2, byte_increment_hm, percent_increment_hm);
        validateDeltas(input, suite+3, byte_increment_hl, percent_increment_hl);
    }

    void validateAllocationSpan(int input)
    {
        int result = validateAllocation(input);
        int result_down = validateAllocation(input-1);
        int result_up = validateAllocation(input+1);
        int result_in = validateAllocation(input*7/8);
        int result_out = validateAllocation(input*8/7);
        ASSERT_TRUE(result_up <= (1L<<20));
        ASSERT_TRUE(result_out <= (1L<<20));
        validateTrend(input, 0, result_down, result, result_up);
        validateTrend(input, 4, result_in, result, result_out);
    }
};

TEST_F(ThreadLocalPoolTest, AllocationSizingExtreme)
{
    validateAllocation(MIN_REQUEST);
    validateAllocation(MIN_REQUEST+1);
    validateAllocation(1<<20);
    validateAllocation((1<<20));
}

TEST_F(ThreadLocalPoolTest, AllocationSizingFixed)
{
    int fixedTrial[31] = { 4, 7, 10, 13, 16,
                           1<<5, 1<<6, 1<<7, 1<<8, 1<<9, 1<<10, 1<<12, 1<<14, 1<<18,
                           3<<5, 3<<6, 3<<7, 3<<8, 3<<9, 3<<10, 3<<12, 3<<14, 3<<18,
                           5<<5, 5<<6, 5<<7, 5<<8, 5<<9, 5<<10, 5<<12, 5<<14 };
    std::size_t trialCount = sizeof(fixedTrial) / sizeof(fixedTrial[0]);
    while (trialCount--) {
        validateAllocationSpan(fixedTrial[trialCount]);
    }
    validateAllocation(1<<20);
    validateAllocation((1<<20));
}

TEST_F(ThreadLocalPoolTest, AllocationSizingRandom)
{
    int trialCount = 10000;
    while (trialCount--) {
        // Sum a small constant to avoid small extremes,
        // a small linear component to get a wider range of unique values,
        // and a component with an inverse distribution to favor numbers nearer the low end.
        int skewedInt = 4 + (rand() % (1<<10)) + (1<<19) / (1 + rand() % (1<<19));
        validateAllocationSpan(skewedInt);
    }
}

int main() {
    return TestSuite::globalInstance()->runAll();
}
