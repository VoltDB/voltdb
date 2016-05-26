/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
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



#ifndef TESTS_EE_TEST_UTILS_SIMPLETIMER_HPP
#define TESTS_EE_TEST_UTILS_SIMPLETIMER_HPP

#include <chrono>

struct SimpleTimer {

    typedef std::chrono::microseconds microseconds;
    typedef std::chrono::high_resolution_clock::time_point time_point;

    SimpleTimer()
        : m_start(std::chrono::high_resolution_clock::now())
    {
    }

    std::string elapsedAsString() {
        std::ostringstream oss;
        auto end = std::chrono::high_resolution_clock::now();
        auto elapsed = std::chrono::duration_cast<microseconds>(end - m_start);
        auto micros = elapsed.count();
        if (micros > 1000000) {
            oss <<  (micros / 1000000.0) << " s";
        }
        else if (micros > 1000) {
            oss <<  (micros / 1000.0) << " ms";
        }
        else {
            oss <<  micros << " us";
        }

        return oss.str();
    }

    void reset() {
        m_start = std::chrono::high_resolution_clock::now();
    }

private:
    time_point m_start;

};

#endif // TESTS_EE_TEST_UTILS_SIMPLETIMER_HPP
