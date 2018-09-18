/* This file is part of VoltDB.
 * Copyright (C) 2008-2018 VoltDB Inc.
 *
 * This file contains original code and/or modifications of original code.
 * Any modifications made by VoltDB Inc. are licensed under the following
 * terms and conditions:
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
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

#ifndef HSTOREDEBUGLOG_H
#define HSTOREDEBUGLOG_H

/**
 * Debug logging functions for EE. Unlike the performance counters,
 * these are just printf() turned on/off by VOLT_LOG_LEVEL compile option.
 * The main concern here is not to add any overhead on runtime performance
 * when the logging is turned off. Use VOLT_XXX_ENABLED macros defined here to
 * eliminate all instructions in the final binary.
*/

#include "common/ThreadLocalPool.h"

#include <string>
#include <ctime>
#include <cstdio>
#include <vector>
#include <sys/time.h>

namespace voltdb {

// Log levels.
#define VOLT_LEVEL_OFF    1000
#define VOLT_LEVEL_ERROR  500
#define VOLT_LEVEL_WARN   400
#define VOLT_LEVEL_INFO   300
#define VOLT_LEVEL_DEBUG  200
#define VOLT_LEVEL_TRACE  100
#define VOLT_LEVEL_ALL    0

#define VOLT_LOG_TIME_FORMAT "%Y-%m-%d %T"

// Compile Option
#ifndef VOLT_LOG_LEVEL
    #ifndef NDEBUG
        #define VOLT_LOG_LEVEL VOLT_LEVEL_ERROR
    #else // release builds
        #define VOLT_LOG_LEVEL VOLT_LEVEL_OFF
    #endif
#endif


// For compilers which do not support __FUNCTION__
#if !defined(__FUNCTION__) && !defined(__GNUC__)
    #define __FUNCTION__ ""
#endif

#define VOLT_LOG_START(lvl, msg, ...) do {                                      \
        struct timeval __now__;                                                 \
        ::gettimeofday(&__now__, NULL);                                         \
        tm *__curTime__ = localtime(&__now__.tv_sec);                           \
        char __time_str__[32];                                                  \
        ::strftime(__time_str__, 32, VOLT_LOG_TIME_FORMAT, __curTime__);        \
        ::printf("[%s] [T%d:E%d] [%s:%d:%s()] %s,%03jd - " msg "\n", lvl,       \
                voltdb::ThreadLocalPool::getThreadPartitionIdWithNullCheck(),   \
                voltdb::ThreadLocalPool::getEnginePartitionIdWithNullCheck(),   \
                __FILE__, __LINE__, __FUNCTION__,                               \
                __time_str__, (intmax_t) __now__.tv_usec / 1000, ##__VA_ARGS__);\
    } while (0)

#define VOLT_LOG_END ::fflush(stdout)

#define VOLT_LOG(...) VOLT_LOG_START(__VA_ARGS__); VOLT_LOG_END

#define VOLT_LOG_STACK(lvl) VOLT_LOG_START(lvl, "STACK TRACE");             \
    voltdb::StackTrace::printStackTrace();                                  \
    VOLT_LOG_END

// Two convenient macros for debugging
// 1. Logging macros.
// 2. VOLT_XXX_ENABLED macros. Use these to "eliminate" all the debug blocks from release binary.
#ifdef VOLT_ERROR_ENABLED
    #undef VOLT_ERROR_ENABLED
#endif
#if VOLT_LOG_LEVEL<=VOLT_LEVEL_ERROR
    #define VOLT_ERROR_ENABLED
    //#pragma message("VOLT_ERROR was enabled.")
    #define VOLT_ERROR(...) VOLT_LOG("ERROR", __VA_ARGS__)
    #define VOLT_ERROR_STACK() VOLT_LOG_STACK("ERROR")
#else
    #define VOLT_ERROR(...) ((void)0)
    #define VOLT_ERROR_STACK() ((void)0)
#endif

#ifdef VOLT_WARN_ENABLED
    #undef VOLT_WARN_ENABLED
#endif
#if VOLT_LOG_LEVEL<=VOLT_LEVEL_WARN
    #define VOLT_WARN_ENABLED
    //#pragma message("VOLT_WARN was enabled.")
    #define VOLT_WARN(...) VOLT_LOG("WARN", __VA_ARGS__)
    #define VOLT_WARN_STACK() VOLT_LOG_STACK("WARN")
#else
    #define VOLT_WARN(...) ((void)0)
    #define VOLT_WARN_STACK() ((void)0)
#endif

#ifdef VOLT_INFO_ENABLED
    #undef VOLT_INFO_ENABLED
#endif
#if VOLT_LOG_LEVEL<=VOLT_LEVEL_INFO
    #define VOLT_INFO_ENABLED
    //#pragma message("VOLT_INFO was enabled.")
    #define VOLT_INFO(...) VOLT_LOG("INFO", __VA_ARGS__)
    #define VOLT_INFO_STACK() VOLT_LOG_STACK("INFO")
#else
    #define VOLT_INFO(...) ((void)0)
    #define VOLT_INFO_STACK() ((void)0)
#endif

#ifdef VOLT_DEBUG_ENABLED
    #undef VOLT_DEBUG_ENABLED
#endif
#if VOLT_LOG_LEVEL<=VOLT_LEVEL_DEBUG
    #define VOLT_DEBUG_ENABLED
    //#pragma message("VOLT_DEBUG was enabled.")
    #define VOLT_DEBUG(...) VOLT_LOG("DEBUG", __VA_ARGS__)
    #define VOLT_DEBUG_STACK() VOLT_LOG_STACK("DEBUG")
#else
    #define VOLT_DEBUG(...) ((void)0)
    #define VOLT_DEBUG_STACK() ((void)0)
#endif

#ifdef VOLT_TRACE_ENABLED
    #undef VOLT_TRACE_ENABLED
#endif
#if VOLT_LOG_LEVEL<=VOLT_LEVEL_TRACE
    #define VOLT_TRACE_ENABLED
    //#pragma message("VOLT_TRACE was enabled.")
    #define VOLT_TRACE(...) VOLT_LOG("TRACE", __VA_ARGS__); VOLT_LOG_END
    #define VOLT_TRACE_STACK() VOLT_LOG_STACK("TRACE")
#else
    #define VOLT_TRACE(...) ((void)0)
    #define VOLT_TRACE_STACK() ((void)0)
#endif

class StackTrace {
public:
    StackTrace();
    ~StackTrace();

    static void printMangledAndUnmangledToFile(FILE *targetFile);

    static void printStackTrace() {
        StackTrace st;
        for (int ii=1; ii < st.m_traces.size(); ii++) {
            printf("   %s\n", st.m_traces[ii].c_str());
        }
    }

    static std::string stringStackTrace();

    void printLocalTrace() {
        for (int ii=1; ii < m_traces.size(); ii++) {
            printf("   %s\n", m_traces[ii].c_str());
        }
    }

private:
    char** m_traceSymbols;
    std::vector<std::string> m_traces;
};

#define PRINT_STACK_TRACE() VOLT_LOG_STACK("UNKWN")

} // namespace voltdb

#endif // HSTOREDEBUGLOG_H
