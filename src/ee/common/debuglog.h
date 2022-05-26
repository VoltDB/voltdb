/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
 *
 * This file contains original code and/or modifications of original code.
 * Any modifications made by Volt Active Data Inc. are licensed under the following
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

#pragma once

/**
 * Debug logging functions for EE. Unlike the performance counters,
 * these are just printf() turned on/off by VOLT_LOG_LEVEL compile option.
 * The main concern here is not to add any overhead on runtime performance
 * when the logging is turned off. Use VOLT_XXX_ENABLED macros defined here to
 * eliminate all instructions in the final binary.
*/

#include "common/ThreadLocalPool.h"
#include "common/StackTrace.h"

#include <string>
#include <ctime>
#include <cstdio>
#include <chrono>
#include <sys/time.h>

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

#define _VOLT_LOG(lvl, msg, ...) do {                                           \
        struct timeval __now__;                                                 \
        ::gettimeofday(&__now__, NULL);                                         \
        tm *__curTime__ = localtime(&__now__.tv_sec);                           \
        char __time_str__[32];                                                  \
        ::strftime(__time_str__, 32, VOLT_LOG_TIME_FORMAT, __curTime__);        \
        ::printf("[%s] [T%d:E%d] [%s:%d:%s()] %s,%03jd - " msg, lvl,            \
                voltdb::ThreadLocalPool::getThreadPartitionIdWithNullCheck(),   \
                voltdb::ThreadLocalPool::getEnginePartitionIdWithNullCheck(),   \
                __FILE__, __LINE__, __FUNCTION__,                               \
                __time_str__, (intmax_t) __now__.tv_usec / 1000, ##__VA_ARGS__);\
        ::fflush(stdout);                                                       \
    } while (0)

#define VOLT_LOG(lvl, msg, ...) _VOLT_LOG(lvl, msg  "\n", ##__VA_ARGS__)

#define VOLT_LOG_STACK(lvl) _VOLT_LOG(lvl, "STACK TRACE\n%s", voltdb::StackTrace::stringStackTrace("    ").c_str())

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
    #define VOLT_TRACE(...) VOLT_LOG("TRACE", __VA_ARGS__)
    #define VOLT_TRACE_STACK() VOLT_LOG_STACK("TRACE")
#else
    #define VOLT_TRACE(...) ((void)0)
    #define VOLT_TRACE_STACK() ((void)0)
#endif

#if VOLT_TIMER_ENABLED
template<typename P>
struct _TimerLevels {
    std::chrono::duration<int64_t, P> error;
    std::chrono::duration<int64_t, P> warn;
    std::chrono::duration<int64_t, P> info;
    std::chrono::duration<int64_t, P> debug;
};

#define __TIMER_LVLS_NAME(name) __ ## name ## TimerLevels

// Define log levels based on timer duration. n: name of lvls struct, r: ratio for duration (milli, micro, ...),
// e: error duration, w: warning duration, i: info duration, d: debug duration
#define TIMER_LVLS(n, r, e, w, i, d) const static struct _TimerLevels<std::r> __TIMER_LVLS_NAME(n) = {    \
        .error = std::chrono::r##seconds(e), .warn = std::chrono::r##seconds(w),                          \
        .info = std::chrono::r##seconds(i), .debug = std::chrono::r##seconds(d) };

#define __TIMER_NAME(name) __ ## name ## StartTime

// Start a timer with given name
#define START_TIMER(name) auto __TIMER_NAME(name) = std::chrono::steady_clock::now()

#define __TIMER_LOG(lvl, strfmt, duration, ...) do { VOLT_ ## lvl("Took %ld ns: " \
        strfmt, duration.count(), ##__VA_ARGS__); } while (0)

// Stop a timer with name and log at appropriate log level as defined by lvls.
// strfmt: format string to log, and arguments for format string
#define STOP_TIMER(name, lvls, strfmt, ...)  do {                                                                      \
        auto __duration__ = std::chrono::steady_clock::now() - __TIMER_NAME(name);                                     \
        if (__duration__ > __TIMER_LVLS_NAME(lvls).error) __TIMER_LOG(ERROR, strfmt, __duration__, ##__VA_ARGS__);     \
        else if (__duration__ > __TIMER_LVLS_NAME(lvls).warn) __TIMER_LOG(WARN, strfmt, __duration__, ##__VA_ARGS__);  \
        else if (__duration__ > __TIMER_LVLS_NAME(lvls).info) __TIMER_LOG(INFO, strfmt, __duration__, ##__VA_ARGS__);  \
        else if (__duration__ > __TIMER_LVLS_NAME(lvls).debug) __TIMER_LOG(DEBUG, strfmt, __duration__, ##__VA_ARGS__);\
    } while (0)
#else
#define TIMER_LVLS(...)
#define START_TIMER(...) ((void)0)
#define STOP_TIMER(...) ((void)0)
#endif

#define PRINT_STACK_TRACE() VOLT_LOG_STACK("UNKWN")

// A custom assert macro that adds stacktrace on message
#ifdef NDEBUG
#define vassert(expr) (void)0
#else
#ifdef MACOSX
// MACOS does not have an equivalent of __assert_fail; so we do
// what the header there does, see
// https://gist.github.com/vrx/17c31cbfe0511645a41a#file-assert-h-L71
#define vassert(expr)             \
   if(! (expr)) {                 \
       printf("%s%u: failed assertion\n(STACK TRACE:\n%s)\n", __FILE__, __LINE__,              \
               voltdb::StackTrace::stringStackTrace("\t").c_str());                            \
       abort();                                                                                \
   }
#else
extern char __assert_failure_msg__[4096];
#define vassert(expr)             \
   if(! (expr)) {                 \
       snprintf(__assert_failure_msg__, sizeof __assert_failure_msg__,                         \
               "%s\n(STACK TRACE:\n%s)\n", #expr,                                              \
               voltdb::StackTrace::stringStackTrace("\t").c_str());                            \
       __assert_failure_msg__[sizeof __assert_failure_msg__ - 1] = '\0';                       \
       __assert_fail(__assert_failure_msg__, __FILE__, __LINE__, __ASSERT_FUNCTION);           \
   }
#endif

#endif

