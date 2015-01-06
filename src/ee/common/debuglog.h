/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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

#include <string>
#include <ctime>
#include <cstdio>

// Log levels.
#define VOLT_LEVEL_OFF    1000
#define VOLT_LEVEL_ERROR  500
#define VOLT_LEVEL_WARN   400
#define VOLT_LEVEL_INFO   300
#define VOLT_LEVEL_DEBUG  200
#define VOLT_LEVEL_TRACE  100
#define VOLT_LEVEL_ALL    0

#define VOLT_LOG_TIME_FORMAT "%Y-%m-%d %H:%M:%S"

// Compile Option
#ifndef VOLT_LOG_LEVEL
    // TODO : any way to use pragma message in GCC?
    //#pragma message("Warning: VOLT_LOG_LEVEL compile option was not explicitly given.")
    #if defined(DEBUG) || defined(_DEBUG) || defined(_DEBUG_)
        //#pragma message("VOLT_LEVEL_DEBUG is used instead as DEBUG option is on.")
        #define VOLT_LOG_LEVEL VOLT_LEVEL_DEBUG
    #else
        //#pragma message("VOLT_LEVEL_WARN is used instead as DEBUG option is off.")
        #define VOLT_LOG_LEVEL VOLT_LEVEL_WARN
    #endif
    //#pragma message("Give VOLT_LOG_LEVEL compile option to overwrite the default level.")
#endif


// For compilers which do not support __FUNCTION__
#if !defined(__FUNCTION__) && !defined(__GNUC__)
    #define __FUNCTION__ ""
#endif

void outputLogHeader_(const char *file, int line, const char *func, int level);

// Two convenient macros for debugging
// 1. Logging macros.
// 2. VOLT_XXX_ENABLED macros. Use these to "eliminate" all the debug blocks from release binary.
#ifdef VOLT_ERROR_ENABLED
    #undef VOLT_ERROR_ENABLED
#endif
#if VOLT_LOG_LEVEL<=VOLT_LEVEL_ERROR
    #define VOLT_ERROR_ENABLED
    //#pragma message("VOLT_ERROR was enabled.")
    #define VOLT_ERROR(...) outputLogHeader_(__FILE__, __LINE__, __FUNCTION__, VOLT_LEVEL_ERROR);::printf(__VA_ARGS__);printf("\n");::fflush(stdout)
#else
    #define VOLT_ERROR(...) ((void)0)
#endif

#ifdef VOLT_WARN_ENABLED
    #undef VOLT_WARN_ENABLED
#endif
#if VOLT_LOG_LEVEL<=VOLT_LEVEL_WARN
    #define VOLT_WARN_ENABLED
    //#pragma message("VOLT_WARN was enabled.")
    #define VOLT_WARN(...) outputLogHeader_(__FILE__, __LINE__, __FUNCTION__, VOLT_LEVEL_WARN);::printf(__VA_ARGS__);printf("\n");::fflush(stdout)
#else
    #define VOLT_WARN(...) ((void)0)
#endif

#ifdef VOLT_INFO_ENABLED
    #undef VOLT_INFO_ENABLED
#endif
#if VOLT_LOG_LEVEL<=VOLT_LEVEL_INFO
    #define VOLT_INFO_ENABLED
    //#pragma message("VOLT_INFO was enabled.")
    #define VOLT_INFO(...) outputLogHeader_(__FILE__, __LINE__, __FUNCTION__, VOLT_LEVEL_INFO);::printf(__VA_ARGS__);printf("\n");::fflush(stdout)
#else
    #define VOLT_INFO(...) ((void)0)
#endif

#ifdef VOLT_DEBUG_ENABLED
    #undef VOLT_DEBUG_ENABLED
#endif
#if VOLT_LOG_LEVEL<=VOLT_LEVEL_DEBUG
    #define VOLT_DEBUG_ENABLED
    //#pragma message("VOLT_DEBUG was enabled.")
    #define VOLT_DEBUG(...) outputLogHeader_(__FILE__, __LINE__, __FUNCTION__, VOLT_LEVEL_DEBUG);::printf(__VA_ARGS__);printf("\n");::fflush(stdout)
#else
    #define VOLT_DEBUG(...) ((void)0)
#endif

#ifdef VOLT_TRACE_ENABLED
    #undef VOLT_TRACE_ENABLED
#endif
#if VOLT_LOG_LEVEL<=VOLT_LEVEL_TRACE
    #define VOLT_TRACE_ENABLED
    //#pragma message("VOLT_TRACE was enabled.")
    #define VOLT_TRACE(...) outputLogHeader_(__FILE__, __LINE__, __FUNCTION__, VOLT_LEVEL_TRACE);::printf(__VA_ARGS__);printf("\n");::fflush(stdout)
#else
    #define VOLT_TRACE(...) ((void)0)
#endif

// Output log message header in this format: [type] [file:line:function] time -
// ex: [ERROR] [somefile.cpp:123:doSome()] 2008/07/06 10:00:00 -
inline void outputLogHeader_(const char *file, int line, const char *func, int level) {
    time_t t = ::time(NULL) ;
    tm *curTime = localtime(&t);
    char time_str[32]; // FIXME
    ::strftime(time_str, 32, VOLT_LOG_TIME_FORMAT, curTime);
    const char* type;
    switch (level) {
        case VOLT_LEVEL_ERROR:
            type = "ERROR";
            break;
        case VOLT_LEVEL_WARN:
            type = "WARN ";
            break;
        case VOLT_LEVEL_INFO:
            type = "INFO ";
            break;
        case VOLT_LEVEL_DEBUG:
            type = "DEBUG";
            break;
        case VOLT_LEVEL_TRACE:
            type = "TRACE";
            break;
        default:
            type = "UNKWN";
    }
    printf("[%s] [%s:%d:%s()] %s - ", type, file, line, func, time_str);
}

#endif // HSTOREDEBUGLOG_H
