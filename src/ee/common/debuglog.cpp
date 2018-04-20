/* This file is part of VoltDB.
 * Copyright (C) 2008-2018 VoltDB Inc.
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

// For backtrace and backtrace_symbols
// These do not appear to work on the Mac in a JNI library
#include <cxxabi.h>
#include <execinfo.h>
#include <cstring>
#include <cxxabi.h>   // for abi
#include <cstdlib>    // for malloc/free
#include <sstream>    // for std::ostringstream

#include "common/debuglog.h"
#include "common/executorcontext.hpp"
#include "common/Topend.h"

#include "execution/JNITopend.h"

namespace voltdb {

namespace {

bool backtraceIsSupported() {
#ifndef MACOSX
    // On Linux, it's always safe to do a backtrace.
    return true;
#else
    // On the Mac, calling backtrace_symbols can crash if done from
    // a Java process.  But it's okay when done from an IPC EE.
    ExecutorContext* ec = ExecutorContext::getExecutorContext();
    if (!ec) {
        // Either we're in a unit test, or before the DB has completely initialized.
        // No way to know if we're in a JNI environment or not...
        return false;
    }

    VoltDBEngine* engine = ExecutorContext::getEngine();
    if (engine == NULL) {
        return false;
    }

    Topend* topend = engine->getTopend();
    if (dynamic_cast<JNITopend*>(topend) != NULL) {
        return false;
    }
    else {
        // Must be an IPC or some other top end for testing.
        return true;
    }
#endif
}

}

// Output log message header in this format: [type] [threadPartition:enginePartition] [file:line:function] time -
// ex: [ERROR] [1:1] [somefile.cpp:123:doSome()] 2008/07/06 10:00:00 -
void outputLogHeader(const char *file, int line, const char *func, int level) {
    time_t t = ::time(NULL) ;
    tm *curTime = localtime(&t);
    char time_str[32]; // FIXME
    ::strftime(time_str, 32, VOLT_LOG_TIME_FORMAT, curTime);
    const int32_t tPartId = ThreadLocalPool::getThreadPartitionIdWithNullCheck();
    const int32_t ePartId = ThreadLocalPool::getEnginePartitionIdWithNullCheck();
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
    printf("[%s] [T%d:E%d] [%s:%d:%s()] %s - ", type, tPartId, ePartId, file, line, func, time_str);
}

StackTrace::StackTrace() {

    if (backtraceIsSupported()) {
        /**
         * Stack trace code from http://tombarta.wordpress.com/2008/08/01/c-stack-traces-with-gcc/
         */
        void *traces[128];
        char mangledName[256];
        for (int i=0; i < 128; i++) traces[i] = NULL; // silence valgrind
        const int numTraces = backtrace( traces, 128);
        m_traceSymbols = backtrace_symbols( traces, numTraces);

        for (int ii = 0; ii < numTraces; ii++) {
            std::size_t sz = 200;
            // Note: must use malloc vs. new so __cxa_demangle can use realloc.
            char *function = static_cast<char*>(::malloc(sz));
            char *begin = NULL, *end = NULL;

            //Find parens surrounding mangled name
            for (char *j = m_traceSymbols[ii]; *j; ++j) {
                if (*j == '(') {
                    begin = j;
                } else if (*j == '+') {
                    end = j;
                }
            }

            if (begin && end) {
                begin++;
                ::memcpy(mangledName, begin, end-begin);
                mangledName[end-begin] = '\0';
                int status;
                char *ret = abi::__cxa_demangle(mangledName, function, &sz, &status);
                if (ret) {
                    //return value may be a realloc of input
                    function = ret;
                } else {
                    // demangle failed, treat it like a C function with no args
                    strncpy(function, mangledName, sz);
                    strncat(function, "()", sz);
                    function[sz-1] = '\0';
                }
                m_traces.push_back(std::string(function));
            } else {
                //didn't find the mangled name in the trace
                m_traces.push_back(std::string(m_traceSymbols[ii]));
            }
            ::free(function);
        }
    }
    else {
        m_traces.push_back("Stack traces disabled from Mac OS X Java process");
        m_traceSymbols = NULL;
    }
}

StackTrace::~StackTrace() {
    ::free(m_traceSymbols);
}

void StackTrace::printMangledAndUnmangledToFile(FILE *targetFile) {
    StackTrace st;
    // write header for backtrace file
    int numFrames = (int) st.m_traces.size();
    // Ignore the stack frames specific to StackTrace object
    fprintf(targetFile, "VoltDB Backtrace (%d stack frames)\n", numFrames - 2);
    for (int ii = 2; ii < numFrames; ii++) {
        // write original symbol to file.
        fprintf(targetFile, "raw[%d]: %s\n", ii, st.m_traceSymbols[ii]);
    }
    for (int ii = 2; ii < numFrames; ii++) {
        const char *str = st.m_traces[ii].c_str();
        fprintf(targetFile, "demangled[%d]: %s\n", ii, str);
    }
}

std::string StackTrace::stringStackTrace()
{
    StackTrace st;
    std::ostringstream stacked;
    for (int ii=2; ii < st.m_traces.size(); ii++) {
        stacked << st.m_traces[ii] << "\n";
    }
    return stacked.str();
}

} // namespace voltdb
