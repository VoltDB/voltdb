/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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

#include "common/FatalException.hpp"

#include <cxxabi.h>   // for abi
#include <execinfo.h> // for backtrace, backtrace_symbols

#include <cstdlib> // for malloc/free
#include <cstring> // for strn*
#include <dlfcn.h>
#include <stdio.h> // for fopen, fprintf, fclose
#include <string.h>

#ifdef MACOSX // mac os requires _XOPEN_SOURCE for ucontext for some reason
#define _XOPEN_SOURCE
#endif
#include <ucontext.h>

namespace voltdb {
FatalException::FatalException(std::string message,
                               const char *filename, unsigned long lineno,
                               std::string backtrace_path)
  : m_reason(message)
  , m_filename(filename), m_lineno(lineno)
  , m_backtracepath(backtrace_path)
{
    FILE *bt = fopen(m_backtracepath.c_str(), "a+");

    /**
     * Stack trace code from http://tombarta.wordpress.com/2008/08/01/c-stack-traces-with-gcc/
     */
    void *traces[128];
    for (int i=0; i < 128; i++) traces[i] = NULL; // silence valgrind
    const int numTraces = backtrace( traces, 128);
    char** traceSymbols = backtrace_symbols( traces, numTraces);

    // write header for backtrace file
    if (bt) fprintf(bt, "VoltDB Backtrace (%d)\n", numTraces);

    for (int ii = 0; ii < numTraces; ii++) {
        std::size_t sz = 200;
        // Note: must use malloc vs. new so __cxa_demangle can use realloc.
        char *function = static_cast<char*>(::malloc(sz));
        char *begin = NULL, *end = NULL;

        // write original symbol to file.
        if (bt) fprintf(bt, "raw[%d]: %s\n", ii, traceSymbols[ii]);

        //Find parens surrounding mangled name
        for (char *j = traceSymbols[ii]; *j; ++j) {
            if (*j == '(') {
                begin = j;
            }
            else if (*j == '+') {
                end = j;
            }
        }

        if (begin && end) {
            *begin++ = '\0';
            *end = '\0';

            int status;
            char *ret = abi::__cxa_demangle(begin, function, &sz, &status);
            if (ret) {
                //return value may be a realloc of input
                function = ret;
            } else {
                // demangle failed, treat it like a C function with no args
                strncpy(function, begin, sz);
                strncat(function, "()", sz);
                function[sz-1] = '\0';
            }
            m_traces.push_back(std::string(function));
        } else {
            //didn't find the mangled name in the trace
            m_traces.push_back(std::string(traceSymbols[ii]));
        }
        ::free(function);
    }

    if (bt) {
        for (int ii=0; ii < m_traces.size(); ii++) {
            const char* str = m_traces[ii].c_str();
            fprintf(bt, "demangled[%d]: %s\n", ii, str);
        }
        fclose(bt);
    }
    ::free(traceSymbols);
}

void FatalException::reportAnnotations(const std::string& str)
{
    FILE *bt = fopen(m_backtracepath.c_str(), "a+");
    if (!bt) {
        return;
    }
    // append to backtrace file
    fprintf(bt, "Additional annotations to the above Fatal Exception:\n%s", str.c_str());
    fclose(bt);
}

FatalLogicError::FatalLogicError(const std::string buffer, const char *filename, unsigned long lineno)
  : FatalLogicErrorBaseInitializer("FatalLogicError")
  , m_fatality(buffer, filename, lineno)
{
    initWhat();
}

FatalLogicError::~FatalLogicError() throw () {} // signature required by exception base class?

void FatalLogicError::initWhat()
{
    std::ostringstream buffer;
    buffer << m_fatality;
    m_whatwhat = buffer.str();
}

void FatalLogicError::appendAnnotation(const std::string& buffer)
{
    m_whatwhat += buffer;
    m_fatality.reportAnnotations(buffer);
}

const char* FatalLogicError::what() const throw()
{
    return m_whatwhat.c_str();
}

// Reset either or both of these control variables from the debugger to dynamically
// control the error responses that depend on them.
int control_assert_or_throw_fatal_or_crash_123 =
    /* assert             */ 1;  //default
    // throw fatal                            *-/ 2;
    // crash on the spot                      */ 3;
int control_ignore_or_throw_fatal_or_crash_123 =
    /* fall through to throw something softer */ 1;  //default
    // throw fatal        *-/ 2;
    // crash on the spot  */ 3;


}
