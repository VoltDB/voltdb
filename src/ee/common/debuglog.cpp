/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
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
#include "debuglog.h"
#include <execinfo.h>
#include <cstring>
#include <cxxabi.h>   // for abi
#include <cstdlib> // for malloc/free
#include <sstream> // for std::ostringstream

namespace voltdb {
StackTrace::StackTrace() {
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
            }
            else if (*j == '+') {
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

StackTrace::~StackTrace() {
    ::free(m_traceSymbols);
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
