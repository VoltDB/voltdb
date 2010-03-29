/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB L.L.C.
 *
 * VoltDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * VoltDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

#ifndef FATALEXCEPTION_HPP_
#define FATALEXCEPTION_HPP_

#include <string>
#include <string.h>
#include <execinfo.h>
#include <cxxabi.h>
#include <vector>
#include "boost/scoped_array.hpp"

#define throwFatalException(...) { char reallysuperbig_nonce_message[8192]; sprintf(reallysuperbig_nonce_message, __VA_ARGS__); throw voltdb::FatalException( reallysuperbig_nonce_message, __FILE__, __LINE__); }
namespace voltdb {
class FatalException {
public:
    /**
     * Stack trace code from http://tombarta.wordpress.com/2008/08/01/c-stack-traces-with-gcc/
     * 
     */
    FatalException(std::string message, const char *filename, unsigned long lineno) :
        m_reason(message), m_filename(filename), m_lineno(lineno) {
        void *traces[128];
        const int numTraces = backtrace( traces, 128);
        boost::scoped_array<char*> traceSymbols(backtrace_symbols( traces, 128));
        for (int ii = 0; ii < numTraces; ii++) {
            std::size_t sz = 200;
            char *function = static_cast<char*>(malloc(sz));
            char *begin = NULL, *end = NULL;
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
            free(function);
        }
    }
    const std::string m_reason;
    const char *m_filename;
    const unsigned long m_lineno;
    std::vector<std::string> m_traces;
};
}
#endif /* FATALEXCEPTION_HPP_ */
