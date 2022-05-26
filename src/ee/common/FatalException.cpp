/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
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

#ifdef MACOSX // mac os requires _XOPEN_SOURCE for ucontext for some reason
#define _XOPEN_SOURCE
#endif

namespace voltdb {
FatalException::FatalException(std::string const& message,
        const char *filename, unsigned long lineno, std::string const& backtrace_path) :
    m_reason(message), m_filename(filename), m_lineno(lineno),
    m_backtracepath(backtrace_path) {
    FILE *bt = fopen(m_backtracepath.c_str(), "a+");
    if (bt) {
        StackTrace::printMangledAndUnmangledToFile(bt);
        fclose(bt);
    }
}

void FatalException::reportAnnotations(const std::string& str) {
    FILE *bt = fopen(m_backtracepath.c_str(), "a+");
    if (!bt) {
        return;
    }
    // append to backtrace file
    fprintf(bt, "Additional annotations to the above Fatal Exception:\n%s", str.c_str());
    fclose(bt);
}

FatalLogicError::FatalLogicError(const std::string buffer, const char *filename, unsigned long lineno)
  : FatalLogicErrorBaseInitializer("FatalLogicError"), m_fatality(buffer, filename, lineno) {
    initWhat();
}

FatalLogicError::~FatalLogicError() noexcept {} // signature required by exception base class?

void FatalLogicError::initWhat() {
    std::ostringstream buffer;
    buffer << m_fatality;
    m_whatwhat = buffer.str();
}

void FatalLogicError::appendAnnotation(const std::string& buffer) {
    m_whatwhat += buffer;
    m_fatality.reportAnnotations(buffer);
}

const char* FatalLogicError::what() const throw() {
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
