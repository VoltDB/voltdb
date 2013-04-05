/* This file is part of VoltDB.
 * Copyright (C) 2008-2013 VoltDB Inc.
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

#ifndef FATALEXCEPTION_HPP_
#define FATALEXCEPTION_HPP_

#include <cstdio>
#include <ostream>
#include <string>
#include <sstream>
#include <stdexcept>
#include <vector>

#include "common/debuglog.h"

#define throwFatalException(...) { char reallysuperbig_nonce_message[8192]; snprintf(reallysuperbig_nonce_message, 8192, __VA_ARGS__); throw voltdb::FatalException( reallysuperbig_nonce_message, __FILE__, __LINE__); }
#define HACK_HARDCODED_BACKTRACE_PATH "/tmp/voltdb_backtrace.txt"
namespace voltdb {
class FatalException {
public:
    /**
     * Stack trace code from http://tombarta.wordpress.com/2008/08/01/c-stack-traces-with-gcc/
     *
     */
    FatalException(std::string message, const char *filename, unsigned long lineno,
                   std::string backtrace_path = HACK_HARDCODED_BACKTRACE_PATH);

    void reportAnnotations(const std::string& str);

    const std::string m_reason;
    const char *m_filename;
    const unsigned long m_lineno;
    const std::string m_backtracepath;
    std::vector<std::string> m_traces;
};


inline std::ostream& operator<<(std::ostream& out, const FatalException& fe)
{
    out << fe.m_reason << fe.m_filename << ':' << fe.m_lineno << std::endl;
    for (int ii=0; ii < fe.m_traces.size(); ii++) {
        out << fe.m_traces[ii] << std::endl;
    }
    return out;
}

//TODO: The long-term intent is that there be a ubiquitous exception class that can be thrown from anywhere we
// detect evidence of a significant bug -- worth reporting and crashing the executable, exporting any remotely
// relevent detail, including stack trace AND whatever context information can be piled on at the point
// of the original throw AND any context that can be added via catch/re-throws as the stack is unwound.
// There should be a major distinction between this case and other cases:
// This type should NOT be used for resource issues that can arise unavoidably in production, no matter how
// curious we might be about details when that happens -- that should have its own somewhat separate handling.
// This type should NOT be used to react to user actions that conflict with known system limitations
// -- those should be raised as non-fatal "user error".
// Right now, FatalException seems to be very close to what's needed except that:
//   It is being sometimes used to flag resource issues (in Pool.hpp, maybe elsewhere?).
//   It lacks a friendly API for adding annotations after the initial construction/throw
//   (in a way that allows including the information in reports and feedback).
// This is a little hacky but it's a way of showing the intent at the points of throw/catch/re-throw.

// Purposely avoiding inheritance from FatalException for now, because the handling seems just a little dodgy.
// Instead, FatalException functionality is accessed via a data member that never actually gets thrown/caught.
// In contrast, exception is working out well as a base class, in the normal case when (re)throw goes uncaught.

// Macro-ized base class to aid experimentation
#define FatalLogicErrorBase std::runtime_error
// This is how FatalLogicError ctors initialize their base class.
#define FatalLogicErrorBaseInitializer(NAME) FatalLogicErrorBase(NAME)

class FatalLogicError : public FatalLogicErrorBase {
public:
// ctor wrapper macro supports caller's __FILE__ and __LINE__ and any number of printf-like __VARARGS__ arguments
#define throwFatalLogicErrorFormatted(...) { \
    char reallysuperbig_nonce_message[8192]; \
    snprintf(reallysuperbig_nonce_message, 8192, __VA_ARGS__); \
    throw voltdb::FatalLogicError(reallysuperbig_nonce_message, __FILE__, __LINE__); }

    FatalLogicError(const char* buffer, const char *filename, unsigned long lineno);

// ctor wrapper macro supports caller's __FILE__ and __LINE__ and any number of STREAMABLES separated by '<<'
#define throwFatalLogicErrorStreamed(STREAMABLES) { \
    std::ostringstream tFLESbuffer; tFLESbuffer << STREAMABLES << std::endl; \
    throw voltdb::FatalLogicError(tFLESbuffer.str(), __FILE__, __LINE__); }

    FatalLogicError(const std::string buffer, const char *filename, unsigned long lineno);

    ~FatalLogicError() throw (); // signature required by exception base class?

// member function wrapper macro supports any number of STREAMABLES separated by '<<'
#define appendAnnotationToFatalLogicError(ERROR_AS_CAUGHT, STREAMABLES) { \
    std::ostringstream aATFLEbuffer; \
    aATFLEbuffer << "rethrown from " << __FILE__ << ':' << __LINE__ << ':' << STREAMABLES << std::endl; \
    ERROR_AS_CAUGHT.appendAnnotation(aATFLEbuffer.str()); }

    void appendAnnotation(const std::string& buffer);

    virtual const char* what() const throw();

private:
    void initWhat();

    // FatalLogicError(const voltdb::FatalLogicError&); // Purposely undefined.

    FatalException m_fatality;
    std::string m_whatwhat;
};

// It's probably going to be easier to just use/remember the values 1, 2, 3, but...
const int VOLTDB_DEBUG_IGNORE_123 = 1;
const int VOLTDB_DEBUG_ASSERT_123 = 1;
const int VOLTDB_DEBUG_THROW_123 = 2;
const int VOLTDB_DEBUG_CRASH_123 = 3;
// Enable configurable response to unpossibilies, choosing by convention from a menu of:
// 1 assert/ignore in the caller vs.
// 2 throw in the caller vs.
// 3 crash here and now
inline bool debug_false_or_true_or_crash_123(int one_or_two_or_three) {
    // Get a crash (div by 0) for 3, true for 2, false for 1 (and true for anything else).
    return ( 2 / (3 - one_or_two_or_three) ) == 2;
}
#ifdef NDEBUG
#define DEBUG_ASSERT_OR_THROW_OR_CRASH_123(CONDITION, ONE_OR_TWO_OR_THREE, STREAMBLES) {}
#define DEBUG_IGNORE_OR_THROW_OR_CRASH_123(ONE_OR_TWO_OR_THREE, STREAMABLES) {}
#else
#define DEBUG_ASSERT_OR_THROW_OR_CRASH_123(CONDITION, ONE_OR_TWO_OR_THREE, STREAMABLES) {\
    if ( ! (CONDITION) ) {                                                               \
        if (debug_false_or_true_or_crash_123(ONE_OR_TWO_OR_THREE)) {                     \
            throwFatalLogicErrorStreamed(STREAMABLES);                                   \
        }                                                                                \
        assert(CONDITION);                                                               \
    }                                                                                    \
}

#define DEBUG_IGNORE_OR_THROW_OR_CRASH_123(ONE_OR_TWO_OR_THREE, STREAMABLES) {\
    if (debug_false_or_true_or_crash_123(ONE_OR_TWO_OR_THREE)) {              \
        throwFatalLogicErrorStreamed(STREAMABLES);                            \
    }                                                                         \
}

#endif

}
#endif /* FATALEXCEPTION_HPP_ */
