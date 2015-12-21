/// Copyright 2010 Google
/// Licensed under the Apache License, Version 2.0 (the "License");
/// you may not use this file except in compliance with the License.
/// You may obtain a copy of the License at
///
///     http://www.apache.org/licenses/LICENSE-2.0
///
/// Unless required by applicable law or agreed to in writing, software
/// distributed under the License is distributed on an "AS IS" BASIS,
/// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
/// See the License for the specific language governing permissions and
/// limitations under the License.

#ifndef BASE_LOGGING_H
#define BASE_LOGGING_H

#include <stdlib.h>
#include <iostream>
using std::ostream;
using std::cout;
using std::endl;

#include "s2geo/base/macros.h"

/// Always-on checking
#define CHECK(x)	if(x){}else LogMessageFatal(__FILE__, __LINE__).stream() << "Check failed: " #x
#define CHECK_LT(x, y)	CHECK((x) < (y))
#define CHECK_GT(x, y)	CHECK((x) > (y))
#define CHECK_LE(x, y)	CHECK((x) <= (y))
#define CHECK_GE(x, y)	CHECK((x) >= (y))
#define CHECK_EQ(x, y)	CHECK((x) == (y))
#define CHECK_NE(x, y)	CHECK((x) != (y))
#define CHECK_NOTNULL(x) CHECK((x) != NULL)

#ifndef NDEBUG
/// Debug-only checking.
#define DCHECK(condition) CHECK(condition)
#define DCHECK_EQ(val1, val2) CHECK_EQ(val1, val2)
#define DCHECK_NE(val1, val2) CHECK_NE(val1, val2)
#define DCHECK_LE(val1, val2) CHECK_LE(val1, val2)
#define DCHECK_LT(val1, val2) CHECK_LT(val1, val2)
#define DCHECK_GE(val1, val2) CHECK_GE(val1, val2)
#define DCHECK_GT(val1, val2) CHECK_GT(val1, val2)
#else
#define DCHECK(condition) CHECK(true)
#define DCHECK_EQ(val1, val2) CHECK(true)
#define DCHECK_NE(val1, val2) CHECK(true)
#define DCHECK_LE(val1, val2) CHECK(true)
#define DCHECK_LT(val1, val2) CHECK(true)
#define DCHECK_GE(val1, val2) CHECK(true)
#define DCHECK_GT(val1, val2) CHECK(true)
#endif

#define LOG_INFO LogMessage(__FILE__, __LINE__)
#define LOG_INFO_MSG(STREAM) LogMessage(__FILE__, __LINE__, (STREAM))
#define LOG_ERROR LOG_INFO
#define LOG_WARNING LOG_INFO
#define LOG_FATAL LogMessageFatal(__FILE__, __LINE__)
#define LOG_QFATAL LOG_FATAL

#define VLOG(x) if((x)>0){} else LOG_INFO.stream()
#define VMLOG(x, msg) if (((msg) == NULL) && ((x)>0)) {} else LOG_INFO_MSG(msg).stream()
#ifdef NDEBUG
#define DEBUG_MODE false
#define LOG_DFATAL LOG_ERROR
#else
#define DEBUG_MODE true
#define LOG_DFATAL LOG_FATAL
#endif

#define LOG(severity) LOG_ ## severity.stream()
#define LG LOG_INFO.stream()

namespace google_base {
class DateLogger {
 public:
  DateLogger();
  char* const HumanDate();
 private:
  char buffer_[9];
};
}  // namespace google_base

class LogMessage {
 public:
 LogMessage(const char* file, int line) : m_stream(&(std::cerr)) {
    std::cerr << "[" << pretty_date_.HumanDate() << "] "
              << file << ":" << line << ": ";
  }
  LogMessage(const char* file, int line, std::ostream *msg)
      : m_stream(msg) {
    if (m_stream == NULL) {
        m_stream = &std::cerr;
    }
    // Don't write metadata if we are not logging to std::cerr.
    if (m_stream == &std::cerr) {
        (*m_stream) << "[" << pretty_date_.HumanDate() << "] "
                    << file << ":" << line << ": ";
    }
  }
  ~LogMessage() {
      if (m_stream == &std::cerr) {
          (*m_stream) << "\n";
      }
  }
  std::ostream& stream() { return *m_stream; }

 private:
  google_base::DateLogger pretty_date_;
  std::ostream *m_stream;
  DISALLOW_COPY_AND_ASSIGN(LogMessage);
};

class LogMessageFatal : public LogMessage {
 public:
  LogMessageFatal(const char* file, int line)
    : LogMessage(file, line) { }
  ~LogMessageFatal() {
    std::cerr << "\n";
    abort();
  }
 private:
  DISALLOW_COPY_AND_ASSIGN(LogMessageFatal);
};

#endif  // BASE_LOGGING_H
