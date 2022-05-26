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

#pragma once

#include <chrono>
#include <string>
#include <vector>

#include "boost/algorithm/string/predicate.hpp"
#include "boost/functional/hash.hpp"
#include "common/debuglog.h"

namespace voltdb {

/**
 * Miscellaneous static utility methods.
 */
class MiscUtil {
  public:

    /**
     * Split string on delimiter into any number of sub-strings.
     */
    static std::vector<std::string> splitString(std::string const& str, char delimiter);

    /**
     * Split string on delimiter into two sub-strings.
     */
    static std::vector<std::string> splitToTwoString(std::string const& str, char delimiter);

    /**
     * A hashCombine function that can deal with the quirks of floating point math
     * on the various platforms that we support.
     */
    static void hashCombineFloatingPoint(std::size_t &seed, double value) {
        boost::hash_combine(seed, value);
    }

    /**
     * Return true if the string is "true"
     */
    static bool parseBool(const std::string* value) {
        return value != nullptr && boost::iequals(*value, "true");
    }
};

/**
 * As the name suggests, this is not ready for use.
 * Use one of the following sub-classes, and create
 * the timer with the help of TimerProxy instead.
 */
class AbstractTimer {
protected:
    using time_point = std::chrono::time_point<std::chrono::steady_clock>;
    using usec = std::chrono::duration<double, std::micro>;
    FILE* m_fp;
    std::string const m_operation;
    time_point const m_started;

    static time_point now() noexcept {
        return std::chrono::steady_clock::now();
    }
    static usec diff(time_point const& from, time_point const& to) {
        return std::chrono::duration_cast<usec>(to - from);
    }
    static double format_duration(usec const&, char const*&);
public:
    FILE* file() const noexcept {
        return m_fp;
    }
    AbstractTimer(FILE*, char const*);
};

/**
 * A simple, one-shot timer, which starts at object creation
 * time, and stop at destruction time by RAII.
 * Timing is written to the file handle, which is not validated
 * nor closed.
 *
 * Use TimerProxy<SimpleTimer> instead of SimpleTimer for safer
 * file handling.
 */
class SimpleTimer: public AbstractTimer {
    void report(usec const&) const;
public:
    SimpleTimer(FILE* sink, char const* op) : AbstractTimer(sink, op) {}
    ~SimpleTimer();
};

/**
 * A RestartableTimer is a timer which, is not automatically
 * started upon RestartableTimer object creation. Instead, you
 * call RestartableTimer::get() to get a fire of the timer. It,
 * too, is managed using RAII; but RestartableTimer::get() may be
 * called as many times as you like. When the RestartableTimer
 * object gets destructed, a detailed report is written to the
 * file handle.
 * Use TimerProxy<RestartableTimer> instead of bare
 * RestartableTimer for safer file handling.
 */
class RestartableTimer: public AbstractTimer {
    bool m_active = false;
    size_t const m_flushPeriod, m_binWidth;
    std::vector<size_t> m_elapsed;
    static std::string statistics(std::vector<size_t> const&, size_t bin_size);
    void report(bool final_report) const;
    void stop();
    void restart();
protected:
    bool active() const noexcept {
        return m_active;
    }
public:
    /**
     * Ctor.
     * \arg file handle
     * \arg name of current restartable timer
     * \arg period - statistics calculation/flushing period.
     * For example, if get() is called 1,000 times, you get
     * 1,000 timing events. Then, if period = 200, a detailed
     * report is generated every 200 fires.
     * Set to 0 to disable periodic flushing.
     *
     * \arg binWidht - statististics aggregation bin width. In
     * the detailed report, it summarizes important statistics of
     * **all** firings, as well as firings as separated into
     * non-overlapping bins. For example, if get() is called
     * 1,000 times cumulatively, with a bin width of 200, then, a
     * full report of statistics based on 1,000 timings as well
     * as every 200 timings are calculated and presented in the
     * report.
     */
    RestartableTimer(FILE*, char const*, size_t period, size_t binWidth);
    size_t count() const noexcept;
    class ScopedTimer {
        RestartableTimer& m_tm;
        ScopedTimer(RestartableTimer&) noexcept;
    public:
        static ScopedTimer create(RestartableTimer&) noexcept;
        ~ScopedTimer() noexcept;
    };
    /**
     * Usage:
     * {
     *   auto tm = instance_RestartableTimer.get();
     *   ...
     * }
     */
    ScopedTimer get();
    ~RestartableTimer();
};

/**
 * A total, RestartableTimer that also includes any number of
 * one-level, named, sub-timers that is scoped by the "total"
 * timer.
 *
 * e.g.
 * RestartableTimer totalTm("TotalTimer", "stdout",
 *      512, // summarize in every 512-events window
 *      {{"foo", "bar", "baz"}});   // sub timers
 *  ...
 *  {
 *      // start total timer, the same way as RestartableTimer
 *      auto const totalTm = TotalTm.get();
 *      if (foo) {
 *          // start named sub timer
 *          auto const fooTm = TotalTm.get("foo");
 *          ...
 *      }
 *      while (bar) {
 *          // start anoher named sub timer
 *          auto const barTm = TotalTm.get("bar");
 *      }
 *  }
 */
class RestartableTimers: RestartableTimer {
    using super = RestartableTimer;
    std::unordered_map<std::string, super> m_subTimers;
public:
    RestartableTimers(FILE*, char const*, size_t,
            std::vector<std::string> const&&/* named sub-timers */);
    using super::get; using super::file;
    typename super::ScopedTimer get(char const*);
};

/**
 * Wrapper that scopes file handle, by requesting for file name
 * only, and transforms into 2nd arg of respective type's ctor.
 *
 * e.g.
 * auto tm = TimerProxy<RestartableTimer>("/tmp/foo.z", "Timer", 0,
 *          500);
 *  // fwds to:
 *  // RestartableTimer tm(fopen("/tmp/foo.z", "w"), "Timer", 0,
 *                  500);
 *  // only with scoped control of fclose when tm is destructed.
 *
 *  When the 1st \arg sink is "stdout" or "stderr", the standard
 *  output/error instead of actual file is used for writing
 *  report.
 */
template<typename T>
class TimerProxy {
#ifndef NDEBUG
    // debug build fwds to underlying timer
    T* m_instance;
public:
    template<typename... Args> inline
    TimerProxy(char const* sink, Args&&... args):
        m_instance(new T{! strcmp(sink, "stdout") ? stdout :
                ! strcmp(sink, "stderr") ? stderr : fopen(sink, "w"),
            std::forward<Args&&>(args)...}) {
        if (! m_instance->file()) {
            char buf[128];
            snprintf(buf, sizeof buf,
                    "Failed to open sink \"%s\" for write or append", sink);
            buf[sizeof buf - 1] = '\0';
            throw std::runtime_error(buf);
        }
    }
    ~TimerProxy() {
        auto* fp = m_instance->file();
        delete m_instance;
        if (fp) {
            if (fp != stdout && fp != stderr) {
                fclose(fp);
            } else {
                fflush(fp);
            }
        }
    }
    T& operator()() const {
        return *m_instance;
    }
#else
    // release build uses zero-cost dummy object that does
    // absolutely nothing. The class template arg T is simply
    // ignored.
    struct DummyTimer {
        constexpr bool get(char const*) const noexcept {
            return get();
        }
        constexpr bool get() const noexcept {
            return false;
        }
    } m_dummyTimer;
public:
    template<typename... Args> inline constexpr
    TimerProxy(char const*, Args&&...) noexcept {}
    constexpr DummyTimer const& operator()() const noexcept {
        return m_dummyTimer;
    }
#endif
};

} // namespace voltdb

