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

#include <boost/accumulators/accumulators.hpp>
#include <boost/accumulators/statistics.hpp>
#include "common/MiscUtil.h"

namespace voltdb {
using namespace std;
vector<string> MiscUtil::splitString(string const& str, char delimiter) {
    vector<string> vec;
    size_t begin = 0;
    while (true) {
        size_t end = str.find(delimiter, begin);
        if (end == string::npos) {
            if (begin != str.size()) {
                vec.push_back(str.substr(begin));
            }
            break;
        }
        vec.push_back(str.substr(begin, end - begin));
        begin = end + 1;
    }
    return vec;
}

vector<string> MiscUtil::splitToTwoString(string const& str, char delimiter) {
    vector<string> vec;
    size_t end = str.find(delimiter);
    if (end == string::npos) {
        vec.push_back(str);
    } else {
        vec.push_back(str.substr(0, end));
        vec.push_back(str.substr(end + 1));
    }
    return vec;
}

AbstractTimer::AbstractTimer(FILE* sink, char const* op):
    m_fp(sink), m_operation(op), m_started(now()) {}

double AbstractTimer::format_duration(usec const& elapsed, char const*& unit) {
    static char const* usec = "usec", *msec = "msec";
    auto const val = elapsed.count();
    if (val < 1e3) {
        unit = usec;
        return val;
    } else {
        unit = msec;
        return val / 1e3;
    }
}

SimpleTimer::~SimpleTimer() {
    report(diff(m_started, now()));
}

void SimpleTimer::report(usec const& elapsed) const {
    char const* unit;
    auto const val = format_duration(elapsed, unit);
    fprintf(file(), "%s took %.1g %s\n", m_operation.c_str(), val, unit);
}

using namespace boost::accumulators;
struct statistics_calculator {
    accumulator_set<double,
        features<tag::count, tag::min, tag::max, tag::mean, tag::median>> m_stats;
public:
    template<typename iterator>
    statistics_calculator(iterator from, iterator to) {
        for_each(from, to, bind<void>(ref(m_stats), std::placeholders::_1));
    }
#define getter(T, name)                              \
    T name() const noexcept {                        \
        return extract_result<tag::name>(m_stats);   \
    }
    getter(size_t, count);
    getter(double, min);
    getter(double, max);
    getter(double, mean);
    getter(double, median);
#undef getter
};

/**
 * Use unit 1 with given stat values, if average value is below 5000;
 * use unit 2 with values divided by 1000 otherwise
 */
string to_string(char const* prefix, char const* unit1, char const* unit2,
        statistics_calculator const& stat) {
    ostringstream oss;
    bool const converted = stat.mean() > 1e3;
    char const* unit = converted ? unit2 : unit1;
    if (converted) {
        // convert from usec to msec
        oss << prefix << "{ count = " << stat.count()
            << ", min = " << stat.min() / 1e3 << " " << unit
            << ", max = " << stat.max() / 1e3 << " " << unit
            << ", mean = " << stat.mean() / 1e3 << " " << unit
            << ", median = " << stat.median() / 1e3 << " " << unit
            << "}\n";
    } else {
        oss << prefix << "{ count = " << stat.count()
            << ", min = " << stat.min() << " " << unit
            << ", max = " << stat.max() << " " << unit
            << ", mean = " << stat.mean() << " " << unit
            << ", median = " << stat.median() << " " << unit
            << "}\n";
    }
    return oss.str();
}

RestartableTimer::RestartableTimer(FILE* sink, char const* op,
        size_t periodic_report, size_t binWidth):
    AbstractTimer(sink, op),
    m_flushPeriod(periodic_report), m_binWidth(binWidth) {
    m_elapsed.reserve(m_flushPeriod * 64);
}

void RestartableTimer::report(bool final_report) const {
    auto const* prefix = final_report ? "Final" : "Periodic";
    if (count()) {
        fprintf(file(), "[%s] %s summary:\n# # # # # # # # # # %s\n# # # # # # # # # #\n",
                m_operation.c_str(), prefix,
                statistics(const_cast<std::vector<size_t>&>(m_elapsed),
                    m_binWidth).c_str());
    } else {
        fprintf(file(), "[%s] %s summary: No timed events occurred\n",
                m_operation.c_str(), prefix);
    }
}

void RestartableTimer::stop() {
    assert(m_active);
    m_elapsed.emplace_back(diff(m_started, now()).count());
    m_active = false;
    if (m_flushPeriod && count() % m_flushPeriod == 0) {
        report(false);
    }
}

void RestartableTimer::restart() {
    assert(! m_active);
    m_active = true;
    const_cast<time_point&>(m_started) = now();
}

size_t RestartableTimer::count() const noexcept {
    return m_elapsed.size();
}

RestartableTimer::~RestartableTimer() {
    if (active()) {
        stop();
    }
    report(true);
}

inline RestartableTimer::ScopedTimer::ScopedTimer(RestartableTimer& tm) noexcept:
    m_tm(tm) {
    m_tm.restart();
}

RestartableTimer::ScopedTimer::~ScopedTimer() noexcept {
    m_tm.stop();
}

string RestartableTimer::statistics(
        vector<size_t> const& elapsed, size_t bin_size) {
    using iterator = typename vector<size_t>::const_iterator;
    size_t from = 0;
    size_t const full = elapsed.size(),
           est_bin_size = full / bin_size + 1;
    vector<statistics_calculator> bin_stats;
    bin_stats.reserve(est_bin_size);
    vector<pair<size_t, size_t>> bin_positions;
    bin_positions.reserve(est_bin_size);
    while (from < full) {
        auto const to = std::min(from + bin_size, full);
        bin_stats.emplace_back(next(elapsed.begin(), from), next(elapsed.begin(), to));
        bin_positions.emplace_back(from, to);
        from = to;
    }
    ostringstream oss;
    auto const full_stat = statistics_calculator{elapsed.begin(), elapsed.end()};
    oss << to_string("Total statistics\n# # # # # # # # # #\n", "usec", "msec", full_stat)
        << "\nBreak-down statistics\n# # # # # # # # # #\n";
    for (size_t i = 0; i < bin_positions.size(); ++i) {
        string prefix("[#");
        oss << to_string(
                prefix.append(std::to_string(bin_positions[i].first))
                .append(" - #").append(std::to_string(bin_positions[i].second - 1))
                .append("]: ").c_str(),
                "usec", "msec", bin_stats[i])
            << endl;
    }
    return oss.str();
}

RestartableTimers::RestartableTimers(FILE* sink, char const* op,
        size_t binWidth, vector<string> const&& names):
    super(sink, op, 0/* disables periodic report */, binWidth) {
    if (names.empty()) {
        printf("Warning: named timers not provided");
    } else {
        for_each(names.cbegin(), names.cend(),
                [sink, op, binWidth, this] (string const& nm) {
                    if (! m_subTimers.emplace(make_pair(nm,
                                RestartableTimer(sink,
                                        string(op).append(".").append(nm).c_str(),
                                        0, binWidth))).second) {
                        char buf[128];
                        snprintf(buf, sizeof buf,
                                "Duplicated name \"%s\" found for RestartableTimers",
                                nm.c_str());
                        buf[sizeof buf - 1] = '\0';
                        throw logic_error(buf);
                    }
                });
    }
}

typename RestartableTimer::ScopedTimer RestartableTimers::get(char const* k) {
    auto iter = m_subTimers.find(k);
    if (iter == m_subTimers.cend()) {
        char buf[128];
        snprintf(buf, sizeof buf, "Cannot find sub-timer named \"%s\". Typo?", k);
        buf[sizeof buf - 1] = '\0';
        throw logic_error(buf);
    } else {
        return iter->second.get();
    }
}

inline typename RestartableTimer::ScopedTimer RestartableTimer::ScopedTimer::create(
        RestartableTimer& tm) noexcept {
    return {tm};
}

typename RestartableTimer::ScopedTimer RestartableTimer::get() {
    return ScopedTimer::create(*this);
}

} // namespace voltdb
