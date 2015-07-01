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

#include "boost/date_time/gregorian/greg_date.hpp"
#include "boost/date_time/posix_time/posix_time_types.hpp"
#include "boost/date_time/posix_time/posix_time_duration.hpp"
#include "boost/date_time/posix_time/ptime.hpp"
#include "boost/date_time/posix_time/conversion.hpp"
#include <ctime>
#include "common/SQLException.h"
#include "common/executorcontext.hpp"
#include "common/NValue.hpp"

static const boost::posix_time::ptime EPOCH(boost::gregorian::date(1970,1,1));
static const int64_t GREGORIAN_EPOCH = -12212553600000000;  // 1583-01-01 00:00:00
static const int8_t QUARTER_START_MONTH_BY_MONTH[] = {
        /*[0] not used*/-1,  1, 1, 1,  4, 4, 4,  7, 7, 7,  10, 10, 10 };

/** Convert from epoch_micros to date **/
static inline void micros_to_date(int64_t epoch_micros_in, boost::gregorian::date& date_out) {
    if (epoch_micros_in < GREGORIAN_EPOCH) {
        throw voltdb::SQLException(voltdb::SQLException::data_exception_numeric_value_out_of_range,
                "Value out of range. Cannot convert dates prior to the year 1583");
    }
    boost::posix_time::ptime input_ptime = EPOCH + boost::posix_time::microseconds(epoch_micros_in);
    date_out = input_ptime.date();
}

/** Convert from epoch_micros to time **/
static inline void micros_to_time(int64_t epoch_micros_in, boost::posix_time::time_duration& time_out) {
    if (epoch_micros_in < GREGORIAN_EPOCH) {
        throw voltdb::SQLException(voltdb::SQLException::data_exception_numeric_value_out_of_range,
                "Value out of range. Cannot convert dates prior to the year 1583");
    }
    boost::posix_time::ptime input_ptime = EPOCH + boost::posix_time::microseconds(epoch_micros_in);
    time_out = input_ptime.time_of_day();
}

/** Convert from epoch_micros to date and time **/
static inline void micros_to_date_and_time(int64_t epoch_micros_in, boost::gregorian::date& date_out,
        boost::posix_time::time_duration& time_out) {
    if (epoch_micros_in < GREGORIAN_EPOCH) {
        throw voltdb::SQLException(voltdb::SQLException::data_exception_numeric_value_out_of_range,
                "Value out of range. Cannot convert dates prior to the year 1583");
    }
    boost::posix_time::ptime input_ptime = EPOCH + boost::posix_time::microseconds(epoch_micros_in);
    date_out = input_ptime.date();
    time_out = input_ptime.time_of_day();
}

/** Convert from timestamp to micros since epoch **/
static inline int64_t epoch_microseconds_from_components(unsigned short int year, unsigned short int month = 1,
        unsigned short int day = 1, int hour = 0, int minute = 0, int second = 0) {
    boost::gregorian::date goal_date = boost::gregorian::date(year, month, day);
    boost::posix_time::time_duration goal_time = boost::posix_time::time_duration(hour,minute,second);
    boost::posix_time::ptime goal_ptime = boost::posix_time::ptime(goal_date,goal_time);
    boost::posix_time::time_period goal_period (EPOCH, goal_ptime);
    boost::posix_time::time_duration goal_duration = goal_period.length();
    int64_t epoch_seconds = goal_duration.ticks() / goal_duration.ticks_per_second();
    return epoch_seconds * 1000000;
}

namespace voltdb {

// REFER JAVA class: UniqueIdGenerator.
// 23 bits are used for COUNTER_BITS and PARTITIONID_BITS.
// The left 41 bits (64 - 23) are used for TIMESTAMP.

static const long COUNTER_BITS = 9;
static const long PARTITIONID_BITS = 14;
static const int64_t VOLT_EPOCH = epoch_microseconds_from_components(2008);

/** implement the timestamp YEAR extract function **/
template<> inline NValue NValue::callUnary<FUNC_EXTRACT_YEAR>() const {
    if (isNull()) {
        return *this;
    }
    int64_t epoch_micros = getTimestamp();
    boost::gregorian::date as_date;
    micros_to_date(epoch_micros, as_date);
    return getIntegerValue(as_date.year());
}

/** implement the timestamp MONTH extract function **/
template<> inline NValue NValue::callUnary<FUNC_EXTRACT_MONTH>() const {
    if (isNull()) {
        return *this;
    }
    int64_t epoch_micros = getTimestamp();
    boost::gregorian::date as_date;
    micros_to_date(epoch_micros, as_date);
    return getTinyIntValue((int8_t)as_date.month());
}

/** implement the timestamp DAY extract function **/
template<> inline NValue NValue::callUnary<FUNC_EXTRACT_DAY>() const {
    if (isNull()) {
        return *this;
    }
    int64_t epoch_micros = getTimestamp();
    boost::gregorian::date as_date;
    micros_to_date(epoch_micros, as_date);
    return getTinyIntValue((int8_t)as_date.day());
}

/** implement the timestamp DAY OF WEEK extract function **/
template<> inline NValue NValue::callUnary<FUNC_EXTRACT_DAY_OF_WEEK>() const {
    if (isNull()) {
        return *this;
    }
    int64_t epoch_micros = getTimestamp();
    boost::gregorian::date as_date;
    micros_to_date(epoch_micros, as_date);
    return getTinyIntValue((int8_t)(as_date.day_of_week() + 1)); // Have 0-based, want 1-based.
}

/** implement the timestamp WEEKDAY extract function **/
// It is almost the same as FUNC_EXTRACT_DAY_OF_WEEK
// Monday-0, ..., Saturday-5, Sunday-6
template<> inline NValue NValue::callUnary<FUNC_EXTRACT_WEEKDAY>() const {
    if (isNull()) {
        return *this;
    }
    int64_t epoch_micros = getTimestamp();
    boost::gregorian::date as_date;
    micros_to_date(epoch_micros, as_date);
    return getTinyIntValue((int8_t)((as_date.day_of_week() + 6) % 7));
}

/** implement the timestamp WEEK OF YEAR extract function **/
template<> inline NValue NValue::callUnary<FUNC_EXTRACT_WEEK_OF_YEAR>() const {
    if (isNull()) {
        return *this;
    }
    int64_t epoch_micros = getTimestamp();
    boost::gregorian::date as_date;
    micros_to_date(epoch_micros, as_date);
    return getTinyIntValue((int8_t)as_date.week_number());
}

/** implement the timestamp DAY OF YEAR extract function **/
template<> inline NValue NValue::callUnary<FUNC_EXTRACT_DAY_OF_YEAR>() const {
    if (isNull()) {
        return *this;
    }
    int64_t epoch_micros = getTimestamp();
    boost::gregorian::date as_date;
    micros_to_date(epoch_micros, as_date);
    return getSmallIntValue((int16_t)as_date.day_of_year());
}

/** implement the timestamp QUARTER extract function **/
template<> inline NValue NValue::callUnary<FUNC_EXTRACT_QUARTER>() const {
    if (isNull()) {
        return *this;
    }
    int64_t epoch_micros = getTimestamp();
    boost::gregorian::date as_date;
    micros_to_date(epoch_micros, as_date);
    return getTinyIntValue((int8_t)((as_date.month() + 2) / 3));
}

/** implement the timestamp HOUR extract function **/
template<> inline NValue NValue::callUnary<FUNC_EXTRACT_HOUR>() const {
    if (isNull()) {
        return *this;
    }
    int64_t epoch_micros = getTimestamp();
    boost::posix_time::time_duration as_time;
    micros_to_time(epoch_micros, as_time);
    return getTinyIntValue((int8_t)as_time.hours());
}

/** implement the timestamp MINUTE extract function **/
template<> inline NValue NValue::callUnary<FUNC_EXTRACT_MINUTE>() const {
    if (isNull()) {
        return *this;
    }
    int64_t epoch_micros = getTimestamp();
    boost::posix_time::time_duration as_time;
    micros_to_time(epoch_micros, as_time);
    return getTinyIntValue((int8_t)as_time.minutes());
}

/** implement the timestamp SECOND extract function **/
template<> inline NValue NValue::callUnary<FUNC_EXTRACT_SECOND>() const {
    if (isNull()) {
        return *this;
    }
    int64_t epoch_micros = getTimestamp();
    boost::posix_time::time_duration as_time;
    micros_to_time(epoch_micros, as_time);
    int second = as_time.seconds();
    int fraction = static_cast<int>(epoch_micros % 1000000);
    if (epoch_micros < 0 && fraction != 0) {
        fraction = 1000000 + fraction;
    }
    TTInt ttSecond(second);
    ttSecond *= NValue::kMaxScaleFactor;
    TTInt ttMicro(fraction);
    ttMicro *= NValue::kMaxScaleFactor / 1000000;
    return getDecimalValue(ttSecond + ttMicro);
}

/** implement the timestamp SINCE_EPOCH in SECONDs function **/
template<> inline NValue NValue::callUnary<FUNC_SINCE_EPOCH_SECOND>() const {
    if (isNull()) {
        return *this;
    }
    int64_t epoch_micros = getTimestamp();
    int64_t epoch_seconds = epoch_micros / 1000000;
    return getBigIntValue(epoch_seconds);
}

/** implement the timestamp SINCE_EPOCH in MILLISECONDs function **/
template<> inline NValue NValue::callUnary<FUNC_SINCE_EPOCH_MILLISECOND>() const {
    if (isNull()) {
        return *this;
    }
    int64_t epoch_micros = getTimestamp();
    int64_t epoch_milliseconds = epoch_micros / 1000;
    return getBigIntValue(epoch_milliseconds);
}

/** implement the timestamp SINCE_EPOCH in MICROSECONDs function **/
template<> inline NValue NValue::callUnary<FUNC_SINCE_EPOCH_MICROSECOND>() const {
    if (isNull()) {
        return *this;
    }
    int64_t epoch_micros = getTimestamp();
    return getBigIntValue(epoch_micros);
}

/** implement the timestamp TO_TIMESTAMP from SECONDs function **/
template<> inline NValue NValue::callUnary<FUNC_TO_TIMESTAMP_SECOND>() const {
    if (isNull()) {
        return *this;
    }
    int64_t seconds = castAsBigIntAndGetValue();
    int64_t epoch_micros = seconds * 1000000;
    return getTimestampValue(epoch_micros);
}

/** implement the timestamp TO_TIMESTAMP from MILLISECONDs function **/
template<> inline NValue NValue::callUnary<FUNC_TO_TIMESTAMP_MILLISECOND>() const {
    if (isNull()) {
        return *this;
    }
    int64_t millis = castAsBigIntAndGetValue();
    int64_t epoch_micros = millis * 1000;
    return getTimestampValue(epoch_micros);
}

/** implement the timestamp TO_TIMESTAMP from MICROSECONDs function **/
template<> inline NValue NValue::callUnary<FUNC_TO_TIMESTAMP_MICROSECOND>() const {
    if (isNull()) {
        return *this;
    }
    int64_t epoch_micros = castAsBigIntAndGetValue();
    return getTimestampValue(epoch_micros);
}

/** implement the timestamp TRUNCATE to YEAR function **/
template<> inline NValue NValue::callUnary<FUNC_TRUNCATE_YEAR>() const {
    if (isNull()) {
        return *this;
    }
    int64_t epoch_micros = getTimestamp();
    boost::gregorian::date as_date;
    micros_to_date(epoch_micros, as_date);
    int64_t truncate_epoch_micros = epoch_microseconds_from_components(as_date.year());
    return getTimestampValue(truncate_epoch_micros);
}

/** implement the timestamp TRUNCATE to QUARTER function **/
template<> inline NValue NValue::callUnary<FUNC_TRUNCATE_QUARTER>() const {
    if (isNull()) {
        return *this;
    }
    int64_t epoch_micros = getTimestamp();
    boost::gregorian::date as_date;
    micros_to_date(epoch_micros, as_date);
    int8_t quarter_start_month = QUARTER_START_MONTH_BY_MONTH[as_date.month()];
    int64_t truncate_epoch_micros = epoch_microseconds_from_components(as_date.year(), quarter_start_month);
    return getTimestampValue(truncate_epoch_micros);
}

/** implement the timestamp TRUNCATE to MONTH function **/
template<> inline NValue NValue::callUnary<FUNC_TRUNCATE_MONTH>() const {
    if (isNull()) {
        return *this;
    }
    int64_t epoch_micros = getTimestamp();
    boost::gregorian::date as_date;
    micros_to_date(epoch_micros, as_date);
    int64_t truncate_epoch_micros = epoch_microseconds_from_components(as_date.year(),as_date.month());
    return getTimestampValue(truncate_epoch_micros);
}

/** implement the timestamp TRUNCATE to DAY function **/
template<> inline NValue NValue::callUnary<FUNC_TRUNCATE_DAY>() const {
    if (isNull()) {
        return *this;
    }
    int64_t epoch_micros = getTimestamp();
    boost::gregorian::date as_date;
    micros_to_date(epoch_micros, as_date);
    int64_t truncate_epoch_micros =
            epoch_microseconds_from_components(as_date.year(),as_date.month(), as_date.day());
    return getTimestampValue(truncate_epoch_micros);
}

/** implement the timestamp TRUNCATE to HOUR function **/
template<> inline NValue NValue::callUnary<FUNC_TRUNCATE_HOUR>() const {
    if (isNull()) {
        return *this;
    }
    int64_t epoch_micros = getTimestamp();
    boost::gregorian::date as_date;
    boost::posix_time::time_duration as_time;
    micros_to_date_and_time(epoch_micros, as_date, as_time);
    int64_t truncate_epoch_micros = epoch_microseconds_from_components(as_date.year(),as_date.month(),
            as_date.day(), as_time.hours());
    return getTimestampValue(truncate_epoch_micros);
}

/** implement the timestamp TRUNCATE to MINUTE function **/
template<> inline NValue NValue::callUnary<FUNC_TRUNCATE_MINUTE>() const {
    if (isNull()) {
        return *this;
    }
    int64_t epoch_micros = getTimestamp();
    boost::gregorian::date as_date;
    boost::posix_time::time_duration as_time;
    micros_to_date_and_time(epoch_micros, as_date, as_time);
    int64_t truncate_epoch_micros = epoch_microseconds_from_components(as_date.year(),as_date.month(),
            as_date.day(), as_time.hours(), as_time.minutes());
    return getTimestampValue(truncate_epoch_micros);
}

/** implement the timestamp TRUNCATE to SECOND function **/
template<> inline NValue NValue::callUnary<FUNC_TRUNCATE_SECOND>() const {
    if (isNull()) {
        return *this;
    }
    int64_t epoch_micros = getTimestamp();
    boost::gregorian::date as_date;
    boost::posix_time::time_duration as_time;
    micros_to_date_and_time(epoch_micros, as_date, as_time);
    int64_t truncate_epoch_micros = epoch_microseconds_from_components(as_date.year(),as_date.month(),
            as_date.day(), as_time.hours(), as_time.minutes(),as_time.seconds());
    return getTimestampValue(truncate_epoch_micros);
}

/** implement the timestamp TRUNCATE to MILLIS function **/
template<> inline NValue NValue::callUnary<FUNC_TRUNCATE_MILLISECOND>() const {
    if (isNull()) {
        return *this;
    }
    int64_t epoch_micros = getTimestamp();
    int64_t epoch_millis = static_cast<int64_t>(epoch_micros / 1000);
    if (epoch_micros < 0) {
        epoch_millis -= 1;
    }
    return getTimestampValue(epoch_millis * 1000);
}

/** implement the timestamp TRUNCATE to MICROS function **/
template<> inline NValue NValue::callUnary<FUNC_TRUNCATE_MICROSECOND>() const {
    if (isNull()) {
        return *this;
    }
    int64_t epoch_micros = getTimestamp();
    return getTimestampValue(epoch_micros);
}

template<> inline NValue NValue::callConstant<FUNC_CURRENT_TIMESTAMP>() {
    ExecutorContext * context = voltdb::ExecutorContext::getExecutorContext();
    int64_t currentTimeMillis = context->currentUniqueId() >> (COUNTER_BITS + PARTITIONID_BITS);
    return getTimestampValue(currentTimeMillis * 1000 + VOLT_EPOCH);
}

}
