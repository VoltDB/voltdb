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

#include <ctime>
#include "common/SQLException.h"
#include "common/executorcontext.hpp"
#include "common/NValue.hpp"
#include "expressions/dateconstants.h"

static const int8_t QUARTER_START_MONTH_BY_MONTH[] = {
        /*[0] not used*/-1,  1, 1, 1,  4, 4, 4,  7, 7, 7,  10, 10, 10 };

static const int64_t PTIME_MAX_YEARS = 10000;
static const int64_t PTIME_MIN_YEARS = boost::gregorian::date(boost::gregorian::min_date_time).year();
static const int64_t PTIME_MAX_YEAR_INTERVAL = PTIME_MAX_YEARS - PTIME_MIN_YEARS;
static const int64_t PTIME_MIN_YEAR_INTERVAL = -PTIME_MAX_YEAR_INTERVAL;
static const int64_t PTIME_MAX_QUARTER_INTERVAL = PTIME_MAX_YEAR_INTERVAL * 4;
static const int64_t PTIME_MIN_QUARTER_INTERVAL = -PTIME_MAX_QUARTER_INTERVAL;
static const int64_t PTIME_MAX_MONTH_INTERVAL = PTIME_MAX_YEAR_INTERVAL * 12;
static const int64_t PTIME_MIN_MONTH_INTERVAL = -PTIME_MAX_MONTH_INTERVAL;
static const int64_t PTIME_MAX_DAY_INTERVAL =  PTIME_MAX_YEAR_INTERVAL * 365 + (PTIME_MAX_YEARS / 4);
static const int64_t PTIME_MIN_DAY_INTERVAL = -PTIME_MAX_DAY_INTERVAL;
static const int64_t PTIME_MAX_HOUR_INTERVAL = PTIME_MAX_DAY_INTERVAL * 24;
static const int64_t PTIME_MIN_HOUR_INTERVAL = -PTIME_MAX_HOUR_INTERVAL;
static const int64_t PTIME_MAX_MINUTE_INTERVAL = PTIME_MAX_HOUR_INTERVAL * 60;
static const int64_t PTIME_MIN_MINUTE_INTERVAL = -PTIME_MAX_MINUTE_INTERVAL;
static const int64_t PTIME_MAX_SECOND_INTERVAL = PTIME_MAX_MINUTE_INTERVAL * 60;
static const int64_t PTIME_MIN_SECOND_INTERVAL =  -PTIME_MAX_SECOND_INTERVAL;
static const int64_t PTIME_MAX_MILLISECOND_INTERVAL = PTIME_MAX_SECOND_INTERVAL * 1000;
static const int64_t PTIME_MIN_MILLISECOND_INTERVAL = -PTIME_MAX_MILLISECOND_INTERVAL;
static const int64_t PTIME_MAX_MICROSECOND_INTERVAL = PTIME_MAX_MILLISECOND_INTERVAL * 1000;
static const int64_t PTIME_MIN_MICROSECOND_INTERVAL = -PTIME_MAX_MICROSECOND_INTERVAL;

#define EMICROS (((int64_t )epoch_microseconds_from_components(EPOCH_DATE.year())))
// Given value is mapped to start or end of the interval if value is less than EPOCH ceil and floor is flipped.
#define START_OR_END_OFFSET_BY_INTERVAL(start, isLtEpoch, value, multiplier)        (isLtEpoch ? ((start ? std::ceil(value) : std::floor(value)) * multiplier) : ((start ? std::floor(value) : std::ceil(value)) * multiplier))

static inline bool epochMicrosOutOfRange(int64_t epochMicros) {
    return (epochMicros < GREGORIAN_EPOCH || epochMicros > NYE9999);
}

static inline void throwOutOfRangeTimestampInput(const std::string& func) {
    std::ostringstream oss;

    oss << "Input to SQL function " << func << " is outside of the supported range (years 1583 to 9999, inclusive).";

    throw voltdb::SQLException(voltdb::SQLException::data_exception_numeric_value_out_of_range,
                               oss.str());
}

static inline void throwOutOfRangeTimestampOutput(const std::string& func) {
    std::ostringstream oss;

    oss << "SQL function " << func << " would produce a value outside of the supported range (years 1583 to 9999, inclusive).";

    throw voltdb::SQLException(voltdb::SQLException::data_exception_numeric_value_out_of_range,
                               oss.str());
}



/** Convert from epoch_micros to date **/
static inline void micros_to_date(int64_t epoch_micros_in, boost::gregorian::date& date_out) {
    boost::posix_time::ptime input_ptime = EPOCH + boost::posix_time::microseconds(epoch_micros_in);
    date_out = input_ptime.date();
}

/** Convert from epoch_micros to time **/
static inline void micros_to_time(int64_t epoch_micros_in, boost::posix_time::time_duration& time_out) {
    boost::posix_time::ptime input_ptime = EPOCH + boost::posix_time::microseconds(epoch_micros_in);
    time_out = input_ptime.time_of_day();
}

/** Convert from epoch_micros to ptime **/
static inline void micros_to_ptime(int64_t epoch_micros_in, boost::posix_time::ptime& ptime_out) {
    ptime_out = EPOCH + boost::posix_time::microseconds(epoch_micros_in);
}

/** Convert from epoch_micros to date and time **/
static inline void micros_to_date_and_time(int64_t epoch_micros_in, boost::gregorian::date& date_out,
                                           boost::posix_time::time_duration& time_out) {
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

static inline int64_t addMonths(int64_t epoch_micros, int64_t months) {
    boost::posix_time::ptime ts;
    micros_to_ptime(epoch_micros, ts);

    try {
        ts += boost::gregorian::months(static_cast<int>(months));
    } catch (std::out_of_range &e) {
        throw voltdb::SQLException(voltdb::SQLException::data_exception_numeric_value_out_of_range, "interval is too large for DATEADD function");
    }
    boost::posix_time::time_duration td = ts - EPOCH;
    return td.total_microseconds();
}

namespace voltdb {

// REFER JAVA class: UniqueIdGenerator.
// 23 bits are used for COUNTER_BITS and PARTITIONID_BITS.
// The left 41 bits (64 - 23) are used for TIMESTAMP.

static const long COUNTER_BITS = 9;
static const long PARTITIONID_BITS = 14;

/** implement the timestamp YEAR extract function **/
template<> inline NValue NValue::callUnary<FUNC_EXTRACT_YEAR>() const {
    if (isNull()) {
        return *this;
    }

    if (getValueType() != ValueType::tTIMESTAMP) {
        throwCastSQLException(getValueType(), ValueType::tTIMESTAMP);
    }

    int64_t epoch_micros = getTimestamp();
    if (epochMicrosOutOfRange(epoch_micros)) {
        throwOutOfRangeTimestampInput("YEAR");
    }

    boost::gregorian::date as_date;
    micros_to_date(epoch_micros, as_date);
    return getIntegerValue(as_date.year());
}

/** implement the timestamp MONTH extract function **/
template<> inline NValue NValue::callUnary<FUNC_EXTRACT_MONTH>() const {
    if (isNull()) {
        return *this;
    }

    if (getValueType() != ValueType::tTIMESTAMP) {
        throwCastSQLException(getValueType(), ValueType::tTIMESTAMP);
    }

    int64_t epoch_micros = getTimestamp();
    if (epochMicrosOutOfRange(epoch_micros)) {
        throwOutOfRangeTimestampInput("MONTH");
    }

    boost::gregorian::date as_date;
    micros_to_date(epoch_micros, as_date);
    return getTinyIntValue((int8_t)as_date.month());
}

/** implement the timestamp DAY extract function **/
template<> inline NValue NValue::callUnary<FUNC_EXTRACT_DAY>() const {
    if (isNull()) {
        return *this;
    }

    if (getValueType() != ValueType::tTIMESTAMP) {
        throwCastSQLException(getValueType(), ValueType::tTIMESTAMP);
    }

    int64_t epoch_micros = getTimestamp();
    if (epochMicrosOutOfRange(epoch_micros)) {
        throwOutOfRangeTimestampInput("DAY");
    }

    boost::gregorian::date as_date;
    micros_to_date(epoch_micros, as_date);
    return getTinyIntValue((int8_t)as_date.day());
}

/** implement the timestamp DAY OF WEEK extract function **/
template<> inline NValue NValue::callUnary<FUNC_EXTRACT_DAY_OF_WEEK>() const {
    if (isNull()) {
        return *this;
    }

    if (getValueType() != ValueType::tTIMESTAMP) {
        throwCastSQLException(getValueType(), ValueType::tTIMESTAMP);
    }

    int64_t epoch_micros = getTimestamp();
    if (epochMicrosOutOfRange(epoch_micros)) {
        throwOutOfRangeTimestampInput("DAY_OF_WEEK");
    }

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

    if (getValueType() != ValueType::tTIMESTAMP) {
        throwCastSQLException(getValueType(), ValueType::tTIMESTAMP);
    }

    int64_t epoch_micros = getTimestamp();
    if (epochMicrosOutOfRange(epoch_micros)) {
        throwOutOfRangeTimestampInput("WEEKDAY");
    }

    boost::gregorian::date as_date;
    micros_to_date(epoch_micros, as_date);
    return getTinyIntValue((int8_t)((as_date.day_of_week() + 6) % 7));
}

/** implement the timestamp WEEK OF YEAR extract function **/
template<> inline NValue NValue::callUnary<FUNC_EXTRACT_WEEK_OF_YEAR>() const {
    if (isNull()) {
        return *this;
    }

    if (getValueType() != ValueType::tTIMESTAMP) {
        throwCastSQLException(getValueType(), ValueType::tTIMESTAMP);
    }

    int64_t epoch_micros = getTimestamp();
    if (epochMicrosOutOfRange(epoch_micros)) {
        throwOutOfRangeTimestampInput("WEEK_OF_YEAR");
    }

    boost::gregorian::date as_date;
    micros_to_date(epoch_micros, as_date);
    return getTinyIntValue((int8_t)as_date.week_number());
}

/** implement the timestamp DAY OF YEAR extract function **/
template<> inline NValue NValue::callUnary<FUNC_EXTRACT_DAY_OF_YEAR>() const {
    if (isNull()) {
        return *this;
    }

    if (getValueType() != ValueType::tTIMESTAMP) {
        throwCastSQLException(getValueType(), ValueType::tTIMESTAMP);
    }

    int64_t epoch_micros = getTimestamp();
    if (epochMicrosOutOfRange(epoch_micros)) {
        throwOutOfRangeTimestampInput("DAY_OF_YEAR");
    }

    boost::gregorian::date as_date;
    micros_to_date(epoch_micros, as_date);
    return getSmallIntValue((int16_t)as_date.day_of_year());
}

/** implement the timestamp QUARTER extract function **/
template<> inline NValue NValue::callUnary<FUNC_EXTRACT_QUARTER>() const {
    if (isNull()) {
        return *this;
    }

    if (getValueType() != ValueType::tTIMESTAMP) {
        throwCastSQLException(getValueType(), ValueType::tTIMESTAMP);
    }

    int64_t epoch_micros = getTimestamp();
    if (epochMicrosOutOfRange(epoch_micros)) {
        throwOutOfRangeTimestampInput("QUARTER");
    }

    boost::gregorian::date as_date;
    micros_to_date(epoch_micros, as_date);
    return getTinyIntValue((int8_t)((as_date.month() + 2) / 3));
}

/** implement the timestamp HOUR extract function **/
template<> inline NValue NValue::callUnary<FUNC_EXTRACT_HOUR>() const {
    if (isNull()) {
        return *this;
    }

    if (getValueType() != ValueType::tTIMESTAMP) {
        throwCastSQLException(getValueType(), ValueType::tTIMESTAMP);
    }

    int64_t epoch_micros = getTimestamp();
    if (epochMicrosOutOfRange(epoch_micros)) {
        throwOutOfRangeTimestampInput("HOUR");
    }

    boost::posix_time::time_duration as_time;
    micros_to_time(epoch_micros, as_time);
    return getTinyIntValue((int8_t)as_time.hours());
}

/** implement the timestamp MINUTE extract function **/
template<> inline NValue NValue::callUnary<FUNC_EXTRACT_MINUTE>() const {
    if (isNull()) {
        return *this;
    }

    if (getValueType() != ValueType::tTIMESTAMP) {
        throwCastSQLException(getValueType(), ValueType::tTIMESTAMP);
    }

    int64_t epoch_micros = getTimestamp();
    if (epochMicrosOutOfRange(epoch_micros)) {
        throwOutOfRangeTimestampInput("MINUTE");
    }

    boost::posix_time::time_duration as_time;
    micros_to_time(epoch_micros, as_time);
    return getTinyIntValue((int8_t)as_time.minutes());
}

/** implement the timestamp SECOND extract function **/
template<> inline NValue NValue::callUnary<FUNC_EXTRACT_SECOND>() const {
    if (isNull()) {
        return *this;
    }

    if (getValueType() != ValueType::tTIMESTAMP) {
        throwCastSQLException(getValueType(), ValueType::tTIMESTAMP);
    }

    int64_t epoch_micros = getTimestamp();
    if (epochMicrosOutOfRange(epoch_micros)) {
        throwOutOfRangeTimestampInput("SECOND");
    }

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

    if (getValueType() != ValueType::tTIMESTAMP) {
        throwCastSQLException(getValueType(), ValueType::tTIMESTAMP);
    }

    int64_t epoch_micros = getTimestamp();
    if (epochMicrosOutOfRange(epoch_micros)) {
        throwOutOfRangeTimestampInput("SINCE_EPOCH");
    }

    int64_t epoch_seconds = epoch_micros / 1000000;
    return getBigIntValue(epoch_seconds);
}

/** implement the timestamp SINCE_EPOCH in MILLISECONDs function **/
template<> inline NValue NValue::callUnary<FUNC_SINCE_EPOCH_MILLISECOND>() const {
    if (isNull()) {
        return *this;
    }

    if (getValueType() != ValueType::tTIMESTAMP) {
        throwCastSQLException(getValueType(), ValueType::tTIMESTAMP);
    }

    int64_t epoch_micros = getTimestamp();
    if (epochMicrosOutOfRange(epoch_micros)) {
        throwOutOfRangeTimestampInput("SINCE_EPOCH");
    }

    int64_t epoch_milliseconds = epoch_micros / 1000;
    return getBigIntValue(epoch_milliseconds);
}

/** implement the timestamp SINCE_EPOCH in MICROSECONDs function **/
template<> inline NValue NValue::callUnary<FUNC_SINCE_EPOCH_MICROSECOND>() const {
    if (isNull()) {
        return *this;
    }

    if (getValueType() != ValueType::tTIMESTAMP) {
        throwCastSQLException(getValueType(), ValueType::tTIMESTAMP);
    }

    int64_t epoch_micros = getTimestamp();
    if (epochMicrosOutOfRange(epoch_micros)) {
        throwOutOfRangeTimestampInput("SINCE_EPOCH");
    }

    return getBigIntValue(epoch_micros);
}

/** implement the timestamp TO_TIMESTAMP from SECONDs function **/
template<> inline NValue NValue::callUnary<FUNC_TO_TIMESTAMP_SECOND>() const {
    static const int64_t MAX_SECONDS = std::numeric_limits<int64_t>::max() / 1000000;

    if (isNull()) {
        return *this;
    }

    int64_t seconds = castAsBigIntAndGetValue();
    if (seconds > MAX_SECONDS || seconds < -MAX_SECONDS) {
        // This would overflow the valid range of the 64-bit int storage, so decline to
        // produce a result from this undefined behavior
        std::string message = "Input to TO_TIMESTAMP would overflow TIMESTAMP data type";
        throw SQLException(SQLException::data_exception_numeric_value_out_of_range, message);
    }

    int64_t epoch_micros = seconds * 1000000;
    if (epochMicrosOutOfRange(epoch_micros)) {
        throwOutOfRangeTimestampOutput("TO_TIMESTAMP");
    }

    return getTimestampValue(epoch_micros);
}

/** implement the timestamp TO_TIMESTAMP from MILLISECONDs function **/
template<> inline NValue NValue::callUnary<FUNC_TO_TIMESTAMP_MILLISECOND>() const {
    static const int64_t MAX_MILLIS = std::numeric_limits<int64_t>::max() / 1000;

    if (isNull()) {
        return *this;
    }

    int64_t millis = castAsBigIntAndGetValue();
    if (millis > MAX_MILLIS || millis < -MAX_MILLIS) {
        // This would overflow the valid range of the 64-bit int storage, so decline to
        // produce a result from this undefined behavior
        std::string message = "Input to TO_TIMESTAMP would overflow TIMESTAMP data type";
        throw SQLException(SQLException::data_exception_numeric_value_out_of_range, message);
    }

    int64_t epoch_micros = millis * 1000;
    if (epochMicrosOutOfRange(epoch_micros)) {
        throwOutOfRangeTimestampOutput("TO_TIMESTAMP");
    }

    return getTimestampValue(epoch_micros);
}

/** implement the timestamp TO_TIMESTAMP from MICROSECONDs function **/
template<> inline NValue NValue::callUnary<FUNC_TO_TIMESTAMP_MICROSECOND>() const {
    if (isNull()) {
        return *this;
    }

    int64_t epoch_micros = castAsBigIntAndGetValue();
    if (epochMicrosOutOfRange(epoch_micros)) {
        throwOutOfRangeTimestampOutput("TO_TIMESTAMP");
    }

    return getTimestampValue(epoch_micros);
}

/** implement the timestamp TRUNCATE to YEAR function **/
template<> inline NValue NValue::callUnary<FUNC_TRUNCATE_YEAR>() const {
    if (isNull()) {
        return *this;
    }

    if (getValueType() != ValueType::tTIMESTAMP) {
        throwCastSQLException(getValueType(), ValueType::tTIMESTAMP);
    }

    int64_t epoch_micros = getTimestamp();
    if (epochMicrosOutOfRange(epoch_micros)) {
        throwOutOfRangeTimestampInput("TRUNCATE");
    }

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

    if (getValueType() != ValueType::tTIMESTAMP) {
        throwCastSQLException(getValueType(), ValueType::tTIMESTAMP);
    }

    int64_t epoch_micros = getTimestamp();
    if (epochMicrosOutOfRange(epoch_micros)) {
        throwOutOfRangeTimestampInput("TRUNCATE");
    }

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

    if (getValueType() != ValueType::tTIMESTAMP) {
        throwCastSQLException(getValueType(), ValueType::tTIMESTAMP);
    }

    int64_t epoch_micros = getTimestamp();
    if (epochMicrosOutOfRange(epoch_micros)) {
        throwOutOfRangeTimestampInput("TRUNCATE");
    }

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

    if (getValueType() != ValueType::tTIMESTAMP) {
        throwCastSQLException(getValueType(), ValueType::tTIMESTAMP);
    }

    int64_t epoch_micros = getTimestamp();
    if (epochMicrosOutOfRange(epoch_micros)) {
        throwOutOfRangeTimestampInput("TRUNCATE");
    }

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

    if (getValueType() != ValueType::tTIMESTAMP) {
        throwCastSQLException(getValueType(), ValueType::tTIMESTAMP);
    }

    int64_t epoch_micros = getTimestamp();
    if (epochMicrosOutOfRange(epoch_micros)) {
        throwOutOfRangeTimestampInput("TRUNCATE");
    }

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

    if (getValueType() != ValueType::tTIMESTAMP) {
        throwCastSQLException(getValueType(), ValueType::tTIMESTAMP);
    }

    int64_t epoch_micros = getTimestamp();
    if (epochMicrosOutOfRange(epoch_micros)) {
        throwOutOfRangeTimestampInput("TRUNCATE");
    }

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

    if (getValueType() != ValueType::tTIMESTAMP) {
        throwCastSQLException(getValueType(), ValueType::tTIMESTAMP);
    }

    int64_t epoch_micros = getTimestamp();
    if (epochMicrosOutOfRange(epoch_micros)) {
        throwOutOfRangeTimestampInput("TRUNCATE");
    }

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

    if (getValueType() != ValueType::tTIMESTAMP) {
        throwCastSQLException(getValueType(), ValueType::tTIMESTAMP);
    }

    int64_t epoch_micros = getTimestamp();
    if (epochMicrosOutOfRange(epoch_micros)) {
        throwOutOfRangeTimestampInput("TRUNCATE");
    }

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

    if (getValueType() != ValueType::tTIMESTAMP) {
        throwCastSQLException(getValueType(), ValueType::tTIMESTAMP);
    }

    int64_t epoch_micros = getTimestamp();
    if (epochMicrosOutOfRange(epoch_micros)) {
        throwOutOfRangeTimestampInput("TRUNCATE");
    }

    return getTimestampValue(epoch_micros);
}

template<> inline NValue NValue::callConstant<FUNC_CURRENT_TIMESTAMP>() {
    ExecutorContext * context = voltdb::ExecutorContext::getExecutorContext();
    int64_t currentTimeMillis = context->currentUniqueId() >> (COUNTER_BITS + PARTITIONID_BITS);
    return getTimestampValue(currentTimeMillis * 1000 + VOLT_EPOCH);
}

template<> inline NValue NValue::call<FUNC_VOLT_DATEADD_YEAR>(const std::vector<NValue>& arguments) {
    vassert(arguments.size() == 2);

    const NValue& number = arguments[0];
    if (number.isNull()) {
        return getNullValue(ValueType::tTIMESTAMP);
    }

    const NValue& date = arguments[1];
    if (date.isNull()) {
        return getNullValue(ValueType::tTIMESTAMP);
    }

    int64_t interval = number.castAsBigIntAndGetValue();
    if (interval > PTIME_MAX_YEAR_INTERVAL || interval < PTIME_MIN_YEAR_INTERVAL) {
        throw SQLException(SQLException::data_exception_numeric_value_out_of_range, "interval is too large for DATEADD function");
    }

    if (date.getValueType() != ValueType::tTIMESTAMP) {
        throwCastSQLException(date.getValueType(), ValueType::tTIMESTAMP);
    }

    boost::posix_time::ptime ts;
    int64_t epochMicrosIn = date.getTimestamp();
    if (epochMicrosOutOfRange(epochMicrosIn)) {
        throwOutOfRangeTimestampInput("DATEADD");
    }

    micros_to_ptime(date.getTimestamp(), ts);

    try {
        ts += boost::gregorian::years(static_cast<int>(interval));
        boost::posix_time::time_duration td = ts - EPOCH;
        int64_t epochMicros = td.total_microseconds();
        if (epochMicrosOutOfRange(epochMicros)) {
            throwOutOfRangeTimestampOutput("DATEADD");
        }

        return getTimestampValue(epochMicros);
    } catch (std::out_of_range &e) {
        throw SQLException(SQLException::data_exception_numeric_value_out_of_range, "interval is too large for DATEADD function");
    }
}

template<> inline NValue NValue::call<FUNC_VOLT_DATEADD_QUARTER>(const std::vector<NValue>& arguments) {
    vassert(arguments.size() == 2);

    const NValue& number = arguments[0];
    if (number.isNull()) {
        return getNullValue(ValueType::tTIMESTAMP);
    }

    const NValue& date = arguments[1];
    if (date.isNull()) {
        return getNullValue(ValueType::tTIMESTAMP);
    }

    int64_t interval = number.castAsBigIntAndGetValue();
    if (interval > PTIME_MAX_QUARTER_INTERVAL || interval < PTIME_MIN_QUARTER_INTERVAL) {
        throw SQLException(SQLException::data_exception_numeric_value_out_of_range, "interval is too large for DATEADD function");
    }

    if (date.getValueType() != ValueType::tTIMESTAMP) {
        throwCastSQLException(date.getValueType(), ValueType::tTIMESTAMP);
    }

    int64_t epochMicrosIn = date.getTimestamp();
    if (epochMicrosOutOfRange(epochMicrosIn)) {
        throwOutOfRangeTimestampInput("DATEADD");
    }

    int64_t epochMicros = addMonths(date.getTimestamp(), 3 * interval);
    if (epochMicrosOutOfRange(epochMicros)) {
        throwOutOfRangeTimestampOutput("DATEADD");
    }

    return getTimestampValue(epochMicros);
}

template<> inline NValue NValue::call<FUNC_VOLT_DATEADD_MONTH>(const std::vector<NValue>& arguments) {
    vassert(arguments.size() == 2);

    const NValue& number = arguments[0];
    if (number.isNull()) {
        return getNullValue(ValueType::tTIMESTAMP);
    }

    const NValue& date = arguments[1];
    if (date.isNull()) {
        return getNullValue(ValueType::tTIMESTAMP);
    }

    int64_t interval = number.castAsBigIntAndGetValue();
    if (interval > PTIME_MAX_MONTH_INTERVAL || interval < PTIME_MIN_MONTH_INTERVAL) {
        throw SQLException(SQLException::data_exception_numeric_value_out_of_range, "interval is too large for DATEADD function");
    }

    if (date.getValueType() != ValueType::tTIMESTAMP) {
        throwCastSQLException(date.getValueType(), ValueType::tTIMESTAMP);
    }

    int64_t epochMicrosIn = date.getTimestamp();
    if (epochMicrosOutOfRange(epochMicrosIn)) {
        throwOutOfRangeTimestampInput("DATEADD");
    }

    int64_t epochMicros = addMonths(date.getTimestamp(), interval);
    if (epochMicrosOutOfRange(epochMicros)) {
        throwOutOfRangeTimestampOutput("DATEADD");
    }

    return getTimestampValue(epochMicros);
}

template<> inline NValue NValue::call<FUNC_VOLT_DATEADD_DAY>(const std::vector<NValue>& arguments) {
    vassert(arguments.size() == 2);

    const NValue& number = arguments[0];
    if (number.isNull()) {
        return getNullValue(ValueType::tTIMESTAMP);
    }

    const NValue& date = arguments[1];
    if (date.isNull()) {
        return getNullValue(ValueType::tTIMESTAMP);
    }

    int64_t interval = number.castAsBigIntAndGetValue();
    if (interval > PTIME_MAX_DAY_INTERVAL || interval < PTIME_MIN_DAY_INTERVAL) {
        throw SQLException(SQLException::data_exception_numeric_value_out_of_range, "interval is too large for DATEADD function");
    }

    if (date.getValueType() != ValueType::tTIMESTAMP) {
        throwCastSQLException(date.getValueType(), ValueType::tTIMESTAMP);
    }

    int64_t epochMicrosIn = date.getTimestamp();
    if (epochMicrosOutOfRange(epochMicrosIn)) {
        throwOutOfRangeTimestampInput("DATEADD");
    }

    boost::posix_time::ptime ts;
    micros_to_ptime(date.getTimestamp(), ts);

    try {
        ts += boost::gregorian::days(interval);
        boost::posix_time::time_duration td = ts - EPOCH;
        int64_t epochMicros = td.total_microseconds();
        if (epochMicrosOutOfRange(epochMicros)) {
            throwOutOfRangeTimestampOutput("DATEADD");
        }

        return getTimestampValue(epochMicros);
    } catch (std::out_of_range &e) {
        throw SQLException(SQLException::data_exception_numeric_value_out_of_range, "interval is too large for DATEADD function");
    }
}

template<> inline NValue NValue::call<FUNC_VOLT_DATEADD_HOUR>(const std::vector<NValue>& arguments) {
    vassert(arguments.size() == 2);

    const NValue& number = arguments[0];
    if (number.isNull()) {
        return getNullValue(ValueType::tTIMESTAMP);
    }

    const NValue& date = arguments[1];
    if (date.isNull()) {
        return getNullValue(ValueType::tTIMESTAMP);
    }

    int64_t interval = number.castAsBigIntAndGetValue();
    if (interval > PTIME_MAX_HOUR_INTERVAL || interval < PTIME_MIN_HOUR_INTERVAL) {
        throw SQLException(SQLException::data_exception_numeric_value_out_of_range, "interval is too large for DATEADD function");
    }

    if (date.getValueType() != ValueType::tTIMESTAMP) {
        throwCastSQLException(date.getValueType(), ValueType::tTIMESTAMP);
    }

    int64_t epochMicrosIn = date.getTimestamp();
    if (epochMicrosOutOfRange(epochMicrosIn)) {
        throwOutOfRangeTimestampInput("DATEADD");
    }

    boost::posix_time::ptime ts;
    micros_to_ptime(epochMicrosIn, ts);

    try {
        ts += boost::posix_time::hours(interval);
        boost::posix_time::time_duration td = ts - EPOCH;
        int64_t epochMicros = td.total_microseconds();
        if (epochMicrosOutOfRange(epochMicros)) {
            throwOutOfRangeTimestampOutput("DATEADD");
        }

        return getTimestampValue(epochMicros);
    } catch (std::out_of_range &e) {
        throw SQLException(SQLException::data_exception_numeric_value_out_of_range, "interval is too large for DATEADD function");
    }
}

template<> inline NValue NValue::call<FUNC_VOLT_DATEADD_MINUTE>(const std::vector<NValue>& arguments) {
    vassert(arguments.size() == 2);

    const NValue& number = arguments[0];
    if (number.isNull()) {
        return getNullValue(ValueType::tTIMESTAMP);
    }

    const NValue& date = arguments[1];
    if (date.isNull()) {
        return getNullValue(ValueType::tTIMESTAMP);
    }

    int64_t interval = number.castAsBigIntAndGetValue();
    if (interval > PTIME_MAX_MINUTE_INTERVAL || interval < PTIME_MIN_MINUTE_INTERVAL) {
        throw SQLException(SQLException::data_exception_numeric_value_out_of_range, "interval is too large for DATEADD function");
    }

    if (date.getValueType() != ValueType::tTIMESTAMP) {
        throwCastSQLException(date.getValueType(), ValueType::tTIMESTAMP);
    }

    int64_t epochMicrosIn = date.getTimestamp();
    if (epochMicrosOutOfRange(epochMicrosIn)) {
        throwOutOfRangeTimestampInput("DATEADD");
    }

    boost::posix_time::ptime ts;
    micros_to_ptime(epochMicrosIn, ts);

    try {
        ts += boost::posix_time::minutes(interval);
        boost::posix_time::time_duration td = ts - EPOCH;
        int64_t epochMicros = td.total_microseconds();
        if (epochMicrosOutOfRange(epochMicros)) {
            throwOutOfRangeTimestampOutput("DATEADD");
        }

        return getTimestampValue(epochMicros);
    } catch (std::out_of_range &e) {
        throw SQLException(SQLException::data_exception_numeric_value_out_of_range, "interval is too large for DATEADD function");
    }
}

template<> inline NValue NValue::call<FUNC_VOLT_DATEADD_SECOND>(const std::vector<NValue>& arguments) {
    vassert(arguments.size() == 2);

    const NValue& number = arguments[0];
    if (number.isNull()) {
        return getNullValue(ValueType::tTIMESTAMP);
    }

    const NValue& date = arguments[1];
    if (date.isNull()) {
        return getNullValue(ValueType::tTIMESTAMP);
    }

    int64_t interval = number.castAsBigIntAndGetValue();
    if (interval > PTIME_MAX_SECOND_INTERVAL || interval < PTIME_MIN_SECOND_INTERVAL) {
        throw SQLException(SQLException::data_exception_numeric_value_out_of_range, "interval is too large for DATEADD function");
    }

    if (date.getValueType() != ValueType::tTIMESTAMP) {
        throwCastSQLException(date.getValueType(), ValueType::tTIMESTAMP);
    }

    int64_t epochMicrosIn = date.getTimestamp();
    if (epochMicrosOutOfRange(epochMicrosIn)) {
        throwOutOfRangeTimestampInput("DATEADD");
    }

    boost::posix_time::ptime ts;
    micros_to_ptime(epochMicrosIn, ts);

    try {
        ts += boost::posix_time::seconds(interval);
        boost::posix_time::time_duration td = ts - EPOCH;
        int64_t epochMicros = td.total_microseconds();
        if (epochMicrosOutOfRange(epochMicros)) {
            throwOutOfRangeTimestampOutput("DATEADD");
        }

        return getTimestampValue(epochMicros);
    } catch (std::out_of_range &e) {
        throw SQLException(SQLException::data_exception_numeric_value_out_of_range, "interval is too large in DATEADD function");
    }
}

template<> inline NValue NValue::call<FUNC_VOLT_DATEADD_MILLISECOND>(const std::vector<NValue>& arguments) {
    vassert(arguments.size() == 2);

    const NValue& number = arguments[0];
    if (number.isNull()) {
        return getNullValue(ValueType::tTIMESTAMP);
    }

    const NValue& date = arguments[1];
    if (date.isNull()) {
        return getNullValue(ValueType::tTIMESTAMP);
    }

    int64_t interval = number.castAsBigIntAndGetValue();
    if (interval > PTIME_MAX_MILLISECOND_INTERVAL || interval < PTIME_MIN_MILLISECOND_INTERVAL) {
        throw SQLException(SQLException::data_exception_numeric_value_out_of_range, "interval is too large for DATEADD function");
    }

    if (date.getValueType() != ValueType::tTIMESTAMP) {
        throwCastSQLException(date.getValueType(), ValueType::tTIMESTAMP);
    }

    int64_t epochMicrosIn = date.getTimestamp();
    if (epochMicrosOutOfRange(epochMicrosIn)) {
        throwOutOfRangeTimestampInput("DATEADD");
    }

    boost::posix_time::ptime ts;
    micros_to_ptime(epochMicrosIn, ts);

    try {
        ts += boost::posix_time::milliseconds(interval);
        boost::posix_time::time_duration td = ts - EPOCH;
        int64_t epochMicros = td.total_microseconds();
        if (epochMicrosOutOfRange(epochMicros)) {
            throwOutOfRangeTimestampOutput("DATEADD");
        }

        return getTimestampValue(epochMicros);
    } catch (std::out_of_range &e) {
        throw SQLException(SQLException::data_exception_numeric_value_out_of_range, "interval is too large in DATEADD function");
    }
}

template<> inline NValue NValue::call<FUNC_VOLT_DATEADD_MICROSECOND>(const std::vector<NValue>& arguments) {
    vassert(arguments.size() == 2);

    const NValue& number = arguments[0];
    if (number.isNull()) {
        return getNullValue(ValueType::tTIMESTAMP);
    }

    const NValue& date = arguments[1];
    if (date.isNull()) {
        return getNullValue(ValueType::tTIMESTAMP);
    }

    int64_t interval = number.castAsBigIntAndGetValue();
    if (interval > PTIME_MAX_MICROSECOND_INTERVAL || interval < PTIME_MIN_MICROSECOND_INTERVAL) {
        throw SQLException(SQLException::data_exception_numeric_value_out_of_range, "interval is too large for DATEADD function");
    }

    if (date.getValueType() != ValueType::tTIMESTAMP) {
        throwCastSQLException(date.getValueType(), ValueType::tTIMESTAMP);
    }

    int64_t epochMicrosIn = date.getTimestamp();
    if (epochMicrosOutOfRange(epochMicrosIn)) {
        throwOutOfRangeTimestampInput("DATEADD");
    }

    boost::posix_time::ptime ts;
    micros_to_ptime(epochMicrosIn, ts);

    try {
        ts += boost::posix_time::microseconds(interval);
        boost::posix_time::time_duration td = ts - EPOCH;
        int64_t epochMicros = td.total_microseconds();
        if (epochMicrosOutOfRange(epochMicros)) {
            throwOutOfRangeTimestampOutput("DATEADD");
        }

        return getTimestampValue(epochMicros);
    } catch (std::out_of_range &e) {
        throw SQLException(SQLException::data_exception_numeric_value_out_of_range, "interval is too large in DATEADD function");
    }
}

const int64_t MIN_VALID_TIMESTAMP_VALUE = GREGORIAN_EPOCH;
const int64_t MAX_VALID_TIMESTAMP_VALUE = NYE9999;

inline bool timestampIsValid(int64_t ts) {
    return (MIN_VALID_TIMESTAMP_VALUE <= ts) && (ts <= MAX_VALID_TIMESTAMP_VALUE);
}

template<> inline NValue NValue::callConstant<FUNC_VOLT_MIN_VALID_TIMESTAMP>() {
    return getTimestampValue(MIN_VALID_TIMESTAMP_VALUE);
}

template<> inline NValue NValue::callConstant<FUNC_VOLT_MAX_VALID_TIMESTAMP>() {
    return getTimestampValue(MAX_VALID_TIMESTAMP_VALUE);
}

template<> inline NValue NValue::callUnary<FUNC_VOLT_IS_VALID_TIMESTAMP>() const {
    if (isNull()) {
        return getNullValue(ValueType::tBOOLEAN);
    }
    int64_t timestamp_number = castAsBigIntAndGetValue();
    return getBooleanValue(timestampIsValid(timestamp_number));
}

static inline int64_t getYearByIntervalWindow(bool start, int64_t epochMicrosIn, int64_t interval) {
    boost::posix_time::time_duration tsf;
    boost::gregorian::date dsf;

    try {
        micros_to_date_and_time ( epochMicrosIn, dsf, tsf);

        if (dsf.year() < EPOCH_DATE.year()) {
            double x = (EPOCH_DATE.year()-dsf.year()) / (double )interval;
            double y = START_OR_END_OFFSET_BY_INTERVAL(start, true, x, interval);
            boost::gregorian::date d = EPOCH_DATE - boost::gregorian::years ( y);
            return epoch_microseconds_from_components( d.year());
        } else {
            double x = (dsf.year()-EPOCH_DATE.year()) / (double )interval;
            double y = START_OR_END_OFFSET_BY_INTERVAL(start, false, x, interval);
            boost::gregorian::date d = EPOCH_DATE + boost::gregorian::years ( y);
            return epoch_microseconds_from_components(d.year());
        }
    } catch (std::out_of_range &e) {
        throw voltdb::SQLException(voltdb::SQLException::data_exception_numeric_value_out_of_range, "invalid interval or interval is too large for TIME_WINDOW function");
    }
}

template<> inline NValue NValue::call<FUNC_VOLT_TIME_WINDOW_YEAR_START>(const std::vector<NValue>& arguments) {
    vassert(arguments.size() == 2);

    const NValue& number = arguments[0];
    if (number.isNull()) {
        return getNullValue(ValueType::tTIMESTAMP);
    }
    int64_t interval = number.castAsBigIntAndGetValue();
    if (interval <= 0) {
        throw SQLException(SQLException::data_exception_numeric_value_out_of_range, "invalid interval or interval is too large for TIME_WINDOW function");
    }
    const NValue& date = arguments[1];
    if (date.isNull()) {
        return getNullValue(ValueType::tTIMESTAMP);
    }

    int64_t epochMicrosIn = date.getTimestamp();
    if (epochMicrosOutOfRange(epochMicrosIn)) {
        throwOutOfRangeTimestampInput("TIME_WINDOW");
    }
    return getTimestampValue(getYearByIntervalWindow(true, epochMicrosIn, interval));
}

template<> inline NValue NValue::call<FUNC_VOLT_TIME_WINDOW_YEAR_END>(const std::vector<NValue>& arguments) {
    vassert(arguments.size() == 2);

    const NValue& number = arguments[0];
    if (number.isNull()) {
        return getNullValue(ValueType::tTIMESTAMP);
    }
    int64_t interval = number.castAsBigIntAndGetValue();
    if (interval <= 0) {
        throw SQLException(SQLException::data_exception_numeric_value_out_of_range, "invalid interval or interval is too large for TIME_WINDOW function");
    }
    const NValue& date = arguments[1];
    if (date.isNull()) {
        return getNullValue(ValueType::tTIMESTAMP);
    }

    int64_t epochMicrosIn = date.getTimestamp();
    if (epochMicrosOutOfRange(epochMicrosIn)) {
        throwOutOfRangeTimestampInput("TIME_WINDOW");
    }
    return getTimestampValue(getYearByIntervalWindow(false, epochMicrosIn, interval));
}

static inline int64_t getQuarterByIntervalWindow(bool start, int64_t epochMicrosIn, int64_t interval) {
    boost::posix_time::time_duration tsf;
    boost::gregorian::date dsf;

    try {
        micros_to_date_and_time ( epochMicrosIn, dsf, tsf);
        if (dsf.year() < EPOCH_DATE.year()) {
            double x = ( ((EPOCH_DATE.year() - dsf.year()) * 12) - (dsf.month()) ) / (double )(interval * 3.0);
            double y = START_OR_END_OFFSET_BY_INTERVAL(start, true, x, (interval * 3.0));
            boost::gregorian::date d(1970, boost::date_time::Jan, 1);
            d = d - boost::gregorian::months ( y);
            return epoch_microseconds_from_components( d.year(), d.month());
        } else {
            double x = ( ((dsf.year() - EPOCH_DATE.year()) * 12) + (dsf.month()) ) / (double (interval * 3.0));
            double y = START_OR_END_OFFSET_BY_INTERVAL(start, false, x, (interval * 3.0));
            boost::gregorian::date d(1970, boost::date_time::Jan, 1);
            d = d + boost::gregorian::months(y);
            return epoch_microseconds_from_components( d.year(), d.month());
        }
    } catch (std::out_of_range &e) {
        throw voltdb::SQLException(voltdb::SQLException::data_exception_numeric_value_out_of_range, "invalid interval or interval is too large for TIME_WINDOW function");
    }
}

template<> inline NValue NValue::call<FUNC_VOLT_TIME_WINDOW_QUARTER_START>(const std::vector<NValue>& arguments) {
    vassert(arguments.size() == 2);

    const NValue& number = arguments[0];
    if (number.isNull()) {
        return getNullValue(ValueType::tTIMESTAMP);
    }
    int64_t interval = number.castAsBigIntAndGetValue();
    if (interval <= 0) {
        throw SQLException(SQLException::data_exception_numeric_value_out_of_range, "invalid interval or interval is too large for TIME_WINDOW function");
    }
    const NValue& date = arguments[1];
    if (date.isNull()) {
        return getNullValue(ValueType::tTIMESTAMP);
    }

    int64_t epochMicrosIn = date.getTimestamp();
    if (epochMicrosOutOfRange(epochMicrosIn)) {
        throwOutOfRangeTimestampInput("TIME_WINDOW");
    }
    return getTimestampValue(getQuarterByIntervalWindow(true, epochMicrosIn, interval));
}

template<> inline NValue NValue::call<FUNC_VOLT_TIME_WINDOW_QUARTER_END>(const std::vector<NValue>& arguments) {
    vassert(arguments.size() == 2);

    const NValue& number = arguments[0];
    if (number.isNull()) {
        return getNullValue(ValueType::tTIMESTAMP);
    }
    int64_t interval = number.castAsBigIntAndGetValue();
    if (interval <= 0) {
        throw SQLException(SQLException::data_exception_numeric_value_out_of_range, "invalid interval or interval is too large for TIME_WINDOW function");
    }
    const NValue& date = arguments[1];
    if (date.isNull()) {
        return getNullValue(ValueType::tTIMESTAMP);
    }

    int64_t epochMicrosIn = date.getTimestamp();
    if (epochMicrosOutOfRange(epochMicrosIn)) {
        throwOutOfRangeTimestampInput("TIME_WINDOW");
    }
    return getTimestampValue(getQuarterByIntervalWindow(false, epochMicrosIn, interval));
}

static inline int64_t getMonthByIntervalWindow(bool start, int64_t epochMicrosIn, int64_t interval) {
    boost::posix_time::time_duration tsf;
    boost::gregorian::date dsf;

    try {
        micros_to_date_and_time ( epochMicrosIn, dsf, tsf);
        if (dsf.year() < EPOCH_DATE.year()) {
            double x = ( ((EPOCH_DATE.year() - dsf.year()) * 12) - (dsf.month()) ) / (double )interval;
            double y = START_OR_END_OFFSET_BY_INTERVAL(start, true, x, interval);
            boost::gregorian::date d = EPOCH_DATE - boost::gregorian::months ( y);
            return epoch_microseconds_from_components( d.year(), d.month());
        } else {
            double x = ( ((dsf.year() - EPOCH_DATE.year()) * 12) + (dsf.month()) )/ (double )interval;
            double y = START_OR_END_OFFSET_BY_INTERVAL(start, false, x, interval);
            boost::gregorian::date d = EPOCH_DATE + boost::gregorian::months(y);
            return epoch_microseconds_from_components( d.year(), d.month());
        }
    } catch (std::out_of_range &e) {
        throw voltdb::SQLException(voltdb::SQLException::data_exception_numeric_value_out_of_range, "invalid interval or interval is too large for TIME_WINDOW function");
    }
}

template<> inline NValue NValue::call<FUNC_VOLT_TIME_WINDOW_MONTH_START>(const std::vector<NValue>& arguments) {
    vassert(arguments.size() == 2);

    const NValue& number = arguments[0];
    if (number.isNull()) {
        return getNullValue(ValueType::tTIMESTAMP);
    }
    int64_t interval = number.castAsBigIntAndGetValue();
    if (interval <= 0) {
        throw SQLException(SQLException::data_exception_numeric_value_out_of_range, "invalid interval or interval is too large for TIME_WINDOW function");
    }
    const NValue& date = arguments[1];
    if (date.isNull()) {
        return getNullValue(ValueType::tTIMESTAMP);
    }

    int64_t epochMicrosIn = date.getTimestamp();
    if (epochMicrosOutOfRange(epochMicrosIn)) {
        throwOutOfRangeTimestampInput("TIME_WINDOW");
    }

    return getTimestampValue(getMonthByIntervalWindow(true, epochMicrosIn, interval));
}

template<> inline NValue NValue::call<FUNC_VOLT_TIME_WINDOW_MONTH_END>(const std::vector<NValue>& arguments) {
    vassert(arguments.size() == 2);

    const NValue& number = arguments[0];
    if (number.isNull()) {
        return getNullValue(ValueType::tTIMESTAMP);
    }
    int64_t interval = number.castAsBigIntAndGetValue();
    if (interval <= 0) {
        throw SQLException(SQLException::data_exception_numeric_value_out_of_range, "invalid interval or interval is too large for TIME_WINDOW function");
    }
    const NValue& date = arguments[1];
    if (date.isNull()) {
        return getNullValue(ValueType::tTIMESTAMP);
    }

    int64_t epochMicrosIn = date.getTimestamp();
    if (epochMicrosOutOfRange(epochMicrosIn)) {
        throwOutOfRangeTimestampInput("TIME_WINDOW");
    }

    return getTimestampValue(getMonthByIntervalWindow(false, epochMicrosIn, interval));
}

static  int64_t inline getWeekByIntervalWindow(bool start, int64_t epochMicrosIn, int64_t interval) {
    boost::posix_time::time_duration tsf;
    boost::gregorian::date dsf;

    try {
        // We do week calculations based on 12/29/1969 is a Monday just befoe EPOCH
        micros_to_date_and_time ( epochMicrosIn, dsf, tsf);
        if (dsf.year() < EPOCH_DATE.year()) {
            int64_t days = (((EPOCH_DATE - dsf).days())) + 3;
            double x = ((days) / (double )(interval * 7));
            double y = START_OR_END_OFFSET_BY_INTERVAL(start, true, x, (interval * 7));
            boost::gregorian::date d(1969, boost::date_time::Dec, 29);
            d = d - boost::gregorian::days(y);
            return epoch_microseconds_from_components(d.year(), d.month(), d.day());
        } else {
            int64_t days = (((dsf - EPOCH_DATE).days())) + 3;
            double x = ((days) / (double )(interval * 7));
            double y = START_OR_END_OFFSET_BY_INTERVAL(start, false, x, (interval * 7));
            boost::gregorian::date d(1969, boost::date_time::Dec, 29);
            d = d + boost::gregorian::days(y);
            return epoch_microseconds_from_components(d.year(), d.month(), d.day());
        }
    } catch (std::out_of_range &e) {
        throw voltdb::SQLException(voltdb::SQLException::data_exception_numeric_value_out_of_range, "invalid interval or interval is too large for TIME_WINDOW function");
    }

}

template<> inline NValue NValue::call<FUNC_VOLT_TIME_WINDOW_WEEK_START>(const std::vector<NValue>& arguments) {
    vassert(arguments.size() == 2);

    const NValue& number = arguments[0];
    if (number.isNull()) {
        return getNullValue(ValueType::tTIMESTAMP);
    }
    int64_t interval = number.castAsBigIntAndGetValue();
    if (interval <= 0) {
        throw SQLException(SQLException::data_exception_numeric_value_out_of_range, "invalid interval or interval is too large for TIME_WINDOW function");
    }
    const NValue& date = arguments[1];
    if (date.isNull()) {
        return getNullValue(ValueType::tTIMESTAMP);
    }

    int64_t epochMicrosIn = date.getTimestamp();
    if (epochMicrosOutOfRange(epochMicrosIn)) {
        throwOutOfRangeTimestampInput("TIME_WINDOW");
    }
    return getTimestampValue(getWeekByIntervalWindow(true, epochMicrosIn, interval));
}

template<> inline NValue NValue::call<FUNC_VOLT_TIME_WINDOW_WEEK_END>(const std::vector<NValue>& arguments) {
    vassert(arguments.size() == 2);

    const NValue& number = arguments[0];
    if (number.isNull()) {
        return getNullValue(ValueType::tTIMESTAMP);
    }
    int64_t interval = number.castAsBigIntAndGetValue();
    if (interval <= 0) {
        throw SQLException(SQLException::data_exception_numeric_value_out_of_range, "invalid interval or interval is too large for TIME_WINDOW function");
    }
    const NValue& date = arguments[1];
    if (date.isNull()) {
        return getNullValue(ValueType::tTIMESTAMP);
    }

    int64_t epochMicrosIn = date.getTimestamp();
    if (epochMicrosOutOfRange(epochMicrosIn)) {
        throwOutOfRangeTimestampInput("TIME_WINDOW");
    }
    return getTimestampValue(getWeekByIntervalWindow(false, epochMicrosIn, interval));
}

static inline int64_t getDayByIntervalWindow(bool start, int64_t epochMicrosIn, int64_t interval) {
    boost::posix_time::time_duration tsf;
    boost::gregorian::date dsf;

    try {
        micros_to_date_and_time(epochMicrosIn, dsf, tsf);
        if (dsf.year() < EPOCH_DATE.year()) {
            int64_t days = (((EPOCH_DATE - dsf).days()));
            double x = ((days) / (double) interval);
            double y = START_OR_END_OFFSET_BY_INTERVAL(start, true, x, interval);
            boost::gregorian::date d = EPOCH_DATE - boost::gregorian::days(y);
            return epoch_microseconds_from_components(d.year(), d.month(), d.day());
        } else {
            int64_t days = (((dsf - EPOCH_DATE).days()));
            double x = ((days) / (double) interval);
            double y = START_OR_END_OFFSET_BY_INTERVAL(start, false, x, interval);
            boost::gregorian::date d = EPOCH_DATE + boost::gregorian::days(y);
            return epoch_microseconds_from_components(d.year(), d.month(), d.day());
        }
    } catch (std::out_of_range &e) {
        throw voltdb::SQLException(voltdb::SQLException::data_exception_numeric_value_out_of_range, "invalid interval or interval is too large for TIME_WINDOW function");
    }
}

template<> inline NValue NValue::call<FUNC_VOLT_TIME_WINDOW_DAY_START>(const std::vector<NValue>& arguments) {
    vassert(arguments.size() == 2);

    const NValue& number = arguments[0];
    if (number.isNull()) {
        return getNullValue(ValueType::tTIMESTAMP);
    }
    int64_t interval = number.castAsBigIntAndGetValue();
    if (interval <= 0) {
        throw SQLException(SQLException::data_exception_numeric_value_out_of_range, "invalid interval or interval is too large for TIME_WINDOW function");
    }
    const NValue& date = arguments[1];
    if (date.isNull()) {
        return getNullValue(ValueType::tTIMESTAMP);
    }

    int64_t epochMicrosIn = date.getTimestamp();
    if (epochMicrosOutOfRange(epochMicrosIn)) {
        throwOutOfRangeTimestampInput("TIME_WINDOW");
    }
    return getTimestampValue(getDayByIntervalWindow(true, epochMicrosIn, interval));
}

template<> inline NValue NValue::call<FUNC_VOLT_TIME_WINDOW_DAY_END>(const std::vector<NValue>& arguments) {
    vassert(arguments.size() == 2);

    const NValue& number = arguments[0];
    if (number.isNull()) {
        return getNullValue(ValueType::tTIMESTAMP);
    }
    int64_t interval = number.castAsBigIntAndGetValue();
    if (interval <= 0 || interval > PTIME_MAX_DAY_INTERVAL) {
        throw SQLException(SQLException::data_exception_numeric_value_out_of_range, "invalid interval or interval is too large for TIME_WINDOW function");
    }
    const NValue& date = arguments[1];
    if (date.isNull()) {
        return getNullValue(ValueType::tTIMESTAMP);
    }

    int64_t epochMicrosIn = date.getTimestamp();
    if (epochMicrosOutOfRange(epochMicrosIn)) {
        throwOutOfRangeTimestampInput("TIME_WINDOW");
    }
    return getTimestampValue(getDayByIntervalWindow(false, epochMicrosIn, interval));
}

static inline int64_t getHourByIntervalWindow(bool start, int64_t epochMicrosIn, int64_t interval) {
    boost::posix_time::time_duration tsf;
    boost::gregorian::date dsf;

    micros_to_date_and_time ( epochMicrosIn, dsf, tsf);
    if (dsf.year() < EPOCH_DATE.year()) {
        int64_t hours = (((EPOCH_DATE - dsf).days()) * 24) - tsf.hours();
        double x = ((hours) / (double )interval);
        double y = START_OR_END_OFFSET_BY_INTERVAL(start, true, x, interval);
        return EMICROS - (int64_t )y * 60L * 60L * 1000L * 1000L;
    } else {
        int64_t hours = (((dsf - EPOCH_DATE).days()) * 24) + tsf.hours();
        double x = ((hours) / (double )interval);
        double y = START_OR_END_OFFSET_BY_INTERVAL(start, false, x, interval);
        return (int64_t )y * 60L * 60L * 1000L * 1000L;
    }
}

template<> inline NValue NValue::call<FUNC_VOLT_TIME_WINDOW_HOUR_START>(const std::vector<NValue>& arguments) {
    vassert(arguments.size() == 2);

    const NValue& number = arguments[0];
    if (number.isNull()) {
        return getNullValue(ValueType::tTIMESTAMP);
    }
    int64_t interval = number.castAsBigIntAndGetValue();
    if (interval < 0) {
        return getNullValue(ValueType::tTIMESTAMP);
    }
    if (interval <= 0 || interval > PTIME_MAX_HOUR_INTERVAL) {
        throw SQLException(SQLException::data_exception_numeric_value_out_of_range, "invalid interval or interval is too large for TIME_WINDOW function");
    }
    const NValue& date = arguments[1];
    if (date.isNull()) {
        return getNullValue(ValueType::tTIMESTAMP);
    }

    int64_t epochMicrosIn = date.getTimestamp();
    if (epochMicrosOutOfRange(epochMicrosIn)) {
        throwOutOfRangeTimestampInput("TIME_WINDOW");
    }

    return getTimestampValue(getHourByIntervalWindow(true, epochMicrosIn, interval));
}

template<> inline NValue NValue::call<FUNC_VOLT_TIME_WINDOW_HOUR_END>(const std::vector<NValue>& arguments) {
    vassert(arguments.size() == 2);

    const NValue& number = arguments[0];
    if (number.isNull()) {
        return getNullValue(ValueType::tTIMESTAMP);
    }
    int64_t interval = number.castAsBigIntAndGetValue();
    if (interval <= 0 || interval > PTIME_MAX_HOUR_INTERVAL) {
        throw SQLException(SQLException::data_exception_numeric_value_out_of_range, "invalid interval or interval is too large for TIME_WINDOW function");
    }
    const NValue& date = arguments[1];
    if (date.isNull()) {
        return getNullValue(ValueType::tTIMESTAMP);
    }

    int64_t epochMicrosIn = date.getTimestamp();
    if (epochMicrosOutOfRange(epochMicrosIn)) {
        throwOutOfRangeTimestampInput("TIME_WINDOW");
    }

    return getTimestampValue(getHourByIntervalWindow(false, epochMicrosIn, interval));
}

static inline int64_t getMinuteByIntervalWindow(bool start, int64_t epochMicrosIn, int64_t interval) {
    boost::posix_time::time_duration tsf;
    boost::gregorian::date dsf;

    micros_to_date_and_time ( epochMicrosIn, dsf, tsf);
    if (dsf.year() < EPOCH_DATE.year()) {
        int64_t minutes = ( (((EPOCH_DATE - dsf).days()) * 24) - tsf.hours() ) * 60L - tsf.minutes();
        double x = ((minutes) / (double )interval);
        double y = START_OR_END_OFFSET_BY_INTERVAL(start, true, x, interval);
        return EMICROS - (int64_t )y * 60L * 1000L * 1000L;
    } else {
        int64_t minutes = ( (((dsf - EPOCH_DATE).days()) * 24) + tsf.hours() ) * 60L + tsf.minutes();
        double x = ((minutes) / (double )interval);
        double y = START_OR_END_OFFSET_BY_INTERVAL(start, false, x, interval);
        return (int64_t )y * 60L * 1000L * 1000L;
    }
}

template<> inline NValue NValue::call<FUNC_VOLT_TIME_WINDOW_MINUTE_START>(const std::vector<NValue>& arguments) {
    vassert(arguments.size() == 2);

    const NValue& number = arguments[0];
    if (number.isNull()) {
        return getNullValue(ValueType::tTIMESTAMP);
    }
    int64_t interval = number.castAsBigIntAndGetValue();
    if (interval <= 0) {
        throw SQLException(SQLException::data_exception_numeric_value_out_of_range, "invalid interval or interval is too large for TIME_WINDOW function");
    }
    const NValue& date = arguments[1];
    if (date.isNull()) {
        return getNullValue(ValueType::tTIMESTAMP);
    }

    int64_t epochMicrosIn = date.getTimestamp();
    if (epochMicrosOutOfRange(epochMicrosIn)) {
        throwOutOfRangeTimestampInput("TIME_WINDOW");
    }

    return getTimestampValue(getMinuteByIntervalWindow(true, epochMicrosIn, interval));
}

template<> inline NValue NValue::call<FUNC_VOLT_TIME_WINDOW_MINUTE_END>(const std::vector<NValue>& arguments) {
    vassert(arguments.size() == 2);

    const NValue& number = arguments[0];
    if (number.isNull()) {
        return getNullValue(ValueType::tTIMESTAMP);
    }
    int64_t interval = number.castAsBigIntAndGetValue();
    if (interval <= 0) {
        throw SQLException(SQLException::data_exception_numeric_value_out_of_range, "invalid interval or interval is too large for TIME_WINDOW function");
    }
    const NValue& date = arguments[1];
    if (date.isNull()) {
        return getNullValue(ValueType::tTIMESTAMP);
    }

    int64_t epochMicrosIn = date.getTimestamp();
    if (epochMicrosOutOfRange(epochMicrosIn)) {
        throwOutOfRangeTimestampInput("TIME_WINDOW");
    }

    return getTimestampValue(getMinuteByIntervalWindow(false, epochMicrosIn, interval));
}

static inline int64_t getSecondByIntervalWindow(bool start, int64_t epochMicrosIn, int64_t interval) {
    boost::posix_time::time_duration tsf;
    boost::gregorian::date dsf;

    micros_to_date_and_time ( epochMicrosIn, dsf, tsf);
    if (dsf.year() < EPOCH_DATE.year()) {
        int64_t seconds = ( ( (((EPOCH_DATE - dsf).days()) * 24) - tsf.hours() ) * 60L - tsf.minutes()) * 60L - tsf.seconds();
        double x = ((seconds) / (double )interval);
        double y = START_OR_END_OFFSET_BY_INTERVAL(start, true, x, interval);
        return EMICROS - (int64_t )y * 1000L * 1000L;
    } else {
        int64_t minutes = ( ( (((dsf - EPOCH_DATE).days()) * 24) + tsf.hours() ) * 60L + tsf.minutes()) * 60L + tsf.seconds();
        double x = ((minutes) / (double )interval);
        double y = START_OR_END_OFFSET_BY_INTERVAL(start, false, x, interval);
        return (int64_t )y * 1000L * 1000L;
    }
}

template<> inline NValue NValue::call<FUNC_VOLT_TIME_WINDOW_SECOND_START>(const std::vector<NValue>& arguments) {
    vassert(arguments.size() == 2);

    const NValue& number = arguments[0];
    if (number.isNull()) {
        return getNullValue(ValueType::tTIMESTAMP);
    }
    int64_t interval = number.castAsBigIntAndGetValue();
    if (interval <= 0) {
        throw SQLException(SQLException::data_exception_numeric_value_out_of_range, "invalid interval or interval is too large for TIME_WINDOW function");
    }
    const NValue& date = arguments[1];
    if (date.isNull()) {
        return getNullValue(ValueType::tTIMESTAMP);
    }

    int64_t epochMicrosIn = date.getTimestamp();
    if (epochMicrosOutOfRange(epochMicrosIn)) {
        throwOutOfRangeTimestampInput("TIME_WINDOW");
    }

    return getTimestampValue(getSecondByIntervalWindow(true, epochMicrosIn, interval));
}

template<> inline NValue NValue::call<FUNC_VOLT_TIME_WINDOW_SECOND_END>(const std::vector<NValue>& arguments) {
    vassert(arguments.size() == 2);

    const NValue& number = arguments[0];
    if (number.isNull()) {
        return getNullValue(ValueType::tTIMESTAMP);
    }
    int64_t interval = number.castAsBigIntAndGetValue();
    if (interval <= 0) {
        throw SQLException(SQLException::data_exception_numeric_value_out_of_range, "invalid interval or interval is too large for TIME_WINDOW function");
    }
    const NValue& date = arguments[1];
    if (date.isNull()) {
        return getNullValue(ValueType::tTIMESTAMP);
    }

    int64_t epochMicrosIn = date.getTimestamp();
    if (epochMicrosOutOfRange(epochMicrosIn)) {
        throwOutOfRangeTimestampInput("TIME_WINDOW");
    }

    return getTimestampValue(getSecondByIntervalWindow(false, epochMicrosIn, interval));
}

static inline int64_t getMilliByIntervalWindow(bool start, int64_t epochMicrosIn, int64_t interval) {
    boost::posix_time::time_duration tsf;
    boost::gregorian::date dsf;

    micros_to_date_and_time ( epochMicrosIn, dsf, tsf);
    if (dsf.year() < EPOCH_DATE.year()) {
        double x = ((EMICROS - epochMicrosIn)/1000L) / (double )interval;
        double y = START_OR_END_OFFSET_BY_INTERVAL(start, true, x, interval);
        return EMICROS - ((int64_t )y * 1000L);
    } else {
        double x = (((epochMicrosIn-EMICROS)/1000L) / (double )interval);
        double y = START_OR_END_OFFSET_BY_INTERVAL(start, false, x, interval);
        return (int64_t )y * 1000L;
    }
}

template<> inline NValue NValue::call<FUNC_VOLT_TIME_WINDOW_MILLIS_START>(const std::vector<NValue>& arguments) {
    vassert(arguments.size() == 2);

    const NValue& number = arguments[0];
    if (number.isNull()) {
        return getNullValue(ValueType::tTIMESTAMP);
    }
    int64_t interval = number.castAsBigIntAndGetValue();
    if (interval <= 0) {
        throw SQLException(SQLException::data_exception_numeric_value_out_of_range, "invalid interval or interval is too large for TIME_WINDOW function");
    }
    const NValue& date = arguments[1];
    if (date.isNull()) {
        return getNullValue(ValueType::tTIMESTAMP);
    }

    int64_t epochMicrosIn = date.getTimestamp();
    if (epochMicrosOutOfRange(epochMicrosIn)) {
        throwOutOfRangeTimestampInput("TIME_WINDOW");
    }
    return getTimestampValue(getMilliByIntervalWindow(true, epochMicrosIn, interval));
}

template<> inline NValue NValue::call<FUNC_VOLT_TIME_WINDOW_MILLIS_END>(const std::vector<NValue>& arguments) {
    vassert(arguments.size() == 2);

    const NValue& number = arguments[0];
    if (number.isNull()) {
        return getNullValue(ValueType::tTIMESTAMP);
    }
    int64_t interval = number.castAsBigIntAndGetValue();
    if (interval <= 0) {
        throw SQLException(SQLException::data_exception_numeric_value_out_of_range, "invalid interval or interval is too large for TIME_WINDOW function");
    }
    const NValue& date = arguments[1];
    if (date.isNull()) {
        return getNullValue(ValueType::tTIMESTAMP);
    }

    int64_t epochMicrosIn = date.getTimestamp();
    if (epochMicrosOutOfRange(epochMicrosIn)) {
        throwOutOfRangeTimestampInput("TIME_WINDOW");
    }
    return getTimestampValue(getMilliByIntervalWindow(false, epochMicrosIn, interval));
}

template<> inline NValue NValue::call<FUNC_VOLT_DATETIME_DIFF_YEAR>(const std::vector<NValue>& arguments) {
    vassert(arguments.size() == 2);

    const NValue& first = arguments[0];
    if (first.isNull()) {
        return getNullValue(ValueType::tBIGINT);
    }
    const NValue& second = arguments[1];
    if (second.isNull()) {
        return getNullValue(ValueType::tBIGINT);
    }

    boost::posix_time::time_duration tsf;
    boost::gregorian::date dsf;
    micros_to_date_and_time(first.getTimestamp(), dsf, tsf);

    boost::posix_time::time_duration tss;
    boost::gregorian::date dss;
    micros_to_date_and_time(second.getTimestamp(), dss, tss);

    return getBigIntValue(dss.year() - dsf.year());
}

template<> inline NValue NValue::call<FUNC_VOLT_DATETIME_DIFF_QUARTER>(const std::vector<NValue>& arguments) {
    vassert(arguments.size() == 2);

    const NValue& first = arguments[0];
    if (first.isNull()) {
        return getNullValue(ValueType::tBIGINT);
    }
    const NValue& second = arguments[1];
    if (second.isNull()) {
        return getNullValue(ValueType::tBIGINT);
    }
    boost::posix_time::time_duration tsf;
    boost::gregorian::date dsf;
    micros_to_date_and_time(first.getTimestamp(), dsf, tsf);

    boost::posix_time::time_duration tss;
    boost::gregorian::date dss;
    micros_to_date_and_time(second.getTimestamp(), dss, tss);

    int64_t ret_val = (((dss.year() - dsf.year()) * 12) + (dss.month()-dsf.month()))/3;
    return getBigIntValue(ret_val);
}


template<> inline NValue NValue::call<FUNC_VOLT_DATETIME_DIFF_MONTH>(const std::vector<NValue>& arguments) {
    vassert(arguments.size() == 2);

    const NValue& first = arguments[0];
    if (first.isNull()) {
        return getNullValue(ValueType::tBIGINT);
    }
    const NValue& second = arguments[1];
    if (second.isNull()) {
        return getNullValue(ValueType::tBIGINT);
    }
    boost::posix_time::time_duration tsf;
    boost::gregorian::date dsf;
    micros_to_date_and_time(first.getTimestamp(), dsf, tsf);

    boost::posix_time::time_duration tss;
    boost::gregorian::date dss;
    micros_to_date_and_time(second.getTimestamp(), dss, tss);

    int64_t ret_val = ((dss.year() - dsf.year()) * 12) + (dss.month()-dsf.month());
    return getBigIntValue(ret_val);
}

template<> inline NValue NValue::call<FUNC_VOLT_DATETIME_DIFF_WEEK>(const std::vector<NValue>& arguments) {
    vassert(arguments.size() == 2);

    const NValue& first = arguments[0];
    if (first.isNull()) {
        return getNullValue(ValueType::tBIGINT);
    }
    const NValue& second = arguments[1];
    if (second.isNull()) {
        return getNullValue(ValueType::tBIGINT);
    }
    boost::posix_time::time_duration tsf;
    boost::gregorian::date dsf;
    micros_to_date_and_time(first.getTimestamp(), dsf, tsf);

    boost::posix_time::time_duration tss;
    boost::gregorian::date dss;
    micros_to_date_and_time(second.getTimestamp(), dss, tss);

    int64_t ret_val = (dss - dsf).days()/7;
    return getBigIntValue(ret_val);
}

template<> inline NValue NValue::call<FUNC_VOLT_DATETIME_DIFF_DAY>(const std::vector<NValue>& arguments) {
    vassert(arguments.size() == 2);

    const NValue& first = arguments[0];
    if (first.isNull()) {
        return getNullValue(ValueType::tBIGINT);
    }
    boost::posix_time::time_duration tsf;
    boost::gregorian::date dsf;
    micros_to_date_and_time(first.getTimestamp(), dsf, tsf);

    const NValue& second = arguments[1];
    if (second.isNull()) {
        return getNullValue(ValueType::tBIGINT);
    }
    boost::posix_time::time_duration tss;
    boost::gregorian::date dss;
    micros_to_date_and_time(second.getTimestamp(), dss, tss);

    int64_t ret_val = (dss-dsf).days();
    return getBigIntValue(ret_val);
}

template<> inline NValue NValue::call<FUNC_VOLT_DATETIME_DIFF_HOUR>(const std::vector<NValue>& arguments) {
    vassert(arguments.size() == 2);

    const NValue& first = arguments[0];
    if (first.isNull()) {
        return getNullValue(ValueType::tBIGINT);
    }
    boost::posix_time::time_duration tsf;
    boost::gregorian::date dsf;
    micros_to_date_and_time(first.getTimestamp(), dsf, tsf);

    const NValue& second = arguments[1];
    if (second.isNull()) {
        return getNullValue(ValueType::tBIGINT);
    }
    boost::posix_time::time_duration tss;
    boost::gregorian::date dss;
    micros_to_date_and_time(second.getTimestamp(), dss, tss);

    int64_t days = (dss-dsf).days();
    int64_t hours = (tss - tsf).hours();

    int64_t ret_val = (days * 24) + hours;
    return getBigIntValue(ret_val);
}

template<> inline NValue NValue::call<FUNC_VOLT_DATETIME_DIFF_MINUTE>(const std::vector<NValue>& arguments) {
    vassert(arguments.size() == 2);

    const NValue& first = arguments[0];
    if (first.isNull()) {
        return getNullValue(ValueType::tBIGINT);
    }
    boost::posix_time::time_duration tsf;
    boost::gregorian::date dsf;
    micros_to_date_and_time(first.getTimestamp(), dsf, tsf);

    const NValue& second = arguments[1];
    if (second.isNull()) {
        return getNullValue(ValueType::tBIGINT);
    }
    boost::posix_time::time_duration tss;
    boost::gregorian::date dss;
    micros_to_date_and_time(second.getTimestamp(), dss, tss);

    int64_t days = (dss-dsf).days();
    int64_t hours = (tss - tsf).hours();
    int64_t minutes = (tss - tsf).minutes();

    int64_t ret_val = 0;
    ret_val = ((days * 24) + hours) * 60 + minutes;
    return getBigIntValue(ret_val);
}

template<> inline NValue NValue::call<FUNC_VOLT_DATETIME_DIFF_SECOND>(const std::vector<NValue>& arguments) {
    vassert(arguments.size() == 2);

    const NValue& first = arguments[0];
    if (first.isNull()) {
        return getNullValue(ValueType::tBIGINT);
    }
    boost::posix_time::time_duration tsf;
    boost::gregorian::date dsf;
    micros_to_date_and_time(first.getTimestamp(), dsf, tsf);

    const NValue& second = arguments[1];
    if (second.isNull()) {
        return getNullValue(ValueType::tBIGINT);
    }
    boost::posix_time::time_duration tss;
    boost::gregorian::date dss;
    micros_to_date_and_time(second.getTimestamp(), dss, tss);

    int64_t days = (dss-dsf).days();
    int64_t hours = (tss - tsf).hours();
    int64_t minutes = (tss - tsf).minutes();
    int64_t seconds = (tss - tsf).seconds();

    int64_t ret_val = 0;
    ret_val = ( ((days * 24) + hours) * 60 + minutes) * 60 + seconds;
    return getBigIntValue(ret_val);
}

template<> inline NValue NValue::call<FUNC_VOLT_DATETIME_DIFF_MILLIS>(const std::vector<NValue>& arguments) {
    vassert(arguments.size() == 2);

    const NValue& first = arguments[0];
    if (first.isNull()) {
        return getNullValue(ValueType::tBIGINT);
    }

    const NValue& second = arguments[1];
    if (second.isNull()) {
        return getNullValue(ValueType::tBIGINT);
    }
    return getBigIntValue((second.getTimestamp() - first.getTimestamp())/1000);
}

template<> inline NValue NValue::call<FUNC_VOLT_DATETIME_DIFF_MICROS>(const std::vector<NValue>& arguments) {
    vassert(arguments.size() == 2);

    const NValue& first = arguments[0];
    if (first.isNull()) {
        return getNullValue(ValueType::tBIGINT);
    }
    const NValue& second = arguments[1];
    if (second.isNull()) {
        return getNullValue(ValueType::tBIGINT);
    }
    return getBigIntValue((second.getTimestamp() - first.getTimestamp()));
}

}
