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

#include "boost/date_time/gregorian/greg_date.hpp"
#include "boost/date_time/posix_time/posix_time_types.hpp"
#include "boost/date_time/posix_time/posix_time_duration.hpp"
#include "boost/date_time/posix_time/conversion.hpp"
#include <ctime>

static inline boost::gregorian::date date_from_epoch_micros(int64_t epoch_micros) {
    time_t epoch_seconds = epoch_micros / 1000000;
    return boost::posix_time::from_time_t(epoch_seconds).date();
}

static inline boost::posix_time::time_duration time_of_day_from_epoch_micros(int64_t epoch_micros) {
    time_t epoch_seconds = epoch_micros / 1000000;
    return boost::posix_time::from_time_t(epoch_seconds).time_of_day();
}

namespace voltdb {

/** implement the timestamp YEAR extract function **/
template<> inline NValue NValue::callUnary<FUNC_EXTRACT_YEAR>() const {
    if (isNull()) {
        return *this;
    }
    int64_t epoch_micros = getTimestamp();
    boost::gregorian::date as_date = date_from_epoch_micros(epoch_micros);
    return getIntegerValue(as_date.year());
}

/** implement the timestamp MONTH extract function **/
template<> inline NValue NValue::callUnary<FUNC_EXTRACT_MONTH>() const {
    if (isNull()) {
        return *this;
    }
    int64_t epoch_micros = getTimestamp();
    boost::gregorian::date as_date = date_from_epoch_micros(epoch_micros);
    return getTinyIntValue((int8_t)as_date.month());
}

/** implement the timestamp DAY extract function **/
template<> inline NValue NValue::callUnary<FUNC_EXTRACT_DAY>() const {
    if (isNull()) {
        return *this;
    }
    int64_t epoch_micros = getTimestamp();
    boost::gregorian::date as_date = date_from_epoch_micros(epoch_micros);
    return getTinyIntValue((int8_t)as_date.day());
}

/** implement the timestamp DAY OF WEEK extract function **/
template<> inline NValue NValue::callUnary<FUNC_EXTRACT_DAY_OF_WEEK>() const {
    if (isNull()) {
        return *this;
    }
    int64_t epoch_micros = getTimestamp();
    boost::gregorian::date as_date = date_from_epoch_micros(epoch_micros);
    return getTinyIntValue((int8_t)(as_date.day_of_week() + 1)); // Have 0-based, want 1-based.
}

/** implement the timestamp WEEK OF YEAR extract function **/
template<> inline NValue NValue::callUnary<FUNC_EXTRACT_WEEK_OF_YEAR>() const {
    if (isNull()) {
        return *this;
    }
    int64_t epoch_micros = getTimestamp();
    boost::gregorian::date as_date = date_from_epoch_micros(epoch_micros);
    return getTinyIntValue((int8_t)as_date.week_number());
}

/** implement the timestamp DAY OF YEAR extract function **/
template<> inline NValue NValue::callUnary<FUNC_EXTRACT_DAY_OF_YEAR>() const {
    if (isNull()) {
        return *this;
    }
    int64_t epoch_micros = getTimestamp();
    boost::gregorian::date as_date = date_from_epoch_micros(epoch_micros);
    return getSmallIntValue((int16_t)as_date.day_of_year());
}

/** implement the timestamp QUARTER extract function **/
template<> inline NValue NValue::callUnary<FUNC_EXTRACT_QUARTER>() const {
    if (isNull()) {
        return *this;
    }
    int64_t epoch_micros = getTimestamp();
    boost::gregorian::date as_date = date_from_epoch_micros(epoch_micros);
    return getTinyIntValue((int8_t)((as_date.month() + 2) / 3));
}

/** implement the timestamp HOUR extract function **/
template<> inline NValue NValue::callUnary<FUNC_EXTRACT_HOUR>() const {
    if (isNull()) {
        return *this;
    }
    int64_t epoch_micros = getTimestamp();
    // time_of_day manages the wrap at 24 hours
    boost::posix_time::time_duration as_time = time_of_day_from_epoch_micros(epoch_micros);
    return getTinyIntValue((int8_t)as_time.hours());
}

/** implement the timestamp MINUTE extract function **/
template<> inline NValue NValue::callUnary<FUNC_EXTRACT_MINUTE>() const {
    if (isNull()) {
        return *this;
    }
    int64_t epoch_micros = getTimestamp();
    boost::posix_time::time_duration as_time = time_of_day_from_epoch_micros(epoch_micros);
    return getTinyIntValue((int8_t)(as_time.minutes()));
}

/** implement the timestamp SECOND extract function **/
template<> inline NValue NValue::callUnary<FUNC_EXTRACT_SECOND>() const {
    if (isNull()) {
        return *this;
    }
    int64_t epoch_micros = getTimestamp();
    boost::posix_time::time_duration as_time = time_of_day_from_epoch_micros(epoch_micros);
    int fraction = static_cast<int>(epoch_micros % 1000000);
    int second = as_time.seconds();
    if (epoch_micros < 0 && fraction != 0) {
        second -= 1;
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
    int64_t seconds = getBigInt();
    //printf("input Seconds from Java: %lld\n", seconds);
    int64_t epoch_micros = seconds * 1000000;
    return getTimestampValue(epoch_micros);
}

/** implement the timestamp TO_TIMESTAMP from MILLISECONDs function **/
template<> inline NValue NValue::callUnary<FUNC_TO_TIMESTAMP_MILLISECOND>() const {
    if (isNull()) {
        return *this;
    }
    int64_t millis = getBigInt();
    int64_t epoch_micros = millis * 1000;
    return getTimestampValue(epoch_micros);
}

/** implement the timestamp TO_TIMESTAMP from MICROSECONDs function **/
template<> inline NValue NValue::callUnary<FUNC_TO_TIMESTAMP_MICROSECOND>() const {
    if (isNull()) {
        return *this;
    }
    int64_t epoch_micros = getBigInt();
    return getTimestampValue(epoch_micros);
}

}
