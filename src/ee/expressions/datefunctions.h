/* This file is part of VoltDB.
 * Copyright (C) 2008-2012 VoltDB Inc.
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
    // divide by 60 million micros, and wrap at 60.
    return getTinyIntValue((int8_t)((epoch_micros / 60000000) % 60));
}

/** implement the timestamp SECOND extract function **/
template<> inline NValue NValue::callUnary<FUNC_EXTRACT_SECOND>() const {
    if (isNull()) {
        return *this;
    }
    int64_t epoch_micros = getTimestamp();
    TTInt retval(epoch_micros % 60000000);
    retval *= NValue::kMaxScaleFactor/1000000;
    return getDecimalValue(retval);
}

}
