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

#include "common/NValue.hpp"
#include "common/StlFriendlyNValue.h"
#include "common/executorcontext.hpp"
#include "expressions/functionexpression.h" // Really for datefunctions and its dependencies.
#include "logging/LogManager.h"

#include <cstdio>
#include <sstream>
#include <algorithm>
#include <set>

namespace voltdb {

Pool* NValue::getTempStringPool() {
    return ExecutorContext::getTempStringPool();
}


// For x<op>y where x is an integer,
// promote x and y to s_intPromotionTable[y]
ValueType NValue::s_intPromotionTable[] = {
    VALUE_TYPE_INVALID,   // 0 invalid
    VALUE_TYPE_NULL,      // 1 null
    VALUE_TYPE_INVALID,   // 2 <unused>
    VALUE_TYPE_BIGINT,    // 3 tinyint
    VALUE_TYPE_BIGINT,    // 4 smallint
    VALUE_TYPE_BIGINT,    // 5 integer
    VALUE_TYPE_BIGINT,    // 6 bigint
    VALUE_TYPE_INVALID,   // 7 <unused>
    VALUE_TYPE_DOUBLE,    // 8 double
    VALUE_TYPE_INVALID,   // 9 varchar
    VALUE_TYPE_INVALID,   // 10 <unused>
    VALUE_TYPE_BIGINT,    // 11 timestamp
    // 12 - 21 unused
    VALUE_TYPE_INVALID, VALUE_TYPE_INVALID, VALUE_TYPE_INVALID, VALUE_TYPE_INVALID,
    VALUE_TYPE_INVALID, VALUE_TYPE_INVALID, VALUE_TYPE_INVALID, VALUE_TYPE_INVALID,
    VALUE_TYPE_INVALID, VALUE_TYPE_INVALID,
    VALUE_TYPE_DECIMAL,   // 22 decimal
    VALUE_TYPE_INVALID,   // 23 boolean
    VALUE_TYPE_INVALID,   // 24 address
};

// for x<op>y where x is a double
// promote x and y to s_doublePromotionTable[y]
ValueType NValue::s_doublePromotionTable[] = {
    VALUE_TYPE_INVALID,   // 0 invalid
    VALUE_TYPE_NULL,      // 1 null
    VALUE_TYPE_INVALID,   // 2 <unused>
    VALUE_TYPE_DOUBLE,    // 3 tinyint
    VALUE_TYPE_DOUBLE,    // 4 smallint
    VALUE_TYPE_DOUBLE,    // 5 integer
    VALUE_TYPE_DOUBLE,    // 6 bigint
    VALUE_TYPE_INVALID,   // 7 <unused>
    VALUE_TYPE_DOUBLE,    // 8 double
    VALUE_TYPE_INVALID,   // 9 varchar
    VALUE_TYPE_INVALID,   // 10 <unused>
    VALUE_TYPE_DOUBLE,    // 11 timestamp
    // 12 - 21 unused.
    VALUE_TYPE_INVALID, VALUE_TYPE_INVALID, VALUE_TYPE_INVALID, VALUE_TYPE_INVALID,
    VALUE_TYPE_INVALID, VALUE_TYPE_INVALID, VALUE_TYPE_INVALID, VALUE_TYPE_INVALID,
    VALUE_TYPE_INVALID, VALUE_TYPE_INVALID,
    VALUE_TYPE_INVALID,   // 22 decimal  (todo)
    VALUE_TYPE_INVALID,   // 23 boolean
    VALUE_TYPE_INVALID,   // 24 address
};

// for x<op>y where x is a decimal
// promote x and y to s_decimalPromotionTable[y]
ValueType NValue::s_decimalPromotionTable[] = {
    VALUE_TYPE_INVALID,   // 0 invalid
    VALUE_TYPE_NULL,      // 1 null
    VALUE_TYPE_INVALID,   // 2 <unused>
    VALUE_TYPE_DECIMAL,   // 3 tinyint
    VALUE_TYPE_DECIMAL,   // 4 smallint
    VALUE_TYPE_DECIMAL,   // 5 integer
    VALUE_TYPE_DECIMAL,   // 6 bigint
    VALUE_TYPE_INVALID,   // 7 <unused>
    VALUE_TYPE_INVALID,   // 8 double (todo)
    VALUE_TYPE_INVALID,   // 9 varchar
    VALUE_TYPE_INVALID,   // 10 <unused>
    VALUE_TYPE_DECIMAL,   // 11 timestamp
    // 12 - 21 unused. ick.
    VALUE_TYPE_INVALID, VALUE_TYPE_INVALID, VALUE_TYPE_INVALID, VALUE_TYPE_INVALID,
    VALUE_TYPE_INVALID, VALUE_TYPE_INVALID, VALUE_TYPE_INVALID, VALUE_TYPE_INVALID,
    VALUE_TYPE_INVALID, VALUE_TYPE_INVALID,
    VALUE_TYPE_DECIMAL,   // 22 decimal
    VALUE_TYPE_INVALID,   // 23 boolean
    VALUE_TYPE_INVALID,   // 24 address
};

TTInt NValue::s_maxDecimalValue("9999999999"   //10 digits
                                "9999999999"   //20 digits
                                "9999999999"   //30 digits
                                "99999999");    //38 digits

TTInt NValue::s_minDecimalValue("-9999999999"   //10 digits
                                 "9999999999"   //20 digits
                                 "9999999999"   //30 digits
                                 "99999999");    //38 digits

const double NValue::s_gtMaxDecimalAsDouble = 1E26;
const double NValue::s_ltMinDecimalAsDouble = -1E26;

TTInt NValue::s_maxInt64AsDecimal(TTInt(INT64_MAX) * kMaxScaleFactor);
TTInt NValue::s_minInt64AsDecimal(TTInt(-INT64_MAX) * kMaxScaleFactor);

/*
 * Produce a debugging string describing an NValue.
 */
std::string NValue::debug() const {
    const ValueType type = getValueType();
    if (isNull()) {
        return "<NULL>";
    }
    std::ostringstream buffer;
    std::string out_val;
    const char* ptr;
    int64_t addr;
    buffer << getTypeName(type) << "::";
    switch (type) {
    case VALUE_TYPE_BOOLEAN:
        buffer << (getBoolean() ? "true" : "false");
        break;
    case VALUE_TYPE_TINYINT:
        buffer << static_cast<int32_t>(getTinyInt());
        break;
    case VALUE_TYPE_SMALLINT:
        buffer << getSmallInt();
        break;
    case VALUE_TYPE_INTEGER:
        buffer << getInteger();
        break;
    case VALUE_TYPE_BIGINT:
    case VALUE_TYPE_TIMESTAMP:
        buffer << getBigInt();
        break;
    case VALUE_TYPE_DOUBLE:
        buffer << getDouble();
        break;
    case VALUE_TYPE_VARCHAR:
        ptr = reinterpret_cast<const char*>(getObjectValue_withoutNull());
        addr = reinterpret_cast<int64_t>(ptr);
        out_val = std::string(ptr, getObjectLength_withoutNull());
        buffer << "[" << getObjectLength_withoutNull() << "]";
        buffer << "\"" << out_val << "\"[@" << addr << "]";
        break;
    case VALUE_TYPE_VARBINARY:
        ptr = reinterpret_cast<const char*>(getObjectValue_withoutNull());
        addr = reinterpret_cast<int64_t>(ptr);
        out_val = std::string(ptr, getObjectLength_withoutNull());
        buffer << "[" << getObjectLength_withoutNull() << "]";
        buffer << "-bin[@" << addr << "]";
        break;
    case VALUE_TYPE_DECIMAL:
        buffer << createStringFromDecimal();
        break;
    default:
        buffer << "(no details)";
        break;
    }
    std::string ret(buffer.str());
    return (ret);
}


/**
 * Serialize sign and value using radix point (no exponent).
 */
std::string NValue::createStringFromDecimal() const {
    assert(!isNull());
    std::ostringstream buffer;
    TTInt scaledValue = getDecimal();
    if (scaledValue.IsSign()) {
        buffer << '-';
    }
    TTInt whole(scaledValue);
    TTInt fractional(scaledValue);
    whole /= NValue::kMaxScaleFactor;
    fractional %= NValue::kMaxScaleFactor;
    if (whole.IsSign()) {
        whole.ChangeSign();
    }
    buffer << whole.ToString(10);
    buffer << '.';
    if (fractional.IsSign()) {
        fractional.ChangeSign();
    }
    std::string fractionalString = fractional.ToString(10);
    for (int ii = static_cast<int>(fractionalString.size()); ii < NValue::kMaxDecScale; ii++) {
        buffer << '0';
    }
    buffer << fractionalString;
    return buffer.str();
}

/**
 *   set a decimal value from a serialized representation
 */
void NValue::createDecimalFromString(const std::string &txt) {
    if (txt.length() == 0) {
        throw SQLException(SQLException::volt_decimal_serialization_error,
                                       "Empty string provided");
    }
    bool setSign = false;
    if (txt[0] == '-') {
        setSign = true;
    }

    /**
     * Check for invalid characters
     */
    for (int ii = (setSign ? 1 : 0); ii < static_cast<int>(txt.size()); ii++) {
        if ((txt[ii] < '0' || txt[ii] > '9') && txt[ii] != '.') {
            char message[4096];
            snprintf(message, 4096, "Invalid characters in decimal string: %s",
                     txt.c_str());
            throw SQLException(SQLException::volt_decimal_serialization_error,
                               message);
        }
    }

    std::size_t separatorPos = txt.find( '.', 0);
    if (separatorPos == std::string::npos) {
        const std::string wholeString = txt.substr( setSign ? 1 : 0, txt.size());
        const std::size_t wholeStringSize = wholeString.size();
        if (wholeStringSize > 26) {
            throw SQLException(SQLException::volt_decimal_serialization_error,
                               "Maximum precision exceeded. Maximum of 26 digits to the left of the decimal point");
        }
        TTInt whole(wholeString);
        if (setSign) {
            whole.SetSign();
        }
        whole *= kMaxScaleFactor;
        getDecimal() = whole;
        return;
    }

    if (txt.find( '.', separatorPos + 1) != std::string::npos) {
        throw SQLException(SQLException::volt_decimal_serialization_error,
                           "Too many decimal points");
    }

    const std::string wholeString = txt.substr( setSign ? 1 : 0, separatorPos - (setSign ? 1 : 0));
    const std::size_t wholeStringSize = wholeString.size();
    if (wholeStringSize > 26) {
        throw SQLException(SQLException::volt_decimal_serialization_error,
                           "Maximum precision exceeded. Maximum of 26 digits to the left of the decimal point");
    }
    TTInt whole(wholeString);
    std::string fractionalString = txt.substr( separatorPos + 1, txt.size() - (separatorPos + 1));
    // remove trailing zeros
    while (fractionalString.size() > 0 && fractionalString[fractionalString.size() - 1] == '0')
        fractionalString.erase(fractionalString.size() - 1, 1);
    // check if too many decimal places
    if (fractionalString.size() > 12) {
        throw SQLException(SQLException::volt_decimal_serialization_error,
                           "Maximum scale exceeded. Maximum of 12 digits to the right of the decimal point");
    }
    while(fractionalString.size() < NValue::kMaxDecScale) {
        fractionalString.push_back('0');
    }
    TTInt fractional(fractionalString);

    whole *= kMaxScaleFactor;
    whole += fractional;

    if (setSign) {
        whole.SetSign();
    }

    getDecimal() = whole;
}

struct NValueList {
    static int allocationSizeForLength(size_t length)
    {
        //TODO: May want to consider extra allocation, here,
        // such as space for a sorted copy of the array.
        // This allocation has the advantage of getting freed via NValue::free.
        return (int)(sizeof(NValueList) + length*sizeof(StlFriendlyNValue));
    }

    void* operator new(size_t size, char* placement)
    {
        return placement;
    }
    void operator delete(void*, char*) {}
    void operator delete(void*) {}

    NValueList(size_t length, ValueType elementType) : m_length(length), m_elementType(elementType)
    { }

    void deserializeNValues(SerializeInputBE &input, Pool *dataPool)
    {
        for (int ii = 0; ii < m_length; ++ii) {
            m_values[ii].deserializeFromAllocateForStorage(m_elementType, input, dataPool);
        }
    }

    StlFriendlyNValue const* begin() const { return m_values; }
    StlFriendlyNValue const* end() const { return m_values + m_length; }

    const size_t m_length;
    const ValueType m_elementType;
    StlFriendlyNValue m_values[0];
};

/**
 * This NValue can be of any scalar value type.
 * @param rhs  a VALUE_TYPE_ARRAY NValue whose referent must be an NValueList.
 *             The NValue elements of the NValueList should be comparable to and ideally
 *             of exactly the same VALUE_TYPE as "this".
 * The planner and/or deserializer should have taken care of this with checks and
 * explicit cast operators and and/or constant promotions as needed.
 * @return a VALUE_TYPE_BOOLEAN NValue.
 */
bool NValue::inList(const NValue& rhs) const
{
    //TODO: research: does the SQL standard allow a null to match a null list element
    // vs. returning FALSE or NULL?
    const bool lhsIsNull = isNull();
    if (lhsIsNull) {
        return false;
    }

    const ValueType rhsType = rhs.getValueType();
    if (rhsType != VALUE_TYPE_ARRAY) {
        throwDynamicSQLException("rhs of IN expression is of a non-list type %s", rhs.getValueTypeString().c_str());
    }
    const NValueList* listOfNValues = (NValueList*)rhs.getObjectValue_withoutNull();
    const StlFriendlyNValue& value = *static_cast<const StlFriendlyNValue*>(this);
    //TODO: An O(ln(length)) implementation vs. the current O(length) implementation
    // such as binary search would likely require some kind of sorting/re-org of values
    // post-update/pre-lookup, and would likely require some sortable inequality method
    // (operator<()?) to be defined on StlFriendlyNValue.
    return std::find(listOfNValues->begin(), listOfNValues->end(), value) != listOfNValues->end();
}

void NValue::deserializeIntoANewNValueList(SerializeInputBE &input, Pool *dataPool)
{
    ValueType elementType = (ValueType)input.readByte();
    size_t length = input.readShort();
    int trueSize = NValueList::allocationSizeForLength(length);
    char* storage = allocateValueStorage(trueSize, dataPool);
    ::memset(storage, 0, trueSize);
    NValueList* nvset = new (storage) NValueList(length, elementType);
    nvset->deserializeNValues(input, dataPool);
    //TODO: An O(ln(length)) implementation vs. the current O(length) implementation of NValue::inList
    // would likely require some kind of sorting/re-org of values at this point post-update pre-lookup.
}

void NValue::allocateANewNValueList(size_t length, ValueType elementType)
{
    int trueSize = NValueList::allocationSizeForLength(length);
    char* storage = allocateValueStorage(trueSize, NULL);
    ::memset(storage, 0, trueSize);
    new (storage) NValueList(length, elementType);
}

void NValue::setArrayElements(std::vector<NValue> &args) const
{
    assert(m_valueType == VALUE_TYPE_ARRAY);
    NValueList* listOfNValues = (NValueList*)getObjectValue();
    // Assign each of the elements.
    int ii = (int)args.size();
    assert(ii == listOfNValues->m_length);
    while (ii--) {
        listOfNValues->m_values[ii] = args[ii];
    }
    //TODO: An O(ln(length)) implementation vs. the current O(length) implementation of NValue::inList
    // would likely require some kind of sorting/re-org of values at this point post-update pre-lookup.
}

int NValue::arrayLength() const
{
    assert(m_valueType == VALUE_TYPE_ARRAY);
    NValueList* listOfNValues = (NValueList*)getObjectValue();
    return static_cast<int>(listOfNValues->m_length);
}

NValue NValue::itemAtIndex(int index) const
{
    assert(m_valueType == VALUE_TYPE_ARRAY);
    NValueList* listOfNValues = (NValueList*)getObjectValue();
    assert(index >= 0);
    assert(index < listOfNValues->m_length);
    return listOfNValues->m_values[index];
}

void NValue::castAndSortAndDedupArrayForInList(const ValueType outputType, std::vector<NValue> &outList) const
{
    int size = arrayLength();

    // make a set to eliminate unique values in O(nlogn) time
    std::set<StlFriendlyNValue> uniques;

    // iterate over the array of values and build a sorted set of unique
    // values that don't overflow or violate unique constaints
    // (n.b. sorted set means dups are removed)
    for (int i = 0; i < size; i++) {
        NValue value = itemAtIndex(i);
        // cast the value to the right type and catch overflow/cast problems
        try {
            StlFriendlyNValue stlValue;
            stlValue = value.castAs(outputType);
            std::pair<std::set<StlFriendlyNValue>::iterator, bool> ret;
            ret = uniques.insert(stlValue);
        }
        // cast exceptions mean the in-list test is redundant
        // don't include these values in the materialized table
        // TODO: make this less hacky
        catch (SQLException &sqlException) {}
    }

    // insert all items in the set in order
    std::set<StlFriendlyNValue>::const_iterator iter;
    for (iter = uniques.begin(); iter != uniques.end(); iter++) {
        outList.push_back(*iter);
    }
}

void NValue::streamTimestamp(std::stringstream& value) const
{
    int64_t epoch_micros = getTimestamp();
    boost::gregorian::date as_date;
    boost::posix_time::time_duration as_time;
    micros_to_date_and_time(epoch_micros, as_date, as_time);

    long micro = epoch_micros % 1000000;
    if (epoch_micros < 0 && micro < 0) {
        // deal with negative micros (for dates before 1970)
        // by taking back the 1 whole second that was rounded down from the formatted date/time
        // and converting it to 1000000 micros
        micro += 1000000;
    }
    char mbstr[27];    // Format: "YYYY-MM-DD HH:MM:SS."- 20 characters + terminator
    snprintf(mbstr, sizeof(mbstr), "%04d-%02d-%02d %02d:%02d:%02d.%06d",
             (int)as_date.year(), (int)as_date.month(), (int)as_date.day(),
             (int)as_time.hours(), (int)as_time.minutes(), (int)as_time.seconds(), (int)micro);
    value << mbstr;
}

inline static void throwTimestampFormatError(const std::string &str)
{
    char message[4096];
    // No space separator for between the date and time
    snprintf(message, 4096, "Attempted to cast \'%s\' to type %s failed. Supported format: \'YYYY-MM-DD HH:MM:SS.UUUUUU\'"
             "or \'YYYY-MM-DD\'",
             str.c_str(), valueToString(VALUE_TYPE_TIMESTAMP).c_str());
    throw SQLException(SQLException::dynamic_sql_error, message);
}

int64_t NValue::parseTimestampString(const std::string &str)
{
    // date_str
    std::string date_str(str);
    // This is the std:string API for "ltrim" and "rtrim".
    date_str.erase(date_str.begin(), std::find_if(date_str.begin(), date_str.end(), std::not1(std::ptr_fun<int, int>(std::isspace))));
    date_str.erase(std::find_if(date_str.rbegin(), date_str.rend(), std::not1(std::ptr_fun<int, int>(std::isspace))).base(), date_str.end());
    std::size_t sep_pos;


    int year = 0;
    int month = 0;
    int day = 0;
    int hour = 0;
    int minute = 0;
    int second = 0;
    int micro = 1000000;
    // time_str
    std::string time_str;
    std::string number_string;
    const char * pch;

    switch (date_str.size()) {
    case 26:
        sep_pos  = date_str.find(' ');
        if (sep_pos != 10) {
            throwTimestampFormatError(str);
        }

        time_str = date_str.substr(sep_pos + 1);
        // This is the std:string API for "ltrim"
        time_str.erase(time_str.begin(), std::find_if(time_str.begin(), time_str.end(), std::not1(std::ptr_fun<int, int>(std::isspace))));
        if (time_str.length() != 15) {
            throwTimestampFormatError(str);
        }

        // tokenize time_str: HH:MM:SS.mmmmmm
        if (time_str.at(2) != ':' || time_str.at(5) != ':' || time_str.at(8) != '.') {
            throwTimestampFormatError(str);
        }

        // HH
        number_string = time_str.substr(0,2);
        pch = number_string.c_str();
        if (pch[0] == '0') {
            hour = 0;
        } else if (pch[0] == '1') {
            hour = 10;
        } else if (pch[0] == '2') {
            hour = 20;
        } else {
            throwTimestampFormatError(str);
        }
        if (pch[1] > '9' || pch[1] < '0') {
            throwTimestampFormatError(str);
        }
        hour += pch[1] - '0';
        if (hour > 23 || hour < 0) {
            throwTimestampFormatError(str);
        }

        // MM
        number_string = time_str.substr(3,2);
        pch = number_string.c_str();
        if (pch[0] > '5' || pch[0] < '0') {
            throwTimestampFormatError(str);
        }
        minute = 10*(pch[0] - '0');
        if (pch[1] > '9' || pch[1] < '0') {
            throwTimestampFormatError(str);
        }
        minute += pch[1] - '0';
        if (minute > 59 || minute < 0) {
            throwTimestampFormatError(str);
        }

        // SS
        number_string = time_str.substr(6,2);
        pch = number_string.c_str();
        if (pch[0] > '5' || pch[0] < '0') {
            throwTimestampFormatError(str);
        }
        second = 10*(pch[0] - '0');
        if (pch[1] > '9' || pch[1] < '0') {
            throwTimestampFormatError(str);
        }
        second += pch[1] - '0';
        if (second > 59 || second < 0) {
            throwTimestampFormatError(str);
        }
        // hack a '1' in the place if the decimal and use atoi to get a value that
        // MUST be between 1 and 2 million if all 6 digits of micros were included.
        number_string = time_str.substr(8,7);
        number_string.at(0) = '1';
        pch = number_string.c_str();
        micro = atoi(pch);
        if (micro >= 2000000 || micro < 1000000) {
            throwTimestampFormatError(str);
        }
    case 10:
        if (date_str.at(4) != '-' || date_str.at(7) != '-') {
            throwTimestampFormatError(str);
        }

        number_string = date_str.substr(0,4);
        pch = number_string.c_str();

        // YYYY
        year = atoi(pch);
        // new years day 10000 is likely to cause problems.
        // There's a boost library limitation against years before 1400.
        if (year > 9999 || year < 1400) {
            throwTimestampFormatError(str);
        }

        // MM
        number_string = date_str.substr(5,2);
        pch = number_string.c_str();
        if (pch[0] == '0') {
            month = 0;
        } else if (pch[0] == '1') {
            month = 10;
        } else {
            throwTimestampFormatError(str);
        }
        if (pch[1] > '9' || pch[1] < '0') {
            throwTimestampFormatError(str);
        }
        month += pch[1] - '0';
        if (month > 12 || month < 1) {
            throwTimestampFormatError(str);
        }

        // DD
        number_string = date_str.substr(8,2);
        pch = number_string.c_str();
        if (pch[0] == '0') {
            day = 0;
        } else if (pch[0] == '1') {
            day = 10;
        } else if (pch[0] == '2') {
            day = 20;
        } else if (pch[0] == '3') {
            day = 30;
        } else {
            throwTimestampFormatError(str);
        }
        if (pch[1] > '9' || pch[1] < '0') {
            throwTimestampFormatError(str);
        }
        day += pch[1] - '0';
        if (day > 31 || day < 1) {
            throwTimestampFormatError(str);
        }
        break;
    default:
        throwTimestampFormatError(str);
    }

    int64_t result = 0;
    try {
        result = epoch_microseconds_from_components(
            (unsigned short int)year, (unsigned short int)month, (unsigned short int)day,
            hour, minute, second);
    } catch (const std::out_of_range& bad) {
        throwTimestampFormatError(str);
    }
    result += (micro - 1000000);
    return result;
}


int warn_if(int condition, const char* message)
{
    if (condition) {
        LogManager::getThreadLogger(LOGGERID_HOST)->log(LOGLEVEL_WARN, message);
    }
    return condition;
}

};
