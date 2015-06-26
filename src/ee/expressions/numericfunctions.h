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

namespace voltdb {

/** implement the SQL ABS (absolute value) function for all numeric types */
template<> inline NValue NValue::callUnary<FUNC_ABS>() const {
    if (isNull()) {
        return *this;
    }
    const ValueType type = getValueType();
    NValue retval(type);
    switch(type) {
    /*abs() in C++ returns int (32-bits) if input is int, and long (64-bit) if input is long.
      VoltDB INTEGER is 32-bit, BIGINT is 64-bit, so for TINYINT (8-bit) and SMALLINT (16-bit),
      we need to cast.*/
    case VALUE_TYPE_TINYINT:
        retval.getTinyInt() = static_cast<int8_t>(std::abs(getTinyInt())); break;
    case VALUE_TYPE_SMALLINT:
        retval.getSmallInt() = static_cast<int16_t>(std::abs(getSmallInt())); break;
    case VALUE_TYPE_INTEGER:
        retval.getInteger() = std::abs(getInteger()); break;
    case VALUE_TYPE_BIGINT:
        retval.getBigInt() = std::abs(getBigInt()); break;
    case VALUE_TYPE_DOUBLE:
        retval.getDouble() = std::abs(getDouble()); break;
    case VALUE_TYPE_DECIMAL: {
        retval.getDecimal() = getDecimal();
        retval.getDecimal().Abs(); // updates in place!
    }
    break;
    case VALUE_TYPE_TIMESTAMP:
    default:
        throwCastSQLException (type, VALUE_TYPE_FOR_DIAGNOSTICS_ONLY_NUMERIC);
        break;
    }
    return retval;
}

/** implement the SQL FLOOR function for all numeric values */
template<> inline NValue NValue::callUnary<FUNC_FLOOR>() const {
    if (isNull()) {
        return *this;
    }
    const ValueType type = getValueType();
    NValue retval(type);
    switch(type) {

    case VALUE_TYPE_TINYINT:
    case VALUE_TYPE_SMALLINT:
    case VALUE_TYPE_INTEGER:
    case VALUE_TYPE_BIGINT:
        return *this;

    /*floor() in C++ returns double (64-bits) if input is double, float (32-bit) if input is float,
      and long double (128-bit) if input is long double (128-bit).*/
    case VALUE_TYPE_DOUBLE:
        retval.getDouble() = std::floor(getDouble());
        break;
    case VALUE_TYPE_DECIMAL: {
        TTInt scaledValue = getDecimal();
        TTInt fractional(scaledValue);
        fractional %= NValue::kMaxScaleFactor;
        if (fractional == 0) {
            return *this;
        }

        TTInt whole(scaledValue);
        whole /= NValue::kMaxScaleFactor;
        if (scaledValue.IsSign()) {
            //whole has the sign at this point.
            whole--;
        }
        whole *= NValue::kMaxScaleFactor;
        retval.getDecimal() = whole;
    }
    break;
    default:
        throwCastSQLException (type, VALUE_TYPE_FOR_DIAGNOSTICS_ONLY_NUMERIC);
        break;
    }
    return retval;
}


/** implement the SQL CEIL function for all numeric values */
template<> inline NValue NValue::callUnary<FUNC_CEILING>() const {
    if (isNull()) {
        return *this;
    }
    const ValueType type = getValueType();
    NValue retval(type);
    switch(type) {

    case VALUE_TYPE_TINYINT:
    case VALUE_TYPE_SMALLINT:
    case VALUE_TYPE_INTEGER:
    case VALUE_TYPE_BIGINT:
        return *this;

    /*ceil() in C++ returns double (64-bits) if input is double, float (32-bit) if input is float,
      and long double (128-bit) if input is long double (128-bit). VoltDB INTEGER is 32-bit, BIGINT is 64-bit,
      so for TINYINT (8-bit) and SMALLINT (16-bit), we need to cast.*/

    case VALUE_TYPE_DOUBLE:
        retval.getDouble() = std::ceil(getDouble());
        break;
    case VALUE_TYPE_DECIMAL: {
        TTInt scaledValue = getDecimal();
        TTInt fractional(scaledValue);
        fractional %= NValue::kMaxScaleFactor;
        if (fractional == 0) {
            return *this;
        }

        TTInt whole(scaledValue);
        whole /= NValue::kMaxScaleFactor;
        if (!scaledValue.IsSign()) {
            whole++;
        }
        whole *= NValue::kMaxScaleFactor;
        retval.getDecimal() = whole;
    }
    break;
    default:
        throwCastSQLException (type, VALUE_TYPE_FOR_DIAGNOSTICS_ONLY_NUMERIC);
        break;
    }
    return retval;
}



/** implement the SQL SQRT function for all numeric values */
template<> inline NValue NValue::callUnary<FUNC_SQRT>() const {
    if (isNull()) {
        return *this;
    }
    NValue retval(VALUE_TYPE_DOUBLE);
    /*sqrt() in C++ returns double (64-bits) if input is double, float (32-bit) if input is float,
      and long double (128-bit) if input is long double (128-bit).*/
    double inputValue = castAsDoubleAndGetValue();
    double resultDouble = std::sqrt(inputValue);
    throwDataExceptionIfInfiniteOrNaN(resultDouble, "function SQRT");
    retval.getDouble() = resultDouble;
    return retval;
}


/** implement the SQL EXP function for all numeric values */
template<> inline NValue NValue::callUnary<FUNC_EXP>() const {
    if (isNull()) {
        return *this;
    }
    NValue retval(VALUE_TYPE_DOUBLE);
    //exp() in C++ returns double (64-bits) if input is double, float (32-bit) if input is float,
    //and long double (128-bit) if input is long double (128-bit).
    double exponentValue = castAsDoubleAndGetValue();
    double resultDouble = std::exp(exponentValue);
    throwDataExceptionIfInfiniteOrNaN(resultDouble, "function EXP");
    retval.getDouble() = resultDouble;
    return retval;
}


/** implement the SQL POWER function for all numeric values */
template<> inline NValue NValue::call<FUNC_POWER>(const std::vector<NValue>& arguments) {
    assert(arguments.size() == 2);
    NValue retval(VALUE_TYPE_DOUBLE);
    const NValue& base = arguments[0];
    const NValue& exponent = arguments[1];

    if (base.isNull()) {
        return base;
    }
    if (exponent.isNull())
    {
      return exponent;
    }
    double baseValue = base.castAsDoubleAndGetValue();
    double exponentValue = exponent.castAsDoubleAndGetValue();
    double resultDouble = std::pow(baseValue, exponentValue);
    throwDataExceptionIfInfiniteOrNaN(resultDouble, "function POWER");
    retval.getDouble() = resultDouble;
    return retval;
}



}
