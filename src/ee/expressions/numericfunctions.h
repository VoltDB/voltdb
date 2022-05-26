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

#include "common/NValue.hpp"
#include "boost/math/constants/constants.hpp"


namespace voltdb {

static const TTInt CONST_ONE("1");
static const TTInt CONST_FIVE("5");

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
        case ValueType::tTINYINT:
            retval.getTinyInt() = static_cast<int8_t>(std::abs(getTinyInt())); break;
        case ValueType::tSMALLINT:
            retval.getSmallInt() = static_cast<int16_t>(std::abs(getSmallInt())); break;
        case ValueType::tINTEGER:
            retval.getInteger() = std::abs(getInteger()); break;
        case ValueType::tBIGINT:
            retval.getBigInt() = std::abs(getBigInt()); break;
        case ValueType::tDOUBLE:
            retval.getDouble() = std::abs(getDouble()); break;
        case ValueType::tDECIMAL:
            retval.getDecimal() = getDecimal();
            retval.getDecimal().Abs(); // updates in place!
            break;
        case ValueType::tTIMESTAMP:
        default:
            throwCastSQLException (type, ValueType::NumericDiagnostics);
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

        case ValueType::tTINYINT:
        case ValueType::tSMALLINT:
        case ValueType::tINTEGER:
        case ValueType::tBIGINT:
        return *this;

    /*floor() in C++ returns double (64-bits) if input is double, float (32-bit) if input is float,
      and long double (128-bit) if input is long double (128-bit).*/
        case ValueType::tDOUBLE:
        retval.getDouble() = std::floor(getDouble());
        break;
        case ValueType::tDECIMAL: {
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
        throwCastSQLException (type, ValueType::NumericDiagnostics);
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
        case ValueType::tTINYINT:
        case ValueType::tSMALLINT:
        case ValueType::tINTEGER:
        case ValueType::tBIGINT:
            return *this;

            /*ceil() in C++ returns double (64-bits) if input is double, float (32-bit) if input is float,
              and long double (128-bit) if input is long double (128-bit). VoltDB INTEGER is 32-bit, BIGINT is 64-bit,
              so for TINYINT (8-bit) and SMALLINT (16-bit), we need to cast.*/

        case ValueType::tDOUBLE:
            retval.getDouble() = std::ceil(getDouble());
            break;
        case ValueType::tDECIMAL:
            {
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
            throwCastSQLException(type, ValueType::NumericDiagnostics);
            break;
    }
    return retval;
}



/** implement the SQL SQRT function for all numeric values */
template<> inline NValue NValue::callUnary<FUNC_SQRT>() const {
    if (isNull()) {
        return *this;
    }
    NValue retval(ValueType::tDOUBLE);
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
    NValue retval(ValueType::tDOUBLE);
    //exp() in C++ returns double (64-bits) if input is double, float (32-bit) if input is float,
    //and long double (128-bit) if input is long double (128-bit).
    double exponentValue = castAsDoubleAndGetValue();
    double resultDouble = std::exp(exponentValue);
    throwDataExceptionIfInfiniteOrNaN(resultDouble, "function EXP");
    retval.getDouble() = resultDouble;
    return retval;
}


/** implement the SQL LOG/LN function for all numeric values */
template<> inline NValue NValue::callUnary<FUNC_LN>() const {
    if (isNull()) {
        return *this;
    }
    NValue retval(ValueType::tDOUBLE);
    double inputValue = castAsDoubleAndGetValue();
    double resultDouble = std::log(inputValue);
    throwDataExceptionIfInfiniteOrNaN(resultDouble, "function LN");
    retval.getDouble() = resultDouble;
    return retval;
}

/** implement the SQL LOG10 function for all numeric values */
template<> inline NValue NValue::callUnary<FUNC_LOG10>() const {
    if (isNull()) {
        return *this;
    }
    NValue retval(ValueType::tDOUBLE);
    double inputValue = castAsDoubleAndGetValue();
    double resultDouble = std::log10(inputValue);
    throwDataExceptionIfInfiniteOrNaN(resultDouble, "function LOG10");
    retval.getDouble() = resultDouble;
    return retval;
}


/** implement the SQL SIN function for all numeric values */
template<> inline NValue NValue::callUnary<FUNC_SIN>() const {
    if (isNull()) {
        return *this;
    }
    NValue retval(ValueType::tDOUBLE);
    double inputValue = castAsDoubleAndGetValue();
    double resultDouble = std::sin(inputValue);
    throwDataExceptionIfInfiniteOrNaN(resultDouble, "function SIN");
    retval.getDouble() = resultDouble;
    return retval;
}

/** implement the SQL COS function for all numeric values */
template<> inline NValue NValue::callUnary<FUNC_COS>() const {
    if (isNull()) {
        return *this;
    }
    NValue retval(ValueType::tDOUBLE);
    double inputValue = castAsDoubleAndGetValue();
    double resultDouble = std::cos(inputValue);
    throwDataExceptionIfInfiniteOrNaN(resultDouble, "function COS");
    retval.getDouble() = resultDouble;
    return retval;
}

/** implement the SQL TAN function for all numeric values */
template<> inline NValue NValue::callUnary<FUNC_TAN>() const {
    if (isNull()) {
        return *this;
    }
    NValue retval(ValueType::tDOUBLE);
    double inputValue = castAsDoubleAndGetValue();
    double resultDouble = std::tan(inputValue);
    throwDataExceptionIfInfiniteOrNaN(resultDouble, "function TAN");
    retval.getDouble() = resultDouble;
    return retval;
}

/** implement the SQL COT function for all numeric values */
template<> inline NValue NValue::callUnary<FUNC_COT>() const {
    if (isNull()) {
        return *this;
    }
    NValue retval(ValueType::tDOUBLE);
    double inputValue = castAsDoubleAndGetValue();
    double tanDouble = std::tan(inputValue);
    double resultDouble = 1 / tanDouble;
    throwDataExceptionIfInfiniteOrNaN(resultDouble, "function COT");
    retval.getDouble() = resultDouble;
    return retval;
}

/** implement the SQL CSC function for all numeric values */
template<> inline NValue NValue::callUnary<FUNC_CSC>() const {
    if (isNull()) {
        return *this;
    }
    NValue retval(ValueType::tDOUBLE);
    double inputValue = castAsDoubleAndGetValue();
    double sinDouble = std::sin(inputValue);
    double resultDouble = 1 / sinDouble;
    throwDataExceptionIfInfiniteOrNaN(resultDouble, "function CSC");

    retval.getDouble() = resultDouble;
    return retval;
}

/** implement the SQL SEC function for all numeric values */
template<> inline NValue NValue::callUnary<FUNC_SEC>() const {
    if (isNull()) {
        return *this;
    }
    NValue retval(ValueType::tDOUBLE);
    double inputValue = castAsDoubleAndGetValue();
    double cosDouble = std::cos(inputValue);
    double resultDouble = 1 / cosDouble;
    throwDataExceptionIfInfiniteOrNaN(resultDouble, "function SEC");
    retval.getDouble() = resultDouble;
    return retval;
}

/** implement the SQL DEGREE function for all numeric values */
template<> inline NValue NValue::callUnary<FUNC_DEGREES>() const {
    if (isNull()) {
        return *this;
    }
    NValue retval(ValueType::tDOUBLE);
    double inputValue = castAsDoubleAndGetValue();
    double resultDouble = inputValue*(180.0 / M_PI);
    throwDataExceptionIfInfiniteOrNaN(resultDouble, "function DEGREES");
    retval.getDouble() = resultDouble;
    return retval;
}

/** implement the SQL RADIAN function for all numeric values */
template<> inline NValue NValue::callUnary<FUNC_RADIANS>() const {
    if (isNull()) {
        return *this;
    }
    NValue retval(ValueType::tDOUBLE);
    double inputValue = castAsDoubleAndGetValue();
    double resultDouble = inputValue*(M_PI / 180.0);
    throwDataExceptionIfInfiniteOrNaN(resultDouble, "function Radians");
    retval.getDouble() = resultDouble;
    return retval;
}

/** implement the SQL POWER function for all numeric values */
template<> inline NValue NValue::call<FUNC_POWER>(const std::vector<NValue>& arguments) {
    vassert(arguments.size() == 2);
    NValue retval(ValueType::tDOUBLE);
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

/**
 * FYI, http://stackoverflow.com/questions/7594508/modulo-operator-with-negative-values
 *
 * It looks like having any negative operand results in undefined behavior,
 * meaning different C++ compilers could get different answers here.
 * In C++2003, the modulo operator (%) is implementation defined.
 * In C++2011 the policy is slavish devotion to Fortran semantics. This makes sense because,
 * apparently, all the current hardware uses Fortran semantics anyway. So, even if it's implementation
 * defined it's likely to be implementation defined in the same way.
 *
 * FYI, Fortran semantics: https://gcc.gnu.org/onlinedocs/gfortran/MOD.html
 * It has the same semantics with C99 as: int(a / b) * b + MOD(a,b)  == a
 */
template<> inline NValue NValue::call<FUNC_MOD>(const std::vector<NValue>& arguments) {
    vassert(arguments.size() == 2);
    const NValue& base = arguments[0];
    const NValue& divisor = arguments[1];

    const ValueType baseType = base.getValueType();
    const ValueType divisorType = divisor.getValueType();

    // planner should guard against any invalid number type
    if (!isNumeric(baseType) || !isNumeric(divisorType)) {
        throw SQLException(SQLException::dynamic_sql_error, "unsupported non-numeric type for SQL MOD function");
    }

    bool areAllIntegralOrDecimalType = (isIntegralType(baseType) && isIntegralType(divisorType))
        || (baseType == ValueType::tDECIMAL && divisorType == ValueType::tDECIMAL);

    if (! areAllIntegralOrDecimalType) {
        throw SQLException(SQLException::dynamic_sql_error, "unsupported non-integral or non-decimal type for SQL MOD function");
    }

    if (base.isNull() || divisor.isNull()) {
        return getNullValue(ValueType::tBIGINT);
    } else if (divisor.castAsDoubleAndGetValue() == 0) {
        throw SQLException(SQLException::data_exception_division_by_zero, "division by zero");
    }

    if (isIntegralType(baseType)){
        int64_t baseValue = base.castAsBigIntAndGetValue();
        int64_t divisorValue = divisor.castAsBigIntAndGetValue();

        int64_t result = std::abs(baseValue) % std::abs(divisorValue);
        if (baseValue < 0) {
            result *= -1;
        }

        return getBigIntValue(result);
    } else {
        TTInt result_decimal = base.castAsDecimalAndGetValue() % divisor.castAsDecimalAndGetValue();
        return NValue::getDecimalValue(result_decimal);
    }
}

/*
 * implement the SQL PI function
 */
template<> inline NValue NValue::callConstant<FUNC_PI>() {
    return getDoubleValue(boost::math::constants::pi<double>());
}

/** implement the Volt SQL round function for decimal values */
template<> inline NValue NValue::call<FUNC_VOLT_ROUND>(const std::vector<NValue>& arguments) {
    vassert(arguments.size() == 2);
    const NValue &arg1 = arguments[0];
    if (arg1.isNull()) {
        return getNullStringValue();
    }
    const ValueType type = arg1.getValueType();
    //only double and decimal is allowed

    if (type != ValueType::tDECIMAL && type != ValueType::tDOUBLE) {
        throwCastSQLException (type, ValueType::tDECIMAL);
    }

    std::ostringstream out;

    TTInt scaledValue;
    if(type == ValueType::tDOUBLE) {
        scaledValue = arg1.castAsDecimal().castAsDecimalAndGetValue();
    } else {
        scaledValue = arg1.castAsDecimalAndGetValue();
    }

    if (scaledValue.IsSign()) {
        out << '-';
        scaledValue.ChangeSign();
    }

    // rounding
    const NValue &arg2 = arguments[1];
    int32_t places = arg2.castAsIntegerAndGetValue();
    if (places >= 12 || places <= -26) {
        throw SQLException(SQLException::data_exception_numeric_value_out_of_range,
            "the second parameter should be < 12 and > -26");
    }

    TTInt ten(10);
    if (places <= 0) {
        ten.Pow(-places);
    }
    else {
        ten.Pow(places);
    }
    TTInt denominator = (places <= 0) ? (TTInt(kMaxScaleFactor) * ten):
                                        (TTInt(kMaxScaleFactor) / ten);
    TTInt fractional(scaledValue);
    fractional %= denominator;
    TTInt barrier = CONST_FIVE * (denominator / 10);

    if (fractional > barrier) {
        scaledValue += denominator;
    }

    if (fractional == barrier) {
        TTInt prev = scaledValue / denominator;
        if (prev % 2 == CONST_ONE) {
            scaledValue += denominator;
        }
    }

    if (places <= 0) {
        scaledValue -= fractional;
        int64_t whole = narrowDecimalToBigInt(scaledValue);
        out << std::fixed << whole;
    }
    else {
        int64_t whole = narrowDecimalToBigInt(scaledValue);
        int64_t fraction = getFractionalPart(scaledValue);
        // here denominator is guarateed to be able to converted to int64_t
        fraction /= denominator.ToInt();
        out << std::fixed << whole;
        // fractional part does not need groups
        out << '.' << std::setfill('0') << std::setw(places) << fraction;
    }
    // TODO: Although there should be only one copy of newloc (and money_numpunct),
    // we still need to test and make sure no memory leakage in this piece of code.
    std::string rv = out.str();
    return getDecimalValueFromString(rv);
}

}
