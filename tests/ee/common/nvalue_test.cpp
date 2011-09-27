/* This file is part of VoltDB.
 * Copyright (C) 2008-2011 VoltDB Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */
#include "harness.h"
#include "common/NValue.hpp"
#include "common/ValueFactory.hpp"
#include "common/ValuePeeker.hpp"
#include "common/serializeio.h"
#include "common/ThreadLocalPool.h"

#include <cfloat>
#include <limits>

using namespace std;
using namespace voltdb;

class NValueTest : public Test {
    ThreadLocalPool m_pool;
};

void deserDecHelper(NValue nv, ValueType &vt,
                    TTInt &value, string &str) {
    vt    = ValuePeeker::peekValueType(nv);
    value = ValuePeeker::peekDecimal(nv);
    str   = ValuePeeker::peekDecimalString(nv);
}

TEST_F(NValueTest, DeserializeDecimal)
{
    int64_t scale = 1000000000000;
    string str;

    ValueType vt;
    TTInt value;
    NValue nv;


    nv = ValueFactory::getDecimalValueFromString("6.0000000");
    deserDecHelper(nv, vt, value, str);
    ASSERT_FALSE(nv.isNull());
    ASSERT_EQ(vt, VALUE_TYPE_DECIMAL);
    ASSERT_EQ(value, TTInt("6000000000000"));
    ASSERT_EQ(str, "6.000000000000");

    nv = ValueFactory::getDecimalValueFromString("-0");
    deserDecHelper(nv, vt, value, str);
    ASSERT_FALSE(nv.isNull());
    ASSERT_EQ(vt, VALUE_TYPE_DECIMAL);
    ASSERT_EQ(value, TTInt(0));
    // Decimals in Volt are currently hardwired with 12 fractional
    // decimal places.
    ASSERT_EQ(str, "0.000000000000");

    nv = ValueFactory::getDecimalValueFromString("0");
    deserDecHelper(nv, vt, value, str);
    ASSERT_FALSE(nv.isNull());
    ASSERT_EQ(vt, VALUE_TYPE_DECIMAL);
    ASSERT_EQ(value, TTInt(0));
    ASSERT_EQ(str, "0.000000000000");

    nv = ValueFactory::getDecimalValueFromString("0.0");
    deserDecHelper(nv, vt, value, str);
    ASSERT_FALSE(nv.isNull());
    ASSERT_EQ(vt, VALUE_TYPE_DECIMAL);
    ASSERT_EQ(value, TTInt(0));
    ASSERT_EQ(str, "0.000000000000");

    nv = ValueFactory::getDecimalValueFromString("1");
    deserDecHelper(nv, vt, value, str);
    ASSERT_FALSE(nv.isNull());
    ASSERT_EQ(vt, VALUE_TYPE_DECIMAL);
    ASSERT_EQ(value, TTInt("1000000000000"));
    ASSERT_EQ(str, "1.000000000000");

    nv = ValueFactory::getDecimalValueFromString("-1");
    deserDecHelper(nv, vt, value, str);
    ASSERT_FALSE(nv.isNull());
    ASSERT_EQ(vt, VALUE_TYPE_DECIMAL);
    ASSERT_EQ(value, TTInt("-1000000000000"));
    ASSERT_EQ(str, "-1.000000000000");

    // min value
    nv = ValueFactory::getDecimalValueFromString("-9999999999"  //10 digits
                                       "9999999999"   //20 digits
                                       "999999.9999"   //30 digits
                                       "99999999");   //38 digits
    deserDecHelper(nv, vt, value, str);
    ASSERT_FALSE(nv.isNull());
    ASSERT_EQ(vt, VALUE_TYPE_DECIMAL);
    ASSERT_EQ(value, TTInt("-9999999999"  //10 digits
            "9999999999"   //20 digits
            "9999999999"   //30 digits
            "99999999"));
    ASSERT_FALSE(strcmp(str.c_str(), "-9999999999"  //10 digits
            "9999999999"   //20 digits
            "999999.9999"   //30 digits
            "99999999"));

    // max value
    nv = ValueFactory::getDecimalValueFromString("9999999999"  //10 digits
            "9999999999"   //20 digits
            "999999.9999"   //30 digits
            "99999999");
    deserDecHelper(nv, vt, value, str);
    ASSERT_FALSE(nv.isNull());
    ASSERT_EQ(vt, VALUE_TYPE_DECIMAL);
    ASSERT_EQ(value, TTInt("9999999999"  //10 digits
            "9999999999"   //20 digits
            "9999999999"   //30 digits
            "99999999"));
    ASSERT_FALSE(strcmp(str.c_str(), "9999999999"  //10 digits
            "9999999999"   //20 digits
            "999999.9999"   //30 digits
            "99999999"));

    nv = ValueFactory::getDecimalValueFromString("1234");
    deserDecHelper(nv, vt, value, str);
    ASSERT_FALSE(nv.isNull());
    ASSERT_EQ(vt, VALUE_TYPE_DECIMAL);
    ASSERT_EQ(value, TTInt(1234 * scale));
    ASSERT_EQ(str, "1234.000000000000");

    nv = ValueFactory::getDecimalValueFromString("12.34");
    deserDecHelper(nv, vt, value, str);
    ASSERT_FALSE(nv.isNull());
    ASSERT_EQ(vt, VALUE_TYPE_DECIMAL);
    ASSERT_EQ(value, TTInt(static_cast<int64_t>(12340000000000)));
    ASSERT_EQ(str, "12.340000000000");

    nv = ValueFactory::getDecimalValueFromString("-1234");
    deserDecHelper(nv, vt, value, str);
    ASSERT_FALSE(nv.isNull());
    ASSERT_EQ(vt, VALUE_TYPE_DECIMAL);
    ASSERT_EQ(value, TTInt(-1234 * scale));
    ASSERT_EQ(str, "-1234.000000000000");

    nv = ValueFactory::getDecimalValueFromString("-12.34");
    deserDecHelper(nv, vt, value, str);
    ASSERT_FALSE(nv.isNull());
    ASSERT_EQ(vt, VALUE_TYPE_DECIMAL);
    ASSERT_EQ(value, TTInt(static_cast<int64_t>(-12340000000000)));
    ASSERT_EQ(str, "-12.340000000000");

    // illegal deserializations
    try {
        // too few digits
        nv = ValueFactory::getDecimalValueFromString("");
        ASSERT_EQ(0,1);
    }
    catch (SerializableEEException &e) {
    }

    try {
        // too many digits
        nv = ValueFactory::getDecimalValueFromString("11111111111111111111111111111");
        ASSERT_EQ(0,1);
    }
    catch (SerializableEEException &e) {
    }

    try {
        // too much precision
        nv = ValueFactory::getDecimalValueFromString("999999999999999999999999999.999999999999");
        ASSERT_EQ(0,1);
    }
    catch (SerializableEEException &e) {
    }

    try {
        // too many decimal points
        nv = ValueFactory::getDecimalValueFromString("9.9.9");
        ASSERT_EQ(0,1);
    }
    catch (SerializableEEException &e) {
    }

    try {
        // too many decimal points
        nv = ValueFactory::getDecimalValueFromString("..0");
        ASSERT_EQ(0,1);
    }
    catch (SerializableEEException &e) {
    }

    try {
        // invalid character
        nv = ValueFactory::getDecimalValueFromString("0b.5");
        ASSERT_EQ(0,1);
    }
    catch (SerializableEEException &e) {
    }

    ASSERT_EQ(1,1);
}

TEST_F(NValueTest, TestCastToBigInt) {
    NValue tinyInt = ValueFactory::getTinyIntValue(120);
    NValue smallInt = ValueFactory::getSmallIntValue(255);
    NValue integer = ValueFactory::getIntegerValue(243432);
    NValue bigInt = ValueFactory::getBigIntValue(2323325432453);
    NValue doubleValue = ValueFactory::getDoubleValue(244643.1236);
    NValue stringValue = ValueFactory::getStringValue("dude");
    NValue decimalValue = ValueFactory::getDecimalValueFromString("10.22");

    NValue bigIntCastToBigInt = ValueFactory::castAsBigInt(bigInt);
    EXPECT_EQ(ValuePeeker::peekBigInt(bigIntCastToBigInt), 2323325432453);

    NValue integerCastToBigInt = ValueFactory::castAsBigInt(integer);
    EXPECT_EQ(ValuePeeker::peekBigInt(integerCastToBigInt), 243432);

    NValue smallIntCastToBigInt = ValueFactory::castAsBigInt(smallInt);
    EXPECT_EQ(ValuePeeker::peekBigInt(smallIntCastToBigInt), 255);

    NValue tinyIntCastToBigInt = ValueFactory::castAsBigInt(tinyInt);
    EXPECT_EQ(ValuePeeker::peekBigInt(tinyIntCastToBigInt), 120);

    NValue doubleCastToBigInt = ValueFactory::castAsBigInt(doubleValue);
    EXPECT_EQ(ValuePeeker::peekBigInt(doubleCastToBigInt), 244643);

    bool caught = false;
    try
    {
        NValue decimalCastToBigInt = ValueFactory::castAsBigInt(decimalValue);
    }
    catch (SQLException& ex)
    {
        caught = true;
    }
    EXPECT_TRUE(caught);

    caught = false;
    try
    {
        NValue stringCastToBigInt = ValueFactory::castAsBigInt(stringValue);
    }
    catch (SQLException& ex)
    {
        caught = true;
    }
    EXPECT_TRUE(caught);

    /*
     * Now run a series of tests to make sure that out of range casts fail
     * For BigInt only a double can be out of range.
     */
    NValue doubleOutOfRangeH = ValueFactory::getDoubleValue(92233720368547075809.0);
    NValue doubleOutOfRangeL = ValueFactory::getDoubleValue(-92233720368547075809.0);

    caught = false;
    try
    {
        NValue doubleCastToBigInt = ValueFactory::castAsBigInt(doubleOutOfRangeH);
    } catch (SQLException& ex)
    {
        caught = true;
    }
    EXPECT_TRUE(caught);

    caught = false;
    try
    {
        NValue doubleCastToBigInt = ValueFactory::castAsBigInt(doubleOutOfRangeL);
    } catch (SQLException& ex)
    {
        caught = true;
    }
    EXPECT_TRUE(caught);

    // Make valgrind happy
    stringValue.free();
}

TEST_F(NValueTest, TestCastToInteger) {
    NValue tinyInt = ValueFactory::getTinyIntValue(120);
    NValue smallInt = ValueFactory::getSmallIntValue(255);
    NValue integer = ValueFactory::getIntegerValue(243432);
    NValue bigInt = ValueFactory::getBigIntValue(232332);
    NValue doubleValue = ValueFactory::getDoubleValue(244643.1236);
    NValue stringValue = ValueFactory::getStringValue("dude");
    NValue decimalValue = ValueFactory::getDecimalValueFromString("10.22");

    NValue bigIntCastToInteger = ValueFactory::castAsInteger(bigInt);
    EXPECT_EQ(ValuePeeker::peekInteger(bigIntCastToInteger), 232332);

    NValue integerCastToInteger = ValueFactory::castAsInteger(integer);
    EXPECT_EQ(ValuePeeker::peekInteger(integerCastToInteger), 243432);

    NValue smallIntCastToInteger = ValueFactory::castAsInteger(smallInt);
    EXPECT_EQ(ValuePeeker::peekInteger(smallIntCastToInteger), 255);

    NValue tinyIntCastToInteger = ValueFactory::castAsInteger(tinyInt);
    EXPECT_EQ(ValuePeeker::peekInteger(tinyIntCastToInteger), 120);

    NValue doubleCastToInteger = ValueFactory::castAsInteger(doubleValue);
    EXPECT_EQ(ValuePeeker::peekInteger(doubleCastToInteger), 244643);

    bool caught = false;
    try
    {
        NValue decimalCast = ValueFactory::castAsInteger(decimalValue);
    }
    catch (SQLException& ex)
    {
        caught = true;
    }
    EXPECT_TRUE(caught);

    caught = false;
    try
    {
        NValue stringCast = ValueFactory::castAsInteger(stringValue);
    }
    catch (SQLException& ex)
    {
        caught = true;
    }
    EXPECT_TRUE(caught);

    /*
     * Now run a series of tests to make sure that out of range casts fail
     * For Integer only a double and BigInt can be out of range.
     */
    NValue doubleOutOfRangeH = ValueFactory::getDoubleValue(92233720368547075809.0);
    NValue doubleOutOfRangeL = ValueFactory::getDoubleValue(-92233720368547075809.0);
    caught = false;
    try
    {
        NValue doubleCastToInteger = ValueFactory::castAsInteger(doubleOutOfRangeH);
    } catch (SQLException& ex)
    {
        caught = true;
    }
    EXPECT_TRUE(caught);

    caught = false;
    try
    {
        NValue doubleCastToInteger = ValueFactory::castAsInteger(doubleOutOfRangeL);
    } catch (SQLException& ex)
    {
        caught = true;
    }
    EXPECT_TRUE(caught);

    NValue bigIntOutOfRangeH = ValueFactory::getBigIntValue(4294967297);
    NValue bigIntOutOfRangeL = ValueFactory::getBigIntValue(-4294967297);

    caught = false;
    try
    {
        NValue bigIntCastToInteger = ValueFactory::castAsInteger(bigIntOutOfRangeH);
    } catch (SQLException& ex)
    {
        caught = true;
    }
    EXPECT_TRUE(caught);

    caught = false;
    try
    {
        NValue bigIntCastToInteger = ValueFactory::castAsInteger(bigIntOutOfRangeL);
    } catch (SQLException& ex)
    {
        caught = true;
    }
    EXPECT_TRUE(caught);

    // Make valgrind happy
    stringValue.free();
}

TEST_F(NValueTest, TestCastToSmallInt) {
    NValue tinyInt = ValueFactory::getTinyIntValue(120);
    NValue smallInt = ValueFactory::getSmallIntValue(255);
    NValue integer = ValueFactory::getIntegerValue(3432);
    NValue bigInt = ValueFactory::getBigIntValue(2332);
    NValue doubleValue = ValueFactory::getDoubleValue(4643.1236);
    NValue stringValue = ValueFactory::getStringValue("dude");
    NValue decimalValue = ValueFactory::getDecimalValueFromString("10.22");

    NValue bigIntCastToSmallInt = ValueFactory::castAsSmallInt(bigInt);
    EXPECT_EQ(ValuePeeker::peekSmallInt(bigIntCastToSmallInt), 2332);

    NValue integerCastToSmallInt = ValueFactory::castAsSmallInt(integer);
    EXPECT_EQ(ValuePeeker::peekSmallInt(integerCastToSmallInt), 3432);

    NValue smallIntCastToSmallInt = ValueFactory::castAsSmallInt(smallInt);
    EXPECT_EQ(ValuePeeker::peekSmallInt(smallIntCastToSmallInt), 255);

    NValue tinyIntCastToSmallInt = ValueFactory::castAsSmallInt(tinyInt);
    EXPECT_EQ(ValuePeeker::peekSmallInt(tinyIntCastToSmallInt), 120);

    NValue doubleCastToSmallInt = ValueFactory::castAsSmallInt(doubleValue);
    EXPECT_EQ(ValuePeeker::peekSmallInt(doubleCastToSmallInt), 4643);

    bool caught = false;
    try
    {
        NValue decimalCast = ValueFactory::castAsSmallInt(decimalValue);
    }
    catch (SQLException& ex)
    {
        caught = true;
    }
    EXPECT_TRUE(caught);

    caught = false;
    try
    {
        NValue stringCast = ValueFactory::castAsSmallInt(stringValue);
    }
    catch (SQLException& ex)
    {
        caught = true;
    }
    EXPECT_TRUE(caught);

    /*
     * Now run a series of tests to make sure that out of range casts fail
     * For SmallInt only a double, BigInt, and Integer can be out of range.
     */
    NValue doubleOutOfRangeH = ValueFactory::getDoubleValue(92233720368547075809.0);
    NValue doubleOutOfRangeL = ValueFactory::getDoubleValue(-92233720368547075809.0);
    caught = false;
    try
    {
        NValue doubleCastToSmallInt = ValueFactory::castAsSmallInt(doubleOutOfRangeH);
    } catch (SQLException& ex)
    {
        caught = true;
    }
    EXPECT_TRUE(caught);

    caught = false;
    try
    {
        NValue doubleCastToSmallInt = ValueFactory::castAsSmallInt(doubleOutOfRangeL);
    } catch (SQLException& ex)
    {
        caught = true;
    }
    EXPECT_TRUE(caught);

    NValue bigIntOutOfRangeH = ValueFactory::getBigIntValue(4294967297);
    NValue bigIntOutOfRangeL = ValueFactory::getBigIntValue(-4294967297);

    caught = false;
    try
    {
        NValue bigIntCastToSmallInt = ValueFactory::castAsSmallInt(bigIntOutOfRangeH);
    } catch (SQLException& ex)
    {
        caught = true;
    }
    EXPECT_TRUE(caught);

    caught = false;
    try
    {
        NValue bigIntCastToSmallInt = ValueFactory::castAsSmallInt(bigIntOutOfRangeL);
    } catch (SQLException& ex)
    {
        caught = true;
    }
    EXPECT_TRUE(caught);

    NValue integerOutOfRangeH = ValueFactory::getIntegerValue(429496729);
    NValue integerOutOfRangeL = ValueFactory::getIntegerValue(-429496729);

    caught = false;
    try
    {
        NValue integerCastToSmallInt = ValueFactory::castAsSmallInt(integerOutOfRangeH);
    } catch (SQLException& ex)
    {
        caught = true;
    }
    EXPECT_TRUE(caught);

    caught = false;
    try
    {
        NValue integerCastToSmallInt = ValueFactory::castAsSmallInt(integerOutOfRangeL);
    } catch (SQLException& ex)
    {
        caught = true;
    }
    EXPECT_TRUE(caught);

    // Make valgrind happy
    stringValue.free();
}

TEST_F(NValueTest, TestCastToTinyInt) {
    NValue tinyInt = ValueFactory::getTinyIntValue(120);
    NValue smallInt = ValueFactory::getSmallIntValue(120);
    NValue integer = ValueFactory::getIntegerValue(120);
    NValue bigInt = ValueFactory::getBigIntValue(-64);
    NValue doubleValue = ValueFactory::getDoubleValue(-32);
    NValue stringValue = ValueFactory::getStringValue("dude");
    NValue decimalValue = ValueFactory::getDecimalValueFromString("10.22");

    NValue bigIntCastToTinyInt = ValueFactory::castAsTinyInt(bigInt);
    EXPECT_EQ(ValuePeeker::peekTinyInt(bigIntCastToTinyInt), -64);

    NValue integerCastToTinyInt = ValueFactory::castAsTinyInt(integer);
    EXPECT_EQ(ValuePeeker::peekTinyInt(integerCastToTinyInt), 120);

    NValue smallIntCastToTinyInt = ValueFactory::castAsTinyInt(smallInt);
    EXPECT_EQ(ValuePeeker::peekTinyInt(smallIntCastToTinyInt), 120);

    NValue tinyIntCastToTinyInt = ValueFactory::castAsTinyInt(tinyInt);
    EXPECT_EQ(ValuePeeker::peekTinyInt(tinyIntCastToTinyInt), 120);

    NValue doubleCastToTinyInt = ValueFactory::castAsTinyInt(doubleValue);
    EXPECT_EQ(ValuePeeker::peekTinyInt(doubleCastToTinyInt), -32);

    bool caught = false;
    try
    {
        NValue decimalCast = ValueFactory::castAsTinyInt(decimalValue);
    }
    catch (SQLException& ex)
    {
        caught = true;
    }
    EXPECT_TRUE(caught);

    caught = false;
    try
    {
        NValue stringCast = ValueFactory::castAsTinyInt(stringValue);
    }
    catch (SQLException& ex)
    {
        caught = true;
    }
    EXPECT_TRUE(caught);

    /*
     * Now run a series of tests to make sure that out of range casts fail
     * For TinyInt only a double, BigInt, Integer, and SmallInt can be out of range.
     */
    NValue doubleOutOfRangeH = ValueFactory::getDoubleValue(92233720368547075809.0);
    NValue doubleOutOfRangeL = ValueFactory::getDoubleValue(-92233720368547075809.0);
    caught = false;
    try
    {
        NValue doubleCastToTinyInt = ValueFactory::castAsTinyInt(doubleOutOfRangeH);
    } catch (SQLException& ex)
    {
        caught = true;
    }
    EXPECT_TRUE(caught);

    caught = false;
    try
    {
        NValue doubleCastToTinyInt = ValueFactory::castAsTinyInt(doubleOutOfRangeL);
    } catch (SQLException& ex)
    {
        caught = true;
    }
    EXPECT_TRUE(caught);

    NValue bigIntOutOfRangeH = ValueFactory::getBigIntValue(4294967297);
    NValue bigIntOutOfRangeL = ValueFactory::getBigIntValue(-4294967297);

    caught = false;
    try
    {
        NValue bigIntCastToTinyInt = ValueFactory::castAsTinyInt(bigIntOutOfRangeH);
    } catch (SQLException& ex)
    {
        caught = true;
    }
    EXPECT_TRUE(caught);

    caught = false;
    try
    {
        NValue bigIntCastToTinyInt = ValueFactory::castAsTinyInt(bigIntOutOfRangeL);
    } catch (SQLException& ex)
    {
        caught = true;
    }
    EXPECT_TRUE(caught);

    NValue integerOutOfRangeH = ValueFactory::getIntegerValue(429496729);
    NValue integerOutOfRangeL = ValueFactory::getIntegerValue(-429496729);

    caught = false;
    try
    {
        NValue integerCastToTinyInt = ValueFactory::castAsTinyInt(integerOutOfRangeH);
    } catch (SQLException& ex)
    {
        caught = true;
    }
    EXPECT_TRUE(caught);

    caught = false;
    try
    {
        NValue integerCastToTinyInt = ValueFactory::castAsTinyInt(integerOutOfRangeL);
    } catch (SQLException& ex)
    {
        caught = true;
    }
    EXPECT_TRUE(caught);

    NValue smallIntOutOfRangeH = ValueFactory::getSmallIntValue(32000);
    NValue smallIntOutOfRangeL = ValueFactory::getSmallIntValue(-3200);

    caught = false;
    try
    {
        NValue smallIntCastToTinyInt = ValueFactory::castAsTinyInt(smallIntOutOfRangeH);
    } catch (SQLException& ex)
    {
        caught = true;
    }
    EXPECT_TRUE(caught);

    caught = false;
    try
    {
        NValue smallIntCastToTinyInt = ValueFactory::castAsTinyInt(smallIntOutOfRangeL);
    } catch (SQLException& ex)
    {
        caught = true;
    }
    EXPECT_TRUE(caught);

    // Make valgrind happy
    stringValue.free();
}

TEST_F(NValueTest, TestCastToDouble) {
    NValue tinyInt = ValueFactory::getTinyIntValue(120);
    NValue smallInt = ValueFactory::getSmallIntValue(120);
    NValue integer = ValueFactory::getIntegerValue(120);
    NValue bigInt = ValueFactory::getBigIntValue(120);
    NValue doubleValue = ValueFactory::getDoubleValue(120.005);
    NValue stringValue = ValueFactory::getStringValue("dude");
    NValue decimalValue = ValueFactory::getDecimalValueFromString("10.22");

    NValue bigIntCastToDouble = ValueFactory::castAsDouble(bigInt);
    EXPECT_LT(ValuePeeker::peekDouble(bigIntCastToDouble), 120.1);
    EXPECT_GT(ValuePeeker::peekDouble(bigIntCastToDouble), 119.9);

    NValue integerCastToDouble = ValueFactory::castAsDouble(integer);
    EXPECT_LT(ValuePeeker::peekDouble(integerCastToDouble), 120.1);
    EXPECT_GT(ValuePeeker::peekDouble(integerCastToDouble), 119.9);

    NValue smallIntCastToDouble = ValueFactory::castAsDouble(smallInt);
    EXPECT_LT(ValuePeeker::peekDouble(smallIntCastToDouble), 120.1);
    EXPECT_GT(ValuePeeker::peekDouble(smallIntCastToDouble), 119.9);

    NValue tinyIntCastToDouble = ValueFactory::castAsDouble(tinyInt);
    EXPECT_LT(ValuePeeker::peekDouble(tinyIntCastToDouble), 120.1);
    EXPECT_GT(ValuePeeker::peekDouble(tinyIntCastToDouble), 119.9);

    NValue doubleCastToDouble = ValueFactory::castAsDouble(doubleValue);
    EXPECT_LT(ValuePeeker::peekDouble(doubleCastToDouble), 120.1);
    EXPECT_GT(ValuePeeker::peekDouble(doubleCastToDouble), 119.9);

    bool caught = false;
    try
    {
        NValue decimalCast = ValueFactory::castAsDouble(decimalValue);
    }
    catch (SQLException& ex)
    {
        caught = true;
    }
    EXPECT_TRUE(caught);

    caught = false;
    try
    {
        NValue stringCast = ValueFactory::castAsDouble(stringValue);
    }
    catch (SQLException& ex)
    {
        caught = true;
    }
    EXPECT_TRUE(caught);

    // Make valgrind happy
    stringValue.free();
}

TEST_F(NValueTest, TestCastToString) {
    NValue tinyInt = ValueFactory::getTinyIntValue(120);
    NValue smallInt = ValueFactory::getSmallIntValue(120);
    NValue integer = ValueFactory::getIntegerValue(120);
    NValue bigInt = ValueFactory::getBigIntValue(-64);
    NValue doubleValue = ValueFactory::getDoubleValue(-32);
    NValue stringValue = ValueFactory::getStringValue("dude");
    NValue decimalValue = ValueFactory::getDecimalValueFromString("10.22");

    bool caught = false;
    try
    {
        NValue cast = ValueFactory::castAsString(tinyInt);
    }
    catch (SQLException& ex)
    {
        caught = true;
    }
    EXPECT_TRUE(caught);

    caught = false;
    try
    {
        NValue cast = ValueFactory::castAsString(smallInt);
    }
    catch (SQLException& ex)
    {
        caught = true;
    }
    EXPECT_TRUE(caught);

    caught = false;
    try
    {
        NValue cast = ValueFactory::castAsString(integer);
    }
    catch (SQLException& ex)
    {
        caught = true;
    }
    EXPECT_TRUE(caught);

    caught = false;
    try
    {
        NValue cast = ValueFactory::castAsString(bigInt);
    }
    catch (SQLException& ex)
    {
        caught = true;
    }
    EXPECT_TRUE(caught);

    caught = false;
    try
    {
        NValue cast = ValueFactory::castAsString(doubleValue);
    }
    catch (SQLException& ex)
    {
        caught = true;
    }
    EXPECT_TRUE(caught);

    caught = false;
    try
    {
        NValue cast = ValueFactory::castAsString(decimalValue);
    }
    catch (SQLException& ex)
    {
        caught = true;
    }
    EXPECT_TRUE(caught);

    // Make valgrind happy
    stringValue.free();
}

TEST_F(NValueTest, TestCastToDecimal) {
    NValue tinyInt = ValueFactory::getTinyIntValue(120);
    NValue smallInt = ValueFactory::getSmallIntValue(120);
    NValue integer = ValueFactory::getIntegerValue(120);
    NValue bigInt = ValueFactory::getBigIntValue(120);
    NValue doubleValue = ValueFactory::getDoubleValue(120);
    NValue stringValue = ValueFactory::getStringValue("dude");
    NValue decimalValue = ValueFactory::getDecimalValueFromString("120");

    NValue castTinyInt = ValueFactory::castAsDecimal(tinyInt);
    EXPECT_EQ(0, decimalValue.compare(castTinyInt));
    NValue castSmallInt = ValueFactory::castAsDecimal(smallInt);
    EXPECT_EQ(0, decimalValue.compare(castSmallInt));
    NValue castInteger = ValueFactory::castAsDecimal(integer);
    EXPECT_EQ(0, decimalValue.compare(castInteger));
    NValue castBigInt = ValueFactory::castAsDecimal(bigInt);
    EXPECT_EQ(0, decimalValue.compare(castBigInt));

    bool caught = false;
    try
    {
        NValue cast = ValueFactory::castAsDecimal(doubleValue);
    }
    catch (SQLException& ex)
    {
        caught = true;
    }
    EXPECT_TRUE(caught);

    caught = false;
    try
    {
        NValue cast = ValueFactory::castAsDecimal(stringValue);
    }
    catch (SQLException& ex)
    {
        caught = true;
    }
    EXPECT_TRUE(caught);

    // Make valgrind happy
    stringValue.free();

    /*
     * Now run a series of tests to make sure that out of range casts fail
     * For Decimal only a double, BigInt, and Integer can be out of range.
     */
    NValue doubleOutOfRangeH = ValueFactory::getDoubleValue(92233720368547075809.0);
    NValue doubleOutOfRangeL = ValueFactory::getDoubleValue(-92233720368547075809.0);
    caught = false;
    try
    {
        NValue doubleCastToDecimal = ValueFactory::castAsDecimal(doubleOutOfRangeH);
    } catch (SQLException& ex)
    {
        caught = true;
    }
    EXPECT_TRUE(caught);

    caught = false;
    try
    {
        NValue doubleCastToDecimal = ValueFactory::castAsDecimal(doubleOutOfRangeL);
    } catch (SQLException& ex)
    {
        caught = true;
    }
    EXPECT_TRUE(caught);
}

/**
 * Adding can only overflow BigInt since they are all cast to BigInt before addition takes place.
 */
TEST_F(NValueTest, TestBigIntOpAddOverflow) {
    NValue lhs = ValueFactory::getBigIntValue(INT64_MAX - 10);
    NValue rhs = ValueFactory::getBigIntValue(INT32_MAX);
    bool caught = false;
    try {
        NValue result = lhs.op_add(rhs);
    } catch (SQLException& ex) {
        caught = true;
    }
    EXPECT_TRUE(caught);

    lhs = ValueFactory::getBigIntValue(-(INT64_MAX - 10));
    rhs = ValueFactory::getBigIntValue(-INT32_MAX);
    caught = false;
    try {
        NValue result = lhs.op_add(rhs);
    } catch (SQLException& ex) {
        caught = true;
    }
    EXPECT_TRUE(caught);

    /**
     * Sanity check that yes indeed regular addition doesn't throw...
     */
    lhs = ValueFactory::getBigIntValue(1);
    rhs = ValueFactory::getBigIntValue(4);
    NValue result = lhs.op_add(rhs);
}

/**
 * Subtraction can only overflow BigInt since they are all cast to BigInt before addition takes place.
 */
TEST_F(NValueTest, TestBigIntOpSubtractOverflow) {
    NValue lhs = ValueFactory::getBigIntValue(INT64_MAX - 10);
    NValue rhs = ValueFactory::getBigIntValue(-INT32_MAX);
    bool caught = false;
    try {
        NValue result = lhs.op_subtract(rhs);
    } catch (SQLException& ex) {
        caught = true;
    }
    EXPECT_TRUE(caught);

    lhs = ValueFactory::getBigIntValue(INT64_MAX - 10);
    rhs = ValueFactory::getBigIntValue(-INT32_MAX);
    caught = false;
    try {
        NValue result = lhs.op_subtract(rhs);
    } catch (SQLException& ex) {
        caught = true;
    }
    EXPECT_TRUE(caught);

    /**
     * Sanity check that yes indeed regular subtraction doesn't throw...
     */
    lhs = ValueFactory::getBigIntValue(1);
    rhs = ValueFactory::getBigIntValue(4);
    NValue result = lhs.op_subtract(rhs);
}

/**
 * Multiplication can only overflow BigInt since they are all cast to BigInt before addition takes place.
 */
TEST_F(NValueTest, TestBigIntOpMultiplyOverflow) {
    NValue lhs = ValueFactory::getBigIntValue(INT64_MAX);
    NValue rhs = ValueFactory::getBigIntValue(INT32_MAX);
    bool caught = false;
    try {
        NValue result = lhs.op_multiply(rhs);
    } catch (SQLException& ex) {
        caught = true;
    }
    EXPECT_TRUE(caught);

    lhs = ValueFactory::getBigIntValue(-(INT64_MAX - 10));
    rhs = ValueFactory::getBigIntValue(INT32_MAX);
    caught = false;
    try {
        NValue result = lhs.op_multiply(rhs);
    } catch (SQLException& ex) {
        caught = true;
    }
    EXPECT_TRUE(caught);

    lhs = ValueFactory::getBigIntValue(INT64_MAX - 10);
    rhs = ValueFactory::getBigIntValue(-INT32_MAX);
    caught = false;
    try {
        NValue result = lhs.op_multiply(rhs);
    } catch (SQLException& ex) {
        caught = true;
    }
    EXPECT_TRUE(caught);

    lhs = ValueFactory::getBigIntValue(-(INT64_MAX - 10));
    rhs = ValueFactory::getBigIntValue(-INT32_MAX);
    caught = false;
    try {
        NValue result = lhs.op_multiply(rhs);
    } catch (SQLException& ex) {
        caught = true;
    }
    EXPECT_TRUE(caught);

    /**
     * Sanity check that yes indeed regular multiplication doesn't throw...
     */
    lhs = ValueFactory::getBigIntValue(1);
    rhs = ValueFactory::getBigIntValue(4);
    NValue result = lhs.op_multiply(rhs);
}


TEST_F(NValueTest, TestDoubleOpAddOverflow) {
    //Positive infinity
    NValue lhs = ValueFactory::getDoubleValue(std::numeric_limits<double>::max());
    NValue rhs = ValueFactory::getDoubleValue(std::numeric_limits<double>::max());
    bool caught = false;
    try {
        NValue result = lhs.op_add(rhs);
    } catch (SQLException& ex) {
        caught = true;
    }
    EXPECT_TRUE(caught);

    //Negative infinity
    lhs = ValueFactory::getDoubleValue(-(std::numeric_limits<double>::max() * ((double).7)));
    rhs = ValueFactory::getDoubleValue(-(std::numeric_limits<double>::max() * ((double).7)));
    caught = false;
    try {
        NValue result = lhs.op_add(rhs);
    } catch (SQLException& ex) {
        caught = true;
    }
    EXPECT_TRUE(caught);

    /**
     * Sanity check that yes indeed regular addition doesn't throw...
     */
    lhs = ValueFactory::getDoubleValue(1);
    rhs = ValueFactory::getDoubleValue(4);
    NValue result = lhs.op_add(rhs);
}

TEST_F(NValueTest, TestDoubleOpSubtractOverflow) {
    //Positive infinity
    NValue lhs = ValueFactory::getDoubleValue(std::numeric_limits<double>::max());
    NValue rhs = ValueFactory::getDoubleValue(-(std::numeric_limits<double>::max() * ((double).5)));
    bool caught = false;
    try {
        NValue result = lhs.op_subtract(rhs);
    } catch (SQLException& ex) {
        caught = true;
    }
    EXPECT_TRUE(caught);

    //Negative infinity
    lhs = ValueFactory::getDoubleValue(-(std::numeric_limits<double>::max() * ((double).5)));
    rhs = ValueFactory::getDoubleValue(std::numeric_limits<double>::max());
    caught = false;
    try {
        NValue result = lhs.op_subtract(rhs);
    } catch (SQLException& ex) {
        caught = true;
    }
    EXPECT_TRUE(caught);

    /**
     * Sanity check that yes indeed regular subtraction doesn't throw...
     */
    lhs = ValueFactory::getDoubleValue(1.23);
    rhs = ValueFactory::getDoubleValue(4.2345346);
    NValue result = lhs.op_subtract(rhs);
}

TEST_F(NValueTest, TestDoubleOpMultiplyOverflow) {
    //Positive infinity
    NValue lhs = ValueFactory::getDoubleValue(std::numeric_limits<double>::max());
    NValue rhs = ValueFactory::getDoubleValue(std::numeric_limits<double>::max());
    bool caught = false;
    try {
        NValue result = lhs.op_multiply(rhs);
    } catch (SQLException& ex) {
        caught = true;
    }
    EXPECT_TRUE(caught);

    //Negative infinity
    lhs = ValueFactory::getDoubleValue(-(std::numeric_limits<double>::max() * ((double).5)));
    rhs = ValueFactory::getDoubleValue(std::numeric_limits<double>::max());
    caught = false;
    try {
        NValue result = lhs.op_multiply(rhs);
    } catch (SQLException& ex) {
        caught = true;
    }
    EXPECT_TRUE(caught);

    /**
     * Sanity check that yes indeed regular multiplication doesn't throw...
     */
    lhs = ValueFactory::getDoubleValue(1.23);
    rhs = ValueFactory::getDoubleValue(4.2345346);
    NValue result = lhs.op_multiply(rhs);
}

TEST_F(NValueTest, TestDoubleOpDivideOverflow) {
    //Positive infinity
    NValue lhs = ValueFactory::getDoubleValue(std::numeric_limits<double>::max());
    NValue rhs = ValueFactory::getDoubleValue(std::numeric_limits<double>::min());
    bool caught = false;
    try {
        NValue result = lhs.op_divide(rhs);
    } catch (SQLException& ex) {
        caught = true;
    }
    EXPECT_TRUE(caught);

    //Negative infinity
    lhs = ValueFactory::getDoubleValue(-(std::numeric_limits<double>::max() * ((double).5)));
    rhs = ValueFactory::getDoubleValue(std::numeric_limits<double>::min());
    caught = false;
    try {
        NValue result = lhs.op_divide(rhs);
    } catch (SQLException& ex) {
        caught = true;
    }
    EXPECT_TRUE(caught);

    /**
     * Sanity check that yes indeed regular division doesn't throw...
     */
    lhs = ValueFactory::getDoubleValue(1.23);
    rhs = ValueFactory::getDoubleValue(4.2345346);
    NValue result = lhs.op_divide(rhs);
}

TEST_F(NValueTest, TestOpIncrementOverflow) {
    NValue bigIntValue = ValueFactory::getBigIntValue(INT64_MAX);
    NValue integerValue = ValueFactory::getIntegerValue(INT32_MAX);
    NValue smallIntValue = ValueFactory::getSmallIntValue(INT16_MAX);
    NValue tinyIntValue = ValueFactory::getTinyIntValue(INT8_MAX);

    bool caught = false;
    try {
        bigIntValue.op_increment();
    } catch (SQLException& ex) {
        caught = true;
    }
    EXPECT_TRUE(caught);

    caught = false;
    try {
        integerValue.op_increment();
    } catch (SQLException& ex) {
        caught = true;
    }
    EXPECT_TRUE(caught);

    caught = false;
    try {
        smallIntValue.op_increment();
    } catch (SQLException& ex) {
        caught = true;
    }
    EXPECT_TRUE(caught);

    caught = false;
    try {
        tinyIntValue.op_increment();
    } catch (SQLException& ex) {
        caught = true;
    }
    EXPECT_TRUE(caught);
}

TEST_F(NValueTest, TestOpDecrementOverflow) {
    NValue bigIntValue = ValueFactory::getBigIntValue(VOLT_INT64_MIN);
    NValue integerValue = ValueFactory::getIntegerValue(VOLT_INT32_MIN);
    NValue smallIntValue = ValueFactory::getSmallIntValue(VOLT_INT16_MIN);
    NValue tinyIntValue = ValueFactory::getTinyIntValue(VOLT_INT8_MIN);

    bool caught = false;
    try {
        bigIntValue.op_decrement();
    } catch (SQLException& ex) {
        caught = true;
    }
    EXPECT_TRUE(caught);

    caught = false;
    try {
        integerValue.op_decrement();
    } catch (SQLException& ex) {
        caught = true;
    }
    EXPECT_TRUE(caught);

    caught = false;
    try {
        smallIntValue.op_decrement();
    } catch (SQLException& ex) {
        caught = true;
    }
    EXPECT_TRUE(caught);

    caught = false;
    try {
        tinyIntValue.op_decrement();
    } catch (SQLException& ex) {
        caught = true;
    }
    EXPECT_TRUE(caught);
}

TEST_F(NValueTest, TestComparisonOps)
{
    NValue tinyInt = ValueFactory::getTinyIntValue(101);
    NValue smallInt = ValueFactory::getSmallIntValue(1001);
    NValue integer = ValueFactory::getIntegerValue(1000001);
    NValue bigInt = ValueFactory::getBigIntValue(10000000000001);
    NValue floatVal = ValueFactory::getDoubleValue(12000.456);
    EXPECT_TRUE(smallInt.op_greaterThan(tinyInt).isTrue());
    EXPECT_TRUE(integer.op_greaterThan(smallInt).isTrue());
    EXPECT_TRUE(bigInt.op_greaterThan(integer).isTrue());
    EXPECT_TRUE(tinyInt.op_lessThan(smallInt).isTrue());
    EXPECT_TRUE(smallInt.op_lessThan(integer).isTrue());
    EXPECT_TRUE(integer.op_lessThan(bigInt).isTrue());
    EXPECT_TRUE(tinyInt.op_lessThan(floatVal).isTrue());
    EXPECT_TRUE(smallInt.op_lessThan(floatVal).isTrue());
    EXPECT_TRUE(integer.op_greaterThan(floatVal).isTrue());
    EXPECT_TRUE(bigInt.op_greaterThan(floatVal).isTrue());
    EXPECT_TRUE(floatVal.op_lessThan(bigInt).isTrue());
    EXPECT_TRUE(floatVal.op_lessThan(integer).isTrue());
    EXPECT_TRUE(floatVal.op_greaterThan(smallInt).isTrue());
    EXPECT_TRUE(floatVal.op_greaterThan(tinyInt).isTrue());

    tinyInt = ValueFactory::getTinyIntValue(-101);
    smallInt = ValueFactory::getSmallIntValue(-1001);
    integer = ValueFactory::getIntegerValue(-1000001);
    bigInt = ValueFactory::getBigIntValue(-10000000000001);
    floatVal = ValueFactory::getDoubleValue(-12000.456);
    EXPECT_TRUE(smallInt.op_lessThan(tinyInt).isTrue());
    EXPECT_TRUE(integer.op_lessThan(smallInt).isTrue());
    EXPECT_TRUE(bigInt.op_lessThan(integer).isTrue());
    EXPECT_TRUE(tinyInt.op_greaterThan(smallInt).isTrue());
    EXPECT_TRUE(smallInt.op_greaterThan(integer).isTrue());
    EXPECT_TRUE(integer.op_greaterThan(bigInt).isTrue());
    EXPECT_TRUE(tinyInt.op_greaterThan(floatVal).isTrue());
    EXPECT_TRUE(smallInt.op_greaterThan(floatVal).isTrue());
    EXPECT_TRUE(integer.op_lessThan(floatVal).isTrue());
    EXPECT_TRUE(bigInt.op_lessThan(floatVal).isTrue());
    EXPECT_TRUE(floatVal.op_greaterThan(bigInt).isTrue());
    EXPECT_TRUE(floatVal.op_greaterThan(integer).isTrue());
    EXPECT_TRUE(floatVal.op_lessThan(smallInt).isTrue());
    EXPECT_TRUE(floatVal.op_lessThan(tinyInt).isTrue());
}

TEST_F(NValueTest, TestNullHandling)
{
    NValue nullTinyInt = ValueFactory::getTinyIntValue(INT8_NULL);
    EXPECT_TRUE(nullTinyInt.isNull());
}

TEST_F(NValueTest, TestDivideByZero)
{
    NValue zeroBigInt = ValueFactory::getBigIntValue(0);
    NValue oneBigInt = ValueFactory::getBigIntValue(1);
    NValue zeroDouble = ValueFactory::getDoubleValue(0.0);
    NValue oneDouble = ValueFactory::getDoubleValue(1);
    NValue oneDecimal = ValueFactory::getDecimalValueFromString("1");
    NValue zeroDecimal = ValueFactory::getDecimalValueFromString("0");

    NValue smallDouble = ValueFactory::getDoubleValue(DBL_MIN);
    NValue smallDecimal = ValueFactory::getDecimalValueFromString(".000000000001");

    // DECIMAL / DECIMAL
    bool caught_exception = false;
    try
    {
        oneDecimal.op_divide(zeroDecimal);
    }
    catch (voltdb::SQLException& e)
    {
        caught_exception = true;
    }
    EXPECT_TRUE(caught_exception);

    // DECIMAL / INT
    caught_exception = false;
    try
    {
        oneDecimal.op_divide(zeroBigInt);
    }
    catch (voltdb::SQLException& e)
    {
        caught_exception = true;
    }
    EXPECT_TRUE(caught_exception);

    // INT / DECIMAL
    caught_exception = false;
    try
    {
        oneDecimal.op_divide(zeroDecimal);
    }
    catch (voltdb::SQLException& e)
    {
        caught_exception = true;
    }
    EXPECT_TRUE(caught_exception);

    // check result for a really small but non-zero divisor
    caught_exception = false;
    try
    {
        oneDecimal.op_divide(smallDecimal);
    }
    catch (voltdb::SQLException& e)
    {
        caught_exception = true;
    }
    EXPECT_FALSE(caught_exception);

    // INT / INT
    caught_exception = false;
    try
    {
        oneBigInt.op_divide(zeroBigInt);
    }
    catch (voltdb::SQLException& e)
    {
        caught_exception = true;
    }
    EXPECT_TRUE(caught_exception);

    // FLOAT / INT
    caught_exception = false;
    try
    {
        oneDouble.op_divide(zeroBigInt);
    }
    catch (voltdb::SQLException& e)
    {
        caught_exception = true;
    }
    EXPECT_TRUE(caught_exception);

    // INT / FLOAT
    caught_exception = false;
    try
    {
        oneBigInt.op_divide(zeroDouble);
    }
    catch (voltdb::SQLException& e)
    {
        caught_exception = true;
    }
    EXPECT_TRUE(caught_exception);

    // FLOAT / FLOAT
    caught_exception = false;
    try
    {
        oneDouble.op_divide(zeroDouble);
    }
    catch (voltdb::SQLException& e)
    {
        caught_exception = true;
    }
    EXPECT_TRUE(caught_exception);

    // check result for a really small but non-zero divisor
    caught_exception = false;
    try
    {
        oneDouble.op_divide(smallDouble);
    }
    catch (voltdb::SQLException& e)
    {
        caught_exception = true;
    }
    EXPECT_FALSE(caught_exception);

    caught_exception = false;
    try
    {
        oneBigInt.op_divide(smallDouble);
    }
    catch (voltdb::SQLException& e)
    {
        caught_exception = true;
    }
    EXPECT_FALSE(caught_exception);
}

TEST_F(NValueTest, CompareDecimal)
{
    NValue intv;
    NValue decv;

    // d.comp(int) pos/pos
    intv = ValueFactory::getTinyIntValue(120);
    decv = ValueFactory::getDecimalValueFromString("9999");
    ASSERT_EQ(1, decv.compare(intv));

    intv = ValueFactory::getTinyIntValue(120);
    decv = ValueFactory::getDecimalValueFromString("120");
    ASSERT_EQ(0, decv.compare(intv));

    intv = ValueFactory::getTinyIntValue(121);
    decv = ValueFactory::getDecimalValueFromString("120");
    ASSERT_EQ(-1, decv.compare(intv));

    // d.comp(int) pos/neg
    intv = ValueFactory::getTinyIntValue(24);
    decv = ValueFactory::getDecimalValueFromString("-100");
    ASSERT_EQ(-1, decv.compare(intv));

    // d.comp(int) neg/pos
    intv = ValueFactory::getTinyIntValue(-24);
    decv = ValueFactory::getDecimalValueFromString("23");
    ASSERT_EQ(1, decv.compare(intv));

    // d.comp(int) neg/neg
    intv = ValueFactory::getTinyIntValue(-120);
    decv = ValueFactory::getDecimalValueFromString("-9999");
    ASSERT_EQ(-1, decv.compare(intv));

    intv = ValueFactory::getTinyIntValue(-120);
    decv = ValueFactory::getDecimalValueFromString("-120");
    ASSERT_EQ(0, decv.compare(intv));

    intv = ValueFactory::getTinyIntValue(-121);
    decv = ValueFactory::getDecimalValueFromString("-120");
    ASSERT_EQ(1, decv.compare(intv));

    // Do int.compare(decimal)

    // d.comp(int) pos/pos
    intv = ValueFactory::getTinyIntValue(120);
    decv = ValueFactory::getDecimalValueFromString("9999");
    ASSERT_EQ(-1, intv.compare(decv));

    intv = ValueFactory::getTinyIntValue(120);
    decv = ValueFactory::getDecimalValueFromString("120");
    ASSERT_EQ(0, intv.compare(decv));

    intv = ValueFactory::getTinyIntValue(121);
    decv = ValueFactory::getDecimalValueFromString("120");
    ASSERT_EQ(1, intv.compare(decv));

    // d.comp(int) pos/neg
    intv = ValueFactory::getTinyIntValue(24);
    decv = ValueFactory::getDecimalValueFromString("-100");
    ASSERT_EQ(1, intv.compare(decv));

    // d.comp(int) neg/pos
    intv = ValueFactory::getTinyIntValue(-24);
    decv = ValueFactory::getDecimalValueFromString("23");
    ASSERT_EQ(-1, intv.compare(decv));

    // d.comp(int) neg/neg
    intv = ValueFactory::getTinyIntValue(-120);
    decv = ValueFactory::getDecimalValueFromString("-9999");
    ASSERT_EQ(1, intv.compare(decv));

    intv = ValueFactory::getTinyIntValue(-120);
    decv = ValueFactory::getDecimalValueFromString("-120");
    ASSERT_EQ(0, intv.compare(decv));

    intv = ValueFactory::getTinyIntValue(-121);
    decv = ValueFactory::getDecimalValueFromString("-120");
    ASSERT_EQ(-1, intv.compare(decv));
}

TEST_F(NValueTest, AddDecimal)
{
    NValue rhs;
    NValue lhs;
    NValue ans;
    NValue sum;

    // add two decimals
    rhs = ValueFactory::getDecimalValueFromString("100");
    lhs = ValueFactory::getDecimalValueFromString("200");
    ans = ValueFactory::getDecimalValueFromString("300");
    sum = lhs.op_add(rhs);
    ASSERT_EQ(VALUE_TYPE_DECIMAL, ValuePeeker::peekValueType(sum));
    ASSERT_EQ(0, ans.compare(sum));
    sum = rhs.op_add(lhs);
    ASSERT_EQ(VALUE_TYPE_DECIMAL, ValuePeeker::peekValueType(sum));
    ASSERT_EQ(0, ans.compare(sum));

    // add a big int and a decimal
    rhs = ValueFactory::getBigIntValue(100);
    sum = lhs.op_add(rhs);
    ASSERT_EQ(VALUE_TYPE_DECIMAL, ValuePeeker::peekValueType(sum));
    ASSERT_EQ(0, ans.compare(sum));

    //Overflow
    rhs = ValueFactory::getDecimalValueFromString("9999999999"  //10 digits
                                       "9999999999"   //20 digits
                                       "999999.9999"   //30 digits
                                       "99999999");
    lhs = ValueFactory::getDecimalValueFromString("111111111"  //10 digits
            "1111111111"   //20 digits
            "111111.1111"   //30 digits
            "11111111");

    bool caughtException = false;
    try {
        lhs.op_add(rhs);
    } catch (...) {
        caughtException = true;
    }
    ASSERT_TRUE(caughtException);

    //Underflow
    rhs = ValueFactory::getDecimalValueFromString("-9999999999"  //10 digits
                                       "9999999999"   //20 digits
                                       "999999.9999"   //30 digits
                                       "99999999");
    lhs = ValueFactory::getDecimalValueFromString("-111111111"  //10 digits
            "1111111111"   //20 digits
            "111111.1111"   //30 digits
            "11111111");

    caughtException = false;
    try {
        lhs.op_add(rhs);
    } catch (...) {
        caughtException = true;
    }
    ASSERT_TRUE(caughtException);
}

TEST_F(NValueTest, SubtractDecimal)
{
    NValue rhs;
    NValue lhs;
    NValue ans;
    NValue sum;

    // Subtract two decimals
    rhs = ValueFactory::getDecimalValueFromString("100");
    lhs = ValueFactory::getDecimalValueFromString("200");
    ans = ValueFactory::getDecimalValueFromString("100");
    sum = lhs.op_subtract(rhs);
    ASSERT_EQ(VALUE_TYPE_DECIMAL, ValuePeeker::peekValueType(sum));
    ASSERT_EQ(0, ans.compare(sum));
    sum = rhs.op_subtract(lhs);
    ans = ValueFactory::getDecimalValueFromString("-100");
    ASSERT_EQ(VALUE_TYPE_DECIMAL, ValuePeeker::peekValueType(sum));
    ASSERT_EQ(0, ans.compare(sum));

    // Subtract a big int and a decimal
    rhs = ValueFactory::getBigIntValue(100);
    sum = lhs.op_subtract(rhs);
    ans = ValueFactory::getDecimalValueFromString("100");
    ASSERT_EQ(VALUE_TYPE_DECIMAL, ValuePeeker::peekValueType(sum));
    ASSERT_EQ(0, ans.compare(sum));

    //Overflow
    rhs = ValueFactory::getDecimalValueFromString("-9999999999"  //10 digits
                                       "9999999999"   //20 digits
                                       "999999.9999"   //30 digits
                                       "99999999");
    lhs = ValueFactory::getDecimalValueFromString("111111111"  //10 digits
            "1111111111"   //20 digits
            "111111.1111"   //30 digits
            "11111111");

    bool caughtException = false;
    try {
        lhs.op_subtract(rhs);
    } catch (...) {
        caughtException = true;
    }
    ASSERT_TRUE(caughtException);

    //Underflow
    rhs = ValueFactory::getDecimalValueFromString("9999999999"  //10 digits
                                       "9999999999"   //20 digits
                                       "999999.9999"   //30 digits
                                       "99999999");
    lhs = ValueFactory::getDecimalValueFromString("-111111111"  //10 digits
            "1111111111"   //20 digits
            "111111.1111"   //30 digits
            "11111111");

    caughtException = false;
    try {
        lhs.op_subtract(rhs);
    } catch (...) {
        caughtException = true;
    }
    ASSERT_TRUE(caughtException);
}

TEST_F(NValueTest, DecimalProducts)
{
    NValue rhs;
    NValue lhs;
    NValue product;
    NValue ans;

    // decimal * int
    rhs = ValueFactory::getDecimalValueFromString("218772.7686110");
    lhs = ValueFactory::getBigIntValue((int64_t)2);
    product = rhs.op_multiply(lhs);
    ans = ValueFactory::getDecimalValueFromString("437545.537222");
    ASSERT_EQ(VALUE_TYPE_DECIMAL, ValuePeeker::peekValueType(product));
    ASSERT_EQ(ValuePeeker::peekDecimal(product),
              ValuePeeker::peekDecimal(ans));

    // int * decimal
    product = lhs.op_multiply(rhs);
    ASSERT_EQ(VALUE_TYPE_DECIMAL, ValuePeeker::peekValueType(product));
    ASSERT_EQ(ValuePeeker::peekDecimal(product),
              ValuePeeker::peekDecimal(ans));

    // decimal * decimal
    lhs = ValueFactory::getDecimalValueFromString("2");
    product = rhs.op_multiply(lhs);
    ans = ValueFactory::getDecimalValueFromString("437545.537222");
    ASSERT_EQ(VALUE_TYPE_DECIMAL, ValuePeeker::peekValueType(product));
    ASSERT_EQ(ValuePeeker::peekDecimal(product),
              ValuePeeker::peekDecimal(ans));

    // decimal * (decimal < 1)
    lhs = ValueFactory::getDecimalValueFromString("0.21");
    product = rhs.op_multiply(lhs);
    ans = ValueFactory::getDecimalValueFromString("45942.281408310");
    ASSERT_EQ(VALUE_TYPE_DECIMAL, ValuePeeker::peekValueType(product));
    ASSERT_EQ(ValuePeeker::peekDecimal(product),
              ValuePeeker::peekDecimal(ans));

    // decimal that must be rescaled
    rhs = ValueFactory::getDecimalValueFromString("218772.11111111");
    lhs = ValueFactory::getDecimalValueFromString("2.001");
    product = rhs.op_multiply(lhs);
    ans = ValueFactory::getDecimalValueFromString("437762.99433333111");
    //    cout << "\nlhs " << lhs.debug() << endl;
    //    cout << "rhs " << rhs.debug() << endl;
    //    cout << "answer " << ans.debug() << endl;
    //    cout << "sum    " << sum.debug() << endl;

    // can't produce the answer as a double to compare directly
    ASSERT_EQ(VALUE_TYPE_DECIMAL, ValuePeeker::peekValueType(product));
    ASSERT_EQ(ValuePeeker::peekDecimal(product),
              ValuePeeker::peekDecimal(ans));

    //Overflow
    rhs = ValueFactory::getDecimalValueFromString("9999999999"  //10 digits
                                       "9999999999"   //20 digits
                                       "999999.9999"   //30 digits
                                       "99999999");
    lhs = ValueFactory::getDecimalValueFromString("2");

    bool caughtException = false;
    try {
        lhs.op_multiply(rhs);
    } catch (...) {
        caughtException = true;
    }
    ASSERT_TRUE(caughtException);

    //Underflow
    rhs = ValueFactory::getDecimalValueFromString("9999999999"  //10 digits
                                       "9999999999"   //20 digits
                                       "999999.9999"   //30 digits
                                       "99999999");
    lhs = ValueFactory::getDecimalValueFromString("-2");

    caughtException = false;
    try {
        lhs.op_multiply(rhs);
    } catch (...) {
        caughtException = true;
    }
    ASSERT_TRUE(caughtException);
}

TEST_F(NValueTest, DecimalQuotients) {

   NValue rhs;
   NValue lhs;
   NValue quo;
   NValue ans;

   rhs = ValueFactory::getDecimalValueFromString("200");
   lhs = ValueFactory::getDecimalValueFromString("5");
   quo = rhs.op_divide(lhs);
   ans = ValueFactory::getDecimalValueFromString("40");
   ASSERT_EQ(VALUE_TYPE_DECIMAL, ValuePeeker::peekValueType(quo));
   ASSERT_EQ(ValuePeeker::peekDecimal(quo),
             ValuePeeker::peekDecimal(ans));

   rhs = ValueFactory::getDecimalValueFromString("4003");
   lhs = ValueFactory::getDecimalValueFromString("20");
   quo = rhs.op_divide(lhs);
   ans = ValueFactory::getDecimalValueFromString("200.15");
   ASSERT_EQ(VALUE_TYPE_DECIMAL, ValuePeeker::peekValueType(quo));
   ASSERT_EQ(ValuePeeker::peekDecimal(quo),
             ValuePeeker::peekDecimal(ans));

   rhs = ValueFactory::getDecimalValueFromString("10");
   lhs = ValueFactory::getDecimalValueFromString("3");
   quo = rhs.op_divide(lhs);
   ans = ValueFactory::getDecimalValueFromString("3.333333333333");
   ASSERT_EQ(VALUE_TYPE_DECIMAL, ValuePeeker::peekValueType(quo));
   ASSERT_EQ(ValuePeeker::peekDecimal(quo),
             ValuePeeker::peekDecimal(ans));

   // sql coverage generated this... and it didn't work
   rhs = ValueFactory::getDecimalValueFromString("284534.796411");
   lhs = ValueFactory::getDecimalValueFromString("6");
   quo = rhs.op_divide(lhs);
   ans = ValueFactory::getDecimalValueFromString("47422.4660685");
   ASSERT_EQ(VALUE_TYPE_DECIMAL, ValuePeeker::peekValueType(quo));
   ASSERT_EQ(ValuePeeker::peekDecimal(quo),
             ValuePeeker::peekDecimal(ans));

   rhs = ValueFactory::getDecimalValueFromString("1");
   lhs = ValueFactory::getDecimalValueFromString("3000");
   quo = rhs.op_divide(lhs);
   ans = ValueFactory::getDecimalValueFromString("0.000333333333");
   ASSERT_EQ(VALUE_TYPE_DECIMAL, ValuePeeker::peekValueType(quo));
   ASSERT_EQ(ValuePeeker::peekDecimal(quo),
             ValuePeeker::peekDecimal(ans));

   rhs = ValueFactory::getDecimalValueFromString("1");
   lhs = ValueFactory::getDecimalValueFromString("300");
   quo = rhs.op_divide(lhs);
   ans = ValueFactory::getDecimalValueFromString("0.003333333333");
   ASSERT_EQ(VALUE_TYPE_DECIMAL, ValuePeeker::peekValueType(quo));
   ASSERT_EQ(ValuePeeker::peekDecimal(quo),
             ValuePeeker::peekDecimal(ans));

   rhs = ValueFactory::getDecimalValueFromString("1");
   lhs = ValueFactory::getDecimalValueFromString("30");
   quo = rhs.op_divide(lhs);
   ans = ValueFactory::getDecimalValueFromString("0.033333333333");
   ASSERT_EQ(VALUE_TYPE_DECIMAL, ValuePeeker::peekValueType(quo));
   ASSERT_EQ(ValuePeeker::peekDecimal(quo),
             ValuePeeker::peekDecimal(ans));

   rhs = ValueFactory::getDecimalValueFromString("1");
   lhs = ValueFactory::getDecimalValueFromString("-3");
   quo = rhs.op_divide(lhs);
   ans = ValueFactory::getDecimalValueFromString("-0.333333333333");
   ASSERT_EQ(VALUE_TYPE_DECIMAL, ValuePeeker::peekValueType(quo));
   ASSERT_EQ(ValuePeeker::peekDecimal(quo),
             ValuePeeker::peekDecimal(ans));

   rhs = ValueFactory::getDecimalValueFromString("-.0001");
   lhs = ValueFactory::getDecimalValueFromString(".0003");
   quo = rhs.op_divide(lhs);
   ans = ValueFactory::getDecimalValueFromString("-0.333333333333");
   ASSERT_EQ(VALUE_TYPE_DECIMAL, ValuePeeker::peekValueType(quo));
   ASSERT_EQ(ValuePeeker::peekDecimal(quo),
             ValuePeeker::peekDecimal(ans));


   rhs = ValueFactory::getDecimalValueFromString("-.5555");
   lhs = ValueFactory::getDecimalValueFromString("-.11");
   quo = rhs.op_divide(lhs);
   ans = ValueFactory::getDecimalValueFromString("5.05");
   ASSERT_EQ(VALUE_TYPE_DECIMAL, ValuePeeker::peekValueType(quo));
   ASSERT_EQ(ValuePeeker::peekDecimal(quo),
             ValuePeeker::peekDecimal(ans));

   rhs = ValueFactory::getDecimalValueFromString("0.11");
   lhs = ValueFactory::getDecimalValueFromString("0.55");
   quo = rhs.op_divide(lhs);
   ans = ValueFactory::getDecimalValueFromString("0.2");
   ASSERT_EQ(VALUE_TYPE_DECIMAL, ValuePeeker::peekValueType(quo));
   ASSERT_EQ(ValuePeeker::peekDecimal(quo),
             ValuePeeker::peekDecimal(ans));

   rhs = ValueFactory::getDecimalValueFromString("0");
   lhs = ValueFactory::getDecimalValueFromString("0.55");
   quo = rhs.op_divide(lhs);
   ans = ValueFactory::getDecimalValueFromString("0");
   ASSERT_EQ(VALUE_TYPE_DECIMAL, ValuePeeker::peekValueType(quo));
   ASSERT_EQ(ValuePeeker::peekDecimal(quo),
             ValuePeeker::peekDecimal(ans));

   try {
       rhs = ValueFactory::getDecimalValueFromString("1");
       lhs = ValueFactory::getDecimalValueFromString("0");
       quo = rhs.op_divide(lhs);
       ASSERT_EQ(0, 1);
   }
   catch (voltdb::SQLException& e) {
       ASSERT_EQ(1,1);
   }
}

TEST_F(NValueTest, SerializeToExport)
{
    // test basic nvalue elt serialization. Note that
    // NULL values and buffer length checking are done
    // before this primitive function.

    NValue nv;

    // a plenty-large-buffer(tm)
    char buf[1024];
    ExportSerializeInput sin(buf, 1024);
    ExportSerializeOutput out(buf, 1024);

    // tinyint
    nv = ValueFactory::getTinyIntValue(-50);
    nv.serializeToExport(out);
    EXPECT_EQ(8, out.position());
    EXPECT_EQ(-50, sin.readLong());
    sin.unread(out.position());
    out.position(0);

    nv = ValueFactory::getTinyIntValue(0);
    nv.serializeToExport(out);
    EXPECT_EQ(8, out.position());
    EXPECT_EQ(0, sin.readLong());
    sin.unread(out.position());
    out.position(0);

    nv = ValueFactory::getTinyIntValue(50);
    nv.serializeToExport(out);
    EXPECT_EQ(8, out.position());
    EXPECT_EQ(50, sin.readLong());
    sin.unread(out.position());
    out.position(0);

    // smallint
    nv = ValueFactory::getSmallIntValue(-128);
    nv.serializeToExport(out);
    EXPECT_EQ(8, out.position());
    EXPECT_EQ(-128, sin.readLong());
    sin.unread(out.position());
    out.position(0);

    nv = ValueFactory::getSmallIntValue(0);
    nv.serializeToExport(out);
    EXPECT_EQ(8, out.position());
    EXPECT_EQ(0, sin.readLong());
    sin.unread(out.position());
    out.position(0);

    nv = ValueFactory::getSmallIntValue(128);
    nv.serializeToExport(out);
    EXPECT_EQ(8, out.position());
    EXPECT_EQ(128, sin.readLong());
    sin.unread(out.position());
    out.position(0);

    // int
    nv = ValueFactory::getIntegerValue(-4999999);
    nv.serializeToExport(out);
    EXPECT_EQ(8, out.position());
    EXPECT_EQ(-4999999, sin.readLong());
    sin.unread(out.position());
    out.position(0);

    nv = ValueFactory::getIntegerValue(0);
    nv.serializeToExport(out);
    EXPECT_EQ(8, out.position());
    EXPECT_EQ(0, sin.readLong());
    sin.unread(out.position());
    out.position(0);

    nv = ValueFactory::getIntegerValue(128);
    nv.serializeToExport(out);
    EXPECT_EQ(8, out.position());
    EXPECT_EQ(128, sin.readLong());
    sin.unread(out.position());
    out.position(0);

    // bigint
    nv = ValueFactory::getBigIntValue(-4999999);
    nv.serializeToExport(out);
    EXPECT_EQ(8, out.position());
    EXPECT_EQ(-4999999, sin.readLong());
    sin.unread(out.position());
    out.position(0);

    nv = ValueFactory::getBigIntValue(0);
    nv.serializeToExport(out);
    EXPECT_EQ(8, out.position());
    EXPECT_EQ(0, sin.readLong());
    sin.unread(out.position());
    out.position(0);

    nv = ValueFactory::getBigIntValue(128);
    nv.serializeToExport(out);
    EXPECT_EQ(8, out.position());
    EXPECT_EQ(128, sin.readLong());
    sin.unread(out.position());
    out.position(0);

    // timestamp
    nv = ValueFactory::getTimestampValue(99999999);
    nv.serializeToExport(out);
    EXPECT_EQ(8, out.position());
    EXPECT_EQ(99999999, sin.readLong());
    sin.unread(out.position());
    out.position(0);

    // double
    nv = ValueFactory::getDoubleValue(-5.5555);
    nv.serializeToExport(out);
    EXPECT_EQ(8, out.position());
    EXPECT_EQ(-5.5555, sin.readDouble());
    sin.unread(out.position());
    out.position(0);

    nv = ValueFactory::getDoubleValue(0.0);
    nv.serializeToExport(out);
    EXPECT_EQ(8, out.position());
    EXPECT_EQ(0.0, sin.readDouble());
    sin.unread(out.position());
    out.position(0);

    nv = ValueFactory::getDoubleValue(128.256);
    nv.serializeToExport(out);
    EXPECT_EQ(8, out.position());
    EXPECT_EQ(128.256, sin.readDouble());
    sin.unread(out.position());
    out.position(0);

    // varchar
    nv = ValueFactory::getStringValue("ABCDEFabcdef");
    nv.serializeToExport(out);
    nv.free();
    EXPECT_EQ(12 + 4, out.position());         // chardata plus prefix
    EXPECT_EQ(12, sin.readInt()); // 32 bit length prefix
    EXPECT_EQ('A', sin.readChar());
    EXPECT_EQ('B', sin.readChar());
    EXPECT_EQ('C', sin.readChar());
    EXPECT_EQ('D', sin.readChar());
    EXPECT_EQ('E', sin.readChar());
    EXPECT_EQ('F', sin.readChar());
    EXPECT_EQ('a', sin.readChar());
    EXPECT_EQ('b', sin.readChar());
    EXPECT_EQ('c', sin.readChar());
    EXPECT_EQ('d', sin.readChar());
    EXPECT_EQ('e', sin.readChar());
    EXPECT_EQ('f', sin.readChar());
    sin.unread(out.position());
    out.position(0);

    // decimal
    nv = ValueFactory::getDecimalValueFromString("-1234567890.456123000000");
    nv.serializeToExport(out);
    EXPECT_EQ(24 + 4, out.position());
    EXPECT_EQ(24, sin.readInt()); // 32 bit length prefix
    EXPECT_EQ('-', sin.readChar());
    EXPECT_EQ('1', sin.readChar());
    EXPECT_EQ('2', sin.readChar());
    EXPECT_EQ('3', sin.readChar());
    EXPECT_EQ('4', sin.readChar());
    EXPECT_EQ('5', sin.readChar());
    EXPECT_EQ('6', sin.readChar());
    EXPECT_EQ('7', sin.readChar());
    EXPECT_EQ('8', sin.readChar());
    EXPECT_EQ('9', sin.readChar());
    EXPECT_EQ('0', sin.readChar());
    EXPECT_EQ('.', sin.readChar());
    EXPECT_EQ('4', sin.readChar());
    EXPECT_EQ('5', sin.readChar());
    EXPECT_EQ('6', sin.readChar());
    EXPECT_EQ('1', sin.readChar());
    EXPECT_EQ('2', sin.readChar());
    EXPECT_EQ('3', sin.readChar());
    EXPECT_EQ('0', sin.readChar());
    EXPECT_EQ('0', sin.readChar());
    EXPECT_EQ('0', sin.readChar());
    EXPECT_EQ('0', sin.readChar());
    EXPECT_EQ('0', sin.readChar());
    EXPECT_EQ('0', sin.readChar());
    sin.unread(out.position());
    out.position(0);
}

int main() {
    return TestSuite::globalInstance()->runAll();
}
