/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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
#include "common/executorcontext.hpp"
#include "expressions/functionexpression.h"
#include "expressions/expressionutil.h"
#include "expressions/constantvalueexpression.h"

#include <cfloat>
#include <limits>

using namespace std;
using namespace voltdb;

static const int64_t scale = 1000000000000;
static const double floatDelta = 0.000000000001;

class NValueTest : public Test {
    ThreadLocalPool m_pool;
public:
    string str;
    ValueType vt;
    TTInt value;
    NValue nv;
    int64_t scaledValue;
    int64_t scaledDirect;
    NValue viaDouble;
    NValue lower;
    NValue upper;

    void deserDecHelper()
    {
        vt    = ValuePeeker::peekValueType(nv);
        value = ValuePeeker::peekDecimal(nv);
        str   = ValuePeeker::peekDecimalString(nv);
    }

    void deserDecValidator(const char* textValue)
    {
        nv = ValueFactory::getDecimalValueFromString(textValue);
        deserDecHelper();
        NValue floatEquivalent = nv.castAs(VALUE_TYPE_DOUBLE);
        double floatValue = ValuePeeker::peekDouble(floatEquivalent);
        floatValue *= static_cast<double>(scale);
        scaledValue = (int64_t)floatValue;
        double floatDirect = atof(textValue);
        viaDouble = ValueFactory::getDoubleValue(floatDirect).castAs(VALUE_TYPE_DECIMAL);
        lower = ValueFactory::getDoubleValue(floatDirect - floatDelta);
        upper = ValueFactory::getDoubleValue(floatDirect + floatDelta);
        scaledDirect = (int64_t)(floatDirect * scale);
    }


};

TEST_F(NValueTest, DeserializeDecimal)
{
    deserDecValidator("6.0000000");
    ASSERT_FALSE(nv.isNull());
    ASSERT_EQ(vt, VALUE_TYPE_DECIMAL);
    ASSERT_TRUE(viaDouble.compare(nv) == 0);
    ASSERT_TRUE(lower.compare(nv) < 0);
    ASSERT_TRUE(nv.compare(lower) > 0);
    ASSERT_TRUE(upper.compare(nv) > 0);
    ASSERT_TRUE(nv.compare(upper) < 0);
    ASSERT_EQ(scaledValue, scaledDirect);
    ASSERT_EQ(value, TTInt("6000000000000"));
    ASSERT_EQ(str, "6.000000000000");

    deserDecValidator("-0");
    ASSERT_FALSE(nv.isNull());
    ASSERT_EQ(vt, VALUE_TYPE_DECIMAL);
    ASSERT_TRUE(viaDouble.compare(nv) == 0);
    ASSERT_TRUE(lower.compare(nv) < 0);
    ASSERT_TRUE(nv.compare(lower) > 0);
    ASSERT_TRUE(upper.compare(nv) > 0);
    ASSERT_TRUE(nv.compare(upper) < 0);
    ASSERT_EQ(scaledValue, scaledDirect);
    ASSERT_EQ(value, TTInt(0));
    // Decimals in Volt are currently hardwired with 12 fractional
    // decimal places.
    ASSERT_EQ(str, "0.000000000000");

    deserDecValidator("0");
    ASSERT_FALSE(nv.isNull());
    ASSERT_EQ(vt, VALUE_TYPE_DECIMAL);
    ASSERT_TRUE(viaDouble.compare(nv) == 0);
    ASSERT_TRUE(lower.compare(nv) < 0);
    ASSERT_TRUE(nv.compare(lower) > 0);
    ASSERT_TRUE(upper.compare(nv) > 0);
    ASSERT_TRUE(nv.compare(upper) < 0);
    ASSERT_EQ(value, TTInt(0));
    ASSERT_EQ(str, "0.000000000000");

    deserDecValidator("0.0");
    ASSERT_FALSE(nv.isNull());
    ASSERT_EQ(vt, VALUE_TYPE_DECIMAL);
    ASSERT_TRUE(viaDouble.compare(nv) == 0);
    ASSERT_TRUE(lower.compare(nv) < 0);
    ASSERT_TRUE(nv.compare(lower) > 0);
    ASSERT_TRUE(upper.compare(nv) > 0);
    ASSERT_TRUE(nv.compare(upper) < 0);
    ASSERT_EQ(value, TTInt(0));
    ASSERT_EQ(str, "0.000000000000");

    deserDecValidator("1");
    ASSERT_FALSE(nv.isNull());
    ASSERT_EQ(vt, VALUE_TYPE_DECIMAL);
    ASSERT_TRUE(viaDouble.compare(nv) == 0);
    ASSERT_TRUE(lower.compare(nv) < 0);
    ASSERT_TRUE(nv.compare(lower) > 0);
    ASSERT_TRUE(upper.compare(nv) > 0);
    ASSERT_TRUE(nv.compare(upper) < 0);
    ASSERT_EQ(value, TTInt("1000000000000"));
    ASSERT_EQ(str, "1.000000000000");

    deserDecValidator("-1");
    ASSERT_FALSE(nv.isNull());
    ASSERT_EQ(vt, VALUE_TYPE_DECIMAL);
    ASSERT_TRUE(viaDouble.compare(nv) == 0);
    ASSERT_TRUE(lower.compare(nv) < 0);
    ASSERT_TRUE(nv.compare(lower) > 0);
    ASSERT_TRUE(upper.compare(nv) > 0);
    ASSERT_TRUE(nv.compare(upper) < 0);
    ASSERT_EQ(value, TTInt("-1000000000000"));
    ASSERT_EQ(str, "-1.000000000000");

    // min value
    nv = ValueFactory::getDecimalValueFromString("-9999999999"  //10 digits
                                       "9999999999"   //20 digits
                                       "999999.9999"   //30 digits
                                       "99999999");   //38 digits
    deserDecHelper();
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
    deserDecHelper();
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

    deserDecValidator("1234");
    ASSERT_FALSE(nv.isNull());
    ASSERT_EQ(vt, VALUE_TYPE_DECIMAL);
    ASSERT_TRUE(viaDouble.compare(nv) == 0);
    ASSERT_TRUE(lower.compare(nv) < 0);
    ASSERT_TRUE(nv.compare(lower) > 0);
    ASSERT_TRUE(upper.compare(nv) > 0);
    ASSERT_TRUE(nv.compare(upper) < 0);
    ASSERT_EQ(value, TTInt(1234 * scale));
    ASSERT_EQ(str, "1234.000000000000");

    deserDecValidator("12.34");
    ASSERT_FALSE(nv.isNull());
    ASSERT_EQ(vt, VALUE_TYPE_DECIMAL);
    ASSERT_TRUE(viaDouble.compare(nv) == 0);
    ASSERT_TRUE(lower.compare(nv) < 0);
    ASSERT_TRUE(nv.compare(lower) > 0);
    ASSERT_TRUE(upper.compare(nv) > 0);
    ASSERT_TRUE(nv.compare(upper) < 0);
    ASSERT_EQ(value, TTInt(static_cast<int64_t>(12340000000000)));
    ASSERT_EQ(str, "12.340000000000");

    deserDecValidator("-1234");
    ASSERT_FALSE(nv.isNull());
    ASSERT_EQ(vt, VALUE_TYPE_DECIMAL);
    ASSERT_TRUE(viaDouble.compare(nv) == 0);
    ASSERT_TRUE(lower.compare(nv) < 0);
    ASSERT_TRUE(nv.compare(lower) > 0);
    ASSERT_TRUE(upper.compare(nv) > 0);
    ASSERT_TRUE(nv.compare(upper) < 0);
    ASSERT_EQ(value, TTInt(-1234 * scale));
    ASSERT_EQ(str, "-1234.000000000000");

    deserDecValidator("-12.34");
    ASSERT_FALSE(nv.isNull());
    ASSERT_EQ(vt, VALUE_TYPE_DECIMAL);
    ASSERT_TRUE(viaDouble.compare(nv) == 0);
    ASSERT_TRUE(lower.compare(nv) < 0);
    ASSERT_TRUE(nv.compare(lower) > 0);
    ASSERT_TRUE(upper.compare(nv) > 0);
    ASSERT_TRUE(nv.compare(upper) < 0);
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

    NValue decimalCastToBigInt = ValueFactory::castAsBigInt(decimalValue);
    EXPECT_EQ(ValuePeeker::peekBigInt(decimalCastToBigInt), 10);

    bool caught = false;
    try
    {
        NValue stringCastToBigInt = ValueFactory::castAsBigInt(stringValue);
        stringCastToBigInt.debug(); // This expected dead code is a harmless way to avoid unused variable warnings.
    }
    catch (SQLException& ex)
    {
        caught = true;
    }
    EXPECT_TRUE(caught);

    //
    // Now run a series of tests to make sure that out of range casts fail
    // For BigInt only a double can be out of range.
    //
    NValue doubleOutOfRangeH = ValueFactory::getDoubleValue(92233720368547075809.0);
    NValue doubleOutOfRangeL = ValueFactory::getDoubleValue(-92233720368547075809.0);

    caught = false;
    try
    {
        NValue doubleCastToBigInt = ValueFactory::castAsBigInt(doubleOutOfRangeH);
        doubleCastToBigInt.debug(); // This expected dead code is a harmless way to avoid unused variable warnings.
    } catch (SQLException& ex)
    {
        caught = true;
    }
    EXPECT_TRUE(caught);

    caught = false;
    try
    {
        NValue doubleCastToBigInt = ValueFactory::castAsBigInt(doubleOutOfRangeL);
        doubleCastToBigInt.debug(); // This expected dead code is a harmless way to avoid unused variable warnings.
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

    NValue decimalCastToInteger = ValueFactory::castAsInteger(decimalValue);
    EXPECT_EQ(ValuePeeker::peekInteger(decimalCastToInteger), 10);

    bool caught = false;
    try
    {
        NValue stringCast = ValueFactory::castAsInteger(stringValue);
        stringCast.debug(); // This expected dead code is a harmless way to avoid unused variable warnings.
    }
    catch (SQLException& ex)
    {
        caught = true;
    }
    EXPECT_TRUE(caught);

    //
    // Now run a series of tests to make sure that out of range casts fail
    // For Integer only a double and BigInt can be out of range.
    //
    NValue doubleOutOfRangeH = ValueFactory::getDoubleValue(92233720368547075809.0);
    NValue doubleOutOfRangeL = ValueFactory::getDoubleValue(-92233720368547075809.0);
    caught = false;
    try
    {
        NValue doubleCastToInteger = ValueFactory::castAsInteger(doubleOutOfRangeH);
        doubleCastToInteger.debug(); // This expected dead code is a harmless way to avoid unused variable warnings.
    } catch (SQLException& ex)
    {
        caught = true;
    }
    EXPECT_TRUE(caught);

    caught = false;
    try
    {
        NValue doubleCastToInteger = ValueFactory::castAsInteger(doubleOutOfRangeL);
        doubleCastToInteger.debug(); // This expected dead code is a harmless way to avoid unused variable warnings.
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
        bigIntCastToInteger.debug(); // This expected dead code is a harmless way to avoid unused variable warnings.
    } catch (SQLException& ex)
    {
        caught = true;
    }
    EXPECT_TRUE(caught);

    caught = false;
    try
    {
        NValue bigIntCastToInteger = ValueFactory::castAsInteger(bigIntOutOfRangeL);
        bigIntCastToInteger.debug(); // This expected dead code is a harmless way to avoid unused variable warnings.
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

    NValue decimalCastToSmallInt = ValueFactory::castAsSmallInt(decimalValue);
    EXPECT_EQ(ValuePeeker::peekSmallInt(decimalCastToSmallInt), 10);

    bool caught = false;
    try
    {
        NValue stringCast = ValueFactory::castAsSmallInt(stringValue);
        stringCast.debug(); // This expected dead code is a harmless way to avoid unused variable warnings.
    }
    catch (SQLException& ex)
    {
        caught = true;
    }
    EXPECT_TRUE(caught);

    //
    // Now run a series of tests to make sure that out of range casts fail
    // For SmallInt only a double, BigInt, and Integer can be out of range.
    //
    NValue doubleOutOfRangeH = ValueFactory::getDoubleValue(92233720368547075809.0);
    NValue doubleOutOfRangeL = ValueFactory::getDoubleValue(-92233720368547075809.0);
    caught = false;
    try
    {
        NValue doubleCastToSmallInt = ValueFactory::castAsSmallInt(doubleOutOfRangeH);
        doubleCastToSmallInt.debug(); // This expected dead code is a harmless way to avoid unused variable warnings.
    } catch (SQLException& ex)
    {
        caught = true;
    }
    EXPECT_TRUE(caught);

    caught = false;
    try
    {
        NValue doubleCastToSmallInt = ValueFactory::castAsSmallInt(doubleOutOfRangeL);
        doubleCastToSmallInt.debug(); // This expected dead code is a harmless way to avoid unused variable warnings.
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
        bigIntCastToSmallInt.debug(); // This expected dead code is a harmless way to avoid unused variable warnings.
    } catch (SQLException& ex)
    {
        caught = true;
    }
    EXPECT_TRUE(caught);

    caught = false;
    try
    {
        NValue bigIntCastToSmallInt = ValueFactory::castAsSmallInt(bigIntOutOfRangeL);
        bigIntCastToSmallInt.debug(); // This expected dead code is a harmless way to avoid unused variable warnings.
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
        integerCastToSmallInt.debug(); // This expected dead code is a harmless way to avoid unused variable warnings.
    } catch (SQLException& ex)
    {
        caught = true;
    }
    EXPECT_TRUE(caught);

    caught = false;
    try
    {
        NValue integerCastToSmallInt = ValueFactory::castAsSmallInt(integerOutOfRangeL);
        integerCastToSmallInt.debug(); // This expected dead code is a harmless way to avoid unused variable warnings.
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

    NValue decimalCastToTinyInt = ValueFactory::castAsTinyInt(decimalValue);
    EXPECT_EQ(ValuePeeker::peekTinyInt(decimalCastToTinyInt), 10);

    bool caught = false;
    try
    {
        NValue stringCast = ValueFactory::castAsTinyInt(stringValue);
        stringCast.debug(); // This expected dead code is a harmless way to avoid unused variable warnings.
    }
    catch (SQLException& ex)
    {
        caught = true;
    }
    EXPECT_TRUE(caught);

    //
    // Now run a series of tests to make sure that out of range casts fail
    // For TinyInt only a double, BigInt, Integer, and SmallInt can be out of range.
    //
    NValue doubleOutOfRangeH = ValueFactory::getDoubleValue(92233720368547075809.0);
    NValue doubleOutOfRangeL = ValueFactory::getDoubleValue(-92233720368547075809.0);
    caught = false;
    try
    {
        NValue doubleCastToTinyInt = ValueFactory::castAsTinyInt(doubleOutOfRangeH);
        doubleCastToTinyInt.debug(); // This expected dead code is a harmless way to avoid unused variable warnings.
    } catch (SQLException& ex)
    {
        caught = true;
    }
    EXPECT_TRUE(caught);

    caught = false;
    try
    {
        NValue doubleCastToTinyInt = ValueFactory::castAsTinyInt(doubleOutOfRangeL);
        doubleCastToTinyInt.debug(); // This expected dead code is a harmless way to avoid unused variable warnings.
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
        bigIntCastToTinyInt.debug(); // This expected dead code is a harmless way to avoid unused variable warnings.
    } catch (SQLException& ex)
    {
        caught = true;
    }
    EXPECT_TRUE(caught);

    caught = false;
    try
    {
        NValue bigIntCastToTinyInt = ValueFactory::castAsTinyInt(bigIntOutOfRangeL);
        bigIntCastToTinyInt.debug(); // This expected dead code is a harmless way to avoid unused variable warnings.
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
        integerCastToTinyInt.debug(); // This expected dead code is a harmless way to avoid unused variable warnings.
    } catch (SQLException& ex)
    {
        caught = true;
    }
    EXPECT_TRUE(caught);

    caught = false;
    try
    {
        NValue integerCastToTinyInt = ValueFactory::castAsTinyInt(integerOutOfRangeL);
        integerCastToTinyInt.debug(); // This expected dead code is a harmless way to avoid unused variable warnings.
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
        smallIntCastToTinyInt.debug(); // This expected dead code is a harmless way to avoid unused variable warnings.
    } catch (SQLException& ex)
    {
        caught = true;
    }
    EXPECT_TRUE(caught);

    caught = false;
    try
    {
        NValue smallIntCastToTinyInt = ValueFactory::castAsTinyInt(smallIntOutOfRangeL);
        smallIntCastToTinyInt.debug(); // This expected dead code is a harmless way to avoid unused variable warnings.
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

    NValue decimalCastToDouble = ValueFactory::castAsDouble(decimalValue);
    EXPECT_LT(ValuePeeker::peekDouble(decimalCastToDouble), 10.221);
    EXPECT_GT(ValuePeeker::peekDouble(decimalCastToDouble), 10.219);

    bool caught = false;
    try
    {
        NValue stringCast = ValueFactory::castAsDouble(stringValue);
        stringCast.debug(); // This expected dead code is a harmless way to avoid unused variable warnings.
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
    assert(ExecutorContext::getExecutorContext() == NULL);
    Pool* testPool = new Pool();
    UndoQuantum* wantNoQuantum = NULL;
    Topend* topless = NULL;
    ExecutorContext* poolHolder = new ExecutorContext(0, 0, wantNoQuantum, topless, testPool, NULL, "", 0, NULL);

    NValue tinyInt = ValueFactory::getTinyIntValue(120);
    NValue smallInt = ValueFactory::getSmallIntValue(120);
    NValue integer = ValueFactory::getIntegerValue(120);
    NValue bigInt = ValueFactory::getBigIntValue(-64);
    NValue doubleValue = ValueFactory::getDoubleValue(-32);
    NValue stringValue = ValueFactory::getStringValue("dude");
    NValue decimalValue = ValueFactory::getDecimalValueFromString("10.22");


    NValue bigIntCastToString = ValueFactory::castAsString(bigInt);
    std::string bigIntPeekedString = ValuePeeker::peekStringCopy_withoutNull(bigIntCastToString);
    EXPECT_EQ(strcmp(bigIntPeekedString.c_str(), "-64"), 0);

    NValue integerCastToString = ValueFactory::castAsString(integer);
    std::string integerPeekedString = ValuePeeker::peekStringCopy_withoutNull(integerCastToString);
    EXPECT_EQ(strcmp(integerPeekedString.c_str(), "120"), 0);

    NValue smallIntCastToString = ValueFactory::castAsString(smallInt);
    std::string smallIntPeekedString = ValuePeeker::peekStringCopy_withoutNull(smallIntCastToString);
    EXPECT_EQ(strcmp(smallIntPeekedString.c_str(), "120"), 0);

    NValue tinyIntCastToString = ValueFactory::castAsString(tinyInt);
    std::string tinyIntPeekedString = ValuePeeker::peekStringCopy_withoutNull(tinyIntCastToString);
    EXPECT_EQ(strcmp(tinyIntPeekedString.c_str(), "120"), 0);

    NValue doubleCastToString = ValueFactory::castAsString(doubleValue);
    std::string doublePeekedString = ValuePeeker::peekStringCopy_withoutNull(doubleCastToString);
    EXPECT_EQ(strcmp(doublePeekedString.c_str(), "-3.2E1"), 0);

    NValue decimalCastToString = ValueFactory::castAsString(decimalValue);
    std::string decimalPeekedString = ValuePeeker::peekStringCopy_withoutNull(decimalCastToString);
    EXPECT_EQ(strcmp(decimalPeekedString.c_str(), "10.220000000000"), 0);

    NValue stringCastToString = ValueFactory::castAsString(stringValue);
    std::string stringPeekedString = ValuePeeker::peekStringCopy_withoutNull(stringCastToString);
    EXPECT_EQ(strcmp(stringPeekedString.c_str(), "dude"), 0);

    // Make valgrind happy
    stringValue.free();
    delete poolHolder;
    delete testPool;
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

    NValue castDouble = ValueFactory::castAsDecimal(doubleValue);

    EXPECT_EQ(0, decimalValue.compare(castDouble));

    bool caught = false;
    try
    {
        NValue cast = ValueFactory::castAsDecimal(stringValue);
        cast.debug(); // This expected dead code is a harmless way to avoid unused variable warnings.
    }
    catch (SQLException& ex)
    {
        caught = true;
    }
    EXPECT_TRUE(caught);

    // Make valgrind happy
    stringValue.free();

    //
    // Now run a series of tests to make sure that out of range casts fail
    // For Decimal, only a double can be out of range.
    //
    NValue doubleOutOfRangeH = ValueFactory::getDoubleValue(100000000000000000000000000.0);
    NValue doubleOutOfRangeL = ValueFactory::getDoubleValue(-100000000000000000000000000.0);
    caught = false;
    try
    {
        NValue doubleCastToDecimal = ValueFactory::castAsDecimal(doubleOutOfRangeH);
        doubleCastToDecimal.debug(); // This expected dead code is a harmless way to avoid unused variable warnings.
    } catch (SQLException& ex)
    {
        caught = true;
    }
    EXPECT_TRUE(caught);

    caught = false;
    try
    {
        NValue doubleCastToDecimal = ValueFactory::castAsDecimal(doubleOutOfRangeL);
        doubleCastToDecimal.debug(); // This expected dead code is a harmless way to avoid unused variable warnings.
    } catch (SQLException& ex)
    {
        caught = true;
    }
    EXPECT_TRUE(caught);
}

//
// Adding can only overflow BigInt since they are all cast to BigInt before addition takes place.
//
TEST_F(NValueTest, TestBigIntOpAddOverflow) {
    NValue lhs = ValueFactory::getBigIntValue(INT64_MAX - 10);
    NValue rhs = ValueFactory::getBigIntValue(INT32_MAX);
    bool caught = false;
    try {
        NValue result = lhs.op_add(rhs);
        result.debug(); // This expected dead code is a harmless way to avoid unused variable warnings.
    } catch (SQLException& ex) {
        caught = true;
    }
    EXPECT_TRUE(caught);

    lhs = ValueFactory::getBigIntValue(-(INT64_MAX - 10));
    rhs = ValueFactory::getBigIntValue(-INT32_MAX);
    caught = false;
    try {
        NValue result = lhs.op_add(rhs);
        result.debug(); // This expected dead code is a harmless way to avoid unused variable warnings.
    } catch (SQLException& ex) {
        caught = true;
    }
    EXPECT_TRUE(caught);

    //
    // Sanity check that yes indeed regular addition doesn't throw...
    //
    lhs = ValueFactory::getBigIntValue(1);
    rhs = ValueFactory::getBigIntValue(4);
    NValue result = lhs.op_add(rhs);
    result.debug(); // A harmless way to avoid unused variable warnings.
}

//
// Subtraction can only overflow BigInt since they are all cast to BigInt before addition takes place.
//
TEST_F(NValueTest, TestBigIntOpSubtractOverflow) {
    NValue lhs = ValueFactory::getBigIntValue(INT64_MAX - 10);
    NValue rhs = ValueFactory::getBigIntValue(-INT32_MAX);
    bool caught = false;
    try {
        NValue result = lhs.op_subtract(rhs);
        result.debug(); // This expected dead code is a harmless way to avoid unused variable warnings.
    } catch (SQLException& ex) {
        caught = true;
    }
    EXPECT_TRUE(caught);

    lhs = ValueFactory::getBigIntValue(INT64_MAX - 10);
    rhs = ValueFactory::getBigIntValue(-INT32_MAX);
    caught = false;
    try {
        NValue result = lhs.op_subtract(rhs);
        result.debug(); // This expected dead code is a harmless way to avoid unused variable warnings.
    } catch (SQLException& ex) {
        caught = true;
    }
    EXPECT_TRUE(caught);

    //
    // Sanity check that yes indeed regular subtraction doesn't throw...
    //
    lhs = ValueFactory::getBigIntValue(1);
    rhs = ValueFactory::getBigIntValue(4);
    NValue result = lhs.op_subtract(rhs);
    result.debug(); // This is a harmless way to avoid unused variable warnings.
}

//
// Multiplication can only overflow BigInt since they are all cast to BigInt before addition takes place.
//
TEST_F(NValueTest, TestBigIntOpMultiplyOverflow) {
    NValue lhs = ValueFactory::getBigIntValue(INT64_MAX);
    NValue rhs = ValueFactory::getBigIntValue(INT32_MAX);
    bool caught = false;
    try {
        NValue result = lhs.op_multiply(rhs);
        result.debug(); // This expected dead code is a harmless way to avoid unused variable warnings.
    } catch (SQLException& ex) {
        caught = true;
    }
    EXPECT_TRUE(caught);

    lhs = ValueFactory::getBigIntValue(-(INT64_MAX - 10));
    rhs = ValueFactory::getBigIntValue(INT32_MAX);
    caught = false;
    try {
        NValue result = lhs.op_multiply(rhs);
        result.debug(); // This expected dead code is a harmless way to avoid unused variable warnings.
    } catch (SQLException& ex) {
        caught = true;
    }
    EXPECT_TRUE(caught);

    lhs = ValueFactory::getBigIntValue(INT64_MAX - 10);
    rhs = ValueFactory::getBigIntValue(-INT32_MAX);
    caught = false;
    try {
        NValue result = lhs.op_multiply(rhs);
        result.debug(); // This expected dead code is a harmless way to avoid unused variable warnings.
    } catch (SQLException& ex) {
        caught = true;
    }
    EXPECT_TRUE(caught);

    lhs = ValueFactory::getBigIntValue(-(INT64_MAX - 10));
    rhs = ValueFactory::getBigIntValue(-INT32_MAX);
    caught = false;
    try {
        NValue result = lhs.op_multiply(rhs);
        result.debug(); // This expected dead code is a harmless way to avoid unused variable warnings.
    } catch (SQLException& ex) {
        caught = true;
    }
    EXPECT_TRUE(caught);

    //
    // Sanity check that yes indeed regular multiplication doesn't throw...
    //
    lhs = ValueFactory::getBigIntValue(1);
    rhs = ValueFactory::getBigIntValue(4);
    NValue result = lhs.op_multiply(rhs);
    result.debug(); // This is a harmless way to avoid unused variable warnings.
}


TEST_F(NValueTest, TestDoubleOpAddOverflow) {
    //Positive infinity
    NValue lhs = ValueFactory::getDoubleValue(std::numeric_limits<double>::max());
    NValue rhs = ValueFactory::getDoubleValue(std::numeric_limits<double>::max());
    bool caught = false;
    try {
        NValue result = lhs.op_add(rhs);
        result.debug(); // This expected dead code is a harmless way to avoid unused variable warnings.
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
        result.debug(); // This expected dead code is a harmless way to avoid unused variable warnings.
    } catch (SQLException& ex) {
        caught = true;
    }
    EXPECT_TRUE(caught);

    //
    // Sanity check that yes indeed regular addition doesn't throw...
    //
    lhs = ValueFactory::getDoubleValue(1);
    rhs = ValueFactory::getDoubleValue(4);
    NValue result = lhs.op_add(rhs);
        result.debug(); // This expected dead code is a harmless way to avoid unused variable warnings.
}

TEST_F(NValueTest, TestDoubleOpSubtractOverflow) {
    //Positive infinity
    NValue lhs = ValueFactory::getDoubleValue(std::numeric_limits<double>::max());
    NValue rhs = ValueFactory::getDoubleValue(-(std::numeric_limits<double>::max() * ((double).5)));
    bool caught = false;
    try {
        NValue result = lhs.op_subtract(rhs);
        result.debug(); // This expected dead code is a harmless way to avoid unused variable warnings.
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
        result.debug(); // This expected dead code is a harmless way to avoid unused variable warnings.
    } catch (SQLException& ex) {
        caught = true;
    }
    EXPECT_TRUE(caught);

    //
    // Sanity check that yes indeed regular subtraction doesn't throw...
    //
    lhs = ValueFactory::getDoubleValue(1.23);
    rhs = ValueFactory::getDoubleValue(4.2345346);
    NValue result = lhs.op_subtract(rhs);
        result.debug(); // This expected dead code is a harmless way to avoid unused variable warnings.
}

TEST_F(NValueTest, TestDoubleOpMultiplyOverflow) {
    //Positive infinity
    NValue lhs = ValueFactory::getDoubleValue(std::numeric_limits<double>::max());
    NValue rhs = ValueFactory::getDoubleValue(std::numeric_limits<double>::max());
    bool caught = false;
    try {
        NValue result = lhs.op_multiply(rhs);
        result.debug(); // This expected dead code is a harmless way to avoid unused variable warnings.
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
        result.debug(); // This expected dead code is a harmless way to avoid unused variable warnings.
    } catch (SQLException& ex) {
        caught = true;
    }
    EXPECT_TRUE(caught);

    //
    // Sanity check that yes indeed regular multiplication doesn't throw...
    //
    lhs = ValueFactory::getDoubleValue(1.23);
    rhs = ValueFactory::getDoubleValue(4.2345346);
    NValue result = lhs.op_multiply(rhs);
        result.debug(); // This expected dead code is a harmless way to avoid unused variable warnings.
}

TEST_F(NValueTest, TestDoubleOpDivideOverflow) {
    //Positive infinity
    NValue lhs = ValueFactory::getDoubleValue(std::numeric_limits<double>::max());
    NValue rhs = ValueFactory::getDoubleValue(std::numeric_limits<double>::min());
    bool caught = false;
    try {
        NValue result = lhs.op_divide(rhs);
        result.debug(); // This expected dead code is a harmless way to avoid unused variable warnings.
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
        result.debug(); // This expected dead code is a harmless way to avoid unused variable warnings.
    } catch (SQLException& ex) {
        caught = true;
    }
    EXPECT_TRUE(caught);

    //
    // Sanity check that yes indeed regular division doesn't throw...
    //
    lhs = ValueFactory::getDoubleValue(1.23);
    rhs = ValueFactory::getDoubleValue(4.2345346);
    NValue result = lhs.op_divide(rhs);
    result.debug(); // This is a harmless way to avoid unused variable warnings.
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
    EXPECT_TRUE(smallInt.compare(tinyInt) > 0);
    EXPECT_TRUE(integer.compare(smallInt) > 0);
    EXPECT_TRUE(bigInt.compare(integer) > 0);
    EXPECT_TRUE(tinyInt.compare(smallInt) < 0);
    EXPECT_TRUE(smallInt.compare(integer) < 0);
    EXPECT_TRUE(integer.compare(bigInt) < 0);
    EXPECT_TRUE(tinyInt.compare(floatVal) < 0);
    EXPECT_TRUE(smallInt.compare(floatVal) < 0);
    EXPECT_TRUE(integer.compare(floatVal) > 0);
    EXPECT_TRUE(bigInt.compare(floatVal) > 0);
    EXPECT_TRUE(floatVal.compare(bigInt) < 0);
    EXPECT_TRUE(floatVal.compare(integer) < 0);
    EXPECT_TRUE(floatVal.compare(smallInt) > 0);
    EXPECT_TRUE(floatVal.compare(tinyInt) > 0);

    tinyInt = ValueFactory::getTinyIntValue(-101);
    smallInt = ValueFactory::getSmallIntValue(-1001);
    integer = ValueFactory::getIntegerValue(-1000001);
    bigInt = ValueFactory::getBigIntValue(-10000000000001);
    floatVal = ValueFactory::getDoubleValue(-12000.456);
    EXPECT_TRUE(smallInt.compare(tinyInt) < 0);
    EXPECT_TRUE(integer.compare(smallInt) < 0);
    EXPECT_TRUE(bigInt.compare(integer) < 0);
    EXPECT_TRUE(tinyInt.compare(smallInt) > 0);
    EXPECT_TRUE(smallInt.compare(integer) > 0);
    EXPECT_TRUE(integer.compare(bigInt) > 0);
    EXPECT_TRUE(tinyInt.compare(floatVal) > 0);
    EXPECT_TRUE(smallInt.compare(floatVal) > 0);
    EXPECT_TRUE(integer.compare(floatVal) < 0);
    EXPECT_TRUE(bigInt.compare(floatVal) < 0);
    EXPECT_TRUE(floatVal.compare(bigInt) > 0);
    EXPECT_TRUE(floatVal.compare(integer) > 0);
    EXPECT_TRUE(floatVal.compare(smallInt) < 0);
    EXPECT_TRUE(floatVal.compare(tinyInt) < 0);
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
    nv.serializeToExport_withoutNull(out);
    EXPECT_EQ(1, out.position());
    EXPECT_EQ(-50, sin.readByte());
    sin.unread(out.position());
    out.position(0);

    nv = ValueFactory::getTinyIntValue(0);
    nv.serializeToExport_withoutNull(out);
    EXPECT_EQ(1, out.position());
    EXPECT_EQ(0, sin.readByte());
    sin.unread(out.position());
    out.position(0);

    nv = ValueFactory::getTinyIntValue(50);
    nv.serializeToExport_withoutNull(out);
    EXPECT_EQ(1, out.position());
    EXPECT_EQ(50, sin.readByte());
    sin.unread(out.position());
    out.position(0);

    // smallint
    nv = ValueFactory::getSmallIntValue(-128);
    nv.serializeToExport_withoutNull(out);
    EXPECT_EQ(2, out.position());
    EXPECT_EQ(-128, sin.readShort());
    sin.unread(out.position());
    out.position(0);

    nv = ValueFactory::getSmallIntValue(0);
    nv.serializeToExport_withoutNull(out);
    EXPECT_EQ(2, out.position());
    EXPECT_EQ(0, sin.readShort());
    sin.unread(out.position());
    out.position(0);

    nv = ValueFactory::getSmallIntValue(128);
    nv.serializeToExport_withoutNull(out);
    EXPECT_EQ(2, out.position());
    EXPECT_EQ(128, sin.readShort());
    sin.unread(out.position());
    out.position(0);

    // int
    nv = ValueFactory::getIntegerValue(-4999999);
    nv.serializeToExport_withoutNull(out);
    EXPECT_EQ(4, out.position());
    EXPECT_EQ(-4999999, sin.readInt());
    sin.unread(out.position());
    out.position(0);

    nv = ValueFactory::getIntegerValue(0);
    nv.serializeToExport_withoutNull(out);
    EXPECT_EQ(4, out.position());
    EXPECT_EQ(0, sin.readInt());
    sin.unread(out.position());
    out.position(0);

    nv = ValueFactory::getIntegerValue(128);
    nv.serializeToExport_withoutNull(out);
    EXPECT_EQ(4, out.position());
    EXPECT_EQ(128, sin.readInt());
    sin.unread(out.position());
    out.position(0);

    // bigint
    nv = ValueFactory::getBigIntValue(-4999999);
    nv.serializeToExport_withoutNull(out);
    EXPECT_EQ(8, out.position());
    EXPECT_EQ(-4999999, sin.readLong());
    sin.unread(out.position());
    out.position(0);

    nv = ValueFactory::getBigIntValue(0);
    nv.serializeToExport_withoutNull(out);
    EXPECT_EQ(8, out.position());
    EXPECT_EQ(0, sin.readLong());
    sin.unread(out.position());
    out.position(0);

    nv = ValueFactory::getBigIntValue(128);
    nv.serializeToExport_withoutNull(out);
    EXPECT_EQ(8, out.position());
    EXPECT_EQ(128, sin.readLong());
    sin.unread(out.position());
    out.position(0);

    // timestamp
    nv = ValueFactory::getTimestampValue(99999999);
    nv.serializeToExport_withoutNull(out);
    EXPECT_EQ(8, out.position());
    EXPECT_EQ(99999999, sin.readLong());
    sin.unread(out.position());
    out.position(0);

    // double
    nv = ValueFactory::getDoubleValue(-5.5555);
    nv.serializeToExport_withoutNull(out);
    EXPECT_EQ(8, out.position());
    EXPECT_EQ(-5.5555, sin.readDouble());
    sin.unread(out.position());
    out.position(0);

    nv = ValueFactory::getDoubleValue(0.0);
    nv.serializeToExport_withoutNull(out);
    EXPECT_EQ(8, out.position());
    EXPECT_EQ(0.0, sin.readDouble());
    sin.unread(out.position());
    out.position(0);

    nv = ValueFactory::getDoubleValue(128.256);
    nv.serializeToExport_withoutNull(out);
    EXPECT_EQ(8, out.position());
    EXPECT_EQ(128.256, sin.readDouble());
    sin.unread(out.position());
    out.position(0);

    // varchar
    nv = ValueFactory::getStringValue("ABCDEFabcdef");
    nv.serializeToExport_withoutNull(out);
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
    nv.serializeToExport_withoutNull(out);
    EXPECT_EQ(18, out.position());
    EXPECT_EQ(12, sin.readByte());//12 digit scale
    EXPECT_EQ(16, sin.readByte());//16 bytes of precision
    int64_t low = sin.readLong();
    low = ntohll(low);
    int64_t high = sin.readLong();
    high = ntohll(high);
    TTInt val = ValuePeeker::peekDecimal(nv);
    EXPECT_EQ(low, val.table[1]);
    EXPECT_EQ(high, val.table[0]);
    sin.unread(out.position());
    out.position(0);
}

TEST_F(NValueTest, TestLike)
{
    std::vector<const char *> testData;
    testData.push_back("aaaaaaa");
    testData.push_back("abcccc%");
    testData.push_back("abcdefg");
    testData.push_back("âxxxéyy");
    testData.push_back("â🀲x一xxéyyԱ");

    std::vector<const char *> testExpressions;
    std::vector<int> testMatches;

    testExpressions.push_back("aaa%"); testMatches.push_back(1);
    testExpressions.push_back("abc%"); testMatches.push_back(2);
    testExpressions.push_back("AbC%"); testMatches.push_back(0);
    testExpressions.push_back("zzz%"); testMatches.push_back(0);
    testExpressions.push_back("%"); testMatches.push_back(static_cast<int>(testData.size()));
    testExpressions.push_back("a%"); testMatches.push_back(3);
    testExpressions.push_back("âxxx%"); testMatches.push_back(1);
    testExpressions.push_back("aaaaaaa"); testMatches.push_back(1);
    testExpressions.push_back("aaa"); testMatches.push_back(0);
    testExpressions.push_back("abcdef_"); testMatches.push_back(1);
    testExpressions.push_back("ab_d_fg"); testMatches.push_back(1);
    testExpressions.push_back("%defg"); testMatches.push_back(1);
    testExpressions.push_back("%de%"); testMatches.push_back(1);
    testExpressions.push_back("%%g"); testMatches.push_back(1);
    testExpressions.push_back("%_a%"); testMatches.push_back(1);
    testExpressions.push_back("%__c%"); testMatches.push_back(2);
    testExpressions.push_back("a_%c%"); testMatches.push_back(2);
    //Take me down like i'm a domino
    testExpressions.push_back("â🀲x一xxéyyԱ"); testMatches.push_back(1);
    testExpressions.push_back("â_x一xxéyyԱ"); testMatches.push_back(1);
    testExpressions.push_back("â🀲x_xxéyyԱ"); testMatches.push_back(1);
    testExpressions.push_back("â🀲x一xxéyy_"); testMatches.push_back(1);
    testExpressions.push_back("â🀲x一xéyyԱ"); testMatches.push_back(0);

    for (int ii = 0; ii < testExpressions.size(); ii++) {
        const char *testExpression = testExpressions[ii];
        const int testMatch = testMatches[ii];
        int foundMatches = 0;

        voltdb::NValue pattern = voltdb::ValueFactory::getStringValue(testExpression);
        for (int jj = 0; jj < testData.size(); jj++) {
            const char *testDatum = testData[jj];
            NValue testString = voltdb::ValueFactory::getStringValue(testDatum);

            if (testString.like(pattern).isTrue()) {
                foundMatches++;
            }
            testString.free();
        }
        pattern.free();
        if (foundMatches != testMatch) {
            printf("Pattern %s failed to match %d, matched %d instead\n", testExpression, testMatch, foundMatches);
        }
        EXPECT_EQ( foundMatches, testMatch);
    }

    //
    // Test an edge case Paul noticed during his review
    // https://github.com/VoltDB/voltdb/pull/33#discussion_r926110
    //
    NValue value = voltdb::ValueFactory::getStringValue("XY");
    NValue pattern1 = voltdb::ValueFactory::getStringValue("X%_");
    NValue pattern2 = voltdb::ValueFactory::getStringValue("X%%");
    EXPECT_TRUE(value.like(pattern1).isTrue());
    EXPECT_TRUE(value.like(pattern2).isTrue());
    pattern2.free();
    pattern1.free();
    value.free();
}

TEST_F(NValueTest, TestSubstring)
{
    assert(ExecutorContext::getExecutorContext() == NULL);
    Pool* testPool = new Pool();
    UndoQuantum* wantNoQuantum = NULL;
    Topend* topless = NULL;
    ExecutorContext* poolHolder = new ExecutorContext(0, 0, wantNoQuantum, topless, testPool, NULL, "", 0, NULL);
    std::vector<std::string> testData;
    testData.push_back("abcdefg");
    testData.push_back("âbcdéfg");
    testData.push_back("â🀲c一éfԱ");

    NValue startAtOne = ValueFactory::getIntegerValue(1);
    NValue sureEnd = ValueFactory::getIntegerValue(7);
    for (int jj = 0; jj < testData.size(); jj++) {
        std::string& testDatum = testData[jj];
        NValue testString = ValueFactory::getStringValue(testDatum);
        size_t testTotalByteLength = testDatum.length();
        int maxStart = -1;
        for (int start = 1; start <= 7; start++) {
            NValue leftLength = ValueFactory::getIntegerValue(start-1);
            NValue startAt = ValueFactory::getIntegerValue(start);
            size_t minEnd = testTotalByteLength + 1;
            size_t nextStart = start;
            for (int length = 7; length >= 1; length--) {
                NValue lengthValue = ValueFactory::getIntegerValue(length);
                NValue endAt = ValueFactory::getIntegerValue(start + length);
                NValue rightLength = ValueFactory::getIntegerValue(std::max(0, 7 - (start-1 + length)));

                std::vector<NValue> leftArgs(3);
                leftArgs[0] = testString;
                leftArgs[1] = startAtOne;
                leftArgs[2] = leftLength;
                NValue leftStringValue = NValue::call<FUNC_SUBSTRING_CHAR>(leftArgs);

                std::vector<NValue> midArgs(3);
                midArgs[0] = testString;
                midArgs[1] = startAt;
                midArgs[2] = lengthValue;
                NValue midStringValue = NValue::call<FUNC_SUBSTRING_CHAR>(midArgs);

                std::vector<NValue> rightArgs(3);
                rightArgs[0] = testString;
                rightArgs[1] = endAt;
                rightArgs[2] = rightLength;
                NValue rightExactStringValue = NValue::call<FUNC_SUBSTRING_CHAR>(rightArgs);

                // Typically, this extends the substring PAST the end of the string.
                rightArgs[2] = sureEnd;
                NValue rightSureStringValue = NValue::call<FUNC_SUBSTRING_CHAR>(rightArgs);

                std::vector<NValue> rightDefaultArgs(2);
                rightDefaultArgs[0] = testString;
                rightDefaultArgs[1] = endAt;
                NValue rightDefaultStringValue = NValue::call<FUNC_VOLT_SUBSTRING_CHAR_FROM>(rightDefaultArgs);

                // specifying a length that goes exactly to or past the end of the input string
                // should have the same effect as not specifying a length at all.
                EXPECT_TRUE(rightExactStringValue.compare(rightDefaultStringValue) == 0);
                EXPECT_TRUE(rightSureStringValue.compare(rightDefaultStringValue) == 0);

                std::string leftString = ValuePeeker::peekStringCopy_withoutNull(leftStringValue);
                std::string midString = ValuePeeker::peekStringCopy_withoutNull(midStringValue);
                std::string rightString = ValuePeeker::peekStringCopy_withoutNull(rightExactStringValue);
                std::string recombined = leftString + midString + rightString;
                EXPECT_TRUE(testDatum.compare(recombined) == 0);

                if (midString.length() > 0) {
                    nextStart = testDatum.find(midString);
                    EXPECT_TRUE(nextStart != std::string::npos);
                    // The offset for a given value, in number of bytes skipped,
                    // should be at least start-1, the number of characters skipped.
                    EXPECT_TRUE(nextStart >= start-1);
                }

                if (rightString.length() > 0) {
                    size_t nextEnd = testDatum.find(rightString);
                    EXPECT_TRUE(nextEnd != std::string::npos);
                    EXPECT_TRUE(minEnd == 0 || minEnd > nextEnd);
                    minEnd = nextEnd;
                }
            }
            // The offset for a given value of start should increase (at least by 1) as start increases.
            EXPECT_TRUE(((int)nextStart) > maxStart);
            maxStart = (int)nextStart;
        }
        testString.free();
    }
    delete poolHolder;
    delete testPool;
}

TEST_F(NValueTest, TestExtract)
{
    assert(ExecutorContext::getExecutorContext() == NULL);
    Pool* testPool = new Pool();
    UndoQuantum* wantNoQuantum = NULL;
    Topend* topless = NULL;
    ExecutorContext* poolHolder = new ExecutorContext(0, 0, wantNoQuantum, topless, testPool, NULL, "", 0, NULL);

    NValue result;
    NValue midSeptember = ValueFactory::getTimestampValue(1000000000000000);

    int EXPECTED_YEAR = 2001;
    result = midSeptember.callUnary<FUNC_EXTRACT_YEAR>();
    EXPECT_EQ(0, result.compare(ValueFactory::getIntegerValue(EXPECTED_YEAR)));

    int8_t EXPECTED_MONTH = 9;
    result = midSeptember.callUnary<FUNC_EXTRACT_MONTH>();
    EXPECT_EQ(0, result.compare(ValueFactory::getTinyIntValue(EXPECTED_MONTH)));

    int8_t EXPECTED_DAY = 9;
    result = midSeptember.callUnary<FUNC_EXTRACT_DAY>();
    EXPECT_EQ(0, result.compare(ValueFactory::getTinyIntValue(EXPECTED_DAY)));

    int8_t EXPECTED_DOW = 1;
    result = midSeptember.callUnary<FUNC_EXTRACT_DAY_OF_WEEK>();
    EXPECT_EQ(0, result.compare(ValueFactory::getTinyIntValue(EXPECTED_DOW)));

    int16_t EXPECTED_DOY = 252;
    result = midSeptember.callUnary<FUNC_EXTRACT_DAY_OF_YEAR>();
    EXPECT_EQ(0, result.compare(ValueFactory::getSmallIntValue(EXPECTED_DOY)));

    int8_t EXPECTED_WOY = 36;
    result = midSeptember.callUnary<FUNC_EXTRACT_WEEK_OF_YEAR>();
    EXPECT_EQ(0, result.compare(ValueFactory::getTinyIntValue(EXPECTED_WOY)));

    int8_t EXPECTED_QUARTER = 3;
    result = midSeptember.callUnary<FUNC_EXTRACT_QUARTER>();
    EXPECT_EQ(0, result.compare(ValueFactory::getTinyIntValue(EXPECTED_QUARTER)));

    int8_t EXPECTED_HOUR = 1;
    result = midSeptember.callUnary<FUNC_EXTRACT_HOUR>();
    EXPECT_EQ(0, result.compare(ValueFactory::getTinyIntValue(EXPECTED_HOUR)));

    int8_t EXPECTED_MINUTE = 46;
    result = midSeptember.callUnary<FUNC_EXTRACT_MINUTE>();
    EXPECT_EQ(0, result.compare(ValueFactory::getTinyIntValue(EXPECTED_MINUTE)));

    std::string EXPECTED_SECONDS = "40";
    result = midSeptember.callUnary<FUNC_EXTRACT_SECOND>();
    EXPECT_EQ(0, result.compare(ValueFactory::getDecimalValueFromString(EXPECTED_SECONDS)));

    // test time before epoch, Thu, 18 Nov 1948 16:32:03 GMT
    NValue beforeEpoch = ValueFactory::getTimestampValue(-666430077000000);

    EXPECTED_YEAR = 1948;
    result = beforeEpoch.callUnary<FUNC_EXTRACT_YEAR>();
    EXPECT_EQ(0, result.compare(ValueFactory::getIntegerValue(EXPECTED_YEAR)));

    EXPECTED_MONTH = 11;
    result = beforeEpoch.callUnary<FUNC_EXTRACT_MONTH>();
    EXPECT_EQ(0, result.compare(ValueFactory::getTinyIntValue(EXPECTED_MONTH)));

    EXPECTED_DAY = 18;
    result = beforeEpoch.callUnary<FUNC_EXTRACT_DAY>();
    EXPECT_EQ(0, result.compare(ValueFactory::getTinyIntValue(EXPECTED_DAY)));

    EXPECTED_DOW = 5;
    result = beforeEpoch.callUnary<FUNC_EXTRACT_DAY_OF_WEEK>();
    EXPECT_EQ(0, result.compare(ValueFactory::getTinyIntValue(EXPECTED_DOW)));

    EXPECTED_DOY = 323;
    result = beforeEpoch.callUnary<FUNC_EXTRACT_DAY_OF_YEAR>();
    EXPECT_EQ(0, result.compare(ValueFactory::getSmallIntValue(EXPECTED_DOY)));

    EXPECTED_QUARTER = 4;
    result = beforeEpoch.callUnary<FUNC_EXTRACT_QUARTER>();
    EXPECT_EQ(0, result.compare(ValueFactory::getTinyIntValue(EXPECTED_QUARTER)));

    EXPECTED_HOUR = 16;
    result = beforeEpoch.callUnary<FUNC_EXTRACT_HOUR>();
    EXPECT_EQ(0, result.compare(ValueFactory::getTinyIntValue(EXPECTED_HOUR)));

    EXPECTED_MINUTE = 32;
    result = beforeEpoch.callUnary<FUNC_EXTRACT_MINUTE>();
    EXPECT_EQ(0, result.compare(ValueFactory::getTinyIntValue(EXPECTED_MINUTE)));

    EXPECTED_SECONDS = "3";
    result = beforeEpoch.callUnary<FUNC_EXTRACT_SECOND>();
    EXPECT_EQ(0, result.compare(ValueFactory::getDecimalValueFromString(EXPECTED_SECONDS)));


    // test time before epoch, Human time (GMT): Fri, 05 Jul 1658 14:22:28 GMT
    NValue longAgo = ValueFactory::getTimestampValue(-9829676252000000);

    EXPECTED_YEAR = 1658;
    result = longAgo.callUnary<FUNC_EXTRACT_YEAR>();
    EXPECT_EQ(0, result.compare(ValueFactory::getIntegerValue(EXPECTED_YEAR)));

    EXPECTED_MONTH = 7;
    result = longAgo.callUnary<FUNC_EXTRACT_MONTH>();
    EXPECT_EQ(0, result.compare(ValueFactory::getTinyIntValue(EXPECTED_MONTH)));

    EXPECTED_DAY = 5;
    result = longAgo.callUnary<FUNC_EXTRACT_DAY>();
    EXPECT_EQ(0, result.compare(ValueFactory::getTinyIntValue(EXPECTED_DAY)));

    EXPECTED_DOW = 6;
    result = longAgo.callUnary<FUNC_EXTRACT_DAY_OF_WEEK>();
    EXPECT_EQ(0, result.compare(ValueFactory::getTinyIntValue(EXPECTED_DOW)));

    EXPECTED_DOY = 186;
    result = longAgo.callUnary<FUNC_EXTRACT_DAY_OF_YEAR>();
    EXPECT_EQ(0, result.compare(ValueFactory::getSmallIntValue(EXPECTED_DOY)));

    EXPECTED_QUARTER = 3;
    result = longAgo.callUnary<FUNC_EXTRACT_QUARTER>();
    EXPECT_EQ(0, result.compare(ValueFactory::getTinyIntValue(EXPECTED_QUARTER)));

    EXPECTED_HOUR = 14;
    result = longAgo.callUnary<FUNC_EXTRACT_HOUR>();
    EXPECT_EQ(0, result.compare(ValueFactory::getTinyIntValue(EXPECTED_HOUR)));

    EXPECTED_MINUTE = 22;
    result = longAgo.callUnary<FUNC_EXTRACT_MINUTE>();
    EXPECT_EQ(0, result.compare(ValueFactory::getTinyIntValue(EXPECTED_MINUTE)));

    EXPECTED_SECONDS = "28";
    result = longAgo.callUnary<FUNC_EXTRACT_SECOND>();
    EXPECT_EQ(0, result.compare(ValueFactory::getDecimalValueFromString(EXPECTED_SECONDS)));

    delete poolHolder;
    delete testPool;
}

static NValue streamNValueArrayintoInList(ValueType vt, NValue* nvalue, int length, Pool* testPool)
{
    char serial_buffer[1024];
    // This requires intimate knowledge of ARRAY wire protocol
    ReferenceSerializeOutput setup(serial_buffer, sizeof(serial_buffer));
    ReferenceSerializeInputBE input(serial_buffer, sizeof(serial_buffer));
    setup.writeByte(VALUE_TYPE_ARRAY);
    setup.writeByte(vt);
    setup.writeShort((short)length); // number of list elements
    for (int ii = 0; ii < length; ++ii) {
        nvalue[ii].serializeTo(setup);
    }
    NValue list;
    list.deserializeFromAllocateForStorage(input, testPool);
    return list;
}

static void initNValueArray(NValue* int_NV_set, int* int_set, size_t length)
{
    size_t ii = length;
    while (ii--) {
        int_NV_set[ii] = ValueFactory::getIntegerValue(int_set[ii]);
    }
}

static void initNValueArray(NValue* string_NV_set, const char ** string_set, size_t length)
{
    size_t ii = length;
    while (ii--) {
        string_NV_set[ii] = ValueFactory::getStringValue(string_set[ii]);
    }
}

static void initConstantArray(std::vector<AbstractExpression*>& constants, NValue* int_NV_set)
{
    size_t ii = constants.size();
    while (ii--) {
        AbstractExpression* cve = new ConstantValueExpression(int_NV_set[ii]);
        constants[ii] = cve;
    }
}

static void initConstantArray(std::vector<AbstractExpression*>& constants, const char** string_set)
{
    size_t ii = constants.size();
    while (ii--) {
        AbstractExpression* cve =
            new ConstantValueExpression(ValueFactory::getStringValue(string_set[ii]));
        constants[ii] = cve;
    }
}

static void freeNValueArray(NValue* string_NV_set, size_t length)
{
    size_t ii = length;
    while (ii--) {
        string_NV_set[ii].free();
    }
}

#define SIZE_OF_ARRAY(ARRAY) (sizeof(ARRAY) / sizeof(ARRAY[0]))
TEST_F(NValueTest, TestInList)
{
    assert(ExecutorContext::getExecutorContext() == NULL);
    Pool* testPool = new Pool();
    UndoQuantum* wantNoQuantum = NULL;
    Topend* topless = NULL;
    ExecutorContext* poolHolder =
        new ExecutorContext(0, 0, wantNoQuantum, topless, testPool, NULL, "", 0, NULL);

    int int_set1[] = { 10, 2, -3 };
    int int_set2[] = { 0, 1, 100, 10000, 1000000 };

    const size_t int_length1 = SIZE_OF_ARRAY(int_set1);
    const size_t int_length2 = SIZE_OF_ARRAY(int_set2);
    NValue int_NV_set1[int_length1];
    NValue int_NV_set2[int_length2];
    initNValueArray(int_NV_set1, int_set1, int_length1);
    initNValueArray(int_NV_set2, int_set2, int_length2);

    NValue int_list1 =
        streamNValueArrayintoInList(VALUE_TYPE_INTEGER, int_NV_set1, int_length1, testPool);
    NValue int_list2 =
        streamNValueArrayintoInList(VALUE_TYPE_INTEGER, int_NV_set2, int_length2, testPool);

    for (size_t ii = 0; ii < int_length1; ++ii) {
        EXPECT_TRUE(int_NV_set1[ii].inList(int_list1));
        EXPECT_FALSE(int_NV_set1[ii].inList(int_list2));
    }
    for (size_t jj = 0; jj < int_length2; ++jj) {
        EXPECT_FALSE(int_NV_set2[jj].inList(int_list1));
        EXPECT_TRUE(int_NV_set2[jj].inList(int_list2));
    }

    // Repeat through the slow-path interface.
    // This involves lots of copying because expression trees must be destroyed recursively.

    vector<AbstractExpression*> int_constants_lhs1_1(int_length1);
    vector<AbstractExpression*> int_constants_lhs2_1(int_length2);
    vector<AbstractExpression*> int_constants_lhs1_2(int_length1);
    vector<AbstractExpression*> int_constants_lhs2_2(int_length2);
    initConstantArray(int_constants_lhs1_1, int_NV_set1);
    initConstantArray(int_constants_lhs2_1, int_NV_set2);
    initConstantArray(int_constants_lhs1_2, int_NV_set1);
    initConstantArray(int_constants_lhs2_2, int_NV_set2);

    AbstractExpression* in_expression = NULL;
    for (size_t kk = 0; kk < int_length1; ++kk) {
        vector<AbstractExpression*>* int_constants_rhs1 =
            new vector<AbstractExpression*>(int_length1);
        vector<AbstractExpression*>* int_constants_rhs2 =
            new vector<AbstractExpression*>(int_length2);
        initConstantArray(*int_constants_rhs1, int_NV_set1);
        initConstantArray(*int_constants_rhs2, int_NV_set2);

        AbstractExpression* in_list_of_int_constants1 =
           ExpressionUtil::vectorFactory(VALUE_TYPE_INTEGER, int_constants_rhs1);
        AbstractExpression* in_list_of_int_constants2 =
           ExpressionUtil::vectorFactory(VALUE_TYPE_INTEGER, int_constants_rhs2);

        in_expression =
            ExpressionUtil::comparisonFactory(EXPRESSION_TYPE_COMPARE_IN,
                                              int_constants_lhs1_1[kk],
                                              in_list_of_int_constants1);
        EXPECT_TRUE(in_expression->eval(NULL, NULL).isTrue());
        delete in_expression;

        in_expression =
            ExpressionUtil::comparisonFactory(EXPRESSION_TYPE_COMPARE_IN,
                                              int_constants_lhs1_2[kk],
                                              in_list_of_int_constants2);
        EXPECT_FALSE(in_expression->eval(NULL, NULL).isTrue());
        delete in_expression;
    }
    for (size_t ll = 0; ll < int_length2; ++ll) {
        vector<AbstractExpression*>* int_constants_rhs1 =
            new vector<AbstractExpression*>(int_length1);
        vector<AbstractExpression*>* int_constants_rhs2 =
            new vector<AbstractExpression*>(int_length2);
        initConstantArray(*int_constants_rhs1, int_NV_set1);
        initConstantArray(*int_constants_rhs2, int_NV_set2);

        AbstractExpression* in_list_of_int_constants1 =
           ExpressionUtil::vectorFactory(VALUE_TYPE_INTEGER, int_constants_rhs1);
        AbstractExpression* in_list_of_int_constants2 =
           ExpressionUtil::vectorFactory(VALUE_TYPE_INTEGER, int_constants_rhs2);

        in_expression =
            ExpressionUtil::comparisonFactory(EXPRESSION_TYPE_COMPARE_IN,
                                              int_constants_lhs2_1[ll],
                                              in_list_of_int_constants2);
        EXPECT_TRUE(in_expression->eval(NULL, NULL).isTrue());
        delete in_expression;

        in_expression =
            ExpressionUtil::comparisonFactory(EXPRESSION_TYPE_COMPARE_IN,
                                              int_constants_lhs2_2[ll],
                                              in_list_of_int_constants1);
        EXPECT_FALSE(in_expression->eval(NULL, NULL).isTrue());
        delete in_expression;
    }

    const char* string_set1[] = { "10", "2", "-3" };
    const char* string_set2[] = { "0", "1", "100", "10000", "1000000" };

    const size_t string_length1 = SIZE_OF_ARRAY(string_set1);
    const size_t string_length2 = SIZE_OF_ARRAY(string_set2);
    NValue string_NV_set1[string_length1];
    NValue string_NV_set2[string_length2];
    initNValueArray(string_NV_set1, string_set1, string_length1);
    initNValueArray(string_NV_set2, string_set2, string_length2);

    NValue string_list1 =
        streamNValueArrayintoInList(VALUE_TYPE_VARCHAR, string_NV_set1, string_length1, testPool);
    NValue string_list2 =
        streamNValueArrayintoInList(VALUE_TYPE_VARCHAR, string_NV_set2, string_length2, testPool);
    for (size_t ii = 0; ii < string_length1; ++ii) {
        EXPECT_TRUE(string_NV_set1[ii].inList(string_list1));
        EXPECT_FALSE(string_NV_set1[ii].inList(string_list2));
    }
    for (size_t jj = 0; jj < string_length2; ++jj) {
        EXPECT_FALSE(string_NV_set2[jj].inList(string_list1));
        EXPECT_TRUE(string_NV_set2[jj].inList(string_list2));
    }

    freeNValueArray(string_NV_set1, string_length1);
    freeNValueArray(string_NV_set2, string_length2);

    // Repeat through the slow-path interface.
    // This involves lots of copying because expression trees must be destroyed recursively.

    vector<AbstractExpression*> string_constants_lhs1_1(string_length1);
    vector<AbstractExpression*> string_constants_lhs2_1(string_length2);
    vector<AbstractExpression*> string_constants_lhs1_2(string_length1);
    vector<AbstractExpression*> string_constants_lhs2_2(string_length2);
    initConstantArray(string_constants_lhs1_1, string_set1);
    initConstantArray(string_constants_lhs2_1, string_set2);
    initConstantArray(string_constants_lhs1_2, string_set1);
    initConstantArray(string_constants_lhs2_2, string_set2);

    for (size_t kk = 0; kk < string_length1; ++kk) {
        vector<AbstractExpression*>* string_constants_rhs1
            = new vector<AbstractExpression*>(string_length1);
        vector<AbstractExpression*>* string_constants_rhs2
            = new vector<AbstractExpression*>(string_length2);
        initConstantArray(*string_constants_rhs1, string_set1);
        initConstantArray(*string_constants_rhs2, string_set2);

        AbstractExpression* in_list_of_string_constants1 =
           ExpressionUtil::vectorFactory(VALUE_TYPE_VARCHAR, string_constants_rhs1);
        AbstractExpression* in_list_of_string_constants2 =
           ExpressionUtil::vectorFactory(VALUE_TYPE_VARCHAR, string_constants_rhs2);

        in_expression =
            ExpressionUtil::comparisonFactory(EXPRESSION_TYPE_COMPARE_IN,
                                              string_constants_lhs1_1[kk],
                                              in_list_of_string_constants1);
        EXPECT_TRUE(in_expression->eval(NULL, NULL).isTrue());
        delete in_expression;

        in_expression =
            ExpressionUtil::comparisonFactory(EXPRESSION_TYPE_COMPARE_IN,
                                              string_constants_lhs1_2[kk],
                                              in_list_of_string_constants2);
        EXPECT_FALSE(in_expression->eval(NULL, NULL).isTrue());
        delete in_expression;
    }
    for (size_t ll = 0; ll < string_length2; ++ll) {
        vector<AbstractExpression*>* string_constants_rhs1
            = new vector<AbstractExpression*>(string_length1);
        vector<AbstractExpression*>* string_constants_rhs2
            = new vector<AbstractExpression*>(string_length2);
        initConstantArray(*string_constants_rhs1, string_set1);
        initConstantArray(*string_constants_rhs2, string_set2);

        AbstractExpression* in_list_of_string_constants1 =
           ExpressionUtil::vectorFactory(VALUE_TYPE_VARCHAR, string_constants_rhs1);
        AbstractExpression* in_list_of_string_constants2 =
           ExpressionUtil::vectorFactory(VALUE_TYPE_VARCHAR, string_constants_rhs2);

        in_expression =
            ExpressionUtil::comparisonFactory(EXPRESSION_TYPE_COMPARE_IN,
                                              string_constants_lhs2_1[ll],
                                              in_list_of_string_constants2);
        EXPECT_TRUE(in_expression->eval(NULL, NULL).isTrue());
        delete in_expression;

        in_expression =
            ExpressionUtil::comparisonFactory(EXPRESSION_TYPE_COMPARE_IN,
                                              string_constants_lhs2_2[ll],
                                              in_list_of_string_constants1);
        EXPECT_FALSE(in_expression->eval(NULL, NULL).isTrue());
        delete in_expression;
    }

    delete poolHolder;
    delete testPool;
}

bool checkValueVector(vector<NValue> &values) {
    // check the array by verifying all values are larger than the previous value
    // this checks order and the lack of duplicates
    for (int j = 0; j < (values.size() - 1); j++) {
        if (values[j].compare(values[j+1]) >= 0) {
            return false;
        }
    }
    return true;
}

TEST_F(NValueTest, TestDedupAndSort) {
    assert(ExecutorContext::getExecutorContext() == NULL);
    Pool* testPool = new Pool();
    UndoQuantum* wantNoQuantum = NULL;
    Topend* topless = NULL;
    ExecutorContext* poolHolder =
    new ExecutorContext(0, 0, wantNoQuantum, topless, testPool, NULL, "", 0, NULL);

    std::vector<NValue> vectorValues;
    NValue arrayValue;
    NValue nvalue;
    bool coinFlip;

    /////////////////////////////////////////////////////////////
    // Automatic Test with Integers
    /////////////////////////////////////////////////////////////

    // run 20 random tests (can be increased for stress tests)
    for (int i = 0; i < 20; i++) {
        // pick a vector length
        int len = rand() % 1000;
        // pick a max integer value to use for about half of the values
        // this number is low to encourage duplicates and to ensure most
        // values will cast to tinyint
        int maxValue = rand() % 100;

        // get a random NValue array thingy
        for (int j = 0; j < len; j++) {
            // 1/50 are null
            if ((rand() % 50) == 0) {
                nvalue = ValueFactory::getNullValue();
            }
            // 49/50 not null
            else {
                coinFlip = (rand() % 2) == 0;
                // half the values are smallish, others half is random
                int64_t localMaxValue = coinFlip ? maxValue : INT_MAX;
                int64_t value = rand() % localMaxValue;
                nvalue = ValueFactory::getBigIntValue(value);
            }
            vectorValues.push_back(nvalue);
        }
        arrayValue = ValueFactory::getArrayValueFromSizeAndType(len, VALUE_TYPE_BIGINT);
        EXPECT_TRUE(vectorValues.size() == len);
        arrayValue.setArrayElements(vectorValues);

        // dedup, cast and sort
        vectorValues.clear();
        coinFlip = (rand() % 2) == 0;
        // 50% are cast down... 50% are not
        ValueType type = coinFlip ? VALUE_TYPE_BIGINT : VALUE_TYPE_SMALLINT;
        arrayValue.castAndSortAndDedupArrayForInList(type, vectorValues);

        // verify
        EXPECT_TRUE(checkValueVector(vectorValues));
        vectorValues.clear();
        arrayValue.free();
    }

    /////////////////////////////////////////////////////////////
    // Manual Test with Strings
    /////////////////////////////////////////////////////////////

    NValue v0, v1, v2, v3, v4, v5;
    vectorValues.clear();

    v0 = ValueFactory::getStringValue("b");
    vectorValues.push_back(v0);
    v1 = ValueFactory::getStringValue("");
    vectorValues.push_back(v1);
    v2 = ValueFactory::getStringValue("a");
    vectorValues.push_back(v2);
    v3 = ValueFactory::getStringValue("A");
    vectorValues.push_back(v3);
    v4 = ValueFactory::getStringValue("");
    vectorValues.push_back(v4);
    v5 = ValueFactory::getNullStringValue();
    vectorValues.push_back(v5);

    arrayValue = ValueFactory::getArrayValueFromSizeAndType(6, VALUE_TYPE_VARCHAR);
    arrayValue.setArrayElements(vectorValues);

    // dedup, cast and sort
    vectorValues.clear();
    arrayValue.castAndSortAndDedupArrayForInList(VALUE_TYPE_VARCHAR, vectorValues);
    EXPECT_TRUE(vectorValues.size() == 5);

    // verify
    EXPECT_TRUE(checkValueVector(vectorValues));
    vectorValues.clear();
    v0.free();
    v1.free();
    v2.free();
    v3.free();
    v4.free();
    arrayValue.free();

    /////////////////////////////////////////////////////////////
    // Manual Test with Floats
    /////////////////////////////////////////////////////////////

    vectorValues.clear();

    v0 = ValueFactory::getDoubleValue(1.5);
    vectorValues.push_back(v0);
    v1 = ValueFactory::getDoubleValue(1.1E10);
    vectorValues.push_back(v1);
    v2 = ValueFactory::getDoubleValue(2.2);
    vectorValues.push_back(v2);
    v3 = ValueFactory::getDoubleValue(2.21);
    vectorValues.push_back(v3);
    v4 = ValueFactory::getDoubleValue(2.2);
    vectorValues.push_back(v4);
    v5 = ValueFactory::getNullValue();
    vectorValues.push_back(v5);

    arrayValue = ValueFactory::getArrayValueFromSizeAndType(6, VALUE_TYPE_DOUBLE);
    arrayValue.setArrayElements(vectorValues);

    // dedup, cast and sort
    vectorValues.clear();
    arrayValue.castAndSortAndDedupArrayForInList(VALUE_TYPE_DOUBLE, vectorValues);
    EXPECT_TRUE(vectorValues.size() == 5);

    // verify
    EXPECT_TRUE(checkValueVector(vectorValues));
    vectorValues.clear();
    arrayValue.free();

    /////////////////////////////////////////////////////////////
    // Manual Test with Decimals
    /////////////////////////////////////////////////////////////

    vectorValues.clear();

    v0 = ValueFactory::getDecimalValueFromString("1.5");
    vectorValues.push_back(v0);
    v1 = ValueFactory::getDecimalValueFromString("111111.11111");
    vectorValues.push_back(v1);
    v2 = ValueFactory::getDecimalValueFromString("2.2");
    vectorValues.push_back(v2);
    v3 = ValueFactory::getDecimalValueFromString("2.21");
    vectorValues.push_back(v3);
    v4 = ValueFactory::getDecimalValueFromString("2.2");
    vectorValues.push_back(v4);
    v5 = ValueFactory::getNullValue();
    vectorValues.push_back(v5);

    arrayValue = ValueFactory::getArrayValueFromSizeAndType(6, VALUE_TYPE_DECIMAL);
    arrayValue.setArrayElements(vectorValues);

    // dedup, cast and sort
    vectorValues.clear();
    arrayValue.castAndSortAndDedupArrayForInList(VALUE_TYPE_DECIMAL, vectorValues);
    EXPECT_TRUE(vectorValues.size() == 5);

    // verify
    EXPECT_TRUE(checkValueVector(vectorValues));
    vectorValues.clear();
    arrayValue.free();

    /////////////////////////////////////////////////////////////
    // Manual Test with Binary
    /////////////////////////////////////////////////////////////

    vectorValues.clear();

    v0 = ValueFactory::getBinaryValue("AA");
    vectorValues.push_back(v0);
    v1 = ValueFactory::getBinaryValue("BCDE");
    vectorValues.push_back(v1);
    v2 = ValueFactory::getBinaryValue("1F");
    vectorValues.push_back(v2);
    v3 = ValueFactory::getBinaryValue("1F55");
    vectorValues.push_back(v3);
    v4 = ValueFactory::getBinaryValue("1F");
    vectorValues.push_back(v4);
    v5 = ValueFactory::getNullBinaryValue();
    vectorValues.push_back(v5);

    arrayValue = ValueFactory::getArrayValueFromSizeAndType(6, VALUE_TYPE_VARBINARY);
    arrayValue.setArrayElements(vectorValues);

    // dedup, cast and sort
    vectorValues.clear();
    arrayValue.castAndSortAndDedupArrayForInList(VALUE_TYPE_VARBINARY, vectorValues);
    EXPECT_TRUE(vectorValues.size() == 5);

    // verify
    EXPECT_TRUE(checkValueVector(vectorValues));
    vectorValues.clear();
    v0.free();
    v1.free();
    v2.free();
    v3.free();
    v4.free();
    arrayValue.free();

    /////////////////////////////////////////////////////////////
    // Cleanup
    /////////////////////////////////////////////////////////////

    delete poolHolder;
    delete testPool;
}

TEST_F(NValueTest, TestTimestampStringParse)
{
    assert(ExecutorContext::getExecutorContext() == NULL);
    Pool* testPool = new Pool();
    UndoQuantum* wantNoQuantum = NULL;
    Topend* topless = NULL;
    ExecutorContext* poolHolder = new ExecutorContext(0, 0, wantNoQuantum, topless, testPool, NULL, "", 0, NULL);

    bool failed = false;
    const char* trials[] = {
        "",
        //Variants of "2000-01-01 01:01:01.000000" with a dropped character,
        "200-01-01 01:01:01.000000",
        "200001-01 01:01:01.000000",
        "2000-1-01 01:01:01.000000",
        "2000-0-01 01:01:01.000000",
        "2000-0101 01:01:01.000000",
        "2000-01-1 01:01:01.000000",
        "2000-01-0 01:01:01.000000",
        "2000-01-0101:01:01.000000",
        "2000-01-01 1:01:01.000000",
        "2000-01-01 0:01:01.000000",
        "2000-01-01 0101:01.000000",
        "2000-01-01 01:1:01.000000",
        "2000-01-01 01:0:01.000000",
        "2000-01-01 01:0101.000000",
        "2000-01-01 01:01:1.000000",
        "2000-01-01 01:01:0.000000",
        "2000-01-01 01:01:01000000",
        "2000-01-01 01:01:01.00000",
        "2000-01-01 01:01:01.999 ",
        //Variants of "2000-01-01 01:01:01.000000" with an added character,
        "02000-01-01 01:01:01.000000",
        "20000-01-01 01:01:01.000000",
        "2000-001-01 01:01:01.000000",
        "2000-010-01 01:01:01.000000",
        "2000-01-001 01:01:01.000000",
        "2000-01-010 01:01:01.000000",
        "2000-01-01 001:01:01.000000",
        "2000-01-01 010:01:01.000000",
        "2000-01-01 01:001:01.000000",
        "2000-01-01 01:010:01.000000",
        "2000-01-01 01:01:001.000000",
        "2000-01-01 01:01:010.000000",
        "2000-01-01 01:01:01.0000000",
        //Variants of "2000-01-01 01:01:01.000000" with an out-of-range component,
        "2000-21-01 01:01:01.000000",
        "2000-13-01 01:01:01.000000",
        "2000-01-41 01:01:01.000000",
        "2000-01-32 01:01:01.000000",
        "2000-01-01 30:01:01.000000",
        "2000-01-01 25:01:01.000000",
        "2000-01-01 01:60:01.000000",
        "2000-01-01 01:60:01.-00001",
        "2000-01-01 01:60:01.-12345",
        "2000-01-01 01:60:01.-123456",
        "2000-01-01 01:60:01.-9999999",
        "2000-01-01 01:60:01.9999999",
        "2000-01-01 01:01:01.999abc",
        "2000-01-01 01:01:01.a999bc",
        "2000-01-01 01:01:01. 999bc",
        "2000-01-01 01:01:01.aaaaaa",
        //Variants of "2000-01-01" with a dropped character
        "200-01-01",
        "200001-01",
        "2000-1-01",
        "2000-0-01",
        "2000-0101",
        "2000-01-1",
        "2000-01-0",
        //Variants of "2000-01-01" with an added character,
        "02000-01-01",
        "20000-01-01",
        "2000-001-01",
        "2000-010-01",
        "2000-01-001",
        "2000-01-010",
        //Variants of "2000-01-01" with an out-of-range component,
        "2000-21-01",
        "2000-13-01",
        "2000-01-41",
        "2000-01-32",
        "2000-01-2a",
        "2000-01-a2",
        "2000-01-aa",
        "2000-01- 2",
        "2000-01-2 ",
        };
    size_t ii = sizeof(trials) / sizeof(const char*);
    while (ii--) {
        try {
            NValue::parseTimestampString(trials[ii]);
            cout << "Timestamp cast should have failed for string '" << trials[ii] << "'.\n";
            failed = true;
        } catch(SQLException& exc) {
            const char* msg = exc.message().c_str();
            string to_find = "\'";
            to_find += trials[ii];
            to_find += "\'";
            const char* found = strstr(msg, to_find.c_str());
            if (found && found > msg && found[0] == '\'' && found[strlen(trials[ii])+1] == '\'') {
                continue;
            }
            cout << "Timestamp cast exception message looks corrupted: '" << msg << "'.\n";
            failed = true;
        }
    }
    EXPECT_FALSE(failed);

    long base;
    std::string peekString = "Failed to start";

    // Test round-trip conversion for a pivotal value that would fail a mktime-based implementation.
    // leveraged in the algorithm for the high end of the range.
    base = NValue::parseTimestampString("2038-12-31 23:59:59.999999");
    try {
        NValue ts = ValueFactory::getTimestampValue(base);
        NValue str = ts.castAs(VALUE_TYPE_VARCHAR);
        peekString = ValuePeeker::peekStringCopy_withoutNull(str);
        long roundtrip = NValue::parseTimestampString(peekString.c_str());
        EXPECT_EQ(base, roundtrip);
        if (base != roundtrip) {
            cout << "Failing for base " << base << " vs roundtrip " << roundtrip <<
                "as string \'" << peekString << "\'.";
        }
    } catch(SQLException& exc) {
        cout << "Low timestamp did not work: '" << peekString << "' / " << base << ".\n";
        EXPECT_FALSE(true);
    }

    char dateStr[27];
    dateStr[0] = '\0';
    try {
        base = NValue::parseTimestampString("1400-12-31 23:59:59.999999");
        // Test that various centuries are of equal length.
        long centuryMicros = NValue::parseTimestampString("1500-12-31 23:59:59.999999") - base;
        long extraLeapdayMicros = 24L * 60L * 60L * 1000L * 1000L;
        // Test parsing up through year 9000.
        for (int century = 15; century <= 90; ++century) {
            snprintf(dateStr, sizeof(dateStr), "%02d00-12-31 23:59:59.999999", century);
            long newbase = NValue::parseTimestampString(dateStr);
            if ((newbase - base != centuryMicros) &&
                (newbase - base != centuryMicros + extraLeapdayMicros)) {
                cout << "Failing century for \'" << dateStr <<
                    "\' (off by " << (newbase - base - centuryMicros) << ").\n";
            }
            base = newbase;
        }
    } catch(SQLException& exc) {
        cout << "Century parse did not work: '" << exc.message() << "' / " << dateStr << "." << endl;
        EXPECT_FALSE(true);
    }


    dateStr[0] = '\0';
    try {
        base = NValue::parseTimestampString("1837-12-31 23:59:59.999999");
        long decadeMicros = NValue::parseTimestampString("1847-12-31 23:59:59.999999") - base;
        long extraLeapdayMicros = 24L * 60L * 60L * 1000L * 1000L;
        // Test parsing up through 2437.
        for (int decade = 184; decade <= 243; ++decade) {
            snprintf(dateStr, sizeof(dateStr), "%03d7-12-31 23:59:59.999999", decade);
            long newbase = NValue::parseTimestampString(dateStr);
            if ((newbase - base != decadeMicros) &&
                (newbase - base != decadeMicros + extraLeapdayMicros) &&
                (newbase - base + extraLeapdayMicros != decadeMicros)) {
                cout << "Failing decade for \'" << dateStr <<
                    "\' (value " << (newbase - base) << " off by " << (newbase - base - decadeMicros) << ").\n";
            }
            base = newbase;
        }
    } catch(SQLException& exc) {
        cout << "Decade parse did not work: '" << exc.message() << "' / " << dateStr << ".\n";
        EXPECT_FALSE(true);
    }


    dateStr[0] = '\0';
    try {
        base = NValue::parseTimestampString("1925-12-31 23:59:59.999999");
        long yearMicros = NValue::parseTimestampString("1926-12-31 23:59:59.999999") - base;
        long extraLeapdayMicros = 24L * 60L * 60L * 1000L * 1000L;
        // Test parsing up through 2126.
        for (int year = 1926; year <= 2126; ++year) {
            snprintf(dateStr, sizeof(dateStr), "%04d-12-31 23:59:59.999999", year);
            long newbase = NValue::parseTimestampString(dateStr);
            if ((newbase - base != yearMicros) &&
                (newbase - base != yearMicros + extraLeapdayMicros)) {
                cout << "Failing year for \'" << dateStr <<
                    "\' (value " << (newbase - base) << " off by " << (newbase - base - yearMicros) << ").\n";
            }
            base = newbase;
        }
    } catch(SQLException& exc) {
        cout << "Annual parse did not work: '" << exc.message() << "' / " << dateStr << ".\n";
        EXPECT_FALSE(true);
    }

    dateStr[0] = '\0';
    try {
        base = NValue::parseTimestampString("1982-12-13 23:59:59.999999");
        long dayMicros = NValue::parseTimestampString("1982-12-14 23:59:59.999999") - base;
        EXPECT_EQ(0, dayMicros % 100000000L);
        // Test parsing through 1983.
        for (int month = 1; month <= 12; ++month) {
            snprintf(dateStr, sizeof(dateStr), "1983-%02d-13 23:59:59.999999", month);
            long newbase = NValue::parseTimestampString(dateStr);
            if ((newbase - base) % 100000000L) {
                cout << "Failing month for \'" << dateStr <<
                    "\' (value " << (newbase - base) << " off by " << ((newbase - base) % 100000000L) << ").\n";
            }
            base = newbase;
        }
    } catch(SQLException& exc) {
        cout << "Monthly parse did not work: '" << exc.message() << "' / " << dateStr << ".\n";
        EXPECT_FALSE(true);
    }


    dateStr[0] = '\0';
    try {
        base = NValue::parseTimestampString("1883-10-31 23:59:59.999999");
        long dayMicros = NValue::parseTimestampString("1883-11-01 23:59:59.999999") - base;
        EXPECT_EQ(0, dayMicros % 100000000L);
        // Test parsing through a month.
        for (int day = 1; day <= 30; ++day) {
            snprintf(dateStr, sizeof(dateStr), "1883-11-%02d 23:59:59.999999", day);
            long newbase = NValue::parseTimestampString(dateStr);
            EXPECT_EQ(newbase, base + dayMicros);
            if (newbase != base + dayMicros) {
                cout << "Failing day for \'" << dateStr <<
                    "\' (value " << (newbase - base) << " off by " << ((newbase - base) % 100000000L) << ").\n";
            }
            base = newbase;
        }
    } catch(SQLException& exc) {
        cout << "Daily parse did not work: '" << exc.message() << "' / " << dateStr << ".\n";
        EXPECT_FALSE(true);
    }


    // Test round-trip conversions for sample dates in a broad range.
    base = NValue::parseTimestampString("1900-01-01 00:00:00.000000");
    long top = NValue::parseTimestampString("2900-12-31 23:59:59.999999");
    long increment = (top - base) / 997;
    for (long jj = base; jj <= top; jj+=increment) {
        try {
            NValue ts = ValueFactory::getTimestampValue(jj);
            NValue str = ts.castAs(VALUE_TYPE_VARCHAR);
            peekString = ValuePeeker::peekStringCopy_withoutNull(str);
            long roundtrip = NValue::parseTimestampString(peekString.c_str());
            EXPECT_EQ(jj, roundtrip);
            if (jj != roundtrip) {
                cout << "Failing for iteration " << ((jj-base)/increment) << " base " << base <<
                    " vs roundtrip " << roundtrip << " as string \'" << peekString << "\'." << endl;
                cout << "Off by " << (jj - roundtrip) << "us " << (jj - roundtrip)/1000000L << "s ";
            }
        } catch(SQLException& exc) {
            cout << "Timestamp incremented past cast range at or after: '" << peekString <<
                "' / " << jj << " iteration " << ((jj-base)/increment) << ".\n";
            EXPECT_FALSE(true);
            break;
        }
    }
    delete poolHolder;
    delete testPool;
}

TEST_F(NValueTest, TestTimestampStringParseShort)
{
    assert(ExecutorContext::getExecutorContext() == NULL);
    Pool* testPool = new Pool();
    UndoQuantum* wantNoQuantum = NULL;
    Topend* topless = NULL;
    ExecutorContext* poolHolder = new ExecutorContext(0, 0, wantNoQuantum, topless, testPool, NULL, "", 0, NULL);

    std::string peekString;

    char dateStr[11] = {0};
    char dateStr2[27] = {0};
    try {
        // volt does not support date prior to 1583-01-01
        // see src/ee/expressions/datefunctions.h
        for (int century = 16; century <= 90; ++century) {
            snprintf(dateStr, sizeof(dateStr), "%02d00-12-31", century);
            snprintf(dateStr2, sizeof(dateStr2), "%02d00-12-31 00:00:00.000000", century);
            int64_t value = NValue::parseTimestampString(dateStr);
            NValue ts = ValueFactory::getTimestampValue(value);
            NValue str = ts.castAs(VALUE_TYPE_VARCHAR);
            peekString = ValuePeeker::peekStringCopy_withoutNull(str);
            EXPECT_EQ(peekString, dateStr2);
            if (peekString.compare(dateStr2) != 0) {
                cout << "Failing for compare ts string " << peekString << " vs ts string " <<
                    dateStr2 << endl;
            }
            str.free();
        }
    } catch(SQLException& exc) {
        cout << "I have no idea what happen here " << exc.message() << " " << dateStr << endl;
        EXPECT_FALSE(true);
    }

    try {
        // volt does not support date prior to 1583-01-01
        // see src/ee/expressions/datefunctions.h
        for (int century = 16; century <= 90; ++century) {
            snprintf(dateStr, sizeof(dateStr), "%02d00-12-31", century);
            snprintf(dateStr2, sizeof(dateStr2), "%02d00-12-31 00:00:00.000000", century);
            int64_t base = NValue::parseTimestampString(dateStr2);
            NValue str = ValueFactory::getStringValue(dateStr);
            NValue ts = str.castAs(VALUE_TYPE_TIMESTAMP);
            int64_t value = ValuePeeker::peekTimestamp(ts);
            EXPECT_EQ(base, value);
            if (base != value) {
                cout << "Failing for converting ts string " << dateStr << " to the same value as " <<
                    dateStr2 << endl;
            }
            str.free();
        }
    } catch(SQLException& exc) {
        cout << "I have no idea what happen here " << exc.message() << " " << dateStr << endl;
        EXPECT_FALSE(true);
    }

    delete poolHolder;
    delete testPool;
}

TEST_F(NValueTest, TestTimestampStringParseWithLeadingAndTrailingSpaces)
{
    assert(ExecutorContext::getExecutorContext() == NULL);
    Pool* testPool = new Pool();
    UndoQuantum* wantNoQuantum = NULL;
    Topend* topless = NULL;
    ExecutorContext* poolHolder = new ExecutorContext(0, 0, wantNoQuantum, topless, testPool, NULL, "", 0, NULL);

    std::string peekString;

    char dateStr[32] = {0};
    char dateStr2[27] = {0};

    // test leading space
    try {
        for (int century = 16; century <= 90; ++century) {
            snprintf(dateStr, sizeof(dateStr), "  %02d00-11-30", century);
            dateStr[12] = 0;
            snprintf(dateStr2, sizeof(dateStr2), "%02d00-11-30 00:00:00.000000", century);
            int64_t value = NValue::parseTimestampString(dateStr);
            NValue ts = ValueFactory::getTimestampValue(value);
            NValue str = ts.castAs(VALUE_TYPE_VARCHAR);
            peekString = ValuePeeker::peekStringCopy_withoutNull(str);
            EXPECT_EQ(peekString, dateStr2);
            if (peekString.compare(dateStr2) != 0) {
                cout << "Failing for compare ts string " << peekString << " vs ts string " <<
                    dateStr2 << endl;
            }
            str.free();
        }
    } catch(SQLException& exc) {
        cout << "I have no idea what happen here " << exc.message() << " " << dateStr << endl;
        EXPECT_FALSE(true);
    }

    try {
        for (int century = 16; century <= 90; ++century) {
            snprintf(dateStr, sizeof(dateStr), "  %02d00-11-30 00:00:00.000000", century);
            dateStr[28] = 0;
            snprintf(dateStr2, sizeof(dateStr2), "%02d00-11-30 00:00:00.000000", century);
            int64_t value = NValue::parseTimestampString(dateStr);
            NValue ts = ValueFactory::getTimestampValue(value);
            NValue str = ts.castAs(VALUE_TYPE_VARCHAR);
            peekString = ValuePeeker::peekStringCopy_withoutNull(str);
            EXPECT_EQ(peekString, dateStr2);
            if (peekString.compare(dateStr2) != 0) {
                cout << "Failing for compare ts string " << peekString << " vs ts string " <<
                    dateStr2 << endl;
            }
            str.free();
        }
    } catch(SQLException& exc) {
        cout << "I have no idea what happen here " << exc.message() << " " << dateStr << endl;
        EXPECT_FALSE(true);
    }

    // test trailing space
    try {
        for (int century = 16; century <= 90; ++century) {
            snprintf(dateStr, sizeof(dateStr), "%02d00-10-29  ", century);
            dateStr[12] = 0;
            snprintf(dateStr2, sizeof(dateStr2), "%02d00-10-29 00:00:00.000000", century);
            int64_t value = NValue::parseTimestampString(dateStr);
            NValue ts = ValueFactory::getTimestampValue(value);
            NValue str = ts.castAs(VALUE_TYPE_VARCHAR);
            peekString = ValuePeeker::peekStringCopy_withoutNull(str);
            EXPECT_EQ(peekString, dateStr2);
            if (peekString.compare(dateStr2) != 0) {
                cout << "Failing for compare ts string " << peekString << " vs ts string " <<
                    dateStr2 << endl;
            }
            str.free();
        }
    } catch(SQLException& exc) {
        cout << "I have no idea what happen here " << exc.message() << " " << dateStr << endl;
        EXPECT_FALSE(true);
    }

    try {
        for (int century = 16; century <= 90; ++century) {
            snprintf(dateStr, sizeof(dateStr), "%02d00-11-30 00:00:00.000000  ", century);
            dateStr[28] = 0;
            snprintf(dateStr2, sizeof(dateStr2), "%02d00-11-30 00:00:00.000000", century);
            int64_t value = NValue::parseTimestampString(dateStr);
            NValue ts = ValueFactory::getTimestampValue(value);
            NValue str = ts.castAs(VALUE_TYPE_VARCHAR);
            peekString = ValuePeeker::peekStringCopy_withoutNull(str);
            EXPECT_EQ(peekString, dateStr2);
            if (peekString.compare(dateStr2) != 0) {
                cout << "Failing for compare ts string " << peekString << " vs ts string " <<
                    dateStr2 << endl;
            }
            str.free();
        }
    } catch(SQLException& exc) {
        cout << "I have no idea what happen here " << exc.message() << " " << dateStr << endl;
        EXPECT_FALSE(true);
    }

    // test leading and trailing space
    try {
        for (int century = 16; century <= 90; ++century) {
            snprintf(dateStr, sizeof(dateStr), " %02d00-12-31 ", century);
            dateStr[12] = 0;
            snprintf(dateStr2, sizeof(dateStr2), "%02d00-12-31 00:00:00.000000", century);
            int64_t value = NValue::parseTimestampString(dateStr);
            NValue ts = ValueFactory::getTimestampValue(value);
            NValue str = ts.castAs(VALUE_TYPE_VARCHAR);
            peekString = ValuePeeker::peekStringCopy_withoutNull(str);
            EXPECT_EQ(peekString, dateStr2);
            if (peekString.compare(dateStr2) != 0) {
                cout << "Failing for compare ts string " << peekString << " vs ts string " <<
                    dateStr2 << endl;
            }
            str.free();
        }
    } catch(SQLException& exc) {
        cout << "I have no idea what happen here " << exc.message() << " " << dateStr << endl;
        EXPECT_FALSE(true);
    }

    try {
        for (int century = 16; century <= 90; ++century) {
            snprintf(dateStr, sizeof(dateStr), " %02d00-11-30 00:00:00.000000 ", century);
            dateStr[28] = 0;
            snprintf(dateStr2, sizeof(dateStr2), "%02d00-11-30 00:00:00.000000", century);
            int64_t value = NValue::parseTimestampString(dateStr);
            NValue ts = ValueFactory::getTimestampValue(value);
            NValue str = ts.castAs(VALUE_TYPE_VARCHAR);
            peekString = ValuePeeker::peekStringCopy_withoutNull(str);
            EXPECT_EQ(peekString, dateStr2);
            if (peekString.compare(dateStr2) != 0) {
                cout << "Failing for compare ts string " << peekString << " vs ts string " <<
                    dateStr2 << endl;
            }
            str.free();
        }
    } catch(SQLException& exc) {
        cout << "I have no idea what happen here " << exc.message() << " " << dateStr << endl;
        EXPECT_FALSE(true);
    }

    delete poolHolder;
    delete testPool;
}

int main() {
    return TestSuite::globalInstance()->runAll();
}
