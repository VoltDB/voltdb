/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
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
#include "indexes/indexkey.h"
#include "common/NValue.hpp"
#include "common/ValueFactory.hpp"
#include "common/TupleSchema.h"
#include "common/tabletuple.h"
#include "common/ThreadLocalPool.h"

using namespace voltdb;

class IndexKeyTest : public Test {
    public:
        IndexKeyTest() {}
        ThreadLocalPool m_pool;
};

TEST_F(IndexKeyTest, Int64KeyTest) {
    std::vector<voltdb::ValueType> columnTypes(1, voltdb::ValueType::tBIGINT);
    std::vector<int32_t> columnLengths(1, NValue::getTupleStorageSize(voltdb::ValueType::tBIGINT));
    std::vector<bool> columnAllowNull(1, true);
    voltdb::TupleSchema *keySchema = voltdb::TupleSchema::createTupleSchemaForTest(columnTypes, columnLengths, columnAllowNull);

    voltdb::IntsKey<1>::KeyComparator comparator(keySchema);
    voltdb::IntsKey<1>::KeyHasher hasher(keySchema);
    voltdb::IntsKey<1>::KeyEqualityChecker equality(keySchema);

    voltdb::TableTuple keyTuple(keySchema);
    keyTuple.move(new char[keyTuple.tupleLength()]());
    keyTuple.setNValue(0, ValueFactory::getBigIntValue(static_cast<int64_t>(50)));

    voltdb::TableTuple otherTuple(keySchema);
    otherTuple.move(new char[otherTuple.tupleLength()]());
    otherTuple.setNValue(0, ValueFactory::getBigIntValue(static_cast<int64_t>(25)));

    voltdb::IntsKey<1> keyKey(&keyTuple);

    voltdb::IntsKey<1> otherKey(&otherTuple);

    EXPECT_FALSE(equality.operator()(keyKey, otherKey));
    EXPECT_EQ( 1, comparator.operator()(keyKey, otherKey));
    EXPECT_EQ( 0, comparator.operator()(keyKey,keyKey));
    EXPECT_EQ( 0, comparator.operator()(otherKey, otherKey));
    EXPECT_EQ(-1, comparator.operator()(otherKey, keyKey));

    voltdb::TableTuple thirdTuple(keySchema);
    thirdTuple.move(new char[thirdTuple.tupleLength()]());
    thirdTuple.setNValue(0, ValueFactory::getBigIntValue(static_cast<int64_t>(50)));
    voltdb::IntsKey<1> thirdKey(&thirdTuple);
    EXPECT_TRUE(equality.operator()(keyKey, thirdKey));

    otherTuple.setNValue(0, ValueFactory::getBigIntValue(static_cast<int64_t>(50)));
    voltdb::IntsKey<1> anotherKey(&otherTuple);

    EXPECT_TRUE(equality.operator()(keyKey, anotherKey));

    EXPECT_EQ( 0, comparator.operator()(keyKey, anotherKey));

    delete [] keyTuple.address();
    delete [] otherTuple.address();
    delete [] thirdTuple.address();
    voltdb::TupleSchema::freeTupleSchema(keySchema);
}

TEST_F(IndexKeyTest, TwoInt64KeyTest) {
    std::vector<voltdb::ValueType> columnTypes(2, voltdb::ValueType::tBIGINT);
    std::vector<int32_t> columnLengths(2, NValue::getTupleStorageSize(voltdb::ValueType::tBIGINT));
    std::vector<bool> columnAllowNull(2, true);
    voltdb::TupleSchema *keySchema = voltdb::TupleSchema::createTupleSchemaForTest(columnTypes, columnLengths, columnAllowNull);


    voltdb::IntsKey<2>::KeyComparator comparator(keySchema);
    voltdb::IntsKey<2>::KeyHasher hasher(keySchema);
    voltdb::IntsKey<2>::KeyEqualityChecker equality(keySchema);

    voltdb::TableTuple keyTuple(keySchema);
    keyTuple.move(new char[keyTuple.tupleLength()]());
    keyTuple.setNValue(0, ValueFactory::getBigIntValue(static_cast<int64_t>(50)));
    keyTuple.setNValue(1, ValueFactory::getBigIntValue(static_cast<int64_t>(70)));

    voltdb::TableTuple otherTuple(keySchema);
    otherTuple.move(new char[otherTuple.tupleLength()]());
    otherTuple.setNValue(0, ValueFactory::getBigIntValue(static_cast<int64_t>(50)));
    otherTuple.setNValue(1, ValueFactory::getBigIntValue(static_cast<int64_t>(50)));

    voltdb::IntsKey<2> keyKey(&keyTuple);

    voltdb::IntsKey<2> otherKey(&otherTuple);

    EXPECT_FALSE(equality.operator()(keyKey, otherKey));
    EXPECT_EQ( 1, comparator.operator()(keyKey, otherKey));
    EXPECT_EQ( 0, comparator.operator()(keyKey,keyKey));
    EXPECT_EQ( 0, comparator.operator()(otherKey, otherKey));
    EXPECT_EQ(-1, comparator.operator()(otherKey, keyKey));

    voltdb::TableTuple thirdTuple(keySchema);
    thirdTuple.move(new char[thirdTuple.tupleLength()]());
    thirdTuple.setNValue(0, ValueFactory::getBigIntValue(static_cast<int64_t>(50)));
    thirdTuple.setNValue(1, ValueFactory::getBigIntValue(static_cast<int64_t>(70)));
    voltdb::IntsKey<2> thirdKey(&thirdTuple);
    EXPECT_TRUE(equality.operator()(keyKey, thirdKey));
    EXPECT_EQ( 0, comparator.operator()(keyKey, thirdKey));

    otherTuple.setNValue(0, ValueFactory::getBigIntValue(static_cast<int64_t>(50)));
    otherTuple.setNValue(1, ValueFactory::getBigIntValue(static_cast<int64_t>(70)));
    voltdb::IntsKey<2> anotherKey(&otherTuple);

    EXPECT_TRUE(equality.operator()(keyKey, anotherKey));

    EXPECT_EQ( 0, comparator.operator()(keyKey, anotherKey));

    delete [] keyTuple.address();
    delete [] otherTuple.address();
    delete [] thirdTuple.address();
    voltdb::TupleSchema::freeTupleSchema(keySchema);
}

TEST_F(IndexKeyTest, TwoInt64RegressionKeyTest) {
    std::vector<voltdb::ValueType> columnTypes(2, voltdb::ValueType::tBIGINT);
    std::vector<int32_t> columnLengths(2, NValue::getTupleStorageSize(voltdb::ValueType::tBIGINT));
    std::vector<bool> columnAllowNull(2, true);
    voltdb::TupleSchema *keySchema = voltdb::TupleSchema::createTupleSchemaForTest(columnTypes, columnLengths, columnAllowNull);


    voltdb::IntsKey<2>::KeyComparator comparator(keySchema);
    voltdb::IntsKey<2>::KeyHasher hasher(keySchema);
    voltdb::IntsKey<2>::KeyEqualityChecker equality(keySchema);

    voltdb::TableTuple firstTuple(keySchema);
    firstTuple.move(new char[firstTuple.tupleLength()]());
    firstTuple.setNValue(0, ValueFactory::getBigIntValue(static_cast<int64_t>(3)));
    firstTuple.setNValue(1, ValueFactory::getBigIntValue(static_cast<int64_t>(1)));

    voltdb::TableTuple secondTuple(keySchema);
    secondTuple.move(new char[secondTuple.tupleLength()]());
    secondTuple.setNValue(0, ValueFactory::getBigIntValue(static_cast<int64_t>(2)));
    secondTuple.setNValue(1, ValueFactory::getBigIntValue(static_cast<int64_t>(0)));

    voltdb::IntsKey<2> firstKey(&firstTuple);

    voltdb::IntsKey<2> secondKey(&secondTuple);

    EXPECT_FALSE(equality.operator()(firstKey, secondKey));
    EXPECT_EQ( 1, comparator.operator()(firstKey, secondKey));
    EXPECT_EQ(-1, comparator.operator()(secondKey, firstKey));

    voltdb::TableTuple thirdTuple(keySchema);
    thirdTuple.move(new char[thirdTuple.tupleLength()]());
    thirdTuple.setNValue(0, ValueFactory::getBigIntValue(static_cast<int64_t>(1)));
    thirdTuple.setNValue(1, ValueFactory::getBigIntValue(static_cast<int64_t>(1)));
    voltdb::IntsKey<2> thirdKey(&thirdTuple);
    EXPECT_FALSE(equality.operator()(firstKey, thirdKey));
    EXPECT_EQ( 1, comparator.operator()(firstKey, thirdKey));
    EXPECT_EQ(-1, comparator.operator()(thirdKey, firstKey));

    delete [] firstTuple.address();
    delete [] secondTuple.address();
    delete [] thirdTuple.address();
    voltdb::TupleSchema::freeTupleSchema(keySchema);
}

TEST_F(IndexKeyTest, Int32AndTwoInt8KeyTest) {
    std::vector<voltdb::ValueType> columnTypes;
    std::vector<int32_t> columnLengths;
    std::vector<bool> columnAllowNull(3, true);

    columnTypes.push_back(voltdb::ValueType::tINTEGER);
    columnTypes.push_back(voltdb::ValueType::tTINYINT);
    columnTypes.push_back(voltdb::ValueType::tTINYINT);

    columnLengths.push_back(NValue::getTupleStorageSize(voltdb::ValueType::tINTEGER));
    columnLengths.push_back(NValue::getTupleStorageSize(voltdb::ValueType::tTINYINT));
    columnLengths.push_back(NValue::getTupleStorageSize(voltdb::ValueType::tTINYINT));

    voltdb::TupleSchema *keySchema = voltdb::TupleSchema::createTupleSchemaForTest(columnTypes, columnLengths, columnAllowNull);

    voltdb::IntsKey<1>::KeyComparator comparator(keySchema);
    voltdb::IntsKey<1>::KeyHasher hasher(keySchema);
    voltdb::IntsKey<1>::KeyEqualityChecker equality(keySchema);

    voltdb::TableTuple firstTuple(keySchema);
    firstTuple.move(new char[firstTuple.tupleLength()]());
    firstTuple.setNValue(0, ValueFactory::getIntegerValue(static_cast<int32_t>(3300)));
    firstTuple.setNValue(1, ValueFactory::getTinyIntValue(static_cast<int8_t>(1)));
    firstTuple.setNValue(2, ValueFactory::getTinyIntValue(static_cast<int8_t>(1)));

    voltdb::TableTuple secondTuple(keySchema);
    secondTuple.move(new char[secondTuple.tupleLength()]());
    secondTuple.setNValue(0, ValueFactory::getIntegerValue(static_cast<int32_t>(2200)));
    secondTuple.setNValue(1, ValueFactory::getTinyIntValue(static_cast<int8_t>(1)));
    secondTuple.setNValue(2, ValueFactory::getTinyIntValue(static_cast<int8_t>(1)));

    voltdb::IntsKey<1> firstKey(&firstTuple);

    voltdb::IntsKey<1> secondKey(&secondTuple);

    EXPECT_FALSE(equality.operator()(firstKey, secondKey));
    EXPECT_EQ( 1, comparator.operator()(firstKey, secondKey));
    EXPECT_EQ(-1, comparator.operator()(secondKey, firstKey));

    voltdb::TableTuple thirdTuple(keySchema);
    thirdTuple.move(new char[thirdTuple.tupleLength()]());
    thirdTuple.setNValue(0, ValueFactory::getIntegerValue(static_cast<int32_t>(3300)));
    thirdTuple.setNValue(1, ValueFactory::getTinyIntValue(static_cast<int8_t>(1)));
    thirdTuple.setNValue(2, ValueFactory::getTinyIntValue(static_cast<int8_t>(1)));
    voltdb::IntsKey<1> thirdKey(&thirdTuple);
    EXPECT_TRUE(equality.operator()(firstKey, thirdKey));
    EXPECT_EQ( 0, comparator.operator()(firstKey, thirdKey));
    EXPECT_EQ( 0, comparator.operator()(thirdKey, firstKey));

    delete [] firstTuple.address();
    delete [] secondTuple.address();
    delete [] thirdTuple.address();
    voltdb::TupleSchema::freeTupleSchema(keySchema);
}

TEST_F(IndexKeyTest, Int32AndTwoInt8KeyTest2) {

    std::vector<voltdb::ValueType> columnTypes;
    std::vector<int32_t> columnLengths;
    std::vector<bool> columnAllowNull(3, true);

    columnTypes.push_back(voltdb::ValueType::tTINYINT);
    columnTypes.push_back(voltdb::ValueType::tTINYINT);
    columnTypes.push_back(voltdb::ValueType::tINTEGER);


    columnLengths.push_back(NValue::getTupleStorageSize(voltdb::ValueType::tTINYINT));
    columnLengths.push_back(NValue::getTupleStorageSize(voltdb::ValueType::tTINYINT));
    columnLengths.push_back(NValue::getTupleStorageSize(voltdb::ValueType::tINTEGER));


    voltdb::TupleSchema *keySchema = voltdb::TupleSchema::createTupleSchemaForTest(columnTypes, columnLengths, columnAllowNull);

    voltdb::IntsKey<1>::KeyComparator comparator(keySchema);
    voltdb::IntsKey<1>::KeyHasher hasher(keySchema);
    voltdb::IntsKey<1>::KeyEqualityChecker equality(keySchema);

    voltdb::TableTuple firstTuple(keySchema);
    firstTuple.move(new char[firstTuple.tupleLength()]());
    firstTuple.setNValue(0, ValueFactory::getTinyIntValue(static_cast<int8_t>(1)));
    firstTuple.setNValue(1, ValueFactory::getTinyIntValue(static_cast<int8_t>(1)));
    firstTuple.setNValue(2, ValueFactory::getIntegerValue(static_cast<int32_t>(-1)));

    voltdb::TableTuple secondTuple(keySchema);
    secondTuple.move(new char[secondTuple.tupleLength()]());
    secondTuple.setNValue(0, ValueFactory::getTinyIntValue(static_cast<int8_t>(1)));
    secondTuple.setNValue(1, ValueFactory::getTinyIntValue(static_cast<int8_t>(32)));
    secondTuple.setNValue(2, ValueFactory::getIntegerValue(static_cast<int32_t>(200)));

    voltdb::IntsKey<1> firstKey(&firstTuple);

    voltdb::IntsKey<1> secondKey(&secondTuple);

    EXPECT_FALSE(equality.operator()(firstKey, secondKey));
    EXPECT_EQ(-1, comparator.operator()(firstKey, secondKey));
    EXPECT_EQ( 1, comparator.operator()(secondKey, firstKey));

    voltdb::TableTuple thirdTuple(keySchema);
    thirdTuple.move(new char[thirdTuple.tupleLength()]());
    thirdTuple.setNValue(0, ValueFactory::getTinyIntValue(static_cast<int8_t>(1)));
    thirdTuple.setNValue(1, ValueFactory::getTinyIntValue(static_cast<int8_t>(1)));
    thirdTuple.setNValue(2, ValueFactory::getIntegerValue(static_cast<int32_t>(-1)));
    voltdb::IntsKey<1> thirdKey(&thirdTuple);
    EXPECT_TRUE(equality.operator()(firstKey, thirdKey));
    EXPECT_EQ( 0, comparator.operator()(firstKey, thirdKey));
    EXPECT_EQ( 0, comparator.operator()(thirdKey, firstKey));

    voltdb::TableTuple fourthTuple(keySchema);
    fourthTuple.move(new char[fourthTuple.tupleLength()]());
    fourthTuple.setNValue(0, ValueFactory::getTinyIntValue(static_cast<int8_t>(2)));
    fourthTuple.setNValue(1, ValueFactory::getTinyIntValue(static_cast<int8_t>(1)));
    fourthTuple.setNValue(2, ValueFactory::getIntegerValue(static_cast<int32_t>(-1)));
    voltdb::IntsKey<1> fourthKey(&fourthTuple);

    EXPECT_FALSE(equality.operator ()(fourthKey, firstKey));
    EXPECT_FALSE(equality.operator ()(fourthKey, secondKey));
    EXPECT_FALSE(equality.operator ()(fourthKey, thirdKey));

    EXPECT_EQ(-1, comparator.operator()(firstKey, fourthKey));
    EXPECT_EQ( 1, comparator.operator()(fourthKey, firstKey));

    delete [] firstTuple.address();
    delete [] secondTuple.address();
    delete [] thirdTuple.address();
    delete [] fourthTuple.address();
    voltdb::TupleSchema::freeTupleSchema(keySchema);
}

TEST_F(IndexKeyTest, Int32AndTwoInt8RegressionTest) {
    std::vector<voltdb::ValueType> columnTypes;
    std::vector<int32_t> columnLengths;
    std::vector<bool> columnAllowNull(3, true);

    columnTypes.push_back(voltdb::ValueType::tTINYINT);
    columnTypes.push_back(voltdb::ValueType::tTINYINT);
    columnTypes.push_back(voltdb::ValueType::tINTEGER);


    columnLengths.push_back(NValue::getTupleStorageSize(voltdb::ValueType::tTINYINT));
    columnLengths.push_back(NValue::getTupleStorageSize(voltdb::ValueType::tTINYINT));
    columnLengths.push_back(NValue::getTupleStorageSize(voltdb::ValueType::tINTEGER));

    voltdb::TupleSchema *keySchema = voltdb::TupleSchema::createTupleSchemaForTest(columnTypes, columnLengths, columnAllowNull);

    voltdb::IntsKey<1>::KeyComparator comparator(keySchema);
    voltdb::IntsKey<1>::KeyHasher hasher(keySchema);
    voltdb::IntsKey<1>::KeyEqualityChecker equality(keySchema);

    voltdb::TableTuple firstTuple(keySchema);
    firstTuple.move(new char[firstTuple.tupleLength()]());
    firstTuple.setNValue(0, ValueFactory::getTinyIntValue(static_cast<int8_t>(6)));
    firstTuple.setNValue(1, ValueFactory::getTinyIntValue(static_cast<int8_t>(1)));
    firstTuple.setNValue(2, ValueFactory::getIntegerValue(static_cast<int32_t>(3001)));

    voltdb::TableTuple secondTuple(keySchema);
    secondTuple.move(new char[secondTuple.tupleLength()]());
    secondTuple.setNValue(0, ValueFactory::getTinyIntValue(static_cast<int8_t>(7)));
    secondTuple.setNValue(1, ValueFactory::getTinyIntValue(static_cast<int8_t>(1)));
    secondTuple.setNValue(2, ValueFactory::getIntegerValue(static_cast<int32_t>(3000)));

    voltdb::IntsKey<1> firstKey(&firstTuple);

    voltdb::IntsKey<1> secondKey(&secondTuple);

    EXPECT_FALSE(equality.operator()(firstKey, secondKey));
    EXPECT_EQ(-1, comparator.operator()(firstKey, secondKey));
    EXPECT_EQ( 1, comparator.operator()(secondKey, firstKey));

    delete [] firstTuple.address();
    delete [] secondTuple.address();
    voltdb::TupleSchema::freeTupleSchema(keySchema);
}

TEST_F(IndexKeyTest, SingleVarChar30) {
    std::vector<voltdb::ValueType> columnTypes;
    std::vector<int32_t> columnLengths;
    std::vector<bool> columnAllowNull(1, true);

    columnTypes.push_back(voltdb::ValueType::tVARCHAR);
    columnLengths.push_back(30);

    voltdb::TupleSchema *keySchema = voltdb::TupleSchema::createKeySchema(columnTypes, columnLengths, columnAllowNull);

    voltdb::GenericKey<40>::KeyComparator comparator(keySchema);
    voltdb::GenericKey<40>::KeyHasher hasher(keySchema);
    voltdb::GenericKey<40>::KeyEqualityChecker equality(keySchema);

    voltdb::TableTuple firstTuple(keySchema);
    firstTuple.move(new char[firstTuple.tupleLength()]());
    voltdb::NValue firstValue = ValueFactory::getStringValue("value");
    firstTuple.setNValue(0, firstValue);

    voltdb::TableTuple secondTuple(keySchema);
    secondTuple.move(new char[secondTuple.tupleLength()]());
    voltdb::NValue secondValue = ValueFactory::getStringValue("value2");
    secondTuple.setNValue(0, secondValue);

    voltdb::TableTuple thirdTuple(keySchema);
    thirdTuple.move(new char[thirdTuple.tupleLength()]());
    voltdb::NValue thirdValue = ValueFactory::getStringValue("value");
    thirdTuple.setNValue(0, thirdValue);

    voltdb::GenericKey<40> firstKey(&firstTuple);
    voltdb::GenericKey<40> secondKey(&secondTuple);
    voltdb::GenericKey<40> thirdKey(&thirdTuple);

    EXPECT_FALSE(equality.operator()(firstKey, secondKey));
    EXPECT_TRUE(equality.operator()(firstKey, thirdKey));

    delete [] firstTuple.address();
    delete [] secondTuple.address();
    delete [] thirdTuple.address();
    firstValue.free();
    secondValue.free();
    thirdValue.free();
    voltdb::TupleSchema::freeTupleSchema(keySchema);
}

TEST_F(IndexKeyTest, Int64Packing2Int32sWithSecondNull) {
    std::vector<voltdb::ValueType> columnTypes;
    std::vector<int32_t> columnLengths;
    std::vector<bool> columnAllowNull(2, true);

    columnTypes.push_back(voltdb::ValueType::tINTEGER);
    columnTypes.push_back(voltdb::ValueType::tINTEGER);
    columnLengths.push_back(NValue::getTupleStorageSize(voltdb::ValueType::tINTEGER));
    columnLengths.push_back(NValue::getTupleStorageSize(voltdb::ValueType::tINTEGER));

    voltdb::TupleSchema *keySchema = voltdb::TupleSchema::createTupleSchemaForTest(columnTypes, columnLengths, columnAllowNull);

    voltdb::IntsKey<1>::KeyComparator comparator(keySchema);

    voltdb::TableTuple firstTuple(keySchema);
    firstTuple.move(new char[firstTuple.tupleLength()]());

    firstTuple.setNValue(0, ValueFactory::getIntegerValue(0));
    firstTuple.setNValue(1, ValueFactory::getIntegerValue(INT32_NULL));

    voltdb::TableTuple secondTuple(keySchema);
    secondTuple.move(new char[secondTuple.tupleLength()]());
    secondTuple.setNValue(0, ValueFactory::getIntegerValue(0));
    secondTuple.setNValue(1, ValueFactory::getIntegerValue(0));


    voltdb::TableTuple thirdTuple(keySchema);
    thirdTuple.move(new char[thirdTuple.tupleLength()]());
    thirdTuple.setNValue(0, ValueFactory::getIntegerValue(0));
    thirdTuple.setNValue(1, ValueFactory::getIntegerValue(1));

    voltdb::IntsKey<1>  firstKey(&firstTuple);
    voltdb::IntsKey<1>  secondKey(&secondTuple);
    voltdb::IntsKey<1>  thirdKey(&thirdTuple);

    EXPECT_EQ(-1, comparator.operator()(firstKey, thirdKey));
    EXPECT_EQ(-1, comparator.operator()(firstKey, secondKey));

    delete [] firstTuple.address();
    delete [] secondTuple.address();
    delete [] thirdTuple.address();
    voltdb::TupleSchema::freeTupleSchema(keySchema);
}

int main() {
    return TestSuite::globalInstance()->runAll();
}
