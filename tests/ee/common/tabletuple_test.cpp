/* This file is part of VoltDB.
 * Copyright (C) 2008-2017 VoltDB Inc.
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
#include "common/tabletuple.h"
#include "common/ValueFactory.hpp"
#include "common/ThreadLocalPool.h"
#include "common/TupleSchemaBuilder.h"
#include "test_utils/ScopedTupleSchema.hpp"

using namespace voltdb;
using namespace std;

class TableTupleTest : public Test
{
    ThreadLocalPool m_pool;
};

TEST_F(TableTupleTest, ComputeNonInlinedMemory)
{
    vector<bool> column_allow_null(2, true);
    vector<ValueType> all_types;
    all_types.push_back(VALUE_TYPE_BIGINT);
    all_types.push_back(VALUE_TYPE_VARCHAR);

    // Make sure that inlined strings are actually inlined
    vector<int32_t> all_inline_lengths;
    all_inline_lengths.push_back(NValue::
                                 getTupleStorageSize(VALUE_TYPE_BIGINT));
    all_inline_lengths.push_back(UNINLINEABLE_OBJECT_LENGTH/MAX_BYTES_PER_UTF8_CHARACTER - 1);
    TupleSchema* all_inline_schema =
        TupleSchema::createTupleSchemaForTest(all_types,
                                       all_inline_lengths,
                                       column_allow_null);

    TableTuple inline_tuple(all_inline_schema);
    inline_tuple.move(new char[inline_tuple.tupleLength()]);
    inline_tuple.setNValue(0, ValueFactory::getBigIntValue(100));
    NValue inline_string = ValueFactory::getStringValue("dude");
    inline_tuple.setNValue(1, inline_string);
    EXPECT_EQ(0, inline_tuple.getNonInlinedMemorySize());

    delete[] inline_tuple.address();
    inline_string.free();
    TupleSchema::freeTupleSchema(all_inline_schema);

    // Now check that an non-inlined schema returns the right thing.
    vector<int32_t> non_inline_lengths;
    non_inline_lengths.push_back(NValue::
                                 getTupleStorageSize(VALUE_TYPE_BIGINT));
    non_inline_lengths.push_back(UNINLINEABLE_OBJECT_LENGTH + 10000);
    TupleSchema* non_inline_schema =
        TupleSchema::createTupleSchemaForTest(all_types,
                                       non_inline_lengths,
                                       column_allow_null);

    TableTuple non_inline_tuple(non_inline_schema);
    non_inline_tuple.move(new char[non_inline_tuple.tupleLength()]);
    non_inline_tuple.setNValue(0, ValueFactory::getBigIntValue(100));
    string strval = "123456";
    NValue non_inline_string = ValueFactory::getStringValue(strval);
    non_inline_tuple.setNValue(1, non_inline_string);
    EXPECT_EQ(non_inline_string.getAllocationSizeForObject(),
              non_inline_tuple.getNonInlinedMemorySize());

    delete[] non_inline_tuple.address();
    non_inline_string.free();
    TupleSchema::freeTupleSchema(non_inline_schema);
}

TEST_F(TableTupleTest, HiddenColumns)
{
    TupleSchemaBuilder builder(2, 2);
    builder.setColumnAtIndex(0, VALUE_TYPE_BIGINT);
    builder.setColumnAtIndex(1, VALUE_TYPE_VARCHAR, 256);
    builder.setHiddenColumnAtIndex(0, VALUE_TYPE_BIGINT);
    builder.setHiddenColumnAtIndex(1, VALUE_TYPE_VARCHAR, 10);
    ScopedTupleSchema schema(builder.build());

    StandAloneTupleStorage autoStorage(schema.get());
    const TableTuple& tuple = autoStorage.tuple();

    NValue nvalVisibleBigint = ValueFactory::getBigIntValue(999);
    NValue nvalVisibleString = ValueFactory::getStringValue("catdog");
    NValue nvalHiddenBigint = ValueFactory::getBigIntValue(1066);
    NValue nvalHiddenString = ValueFactory::getStringValue("platypus");

    tuple.setNValue(0, nvalVisibleBigint);
    tuple.setNValue(1, nvalVisibleString);
    tuple.setHiddenNValue(0, nvalHiddenBigint);
    tuple.setHiddenNValue(1, nvalHiddenString);

    EXPECT_EQ(0, tuple.getNValue(0).compare(nvalVisibleBigint));
    EXPECT_EQ(0, tuple.getNValue(1).compare(nvalVisibleString));
    EXPECT_EQ(0, tuple.getHiddenNValue(0).compare(nvalHiddenBigint));
    EXPECT_EQ(0, tuple.getHiddenNValue(1).compare(nvalHiddenString));

    EXPECT_EQ(8 + (4 + 6) + 8 + (4 + 8), tuple.maxDRSerializationSize());

    tuple.setHiddenNValue(1, ValueFactory::getNullStringValue());
    nvalHiddenString.free();

    // The hidden string is null, takes 0 serialized byte
    EXPECT_EQ(8 + (4 + 6) + 8, tuple.maxDRSerializationSize());

    nvalVisibleString.free();
}

TEST_F(TableTupleTest, ToJsonArray)
{
    TupleSchemaBuilder builder(3, 2);
    builder.setColumnAtIndex(0, VALUE_TYPE_BIGINT);
    builder.setColumnAtIndex(1, VALUE_TYPE_VARCHAR, 256);
    builder.setColumnAtIndex(2, VALUE_TYPE_VARCHAR, 256);
    builder.setHiddenColumnAtIndex(0, VALUE_TYPE_BIGINT);
    builder.setHiddenColumnAtIndex(1, VALUE_TYPE_VARCHAR, 10);
    ScopedTupleSchema schema(builder.build());

    StandAloneTupleStorage autoStorage(schema.get());
    const TableTuple& tuple = autoStorage.tuple();

    NValue nvalVisibleBigint = ValueFactory::getBigIntValue(999);
    NValue nvalVisibleString = ValueFactory::getStringValue("数据库");
    NValue nvalHiddenBigint = ValueFactory::getBigIntValue(1066);
    NValue nvalHiddenString = ValueFactory::getStringValue("platypus");

    tuple.setNValue(0, nvalVisibleBigint);
    tuple.setNValue(1, nvalVisibleString);
    tuple.setNValue(2, ValueFactory::getNullValue());
    tuple.setHiddenNValue(0, nvalHiddenBigint);
    tuple.setHiddenNValue(1, nvalHiddenString);

    EXPECT_EQ(0, strcmp(tuple.toJsonArray().c_str(), "[\"999\",\"数据库\",\"null\"]"));

    nvalHiddenString.free();
    nvalVisibleString.free();
}

int main() {
    return TestSuite::globalInstance()->runAll();
}
