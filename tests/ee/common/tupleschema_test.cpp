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

#include <vector>

#include "harness.h"
#include "common/TupleSchema.h"

using voltdb::TupleSchema;
using voltdb::ValueType;
using voltdb::VALUE_TYPE_INTEGER;
using voltdb::VALUE_TYPE_VARCHAR;

class TupleSchemaTest : public Test
{

};

TEST_F(TupleSchemaTest, Basic)
{
    std::vector<ValueType> types;
    std::vector<int32_t> sizes;
    std::vector<bool> allowNullFlags;
    std::vector<bool> inBytesFlags;

    types.push_back(voltdb::VALUE_TYPE_INTEGER);
    sizes.push_back(4);
    allowNullFlags.push_back(true);
    inBytesFlags.push_back(false);

    types.push_back(VALUE_TYPE_VARCHAR);
    sizes.push_back(256);
    allowNullFlags.push_back(false);
    inBytesFlags.push_back(true);

    TupleSchema* schema = TupleSchema::createTupleSchema(types,
                                                         sizes,
                                                         allowNullFlags,
                                                         inBytesFlags);

    ASSERT_NE(NULL, schema);
    ASSERT_EQ(2, schema->columnCount());

    // 4 bytes for the integer
    // 8 bytes for the string pointer
    EXPECT_EQ(12, schema->tupleLength());
    EXPECT_EQ(1, schema->getUninlinedObjectColumnCount());
    EXPECT_EQ(1, schema->getUninlinedObjectColumnInfoIndex(0));

    const TupleSchema::ColumnInfo *colInfo = schema->getColumnInfo(0);
    ASSERT_NE(NULL, colInfo);
    EXPECT_EQ(0, colInfo->offset);
    EXPECT_EQ(4, colInfo->length);
    EXPECT_EQ(VALUE_TYPE_INTEGER, colInfo->type);
    EXPECT_EQ(1, colInfo->allowNull);
    EXPECT_EQ(true, colInfo->inlined);
    EXPECT_EQ(false, colInfo->inBytes);

    colInfo = schema->getColumnInfo(1);
    ASSERT_NE(NULL, colInfo);
    EXPECT_EQ(4, colInfo->offset);
    EXPECT_EQ(256, colInfo->length);
    EXPECT_EQ(VALUE_TYPE_VARCHAR, colInfo->type);
    EXPECT_EQ(false, colInfo->allowNull);
    EXPECT_EQ(false, colInfo->inlined);
    EXPECT_EQ(true, colInfo->inBytes);

    TupleSchema::freeTupleSchema(schema);
    schema = NULL;
}

int main() {
    return TestSuite::globalInstance()->runAll();
}
