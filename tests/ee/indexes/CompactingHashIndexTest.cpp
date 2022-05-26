/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
 *
 * This file contains original code and/or modifications of original code.
 * Any modifications made by Volt Active Data Inc. are licensed under the following
 * terms and conditions:
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
/* Copyright (C) 2008 by H-Store Project
 * Brown University
 * Massachusetts Institute of Technology
 * Yale University
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
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

#include "harness.h"
#include "common/common.h"
#include "common/NValue.hpp"
#include "common/ValueFactory.hpp"
#include "common/tabletuple.h"
#include "indexes/tableindex.h"
#include "indexes/tableindexfactory.h"

using namespace std;
using namespace voltdb;

class CompactingHashIndexTest : public Test {
public:
    CompactingHashIndexTest() {}
};

TableTuple *newTuple(TupleSchema *schema, int idx, long value) {
    TableTuple *tuple = new TableTuple(schema);
    char *data = new char[tuple->tupleLength()];
    memset(data, 0, tuple->tupleLength());
    tuple->move(data);

    tuple->setNValue(idx, ValueFactory::getBigIntValue(value));
    return tuple;
}

TEST_F(CompactingHashIndexTest, ENG1193) {
    TableIndex *index = NULL;
    vector<int> columnIndices;
    vector<ValueType> columnTypes;
    vector<int32_t> columnLengths;
    vector<bool> columnAllowNull;

    columnIndices.push_back(0);
    columnTypes.push_back(ValueType::tBIGINT);
    columnLengths.push_back(NValue::getTupleStorageSize(ValueType::tBIGINT));
    columnAllowNull.push_back(false);

    TupleSchema *schema = TupleSchema::createTupleSchemaForTest(columnTypes,
                                                         columnLengths,
                                                         columnAllowNull);

    TableIndexScheme scheme("test_index", HASH_TABLE_INDEX,
                            columnIndices, TableIndex::simplyIndexColumns(),
                            false, false, false, schema);
    index = TableIndexFactory::getInstance(scheme);

    TableTuple *tuple1 = newTuple(schema, 0, 10);
    index->addEntry(tuple1, NULL);
    TableTuple *tuple2 = newTuple(schema, 0, 11);
    index->addEntry(tuple2, NULL);
    TableTuple *tuple3 = newTuple(schema, 0, 12);
    index->addEntry(tuple3, NULL);

    TableTuple *tuple4 = newTuple(schema, 0, 10);
    EXPECT_TRUE(index->replaceEntryNoKeyChange(*tuple4, *tuple1));

    EXPECT_FALSE(index->exists(tuple1));
    EXPECT_TRUE(index->exists(tuple2));
    EXPECT_TRUE(index->exists(tuple3));
    EXPECT_TRUE(index->exists(tuple4));

    delete index;
    TupleSchema::freeTupleSchema(schema);
    delete[] tuple1->address();
    delete tuple1;
    delete[] tuple2->address();
    delete tuple2;
    delete[] tuple3->address();
    delete tuple3;
    delete[] tuple4->address();
    delete tuple4;
}

int main()
{
    return TestSuite::globalInstance()->runAll();
}
