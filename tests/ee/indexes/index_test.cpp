/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB L.L.C.
 *
 * This file contains original code and/or modifications of original code.
 * Any modifications made by VoltDB L.L.C. are licensed under the following
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
#include "common/debuglog.h"
#include "common/SerializableEEException.h"
#include "common/tabletuple.h"
#include "storage/table.h"
#include "storage/temptable.h"
#include "storage/persistenttable.h"
#include "storage/tablefactory.h"
#include "storage/tableiterator.h"
#include "storage/tableutil.h"
#include "indexes/tableindex.h"
#include "indexes/tableindexfactory.h"
#include "execution/VoltDBEngine.h"


using namespace std;
using namespace voltdb;

#define NUM_OF_COLUMNS 5
#define NUM_OF_TUPLES 1000
#define PKEY_ID 100
#define INT_UNIQUE_ID 101
#define INT_MULTI_ID 102
#define INTS_UNIQUE_ID 103
#define INTS_MULTI_ID 104
#define ARRAY_UNIQUE_ID 105

class IndexTest : public Test {
public:
    IndexTest() : table(NULL) {}
    ~IndexTest()
    {
        delete table;
        delete[] m_exceptionBuffer;
        delete m_engine;
    }

    void init(TableIndexScheme index)
    {
        CatalogId database_id = 1000;
        vector<boost::shared_ptr<const TableColumn> > columns;

        string *columnNames = new string[NUM_OF_COLUMNS];

        char buffer[32];
        vector<ValueType> columnTypes(NUM_OF_COLUMNS, VALUE_TYPE_BIGINT);
        vector<int32_t>
            columnLengths(NUM_OF_COLUMNS,
                          NValue::getTupleStorageSize(VALUE_TYPE_BIGINT));
        vector<bool> columnAllowNull(NUM_OF_COLUMNS, false);
        for (int ctr = 0; ctr < NUM_OF_COLUMNS; ctr++)
        {
            sprintf(buffer, "column%02d", ctr);
            columnNames[ctr] = buffer;
        }
        TupleSchema* schema =
            TupleSchema::createTupleSchema(columnTypes,
                                           columnLengths,
                                           columnAllowNull,
                                           true);

        index.tupleSchema = schema;
        vector<int> pkey_column_indices;
        vector<ValueType> pkey_column_types;
        pkey_column_indices.push_back(0);
        pkey_column_indices.push_back(1);
        pkey_column_types.push_back(VALUE_TYPE_BIGINT);
        pkey_column_types.push_back(VALUE_TYPE_BIGINT);
        TableIndexScheme pkey("idx_pkey",
                              BALANCED_TREE_INDEX,
                              pkey_column_indices,
                              pkey_column_types,
                              true, true, schema);

        vector<TableIndexScheme> indexes;
        indexes.push_back(index);
        m_engine = new VoltDBEngine();
        m_exceptionBuffer = new char[4096];
        m_engine->setBuffers( NULL, 0, NULL, 0, m_exceptionBuffer, 4096);
        m_engine->initialize(0, 0, 0, 0, "");
        table =
            dynamic_cast<PersistentTable*>
          (TableFactory::getPersistentTable(database_id, m_engine->getExecutorContext(),
                                            "test_table", schema,
                                            columnNames, pkey, indexes, -1, false, false));

        delete[] columnNames;

        for (int64_t i = 1; i <= NUM_OF_TUPLES; ++i)
        {
            TableTuple &tuple = table->tempTuple();
            tuple.setNValue(0, ValueFactory::getBigIntValue(i));
            tuple.setNValue(1, ValueFactory::getBigIntValue(i % 2));
            tuple.setNValue(2, ValueFactory::getBigIntValue(i % 3));
            tuple.setNValue(3, ValueFactory::getBigIntValue(i + 20));
            tuple.setNValue(4, ValueFactory::getBigIntValue(i * 11));
            assert(true == table->insertTuple(tuple));
        }
    }

protected:
    PersistentTable* table;
    char* m_exceptionBuffer;
    VoltDBEngine* m_engine;
};

TEST_F(IndexTest, IntUnique) {
    vector<int> iu_column_indices;
    vector<ValueType> iu_column_types;
    iu_column_indices.push_back(3);
    iu_column_types.push_back(VALUE_TYPE_BIGINT);

    init(TableIndexScheme("iu",
                          BALANCED_TREE_INDEX,
                          iu_column_indices,
                          iu_column_types,
                          true, true, NULL));
    TableIndex* index = table->index("iu");
    EXPECT_EQ(true, index != NULL);
    // TODO
}

TEST_F(IndexTest, ArrayUnique) {
    vector<int> iu_column_indices;
    vector<ValueType> iu_column_types;
    iu_column_indices.push_back(0);
    iu_column_types.push_back(VALUE_TYPE_BIGINT);
    init(TableIndexScheme("iu2",
                          BALANCED_TREE_INDEX,
                          iu_column_indices,
                          iu_column_types,
                          true, true, NULL));
    TableIndex* index = table->index("iu2");
    EXPECT_EQ(true, index != NULL);

    TableTuple tuple(table->schema());
    vector<ValueType> keyColumnTypes(1, VALUE_TYPE_BIGINT);
    vector<int32_t>
        keyColumnLengths(1, NValue::getTupleStorageSize(VALUE_TYPE_BIGINT));
    vector<bool> keyColumnAllowNull(1, true);
    TupleSchema* keySchema =
        TupleSchema::createTupleSchema(keyColumnTypes,
                                       keyColumnLengths,
                                       keyColumnAllowNull,
                                       true);
    TableTuple searchkey(keySchema);
    searchkey.move(new char[searchkey.tupleLength()]);
    searchkey.
        setNValue(0, ValueFactory::getBigIntValue(static_cast<int64_t>(50)));
    bool found = index->moveToKey(&searchkey);
    EXPECT_EQ(found, true);
    int count = 0;
    while (!(tuple = index->nextValueAtKey()).isNullTuple())
    {
        ++count;
        EXPECT_TRUE(ValueFactory::getBigIntValue(50).
                    op_equals(tuple.getNValue(0)).isTrue());
        EXPECT_TRUE(ValueFactory::getBigIntValue(50 % 2).
                    op_equals(tuple.getNValue(1)).isTrue());
        EXPECT_TRUE(ValueFactory::getBigIntValue(50 % 3).
                    op_equals(tuple.getNValue(2)).isTrue());
        EXPECT_TRUE(ValueFactory::getBigIntValue(50 + 20).
                    op_equals(tuple.getNValue(3)).isTrue());
        EXPECT_TRUE(ValueFactory::getBigIntValue(50 * 11).
                    op_equals(tuple.getNValue(4)).isTrue());
    }
    EXPECT_EQ(1, count);

    searchkey.
        setNValue(0, ValueFactory::getBigIntValue(static_cast<int64_t>(1001)));
    found = index->moveToKey(&searchkey);
    count = 0;
    while (!(tuple = index->nextValueAtKey()).isNullTuple())
    {
        ++count;
    }
    EXPECT_EQ(0, count);

    TableTuple &tmptuple = table->tempTuple();
    tmptuple.
        setNValue(0, ValueFactory::getBigIntValue(static_cast<int64_t>(1234)));
    tmptuple.setNValue(1, ValueFactory::getBigIntValue(0));
    tmptuple.
        setNValue(2, ValueFactory::getBigIntValue(static_cast<int64_t>(3333)));
    tmptuple.
        setNValue(3, ValueFactory::getBigIntValue(static_cast<int64_t>(-200)));
    tmptuple.
        setNValue(4, ValueFactory::getBigIntValue(static_cast<int64_t>(550)));
    EXPECT_EQ(true, table->insertTuple(tmptuple));
    tmptuple.
        setNValue(0, ValueFactory::getBigIntValue(static_cast<int64_t>(1234)));
    tmptuple.
        setNValue(1, ValueFactory::getBigIntValue(0));
    tmptuple.
        setNValue(2, ValueFactory::getBigIntValue(static_cast<int64_t>(50 % 3)));
    tmptuple.
        setNValue(3, ValueFactory::getBigIntValue(static_cast<int64_t>(-200)));
    tmptuple.
        setNValue(4, ValueFactory::getBigIntValue(static_cast<int64_t>(550)));
    TupleSchema::freeTupleSchema(keySchema);
    delete[] searchkey.address();
    bool exceptionThrown = false;
    try
    {
        EXPECT_EQ(false, table->insertTuple(tmptuple));
    }
    catch (SerializableEEException &e)
    {
        exceptionThrown = true;
    }
    EXPECT_TRUE(exceptionThrown);
}

TEST_F(IndexTest, IntMulti) {
    vector<int> im_column_indices;
    vector<ValueType> im_column_types;
    im_column_indices.push_back(3);
    im_column_types.push_back(VALUE_TYPE_BIGINT);
    init(TableIndexScheme("im",
                          BALANCED_TREE_INDEX,
                          im_column_indices,
                          im_column_types,
                          false, true, NULL));
    TableIndex* index = table->index("im");
    EXPECT_EQ(true, index != NULL);
    // TODO
}

TEST_F(IndexTest, IntsUnique) {
    vector<int> ixu_column_indices;
    vector<ValueType> ixu_column_types;
    ixu_column_indices.push_back(4);
    ixu_column_indices.push_back(2);
    ixu_column_types.push_back(VALUE_TYPE_BIGINT);
    ixu_column_types.push_back(VALUE_TYPE_BIGINT);
    init(TableIndexScheme("ixu",
                          BALANCED_TREE_INDEX,
                          ixu_column_indices,
                          ixu_column_types,
                          true, true, NULL));

    TableIndex* index = table->index("ixu");
    EXPECT_EQ(true, index != NULL);

    TableTuple tuple(table->schema());
    vector<ValueType> keyColumnTypes(2, VALUE_TYPE_BIGINT);
    vector<int32_t>
        keyColumnLengths(2, NValue::getTupleStorageSize(VALUE_TYPE_BIGINT));
    vector<bool> keyColumnAllowNull(2, true);
    TupleSchema* keySchema =
        TupleSchema::createTupleSchema(keyColumnTypes,
                                       keyColumnLengths,
                                       keyColumnAllowNull,
                                       true);
    TableTuple searchkey(keySchema);
    searchkey.move(new char[searchkey.tupleLength()]);
    searchkey.
        setNValue(0, ValueFactory::getBigIntValue(static_cast<int64_t>(550)));
    searchkey.
        setNValue(1, ValueFactory::getBigIntValue(static_cast<int64_t>(2)));
    index->moveToKey(&searchkey);
    int count = 0;
    while (!(tuple = index->nextValueAtKey()).isNullTuple())
    {
        ++count;
        EXPECT_TRUE(ValueFactory::getBigIntValue(50).
                    op_equals(tuple.getNValue(0)).isTrue());
        EXPECT_TRUE(ValueFactory::getBigIntValue(50 % 2).
                    op_equals(tuple.getNValue(1)).isTrue());
        EXPECT_TRUE(ValueFactory::getBigIntValue(50 % 3).
                    op_equals(tuple.getNValue(2)).isTrue());
        EXPECT_TRUE(ValueFactory::getBigIntValue(50 + 20).
                    op_equals(tuple.getNValue(3)).isTrue());
        EXPECT_TRUE(ValueFactory::getBigIntValue(50 * 11).
                    op_equals(tuple.getNValue(4)).isTrue());
    }
    EXPECT_EQ(1, count);

    searchkey.
        setNValue(0, ValueFactory::getBigIntValue(static_cast<int64_t>(550)));
    searchkey.
        setNValue(0, ValueFactory::getBigIntValue(static_cast<int64_t>(1)));
    index->moveToKey(&searchkey);
    count = 0;
    while (!(tuple = index->nextValueAtKey()).isNullTuple())
    {
        ++count;
    }
    EXPECT_EQ(0, count);

    // partial index search test
    searchkey.
        setNValue(0, ValueFactory::getBigIntValue(static_cast<int64_t>(440)));
    searchkey.
        setNValue(1, ValueFactory::getBigIntValue(static_cast<int64_t>(-10000000)));
    index->moveToKeyOrGreater(&searchkey);
    tuple = index->nextValue();
    EXPECT_TRUE(ValueFactory::getBigIntValue(40).
                op_equals(tuple.getNValue(0)).isTrue());
    EXPECT_TRUE(ValueFactory::getBigIntValue(40 % 2).
                op_equals(tuple.getNValue(1)).isTrue());
    EXPECT_TRUE(ValueFactory::getBigIntValue(40 % 3).
                op_equals(tuple.getNValue(2)).isTrue());
    EXPECT_TRUE(ValueFactory::getBigIntValue(40 + 20).
                op_equals(tuple.getNValue(3)).isTrue());
    EXPECT_TRUE(ValueFactory::getBigIntValue(40 * 11).
                op_equals(tuple.getNValue(4)).isTrue());
    tuple = index->nextValue();
    EXPECT_TRUE(ValueFactory::getBigIntValue(41).
                op_equals(tuple.getNValue(0)).isTrue());
    EXPECT_TRUE(ValueFactory::getBigIntValue(41 % 2).
                op_equals(tuple.getNValue(1)).isTrue());
    EXPECT_TRUE(ValueFactory::getBigIntValue(41 % 3).
                op_equals(tuple.getNValue(2)).isTrue());
    EXPECT_TRUE(ValueFactory::getBigIntValue(41 + 20).
                op_equals(tuple.getNValue(3)).isTrue());
    EXPECT_TRUE(ValueFactory::getBigIntValue(41 * 11).
                op_equals(tuple.getNValue(4)).isTrue());

    searchkey.
        setNValue(0, ValueFactory::getBigIntValue(static_cast<int64_t>(440)));
    searchkey.
        setNValue(1, ValueFactory::getBigIntValue(static_cast<int64_t>(10000000)));
    index->moveToKeyOrGreater(&searchkey);
    tuple = index->nextValue();
    EXPECT_TRUE(ValueFactory::getBigIntValue(41).
                op_equals(tuple.getNValue(0)).isTrue());
    EXPECT_TRUE(ValueFactory::getBigIntValue(41 % 2).
                op_equals(tuple.getNValue(1)).isTrue());
    EXPECT_TRUE(ValueFactory::getBigIntValue(41 % 3).
                op_equals(tuple.getNValue(2)).isTrue());
    EXPECT_TRUE(ValueFactory::getBigIntValue(41 + 20).
                op_equals(tuple.getNValue(3)).isTrue());
    EXPECT_TRUE(ValueFactory::getBigIntValue(41 * 11).
                op_equals(tuple.getNValue(4)).isTrue());
    tuple = index->nextValue();
    EXPECT_TRUE(ValueFactory::getBigIntValue(42).
                op_equals(tuple.getNValue(0)).isTrue());
    EXPECT_TRUE(ValueFactory::getBigIntValue(42 % 2).
                op_equals(tuple.getNValue(1)).isTrue());
    EXPECT_TRUE(ValueFactory::getBigIntValue(42 % 3).
                op_equals(tuple.getNValue(2)).isTrue());
    EXPECT_TRUE(ValueFactory::getBigIntValue(42 + 20).
                op_equals(tuple.getNValue(3)).isTrue());
    EXPECT_TRUE(ValueFactory::getBigIntValue(42 * 11).
                op_equals(tuple.getNValue(4)).isTrue());

    // moveToGreaterThanKey test
    searchkey.
        setNValue(0, ValueFactory::getBigIntValue(static_cast<int64_t>(330)));
    searchkey.
        setNValue(1, ValueFactory::getBigIntValue(static_cast<int64_t>(30%3)));
    index->moveToGreaterThanKey(&searchkey);
    tuple = index->nextValue();
    EXPECT_TRUE(ValueFactory::getBigIntValue(31).
                op_equals(tuple.getNValue(0)).isTrue());
    EXPECT_TRUE(ValueFactory::getBigIntValue(31 % 2).
                op_equals(tuple.getNValue(1)).isTrue());
    EXPECT_TRUE(ValueFactory::getBigIntValue(31 % 3).
                op_equals(tuple.getNValue(2)).isTrue());
    EXPECT_TRUE(ValueFactory::getBigIntValue(31 + 20).
                op_equals(tuple.getNValue(3)).isTrue());
    EXPECT_TRUE(ValueFactory::getBigIntValue(31 * 11).
                op_equals(tuple.getNValue(4)).isTrue());

    TableTuple &tmptuple = table->tempTuple();
    tmptuple.
        setNValue(0, ValueFactory::getBigIntValue(static_cast<int16_t>(1234)));
    tmptuple.
        setNValue(1, ValueFactory::getBigIntValue(static_cast<int64_t>(0)));
    tmptuple.
        setNValue(2, ValueFactory::getBigIntValue(static_cast<int64_t>(3333)));
    tmptuple.
        setNValue(3, ValueFactory::getBigIntValue(static_cast<int64_t>(-200)));
    tmptuple.
        setNValue(4, ValueFactory::getBigIntValue(static_cast<int64_t>(550)));
    EXPECT_EQ(true, table->insertTuple(tmptuple));
    tmptuple.
        setNValue(0, ValueFactory::getBigIntValue(static_cast<int16_t>(1235)));
    tmptuple.
        setNValue(1, ValueFactory::getBigIntValue(static_cast<int64_t>(0)));
    tmptuple.
        setNValue(2, ValueFactory::getBigIntValue(static_cast<int64_t>(50 % 3)));
    tmptuple.
        setNValue(3, ValueFactory::getBigIntValue(static_cast<int64_t>(-200)));
    tmptuple.
        setNValue(4, ValueFactory::getBigIntValue(static_cast<int64_t>(550)));
    bool exceptionThrown = false;
    try
    {
        EXPECT_EQ(false, table->insertTuple(tmptuple));
    }
    catch (SerializableEEException &e)
    {
        exceptionThrown = true;
    }
    EXPECT_TRUE(exceptionThrown);
    TupleSchema::freeTupleSchema(keySchema);
    delete[] searchkey.address();
}

TEST_F(IndexTest, IntsMulti) {
    vector<int> ixm_column_indices;
    vector<ValueType> ixm_column_types;
    ixm_column_indices.push_back(4);
    ixm_column_indices.push_back(2);
    ixm_column_types.push_back(VALUE_TYPE_BIGINT);
    ixm_column_types.push_back(VALUE_TYPE_BIGINT);
    init(TableIndexScheme("ixm2",
                          BALANCED_TREE_INDEX,
                          ixm_column_indices,
                          ixm_column_types,
                          false, true, NULL));

    TableIndex* index = table->index("ixm2");
    EXPECT_EQ(true, index != NULL);
    TableTuple tuple(table->schema());
    vector<ValueType> keyColumnTypes(2, VALUE_TYPE_BIGINT);
    vector<int32_t>
        keyColumnLengths(2, NValue::getTupleStorageSize(VALUE_TYPE_BIGINT));
    vector<bool> keyColumnAllowNull(2, true);
    TupleSchema* keySchema =
        TupleSchema::createTupleSchema(keyColumnTypes,
                                       keyColumnLengths,
                                       keyColumnAllowNull,
                                       true);
    TableTuple searchkey(keySchema);
    searchkey.move(new char[searchkey.tupleLength()]);
    searchkey.
        setNValue(0, ValueFactory::getBigIntValue(static_cast<int64_t>(550)));
    searchkey.
        setNValue(1, ValueFactory::getBigIntValue(static_cast<int64_t>(2)));
    index->moveToKey(&searchkey);
    int count = 0;
    while (!(tuple = index->nextValueAtKey()).isNullTuple())
    {
        ++count;
        EXPECT_TRUE(ValueFactory::getBigIntValue(50).
                    op_equals(tuple.getNValue(0)).isTrue());
        EXPECT_TRUE(ValueFactory::getBigIntValue(50 % 2).
                    op_equals(tuple.getNValue(1)).isTrue());
        EXPECT_TRUE(ValueFactory::getBigIntValue(50 % 3).
                    op_equals(tuple.getNValue(2)).isTrue());
        EXPECT_TRUE(ValueFactory::getBigIntValue(50 + 20).
                    op_equals(tuple.getNValue(3)).isTrue());
        EXPECT_TRUE(ValueFactory::getBigIntValue(50 * 11).
                    op_equals(tuple.getNValue(4)).isTrue());
    }
    EXPECT_EQ(1, count);

    searchkey.
        setNValue(0, ValueFactory::getBigIntValue(static_cast<int64_t>(550)));
    searchkey.
        setNValue(1, ValueFactory::getBigIntValue(static_cast<int64_t>(1)));
    index->moveToKey(&searchkey);
    count = 0;
    while (!(tuple = index->nextValueAtKey()).isNullTuple())
    {
        ++count;
    }
    EXPECT_EQ(0, count);

    // partial index search test
    searchkey.
        setNValue(0, ValueFactory::getBigIntValue(static_cast<int64_t>(440)));
    searchkey.
        setNValue(1, ValueFactory::getBigIntValue(static_cast<int64_t>(-10000000)));

    index->moveToKeyOrGreater(&searchkey);
    tuple = index->nextValue();
    EXPECT_TRUE(ValueFactory::getBigIntValue(40).
                op_equals(tuple.getNValue(0)).isTrue());
    EXPECT_TRUE(ValueFactory::getBigIntValue(40 % 2).
                op_equals(tuple.getNValue(1)).isTrue());
    EXPECT_TRUE(ValueFactory::getBigIntValue(40 % 3).
                op_equals(tuple.getNValue(2)).isTrue());
    EXPECT_TRUE(ValueFactory::getBigIntValue(40 + 20).
                op_equals(tuple.getNValue(3)).isTrue());
    EXPECT_TRUE(ValueFactory::getBigIntValue(40 * 11).
                op_equals(tuple.getNValue(4)).isTrue());
    tuple = index->nextValue();
    EXPECT_TRUE(ValueFactory::getBigIntValue(41).
                op_equals(tuple.getNValue(0)).isTrue());
    EXPECT_TRUE(ValueFactory::getBigIntValue(41 % 2).
                op_equals(tuple.getNValue(1)).isTrue());
    EXPECT_TRUE(ValueFactory::getBigIntValue(41 % 3).
                op_equals(tuple.getNValue(2)).isTrue());
    EXPECT_TRUE(ValueFactory::getBigIntValue(41 + 20).
                op_equals(tuple.getNValue(3)).isTrue());
    EXPECT_TRUE(ValueFactory::getBigIntValue(41 * 11).
                op_equals(tuple.getNValue(4)).isTrue());

    searchkey.
        setNValue(0, ValueFactory::getBigIntValue(static_cast<int64_t>(440)));
    searchkey.
        setNValue(1, ValueFactory::getBigIntValue(static_cast<int64_t>(10000000)));
    index->moveToKeyOrGreater(&searchkey);
    tuple = index->nextValue();
    EXPECT_TRUE(ValueFactory::getBigIntValue(41).
                op_equals(tuple.getNValue(0)).isTrue());
    EXPECT_TRUE(ValueFactory::getBigIntValue(41 % 2).
                op_equals(tuple.getNValue(1)).isTrue());
    EXPECT_TRUE(ValueFactory::getBigIntValue(41 % 3).
                op_equals(tuple.getNValue(2)).isTrue());
    EXPECT_TRUE(ValueFactory::getBigIntValue(41 + 20).
                op_equals(tuple.getNValue(3)).isTrue());
    EXPECT_TRUE(ValueFactory::getBigIntValue(41 * 11).
                op_equals(tuple.getNValue(4)).isTrue());
    tuple = index->nextValue();
    EXPECT_TRUE(ValueFactory::getBigIntValue(42).
                op_equals(tuple.getNValue(0)).isTrue());
    EXPECT_TRUE(ValueFactory::getBigIntValue(42 % 2).
                op_equals(tuple.getNValue(1)).isTrue());
    EXPECT_TRUE(ValueFactory::getBigIntValue(42 % 3).
                op_equals(tuple.getNValue(2)).isTrue());
    EXPECT_TRUE(ValueFactory::getBigIntValue(42 + 20).
                op_equals(tuple.getNValue(3)).isTrue());
    EXPECT_TRUE(ValueFactory::getBigIntValue(42 * 11).
                op_equals(tuple.getNValue(4)).isTrue());

    // moveToGreaterThanKey test
    searchkey.
        setNValue(0, ValueFactory::getBigIntValue(static_cast<int64_t>(330)));
    searchkey.
        setNValue(1, ValueFactory::getBigIntValue(static_cast<int64_t>(30%3)));

    index->moveToGreaterThanKey(&searchkey);
    tuple = index->nextValue();
    EXPECT_TRUE(ValueFactory::getBigIntValue(31).
                op_equals(tuple.getNValue(0)).isTrue());
    EXPECT_TRUE(ValueFactory::getBigIntValue(31 % 2).
                op_equals(tuple.getNValue(1)).isTrue());
    EXPECT_TRUE(ValueFactory::getBigIntValue(31 % 3).
                op_equals(tuple.getNValue(2)).isTrue());
    EXPECT_TRUE(ValueFactory::getBigIntValue(31 + 20).
                op_equals(tuple.getNValue(3)).isTrue());
    EXPECT_TRUE(ValueFactory::getBigIntValue(31 * 11).
                op_equals(tuple.getNValue(4)).isTrue());

    TableTuple& tmptuple = table->tempTuple();
    tmptuple.
        setNValue(0, ValueFactory::getBigIntValue(static_cast<int64_t>(1234)));
    tmptuple.
        setNValue(1, ValueFactory::getBigIntValue(static_cast<int64_t>(0)));
    tmptuple.
        setNValue(2, ValueFactory::getBigIntValue(static_cast<int64_t>(3333)));
    tmptuple.
        setNValue(3, ValueFactory::getBigIntValue(static_cast<int64_t>(-200)));
    tmptuple.
        setNValue(4, ValueFactory::getBigIntValue(static_cast<int64_t>(550)));
    EXPECT_EQ(true, table->insertTuple(tmptuple));
    tmptuple.
        setNValue(0, ValueFactory::getBigIntValue(static_cast<int64_t>(12345)));
    tmptuple.
        setNValue(1, ValueFactory::getBigIntValue(static_cast<int64_t>(0)));
    tmptuple.
        setNValue(2, ValueFactory::getBigIntValue(static_cast<int64_t>(50 %3)));
    tmptuple.
        setNValue(3, ValueFactory::getBigIntValue(static_cast<int64_t>(-200)));
    tmptuple.
        setNValue(4, ValueFactory::getBigIntValue(static_cast<int64_t>(550)));
    EXPECT_EQ(true, table->insertTuple(tmptuple));
    TupleSchema::freeTupleSchema(keySchema);
    delete[] searchkey.address();
}

int main()
{
    return TestSuite::globalInstance()->runAll();
}
