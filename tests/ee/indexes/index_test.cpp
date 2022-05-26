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

#include <boost/foreach.hpp>
#include <boost/scoped_ptr.hpp>

#include "harness.h"
#include "common/common.h"
#include "common/NValue.hpp"
#include "common/ValueFactory.hpp"
#include "common/debuglog.h"
#include "common/SerializableEEException.h"
#include "common/SynchronizedThreadLock.h"
#include "common/tabletuple.h"
#include "storage/table.h"
#include "storage/temptable.h"
#include "storage/persistenttable.h"
#include "storage/tablefactory.h"
#include "storage/tableiterator.h"
#include "storage/tableutil.h"
#include "storage/DRTupleStream.h"
#include "indexes/tableindex.h"
#include "indexes/tableindexfactory.h"
#include "execution/VoltDBEngine.h"
#include "common/ThreadLocalPool.h"

using namespace std;
using namespace voltdb;

#define NUM_OF_COLUMNS 5
#define NUM_OF_TUPLES 1000
#define NUM_OF_WIDE_TUPLES 5
#define PKEY_ID 100
#define INT_UNIQUE_ID 101
#define INT_MULTI_ID 102
#define INTS_UNIQUE_ID 103
#define INTS_MULTI_ID 104

class IndexTest : public Test {
public:
    IndexTest() : table(NULL)
    {}
    ~IndexTest()
    {
        delete table;
        delete[] m_exceptionBuffer;
        delete m_engine;
        voltdb::globalDestroyOncePerProcess();
    }

    void initWideTable(string name)
    {
        int num_of_columns = 100;
        CatalogId database_id = 1000;
        vector<boost::shared_ptr<const TableColumn> > columns;
        vector<string> columnNames(num_of_columns);

        vector<ValueType> columnTypes(num_of_columns, ValueType::tBIGINT);
        vector<int32_t> columnLengths(num_of_columns, NValue::getTupleStorageSize(ValueType::tBIGINT));
        vector<bool> columnAllowNull(num_of_columns, false);

        char buffer[32];
        for (int ctr = 0; ctr < num_of_columns; ctr++)
        {
            snprintf(buffer, 32, "column%02d", ctr);
            columnNames[ctr] = buffer;
        }

        TupleSchema* schema =
            TupleSchema::createTupleSchemaForTest(columnTypes,
                                           columnLengths,
                                           columnAllowNull);

        // make up 40 column index (320 byte key). this is intentionally arranged to
        // not be all consecutive columns and not strictly ordered from left to right
        vector<int> pkey_column_indices;
        pkey_column_indices.push_back(0);
        pkey_column_indices.push_back(1);
        pkey_column_indices.push_back(2);
        pkey_column_indices.push_back(3);
        pkey_column_indices.push_back(4);
        pkey_column_indices.push_back(5);
        pkey_column_indices.push_back(6);
        pkey_column_indices.push_back(7);
        pkey_column_indices.push_back(8);
        pkey_column_indices.push_back(9); // 10

        pkey_column_indices.push_back(10);
        pkey_column_indices.push_back(11);
        pkey_column_indices.push_back(12);
        pkey_column_indices.push_back(13);
        pkey_column_indices.push_back(14);
        pkey_column_indices.push_back(15);
        pkey_column_indices.push_back(16);
        pkey_column_indices.push_back(17);
        pkey_column_indices.push_back(18);
        pkey_column_indices.push_back(19);

        pkey_column_indices.push_back(20);
        pkey_column_indices.push_back(21);
        pkey_column_indices.push_back(22);
        pkey_column_indices.push_back(23);
        pkey_column_indices.push_back(24);
        pkey_column_indices.push_back(25);
        pkey_column_indices.push_back(26);
        pkey_column_indices.push_back(27);
        pkey_column_indices.push_back(28);
        pkey_column_indices.push_back(29);

        pkey_column_indices.push_back(30);
        pkey_column_indices.push_back(31);
        pkey_column_indices.push_back(32);
        pkey_column_indices.push_back(33);
        pkey_column_indices.push_back(34);
        pkey_column_indices.push_back(35);
        pkey_column_indices.push_back(36);
        pkey_column_indices.push_back(37);
        pkey_column_indices.push_back(38);
        pkey_column_indices.push_back(39);


        TableIndexScheme pkeyScheme(name,
                                    BALANCED_TREE_INDEX,
                                    pkey_column_indices, TableIndex::simplyIndexColumns(),
                                    true, true, false, schema);
        vector<TableIndexScheme> indexes;
        indexes.push_back(pkeyScheme);

        m_engine = new VoltDBEngine();
        m_exceptionBuffer = new char[4096];
        m_engine->setBuffers(NULL, 0, NULL, 0, NULL, 0, NULL, 0, NULL, 0, m_exceptionBuffer, 4096);
        int partitionCount = 1;
        m_engine->initialize(0, 0, 0, partitionCount, 0, "", 0, 1024, false, -1, false, DEFAULT_TEMP_TABLE_MEMORY, true);
        partitionCount = htonl(partitionCount);
        m_engine->updateHashinator((char*)&partitionCount, NULL, 0);
        table = dynamic_cast<PersistentTable*>(
            TableFactory::getPersistentTable(database_id, "test_wide_table",
                                             schema, columnNames, signature));

        TableIndex *pkeyIndex = TableIndexFactory::TableIndexFactory::getInstance(pkeyScheme);
        assert(pkeyIndex);
        table->addIndex(pkeyIndex);
        table->setPrimaryKeyIndex(pkeyIndex);

        for (int64_t row = 1; row <= NUM_OF_WIDE_TUPLES; ++row)
        {
            setWideTableToRow(table->tempTuple(), row);
            bool result = table->insertTuple(table->tempTuple());
            // Insert on init wide table failed - stupidly coded to avoid unused var arg
            // not sure why this isn't caught by the trick at the top of harness.h
            if (!result) assert(result || !"Insert on init wide table failed");
        }
    }

    void makeColsForRow_00(int offset, int64_t row, TableTuple &tuple) {
        for (int i = 0; i < 10; i++, offset++)
            tuple.setNValue(offset, ValueFactory::getBigIntValue(static_cast<int64_t>((row << 32) + i)));
    }
    void makeColsForRow_10(int offset, int64_t row, TableTuple &tuple) {
        for (int i = 10; i < 20; i++, offset++)
            tuple.setNValue(offset, ValueFactory::getBigIntValue(static_cast<int64_t>(i + (row % 2))));
    }
    void makeColsForRow_20(int offset, int64_t row, TableTuple &tuple) {
        for (int i = 20; i < 30; i++, offset++)
            tuple.setNValue(offset, ValueFactory::getBigIntValue(static_cast<int64_t>(i + (row % 3))));
    }
    void makeColsForRow_30(int offset, int64_t row, TableTuple &tuple) {
        for (int i = 30; i < 40; i++, offset++)
            tuple.setNValue(offset, ValueFactory::getBigIntValue(static_cast<int64_t>(i + (row % 4))));
    }
    void makeColsForRow_40(int offset, int64_t row, TableTuple &tuple) {
        for (int i = 40; i < 50; i++, offset++)
            tuple.setNValue(offset, ValueFactory::getBigIntValue(static_cast<int64_t>(i + (row % 5))));
    }
    void makeColsForRow_50(int offset, int64_t row, TableTuple &tuple) {
        for (int i = 50; i < 60; i++, offset++)
            tuple.setNValue(offset, ValueFactory::getBigIntValue(static_cast<int64_t>(i + (row % 6))));
    }
    void makeColsForRow_60(int offset, int64_t row, TableTuple &tuple) {
        for (int i = 60; i < 70; i++, offset++)
            tuple.setNValue(offset, ValueFactory::getBigIntValue(static_cast<int64_t>(i + (row % 7))));
    }
    void makeColsForRow_70(int offset, int64_t row, TableTuple &tuple) {
        for (int i = 70; i < 80; i++, offset++)
            tuple.setNValue(offset, ValueFactory::getBigIntValue(static_cast<int64_t>(i + (row % 8))));
    }
    void makeColsForRow_80(int offset, int64_t row, TableTuple &tuple) {
        for (int i = 80; i < 90; i++, offset++)
            tuple.setNValue(offset, ValueFactory::getBigIntValue(static_cast<int64_t>(i + (row % 9))));
    }
    void makeColsForRow_90(int offset, int64_t row, TableTuple &tuple) {
        for (int i = 90; i < 100; i++, offset++)
            tuple.setNValue(offset, ValueFactory::getBigIntValue(static_cast<int64_t>(i + (row % 10))));
    }

    /*
     * populate a tuple with the wide table schema.
     */
    void setWideTableToRow(TableTuple &tuple, int64_t row) {
        makeColsForRow_00(0, row, tuple);
        makeColsForRow_10(10, row, tuple);
        makeColsForRow_20(20, row, tuple);
        makeColsForRow_30(30, row, tuple);
        makeColsForRow_40(40, row, tuple);
        makeColsForRow_50(50, row, tuple);
        makeColsForRow_60(60, row, tuple);
        makeColsForRow_70(70, row, tuple);
        makeColsForRow_80(80, row, tuple);
        makeColsForRow_90(90, row, tuple);
    }

    /*
     * populate a tuple with the wide index schema using
     * the expression used to generate a table value for the
     * corresponding table columns in init_wide_table
     */
    void setWideIndexToRow(TableTuple &tuple, int64_t row) {
        makeColsForRow_00(0,  row, tuple);
        makeColsForRow_10(10, row, tuple);
        makeColsForRow_20(20, row, tuple);
        makeColsForRow_30(30, row, tuple);
    }

    void init(std::string name, TableIndexType type, std::vector<int32_t> &ix_columnIndices,
              std::vector<ValueType> &ix_columnTypes, bool unique)
    {
        bool countable = true;
        TupleSchema *initiallyNullTupleSchema = NULL;
        TableIndexScheme index(name, type,
                               ix_columnIndices, TableIndex::simplyIndexColumns(),
                               unique, countable, false, initiallyNullTupleSchema);

        CatalogId database_id = 1000;
        vector<boost::shared_ptr<const TableColumn> > columns;

        vector<string> columnNames(NUM_OF_COLUMNS);

        char buffer[32];
        vector<ValueType> columnTypes(NUM_OF_COLUMNS, ValueType::tBIGINT);
        vector<int32_t>
            columnLengths(NUM_OF_COLUMNS,
                          NValue::getTupleStorageSize(ValueType::tBIGINT));
        vector<bool> columnAllowNull(NUM_OF_COLUMNS, false);
        for (int ctr = 0; ctr < NUM_OF_COLUMNS; ctr++)
        {
            snprintf(buffer, 32, "column%02d", ctr);
            columnNames[ctr] = buffer;
        }
        TupleSchema* schema =
            TupleSchema::createTupleSchemaForTest(columnTypes,
                                           columnLengths,
                                           columnAllowNull);

        index.tupleSchema = schema;
        vector<int> pkey_column_indices;
        vector<ValueType> pkey_column_types;
        pkey_column_indices.push_back(0);
        pkey_column_indices.push_back(1);

        TableIndexScheme pkeyScheme("idx_pkey", BALANCED_TREE_INDEX,
                                    pkey_column_indices, TableIndex::simplyIndexColumns(),
                                    true, true, false, schema);

        vector<TableIndexScheme> indexes;
        indexes.push_back(index);
        m_engine = new VoltDBEngine();
        m_exceptionBuffer = new char[4096];
        m_engine->setBuffers(NULL, 0, NULL, 0, NULL, 0, NULL, 0, NULL, 0, m_exceptionBuffer, 4096);
        int partitionCount = 1;
        m_engine->initialize(0, 0, 0, partitionCount, 0, "", 0, 1024, false, -1, false, DEFAULT_TEMP_TABLE_MEMORY, true);
        partitionCount = htonl(partitionCount);
        m_engine->updateHashinator((char*)&partitionCount, NULL, 0);
        table = dynamic_cast<PersistentTable*>(TableFactory::getPersistentTable(database_id,
                    "test_table", schema, columnNames, signature));

        TableIndex *pkeyIndex = TableIndexFactory::TableIndexFactory::getInstance(pkeyScheme);
        assert(pkeyIndex);
        table->addIndex(pkeyIndex);
        table->setPrimaryKeyIndex(pkeyIndex);

        // add other indexes
        BOOST_FOREACH(TableIndexScheme &scheme, indexes) {
            TableIndex *index = TableIndexFactory::getInstance(scheme);
            assert(index);
            table->addIndex(index);
        }

        for (int64_t i = 1; i <= NUM_OF_TUPLES; ++i)
        {
            TableTuple &tuple = table->tempTuple();
            tuple.setNValue(0, ValueFactory::getBigIntValue(i));
            tuple.setNValue(1, ValueFactory::getBigIntValue(i % 2));
            tuple.setNValue(2, ValueFactory::getBigIntValue(i % 3));
            tuple.setNValue(3, ValueFactory::getBigIntValue(i + 20));
            tuple.setNValue(4, ValueFactory::getBigIntValue(i * 11));
            __attribute__((unused)) bool r = table->insertTuple(tuple);
            vassert(r);
        }
    }


    void verifyWideRow(TableTuple &tuple, int64_t row) {
        for (int i = 0; i < 10; i++)
            EXPECT_TRUE(ValueFactory::getBigIntValue(static_cast<int64_t>((row << 32) + i))
                        .op_equals(tuple.getNValue(i)).isTrue());

        for (int i = 10; i < 20; i++)
            EXPECT_TRUE(ValueFactory::getBigIntValue(static_cast<int64_t>(i + (row % 2)))
                        .op_equals(tuple.getNValue(i)).isTrue());

        for (int i = 20; i < 30; i++)
            EXPECT_TRUE(ValueFactory::getBigIntValue(static_cast<int64_t>(i + (row % 3)))
                        .op_equals(tuple.getNValue(i)).isTrue());

        for (int i = 30; i < 40; i++)
            EXPECT_TRUE(ValueFactory::getBigIntValue(static_cast<int64_t>(i + (row % 4)))
                        .op_equals(tuple.getNValue(i)).isTrue());

        for (int i = 40; i < 50; i++)
            EXPECT_TRUE(ValueFactory::getBigIntValue(static_cast<int64_t>(i + (row % 5)))
                        .op_equals(tuple.getNValue(i)).isTrue());

        for (int i = 50; i < 60; i++)
            EXPECT_TRUE(ValueFactory::getBigIntValue(static_cast<int64_t>(i + (row % 6)))
                        .op_equals(tuple.getNValue(i)).isTrue());

        for (int i = 60; i < 70; i++)
            EXPECT_TRUE(ValueFactory::getBigIntValue(static_cast<int64_t>(i + (row % 7)))
                        .op_equals(tuple.getNValue(i)).isTrue());

        for (int i = 70; i < 80; i++)
            EXPECT_TRUE(ValueFactory::getBigIntValue(static_cast<int64_t>(i + (row % 8)))
                        .op_equals(tuple.getNValue(i)).isTrue());

        for (int i = 80; i < 90; i++)
            EXPECT_TRUE(ValueFactory::getBigIntValue(static_cast<int64_t>(i + (row % 9)))
                        .op_equals(tuple.getNValue(i)).isTrue());

        for (int i = 90; i < 100; i++)
            EXPECT_TRUE(ValueFactory::getBigIntValue(static_cast<int64_t>(i + (row % 10)))
                        .op_equals(tuple.getNValue(i)).isTrue());
    }

protected:
    PersistentTable* table;
    char* m_exceptionBuffer;
    VoltDBEngine* m_engine;
    char signature[20];
};

TEST_F(IndexTest, IntUnique) {
    vector<int> iu_column_indices;
    vector<ValueType> iu_column_types;
    iu_column_indices.push_back(3);
    iu_column_types.push_back(ValueType::tBIGINT);

    init("iu",
         BALANCED_TREE_INDEX,
         iu_column_indices,
         iu_column_types,
         true);
    TableIndex* index = table->index("iu");
    EXPECT_TRUE(index != NULL);

    //EXPECT_EQ(38528, index->getMemoryEstimate());

    // TODO
}

TEST_F(IndexTest, IntMulti) {
    vector<int> im_column_indices;
    vector<ValueType> im_column_types;
    im_column_indices.push_back(3);
    im_column_types.push_back(ValueType::tBIGINT);
    init("im",
         BALANCED_TREE_INDEX,
         im_column_indices,
         im_column_types,
         false);
    TableIndex* index = table->index("im");
    EXPECT_TRUE(index != NULL);

    //EXPECT_EQ(44000, index->getMemoryEstimate());
    // TODO
}

TEST_F(IndexTest, IntsUnique) {
    vector<int> ixu_column_indices;
    vector<ValueType> ixu_column_types;
    ixu_column_indices.push_back(4);
    ixu_column_indices.push_back(2);
    ixu_column_types.push_back(ValueType::tBIGINT);
    ixu_column_types.push_back(ValueType::tBIGINT);
    init("ixu",
         BALANCED_TREE_INDEX,
         ixu_column_indices,
         ixu_column_types,
         true);

    TableIndex* index = table->index("ixu");
    EXPECT_TRUE(index != NULL);

    IndexCursor indexCursor(index->getTupleSchema());

    //EXPECT_EQ(62520, index->getMemoryEstimate());

    TableTuple tuple(table->schema());
    vector<ValueType> keyColumnTypes(2, ValueType::tBIGINT);
    vector<int32_t>
        keyColumnLengths(2, NValue::getTupleStorageSize(ValueType::tBIGINT));
    vector<bool> keyColumnAllowNull(2, true);
    TupleSchema* keySchema =
        TupleSchema::createTupleSchemaForTest(keyColumnTypes,
                                       keyColumnLengths,
                                       keyColumnAllowNull);
    TableTuple searchkey(keySchema);
    searchkey.move(new char[searchkey.tupleLength()]);
    searchkey.setNValue(0, ValueFactory::getBigIntValue(static_cast<int64_t>(550)));
    searchkey.setNValue(1, ValueFactory::getBigIntValue(static_cast<int64_t>(2)));
    EXPECT_TRUE(index->moveToKey(&searchkey, indexCursor));

    tuple = index->nextValueAtKey(indexCursor);
    EXPECT_FALSE(tuple.isNullTuple());
    EXPECT_TRUE(ValueFactory::getBigIntValue(50).op_equals(tuple.getNValue(0)).isTrue());
    EXPECT_TRUE(ValueFactory::getBigIntValue(50 % 2).op_equals(tuple.getNValue(1)).isTrue());
    EXPECT_TRUE(ValueFactory::getBigIntValue(50 % 3).op_equals(tuple.getNValue(2)).isTrue());
    EXPECT_TRUE(ValueFactory::getBigIntValue(50 + 20).op_equals(tuple.getNValue(3)).isTrue());
    EXPECT_TRUE(ValueFactory::getBigIntValue(50 * 11).op_equals(tuple.getNValue(4)).isTrue());

    tuple = index->nextValueAtKey(indexCursor);
    EXPECT_TRUE(tuple.isNullTuple());

    searchkey.setNValue(0, ValueFactory::getBigIntValue(static_cast<int64_t>(550)));
    searchkey.setNValue(1, ValueFactory::getBigIntValue(static_cast<int64_t>(1)));
    EXPECT_FALSE(index->moveToKey(&searchkey, indexCursor));
    tuple = index->nextValueAtKey(indexCursor);
    EXPECT_TRUE(tuple.isNullTuple());

    // partial index search test
    searchkey.
        setNValue(0, ValueFactory::getBigIntValue(static_cast<int64_t>(440)));
    searchkey.
        setNValue(1, ValueFactory::getBigIntValue(static_cast<int64_t>(-10000000)));
    index->moveToKeyOrGreater(&searchkey, indexCursor);
    EXPECT_FALSE((tuple = index->nextValue(indexCursor)).isNullTuple());

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
    EXPECT_FALSE((tuple = index->nextValue(indexCursor)).isNullTuple());
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
    index->moveToKeyOrGreater(&searchkey, indexCursor);
    EXPECT_FALSE((tuple = index->nextValue(indexCursor)).isNullTuple());
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
    EXPECT_FALSE((tuple = index->nextValue(indexCursor)).isNullTuple());
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
    index->moveToGreaterThanKey(&searchkey, indexCursor);
    EXPECT_FALSE((tuple = index->nextValue(indexCursor)).isNullTuple());
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
    EXPECT_TRUE(table->insertTuple(tmptuple));
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
    try {
        EXPECT_EQ(false, table->insertTuple(tmptuple));
    } catch (SerializableEEException &e)
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
    ixm_column_types.push_back(ValueType::tBIGINT);
    ixm_column_types.push_back(ValueType::tBIGINT);
    init("ixm2",
         BALANCED_TREE_INDEX,
         ixm_column_indices,
         ixm_column_types,
         false);

    TableIndex* index = table->index("ixm2");
    EXPECT_TRUE(index != NULL);
    IndexCursor indexCursor(index->getTupleSchema());

    //EXPECT_EQ(52000, index->getMemoryEstimate());

    TableTuple tuple(table->schema());
    vector<ValueType> keyColumnTypes(2, ValueType::tBIGINT);
    vector<int32_t>
        keyColumnLengths(2, NValue::getTupleStorageSize(ValueType::tBIGINT));
    vector<bool> keyColumnAllowNull(2, true);
    TupleSchema* keySchema =
        TupleSchema::createTupleSchemaForTest(keyColumnTypes,
                                       keyColumnLengths,
                                       keyColumnAllowNull);
    TableTuple searchkey(keySchema);
    searchkey.move(new char[searchkey.tupleLength()]);
    searchkey.setNValue(0, ValueFactory::getBigIntValue(static_cast<int64_t>(550)));
    searchkey.setNValue(1, ValueFactory::getBigIntValue(static_cast<int64_t>(2)));
    EXPECT_TRUE(index->moveToKey(&searchkey, indexCursor));

    tuple = index->nextValueAtKey(indexCursor);
    EXPECT_FALSE(tuple.isNullTuple());
    EXPECT_TRUE(ValueFactory::getBigIntValue(50).op_equals(tuple.getNValue(0)).isTrue());
    EXPECT_TRUE(ValueFactory::getBigIntValue(50 % 2).op_equals(tuple.getNValue(1)).isTrue());
    EXPECT_TRUE(ValueFactory::getBigIntValue(50 % 3).op_equals(tuple.getNValue(2)).isTrue());
    EXPECT_TRUE(ValueFactory::getBigIntValue(50 + 20).op_equals(tuple.getNValue(3)).isTrue());
    EXPECT_TRUE(ValueFactory::getBigIntValue(50 * 11).op_equals(tuple.getNValue(4)).isTrue());

    tuple = index->nextValueAtKey(indexCursor);
    EXPECT_TRUE(tuple.isNullTuple());

    searchkey.setNValue(0, ValueFactory::getBigIntValue(static_cast<int64_t>(550)));
    searchkey.setNValue(1, ValueFactory::getBigIntValue(static_cast<int64_t>(1)));
    EXPECT_FALSE(index->moveToKey(&searchkey, indexCursor));
    tuple = index->nextValueAtKey(indexCursor);
    EXPECT_TRUE(tuple.isNullTuple());

    // partial index search test
    searchkey.
        setNValue(0, ValueFactory::getBigIntValue(static_cast<int64_t>(440)));
    searchkey.
        setNValue(1, ValueFactory::getBigIntValue(static_cast<int64_t>(-10000000)));

    index->moveToKeyOrGreater(&searchkey, indexCursor);
    EXPECT_FALSE((tuple = index->nextValue(indexCursor)).isNullTuple());
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
    EXPECT_FALSE((tuple = index->nextValue(indexCursor)).isNullTuple());
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
    index->moveToKeyOrGreater(&searchkey, indexCursor);
    EXPECT_FALSE((tuple = index->nextValue(indexCursor)).isNullTuple());
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
    EXPECT_FALSE((tuple = index->nextValue(indexCursor)).isNullTuple());
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

    index->moveToGreaterThanKey(&searchkey, indexCursor);
    EXPECT_FALSE((tuple = index->nextValue(indexCursor)).isNullTuple());
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
    EXPECT_TRUE(table->insertTuple(tmptuple));
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
    EXPECT_TRUE(table->insertTuple(tmptuple));
    TupleSchema::freeTupleSchema(keySchema);
    delete[] searchkey.address();
}

TEST_F(IndexTest, TupleKeyUnique) {

    // make a tuple with the index key schema
    int indexWidth = 40;
    vector<bool> keyColumnAllowNull(indexWidth, true);
    vector<ValueType> keyColumnTypes(indexWidth, ValueType::tBIGINT);
    vector<int32_t> keyColumnLengths(indexWidth, NValue::getTupleStorageSize(ValueType::tBIGINT));
    TupleSchema *keySchema = TupleSchema::createTupleSchemaForTest(keyColumnTypes, keyColumnLengths, keyColumnAllowNull);
    TableTuple searchkey(keySchema);
    // provide storage for search key tuple
    searchkey.move(new char[searchkey.tupleLength()]);

    // TEST that factory returns an index.
    initWideTable("ixu_wide");
    TableIndex* index = table->index("ixu_wide");
    EXPECT_TRUE(index != NULL);

    IndexCursor indexCursor(index->getTupleSchema());

    //EXPECT_EQ(280, index->getMemoryEstimate());

    // make a tuple with the table's schema
    TableTuple tuple(table->schema());
    char tuplestorage[tuple.tupleLength()];

    // TEST checkForIndexChange replaceEntry

    // TEST exists
    tuple.move(tuplestorage);
    setWideTableToRow(tuple, 2);
    EXPECT_TRUE(index->exists(&tuple));

    tuple.move(tuplestorage);
    setWideTableToRow(tuple, 100); // this tuple does not exist.
    EXPECT_FALSE(index->exists(&tuple));

    // TEST moveToKey and nextValueAtKey
    int64_t row = 2;
    setWideIndexToRow(searchkey, row);
    EXPECT_TRUE(index->moveToKey(&searchkey, indexCursor));
    tuple = index->nextValueAtKey(indexCursor);
    EXPECT_FALSE(tuple.isNullTuple());
    verifyWideRow(tuple, row);
    tuple = index->nextValueAtKey(indexCursor);
    EXPECT_TRUE(tuple.isNullTuple());

    // TEST remove tuple
    // remove the tuple found above from the table (which updates the index)
    setWideIndexToRow(searchkey, 2);  // DELETE row 2.
    EXPECT_TRUE(index->moveToKey(&searchkey, indexCursor));
    tuple = index->nextValueAtKey(indexCursor);
    table->deleteTuple(tuple, true);

    // and now that tuple is gone
    EXPECT_FALSE(index->moveToKey(&searchkey, indexCursor));
    tuple = index->nextValueAtKey(indexCursor);
    EXPECT_TRUE(tuple.isNullTuple());

    tuple.move(tuplestorage);
    setWideTableToRow(tuple, 2);
    EXPECT_FALSE(index->exists(&tuple));

    // TEST moveToKeyOrGreater nextValue

    // TEST moveToGreaterThanKey

    // TEST moveToEnd

    TupleSchema::freeTupleSchema(keySchema);
    delete[] searchkey.address();
}

/**
 * The next test case aims to test the re-entrant unique tree index feature.
 * The search key values and data preparation work are all borrowed from the previous
 * IntsUnique test.
 */
TEST_F(IndexTest, ReentrantTreeUnique) {
    vector<int> ixu_column_indices;
    vector<ValueType> ixu_column_types;
    ixu_column_indices.push_back(4);
    ixu_column_indices.push_back(2);
    ixu_column_types.push_back(ValueType::tBIGINT);
    ixu_column_types.push_back(ValueType::tBIGINT);
    init("ixu",
            BALANCED_TREE_INDEX,
            ixu_column_indices,
            ixu_column_types,
            true);

    TableIndex* index = table->index("ixu");
    EXPECT_TRUE(index != NULL);

    IndexCursor indexCursor(index->getTupleSchema());

    TableTuple tuple(table->schema());
    vector<ValueType> keyColumnTypes(2, ValueType::tBIGINT);
    vector<int32_t>
    keyColumnLengths(2, NValue::getTupleStorageSize(ValueType::tBIGINT));
    vector<bool> keyColumnAllowNull(2, true);
    TupleSchema* keySchema =
            TupleSchema::createTupleSchemaForTest(keyColumnTypes,
                    keyColumnLengths,
                    keyColumnAllowNull);
    TableTuple searchkey(keySchema);
    searchkey.move(new char[searchkey.tupleLength()]);

    // partial index search test
    searchkey.setNValue(0, ValueFactory::getBigIntValue(static_cast<int64_t>(440)));
    searchkey.setNValue(1, ValueFactory::getBigIntValue(static_cast<int64_t>(-10000000)));
    index->moveToKeyOrGreater(&searchkey, indexCursor);
    EXPECT_FALSE((tuple = index->nextValue(indexCursor)).isNullTuple());

    EXPECT_TRUE(ValueFactory::getBigIntValue(40).
            op_equals(tuple.getNValue(0)).isTrue());
    //
    // Before index moves to the next value, re-use the same index to iterate
    //
    IndexCursor indexCursorNew(index->getTupleSchema());

    searchkey.setNValue(0, ValueFactory::getBigIntValue(static_cast<int64_t>(440)));
    searchkey.setNValue(1, ValueFactory::getBigIntValue(static_cast<int64_t>(10000000)));
    index->moveToKeyOrGreater(&searchkey, indexCursorNew);
    EXPECT_FALSE((tuple = index->nextValue(indexCursorNew)).isNullTuple());
    EXPECT_TRUE(ValueFactory::getBigIntValue(41).
            op_equals(tuple.getNValue(0)).isTrue());
    EXPECT_FALSE((tuple = index->nextValue(indexCursorNew)).isNullTuple());
    EXPECT_TRUE(ValueFactory::getBigIntValue(42).
            op_equals(tuple.getNValue(0)).isTrue());

    // moveToGreaterThanKey test on the re-entrant-index
    searchkey.setNValue(0, ValueFactory::getBigIntValue(static_cast<int64_t>(330)));
    searchkey.setNValue(1, ValueFactory::getBigIntValue(static_cast<int64_t>(30%3)));
    index->moveToGreaterThanKey(&searchkey, indexCursorNew);
    EXPECT_FALSE((tuple = index->nextValue(indexCursorNew)).isNullTuple());
    EXPECT_TRUE(ValueFactory::getBigIntValue(31).
            op_equals(tuple.getNValue(0)).isTrue());

    //
    // Go back to the original index places and continue to iterate the next value
    //
    EXPECT_FALSE((tuple = index->nextValue(indexCursor)).isNullTuple());
    EXPECT_TRUE(ValueFactory::getBigIntValue(41).
            op_equals(tuple.getNValue(0)).isTrue());

    // go to indexCursorNew again
    searchkey.setNValue(0, ValueFactory::getBigIntValue(static_cast<int64_t>(550)));
    searchkey.setNValue(1, ValueFactory::getBigIntValue(static_cast<int64_t>(2)));
    EXPECT_TRUE(index->moveToKey(&searchkey, indexCursorNew));

    tuple = index->nextValueAtKey(indexCursorNew);
    EXPECT_FALSE(tuple.isNullTuple());
    EXPECT_TRUE(ValueFactory::getBigIntValue(50).op_equals(tuple.getNValue(0)).isTrue());

    tuple = index->nextValueAtKey(indexCursorNew);
    EXPECT_TRUE(tuple.isNullTuple());

    // clean up
    TupleSchema::freeTupleSchema(keySchema);
    delete[] searchkey.address();
}

/**
 * The next test case aims to test the re-entrant multiple tree index feature.
 * The search key values and data preparation work are all borrowed from the previous
 * IntsUnique test.
 */
TEST_F(IndexTest, ReentrantTreeMultiple) {
    vector<int> ixm_column_indices;
    vector<ValueType> ixm_column_types;
    ixm_column_indices.push_back(4);
    ixm_column_indices.push_back(2);
    ixm_column_types.push_back(ValueType::tBIGINT);
    ixm_column_types.push_back(ValueType::tBIGINT);
    init("ixm2",
         BALANCED_TREE_INDEX,
         ixm_column_indices,
         ixm_column_types,
         false);

    TableIndex* index = table->index("ixm2");
    EXPECT_TRUE(index != NULL);
    IndexCursor indexCursor(index->getTupleSchema());

    TableTuple tuple(table->schema());
    vector<ValueType> keyColumnTypes(2, ValueType::tBIGINT);
    vector<int32_t>keyColumnLengths(2, NValue::getTupleStorageSize(ValueType::tBIGINT));
    vector<bool> keyColumnAllowNull(2, true);
    TupleSchema* keySchema =
        TupleSchema::createTupleSchemaForTest(keyColumnTypes,
                                       keyColumnLengths,
                                       keyColumnAllowNull);
    TableTuple searchkey(keySchema);
    searchkey.move(new char[searchkey.tupleLength()]);

    searchkey.setNValue(0, ValueFactory::getBigIntValue(static_cast<int64_t>(550)));
    searchkey.setNValue(1, ValueFactory::getBigIntValue(static_cast<int64_t>(1)));
    EXPECT_FALSE(index->moveToKey(&searchkey, indexCursor));
    tuple = index->nextValueAtKey(indexCursor);
    EXPECT_TRUE(tuple.isNullTuple());

    searchkey.setNValue(0, ValueFactory::getBigIntValue(static_cast<int64_t>(440)));
    searchkey.setNValue(1, ValueFactory::getBigIntValue(static_cast<int64_t>(-10000000)));

    index->moveToKeyOrGreater(&searchkey, indexCursor);
    EXPECT_FALSE((tuple = index->nextValue(indexCursor)).isNullTuple());
    EXPECT_TRUE(ValueFactory::getBigIntValue(40).
                op_equals(tuple.getNValue(0)).isTrue());

    //
    // Before index moves to the next value, re-use the same index to iterate
    //
    IndexCursor indexCursorNew(index->getTupleSchema());

    searchkey.setNValue(0, ValueFactory::getBigIntValue(static_cast<int64_t>(440)));
    searchkey.setNValue(1, ValueFactory::getBigIntValue(static_cast<int64_t>(10000000)));
    index->moveToKeyOrGreater(&searchkey, indexCursorNew);
    EXPECT_FALSE((tuple = index->nextValue(indexCursorNew)).isNullTuple());
    EXPECT_TRUE(ValueFactory::getBigIntValue(41).
            op_equals(tuple.getNValue(0)).isTrue());

    // moveToGreaterThanKey test
    searchkey.setNValue(0, ValueFactory::getBigIntValue(static_cast<int64_t>(330)));
    searchkey.setNValue(1, ValueFactory::getBigIntValue(static_cast<int64_t>(30%3)));

    index->moveToGreaterThanKey(&searchkey, indexCursorNew);
    EXPECT_FALSE((tuple = index->nextValue(indexCursorNew)).isNullTuple());
    EXPECT_TRUE(ValueFactory::getBigIntValue(31).
                op_equals(tuple.getNValue(0)).isTrue());

    //
    // Go back to the original index places and continue to iterate the next value
    //
    EXPECT_FALSE((tuple = index->nextValue(indexCursor)).isNullTuple());
    EXPECT_TRUE(ValueFactory::getBigIntValue(41).
                op_equals(tuple.getNValue(0)).isTrue());

    // go to indexCursorNew again
    searchkey.setNValue(0, ValueFactory::getBigIntValue(static_cast<int64_t>(550)));
    searchkey.setNValue(1, ValueFactory::getBigIntValue(static_cast<int64_t>(2)));
    EXPECT_TRUE(index->moveToKey(&searchkey, indexCursorNew));

    tuple = index->nextValueAtKey(indexCursorNew);
    EXPECT_FALSE(tuple.isNullTuple());
    EXPECT_TRUE(ValueFactory::getBigIntValue(50).op_equals(tuple.getNValue(0)).isTrue());

    tuple = index->nextValueAtKey(indexCursorNew);
    EXPECT_TRUE(tuple.isNullTuple());

    TupleSchema::freeTupleSchema(keySchema);
    delete[] searchkey.address();
}

TEST_F(IndexTest, ReentrantHashUnique) {
    vector<int> ixm_column_indices;
    vector<ValueType> ixm_column_types;
    ixm_column_indices.push_back(4);
    ixm_column_indices.push_back(2);
    ixm_column_types.push_back(ValueType::tBIGINT);
    ixm_column_types.push_back(ValueType::tBIGINT);
    init("ixh1",
         HASH_TABLE_INDEX,
         ixm_column_indices,
         ixm_column_types,
         true);

    TableIndex* index = table->index("ixh1");
    EXPECT_TRUE(index != NULL);
    IndexCursor indexCursor(index->getTupleSchema());

    TableTuple tuple(table->schema());
    vector<ValueType> keyColumnTypes(2, ValueType::tBIGINT);
    vector<int32_t>keyColumnLengths(2, NValue::getTupleStorageSize(ValueType::tBIGINT));
    vector<bool> keyColumnAllowNull(2, true);
    TupleSchema* keySchema =
        TupleSchema::createTupleSchemaForTest(keyColumnTypes,
                                       keyColumnLengths,
                                       keyColumnAllowNull);
    TableTuple searchkey(keySchema);
    searchkey.move(new char[searchkey.tupleLength()]);

    searchkey.setNValue(0, ValueFactory::getBigIntValue(static_cast<int64_t>(550)));
    searchkey.setNValue(1, ValueFactory::getBigIntValue(static_cast<int64_t>(2)));
    EXPECT_TRUE(index->moveToKey(&searchkey, indexCursor));
    tuple = index->nextValueAtKey(indexCursor);
    EXPECT_FALSE(tuple.isNullTuple());
    EXPECT_TRUE(ValueFactory::getBigIntValue(50).op_equals(tuple.getNValue(0)).isTrue());

    //
    // Before index moves to the next value, re-use the same index to iterate
    //
    IndexCursor indexCursorNew(index->getTupleSchema());
    searchkey.setNValue(0, ValueFactory::getBigIntValue(static_cast<int64_t>(440)));
    searchkey.setNValue(1, ValueFactory::getBigIntValue(static_cast<int64_t>(40 % 3)));

    index->moveToKey(&searchkey, indexCursorNew);
    EXPECT_FALSE((tuple = index->nextValueAtKey(indexCursorNew)).isNullTuple());
    EXPECT_TRUE(ValueFactory::getBigIntValue(40).
            op_equals(tuple.getNValue(0)).isTrue());
    //
    // Go back to the original index places and continue to iterate the next value
    //
    tuple = index->nextValueAtKey(indexCursor);
    EXPECT_TRUE(tuple.isNullTuple());

    TupleSchema::freeTupleSchema(keySchema);
    delete[] searchkey.address();
}

TEST_F(IndexTest, ReentrantHashMultiple) {
    vector<int> ixm_column_indices;
    vector<ValueType> ixm_column_types;
    ixm_column_indices.push_back(4);
    ixm_column_indices.push_back(2);
    ixm_column_types.push_back(ValueType::tBIGINT);
    ixm_column_types.push_back(ValueType::tBIGINT);
    init("ixh2",
         HASH_TABLE_INDEX,
         ixm_column_indices,
         ixm_column_types,
         false);

    TableIndex* index = table->index("ixh2");
    EXPECT_TRUE(index != NULL);
    IndexCursor indexCursor(index->getTupleSchema());

    TableTuple tuple(table->schema());
    vector<ValueType> keyColumnTypes(2, ValueType::tBIGINT);
    vector<int32_t>keyColumnLengths(2, NValue::getTupleStorageSize(ValueType::tBIGINT));
    vector<bool> keyColumnAllowNull(2, true);
    TupleSchema* keySchema =
        TupleSchema::createTupleSchemaForTest(keyColumnTypes,
                                       keyColumnLengths,
                                       keyColumnAllowNull);
    TableTuple searchkey(keySchema);
    searchkey.move(new char[searchkey.tupleLength()]);

    searchkey.setNValue(0, ValueFactory::getBigIntValue(static_cast<int64_t>(550)));
    searchkey.setNValue(1, ValueFactory::getBigIntValue(static_cast<int64_t>(2)));
    EXPECT_TRUE(index->moveToKey(&searchkey, indexCursor));
    tuple = index->nextValueAtKey(indexCursor);
    EXPECT_FALSE(tuple.isNullTuple());
    EXPECT_TRUE(ValueFactory::getBigIntValue(50).op_equals(tuple.getNValue(0)).isTrue());

    //
    // Before index moves to the next value, re-use the same index to iterate
    //
    IndexCursor indexCursorNew(index->getTupleSchema());
    searchkey.setNValue(0, ValueFactory::getBigIntValue(static_cast<int64_t>(440)));
    searchkey.setNValue(1, ValueFactory::getBigIntValue(static_cast<int64_t>(40 % 3)));

    index->moveToKey(&searchkey, indexCursorNew);
    EXPECT_FALSE((tuple = index->nextValueAtKey(indexCursorNew)).isNullTuple());
    EXPECT_TRUE(ValueFactory::getBigIntValue(40).
            op_equals(tuple.getNValue(0)).isTrue());

    //
    // Go back to the original index places and continue to iterate the next value
    //
    tuple = index->nextValueAtKey(indexCursor);
    EXPECT_TRUE(tuple.isNullTuple());

    TupleSchema::freeTupleSchema(keySchema);
    delete[] searchkey.address();
}


int main()
{
    return TestSuite::globalInstance()->runAll();
}
