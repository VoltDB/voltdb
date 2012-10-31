/* This file is part of VoltDB.
 * Copyright (C) 2008-2012 VoltDB Inc.
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
#include "common/TupleSchema.h"
#include "common/types.h"
#include "common/NValue.hpp"
#include "common/ValueFactory.hpp"
#include "execution/VoltDBEngine.h"
#include "storage/persistenttable.h"
#include "storage/tablefactory.h"
#include "storage/tableutil.h"
#include "indexes/tableindex.h"
#include <vector>
#include <string>
#include <stdint.h>

using namespace voltdb;

class PersistentTableLogTest : public Test {
public:
    PersistentTableLogTest() {
        m_engine = new voltdb::VoltDBEngine();
        m_engine->initialize(1,1, 0, 0, "", DEFAULT_TEMP_TABLE_MEMORY, 1);

        m_columnNames.push_back("1");
        m_columnNames.push_back("2");
        m_columnNames.push_back("3");
        m_columnNames.push_back("4");
        m_columnNames.push_back("5");
        m_columnNames.push_back("6");
        m_columnNames.push_back("7");
        m_columnNames.push_back("8");
        m_columnNames.push_back("9");
        m_columnNames.push_back("10");

        m_tableSchemaTypes.push_back(voltdb::VALUE_TYPE_BIGINT);
        m_tableSchemaTypes.push_back(voltdb::VALUE_TYPE_TINYINT);
        m_tableSchemaTypes.push_back(voltdb::VALUE_TYPE_INTEGER);
        m_tableSchemaTypes.push_back(voltdb::VALUE_TYPE_BIGINT);
        m_tableSchemaTypes.push_back(voltdb::VALUE_TYPE_SMALLINT);
        m_tableSchemaTypes.push_back(voltdb::VALUE_TYPE_DOUBLE);
        m_tableSchemaTypes.push_back(voltdb::VALUE_TYPE_VARCHAR);
        m_tableSchemaTypes.push_back(voltdb::VALUE_TYPE_VARCHAR);
        m_tableSchemaTypes.push_back(voltdb::VALUE_TYPE_VARCHAR);
        m_tableSchemaTypes.push_back(voltdb::VALUE_TYPE_VARCHAR);

        m_tableSchemaColumnSizes.push_back(NValue::getTupleStorageSize(voltdb::VALUE_TYPE_BIGINT));
        m_tableSchemaColumnSizes.push_back(NValue::getTupleStorageSize(voltdb::VALUE_TYPE_TINYINT));
        m_tableSchemaColumnSizes.push_back(NValue::getTupleStorageSize(voltdb::VALUE_TYPE_INTEGER));
        m_tableSchemaColumnSizes.push_back(NValue::getTupleStorageSize(voltdb::VALUE_TYPE_BIGINT));
        m_tableSchemaColumnSizes.push_back(NValue::getTupleStorageSize(voltdb::VALUE_TYPE_SMALLINT));
        m_tableSchemaColumnSizes.push_back(NValue::getTupleStorageSize(voltdb::VALUE_TYPE_DOUBLE));
        m_tableSchemaColumnSizes.push_back(300);
        m_tableSchemaColumnSizes.push_back(16);
        m_tableSchemaColumnSizes.push_back(500);
        m_tableSchemaColumnSizes.push_back(32);

        m_tableSchemaAllowNull.push_back(false);
        m_tableSchemaAllowNull.push_back(false);
        m_tableSchemaAllowNull.push_back(true);
        m_tableSchemaAllowNull.push_back(true);
        m_tableSchemaAllowNull.push_back(true);
        m_tableSchemaAllowNull.push_back(true);
        m_tableSchemaAllowNull.push_back(false);
        m_tableSchemaAllowNull.push_back(false);
        m_tableSchemaAllowNull.push_back(true);
        m_tableSchemaAllowNull.push_back(true);

        m_primaryKeyIndexColumns.push_back(0);
        m_primaryKeyIndexColumns.push_back(1);
        m_primaryKeyIndexColumns.push_back(6);
        m_primaryKeyIndexColumns.push_back(7);

        m_engine->setUndoToken(INT64_MIN + 1);
    }

    ~PersistentTableLogTest() {
        delete m_engine;
        delete m_table;
    }

    void initTable(bool allowInlineStrings) {
        m_tableSchema = voltdb::TupleSchema::createTupleSchema(m_tableSchemaTypes,
                                                               m_tableSchemaColumnSizes,
                                                               m_tableSchemaAllowNull,
                                                               allowInlineStrings);

        voltdb::TableIndexScheme indexScheme("primaryKeyIndex",
                                             voltdb::BALANCED_TREE_INDEX,
                                             m_primaryKeyIndexColumns,
                                             TableIndex::simplyIndexColumns(),
                                             true, true, m_tableSchema);
        std::vector<voltdb::TableIndexScheme> indexes;

        m_table = dynamic_cast<voltdb::PersistentTable*>(
            voltdb::TableFactory::getPersistentTable(0, "Foo", m_tableSchema, m_columnNames, 0));

        TableIndex *pkeyIndex = TableIndexFactory::TableIndexFactory::getInstance(indexScheme);
        assert(pkeyIndex);
        m_table->addIndex(pkeyIndex);
        m_table->setPrimaryKeyIndex(pkeyIndex);
    }



    voltdb::VoltDBEngine *m_engine;
    voltdb::TupleSchema *m_tableSchema;
    voltdb::PersistentTable *m_table;
    std::vector<std::string> m_columnNames;
    std::vector<voltdb::ValueType> m_tableSchemaTypes;
    std::vector<int32_t> m_tableSchemaColumnSizes;
    std::vector<bool> m_tableSchemaAllowNull;
    std::vector<int> m_primaryKeyIndexColumns;
};

class StackCleaner {
public:
    StackCleaner(TableTuple tuple) : m_tuple(tuple) {}
    ~StackCleaner() {
        m_tuple.freeObjectColumns();
        delete [] m_tuple.address();
    }
private:
    TableTuple m_tuple;
};

TEST_F(PersistentTableLogTest, InsertDeleteThenUndoOneTest) {
    initTable(true);
    tableutil::addRandomTuples(m_table, 1000);
    voltdb::TableTuple tuple(m_tableSchema);

    tableutil::getRandomTuple(m_table, tuple);

    ASSERT_FALSE( m_table->lookupTuple(tuple).isNullTuple());

    voltdb::TableTuple tupleBackup(m_tableSchema);
    tupleBackup.move(new char[tupleBackup.tupleLength()]);
    tupleBackup.copyForPersistentInsert(tuple);
    StackCleaner cleaner(tupleBackup);

    m_engine->setUndoToken(INT64_MIN + 2);
    // this next line is a testing hack until engine data is
    // de-duplicated with executorcontext data
    m_engine->getExecutorContext();

    m_table->deleteTuple(tuple, true);

    ASSERT_TRUE( m_table->lookupTuple(tupleBackup).isNullTuple());

    m_engine->undoUndoToken(INT64_MIN + 2);

    ASSERT_FALSE(m_table->lookupTuple(tuple).isNullTuple());
}

TEST_F(PersistentTableLogTest, InsertUpdateThenUndoOneTest) {
    initTable(true);
    tableutil::addRandomTuples(m_table, 1);
    voltdb::TableTuple tuple(m_tableSchema);

    tableutil::getRandomTuple(m_table, tuple);
    //std::cout << "Retrieved random tuple " << std::endl << tuple.debugNoHeader() << std::endl;

    ASSERT_FALSE( m_table->lookupTuple(tuple).isNullTuple());

    /*
     * A backup copy of what the tuple looked like before updates
     */
    voltdb::TableTuple tupleBackup(m_tableSchema);
    tupleBackup.move(new char[tupleBackup.tupleLength()]);
    tupleBackup.copyForPersistentInsert(tuple);

    /*
     * A copy of the tuple to modify and use as a source tuple when updating the new tuple.
     */
    voltdb::TableTuple tupleCopy(m_tableSchema);
    tupleCopy.move(new char[tupleCopy.tupleLength()]);
    tupleCopy.copyForPersistentInsert(tuple);

    m_engine->setUndoToken(INT64_MIN + 2);

    // this next line is a testing hack until engine data is
    // de-duplicated with executorcontext data
    m_engine->getExecutorContext();

    /*
     * Update a few columns
     */
    tupleCopy.setNValue(0, ValueFactory::getBigIntValue(5));
    NValue newStringValue = ValueFactory::getStringValue("foo");
    tupleCopy.setNValue(7, newStringValue);
    NValue oldStringValue = tupleCopy.getNValue(6);
    tupleCopy.setNValue(6, ValueFactory::getStringValue("bar"));

    m_table->updateTuple(tuple, tupleCopy);

    ASSERT_TRUE( m_table->lookupTuple(tupleBackup).isNullTuple());
    ASSERT_FALSE( m_table->lookupTuple(tupleCopy).isNullTuple());
    m_engine->undoUndoToken(INT64_MIN + 2);

    ASSERT_FALSE(m_table->lookupTuple(tuple).isNullTuple());
    ASSERT_TRUE( m_table->lookupTuple(tupleCopy).isNullTuple());
    tupleBackup.freeObjectColumns();
    tupleCopy.freeObjectColumns();
    delete [] tupleBackup.address();
    delete [] tupleCopy.address();
    newStringValue.free();
    oldStringValue.free();
}

TEST_F(PersistentTableLogTest, InsertThenUndoInsertsOneTest) {
    initTable(true);
    tableutil::addRandomTuples(m_table, 10);
    ASSERT_EQ( m_table->activeTupleCount(), 10);
    m_engine->undoUndoToken(INT64_MIN + 1);
    ASSERT_EQ( m_table->activeTupleCount(), 0);
}

int main() {
    return TestSuite::globalInstance()->runAll();
}
