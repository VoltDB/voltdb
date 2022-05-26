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

#include "common/serializeio.h"
#include "common/TupleSchema.h"
#include "common/types.h"
#include "common/ValueFactory.hpp"
#include "execution/VoltDBEngine.h"
#include "storage/DRTupleStream.h"
#include "indexes/tableindex.h"
#include "indexes/tableindexfactory.h"
#include "storage/persistenttable.h"
#include "storage/tablefactory.h"
#include "storage/tableutil.h"

#include <stdint.h>
#include <string>
#include <vector>

using namespace voltdb;

class PersistentTableLogTest : public Test {
public:
    PersistentTableLogTest() {
        m_engine = new voltdb::VoltDBEngine();
        int partitionCount = 1;
        m_engine->initialize(1, 1, 0, partitionCount, 0, "", 0, 1024, false, -1, false, DEFAULT_TEMP_TABLE_MEMORY, true);
        partitionCount = htonl(partitionCount);
        m_engine->updateHashinator((char*)&partitionCount, NULL, 0);

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

        m_tableSchemaTypes.push_back(voltdb::ValueType::tBIGINT);
        m_tableSchemaTypes.push_back(voltdb::ValueType::tTINYINT);
        m_tableSchemaTypes.push_back(voltdb::ValueType::tINTEGER);
        m_tableSchemaTypes.push_back(voltdb::ValueType::tBIGINT);
        m_tableSchemaTypes.push_back(voltdb::ValueType::tSMALLINT);
        m_tableSchemaTypes.push_back(voltdb::ValueType::tDOUBLE);
        m_tableSchemaTypes.push_back(voltdb::ValueType::tVARCHAR);
        m_tableSchemaTypes.push_back(voltdb::ValueType::tVARCHAR);
        m_tableSchemaTypes.push_back(voltdb::ValueType::tVARCHAR);
        m_tableSchemaTypes.push_back(voltdb::ValueType::tVARCHAR);

        m_tableSchemaColumnSizes.push_back(NValue::getTupleStorageSize(voltdb::ValueType::tBIGINT));
        m_tableSchemaColumnSizes.push_back(NValue::getTupleStorageSize(voltdb::ValueType::tTINYINT));
        m_tableSchemaColumnSizes.push_back(NValue::getTupleStorageSize(voltdb::ValueType::tINTEGER));
        m_tableSchemaColumnSizes.push_back(NValue::getTupleStorageSize(voltdb::ValueType::tBIGINT));
        m_tableSchemaColumnSizes.push_back(NValue::getTupleStorageSize(voltdb::ValueType::tSMALLINT));
        m_tableSchemaColumnSizes.push_back(NValue::getTupleStorageSize(voltdb::ValueType::tDOUBLE));
        m_tableSchemaColumnSizes.push_back(300);
        m_tableSchemaColumnSizes.push_back(10);
        m_tableSchemaColumnSizes.push_back(500);
        m_tableSchemaColumnSizes.push_back(15);

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

        // Narrower table with no uninlined column
        m_narrowColumnNames.push_back("1");
        m_narrowColumnNames.push_back("2");

        m_narrowTableSchemaTypes.push_back(voltdb::ValueType::tBIGINT);
        m_narrowTableSchemaTypes.push_back(voltdb::ValueType::tVARCHAR);

        m_narrowTableSchemaColumnSizes.push_back(NValue::getTupleStorageSize(voltdb::ValueType::tBIGINT));
        m_narrowTableSchemaColumnSizes.push_back(15);

        m_narrowTableSchemaAllowNull.push_back(false);
        m_narrowTableSchemaAllowNull.push_back(true);

        m_engine->setUndoToken(INT64_MIN + 1);
    }

    ~PersistentTableLogTest() {
        delete m_engine;
        delete m_table;
        voltdb::globalDestroyOncePerProcess();
    }

    void initTable(bool withPK = true) {
        m_tableSchema = TupleSchema::createTupleSchemaForTest(m_tableSchemaTypes,
                                                              m_tableSchemaColumnSizes,
                                                              m_tableSchemaAllowNull);
        m_table = dynamic_cast<PersistentTable*>(
            TableFactory::getPersistentTable(0, "Foo", m_tableSchema, m_columnNames, signature));

        if ( ! withPK ) {
            return;
        }
        voltdb::TableIndexScheme indexScheme("primaryKeyIndex",
                                             BALANCED_TREE_INDEX,
                                             m_primaryKeyIndexColumns,
                                             TableIndex::simplyIndexColumns(),
                                             true, true, false, m_tableSchema);

        TableIndex *pkeyIndex = TableIndexFactory::getInstance(indexScheme);
        assert(pkeyIndex);
        m_table->addIndex(pkeyIndex);
        m_table->setPrimaryKeyIndex(pkeyIndex);
    }

    void initNarrowTable() {
        m_tableSchema = TupleSchema::createTupleSchemaForTest(m_narrowTableSchemaTypes,
                                                              m_narrowTableSchemaColumnSizes,
                                                              m_narrowTableSchemaAllowNull);
        m_table = dynamic_cast<PersistentTable*>(
            TableFactory::getPersistentTable(0, "Foo", m_tableSchema, m_narrowColumnNames, signature));
    }

    VoltDBEngine *m_engine;
    TupleSchema *m_tableSchema;
    PersistentTable *m_table;
    std::vector<std::string> m_columnNames;
    std::vector<ValueType> m_tableSchemaTypes;
    std::vector<int32_t> m_tableSchemaColumnSizes;
    std::vector<bool> m_tableSchemaAllowNull;
    std::vector<int> m_primaryKeyIndexColumns;

    std::vector<std::string> m_narrowColumnNames;
    std::vector<ValueType> m_narrowTableSchemaTypes;
    std::vector<int32_t> m_narrowTableSchemaColumnSizes;
    std::vector<bool> m_narrowTableSchemaAllowNull;

    char signature[20];
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
    initTable();
    tableutil::addRandomTuples(m_table, 1000);
    voltdb::TableTuple tuple(m_tableSchema);

    tableutil::getRandomTuple(m_table, tuple);

    ASSERT_FALSE( m_table->lookupTupleForUndo(tuple).isNullTuple());

    voltdb::TableTuple tupleBackup(m_tableSchema);
    tupleBackup.move(new char[tupleBackup.tupleLength()]);
    tupleBackup.copyForPersistentInsert(tuple);
    StackCleaner cleaner(tupleBackup);

    m_engine->setUndoToken(INT64_MIN + 2);
    // this next line is a testing hack until engine data is
    // de-duplicated with executorcontext data
    m_engine->updateExecutorContextUndoQuantumForTest();

    m_table->deleteTuple(tuple, true);

    ASSERT_TRUE( m_table->lookupTupleForUndo(tupleBackup).isNullTuple());

    m_engine->undoUndoToken(INT64_MIN + 2);

    ASSERT_FALSE(m_table->lookupTupleForUndo(tuple).isNullTuple());
}

TEST_F(PersistentTableLogTest, LoadTableThenUndoTest) {
    initTable();
    tableutil::addRandomTuples(m_table, 1000);

    CopySerializeOutput serialize_out;
    m_table->serializeTo(serialize_out);

    m_engine->setUndoToken(INT64_MIN + 2);
    // this next line is a testing hack until engine data is
    // de-duplicated with executorcontext data
    m_engine->updateExecutorContextUndoQuantumForTest();

    m_table->deleteAllTuples();
    m_engine->releaseUndoToken(INT64_MIN + 2, false);

    delete m_table;

    initTable();

    ReferenceSerializeInputBE serialize_in(serialize_out.data() + sizeof(int32_t), serialize_out.size() - sizeof(int32_t));

    m_engine->setUndoToken(INT64_MIN + 3);
    // this next line is a testing hack until engine data is
    // de-duplicated with executorcontext data
    m_engine->updateExecutorContextUndoQuantumForTest();

    m_table->loadTuplesFrom(serialize_in, NULL);
    voltdb::TableTuple tuple(m_tableSchema);

    tableutil::getRandomTuple(m_table, tuple);
    ASSERT_FALSE( m_table->lookupTupleForUndo(tuple).isNullTuple());

    // After calling undoUndoToken(), variable "tuple" is deactivated and the uninlined
    // data it contains maybe freed, the safe way is to copy the "tuple" before undo.
    voltdb::TableTuple tupleBackup(m_tableSchema);
    tupleBackup.move(new char[tupleBackup.tupleLength()]);
    tupleBackup.copyForPersistentInsert(tuple);
    StackCleaner cleaner(tupleBackup);

    m_engine->undoUndoToken(INT64_MIN + 3);

    ASSERT_TRUE(m_table->lookupTupleForUndo(tupleBackup).isNullTuple());
    ASSERT_TRUE(m_table->activeTupleCount() == (int64_t)0);
}

TEST_F(PersistentTableLogTest, LoadTableThenReleaseTest) {
    initTable();
    tableutil::addRandomTuples(m_table, 1000);

    CopySerializeOutput serialize_out;
    m_table->serializeTo(serialize_out);

    m_engine->setUndoToken(INT64_MIN + 2);
    // this next line is a testing hack until engine data is
    // de-duplicated with executorcontext data
    m_engine->updateExecutorContextUndoQuantumForTest();

    m_table->deleteAllTuples();
    m_engine->releaseUndoToken(INT64_MIN + 2, false);

    delete m_table;

    initTable();

    ReferenceSerializeInputBE serialize_in(serialize_out.data() + sizeof(int32_t), serialize_out.size() - sizeof(int32_t));

    m_engine->setUndoToken(INT64_MIN + 3);
    // this next line is a testing hack until engine data is
    // de-duplicated with executorcontext data
    m_engine->updateExecutorContextUndoQuantumForTest();

    m_table->loadTuplesFrom(serialize_in, NULL);
    voltdb::TableTuple tuple(m_tableSchema);

    tableutil::getRandomTuple(m_table, tuple);
    ASSERT_FALSE( m_table->lookupTupleForUndo(tuple).isNullTuple());

    m_engine->releaseUndoToken(INT64_MIN + 3, false);

    ASSERT_FALSE(m_table->lookupTupleForUndo(tuple).isNullTuple());
    ASSERT_TRUE(m_table->activeTupleCount() == (int64_t)1000);
}

TEST_F(PersistentTableLogTest, InsertUpdateThenUndoOneTest) {
    initTable();
    tableutil::addRandomTuples(m_table, 1);
    voltdb::TableTuple tuple(m_tableSchema);

    tableutil::getRandomTuple(m_table, tuple);
    //std::cout << "Retrieved random tuple " << std::endl << tuple.debugNoHeader() << std::endl;

    ASSERT_FALSE( m_table->lookupTupleForUndo(tuple).isNullTuple());

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
    m_engine->updateExecutorContextUndoQuantumForTest();

    /*
     * Update a few columns
     */
    tupleCopy.setNValue(0, ValueFactory::getBigIntValue(5));
    NValue newStringValue = ValueFactory::getStringValue("foo");
    tupleCopy.setNValue(7, newStringValue);
    NValue oldStringValue = tupleCopy.getNValue(6);
    tupleCopy.setNValue(6, ValueFactory::getStringValue("bar"));

    m_table->updateTuple(tuple, tupleCopy);

    ASSERT_TRUE( m_table->lookupTupleForUndo(tupleBackup).isNullTuple());
    ASSERT_FALSE( m_table->lookupTupleForUndo(tupleCopy).isNullTuple());
    m_engine->undoUndoToken(INT64_MIN + 2);

    ASSERT_FALSE(m_table->lookupTupleForUndo(tuple).isNullTuple());
    ASSERT_TRUE( m_table->lookupTupleForUndo(tupleCopy).isNullTuple());
    tupleBackup.freeObjectColumns();
    tupleCopy.freeObjectColumns();
    delete [] tupleBackup.address();
    delete [] tupleCopy.address();
    newStringValue.free();
    oldStringValue.free();
}

TEST_F(PersistentTableLogTest, InsertThenUndoInsertsOneTest) {
    initTable();
    tableutil::addRandomTuples(m_table, 10);
    ASSERT_EQ( m_table->activeTupleCount(), 10);
    m_engine->undoUndoToken(INT64_MIN + 1);
    ASSERT_EQ( m_table->activeTupleCount(), 0);
}

TEST_F(PersistentTableLogTest, InsertDupsThenUndoWorksTest) {
    initTable(false);
    tableutil::addDuplicateRandomTuples(m_table, 2);
    tableutil::addDuplicateRandomTuples(m_table, 3);
    ASSERT_EQ(5, m_table->activeTupleCount());
    m_engine->undoUndoToken(INT64_MIN + 1);
    ASSERT_EQ(0, m_table->activeTupleCount());
}

TEST_F(PersistentTableLogTest, FindBlockTest) {
    initTable();
    const int blockSize = m_table->getTableAllocationSize();
    TBBucketPtr bucket(new TBBucket());

    // these will be used as artificial tuple block addresses
    TBPtr block1(new TupleBlock(m_table, bucket));
    TBPtr block2(new TupleBlock(m_table, bucket));
    TBPtr block3(new TupleBlock(m_table, bucket));

    TBMap blocks;
    char *base = block1->address();

    // block2 is adjacent to block1, block3 is 1 block away from block2
    blocks.insert(base,                 block1);
    blocks.insert(base + blockSize,     block2);
    blocks.insert(base + blockSize * 3, block3);

    // in the middle but land on a missing boundary
    ASSERT_EQ(PersistentTable::findBlock(base + blockSize * 2, blocks, blockSize).get(), NULL);
    // past the end but on a boundary
    ASSERT_EQ(PersistentTable::findBlock(base + blockSize * 4, blocks, blockSize).get(), NULL);

    // the following tuples should be found in the map
    ASSERT_EQ(PersistentTable::findBlock(base,                     blocks, blockSize)->address(),
              block1->address());
    ASSERT_EQ(PersistentTable::findBlock(base + blockSize - 1,     blocks, blockSize)->address(),
              block1->address());
    ASSERT_EQ(PersistentTable::findBlock(base + blockSize * 4 - 1, blocks, blockSize)->address(),
              block3->address());
}

TEST_F(PersistentTableLogTest, LookupTupleForUndoNoPKTest) {
    initTable(false);
    tableutil::addDuplicateRandomTuples(m_table, 2);
    tableutil::addDuplicateRandomTuples(m_table, 3);

    // assert that lookupTupleForUndo finds the correct tuple
    voltdb::TableTuple tuple(m_tableSchema);
    tableutil::getRandomTuple(m_table, tuple);
    ASSERT_EQ(m_table->lookupTupleForUndo(tuple).address(), tuple.address());
}

TEST_F(PersistentTableLogTest, LookupTupleUsingTempTupleTest) {
    initNarrowTable();

    // Create three tuple with a variable length VARCHAR column, then call
    // lookupTupleForUndo() to look each tuple up from wide to narrower column.
    // It will use the memcmp() code path for the comparison, which should all
    // succeed because there is no uninlined stuff.

    NValue wideStr = ValueFactory::getStringValue("a long string");
    NValue narrowStr = ValueFactory::getStringValue("a");
    NValue nullStr = ValueFactory::getNullStringValue();

    TableTuple wideTuple(m_tableSchema);
    wideTuple.move(new char[wideTuple.tupleLength()]);
    ::memset(wideTuple.address(), 0, wideTuple.tupleLength());
    wideTuple.setNValue(0, ValueFactory::getBigIntValue(1));
    wideTuple.setNValue(1, wideStr);
    m_table->insertTuple(wideTuple);
    delete[] wideTuple.address();

    TableTuple narrowTuple(m_tableSchema);
    narrowTuple.move(new char[narrowTuple.tupleLength()]);
    ::memset(narrowTuple.address(), 0, narrowTuple.tupleLength());
    narrowTuple.setNValue(0, ValueFactory::getBigIntValue(2));
    narrowTuple.setNValue(1, narrowStr);
    m_table->insertTuple(narrowTuple);
    delete[] narrowTuple.address();

    TableTuple nullTuple(m_tableSchema);
    nullTuple.move(new char[nullTuple.tupleLength()]);
    ::memset(nullTuple.address(), 0, nullTuple.tupleLength());
    nullTuple.setNValue(0, ValueFactory::getBigIntValue(3));
    nullTuple.setNValue(1, nullStr);
    m_table->insertTuple(nullTuple);
    delete[] nullTuple.address();

    TableTuple tempTuple = m_table->tempTuple();
    tempTuple.setNValue(0, ValueFactory::getBigIntValue(1));
    tempTuple.setNValue(1, wideStr);
    TableTuple result = m_table->lookupTupleForUndo(tempTuple);
    ASSERT_FALSE(result.isNullTuple());

    tempTuple = m_table->tempTuple();
    tempTuple.setNValue(0, ValueFactory::getBigIntValue(2));
    tempTuple.setNValue(1, narrowStr);
    result = m_table->lookupTupleForUndo(tempTuple);
    ASSERT_FALSE(result.isNullTuple());

    tempTuple = m_table->tempTuple();
    tempTuple.setNValue(0, ValueFactory::getBigIntValue(3));
    tempTuple.setNValue(1, nullStr);
    result = m_table->lookupTupleForUndo(tempTuple);
    ASSERT_FALSE(result.isNullTuple());

    wideStr.free();
    narrowStr.free();
    nullStr.free();
}

int main() {
    return TestSuite::globalInstance()->runAll();
}
