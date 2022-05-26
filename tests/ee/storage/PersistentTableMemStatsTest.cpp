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

#include "common/TupleSchema.h"
#include "common/types.h"
#include "common/ValueFactory.hpp"
#include "execution/VoltDBEngine.h"
#include "indexes/tableindex.h"
#include "indexes/tableindexfactory.h"
#include "storage/DRTupleStream.h"
#include "storage/persistenttable.h"
#include "storage/tablefactory.h"
#include "storage/tableutil.h"

#include <boost/foreach.hpp>

#include <stdint.h>
#include <string>
#include <vector>

using namespace std;
using namespace voltdb;

class PersistentTableMemStatsTest : public Test {
public:
    PersistentTableMemStatsTest() {
        m_engine = new VoltDBEngine();
        int partitionCount = 1;
        m_engine->initialize(1, 1, 0, partitionCount, 0, "", 0, 1024, false, -1, false, DEFAULT_TEMP_TABLE_MEMORY, true);
        partitionCount = htonl(partitionCount);
        m_engine->updateHashinator((char*)&partitionCount, NULL, 0);

        m_columnNames.push_back("0");
        m_columnNames.push_back("1");
        m_columnNames.push_back("2");

        m_tableSchemaTypes.push_back(ValueType::tTINYINT);
        m_tableSchemaTypes.push_back(ValueType::tVARCHAR);
        m_tableSchemaTypes.push_back(ValueType::tVARCHAR);

        m_tableSchemaColumnSizes.push_back(NValue::getTupleStorageSize(ValueType::tTINYINT));
        m_tableSchemaColumnSizes.push_back(300);
        m_tableSchemaColumnSizes.push_back(100);

        m_tableSchemaAllowNull.push_back(false);
        m_tableSchemaAllowNull.push_back(false);
        m_tableSchemaAllowNull.push_back(false);

        m_primaryKeyIndexColumns.push_back(0);
        m_primaryKeyIndexColumns.push_back(1);
        m_primaryKeyIndexColumns.push_back(2);

        m_engine->setUndoToken(INT64_MIN + 1);
    }

    ~PersistentTableMemStatsTest() {
        delete m_engine;
        delete m_table;
        voltdb::globalDestroyOncePerProcess();
    }

    void initTable() {
        m_tableSchema = TupleSchema::createTupleSchemaForTest(m_tableSchemaTypes,
                                                       m_tableSchemaColumnSizes,
                                                       m_tableSchemaAllowNull);

        TableIndexScheme indexScheme("primaryKeyIndex",
                                     BALANCED_TREE_INDEX,
                                     m_primaryKeyIndexColumns,
                                     TableIndex::simplyIndexColumns(),
                                     true, true, false, m_tableSchema);

        vector<TableIndexScheme> indexes;

        m_table = dynamic_cast<PersistentTable*>(
            TableFactory::getPersistentTable(0, "Foo", m_tableSchema, m_columnNames, signature));

        TableIndex *pkeyIndex = TableIndexFactory::getInstance(indexScheme);
        assert(pkeyIndex);
        m_table->addIndex(pkeyIndex);
        m_table->setPrimaryKeyIndex(pkeyIndex);

        // add other indexes
        BOOST_FOREACH(TableIndexScheme &scheme, indexes) {
            TableIndex *index = TableIndexFactory::getInstance(scheme);
            assert(index);
            m_table->addIndex(index);
        }
    }

    VoltDBEngine *m_engine;
    TupleSchema *m_tableSchema;
    PersistentTable *m_table;
    vector<string> m_columnNames;
    vector<ValueType> m_tableSchemaTypes;
    vector<int32_t> m_tableSchemaColumnSizes;
    vector<bool> m_tableSchemaAllowNull;
    vector<int> m_primaryKeyIndexColumns;
    char signature[20];
};

TEST_F(PersistentTableMemStatsTest, InsertTest) {
    initTable();
    tableutil::addRandomTuples(m_table, 10);
    int64_t orig_size = m_table->nonInlinedMemorySize();
    //cout << "Original non-inline size: " << orig_size << endl;

    TableTuple tuple(m_tableSchema);
    tuple.move(new char[tuple.tupleLength()]());
    tableutil::setRandomTupleValues(m_table, &tuple);
    //cout << "Created random tuple " << endl << tuple.debugNoHeader() << endl;
    size_t added_bytes =
        tuple.getNValue(1).getAllocationSizeForObjectInPersistentStorage() +
        tuple.getNValue(2).getAllocationSizeForObjectInPersistentStorage();
    //cout << "Allocating string mem for bytes: " << ValuePeeker::peekObjectLength(tuple.getNValue(1)) + sizeof(int32_t) << endl;
    //cout << "Adding bytes to table: " << added_bytes << endl;

    m_engine->setUndoToken(INT64_MIN + 2);
    // this next line is a testing hack until engine data is
    // de-duplicated with executorcontext data
    m_engine->updateExecutorContextUndoQuantumForTest();

    m_table->insertTuple(tuple);

    m_engine->releaseUndoToken(INT64_MIN + 2, false);

    ASSERT_EQ(orig_size + added_bytes, m_table->nonInlinedMemorySize());

    tuple.freeObjectColumns();
    delete [] tuple.address();
}

TEST_F(PersistentTableMemStatsTest, InsertThenUndoInsertTest) {
    initTable();
    tableutil::addRandomTuples(m_table, 10);
    int64_t orig_size = m_table->nonInlinedMemorySize();
    //cout << "Original non-inline size: " << orig_size << endl;

    TableTuple tuple(m_tableSchema);
    tuple.move(new char[tuple.tupleLength()]());
    tableutil::setRandomTupleValues(m_table, &tuple);
    //cout << "Created random tuple " << endl << tuple.debugNoHeader() << endl;
    size_t added_bytes =
        tuple.getNValue(1).getAllocationSizeForObjectInPersistentStorage() +
        tuple.getNValue(2).getAllocationSizeForObjectInPersistentStorage();
    //cout << "Adding bytes to table: " << added_bytes << endl;

    m_engine->setUndoToken(INT64_MIN + 2);
    // this next line is a testing hack until engine data is
    // de-duplicated with executorcontext data
    m_engine->updateExecutorContextUndoQuantumForTest();

    m_table->insertTuple(tuple);

    //cout << "pre-undo non-inline size: " << m_table->nonInlinedMemorySize() << endl;
    ASSERT_EQ(orig_size + added_bytes, m_table->nonInlinedMemorySize());

    m_engine->undoUndoToken(INT64_MIN + 2);

    //cout << "post-undo non-inline size: " << m_table->nonInlinedMemorySize() << endl;
    ASSERT_EQ(orig_size, m_table->nonInlinedMemorySize());

    tuple.freeObjectColumns();
    delete [] tuple.address();
}

TEST_F(PersistentTableMemStatsTest, UpdateTest) {
    initTable();
    tableutil::addRandomTuples(m_table, 10);
    int64_t orig_size = m_table->nonInlinedMemorySize();
    //cout << "Original non-inline size: " << orig_size << endl;

    TableTuple tuple(m_tableSchema);
    tableutil::getRandomTuple(m_table, tuple);
    //cout << "Retrieved random tuple " << endl << tuple.debugNoHeader() << endl;

    size_t removed_bytes =
        tuple.getNValue(1).getAllocationSizeForObjectInPersistentStorage() +
        tuple.getNValue(2).getAllocationSizeForObjectInPersistentStorage();
    //cout << "Removing bytes from table: " << removed_bytes << endl;

    /*
     * A copy of the tuple to modify and use as a source tuple when
     * updating the new tuple.
     */
    TableTuple tempTuple = m_table->tempTuple();
    tempTuple.copy(tuple);
    string strval = "123456";
    NValue new_string = ValueFactory::getStringValue(strval);
    tempTuple.setNValue(1, new_string);
    //cout << "Created updated tuple " << endl << tempTuple.debugNoHeader() << endl;
    size_t added_bytes =
        tempTuple.getNValue(1).getAllocationSizeForObjectInPersistentStorage() +
        tempTuple.getNValue(2).getAllocationSizeForObjectInPersistentStorage();
    //cout << "Adding bytes to table: " << added_bytes << endl;

    m_engine->setUndoToken(INT64_MIN + 2);
    // this next line is a testing hack until engine data is
    // de-duplicated with executorcontext data
    m_engine->updateExecutorContextUndoQuantumForTest();

    m_table->updateTuple(tuple, tempTuple);

    m_engine->releaseUndoToken(INT64_MIN + 2, false);

    ASSERT_EQ(orig_size + added_bytes - removed_bytes, m_table->nonInlinedMemorySize());

    //cout << "final non-inline size: " << m_table->nonInlinedMemorySize() << endl;
    //tuple.freeObjectColumns();
    //tempTuple.freeObjectColumns();
    //delete [] tuple.address();
    //delete[] tempTuple.address();
    new_string.free();
}

TEST_F(PersistentTableMemStatsTest, UpdateAndUndoTest) {
    initTable();
    tableutil::addRandomTuples(m_table, 10);
    int64_t orig_size = m_table->nonInlinedMemorySize();
    //cout << "Original non-inline size: " << orig_size << endl;

    TableTuple tuple(m_tableSchema);
    tableutil::getRandomTuple(m_table, tuple);
    //cout << "Retrieved random tuple " << endl << tuple.debugNoHeader() << endl;

    size_t removed_bytes =
        tuple.getNValue(1).getAllocationSizeForObjectInPersistentStorage() +
        tuple.getNValue(2).getAllocationSizeForObjectInPersistentStorage();
    //cout << "Removing bytes from table: " << removed_bytes << endl;

    /*
     * A copy of the tuple to modify and use as a source tuple when
     * updating the new tuple.
     */
    TableTuple tempTuple = m_table->tempTuple();
    tempTuple.copy(tuple);
    string strval = "123456";
    NValue new_string = ValueFactory::getStringValue(strval);
    tempTuple.setNValue(1, new_string);
    //cout << "Created random tuple " << endl << tempTuple.debugNoHeader() << endl;
    size_t added_bytes =
        tempTuple.getNValue(1).getAllocationSizeForObjectInPersistentStorage() +
        tempTuple.getNValue(2).getAllocationSizeForObjectInPersistentStorage();
    //cout << "Adding bytes to table: " << added_bytes << endl;

    m_engine->setUndoToken(INT64_MIN + 2);
    // this next line is a testing hack until engine data is
    // de-duplicated with executorcontext data
    m_engine->updateExecutorContextUndoQuantumForTest();

    m_table->updateTuple(tuple, tempTuple);

    ASSERT_EQ(orig_size + added_bytes - removed_bytes, m_table->nonInlinedMemorySize());

    m_engine->undoUndoToken(INT64_MIN + 2);

    ASSERT_EQ(orig_size, m_table->nonInlinedMemorySize());

    //tuple.freeObjectColumns();
    //tempTuple.freeObjectColumns();
    //delete [] tuple.address();
    //delete[] tempTuple.address();
    new_string.free();
}

TEST_F(PersistentTableMemStatsTest, DeleteTest) {
    initTable();
    tableutil::addRandomTuples(m_table, 10);
    int64_t orig_size = m_table->nonInlinedMemorySize();
    //cout << "Original non-inline size: " << orig_size << endl;

    TableTuple tuple(m_tableSchema);
    tableutil::getRandomTuple(m_table, tuple);
    //cout << "Retrieved random tuple " << endl << tuple.debugNoHeader() << endl;

    size_t removed_bytes =
        tuple.getNValue(1).getAllocationSizeForObjectInPersistentStorage() +
        tuple.getNValue(2).getAllocationSizeForObjectInPersistentStorage();
    //cout << "Removing bytes from table: " << removed_bytes << endl;

    m_engine->setUndoToken(INT64_MIN + 2);
    // this next line is a testing hack until engine data is
    // de-duplicated with executorcontext data
    m_engine->updateExecutorContextUndoQuantumForTest();

    m_table->deleteTuple(tuple, true);

    m_engine->releaseUndoToken(INT64_MIN + 2, false);

    //cout << "Final non-inline size: " << m_table->nonInlinedMemorySize() << endl;
    ASSERT_EQ(orig_size - removed_bytes, m_table->nonInlinedMemorySize());

    //tuple.freeObjectColumns();
    //delete [] tuple.address();
}

TEST_F(PersistentTableMemStatsTest, DeleteAndUndoTest) {
    initTable();
    tableutil::addRandomTuples(m_table, 10);
    int64_t orig_size = m_table->nonInlinedMemorySize();
    //cout << "Original non-inline size: " << orig_size << endl;

    TableTuple tuple(m_tableSchema);
    tableutil::getRandomTuple(m_table, tuple);
    //cout << "Retrieved random tuple " << endl << tuple.debugNoHeader() << endl;

    //cout << "Removing bytes from table: " << removed_bytes << endl;

    m_engine->setUndoToken(INT64_MIN + 2);
    // this next line is a testing hack until engine data is
    // de-duplicated with executorcontext data
    m_engine->updateExecutorContextUndoQuantumForTest();

    m_table->deleteTuple(tuple, true);

    ASSERT_EQ(orig_size, m_table->nonInlinedMemorySize());

    m_engine->undoUndoToken(INT64_MIN + 2);

    ASSERT_EQ(orig_size, m_table->nonInlinedMemorySize());

    //tuple.freeObjectColumns();
    //delete [] tuple.address();
}

int main() {
    return TestSuite::globalInstance()->runAll();
}
