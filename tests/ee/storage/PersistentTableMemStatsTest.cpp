/* This file is part of VoltDB.
 * Copyright (C) 2008-2011 VoltDB Inc.
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

using namespace std;
using namespace voltdb;

class PersistentTableMemStatsTest : public Test {
public:
    PersistentTableMemStatsTest() {
        m_engine = new VoltDBEngine();
        m_engine->initialize(1,1, 0, 0, "", DEFAULT_TEMP_TABLE_MEMORY);

        m_columnNames.push_back("0");
        m_columnNames.push_back("1");
        m_columnNames.push_back("2");

        m_tableSchemaTypes.push_back(VALUE_TYPE_TINYINT);
        m_primaryKeyIndexSchemaTypes.push_back(VALUE_TYPE_TINYINT);
        m_tableSchemaTypes.push_back(VALUE_TYPE_VARCHAR);
        m_primaryKeyIndexSchemaTypes.push_back(VALUE_TYPE_VARCHAR);
        m_tableSchemaTypes.push_back(VALUE_TYPE_VARCHAR);
        m_primaryKeyIndexSchemaTypes.push_back(VALUE_TYPE_VARCHAR);

        m_tableSchemaColumnSizes.push_back(NValue::getTupleStorageSize(VALUE_TYPE_TINYINT));
        m_primaryKeyIndexSchemaColumnSizes.push_back(VALUE_TYPE_TINYINT);
        m_tableSchemaColumnSizes.push_back(300);
        m_primaryKeyIndexSchemaColumnSizes.push_back(300);
        m_tableSchemaColumnSizes.push_back(100);
        m_primaryKeyIndexSchemaColumnSizes.push_back(100);

        m_tableSchemaAllowNull.push_back(false);
        m_primaryKeyIndexSchemaAllowNull.push_back(false);
        m_tableSchemaAllowNull.push_back(false);
        m_primaryKeyIndexSchemaAllowNull.push_back(false);
        m_tableSchemaAllowNull.push_back(false);
        m_primaryKeyIndexSchemaAllowNull.push_back(false);

        m_primaryKeyIndexColumns.push_back(0);
        m_primaryKeyIndexColumns.push_back(1);
        m_primaryKeyIndexColumns.push_back(2);

        m_engine->setUndoToken(INT64_MIN + 1);
    }

    ~PersistentTableMemStatsTest() {
        delete m_engine;
        delete m_table;
        TupleSchema::freeTupleSchema(m_primaryKeyIndexSchema);
    }

    void initTable(bool allowInlineStrings) {
        m_tableSchema = TupleSchema::createTupleSchema(m_tableSchemaTypes,
                                                       m_tableSchemaColumnSizes,
                                                       m_tableSchemaAllowNull,
                                                       allowInlineStrings);

        m_primaryKeyIndexSchema =
            TupleSchema::createTupleSchema(m_primaryKeyIndexSchemaTypes,
                                           m_primaryKeyIndexSchemaColumnSizes,
                                           m_primaryKeyIndexSchemaAllowNull,
                                           allowInlineStrings);

        TableIndexScheme indexScheme =
            TableIndexScheme("primaryKeyIndex",
                             BALANCED_TREE_INDEX,
                             m_primaryKeyIndexColumns,
                             m_primaryKeyIndexSchemaTypes,
                             true, false, m_tableSchema);

        indexScheme.keySchema = m_primaryKeyIndexSchema;

        vector<TableIndexScheme> indexes;

        m_table =
            dynamic_cast<PersistentTable*>(TableFactory::getPersistentTable
                                           (0, m_engine->getExecutorContext(),
                                            "Foo", m_tableSchema,
                                            &m_columnNames[0], indexScheme,
                                            indexes, 0, false, false));
    }

    VoltDBEngine *m_engine;
    TupleSchema *m_tableSchema;
    TupleSchema *m_primaryKeyIndexSchema;
    PersistentTable *m_table;
    vector<string> m_columnNames;
    vector<ValueType> m_tableSchemaTypes;
    vector<int32_t> m_tableSchemaColumnSizes;
    vector<bool> m_tableSchemaAllowNull;
    vector<ValueType> m_primaryKeyIndexSchemaTypes;
    vector<int32_t> m_primaryKeyIndexSchemaColumnSizes;
    vector<bool> m_primaryKeyIndexSchemaAllowNull;
    vector<int> m_primaryKeyIndexColumns;
};

TEST_F(PersistentTableMemStatsTest, InsertTest) {
    initTable(true);
    tableutil::addRandomTuples(m_table, 10);
    int64_t orig_size = m_table->nonInlinedMemorySize();
    //cout << "Original non-inline size: " << orig_size << endl;

    TableTuple tuple(m_tableSchema);
    tuple.move(new char[tuple.tupleLength()]);
    tableutil::setRandomTupleValues(m_table, &tuple);
    //cout << "Created random tuple " << endl << tuple.debugNoHeader() << endl;
    size_t added_bytes =
        StringRef::computeStringMemoryUsed(ValuePeeker::peekObjectLength(tuple.getNValue(1))) +
        StringRef::computeStringMemoryUsed(ValuePeeker::peekObjectLength(tuple.getNValue(2)));
    //cout << "Allocating string mem for bytes: " << ValuePeeker::peekObjectLength(tuple.getNValue(1)) + sizeof(int32_t) << endl;
    cout << "Adding bytes to table: " << added_bytes << endl;

    m_engine->setUndoToken(INT64_MIN + 2);
    // this next line is a testing hack until engine data is
    // de-duplicated with executorcontext data
    m_engine->getExecutorContext();

    m_table->insertTuple(tuple);

    m_engine->releaseUndoToken(INT64_MIN + 2);

    ASSERT_EQ(orig_size + added_bytes, m_table->nonInlinedMemorySize());

    tuple.freeObjectColumns();
    delete [] tuple.address();
}

TEST_F(PersistentTableMemStatsTest, InsertThenUndoInsertTest) {
    initTable(true);
    tableutil::addRandomTuples(m_table, 10);
    int64_t orig_size = m_table->nonInlinedMemorySize();
    //cout << "Original non-inline size: " << orig_size << endl;

    TableTuple tuple(m_tableSchema);
    tuple.move(new char[tuple.tupleLength()]);
    tableutil::setRandomTupleValues(m_table, &tuple);
    //cout << "Created random tuple " << endl << tuple.debugNoHeader() << endl;
    size_t added_bytes =
        StringRef::computeStringMemoryUsed(ValuePeeker::peekObjectLength(tuple.getNValue(1))) +
        StringRef::computeStringMemoryUsed(ValuePeeker::peekObjectLength(tuple.getNValue(2)));
    //cout << "Adding bytes to table: " << added_bytes << endl;

    m_engine->setUndoToken(INT64_MIN + 2);
    // this next line is a testing hack until engine data is
    // de-duplicated with executorcontext data
    m_engine->getExecutorContext();

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
    initTable(true);
    tableutil::addRandomTuples(m_table, 10);
    int64_t orig_size = m_table->nonInlinedMemorySize();
    //cout << "Original non-inline size: " << orig_size << endl;

    TableTuple tuple(m_tableSchema);
    tableutil::getRandomTuple(m_table, tuple);
    //cout << "Retrieved random tuple " << endl << tuple.debugNoHeader() << endl;

    size_t removed_bytes =
        StringRef::computeStringMemoryUsed(ValuePeeker::peekObjectLength(tuple.getNValue(1))) +
        StringRef::computeStringMemoryUsed(ValuePeeker::peekObjectLength(tuple.getNValue(2)));
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
        StringRef::computeStringMemoryUsed(ValuePeeker::peekObjectLength(tempTuple.getNValue(1))) +
        StringRef::computeStringMemoryUsed(ValuePeeker::peekObjectLength(tempTuple.getNValue(2)));
    //cout << "Adding bytes to table: " << added_bytes << endl;

    m_engine->setUndoToken(INT64_MIN + 2);
    // this next line is a testing hack until engine data is
    // de-duplicated with executorcontext data
    m_engine->getExecutorContext();

    m_table->updateTuple(tempTuple, tuple, true);

    m_engine->releaseUndoToken(INT64_MIN + 2);

    ASSERT_EQ(orig_size + added_bytes - removed_bytes, m_table->nonInlinedMemorySize());

    //cout << "final non-inline size: " << m_table->nonInlinedMemorySize() << endl;
    //tuple.freeObjectColumns();
    //tempTuple.freeObjectColumns();
    //delete [] tuple.address();
    //delete[] tempTuple.address();
    new_string.free();
}

TEST_F(PersistentTableMemStatsTest, UpdateAndUndoTest) {
    initTable(true);
    tableutil::addRandomTuples(m_table, 10);
    int64_t orig_size = m_table->nonInlinedMemorySize();
    //cout << "Original non-inline size: " << orig_size << endl;

    TableTuple tuple(m_tableSchema);
    tableutil::getRandomTuple(m_table, tuple);
    //cout << "Retrieved random tuple " << endl << tuple.debugNoHeader() << endl;

    size_t removed_bytes =
        StringRef::computeStringMemoryUsed(ValuePeeker::peekObjectLength(tuple.getNValue(1))) +
        StringRef::computeStringMemoryUsed(ValuePeeker::peekObjectLength(tuple.getNValue(2)));
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
        StringRef::computeStringMemoryUsed(ValuePeeker::peekObjectLength(tempTuple.getNValue(1))) +
        StringRef::computeStringMemoryUsed(ValuePeeker::peekObjectLength(tempTuple.getNValue(2)));
    //cout << "Adding bytes to table: " << added_bytes << endl;

    m_engine->setUndoToken(INT64_MIN + 2);
    // this next line is a testing hack until engine data is
    // de-duplicated with executorcontext data
    m_engine->getExecutorContext();

    m_table->updateTuple(tempTuple, tuple, true);

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
    initTable(true);
    tableutil::addRandomTuples(m_table, 10);
    int64_t orig_size = m_table->nonInlinedMemorySize();
    //cout << "Original non-inline size: " << orig_size << endl;

    TableTuple tuple(m_tableSchema);
    tableutil::getRandomTuple(m_table, tuple);
    //cout << "Retrieved random tuple " << endl << tuple.debugNoHeader() << endl;

    size_t removed_bytes =
        StringRef::computeStringMemoryUsed(ValuePeeker::peekObjectLength(tuple.getNValue(1))) +
        StringRef::computeStringMemoryUsed(ValuePeeker::peekObjectLength(tuple.getNValue(2)));
    //cout << "Removing bytes from table: " << removed_bytes << endl;

    m_engine->setUndoToken(INT64_MIN + 2);
    // this next line is a testing hack until engine data is
    // de-duplicated with executorcontext data
    m_engine->getExecutorContext();

    m_table->deleteTuple(tuple, true);

    m_engine->releaseUndoToken(INT64_MIN + 2);

    //cout << "Final non-inline size: " << m_table->nonInlinedMemorySize() << endl;
    ASSERT_EQ(orig_size - removed_bytes, m_table->nonInlinedMemorySize());

    //tuple.freeObjectColumns();
    //delete [] tuple.address();
}

TEST_F(PersistentTableMemStatsTest, DeleteAndUndoTest) {
    initTable(true);
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
    m_engine->getExecutorContext();

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
