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

#include <string>
#include <boost/foreach.hpp>
#include <boost/unordered_map.hpp>

#include "harness.h"
#include "common/executorcontext.hpp"
#include "common/TupleSchema.h"
#include "common/debuglog.h"
#include "common/types.h"
#include "common/NValue.hpp"
#include "common/ValueFactory.hpp"
#include "common/tabletuple.h"
#include "storage/BinaryLogSink.h"
#include "storage/persistenttable.h"
#include "storage/tableiterator.h"
#include "storage/table.h"
#include "storage/temptable.h"
#include "storage/tablefactory.h"
#include "storage/tableiterator.h"
#include "storage/DRTupleStream.h"
#include "indexes/tableindex.h"

using namespace std;
using namespace voltdb;

const int COLUMN_COUNT = 9;

static int64_t addPartitionId(int64_t value) {
    return (value << 14) | 42;
}

class DRBinaryLogTest : public Test {
public:
    DRBinaryLogTest() : m_undoToken(0), m_context(new ExecutorContext( 1, 1, NULL, &m_topend, &m_pool, NULL, "localhost", 2, &m_drStream, &m_drReplicatedStream)) {
        m_drStream.m_enabled = true;
        m_drReplicatedStream.m_enabled = true;
        *reinterpret_cast<int64_t*>(tableHandle) = 42;
        *reinterpret_cast<int64_t*>(replicatedTableHandle) = 24;

        std::vector<ValueType> columnTypes;
        std::vector<int32_t> columnLengths;
        std::vector<bool> columnAllowNull(COLUMN_COUNT, true);
        columnTypes.push_back(VALUE_TYPE_TINYINT);   columnLengths.push_back(NValue::getTupleStorageSize(VALUE_TYPE_TINYINT));
        columnTypes.push_back(VALUE_TYPE_BIGINT);    columnLengths.push_back(NValue::getTupleStorageSize(VALUE_TYPE_BIGINT));
        columnTypes.push_back(VALUE_TYPE_DECIMAL);   columnLengths.push_back(NValue::getTupleStorageSize(VALUE_TYPE_DECIMAL));
        columnTypes.push_back(VALUE_TYPE_VARCHAR);   columnLengths.push_back(15);
        columnTypes.push_back(VALUE_TYPE_VARCHAR);   columnLengths.push_back(300);
        columnTypes.push_back(VALUE_TYPE_TIMESTAMP); columnLengths.push_back(NValue::getTupleStorageSize(VALUE_TYPE_TIMESTAMP));

        m_replicatedSchema = TupleSchema::createTupleSchemaForTest(columnTypes, columnLengths, columnAllowNull);
        m_replicatedSchemaReplica = TupleSchema::createTupleSchemaForTest(columnTypes, columnLengths, columnAllowNull);
        columnAllowNull[0] = false;
        m_schema = TupleSchema::createTupleSchemaForTest(columnTypes, columnLengths, columnAllowNull);
        m_schemaReplica = TupleSchema::createTupleSchemaForTest(columnTypes, columnLengths, columnAllowNull);

        string columnNamesArray[COLUMN_COUNT] = {
            "C_TINYINT", "C_SMALLINT", "C_INTEGER", "C_BIGINT", "C_DOUBLE", "C_DECIMAL",
            "C_INLINE_VARCHAR", "C_OUTLINE_VARCHAR", "C_TIMESTAMP" };
        const vector<string> columnNames(columnNamesArray, columnNamesArray + COLUMN_COUNT);

        m_table = reinterpret_cast<PersistentTable*>(voltdb::TableFactory::getPersistentTable(0, "P_TABLE", m_schema, columnNames, tableHandle, false, 0));
        m_tableReplica = reinterpret_cast<PersistentTable*>(voltdb::TableFactory::getPersistentTable(0, "P_TABLE", m_schemaReplica, columnNames, tableHandle, false, 0));
        m_replicatedTable = reinterpret_cast<PersistentTable*>(voltdb::TableFactory::getPersistentTable(0, "R_TABLE", m_replicatedSchema, columnNames, replicatedTableHandle, false, -1));
        m_replicatedTableReplica = reinterpret_cast<PersistentTable*>(voltdb::TableFactory::getPersistentTable(0, "R_TABLE", m_replicatedSchemaReplica, columnNames, replicatedTableHandle, false, -1));

        m_table->setDR(true);
        m_tableReplica->setDR(true);
        m_replicatedTable->setDR(true);
        m_replicatedTableReplica->setDR(true);

        // allocate a new buffer and wrap it
        m_drStream.configure(42);
        m_drReplicatedStream.configure(16383);
    }

    virtual ~DRBinaryLogTest() {
        for (vector<NValue>::const_iterator cit = m_cachedStringValues.begin(); cit != m_cachedStringValues.end(); ++cit) {
            (*cit).free();
        }
        delete m_table;
        delete m_replicatedTable;
        delete m_tableReplica;
        delete m_replicatedTableReplica;
    }

    void beginTxn(int64_t txnId, int64_t spHandle, int64_t lastCommittedSpHandle, int64_t uniqueId) {
        UndoQuantum* uq = m_undoLog.generateUndoQuantum(m_undoToken);
        m_context->setupForPlanFragments(uq, addPartitionId(txnId), addPartitionId(spHandle),
                addPartitionId(lastCommittedSpHandle), addPartitionId(uniqueId));
    }

    void endTxn(bool success) {
        if (!success) {
            m_undoLog.undo(m_undoToken);
        }
        m_undoLog.release(m_undoToken++);
    }

    TableTuple insertTuple(PersistentTable* table, TableTuple temp_tuple) {
        table->insertTuple(temp_tuple);
        TableTuple tuple = table->lookupTupleByValues(temp_tuple);
        assert(!tuple.isNullTuple());
        return tuple;
    }

    TableTuple prepareTempTuple(PersistentTable* table, int8_t tinyint, int64_t bigint, const std::string& decimal,
            const std::string& short_varchar, const std::string& long_varchar, int64_t timestamp) {
        TableTuple temp_tuple = table->tempTuple();
        temp_tuple.setNValue(0, ValueFactory::getTinyIntValue(tinyint));
        temp_tuple.setNValue(1, ValueFactory::getBigIntValue(bigint));
        temp_tuple.setNValue(2, ValueFactory::getDecimalValueFromString(decimal));
        m_cachedStringValues.push_back(ValueFactory::getStringValue(short_varchar));
        temp_tuple.setNValue(3, m_cachedStringValues.back());
        m_cachedStringValues.push_back(ValueFactory::getStringValue(long_varchar));
        temp_tuple.setNValue(4, m_cachedStringValues.back());
        temp_tuple.setNValue(5, ValueFactory::getTimestampValue(timestamp));
        return temp_tuple;
    }

    bool flush(int64_t lastCommittedSpHandle) {
        m_drStream.periodicFlush(-1, addPartitionId(lastCommittedSpHandle));
        m_drReplicatedStream.periodicFlush(-1, addPartitionId(lastCommittedSpHandle));
        return m_topend.receivedDRBuffer;
    }

    void flushAndApply(int64_t lastCommittedSpHandle, bool success = true) {
        ASSERT_TRUE(flush(lastCommittedSpHandle));

        m_context->setupForPlanFragments(m_undoLog.generateUndoQuantum(m_undoToken));
        boost::unordered_map<int64_t, PersistentTable*> tables;
        tables[42] = m_tableReplica;
        tables[24] = m_replicatedTableReplica;

        for (int i = static_cast<int>(m_topend.blocks.size()); i > 0; i--) {
            boost::shared_ptr<StreamBlock> sb = m_topend.blocks[i - 1];
            m_topend.blocks.pop_back();
            boost::shared_array<char> data = m_topend.data[i - 1];
            m_topend.data.pop_back();

            *reinterpret_cast<int32_t*>(&data.get()[4]) = htonl(static_cast<int32_t>(sb->offset()));
            m_drStream.m_enabled = false;
            m_drReplicatedStream.m_enabled = false;
            m_sink.apply(&data[4], tables, &m_pool, NULL);
            m_drStream.m_enabled = true;
            m_drReplicatedStream.m_enabled = true;
        }
        m_topend.receivedDRBuffer = false;
        endTxn(success);
    }


protected:
    DRTupleStream m_drStream;
    DRTupleStream m_drReplicatedStream;

    TupleSchema* m_schema;
    TupleSchema* m_replicatedSchema;
    TupleSchema* m_schemaReplica;
    TupleSchema* m_replicatedSchemaReplica;

    PersistentTable* m_table;
    PersistentTable* m_replicatedTable;
    PersistentTable* m_tableReplica;
    PersistentTable* m_replicatedTableReplica;

    UndoLog m_undoLog;
    int64_t m_undoToken;

    DummyTopend m_topend;
    Pool m_pool;
    BinaryLogSink m_sink;
    boost::scoped_ptr<ExecutorContext> m_context;
    char tableHandle[20];
    char replicatedTableHandle[20];

    vector<NValue> m_cachedStringValues;//To free at the end of the test
};

TEST_F(DRBinaryLogTest, PartitionedTableNoRollbacks) {
    ASSERT_FALSE(flush(98));

    // single row write transaction
    beginTxn(99, 99, 98, 70);
    TableTuple first_tuple = insertTuple(m_table, prepareTempTuple(m_table, 42, 55555, "349508345.34583", "a thing", "a totally different thing altogether", 5433));
    endTxn(true);

    // single row write transaction
    beginTxn(100, 100, 99, 71);
    TableTuple second_tuple = insertTuple(m_table, prepareTempTuple(m_table, 99, 29058, "92384598.2342", "what", "really, why am I writing anything in these?", 3455));
    endTxn(true);

    flushAndApply(100);

    EXPECT_EQ(2, m_tableReplica->activeTupleCount());
    TableTuple tuple = m_tableReplica->lookupTupleByValues(first_tuple);
    ASSERT_FALSE(tuple.isNullTuple());
    tuple = m_tableReplica->lookupTupleByValues(second_tuple);
    ASSERT_FALSE(tuple.isNullTuple());

    // multiple row, multipart write transaction
    beginTxn(111, 101, 100, 72);
    first_tuple = insertTuple(m_table, prepareTempTuple(m_table, 72, 345, "4256.345", "something", "more tuple data, really not the same", 1812));

    // Tick during an ongoing txn -- should not push out a buffer
    ASSERT_FALSE(flush(100));

    second_tuple = insertTuple(m_table, prepareTempTuple(m_table, 7, 234, "23452436.54", "what", "this is starting to get silly", 2342));
    endTxn(true);

    // delete the second row inserted in the last write
    beginTxn(112, 102, 101, 73);
    TableTuple tuple_to_delete = m_table->lookupTupleByValues(second_tuple);
    ASSERT_FALSE(tuple_to_delete.isNullTuple());
    m_table->deleteTuple(tuple_to_delete, true);
    endTxn(true);
    // Tick before the delete
    flushAndApply(101);

    EXPECT_EQ(4, m_tableReplica->activeTupleCount());
    tuple = m_tableReplica->lookupTupleByValues(first_tuple);
    ASSERT_FALSE(tuple.isNullTuple());
    tuple = m_tableReplica->lookupTupleByValues(prepareTempTuple(m_table, 7, 234, "23452436.54", "what", "this is starting to get silly", 2342));
    ASSERT_FALSE(tuple.isNullTuple());

    // Propagate the delete
    flushAndApply(102);
    EXPECT_EQ(3, m_tableReplica->activeTupleCount());
    tuple = m_tableReplica->lookupTupleByValues(first_tuple);
    ASSERT_FALSE(tuple.isNullTuple());
    tuple = m_tableReplica->lookupTupleByValues(second_tuple);
    ASSERT_TRUE(tuple.isNullTuple());

    EXPECT_EQ(3, m_drStream.getLastCommittedSequenceNumberAndUniqueId().first);
    EXPECT_EQ(-1, m_drReplicatedStream.getLastCommittedSequenceNumberAndUniqueId().first);
}

TEST_F(DRBinaryLogTest, PartitionedTableRollbacks) {
    beginTxn(99, 99, 98, 70);
    TableTuple source_tuple = insertTuple(m_table, prepareTempTuple(m_table, 42, 55555, "349508345.34583", "a thing", "a totally different thing altogether", 5433));
    endTxn(false);

    // Intentionally ignore the fact that a rollback wouldn't have actually advanced the
    // lastCommittedSpHandle. Our goal is to tick such that, if data had been produced,
    // it would flush itself out now
    ASSERT_FALSE(flush(99));

    EXPECT_EQ(-1, m_drStream.getLastCommittedSequenceNumberAndUniqueId().first);
    EXPECT_EQ(0, m_tableReplica->activeTupleCount());

    beginTxn(100, 100, 99, 71);
    source_tuple = insertTuple(m_table, prepareTempTuple(m_table, 99, 29058, "92384598.2342", "what", "really, why am I writing anything in these?", 3455));
    endTxn(true);

    // Roll back a txn that hasn't applied any binary log data
    beginTxn(101, 101, 100, 72);
    endTxn(false);

    flushAndApply(101);

    EXPECT_EQ(1, m_tableReplica->activeTupleCount());
    TableTuple tuple = m_tableReplica->lookupTupleByValues(source_tuple);
    ASSERT_FALSE(tuple.isNullTuple());

    EXPECT_EQ(0, m_drStream.getLastCommittedSequenceNumberAndUniqueId().first);
}

TEST_F(DRBinaryLogTest, ReplicatedTableWrites) {
    // write to only the replicated table
    beginTxn(109, 99, 98, 70);
    TableTuple first_tuple = insertTuple(m_replicatedTable, prepareTempTuple(m_replicatedTable, 42, 55555, "349508345.34583", "a thing", "a totally different thing altogether", 5433));
    endTxn(true);

    flushAndApply(99);

    EXPECT_EQ(0, m_tableReplica->activeTupleCount());
    EXPECT_EQ(1, m_replicatedTableReplica->activeTupleCount());
    TableTuple tuple = m_replicatedTableReplica->lookupTupleByValues(first_tuple);
    ASSERT_FALSE(tuple.isNullTuple());

    // write to both the partitioned and replicated table
    beginTxn(110, 100, 99, 71);
    first_tuple = insertTuple(m_table, prepareTempTuple(m_table, 72, 345, "4256.345", "something", "more tuple data, really not the same", 1812));
    TableTuple second_tuple = insertTuple(m_replicatedTable, prepareTempTuple(m_replicatedTable, 7, 234, "23452436.54", "what", "this is starting to get silly", 2342));
    endTxn(true);

    flushAndApply(100);

    EXPECT_EQ(1, m_tableReplica->activeTupleCount());
    EXPECT_EQ(2, m_replicatedTableReplica->activeTupleCount());
    tuple = m_tableReplica->lookupTupleByValues(first_tuple);
    ASSERT_FALSE(tuple.isNullTuple());
    tuple = m_replicatedTableReplica->lookupTupleByValues(second_tuple);
    ASSERT_FALSE(tuple.isNullTuple());

    // write to the partitioned and replicated table and roll it back
    beginTxn(111, 101, 100, 72);
    first_tuple = insertTuple(m_table, prepareTempTuple(m_table, 11, 34534, "3453.4545", "another", "blah blah blah blah blah blah", 2344));
    second_tuple = insertTuple(m_replicatedTable, prepareTempTuple(m_replicatedTable, 24, 2321, "23455.5554", "and another", "this is starting to get even sillier", 2222));
    endTxn(false);

    ASSERT_FALSE(flush(101));

    // one more write to the replicated table for good measure
    beginTxn(112, 102, 101, 73);
    second_tuple = insertTuple(m_replicatedTable, prepareTempTuple(m_replicatedTable, 99, 29058, "92384598.2342", "what", "really, why am I writing anything in these?", 3455));
    endTxn(true);

    flushAndApply(102);
    EXPECT_EQ(1, m_tableReplica->activeTupleCount());
    EXPECT_EQ(3, m_replicatedTableReplica->activeTupleCount());
    tuple = m_replicatedTableReplica->lookupTupleByValues(second_tuple);
    ASSERT_FALSE(tuple.isNullTuple());

    EXPECT_EQ(0, m_drStream.getLastCommittedSequenceNumberAndUniqueId().first);
    EXPECT_EQ(2, m_drReplicatedStream.getLastCommittedSequenceNumberAndUniqueId().first);
}

TEST_F(DRBinaryLogTest, SerializeNulls) {
    beginTxn(109, 99, 98, 70);
    TableTuple temp_tuple = m_replicatedTable->tempTuple();
    temp_tuple.setNValue(0, NValue::getNullValue(VALUE_TYPE_TINYINT));
    temp_tuple.setNValue(1, ValueFactory::getBigIntValue(489735));
    temp_tuple.setNValue(2, NValue::getNullValue(VALUE_TYPE_DECIMAL));
    m_cachedStringValues.push_back(ValueFactory::getStringValue("whatever"));
    temp_tuple.setNValue(3, m_cachedStringValues.back());
    temp_tuple.setNValue(4, ValueFactory::getNullStringValue());
    temp_tuple.setNValue(5, ValueFactory::getTimestampValue(3495));
    TableTuple first_tuple = insertTuple(m_replicatedTable, temp_tuple);

    temp_tuple = m_replicatedTable->tempTuple();
    temp_tuple.setNValue(0, ValueFactory::getTinyIntValue(42));
    temp_tuple.setNValue(1, NValue::getNullValue(VALUE_TYPE_BIGINT));
    temp_tuple.setNValue(2, ValueFactory::getDecimalValueFromString("234234.243"));
    temp_tuple.setNValue(3, ValueFactory::getNullStringValue());
    m_cachedStringValues.push_back(ValueFactory::getStringValue("whatever and ever and ever and ever"));
    temp_tuple.setNValue(4, m_cachedStringValues.back());
    temp_tuple.setNValue(5, NValue::getNullValue(VALUE_TYPE_TIMESTAMP));
    TableTuple second_tuple = insertTuple(m_replicatedTable, temp_tuple);
    endTxn(true);

    flushAndApply(99);

    EXPECT_EQ(2, m_replicatedTableReplica->activeTupleCount());
    TableTuple tuple = m_replicatedTableReplica->lookupTupleByValues(first_tuple);
    ASSERT_FALSE(tuple.isNullTuple());
    tuple = m_replicatedTableReplica->lookupTupleByValues(second_tuple);
    ASSERT_FALSE(tuple.isNullTuple());
}

TEST_F(DRBinaryLogTest, RollbackNulls) {
    beginTxn(109, 99, 98, 70);
    TableTuple temp_tuple = m_replicatedTable->tempTuple();
    temp_tuple.setNValue(0, NValue::getNullValue(VALUE_TYPE_TINYINT));
    temp_tuple.setNValue(1, ValueFactory::getBigIntValue(489735));
    temp_tuple.setNValue(2, NValue::getNullValue(VALUE_TYPE_DECIMAL));
    m_cachedStringValues.push_back(ValueFactory::getStringValue("whatever"));
    temp_tuple.setNValue(3, m_cachedStringValues.back());
    temp_tuple.setNValue(4, ValueFactory::getNullStringValue());
    temp_tuple.setNValue(5, ValueFactory::getTimestampValue(3495));
    insertTuple(m_replicatedTable, temp_tuple);
    endTxn(false);

    beginTxn(110, 100, 99, 71);
    TableTuple source_tuple = insertTuple(m_replicatedTable, prepareTempTuple(m_replicatedTable, 99, 29058, "92384598.2342", "what", "really, why am I writing anything in these?", 3455));
    endTxn(true);

    flushAndApply(100);

    EXPECT_EQ(1, m_replicatedTableReplica->activeTupleCount());
    TableTuple tuple = m_replicatedTableReplica->lookupTupleByValues(source_tuple);
    ASSERT_FALSE(tuple.isNullTuple());
}

TEST_F(DRBinaryLogTest, RollbackOnReplica) {
    // single row write transaction
    beginTxn(99, 99, 98, 70);
    insertTuple(m_table, prepareTempTuple(m_table, 42, 55555, "349508345.34583", "a thing", "a totally different thing altogether", 5433));
    endTxn(true);

    // try and fail to apply this on the replica
    flushAndApply(99, false);

    EXPECT_EQ(0, m_tableReplica->activeTupleCount());

    // successfully apply some data for, I don't know, verisimilitude?
    beginTxn(100, 100, 99, 71);
    TableTuple source_tuple = insertTuple(m_table, prepareTempTuple(m_table, 99, 29058, "92384598.2342", "what", "really, why am I writing anything in these?", 3455));
    endTxn(true);

    flushAndApply(100);

    EXPECT_EQ(1, m_tableReplica->activeTupleCount());
    TableTuple tuple = m_tableReplica->lookupTupleByValues(source_tuple);
    ASSERT_FALSE(tuple.isNullTuple());

    // inserts followed by some deletes
    beginTxn(101, 101, 100, 72);
    TableTuple first_tuple = insertTuple(m_table, prepareTempTuple(m_table, 11, 34534, "3453.4545", "another", "blah blah blah blah blah blah", 2344));
    TableTuple second_tuple = insertTuple(m_table, prepareTempTuple(m_table, 24, 2321, "23455.5554", "and another", "this is starting to get even sillier", 2222));
    insertTuple(m_table, prepareTempTuple(m_table, 72, 345, "4256.345", "something", "more tuple data, really not the same", 1812));
    TableTuple temp_tuple = m_table->lookupTupleByValues(first_tuple);
    ASSERT_FALSE(temp_tuple.isNullTuple());
    m_table->deleteTuple(temp_tuple, true);
    temp_tuple = m_table->lookupTupleByValues(second_tuple);
    ASSERT_FALSE(temp_tuple.isNullTuple());
    m_table->deleteTuple(temp_tuple, true);
    endTxn(true);

    flushAndApply(101, false);

    EXPECT_EQ(1, m_tableReplica->activeTupleCount());
    tuple = m_tableReplica->lookupTupleByValues(source_tuple);
    ASSERT_FALSE(tuple.isNullTuple());
}

int main() {
    return TestSuite::globalInstance()->runAll();
}
