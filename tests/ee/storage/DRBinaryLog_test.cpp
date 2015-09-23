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
#include "execution/VoltDBEngine.h"
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

const int COLUMN_COUNT = 6;
const int HIDDEN_COLUMN_COUNT = 1;

static int64_t addPartitionId(int64_t value) {
    return (value << 14) | 42;
}

class MockExportTupleStream : public ExportTupleStream {
public:
    MockExportTupleStream(CatalogId partitionId, int64_t siteId) : ExportTupleStream(partitionId, siteId) {}
    virtual size_t appendTuple(int64_t lastCommittedSpHandle,
                                           int64_t spHandle,
                                           int64_t seqNo,
                                           int64_t uniqueId,
                                           int64_t timestamp,
                                           TableTuple &tuple,
                                           ExportTupleStream::Type type) {
        receivedTuples.push_back(tuple);
        return 0;
    }
    std::vector<TableTuple> receivedTuples;
};

class DRBinaryLogTest : public Test {
public:
    DRBinaryLogTest()
      : m_undoToken(0)
      , m_context(new ExecutorContext(1, 1, NULL, &m_topend, &m_pool,
            NULL, NULL, "localhost", 2, &m_drStream, &m_drReplicatedStream, 0))
    {
        m_drStream.m_enabled = true;
        m_drReplicatedStream.m_enabled = true;
        *reinterpret_cast<int64_t*>(tableHandle) = 42;
        *reinterpret_cast<int64_t*>(replicatedTableHandle) = 24;
        *reinterpret_cast<int64_t*>(otherTableHandleWithIndex) = 43;
        *reinterpret_cast<int64_t*>(otherTableHandleWithoutIndex) = 44;
        *reinterpret_cast<int64_t*>(exportTableHandle) = 55;

        std::vector<ValueType> columnTypes;
        std::vector<int32_t> columnLengths;
        std::vector<bool> columnAllowNull(COLUMN_COUNT, true);
        const std::vector<bool> columnInBytes (columnAllowNull.size(), false);

        columnTypes.push_back(VALUE_TYPE_TINYINT);   columnLengths.push_back(NValue::getTupleStorageSize(VALUE_TYPE_TINYINT));
        columnTypes.push_back(VALUE_TYPE_BIGINT);    columnLengths.push_back(NValue::getTupleStorageSize(VALUE_TYPE_BIGINT));
        columnTypes.push_back(VALUE_TYPE_DECIMAL);   columnLengths.push_back(NValue::getTupleStorageSize(VALUE_TYPE_DECIMAL));
        columnTypes.push_back(VALUE_TYPE_VARCHAR);   columnLengths.push_back(15);
        columnTypes.push_back(VALUE_TYPE_VARCHAR);   columnLengths.push_back(300);
        columnTypes.push_back(VALUE_TYPE_TIMESTAMP); columnLengths.push_back(NValue::getTupleStorageSize(VALUE_TYPE_TIMESTAMP));

        std::vector<ValueType> hiddenTypes;
        std::vector<int32_t> hiddenColumnLengths;
        std::vector<bool> hiddenColumnAllowNull(HIDDEN_COLUMN_COUNT, false);
        const std::vector<bool> hiddenColumnInBytes (hiddenColumnAllowNull.size(), false);

        hiddenTypes.push_back(VALUE_TYPE_BIGINT);    hiddenColumnLengths.push_back(NValue::getTupleStorageSize(VALUE_TYPE_BIGINT));


        m_replicatedSchema = TupleSchema::createTupleSchema(columnTypes, columnLengths, columnAllowNull, columnInBytes, hiddenTypes, hiddenColumnLengths, hiddenColumnAllowNull, hiddenColumnInBytes);
        m_replicatedSchemaReplica = TupleSchema::createTupleSchema(columnTypes, columnLengths, columnAllowNull, columnInBytes, hiddenTypes, hiddenColumnLengths, hiddenColumnAllowNull, hiddenColumnInBytes);
        columnAllowNull[0] = false;
        m_schema = TupleSchema::createTupleSchema(columnTypes, columnLengths, columnAllowNull, columnInBytes, hiddenTypes, hiddenColumnLengths, hiddenColumnAllowNull, hiddenColumnInBytes);
        m_schemaReplica = TupleSchema::createTupleSchema(columnTypes, columnLengths, columnAllowNull, columnInBytes, hiddenTypes, hiddenColumnLengths, hiddenColumnAllowNull, hiddenColumnInBytes);

        string columnNamesArray[COLUMN_COUNT] = {
            "C_TINYINT", "C_BIGINT", "C_DECIMAL",
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

        std::vector<ValueType> otherColumnTypes;
        std::vector<int32_t> otherColumnLengths;
        std::vector<bool> otherColumnAllowNull(2, false);
        otherColumnTypes.push_back(VALUE_TYPE_TINYINT); otherColumnLengths.push_back(NValue::getTupleStorageSize(VALUE_TYPE_TINYINT));
        otherColumnTypes.push_back(VALUE_TYPE_BIGINT);  otherColumnLengths.push_back(NValue::getTupleStorageSize(VALUE_TYPE_BIGINT));

        m_otherSchemaWithIndex = TupleSchema::createTupleSchemaForTest(otherColumnTypes, otherColumnLengths, otherColumnAllowNull);
        m_otherSchemaWithoutIndex = TupleSchema::createTupleSchemaForTest(otherColumnTypes, otherColumnLengths, otherColumnAllowNull);
        m_otherSchemaWithIndexReplica = TupleSchema::createTupleSchemaForTest(otherColumnTypes, otherColumnLengths, otherColumnAllowNull);
        m_otherSchemaWithoutIndexReplica = TupleSchema::createTupleSchemaForTest(otherColumnTypes, otherColumnLengths, otherColumnAllowNull);

        string otherColumnNamesArray[2] = { "C_TINYINT", "C_BIGINT" };
        const vector<string> otherColumnNames(otherColumnNamesArray, otherColumnNamesArray + 2);

        m_otherTableWithIndex = reinterpret_cast<PersistentTable*>(voltdb::TableFactory::getPersistentTable(0, "OTHER_TABLE_1", m_otherSchemaWithIndex, otherColumnNames, otherTableHandleWithIndex, false, 0));
        m_otherTableWithoutIndex = reinterpret_cast<PersistentTable*>(voltdb::TableFactory::getPersistentTable(0, "OTHER_TABLE_2", m_otherSchemaWithoutIndex, otherColumnNames, otherTableHandleWithoutIndex, false, 0));
        m_otherTableWithIndexReplica = reinterpret_cast<PersistentTable*>(voltdb::TableFactory::getPersistentTable(0, "OTHER_TABLE_1", m_otherSchemaWithIndexReplica, otherColumnNames, otherTableHandleWithIndex, false, 0));
        m_otherTableWithoutIndexReplica = reinterpret_cast<PersistentTable*>(voltdb::TableFactory::getPersistentTable(0, "OTHER_TABLE_2", m_otherSchemaWithoutIndexReplica, otherColumnNames, otherTableHandleWithoutIndex, false, 0));

        vector<int> columnIndices(1, 0);
        TableIndexScheme scheme = TableIndexScheme("the_index", HASH_TABLE_INDEX,
                                                   columnIndices, TableIndex::simplyIndexColumns(),
                                                   true, true, m_otherSchemaWithIndex);
        TableIndex *index = TableIndexFactory::getInstance(scheme);
        m_otherTableWithIndex->addIndex(index);
        scheme = TableIndexScheme("the_index", HASH_TABLE_INDEX,
                                  columnIndices, TableIndex::simplyIndexColumns(),
                                  true, true, m_otherSchemaWithIndexReplica);
        TableIndex *replicaIndex = TableIndexFactory::getInstance(scheme);
        m_otherTableWithIndexReplica->addIndex(replicaIndex);

        m_otherTableWithIndex->setDR(true);
        m_otherTableWithoutIndex->setDR(true);
        m_otherTableWithIndexReplica->setDR(true);
        m_otherTableWithoutIndexReplica->setDR(true);

        // allocate a new buffer and wrap it
        m_drStream.configure(42);
        m_drReplicatedStream.configure(16383);

        // create a table with different schema only on the master
        std::vector<ValueType> singleColumnType;
        std::vector<int32_t> singleColumnLength;
        std::vector<bool> singleColumnAllowNull(1, false);
        singleColumnType.push_back(VALUE_TYPE_TINYINT); singleColumnLength.push_back(NValue::getTupleStorageSize(VALUE_TYPE_TINYINT));
        m_singleColumnSchema = TupleSchema::createTupleSchemaForTest(singleColumnType, singleColumnLength, singleColumnAllowNull);
        string singleColumnNameArray[1] = { "NOTHING" };
        const vector<string> singleColumnName(singleColumnNameArray, singleColumnNameArray + 1);

        m_singleColumnTable = reinterpret_cast<PersistentTable*>(voltdb::TableFactory::getPersistentTable(0, "P_SINGLE_COLUMN_TABLE",
                                                                                                          m_singleColumnSchema,
                                                                                                          singleColumnName,
                                                                                                          tableHandle + 1, false, 0));
        m_singleColumnTable->setDR(true);

        // create a export table has the same column of other_table_2, plus 4 additional columns for DR conflict resolution purpose.
        std::vector<ValueType> exportColumnType;
        std::vector<int32_t> exportColumnLength;
        std::vector<bool> exportColumnAllowNull(10, false);
        exportColumnType.push_back(VALUE_TYPE_VARCHAR);     exportColumnLength.push_back(7); /* length of 'P_TABLE' */
        exportColumnType.push_back(VALUE_TYPE_TINYINT);     exportColumnLength.push_back(NValue::getTupleStorageSize(VALUE_TYPE_TINYINT));
        exportColumnType.push_back(VALUE_TYPE_BIGINT);      exportColumnLength.push_back(NValue::getTupleStorageSize(VALUE_TYPE_BIGINT));
        exportColumnType.push_back(VALUE_TYPE_TINYINT);     exportColumnLength.push_back(NValue::getTupleStorageSize(VALUE_TYPE_TINYINT));
        exportColumnType.push_back(VALUE_TYPE_TINYINT);     exportColumnLength.push_back(NValue::getTupleStorageSize(VALUE_TYPE_TINYINT));
        exportColumnType.push_back(VALUE_TYPE_BIGINT);      exportColumnLength.push_back(NValue::getTupleStorageSize(VALUE_TYPE_BIGINT));
        exportColumnType.push_back(VALUE_TYPE_DECIMAL);     exportColumnLength.push_back(NValue::getTupleStorageSize(VALUE_TYPE_DECIMAL));
        exportColumnType.push_back(VALUE_TYPE_VARCHAR);     exportColumnLength.push_back(15);
        exportColumnType.push_back(VALUE_TYPE_VARCHAR);     exportColumnLength.push_back(300);
        exportColumnType.push_back(VALUE_TYPE_TIMESTAMP);   exportColumnLength.push_back(NValue::getTupleStorageSize(VALUE_TYPE_TIMESTAMP));

        m_exportSchema = TupleSchema::createTupleSchemaForTest(exportColumnType, exportColumnLength, exportColumnAllowNull);
        string exportColumnNamesArray[10] = { "VOLTDB_AUTOGEN_TABLE_NAME", "VOLTDB_AUTOGEN_CLUSTER_ID", "VOLTDB_AUTOGEN_TIMESTAMP",
                                            "VOLTDB_AUTOGEN_OPERATION_TYPE", "C_TINYINT", "C_BIGINT", "C_DECIMAL",
                                            "C_INLINE_VARCHAR", "C_OUTLINE_VARCHAR", "C_TIMESTAMP"};
        const vector<string> exportColumnName(exportColumnNamesArray, exportColumnNamesArray + 10);

        m_exportTable = voltdb::TableFactory::getPersistentTable(0, "VOLTDB_AUTOGEN_DR_CONFLICTS__P_TABLE",
                                                                 m_exportSchema, exportColumnName,
                                                                 exportTableHandle, false, 0, true, true);
        m_exportTable->setExportStreamWrapper(new MockExportTupleStream(1, 1));
    }

    virtual ~DRBinaryLogTest() {
        for (vector<NValue>::const_iterator cit = m_cachedStringValues.begin(); cit != m_cachedStringValues.end(); ++cit) {
            (*cit).free();
        }
        delete m_table;
        delete m_replicatedTable;
        delete m_tableReplica;
        delete m_replicatedTableReplica;
        delete m_singleColumnTable;
        delete m_otherTableWithIndex;
        delete m_otherTableWithoutIndex;
        delete m_otherTableWithIndexReplica;
        delete m_otherTableWithoutIndexReplica;
        delete m_exportTable;
    }

    void beginTxn(int64_t txnId, int64_t spHandle, int64_t lastCommittedSpHandle, int64_t uniqueId) {
        UndoQuantum* uq = m_undoLog.generateUndoQuantum(m_undoToken);
        m_context->setupForPlanFragments(uq, addPartitionId(txnId), addPartitionId(spHandle),
                addPartitionId(lastCommittedSpHandle), addPartitionId(uniqueId));
    }

    void endTxn(bool success) {
        if (!success) {
            m_undoLog.undo(m_undoToken);
        } else {
            m_undoLog.release(m_undoToken++);
            m_drStream.endTransaction();
            m_drReplicatedStream.endTransaction();
        }
    }

    TableTuple insertTuple(PersistentTable* table, TableTuple temp_tuple) {
        table->insertTuple(temp_tuple);
        TableTuple tuple = table->lookupTupleByValues(temp_tuple);
        assert(!tuple.isNullTuple());
        return tuple;
    }

    void deleteTuple(PersistentTable* table, TableTuple tuple) {
        TableTuple tuple_to_delete = table->lookupTupleByValues(tuple);
        ASSERT_FALSE(tuple_to_delete.isNullTuple());
        table->deleteTuple(tuple_to_delete, true);
    }

    void updateTuple(PersistentTable* table, TableTuple tuple, int8_t new_index_value, const std::string& new_nonindex_value) {
        TableTuple tuple_to_update = table->lookupTupleByValues(tuple);
        ASSERT_FALSE(tuple_to_update.isNullTuple());
        TableTuple new_tuple = table->tempTuple();
        new_tuple.copy(tuple_to_update);
        new_tuple.setNValue(0, ValueFactory::getTinyIntValue(new_index_value));
        m_cachedStringValues.push_back(ValueFactory::getStringValue(new_nonindex_value));
        new_tuple.setNValue(3, m_cachedStringValues.back());
        table->updateTuple(tuple_to_update, new_tuple);
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
        tables[43] = m_otherTableWithIndexReplica;
        tables[44] = m_otherTableWithoutIndexReplica;
        tables[24] = m_replicatedTableReplica;

        for (int i = static_cast<int>(m_topend.blocks.size()); i > 0; i--) {
            boost::shared_ptr<StreamBlock> sb = m_topend.blocks[i - 1];
            m_topend.blocks.pop_back();
            boost::shared_array<char> data = m_topend.data[i - 1];
            m_topend.data.pop_back();

            size_t startPos = sb->headerSize() - 4;
            *reinterpret_cast<int32_t*>(&data.get()[startPos]) = htonl(static_cast<int32_t>(sb->offset()));
            m_drStream.m_enabled = false;
            m_drReplicatedStream.m_enabled = false;
            m_sink.apply(&data[startPos], tables, &m_pool, NULL);
            m_drStream.m_enabled = true;
            m_drReplicatedStream.m_enabled = true;
        }
        m_topend.receivedDRBuffer = false;
        endTxn(success);
    }

    void createIndexes() {
        vector<int> firstColumnIndices;
        firstColumnIndices.push_back(1); // BIGINT
        firstColumnIndices.push_back(0); // TINYINT
        TableIndexScheme scheme = TableIndexScheme("first_unique_index", HASH_TABLE_INDEX,
                                                   firstColumnIndices, TableIndex::simplyIndexColumns(),
                                                   true, true, m_schema);
        TableIndex *firstIndex = TableIndexFactory::getInstance(scheme);
        scheme = TableIndexScheme("first_unique_index", HASH_TABLE_INDEX,
                                  firstColumnIndices, TableIndex::simplyIndexColumns(),
                                  true, true, m_schemaReplica);
        TableIndex *firstReplicaIndex = TableIndexFactory::getInstance(scheme);

        vector<int> secondColumnIndices;
        secondColumnIndices.push_back(0); // TINYINT
        secondColumnIndices.push_back(1); // BIGINT
        secondColumnIndices.push_back(4); // non-inline VARCHAR
        scheme = TableIndexScheme("second_unique_index", HASH_TABLE_INDEX,
                                  secondColumnIndices, TableIndex::simplyIndexColumns(),
                                  true, true, m_schema);
        TableIndex *secondIndex = TableIndexFactory::getInstance(scheme);
        scheme = TableIndexScheme("second_unique_index", HASH_TABLE_INDEX,
                                  secondColumnIndices, TableIndex::simplyIndexColumns(),
                                  true, true, m_schemaReplica);
        TableIndex *secondReplicaIndex = TableIndexFactory::getInstance(scheme);

        m_table->addIndex(firstIndex);
        m_tableReplica->addIndex(secondReplicaIndex);
        m_table->addIndex(secondIndex);
        m_tableReplica->addIndex(firstReplicaIndex);

        // smaller, non-unique, only on master
        vector<int> thirdColumnIndices(1, 0);
        scheme = TableIndexScheme("third_index", HASH_TABLE_INDEX,
                                  secondColumnIndices, TableIndex::simplyIndexColumns(),
                                  false, false, m_schema);
        TableIndex *thirdIndex = TableIndexFactory::getInstance(scheme);
        m_table->addIndex(thirdIndex);
    }

    void simpleDeleteTest() {
        std::pair<const TableIndex*, uint32_t> indexPair = m_table->getSmallestUniqueIndex();
        std::pair<const TableIndex*, uint32_t> indexPairReplica = m_tableReplica->getSmallestUniqueIndex();
        ASSERT_FALSE(indexPair.first == NULL);
        ASSERT_FALSE(indexPairReplica.first == NULL);
        EXPECT_EQ(indexPair.second, indexPairReplica.second);

        beginTxn(99, 99, 98, 70);
        TableTuple first_tuple = insertTuple(m_table, prepareTempTuple(m_table, 42, 55555, "349508345.34583", "a thing", "a totally different thing altogether", 5433));
        TableTuple second_tuple = insertTuple(m_table, prepareTempTuple(m_table, 24, 2321, "23455.5554", "and another", "this is starting to get even sillier", 2222));
        TableTuple third_tuple = insertTuple(m_table, prepareTempTuple(m_table, 72, 345, "4256.345", "something", "more tuple data, really not the same", 1812));
        endTxn(true);

        flushAndApply(99);

        EXPECT_EQ(3, m_tableReplica->activeTupleCount());

        beginTxn(100, 100, 99, 71);
        deleteTuple(m_table, first_tuple);
        deleteTuple(m_table, second_tuple);
        endTxn(true);

        flushAndApply(100);

        EXPECT_EQ(1, m_tableReplica->activeTupleCount());
        TableTuple tuple = m_tableReplica->lookupTupleByValues(third_tuple);
        ASSERT_FALSE(tuple.isNullTuple());
    }

    void simpleUpdateTest() {
        beginTxn(99, 99, 98, 70);
        TableTuple first_tuple = insertTuple(m_table, prepareTempTuple(m_table, 42, 55555, "349508345.34583", "a thing", "a totally different thing altogether", 5433));
        TableTuple second_tuple = insertTuple(m_table, prepareTempTuple(m_table, 24, 2321, "23455.5554", "and another", "this is starting to get even sillier", 2222));
        endTxn(true);

        flushAndApply(99);

        EXPECT_EQ(2, m_tableReplica->activeTupleCount());

        beginTxn(100, 100, 99, 71);
        // update the non-index column only
        updateTuple(m_table, first_tuple, 42, "not that");
        endTxn(true);

        flushAndApply(100);

        EXPECT_EQ(2, m_tableReplica->activeTupleCount());
        TableTuple expected_tuple = prepareTempTuple(m_table, 42, 55555, "349508345.34583", "not that", "a totally different thing altogether", 5433);
        TableTuple tuple = m_tableReplica->lookupTupleByValues(expected_tuple);
        ASSERT_FALSE(tuple.isNullTuple());
        tuple = m_table->lookupTupleByValues(second_tuple);
        ASSERT_FALSE(tuple.isNullTuple());

        beginTxn(101, 101, 100, 72);
        // update the index column only
        updateTuple(m_table, second_tuple, 99, "and another");
        endTxn(true);

        flushAndApply(101);

        EXPECT_EQ(2, m_tableReplica->activeTupleCount());
        tuple = m_tableReplica->lookupTupleByValues(expected_tuple);
        ASSERT_FALSE(tuple.isNullTuple());
        expected_tuple = prepareTempTuple(m_table, 99, 2321, "23455.5554", "and another", "this is starting to get even sillier", 2222);
        tuple = m_table->lookupTupleByValues(second_tuple);
        ASSERT_FALSE(tuple.isNullTuple());
    }

protected:
    DRTupleStream m_drStream;
    DRTupleStream m_drReplicatedStream;

    TupleSchema* m_schema;
    TupleSchema* m_replicatedSchema;
    TupleSchema* m_schemaReplica;
    TupleSchema* m_replicatedSchemaReplica;
    TupleSchema* m_otherSchemaWithIndex;
    TupleSchema* m_otherSchemaWithoutIndex;
    TupleSchema* m_otherSchemaWithIndexReplica;
    TupleSchema* m_otherSchemaWithoutIndexReplica;
    TupleSchema* m_singleColumnSchema;
    TupleSchema* m_exportSchema;

    PersistentTable* m_table;
    PersistentTable* m_replicatedTable;
    PersistentTable* m_tableReplica;
    PersistentTable* m_replicatedTableReplica;
    PersistentTable* m_otherTableWithIndex;
    PersistentTable* m_otherTableWithoutIndex;
    PersistentTable* m_otherTableWithIndexReplica;
    PersistentTable* m_otherTableWithoutIndexReplica;
    // This table does not exist on the replica
    PersistentTable* m_singleColumnTable;
    Table* m_exportTable;

    UndoLog m_undoLog;
    int64_t m_undoToken;

    DummyTopend m_topend;
    Pool m_pool;
    BinaryLogSink m_sink;
    boost::scoped_ptr<ExecutorContext> m_context;
    char tableHandle[20];
    char replicatedTableHandle[20];
    char otherTableHandleWithIndex[20];
    char otherTableHandleWithoutIndex[20];
    char exportTableHandle[20];

    vector<NValue> m_cachedStringValues;//To free at the end of the test
};

TEST_F(DRBinaryLogTest, VerifyHiddenColumns) {
    ASSERT_FALSE(flush(98));

    // single row write transaction
    beginTxn(99, 99, 98, 70);
    TableTuple first_tuple = insertTuple(m_table, prepareTempTuple(m_table, 42, 55555, "349508345.34583", "a thing", "a totally different thing altogether", 5433));
    endTxn(true);

    flushAndApply(99);

    TableTuple tuple = m_tableReplica->lookupTupleByValues(first_tuple);
    NValue drTimestamp = tuple.getHiddenNValue(m_table->getDRTimestampColumnIndex());
    NValue drTimestampReplica = tuple.getHiddenNValue(m_tableReplica->getDRTimestampColumnIndex());
    EXPECT_EQ(ValuePeeker::peekAsBigInt(drTimestamp), 70);
    EXPECT_EQ(0, drTimestamp.compare(drTimestampReplica));
}

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
    deleteTuple(m_table, second_tuple);
    // Tick before the delete
    ASSERT_TRUE(flush(101));
    endTxn(true);
    // Apply the binary log after endTxn() to get a valid undoToken.
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

// TODO: when we implement conflict detection, the testcase should rewrite to manually create a conflict
TEST_F(DRBinaryLogTest, WriteDRConflictToExportTable) {
    ASSERT_FALSE(flush(98));

    // single row write transaction
    beginTxn(99, 99, 98, 70);
    TableTuple first_tuple = insertTuple(m_table, prepareTempTuple(m_table, 42, 55555, "349508345.34583", "a thing", "a totally different thing altogether", 5433));
    m_sink.exportDRConflict(m_table, m_exportTable, DR_RECORD_INSERT, first_tuple);
    endTxn(true);

    EXPECT_EQ(1, reinterpret_cast<MockExportTupleStream*>(m_exportTable->getExportStreamWrapper())->receivedTuples.size());
    TableTuple received_tuple = reinterpret_cast<MockExportTupleStream*>(m_exportTable->getExportStreamWrapper())->receivedTuples[0];
    ASSERT_FALSE(received_tuple.isNullTuple());
    ASSERT_TRUE(ValuePeeker::peekStringCopy_withoutNull(received_tuple.getNValue(0)).compare("P_TABLE") == 0);
    ASSERT_TRUE(ValuePeeker::peekTinyInt(received_tuple.getNValue(1)) == 0);
    ASSERT_TRUE(ValuePeeker::peekBigInt(received_tuple.getNValue(2)) == 0);
    ASSERT_TRUE(ValuePeeker::peekTinyInt(received_tuple.getNValue(3)) == DR_RECORD_INSERT);
    // Now compare the rest of columns
    ASSERT_TRUE(first_tuple.getNValue(0).op_equals(received_tuple.getNValue(4)).isTrue());
    ASSERT_TRUE(first_tuple.getNValue(1).op_equals(received_tuple.getNValue(5)).isTrue());
    ASSERT_TRUE(first_tuple.getNValue(2).op_equals(received_tuple.getNValue(6)).isTrue());
    ASSERT_TRUE(first_tuple.getNValue(3).op_equals(received_tuple.getNValue(7)).isTrue());
    ASSERT_TRUE(first_tuple.getNValue(4).op_equals(received_tuple.getNValue(8)).isTrue());
    ASSERT_TRUE(first_tuple.getNValue(5).op_equals(received_tuple.getNValue(9)).isTrue());
}


TEST_F(DRBinaryLogTest, PartitionedTableRollbacks) {
    m_singleColumnTable->setDR(false);

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
    TableTuple temp_tuple = m_singleColumnTable->tempTuple();
    temp_tuple.setNValue(0, ValueFactory::getTinyIntValue(1));
    insertTuple(m_singleColumnTable, temp_tuple);
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
    deleteTuple(m_table, first_tuple);
    deleteTuple(m_table, second_tuple);
    endTxn(true);

    flushAndApply(101, false);

    EXPECT_EQ(1, m_tableReplica->activeTupleCount());
    tuple = m_tableReplica->lookupTupleByValues(source_tuple);
    ASSERT_FALSE(tuple.isNullTuple());
}

TEST_F(DRBinaryLogTest, CantFindTable) {
    beginTxn(99, 99, 98, 70);
    TableTuple temp_tuple = m_singleColumnTable->tempTuple();
    temp_tuple.setNValue(0, ValueFactory::getTinyIntValue(1));
    insertTuple(m_singleColumnTable, temp_tuple);
    endTxn(true);

    // try and fail to apply this on the replica because the table cannot be found.
    // should not throw fatal exception.
    try {
        flushAndApply(99, false);
    } catch (SerializableEEException &e) {
        endTxn(false);
    } catch (...) {
        ASSERT_TRUE(false);
    }
}

TEST_F(DRBinaryLogTest, DeleteWithUniqueIndex) {
    createIndexes();
    simpleDeleteTest();
}

TEST_F(DRBinaryLogTest, DeleteWithUniqueIndexMultipleTables) {
    createIndexes();

    std::pair<const TableIndex*, uint32_t> indexPair1 = m_otherTableWithIndex->getSmallestUniqueIndex();
    std::pair<const TableIndex*, uint32_t> indexPair2 = m_otherTableWithoutIndex->getSmallestUniqueIndex();
    ASSERT_FALSE(indexPair1.first == NULL);
    ASSERT_TRUE(indexPair2.first == NULL);

    beginTxn(99, 99, 98, 70);
    TableTuple first_tuple = insertTuple(m_table, prepareTempTuple(m_table, 42, 55555, "349508345.34583", "a thing", "a totally different thing altogether", 5433));
    TableTuple second_tuple = insertTuple(m_table, prepareTempTuple(m_table, 24, 2321, "23455.5554", "and another", "this is starting to get even sillier", 2222));
    TableTuple temp_tuple = m_otherTableWithIndex->tempTuple();
    temp_tuple.setNValue(0, ValueFactory::getTinyIntValue(0));
    temp_tuple.setNValue(1, ValueFactory::getBigIntValue(1));
    TableTuple third_tuple = insertTuple(m_otherTableWithIndex, temp_tuple);
    temp_tuple = m_otherTableWithoutIndex->tempTuple();
    temp_tuple.setNValue(0, ValueFactory::getTinyIntValue(2));
    temp_tuple.setNValue(1, ValueFactory::getBigIntValue(3));
    TableTuple fourth_tuple = insertTuple(m_otherTableWithoutIndex, temp_tuple);
    endTxn(true);

    flushAndApply(99);

    EXPECT_EQ(2, m_tableReplica->activeTupleCount());
    EXPECT_EQ(1, m_otherTableWithIndexReplica->activeTupleCount());
    EXPECT_EQ(1, m_otherTableWithoutIndexReplica->activeTupleCount());

    beginTxn(100, 100, 99, 71);
    deleteTuple(m_table, first_tuple);
    temp_tuple = m_otherTableWithIndex->tempTuple();
    temp_tuple.setNValue(0, ValueFactory::getTinyIntValue(4));
    temp_tuple.setNValue(1, ValueFactory::getBigIntValue(5));
    TableTuple fifth_tuple = insertTuple(m_otherTableWithIndex, temp_tuple);
    deleteTuple(m_otherTableWithIndex, third_tuple);
    deleteTuple(m_table, second_tuple);
    deleteTuple(m_otherTableWithoutIndex, fourth_tuple);
    endTxn(true);

    flushAndApply(100);

    EXPECT_EQ(0, m_tableReplica->activeTupleCount());
    EXPECT_EQ(1, m_otherTableWithIndexReplica->activeTupleCount());
    TableTuple tuple = m_otherTableWithIndexReplica->lookupTupleByValues(fifth_tuple);
    ASSERT_FALSE(tuple.isNullTuple());
    EXPECT_EQ(0, m_otherTableWithoutIndexReplica->activeTupleCount());
}

TEST_F(DRBinaryLogTest, DeleteWithUniqueIndexNoninlineVarchar) {
    vector<int> columnIndices;
    columnIndices.push_back(0); // TINYINT
    columnIndices.push_back(4); // non-inline VARCHAR
    TableIndexScheme scheme = TableIndexScheme("the_index", HASH_TABLE_INDEX,
                                               columnIndices, TableIndex::simplyIndexColumns(),
                                               true, true, m_schema);
    TableIndex *index = TableIndexFactory::getInstance(scheme);
    scheme = TableIndexScheme("the_index", HASH_TABLE_INDEX,
                              columnIndices, TableIndex::simplyIndexColumns(),
                              true, true, m_schemaReplica);
    TableIndex *replicaIndex = TableIndexFactory::getInstance(scheme);

    m_table->addIndex(index);
    m_tableReplica->addIndex(replicaIndex);

    simpleDeleteTest();
}

TEST_F(DRBinaryLogTest, BasicUpdate) {
    simpleUpdateTest();
}

TEST_F(DRBinaryLogTest, UpdateWithUniqueIndex) {
    createIndexes();
    std::pair<const TableIndex*, uint32_t> indexPair = m_table->getSmallestUniqueIndex();
    std::pair<const TableIndex*, uint32_t> indexPairReplica = m_tableReplica->getSmallestUniqueIndex();
    ASSERT_FALSE(indexPair.first == NULL);
    ASSERT_FALSE(indexPairReplica.first == NULL);
    EXPECT_EQ(indexPair.second, indexPairReplica.second);
    simpleUpdateTest();
}

int main() {
    return TestSuite::globalInstance()->runAll();
}
