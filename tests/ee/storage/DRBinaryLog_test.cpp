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

#include "execution/VoltDBEngine.h"
#include "common/executorcontext.hpp"
#include "common/ExecuteWithMpMemory.h"
#include "common/TupleSchema.h"
#include "common/debuglog.h"
#include "common/types.h"
#include "common/NValue.hpp"
#include "common/ValueFactory.hpp"
#include "common/tabletuple.h"
#include "indexes/tableindex.h"
#include "indexes/tableindexfactory.h"
#include "storage/BinaryLogSinkWrapper.h"
#include "storage/persistenttable.h"
#include "storage/streamedtable.h"
#include "storage/tableiterator.h"
#include "storage/table.h"
#include "storage/temptable.h"
#include "storage/tablefactory.h"
#include "storage/tableiterator.h"
#include "storage/DRTupleStream.h"

#include <boost/foreach.hpp>
#include <boost/unordered_map.hpp>

#include <cstdio>
#include <string>

using namespace std;
using namespace voltdb;

const int COLUMN_COUNT = 7;
const int HIDDEN_COLUMN_COUNT = 1;
const int CLUSTER_ID = 1;
const int CLUSTER_ID_REPLICA = 2;

const int BUFFER_SIZE = 4096;
const int LARGE_BUFFER_SIZE = 32768;

static const string exportColumnNamesArray[12] = { "ROW_TYPE", "ACTION_TYPE", "CONFLICT_TYPE", "CONFLICTS_ON_PRIMARY_KEY",
                                                   "ROW_DECISION", "CLUSTER_ID", "TIMESTAMP", "DIVERGENCE", "TABLE_NAME",
                                                   "CURRENT_CLUSTER_ID", "CURRENT_TIMESTAMP", "TUPLE"};
static const std::string tableName = "VOLTDB_AUTOGEN_DR_CONFLICTS_PARTITIONED";


static bool s_multiPartitionFlag = false;
static int64_t addPartitionId(int64_t value) {
    return s_multiPartitionFlag ? ((value << 14) | 16383) : ((value << 14) | 0);
}

struct ClusterCtx {
    ClusterCtx(VoltDBEngine* engine,
               EngineLocals mpEngineLocals,
               SharedEngineLocalsType enginesByPartitionId,
               int32_t* threadPartitionId)
    : m_engine(engine)
    , m_mpEngineLocals(mpEngineLocals)
    , m_enginesByPartitionId(enginesByPartitionId)
    , m_threadPartitionId(threadPartitionId)
    {
    }

    ClusterCtx()
    : m_engine(NULL)
    , m_mpEngineLocals()
    , m_enginesByPartitionId()
    , m_threadPartitionId(NULL)
    {
    }

    VoltDBEngine* getEngine() {
        return m_engine;
    }

    EngineLocals getMpEngineLocals() {
        return m_mpEngineLocals;
    }

    SharedEngineLocalsType getEnginesByPartitionId() {
        return m_enginesByPartitionId;
    }

    int32_t* getThreadPartitionId() {
        return m_threadPartitionId;
    }
private:
    VoltDBEngine* m_engine;
    EngineLocals m_mpEngineLocals;
    SharedEngineLocalsType m_enginesByPartitionId;
    int32_t* m_threadPartitionId;
};

static std::map<int, ClusterCtx> s_clusterMap;

class MockExportTupleStream : public ExportTupleStream {
public:
    MockExportTupleStream(VoltDBEngine* engine, CatalogId partitionId, int64_t siteId,
                          int64_t generation, const std::string &tableName)
        : ExportTupleStream(partitionId, siteId, generation, tableName),
          m_engine(engine)
    { }

    virtual size_t appendTuple(VoltDBEngine* engine,
                               int64_t spHandle,
                               int64_t seqNo,
                               int64_t uniqueId,
                               const TableTuple &tuple,
                               int partitionColumn,
                               ExportTupleStream::STREAM_ROW_TYPE type) {
        receivedTuples.push_back(tuple);
        return ExportTupleStream::appendTuple(m_engine, spHandle, seqNo,
                                              uniqueId, tuple, partitionColumn, type);
    }

    std::vector<TableTuple> receivedTuples;

private:
    VoltDBEngine* m_engine;
};

class MockHashinator : public TheHashinator {
public:
    static MockHashinator* newInstance(int32_t partition = 0) {
        return new MockHashinator(partition);
    }

    std::string debug() const {
        return "MockHashinator";
    }

    ~MockHashinator() {}

protected:
    int32_t hashinate(int64_t value) const {
        return 0;
    }

    int32_t hashinate(const char *string, int32_t length) const {
        return 0;
    }

    int32_t partitionForToken(int32_t hashCode) const {
        // partition of VoltDBEngine super of MockVoltDBEngine is 0
        return m_partition;
    }

private:
    MockHashinator(int32_t partition) : m_partition(partition) {}

    int32_t m_partition;
};

class MockVoltDBEngine : public VoltDBEngine {
public:
    MockVoltDBEngine(int clusterId, Topend* topend, Pool* pool,
                     DRTupleStream* drStream, DRTupleStream* drReplicatedStream)
      : m_exportColumnAllowNull(12, false),
        m_exportColumnName(exportColumnNamesArray, exportColumnNamesArray + 12),
        m_context(new ExecutorContext(0, 0, NULL, topend, pool, this,
                                      "localhost", 2, drStream, drReplicatedStream, clusterId))
    {
        setPartitionIdForTest(0);
        ThreadLocalPool::setPartitionIds(0);
        m_exportColumnAllowNull[2] = true;
        m_exportColumnAllowNull[3] = true;
        m_exportColumnAllowNull[8] = true;
        m_exportColumnAllowNull[11] = true;
        // See DDLCompiler.java to find conflict export table schema
        m_exportColumnType.push_back(ValueType::tVARCHAR);     m_exportColumnLength.push_back(3); //row type
        m_exportColumnType.push_back(ValueType::tVARCHAR);     m_exportColumnLength.push_back(1); // action type
        m_exportColumnType.push_back(ValueType::tVARCHAR);     m_exportColumnLength.push_back(4); // conflict type
        m_exportColumnType.push_back(ValueType::tTINYINT);     m_exportColumnLength.push_back(NValue::getTupleStorageSize(ValueType::tTINYINT)); // conflicts on PK
        m_exportColumnType.push_back(ValueType::tVARCHAR);     m_exportColumnLength.push_back(1); // action decision
        m_exportColumnType.push_back(ValueType::tTINYINT);     m_exportColumnLength.push_back(NValue::getTupleStorageSize(ValueType::tTINYINT)); // remote cluster id
        m_exportColumnType.push_back(ValueType::tBIGINT);      m_exportColumnLength.push_back(NValue::getTupleStorageSize(ValueType::tBIGINT)); // remote timestamp
        m_exportColumnType.push_back(ValueType::tVARCHAR);     m_exportColumnLength.push_back(1);  // flag of divergence
        m_exportColumnType.push_back(ValueType::tVARCHAR);     m_exportColumnLength.push_back(1024); // table name
        m_exportColumnType.push_back(ValueType::tTINYINT);     m_exportColumnLength.push_back(NValue::getTupleStorageSize(ValueType::tTINYINT)); // local cluster id
        m_exportColumnType.push_back(ValueType::tBIGINT);      m_exportColumnLength.push_back(NValue::getTupleStorageSize(ValueType::tBIGINT)); // local timestamp
        m_exportColumnType.push_back(ValueType::tVARCHAR);     m_exportColumnLength.push_back(1048576); // tuple data

        m_exportSchema = TupleSchema::createTupleSchemaForTest(m_exportColumnType, m_exportColumnLength, m_exportColumnAllowNull);

        m_exportStream = new MockExportTupleStream((VoltDBEngine*)this, 1, 1, 0, tableName);
        m_conflictStreamedTable.reset(TableFactory::getStreamedTableForTest(0,
                tableName,
                m_exportSchema,
                m_exportColumnName,
                m_exportStream,
                true));
        setHashinator(MockHashinator::newInstance());

        // Add the engine to the global list tracking replicated tables
        setLowestSiteForTest();
        ThreadLocalPool::setPartitionIds(getPartitionId());
        VOLT_DEBUG("Initializing context %p", m_context.get());
        EngineLocals newLocals = EngineLocals(ExecutorContext::getExecutorContext());
        SynchronizedThreadLock::init(1, newLocals);
    }

    ~MockVoltDBEngine() { }

    StreamedTable* getConflictStreamedTable() const { return m_conflictStreamedTable.get(); }
    UndoLog* getUndoLog() { return & m_undoLog; }

    ExportTupleStream* getExportTupleStream() { return m_exportStream; }
    ExecutorContext* getExecutorContext() { return m_context.get(); }
    void prepareContext() { m_context.get()->bindToThread(); }

    void setHashinator(TheHashinator *hashinator) {
        VoltDBEngine::setHashinator(hashinator);
    }

private:
    boost::scoped_ptr<StreamedTable> m_conflictStreamedTable;
    std::vector<ValueType> m_exportColumnType;
    std::vector<int32_t> m_exportColumnLength;
    std::vector<bool> m_exportColumnAllowNull;
    const vector<string> m_exportColumnName;
    MockExportTupleStream* m_exportStream;
    TupleSchema* m_exportSchema;
    boost::scoped_ptr<ExecutorContext> m_context;
    UndoLog m_undoLog;
};


class ReplicaProcessContextSwitcher {
public:
    ReplicaProcessContextSwitcher() {
        ClusterCtx cc = s_clusterMap[CLUSTER_ID_REPLICA];
        ThreadLocalPool::setThreadPartitionIdForTest(cc.getThreadPartitionId());
        SynchronizedThreadLock::setEngineLocalsForTest(cc.getEngine()->getPartitionId(),
                                                       cc.getMpEngineLocals(),
                                                       cc.getEnginesByPartitionId());
    }

    ~ReplicaProcessContextSwitcher() {
        ClusterCtx cc = s_clusterMap[CLUSTER_ID];
        ThreadLocalPool::setThreadPartitionIdForTest(cc.getThreadPartitionId());
        SynchronizedThreadLock::setEngineLocalsForTest(cc.getEngine()->getPartitionId(),
                                                       cc.getMpEngineLocals(),
                                                       cc.getEnginesByPartitionId());
    }
};


class DRBinaryLogTest : public Test {
public:
    DRBinaryLogTest()
      : m_drStream(0, 64*1024, DRTupleStream::LATEST_PROTOCOL_VERSION),
        m_drReplicatedStream(16383, 64*1024, DRTupleStream::LATEST_PROTOCOL_VERSION),
        m_drStreamReplica(0, 64*1024, DRTupleStream::LATEST_PROTOCOL_VERSION),
        m_drReplicatedStreamReplica(16383, 64*1024, DRTupleStream::LATEST_PROTOCOL_VERSION),
        m_undoToken(0),
        m_spHandleReplica(0)
    {
        m_engine = new MockVoltDBEngine(CLUSTER_ID, &m_topend, &m_enginesPool, &m_drStream, &m_drReplicatedStream);
        s_clusterMap[CLUSTER_ID] = ClusterCtx(m_engine,
                                              SynchronizedThreadLock::s_mpEngine,
                                              SynchronizedThreadLock::s_activeEnginesByPartitionId,
                                              ThreadLocalPool::getThreadPartitionIdForTest());
        SynchronizedThreadLock::resetEngineLocalsForTest();

        m_engineReplica = new MockVoltDBEngine(CLUSTER_ID_REPLICA, &m_topend, &m_enginesPool, &m_drStreamReplica, &m_drReplicatedStreamReplica);
        s_clusterMap[CLUSTER_ID_REPLICA] = ClusterCtx(m_engineReplica,
                                                      SynchronizedThreadLock::s_mpEngine,
                                                      SynchronizedThreadLock::s_activeEnginesByPartitionId,
                                                      ThreadLocalPool::getThreadPartitionIdForTest());

        // Make the master cluster the default, starting now.
        ClusterCtx cc = s_clusterMap[CLUSTER_ID];
        ThreadLocalPool::setThreadPartitionIdForTest(cc.getThreadPartitionId());
        SynchronizedThreadLock::setEngineLocalsForTest(cc.getEngine()->getPartitionId(),
                                                       cc.getMpEngineLocals(),
                                                       cc.getEnginesByPartitionId());


        m_drStream.setDefaultCapacityForTest(BUFFER_SIZE);
        m_drStream.setSecondaryCapacity(LARGE_BUFFER_SIZE);

        m_drStream.setLastCommittedSequenceNumber(0);
        m_drReplicatedStream.setLastCommittedSequenceNumber(0);

        m_drStream.m_enabled = true;
        m_drReplicatedStream.m_enabled = true;
        m_drStreamReplica.m_enabled = false;
        m_drReplicatedStreamReplica.m_enabled = false;

        *reinterpret_cast<int64_t*>(tableHandle) = 42;
        *reinterpret_cast<int64_t*>(replicatedTableHandle) = 24;
        *reinterpret_cast<int64_t*>(otherTableHandleWithIndex) = 43;
        *reinterpret_cast<int64_t*>(otherTableHandleWithoutIndex) = 44;
        *reinterpret_cast<int64_t*>(exportTableHandle) = 55;

        std::vector<ValueType> columnTypes;
        std::vector<int32_t> columnLengths;
        std::vector<bool> columnAllowNull(COLUMN_COUNT, true);
        const std::vector<bool> columnInBytes (columnAllowNull.size(), false);

        columnTypes.push_back(ValueType::tTINYINT);   columnLengths.push_back(NValue::getTupleStorageSize(ValueType::tTINYINT));
        columnTypes.push_back(ValueType::tBIGINT);    columnLengths.push_back(NValue::getTupleStorageSize(ValueType::tBIGINT));
        columnTypes.push_back(ValueType::tDECIMAL);   columnLengths.push_back(NValue::getTupleStorageSize(ValueType::tDECIMAL));
        columnTypes.push_back(ValueType::tVARCHAR);   columnLengths.push_back(15);
        columnTypes.push_back(ValueType::tVARCHAR);   columnLengths.push_back(300);
        columnTypes.push_back(ValueType::tTIMESTAMP); columnLengths.push_back(NValue::getTupleStorageSize(ValueType::tTIMESTAMP));
        columnTypes.push_back(ValueType::tVARBINARY); columnLengths.push_back(300);

        std::vector<HiddenColumn::Type> hiddenTypes(HIDDEN_COLUMN_COUNT,HiddenColumn::XDCR_TIMESTAMP);

        m_replicatedSchema = TupleSchema::createTupleSchema(columnTypes, columnLengths, columnAllowNull, columnInBytes, hiddenTypes);
        m_replicatedSchemaReplica = TupleSchema::createTupleSchema(columnTypes, columnLengths, columnAllowNull, columnInBytes, hiddenTypes);
        columnAllowNull[0] = false;
        m_schema = TupleSchema::createTupleSchema(columnTypes, columnLengths, columnAllowNull, columnInBytes, hiddenTypes);
        m_schemaReplica = TupleSchema::createTupleSchema(columnTypes, columnLengths, columnAllowNull, columnInBytes, hiddenTypes);

        string columnNamesArray[COLUMN_COUNT] = {
            "C_TINYINT", "C_BIGINT", "C_DECIMAL",
            "C_INLINE_VARCHAR", "C_OUTLINE_VARCHAR", "C_TIMESTAMP", "C_OUTLINE_VARBINARY" };
        const vector<string> columnNames(columnNamesArray, columnNamesArray + COLUMN_COUNT);


        m_table = reinterpret_cast<PersistentTable*>(voltdb::TableFactory::getPersistentTable(0, "P_TABLE", m_schema, columnNames, tableHandle, false, 0));
        {
            ScopedReplicatedResourceLock scopedLock;
            ExecuteWithMpMemory useMpMemory;
            m_replicatedTable = reinterpret_cast<PersistentTable *>(voltdb::TableFactory::getPersistentTable(0,
                                                                                                             "R_TABLE",
                                                                                                             m_replicatedSchema,
                                                                                                             columnNames,
                                                                                                             replicatedTableHandle,
                                                                                                             false, -1,
                                                                                                             PERSISTENT, 0,
                                                                                                             95, true,
                                                                                                             true));
        }

        {
            ReplicaProcessContextSwitcher switcher;
            m_tableReplica = reinterpret_cast<PersistentTable*>(voltdb::TableFactory::getPersistentTable(0,
                                                                                                         "P_TABLE_REPLICA",
                                                                                                         m_schemaReplica,
                                                                                                         columnNames,
                                                                                                         tableHandle,
                                                                                                         false, 0));
            ScopedReplicatedResourceLock scopedLock;
            ExecuteWithMpMemory useMpMemory;
            m_replicatedTableReplica = reinterpret_cast<PersistentTable*>(voltdb::TableFactory::getPersistentTable(0,
                                                                                                                   "R_TABLE_REPLICA",
                                                                                                                   m_replicatedSchemaReplica,
                                                                                                                   columnNames,
                                                                                                                   replicatedTableHandle,
                                                                                                                   false, -1,
                                                                                                                   PERSISTENT, 0,
                                                                                                                   95, false,
                                                                                                                   true));
        }
        m_table->setDR(true);

        std::vector<ValueType> otherColumnTypes;
        std::vector<int32_t> otherColumnLengths;
        std::vector<bool> otherColumnAllowNull(2, false);
        otherColumnTypes.push_back(ValueType::tTINYINT); otherColumnLengths.push_back(NValue::getTupleStorageSize(ValueType::tTINYINT));
        otherColumnTypes.push_back(ValueType::tBIGINT);  otherColumnLengths.push_back(NValue::getTupleStorageSize(ValueType::tBIGINT));
        otherColumnAllowNull[1] = true;

        m_otherSchemaWithIndex = TupleSchema::createTupleSchemaForTest(otherColumnTypes, otherColumnLengths, otherColumnAllowNull);
        m_otherSchemaWithoutIndex = TupleSchema::createTupleSchemaForTest(otherColumnTypes, otherColumnLengths, otherColumnAllowNull);
        m_otherSchemaWithIndexReplica = TupleSchema::createTupleSchemaForTest(otherColumnTypes, otherColumnLengths, otherColumnAllowNull);
        m_otherSchemaWithoutIndexReplica = TupleSchema::createTupleSchemaForTest(otherColumnTypes, otherColumnLengths, otherColumnAllowNull);

        string otherColumnNamesArray[2] = { "C_TINYINT", "C_BIGINT" };
        const vector<string> otherColumnNames(otherColumnNamesArray, otherColumnNamesArray + 2);

        m_otherTableWithIndex = reinterpret_cast<PersistentTable*>(voltdb::TableFactory::getPersistentTable(0, "OTHER_TABLE_1", m_otherSchemaWithIndex, otherColumnNames, otherTableHandleWithIndex, false, 0));
        m_otherTableWithoutIndex = reinterpret_cast<PersistentTable*>(voltdb::TableFactory::getPersistentTable(0, "OTHER_TABLE_2", m_otherSchemaWithoutIndex, otherColumnNames, otherTableHandleWithoutIndex, false, 0));

        {
            ReplicaProcessContextSwitcher switcher;
            m_otherTableWithIndexReplica = reinterpret_cast<PersistentTable *>(voltdb::TableFactory::getPersistentTable(
                    0, "OTHER_TABLE_1", m_otherSchemaWithIndexReplica, otherColumnNames, otherTableHandleWithIndex,
                    false, 0));
            m_otherTableWithoutIndexReplica = reinterpret_cast<PersistentTable *>(voltdb::TableFactory::getPersistentTable(
                    0, "OTHER_TABLE_2", m_otherSchemaWithoutIndexReplica, otherColumnNames,
                    otherTableHandleWithoutIndex, false, 0));
        }

        vector<int> columnIndices;
        columnIndices.push_back(1);
        columnIndices.push_back(0);
        TableIndexScheme scheme = TableIndexScheme("the_index", HASH_TABLE_INDEX,
                                                   columnIndices, TableIndex::simplyIndexColumns(),
                                                   true, true, false, m_otherSchemaWithIndex);
        TableIndex *index = TableIndexFactory::getInstance(scheme);
        m_otherTableWithIndex->addIndex(index);

        {
            ReplicaProcessContextSwitcher switcher;
            scheme = TableIndexScheme("the_index", HASH_TABLE_INDEX,
                                      columnIndices, TableIndex::simplyIndexColumns(),
                                      true, true, false, m_otherSchemaWithIndexReplica);
            TableIndex *replicaIndex = TableIndexFactory::getInstance(scheme);
            m_otherTableWithIndexReplica->addIndex(replicaIndex);
        }

        m_otherTableWithIndex->setDR(true);
        m_otherTableWithoutIndex->setDR(true);
        m_otherTableWithIndexReplica->setDR(true);
        m_otherTableWithoutIndexReplica->setDR(true);

        // create a table with different schema only on the master
        std::vector<ValueType> singleColumnType;
        std::vector<int32_t> singleColumnLength;
        std::vector<bool> singleColumnAllowNull(1, false);
        singleColumnType.push_back(ValueType::tTINYINT); singleColumnLength.push_back(NValue::getTupleStorageSize(ValueType::tTINYINT));
        m_singleColumnSchema = TupleSchema::createTupleSchemaForTest(singleColumnType, singleColumnLength, singleColumnAllowNull);
        string singleColumnNameArray[1] = { "NOTHING" };
        const vector<string> singleColumnName(singleColumnNameArray, singleColumnNameArray + 1);

        m_singleColumnTable = reinterpret_cast<PersistentTable*>(voltdb::TableFactory::getPersistentTable(0, "P_SINGLE_COLUMN_TABLE",
                                                                                                          m_singleColumnSchema,
                                                                                                          singleColumnName,
                                                                                                          tableHandle + 1, false, 0));
        m_singleColumnTable->setDR(true);
    }

    virtual ~DRBinaryLogTest() {
        cleanUpTopend();

        {
            delete m_table;
            ScopedReplicatedResourceLock scopedLock;
            ExecuteWithMpMemory usingMpMemory;
            delete m_replicatedTable;
        }

        {
            ReplicaProcessContextSwitcher switcher;
            delete m_tableReplica;
            ScopedReplicatedResourceLock scopedLock;
            ExecuteWithMpMemory usingMpMemory;
            delete m_replicatedTableReplica;
        }

        delete m_singleColumnTable;
        delete m_otherTableWithIndex;
        delete m_otherTableWithoutIndex;
        delete m_engine;

        ClusterCtx cc = s_clusterMap[CLUSTER_ID_REPLICA];
        ThreadLocalPool::setThreadPartitionIdForTest(cc.getThreadPartitionId());
        SynchronizedThreadLock::setEngineLocalsForTest(cc.getEngine()->getPartitionId(),
                                                       cc.getMpEngineLocals(),
                                                       cc.getEnginesByPartitionId());
        delete m_otherTableWithIndexReplica;
        delete m_otherTableWithoutIndexReplica;
        delete m_engineReplica;

        s_clusterMap.clear();
        SynchronizedThreadLock::resetEngineLocalsForTest();
    }

    bool isReadOnly() {
        return (m_undoToken == INT64_MAX);
    }

    void beginTxn(MockVoltDBEngine *engine, int64_t txnId, int64_t spHandle, int64_t lastCommittedSpHandle, int64_t uniqueId) {
        engine->prepareContext();
        m_currTxnUniqueId = addPartitionId(uniqueId);

        UndoQuantum* uq = isReadOnly() ? NULL : engine->getUndoLog()->generateUndoQuantum(m_undoToken);
        engine->getExecutorContext()->setupForPlanFragments(uq, addPartitionId(txnId), addPartitionId(spHandle),
                                                            addPartitionId(lastCommittedSpHandle), addPartitionId(uniqueId), false);
        engine->getExecutorContext()->checkTransactionForDR();
    }

    void endTxn(MockVoltDBEngine *engine, bool success) {
        if (isReadOnly()) {
            return;
        }

        if (!success) {
            engine->getUndoLog()->undo(m_undoToken++);
        } else {
            engine->getUndoLog()->release(m_undoToken++);
            if (engine->getExecutorContext()->drStream() != NULL) {
                engine->getExecutorContext()->drStream()->endTransaction(m_currTxnUniqueId);
            }
            if (engine->getExecutorContext()->drReplicatedStream() != NULL) {
                engine->getExecutorContext()->drReplicatedStream()->endTransaction(m_currTxnUniqueId);
            }
        }
    }

    TableTuple insertTuple(PersistentTable* table, TableTuple temp_tuple) {
        if (table->isReplicatedTable()) {
            return insertTupleForReplicated(table, temp_tuple);
        }
        table->insertTuple(temp_tuple);
        if (table->schema()->hiddenColumnCount() > 0) {
            int64_t expectedTimestamp = ExecutorContext::createDRTimestampHiddenValue(static_cast<int64_t>(CLUSTER_ID), m_currTxnUniqueId);
            temp_tuple.setHiddenNValue(table->getDRTimestampColumnIndex(), ValueFactory::getBigIntValue(expectedTimestamp));
        }
        TableTuple tuple = table->lookupTupleForDR(temp_tuple);
        assert(!tuple.isNullTuple());
        return tuple;
    }

    TableTuple insertTupleForReplicated(PersistentTable* table, TableTuple temp_tuple) {
        SynchronizedThreadLock::countDownGlobalTxnStartCount(true);
        TableTuple tuple;
        {
            ExecuteWithMpMemory usingMpMemory;
            table->insertTuple(temp_tuple);
            if (table->schema()->hiddenColumnCount() > 0) {
                int64_t expectedTimestamp = ExecutorContext::createDRTimestampHiddenValue(
                        static_cast<int64_t>(CLUSTER_ID), m_currTxnUniqueId);
                temp_tuple.setHiddenNValue(table->getDRTimestampColumnIndex(),
                                           ValueFactory::getBigIntValue(expectedTimestamp));
            }
            tuple = table->lookupTupleForDR(temp_tuple);
        }

        SynchronizedThreadLock::signalLowestSiteFinished();
        assert(!tuple.isNullTuple());
        return tuple;
    }

    TableTuple updateTuple(PersistentTable* table, TableTuple oldTuple, TableTuple newTuple) {
        assert(!table->isReplicatedTable());
        table->updateTuple(oldTuple, newTuple);
        TableTuple tuple = table->lookupTupleByValues(newTuple);
        assert(!tuple.isNullTuple());
        return tuple;
    }

    void deleteTuple(PersistentTable* table, TableTuple tuple) {
        assert(!table->isReplicatedTable());
        TableTuple tuple_to_delete = table->lookupTupleForDR(tuple);
        ASSERT_FALSE(tuple_to_delete.isNullTuple());
        table->deleteTuple(tuple_to_delete, true);
    }

    TableTuple updateTuple(PersistentTable* table, TableTuple tuple, int8_t new_index_value, const std::string& new_nonindex_value) {
        assert(!table->isReplicatedTable());
        TableTuple tuple_to_update = table->lookupTupleForDR(tuple);
        assert(!tuple_to_update.isNullTuple());
        TableTuple new_tuple = table->tempTuple();
        new_tuple.copy(tuple_to_update);
        new_tuple.setNValue(0, ValueFactory::getTinyIntValue(new_index_value));
        new_tuple.setNValue(3, ValueFactory::getStringValue(new_nonindex_value, &m_longLivedPool));
        table->updateTuple(tuple_to_update, new_tuple);
        return table->lookupTupleForDR(new_tuple);
    }

    TableTuple updateTupleFirstAndSecondColumn(PersistentTable* table, TableTuple tuple, int8_t new_tinyint_value, int64_t new_bigint_value) {
        assert(!table->isReplicatedTable());
        TableTuple tuple_to_update = table->lookupTupleByValues(tuple);
        assert(!tuple_to_update.isNullTuple());
        TableTuple new_tuple = table->tempTuple();
        new_tuple.copy(tuple_to_update);
        new_tuple.setNValue(0, ValueFactory::getTinyIntValue(new_tinyint_value));
        new_tuple.setNValue(1, ValueFactory::getBigIntValue(new_bigint_value));
        table->updateTuple(tuple_to_update, new_tuple);
        return new_tuple;
    }

    TableTuple prepareTempTuple(PersistentTable* table, int8_t tinyint, int64_t bigint, const std::string& decimal,
            const std::string& short_varchar, const std::string& long_varchar, int64_t timestamp) {
        TableTuple temp_tuple = table->tempTuple();
        if (table->schema()->hiddenColumnCount() > 0) {
            temp_tuple.setHiddenNValue(0, NValue::getNullValue(ValueType::tBIGINT));
        }
        temp_tuple.setNValue(0, ValueFactory::getTinyIntValue(tinyint));
        temp_tuple.setNValue(1, ValueFactory::getBigIntValue(bigint));
        temp_tuple.setNValue(2, ValueFactory::getDecimalValueFromString(decimal));
        temp_tuple.setNValue(3, ValueFactory::getStringValue(short_varchar, &m_longLivedPool));
        temp_tuple.setNValue(4, ValueFactory::getStringValue(long_varchar, &m_longLivedPool));
        temp_tuple.setNValue(5, ValueFactory::getTimestampValue(timestamp));
        temp_tuple.setNValue(6, ValueFactory::getBinaryValue("74686973206973206120726174686572206C6F6E6720737472696E67206F6620746578742074686174206973207573656420746F206361757365206E76616C756520746F20757365206F75746C696E652073746F7261676520666F722074686520756E6465726C79696E6720646174612E2049742073686F756C64206265206C6F6E676572207468616E2036342062797465732E",
                                                                 &m_longLivedPool));
        return temp_tuple;
    }

    void deepCopy(TableTuple *dst, const TableTuple &src) {
        for (int i = 0; i < dst->columnCount(); ++i) {
            dst->setNValueAllocateForObjectCopies(i, src.getNValue(i), &m_longLivedPool);
        }

        for (int i = 0; i < dst->getSchema()->hiddenColumnCount(); ++i) {
            dst->setHiddenNValue(i, src.getHiddenNValue(i));
        }
    }

    bool flush(int64_t lastCommittedSpHandle) {
        m_drStream.periodicFlush(-1, addPartitionId(lastCommittedSpHandle));
        m_drReplicatedStream.periodicFlush(-1, addPartitionId(lastCommittedSpHandle));
        return m_topend.receivedDRBuffer;
    }

    void applyNull() {
        for (int i = static_cast<int>(m_topend.drBlocks.size()); i > 0; i--) {
            m_topend.drBlocks.pop_back();
            m_topend.data.pop_back();
        }
    }

    void flushButDontApply(int64_t lastCommittedSpHandle) {
        flush(lastCommittedSpHandle);
        applyNull();
    }

    typedef std::pair<boost::shared_array<char>, size_t> DRStreamData;
    DRStreamData getDRStreamData() {
        boost::shared_ptr<DrStreamBlock> sb = m_topend.drBlocks.front();
        m_topend.drBlocks.pop_front();
        boost::shared_array<char> data = m_topend.data.front();
        m_topend.data.pop_front();

        // Subtract 8 to fit the log count and size of logs
        size_t startPos = sb->headerSize() - 8;
        *reinterpret_cast<int32_t*>(&data.get()[startPos]) = htonl(1);
        *reinterpret_cast<int32_t*>(&data.get()[startPos + 4]) = htonl(static_cast<int32_t>(sb->offset()));
        return std::make_pair(data, startPos);
    }

    CopySerializeInputLE* getDRTaskInfo() {
        DRStreamData data = getDRStreamData();
        const char* taskParams = &data.first[data.second];

        // Add 8 to skip the log count and size of logs
        return new CopySerializeInputLE(taskParams + 8,
                ntohl(*reinterpret_cast<const int32_t*>(taskParams + 4)));
    }

    void flushAndApply(int64_t lastCommittedSpHandle, bool success = true, bool forReplicated = false) {
        ReplicaProcessContextSwitcher switcher;
        ASSERT_TRUE(flush(lastCommittedSpHandle));

        int64_t uniqueId = addPartitionId(m_spHandleReplica);
        beginTxn(m_engineReplica,
                 uniqueId, // txnid
                 uniqueId, // sphandle
                 addPartitionId(m_spHandleReplica - 1), // last sphandle
                 uniqueId); // fake uniqueid
        m_spHandleReplica++;

        std::unordered_map<int64_t, PersistentTable*> tables;
        tables[42] = m_tableReplica;
        tables[43] = m_otherTableWithIndexReplica;
        tables[44] = m_otherTableWithoutIndexReplica;
        tables[24] = m_replicatedTableReplica;

        while (!m_topend.drBlocks.empty()) {
            m_drStream.m_enabled = false;
            m_drReplicatedStream.m_enabled = false;
            DRStreamData data = getDRStreamData();
            m_sinkWrapper.apply(&data.first[data.second], tables, &m_enginesPool, m_engineReplica, 1, uniqueId);
            m_drStream.m_enabled = true;
            m_drReplicatedStream.m_enabled = true;
        }
        m_topend.receivedDRBuffer = false;
        endTxn(m_engineReplica, success);

        m_engine->prepareContext();
    }

    void enableActiveActive() {
        m_engine->enableActiveActiveForTest(m_engine->getConflictStreamedTable(), NULL);
        m_engineReplica->enableActiveActiveForTest(m_engineReplica->getConflictStreamedTable(), NULL);
    }

    void createIndexes() {
        vector<int> firstColumnIndices;
        firstColumnIndices.push_back(1); // BIGINT
        firstColumnIndices.push_back(0); // TINYINT
        TableIndexScheme scheme = TableIndexScheme("first_unique_index", HASH_TABLE_INDEX,
                                                   firstColumnIndices, TableIndex::simplyIndexColumns(),
                                                   true, true, false, m_schema);
        TableIndex *firstIndex = TableIndexFactory::getInstance(scheme);
        m_table->addIndex(firstIndex);

        {
            ReplicaProcessContextSwitcher switcher;
            scheme = TableIndexScheme("first_unique_index", HASH_TABLE_INDEX,
                                      firstColumnIndices, TableIndex::simplyIndexColumns(),
                                      true, true, false, m_schemaReplica);
            TableIndex *firstReplicaIndex = TableIndexFactory::getInstance(scheme);
            m_tableReplica->addIndex(firstReplicaIndex);
        }

        vector<int> secondColumnIndices;
        secondColumnIndices.push_back(0); // TINYINT
        secondColumnIndices.push_back(1); // BIGINT
        secondColumnIndices.push_back(4); // non-inline VARCHAR
        scheme = TableIndexScheme("second_unique_index", HASH_TABLE_INDEX,
                                  secondColumnIndices, TableIndex::simplyIndexColumns(),
                                  true, true, false, m_schema);
        TableIndex *secondIndex = TableIndexFactory::getInstance(scheme);
        m_table->addIndex(secondIndex);

        {
            ReplicaProcessContextSwitcher switcher;
            scheme = TableIndexScheme("second_unique_index", HASH_TABLE_INDEX,
                                      secondColumnIndices, TableIndex::simplyIndexColumns(),
                                      true, true, false, m_schemaReplica);
            TableIndex *secondReplicaIndex = TableIndexFactory::getInstance(scheme);
            m_tableReplica->addIndex(secondReplicaIndex);
        }

        // smaller, non-unique, only on master
        vector<int> thirdColumnIndices(1, 0);
        scheme = TableIndexScheme("third_index", HASH_TABLE_INDEX,
                                  secondColumnIndices, TableIndex::simplyIndexColumns(),
                                  false, false, false, m_schema);
        TableIndex *thirdIndex = TableIndexFactory::getInstance(scheme);
        m_table->addIndex(thirdIndex);
    }

    TableTuple firstTupleWithNulls(PersistentTable* table, bool indexFriendly = false) {
        TableTuple temp_tuple = table->tempTuple();
        temp_tuple.setNValue(0, (indexFriendly ? ValueFactory::getTinyIntValue(99) : NValue::getNullValue(ValueType::tTINYINT)));
        temp_tuple.setNValue(1, ValueFactory::getBigIntValue(489735));
        temp_tuple.setNValue(2, NValue::getNullValue(ValueType::tDECIMAL));
        temp_tuple.setNValue(3, ValueFactory::getStringValue("whatever", &m_longLivedPool));
        temp_tuple.setNValue(4, ValueFactory::getNullStringValue());
        temp_tuple.setNValue(5, ValueFactory::getTimestampValue(3495));
        return temp_tuple;
    }

    TableTuple secondTupleWithNulls(PersistentTable* table, bool indexFriendly = false) {
        TableTuple temp_tuple = table->tempTuple();
        temp_tuple.setNValue(0, ValueFactory::getTinyIntValue(42));
        temp_tuple.setNValue(1, (indexFriendly ? ValueFactory::getBigIntValue(31241) : NValue::getNullValue(ValueType::tBIGINT)));
        temp_tuple.setNValue(2, ValueFactory::getDecimalValueFromString("234234.243"));
        temp_tuple.setNValue(3, ValueFactory::getNullStringValue());
        temp_tuple.setNValue(4, ValueFactory::getStringValue("whatever and ever and ever and ever", &m_longLivedPool));
        temp_tuple.setNValue(5, NValue::getNullValue(ValueType::tTIMESTAMP));
        temp_tuple.setNValue(6, ValueFactory::getBinaryValue("DEADBEEF", &m_longLivedPool));
        return temp_tuple;
    }

    void createUniqueIndexes() {
        createUniqueIndexes(m_table);
        {
            ReplicaProcessContextSwitcher switcher;
            createUniqueIndexes(m_tableReplica);
        }
    }

    void createUniqueIndexes(PersistentTable* table) {
        createUniqueIndex(table, 0, true);
        createUniqueIndex(table, 1);
    }

    void createUniqueIndex(PersistentTable* table, int indexColumn, bool isPrimaryKey = false) {
        vector<int> columnIndices;
        columnIndices.push_back(indexColumn);
        TableIndexScheme scheme = TableIndexScheme("UniqueIndex", HASH_TABLE_INDEX,
                                                    columnIndices,
                                                    TableIndex::simplyIndexColumns(),
                                                    true, true, false, table->schema());
        TableIndex *pkeyIndex = TableIndexFactory::TableIndexFactory::getInstance(scheme);
        assert(pkeyIndex);
        table->addIndex(pkeyIndex);
        if (isPrimaryKey) {
            table->setPrimaryKeyIndex(pkeyIndex);
        }
    }

    TableTuple verifyExistingTableForDelete(TableTuple &existingTuple, bool existingOlder) {
        Table *metaTable = m_topend.existingMetaRowsForDelete.get();
        TableTuple tempMetaTuple(metaTable->schema());
        TableIterator metaIter = metaTable->iterator();
        EXPECT_EQ(true, metaIter.next(tempMetaTuple));
        EXPECT_EQ(existingOlder, ValuePeeker::peekAsBigInt(tempMetaTuple.getNValue(metaTable->columnIndex("TIMESTAMP"))) < m_topend.remoteTimestamp);

        TableTuple tuple = reinterpret_cast<PersistentTable*>(m_topend.existingTupleRowsForDelete.get())->lookupTupleForDR(existingTuple);
        EXPECT_EQ(tuple.isNullTuple(), false);

        return tuple;
    }

    TableTuple verifyExpectedTableForDelete(TableTuple &expectedTuple) {
        TableTuple tuple = reinterpret_cast<PersistentTable*>(m_topend.expectedTupleRowsForDelete.get())->lookupTupleForDR(expectedTuple);
        EXPECT_EQ(tuple.isNullTuple(), false);
        return tuple;
    }

    TableTuple verifyExistingTableForInsert(TableTuple &existingTuple) {
        TableTuple tuple = reinterpret_cast<PersistentTable*>(m_topend.existingTupleRowsForInsert.get())->lookupTupleForDR(existingTuple);
        EXPECT_EQ(tuple.isNullTuple(), false);
        return tuple;
    }

    TableTuple verifyNewTableForInsert(TableTuple &newTuple) {
        TableTuple tuple = reinterpret_cast<PersistentTable*>(m_topend.newTupleRowsForInsert.get())->lookupTupleForDR(newTuple);
        EXPECT_EQ(tuple.isNullTuple(), false);
        return tuple;
    }

    void simpleDeleteTest() {
        beginTxn(m_engine, 99, 99, 98, 70);
        TableTuple first_tuple = insertTuple(m_table, prepareTempTuple(m_table, 42, 55555, "349508345.34583", "a thing", "this is a rather long string of text that is used to cause nvalue to use outline storage for the underlying data. It should be longer than 64 bytes.", 5433));
        TableTuple second_tuple = insertTuple(m_table, prepareTempTuple(m_table, 24, 2321, "23455.5554", "and another", "this is starting to get even sillier", 2222));
        TableTuple third_tuple = insertTuple(m_table, prepareTempTuple(m_table, 72, 345, "4256.345", "something", "more tuple data, really not the same", 1812));
        endTxn(m_engine, true);

        flushAndApply(99);

        EXPECT_EQ(3, m_tableReplica->activeTupleCount());

        beginTxn(m_engine, 100, 100, 99, 71);
        deleteTuple(m_table, first_tuple);
        deleteTuple(m_table, second_tuple);
        endTxn(m_engine, true);

        flushAndApply(100);

        EXPECT_EQ(1, m_tableReplica->activeTupleCount());
        TableTuple tuple = m_tableReplica->lookupTupleForDR(third_tuple);
        ASSERT_FALSE(tuple.isNullTuple());
    }

    void simpleUpdateTest() {
        beginTxn(m_engine, 99, 99, 98, 70);
        TableTuple first_tuple = insertTuple(m_table, prepareTempTuple(m_table, 42, 55555, "349508345.34583", "a thing", "this is a rather long string of text that is used to cause nvalue to use outline storage for the underlying data. It should be longer than 64 bytes.", 5433));
        TableTuple second_tuple = insertTuple(m_table, prepareTempTuple(m_table, 24, 2321, "23455.5554", "and another", "this is starting to get even sillier", 2222));
        endTxn(m_engine, true);

        flushAndApply(99);

        EXPECT_EQ(2, m_tableReplica->activeTupleCount());

        beginTxn(m_engine, 100, 100, 99, 71);
        // update the non-index column only
        TableTuple first_updated_tuple = updateTuple(m_table, first_tuple, 42, "not that");
        endTxn(m_engine, true);

        flushAndApply(100);

        EXPECT_EQ(2, m_tableReplica->activeTupleCount());
        TableTuple expected_tuple = prepareTempTuple(m_table, 42, 55555, "349508345.34583", "not that", "this is a rather long string of text that is used to cause nvalue to use outline storage for the underlying data. It should be longer than 64 bytes.", 5433);
        TableTuple tuple = m_tableReplica->lookupTupleByValues(expected_tuple);
        ASSERT_FALSE(tuple.isNullTuple());
        if (m_table->schema()->hiddenColumnCount() > 0) {
            NValue expectedTimestamp = first_updated_tuple.getHiddenNValue(m_table->getDRTimestampColumnIndex());
            EXPECT_EQ(0, expectedTimestamp.compare(tuple.getHiddenNValue(m_table->getDRTimestampColumnIndex())));
        }
        tuple = m_table->lookupTupleForDR(second_tuple);
        ASSERT_FALSE(tuple.isNullTuple());

        beginTxn(m_engine, 101, 101, 100, 72);
        // update the index column only
        TableTuple second_updated_tuple = updateTuple(m_table, second_tuple, 99, "and another");
        endTxn(m_engine, true);

        flushAndApply(101);

        EXPECT_EQ(2, m_tableReplica->activeTupleCount());
        tuple = m_tableReplica->lookupTupleForDR(expected_tuple);
        ASSERT_FALSE(tuple.isNullTuple());
        expected_tuple = prepareTempTuple(m_table, 99, 2321, "23455.5554", "and another", "this is starting to get even sillier", 2222);
        tuple = m_table->lookupTupleForDR(second_tuple);
        ASSERT_FALSE(tuple.isNullTuple());
        if (m_table->schema()->hiddenColumnCount() > 0) {
            NValue expectedTimestamp = second_updated_tuple.getHiddenNValue(m_table->getDRTimestampColumnIndex());
            EXPECT_EQ(0, expectedTimestamp.compare(tuple.getHiddenNValue(m_table->getDRTimestampColumnIndex())));
        }
    }

    void updateWithNullsTest() {
        beginTxn(m_engine, 99, 99, 98, 70);
        TableTuple first_tuple = insertTuple(m_table, prepareTempTuple(m_table, 42, 31241, "349508345.34583", "a thing", "this is a rather long string of text that is used to cause nvalue to use outline storage for the underlying data. It should be longer than 64 bytes.", 5433));
        TableTuple second_tuple = insertTuple(m_table, prepareTempTuple(m_table, 24, 2321, "23455.5554", "and another", "this is starting to get even sillier", 2222));
        endTxn(m_engine, true);

        flushAndApply(99);

        EXPECT_EQ(2, m_tableReplica->activeTupleCount());

        beginTxn(m_engine, 100, 100, 99, 71);
        TableTuple tuple_to_update = m_table->lookupTupleForDR(first_tuple);
        ASSERT_FALSE(tuple_to_update.isNullTuple());
        TableTuple updated_tuple = secondTupleWithNulls(m_table);
        m_table->updateTuple(tuple_to_update, updated_tuple);
        endTxn(m_engine, true);

        flushAndApply(100);

        EXPECT_EQ(2, m_tableReplica->activeTupleCount());
        TableTuple expected_tuple = secondTupleWithNulls(m_table);
        TableTuple tuple = m_tableReplica->lookupTupleForDR(expected_tuple);
        ASSERT_FALSE(tuple.isNullTuple());
        tuple = m_table->lookupTupleForDR(second_tuple);
        ASSERT_FALSE(tuple.isNullTuple());
    }

    void replicatedTableWritesCommon() {
        TableTuple first_tuple;
        TableTuple second_tuple;
        {
            // write to only the replicated table
            beginTxn(m_engine, 109, 99, 98, 70);
            first_tuple = insertTuple(m_replicatedTable,
                    prepareTempTuple(m_replicatedTable, 42, 55555, "349508345.34583", "a thing", "this is a rather long string of text that is used to cause nvalue to "
                            "use outline storage for the underlying data. It should be longer than 64 bytes.", 5433));
            endTxn(m_engine, true);
        }

        {
            ReplicaProcessContextSwitcher switcher;
            flushAndApply(99, true, true);
        }

        EXPECT_EQ(0, m_tableReplica->activeTupleCount());
        EXPECT_EQ(1, m_replicatedTableReplica->activeTupleCount());
        TableTuple tuple = m_replicatedTableReplica->lookupTupleForDR(first_tuple);
        ASSERT_FALSE(tuple.isNullTuple());

        {
            // write to both the partitioned and replicated table
            beginTxn(m_engine, 110, 100, 99, 71);
            first_tuple = insertTuple(m_table, prepareTempTuple(m_table, 72, 345, "4256.345", "something", "more tuple data, really not the same", 1812));
            second_tuple = prepareTempTuple(m_replicatedTable, 7, 234, "23452436.54", "what", "this is starting to get silly", 2342);
            second_tuple = insertTuple(m_replicatedTable, second_tuple);
            endTxn(m_engine, true);
        }

        {
            ReplicaProcessContextSwitcher switcher;
            flushAndApply(100, true, true);
        }

        EXPECT_EQ(1, m_tableReplica->activeTupleCount());
        EXPECT_EQ(2, m_replicatedTableReplica->activeTupleCount());
        tuple = m_tableReplica->lookupTupleForDR(first_tuple);
        ASSERT_FALSE(tuple.isNullTuple());
        tuple = m_replicatedTableReplica->lookupTupleForDR(second_tuple);
        ASSERT_FALSE(tuple.isNullTuple());

        {
            // write to the partitioned and replicated table and roll it back
            beginTxn(m_engine, 111, 101, 100, 72);
            first_tuple = insertTuple(m_table, prepareTempTuple(m_table, 11, 34534, "3453.4545", "another", "blah blah blah blah blah blah", 2344));
            second_tuple = prepareTempTuple(m_replicatedTable, 24, 2321, "23455.5554", "and another", "this is starting to get even sillier", 2222);
            second_tuple = insertTuple(m_replicatedTable, second_tuple);
            endTxn(m_engine, false);
        }


        ASSERT_FALSE(flush(101));

        {
            // one more write to the replicated table for good measure
            beginTxn(m_engine, 112, 102, 101, 73);
            second_tuple = insertTuple(m_replicatedTable, prepareTempTuple(m_replicatedTable, 99, 29058, "92384598.2342", "what", "really, why am I writing anything in these?", 3455));
            endTxn(m_engine, true);
        }

        {
            ReplicaProcessContextSwitcher switcher;
            flushAndApply(102, true, true);
        }

        EXPECT_EQ(1, m_tableReplica->activeTupleCount());
        EXPECT_EQ(3, m_replicatedTableReplica->activeTupleCount());
        tuple = m_replicatedTableReplica->lookupTupleForDR(second_tuple);
        ASSERT_FALSE(tuple.isNullTuple());
    }

    void cleanUpTopend() {
        ReplicaProcessContextSwitcher switcher;
        m_topend.existingMetaRowsForDelete.reset();
        m_topend.existingTupleRowsForDelete.reset();
        m_topend.expectedMetaRowsForDelete.reset();
        m_topend.expectedTupleRowsForDelete.reset();
        m_topend.existingMetaRowsForInsert.reset();
        m_topend.existingTupleRowsForInsert.reset();
        m_topend.newMetaRowsForInsert.reset();
        m_topend.newTupleRowsForInsert.reset();
    }

protected:
    DRTupleStream m_drStream;
    DRTupleStream m_drReplicatedStream;

    DRTupleStream m_drStreamReplica;
    DRTupleStream m_drReplicatedStreamReplica;

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

    int64_t m_undoToken;
    int64_t m_currTxnUniqueId;

    int64_t m_spHandleReplica;

    DummyTopend m_topend;
    Pool m_enginesPool;   // purges whenever transaction commits or rolls back
    Pool m_longLivedPool; // purges at end of test
    BinaryLogSinkWrapper m_sinkWrapper;
    MockVoltDBEngine* m_engine;
    MockVoltDBEngine* m_engineReplica;

    char tableHandle[20];
    char replicatedTableHandle[20];
    char otherTableHandleWithIndex[20];
    char otherTableHandleWithoutIndex[20];
    char exportTableHandle[20];
};

TEST_F(DRBinaryLogTest, VerifyHiddenColumns) {
    ASSERT_FALSE(flush(98));

    // single row write transaction
    beginTxn(m_engine, 99, 99, 98, 70);
    TableTuple first_tuple = insertTuple(m_table, prepareTempTuple(m_table, 42, 55555, "349508345.34583", "a thing", "this is a rather long string of text that is used to cause nvalue to use outline storage for the underlying data. It should be longer than 64 bytes.", 5433));
    endTxn(m_engine, true);

    flushAndApply(99);

    TableTuple tuple = m_tableReplica->lookupTupleForDR(first_tuple);
    ASSERT_FALSE(tuple.isNullTuple());
    NValue drTimestamp = first_tuple.getHiddenNValue(m_table->getDRTimestampColumnIndex());
    NValue drTimestampReplica = tuple.getHiddenNValue(m_tableReplica->getDRTimestampColumnIndex());
    EXPECT_EQ(ValuePeeker::peekAsBigInt(drTimestamp), ExecutorContext::createDRTimestampHiddenValue(CLUSTER_ID, addPartitionId(70)));
    EXPECT_EQ(0, drTimestamp.compare(drTimestampReplica));
}

TEST_F(DRBinaryLogTest, VerifyHiddenColumnLookup) {
    beginTxn(m_engine, 98, 98, 97, 69);
    for (int i = 0; i < 10; i++) {
        insertTuple(m_table, prepareTempTuple(m_table, 42, 55555, "349508345.34583", "a thing", "this is a rather long string of text that is used to cause nvalue to use outline storage for the underlying data. It should be longer than 64 bytes.", 5433));
    }
    endTxn(m_engine, true);

    beginTxn(m_engine, 99, 99, 98, 70);
    TableTuple first_tuple = insertTuple(m_table, prepareTempTuple(m_table, 42, 55555, "349508345.34583", "a thing", "this is a rather long string of text that is used to cause nvalue to use outline storage for the underlying data. It should be longer than 64 bytes.", 5433));
    endTxn(m_engine, true);

    beginTxn(m_engine, 100, 100, 99, 71);
    for (int i = 0; i < 10; i++) {
        insertTuple(m_table, prepareTempTuple(m_table, 42, 55555, "349508345.34583", "a thing", "this is a rather long string of text that is used to cause nvalue to use outline storage for the underlying data. It should be longer than 64 bytes.", 5433));
    }
    endTxn(m_engine, true);

    NValue expectedTimestamp = first_tuple.getHiddenNValue(m_table->getDRTimestampColumnIndex());
    TableTuple lookup_tuple = prepareTempTuple(m_table, 42, 55555, "349508345.34583", "a thing", "this is a rather long string of text that is used to cause nvalue to use outline storage for the underlying data. It should be longer than 64 bytes.", 5433);
    lookup_tuple.setHiddenNValue(m_table->getDRTimestampColumnIndex(), expectedTimestamp);
    TableTuple tuple = m_table->lookupTupleForDR(lookup_tuple);
    ASSERT_FALSE(tuple.isNullTuple());
    NValue drTimestamp = tuple.getHiddenNValue(m_table->getDRTimestampColumnIndex());
    EXPECT_EQ(0, expectedTimestamp.compare(drTimestamp));

    beginTxn(m_engine, 101, 101, 100, 72);
    deleteTuple(m_table, tuple);
    endTxn(m_engine, true);

    flushAndApply(101);

    EXPECT_EQ(20, m_tableReplica->activeTupleCount());
    tuple = m_tableReplica->lookupTupleForDR(lookup_tuple);
    ASSERT_TRUE(tuple.isNullTuple());
    tuple = m_tableReplica->lookupTupleByValues(lookup_tuple);
    ASSERT_FALSE(tuple.isNullTuple());
}

TEST_F(DRBinaryLogTest, PartitionedTableNoRollbacks) {
    ASSERT_FALSE(flush(98));

    // single row write transaction
    beginTxn(m_engine, 99, 99, 98, 70);
    TableTuple first_tuple = insertTuple(m_table, prepareTempTuple(m_table, 42, 55555, "349508345.34583", "a thing", "this is a rather long string of text that is used to cause nvalue to use outline storage for the underlying data. It should be longer than 64 bytes.", 5433));
    endTxn(m_engine, true);

    // single row write transaction
    beginTxn(m_engine, 100, 100, 99, 71);
    TableTuple second_tuple = insertTuple(m_table, prepareTempTuple(m_table, 99, 29058, "92384598.2342", "what", "really, why am I writing anything in these?", 3455));
    endTxn(m_engine, true);

    flushAndApply(100);

    EXPECT_EQ(2, m_tableReplica->activeTupleCount());
    TableTuple tuple = m_tableReplica->lookupTupleForDR(first_tuple);
    ASSERT_FALSE(tuple.isNullTuple());
    tuple = m_tableReplica->lookupTupleForDR(second_tuple);
    ASSERT_FALSE(tuple.isNullTuple());

    // multiple row, multipart write transaction
    beginTxn(m_engine, 111, 101, 100, 72);
    first_tuple = insertTuple(m_table, prepareTempTuple(m_table, 72, 345, "4256.345", "something", "more tuple data, really not the same", 1812));

    // Tick during an ongoing txn -- should not push out a buffer
    ASSERT_FALSE(flush(100));

    second_tuple = insertTuple(m_table, prepareTempTuple(m_table, 7, 234, "23452436.54", "what", "this is starting to get silly", 2342));
    endTxn(m_engine, true);

    StandAloneTupleStorage existedTupleStorage(m_table->schema());
    TableTuple existedTuple = existedTupleStorage.tuple();
    deepCopy(&existedTuple, second_tuple);

    // delete the second row inserted in the last write
    beginTxn(m_engine, 112, 102, 101, 73);
    deleteTuple(m_table, second_tuple);
    // Tick before the delete
    ASSERT_TRUE(flush(101));
    endTxn(m_engine, true);
    // Apply the binary log after endTxn(m_engine, ) to get a valid undoToken.
    flushAndApply(101);

    EXPECT_EQ(4, m_tableReplica->activeTupleCount());
    tuple = m_tableReplica->lookupTupleForDR(first_tuple);
    ASSERT_FALSE(tuple.isNullTuple());
    tuple = m_tableReplica->lookupTupleByValues(prepareTempTuple(m_table, 7, 234, "23452436.54", "what", "this is starting to get silly", 2342));
    ASSERT_FALSE(tuple.isNullTuple());

    // Propagate the delete
    flushAndApply(102);
    EXPECT_EQ(3, m_tableReplica->activeTupleCount());
    tuple = m_tableReplica->lookupTupleForDR(first_tuple);
    ASSERT_FALSE(tuple.isNullTuple());
    tuple = m_tableReplica->lookupTupleForDR(existedTuple);
    ASSERT_TRUE(tuple.isNullTuple());
    DRCommittedInfo committed = m_drStream.getLastCommittedSequenceNumberAndUniqueIds();
    EXPECT_EQ(4, committed.seqNum);
    committed = m_drReplicatedStream.getLastCommittedSequenceNumberAndUniqueIds();
    EXPECT_EQ(0, committed.seqNum);
}

TEST_F(DRBinaryLogTest, PartitionedTableRollbacks) {
    m_singleColumnTable->setDR(false);

    beginTxn(m_engine, 99, 99, 98, 70);
    TableTuple source_tuple = insertTuple(m_table, prepareTempTuple(m_table, 42, 55555, "349508345.34583", "a thing", "this is a rather long string of text that is used to cause nvalue to use outline storage for the underlying data. It should be longer than 64 bytes.", 5433));
    endTxn(m_engine, false);

    // Intentionally ignore the fact that a rollback wouldn't have actually advanced the
    // lastCommittedSpHandle. Our goal is to tick such that, if data had been produced,
    // it would flush itself out now
    ASSERT_FALSE(flush(99));

    DRCommittedInfo committed = m_drStream.getLastCommittedSequenceNumberAndUniqueIds();
    EXPECT_EQ(0, committed.seqNum);
    EXPECT_EQ(0, m_tableReplica->activeTupleCount());

    beginTxn(m_engine, 100, 100, 99, 71);
    source_tuple = insertTuple(m_table, prepareTempTuple(m_table, 99, 29058, "92384598.2342", "what", "really, why am I writing anything in these?", 3455));
    endTxn(m_engine, true);

    // Roll back a txn that hasn't applied any binary log data
    beginTxn(m_engine, 101, 101, 100, 72);
    TableTuple temp_tuple = m_singleColumnTable->tempTuple();
    temp_tuple.setNValue(0, ValueFactory::getTinyIntValue(1));
    insertTuple(m_singleColumnTable, temp_tuple);
    endTxn(m_engine, false);

    flushAndApply(101);

    EXPECT_EQ(1, m_tableReplica->activeTupleCount());
    TableTuple tuple = m_tableReplica->lookupTupleForDR(source_tuple);
    ASSERT_FALSE(tuple.isNullTuple());

    committed = m_drStream.getLastCommittedSequenceNumberAndUniqueIds();
    EXPECT_EQ(1, committed.seqNum);
}

TEST_F(DRBinaryLogTest, ReplicatedTableWritesWithReplicatedStream) {
    // Use the compatible protocol version so that dr replicated stream will be picked up and used
    m_drStream.setDrProtocolVersion(DRTupleStream::COMPATIBLE_PROTOCOL_VERSION);
    m_drReplicatedStream.setDrProtocolVersion(DRTupleStream::COMPATIBLE_PROTOCOL_VERSION);
    replicatedTableWritesCommon();

    DRCommittedInfo committed = m_drStream.getLastCommittedSequenceNumberAndUniqueIds();
    EXPECT_EQ(1, committed.seqNum);
    committed = m_drReplicatedStream.getLastCommittedSequenceNumberAndUniqueIds();
    EXPECT_EQ(3, committed.seqNum);
}

TEST_F(DRBinaryLogTest, SerializeNulls) {
    beginTxn(m_engine, 109, 99, 98, 70);
    TableTuple first_tuple = insertTuple(m_replicatedTable, firstTupleWithNulls(m_replicatedTable));
    TableTuple second_tuple = insertTuple(m_replicatedTable, secondTupleWithNulls(m_replicatedTable));
    endTxn(m_engine, true);

    {
        ReplicaProcessContextSwitcher switcher;
        flushAndApply(99, true, true);
    }

    EXPECT_EQ(2, m_replicatedTableReplica->activeTupleCount());
    TableTuple tuple = m_replicatedTableReplica->lookupTupleForDR(first_tuple);
    ASSERT_FALSE(tuple.isNullTuple());
    tuple = m_replicatedTableReplica->lookupTupleForDR(second_tuple);
    ASSERT_FALSE(tuple.isNullTuple());
}

TEST_F(DRBinaryLogTest, RollbackNulls) {
    beginTxn(m_engine, 109, 99, 98, 70);
    insertTuple(m_replicatedTable, firstTupleWithNulls(m_replicatedTable));
    endTxn(m_engine, false);

    beginTxn(m_engine, 110, 100, 99, 71);
    TableTuple source_tuple = insertTuple(m_replicatedTable, prepareTempTuple(m_replicatedTable, 99, 29058, "92384598.2342", "what", "really, why am I writing anything in these?", 3455));
    endTxn(m_engine, true);

    {
        ReplicaProcessContextSwitcher switcher;
        flushAndApply(100, true, true);
    }

    EXPECT_EQ(1, m_replicatedTableReplica->activeTupleCount());
    TableTuple tuple = m_replicatedTableReplica->lookupTupleForDR(source_tuple);
    ASSERT_FALSE(tuple.isNullTuple());
}

TEST_F(DRBinaryLogTest, RollbackOnReplica) {
    // single row write transaction
    beginTxn(m_engine, 99, 99, 98, 70);
    insertTuple(m_table, prepareTempTuple(m_table, 42, 55555, "349508345.34583", "a thing", "this is a rather long string of text that is used to cause nvalue to use outline storage for the underlying data. It should be longer than 64 bytes.", 5433));
    endTxn(m_engine, true);

    // try and fail to apply this on the replica
    flushAndApply(99, false);

    EXPECT_EQ(0, m_tableReplica->activeTupleCount());

    // successfully apply some data for, I don't know, verisimilitude?
    beginTxn(m_engine, 100, 100, 99, 71);
    TableTuple source_tuple = insertTuple(m_table, prepareTempTuple(m_table, 99, 29058, "92384598.2342", "what", "really, why am I writing anything in these?", 3455));
    endTxn(m_engine, true);

    flushAndApply(100);

    EXPECT_EQ(1, m_tableReplica->activeTupleCount());
    TableTuple tuple = m_tableReplica->lookupTupleForDR(source_tuple);
    ASSERT_FALSE(tuple.isNullTuple());

    // inserts followed by some deletes
    beginTxn(m_engine, 101, 101, 100, 72);
    TableTuple first_tuple = insertTuple(m_table, prepareTempTuple(m_table, 11, 34534, "3453.4545", "another", "blah blah blah blah blah blah", 2344));
    TableTuple second_tuple = insertTuple(m_table, prepareTempTuple(m_table, 24, 2321, "23455.5554", "and another", "this is starting to get even sillier", 2222));
    insertTuple(m_table, prepareTempTuple(m_table, 72, 345, "4256.345", "something", "more tuple data, really not the same", 1812));
    deleteTuple(m_table, first_tuple);
    deleteTuple(m_table, second_tuple);
    endTxn(m_engine, true);

    flushAndApply(101, false);

    EXPECT_EQ(1, m_tableReplica->activeTupleCount());
    tuple = m_tableReplica->lookupTupleForDR(source_tuple);
    ASSERT_FALSE(tuple.isNullTuple());
}

TEST_F(DRBinaryLogTest, CantFindTable) {
    beginTxn(m_engine, 99, 99, 98, 70);
    TableTuple temp_tuple = m_singleColumnTable->tempTuple();
    temp_tuple.setNValue(0, ValueFactory::getTinyIntValue(1));
    insertTuple(m_singleColumnTable, temp_tuple);
    endTxn(m_engine, true);

    // try and fail to apply this on the replica because the table cannot be found.
    // should not throw fatal exception.
    try {
        flushAndApply(99, false);
    } catch (SerializableEEException &e) {
        endTxn(m_engine, false);
    } catch (...) {
        ASSERT_TRUE(false);
    }
}

TEST_F(DRBinaryLogTest, DeleteWithUniqueIndex) {
    createIndexes();
    simpleDeleteTest();
}

TEST_F(DRBinaryLogTest, DeleteWithUniqueIndexWhenAAEnabled) {
    m_engine->prepareContext();
    enableActiveActive();
    createIndexes();

    beginTxn(m_engine, 99, 99, 98, 70);
    TableTuple first_tuple = insertTuple(m_table, prepareTempTuple(m_table, 42, 55555, "349508345.34583", "a thing", "this is a rather long string of text that is used to cause nvalue to use outline storage for the underlying data. It should be longer than 64 bytes.", 5433));
    TableTuple second_tuple = insertTuple(m_table, prepareTempTuple(m_table, 24, 2321, "23455.5554", "and another", "this is starting to get even sillier", 2222));
    TableTuple third_tuple = insertTuple(m_table, prepareTempTuple(m_table, 72, 345, "4256.345", "something", "more tuple data, really not the same", 1812));
    endTxn(m_engine, true);

    flushAndApply(99);

    EXPECT_EQ(3, m_tableReplica->activeTupleCount());

    beginTxn(m_engine, 100, 100, 99, 71);
    deleteTuple(m_table, first_tuple);
    deleteTuple(m_table, second_tuple);
    endTxn(m_engine, true);

    flushAndApply(100);

    EXPECT_EQ(1, m_tableReplica->activeTupleCount());
    TableTuple tuple = m_tableReplica->lookupTupleForDR(third_tuple);
    ASSERT_FALSE(tuple.isNullTuple());
}

TEST_F(DRBinaryLogTest, DeleteWithUniqueIndexMultipleTables) {
    createIndexes();

    beginTxn(m_engine, 99, 99, 98, 70);
    TableTuple first_tuple = insertTuple(m_table, prepareTempTuple(m_table, 42, 55555, "349508345.34583", "a thing", "this is a rather long string of text that is used to cause nvalue to use outline storage for the underlying data. It should be longer than 64 bytes.", 5433));
    TableTuple second_tuple = insertTuple(m_table, prepareTempTuple(m_table, 24, 2321, "23455.5554", "and another", "this is starting to get even sillier", 2222));
    TableTuple temp_tuple = m_otherTableWithIndex->tempTuple();
    temp_tuple.setNValue(0, ValueFactory::getTinyIntValue(0));
    temp_tuple.setNValue(1, ValueFactory::getBigIntValue(1));
    TableTuple third_tuple = insertTuple(m_otherTableWithIndex, temp_tuple);
    temp_tuple = m_otherTableWithoutIndex->tempTuple();
    temp_tuple.setNValue(0, ValueFactory::getTinyIntValue(2));
    temp_tuple.setNValue(1, ValueFactory::getBigIntValue(3));
    TableTuple fourth_tuple = insertTuple(m_otherTableWithoutIndex, temp_tuple);
    endTxn(m_engine, true);

    flushAndApply(99);

    EXPECT_EQ(2, m_tableReplica->activeTupleCount());
    EXPECT_EQ(1, m_otherTableWithIndexReplica->activeTupleCount());
    EXPECT_EQ(1, m_otherTableWithoutIndexReplica->activeTupleCount());

    beginTxn(m_engine, 100, 100, 99, 71);
    deleteTuple(m_table, first_tuple);
    temp_tuple = m_otherTableWithIndex->tempTuple();
    temp_tuple.setNValue(0, ValueFactory::getTinyIntValue(4));
    temp_tuple.setNValue(1, ValueFactory::getBigIntValue(5));
    TableTuple fifth_tuple = insertTuple(m_otherTableWithIndex, temp_tuple);
    deleteTuple(m_otherTableWithIndex, third_tuple);
    deleteTuple(m_table, second_tuple);
    deleteTuple(m_otherTableWithoutIndex, fourth_tuple);
    endTxn(m_engine, true);

    flushAndApply(100);

    EXPECT_EQ(0, m_tableReplica->activeTupleCount());
    EXPECT_EQ(1, m_otherTableWithIndexReplica->activeTupleCount());
    TableTuple tuple = m_otherTableWithIndexReplica->lookupTupleForDR(fifth_tuple);
    ASSERT_FALSE(tuple.isNullTuple());
    EXPECT_EQ(0, m_otherTableWithoutIndexReplica->activeTupleCount());
}

TEST_F(DRBinaryLogTest, DeleteWithUniqueIndexNullColumn) {
    createIndexes();

    beginTxn(m_engine, 99, 99, 98, 70);
    TableTuple temp_tuple = m_otherTableWithIndex->tempTuple();
    temp_tuple.setNValue(0, ValueFactory::getTinyIntValue(0));
    temp_tuple.setNValue(1, NValue::getNullValue(ValueType::tBIGINT));
    TableTuple tuple = insertTuple(m_otherTableWithIndex, temp_tuple);
    endTxn(m_engine, true);

    flushAndApply(99);

    EXPECT_EQ(1, m_otherTableWithIndexReplica->activeTupleCount());

    beginTxn(m_engine, 100, 100, 99, 71);
    deleteTuple(m_otherTableWithIndex, tuple);
    endTxn(m_engine, true);

    flushAndApply(100);

    EXPECT_EQ(0, m_otherTableWithIndexReplica->activeTupleCount());
}

TEST_F(DRBinaryLogTest, DeleteWithUniqueIndexNoninlineVarchar) {
    vector<int> columnIndices;
    columnIndices.push_back(0); // TINYINT
    columnIndices.push_back(4); // non-inline VARCHAR
    TableIndexScheme scheme = TableIndexScheme("the_index", HASH_TABLE_INDEX,
                                               columnIndices, TableIndex::simplyIndexColumns(),
                                               true, true, false, m_schema);
    TableIndex *index = TableIndexFactory::getInstance(scheme);
    m_table->addIndex(index);

    {
        ReplicaProcessContextSwitcher switcher;
        scheme = TableIndexScheme("the_index", HASH_TABLE_INDEX,
                                  columnIndices, TableIndex::simplyIndexColumns(),
                                  true, true, false, m_schemaReplica);
        TableIndex *replicaIndex = TableIndexFactory::getInstance(scheme);
        m_tableReplica->addIndex(replicaIndex);
    }

    simpleDeleteTest();
}

TEST_F(DRBinaryLogTest, BasicUpdate) {
    simpleUpdateTest();
}

TEST_F(DRBinaryLogTest, UpdateWithUniqueIndex) {
    createIndexes();
    simpleUpdateTest();
}

TEST_F(DRBinaryLogTest, UpdateWithUniqueIndexWhenAAEnabled) {
    m_engine->prepareContext();
    enableActiveActive();
    createIndexes();
    simpleUpdateTest();
}

TEST_F(DRBinaryLogTest, PartialTxnRollback) {
    beginTxn(m_engine, 98, 98, 97, 69);
    TableTuple first_tuple = insertTuple(m_table, prepareTempTuple(m_table, 99, 29058, "92384598.2342", "what", "really, why am I writing anything in these?", 3455));
    endTxn(m_engine, true);

    beginTxn(m_engine, 99, 99, 98, 70);

    TableTuple second_tuple = insertTuple(m_table, prepareTempTuple(m_table, 42, 55555, "349508345.34583", "a thing", "this is a rather long string of text that is used to cause nvalue to use outline storage for the underlying data. It should be longer than 64 bytes.", 5433));

    // Simulate a second batch within the same txn
    UndoQuantum* uq = m_engine->getUndoLog()->generateUndoQuantum(m_undoToken + 1);
    m_engine->getExecutorContext()->setupForPlanFragments(uq, addPartitionId(99), addPartitionId(99),
                                                          addPartitionId(98), addPartitionId(70), false);

    insertTuple(m_table, prepareTempTuple(m_table, 24, 2321, "23455.5554", "and another", "this is starting to get even sillier", 2222));

    m_engine->getUndoLog()->undo(m_undoToken + 1);

    endTxn(m_engine, true);

    flushAndApply(100);

    EXPECT_EQ(2, m_tableReplica->activeTupleCount());
    TableTuple tuple = m_tableReplica->lookupTupleForDR(first_tuple);
    ASSERT_FALSE(tuple.isNullTuple());
    tuple = m_tableReplica->lookupTupleForDR(second_tuple);
    ASSERT_FALSE(tuple.isNullTuple());
}

TEST_F(DRBinaryLogTest, UpdateWithNulls) {
    updateWithNullsTest();
}

TEST_F(DRBinaryLogTest, UpdateWithNullsAndUniqueIndex) {
    createIndexes();
    updateWithNullsTest();
}

/*
 * Conflict detection test case - Insert Unique Constraint Violation
 *
 * | Time | DB A                          | DB B                          |
 * |------+-------------------------------+-------------------------------|
 * | T71  |                               | insert 99 (pk), 55555 (uk), X |
 * |      |                               | insert 42 (pk), 34523 (uk), Y |
 * | T72  | insert 42 (pk), 34523 (uk), X |                               |
 *
 * DB B reports: <DELETE no conflict>
 * existingRow: <null>
 * expectedRow: <null>
 *               <INSERT constraint violation>
 * existingRow: <42, 34523, Y>
 * newRow:      <42, 34523, X>
 */
TEST_F(DRBinaryLogTest, DetectInsertUniqueConstraintViolation)
{
    enableActiveActive();
    createUniqueIndexes();
    ASSERT_FALSE(flush(99));

    // write transactions on replica
    TableTuple existingTuple;
    {
        ReplicaProcessContextSwitcher switcher;
        beginTxn(m_engineReplica, 100, 100, 99, 71);
        insertTuple(m_tableReplica, prepareTempTuple(m_tableReplica, 99, 55555,
                                                     "92384598.2342", "what",
                                                     "really, why am I writing anything in these?", 3455));
        existingTuple = insertTuple(m_tableReplica, prepareTempTuple(m_tableReplica, 42, 34523,
                                                                     "7565464.2342", "yes",
                                                                     "no no no, writing more words to make it outline?",
                                                                     1234));
        endTxn(m_engineReplica, true);
        flushButDontApply(100);
    }

    // write transactions on master
    beginTxn(m_engine, 101, 101, 100, 72);
    TableTuple newTuple = insertTuple(m_table, prepareTempTuple(m_table, 42, 34523,
            "92384598.2342", "what", "really, why am I writing anything in these?", 3455));
    endTxn(m_engine, true);

    // trigger a insert unique constraint violation conflict
    flushAndApply(101);

    EXPECT_EQ(m_topend.actionType, DR_RECORD_INSERT);
    EXPECT_EQ(m_topend.deleteConflictType, NO_CONFLICT);
    ASSERT_TRUE(m_topend.existingTupleRowsForDelete.get() == NULL);
    ASSERT_TRUE(m_topend.expectedTupleRowsForDelete.get() == NULL);

    EXPECT_EQ(m_topend.insertConflictType, CONFLICT_CONSTRAINT_VIOLATION);
    // verify existing table
    EXPECT_EQ(1, m_topend.existingTupleRowsForInsert->activeTupleCount());
    /*TableTuple exportTuple1 = */verifyExistingTableForInsert(existingTuple);

    // verify new table
    EXPECT_EQ(1, m_topend.newTupleRowsForInsert->activeTupleCount());
    /*TableTuple exportTuple2 = */verifyNewTableForInsert(newTuple);

    // check export
    MockExportTupleStream *exportStream = reinterpret_cast<MockExportTupleStream*>(m_engineReplica->getExportTupleStream());
    EXPECT_EQ(2, exportStream->receivedTuples.size());
}

/*
 * Conflict detection test case - Delete Missing Tuple
 *
 * | Time | DB A                          | DB B                          |
 * |------+-------------------------------+-------------------------------|
 * | T70  | insert 42 (pk), 55555 (uk), X | insert 42 (pk), 55555 (uk), X |
 * | T71  |                               | delete 42 (pk), 55555 (uk), X |
 * | T72  | delete 42 (pk), 55555 (uk), X |                               |
 *
 * DB B reports: <DELETE missing row>
 * existingRow: <null>
 * expectedRow: <42, 5555, X>
 * deletedRow:  <>
 *               <INSERT no conflict>
 * existingRow: <null>
 * newRow:      <null>
 */
TEST_F(DRBinaryLogTest, DetectDeleteMissingTuple) {
    enableActiveActive();
    createUniqueIndexes();

    // insert rows on both side
    beginTxn(m_engine, 99, 99, 98, 70);
    TableTuple tempExpectedTuple = insertTuple(m_table, prepareTempTuple(m_table, 42, 55555, "349508345.34583", "a thing", "this is a rather long string of text that is used to cause nvalue to use outline storage for the underlying data. It should be longer than 64 bytes.", 5433));
    // do a deep copy because temp tuple of m_table will be rewritten later
    StandAloneTupleStorage expectedTupleStorage(m_table->schema());
    TableTuple expectedTuple = expectedTupleStorage.tuple();
    deepCopy(&expectedTuple, tempExpectedTuple);
    endTxn(m_engine, true);
    flushAndApply(99);

    // delete row on replica
    {
        ReplicaProcessContextSwitcher switcher;
        beginTxn(m_engineReplica, 100, 100, 99, 71);
        deleteTuple(m_tableReplica, expectedTuple);
        endTxn(m_engineReplica, true);
        flushButDontApply(100);
    }

    // delete the same row on master then wait to trigger conflict on replica
    beginTxn(m_engine, 101, 101, 100, 72);
    deleteTuple(m_table, expectedTuple);
    endTxn(m_engine, true);
    // trigger a delete missing tuple conflict
    flushAndApply(101);

    EXPECT_EQ(m_topend.actionType, DR_RECORD_DELETE);

    // 1. check delete conflict part
    EXPECT_EQ(m_topend.deleteConflictType, CONFLICT_EXPECTED_ROW_MISSING);
    // verify existing table
    EXPECT_EQ(0, m_topend.existingTupleRowsForDelete->activeTupleCount());
    // verfiy expected table
    EXPECT_EQ(1, m_topend.expectedTupleRowsForDelete->activeTupleCount());
    /*TableTuple exportTuple = */verifyExpectedTableForDelete(expectedTuple);

    // 2. check insert conflict part
    EXPECT_EQ(m_topend.insertConflictType, NO_CONFLICT);
    ASSERT_TRUE(m_topend.existingTupleRowsForInsert.get() == NULL);
    ASSERT_TRUE(m_topend.newTupleRowsForInsert.get() == NULL);

    // 3. check export
    MockExportTupleStream *exportStream = reinterpret_cast<MockExportTupleStream*>(m_engineReplica->getExportTupleStream());
    EXPECT_EQ(2, exportStream->receivedTuples.size());
}

/*
 * Conflict detection test case - Delete Timestamp Mismatch
 *
 * | Time | DB A                          | DB B                                    |
 * |------+-------------------------------+-----------------------------------------|
 * | T70  | insert 42 (pk), 55555 (uk), X | insert 42 (pk), 55555 (uk), X           |
 * | T71  |                               | update <42, 55555, X> to <42, 1234, X>  |
 * | T72  | delete 42 (pk), 55555 (uk), X |                                         |
 *
 * DB B reports: <DELETE timestamp mismatch>
 * existingRow: <42, 1234, X>
 * expectedRow: <42, 5555, X>
 * deletedRow:  <>
 *               <INSERT no conflict>
 * existingRow: <null>
 * newRow:      <null>
 */
TEST_F(DRBinaryLogTest, DetectDeleteTimestampMismatch) {
    enableActiveActive();
    createUniqueIndexes();

    // insert one row on both side
    beginTxn(m_engine, 99, 99, 98, 70);
    TableTuple tempExpectedTuple = insertTuple(m_table, prepareTempTuple(m_table, 42, 55555, "349508345.34583", "a thing", "this is a rather long string of text that is used to cause nvalue to use outline storage for the underlying data. It should be longer than 64 bytes.", 5433));
    // do a deep copy because temp tuple of replica table will be rewritten later
    StandAloneTupleStorage expectedTupleStorage(m_table->schema());
    TableTuple expectedTuple = expectedTupleStorage.tuple();
    deepCopy(&expectedTuple, tempExpectedTuple);
    endTxn(m_engine, true);
    flushAndApply(99);

    // insert a few rows and update one row on replica
    beginTxn(m_engine, 100, 100, 99, 71);
    TableTuple tempExistingTuple = updateTupleFirstAndSecondColumn(m_tableReplica, tempExpectedTuple, 42/*causes a constraint violation*/, 1234);
    // do a deep copy because temp tuple of replica table will be overwritten when applying binary log
    StandAloneTupleStorage existingTupleStorage(m_tableReplica->schema());
    TableTuple existingTuple = existingTupleStorage.tuple();
    deepCopy(&existingTuple, tempExistingTuple);
    endTxn(m_engine, true);
    flushButDontApply(100);

    // delete the row on master then wait to trigger conflict on replica
    beginTxn(m_engine, 101, 101, 100, 72);
    deleteTuple(m_table, tempExpectedTuple);
    endTxn(m_engine, true);
    // trigger a delete timestamp mismatch conflict
    flushAndApply(101);

    EXPECT_EQ(m_topend.actionType, DR_RECORD_DELETE);

    // 1. check delete conflict part
    EXPECT_EQ(m_topend.deleteConflictType, CONFLICT_EXPECTED_ROW_MISMATCH);
    // verify existing table
    EXPECT_EQ(1, m_topend.existingTupleRowsForDelete->activeTupleCount());
    /*TableTuple exportTuple1 = */verifyExistingTableForDelete(existingTuple, true);
    // verify expected table
    EXPECT_EQ(1, m_topend.expectedTupleRowsForDelete->activeTupleCount());
    /*TableTuple exportTuple2 = */verifyExpectedTableForDelete(expectedTuple);

    // 2. check insert conflict part
    EXPECT_EQ(m_topend.insertConflictType, NO_CONFLICT);
    ASSERT_TRUE(m_topend.existingTupleRowsForInsert.get() == NULL);
    ASSERT_TRUE(m_topend.newTupleRowsForInsert.get() == NULL);

    // 3. check export
    MockExportTupleStream *exportStream = reinterpret_cast<MockExportTupleStream*>(m_engineReplica->getExportTupleStream());
    EXPECT_EQ(3, exportStream->receivedTuples.size());
}

/*
 * Conflict detection test case - Update Unique Constraint Violation
 *
 * | Time | DB A                                    | DB B                           |
 * |------+-----------------------------------------+--------------------------------|
 * | T70  | insert 24 (pk), 2321 (uk), X            | insert 24 (pk), 2321 (uk), X   |
 * | T71  |                                         | insert 42 (pk), 55555 (uk), Y  |
 * |      |                                         | insert 123 (pk), 33333 (uk), Z |
 * | T72  | update <24, 2321, X> to <12, 33333, X> |                                |
 *
 * DB B reports: <DELETE no conflict>
 * existingRow: <null>
 * expectedRow: <24, 2321, X>
 *               <INSERT constraint violation>
 * existingRow: <123, 33333, Z>
 * newRow:      <12, 33333, X>
 */
TEST_F(DRBinaryLogTest, DetectUpdateUniqueConstraintViolation) {
    enableActiveActive();
    createUniqueIndexes();
    ASSERT_FALSE(flush(98));

    // insert row on both side
    beginTxn(m_engine, 99, 99, 98, 70);
    TableTuple tempExpectedTuple = insertTuple(m_table, prepareTempTuple(m_table, 24, 2321, "23455.5554", "and another", "this is starting to get even sillier", 2222));
    // do a deep copy because temp tuple of relica table will be rewritten later
    StandAloneTupleStorage expectedTupleStorage(m_table->schema());
    TableTuple expectedTuple = expectedTupleStorage.tuple();
    deepCopy(&expectedTuple, tempExpectedTuple);

    insertTuple(m_table, prepareTempTuple(m_table, 111, 11111, "11111.1111", "second", "this is starting to get even sillier", 2222));
    insertTuple(m_table, prepareTempTuple(m_table, 65, 22222, "22222.2222", "third", "this is starting to get even sillier", 2222));
    endTxn(m_engine, true);
    flushAndApply(99);

    // insert rows on replica side
    StandAloneTupleStorage existingTupleStorage(m_tableReplica->schema());
    TableTuple existingTuple = existingTupleStorage.tuple();
    {
        ReplicaProcessContextSwitcher switcher;
        beginTxn(m_engineReplica, 100, 100, 99, 71);
        insertTuple(m_tableReplica, prepareTempTuple(m_tableReplica, 42, 55555, "349508345.34583", "a thing",
                                                     "this is a rather long string of text that is used to cause nvalue to use outline storage for the underlying data. It should be longer than 64 bytes.",
                                                     5433));

        TableTuple tempExistingTuple = insertTuple(m_tableReplica,
                                                   prepareTempTuple(m_tableReplica, 123, 33333, "122308345.34583",
                                                                    "another thing",
                                                                    "this is a rather long string of text that is used to cause nvalue to use outline storage for the underlying data. It should be longer than 64 bytes.",
                                                                    5433));
        // do a deep copy because temp tuple of replica table will be overwritten when applying binary log
        deepCopy(&existingTuple, tempExistingTuple);
        endTxn(m_engineReplica, true);
        flushButDontApply(100);
    }

    // update row on master to create conflict
    beginTxn(m_engine, 101, 101, 100, 72);
    TableTuple newTuple = updateTupleFirstAndSecondColumn(m_table, tempExpectedTuple, 12, 33333/*causes a constraint violation*/);
    endTxn(m_engine, true);

    // trigger a update unique constraint violation conflict
    flushAndApply(101);

    EXPECT_EQ(m_topend.actionType, DR_RECORD_UPDATE);

    // 1. check delete conflict part
    EXPECT_EQ(m_topend.deleteConflictType, NO_CONFLICT);
    ASSERT_TRUE(m_topend.existingTupleRowsForDelete.get() == NULL);
    EXPECT_EQ(1, m_topend.expectedTupleRowsForDelete->activeTupleCount());
    verifyExpectedTableForDelete(expectedTuple);

    // 2. check insert conflict part
    EXPECT_EQ(m_topend.insertConflictType, CONFLICT_CONSTRAINT_VIOLATION);
    // verify existing table
    EXPECT_EQ(1, m_topend.existingTupleRowsForInsert->activeTupleCount());
    /*TableTuple exportTuple1 = */verifyExistingTableForInsert(existingTuple);
    // verify new table
    EXPECT_EQ(1, m_topend.newTupleRowsForInsert->activeTupleCount());
    /*TableTuple exportTuple2 = */verifyNewTableForInsert(newTuple);

    // 3. check export
    MockExportTupleStream *exportStream = reinterpret_cast<MockExportTupleStream*>(m_engineReplica->getExportTupleStream());
    EXPECT_EQ(3, exportStream->receivedTuples.size());
}

/*
 * Conflict detection test case - Update Missing Tuple
 *
 * | Time | DB A                                    | DB B                                     |
 * |------+-----------------------------------------+------------------------------------------|
 * | T70  | insert 42 (pk), 55555 (uk), X           | insert 42 (pk), 55555 (uk), X            |
 * | T71  |                                         | update <42, 55555, X> to <35, 12345, X>  |
 * | T72  | update <42, 55555, X> to <42, 54321, X> |                                          |
 *
 * DB B reports: <DELETE missing row>
 * existingRow: <null>
 * expectedRow: <42, 55555, X>
 *               <INSERT no conflict>
 * existingRow: <null>
 * newRow:      <42, 54321, X>
 */
TEST_F(DRBinaryLogTest, DetectUpdateMissingTuple) {
    enableActiveActive();
    createUniqueIndexes();

    // insert rows on both side
    beginTxn(m_engine, 99, 99, 98, 70);
    TableTuple tempExpectedTuple = insertTuple(m_table, prepareTempTuple(m_table, 42, 55555, "349508345.34583", "a thing", "this is a rather long string of text that is used to cause nvalue to use outline storage for the underlying data. It should be longer than 64 bytes.", 5433));
    // do a deep copy because temp tuple of table will be overwritten later
    StandAloneTupleStorage expectedTupleStorage(m_table->schema());
    TableTuple expectedTuple = expectedTupleStorage.tuple();
    deepCopy(&expectedTuple, tempExpectedTuple);
    endTxn(m_engine, true);
    flushAndApply(99);

    // update one row on replica
    {
        ReplicaProcessContextSwitcher switcher;
        beginTxn(m_engineReplica, 100, 100, 99, 71);
        updateTupleFirstAndSecondColumn(m_tableReplica, tempExpectedTuple, 35, 12345);
        endTxn(m_engineReplica, true);
        flushButDontApply(100);
    }

    // update the same row on master then wait to trigger conflict on replica
    beginTxn(m_engine, 101, 101, 100, 72);
    TableTuple tempNewTuple = updateTupleFirstAndSecondColumn(m_table, expectedTuple, 42, 54321);
    // do a deep copy because temp tuple of table will be overwritten later
    StandAloneTupleStorage newTupleStorage(m_table->schema());
    TableTuple newTuple = newTupleStorage.tuple();
    deepCopy(&newTuple, tempNewTuple);
    endTxn(m_engine, true);
    // trigger a update missing tuple conflict
    flushAndApply(101);

    EXPECT_EQ(m_topend.actionType, DR_RECORD_UPDATE);

    // 1. check delete conflict part
    EXPECT_EQ(m_topend.deleteConflictType, CONFLICT_EXPECTED_ROW_MISSING);
    // verify existing table
    EXPECT_EQ(0, m_topend.existingTupleRowsForDelete->activeTupleCount());
    // verify expected table
    EXPECT_EQ(1, m_topend.expectedTupleRowsForDelete->activeTupleCount());
    /*TableTuple exportTuple1 = */verifyExpectedTableForDelete(expectedTuple);

    // 2. check insert conflict part
    EXPECT_EQ(m_topend.insertConflictType, NO_CONFLICT);
    ASSERT_TRUE(m_topend.existingTupleRowsForInsert.get() == NULL);
    EXPECT_EQ(1, m_topend.newTupleRowsForInsert->activeTupleCount());
    /*TableTuple exportTuple2 = */verifyNewTableForInsert(newTuple);

    // 3. check export
    MockExportTupleStream *exportStream = reinterpret_cast<MockExportTupleStream*>(m_engineReplica->getExportTupleStream());
    EXPECT_EQ(2, exportStream->receivedTuples.size());
}


/*
 * Conflict detection test case - Update missing tuple and new row triggers constraint
 *
 * | Time | DB A                                    | DB B                                     |
 * |------+-----------------------------------------+------------------------------------------|
 * | T70  | insert 42 (pk), 55555 (uk), X           | insert 42 (pk), 55555 (uk), X            |
 * |      | insert 24 (pk), 2321 (uk), Y            | insert 24 (pk), 2321 (uk), Y             |
 * |      | insert 72 (pk), 345 (uk), Z             | insert 72 (pk), 345 (uk), Z              |
 * | T71  |                                         | delete <42, 55555, X>                    |
 * |      |                                         | insert 36 (pk), 12345 (uk), X            |
 * | T72  | update <42, 55555, X> to <42, 12345, X> |                                          |
 *
 * DB B reports: <DELETE missing row>
 * existingRow: <null>
 * expectedRow: <42, 55555, X>
 *               <INSERT constraint violation>
 * existingRow: <36, 12345, X>
 * newRow:      <42, 12345, X>
 */
TEST_F(DRBinaryLogTest, DetectUpdateMissingTupleAndNewRowConstraint) {
    enableActiveActive();
    createUniqueIndexes();

    // insert rows on both side
    beginTxn(m_engine, 99, 99, 98, 70);
    TableTuple tempExpectedTuple = insertTuple(m_table, prepareTempTuple(m_table, 42, 55555, "349508345.34583", "a thing", "this is a rather long string of text that is used to cause nvalue to use outline storage for the underlying data. It should be longer than 64 bytes.", 5433));
    // do a deep copy because temp tuple of table will be overwritten later
    StandAloneTupleStorage expectedTupleStorage(m_table->schema());
    TableTuple expectedTuple = expectedTupleStorage.tuple();
    deepCopy(&expectedTuple, tempExpectedTuple);
    insertTuple(m_table, prepareTempTuple(m_table, 24, 2321, "23455.5554", "and another", "this is starting to get even sillier", 2222));
    insertTuple(m_table, prepareTempTuple(m_table, 72, 345, "4256.345", "something", "more tuple data, really not the same", 1812));
    endTxn(m_engine, true);
    flushAndApply(99);

    // update one row on replica
    StandAloneTupleStorage existingTupleStorage(m_tableReplica->schema());
    TableTuple existingTuple = existingTupleStorage.tuple();
    {
        ReplicaProcessContextSwitcher switcher;
        beginTxn(m_engineReplica, 100, 100, 99, 71);
        deleteTuple(m_tableReplica, tempExpectedTuple);
        TableTuple tempExistingTuple = insertTuple(m_tableReplica,
                                                   prepareTempTuple(m_tableReplica, 36, 12345, "349508345.34583",
                                                                    "a thing",
                                                                    "this is a rather long string of text that is used to cause nvalue to use outline storage for the underlying data. It should be longer than 64 bytes.",
                                                                    5433));
        // do a deep copy because temp tuple of relica table will be overwritten when applying binary log
        deepCopy(&existingTuple, tempExistingTuple);
        endTxn(m_engineReplica, true);
        flushButDontApply(100);
    }

    // update the same row on master then wait to trigger conflict on replica
    beginTxn(m_engine, 101, 101, 100, 72);
    TableTuple newTuple = updateTupleFirstAndSecondColumn(m_table, tempExpectedTuple, 42, 12345/*causes a constraint violation*/);
    endTxn(m_engine, true);
    // trigger a update missing tuple conflict
    flushAndApply(101);

    EXPECT_EQ(m_topend.actionType, DR_RECORD_UPDATE);

    // 1. check delete conflict part
    EXPECT_EQ(m_topend.deleteConflictType, CONFLICT_EXPECTED_ROW_MISSING);
    // verify existing table
    EXPECT_EQ(0, m_topend.existingTupleRowsForDelete->activeTupleCount());
    // verify expected table
    EXPECT_EQ(1, m_topend.expectedTupleRowsForDelete->activeTupleCount());
    /*TableTuple exportTuple1 = */verifyExpectedTableForDelete(expectedTuple);

    // 2. check insert conflict part
    EXPECT_EQ(m_topend.insertConflictType, CONFLICT_CONSTRAINT_VIOLATION);
    // verify existing table
    EXPECT_EQ(1, m_topend.existingTupleRowsForInsert->activeTupleCount());
    /*TableTuple exportTuple2 = */verifyExistingTableForInsert(existingTuple);
    // verify new table
    EXPECT_EQ(1, m_topend.newTupleRowsForInsert->activeTupleCount());
    /*TableTuple exportTuple3 = */verifyNewTableForInsert(newTuple);

    // 3. check export
    MockExportTupleStream *exportStream = reinterpret_cast<MockExportTupleStream*>(m_engineReplica->getExportTupleStream());
    EXPECT_EQ(3, exportStream->receivedTuples.size());
}

/*
 * Conflict detection test case - Update Timestamp Mismatch
 *
 * | Time | DB A                                    | DB B                                     |
 * |------+-----------------------------------------+------------------------------------------|
 * | T70  | insert 42 (pk), 55555 (uk), X           | insert 42 (pk), 55555 (uk), X            |
 * |      | insert 24 (pk), 2321 (uk), Y            | insert 24 (pk), 2321 (uk), Y             |
 * |      | insert 72 (pk), 345 (uk), Z             | insert 72 (pk), 345 (uk), Z              |
 * | T71  |                                         | update <42, 55555, X> to <42, 12345, X>  |
 * | T72  | update <42, 55555, X> to <42, 54321, X> |                                          |
 *
 * DB B reports: <DELETE timestamp mismatch>
 * existingRow: <42, 12345, X>
 * expectedRow: <42, 55555, X>
 *               <INSERT no conflict>
 * existingRow: <null>
 * newRow:      <42, 54321, X>
 */
TEST_F(DRBinaryLogTest, DetectUpdateTimestampMismatch) {
    enableActiveActive();
    createUniqueIndexes();

    // insert one row on both side
    beginTxn(m_engine, 99, 99, 98, 70);
    TableTuple tempExpectedTuple = insertTuple(m_table, prepareTempTuple(m_table, 42, 55555, "349508345.34583", "a thing", "this is a rather long string of text that is used to cause nvalue to use outline storage for the underlying data. It should be longer than 64 bytes.", 5433));
    // do a deep copy because temp tuple of table will be overwritten later
    StandAloneTupleStorage expectedTupleStorage(m_table->schema());
    TableTuple expectedTuple = expectedTupleStorage.tuple();
    deepCopy(&expectedTuple, tempExpectedTuple);
    insertTuple(m_table, prepareTempTuple(m_table, 24, 2321, "23455.5554", "and another", "this is starting to get even sillier", 2222));
    insertTuple(m_table, prepareTempTuple(m_table, 72, 345, "4256.345", "something", "more tuple data, really not the same", 1812));
    endTxn(m_engine, true);
    flushAndApply(99);

    // update one row on replica
    StandAloneTupleStorage existingTupleStorage(m_tableReplica->schema());
    TableTuple existingTuple = existingTupleStorage.tuple();
    {
        ReplicaProcessContextSwitcher switcher;
        beginTxn(m_engineReplica, 100, 100, 99, 71);
        TableTuple tempExistingTuple = updateTupleFirstAndSecondColumn(m_tableReplica, tempExpectedTuple, 42, 12345);
        // do a deep copy because temp tuple of replica table will be overwritten when applying binary log
        deepCopy(&existingTuple, tempExistingTuple);
        endTxn(m_engineReplica, true);
        flushButDontApply(100);
    }

    // update the same row on master then wait to trigger conflict on replica
    beginTxn(m_engine, 101, 101, 100, 72);
    TableTuple tempNewTuple = updateTupleFirstAndSecondColumn(m_table, tempExpectedTuple, 42, 54321);
    // do a deep copy because temp tuple of table will be overwritten later
    StandAloneTupleStorage newTupleStorage(m_table->schema());
    TableTuple newTuple = newTupleStorage.tuple();
    deepCopy(&newTuple, tempNewTuple);
    endTxn(m_engine, true);
    // trigger a update timestamp mismatch conflict
    flushAndApply(101);

    EXPECT_EQ(m_topend.actionType, DR_RECORD_UPDATE);

    // 1. check delete conflict part
    EXPECT_EQ(m_topend.deleteConflictType, CONFLICT_EXPECTED_ROW_MISMATCH);
    // verify existing table
    EXPECT_EQ(1, m_topend.existingTupleRowsForDelete->activeTupleCount());
    /*TableTuple exportTuple1 = */verifyExistingTableForDelete(existingTuple, true);
    // verify expected table
    EXPECT_EQ(1, m_topend.expectedTupleRowsForDelete->activeTupleCount());
    /*TableTuple exportTuple2 = */verifyExpectedTableForDelete(expectedTuple);

    // 2. check insert conflict part
    EXPECT_EQ(m_topend.insertConflictType, NO_CONFLICT);
    ASSERT_TRUE(m_topend.existingTupleRowsForInsert.get() == NULL);
    EXPECT_EQ(1, m_topend.newTupleRowsForInsert->activeTupleCount());
    /*TableTuple exportTuple3 = */verifyNewTableForInsert(newTuple);

    // 3. check export
    MockExportTupleStream *exportStream = reinterpret_cast<MockExportTupleStream*>(m_engineReplica->getExportTupleStream());
    EXPECT_EQ(3, exportStream->receivedTuples.size());
}

/*
 * Conflict detection test case - Update Timestamp Mismatch Rejected
 *
 * | Time | DB A                                    | DB B                                     |
 * |------+-----------------------------------------+------------------------------------------|
 * | T70  | insert 42 (pk), 55555 (uk), X           | insert 42 (pk), 55555 (uk), X            |
 * |      | insert 24 (pk), 2321 (uk), Y            | insert 24 (pk), 2321 (uk), Y             |
 * |      | insert 72 (pk), 345 (uk), Z             | insert 72 (pk), 345 (uk), Z              |
 * | T71  | update <42, 55555, X> to <42, 12345, X> |                                          |
 * | T72  |                                         | update <42, 55555, X> to <42, 54321, X>  |
 *
 * DB B reports: <DELETE timestamp mismatch>
 * existingRow: <42, 54321, X>
 * expectedRow: <42, 55555, X>
 *               <INSERT no conflict>
 * existingRow: <null>
 * newRow:      <42, 12345, X>
 */
TEST_F(DRBinaryLogTest, DetectUpdateTimestampMismatchRejected) {
    enableActiveActive();
    createUniqueIndexes();

    // insert one row on both side
    beginTxn(m_engine, 99, 99, 98, 70);
    TableTuple tempExpectedTuple = insertTuple(m_table, prepareTempTuple(m_table, 42, 55555, "349508345.34583", "a thing", "this is a rather long string of text that is used to cause nvalue to use outline storage for the underlying data. It should be longer than 64 bytes.", 5433));
    // do a deep copy because temp tuple of table will be overwritten later
    StandAloneTupleStorage expectedTupleStorage(m_table->schema());
    TableTuple expectedTuple = expectedTupleStorage.tuple();
    deepCopy(&expectedTuple, tempExpectedTuple);
    insertTuple(m_table, prepareTempTuple(m_table, 24, 2321, "23455.5554", "and another", "this is starting to get even sillier", 2222));
    insertTuple(m_table, prepareTempTuple(m_table, 72, 345, "4256.345", "something", "more tuple data, really not the same", 1812));
    endTxn(m_engine, true);
    flushAndApply(99);

    // update one row on replica
    beginTxn(m_engine, 100, 100, 99, 71);
    TableTuple tempNewTuple = updateTupleFirstAndSecondColumn(m_table, tempExpectedTuple, 42, 12345);
    // do a deep copy because temp tuple of table will be overwritten later
    StandAloneTupleStorage newTupleStorage(m_table->schema());
    TableTuple newTuple = newTupleStorage.tuple();
    deepCopy(&newTuple, tempNewTuple);
    endTxn(m_engine, true);
    flush(100);

    // update the same row on master then wait to trigger conflict on replica
    StandAloneTupleStorage existingTupleStorage(m_tableReplica->schema());
    TableTuple existingTuple = existingTupleStorage.tuple();
    {
        ReplicaProcessContextSwitcher switcher;
        beginTxn(m_engineReplica, 101, 101, 100, 72);
        TableTuple tempExistingTuple = updateTupleFirstAndSecondColumn(m_tableReplica, tempExpectedTuple, 42, 54321);
        // do a deep copy because temp tuple of relica table will be overwritten when applying binary log
        deepCopy(&existingTuple, tempExistingTuple);
        endTxn(m_engineReplica, true);
        // trigger a update timestamp mismatch conflict
        flushAndApply(101);
    }

    EXPECT_EQ(m_topend.actionType, DR_RECORD_UPDATE);

    // 1. check delete conflict part
    EXPECT_EQ(m_topend.deleteConflictType, CONFLICT_EXPECTED_ROW_MISMATCH);
    // verify existing table
    EXPECT_EQ(1, m_topend.existingTupleRowsForDelete->activeTupleCount());
    /*TableTuple exportTuple1 = */verifyExistingTableForDelete(existingTuple, false);
    // verify expected table
    EXPECT_EQ(1, m_topend.expectedTupleRowsForDelete->activeTupleCount());
    /*TableTuple exportTuple2 = */verifyExpectedTableForDelete(expectedTuple);

    // 2. check insert conflict part
    EXPECT_EQ(m_topend.insertConflictType, NO_CONFLICT);
    ASSERT_TRUE(m_topend.existingTupleRowsForInsert.get() == NULL);
    EXPECT_EQ(1, m_topend.newTupleRowsForInsert->activeTupleCount());
    /*TableTuple exportTuple3 = */verifyNewTableForInsert(newTuple);

    // 3. check export
    MockExportTupleStream *exportStream = reinterpret_cast<MockExportTupleStream*>(m_engineReplica->getExportTupleStream());
    EXPECT_EQ(3, exportStream->receivedTuples.size());
}

/**
 * Conflict detection test case - Update timstamp mismatch and new row triggers unique
 * constraint violation.
 *
 * | Time | DB A                                    | DB B                                     |
 * |------+-----------------------------------------+------------------------------------------|
 * | T70  | insert 42 (pk), 55555 (uk), X           | insert 42 (pk), 55555 (uk), X            |
 * |      | insert 24 (pk), 2321 (uk), Y            | insert 24 (pk), 2321 (uk), Y             |
 * | T71  |                                         | update <42, 55555, X> to <42, 12345, X>  |
 * |      |                                         | insert 72 (pk), 345 (uk), Z              |
 * | T72  | update <42, 55555, X> to <42, 345, X> |                                          |
 *
 * DB B reports: <DELETE timestamp mismatch>
 * existingRow: <42, 12345, X>
 * expectedRow: <42, 55555, X>
 *               <INSERT constraint violation>
 * existingRow: <72, 345, Z>
 * newRow:      <42, 345, X>
 */
TEST_F(DRBinaryLogTest, DetectUpdateTimestampMismatchAndNewRowConstraint) {
    enableActiveActive();
    createUniqueIndexes();

    // insert one row on both side
    beginTxn(m_engine, 99, 99, 98, 70);
    TableTuple tempExpectedTuple = insertTuple(m_table, prepareTempTuple(m_table, 42, 55555, "349508345.34583", "a thing", "this is a rather long string of text that is used to cause nvalue to use outline storage for the underlying data. It should be longer than 64 bytes.", 5433));
    // do a deep copy because temp tuple of table will be overwritten later
    StandAloneTupleStorage expectedTupleStorage(m_table->schema());
    TableTuple expectedTuple = expectedTupleStorage.tuple();
    deepCopy(&expectedTuple, tempExpectedTuple);
    insertTuple(m_table, prepareTempTuple(m_table, 24, 2321, "23455.5554", "and another", "this is starting to get even sillier", 2222));
    endTxn(m_engine, true);
    flushAndApply(99);

    // update one row on replica
    StandAloneTupleStorage existingTupleFirstStorage(m_tableReplica->schema());
    TableTuple existingTupleFirst = existingTupleFirstStorage.tuple();
    StandAloneTupleStorage existingTupleSecondStorage(m_tableReplica->schema());
    TableTuple existingTupleSecond = existingTupleSecondStorage.tuple();
    {
        ReplicaProcessContextSwitcher switcher;
        beginTxn(m_engineReplica, 100, 100, 99, 71);
        TableTuple tempExistingTupleFirst = updateTupleFirstAndSecondColumn(m_tableReplica, tempExpectedTuple, 42,
                                                                            12345);
        // do a deep copy because temp tuple of replica table will be overwritten when applying binary log
        deepCopy(&existingTupleFirst, tempExistingTupleFirst);
        TableTuple tempExistingTupleSecond = insertTuple(m_tableReplica,
                                                         prepareTempTuple(m_tableReplica, 72, 345, "4256.345",
                                                                          "something",
                                                                          "more tuple data, really not the same",
                                                                          1812));
        // do a deep copy because temp tuple of replica table will be overwritten when applying binary log
        deepCopy(&existingTupleSecond, tempExistingTupleSecond);
        endTxn(m_engineReplica, true);
        flushButDontApply(100);
    }

    // update the same row on master then wait to trigger conflict on replica
    beginTxn(m_engine, 101, 101, 100, 72);
    TableTuple newTuple = updateTupleFirstAndSecondColumn(m_table, tempExpectedTuple, 42, 345/*cause a constraint violation*/);
    endTxn(m_engine, true);
    // trigger a update timestamp mismatch conflict
    flushAndApply(101);

    EXPECT_EQ(2, m_table->activeTupleCount());
    EXPECT_EQ(3, m_tableReplica->activeTupleCount());
    EXPECT_EQ(m_topend.actionType, DR_RECORD_UPDATE);
    // 1. check delete conflict part
    EXPECT_EQ(m_topend.deleteConflictType, CONFLICT_EXPECTED_ROW_MISMATCH);
    // verify existing table
    EXPECT_EQ(1, m_topend.existingTupleRowsForDelete->activeTupleCount());
    /*TableTuple exportTuple1 = */verifyExistingTableForDelete(existingTupleFirst, true);
    // verify expected table
    EXPECT_EQ(1, m_topend.expectedTupleRowsForDelete->activeTupleCount());
    /*TableTuple exportTuple2 = */verifyExpectedTableForDelete(expectedTuple);

    // 2. check insert conflict part
    EXPECT_EQ(m_topend.insertConflictType, CONFLICT_CONSTRAINT_VIOLATION);
    // verify existing table
    EXPECT_EQ(1, m_topend.existingTupleRowsForInsert->activeTupleCount());
    /*TableTuple exportTuple3 = */verifyExistingTableForInsert(existingTupleSecond);
    // verify new table
    EXPECT_EQ(1, m_topend.newTupleRowsForInsert->activeTupleCount());
    /*TableTuple exportTuple4 = */verifyNewTableForInsert(newTuple);

    // 3. check export
    MockExportTupleStream *exportStream = reinterpret_cast<MockExportTupleStream*>(m_engineReplica->getExportTupleStream());
    EXPECT_EQ(4, exportStream->receivedTuples.size());
}

TEST_F(DRBinaryLogTest, InsertOverBufferLimit) {
    createIndexes();
    const int total = 400;
    int spHandle = 1;

    beginTxn(m_engine, spHandle, spHandle, spHandle-1, spHandle);
    try {
        for (int i = 1; i <= total; i++) {
            insertTuple(m_table, prepareTempTuple(m_table, 42, i, "349508345.34583", "a thing", "a totally different thing altogether", i));
        }
    } catch (SerializableEEException& e) {
        endTxn(m_engine, false);
        spHandle++;

        for (int i = 1; i <= total; i++, spHandle++) {
            beginTxn(m_engine, spHandle, spHandle, spHandle-1, spHandle);
            insertTuple(m_table, prepareTempTuple(m_table, 42, i, "349508345.34583", "a thing", "a totally different thing altogether", i));
            endTxn(m_engine, true);
        }

        return;
    }

    ASSERT_TRUE(false);
}

TEST_F(DRBinaryLogTest, UpdateOverBufferLimit) {
    createIndexes();
    const int total = 150;
    long spHandle = 1;

    for (int i = 0; i < total; i++, spHandle++) {
        beginTxn(m_engine, spHandle, spHandle, spHandle-1, spHandle);
        insertTuple(m_table, prepareTempTuple(m_table, 42, i, "349508345.34583", "a thing", "a totally different thing altogether", i));
        endTxn(m_engine, true);
    }

    flushAndApply(spHandle-1);

    // Update all tuples
    beginTxn(m_engine, spHandle, spHandle, spHandle-1, spHandle);
    spHandle++;
    try {
        // Update all rows to new values and update them back to the original
        // values. It would overflow the DR buffer limit and cause the txn to
        // roll back.
        for (int i = 0; i < total; i++) {
            TableTuple newTuple = prepareTempTuple(m_table, 42, i, "349508345.34583", "a thing", "a totally different thing altogether", i);
            TableTuple oldTuple = m_table->lookupTupleByValues(newTuple);
            newTuple.setNValue(1, ValueFactory::getBigIntValue(i+total));
            updateTuple(m_table, oldTuple, newTuple);
        }
        for (int i = 0; i < total; i++) {
            TableTuple newTuple = prepareTempTuple(m_table, 42, i+total, "349508345.34583", "a thing", "a totally different thing altogether", i);
            TableTuple oldTuple = m_table->lookupTupleByValues(newTuple);
            newTuple.setNValue(1, ValueFactory::getBigIntValue(i));
            updateTuple(m_table, oldTuple, newTuple);
        }
    } catch (SerializableEEException& e) {
        endTxn(m_engine, false);

        // Make sure all changes rolled back
        for (int i = 0; i < total; i++) {
            TableTuple tuple = m_table->lookupTupleByValues(prepareTempTuple(m_table, 42, i, "349508345.34583", "a thing", "a totally different thing altogether", i));
            ASSERT_FALSE(tuple.isNullTuple());

            TableTuple tuple2 = m_table->lookupTupleByValues(prepareTempTuple(m_table, 42, i+total, "349508345.34583", "a thing", "a totally different thing altogether", i));
            ASSERT_TRUE(tuple2.isNullTuple());
        }

        return;
    }
    ASSERT_TRUE(false);
}

TEST_F(DRBinaryLogTest, DeleteOverBufferLimit) {
    createIndexes();
    const int total = 2000;
    int spHandle = 1;

    for (int i = 1; i <= total; i++, spHandle++) {
        beginTxn(m_engine, spHandle, spHandle, spHandle-1, spHandle);
        insertTuple(m_table, prepareTempTuple(m_table, 42, i, "349508345.34583", "a thing", "a totally different thing altogether", i));
        endTxn(m_engine, true);
    }

    flushAndApply(spHandle - 1);

    beginTxn(m_engine, spHandle, spHandle, spHandle-1, spHandle);
    try {
        // Delete all rows. It would overflow the DR buffer limit and cause the
        // txn to roll back.
        for (int i = 1; i <= total; i++) {
            TableTuple tuple = m_table->lookupTupleByValues(prepareTempTuple(m_table, 42, i, "349508345.34583", "a thing", "a totally different thing altogether", i));
            deleteTuple(m_table, tuple);
        }
    } catch (SerializableEEException& e) {
        endTxn(m_engine, false);
        spHandle++;

        // Make sure all changes rolled back, try to delete each tuple in single
        // txn to make sure indexes are also correct
        for (int i = 1; i <= total; i++, spHandle++) {
            beginTxn(m_engine, spHandle, spHandle, spHandle-1, spHandle);
            TableTuple tuple = m_table->lookupTupleByValues(prepareTempTuple(m_table, 42, i, "349508345.34583", "a thing", "a totally different thing altogether", i));
            ASSERT_FALSE(tuple.isNullTuple());

            deleteTuple(m_table, tuple);
            endTxn(m_engine, true);
        }

        return;
    }
    ASSERT_TRUE(false);
}

// This test doesn't run in the memcheck build because the tuple block can only
// hold one tuple at a time, so it will never trigger the optimized truncation
// path. The normal truncation path will fail because we don't have a catalog
// loaded.
TEST_F(DRBinaryLogTest, TruncateTable) {
#ifndef MEMCHECK
    createIndexes();
    const int total = 150;
    int spHandle = 1;

    for (int i = 1; i <= total; i++, spHandle++) {
        beginTxn(m_engine, spHandle, spHandle, spHandle-1, spHandle);
        insertTuple(m_table, prepareTempTuple(m_table, 42, i, "349508345.34583", "a thing", "a totally different thing altogether", i));
        endTxn(m_engine, true);
    }

    flushAndApply(spHandle - 1);
    EXPECT_EQ(total, m_table->activeTupleCount());
    EXPECT_EQ(total, m_tableReplica->activeTupleCount());

    beginTxn(m_engine, spHandle, spHandle, spHandle-1, spHandle);
    m_table->truncateTable(m_engine);
    endTxn(m_engine, true);

    flushAndApply(spHandle);
    EXPECT_EQ(0, m_table->activeTupleCount());
    EXPECT_EQ(0, m_tableReplica->activeTupleCount());
#endif
}

TEST_F(DRBinaryLogTest, MultiPartNoDataChange) {
    beginTxn(m_engine, 98, 98, 97, 69);
    endTxn(m_engine, true);
    ASSERT_FALSE(flush(98));
    ASSERT_EQ(0, m_topend.drBlocks.size());

    s_multiPartitionFlag = true;

    beginTxn(m_engine, 99, 99, 98, 70);
    endTxn(m_engine, true);
    ASSERT_TRUE(flush(99));

    EXPECT_EQ(0, m_table->activeTupleCount());
    EXPECT_EQ(0, m_tableReplica->activeTupleCount());
    ASSERT_EQ(2, m_topend.drBlocks.size());

    std::unique_ptr<CopySerializeInputLE> taskInfo(getDRTaskInfo());
    taskInfo->readByte(); // DR version
    DRRecordType type = static_cast<DRRecordType>(taskInfo->readByte());
    ASSERT_EQ(DR_RECORD_BEGIN_TXN, type);
    taskInfo->readLong(); // uniqueId
    taskInfo->readLong(); // sequenceNumber
    DRTxnPartitionHashFlag hashFlag =
            static_cast<DRTxnPartitionHashFlag>(taskInfo->readByte()); // hashFlag
    ASSERT_EQ(TXN_PAR_HASH_PLACEHOLDER, hashFlag);
    taskInfo->readInt(); // txnLength
    int32_t partitionHash = taskInfo->readInt(); // partitionHash
    ASSERT_TRUE(m_engine->isLocalSite(partitionHash)); // -1
    type = static_cast<DRRecordType>(taskInfo->readByte());
    ASSERT_EQ(DR_RECORD_END_TXN, type);

    applyNull();
    ASSERT_EQ(0, m_topend.drBlocks.size());

    beginTxn(m_engine, 100, 100, 99, 71);
    endTxn(m_engine, true);
    flushAndApply(100);

    EXPECT_EQ(0, m_table->activeTupleCount());
    EXPECT_EQ(0, m_tableReplica->activeTupleCount());
    ASSERT_EQ(0, m_topend.drBlocks.size());

    // read-only
    int64_t prevUndoToken = m_undoToken;
    m_undoToken = INT64_MAX;
    beginTxn(m_engine, 101, 101, 100, 72);
    endTxn(m_engine, true);
    ASSERT_FALSE(flush(101));
    ASSERT_EQ(0, m_topend.drBlocks.size());
    ASSERT_EQ(INT64_MAX, m_undoToken);
    m_undoToken = prevUndoToken;

    s_multiPartitionFlag = false;
}

TEST_F(DRBinaryLogTest, MissPartitionedExceptionIsThrown) {
    // replica hashinator puts everything in partition 1
    m_engineReplica->setHashinator(MockHashinator::newInstance(1));

    // Replicated table updates do not throw TXN_MISPARTITIONED
    beginTxn(m_engine, 99, 99, 98, 70);
    TableTuple source_tuple = insertTuple(m_replicatedTable, prepareTempTuple(m_replicatedTable, 99, 29058, "92384598.2342", "what", "really, why am I writing anything in these?", 3455));
    endTxn(m_engine, true);

    {
        ReplicaProcessContextSwitcher switcher;
        flushAndApply(99, true, true);
    }

    EXPECT_EQ(1, m_replicatedTableReplica->activeTupleCount());
    TableTuple tuple = m_replicatedTableReplica->lookupTupleForDR(source_tuple);
    ASSERT_FALSE(tuple.isNullTuple());

    // Partitioned table updates do throw TXN_MISPARTITIONED
    beginTxn(m_engine, 100, 100, 99, 71);
    insertTuple(m_table, prepareTempTuple(m_table, 42, 55555, "349508345.34583", "a thing", "this is a rather long string of text that is used to cause nvalue to use outline storage for the underlying data. It should be longer than 64 bytes.", 5433));
    insertTuple(m_table, prepareTempTuple(m_table, 24, 2321, "23455.5554", "and another", "this is starting to get even sillier", 2222));
    endTxn(m_engine, true);

    try {
        flushAndApply(100);
        FAIL("Should have thrown SerializableEEException");
    } catch (SerializableEEException &e) {
        ASSERT_EQ(VoltEEExceptionType::VOLT_EE_EXCEPTION_TYPE_TXN_MISPARTITIONED, e.getType());
    }
}

int main() {
#ifndef VOLT_POOL_CHECKING
    return TestSuite::globalInstance()->runAll();
#else
    // The pool checking machinery cannot handle
    // this test which attempts to simulate two clusters
    // within the same process.
    return 0;
#endif
}
