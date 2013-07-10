/* This file is part of VoltDB.
 * Copyright (C) 2008-2013 VoltDB Inc.
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
#include "common/RecoveryProtoMessage.h"
#include "common/ValueFactory.hpp"
#include "common/ValuePeeker.hpp"
#include "common/TupleOutputStream.h"
#include "common/TupleOutputStreamProcessor.h"
#include "execution/VoltDBEngine.h"
#include "expressions/expressions.h"
#include "storage/persistenttable.h"
#include "storage/tablefactory.h"
#include "storage/tableutil.h"
#include "indexes/tableindex.h"
#include "storage/tableiterator.h"
#include "storage/CopyOnWriteIterator.h"
#include "storage/ElasticScanner.h"
#include "storage/ElasticContext.h"
#include "stx/btree_set.h"
#include "common/DefaultTupleSerializer.h"
#include "jsoncpp/jsoncpp.h"
#include <vector>
#include <string>
#include <iostream>
#include <stdint.h>
#include <stdarg.h>
#include <boost/foreach.hpp>
#include <boost/ptr_container/ptr_vector.hpp>

using namespace voltdb;

/**
 * Counter for unique primary key values
 */
static int32_t m_primaryKeyIndex = 0;

// Selects extra-small quantity for debugging.
// Remember to comment out before checking in.
//#define EXTRA_SMALL

#if defined(EXTRA_SMALL)

// Extra small quantities for quick debugging runs.
const size_t TUPLE_COUNT = 10;
const size_t BUFFER_SIZE = 1024;
const size_t NUM_REPETITIONS = 2;
const size_t NUM_MUTATIONS = 5;

#elif defined(MEMCHECK)

// The smaller quantity is used for memcheck runs.
const size_t TUPLE_COUNT = 1000;
const size_t BUFFER_SIZE = 131072;
const size_t NUM_REPETITIONS = 10;
const size_t NUM_MUTATIONS = 10;

#else

// Normal/full run quantities.
const size_t TUPLE_COUNT = 174762;
const size_t BUFFER_SIZE = 131072;
const size_t NUM_REPETITIONS = 10;
const size_t NUM_MUTATIONS = 10;

#endif

// Maximum quantity for detailed error display.
const size_t MAX_DETAIL_COUNT = 50;

// Handy types and values.
typedef int64_t T_Value;
typedef stx::btree_set<T_Value> T_ValueSet;
typedef std::pair<int64_t, int64_t> T_HashRange;
typedef std::vector<T_HashRange> T_HashRangeVector;

/**
 * The strategy of this test is to create a table with 5 blocks of tuples with the first column (primary key)
 * sequentially numbered, serialize the whole thing to a block of memory, go COW and start serializing tuples
 * from the table while doing random updates, inserts, and deletes, then take that serialization output, sort it, and
 * then compare it to the original serialization output. They should be bit equivalent. Repeat this process another two
 * times.
 */
class CopyOnWriteTest : public Test {
public:
    CopyOnWriteTest() {
        m_tuplesInserted = 0;
        m_tuplesUpdated = 0;
        m_tuplesDeleted = 0;
        m_tuplesInsertedInLastUndo = 0;
        m_tuplesDeletedInLastUndo = 0;
        m_engine = new voltdb::VoltDBEngine();
        int partitionCount = 1;
        m_engine->initialize(1,1, 0, 0, "", DEFAULT_TEMP_TABLE_MEMORY, HASHINATOR_LEGACY, (char*)&partitionCount);

        m_columnNames.push_back("1");
        m_columnNames.push_back("2");
        m_columnNames.push_back("3");
        m_columnNames.push_back("4");
        m_columnNames.push_back("5");
        m_columnNames.push_back("6");
        m_columnNames.push_back("7");
        m_columnNames.push_back("8");
        m_columnNames.push_back("9");

        m_tableSchemaTypes.push_back(voltdb::VALUE_TYPE_INTEGER);
        m_tableSchemaTypes.push_back(voltdb::VALUE_TYPE_INTEGER);

        //Filler columns
        m_tableSchemaTypes.push_back(voltdb::VALUE_TYPE_BIGINT);
        m_tableSchemaTypes.push_back(voltdb::VALUE_TYPE_BIGINT);
        m_tableSchemaTypes.push_back(voltdb::VALUE_TYPE_BIGINT);
        m_tableSchemaTypes.push_back(voltdb::VALUE_TYPE_BIGINT);
        m_tableSchemaTypes.push_back(voltdb::VALUE_TYPE_BIGINT);
        m_tableSchemaTypes.push_back(voltdb::VALUE_TYPE_BIGINT);
        m_tableSchemaTypes.push_back(voltdb::VALUE_TYPE_BIGINT);

        m_tupleWidth = (sizeof(int32_t) * 2) + (sizeof(int64_t) * 7);

        m_tableSchemaColumnSizes.push_back(NValue::getTupleStorageSize(voltdb::VALUE_TYPE_INTEGER));
        m_tableSchemaColumnSizes.push_back(NValue::getTupleStorageSize(voltdb::VALUE_TYPE_INTEGER));

        m_tableSchemaColumnSizes.push_back(NValue::getTupleStorageSize(voltdb::VALUE_TYPE_BIGINT));
        m_tableSchemaColumnSizes.push_back(NValue::getTupleStorageSize(voltdb::VALUE_TYPE_BIGINT));
        m_tableSchemaColumnSizes.push_back(NValue::getTupleStorageSize(voltdb::VALUE_TYPE_BIGINT));
        m_tableSchemaColumnSizes.push_back(NValue::getTupleStorageSize(voltdb::VALUE_TYPE_BIGINT));
        m_tableSchemaColumnSizes.push_back(NValue::getTupleStorageSize(voltdb::VALUE_TYPE_BIGINT));
        m_tableSchemaColumnSizes.push_back(NValue::getTupleStorageSize(voltdb::VALUE_TYPE_BIGINT));
        m_tableSchemaColumnSizes.push_back(NValue::getTupleStorageSize(voltdb::VALUE_TYPE_BIGINT));

        m_tableSchemaAllowNull.push_back(false);
        m_tableSchemaAllowNull.push_back(false);
        m_tableSchemaAllowNull.push_back(false);
        m_tableSchemaAllowNull.push_back(false);
        m_tableSchemaAllowNull.push_back(false);
        m_tableSchemaAllowNull.push_back(false);
        m_tableSchemaAllowNull.push_back(false);
        m_tableSchemaAllowNull.push_back(false);
        m_tableSchemaAllowNull.push_back(false);
        m_tableSchemaAllowNull.push_back(false);

        m_primaryKeyIndexColumns.push_back(0);

        m_undoToken = 0;

        m_tableId = 0;
    }

    ~CopyOnWriteTest() {
        delete m_engine;
        delete m_table;
    }

    void initTable(bool allowInlineStrings, int tableAllocationTargetSize = 0) {
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
                voltdb::TableFactory::getPersistentTable(m_tableId, "Foo", m_tableSchema,
                                                         m_columnNames, 0, false, false,
                                                         tableAllocationTargetSize));

        TableIndex *pkeyIndex = TableIndexFactory::TableIndexFactory::getInstance(indexScheme);
        assert(pkeyIndex);
        m_table->addIndex(pkeyIndex);
        m_table->setPrimaryKeyIndex(pkeyIndex);
    }

    void addRandomUniqueTuples(Table *table, int numTuples, T_ValueSet *set = NULL) {
        TableTuple tuple = table->tempTuple();
        ::memset(tuple.address() + 1, 0, tuple.tupleLength() - 1);
        for (int ii = 0; ii < numTuples; ii++) {
            int value = rand();
            tuple.setNValue(0, ValueFactory::getIntegerValue(m_primaryKeyIndex++));
            tuple.setNValue(1, ValueFactory::getIntegerValue(value));
            bool success = table->insertTuple(tuple);
            if (!success) {
                std::cout << "Failed to add random unique tuple" << std::endl;
                return;
            }
            if (set != NULL) {
                set->insert(*reinterpret_cast<int64_t*>(tuple.address() + 1));
            }
        }
    }

    void doRandomUndo() {
        int rand = ::rand();
        int op = rand % 2;
        switch (op) {

                /*
                 * Undo the last quantum
                 */
            case 0: {
                m_engine->undoUndoToken(m_undoToken);
                m_tuplesDeleted -= m_tuplesDeletedInLastUndo;
                m_tuplesInserted -= m_tuplesInsertedInLastUndo;
                break;
            }

                /*
                 * Release the last quantum
                 */
            case 1: {
                m_engine->releaseUndoToken(m_undoToken);
                break;
            }
        }
        m_engine->setUndoToken(++m_undoToken);
        m_engine->getExecutorContext()->setupForPlanFragments(m_engine->getCurrentUndoQuantum(), 0, 0, 0);
        m_tuplesDeletedInLastUndo = 0;
        m_tuplesInsertedInLastUndo = 0;
    }

    void doRandomDelete(PersistentTable *table, T_ValueSet *set = NULL) {
        TableTuple tuple(table->schema());
        if (tableutil::getRandomTuple(table, tuple)) {
            if (set != NULL) {
                set->insert(*reinterpret_cast<int64_t*>(tuple.address() + 1));
            }
            table->deleteTuple(tuple, true);
            m_tuplesDeleted++;
            m_tuplesDeletedInLastUndo++;
        }
    }

    void doRandomInsert(PersistentTable *table, T_ValueSet *set = NULL) {
        addRandomUniqueTuples(table, 1, set);
        m_tuplesInserted++;
        m_tuplesInsertedInLastUndo++;
    }

    void doRandomUpdate(PersistentTable *table, T_ValueSet *setFrom = NULL, T_ValueSet *setTo = NULL) {
        voltdb::TableTuple tuple(table->schema());
        voltdb::TableTuple tempTuple = table->tempTuple();
        if (tableutil::getRandomTuple(table, tuple)) {
            tempTuple.copy(tuple);
            int value = ::rand();
            tempTuple.setNValue(1, ValueFactory::getIntegerValue(value));
            if (setFrom != NULL) {
                setFrom->insert(*reinterpret_cast<int64_t*>(tuple.address() + 1));
            }
            if (setTo != NULL) {
                setTo->insert(*reinterpret_cast<int64_t*>(tempTuple.address() + 1));
            }
            table->updateTuple(tuple, tempTuple);
            m_tuplesUpdated++;
        }
    }

    void doRandomTableMutation(PersistentTable *table) {
        int rand = ::rand();
        int op = rand % 3;
        switch (op) {

            /*
             * Delete a tuple
             */
            case 0: {
                doRandomDelete(table);
                break;
            }

            /*
             * Insert a tuple
             */
            case 1: {
                doRandomInsert(table);
                break;
            }

            /*
             * Update a random tuple
             */
            case 2: {
                doRandomUpdate(table);
                break;
            }

            default:
                assert(false);
                break;
        }
    }

    void doForcedCompaction(PersistentTable *table) {
        table->doForcedCompaction();
    }

    void checkTuples(size_t tupleCount, T_ValueSet& expected, T_ValueSet& received) {
        std::vector<int64_t> diff;
        std::insert_iterator<std::vector<int64_t> > ii( diff, diff.begin());
        std::set_difference(expected.begin(), expected.end(), received.begin(), received.end(), ii);
        for (int ii = 0; ii < diff.size(); ii++) {
            int32_t *values = reinterpret_cast<int32_t*>(&diff[ii]);
            printf("Expected tuple was not received: %d/%d\n", values[0], values[1]);
        }

        diff.clear();
        ii = std::insert_iterator<std::vector<int64_t> >(diff, diff.begin());
        std::set_difference( received.begin(), received.end(), expected.begin(), expected.end(), ii);
        for (int ii = 0; ii < diff.size(); ii++) {
            int32_t *values = reinterpret_cast<int32_t*>(&diff[ii]);
            printf("Unexpected tuple received: %d/%d\n", values[0], values[1]);
        }

        size_t numTuples = 0;
        voltdb::TableIterator& iterator = m_table->iterator();
        TableTuple tuple(m_table->schema());
        while (iterator.next(tuple)) {
            if (tuple.isDirty()) {
                printf("Tuple %d is active and dirty\n",
                       ValuePeeker::peekAsInteger(tuple.getNValue(0)));
            }
            numTuples++;
            if (tuple.isDirty()) {
                printf("Dirty tuple is %p, %d, %d\n", tuple.address(), ValuePeeker::peekAsInteger(tuple.getNValue(0)), ValuePeeker::peekAsInteger(tuple.getNValue(1)));
            }
            ASSERT_FALSE(tuple.isDirty());
        }
        if (tupleCount > 0 and numTuples != tupleCount) {
            printf("Expected %lu tuples, received %lu\n", numTuples, tupleCount);
            ASSERT_EQ(numTuples, tupleCount);
        }

        ASSERT_EQ(expected.size(), received.size());
        ASSERT_TRUE(expected == received);
    }

    void getTableValueSet(T_ValueSet &set) {
        voltdb::TableIterator& iterator = m_table->iterator();
        TableTuple tuple(m_table->schema());
        while (iterator.next(tuple)) {
            const std::pair<T_ValueSet::iterator, bool> p =
                    set.insert(*reinterpret_cast<int64_t*>(tuple.address() + 1));
            const bool inserted = p.second;
            if (!inserted) {
                int32_t primaryKey = ValuePeeker::peekAsInteger(tuple.getNValue(0));
                printf("Failed to insert %d\n", primaryKey);
            }
            if (!inserted) {
                ASSERT_TRUE(inserted);
            }
        }
    }

    // Avoid the need to make each individual test a friend by exposing
    // PersistentTable privates from here. Tests should call these methods
    // instead of adding them as friends.

    TBMap &getTableData() {
        return m_table->m_data;
    }

    boost::unordered_set<TBPtr> &getBlocksPendingSnapshot() {
        return m_table->m_blocksPendingSnapshot;
    }

    boost::unordered_set<TBPtr> &getBlocksNotPendingSnapshot() {
        return m_table->m_blocksNotPendingSnapshot;
    }

    TBBucketMap &getBlocksPendingSnapshotLoad() {
        return m_table->m_blocksPendingSnapshotLoad;
    }

    TBBucketMap &getBlocksNotPendingSnapshotLoad() {
        return m_table->m_blocksNotPendingSnapshotLoad;
    }

    bool activateStreamInternal(CatalogId tableId, boost::shared_ptr<TableStreamerInterface> streamer) {
        return m_table->activateStreamInternal(m_tableId, streamer);
    }

    voltdb::VoltDBEngine *m_engine;
    voltdb::TupleSchema *m_tableSchema;
    voltdb::PersistentTable *m_table;
    std::vector<std::string> m_columnNames;
    std::vector<voltdb::ValueType> m_tableSchemaTypes;
    std::vector<int32_t> m_tableSchemaColumnSizes;
    std::vector<bool> m_tableSchemaAllowNull;
    std::vector<int> m_primaryKeyIndexColumns;
    DefaultTupleSerializer m_serializer;

    int32_t m_tuplesInserted;
    int32_t m_tuplesUpdated;
    int32_t m_tuplesDeleted;

    int32_t m_tuplesInsertedInLastUndo;
    int32_t m_tuplesDeletedInLastUndo;

    int64_t m_undoToken;

    int32_t m_tupleWidth;

    CatalogId m_tableId;
};

TEST_F(CopyOnWriteTest, CopyOnWriteIterator) {
    initTable(true);

    int tupleCount = TUPLE_COUNT;
    addRandomUniqueTuples( m_table, tupleCount);

    voltdb::TableIterator& iterator = m_table->iterator();
    TBMap blocks(getTableData());
    getBlocksPendingSnapshot().swap(getBlocksNotPendingSnapshot());
    getBlocksPendingSnapshotLoad().swap(getBlocksNotPendingSnapshotLoad());
    voltdb::CopyOnWriteIterator COWIterator(m_table, blocks.begin(), blocks.end());
    TableTuple tuple(m_table->schema());
    TableTuple COWTuple(m_table->schema());

    int iteration = 0;
    while (true) {
        iteration++;
        if (!iterator.next(tuple)) {
            break;
        }
        ASSERT_TRUE(COWIterator.next(COWTuple));

        if (tuple.address() != COWTuple.address()) {
            printf("Failed in iteration %d with %p and %p\n", iteration, tuple.address(), COWTuple.address());
        }
        ASSERT_EQ(tuple.address(), COWTuple.address());
    }
    ASSERT_FALSE(COWIterator.next(COWTuple));
}

TEST_F(CopyOnWriteTest, TestTableTupleFlags) {
    initTable(true);
    char storage[9];
    std::memset(storage, 0, 9);
    TableTuple tuple(m_table->schema());
    tuple.move(storage);

    tuple.setActiveFalse();
    tuple.setDirtyTrue();
    ASSERT_FALSE(tuple.isActive());
    ASSERT_TRUE(tuple.isDirty());

    tuple.setActiveTrue();
    ASSERT_TRUE(tuple.isDirty());
    ASSERT_TRUE(tuple.isActive());

    tuple.setDirtyFalse();
    ASSERT_TRUE(tuple.isActive());
    ASSERT_FALSE(tuple.isDirty());
}

TEST_F(CopyOnWriteTest, BigTest) {
    initTable(true);
    int tupleCount = TUPLE_COUNT;
    addRandomUniqueTuples( m_table, tupleCount);
    for (int qq = 0; qq < NUM_REPETITIONS; qq++) {
        T_ValueSet originalTuples;
        getTableValueSet(originalTuples);

        char config[4];
        ::memset(config, 0, 4);
        ReferenceSerializeInput input(config, 4);

        m_table->activateStream(m_serializer, TABLE_STREAM_SNAPSHOT, 0, m_tableId, input);

        T_ValueSet COWTuples;
        char serializationBuffer[BUFFER_SIZE];
        int totalInserted = 0;
        while (true) {
            TupleOutputStreamProcessor outputStreams(serializationBuffer, sizeof(serializationBuffer));
            TupleOutputStream &outputStream = outputStreams.at(0);
            std::vector<int> retPositions;
            int64_t remaining = m_table->streamMore(outputStreams, retPositions);
            if (remaining >= 0) {
                ASSERT_EQ(outputStreams.size(), retPositions.size());
            }
            const int serialized = static_cast<int>(outputStream.position());
            if (serialized == 0) {
                break;
            }
            int ii = 12;//skip partition id and row count and first tuple length
            while (ii < (serialized - 4)) {
                int32_t values[2];
                values[0] = ntohl(*reinterpret_cast<int32_t*>(&serializationBuffer[ii]));
                values[1] = ntohl(*reinterpret_cast<int32_t*>(&serializationBuffer[ii + 4]));
                // the following rediculous cast is to placate our gcc treat warnings as errors affliction
                void *valuesVoid = reinterpret_cast<void*>(values);
                int64_t *values64 = reinterpret_cast<int64_t*>(valuesVoid);
                const bool inserted = COWTuples.insert(*values64).second;
                if (!inserted) {
                    printf("Failed in iteration %d, total inserted %d, with values %d and %d\n", qq, totalInserted, values[0], values[1]);
                }
                ASSERT_TRUE(inserted);
                totalInserted++;
                ii += static_cast<int>(m_tupleWidth + sizeof(int32_t));
            }
            for (int jj = 0; jj < NUM_MUTATIONS; jj++) {
                doRandomTableMutation(m_table);
            }
        }

        checkTuples(tupleCount + (m_tuplesInserted - m_tuplesDeleted), originalTuples, COWTuples);
    }
}

TEST_F(CopyOnWriteTest, BigTestWithUndo) {
    initTable(true);
    int tupleCount = TUPLE_COUNT;
    addRandomUniqueTuples( m_table, tupleCount);
    m_engine->setUndoToken(0);
    m_engine->getExecutorContext()->setupForPlanFragments(m_engine->getCurrentUndoQuantum(), 0, 0, 0);
    for (int qq = 0; qq < NUM_REPETITIONS; qq++) {
        T_ValueSet originalTuples;
        voltdb::TableIterator& iterator = m_table->iterator();
        TableTuple tuple(m_table->schema());
        while (iterator.next(tuple)) {
            const std::pair<T_ValueSet::iterator, bool> p =
            originalTuples.insert(*reinterpret_cast<int64_t*>(tuple.address() + 1));
            const bool inserted = p.second;
            if (!inserted) {
                int32_t primaryKey = ValuePeeker::peekAsInteger(tuple.getNValue(0));
                printf("Failed to insert %d\n", primaryKey);
            }
            ASSERT_TRUE(inserted);
        }

        char config[4];
        ::memset(config, 0, 4);
        ReferenceSerializeInput input(config, 4);
        m_table->activateStream(m_serializer, TABLE_STREAM_SNAPSHOT, 0, m_tableId, input);

        T_ValueSet COWTuples;
        char serializationBuffer[BUFFER_SIZE];
        int totalInserted = 0;
        while (true) {
            TupleOutputStreamProcessor outputStreams(serializationBuffer, sizeof(serializationBuffer));
            TupleOutputStream &outputStream = outputStreams.at(0);
            std::vector<int> retPositions;
            int64_t remaining = m_table->streamMore(outputStreams, retPositions);
            if (remaining >= 0) {
                ASSERT_EQ(outputStreams.size(), retPositions.size());
            }
            const int serialized = static_cast<int>(outputStream.position());
            if (serialized == 0) {
                break;
            }
            int ii = 12;//skip partition id and row count and first tuple length
            while (ii < (serialized - 4)) {
                int values[2];
                values[0] = ntohl(*reinterpret_cast<int32_t*>(&serializationBuffer[ii]));
                values[1] = ntohl(*reinterpret_cast<int32_t*>(&serializationBuffer[ii + 4]));
                // the following rediculous cast is to placate our gcc treat warnings as errors affliction
                void *valuesVoid = reinterpret_cast<void*>(values);
                int64_t *values64 = reinterpret_cast<int64_t*>(valuesVoid);
                const bool inserted = COWTuples.insert(*values64).second;
                if (!inserted) {
                    printf("Failed in iteration %d with values %d and %d\n", totalInserted, values[0], values[1]);
                }
                ASSERT_TRUE(inserted);
                totalInserted++;
                ii += static_cast<int>(m_tupleWidth + sizeof(int32_t));
            }
            for (int jj = 0; jj < NUM_MUTATIONS; jj++) {
                doRandomTableMutation(m_table);
            }
            doRandomUndo();
        }

        checkTuples(tupleCount + (m_tuplesInserted - m_tuplesDeleted), originalTuples, COWTuples);
    }
}

TEST_F(CopyOnWriteTest, BigTestUndoEverything) {
    initTable(true);
    int tupleCount = TUPLE_COUNT;
    addRandomUniqueTuples( m_table, tupleCount);
    m_engine->setUndoToken(0);
    m_engine->getExecutorContext()->setupForPlanFragments(m_engine->getCurrentUndoQuantum(), 0, 0, 0);
    for (int qq = 0; qq < NUM_REPETITIONS; qq++) {
        T_ValueSet originalTuples;
        voltdb::TableIterator& iterator = m_table->iterator();
        TableTuple tuple(m_table->schema());
        while (iterator.next(tuple)) {
            const std::pair<T_ValueSet::iterator, bool> p =
            originalTuples.insert(*reinterpret_cast<int64_t*>(tuple.address() + 1));
            const bool inserted = p.second;
            if (!inserted) {
                int32_t primaryKey = ValuePeeker::peekAsInteger(tuple.getNValue(0));
                printf("Failed to insert %d\n", primaryKey);
            }
            ASSERT_TRUE(inserted);
        }

        char config[4];
        ::memset(config, 0, 4);
        ReferenceSerializeInput input(config, 4);
        m_table->activateStream(m_serializer, TABLE_STREAM_SNAPSHOT, 0, m_tableId, input);

        T_ValueSet COWTuples;
        char serializationBuffer[BUFFER_SIZE];
        int totalInserted = 0;
        while (true) {
            TupleOutputStreamProcessor outputStreams(serializationBuffer, sizeof(serializationBuffer));
            TupleOutputStream &outputStream = outputStreams.at(0);
            std::vector<int> retPositions;
            int64_t remaining = m_table->streamMore(outputStreams, retPositions);
            if (remaining >= 0) {
                ASSERT_EQ(outputStreams.size(), retPositions.size());
            }
            const int serialized = static_cast<int>(outputStream.position());
            if (serialized == 0) {
                break;
            }
            int ii = 12;//skip partition id and row count and first tuple length
            while (ii < (serialized - 4)) {
                int values[2];
                values[0] = ntohl(*reinterpret_cast<int32_t*>(&serializationBuffer[ii]));
                values[1] = ntohl(*reinterpret_cast<int32_t*>(&serializationBuffer[ii + 4]));
                // the following rediculous cast is to placate our gcc treat warnings as errors affliction
                void *valuesVoid = reinterpret_cast<void*>(values);
                int64_t *values64 = reinterpret_cast<int64_t*>(valuesVoid);
                const bool inserted = COWTuples.insert(*values64).second;
                if (!inserted) {
                    printf("Failed in iteration %d with values %d and %d\n", totalInserted, values[0], values[1]);
                }
                ASSERT_TRUE(inserted);
                totalInserted++;
                ii += static_cast<int>(m_tupleWidth + sizeof(int32_t));
            }
            for (int jj = 0; jj < NUM_MUTATIONS; jj++) {
                doRandomTableMutation(m_table);
            }
            m_engine->undoUndoToken(m_undoToken);
            m_engine->setUndoToken(++m_undoToken);
            m_engine->getExecutorContext()->setupForPlanFragments(m_engine->getCurrentUndoQuantum(), 0, 0, 0);
        }

        checkTuples(0, originalTuples, COWTuples);
    }
}

/** Tool object holds test state and conveniently displays errors. */
class MultiStreamTestTool {
public:
    MultiStreamTestTool(PersistentTable& table, size_t nparts) :
        table(table),
        nparts(nparts),
        iteration(-1),
        nerrors(0),
        showTuples(TUPLE_COUNT <= MAX_DETAIL_COUNT)
    {
        strcpy(stage, "Initialize");
        TableTuple tuple(table.schema());
        size_t i = 0;
        voltdb::TableIterator& iterator = table.iterator();
        while (iterator.next(tuple)) {
            int64_t value = *reinterpret_cast<int64_t*>(tuple.address() + 1);
            values.push_back(value);
            valueSet.insert(std::pair<int64_t,size_t>(value, i++));
        }
    }

    void iterate() {
        iteration++;
    }

    void context(const std::string& msg, ...) {
        va_list args;
        va_start(args, msg);
        vsnprintf(stage, sizeof stage, msg.c_str(), args);
        va_end(args);
    }

    void verror(const std::string& msg, va_list args) {
        char buffer[256];
        vsnprintf(buffer, sizeof buffer, msg.c_str(), args);
        if (nerrors++ == 0) {
            std::cerr << std::endl;
        }
        std::cerr << "ERROR(";
        if (iteration >= 0) {
            std::cerr << "iteration=" << iteration << ": ";
        }
        std::cerr << stage << "): " << buffer << std::endl;
    }

    void error(const std::string& msg, ...) {
        va_list args;
        va_start(args, msg);
        verror(msg, args);
        va_end(args);
    }

    void valueError(int32_t* pvalues, const std::string& msg, ...) {
        if (showTuples) {
            std::cerr << std::endl << "=== Tuples ===" << std::endl;
            size_t n = 0;
            for (std::vector<int64_t>::iterator i = values.begin(); i != values.end(); ++i) {
                std::cerr << ++n << " " << *i << std::endl;
            }
            std::cerr << std::endl;
            showTuples = false;
        }
        int64_t value = *reinterpret_cast<int64_t*>(pvalues);
        std::ostringstream os;
        os << msg << " value=" << value << "(" << pvalues[0] << "," << pvalues[1] << ") index=";
        std::map<int64_t,size_t>::const_iterator ifound = valueSet.find(value);
        if (ifound != valueSet.end()) {
            os << ifound->second;
        }
        else {
            os << "???";
        }
        os << " modulus=" << value % nparts;
        va_list args;
        va_start(args, msg);
        verror(os.str(), args);
        va_end(args);
    }

    void diff(T_ValueSet set1, T_ValueSet set2) {
        std::vector<int64_t> diff;
        std::insert_iterator<std::vector<int64_t> > idiff( diff, diff.begin());
        std::set_difference(set1.begin(), set1.end(), set2.begin(), set2.end(), idiff);
        if (diff.size() <= MAX_DETAIL_COUNT) {
            for (int ii = 0; ii < diff.size(); ii++) {
                int32_t *values = reinterpret_cast<int32_t*>(&diff[ii]);
                valueError(values, "tuple");
            }
        }
        else {
            error("(%lu tuples)", diff.size());
        }
    }

    //=== Some convenience methods for building a JSON expression.

    // Assume non-ridiculous copy semantics for Object.
    // Structured JSON-building for readibility, not efficiency.

    static Json::Value expr_value_base(const std::string& type) {
        Json::Value value;
        value["TYPE"] = "VALUE_CONSTANT";
        value["VALUE_TYPE"] = type;
        value["VALUE_SIZE"] = 0;
        value["ISNULL"] = false;
        return value;
    }

    static Json::Value expr_value(const std::string& type, std::string& key, int data) {
        Json::Value value = expr_value_base(type);
        value[key.c_str()] = data;
        return value;
    }

    static Json::Value expr_value(const std::string& type, int ivalue) {
        std::string key = "VALUE";
        return expr_value(type, key, ivalue);
    }

    static Json::Value expr_value_tuple(const std::string& type,
                                        const std::string& tblname,
                                        int32_t colidx,
                                        const std::string& colname)
    {
        Json::Value value;
        value["TYPE"] = "VALUE_TUPLE";
        value["VALUE_TYPE"] = type;
        value["VALUE_SIZE"] = 0;
        value["TABLE_NAME"] = tblname;
        value["COLUMN_IDX"] = colidx;
        value["COLUMN_NAME"] = colname;
        value["COLUMN_ALIAS"] = Json::nullValue; // null
        return value;
    }

    static Json::Value expr_binary_op(const std::string& op,
                                      const std::string& type,
                                      const Json::Value& left,
                                      const Json::Value& right)
    {
        Json::Value value;
        value["TYPE"] = op;
        value["VALUE_TYPE"] = type;
        value["VALUE_SIZE"] = 0;
        value["LEFT"] = left;
        value["RIGHT"] = right;
        return value;
    }

    // Work around unsupported modulus operator with other integer operators:
    //    Should be: result = (value % nparts) == ipart
    //  Work-around: result = (value - ((value / nparts) * nparts)) == ipart
    std::string generatePredicateString(int32_t ipart, bool deleteForPredicate) {
        std::string tblname = table.name();
        int colidx = table.partitionColumn();
        std::string colname = table.columnName(colidx);
        Json::Value jsonTuple = expr_value_tuple("INTEGER", tblname, colidx, colname);
        Json::Value json =
        expr_binary_op("COMPARE_EQUAL", "INTEGER",
                       expr_binary_op("OPERATOR_MINUS", "INTEGER",
                                      jsonTuple,
                                      expr_binary_op("OPERATOR_MULTIPLY", "INTEGER",
                                                     expr_binary_op("OPERATOR_DIVIDE", "INTEGER",
                                                                    jsonTuple,
                                                                    expr_value("INTEGER", (int)nparts)),
                                                     expr_value("INTEGER", (int)nparts)
                                                     )
                                      ),
                       expr_value("INTEGER", (int)ipart)
                       );

        Json::Value predicateStuff;
        predicateStuff["triggersDelete"] = deleteForPredicate;
        predicateStuff["predicateExpression"] = json;

        Json::FastWriter writer;
        return writer.write(predicateStuff);
    }

    std::string generateHashRangePredicate(T_HashRangeVector& ranges) {
        int colidx = table.partitionColumn();
        Json::Value json;
        std::string op = expressionToString(EXPRESSION_TYPE_HASH_RANGE);
        json["TYPE"] = op;
        json["VALUE_TYPE"] = valueToString(VALUE_TYPE_BIGINT);
        json["VALUE_SIZE"] = 8;
        json["HASH_COLUMN"] = colidx;
        Json::Value array;
        for (size_t i = 0; i < ranges.size(); i++) {
            Json::Value range;
            range["RANGE_START"] = static_cast<Json::Int64>(ranges[i].first);
            range["RANGE_END"] = static_cast<Json::Int64>(ranges[i].second);
            array.append(range);
        }
        json["RANGES"] = array;
        Json::Value predicateStuff;
        predicateStuff["triggersDelete"] = false;
        predicateStuff["predicateExpression"] = json;

        Json::FastWriter writer;
        return writer.write(predicateStuff);
    }

    PersistentTable& table;
    size_t nparts;
    int iteration;
    char stage[256];
    size_t nerrors;
    std::vector<int64_t> values;
    std::map<int64_t,size_t> valueSet;
    bool showTuples;
};

/**
 * Exercise the multi-COW.
 */
TEST_F(CopyOnWriteTest, MultiStreamTest) {

    // Constants
    const int32_t npartitions = 7;
    const int tupleCount = TUPLE_COUNT;

    initTable(true);
    addRandomUniqueTuples(m_table, tupleCount);

    MultiStreamTestTool tool(*m_table, npartitions);

    for (size_t iteration = 0; iteration < NUM_REPETITIONS; iteration++) {

        // The last repetition does the delete after streaming.
        bool doDelete = (iteration == NUM_REPETITIONS - 1);

        tool.iterate();

        int totalInserted = 0;              // Total tuple counter.
        boost::scoped_ptr<char> buffers[npartitions];   // Stream buffers.
        std::vector<std::string> strings(npartitions);  // Range strings.
        T_ValueSet expected[npartitions]; // Expected tuple values by partition.
        T_ValueSet actual[npartitions];   // Actual tuple values by partition.
        int totalSkipped = 0;

        // Prepare streams by generating ranges and range strings based on
        // the desired number of partitions/predicates.
        // Since integer hashes use a simple modulus we just need to provide
        // the partition number for the range.
        // Also prepare a buffer for each stream.
        // Skip one partition to make it interesting.
        int32_t skippedPartition = npartitions / 2;
        for (int32_t i = 0; i < npartitions; i++) {
            buffers[i].reset(new char[BUFFER_SIZE]);
            if (i != skippedPartition) {
                strings[i] = tool.generatePredicateString(i, doDelete);
            }
            else {
                strings[i] = tool.generatePredicateString(-1, doDelete);
            }
        }

        char buffer[1024 * 256];
        ReferenceSerializeOutput output(buffer, 1024 * 256);
        output.writeInt(npartitions);
        for (std::vector<std::string>::iterator i = strings.begin(); i != strings.end(); i++) {
            output.writeTextString(*i);
        }

        tool.context("precalculate");

        // Map original tuples to expected partitions.
        voltdb::TableIterator& iterator = m_table->iterator();
        int partCol = m_table->partitionColumn();
        TableTuple tuple(m_table->schema());
        while (iterator.next(tuple)) {
            int64_t value = *reinterpret_cast<int64_t*>(tuple.address() + 1);
            int32_t ipart = (int32_t)(ValuePeeker::peekAsRawInt64(tuple.getNValue(partCol)) % npartitions);
            if (ipart != skippedPartition) {
                bool inserted = expected[ipart].insert(value).second;
                if (!inserted) {
                    int32_t primaryKey = ValuePeeker::peekAsInteger(tuple.getNValue(0));
                    tool.error("Duplicate primary key %d iteration=%lu", primaryKey, iteration);
                }
                ASSERT_TRUE(inserted);
            }
            else {
                totalSkipped++;
            }
        }

        tool.context("activate");

        ReferenceSerializeInput input(buffer, output.position());
        bool alreadyActivated = m_table->activateStream(m_serializer, TABLE_STREAM_SNAPSHOT, 0, m_tableId, input);
        if (alreadyActivated) {
            tool.error("COW was previously activated");
        }
        ASSERT_FALSE(alreadyActivated);

        int64_t remaining = tupleCount;
        while (remaining > 0) {

            // Prepare output streams and their buffers.
            TupleOutputStreamProcessor outputStreams;
            for (int32_t i = 0; i < npartitions; i++) {
                outputStreams.add((void*)buffers[i].get(), BUFFER_SIZE);
            }

            std::vector<int> retPositions;
            remaining = m_table->streamMore(outputStreams, retPositions);
            if (remaining >= 0) {
                ASSERT_EQ(outputStreams.size(), retPositions.size());
            }

            // Per-predicate iterators.
            TupleOutputStreamProcessor::iterator outputStream = outputStreams.begin();

            // Record the final result of streaming to each partition/predicate.
            for (size_t ipart = 0; ipart < npartitions; ipart++) {

                tool.context("serialize: partition=%lu remaining=%lld", ipart, remaining);

                const int serialized = static_cast<int>(outputStream->position());
                if (serialized > 0) {
                    // Skip partition id, row count and first tuple length.
                    int ibuf = sizeof(int32_t) * 3;
                    while (ibuf < (serialized - sizeof(int32_t))) {
                        int32_t values[2];
                        values[0] = ntohl(*reinterpret_cast<const int32_t*>(buffers[ipart].get()+ibuf));
                        values[1] = ntohl(*reinterpret_cast<const int32_t*>(buffers[ipart].get()+ibuf+4));
                        // the following rediculous cast is to placate our gcc treat warnings as errors affliction
                        void *valuesVoid = reinterpret_cast<void*>(values);
                        int64_t *values64 = reinterpret_cast<int64_t*>(valuesVoid);
                        const bool inserted = actual[ipart].insert(*values64).second;
                        if (!inserted) {
                            tool.valueError(values, "Buffer duplicate: ipart=%lu totalInserted=%d ibuf=%d",
                                            ipart, totalInserted, ibuf);
                        }
                        ASSERT_TRUE(inserted);

                        totalInserted++;

                        // Account for tuple data and second tuple length.
                        ibuf += static_cast<int>(m_tupleWidth + sizeof(int32_t));
                    }
                }

                // Mozy along to the next predicate/partition.
                // Do a silly cross-check that the iterator doesn't end prematurely.
                ++outputStream;
                ASSERT_TRUE(ipart == npartitions - 1 || outputStream != outputStreams.end());
            }

            // Mutate the table.
            if (!doDelete) {
                for (size_t imutation = 0; imutation < NUM_MUTATIONS; imutation++) {
                    doRandomTableMutation(m_table);
                }
            }
        }

        // Summarize partitions with incorrect tuple counts.
        for (size_t ipart = 0; ipart < npartitions; ipart++) {
            tool.context("check size: partition=%lu", ipart);
            if (expected[ipart].size() != actual[ipart].size()) {
                tool.error("Size mismatch: expected=%lu actual=%lu",
                           expected[ipart].size(), actual[ipart].size());
            }
        }

        // Summarize partitions where expected and actual aren't equal.
        for (size_t ipart = 0; ipart < npartitions; ipart++) {
            tool.context("check equality: partition=%lu", ipart);
            if (expected[ipart] != actual[ipart]) {
                tool.error("Not equal");
            }
        }

        // Look for tuples that are missing from partitions.
        for (size_t ipart = 0; ipart < npartitions; ipart++) {
            tool.context("missing: partition=%lu", ipart);
            tool.diff(expected[ipart], actual[ipart]);
        }

        // Look for extra tuples that don't belong in partitions.
        for (size_t ipart = 0; ipart < npartitions; ipart++) {
            tool.context("extra: partition=%lu", ipart);
            tool.diff(actual[ipart], expected[ipart]);
        }

        // Check tuple diff for each predicate/partition.
        for (size_t ipart = 0; ipart < npartitions; ipart++) {
            tool.context("check equality: partition=%lu", ipart);
            ASSERT_EQ(expected[ipart].size(), actual[ipart].size());
            ASSERT_TRUE(expected[ipart] == actual[ipart]);
        }

        // Check for dirty tuples.
        tool.context("check dirty");
        int numTuples = 0;
        iterator = m_table->iterator();
        while (iterator.next(tuple)) {
            if (tuple.isDirty()) {
                tool.error("Found tuple %d is active and dirty at end of COW",
                           ValuePeeker::peekAsInteger(tuple.getNValue(0)));
            }
            numTuples++;
            ASSERT_FALSE(tuple.isDirty());
        }

        // If deleting check the tuples remaining in the table.
        if (doDelete) {
            ASSERT_EQ(numTuples, totalSkipped);
        }
        else {
            ASSERT_EQ(numTuples, tupleCount + (m_tuplesInserted - m_tuplesDeleted));
        }
        ASSERT_EQ(tool.nerrors, 0);
    }
}

/*
 * Test for the ENG-4524 edge condition where serializeMore() yields on
 * precisely the last tuple which had caused the loop to skip the last call to
 * the iterator next() method. Need to rig this test with the appropriate
 * buffer size and tuple count to force the edge condition.
 *
 * The buffer has to be a smidge larger than what is needed to hold the tuples
 * so that TupleOutputStreamProcessor::writeRow() discovers it can't fit
 * another tuple immediately after writing the last one. It doesn't know how
 * many there are so it yields even if no more tuples will be delivered.
 */
TEST_F(CopyOnWriteTest, BufferBoundaryCondition) {
    const size_t tupleCount = 3;
    const size_t bufferSize = 12 + ((m_tupleWidth + sizeof(int32_t)) * tupleCount);
    initTable(true);
    TableTuple tuple(m_table->schema());
    addRandomUniqueTuples(m_table, tupleCount);
    size_t origPendingCount = m_table->getBlocksNotPendingSnapshotCount();
    // This should succeed in one call to serializeMore().
    char serializationBuffer[bufferSize];
    char config[4];
    ::memset(config, 0, 4);
    ReferenceSerializeInput input(config, 4);
    m_table->activateStream(m_serializer, TABLE_STREAM_SNAPSHOT, 0, m_tableId, input);
    TupleOutputStreamProcessor outputStreams(serializationBuffer, bufferSize);
    std::vector<int> retPositions;
    int64_t remaining = m_table->streamMore(outputStreams, retPositions);
    if (remaining >= 0) {
        ASSERT_EQ(outputStreams.size(), retPositions.size());
    }
    ASSERT_EQ(0, remaining);
    // Expect the same pending count, because it should get reset when
    // serialization finishes cleanly.
    size_t curPendingCount = m_table->getBlocksNotPendingSnapshotCount();
    ASSERT_EQ(origPendingCount, curPendingCount);
}

static void dumpValueSet(const std::string &tag, const T_ValueSet &set) {
    std:: cout << "::: " << tag << " :::" << std::endl;
    if (set.size() >= 10) {
        std::cout << "  (" << set.size() << " items)" << std::endl;
    }
    else {
        for (T_ValueSet::const_iterator i = set.begin(); i != set.end(); ++i) {
            std::cout << *i << std::endl;
        }
    }
}

/**
 * Dummy TableStreamer for intercepting and tracking tuple notifications.
 */
class DummyTableStreamer : public TableStreamerInterface {
public:
    DummyTableStreamer(TableStreamType type) : m_type(type) {}

    virtual bool activateStream(PersistentTable &table, CatalogId tableId) { return true; }

    virtual int64_t streamMore(TupleOutputStreamProcessor &outputStreams,
                               std::vector<int> &retPositions) { return 0; }

    virtual bool canSafelyFreeTuple(TableTuple &tuple) const { return true; }

    // Saying it's already active forces activateStream() to return without doing anything.
    virtual bool isAlreadyActive() const { return true; }

    virtual TableStreamType getStreamType() const { return m_type; }

    virtual TableStreamType getActiveStreamType() const { return m_type; }

    virtual bool notifyTupleInsert(TableTuple &tuple) { return false; }

    virtual bool notifyTupleUpdate(TableTuple &tuple) { return false; }

    virtual bool notifyTupleDelete(TableTuple &tuple) { return false; }

    virtual void notifyBlockWasCompactedAway(TBPtr block) {}

    virtual void notifyTupleMovement(TBPtr sourceBlock, TBPtr targetBlock,
                                     TableTuple &sourceTuple, TableTuple &targetTuple) {
        m_shuffles.insert(*reinterpret_cast<int64_t*>(sourceTuple.address() + 1));
    }

    TableStreamType m_type;
    T_ValueSet m_shuffles;
};

class ElasticTableScrambler {
public:

    ElasticTableScrambler(CopyOnWriteTest &test,
                          int tuplesPerBlock, int numInitial,
                          int freqInsert, int freqDelete, int freqUpdate, int freqCompaction) :
        test(test),
        tuplesPerBlock(tuplesPerBlock),
        numInitial(numInitial),
        freqInsert(freqInsert),
        freqDelete(freqDelete),
        freqUpdate(freqUpdate),
        freqCompaction(freqCompaction),
        icycle(0)
    {}

    void initialize() {
        test.initTable(true, static_cast<int>(test.m_tupleWidth * (tuplesPerBlock + sizeof(int32_t))));

        test.m_table->deleteAllTuples(true);
        test.addRandomUniqueTuples(test.m_table, numInitial, &initial);
    }

    void scramble() {
        // Make sure to offset the initial cycles based on the frequency.
        if (freqInsert > 0 && (icycle + freqInsert - 1) % freqInsert == 0) {
            test.doRandomInsert(test.m_table, &inserts);
        }

        if (freqDelete > 0 && (icycle + freqDelete - 1) % freqDelete == 0) {
            test.doRandomDelete(test.m_table, &deletes);
        }

        if (freqUpdate > 0 && (icycle + freqUpdate - 1) % freqUpdate == 0) {
            test.doRandomUpdate(test.m_table, &updatesSrc, &updatesTgt);
        }

        if (freqCompaction > 0 && (icycle + freqCompaction - 1) % freqCompaction == 0) {
            size_t churn = test.m_table->activeTupleCount() / 2;
            // Delete half the tuples to create enough fragmentation for
            // compaction to happen.
            for (size_t i = 0; i < churn; i++) {
                test.doRandomDelete(test.m_table, &deletes);
            }
            test.doForcedCompaction(test.m_table);
            // Re-insert the same number of tuples.
            for (size_t i = 0; i < churn; i++) {
                test.doRandomInsert(test.m_table, &inserts);
            }
        }
        icycle++;
    }

    CopyOnWriteTest &test;

    int tuplesPerBlock;
    int numInitial;
    int freqInsert;
    int freqDelete;
    int freqUpdate;
    int freqCompaction;
    int icycle;

    // Value sets used for checking results.
    T_ValueSet initial;
    T_ValueSet inserts;
    T_ValueSet updatesSrc;
    T_ValueSet updatesTgt;
    T_ValueSet deletes;
};

// Test the elastic scanner.
TEST_F(CopyOnWriteTest, ElasticScannerTest) {

    const int TUPLES_PER_BLOCK = 50;
    const int NUM_INITIAL = 300;
    const int NUM_CYCLES = 300;
    const int FREQ_INSERT = 1;
    const int FREQ_DELETE = 10;
    const int FREQ_UPDATE = 5;
    const int FREQ_COMPACTION = 100;

    ElasticTableScrambler tableScrambler(*this,
                                         TUPLES_PER_BLOCK, NUM_INITIAL,
                                         FREQ_INSERT, FREQ_DELETE,
                                         FREQ_UPDATE, FREQ_COMPACTION);

    tableScrambler.initialize();

    TableTuple tuple(m_table->schema());

    // Value sets used for checking results.
    T_ValueSet returns;

    DummyTableStreamer *dummyStreamer = new DummyTableStreamer(TABLE_STREAM_ELASTIC);
    boost::shared_ptr<TableStreamerInterface> dummyStreamerPtr(dummyStreamer);
    ElasticScanner scanner(*m_table);
    activateStreamInternal(m_tableId, dummyStreamerPtr);

    bool scanComplete = false;

    // Mutate/scan loop.
    for (size_t icycle = 0; icycle < NUM_CYCLES; icycle++) {
        // Periodically delete, insert, update, compact, etc..
        tableScrambler.scramble();

        scanComplete = !scanner.next(tuple);
        if (scanComplete) {
            break;
        }
        T_Value value = *reinterpret_cast<T_Value*>(tuple.address() + 1);
        returns.insert(value);
    }

    // Scan the remaining tuples that weren't encountered in the mutate/scan loop.
    if (!scanComplete) {
        while (scanner.next(tuple)) {
            T_Value value = *reinterpret_cast<T_Value*>(tuple.address() + 1);
            returns.insert(value);
        }
    }

    //=== Checks

    // Updates, inserts and deletes to tuples in blocks that were already
    // scanned are invisible, unless compaction moves their blocks around.
    // The checks have to be a little loose since we don't keep track of
    // which updates or deletes should be visible or not.

    // 1) Should be able to account for all scan returns in the initial,
    //    inserts or updatesTgt sets.
    T_ValueSet missing;
    for (T_ValueSet::const_iterator iter = returns.begin(); iter != returns.end(); ++iter) {
        T_Value value = *iter;
        if(tableScrambler.initial.find(value) == tableScrambler.initial.end() &&
           tableScrambler.inserts.find(value) == tableScrambler.inserts.end() &&
           tableScrambler.updatesTgt.find(value) == tableScrambler.updatesTgt.end()) {
            missing.insert(value);
        }
    }
    if (!missing.empty()) {
        std::cerr << std::endl << "ERROR: "
                  << missing.size() << " scan tuple(s) received that can not be found"
                  << " in the initial, insert or update (target) sets."
                  << std::endl;
        dumpValueSet("unexpected returned tuple values", missing);
        dumpValueSet("initial tuple values", tableScrambler.initial);
        dumpValueSet("inserted tuple values", tableScrambler.inserts);
        dumpValueSet("updated tuple target values", tableScrambler.updatesTgt);
        ASSERT_TRUE(missing.empty());
    }

    // 2) Should be able to account for all initial values in the returns,
    //    deletes or update (source) sets.
    for (T_ValueSet::const_iterator iter = tableScrambler.initial.begin();
         iter != tableScrambler.initial.end(); ++iter) {
        T_Value value = *iter;
        if(returns.find(value) == returns.end() &&
           tableScrambler.deletes.find(value) == tableScrambler.deletes.end() &&
           tableScrambler.updatesSrc.find(value) == tableScrambler.updatesSrc.end() &&
           dummyStreamer->m_shuffles.find(value) == dummyStreamer->m_shuffles.end()) {
            missing.insert(value);
        }
    }
    if (!missing.empty()) {
        /*
         * All initial tuples should have been returned by the scan, unless they
         * were deleted or updated (to have a different value).
         */
        std::cerr << std::endl << "ERROR: "
                  << missing.size() << " initial tuple(s) can not be found"
                  << " in the scan, delete, update (source), or compacted sets."
                  << std::endl;
        dumpValueSet("missing initial tuple values", missing);
        dumpValueSet("returned tuple values", returns);
        dumpValueSet("deleted tuple values", tableScrambler.deletes);
        dumpValueSet("updated tuple source values", tableScrambler.updatesSrc);
        ASSERT_TRUE(missing.empty());
    }
}

/**
 * Dummy pass-through elastic TableStreamer for testing the index.
 */
class DummyElasticTableStreamer : public DummyTableStreamer {
public:
    DummyElasticTableStreamer(PersistentTable& table,
                              const std::vector<std::string> &predicateStrings) :
        DummyTableStreamer(TABLE_STREAM_ELASTIC),
        m_predicateStrings(predicateStrings)
    {}

    virtual bool activateStream(PersistentTable &table, CatalogId tableId) {
        m_context.reset(new ElasticContext(table, m_predicateStrings));
        return true;
    }

    virtual int64_t streamMore(TupleOutputStreamProcessor &outputStreams,
                               std::vector<int> &retPositions) {
        return m_context->handleStreamMore(outputStreams, retPositions);
    }

    virtual bool isAlreadyActive() const {
        return (m_context != NULL);
    }

    virtual bool notifyTupleInsert(TableTuple &tuple) {
        return m_context->notifyTupleInsert(tuple);
    }

    virtual bool notifyTupleUpdate(TableTuple &tuple) {
        return m_context->notifyTupleUpdate(tuple);
    }

    virtual bool notifyTupleDelete(TableTuple &tuple) {
        return m_context->notifyTupleDelete(tuple);
    }

    virtual void notifyBlockWasCompactedAway(TBPtr block) {
        m_context->notifyBlockWasCompactedAway(block);
    }

    virtual void notifyTupleMovement(TBPtr sourceBlock, TBPtr targetBlock,
                                     TableTuple &sourceTuple, TableTuple &targetTuple) {
        DummyTableStreamer::notifyTupleMovement(sourceBlock, targetBlock, sourceTuple, targetTuple);
        m_context->notifyTupleMovement(sourceBlock, targetBlock, sourceTuple, targetTuple);
        T_Value value = *reinterpret_cast<T_Value*>(sourceTuple.address() + 1);
        m_moved.insert(value);
    }

    ElasticContext& getContext() {
        return *m_context.get();
    }

    ElasticIndex& getIndex() {
        // Abuse the friendship.
        return getContext().m_index;
    }

    const std::vector<std::string> &m_predicateStrings;
    boost::scoped_ptr<ElasticContext> m_context;
    T_ValueSet m_moved;
};

// Test elastic context index creation.
TEST_F(CopyOnWriteTest, ElasticContextIndexTest) {
    const int TUPLES_PER_BLOCK = 50;
    const int NUM_INITIAL = 300;
    const int NUM_CYCLES = 300;
    const int FREQ_INSERT = 1;
    const int FREQ_DELETE = 10;
    const int FREQ_UPDATE = 5;
    const int FREQ_COMPACTION = 100;

    ElasticTableScrambler tableScrambler(*this,
                                         TUPLES_PER_BLOCK, NUM_INITIAL,
                                         FREQ_INSERT, FREQ_DELETE,
                                         FREQ_UPDATE, FREQ_COMPACTION);

    tableScrambler.initialize();

    MultiStreamTestTool tool(*m_table, 1);

    T_HashRangeVector ranges;
    ranges.push_back(T_HashRange(0x0000000000000000, 0x7fffffffffffffff));
    std::vector<std::string> predicateStrings;
    predicateStrings.push_back(tool.generateHashRangePredicate(ranges));
    std::vector<bool> deleteFlags;
    StreamPredicateList predicates;
    std::ostringstream errmsg;
    ASSERT_TRUE(predicates.parseStrings(predicateStrings, errmsg, deleteFlags));

    DummyElasticTableStreamer *streamerPtr =
            new DummyElasticTableStreamer(*m_table, predicateStrings);
    boost::shared_ptr<TableStreamerInterface> streamer(streamerPtr);
    activateStreamInternal(m_tableId, streamer);

    for (size_t icycle = 0; icycle < NUM_CYCLES; icycle++) {
        tableScrambler.scramble();
    }

    TupleOutputStreamProcessor outputStreams;
    std::vector<int> retPositions;
    while (m_table->streamMore(outputStreams, retPositions) > 0) {
        ;
    }

    tool.context("check");
    ElasticIndex &index = streamerPtr->getIndex();
    voltdb::TableIterator& iterator = m_table->iterator();
    TableTuple tuple(m_table->schema());
    T_ValueSet accepted;
    T_ValueSet rejected;
    T_ValueSet missing;
    T_ValueSet extra;
    while (iterator.next(tuple)) {
        bool isAccepted = true;
        for (StreamPredicateList::iterator ipred = predicates.begin();
             ipred != predicates.end(); ++ipred) {
            if (ipred->eval(&tuple).isFalse()) {
                isAccepted = false;
                break;
            }
        }
        T_Value value = *reinterpret_cast<T_Value*>(tuple.address() + 1);
        bool isIndexed = index.has(*m_table, tuple);
        if (isAccepted) {
            accepted.insert(value);
            if (!isIndexed) {
                missing.insert(value);
            }
        }
        else {
            rejected.insert(value);
            if (isIndexed) {
                extra.insert(value);
            }
        }
    }
    if (missing.size() > 0 || extra.size() > 0) {
        size_t ninitialMIA = 0;
        size_t ninsertedMIA = 0;
        size_t nupdatedMIA = 0;
        size_t nmovedMIA = 0;
        size_t wtf = 0;
        if (missing.size() > 0) {
            BOOST_FOREACH(T_Value value, missing) {
                bool wasDeleted = tableScrambler.deletes.find(value) != tableScrambler.deletes.end();
                bool wasUpdated = tableScrambler.updatesSrc.find(value) != tableScrambler.updatesSrc.end();
                bool accountedFor = false;
                if (!wasDeleted && !wasUpdated) {
                    if (tableScrambler.initial.find(value) != tableScrambler.initial.end()) {
                        ninitialMIA++;
                        accountedFor = true;
                    }
                    if (tableScrambler.inserts.find(value) != tableScrambler.inserts.end()) {
                        ninsertedMIA++;
                        accountedFor = true;
                    }
                    if (tableScrambler.updatesTgt.find(value) != tableScrambler.updatesTgt.end()) {
                        nupdatedMIA++;
                        accountedFor = true;
                    }
                    if (streamerPtr->m_moved.find(value) != streamerPtr->m_moved.end()) {
                        nmovedMIA++;
                    }
                }
                if (!accountedFor) {
                    wtf++;
                }
            }
        }
        size_t ninitial = tableScrambler.initial.size();
        size_t ninserted = tableScrambler.inserts.size();
        size_t ndeleted = tableScrambler.deletes.size();
        size_t nupdated = tableScrambler.updatesTgt.size();
        size_t ntotal = ninitial + ninserted - ndeleted;
        size_t nactive = (size_t)m_table->activeTupleCount();
        size_t nrejected = rejected.size();
        size_t nexpected = nactive - nrejected;
        size_t nindexed = index.size();
        size_t nmissing = missing.size();
        size_t nextra = extra.size();
        size_t nmoved = streamerPtr->m_moved.size();
        tool.error("Bad index - tuple statistics:");
        tool.error("     Tuples: %lu = %lu+%lu-%lu (%lu)", ntotal, ninitial,
                   ninserted, ndeleted, nupdated);
        tool.error("   Expected: %lu = %lu-%lu", nexpected, nactive, nrejected);
        tool.error("      Found: %lu", nindexed);
        tool.error("      Moved: %lu", nmoved);
        tool.error("    Missing: %lu (%lu/%lu/%lu/%lu/%lu)", nmissing, ninitialMIA,
                   ninsertedMIA, nupdatedMIA, nmovedMIA, wtf);
        tool.error("      Extra: %lu", nextra);
        /*BOOST_FOREACH(T_Value value, missing) {
            std::cout << value << std::endl;
        }*/
        ASSERT_EQ(0, nmissing);
        ASSERT_EQ(0, nextra);
    }
}

int main() {
    return TestSuite::globalInstance()->runAll();
}
