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
#include "common/ValuePeeker.hpp"
#include "execution/VoltDBEngine.h"
#include "storage/persistenttable.h"
#include "storage/tablefactory.h"
#include "storage/tableutil.h"
#include "indexes/tableindex.h"
#include "storage/tableiterator.h"
#include "storage/CopyOnWriteIterator.h"
#include "stx/btree_set.h"
#include "common/DefaultTupleSerializer.h"
#include <vector>
#include <string>
#include <stdint.h>
#include "boost/scoped_ptr.hpp"

using namespace voltdb;

/**
 * Counter for unique primary key values
 */
static int32_t m_primaryKeyIndex = 0;

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
        m_engine->initialize(1,1, 0, 0, "", DEFAULT_TEMP_TABLE_MEMORY);

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
        m_primaryKeyIndexSchemaTypes.push_back(voltdb::VALUE_TYPE_INTEGER);
        m_tableSchemaTypes.push_back(voltdb::VALUE_TYPE_INTEGER);

        //Filler columns
        m_tableSchemaTypes.push_back(voltdb::VALUE_TYPE_BIGINT);
        m_tableSchemaTypes.push_back(voltdb::VALUE_TYPE_BIGINT);
        m_tableSchemaTypes.push_back(voltdb::VALUE_TYPE_BIGINT);
        m_tableSchemaTypes.push_back(voltdb::VALUE_TYPE_BIGINT);
        m_tableSchemaTypes.push_back(voltdb::VALUE_TYPE_BIGINT);
        m_tableSchemaTypes.push_back(voltdb::VALUE_TYPE_BIGINT);
        m_tableSchemaTypes.push_back(voltdb::VALUE_TYPE_BIGINT);

        m_tableSchemaColumnSizes.push_back(NValue::getTupleStorageSize(voltdb::VALUE_TYPE_INTEGER));
        m_primaryKeyIndexSchemaColumnSizes.push_back(voltdb::VALUE_TYPE_INTEGER);
        m_tableSchemaColumnSizes.push_back(NValue::getTupleStorageSize(voltdb::VALUE_TYPE_INTEGER));
        m_primaryKeyIndexSchemaColumnSizes.push_back(voltdb::VALUE_TYPE_INTEGER);

        m_tableSchemaColumnSizes.push_back(NValue::getTupleStorageSize(voltdb::VALUE_TYPE_BIGINT));
        m_tableSchemaColumnSizes.push_back(NValue::getTupleStorageSize(voltdb::VALUE_TYPE_BIGINT));
        m_tableSchemaColumnSizes.push_back(NValue::getTupleStorageSize(voltdb::VALUE_TYPE_BIGINT));
        m_tableSchemaColumnSizes.push_back(NValue::getTupleStorageSize(voltdb::VALUE_TYPE_BIGINT));
        m_tableSchemaColumnSizes.push_back(NValue::getTupleStorageSize(voltdb::VALUE_TYPE_BIGINT));
        m_tableSchemaColumnSizes.push_back(NValue::getTupleStorageSize(voltdb::VALUE_TYPE_BIGINT));
        m_tableSchemaColumnSizes.push_back(NValue::getTupleStorageSize(voltdb::VALUE_TYPE_BIGINT));

        m_tableSchemaAllowNull.push_back(false);
        m_primaryKeyIndexSchemaAllowNull.push_back(false);
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
    }

    ~CopyOnWriteTest() {
        delete m_engine;
        delete m_table;
        voltdb::TupleSchema::freeTupleSchema(m_primaryKeyIndexSchema);
    }

    void initTable(bool allowInlineStrings) {
        m_tableSchema = voltdb::TupleSchema::createTupleSchema(m_tableSchemaTypes,
                                                               m_tableSchemaColumnSizes,
                                                               m_tableSchemaAllowNull,
                                                               allowInlineStrings);

        m_primaryKeyIndexSchema = voltdb::TupleSchema::createTupleSchema(m_primaryKeyIndexSchemaTypes,
                                                                         m_primaryKeyIndexSchemaColumnSizes,
                                                                         m_primaryKeyIndexSchemaAllowNull,
                                                                         allowInlineStrings);
        voltdb::TableIndexScheme indexScheme = voltdb::TableIndexScheme("primaryKeyIndex",
                                                                        voltdb::BALANCED_TREE_INDEX,
                                                                        m_primaryKeyIndexColumns,
                                                                        m_primaryKeyIndexSchemaTypes,
                                                                        true, false, m_tableSchema);
        indexScheme.keySchema = m_primaryKeyIndexSchema;
        std::vector<voltdb::TableIndexScheme> indexes;

        m_table = dynamic_cast<voltdb::PersistentTable*>(voltdb::TableFactory::getPersistentTable
                                                         (0, m_engine->getExecutorContext(), "Foo",
                                                          m_tableSchema, &m_columnNames[0], indexScheme, indexes, 0,
                                                          false, false));
    }

    void addRandomUniqueTuples(Table *table, int numTuples) {
        TableTuple tuple = table->tempTuple();
        ::memset(tuple.address() + 1, 0, tuple.tupleLength() - 1);
        for (int ii = 0; ii < numTuples; ii++) {
            tuple.setNValue(0, ValueFactory::getIntegerValue(m_primaryKeyIndex++));
            tuple.setNValue(1, ValueFactory::getIntegerValue(rand()));
            bool success = table->insertTuple(tuple);
            if (!success) {
                std::cout << "Failed to add random unique tuple" << std::endl;
                return;
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
        m_engine->getExecutorContext()->setupForPlanFragments(m_engine->getCurrentUndoQuantum(), 0, 0);
        m_tuplesDeletedInLastUndo = 0;
        m_tuplesInsertedInLastUndo = 0;
    }

    void doRandomTableMutation(Table *table) {
        int rand = ::rand();
        int op = rand % 3;
        switch (op) {

        /*
         * Delete a tuple
         */
        case 0: {
            TableTuple tuple(table->schema());
            if (tableutil::getRandomTuple(table, tuple)) {
                table->deleteTuple(tuple, true);
                m_tuplesDeleted++;
                m_tuplesDeletedInLastUndo++;
            }
            break;
        }

        /*
         * Insert a tuple
         */
        case 1: {
            addRandomUniqueTuples(table, 1);
            m_tuplesInserted++;
            m_tuplesInsertedInLastUndo++;
            break;
        }

        /*
         * Update a random tuple
         */
        case 2: {
            voltdb::TableTuple tuple(table->schema());
            voltdb::TableTuple tempTuple = table->tempTuple();
            if (tableutil::getRandomTuple(table, tuple)) {
                tempTuple.copy(tuple);
                tempTuple.setNValue(1, ValueFactory::getIntegerValue(::rand()));
                table->updateTuple( tempTuple, tuple, true);
                m_tuplesUpdated++;
            }
            break;
        }
        default:
            assert(false);
            break;
        }
    }

    voltdb::VoltDBEngine *m_engine;
    voltdb::TupleSchema *m_tableSchema;
    voltdb::TupleSchema *m_primaryKeyIndexSchema;
    voltdb::PersistentTable *m_table;
    std::vector<std::string> m_columnNames;
    std::vector<voltdb::ValueType> m_tableSchemaTypes;
    std::vector<int32_t> m_tableSchemaColumnSizes;
    std::vector<bool> m_tableSchemaAllowNull;
    std::vector<voltdb::ValueType> m_primaryKeyIndexSchemaTypes;
    std::vector<int32_t> m_primaryKeyIndexSchemaColumnSizes;
    std::vector<bool> m_primaryKeyIndexSchemaAllowNull;
    std::vector<int> m_primaryKeyIndexColumns;

    int32_t m_tuplesInserted ;
    int32_t m_tuplesUpdated;
    int32_t m_tuplesDeleted;

    int32_t m_tuplesInsertedInLastUndo;
    int32_t m_tuplesDeletedInLastUndo;

    int64_t m_undoToken;
};

TEST_F(CopyOnWriteTest, CopyOnWriteIterator) {
    initTable(true);

#ifdef MEMCHECK
    int tupleCount = 1000;
#else
    int tupleCount = 174762;
#endif
    addRandomUniqueTuples( m_table, tupleCount);

    voltdb::TableIterator& iterator = m_table->iterator();
    TBMap blocks(m_table->m_data);
    DefaultTupleSerializer serializer;
    m_table->m_blocksPendingSnapshot.swap(m_table->m_blocksNotPendingSnapshot);
    m_table->m_blocksPendingSnapshotLoad.swap(m_table->m_blocksNotPendingSnapshotLoad);
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
#ifdef MEMCHECK
    int tupleCount = 1000;
#else
    int tupleCount = 174762;
#endif
    addRandomUniqueTuples( m_table, tupleCount);
    DefaultTupleSerializer serializer;
    for (int qq = 0; qq < 10; qq++) {
        stx::btree_set<int64_t> originalTuples;
        voltdb::TableIterator& iterator = m_table->iterator();
        TableTuple tuple(m_table->schema());
        while (iterator.next(tuple)) {
            const std::pair<stx::btree_set<int64_t>::iterator, bool> p =
                    originalTuples.insert(*reinterpret_cast<int64_t*>(tuple.address() + 1));
            const bool inserted = p.second;
            if (!inserted) {
                    int32_t primaryKey = ValuePeeker::peekAsInteger(tuple.getNValue(0));
                    printf("Failed to insert %d\n", primaryKey);
            }
            ASSERT_TRUE(inserted);
        }

        m_table->activateCopyOnWrite(&serializer, 0);

        stx::btree_set<int64_t> COWTuples;
        char serializationBuffer[131072];
        int totalInserted = 0;
        while (true) {
            ReferenceSerializeOutput out( serializationBuffer, 131072);
            m_table->serializeMore(&out);
            const int serialized = static_cast<int>(out.position());
            if (out.position() == 0) {
                break;
            }
            int ii = 16;//skip partition id and row count and first tuple length
            while (ii < (serialized - 4)) {
                int values[2];
                values[0] = ntohl(*reinterpret_cast<int32_t*>(&serializationBuffer[ii]));
                values[1] = ntohl(*reinterpret_cast<int32_t*>(&serializationBuffer[ii + 4]));
                const bool inserted =
                        COWTuples.insert(*reinterpret_cast<int64_t*>(values)).second;
                if (!inserted) {
                    printf("Failed in iteration %d, total inserted %d, with values %d and %d\n", qq, totalInserted, values[0], values[1]);
                }
                ASSERT_TRUE(inserted);
                totalInserted++;
                ii += 68;
            }
            for (int jj = 0; jj < 10; jj++) {
                doRandomTableMutation(m_table);
            }
        }

        std::vector<int64_t> diff;
        std::insert_iterator<std::vector<int64_t> > ii( diff, diff.begin());
        std::set_difference(originalTuples.begin(), originalTuples.end(), COWTuples.begin(), COWTuples.end(), ii);
        for (int ii = 0; ii < diff.size(); ii++) {
            int32_t *values = reinterpret_cast<int32_t*>(&diff[ii]);
            printf("Tuple in original not in COW is %d and %d\n", values[0], values[1]);
        }

        diff.clear();
        ii = std::insert_iterator<std::vector<int64_t> >(diff, diff.begin());
        std::set_difference( COWTuples.begin(), COWTuples.end(), originalTuples.begin(), originalTuples.end(), ii);
        for (int ii = 0; ii < diff.size(); ii++) {
            int32_t *values = reinterpret_cast<int32_t*>(&diff[ii]);
            printf("Tuple in COW not in original is %d and %d\n", values[0], values[1]);
        }

        int numTuples = 0;
        iterator = m_table->iterator();
        while (iterator.next(tuple)) {
            if (tuple.isDirty()) {
                printf("Found tuple %d is active and dirty at end of COW\n",
                        ValuePeeker::peekAsInteger(tuple.getNValue(0)));
            }
            numTuples++;
            ASSERT_FALSE(tuple.isDirty());
        }
        ASSERT_EQ(numTuples, tupleCount + (m_tuplesInserted - m_tuplesDeleted));

        ASSERT_EQ(originalTuples.size(), COWTuples.size());
        ASSERT_TRUE(originalTuples == COWTuples);
    }
}

TEST_F(CopyOnWriteTest, BigTestWithUndo) {
    initTable(true);
#ifdef MEMCHECK
    int tupleCount = 1000;
#else
    int tupleCount = 174762;
#endif
    addRandomUniqueTuples( m_table, tupleCount);
    m_engine->setUndoToken(0);
    m_engine->getExecutorContext()->setupForPlanFragments(m_engine->getCurrentUndoQuantum(), 0, 0);
    DefaultTupleSerializer serializer;
    for (int qq = 0; qq < 10; qq++) {
        stx::btree_set<int64_t> originalTuples;
        voltdb::TableIterator& iterator = m_table->iterator();
        TableTuple tuple(m_table->schema());
        while (iterator.next(tuple)) {
            const std::pair<stx::btree_set<int64_t>::iterator, bool> p =
                    originalTuples.insert(*reinterpret_cast<int64_t*>(tuple.address() + 1));
            const bool inserted = p.second;
            if (!inserted) {
                    int32_t primaryKey = ValuePeeker::peekAsInteger(tuple.getNValue(0));
                    printf("Failed to insert %d\n", primaryKey);
            }
            ASSERT_TRUE(inserted);
        }

        m_table->activateCopyOnWrite(&serializer, 0);

        stx::btree_set<int64_t> COWTuples;
        char serializationBuffer[131072];
        int totalInserted = 0;
        while (true) {
            ReferenceSerializeOutput out( serializationBuffer, 131072);
            m_table->serializeMore(&out);
            const int serialized = static_cast<int>(out.position());
            if (out.position() == 0) {
                break;
            }
            int ii = 16;//skip partition id and row count and first tuple length
            while (ii < (serialized - 4)) {
                int values[2];
                values[0] = ntohl(*reinterpret_cast<int32_t*>(&serializationBuffer[ii]));
                values[1] = ntohl(*reinterpret_cast<int32_t*>(&serializationBuffer[ii + 4]));
                const bool inserted =
                        COWTuples.insert(*reinterpret_cast<int64_t*>(values)).second;
                if (!inserted) {
                    printf("Failed in iteration %d with values %d and %d\n", totalInserted, values[0], values[1]);
                }
                ASSERT_TRUE(inserted);
                totalInserted++;
                ii += 68;
            }
            for (int jj = 0; jj < 10; jj++) {
                doRandomTableMutation(m_table);
            }
            doRandomUndo();
        }

        std::vector<int64_t> diff;
        std::insert_iterator<std::vector<int64_t> > ii( diff, diff.begin());
        std::set_difference(originalTuples.begin(), originalTuples.end(), COWTuples.begin(), COWTuples.end(), ii);
        for (int ii = 0; ii < diff.size(); ii++) {
            int32_t *values = reinterpret_cast<int32_t*>(&diff[ii]);
            printf("Tuple in original not in COW is %d and %d\n", values[0], values[1]);
        }

        diff.clear();
        ii = std::insert_iterator<std::vector<int64_t> >(diff, diff.begin());
        std::set_difference( COWTuples.begin(), COWTuples.end(), originalTuples.begin(), originalTuples.end(), ii);
        for (int ii = 0; ii < diff.size(); ii++) {
            int32_t *values = reinterpret_cast<int32_t*>(&diff[ii]);
            printf("Tuple in COW not in original is %d and %d\n", values[0], values[1]);
        }

        int numTuples = 0;
        iterator = m_table->iterator();
        while (iterator.next(tuple)) {
            if (tuple.isDirty()) {
                printf("Found tuple %d is active and dirty at end of COW\n",
                        ValuePeeker::peekAsInteger(tuple.getNValue(0)));
            }
            numTuples++;
            if (tuple.isDirty()) {
                printf("Dirty tuple is %p, %d, %d\n", tuple.address(), ValuePeeker::peekAsInteger(tuple.getNValue(0)), ValuePeeker::peekAsInteger(tuple.getNValue(1)));
            }
            ASSERT_FALSE(tuple.isDirty());
        }
        ASSERT_EQ(numTuples, tupleCount + (m_tuplesInserted - m_tuplesDeleted));

        ASSERT_EQ(originalTuples.size(), COWTuples.size());
        ASSERT_TRUE(originalTuples == COWTuples);
    }
}

TEST_F(CopyOnWriteTest, BigTestUndoEverything) {
    initTable(true);
#ifdef MEMCHECK
    int tupleCount = 1000;
#else
    int tupleCount = 174762;
#endif
    addRandomUniqueTuples( m_table, tupleCount);
    m_engine->setUndoToken(0);
    m_engine->getExecutorContext()->setupForPlanFragments(m_engine->getCurrentUndoQuantum(), 0, 0);
    DefaultTupleSerializer serializer;
    for (int qq = 0; qq < 10; qq++) {
        stx::btree_set<int64_t> originalTuples;
        voltdb::TableIterator& iterator = m_table->iterator();
        TableTuple tuple(m_table->schema());
        while (iterator.next(tuple)) {
            const std::pair<stx::btree_set<int64_t>::iterator, bool> p =
                    originalTuples.insert(*reinterpret_cast<int64_t*>(tuple.address() + 1));
            const bool inserted = p.second;
            if (!inserted) {
                    int32_t primaryKey = ValuePeeker::peekAsInteger(tuple.getNValue(0));
                    printf("Failed to insert %d\n", primaryKey);
            }
            ASSERT_TRUE(inserted);
        }

        m_table->activateCopyOnWrite(&serializer, 0);

        stx::btree_set<int64_t> COWTuples;
        char serializationBuffer[131072];
        int totalInserted = 0;
        while (true) {
            ReferenceSerializeOutput out( serializationBuffer, 131072);
            m_table->serializeMore(&out);
            const int serialized = static_cast<int>(out.position());
            if (out.position() == 0) {
                break;
            }
            int ii = 16;//skip partition id and row count and first tuple length
            while (ii < (serialized - 4)) {
                int values[2];
                values[0] = ntohl(*reinterpret_cast<int32_t*>(&serializationBuffer[ii]));
                values[1] = ntohl(*reinterpret_cast<int32_t*>(&serializationBuffer[ii + 4]));
                const bool inserted =
                        COWTuples.insert(*reinterpret_cast<int64_t*>(values)).second;
                if (!inserted) {
                    printf("Failed in iteration %d with values %d and %d\n", totalInserted, values[0], values[1]);
                }
                ASSERT_TRUE(inserted);
                totalInserted++;
                ii += 68;
            }
            for (int jj = 0; jj < 10; jj++) {
                doRandomTableMutation(m_table);
            }
            m_engine->undoUndoToken(m_undoToken);
            m_engine->setUndoToken(++m_undoToken);
            m_engine->getExecutorContext()->setupForPlanFragments(m_engine->getCurrentUndoQuantum(), 0, 0);
        }

        std::vector<int64_t> diff;
        std::insert_iterator<std::vector<int64_t> > ii( diff, diff.begin());
        std::set_difference(originalTuples.begin(), originalTuples.end(), COWTuples.begin(), COWTuples.end(), ii);
        for (int ii = 0; ii < diff.size(); ii++) {
            int32_t *values = reinterpret_cast<int32_t*>(&diff[ii]);
            printf("Tuple in original not in COW is %d and %d\n", values[0], values[1]);
        }

        diff.clear();
        ii = std::insert_iterator<std::vector<int64_t> >(diff, diff.begin());
        std::set_difference( COWTuples.begin(), COWTuples.end(), originalTuples.begin(), originalTuples.end(), ii);
        for (int ii = 0; ii < diff.size(); ii++) {
            int32_t *values = reinterpret_cast<int32_t*>(&diff[ii]);
            printf("Tuple in COW not in original is %d and %d\n", values[0], values[1]);
        }

        int numTuples = 0;
        iterator = m_table->iterator();
        while (iterator.next(tuple)) {
            if (tuple.isDirty()) {
                printf("Found tuple %d is active and dirty at end of COW\n",
                        ValuePeeker::peekAsInteger(tuple.getNValue(0)));
            }
            numTuples++;
            ASSERT_FALSE(tuple.isDirty());
        }
        ASSERT_EQ(numTuples, tupleCount);

        ASSERT_EQ(originalTuples.size(), COWTuples.size());
        ASSERT_TRUE(originalTuples == COWTuples);
    }
}

int main() {
    return TestSuite::globalInstance()->runAll();
}
