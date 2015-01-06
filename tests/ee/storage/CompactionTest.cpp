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

#include "harness.h"
#include "common/TupleSchema.h"
#include "common/types.h"
#include "common/NValue.hpp"
#include "common/ValueFactory.hpp"
#include "common/ValuePeeker.hpp"
#include "common/TupleOutputStream.h"
#include "common/TupleOutputStreamProcessor.h"
#include "execution/VoltDBEngine.h"
#include "storage/persistenttable.h"
#include "storage/tablefactory.h"
#include "storage/tableutil.h"
#include "indexes/tableindex.h"
#include "storage/tableiterator.h"
#include "storage/CopyOnWriteIterator.h"
#include "storage/DRTupleStream.h"
#include "common/DefaultTupleSerializer.h"
#include "stx/btree_set.h"

#include <vector>
#include <string>
#include <stdint.h>
#include <boost/scoped_array.hpp>
#include <boost/foreach.hpp>

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
class CompactionTest : public Test {
public:
    CompactionTest() {
        m_primaryKeyIndex = 0;
        m_tuplesInserted = 0;
        m_tuplesUpdated = 0;
        m_tuplesDeleted = 0;
        m_tuplesInsertedInLastUndo = 0;
        m_tuplesDeletedInLastUndo = 0;
        m_engine = new voltdb::VoltDBEngine();
        int partitionCount = 1;
        m_engine->initialize(1,1, 0, 0, "", DEFAULT_TEMP_TABLE_MEMORY);
        m_engine->updateHashinator(HASHINATOR_LEGACY, (char*)&partitionCount, NULL, 0);

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

        m_primaryKeyIndexColumns.push_back(0);

        m_undoToken = 0;

        m_tableId = 0;
    }

    ~CompactionTest() {
        delete m_engine;
        delete m_table;
    }

    void initTable() {
        m_tableSchema = voltdb::TupleSchema::createTupleSchemaForTest(m_tableSchemaTypes,
                                                               m_tableSchemaColumnSizes,
                                                               m_tableSchemaAllowNull);

        voltdb::TableIndexScheme indexScheme("BinaryTreeUniqueIndex",
                                             voltdb::BALANCED_TREE_INDEX,
                                             m_primaryKeyIndexColumns,
                                             TableIndex::simplyIndexColumns(),
                                             true, true, m_tableSchema);
        std::vector<voltdb::TableIndexScheme> indexes;

        voltdb::TableIndexScheme indexScheme1("BinaryTreeMultimapIndex",
                                              voltdb::BALANCED_TREE_INDEX,
                                              m_primaryKeyIndexColumns,
                                              TableIndex::simplyIndexColumns(),
                                              false, true, m_tableSchema);
        indexes.push_back(indexScheme1);
        voltdb::TableIndexScheme indexScheme2("HashUniqueIndex",
                                              voltdb::HASH_TABLE_INDEX,
                                              m_primaryKeyIndexColumns,
                                              TableIndex::simplyIndexColumns(),
                                              true, false, m_tableSchema);
        indexes.push_back(indexScheme2);
        voltdb::TableIndexScheme indexScheme3("HashMultimapIndex",
                                              voltdb::HASH_TABLE_INDEX,
                                              m_primaryKeyIndexColumns,
                                              TableIndex::simplyIndexColumns(),
                                              false, false, m_tableSchema);
        indexes.push_back(indexScheme3);



        m_table = dynamic_cast<voltdb::PersistentTable*>(
                voltdb::TableFactory::getPersistentTable(m_tableId, "Foo", m_tableSchema, m_columnNames, signature, &drStream));

        TableIndex *pkeyIndex = TableIndexFactory::TableIndexFactory::getInstance(indexScheme);
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

    void addRandomUniqueTuples(Table *table, int numTuples) {
        TableTuple tuple = table->tempTuple();
        for (int ii = 0; ii < numTuples; ii++) {
            tuple.setNValue(0, ValueFactory::getIntegerValue(m_primaryKeyIndex++));
            tuple.setNValue(1, ValueFactory::getIntegerValue(rand()));
            table->insertTuple(tuple);
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
        ExecutorContext::getExecutorContext()->setupForPlanFragments(m_engine->getCurrentUndoQuantum(), 0, 0, 0, 0);
        m_tuplesDeletedInLastUndo = 0;
        m_tuplesInsertedInLastUndo = 0;
    }

    /// NEVER CALLED???
    void doRandomTableMutation(PersistentTable *table) {
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
                    table->updateTuple(tuple, tempTuple);
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
    voltdb::PersistentTable *m_table;
    std::vector<std::string> m_columnNames;
    std::vector<voltdb::ValueType> m_tableSchemaTypes;
    std::vector<int32_t> m_tableSchemaColumnSizes;
    std::vector<bool> m_tableSchemaAllowNull;
    std::vector<int> m_primaryKeyIndexColumns;

    int32_t m_tuplesInserted ;
    int32_t m_tuplesUpdated;
    int32_t m_tuplesDeleted;

    int32_t m_tuplesInsertedInLastUndo;
    int32_t m_tuplesDeletedInLastUndo;

    int64_t m_undoToken;

    CatalogId m_tableId;
    MockDRTupleStream drStream;
    char signature[20];
};

TEST_F(CompactionTest, BasicCompaction) {
    initTable();
#ifdef MEMCHECK
    int tupleCount = 1000;
#else
    int tupleCount = 645260;
#endif
    addRandomUniqueTuples( m_table, tupleCount);

#ifdef MEMCHECK
    ASSERT_EQ( tupleCount, m_table->m_data.size());
#else
    ASSERT_EQ(20, m_table->m_data.size());
#endif

    stx::btree_set<int32_t> pkeysNotDeleted;
    std::vector<int32_t> pkeysToDelete;
    for (int ii = 0; ii < tupleCount; ii ++) {
        if (ii % 2 == 0) {
            pkeysToDelete.push_back(ii);
        } else {
            pkeysNotDeleted.insert(ii);
        }
    }

    voltdb::TableIndex *pkeyIndex = m_table->primaryKeyIndex();
    TableTuple key(pkeyIndex->getKeySchema());
    boost::scoped_array<char> backingStore(new char[pkeyIndex->getKeySchema()->tupleLength()]);
    key.moveNoHeader(backingStore.get());

    IndexCursor indexCursor(pkeyIndex->getTupleSchema());

    for (std::vector<int32_t>::iterator ii = pkeysToDelete.begin(); ii != pkeysToDelete.end(); ii++) {
        key.setNValue(0, ValueFactory::getIntegerValue(*ii));
        ASSERT_TRUE(pkeyIndex->moveToKey(&key, indexCursor));
        TableTuple tuple = pkeyIndex->nextValueAtKey(indexCursor);
        m_table->deleteTuple(tuple, true);
    }

    m_table->doForcedCompaction();

    stx::btree_set<int32_t> pkeysFoundAfterDelete;
    TableIterator& iter = m_table->iterator();
    TableTuple tuple(m_table->schema());
    while (iter.next(tuple)) {
        int32_t pkey = ValuePeeker::peekAsInteger(tuple.getNValue(0));
        key.setNValue(0, ValueFactory::getIntegerValue(pkey));
        for (int ii = 0; ii < 4; ii++) {
            ASSERT_TRUE(m_table->m_indexes[ii]->moveToKey(&key, indexCursor));
            TableTuple indexTuple = m_table->m_indexes[ii]->nextValueAtKey(indexCursor);
            ASSERT_EQ(indexTuple.address(), tuple.address());
        }
        pkeysFoundAfterDelete.insert(pkey);
    }

    std::vector<int32_t> diff;
    std::insert_iterator<std::vector<int32_t> > ii( diff, diff.begin());
    std::set_difference(pkeysNotDeleted.begin(), pkeysNotDeleted.end(), pkeysFoundAfterDelete.begin(), pkeysFoundAfterDelete.end(), ii);
    for (int ii = 0; ii < diff.size(); ii++) {
        printf("Key that was not deleted, but wasn't found is %d\n", diff[ii]);
    }

    diff.clear();
    ii = std::insert_iterator<std::vector<int32_t> >(diff, diff.begin());
    std::set_difference( pkeysFoundAfterDelete.begin(), pkeysFoundAfterDelete.end(), pkeysNotDeleted.begin(), pkeysNotDeleted.end(), ii);
    for (int ii = 0; ii < diff.size(); ii++) {
        printf("Key that was found after deletes, but shouldn't have been there was %d\n", diff[ii]);
    }

    ASSERT_EQ(pkeysFoundAfterDelete.size(), pkeysNotDeleted.size());
    ASSERT_TRUE(pkeysFoundAfterDelete == pkeysNotDeleted);
    //    std::cout << "Have " << m_table->m_data.size() << " blocks left " << m_table->allocatedTupleCount() << ", " << m_table->activeTupleCount() << std::endl;
#ifdef MEMCHECK
    ASSERT_EQ( m_table->m_data.size(), 500);
#else
    ASSERT_EQ( m_table->m_data.size(), 13);
#endif

    for (stx::btree_set<int32_t>::iterator ii = pkeysNotDeleted.begin(); ii != pkeysNotDeleted.end(); ii++) {
        key.setNValue(0, ValueFactory::getIntegerValue(*ii));
        ASSERT_TRUE(pkeyIndex->moveToKey(&key, indexCursor));
        TableTuple tuple = pkeyIndex->nextValueAtKey(indexCursor);
        m_table->deleteTuple(tuple, true);
    }
    m_table->doForcedCompaction();
    ASSERT_EQ( m_table->m_data.size(), 0);
    ASSERT_EQ( m_table->activeTupleCount(), 0);
}

TEST_F(CompactionTest, CompactionWithCopyOnWrite) {
    initTable();
#ifdef MEMCHECK
    int tupleCount = 1000;
#else
    int tupleCount = 645260;
#endif
    addRandomUniqueTuples( m_table, tupleCount);

#ifdef MEMCHECK
    ASSERT_EQ( tupleCount, m_table->m_data.size());
#else
    ASSERT_EQ(20, m_table->m_data.size());
#endif

    stx::btree_set<int32_t> pkeysNotDeleted[3];
    std::vector<int32_t> pkeysToDelete[3];
    for (int ii = 0; ii < tupleCount; ii ++) {
        int foo = ii % 3;
        pkeysToDelete[foo].push_back(ii);
        if (ii % 3 == 0) {
            //All keys deleted
        } else if (ii % 3 == 1) {
            pkeysNotDeleted[0].insert(ii);
        } else {
            pkeysNotDeleted[0].insert(ii);
            pkeysNotDeleted[1].insert(ii);
        }
    }
    //std::cout << pkeysToDelete[0].size() << "," << pkeysToDelete[1].size() << "," << pkeysToDelete[2].size() << std::endl;

    stx::btree_set<int32_t> COWTuples;
    int totalInsertedCOWTuples = 0;
    DefaultTupleSerializer serializer;
    char config[5];
    ::memset(config, 0, 5);
    ReferenceSerializeInputBE input(config, 5);
    m_table->activateStream(serializer, TABLE_STREAM_SNAPSHOT, 0, m_tableId, input);

    for (int qq = 0; qq < 3; qq++) {
#ifdef MEMCHECK
        int serializationBufferSize = 22700;
#else
        int serializationBufferSize = 131072;
#endif
        char serializationBuffer[serializationBufferSize];
        while (true) {
            TupleOutputStreamProcessor outs( serializationBuffer, serializationBufferSize);
            TupleOutputStream &out = outs.at(0);
            std::vector<int> retPositions;
            m_table->streamMore(outs, TABLE_STREAM_SNAPSHOT, retPositions);
            const int serialized = static_cast<int>(out.position());
            if (out.position() == 0) {
                break;
            }
            int ii = 12;//skip partition id and row count and first tuple length
            while (ii < (serialized - 4)) {
                int32_t value = ntohl(*reinterpret_cast<int32_t*>(&serializationBuffer[ii]));
                const bool inserted =
                COWTuples.insert(value).second;
                if (!inserted) {
                    printf("Failed in iteration %d, total inserted %d, with pkey %d\n", qq, totalInsertedCOWTuples, value);
                }
                ASSERT_TRUE(inserted);
                totalInsertedCOWTuples++;
                ii += 68;
            }
            if (qq == 0) {
                if (totalInsertedCOWTuples > (tupleCount / 3)) {
                    break;
                }
            } else if (qq == 1) {
                if (totalInsertedCOWTuples > ((tupleCount / 3) * 2)) {
                    break;
                }
            }
        }

        voltdb::TableIndex *pkeyIndex = m_table->primaryKeyIndex();
        TableTuple key(pkeyIndex->getKeySchema());
        boost::scoped_array<char> backingStore(new char[pkeyIndex->getKeySchema()->tupleLength()]);
        key.moveNoHeader(backingStore.get());

        IndexCursor indexCursor(pkeyIndex->getTupleSchema());
        for (std::vector<int32_t>::iterator ii = pkeysToDelete[qq].begin(); ii != pkeysToDelete[qq].end(); ii++) {
            key.setNValue(0, ValueFactory::getIntegerValue(*ii));
            ASSERT_TRUE(pkeyIndex->moveToKey(&key, indexCursor));
            TableTuple tuple = pkeyIndex->nextValueAtKey(indexCursor);
            m_table->deleteTuple(tuple, true);
        }

        //std::cout << "Allocated tuple count before idle compactions " << m_table->allocatedTupleCount() << std::endl;
        m_table->doIdleCompaction();
        m_table->doIdleCompaction();
        //std::cout << "Allocated tuple count after idle compactions " << m_table->allocatedTupleCount() << std::endl;
        m_table->doForcedCompaction();

        stx::btree_set<int32_t> pkeysFoundAfterDelete;
        TableIterator& iter = m_table->iterator();
        TableTuple tuple(m_table->schema());
        while (iter.next(tuple)) {
            int32_t pkey = ValuePeeker::peekAsInteger(tuple.getNValue(0));
            key.setNValue(0, ValueFactory::getIntegerValue(pkey));
            for (int ii = 0; ii < 4; ii++) {
                ASSERT_TRUE(m_table->m_indexes[ii]->moveToKey(&key, indexCursor));
                TableTuple indexTuple = m_table->m_indexes[ii]->nextValueAtKey(indexCursor);
                ASSERT_EQ(indexTuple.address(), tuple.address());
            }
            pkeysFoundAfterDelete.insert(pkey);
        }

        std::vector<int32_t> diff;
        std::insert_iterator<std::vector<int32_t> > ii( diff, diff.begin());
        std::set_difference(pkeysNotDeleted[qq].begin(), pkeysNotDeleted[qq].end(), pkeysFoundAfterDelete.begin(), pkeysFoundAfterDelete.end(), ii);
        for (int ii = 0; ii < diff.size(); ii++) {
            printf("Key that was not deleted, but wasn't found is %d\n", diff[ii]);
        }

        diff.clear();
        ii = std::insert_iterator<std::vector<int32_t> >(diff, diff.begin());
        std::set_difference( pkeysFoundAfterDelete.begin(), pkeysFoundAfterDelete.end(), pkeysNotDeleted[qq].begin(), pkeysNotDeleted[qq].end(), ii);
        for (int ii = 0; ii < diff.size(); ii++) {
            printf("Key that was found after deletes, but shouldn't have been there was %d\n", diff[ii]);
        }

        //        ASSERT_EQ(pkeysFoundAfterDelete.size(), pkeysNotDeleted.size());
        //        ASSERT_TRUE(pkeysFoundAfterDelete == pkeysNotDeleted);
        //    std::cout << "Have " << m_table->m_data.size() << " blocks left " << m_table->allocatedTupleCount() << ", " << m_table->activeTupleCount() << std::endl;
        //        ASSERT_EQ( m_table->m_data.size(), 13);
        //
        //        for (stx::btree_set<int32_t>::iterator ii = pkeysNotDeleted.begin(); ii != pkeysNotDeleted.end(); ii++) {
        //            key.setNValue(0, ValueFactory::getIntegerValue(*ii));
        //            ASSERT_TRUE(pkeyIndex->moveToKey(&key));
        //            TableTuple tuple = pkeyIndex->nextValueAtKey();
        //            m_table->deleteTuple(tuple, true);
        //        }

    }
    m_table->doForcedCompaction();
    ASSERT_EQ( m_table->m_data.size(), 0);
    ASSERT_EQ( m_table->activeTupleCount(), 0);
    for (int ii = 0; ii < tupleCount; ii++) {
        ASSERT_TRUE(COWTuples.find(ii) != COWTuples.end());
    }
}

/*
 * The problem I suspect in ENG897 is that the last
 * block handled by the COW iterator is not returned back to the set of
 * blocks that are not pending snapshot. This causes that block to
 * be passed to a null COW iterator when it is compacted.
 */
#ifndef MEMCHECK
TEST_F(CompactionTest, TestENG897) {
    initTable();
    addRandomUniqueTuples( m_table, 32263 * 5);

    //Delete stuff to put everything in a bucket
    voltdb::TableIndex *pkeyIndex = m_table->primaryKeyIndex();
    TableTuple key(pkeyIndex->getKeySchema());
    boost::scoped_array<char> backingStore(new char[pkeyIndex->getKeySchema()->tupleLength()]);
    key.moveNoHeader(backingStore.get());

    IndexCursor indexCursor(pkeyIndex->getTupleSchema());

    for (int ii = 0; ii < 32263 * 5; ii++) {
        if (ii % 2 == 0) {
            key.setNValue(0, ValueFactory::getIntegerValue(ii));
            ASSERT_TRUE(pkeyIndex->moveToKey(&key, indexCursor));
            TableTuple tuple = pkeyIndex->nextValueAtKey(indexCursor);
            m_table->deleteTuple(tuple, true);
        }
    }

    //m_table->printBucketInfo();

    size_t blocksNotPendingSnapshot = m_table->getBlocksNotPendingSnapshotCount();
    ASSERT_EQ(5, blocksNotPendingSnapshot);
    DefaultTupleSerializer serializer;
    char config[5];
    ::memset(config, 0, 5);
    ReferenceSerializeInputBE input(config, 5);

    m_table->activateStream(serializer, TABLE_STREAM_SNAPSHOT, 0, m_tableId, input);
    for (int ii = 0; ii < 16130; ii++) {
        if (ii % 2 == 0) {
            continue;
        }
        key.setNValue(0, ValueFactory::getIntegerValue(ii));
        ASSERT_TRUE(pkeyIndex->moveToKey(&key, indexCursor));
        TableTuple tuple = pkeyIndex->nextValueAtKey(indexCursor);
        m_table->deleteTuple(tuple, true);
    }

    //m_table->printBucketInfo();
    //std::cout << "Starting snapshot serialization" << std::endl;
    char serializationBuffer[2097152];
    while (true) {
        TupleOutputStreamProcessor outs( serializationBuffer, 2097152);
        TupleOutputStream &out = outs.at(0);
        std::vector<int> retPositions;
        m_table->streamMore(outs, TABLE_STREAM_SNAPSHOT, retPositions);
        if (out.position() == 0) {
            break;
        }
        //m_table->printBucketInfo();
    }
    ASSERT_EQ( blocksNotPendingSnapshot, m_table->getBlocksNotPendingSnapshotCount());

    //std::cout << "Finished snapshot serialization" << std::endl;

    for (int ii = 16130; ii < 32261; ii++) {
        if (ii % 2 == 0) {
            continue;
        }
        key.setNValue(0, ValueFactory::getIntegerValue(ii));
        ASSERT_TRUE(pkeyIndex->moveToKey(&key, indexCursor));
        TableTuple tuple = pkeyIndex->nextValueAtKey(indexCursor);
        m_table->deleteTuple(tuple, true);
    }

    //std::cout << "Before idle compaction" << std::endl;
    //m_table->printBucketInfo();
    ReferenceSerializeInputBE input2(config, 5);
    m_table->activateStream(serializer, TABLE_STREAM_SNAPSHOT, 0, m_tableId, input2);
    //std::cout << "Activated COW" << std::endl;
    //m_table->printBucketInfo();
    m_table->doIdleCompaction();
    //m_table->printBucketInfo();
}
#endif
int main() {
    return TestSuite::globalInstance()->runAll();
}
