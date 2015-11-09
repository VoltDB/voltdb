/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
 *
 * This file contains original code and/or modifications of original code.
 * Any modifications made by VoltDB Inc. are licensed under the following
 * terms and conditions:
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */
/* Copyright (C) 2008 by H-Store Project
 * Brown University
 * Massachusetts Institute of Technology
 * Yale University
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
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

#ifndef HSTOREPERSISTENTTABLE_H
#define HSTOREPERSISTENTTABLE_H

#include <string>
#include <vector>
#include <cassert>
#include <iostream>
#include <boost/scoped_ptr.hpp>
#include <boost/shared_ptr.hpp>
#include "common/declarations.h"
#include "common/types.h"
#include "common/ids.h"
#include "common/valuevector.h"
#include "common/tabletuple.h"
#include "execution/VoltDBEngine.h"
#include "storage/CopyOnWriteIterator.h"
#include "storage/ElasticIndex.h"
#include "storage/table.h"
#include "storage/ExportTupleStream.h"
#include "storage/TableStats.h"
#include "storage/PersistentTableStats.h"
#include "storage/TableStreamerInterface.h"
#include "storage/RecoveryContext.h"
#include "storage/ElasticIndex.h"
#include "storage/CopyOnWriteIterator.h"
#include "structures/CompactingSet.h"
#include "common/UndoQuantumReleaseInterest.h"
#include "common/ThreadLocalPool.h"

class CompactionTest_BasicCompaction;
class CompactionTest_CompactionWithCopyOnWrite;
class CopyOnWriteTest;

namespace catalog {
class MaterializedViewInfo;
}

namespace voltdb {

/**
 * Interface used by contexts, scanners, iterators, and undo actions to access
 * normally-private stuff in PersistentTable.
 * Holds persistent state produced by contexts, e.g. the elastic index.
 */
class PersistentTableSurgeon {
    friend class PersistentTable;
    friend class ::CopyOnWriteTest;

public:

    TBMap &getData();
    PersistentTable& getTable();
    void insertTupleForUndo(char *tuple);
    void updateTupleForUndo(char* targetTupleToUpdate,
                            char* sourceTupleWithNewValues,
                            bool revertIndexes);
    bool deleteTuple(TableTuple &tuple, bool fallible=true);
    void deleteTupleForUndo(char* tupleData, bool skipLookup = false);
    void deleteTupleRelease(char* tuple);
    void deleteTupleStorage(TableTuple &tuple, TBPtr block = TBPtr(NULL));

    void snapshotFinishedScanningBlock(TBPtr finishedBlock, TBPtr nextBlock);
    uint32_t getTupleCount() const;

    // Elastic index methods. Used by ElasticContext.
    void clearIndex();
    void createIndex();
    void dropIndex();
    bool hasIndex() const;
    bool isIndexEmpty() const;
    size_t indexSize() const;
    bool isIndexingComplete() const;
    void setIndexingComplete();
    bool indexHas(TableTuple &tuple) const;
    bool indexAdd(TableTuple &tuple);
    bool indexRemove(TableTuple &tuple);
    void initTableStreamer(TableStreamerInterface* streamer);
    bool hasStreamType(TableStreamType streamType) const;
    ElasticIndex::iterator indexIterator();
    ElasticIndex::iterator indexIteratorLowerBound(int32_t lowerBound);
    ElasticIndex::iterator indexIteratorUpperBound(int32_t upperBound);
    ElasticIndex::const_iterator indexIterator() const;
    ElasticIndex::const_iterator indexIteratorLowerBound(int32_t lowerBound) const;
    ElasticIndex::const_iterator indexIteratorUpperBound(int32_t upperBound) const;
    ElasticIndex::iterator indexEnd();
    ElasticIndex::const_iterator indexEnd() const;
    boost::shared_ptr<ElasticIndexTupleRangeIterator>
            getIndexTupleRangeIterator(const ElasticIndexHashRange &range);
    void activateSnapshot();
    void printIndex(std::ostream &os, int32_t limit) const;
    ElasticHash generateTupleHash(TableTuple &tuple) const;
    void DRRollback(size_t drMark, size_t drRowCost);

private:

    /**
     * Only PersistentTable can call the constructor.
     */
    PersistentTableSurgeon(PersistentTable &table);

    /**
     * Only PersistentTable can call the destructor.
     */
    virtual ~PersistentTableSurgeon();

    PersistentTable &m_table;

    /**
     * Elastic index.
     */
    boost::scoped_ptr<ElasticIndex> m_index;

    /**
     * Set to true after handleStreamMore() was called once after building the index.
     */
    bool m_indexingComplete;
};

/**
 * Represents a non-temporary table which permanently resides in
 * storage and also registered to Catalog (see other documents for
 * details of Catalog). PersistentTable has several additional
 * features to Table.  It has indexes, constraints to check NULL and
 * uniqueness as well as undo logs to revert changes.
 *
 * PersistentTable can have one or more Indexes, one of which must be
 * Primary Key Index. Primary Key Index is same as other Indexes except
 * that it's used for deletion and updates. Our Execution Engine collects
 * Primary Key values of deleted/updated tuples and uses it for specifying
 * tuples, assuming every PersistentTable has a Primary Key index.
 *
 * Currently, constraints are not-null constraint and unique
 * constraint.  Not-null constraint is just a flag of TableColumn and
 * checked against insertion and update. Unique constraint is also
 * just a flag of TableIndex and checked against insertion and
 * update. There's no rule constraint or foreign key constraint so far
 * because our focus is performance and simplicity.
 *
 * To revert changes after execution, PersistentTable holds UndoLog.
 * PersistentTable does eager update which immediately changes the
 * value in data and adds an entry to UndoLog. We chose eager update
 * policy because we expect reverting rarely occurs.
 */

class PersistentTable : public Table, public UndoQuantumReleaseInterest,
                        public TupleMovementListener {
    friend class PersistentTableSurgeon;
    friend class TableFactory;
    friend class ::CopyOnWriteTest;
    friend class ::CompactionTest_BasicCompaction;
    friend class ::CompactionTest_CompactionWithCopyOnWrite;

  private:
    // no default ctor, no copy, no assignment
    PersistentTable();
    PersistentTable(PersistentTable const&);
    PersistentTable operator=(PersistentTable const&);

    // default iterator
    TableIterator m_iter;

 protected:

    virtual void initializeWithColumns(TupleSchema *schema, const std::vector<std::string> &columnNames, bool ownsTupleSchema, int32_t compactionThreshold = 95);

  public:
    virtual ~PersistentTable();

    void notifyQuantumRelease() {
        if (compactionPredicate()) {
            doForcedCompaction();
        }
    }

    // Return a table iterator by reference
    TableIterator& iterator() {
        m_iter.reset(m_data.begin());
        return m_iter;
    }

    TableIterator* makeIterator() {
        return new TableIterator(this, m_data.begin());
    }

    TableIterator& iteratorDeletingAsWeGo() {
        m_iter.reset(m_data.begin());
        m_iter.setTempTableDeleteAsGo(false);
        return m_iter;
    }


    // ------------------------------------------------------------------
    // GENERIC TABLE OPERATIONS
    // ------------------------------------------------------------------
    virtual void deleteAllTuples(bool freeAllocatedStrings);

    virtual void truncateTable(VoltDBEngine* engine, bool fallible = true);
    // The fallible flag is used to denote a change to a persistent table
    // which is part of a long transaction that has been vetted and can
    // never fail (e.g. violate a constraint).
    // The initial use case is a live catalog update that changes table schema and migrates tuples
    // and/or adds a materialized view.
    // Constraint checks are bypassed and the change does not make use of "undo" support.
    // TODO: change meaningless bool return type to void (starting in class Table) and migrate callers.
    virtual bool deleteTuple(TableTuple &tuple, bool fallible=true);
    // TODO: change meaningless bool return type to void (starting in class Table) and migrate callers.
    virtual bool insertTuple(TableTuple &tuple);
    // Optimized version of update that only updates specific indexes.
    // The caller knows which indexes MAY need to be updated.
    // Note that inside update tuple the order of sourceTuple and
    // targetTuple is swapped when making calls on the indexes. This
    // is just an inconsistency in the argument ordering.
    // TODO: change meaningless bool return type to void (starting in class Table) and migrate callers.
    // The fallible flag is used to denote a change to a persistent table
    // which is part of a long transaction that has been vetted and can
    // never fail (e.g. violate a constraint).
    // The initial use case is a live catalog update that changes table schema and migrates tuples
    // and/or adds a materialized view.
    // Constraint checks are bypassed and the change does not make use of "undo" support.
    // TODO: change meaningless bool return type to void (starting in class Table) and migrate callers.
    virtual bool updateTupleWithSpecificIndexes(TableTuple &targetTupleToUpdate,
                                                TableTuple &sourceTupleWithNewValues,
                                                std::vector<TableIndex*> const &indexesToUpdate,
                                                bool fallible=true);

    virtual void addIndex(TableIndex *index) {
        Table::addIndex(index);
        m_noAvailableUniqueIndex = false;
        m_smallestUniqueIndex = NULL;
        m_smallestUniqueIndexCrc = 0;
    }
    virtual void removeIndex(TableIndex *index) {
        Table::removeIndex(index);
        m_smallestUniqueIndex = NULL;
        m_smallestUniqueIndexCrc = 0;
    }

    // ------------------------------------------------------------------
    // PERSISTENT TABLE OPERATIONS
    // ------------------------------------------------------------------
    void deleteTupleForSchemaChange(TableTuple &target);

    void insertPersistentTuple(TableTuple &source, bool fallible);

    /// This is not used in any production code path -- it is a convenient wrapper used by tests.
    bool updateTuple(TableTuple &targetTupleToUpdate, TableTuple &sourceTupleWithNewValues)
    {
        updateTupleWithSpecificIndexes(targetTupleToUpdate, sourceTupleWithNewValues, m_indexes, true);
        return true;
    }

    /*
     * Lookup the address of the tuple whose values are identical to the specified tuple.
     * Does a primary key lookup or table scan if necessary.
     */
    voltdb::TableTuple lookupTupleByValues(TableTuple tuple);

    /*
     * Lookup the address of the tuple that is identical to the specified tuple.
     * It is assumed that the tuple argument was first retrieved from this table.
     * Does a primary key lookup or table scan if necessary.
     */
    voltdb::TableTuple lookupTupleForUndo(TableTuple tuple);

    /*
     * Functions the same as lookupTupleByValues(), but takes the DR hidden timestamp
     * column into account.
     */
    voltdb::TableTuple lookupTupleForDR(TableTuple tuple);

    // ------------------------------------------------------------------
    // UTILITY
    // ------------------------------------------------------------------
    std::string tableType() const;
    virtual std::string debug();

    /*
     * Find the block a tuple belongs to. Returns TBPtr(NULL) if no block is found.
     */
    static TBPtr findBlock(char *tuple, TBMap &blocks, int blockSize);

    int partitionColumn() const { return m_partitionColumn; }

    std::vector<MaterializedViewMetadata *> views() const {
        return m_views;
    }

    /** inlined here because it can't be inlined in base Table, as it
     *  uses Tuple.copy.
     */
    TableTuple& getTempTupleInlined(TableTuple &source);

    /** Add/drop/list materialized views to this table */
    void addMaterializedView(MaterializedViewMetadata *view);

    /**
     * Prepare table for streaming from serialized data.
     * Return true on success or false if it was already active.
     */
    bool activateStream(TupleSerializer &tupleSerializer,
                        TableStreamType streamType,
                        int32_t partitionId,
                        CatalogId tableId,
                        ReferenceSerializeInputBE &serializeIn);

    void dropMaterializedView(MaterializedViewMetadata *targetView);
    void segregateMaterializedViews(std::map<std::string, catalog::MaterializedViewInfo*>::const_iterator const & start,
                                    std::map<std::string, catalog::MaterializedViewInfo*>::const_iterator const & end,
                                    std::vector<catalog::MaterializedViewInfo*> &survivingInfosOut,
                                    std::vector<MaterializedViewMetadata*> &survivingViewsOut,
                                    std::vector<MaterializedViewMetadata*> &obsoleteViewsOut);
    void updateMaterializedViewTargetTable(PersistentTable* target, catalog::MaterializedViewInfo* targetMvInfo);

    /**
     * Attempt to stream more tuples from the table to the provided
     * output stream.
     * Return remaining tuple count, 0 if done, or TABLE_STREAM_SERIALIZATION_ERROR on error.
     */
    int64_t streamMore(TupleOutputStreamProcessor &outputStreams,
                       TableStreamType streamType,
                       std::vector<int> &retPositions);

    /**
     * Process the updates from a recovery message
     */
    void processRecoveryMessage(RecoveryProtoMsg* message, Pool *pool);

    /**
     * Create a tree index on the primary key and then iterate it and hash
     * the tuple data.
     */
    size_t hashCode();

    size_t getBlocksNotPendingSnapshotCount() {
        return m_blocksNotPendingSnapshot.size();
    }

    void doIdleCompaction();
    void printBucketInfo();

    void increaseStringMemCount(size_t bytes)
    {
        m_nonInlinedMemorySize += bytes;
    }
    void decreaseStringMemCount(size_t bytes)
    {
        m_nonInlinedMemorySize -= bytes;
    }

    size_t allocatedBlockCount() const {
        return m_data.size();
    }

    // This is a testability feature not intended for use in product logic.
    int visibleTupleCount() const { return m_tupleCount - m_invisibleTuplesPendingDeleteCount; }

    int tupleLimit() const {
        return m_tupleLimit;
    }

    inline bool isReplicatedTable() const {
        return (m_partitionColumn == -1);
    }

    /** Returns true if DR is enabled for this table */
    bool isDREnabled() const { return m_drEnabled; }

    /** Returns true if there is a hidden column in this table for the
        DR timestamp (used to resolve active/active conflicts) */
    bool hasDRTimestampColumn() const { return m_drTimestampColumnIndex != -1; }

    /** Returns the index of the DR timestamp column (relative to the
        hidden columns for the table).  If there's no DR timestamp
        column, returns -1. */
    int getDRTimestampColumnIndex() const { return m_drTimestampColumnIndex; }

    // for test purpose
    void setDR(bool flag) { m_drEnabled = flag; }

    void setTupleLimit(int32_t newLimit) {
        m_tupleLimit = newLimit;
    }

    bool isPersistentTableEmpty()
    {
        // The narrow usage of this function (while updating the catalog)
        // suggests that it could also mean "table is new and never had tuples".
        // So, it's OK and possibly MORE correct to count active tuples and ignore the effect of
        // m_invisibleTuplesPendingDeleteCount even when it would change the answer --
        // if ALL tuples had been deleted earlier in the current transaction.
        // This should never be the case while updating the catalog.
        return m_tupleCount == 0;
    }

    virtual int64_t validatePartitioning(TheHashinator *hashinator, int32_t partitionId);

    void truncateTableForUndo(VoltDBEngine * engine, TableCatalogDelegate * tcd, PersistentTable *originalTable);
    void truncateTableRelease(PersistentTable *originalTable);

    PersistentTable * getPreTruncateTable() {
        return m_preTruncateTable;
    }

    PersistentTable * currentPreTruncateTable() {
        if (m_preTruncateTable != NULL) {
            return m_preTruncateTable;
        }
        return this;
    }

    void setPreTruncateTable(PersistentTable * tb) {
        if (tb->getPreTruncateTable() != NULL) {
            m_preTruncateTable = tb->getPreTruncateTable();
        } else {
            m_preTruncateTable= tb;
        }

        if (m_preTruncateTable != NULL) {
            m_preTruncateTable->incrementRefcount();
        }
    }

    void unsetPreTruncateTable() {
        PersistentTable * prev = this->m_preTruncateTable;
        if (prev != NULL) {
            this->m_preTruncateTable = NULL;
            prev->decrementRefcount();
        }
    }

    /**
     * Returns true if this table has a fragment that may be executed
     * when the table's row limit will be exceeded.
     */
    bool hasPurgeFragment() const {
        return m_purgeExecutorVector.get() != NULL;
    }

    /**
     * Sets the purge executor vector for this table to method
     * argument (Using swap instead of reset so that ExecutorVector
     * may remain a forward-declared incomplete type here)
     */
    void swapPurgeExecutorVector(boost::shared_ptr<ExecutorVector> ev) {
        m_purgeExecutorVector.swap(ev);
    }

    /**
     * Returns the purge executor vector for this table
     */
    boost::shared_ptr<ExecutorVector> getPurgeExecutorVector() {
        assert(hasPurgeFragment());
        return m_purgeExecutorVector;
    }

    std::pair<const TableIndex*, uint32_t> getUniqueIndexForDR();

  private:

    // Zero allocation size uses defaults.
    PersistentTable(int partitionColumn, char *signature, bool isMaterialized, int tableAllocationTargetSize = 0, int tuplelimit = INT_MAX, bool drEnabled = false);

    /**
     * Prepare table for streaming from serialized data (internal for tests).
     * Use custom TableStreamer provided.
     * Return true on success or false if it was already active.
     */
    bool activateWithCustomStreamer(TupleSerializer &tupleSerializer,
                                    TableStreamType streamType,
                                    boost::shared_ptr<TableStreamerInterface> tableStreamer,
                                    CatalogId tableId,
                                    std::vector<std::string> &predicateStrings,
                                    bool skipInternalActivation);

    void snapshotFinishedScanningBlock(TBPtr finishedBlock, TBPtr nextBlock) {
        if (nextBlock != NULL) {
            assert(m_blocksPendingSnapshot.find(nextBlock) != m_blocksPendingSnapshot.end());
            m_blocksPendingSnapshot.erase(nextBlock);
            nextBlock->swapToBucket(TBBucketPtr());
        }
        if (finishedBlock != NULL && !finishedBlock->isEmpty()) {
            m_blocksNotPendingSnapshot.insert(finishedBlock);
            int bucketIndex = finishedBlock->calculateBucketIndex();
            if (bucketIndex != INVALID_NEW_BUCKET_INDEX) {
                finishedBlock->swapToBucket(m_blocksNotPendingSnapshotLoad[bucketIndex]);
            }
        }
    }

    void nextFreeTuple(TableTuple *tuple);
    bool doCompactionWithinSubset(TBBucketPtrVector *bucketVector);
    void doForcedCompaction();

    void insertIntoAllIndexes(TableTuple *tuple);
    void deleteFromAllIndexes(TableTuple *tuple);
    void tryInsertOnAllIndexes(TableTuple *tuple, TableTuple *conflict);
    bool checkUpdateOnUniqueIndexes(TableTuple &targetTupleToUpdate,
                                    const TableTuple &sourceTupleWithNewValues,
                                    std::vector<TableIndex*> const &indexesToUpdate);

    bool checkNulls(TableTuple &tuple) const;

    void onSetColumns();

    void notifyBlockWasCompactedAway(TBPtr block);

    // Call-back from TupleBlock::merge() for each tuple moved.
    virtual void notifyTupleMovement(TBPtr sourceBlock, TBPtr targetBlock,
                                     TableTuple &sourceTuple, TableTuple &targetTuple);

    void swapTuples(TableTuple &sourceTupleWithNewValues, TableTuple &destinationTuple);

    // The source tuple is used to create the ConstraintFailureException if one
    // occurs. In case of exception, target tuple should be released, but the
    // source tuple's memory should still be retained until the exception is
    // handled.
    void insertTupleCommon(TableTuple &source, TableTuple &target, bool fallible, bool shouldDRStream = true);
    void insertTupleForUndo(char *tuple);
    void updateTupleForUndo(char* targetTupleToUpdate,
                            char* sourceTupleWithNewValues,
                            bool revertIndexes);
    void deleteTupleForUndo(char* tupleData, bool skipLookup = false);
    void deleteTupleRelease(char* tuple);
    void deleteTupleFinalize(TableTuple &tuple);
    /**
     * Normally this will return the tuple storage to the free list.
     * In the memcheck build it will return the storage to the heap.
     */
    void deleteTupleStorage(TableTuple &tuple, TBPtr block = TBPtr(NULL));

    /*
     * Implemented by persistent table and called by Table::loadTuplesFrom
     * to do additional processing for views and Export
     */
    virtual void processLoadedTuple(TableTuple &tuple,
                                    ReferenceSerializeOutput *uniqueViolationOutput,
                                    int32_t &serializedTupleCount,
                                    size_t &tupleCountPosition,
                                    bool shouldDRStreamRows);

    enum LookupType {
        LOOKUP_BY_VALUES,
        LOOKUP_FOR_DR,
        LOOKUP_FOR_UNDO
    };
    TableTuple lookupTuple(TableTuple tuple, LookupType lookupType);

    TBPtr allocateNextBlock();

    inline DRTupleStream *getDRTupleStream(ExecutorContext *ec) {
        if (isReplicatedTable()) {
            return ec->drReplicatedStream();
        } else {
            return ec->drStream();
        }
    }

    void setDRTimestampForTuple(ExecutorContext* ec, TableTuple &tuple, bool update);

    void computeSmallestUniqueIndex();

    // CONSTRAINTS
    std::vector<bool> m_allowNulls;

    // partition key
    const int m_partitionColumn;

    // table row count limit
    int m_tupleLimit;

    // Executor vector to be executed when imminent insert will exceed
    // tuple limit
    boost::shared_ptr<ExecutorVector> m_purgeExecutorVector;

    // list of materialized views that are sourced from this table
    std::vector<MaterializedViewMetadata *> m_views;

    // STATS
    voltdb::PersistentTableStats stats_;
    voltdb::TableStats* getTableStats();

    // STORAGE TRACKING

    // Map from load to the blocks with level of load
    TBBucketPtrVector m_blocksNotPendingSnapshotLoad;
    TBBucketPtrVector m_blocksPendingSnapshotLoad;

    // Map containing blocks that aren't pending snapshot
    boost::unordered_set<TBPtr> m_blocksNotPendingSnapshot;

    // Map containing blocks that are pending snapshot
    boost::unordered_set<TBPtr> m_blocksPendingSnapshot;

    // Set of blocks with non-empty free lists or available tuples
    // that have never been allocated
    CompactingSet<TBPtr> m_blocksWithSpace;

    // Provides access to all table streaming apparati, including COW and recovery.
    boost::shared_ptr<TableStreamerInterface> m_tableStreamer;

    // pointers to chunks of data. Specific to table impl. Don't leak this type.
    TBMap m_data;
    int m_failedCompactionCount;

    // This is a testability feature not intended for use in product logic.
    int m_invisibleTuplesPendingDeleteCount;

    // Surgeon passed to classes requiring "deep" access to avoid excessive friendship.
    PersistentTableSurgeon m_surgeon;

    // The original table from the first truncated table
    PersistentTable * m_preTruncateTable;

    //Cache config info, is this a materialized view
    bool m_isMaterialized;

    // is DR enabled
    bool m_drEnabled;

    //SHA-1 of signature string
    char m_signature[20];

    bool m_noAvailableUniqueIndex;
    TableIndex* m_smallestUniqueIndex;
    uint32_t m_smallestUniqueIndexCrc;
    int m_drTimestampColumnIndex;
};

inline PersistentTableSurgeon::PersistentTableSurgeon(PersistentTable &table) :
    m_table(table),
    m_indexingComplete(false)
{}

inline PersistentTableSurgeon::~PersistentTableSurgeon()
{}

inline TBMap &PersistentTableSurgeon::getData() {
    return m_table.m_data;
}

inline PersistentTable& PersistentTableSurgeon::getTable() {
    return m_table;
}

inline void PersistentTableSurgeon::insertTupleForUndo(char *tuple) {
    m_table.insertTupleForUndo(tuple);
}

inline void PersistentTableSurgeon::updateTupleForUndo(char* targetTupleToUpdate,
                                                       char* sourceTupleWithNewValues,
                                                       bool revertIndexes) {
    m_table.updateTupleForUndo(targetTupleToUpdate, sourceTupleWithNewValues, revertIndexes);
}

inline bool PersistentTableSurgeon::deleteTuple(TableTuple &tuple, bool fallible) {
    return m_table.deleteTuple(tuple, fallible);
}

inline void PersistentTableSurgeon::deleteTupleForUndo(char* tupleData, bool skipLookup) {
    m_table.deleteTupleForUndo(tupleData, skipLookup);
}

inline void PersistentTableSurgeon::deleteTupleRelease(char* tuple) {
    m_table.deleteTupleRelease(tuple);
}

inline void PersistentTableSurgeon::deleteTupleStorage(TableTuple &tuple, TBPtr block) {
    m_table.deleteTupleStorage(tuple, block);
}

inline void PersistentTableSurgeon::snapshotFinishedScanningBlock(TBPtr finishedBlock, TBPtr nextBlock) {
    m_table.snapshotFinishedScanningBlock(finishedBlock, nextBlock);
}

inline bool PersistentTableSurgeon::hasIndex() const {
    return (m_index != NULL);
}

inline bool PersistentTableSurgeon::isIndexEmpty() const {
    assert (m_index != NULL);
    return (m_index->size() == (size_t)0);
}

inline size_t PersistentTableSurgeon::indexSize() const {
    assert (m_index != NULL);
    return m_index->size();
}

inline bool PersistentTableSurgeon::isIndexingComplete() const {
    assert (m_index != NULL);
    return m_indexingComplete;
}

inline void PersistentTableSurgeon::setIndexingComplete() {
    assert (m_index != NULL);
    m_indexingComplete = true;
}

inline void PersistentTableSurgeon::createIndex() {
    assert(m_index == NULL);
    m_index.reset(new ElasticIndex());
    m_indexingComplete = false;
}

inline void PersistentTableSurgeon::dropIndex() {
    assert(m_indexingComplete == true);
    m_index.reset(NULL);
    m_indexingComplete = false;
}

inline void PersistentTableSurgeon::clearIndex() {
    assert (m_index != NULL);
    m_index->clear();
    m_indexingComplete = false;
}

inline void PersistentTableSurgeon::printIndex(std::ostream &os, int32_t limit) const {
    assert (m_index != NULL);
    m_index->printKeys(os,limit,m_table.m_schema,m_table);
}

inline ElasticHash PersistentTableSurgeon::generateTupleHash(TableTuple &tuple) const {
    return tuple.getNValue(m_table.partitionColumn()).murmurHash3();
}

inline bool PersistentTableSurgeon::indexHas(TableTuple &tuple) const {
    assert (m_index != NULL);
    return m_index->has(m_table, tuple);
}

inline bool PersistentTableSurgeon::indexAdd(TableTuple &tuple) {
    assert (m_index != NULL);
    return m_index->add(m_table, tuple);
}

inline bool PersistentTableSurgeon::indexRemove(TableTuple &tuple) {
    assert (m_index != NULL);
    return m_index->remove(m_table, tuple);
}

inline ElasticIndex::iterator PersistentTableSurgeon::indexIterator() {
    assert (m_index != NULL);
    return m_index->createIterator();
}

inline ElasticIndex::iterator PersistentTableSurgeon::indexIteratorLowerBound(int32_t lowerBound) {
    assert (m_index != NULL);
    return m_index->createLowerBoundIterator(lowerBound);
}

inline ElasticIndex::iterator PersistentTableSurgeon::indexIteratorUpperBound(int32_t upperBound) {
    assert (m_index != NULL);
    return m_index->createUpperBoundIterator(upperBound);
}

inline ElasticIndex::const_iterator PersistentTableSurgeon::indexIterator() const {
    assert (m_index != NULL);
    return m_index->createIterator();
}

inline ElasticIndex::const_iterator PersistentTableSurgeon::indexIteratorLowerBound(int32_t lowerBound) const {
    assert (m_index != NULL);
    return m_index->createLowerBoundIterator(lowerBound);
}

inline ElasticIndex::const_iterator PersistentTableSurgeon::indexIteratorUpperBound(int32_t upperBound) const {
    assert (m_index != NULL);
    return m_index->createUpperBoundIterator(upperBound);
}

inline ElasticIndex::iterator PersistentTableSurgeon::indexEnd() {
    assert (m_index != NULL);
    return m_index->end();
}

inline ElasticIndex::const_iterator PersistentTableSurgeon::indexEnd() const {
    assert (m_index != NULL);
    return m_index->end();
}

inline uint32_t PersistentTableSurgeon::getTupleCount() const {
    return m_table.m_tupleCount;
}

inline void PersistentTableSurgeon::initTableStreamer(TableStreamerInterface* streamer) {
    assert(m_table.m_tableStreamer == NULL);
    m_table.m_tableStreamer.reset(streamer);
}

inline bool PersistentTableSurgeon::hasStreamType(TableStreamType streamType) const {
    assert(m_table.m_tableStreamer != NULL);
    return m_table.m_tableStreamer->hasStreamType(streamType);
}

inline boost::shared_ptr<ElasticIndexTupleRangeIterator>
PersistentTableSurgeon::getIndexTupleRangeIterator(const ElasticIndexHashRange &range) {
    assert(m_index != NULL);
    assert(m_table.m_schema != NULL);
    return boost::shared_ptr<ElasticIndexTupleRangeIterator>(
            new ElasticIndexTupleRangeIterator(*m_index, *m_table.m_schema, range));
}

inline void
PersistentTableSurgeon::DRRollback(size_t drMark, size_t drRowCost) {
    if (!m_table.m_isMaterialized && m_table.m_drEnabled) {
        if (m_table.m_partitionColumn == -1) {
            if (ExecutorContext::getExecutorContext()->drReplicatedStream()) {
                ExecutorContext::getExecutorContext()->drReplicatedStream()->rollbackTo(drMark, drRowCost);
            }
        } else {
            ExecutorContext::getExecutorContext()->drStream()->rollbackTo(drMark, drRowCost);
        }
    }
}

inline TableTuple& PersistentTable::getTempTupleInlined(TableTuple &source) {
    assert (m_tempTuple.m_data);
    m_tempTuple.copy(source);
    return m_tempTuple;
}


inline void PersistentTable::deleteTupleStorage(TableTuple &tuple, TBPtr block)
{
    // May not delete an already deleted tuple.
    assert(tuple.isActive());

    // The tempTuple is forever!
    assert(&tuple != &m_tempTuple);

    // This frees referenced strings -- when could possibly be a better time?
    if (m_schema->getUninlinedObjectColumnCount() != 0) {
        decreaseStringMemCount(tuple.getNonInlinedMemorySize());
        tuple.freeObjectColumns();
    }

    tuple.setActiveFalse();

    // add to the free list
    m_tupleCount--;
    if (tuple.isPendingDelete()) {
        tuple.setPendingDeleteFalse();
        --m_invisibleTuplesPendingDeleteCount;
    }

    if (block.get() == NULL) {
        block = findBlock(tuple.address(), m_data, m_tableAllocationSize);
        if (block.get() == NULL) {
            throwFatalException("Tried to find a tuple block for a tuple but couldn't find one");
        }
    }

    bool transitioningToBlockWithSpace = !block->hasFreeTuples();

    int retval = block->freeTuple(tuple.address());
    if (retval != INVALID_NEW_BUCKET_INDEX) {
        //Check if if the block is currently pending snapshot
        if (m_blocksNotPendingSnapshot.find(block) != m_blocksNotPendingSnapshot.end()) {
            //std::cout << "Swapping block " << static_cast<void*>(block.get()) << " to bucket " << retval << std::endl;
            block->swapToBucket(m_blocksNotPendingSnapshotLoad[retval]);
        //Check if the block goes into the pending snapshot set of buckets
        } else if (m_blocksPendingSnapshot.find(block) != m_blocksPendingSnapshot.end()) {
            block->swapToBucket(m_blocksPendingSnapshotLoad[retval]);
        } else {
            //In this case the block is actively being snapshotted and isn't eligible for merge operations at all
            //do nothing, once the block is finished by the iterator, the iterator will return it
        }
    }

    if (block->isEmpty()) {
        m_data.erase(block->address());
        m_blocksWithSpace.erase(block);
        m_blocksNotPendingSnapshot.erase(block);
        assert(m_blocksPendingSnapshot.find(block) == m_blocksPendingSnapshot.end());
        //Eliminates circular reference
        block->swapToBucket(TBBucketPtr());
    } else if (transitioningToBlockWithSpace) {
        m_blocksWithSpace.insert(block);
    }
}

inline TBPtr PersistentTable::findBlock(char *tuple, TBMap &blocks, int blockSize) {
    if (!blocks.empty()) {
        TBMapI i = blocks.lowerBound(tuple);

        // Not the first tuple of any known block, move back a block, see if it
        // belongs to the previous block
        if (i.isEnd() || i.key() != tuple) {
            i--;
        }

        // If the tuple is within the block boundaries, we found the block
        if (i.key() <= tuple && tuple < i.key() + blockSize) {
            if (i.value().get() == NULL) {
                throwFatalException("A block has gone missing in the tuple block map.");
            }
            return i.value();
        }
    }

    return TBPtr(NULL);
}

inline TBPtr PersistentTable::allocateNextBlock()
{
    TBPtr block(new TupleBlock(this, m_blocksNotPendingSnapshotLoad[0]));
    m_data.insert(block->address(), block);
    m_blocksNotPendingSnapshot.insert(block);
    return block;
}

inline TableTuple PersistentTable::lookupTupleByValues(TableTuple tuple) {
    return lookupTuple(tuple, LOOKUP_BY_VALUES);
}

inline TableTuple PersistentTable::lookupTupleForUndo(TableTuple tuple) {
    return lookupTuple(tuple, LOOKUP_FOR_UNDO);
}

inline TableTuple PersistentTable::lookupTupleForDR(TableTuple tuple) {
    return lookupTuple(tuple, LOOKUP_FOR_DR);
}

}



#endif
