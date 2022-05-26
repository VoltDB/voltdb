/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
 *
 * This file contains original code and/or modifications of original code.
 * Any modifications made by Volt Active Data Inc. are licensed under the following
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

#pragma once

#include <string>
#include <vector>
#include <iostream>
#include <boost/scoped_ptr.hpp>
#include <boost/shared_ptr.hpp>
#include "common/debuglog.h"
#include "common/types.h"
#include "common/ids.h"
#include "common/valuevector.h"
#include "common/tabletuple.h"
#include "execution/VoltDBEngine.h"
#include "storage/CopyOnWriteIterator.h"
#include "storage/ElasticIndex.h"
#include "storage/ExportTupleStream.h"
#include "storage/TableStats.h"
#include "storage/PersistentTableStats.h"
#include "storage/tableiterator.h"
#include "storage/TableStreamerInterface.h"
#include "storage/ElasticIndex.h"
#include "storage/DRTupleStream.h"
#include "storage/viewableandreplicabletable.h"
#include "storage/streamedtable.h"
#include "common/UndoQuantumReleaseInterest.h"
#include "common/ThreadLocalPool.h"
#include "common/SynchronizedThreadLock.h"
#include <map>
#include <set>


class CompactionTest_BasicCompaction;
class CompactionTest_CompactionWithMigratingRows;
class CompactionTest_CompactionWithCopyOnWrite;
class CopyOnWriteTest;

namespace catalog {
class MaterializedViewInfo;
}

namespace voltdb {
class CoveringCellIndexTest_TableCompaction;
class MaterializedViewTriggerForInsert;
class MaterializedViewTriggerForWrite;
class MaterializedViewHandler;
class TableIndex;

/**
 * Interface used by contexts, scanners, iterators, and undo actions to access
 * normally-private stuff in PersistentTable.
 * Holds persistent state produced by contexts, e.g. the elastic index.
 */
class PersistentTableSurgeon {
    friend class PersistentTable;
    friend class ::CopyOnWriteTest;

public:

    TBMap& getData() const;
    PersistentTable& getTable();
    void insertTupleForUndo(char* tuple);
    void updateTupleForUndo(char* targetTupleToUpdate,
                            char* sourceTupleWithNewValues,
                            bool revertIndexes,
                            bool fromMigrate);
    // The fallible flag is used to denote a change to a persistent table
    // which is part of a long transaction that has been vetted and can
    // never fail (e.g. violate a constraint).
    // The initial use case is a live catalog update that changes table schema and migrates tuples
    // and/or adds a materialized view.
    // Constraint checks are bypassed and the change does not make use of "undo" support.
    void deleteTuple(TableTuple& tuple, bool fallible = true, bool removeMigratingIndex = true);
    void deleteTupleForUndo(char* tupleData, bool skipLookup = false);
    void deleteTupleRelease(char* tuple);
    void deleteTupleStorage(TableTuple& tuple, TBPtr block = TBPtr(NULL));

    size_t getSnapshotPendingBlockCount() const;
    size_t getSnapshotPendingLoadBlockCount() const;
    bool blockCountConsistent() const;
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
    bool indexHas(TableTuple& tuple) const;
    bool indexAdd(TableTuple& tuple);
    bool indexRemove(TableTuple& tuple);
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
            getIndexTupleRangeIterator(ElasticIndexHashRange const& range);
    void activateSnapshot();
    void printIndex(std::ostream& os, int32_t limit) const;
    ElasticHash generateTupleHash(TableTuple& tuple) const;

private:

    /**
     * Only PersistentTable can call the constructor.
     */
    PersistentTableSurgeon(PersistentTable& table);

    /**
     * Only PersistentTable can call the destructor.
     */
    ~PersistentTableSurgeon() { }

    PersistentTable& m_table;

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

class PersistentTable : public ViewableAndReplicableTable<MaterializedViewTriggerForWrite>, public UndoQuantumReleaseInterest,
                        public TupleMovementListener {
    friend class PersistentTableSurgeon;
    friend class TableFactory;
    friend class JumpingTableIterator;
    friend class ::CopyOnWriteTest;
    friend class ::CompactionTest_BasicCompaction;
    friend class ::CompactionTest_CompactionWithMigratingRows;
    friend class ::CompactionTest_CompactionWithCopyOnWrite;
    friend class CoveringCellIndexTest_TableCompaction;
    friend class MaterializedViewHandler;
    friend class ScopedDeltaTableContext;

private:
    // no default ctor, no copy, no assignment
    PersistentTable();
    PersistentTable(PersistentTable const&);
    PersistentTable operator=(PersistentTable const&);

    virtual void initializeWithColumns(TupleSchema* schema,
            std::vector<std::string> const& columnNames,
            bool ownsTupleSchema,
            int32_t compactionThreshold = 95);
    void rollbackIndexChanges(TableTuple* tuple, int upto);

public:
    virtual ~PersistentTable();

    int64_t occupiedTupleMemory() const {
        return m_tupleCount * m_tempTuple.tupleLength();
    }

    void signature(char const* signature) {
        ::memcpy(&m_signature, signature, 20);
    }

    const char* signature() {
        return m_signature;
    }

    void notifyQuantumRelease() {
        if (compactionPredicate()) {
            doForcedCompaction();
        }
    }

    // Return a table iterator by reference
    TableIterator iterator() {
        return TableIterator(this, m_data.begin());
    }

    TableIterator iteratorDeletingAsWeGo() {
        // we don't delete persistent tuples "as we go",
        // so just return a normal iterator.
        return TableIterator(this, m_data.begin());
    }


    // ------------------------------------------------------------------
    // GENERIC TABLE OPERATIONS
    // ------------------------------------------------------------------
    void deleteAllTuples();

    void truncateTable(VoltDBEngine* engine);

    void swapTable
           (PersistentTable* otherTable,
            std::vector<std::string> const& theIndexes,
            std::vector<std::string> const& otherIndexes,
            bool fallible = true,
            bool isUndo = false);

    // The fallible flag is used to denote a change to a persistent table
    // which is part of a long transaction that has been vetted and can
    // never fail (e.g. violate a constraint).
    // The initial use case is a live catalog update that changes table schema
    // and migrates tuples and/or adds a materialized view.
    // Constraint checks are bypassed and the change does not make use of "undo" support.
    void deleteTuple(TableTuple& tuple, bool fallible = true, bool removeMigratingIndex = true);
    // TODO: change meaningless bool return type to void (starting in class Table) and migrate callers.
    virtual bool insertTuple(TableTuple& tuple);
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
    void updateTupleWithSpecificIndexes(TableTuple& targetTupleToUpdate,
          TableTuple& sourceTupleWithNewValues, std::vector<TableIndex*> const& indexesToUpdate,
          bool fallible = true, bool updateDRTimestamp = true, bool fromMigrate = false);

    // ------------------------------------------------------------------
    // INDEXES
    // ------------------------------------------------------------------
    int indexCount() const {
        return static_cast<int>(m_indexes.size());
    }

    int uniqueIndexCount() const {
        return static_cast<int>(m_uniqueIndexes.size());
    }

    // returned via shallow vector copy -- seems good enough.
    std::vector<TableIndex*> const& allIndexes() const { return m_indexes; }

    TableIndex* index(std::string const& name) const;

    TableIndex* primaryKeyIndex() const { return m_pkeyIndex; }

    void configureIndexStats();

    // mutating indexes
    void addIndex(TableIndex* index);
    void removeIndex(TableIndex* index);
    void setPrimaryKeyIndex(TableIndex* index);

    // ------------------------------------------------------------------
    // PERSISTENT TABLE OPERATIONS
    // ------------------------------------------------------------------
    void deleteTupleForSchemaChange(TableTuple& target);

    TableTuple insertPersistentTuple(TableTuple& source, bool fallible);

    /// This is not used in any production code path -- it is a convenient wrapper used by tests.
    bool updateTuple(TableTuple& targetTupleToUpdate, TableTuple& sourceTupleWithNewValues) {
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
    bool equals(PersistentTable* other);

    // Return a string containing info about this table
    std::string debug() const {
        return debug("");
    }
    virtual std::string debug(const std::string &spacer) const;

    /*
     * Find the block a tuple belongs to. Returns TBPtr(NULL) if no block is found.
     */
    static TBPtr findBlock(char* tuple, TBMap& blocks, int blockSize);

    int partitionColumn() const { return m_partitionColumn; }

    // The MatViewType typedef is required to satisfy initMaterializedViews
    // template code that needs to identify
    // "whatever MaterializedView*Trigger class is used by this *Table class".
    // There's no reason to actually use MatViewType in the class definition.
    // That would just make the code a little harder to analyze.
    typedef MaterializedViewTriggerForWrite MatViewType;

    TableTuple& copyIntoTempTuple(TableTuple& source) {
        vassert(m_tempTuple.m_data);
        m_tempTuple.copy(source);
        return m_tempTuple;
    }

    /**
     * Prepare table for streaming from serialized data.
     * Return true on success or false if it was already active.
     */
    bool activateStream(TableStreamType streamType,
                        HiddenColumnFilter::Type hiddenColumnFilterType,
                        int32_t partitionId,
                        CatalogId tableId,
                        ReferenceSerializeInputBE& serializeIn);

    /**
     * Attempt to stream more tuples from the table to the provided
     * output stream.
     * Return remaining tuple count, 0 if done, or TABLE_STREAM_SERIALIZATION_ERROR on error.
     */
    int64_t streamMore(TupleOutputStreamProcessor& outputStreams,
                       TableStreamType streamType,
                       std::vector<int>& retPositions);

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

    void increaseStringMemCount(size_t bytes) {
        m_nonInlinedMemorySize += bytes;
    }

    void decreaseStringMemCount(size_t bytes) {
        m_nonInlinedMemorySize -= bytes;
    }

    size_t allocatedBlockCount() const { return m_data.size(); }

    // This is a testability feature not intended for use in product logic.
    int visibleTupleCount() const { return m_tupleCount - m_invisibleTuplesPendingDeleteCount; }

    UndoQuantumReleaseInterest *getReplicatedInterest() { return &m_releaseReplicated; }
    UndoQuantumReleaseInterest *getDummyReplicatedInterest() { return &m_releaseDummyReplicated; }

    /** Returns true if DR is enabled for this table */
    bool isDREnabled() const { return m_drEnabled; }

    /** Returns true if there is a hidden column in this table for the
        DR timestamp (used to resolve active/active conflicts) */
    bool hasDRTimestampColumn() const { return m_schema->hasHiddenColumn(HiddenColumn::XDCR_TIMESTAMP); }

    /** Returns the index of the DR timestamp column (relative to the
        hidden columns for the table).  If there's no DR timestamp
        column, returns -1. */
    int getDRTimestampColumnIndex() const { return m_schema->getHiddenColumnIndex(HiddenColumn::XDCR_TIMESTAMP); }

    // for test purpose
    void setDR(bool flag) { m_drEnabled = (flag && !m_isMaterialized); }

    void setTableType(TableType tableType) { m_tableType = tableType; }
    bool isPersistentTableEmpty() const {
        // The narrow usage of this function (while updating the catalog)
        // suggests that it could also mean "table is new and never had tuples".
        // So, it's OK and possibly MORE correct to count active tuples and ignore the effect of
        // m_invisibleTuplesPendingDeleteCount even when it would change the answer --
        // if ALL tuples had been deleted earlier in the current transaction.
        // This should never be the case while updating the catalog.
        return m_tupleCount == 0;
    }

    virtual int64_t validatePartitioning(TheHashinator* hashinator, int32_t partitionId);

    void truncateTableUndo(TableCatalogDelegate* tcd, PersistentTable* originalTable);

    void truncateTableRelease(PersistentTable* originalTable);

    /** Once ELASTIC INDEX streaming starts, it needs to continue on the same
     * "generation" of a table -- even after truncations or swaps. */
    PersistentTable* tableForStreamIndexing() {
        if (m_tableForStreamIndexing) {
            return m_tableForStreamIndexing;
        }
        return this;
    }

    void setTableForStreamIndexing(PersistentTable* tb, PersistentTable* tbForStreamIndexing) {
        if (this == tb) {
            // For example, two identical swap statements in the same XA
            // should restore the status quo.
            // Likewise, the swapTable call to undo a SWAP TABLES statement.
            unsetTableForStreamIndexing();
        }
        m_tableForStreamIndexing = tbForStreamIndexing;
        m_tableForStreamIndexing->incrementRefcount();
    }

    void unsetTableForStreamIndexing() {
        if (m_tableForStreamIndexing) {
            m_tableForStreamIndexing->decrementRefcount();
            m_tableForStreamIndexing = NULL;
        }
    }

    std::pair<TableIndex const*, uint32_t> getUniqueIndexForDR();

    MaterializedViewHandler* materializedViewHandler() const { return m_mvHandler; }
    MaterializedViewTriggerForInsert* materializedViewTrigger() const { return m_mvTrigger; }
    void setMaterializedViewTrigger(MaterializedViewTriggerForInsert* trigger) { m_mvTrigger = trigger; }

    PersistentTable* deltaTable() const { return m_deltaTable; }

    bool isDeltaTableActive() { return m_deltaTableActive; }

    // STATS
    TableStats* getTableStats() { return &m_stats; };

    std::vector<uint64_t> getBlockAddresses() const;

    bool doDRActions(AbstractDRTupleStream* drStream);

    // Create a delta table attached to this persistent table using exactly the same table schema.
    void instantiateDeltaTable(bool needToCheckMemoryContext = true);
    void releaseDeltaTable(bool needToCheckMemoryContext = true);

    /**
     * Loads tuple data from the serialized table.
     * Used for snapshot restore and bulkLoad
     */
    void loadTuplesForLoadTable(SerializeInputBE& serialInput,
                                Pool* stringPool,
                                ReferenceSerializeOutput* uniqueViolationOutput,
                                const LoadTableCaller &caller);

    inline TableType getTableType() const {
        return m_tableType;
    }

    /**
     * IW-ENG14804
     * Set a companion streamed table to export tuples
     */
    void setStreamedTable(StreamedTable* st) {
        m_shadowStream = st;
    }

    /**
     * Get the shadow streamed table or nullptr
     */
    StreamedTable* getStreamedTable() {
        return m_shadowStream;
    }

    void migratingAdd(int64_t txnId, TableTuple& tuple);
    bool migratingRemove(int64_t txnId, TableTuple& tuple);
    uint16_t getMigrateColumnIndex();
    /**
     * Delete the rows that have completed the migration process
     */
    bool deleteMigratedRows(int64_t deletableTxnId);

    bool hasViewCountColumn() const { return m_schema->hasHiddenColumn(HiddenColumn::VIEW_COUNT); }
    int getViewCountColumnIndex() const { return m_schema->getHiddenColumnIndex(HiddenColumn::VIEW_COUNT); }

private:
    // Zero allocation size uses defaults.
    PersistentTable(int partitionColumn, char const* signature, bool isMaterialized,
            int tableAllocationTargetSize = 0,
            bool drEnabled = false,
            bool isReplicated = false,
            TableType tableType = PERSISTENT);

    /**
     * Prepare table for streaming from serialized data (internal for tests).
     * Use custom TableStreamer provided.
     * Return true on success or false if it was already active.
     */
    bool activateWithCustomStreamer(TableStreamType streamType,
                                    HiddenColumnFilter::Type hiddenColumnFilterType,
                                    boost::shared_ptr<TableStreamerInterface> tableStreamer,
                                    CatalogId tableId,
                                    std::vector<std::string>& predicateStrings,
                                    bool skipInternalActivation);

    size_t getSnapshotPendingBlockCount() const {
        return m_blocksPendingSnapshot.size();
    }

    size_t getSnapshotPendingLoadBlockCount() const {
        size_t blockCnt = 0;
        for (int ii = 0; ii < TUPLE_BLOCK_NUM_BUCKETS; ii++) {
            blockCnt += m_blocksPendingSnapshotLoad[ii]->size();
        }
        return blockCnt;
    }

    bool blockCountConsistent() const {
        // if the table is empty, the empty cache block will not be present in m_blocksNotPendingSnapshot
        return isPersistentTableEmpty() || m_blocksNotPendingSnapshot.size() == m_data.size();
    }

    void snapshotFinishedScanningBlock(TBPtr finishedBlock, TBPtr nextBlock) {
        if (nextBlock != NULL) {
            vassert(m_blocksPendingSnapshot.find(nextBlock) != m_blocksPendingSnapshot.end());
            m_blocksPendingSnapshot.erase(nextBlock);
            nextBlock->swapToBucket(TBBucketPtr());
        }
        if (finishedBlock != NULL && !finishedBlock->isEmpty()) {
            m_blocksNotPendingSnapshot.insert(finishedBlock);
            int bucketIndex = finishedBlock->calculateBucketIndex();
            if (bucketIndex != NO_NEW_BUCKET_INDEX) {
                finishedBlock->swapToBucket(m_blocksNotPendingSnapshotLoad[bucketIndex]);
            }
        }
    }

    void nextFreeTuple(TableTuple* tuple);

    bool doCompactionWithinSubset(TBBucketPtrVector* bucketVector);

    bool doForcedCompaction();  // Returns true if a compaction was performed

    void insertIntoAllIndexes(TableTuple* tuple);

    void deleteFromAllIndexes(TableTuple* tuple);

    void tryInsertOnAllIndexes(TableTuple* tuple, TableTuple* conflict);

    void checkUpdateOnExpressions(TableTuple const& sourceTupleWithNewValues,
          std::vector<TableIndex*> const& indexesToUpdate);

    bool checkUpdateOnUniqueIndexes(TableTuple& targetTupleToUpdate,
                                    TableTuple const& sourceTupleWithNewValues,
                                    std::vector<TableIndex*> const& indexesToUpdate);

    // Add truncate operation to dr log stream if dr is enabled and running
    void drLogTruncate(const ExecutorContext* ec);

    void notifyBlockWasCompactedAway(TBPtr block);

    // Call-back from TupleBlock::merge() for each tuple moved.
    virtual void notifyTupleMovement(TBPtr sourceBlock, TBPtr targetBlock,
                                     TableTuple& sourceTuple, TableTuple& targetTuple);

    void swapTuples(TableTuple& sourceTupleWithNewValues, TableTuple& destinationTuple);

    // The source tuple is used to create the ConstraintFailureException if one
    // occurs. In case of exception, target tuple should be released, but the
    // source tuple's memory should still be retained until the exception is
    // handled.
    void insertTupleCommon(TableTuple& source, TableTuple& target, bool fallible, bool shouldDRStream = true, bool delayTupleDelete= false);

    void doInsertTupleCommon(TableTuple& source, TableTuple& target, bool fallible, bool shouldDRStream = true, bool delayTupleDelete = false);

    void insertTupleForUndo(char* tuple);

    void updateTupleForUndo(char* targetTupleToUpdate,
                            char* sourceTupleWithNewValues,
                            bool revertIndexes,
                            bool fromMigrate);

    void deleteTupleForUndo(char* tupleData, bool skipLookup = false);

    void deleteTupleRelease(char* tuple);

    void deleteTupleFinalize(TableTuple& tuple);

    /**
     * Normally this will return the tuple storage to the free list.
     * In the memcheck build it will return the storage to the heap.
     */
    void deleteTupleStorage(TableTuple& tuple, TBPtr block = TBPtr(NULL), bool deleteLastEmptyBlock = false);

    /**
     * Implemented by persistent table and called by Table::loadTuplesFrom
     * for loadNextDependency
     */
    virtual void processLoadedTuple(TableTuple& tuple,
                                    ReferenceSerializeOutput* uniqueViolationOutput,
                                    int32_t& serializedTupleCount,
                                    size_t& tupleCountPosition,
                                    bool shouldDRStreamRows = false);

    enum LookupType {
        LOOKUP_BY_VALUES,
        LOOKUP_FOR_DR,
        LOOKUP_FOR_UNDO
    };

    TableTuple lookupTuple(TableTuple tuple, LookupType lookupType);

    TBPtr allocateFirstBlock();

    TBPtr allocateNextBlock();

    AbstractDRTupleStream* getDRTupleStream(const ExecutorContext* ec) {
        if (isReplicatedTable()) {
            return ec->drReplicatedStream();
        }
        return ec->drStream();
    }

    void setDRTimestampForTuple(TableTuple& tuple, bool update);
    void computeSmallestUniqueIndex();

    void addViewHandler(MaterializedViewHandler* viewHandler);

    void dropViewHandler(MaterializedViewHandler* viewHandler);

    // Mark all the view handlers referencing this table as dirty so they will be
    // recreated when being visited.
    // We use this only when a table index is added / dropped.
    void polluteViews();

    // Insert the source tuple into this table's delta table.
    // If there is no delta table affiliated with this table, then take no action.
    void insertTupleIntoDeltaTable(TableTuple& source, bool fallible);

    //
    // SWAP TABLE helpers
    //

    /**
     * Do the actual SWAP TABLES work on the tables before calling specific
     * methods to handle the implications on indexes on the tables.
     */
    void swapTableState(PersistentTable* otherTable);

    /**
     * Process corresponding identically defined indexes on two tables being swapped.
     * The vector arguments contain parallel elements.
     */
    void swapTableIndexes(PersistentTable* otherTable,
                          std::vector<TableIndex*> const& theIndexes,
                          std::vector<TableIndex*> const& otherIndexes);

    // pointers to chunks of data. Specific to table impl. Don't leak this type.
    TBMap m_data;

    // default iterator
    TableIterator m_iter;

    // CONSTRAINTS
    //Is this a materialized view?
    const bool m_isMaterialized;

    std::vector<bool> m_allowNulls;

    // STATS
    PersistentTableStats m_stats;

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
    stx::btree_set<TBPtr > m_blocksWithSpace;

    // Provides access to all table streaming apparati, including COW and recovery.
    boost::shared_ptr<TableStreamerInterface> m_tableStreamer;

    int m_failedCompactionCount;

    // This is a testability feature not intended for use in product logic.
    int m_invisibleTuplesPendingDeleteCount;

    // Surgeon passed to classes requiring "deep" access to avoid excessive friendship.
    PersistentTableSurgeon m_surgeon;

    // The original table subject to ELASTIC INDEX streaming prior to any swaps
    // or truncates in the current transaction.
    PersistentTable*  m_tableForStreamIndexing;

    // is DR enabled
    bool m_drEnabled;

    // SHA-1 of signature string
    char m_signature[20];

    bool m_noAvailableUniqueIndex;

    TableIndex* m_smallestUniqueIndex;

    uint32_t m_smallestUniqueIndexCrc;

    // indexes
    std::vector<TableIndex*> m_indexes;

    std::vector<TableIndex*> m_uniqueIndexes;

    TableIndex* m_pkeyIndex;

    // If this is a view table, maintain a handler to handle the view update work.
    MaterializedViewHandler* m_mvHandler;
    MaterializedViewTriggerForInsert* m_mvTrigger;

    // If this is a source table of a view, notify all the relevant view handlers
    // when an update is needed.
    std::vector<MaterializedViewHandler*> m_viewHandlers;

    // The delta table is only created when a view defined on a join query is
    // referencing this table as one of its source tables.
    // The delta table has an identical definition of this table, including the
    // indices. When m_deltaTableActive = true, the TableCatalogDelegate for
    // this table will return the delta table instead of the original table.
    // WARNING: Do not manually flip this m_deltaTableActive flag. Instead,
    // use ScopedDeltaTableContext (currently defined in MaterializedViewHandler.h).
    PersistentTable* m_deltaTable;

    bool m_deltaTableActive;

    // Objects used to coordinate compaction of Replicated tables
    SynchronizedUndoQuantumReleaseInterest m_releaseReplicated;
    SynchronizedDummyUndoQuantumReleaseInterest m_releaseDummyReplicated;

    // Pointer to Shadow streamed table (For Migrate) or nullptr
    TableType m_tableType;
    StreamedTable* m_shadowStream;
    typedef std::set<void*> MigratingBatch;
    typedef std::map<int64_t, MigratingBatch> MigratingRows;
    MigratingRows m_migratingRows;
};

inline PersistentTableSurgeon::PersistentTableSurgeon(PersistentTable& table) :
    m_table(table),
    m_indexingComplete(false)
{ }

inline TBMap& PersistentTableSurgeon::getData() const {
    return m_table.m_data;
}

inline PersistentTable& PersistentTableSurgeon::getTable() {
    return m_table;
}

inline void PersistentTableSurgeon::insertTupleForUndo(char* tuple) {
    m_table.insertTupleForUndo(tuple);
}

inline void PersistentTableSurgeon::updateTupleForUndo(char* targetTupleToUpdate,
                                                       char* sourceTupleWithNewValues,
                                                       bool revertIndexes,
                                                       bool fromMigrate) {
    m_table.updateTupleForUndo(targetTupleToUpdate, sourceTupleWithNewValues, revertIndexes, fromMigrate);
}

inline void PersistentTableSurgeon::deleteTuple(TableTuple& tuple, bool fallible,  bool removeMigratingIndex) {
    m_table.deleteTuple(tuple, fallible, removeMigratingIndex);
}

inline void PersistentTableSurgeon::deleteTupleForUndo(char* tupleData, bool skipLookup) {
    m_table.deleteTupleForUndo(tupleData, skipLookup);
}

inline void PersistentTableSurgeon::deleteTupleRelease(char* tuple) {
    m_table.deleteTupleRelease(tuple);
}

inline void PersistentTableSurgeon::deleteTupleStorage(TableTuple& tuple, TBPtr block) {
    m_table.deleteTupleStorage(tuple, block);
}

inline size_t PersistentTableSurgeon::getSnapshotPendingBlockCount() const {
    return m_table.getSnapshotPendingBlockCount();
}

inline size_t PersistentTableSurgeon::getSnapshotPendingLoadBlockCount() const {
    return m_table.getSnapshotPendingLoadBlockCount();
}

inline bool PersistentTableSurgeon::blockCountConsistent() const {
    return m_table.blockCountConsistent();
}

inline void PersistentTableSurgeon::snapshotFinishedScanningBlock(TBPtr finishedBlock, TBPtr nextBlock) {
    m_table.snapshotFinishedScanningBlock(finishedBlock, nextBlock);
}

inline bool PersistentTableSurgeon::hasIndex() const {
    return (m_index != NULL);
}

inline bool PersistentTableSurgeon::isIndexEmpty() const {
    vassert(m_index != NULL);
    return (m_index->size() == (size_t)0);
}

inline size_t PersistentTableSurgeon::indexSize() const {
    vassert(m_index != NULL);
    return m_index->size();
}

inline bool PersistentTableSurgeon::isIndexingComplete() const {
    vassert(m_index != NULL);
    return m_indexingComplete;
}

inline void PersistentTableSurgeon::setIndexingComplete() {
    vassert(m_index != NULL);
    m_indexingComplete = true;
}

inline void PersistentTableSurgeon::createIndex() {
    vassert(m_index == NULL);
    m_index.reset(new ElasticIndex());
    m_indexingComplete = false;
}

inline void PersistentTableSurgeon::dropIndex() {
    vassert(m_indexingComplete == true);
    m_index.reset(NULL);
    m_indexingComplete = false;
}

inline void PersistentTableSurgeon::clearIndex() {
    vassert(m_index != NULL);
    m_index->clear();
    m_indexingComplete = false;
}

inline void PersistentTableSurgeon::printIndex(std::ostream& os, int32_t limit) const {
    vassert(m_index != NULL);
    m_index->printKeys(os,limit,m_table.m_schema,m_table);
}

inline ElasticHash PersistentTableSurgeon::generateTupleHash(TableTuple& tuple) const {
    return tuple.getNValue(m_table.partitionColumn()).murmurHash3();
}

inline bool PersistentTableSurgeon::indexHas(TableTuple& tuple) const {
    vassert(m_index != NULL);
    return m_index->has(m_table, tuple);
}

inline bool PersistentTableSurgeon::indexAdd(TableTuple& tuple) {
    vassert(m_index != NULL);
    return m_index->add(m_table, tuple);
}

inline bool PersistentTableSurgeon::indexRemove(TableTuple& tuple) {
    vassert(m_index != NULL);
    return m_index->remove(m_table, tuple);
}

inline ElasticIndex::iterator PersistentTableSurgeon::indexIterator() {
    vassert(m_index != NULL);
    return m_index->createIterator();
}

inline ElasticIndex::iterator PersistentTableSurgeon::indexIteratorLowerBound(int32_t lowerBound) {
    vassert(m_index != NULL);
    return m_index->createLowerBoundIterator(lowerBound);
}

inline ElasticIndex::iterator PersistentTableSurgeon::indexIteratorUpperBound(int32_t upperBound) {
    vassert(m_index != NULL);
    return m_index->createUpperBoundIterator(upperBound);
}

inline ElasticIndex::const_iterator PersistentTableSurgeon::indexIterator() const {
    vassert(m_index != NULL);
    return m_index->createIterator();
}

inline ElasticIndex::const_iterator PersistentTableSurgeon::indexIteratorLowerBound(int32_t lowerBound) const {
    vassert(m_index != NULL);
    return m_index->createLowerBoundIterator(lowerBound);
}

inline ElasticIndex::const_iterator PersistentTableSurgeon::indexIteratorUpperBound(int32_t upperBound) const {
    vassert(m_index != NULL);
    return m_index->createUpperBoundIterator(upperBound);
}

inline ElasticIndex::iterator PersistentTableSurgeon::indexEnd() {
    vassert(m_index != NULL);
    return m_index->end();
}

inline ElasticIndex::const_iterator PersistentTableSurgeon::indexEnd() const {
    vassert(m_index != NULL);
    return m_index->end();
}

inline uint32_t PersistentTableSurgeon::getTupleCount() const {
    return m_table.m_tupleCount;
}

inline void PersistentTableSurgeon::initTableStreamer(TableStreamerInterface* streamer) {
    vassert(m_table.m_tableStreamer == NULL);
    m_table.m_tableStreamer.reset(streamer);
}

inline bool PersistentTableSurgeon::hasStreamType(TableStreamType streamType) const {
    vassert(m_table.m_tableStreamer != NULL);
    return m_table.m_tableStreamer->hasStreamType(streamType);
}

inline boost::shared_ptr<ElasticIndexTupleRangeIterator>
PersistentTableSurgeon::getIndexTupleRangeIterator(ElasticIndexHashRange const& range) {
    vassert(m_index != NULL);
    vassert(m_table.m_schema != NULL);
    return boost::shared_ptr<ElasticIndexTupleRangeIterator>(
            new ElasticIndexTupleRangeIterator(*m_index, *m_table.m_schema, range));
}

inline void PersistentTable::deleteTupleStorage(TableTuple& tuple, TBPtr block, bool deleteLastEmptyBlock) {
    // May not delete an already deleted tuple.
    vassert(tuple.isActive());

    // The tempTuple is forever!
    vassert(&tuple != &m_tempTuple);

    // This frees referenced strings -- when could possibly be a better time?
    if (m_schema->getUninlinedObjectColumnCount() != 0) {
        decreaseStringMemCount(tuple.getNonInlinedMemorySizeForPersistentTable());
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

    bool transitioningToBlockWithSpace = ! block->hasFreeTuples();

    int retval = block->freeTuple(tuple.address());
    if (retval != NO_NEW_BUCKET_INDEX) {
        //Check if if the block is currently pending snapshot
        if (m_blocksNotPendingSnapshot.find(block) != m_blocksNotPendingSnapshot.end()) {
            // std::cout << "Swapping NPS block " << static_cast<void*>(block.get()) << " to bucket " << retval << std::endl;
            block->swapToBucket(m_blocksNotPendingSnapshotLoad[retval]);
        //Check if the block goes into the pending snapshot set of buckets
        } else if (m_blocksPendingSnapshot.find(block) != m_blocksPendingSnapshot.end()) {
            //std::cout << "Swapping PS block " << static_cast<void*>(block.get()) << " to bucket " << retval << std::endl;
            block->swapToBucket(m_blocksPendingSnapshotLoad[retval]);
        } else {
            //In this case the block is actively being snapshotted and isn't eligible for merge operations at all
            //do nothing, once the block is finished by the iterator, the iterator will return it
            //std::cout << "NOT Swapping block " << static_cast<void*>(block.get()) << " to bucket " << retval << std::endl;
        }
    }

    if (block->isEmpty()) {
        if (m_data.size() > 1 || deleteLastEmptyBlock) {
            // Release the empty block unless it's the only remaining block and caller has requested not to do so.
            // The intent of doing so is to avoid block allocation cost at time tuple insertion into the table
            m_data.erase(block->address());
            m_blocksWithSpace.erase(block);
        } else if (transitioningToBlockWithSpace) {
           // In the unlikely event that tuplesPerBlock == 1
           m_blocksWithSpace.insert(block);
        }
        m_blocksNotPendingSnapshot.erase(block);
        if (m_blocksPendingSnapshot.find(block) != m_blocksPendingSnapshot.end()) {
            // A block was emptied while pending snapshot
            vassert(m_tableStreamer != NULL && m_tableStreamer->hasStreamType(TABLE_STREAM_SNAPSHOT));
            m_blocksPendingSnapshot.erase(block);
            m_tableStreamer->notifyBlockWasCompactedAway(block);
        }
        //Eliminates circular reference
        block->swapToBucket(TBBucketPtr());
    } else if (transitioningToBlockWithSpace) {
        m_blocksWithSpace.insert(block);
    }
}

inline TBPtr PersistentTable::findBlock(char* tuple, TBMap& blocks, int blockSize) {
    if (!blocks.empty()) {
        TBMapI i = blocks.lower_bound(tuple);

        // Not the first tuple of any known block, move back a block, see if it
        // belongs to the previous block
        if (i == blocks.end() || i.key() != tuple) {
            i--;
        }

        // If the tuple is within the block boundaries, we found the block
        if (i.key() <= tuple && tuple < i.key() + blockSize) {
            if (i.data().get() == NULL) {
                throwFatalException("A block has gone missing in the tuple block map.");
            }
            return i.data();
        }
    }

    return TBPtr(NULL);
}

inline TBPtr PersistentTable::allocateFirstBlock() {
    TBPtr block(new TupleBlock(this, TBBucketPtr()));
    m_data.insert(block->address(), block);
    return block;
}

inline TBPtr PersistentTable::allocateNextBlock() {
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

