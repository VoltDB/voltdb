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

#include "persistenttable.h"

#include "ConstraintFailureException.h"
#include "CopyOnWriteContext.h"
#include "DRTupleStreamUndoAction.h"
#include "MaterializedViewHandler.h"
#include "MaterializedViewTriggerForWrite.h"
#include "PersistentTableUndoInsertAction.h"
#include "PersistentTableUndoDeleteAction.h"
#include "PersistentTableUndoTruncateTableAction.h"
#include "PersistentTableUndoSwapTableAction.h"
#include "PersistentTableUndoUpdateAction.h"
#include "TableCatalogDelegate.hpp"
#include "tablefactory.h"
#include "TupleStreamException.h"

#include "common/ExecuteWithMpMemory.h"
#include "common/FailureInjection.h"
#include "crc/crc32c.h"
#include "indexes/tableindex.h"
#include "indexes/tableindexfactory.h"
#include "common/ValuePeeker.hpp"

#include <boost/date_time/posix_time/posix_time.hpp>

namespace voltdb {

#define TABLE_BLOCKSIZE 2097152

   template<typename T> inline static T* partialCopyToPool(Pool* pool, const T* src, size_t partialSize) {
      return reinterpret_cast<T*>(memcpy(pool->allocate(partialSize), src, partialSize));
   }

class SetAndRestorePendingDeleteFlag
{
public:
    SetAndRestorePendingDeleteFlag(TableTuple& target) : m_target(target)
    {
        vassert(!m_target.isPendingDelete());
        m_target.setPendingDeleteTrue();
    }

    ~SetAndRestorePendingDeleteFlag() {
        m_target.setPendingDeleteFalse();
    }

private:
    TableTuple& m_target;
};

PersistentTable::PersistentTable(int partitionColumn,
                                 char const* signature,
                                 bool isMaterialized,
                                 int tableAllocationTargetSize,
                                 bool drEnabled,
                                 bool isReplicated,
                                 TableType tableType)
    : ViewableAndReplicableTable(tableAllocationTargetSize == 0 ? TABLE_BLOCKSIZE : tableAllocationTargetSize, partitionColumn, isReplicated)
    , m_data()
    , m_iter(this, m_data.begin())
    , m_isMaterialized(isMaterialized)   // Other constructors are dependent on this one
    , m_allowNulls()
    , m_stats(this)
    , m_blocksNotPendingSnapshotLoad()
    , m_blocksPendingSnapshotLoad()
    , m_blocksNotPendingSnapshot()
    , m_blocksPendingSnapshot()
    , m_blocksWithSpace()
    , m_tableStreamer()
    , m_failedCompactionCount(0)
    , m_invisibleTuplesPendingDeleteCount(0)
    , m_surgeon(*this)
    , m_tableForStreamIndexing(NULL)
    , m_drEnabled(drEnabled && !isMaterialized)
    , m_noAvailableUniqueIndex(false)
    , m_smallestUniqueIndex(NULL)
    , m_smallestUniqueIndexCrc(0)
    , m_pkeyIndex(NULL)
    , m_mvHandler(NULL)
    , m_mvTrigger(NULL)
    , m_viewHandlers()
    , m_deltaTable(NULL)
    , m_deltaTableActive(false)
    , m_releaseReplicated(this)
    , m_tableType(tableType)
    , m_shadowStream(nullptr)
{
    if (!m_isMaterialized && m_isReplicated != (m_partitionColumn == -1)) {
        VOLT_ERROR("CAUTION: detected inconsistent isReplicate flag. Table name: %s, m_isMaterialized: %s, m_partitionColumn: %d, m_isReplicated: %s\n",
                m_name.c_str(), m_isMaterialized ? "true" : "false", m_partitionColumn, m_isReplicated ? "true" : "false");
    }

    for (int ii = 0; ii < TUPLE_BLOCK_NUM_BUCKETS; ii++) {
        m_blocksNotPendingSnapshotLoad.push_back(TBBucketPtr(new TBBucket()));
        m_blocksPendingSnapshotLoad.push_back(TBBucketPtr(new TBBucket()));
    }

    if (signature) {
        ::memcpy(&m_signature, signature, 20);
    } else {
        ::memset(&m_signature, 0, sizeof(m_signature));
    }
}

void PersistentTable::initializeWithColumns(TupleSchema* schema,
                                            std::vector<std::string> const& columnNames,
                                            bool ownsTupleSchema,
                                            int32_t compactionThreshold) {
    vassert(schema != NULL);

    Table::initializeWithColumns(schema, columnNames, ownsTupleSchema, compactionThreshold);

    m_allowNulls.resize(m_columnCount);
    for (int i = m_columnCount - 1; i >= 0; --i) {
        TupleSchema::ColumnInfo const* columnInfo = m_schema->getColumnInfo(i);
        m_allowNulls[i] = columnInfo->allowNull;
    }

    // Also clear some used block state. this structure doesn't have
    // an block ownership semantics - it's just a cache. I think.
    m_blocksWithSpace.clear();

    // note that any allocated memory in m_data is left alone
    // as is m_allocatedTuples
    m_data.clear();
}

PersistentTable::~PersistentTable() {
    VOLT_DEBUG("Deleting TABLE %s as %s", m_name.c_str(), m_isReplicated?"REPLICATED":"PARTITIONED");
    for (int ii = 0; ii < TUPLE_BLOCK_NUM_BUCKETS; ii++) {
        m_blocksNotPendingSnapshotLoad[ii]->clear();
        m_blocksPendingSnapshotLoad[ii]->clear();
    }

    // delete all tuples to free strings
    TableIterator ti(this, m_data.begin());
    TableTuple tuple(m_schema);
#ifdef VOLT_POOL_CHECKING
    auto engine = ExecutorContext::getEngine();
    bool shutdown = engine == nullptr ? true : engine->isDestroying();
    if (!shutdown) {
#endif
        while (ti.next(tuple)) {
            tuple.freeObjectColumns();
            tuple.setActiveFalse();
        }
#ifdef VOLT_POOL_CHECKING
    }
#endif

    // clean up indexes
    BOOST_FOREACH (auto index, m_indexes) {
#ifdef VOLT_POOL_CHECKING
        index->shutdown(shutdown);
#endif
        delete index;
    }

    // free up the materialized view handler if this is a view table.
    delete m_mvHandler;
    // remove this table from the source table list of the views.
    {
        // if we are currently in Replicated table memory, break out because we are
        // updating other (possibly partitioned) tables
        ConditionalExecuteOutsideMpMemory getOutOfMpMemory(m_isReplicated && !m_viewHandlers.empty());
        BOOST_FOREACH (auto viewHandler, m_viewHandlers) {
            viewHandler->dropSourceTable(this);
        }
    }
    if (m_deltaTable) {
        m_deltaTable->decrementRefcount();
    }
    if (m_shadowStream != nullptr) {
        delete m_shadowStream;
    }
}

// ------------------------------------------------------------------
// OPERATIONS
// ------------------------------------------------------------------
void PersistentTable::nextFreeTuple(TableTuple* tuple) {
    // First check whether we have any in our list
    // In the memcheck it uses the heap instead of a free list to help Valgrind.
    if (!m_blocksWithSpace.empty()) {
        VOLT_TRACE("GRABBED FREE TUPLE!\n");
        stx::btree_set<TBPtr >::iterator begin = m_blocksWithSpace.begin();
        TBPtr block = (*begin);
        if (m_tupleCount == 0) {
            vassert(m_blocksNotPendingSnapshot.find(block) == m_blocksNotPendingSnapshot.end());
            m_blocksNotPendingSnapshot.insert(block);
        }
        std::pair<char*, int> retval = block->nextFreeTuple();

        std::vector<TBPtr> fullBlocks;
        while (retval.first == NULL) {
            if (!block->hasFreeTuples())
                fullBlocks.emplace_back(block);
            if (++begin == m_blocksWithSpace.end())
                break;
            block = (*begin);
            retval = block->nextFreeTuple();
        }
        for (auto& bk : fullBlocks) {
            m_blocksWithSpace.erase(bk);
        }

        // We have a block which has space we want to use.
        if (retval.first != NULL) {

        /**
         * Check to see if the block needs to move to a new bucket
         */
            if (retval.second != NO_NEW_BUCKET_INDEX) {
                //Check if if the block is currently pending snapshot
                if (m_blocksNotPendingSnapshot.find(block) != m_blocksNotPendingSnapshot.end()) {
                    block->swapToBucket(m_blocksNotPendingSnapshotLoad[retval.second]);
                //Check if the block goes into the pending snapshot set of buckets
                } else if (m_blocksPendingSnapshot.find(block) != m_blocksPendingSnapshot.end()) {
                    block->swapToBucket(m_blocksPendingSnapshotLoad[retval.second]);
                } else {
                    //In this case the block is actively being snapshotted and isn't eligible for merge operations at all
                    //do nothing, once the block is finished by the iterator, the iterator will return it
                }
            }

            tuple->moveAndInitialize(retval.first);
            ++m_tupleCount;
            if (!block->hasFreeTuples()) {
                m_blocksWithSpace.erase(block);
            }
            vassert(m_columnCount == tuple->columnCount());
            return;
        }
    }

    // if there are no tuples free, we need to grab another chunk of memory
    // Allocate a new set of tuples
    TBPtr block = allocateNextBlock();

    // get free tuple
    vassert(m_columnCount == tuple->columnCount());

    std::pair<char*, int> retval = block->nextFreeTuple();

    /**
     * Check to see if the block needs to move to a new bucket
     */
    if (retval.second != NO_NEW_BUCKET_INDEX) {
        //Check if the block goes into the pending snapshot set of buckets
        if (m_blocksPendingSnapshot.find(block) != m_blocksPendingSnapshot.end()) {
            //std::cout << "Swapping block to nonsnapshot bucket " << static_cast<void*>(block.get()) << " to bucket " << retval.second << std::endl;
            block->swapToBucket(m_blocksPendingSnapshotLoad[retval.second]);
        //Now check if it goes in with the others
        } else if (m_blocksNotPendingSnapshot.find(block) != m_blocksNotPendingSnapshot.end()) {
            //std::cout << "Swapping block to snapshot bucket " << static_cast<void*>(block.get()) << " to bucket " << retval.second << std::endl;
            block->swapToBucket(m_blocksNotPendingSnapshotLoad[retval.second]);
        } else {
            //In this case the block is actively being snapshotted and isn't eligible for merge operations at all
            //do nothing, once the block is finished by the iterator, the iterator will return it
        }
    }

    tuple->moveAndInitialize(retval.first);
    ++m_tupleCount;
    if (block->hasFreeTuples()) {
        m_blocksWithSpace.insert(block);
    }
}

void PersistentTable::drLogTruncate(const ExecutorContext* ec) {
    AbstractDRTupleStream* drStream = getDRTupleStream(ec);
    if (doDRActions(drStream)) {
        int64_t currentSpHandle = ec->currentSpHandle();
        int64_t currentUniqueId = ec->currentUniqueId();
        size_t drMark = drStream->truncateTable(m_signature, m_name, m_partitionColumn,
                currentSpHandle, currentUniqueId);

        UndoQuantum* uq = ec->getCurrentUndoQuantum();
        if (uq) {
            uq->registerUndoAction(
                    new (*uq) DRTupleStreamUndoAction(drStream, drMark, rowCostForDRRecord(DR_RECORD_TRUNCATE_TABLE)));
        }
    }
}

void PersistentTable::deleteAllTuples() {
    // Instead of recording each tuple deletion, log it as a table truncation DR.
    drLogTruncate(m_executorContext);

    // Temporarily disable DR binary logging so that it doesn't record the
    // individual deletions below.
    DRTupleStreamDisableGuard drGuard(m_executorContext, false);

    // nothing interesting
    TableIterator ti(this, m_data.begin());
    TableTuple tuple(m_schema);
    while (ti.next(tuple)) {
        deleteTuple(tuple);
    }
}

bool PersistentTable::doDRActions(AbstractDRTupleStream* drStream) {
    return m_drEnabled && drStream && drStream->drStreamStarted();
}

void PersistentTable::truncateTableUndo(TableCatalogDelegate* tcd,
        PersistentTable* originalTable) {
    VOLT_DEBUG("**** Truncate table undo *****\n");

    if (originalTable->m_tableStreamer != NULL) {
        // Elastic Index may complete when undo Truncate
        unsetTableForStreamIndexing();
    }

    VoltDBEngine* engine = ExecutorContext::getEngine();
    if (m_shadowStream != nullptr) {
        m_shadowStream->moveWrapperTo(originalTable->m_shadowStream);
        engine->setStreamTableByName(m_name, originalTable->m_shadowStream);
    }

    auto views = originalTable->views();
    // reset all view table pointers
    BOOST_FOREACH (auto originalView, views) {
        PersistentTable* destTable = originalView->destTable();
        TableCatalogDelegate* targetTcd = engine->getTableDelegate(destTable->name());
        // call decrement reference count on the newly constructed view table
        targetTcd->deleteCommand();
        // update the view table pointer with the original view
        targetTcd->setTable(destTable);
    }

    // reset base table pointer
    tcd->setTable(originalTable);

    engine->rebuildTableCollections(isReplicatedTable(), false);

    decrementRefcount();
}

// Decrement each view-based table's reference count.
template<class T> static inline void decrementViewReferences(T views) {
    BOOST_FOREACH (auto originalView, views) {
        PersistentTable* destTable = originalView->destTable();
        destTable->decrementRefcount();
    }
}

void PersistentTable::truncateTableRelease(PersistentTable* originalTable) {
    VOLT_DEBUG("**** Truncate table release *****\n");
    m_tuplesPinnedByUndo = 0;
    m_invisibleTuplesPendingDeleteCount = 0;

    if (originalTable->m_tableStreamer != NULL) {
        std::stringstream message;
        message << "Transfering table stream after truncation of table ";
        message << name() << " partition " << originalTable->m_tableStreamer->getPartitionID() << '\n';
        std::string str = message.str();
        LogManager::getThreadLogger(LOGGERID_HOST)->log(voltdb::LOGLEVEL_INFO, &str);

        originalTable->m_tableStreamer->cloneForTruncatedTable(m_surgeon);

        unsetTableForStreamIndexing();
    }

    // Single table view.
    decrementViewReferences(originalTable->m_views);

    // Joined table view.
    decrementViewReferences(originalTable->m_viewHandlers);

    originalTable->decrementRefcount();
}


template<class T> static inline PersistentTable* constructEmptyDestTable(
        VoltDBEngine* engine,
        PersistentTable* destTable,
        catalog::Table* catalogViewTable,
        T* viewHandler) {
    TableCatalogDelegate* destTcd = engine->getTableDelegate(destTable->name());
    destTcd->init(*engine->getDatabase(), *catalogViewTable, engine->getIsActiveActiveDREnabled());
    PersistentTable* destEmptyTable = destTcd->getPersistentTable();
    vassert(destEmptyTable);
    return destEmptyTable;
}

void PersistentTable::truncateTable(VoltDBEngine* engine) {
    if (isPersistentTableEmpty()) {
        // Always log the the truncate if dr is enabled, see ENG-14528.
        drLogTruncate(m_executorContext);
        return;
    }

    // For a materialized view don't optimize truncate,
    // this needs more work - ENG-10323.
    if (m_isMaterialized) {
        /* // enable to debug
        std::cout << "DEBUG: truncating view table (retail) "
                  << activeTupleCount()
                  << " tuples in " << name() << std::endl;
        // */
        deleteAllTuples();
        return;
    }

    // SHORT TERM NOTE: Remove this comment when it no longer applies.
    // For the source table of a joined materialized view don't optimize
    // truncate, this needs more work - ENG-11017.
    // This guard disables much of the code currently being changed
    // on Ethan's and Paul's branches as it relates to truncate
    // and its undo actions for this new case.
    // The guard allowed v6.6 to ship prior to the perfection of those changes.
    // In other words, there are known bugs in the code paths we are
    // disabling here, but we are not bothering to strip out the (dead)
    // buggy code paths from v6.6.  That will just cause merge conflicts
    // with the rework that is currently in progress but will land after
    // v6.6.
    if ( ! m_viewHandlers.empty()) {
        /* // enable to debug
        std::cout << "DEBUG: truncating source of join view table (retail) "
                  << activeTupleCount()
                  << " tuples in " << name() << std::endl;
        // */
        deleteAllTuples();
        return;
    }

    // If the table has only one tuple-storage block, it may be better to truncate
    // table by iteratively deleting table rows. Evaluate if this is the case
    // based on the block and tuple block load factor
    if (m_data.size() == 1) {
        // Determine a threshold cutoff in terms of block load factor beyond
        // which wholesale truncate is estimated to be preferable to
        // tuple-by-tuple retail delete. Cut-off values are based on worst
        // case scenarios with intent to improve performance and to avoid
        // performance regressions. Cut-off numbers were obtained from
        // benchmark tests of a few scenarios:
        // - varying table schema - effect of tables having more columns
        // - varying number of views on table
        // - tables with more varchar columns with size below and above 16
        // - tables with indexes

        // cut-off for table with no views
        double tableWithNoViewLFCutoffForTrunc = 0.105666;
        // cut-off for table with views
        double tableWithViewsLFCutoffForTrunc = 0.015416;

        bool noView = m_views.empty() && m_viewHandlers.empty();
        double cutoff = noView ? tableWithNoViewLFCutoffForTrunc
                               : tableWithViewsLFCutoffForTrunc;
        double blockLoadFactor = m_data.begin().data()->loadFactor();
        if (blockLoadFactor <= cutoff) {
            /* // enable to debug
            std::cout << "DEBUG: truncating (retail) "
                      << activeTupleCount()
                      << " tuples in " << name() << std::endl;
            // */
            deleteAllTuples();
            return;
        }
    }

    TableCatalogDelegate* tcd = engine->getTableDelegate(m_name);
    vassert(tcd);

    catalog::Table* catalogTable = engine->getCatalogTable(m_name);
    tcd->init(*engine->getDatabase(), *catalogTable, engine->getIsActiveActiveDREnabled());

    PersistentTable* emptyTable = tcd->getPersistentTable();
    vassert(emptyTable);
    vassert(emptyTable->views().size() == 0);
    if (m_tableStreamer &&
        m_tableStreamer->hasStreamType(TABLE_STREAM_ELASTIC_INDEX)) {
        // There is Elastic Index work going on and
        // it should continue to access the old table.
        // Add one reference count to keep the original table.
        emptyTable->setTableForStreamIndexing(this, this->tableForStreamIndexing());
    }

    // add matView
    BOOST_FOREACH (auto originalView, m_views) {
        PersistentTable* destTable = originalView->destTable();
        catalog::Table* catalogViewTable = engine->getCatalogTable(destTable->name());
        PersistentTable* destEmptyTable = constructEmptyDestTable(engine,
                destTable, catalogViewTable, originalView);

        MaterializedViewTriggerForWrite::build(emptyTable, destEmptyTable,
                originalView->getMaterializedViewInfo());
    }

    BOOST_FOREACH (auto viewHandler, m_viewHandlers) {
        PersistentTable* destTable = viewHandler->destTable();
        catalog::Table* catalogViewTable = engine->getCatalogTable(destTable->name());
        PersistentTable* destEmptyTable = constructEmptyDestTable(engine,
                destTable, catalogViewTable, viewHandler);

        auto mvHandlerInfo = catalogViewTable->mvHandlerInfo().get("mvHandlerInfo");
        auto newHandler = new MaterializedViewHandler(destEmptyTable,
                                                      mvHandlerInfo,
                                                      mvHandlerInfo->groupByColumnCount(),
                                                      engine);
        if (mvHandlerInfo->groupByColumnCount() == 0) {
            // Pre-load a table-wide summary view row.
            newHandler->catchUpWithExistingData(true);
        }
    }

    if (m_shadowStream != nullptr) {
        m_shadowStream->moveWrapperTo(emptyTable->m_shadowStream);
        engine->setStreamTableByName(m_name, emptyTable->m_shadowStream);
    }

    engine->rebuildTableCollections(isReplicatedTable(), false);

    drLogTruncate(m_executorContext);

    UndoQuantum* uq = m_executorContext->getCurrentUndoQuantum();
    if (uq) {
        emptyTable->m_tuplesPinnedByUndo = emptyTable->m_tupleCount;
        emptyTable->m_invisibleTuplesPendingDeleteCount = emptyTable->m_tupleCount;
        // Create and register an undo action.
        UndoReleaseAction* undoAction = new (*uq) PersistentTableUndoTruncateTableAction(tcd, this, emptyTable);
        SynchronizedThreadLock::addTruncateUndoAction(isReplicatedTable(), uq, undoAction, this);
    }
    else {
        throwFatalException("Attempted to truncate table %s when there was no "
                            "active undo quantum even though one was expected", m_name.c_str());
    }
}

/**
 *  This helper does name validation and resolution for parameters to the SWAP TABLE command.
 *  It provides pointers to the actual metadata components that need to be updated.
 **/
struct CompiledSwap {
    std::vector<TableIndex*> m_theIndexes;
    std::vector<TableIndex*> m_otherIndexes;

    CompiledSwap(PersistentTable& theTable, PersistentTable& otherTable,
            std::vector<std::string> const& theIndexNames,
            std::vector<std::string> const& otherIndexNames) {
        // assert symmetry of the input vectors.
        vassert(theIndexNames.size() == otherIndexNames.size());

        // Claim an initializer for each index defined directly
        // on the tables being swapped.
        size_t nUsedInitializers = theTable.indexCount();
        // assert symmetry of the table definitions.
        vassert(nUsedInitializers == otherTable.indexCount());
        // assert coverage of input vectors.
        vassert(nUsedInitializers == theIndexNames.size());

        for (size_t ii = 0; ii < nUsedInitializers; ++ii) {
            TableIndex* theIndex = theTable.index(theIndexNames[ii]);
            vassert(theIndex);
            TableIndex* otherIndex = otherTable.index(otherIndexNames[ii]);
            vassert(otherIndex);

            m_theIndexes.push_back(theIndex);
            m_otherIndexes.push_back(otherIndex);
        }
    }
};

#ifndef NDEBUG
static bool hasNameIntegrity(std::string const& tableName, std::vector<std::string> const& indexNames) {
    // Validate that future queries will be able to resolve the table
    // name and its associated index names.
    VoltDBEngine* engine = ExecutorContext::getEngine();
    auto tcd = engine->getTableDelegate(tableName);
    auto table = tcd->getPersistentTable();
    char errMsg[1024];
    if (tableName != table->name()) {
        snprintf(errMsg, sizeof(errMsg), "Integrity check failure: "
                 "catalog name %s resolved to table named %s.",
                 tableName.c_str(), table->name().c_str());
        errMsg[sizeof errMsg - 1] = '\0';
        LogManager::getThreadLogger(LOGGERID_SQL)->log(LOGLEVEL_ERROR, errMsg);
        return false;
    }
    BOOST_FOREACH (std::string const& iName, indexNames) {
        if (! table->index(iName)) {
            snprintf(errMsg, sizeof(errMsg), "Integrity check failure: "
                     "table named %s failed to resolve index name %s.",
                     tableName.c_str(), iName.c_str());
            errMsg[sizeof errMsg - 1] = '\0';
            LogManager::getThreadLogger(LOGGERID_SQL)->log(LOGLEVEL_ERROR, errMsg);
            return false;
        }
    }
    return true;
}
#endif

void PersistentTable::swapTable(PersistentTable* otherTable,
        std::vector<std::string> const& theIndexNames,
        std::vector<std::string> const& otherIndexNames,
        bool fallible, bool isUndo) {
    vassert(hasNameIntegrity(name(), theIndexNames));
    vassert(hasNameIntegrity(otherTable->name(), otherIndexNames));
    CompiledSwap compiled(*this, *otherTable,
            theIndexNames, otherIndexNames);
    swapTableState(otherTable);
    swapTableIndexes(otherTable,
            compiled.m_theIndexes,
            compiled.m_otherIndexes);
    vassert(m_drEnabled == otherTable->m_drEnabled);

    if (!isUndo && m_drEnabled) {
        ExecutorContext::getEngine()->swapDRActions(otherTable, this);
    }

    if (fallible) {
        vassert(!isUndo);
        UndoQuantum *uq = ExecutorContext::currentUndoQuantum();
        if (uq) {
            uq->registerUndoAction(
                    new (*uq) PersistentTableUndoSwapTableAction(this, otherTable,
                            theIndexNames,
                            otherIndexNames));
        }
    }

    // Switch arguments here to Account here for the actual table pointers
    // having been switched to use each other's table and index names.
    vassert(hasNameIntegrity(name(), otherIndexNames));
    vassert(hasNameIntegrity(otherTable->name(), theIndexNames));

    ExecutorContext::getEngine()->rebuildTableCollections(m_isReplicated, false);
}

void PersistentTable::swapTableState(PersistentTable* otherTable) {
    VoltDBEngine* engine = ExecutorContext::getEngine();
    auto tcd1 = engine->getTableDelegate(m_name);
    vassert(tcd1->getTable() == this);
    tcd1->setTable(otherTable);

    auto tcd2 = engine->getTableDelegate(otherTable->m_name);
    vassert(tcd2->getTable() == otherTable);
    tcd2->setTable(this);

    // Swap the table attributes that must continue to be associated with each
    // table's name/identity, not its swapped content.
    // We MIGHT want to consider making these attributes of TableCatalogDelegate
    // instead of PersistentTable?

    std::swap(m_name, otherTable->m_name);
    m_stats.updateTableName(m_name);
    otherTable->m_stats.updateTableName(otherTable->m_name);

    if (m_tableStreamer &&
            m_tableStreamer->hasStreamType(TABLE_STREAM_ELASTIC_INDEX)) {
        // There is Elastic Index work going on and
        // it should continue to access the old table.
        // Add one reference count to keep the original table.
        auto heldStreamIndexingTable = tableForStreamIndexing();
        auto heldOtherStreamIndexingTable = otherTable->tableForStreamIndexing();
        setTableForStreamIndexing(otherTable, heldOtherStreamIndexingTable);
        otherTable->setTableForStreamIndexing(this, heldStreamIndexingTable);
    }

    // NOTE: do not swap m_tableStreamers here... we want them to
    // stick to their original tables, so that if a swap occurs during
    // an ongoing snapshot, subsequent changes to the table notify the
    // right TableStreamer instance.
}

void PersistentTable::swapTableIndexes(PersistentTable* otherTable,
        std::vector<TableIndex*> const& theIndexes,
        std::vector<TableIndex*> const& otherIndexes) {
    size_t nSwaps = theIndexes.size();
    vassert(nSwaps == otherIndexes.size());

    // FIXME: FOR NOW, every index on the two tables must be swappable
    // because swapping never repopulates them.
    vassert(nSwaps == otherTable->indexCount());
    vassert(nSwaps == indexCount());

    for (int ii = 0; ii < nSwaps; ++ii) {
        TableIndex* theIndex = theIndexes[ii];
        TableIndex* otherIndex = otherIndexes[ii];

        auto heldName = theIndex->getName();
        theIndex->rename(otherIndex->getName());
        // The table names are already swapped before we swap the indexes.
        theIndex->getIndexStats()->updateTableName(m_name);
        otherIndex->rename(heldName);
        otherIndex->getIndexStats()->updateTableName(otherTable->m_name);
    }
}

void PersistentTable::setDRTimestampForTuple(TableTuple& tuple, bool update) {
    vassert(hasDRTimestampColumn());
    if (update || tuple.getHiddenNValue(getDRTimestampColumnIndex()).isNull()) {
        tuple.setHiddenNValue(getDRTimestampColumnIndex(), HiddenColumn::getDefaultValue(HiddenColumn::XDCR_TIMESTAMP));
    }
}

void PersistentTable::insertTupleIntoDeltaTable(TableTuple& source, bool fallible) {
    // If the current table does not have a delta table, return.
    // If the current table has a delta table, but it is used by
    // a single table view during snapshot restore process, return.
    if (! m_deltaTable || m_mvTrigger) {
        return;
    }

    // If the delta table has data in it, delete the data first.
    if (! m_deltaTable->isPersistentTableEmpty()) {
        TableIterator ti(m_deltaTable, m_deltaTable->m_data.begin());
        TableTuple tuple(m_deltaTable->m_schema);
        ti.next(tuple);
        m_deltaTable->deleteTuple(tuple, fallible);
    }

    TableTuple targetForDelta(m_deltaTable->m_schema);
    m_deltaTable->nextFreeTuple(&targetForDelta);
    targetForDelta.copyForPersistentInsert(source);

    try {
        m_deltaTable->insertTupleCommon(source, targetForDelta, fallible);
    } catch (ConstraintFailureException const& e) {
        m_deltaTable->deleteTupleStorage(targetForDelta);
        throw;
    } catch (TupleStreamException const& e) {
        m_deltaTable->deleteTupleStorage(targetForDelta);
        throw;
    }
    // TODO: we do not catch other types of exceptions, such as
    // SQLException, etc. The assumption we held that no other
    // exceptions should be thrown in the try-block is pretty
    // daring and likely not correct.
}

/*
 * Regular tuple insertion that does an allocation and copy for
 * uninlined strings and creates and registers an UndoAction.
 */
bool PersistentTable::insertTuple(TableTuple& source) {
    insertPersistentTuple(source, true);
    return true;
}

TableTuple PersistentTable::insertPersistentTuple(TableTuple& source, bool fallible) {
    //
    // First get the next free tuple
    // This will either give us one from the free slot list, or
    // grab a tuple at the end of our chunk of memory
    //
    TableTuple target(m_schema);
    nextFreeTuple(&target);
    //
    // Then copy the source into the target
    //
    target.copyForPersistentInsert(source); // tuple in freelist must be already cleared

    try {
        insertTupleCommon(source, target, fallible);
    } catch (TupleStreamException const& e) {
        deleteTupleStorage(target); // also frees object columns
        throw;
    } catch (ConstraintFailureException const& e) {
        deleteTupleStorage(target); // also frees object columns
        throw;
    }
    // TODO: we do not catch other types of exceptions, such as
    // SQLException, etc. The assumption we held that no other
    // exceptions should be thrown in the try-block is pretty
    // daring and likely not correct.

    return target;
}

void PersistentTable::doInsertTupleCommon(TableTuple& source, TableTuple& target,
      bool fallible, bool shouldDRStream, bool delayTupleDelete) {
    if (fallible) {
        // not null checks at first
        FAIL_IF(!checkNulls(target)) {
            throw ConstraintFailureException(this, source, TableTuple(), CONSTRAINT_TYPE_NOT_NULL);
        }
    }

    // Write to DR stream before everything else to ensure nothing gets left in
    // the index if the append fails.
    if (hasDRTimestampColumn()) {
        setDRTimestampForTuple(target, false);
    }

    AbstractDRTupleStream* drStream = getDRTupleStream(m_executorContext);
    if (doDRActions(drStream) && shouldDRStream) {
        int64_t currentSpHandle = m_executorContext->currentSpHandle();
        int64_t currentUniqueId = m_executorContext->currentUniqueId();
        size_t drMark = drStream->appendTuple(m_signature, m_partitionColumn, currentSpHandle,
                                              currentUniqueId, target, DR_RECORD_INSERT);

        UndoQuantum* uq = ExecutorContext::currentUndoQuantum();
        if (uq && fallible) {
            uq->registerUndoAction(new (*uq) DRTupleStreamUndoAction(drStream, drMark, rowCostForDRRecord(DR_RECORD_INSERT)));
        }
    }

    if (m_schema->getUninlinedObjectColumnCount() != 0) {
        increaseStringMemCount(target.getNonInlinedMemorySizeForPersistentTable());
    }

    target.setActiveTrue();
    target.setPendingDeleteFalse();
    target.setPendingDeleteOnUndoReleaseFalse();
    target.setInlinedDataIsVolatileFalse();
    target.setNonInlinedDataIsVolatileFalse();

    // Prepare migrating work
    bool migrating = isTableWithMigrate(m_tableType);
    int64_t txnId = 0ll;
    if (migrating) {
        vassert(m_shadowStream != nullptr);
        uint16_t colIndex = getMigrateColumnIndex();
        NValue txnIdVal = target.getHiddenNValue(colIndex);
        if(!txnIdVal.isNull()){
            txnId = ValuePeeker::peekBigInt(txnIdVal);
            if (txnId == 0) {
                // MaterializedViewTriggerForInsert::processTupleInsert sends tuples
                // initialized from a zeroed-out buffer. Prevent a txnId == 0 from
                // being inserted in the migrating rows. Must do this before indexing.
                target.setHiddenNValue(colIndex, NValue::getNullValue(ValueType::tBIGINT));
                VOLT_DEBUG("Nulled out migrating txnId column %d at address %p", colIndex, target.address());
            }
        }
    }

    TableTuple conflict(m_schema);
    try {
        tryInsertOnAllIndexes(&target, &conflict);    // Also evaluates if the index update might throw
    } catch (std::exception const& e) {
        deleteTupleStorage(target); // also frees object columns
        throw;
    }
    if (!conflict.isNullTuple()) {
        throw ConstraintFailureException(this, source, conflict, CONSTRAINT_TYPE_UNIQUE,
                delayTupleDelete ? &m_surgeon : NULL);
    }

    /**
     * Inserts never "dirty" a tuple since the tuple is new, but...  The
     * COWIterator may still be scanning and if the tuple came from the free
     * list then it may need to be marked as dirty so it will be skipped. If COW
     * is on have it decide. COW should always set the dirty to false unless the
     * tuple is in a to be scanned area.
     */
    if (m_tableStreamer == NULL || !m_tableStreamer->notifyTupleInsert(target)) {
        target.setDirtyFalse();
    }

    // If inserted tuple had hidden txnId, add to migrating index (must only be when loading tuple
    // from a recover or rejoin snapshot)
     if (migrating && txnId != 0ll) {
         VOLT_DEBUG("Add recovered/rejoined migrating txnId %lld at address %p", txnId, target.address());
         migratingAdd(txnId, target);
     }

    // this is skipped for inserts that are never expected to fail,
    // like some (initially, all) cases of tuple migration on schema change
    if (fallible) {
        /*
         * Create and register an undo action.
         */
        UndoQuantum *uq = ExecutorContext::currentUndoQuantum();
        if (uq) {
           char* tupleData = partialCopyToPool(uq->getPool(), target.address(), target.tupleLength());
           //* enable for debug */ std::cout << "DEBUG: inserting " << (void*)target.address()
           //* enable for debug */           << " { " << target.debugNoHeader() << " } "
           //* enable for debug */           << " copied to " << (void*)tupleData << std::endl;
            UndoReleaseAction* undoAction = createInstanceFromPool<PersistentTableUndoInsertAction>(
                  *uq->getPool(), tupleData, &m_surgeon);
            SynchronizedThreadLock::addUndoAction(isReplicatedTable(), uq, undoAction);
            if (isTableWithExportInserts(m_tableType)) {
                vassert(m_shadowStream != nullptr);

                // insert to partitioned table or partition id 0 for replicated
                if (!isReplicatedTable() || m_executorContext->getPartitionId() == 0) {
                     m_shadowStream->streamTuple(target, ExportTupleStream::STREAM_ROW_TYPE::INSERT);
                }
            }
        }
    }

    // Insert the tuple into the delta table first.
    //
    // (Note: we may hit a NOT NULL constraint violation, or any
    // types of constraint violation. In which case, we want to
    // clean up by calling deleteTupleStorage, below)
    insertTupleIntoDeltaTable(source, fallible);
}

void PersistentTable::insertTupleCommon(TableTuple& source, TableTuple& target,
      bool fallible, bool shouldDRStream, bool delayTupleDelete) {
    // If the target table is a replicated table, only one thread can reach here.
    doInsertTupleCommon(source, target, fallible, shouldDRStream, delayTupleDelete);
    BOOST_FOREACH (auto viewHandler, m_viewHandlers) {
        viewHandler->handleTupleInsert(this, fallible);
    }

    // handle any materialized views
    BOOST_FOREACH (auto view, m_views) {
        view->processTupleInsert(target, fallible);
    }
}

/*
 * Insert a tuple but don't allocate a new copy of the uninlineable
 * strings or create an UndoAction or update a materialized view.
 */
void PersistentTable::insertTupleForUndo(char* tuple) {
    TableTuple target(m_schema);
    target.move(tuple);
    target.setPendingDeleteOnUndoReleaseFalse();
    --m_tuplesPinnedByUndo;
    --m_invisibleTuplesPendingDeleteCount;

    /*
     * The only thing to do is reinsert the tuple into the indexes. It was never moved,
     * just marked as deleted.
     */
    TableTuple conflict(m_schema);
    tryInsertOnAllIndexes(&target, &conflict);
    if (!conflict.isNullTuple()) {
        // First off, it should be impossible to violate a constraint when RESTORING an index to a
        // known good state via an UNDO of a delete.  So, assume that something is badly broken, here.
        // It's probably safer NOT to do too much cleanup -- such as trying to call deleteTupleStorage --
        // as there's no guarantee that it will improve things, and is likely just to tamper with
        // the crime scene.
        throwFatalException("Failed to insert tuple into table %s for undo:"
                            " unique constraint violation\n%s\n", m_name.c_str(),
                            target.debugNoHeader().c_str());
    }

    // Add tuple back to migrating index if needed
    if (isTableWithMigrate(m_tableType)) {
       NValue txnId = target.getHiddenNValue(getMigrateColumnIndex());
       if (!txnId.isNull()) {
          VOLT_DEBUG("Re-add migrating txnId %lld at address %p", ValuePeeker::peekBigInt(txnId), target.address());
          migratingAdd(ValuePeeker::peekBigInt(txnId), target);
       }
    }
}

/*
 * Regular tuple update function that does a copy and allocation for
 * updated strings and creates an UndoAction. Additional optimization
 * for callers that know which indexes to update.
 */
void PersistentTable::updateTupleWithSpecificIndexes(
      TableTuple& targetTupleToUpdate, TableTuple& sourceTupleWithNewValues,
      std::vector<TableIndex*> const& indexesToUpdate, bool fallible, bool updateDRTimestamp, bool fromMigrate) {
    UndoQuantum* uq = NULL;
    char* oldTupleData = NULL;
    int tupleLength = targetTupleToUpdate.tupleLength();

    /**
     * Check for index constraint violations.
     */
    if (fallible) {
        if (! checkUpdateOnUniqueIndexes(targetTupleToUpdate, sourceTupleWithNewValues, indexesToUpdate)) {
            throw ConstraintFailureException(
                  this, sourceTupleWithNewValues, targetTupleToUpdate, CONSTRAINT_TYPE_UNIQUE);
        }
        checkUpdateOnExpressions(sourceTupleWithNewValues, indexesToUpdate);
        /**
         * Check for null constraint violations. Assumes source tuple is fully fleshed out.
         */
        FAIL_IF(!checkNulls(sourceTupleWithNewValues)) {
            throw ConstraintFailureException(
                  this, sourceTupleWithNewValues, targetTupleToUpdate, CONSTRAINT_TYPE_NOT_NULL);
        }

        uq = ExecutorContext::currentUndoQuantum();
        if (uq) {
            /*
             * For undo purposes, before making any changes, save a copy of the state of the tuple
             * into the undo pool temp storage and hold onto it with oldTupleData.
             */
           oldTupleData = partialCopyToPool(uq->getPool(), targetTupleToUpdate.address(), targetTupleToUpdate.tupleLength());
           // We assume that only fallible and undoable UPDATEs should be propagated to the EXPORT Shadow Stream
           if (!isReplicatedTable() || m_executorContext->getPartitionId() == 0) {
               if (isTableWithExportUpdateOld(m_tableType)) {
                   m_shadowStream->streamTuple(targetTupleToUpdate, ExportTupleStream::STREAM_ROW_TYPE::UPDATE_OLD);
               }
               if (isTableWithExportUpdateNew(m_tableType)) {
                   m_shadowStream->streamTuple(sourceTupleWithNewValues, ExportTupleStream::STREAM_ROW_TYPE::UPDATE_NEW);
               }
           }
        }
    }

    // Write to the DR stream before doing anything else to ensure we don't
    // leave a half updated tuple behind in case this throws.
    if (hasDRTimestampColumn() && updateDRTimestamp) {
        setDRTimestampForTuple(sourceTupleWithNewValues, true);
    }

    if (isTableWithMigrate(m_tableType)) {
       uint16_t migrateColumnIndex = getMigrateColumnIndex();
       NValue txnIdValue = sourceTupleWithNewValues.getHiddenNValue(migrateColumnIndex);
       if (txnIdValue.isNull()) {
           if (fromMigrate) {
               int64_t txnId = getTableTxnId();
               VOLT_DEBUG("Set migrateColumn to txnId %lld at index %d on source %p, target %p",
                       txnId,
                       migrateColumnIndex,
                       sourceTupleWithNewValues.address(),
                       targetTupleToUpdate.address());
               sourceTupleWithNewValues.setHiddenNValue(migrateColumnIndex, ValueFactory::getBigIntValue(txnId));
           }
       } else {
           int64_t txnId = ValuePeeker::peekBigInt(txnIdValue);
           vassert(txnId != 0ll);       // Failing this might be in an uninitialized view tuple
           VOLT_DEBUG("Remove and null out migrating txnId %lld on source %p, target %p",
                   txnId,
                   sourceTupleWithNewValues.address(),
                   targetTupleToUpdate.address());
           sourceTupleWithNewValues.setHiddenNValue(migrateColumnIndex, NValue::getNullValue(ValueType::tBIGINT));
           migratingRemove(txnId, targetTupleToUpdate);
       }
    }

    AbstractDRTupleStream* drStream = getDRTupleStream(m_executorContext);
    if (!fromMigrate && doDRActions(drStream)) {
        int64_t currentSpHandle = m_executorContext->currentSpHandle();
        int64_t currentUniqueId = m_executorContext->currentUniqueId();
        size_t drMark = drStream->appendUpdateRecord(m_signature, m_partitionColumn, currentSpHandle,
                currentUniqueId, targetTupleToUpdate, sourceTupleWithNewValues);

        UndoQuantum* uq = ExecutorContext::currentUndoQuantum();
        if (uq && fallible) {
            uq->registerUndoAction(createInstanceFromPool<DRTupleStreamUndoAction>(
                     *uq->getPool(), drStream, drMark, rowCostForDRRecord(DR_RECORD_UPDATE)));
        }
    }

    if (m_tableStreamer != NULL) {
        m_tableStreamer->notifyTupleUpdate(targetTupleToUpdate);
    }

    /**
     * Remove the current tuple from any indexes.
     */
    const bool someIndexGotUpdated = !indexesToUpdate.empty();
    bool indexRequiresUpdate[indexesToUpdate.size()];
    if (someIndexGotUpdated) {
        for (int i = 0; i < indexesToUpdate.size(); i++) {
            TableIndex* index = indexesToUpdate[i];
            if (!index->keyUsesNonInlinedMemory() &&
                    !index->checkForIndexChange(&targetTupleToUpdate, &sourceTupleWithNewValues)) {
                indexRequiresUpdate[i] = false;
                continue;
            } else {
                indexRequiresUpdate[i] = true;
                if (!index->deleteEntry(&targetTupleToUpdate)) {
                    throwFatalException(
                            "Failed to remove tuple (%s) from index (during update) in Table: %s Index %s:\n%s",
                            targetTupleToUpdate.debug().c_str(), m_name.c_str(), index->getName().c_str(),
                            index->debug().c_str());
                }
            }
        }
    }

    // handle any materialized views, we first insert the tuple into delta table,
    // then hide the tuple from the scan temporarily.
    // (Cannot do in reversed order because the pending delete flag will also be copied)
    //
    // Note that this is guaranteed to succeed, since we are inserting an existing tuple
    // (soon to be deleted) into the delta table.
    insertTupleIntoDeltaTable(targetTupleToUpdate, fallible);
    {
        SetAndRestorePendingDeleteFlag setPending(targetTupleToUpdate);
        for (auto viewHandler: m_viewHandlers) {
            viewHandler->handleTupleDelete(this, fallible);
        }
        // This is for single table view.
        for (auto view: m_views) {
            view->processTupleDelete(targetTupleToUpdate, fallible);
        }
    }

    if (m_schema->getUninlinedObjectColumnCount() != 0) {
        decreaseStringMemCount(targetTupleToUpdate.getNonInlinedMemorySizeForPersistentTable());
        increaseStringMemCount(sourceTupleWithNewValues.getNonInlinedMemorySizeForPersistentTable());
    }

    // TODO: This is a little messed up.
    // We know what we want the target tuple's flags to look like after the copy,
    // so we carefully set them (rather than, say, ignore them) on the source tuple
    // and make sure to copy them (rather than, say, ignore them) in copyForPersistentUpdate
    // and that allows us to ignore them (rather than, say, set them) afterwards on the actual
    // target tuple that matters. What could be simpler?
    sourceTupleWithNewValues.setActiveTrue();
    // The isDirty flag is especially interesting because the COWcontext found it more convenient
    // to mark it on the target tuple. So, no problem, just copy it from the target tuple to the
    // source tuple so it can get copied back to the target tuple in copyForPersistentUpdate. Brilliant!
    //Copy the dirty status that was set by markTupleDirty.
    if (targetTupleToUpdate.isDirty()) {
        sourceTupleWithNewValues.setDirtyTrue();
    } else {
        sourceTupleWithNewValues.setDirtyFalse();
    }

    // Either the "before" or "after" object reference values that change will come in handy later,
    // so collect them up.
    std::vector<char*> oldObjects;
    std::vector<char*> newObjects;

    // this is the actual write of the new values
    targetTupleToUpdate.copyForPersistentUpdate(sourceTupleWithNewValues, oldObjects, newObjects);

    if (fromMigrate) {
        vassert(isTableWithMigrate(m_tableType) && m_shadowStream != nullptr);
        VOLT_DEBUG("Add migrating txnId %lld on target %p", getTableTxnId(), targetTupleToUpdate.address());
        migratingAdd(getTableTxnId(), targetTupleToUpdate);
        // add to shadow stream if the table is partitioned or partition 0 for replicated table
        if (!isReplicatedTable() || m_executorContext->getPartitionId() == 0) {
            m_shadowStream->streamTuple(sourceTupleWithNewValues, ExportTupleStream::MIGRATE, doDRActions(drStream) ? drStream : NULL);
        }
    }

    if (uq) {
        /*
         * Create and register an undo action with copies of the "before" and "after" tuple storage
         * and the "before" and "after" object pointers for non-inlined columns that changed.
         */
        char* newTupleData = partialCopyToPool(uq->getPool(), targetTupleToUpdate.address(), tupleLength);
        UndoReleaseAction* undoAction = createInstanceFromPool<PersistentTableUndoUpdateAction>(
              *uq->getPool(), oldTupleData, newTupleData, oldObjects, newObjects, &m_surgeon, someIndexGotUpdated, fromMigrate);
        SynchronizedThreadLock::addUndoAction(isReplicatedTable(), uq, undoAction);
    } else {
        // This is normally handled by the Undo Action's release (i.e. when there IS an Undo Action)
        // -- though maybe even that case should delegate memory management back to the PersistentTable
        // to keep the UndoAction stupid simple?
        // Anyway, there is no Undo Action in this case, so DIY.
        NValue::freeObjectsFromTupleStorage(oldObjects);
    }

    /**
     * Insert the updated tuple back into the indexes.
     */
    TableTuple conflict(m_schema);
    for (int i = 0; i < indexesToUpdate.size(); i++) {
        TableIndex* index = indexesToUpdate[i];
        if (!indexRequiresUpdate[i]) {
            continue;
        } else if (fromMigrate && index->isMigratingIndex()) {
           // For migrate, the hidden index should not be added back
            continue;
        }
        index->addEntry(&targetTupleToUpdate, &conflict);
        if (!conflict.isNullTuple()) {
             throwFatalException("Failed to insert updated tuple into index in Table: %s Index %s",
                                m_name.c_str(), index->getName().c_str());
        }
    }

    // Note that inserting into the delta table is guaranteed to
    // succeed, since we checked constraints above.
    insertTupleIntoDeltaTable(targetTupleToUpdate, fallible);
    for (auto viewHandler: m_viewHandlers) {
        viewHandler->handleTupleInsert(this, fallible);
    }

    // handle any materialized views
    for (auto view: m_views) {
        view->processTupleInsert(targetTupleToUpdate, fallible);
    }
}

/*
 * sourceTupleWithNewValues contains a copy of the tuple data before the update
 * and tupleWithUnwantedValues contains a copy of the updated tuple data.
 * First remove the current tuple value from any indexes (if asked to do so).
 * Then revert the tuple to the original preupdate values by copying the source to the target.
 * Then insert the new (or rather, old) value back into the indexes.
 */
void PersistentTable::updateTupleForUndo(char* tupleWithUnwantedValues,
                                         char* sourceTupleDataWithNewValues,
                                         bool revertIndexes,
                                         bool fromMigrate) {
    TableTuple matchable(m_schema);
    // Get the address of the tuple in the table from one of the copies on hand.
    // Any TableScan OR a primary key lookup on an already updated index will find the tuple
    // by its unwanted updated values.
    if (revertIndexes || primaryKeyIndex() == NULL) {
        matchable.move(tupleWithUnwantedValues);
    } else { // A primary key lookup on a not-yet-updated index will find the tuple by its original/new values.
        matchable.move(sourceTupleDataWithNewValues);
    }
    TableTuple targetTupleToUpdate = lookupTupleForUndo(matchable);
    TableTuple sourceTupleWithNewValues(sourceTupleDataWithNewValues, m_schema);

    //If the indexes were never updated there is no need to revert them.
    if (revertIndexes) {
        for (auto index : m_indexes) {
            if (!index->deleteEntry(&targetTupleToUpdate)) {
                throwFatalException("Failed to update tuple in Table: %s Index %s",
                        m_name.c_str(), index->getName().c_str());
            }
        }
    }

    if (m_schema->getUninlinedObjectColumnCount() != 0) {
        decreaseStringMemCount(targetTupleToUpdate.getNonInlinedMemorySizeForPersistentTable());
        increaseStringMemCount(sourceTupleWithNewValues.getNonInlinedMemorySizeForPersistentTable());
    }

    bool dirty = targetTupleToUpdate.isDirty();
    // this is the actual in-place revert to the old version
    targetTupleToUpdate.copy(sourceTupleWithNewValues);
    if (dirty) {
        targetTupleToUpdate.setDirtyTrue();
    } else {
        targetTupleToUpdate.setDirtyFalse();
    }

    //If the indexes were never updated there is no need to revert them.
    if (revertIndexes) {
        TableTuple conflict(m_schema);
        for (auto index : m_indexes) {
            index->addEntry(&targetTupleToUpdate, &conflict);
            if (!conflict.isNullTuple()) {
                throwFatalException("Failed to update tuple in Table: %s Index %s",
                        m_name.c_str(), index->getName().c_str());
            }
        }
    }

    // Revert migrating indexes
    if (fromMigrate) {
        vassert(m_shadowStream != nullptr);
        vassert(targetTupleToUpdate.getHiddenNValue(getMigrateColumnIndex()).isNull());
        VOLT_DEBUG("Remove migrating txnId %lld on target %p", getTableTxnId(), targetTupleToUpdate.address());
        migratingRemove(getTableTxnId(), targetTupleToUpdate);
    } else if (isTableWithMigrate(m_tableType)) {
        NValue const txnId = targetTupleToUpdate.getHiddenNValue(getMigrateColumnIndex());
        if(!txnId.isNull()){
            VOLT_DEBUG("Add migrating txnId %lld on target %p",
                    ValuePeeker::peekBigInt(txnId), targetTupleToUpdate.address());
            migratingAdd(ValuePeeker::peekBigInt(txnId), targetTupleToUpdate);
        }
    }
}

void PersistentTable::deleteTuple(TableTuple& target, bool fallible, bool removeMigratingIndex) {
    UndoQuantum* uq = ExecutorContext::currentUndoQuantum();
    bool createUndoAction = fallible && (uq != NULL);

    // May not delete an already deleted tuple.
    vassert(target.isActive());

    // The tempTuple is forever!
    vassert(&target != &m_tempTuple);

    // Write to the DR stream before doing anything else to ensure nothing will
    // be left forgotten in case this throws.
    AbstractDRTupleStream* drStream = getDRTupleStream(m_executorContext);
    if (doDRActions(drStream)) {
        int64_t currentSpHandle = m_executorContext->currentSpHandle();
        int64_t currentUniqueId = m_executorContext->currentUniqueId();
        size_t drMark = drStream->appendTuple(m_signature, m_partitionColumn, currentSpHandle,
                                              currentUniqueId, target, DR_RECORD_DELETE);

        if (createUndoAction) {
            uq->registerUndoAction(createInstanceFromPool<DRTupleStreamUndoAction>(
                     *uq->getPool(), drStream, drMark, rowCostForDRRecord(DR_RECORD_DELETE)));
        }
    }

    // Just like insert, we want to remove this tuple from all of our indexes
    deleteFromAllIndexes(&target);
    if (isTableWithMigrate(m_tableType) && removeMigratingIndex) {
        NValue txnId = target.getHiddenNValue(getMigrateColumnIndex());
        if (!txnId.isNull()) {
            VOLT_DEBUG("Remove migrating txnId %lld on target %p",
                    ValuePeeker::peekBigInt(txnId), target.address());
            migratingRemove(ValuePeeker::peekBigInt(txnId), target);
        }
    }
    if (createUndoAction) {
        target.setPendingDeleteOnUndoReleaseTrue();
        ++m_tuplesPinnedByUndo;
        ++m_invisibleTuplesPendingDeleteCount;
        UndoReleaseAction* undoAction = createInstanceFromPool<PersistentTableUndoDeleteAction>(
              *uq->getPool(), target.address(), &m_surgeon);
        SynchronizedThreadLock::addUndoAction(isReplicatedTable(), uq, undoAction, this);
        if (isTableWithExportDeletes(m_tableType)) {
            vassert(m_shadowStream != nullptr);
            if (!isReplicatedTable() || m_executorContext->getPartitionId() == 0) {
                m_shadowStream->streamTuple(target, ExportTupleStream::STREAM_ROW_TYPE::DELETE);
            }
        }
    }

    // handle any materialized views, insert the tuple into delta table,
    // then hide the tuple from the scan temporarily.
    //
    // Note that this is guaranteed to succeed, since we are inserting an existing tuple
    // (soon to be deleted) into the delta table.
    insertTupleIntoDeltaTable(target, fallible);
    {
        SetAndRestorePendingDeleteFlag setPending(target);
        // for multi-table views
        BOOST_FOREACH (auto viewHandler, m_viewHandlers) {
            viewHandler->handleTupleDelete(this, fallible);
        }
        // This is for single table view.
        BOOST_FOREACH (auto view, m_views) {
            view->processTupleDelete(target, fallible);
        }
    }

    if (createUndoAction) {
        return;
    }

    // Here, for reasons of infallibility or no active UndoLog, there is no undo, there is only DO.
    deleteTupleFinalize(target);
}


/**
 * This entry point is triggered by the successful release of an UndoDeleteAction.
 */
void PersistentTable::deleteTupleRelease(char* tupleData) {
    TableTuple target(m_schema);
    target.move(tupleData);
    target.setPendingDeleteOnUndoReleaseFalse();
    --m_tuplesPinnedByUndo;
    --m_invisibleTuplesPendingDeleteCount;
    deleteTupleFinalize(target);
}

/**
 * Actually follow through with a "delete" -- this is common code between UndoDeleteAction release and the
 * all-at-once infallible deletes that bypass Undo processing.
 */
void PersistentTable::deleteTupleFinalize(TableTuple& target) {
    // For replicated table
    // delete the tuple directly but preserve the deleted tuples to tempTable for cowIterator
    // the same way as Update

    // A snapshot (background scan) in progress can still cause a hold-up.
    // notifyTupleDelete() defaults to returning true for all context types
    // other than CopyOnWriteContext.
    if (m_tableStreamer != NULL &&
            ! m_tableStreamer->notifyTupleDelete(target)) {
        // Mark it pending delete and let the snapshot land the finishing blow.

        // This "already pending delete" guard prevents any
        // (possible?) case of double-counting a doubly-applied pending delete
        // before it gets ignored.
        // This band-aid guard just keeps such a condition from becoming an
        // inconvenience to a "testability feature" implemented in tableutil.cpp
        // for the benefit of CopyOnWriteTest.cpp.
        // Maybe it should just be an assert --
        // maybe we are missing a final opportunity to detect the "inconceivable",
        // which, if ignored, may leave a wake of mysterious and catastrophic side effects.
        // There's always the option of setting a breakpoint on this return.
        if (target.isPendingDelete()) {
            return;
        }

        ++m_invisibleTuplesPendingDeleteCount;
        target.setPendingDeleteTrue();
        return;
    }

    // No snapshot in progress cares, just whack it.
    deleteTupleStorage(target); // also frees object columns
}

/**
 * Assumptions:
 * All tuples will be deleted in storage order.
 * Indexes and views have been destroyed first.
 */
void PersistentTable::deleteTupleForSchemaChange(TableTuple& target) {
    TBPtr block = findBlock(target.address(), m_data, m_tableAllocationSize);
    // free object columns along with empty tuple block storage
    deleteTupleStorage(target, block, true);
}

/*
 * Delete a tuple by looking it up via table scan or a primary key
 * index lookup. An undo initiated delete like deleteTupleForUndo
 * is in response to the insertion of a new tuple by insertTuple
 * and that by definition is a tuple that is of no interest to
 * the COWContext. The COWContext set the tuple to have the
 * correct dirty setting when the tuple was originally inserted.
 * TODO remove duplication with regular delete. Also no view updates.
 *
 * NB: This is also used as a generic delete for Elastic rebalance.
 *     skipLookup will be true in this case because the passed tuple
 *     can be used directly.
 */
void PersistentTable::deleteTupleForUndo(char* tupleData, bool skipLookup) {
    TableTuple matchable(tupleData, m_schema);
    TableTuple target(tupleData, m_schema);
    //* enable for debug */ std::cout << "DEBUG: undoing "
    //* enable for debug */           << " { " << target.debugNoHeader() << " } "
    //* enable for debug */           << " copied to " << (void*)tupleData << std::endl;
    if (!skipLookup) {
        // The UndoInsertAction got a pooled copy of the tupleData.
        // Relocate the original tuple actually in the table.
        target = lookupTupleForUndo(matchable);
    }
    if (target.isNullTuple()) {
        throwFatalException("Failed to delete tuple from table %s: tuple does not exist\n%s\n", m_name.c_str(),
              matchable.debugNoHeader().c_str());
    }
    //* enable for debug */ std::cout << "DEBUG: finding " << (void*)target.address()
    //* enable for debug */           << " { " << target.debugNoHeader() << " } "
    //* enable for debug */           << " copied to " << (void*)tupleData << std::endl;

    // Make sure that they are not trying to delete the same tuple twice
    vassert(target.isActive());
    deleteFromAllIndexes(&target);

    // The inserted tuple could have been migrated from stream snapshot/rejoin, undo the migrating indexes
    if (isTableWithMigrate(m_tableType)) {
        NValue txnId = target.getHiddenNValue(getMigrateColumnIndex());
        if(!txnId.isNull()){
            VOLT_DEBUG("Remove migrating txnId %lld on target %p",
                    ValuePeeker::peekBigInt(txnId), target.address());
            migratingRemove(ValuePeeker::peekBigInt(txnId), target);
        }
    }
    deleteTupleFinalize(target); // also frees object columns
}

TableTuple PersistentTable::lookupTuple(TableTuple tuple, LookupType lookupType) {
    if (m_pkeyIndex) {
        return m_pkeyIndex->uniqueMatchingTuple(tuple);
    }
    /*
     * Do a table scan.
     */
    TableTuple tableTuple(m_schema);
    TableIterator ti(this, m_data.begin());

    if (lookupType == LOOKUP_FOR_DR && m_schema->hiddenColumnCount()) {
        // Force column compare for DR so we can easily use the filter
        HiddenColumnFilter filter = HiddenColumnFilter::create(HiddenColumnFilter::EXCLUDE_MIGRATE, m_schema);
        while (ti.next(tableTuple)) {
            if (tableTuple.equalsNoSchemaCheck(tuple, &filter)) {
                return tableTuple;
            }
        }
    } else if (lookupType != LOOKUP_FOR_UNDO && m_schema->getUninlinedObjectColumnCount() != 0) {
        while (ti.next(tableTuple)) {
            if (tableTuple.equalsNoSchemaCheck(tuple)) {
                return tableTuple;
            }
        }
    } else {
        size_t tuple_length;
        if (lookupType == LOOKUP_BY_VALUES && m_schema->hiddenColumnCount() > 0) {
            // Looking up a tuple by values should not include any internal
            // hidden column values, which are appended to the end of the
            // tuple.
            tuple_length = m_schema->offsetOfHiddenColumns();
        } else {
            tuple_length = m_schema->tupleLength();
        }
        // Do an inline tuple byte comparison
        // to avoid matching duplicate tuples with different pointers to Object storage
        // -- which would cause erroneous releases of the wrong Object storage copy.
        while (ti.next(tableTuple)) {
            char* tableTupleData = tableTuple.address() + TUPLE_HEADER_SIZE;
            char* tupleData = tuple.address() + TUPLE_HEADER_SIZE;
            if (::memcmp(tableTupleData, tupleData, tuple_length) == 0) {
                return tableTuple;
            }
        }
    }
    TableTuple nullTuple(m_schema);
    return nullTuple;
}

void PersistentTable::insertIntoAllIndexes(TableTuple* tuple) {
    TableTuple conflict(m_schema);
    BOOST_FOREACH (auto index, m_indexes) {
        index->addEntry(tuple, &conflict);
        if (!conflict.isNullTuple()) {
            throwFatalException(
                    "Failed to insert tuple in Table: %s Index %s", m_name.c_str(), index->getName().c_str());
        }
    }
}

void PersistentTable::deleteFromAllIndexes(TableTuple* tuple) {
    BOOST_FOREACH (auto index, m_indexes) {
        if (!index->deleteEntry(tuple)) {
            throwFatalException(
                    "Failed to delete tuple in Table: %s Index %s",
                    m_name.c_str(), index->getName().c_str());
        }
    }
}

void PersistentTable::rollbackIndexChanges(TableTuple* tuple, int upto) {
   for(int i = 0; i < upto; ++i) {
      m_indexes[i]->deleteEntry(tuple);
   }
}

void PersistentTable::tryInsertOnAllIndexes(TableTuple* tuple, TableTuple* conflict) {
   int i = 0;
   try {
      for (; i < indexCount(); ++i) {
         m_indexes[i]->addEntry(tuple, conflict);
         FAIL_IF(!conflict->isNullTuple()) {
            VOLT_DEBUG("Failed to insert into index %s,%s", m_indexes[i]->getTypeName().c_str(),
                  m_indexes[i]->getName().c_str());
            rollbackIndexChanges(tuple, i);
            return;
         }
      }
   } catch (std::exception const& e) {
      rollbackIndexChanges(tuple, i);
      throw;
   }
}

void PersistentTable::checkUpdateOnExpressions(TableTuple const& sourceTupleWithNewValues,
      std::vector<TableIndex*> const& indexesToUpdate) {
   try {
      for (auto& index: indexesToUpdate) {
         for (auto& expr: index->getIndexedExpressions()) {
            expr->eval(&sourceTupleWithNewValues, nullptr);
         }
      }
   } catch (SQLException const& e) {   // TODO: is this necessary?
      throw ConstraintFailureException(this, sourceTupleWithNewValues, e.what());
   }
}

bool PersistentTable::checkUpdateOnUniqueIndexes(TableTuple& targetTupleToUpdate,
      TableTuple const& sourceTupleWithNewValues, std::vector<TableIndex*> const& indexesToUpdate) {
    for(auto const* index: indexesToUpdate) {
        if (index->isUniqueIndex()) {
            if (index->checkForIndexChange(&targetTupleToUpdate, &sourceTupleWithNewValues) == false)
                continue; // no update is needed for this index

            // if there is a change, the new_key has to be checked
            FAIL_IF (index->exists(&sourceTupleWithNewValues)) {
                VOLT_WARN("Unique Index '%s' complained to the update", index->debug().c_str());
                return false; // cannot insert the new value
            }
        }
    }

    return true;
}

// ------------------------------------------------------------------
// UTILITY
// ------------------------------------------------------------------
std::string PersistentTable::tableType() const { return "PersistentTable"; }

bool PersistentTable::equals(PersistentTable* other) {
    if ( ! Table::equals(other)) {
        return false;
    } else if (!(indexCount() == other->indexCount())) {
        return false;
    }

    std::vector<voltdb::TableIndex*> const& indexes = allIndexes();
    std::vector<voltdb::TableIndex*> const& otherIndexes = other->allIndexes();
    if (!(indexes.size() == indexes.size())) {
        return false;
    }
    for (std::size_t ii = 0; ii < indexes.size(); ii++) {
        if (!(indexes[ii]->equals(otherIndexes[ii]))) {
            return false;
        }
    }
    return true;
}

std::string PersistentTable::debug(const std::string& spacer) const {
    std::ostringstream buffer;
    buffer << Table::debug(spacer);
    if (m_shadowStream != nullptr) {
        std::string infoSpacer = spacer + "  |";
        buffer << infoSpacer << "\tSHADOW STREAM: " << m_shadowStream->debug() << "\n";
    }
#ifdef VOLT_TRACE_ENABLED
    std::string infoSpacer = spacer + "  |";
    buffer << infoSpacer << "\tINDEXES: " << m_indexes.size() << "\n";

    // Indexes
    buffer << infoSpacer << "===========================================================\n";
    for (int index_ctr = 0; index_ctr < m_indexes.size(); ++index_ctr) {
        if (m_indexes[index_ctr]) {
            buffer << infoSpacer << "\t[" << index_ctr << "] " << m_indexes[index_ctr]->debug();
            //
            // Primary Key
            //
            if (m_pkeyIndex != NULL && m_pkeyIndex->getName().compare(m_indexes[index_ctr]->getName()) == 0) {
                buffer << " [PRIMARY KEY]";
            }
            buffer << "\n";
        }
    }
#endif

    return buffer.str();
}

/**
 * Loads tuple data from the serialized table.
 * Used for snapshot restore and bulkLoad
 */
void PersistentTable::loadTuplesForLoadTable(SerializeInputBE &serialInput, Pool *stringPool,
      ReferenceSerializeOutput *uniqueViolationOutput, const LoadTableCaller &caller) {
    serialInput.readInt(); // rowstart

    serialInput.readByte();

    int16_t colcount = serialInput.readShort();
    vassert(colcount >= 0);

    // Store the following information so that we can provide them to the user
    // on failure
    ValueType types[colcount];
    boost::scoped_array<std::string> names(new std::string[colcount]);

    // skip the column types
    for (int i = 0; i < colcount; ++i) {
        types[i] = (ValueType) serialInput.readEnumInSingleByte();
    }

    // skip the column names
    for (int i = 0; i < colcount; ++i) {
        names[i] = serialInput.readTextString();
    }

    // Check if the column count matches what the temp table is expecting
    uint16_t expectedColumnCount = caller.getExpectedColumnCount(m_schema);
    if (colcount != expectedColumnCount) {
        std::stringstream message(std::stringstream::in
                                  | std::stringstream::out);
        message << "Column count mismatch. Expecting "
                << expectedColumnCount
                << ", but " << colcount << " given" << std::endl;
        message << "Expecting the following columns:" << std::endl;
        message << debug() << std::endl;
        message << "The following columns are given:" << std::endl;
        for (int i = 0; i < colcount; i++) {
            message << "column " << i << ": " << names[i]
                    << ", type = " << getTypeName(types[i]) << std::endl;
        }
        throw SerializableEEException(message.str().c_str());
    }

    int tupleCount = serialInput.readInt();
    vassert(tupleCount >= 0);

    TableTuple target(m_schema);
    //Reserve space for a length prefix for rows that violate unique constraints
    //If there is no output supplied it will just throw
    size_t lengthPosition = 0;
    int32_t serializedTupleCount = 0;
    size_t tupleCountPosition = 0;
    if (uniqueViolationOutput != NULL) {
        lengthPosition = uniqueViolationOutput->reserveBytes(4);
    }

    for (int i = 0; i < tupleCount; ++i) {
        nextFreeTuple(&target);
        target.setActiveTrue();
        target.setDirtyFalse();
        target.setPendingDeleteFalse();
        target.setPendingDeleteOnUndoReleaseFalse();

        try {
            target.deserializeFrom(serialInput, stringPool, caller);
        } catch (SQLException &e) {
            deleteTupleStorage(target);
            throw;
        }
        // TODO: we do not catch other types of exceptions, such as
        // SQLException, etc. The assumption we held that no other
        // exceptions should be thrown in the try-block is pretty
        // daring and likely not correct.
        processLoadedTuple(target, uniqueViolationOutput, serializedTupleCount, tupleCountPosition,
                           caller.shouldDrStream());
    }

    //If unique constraints are being handled, write the length/size of constraints that occured
    if (uniqueViolationOutput != NULL) {
        if (serializedTupleCount == 0) {
            uniqueViolationOutput->writeIntAt(lengthPosition, 0);
        } else {
            uniqueViolationOutput->writeIntAt(lengthPosition,
                    uniqueViolationOutput->position() - lengthPosition - sizeof(int32_t));
            uniqueViolationOutput->writeIntAt(tupleCountPosition, serializedTupleCount);
        }
    }
}

/*
 * Implemented by persistent table and called by Table::loadTuplesFrom or Table::loadTuplesForLoadTable
 * to do additional processing for views, Export, DR and non-inline
 * memory tracking
 */
void PersistentTable::processLoadedTuple(TableTuple& tuple,
                                         ReferenceSerializeOutput* uniqueViolationOutput,
                                         int32_t& serializedTupleCount,
                                         size_t& tupleCountPosition,
                                         bool shouldDRStreamRows) {
    try {
        insertTupleCommon(tuple, tuple, true, shouldDRStreamRows, !uniqueViolationOutput);
    } catch (ConstraintFailureException& e) {
        if ( ! uniqueViolationOutput) {
            throw;
        } else if (serializedTupleCount == 0) {
            serializeColumnHeaderTo(*uniqueViolationOutput);
            tupleCountPosition = uniqueViolationOutput->reserveBytes(sizeof(int32_t));
        }
        serializedTupleCount++;
        tuple.serializeTo(*uniqueViolationOutput);
        deleteTupleStorage(tuple);
    } catch (TupleStreamException& e) {
        deleteTupleStorage(tuple);
        throw;
    }
    // TODO: we do not catch other types of exceptions, such as
    // SQLException, etc. The assumption we held that no other
    // exceptions should be thrown in the try-block is pretty
    // daring and likely not correct.
}

/** Prepare table for streaming from serialized data. */
bool PersistentTable::activateStream(
    TableStreamType streamType,
    HiddenColumnFilter::Type hiddenColumnFilterType,
    int32_t partitionId,
    CatalogId tableId,
    ReferenceSerializeInputBE& serializeIn) {
    /*
     * Allow multiple stream types for the same partition by holding onto the
     * TableStreamer object. TableStreamer enforces which multiple stream type
     * combinations are allowed. Expect the partition ID not to change.
     */
    if (m_isReplicated) {
        partitionId = -1;
    }
    vassert(m_tableStreamer == NULL || partitionId == m_tableStreamer->getPartitionID());
    if (m_tableStreamer == NULL) {
        m_tableStreamer.reset(new TableStreamer(partitionId, *this, tableId));
    }

    std::vector<std::string> predicateStrings;
    // Grab snapshot or elastic stream predicates.
    if (tableStreamTypeHasPredicates(streamType)) {
        int npreds = serializeIn.readInt();
        if (npreds > 0) {
            predicateStrings.reserve(npreds);
            for (int ipred = 0; ipred < npreds; ipred++) {
                std::string spred = serializeIn.readTextString();
                predicateStrings.push_back(spred);
            }
        }
    }

    const HiddenColumnFilter filter = HiddenColumnFilter::create(hiddenColumnFilterType, m_schema);

    return m_tableStreamer->activateStream(m_surgeon, streamType, filter, predicateStrings);
}

/**
 * Prepare table for streaming from serialized data (internal for tests).
 * Use custom TableStreamer provided.
 * Return true on success or false if it was already active.
 */
bool PersistentTable::activateWithCustomStreamer(TableStreamType streamType,
        HiddenColumnFilter::Type hiddenColumnFilterType, boost::shared_ptr<TableStreamerInterface> tableStreamer,
        CatalogId tableId, std::vector<std::string>& predicateStrings, bool skipInternalActivation) {
    // Expect m_tableStreamer to be null. Only make it fatal in debug builds.
    vassert(m_tableStreamer == NULL);
    m_tableStreamer = tableStreamer;
    bool success = !skipInternalActivation;
    if (!skipInternalActivation) {
        const HiddenColumnFilter filter = HiddenColumnFilter::create(hiddenColumnFilterType, m_schema);
        success = m_tableStreamer->activateStream(m_surgeon, streamType, filter, predicateStrings);
    }
    return success;
}

/**
 * Attempt to serialize more tuples from the table to the provided output streams.
 * Return remaining tuple count, 0 if done, or TABLE_STREAM_SERIALIZATION_ERROR on error.
 */
int64_t PersistentTable::streamMore(TupleOutputStreamProcessor& outputStreams,
        TableStreamType streamType, std::vector<int>& retPositions) {
    if (m_tableStreamer.get() == NULL) {
        char errMsg[1024];
        snprintf(errMsg, sizeof errMsg, "No table streamer of Type %s for table %s.",
                tableStreamTypeToString(streamType).c_str(), name().c_str());
        errMsg[sizeof errMsg - 1] = '\0';
        LogManager::getThreadLogger(LOGGERID_HOST)->log(LOGLEVEL_ERROR, errMsg);

        return TABLE_STREAM_SERIALIZATION_ERROR;
    }
    return m_tableStreamer->streamMore(outputStreams, streamType, retPositions);
}

/**
 * Create a tree index on the primary key and then iterate it and hash
 * the tuple data.
 */
size_t PersistentTable::hashCode() {
    boost::scoped_ptr<TableIndex> pkeyIndex(TableIndexFactory::cloneEmptyTreeIndex(*m_pkeyIndex));
    TableIterator iter(this, m_data.begin());
    TableTuple tuple(schema());
    while (iter.next(tuple)) {
        pkeyIndex->addEntry(&tuple, NULL);
    }

    IndexCursor indexCursor(pkeyIndex->getTupleSchema());
    pkeyIndex->moveToEnd(true, indexCursor);

    size_t hashCode = 0;
    while (true) {
         tuple = pkeyIndex->nextValue(indexCursor);
         if (tuple.isNullTuple()) {
             break;
         }
         tuple.hashCode(hashCode);
    }
    return hashCode;
}

void PersistentTable::notifyBlockWasCompactedAway(TBPtr block) {
    if (m_blocksNotPendingSnapshot.find(block) == m_blocksNotPendingSnapshot.end()) {
        // do not find block in not pending snapshot container
        vassert(m_tableStreamer.get() != NULL);
        vassert(m_blocksPendingSnapshot.find(block) != m_blocksPendingSnapshot.end());
        m_tableStreamer->notifyBlockWasCompactedAway(block);
    } else { // check that block is in pending snapshot container
       vassert(m_blocksPendingSnapshot.find(block) == m_blocksPendingSnapshot.end());
    }
}

// Call-back from TupleBlock::merge() for each tuple moved.
void PersistentTable::notifyTupleMovement(TBPtr sourceBlock, TBPtr targetBlock,
      TableTuple& sourceTuple, TableTuple& targetTuple) {
    if (m_tableStreamer != NULL) {
        m_tableStreamer->notifyTupleMovement(sourceBlock, targetBlock, sourceTuple, targetTuple);
    }
}

void PersistentTable::swapTuples(TableTuple& originalTuple,
                                 TableTuple& destinationTuple) {
    ::memcpy(destinationTuple.address(), originalTuple.address(), m_tupleLength);
    originalTuple.setActiveFalse();
    vassert(!originalTuple.isPendingDeleteOnUndoRelease());

    /*
     * If the tuple is pending deletion then it isn't in any of the indexes.
     * However that contradicts the assertion above that the tuple is not
     * pending deletion. In current Volt there is only one transaction executing
     * at any given time and the commit always releases the undo quantum
     * because there is no speculation. This situation should be impossible
     * as the assertion above implies. It looks like this is forward thinking
     * code for something that shouldn't happen right now.
     *
     * However this still isn't sufficient to actually work if speculation
     * is implemented because moving the tuple will invalidate the pointer
     * in the undo action for deleting the tuple. If the transaction ends
     * up being rolled back it won't find the tuple! You would have to go
     * back and update the undo action (how would you find it?) or
     * not move the tuple.
     */
    if (!originalTuple.isPendingDelete()) {
        BOOST_FOREACH (auto index, m_indexes) {
            if (!index->replaceEntryNoKeyChange(destinationTuple, originalTuple)) {
                throwFatalException("Failed to update tuple in Table: %s Index %s",
                                    m_name.c_str(), index->getName().c_str());
            }
        }
    }

    if (isTableWithMigrate(m_tableType)) {
        int64_t migrateTxnId = ValuePeeker::peekBigInt(originalTuple.getHiddenNValue(getMigrateColumnIndex()));
        if (migrateTxnId != INT64_NULL) {
            MigratingRows::iterator it = m_migratingRows.find(migrateTxnId);

            // The delete-pending tuple should have been removed from migrating index
            if (originalTuple.isPendingDelete()) {
                vassert(it == m_migratingRows.end());
            } else {
                 vassert(it != m_migratingRows.end());
                 MigratingBatch& batch = it->second;
                 void* addr = originalTuple.address();
                 __attribute__((unused)) size_t found = batch.erase(addr);
                 vassert(found == 1);
                 batch.emplace(destinationTuple.address());
            }
        }
    }
}

bool PersistentTable::doCompactionWithinSubset(TBBucketPtrVector* bucketVector) {
    /**
     * First find the two best candidate blocks
     */
    TBPtr fullest;
    TBBucketI fullestIterator;
    bool foundFullest = false;
    for (int ii = (TUPLE_BLOCK_NUM_BUCKETS - 1); ii >= 0; ii--) {
        fullestIterator = (*bucketVector)[ii]->begin();
        if (fullestIterator != (*bucketVector)[ii]->end()) {
            foundFullest = true;
            fullest = *fullestIterator;
            break;
        }
    }
    if (!foundFullest) {
        //std::cout << "Could not find a fullest block for compaction" << std::endl;
        return false;
    }

    int fullestBucketChange = NO_NEW_BUCKET_INDEX;
    while (fullest->hasFreeTuples()) {
        TBPtr lightest;
        TBBucketI lightestIterator;
        bool foundLightest = false;

        for (int ii = 0; ii < TUPLE_BLOCK_NUM_BUCKETS; ii++) {
            lightestIterator = (*bucketVector)[ii]->begin();
            if (lightestIterator != (*bucketVector)[ii]->end()) {
                lightest = lightestIterator.key();
                if (lightest != fullest) {
                    foundLightest = true;
                    break;
                }
                vassert(lightest == fullest);
                lightestIterator++;
                if (lightestIterator != (*bucketVector)[ii]->end()) {
                    lightest = lightestIterator.key();
                    foundLightest = true;
                    break;
                }
            }
        }
        if (!foundLightest) {
            //could not find a lightest block for compaction
            return false;
        }

        std::pair<int, int> bucketChanges = fullest->merge(this, lightest, this);
        int tempFullestBucketChange = bucketChanges.first;
        if (tempFullestBucketChange != NO_NEW_BUCKET_INDEX) {
            fullestBucketChange = tempFullestBucketChange;
        }

        if (lightest->isEmpty()) {
            notifyBlockWasCompactedAway(lightest);
            m_data.erase(lightest->address());
            m_blocksWithSpace.erase(lightest);
            m_blocksNotPendingSnapshot.erase(lightest);
            m_blocksPendingSnapshot.erase(lightest);
            lightest->swapToBucket(TBBucketPtr());
        } else {
            int lightestBucketChange = bucketChanges.second;
            if (lightestBucketChange != NO_NEW_BUCKET_INDEX) {
                lightest->swapToBucket((*bucketVector)[lightestBucketChange]);
            }
        }
    }

    if (fullestBucketChange != NO_NEW_BUCKET_INDEX) {
        fullest->swapToBucket((*bucketVector)[fullestBucketChange]);
    }
    if (!fullest->hasFreeTuples()) {
        m_blocksWithSpace.erase(fullest);
    }
    return true;
}

void PersistentTable::doIdleCompaction() {
    if (!m_blocksNotPendingSnapshot.empty()) {
        doCompactionWithinSubset(&m_blocksNotPendingSnapshotLoad);
    }
    if (!m_blocksPendingSnapshot.empty()) {
        doCompactionWithinSubset(&m_blocksPendingSnapshotLoad);
    }
}

bool PersistentTable::doForcedCompaction() {
    bool hadWork1 = true;
    bool hadWork2 = true;
    int64_t notPendingCompactions = 0;
    int64_t pendingCompactions = 0;

    char msg[512];

    boost::posix_time::ptime startTime(boost::posix_time::microsec_clock::universal_time());

    int failedCompactionCountBefore = m_failedCompactionCount;
    while (compactionPredicate()) {
        vassert(hadWork1 || hadWork2);
        if (!hadWork1 && !hadWork2) {
            /*
             * If this code is reached it means that the compaction predicate
             * thinks that it should be possible to merge some blocks,
             * but there were no blocks found in the load buckets that were
             * eligible to be merged. This is a bug in either the predicate
             * or more likely the code that moves blocks from bucket to bucket.
             * This isn't fatal because the list of blocks with free space
             * and deletion of empty blocks is handled independently of
             * the book keeping for load buckets and merging. As the load
             * of the missing (missing from the load buckets)
             * blocks changes they should end up being inserted
             * into the bucketing system again and will be
             * compacted if necessary or deleted when empty.
             * This is a work around for ENG-939
             */
            if (m_failedCompactionCount % 5000 == 0) {
                snprintf(msg, sizeof(msg), "Compaction predicate said there should be "
                         "blocks to compact but no blocks were found "
                         "to be eligible for compaction. This has "
                         "occurred %d times.", m_failedCompactionCount);
                msg[sizeof msg - 1] = '\0';
                LogManager::getThreadLogger(LOGGERID_SQL)->log(LOGLEVEL_WARN, msg);
            }
            if (m_failedCompactionCount == 0) {
                printBucketInfo();
            }
            m_failedCompactionCount++;
            break;
        }
        if (!m_blocksNotPendingSnapshot.empty() && hadWork1) {
            //std::cout << "Compacting blocks not pending snapshot " << m_blocksNotPendingSnapshot.size() << std::endl;
            hadWork1 = doCompactionWithinSubset(&m_blocksNotPendingSnapshotLoad);
            notPendingCompactions++;
        }
        if (!m_blocksPendingSnapshot.empty() && hadWork2) {
            //std::cout << "Compacting blocks pending snapshot " << m_blocksPendingSnapshot.size() << std::endl;
            hadWork2 = doCompactionWithinSubset(&m_blocksPendingSnapshotLoad);
            pendingCompactions++;
        }
    }
    //If compactions have been failing lately, but it didn't fail this time
    //then compaction progressed until the predicate was satisfied
    if (failedCompactionCountBefore > 0 && failedCompactionCountBefore == m_failedCompactionCount) {
        snprintf(msg, sizeof(msg), "Recovered from a failed compaction scenario "
                "and compacted to the point that the compaction predicate was "
                "satisfied after %d failed attempts", failedCompactionCountBefore);
        msg[sizeof msg - 1] = '\0';
        LogManager::getThreadLogger(LOGGERID_SQL)->log(LOGLEVEL_INFO, msg);
        m_failedCompactionCount = 0;
    }

    vassert(!compactionPredicate());
    boost::posix_time::ptime endTime(boost::posix_time::microsec_clock::universal_time());
    boost::posix_time::time_duration duration = endTime - startTime;
    snprintf(msg, sizeof(msg), "Finished forced compaction of %zd non-snapshot blocks and %zd snapshot blocks with allocated tuple count %zd in %zd ms on table %s",
            (intmax_t)notPendingCompactions, (intmax_t)pendingCompactions, (intmax_t)allocatedTupleCount(),
            (intmax_t)duration.total_milliseconds(), m_name.c_str());
    msg[sizeof msg - 1] = '\0';
    LogManager::getThreadLogger(LOGGERID_SQL)->log(LOGLEVEL_INFO, msg);
    return (notPendingCompactions + pendingCompactions) > 0;
}

void PersistentTable::printBucketInfo() {
    std::cout << std::endl;
    TBMapI iter = m_data.begin();
    while (iter != m_data.end()) {
        std::cout << "Block " << static_cast<void*>(iter.data()->address()) << " has " <<
                iter.data()->activeTuples() << " active tuples and " << iter.data()->lastCompactionOffset()
                << " last compaction offset and is in bucket " <<
                static_cast<void*>(iter.data()->currentBucket().get()) <<
                std::endl;
        iter++;
    }

    boost::unordered_set<TBPtr>::iterator blocksNotPendingSnapshot = m_blocksNotPendingSnapshot.begin();
    std::cout << "Blocks not pending snapshot: ";
    while (blocksNotPendingSnapshot != m_blocksNotPendingSnapshot.end()) {
        std::cout << static_cast<void*>(TBPtr(*blocksNotPendingSnapshot)->address()) << ",";
        blocksNotPendingSnapshot++;
    }
    std::cout << std::endl;
    for (int ii = 0; ii < m_blocksNotPendingSnapshotLoad.size(); ii++) {
        if (m_blocksNotPendingSnapshotLoad[ii]->empty()) {
            continue;
        }
        std::cout << "Bucket " << ii << "(" << static_cast<void*>(m_blocksNotPendingSnapshotLoad[ii].get()) << ") has size " << m_blocksNotPendingSnapshotLoad[ii]->size() << std::endl;
        TBBucketI bucketIter = m_blocksNotPendingSnapshotLoad[ii]->begin();
        while (bucketIter != m_blocksNotPendingSnapshotLoad[ii]->end()) {
            std::cout << "\t" << static_cast<void*>(TBPtr(*bucketIter)->address()) << std::endl;
            bucketIter++;
        }
    }

    boost::unordered_set<TBPtr>::iterator blocksPendingSnapshot = m_blocksPendingSnapshot.begin();
    std::cout << "Blocks pending snapshot: ";
    while (blocksPendingSnapshot != m_blocksPendingSnapshot.end()) {
        std::cout << static_cast<void*>(TBPtr(*blocksPendingSnapshot)->address()) << ",";
        blocksPendingSnapshot++;
    }
    std::cout << std::endl;
    for (int ii = 0; ii < m_blocksPendingSnapshotLoad.size(); ii++) {
        if (m_blocksPendingSnapshotLoad[ii]->empty()) {
            continue;
        }
        std::cout << "Bucket " << ii << "(" << static_cast<void*>(m_blocksPendingSnapshotLoad[ii].get()) << ") has size " << m_blocksPendingSnapshotLoad[ii]->size() << std::endl;
        TBBucketI bucketIter = m_blocksPendingSnapshotLoad[ii]->begin();
        while (bucketIter != m_blocksPendingSnapshotLoad[ii]->end()) {
            std::cout << "\t" << static_cast<void*>(TBPtr(*bucketIter)->address()) << std::endl;
            bucketIter++;
        }
    }
    std::cout << std::endl;
}

int64_t PersistentTable::validatePartitioning(TheHashinator* hashinator, int32_t partitionId) {
    TableIterator iter = iterator();

    int64_t mispartitionedRows = 0;

    TableTuple tuple(schema());
    while (iter.next(tuple)) {
        int32_t newPartitionId = hashinator->hashinate(tuple.getNValue(m_partitionColumn));
        if (newPartitionId != partitionId) {
            std::ostringstream buffer;
            buffer << "@ValidPartitioning found a mispartitioned row (hash: "
                    << m_surgeon.generateTupleHash(tuple)
                    << " should in "<< partitionId
                    << ", but in " << newPartitionId << "):\n"
                    << tuple.debug(name())
                    << std::endl;
            LogManager::getThreadLogger(LOGGERID_HOST)->log(LOGLEVEL_WARN,
                    buffer.str().c_str());
            mispartitionedRows++;
        }
    }
    if (mispartitionedRows > 0) {
        std::ostringstream buffer;
        buffer << "Expected hashinator is "
                << hashinator->debug()
                << std::endl;
        buffer << "Current hashinator is"
                << ExecutorContext::getEngine()->dumpCurrentHashinator()
                << std::endl;
        LogManager::getThreadLogger(LOGGERID_HOST)->log(LOGLEVEL_WARN,
                            buffer.str().c_str());
    }
    return mispartitionedRows;
}

void PersistentTableSurgeon::activateSnapshot() {
    TBMapI blockIterator = m_table.m_data.begin();

    // Persistent table should have minimum of one block in it's block map.
    vassert(m_table.m_data.begin() != m_table.m_data.end());

    if ((m_table.m_data.size() == 1) && blockIterator.data()->isEmpty()) {
        vassert(m_table.activeTupleCount() == 0);
        // The single empty block in an empty table does not need to be considered as pending block
        // for snapshot(load). CopyOnWriteIterator may not and need not expect empty blocks.
        return;
    }

    // All blocks are now pending snapshot
    m_table.m_blocksPendingSnapshot.swap(m_table.m_blocksNotPendingSnapshot);
    m_table.m_blocksPendingSnapshotLoad.swap(m_table.m_blocksNotPendingSnapshotLoad);
    vassert(m_table.m_blocksNotPendingSnapshot.empty());
    for (int ii = 0; ii < m_table.m_blocksNotPendingSnapshotLoad.size(); ii++) {
        vassert(m_table.m_blocksNotPendingSnapshotLoad[ii]->empty());
    }
}

std::pair<TableIndex const*, uint32_t> PersistentTable::getUniqueIndexForDR() {
    // In active-active we always send full tuple instead of just index tuple.
    bool isActiveActive = m_executorContext->getEngine()->getIsActiveActiveDREnabled();
    if (isActiveActive) {
        TableIndex* nullIndex = NULL;
        return std::make_pair(nullIndex, 0);
    }

    if (!m_smallestUniqueIndex && !m_noAvailableUniqueIndex) {
        computeSmallestUniqueIndex();
    }
    return std::make_pair(m_smallestUniqueIndex, m_smallestUniqueIndexCrc);
}

void PersistentTable::computeSmallestUniqueIndex() {
    uint32_t smallestIndexTupleLength = UINT32_MAX;
    m_noAvailableUniqueIndex = true;
    m_smallestUniqueIndex = NULL;
    m_smallestUniqueIndexCrc = 0;
    std::string smallestUniqueIndexName = ""; // use name for determinism
    BOOST_FOREACH (auto index, m_indexes) {
        if (index->isUniqueIndex() && !index->isPartialIndex()) {
            uint32_t indexTupleLength = index->getKeySchema()->tupleLength();
            if (!m_smallestUniqueIndex ||
                (m_smallestUniqueIndex->keyUsesNonInlinedMemory() && !index->keyUsesNonInlinedMemory()) ||
                indexTupleLength < smallestIndexTupleLength ||
                (indexTupleLength == smallestIndexTupleLength && index->getName() < smallestUniqueIndexName)) {
                m_smallestUniqueIndex = index;
                m_noAvailableUniqueIndex = false;
                smallestIndexTupleLength = indexTupleLength;
                smallestUniqueIndexName = index->getName();
            }
        }
    }
    if (m_smallestUniqueIndex) {
        m_smallestUniqueIndexCrc = vdbcrc::crc32cInit();
        m_smallestUniqueIndexCrc = vdbcrc::crc32c(m_smallestUniqueIndexCrc,
                &(m_smallestUniqueIndex->getColumnIndices()[0]),
                m_smallestUniqueIndex->getColumnIndices().size() * sizeof(int));
        m_smallestUniqueIndexCrc = vdbcrc::crc32cFinish(m_smallestUniqueIndexCrc);
    }
}

std::vector<uint64_t> PersistentTable::getBlockAddresses() const {
    std::vector<uint64_t> blockAddresses;
    blockAddresses.reserve(m_data.size());
    for (TBMap::const_iterator i = m_data.begin(); i != m_data.end(); ++i) {
        blockAddresses.push_back((uint64_t)i->second->address());
    }
    return blockAddresses;
}

#ifndef NDEBUG
static bool isExistingTableIndex(std::vector<TableIndex*>& indexes, TableIndex* index) {
    BOOST_FOREACH (auto existingIndex, indexes) {
        if (existingIndex == index) {
            return true;
        }
    }
    return false;
}
#endif

TableIndex* PersistentTable::index(std::string const& name) const {
    BOOST_FOREACH (auto index, m_indexes) {
        if (index->getName().compare(name) == 0) {
            return index;
        }
    }
    std::stringstream errorString;
    errorString << "Could not find Index with name " << name << " among {";
    char const* sep = "";
    BOOST_FOREACH (auto index, m_indexes) {
        errorString << sep << index->getName();
        sep = ", ";
    }
    errorString << "}";
    throwFatalException("%s", errorString.str().c_str());
}

void PersistentTable::addIndex(TableIndex* index) {
    vassert(! isExistingTableIndex(m_indexes, index));

    // fill the index with tuples... potentially the slow bit
    TableTuple tuple(m_schema);
    TableIterator iter = iterator();
    while (iter.next(tuple)) {
        index->addEntry(&tuple, NULL);
    }

    // add the index to the table
    if (index->isUniqueIndex()) {
        m_uniqueIndexes.push_back(index);
    }
    m_indexes.push_back(index);
    m_noAvailableUniqueIndex = false;
    m_smallestUniqueIndex = NULL;
    m_smallestUniqueIndexCrc = 0;
    // Mark view handlers that need to be reconstructed as dirty.
    polluteViews();
}

void PersistentTable::removeIndex(TableIndex* index) {
    vassert(isExistingTableIndex(m_indexes, index));

    std::vector<TableIndex*>::iterator iter;
    for (iter = m_indexes.begin(); iter != m_indexes.end(); iter++) {
        if ((*iter) == index) {
            m_indexes.erase(iter);
            break;
        }
    }
    for (iter = m_uniqueIndexes.begin(); iter != m_uniqueIndexes.end(); iter++) {
        if ((*iter) == index) {
            m_uniqueIndexes.erase(iter);
            break;
        }
    }
    if (m_pkeyIndex == index) {
        m_pkeyIndex = NULL;
    }

    // this should free any memory used by the index
    delete index;
    m_smallestUniqueIndex = NULL;
    m_smallestUniqueIndexCrc = 0;
    // Mark view handlers that need to be reconstructed as dirty.
    polluteViews();
}

void PersistentTable::setPrimaryKeyIndex(TableIndex* index) {
    // for now, no calling on non-empty tables
    vassert(activeTupleCount() == 0);
    vassert(isExistingTableIndex(m_indexes, index));

    m_pkeyIndex = index;
}

void PersistentTable::configureIndexStats() {
    // initialize stats for all the indexes for the table
    BOOST_FOREACH (auto index, m_indexes) {
        index->getIndexStats()->configure(index->getName() + " stats",
                                          name());
    }
}

// Create a delta table attached to this persistent table using exactly the same table schema.
void PersistentTable::instantiateDeltaTable(bool needToCheckMemoryContext) {
    if (m_deltaTable) {
        // Each persistent table can only have exactly one attached delta table.
        return;
    }
    VoltDBEngine* engine = ExecutorContext::getEngine();
    // When adding view handlers from partitioned tables to replicated source tables, all partitions race to
    // add the delta table for the replicated table. Therefore, it is likely that the first to add the delta
    // table is not the lowest site. All add Views are done holding a global mutex so structure management is
    // safe. However when the replicated table is deallocated it also deallocates the delta table so the memory
    // allocation of the delta table needs to be done in the lowest site thread's context.
    vassert(m_deltaTable == NULL);
    VOLT_TRACE("%s to check the memory context to use.\n", needToCheckMemoryContext?"Need":"No need");
    ConditionalExecuteWithMpMemory usingMpMemoryIfReplicated(m_isReplicated && needToCheckMemoryContext);
    TableCatalogDelegate* tcd = engine->getTableDelegate(m_name);
    m_deltaTable = tcd->createDeltaTable(*engine->getDatabase(), *engine->getCatalogTable(m_name));
    VOLT_DEBUG("Engine %p (%d) create delta table %p for table %s", engine,
               engine->getPartitionId(), m_deltaTable, m_name.c_str());
}

void PersistentTable::releaseDeltaTable(bool needToCheckMemoryContext) {
    if (! m_deltaTable) {
        return;
    }
    VOLT_DEBUG("Engine %d drop delta table %p for table %s",
               ExecutorContext::getEngine()->getPartitionId(), m_deltaTable, m_name.c_str());
    VOLT_TRACE("%s to check the memory context to use.\n", needToCheckMemoryContext?"Need":"No need");
    ConditionalExecuteWithMpMemory usingMpMemoryIfReplicated(m_isReplicated && needToCheckMemoryContext);
    // If both the source and dest tables are replicated we are already in the Mp Memory Context
    m_deltaTable->decrementRefcount();
    m_deltaTable = NULL;
}

void PersistentTable::addViewHandler(MaterializedViewHandler* viewHandler) {
    if (m_viewHandlers.size() == 0) {
        instantiateDeltaTable();
    }
    m_viewHandlers.push_back(viewHandler);
}

void PersistentTable::dropViewHandler(MaterializedViewHandler* viewHandler) {
    vassert( ! m_viewHandlers.empty());
    MaterializedViewHandler* lastHandler = m_viewHandlers.back();
    if (viewHandler != lastHandler) {
        // iterator to vector element:
        std::vector<MaterializedViewHandler*>::iterator it = find(m_viewHandlers.begin(),
                                                                  m_viewHandlers.end(),
                                                                  viewHandler);
        vassert(it != m_viewHandlers.end());
        // Use the last view to patch the potential hole.
        *it = lastHandler;
    }
    // The last element is now excess.
    m_viewHandlers.pop_back();
    if (m_viewHandlers.size() == 0) {
        releaseDeltaTable();
    }
}

void PersistentTable::polluteViews() {
    //   This method will be called every time when an index is added or dropped
    // from the table to update the joined table view handler properly.
    //   If the current table is a view source table, adding / dropping an index
    // may change the plan to refresh the view, therefore all the handlers that
    // use the current table as one of their sources need to be updated.
    //   If the current table is a view target table, we need to refresh the list
    // of tracked indices so that the data in the table and its indices can be in sync.
    BOOST_FOREACH (auto mvHandler, m_viewHandlers) {
        mvHandler->pollute();
    }
    if (m_mvHandler) {
        m_mvHandler->pollute();
    }
}

void PersistentTable::migratingAdd(int64_t txnId, TableTuple& tuple) {
    vassert(isTableWithMigrate(m_tableType) && m_shadowStream != nullptr);
    MigratingRows::iterator it = m_migratingRows.lower_bound(txnId);
    if (it == m_migratingRows.end() || it->first != txnId) {
        // txnId not allocated yet
        it = m_migratingRows.emplace_hint(it, txnId, MigratingBatch());
    }
    void* addr = tuple.address();
    __attribute__((unused)) auto const success =
        it->second.insert(addr);
    vassert(success.second);
    VOLT_DEBUG("Added migrating txnId %lld for address %p", txnId, addr);
};

bool PersistentTable::migratingRemove(int64_t txnId, TableTuple& tuple) {
    vassert(isTableWithMigrate(m_tableType) && m_shadowStream != nullptr);
    MigratingRows::iterator it = m_migratingRows.find(txnId);
    if (it == m_migratingRows.end()) {
        // Migrating views may be called to remove a migrating flag that
        // hasn't been set yet
        vassert(m_isMaterialized);
        return false;
    }

    size_t found = it->second.erase(tuple.address());
    if (it->second.empty()) {
        m_migratingRows.erase(it);
    }

    vassert(found == 1);
    VOLT_DEBUG("Removed migrating txnId %lld for address %p", txnId, tuple.address());
    return found == 1;
}

uint16_t PersistentTable::getMigrateColumnIndex() {
    return m_schema->getHiddenColumnIndex(HiddenColumn::MIGRATE_TXN);
}

bool PersistentTable::deleteMigratedRows(int64_t deletableTxnId) {
   if (!isTableWithMigrate(m_tableType) || m_migratingRows.size() == 0) {
       return false;
   }
   vassert(m_shadowStream != nullptr);
   MigratingRows::iterator currIt = m_migratingRows.begin();
   if (currIt == m_migratingRows.end() || currIt->first > deletableTxnId) {
       return false;
   }
   TableTuple targetTuple(m_schema);
   MigratingBatch& batch = currIt->second;
   // Delete the first batch which has a sphandle <= deletableTxnId. Other batches with sphandle <= deletableTxnId
   // will be deleted next round, one batch at a time
   BOOST_FOREACH (auto toDelete, batch) {
      targetTuple.move(toDelete);
      VOLT_DEBUG("Deleting migrated tuple for txnId %lld hidden txnId %lld, address %p",
              currIt->first, ValuePeeker::peekBigInt(targetTuple.getHiddenNValue(getMigrateColumnIndex())),
              targetTuple.address());
      vassert(ValuePeeker::peekBigInt(targetTuple.getHiddenNValue(getMigrateColumnIndex())) == currIt->first);
      deleteTuple(targetTuple, true, false);
   }
   currIt = m_migratingRows.erase(currIt);
   if (currIt == m_migratingRows.end() || currIt->first > deletableTxnId) {
       return false;
   }
   VOLT_DEBUG("Migrated rows deleted. table %s, batch: %ld, target sphandle: %lld, batch remaining: %ld",
        name().c_str(),batch.size(), deletableTxnId, m_migratingRows.size());
   return true;
}

} // namespace voltdb
