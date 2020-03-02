/* This file is part of VoltDB.
 * Copyright (C) 2008-2020 VoltDB Inc.
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
#include <map>
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
                                 int tupleLimit,
                                 bool drEnabled,
                                 bool isReplicated,
                                 TableType tableType)
    : Table(tableAllocationTargetSize == 0 ? TABLE_BLOCKSIZE : tableAllocationTargetSize)
    , m_isMaterialized(isMaterialized)   // Other constructors are dependent on this one
    , m_isReplicated(isReplicated)
    , m_allowNulls()
    , m_partitionColumn(partitionColumn)
    , m_tupleLimit(tupleLimit)
    , m_tuplesPerChunk(0)
    , m_purgeExecutorVector()
    , m_views()
    , m_stats(this)
    , m_tableStreamer()
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
    , m_releaseBatch()
{
    ::memcpy(&m_signature, signature, 20);
}

void PersistentTable::initializeWithColumns(TupleSchema* schema,
                                            std::vector<std::string> const& columnNames,
                                            bool ownsTupleSchema) {
    vassert(schema != NULL);

    Table::initializeWithColumns(schema, columnNames, ownsTupleSchema);

    // TODO: Pass m_tableAllocationSize as template parameter to m_dataStorage and then
    //       uncomment this block remove the block below
//#ifdef MEMCHECK
//    m_tableAllocationSize = m_tupleLength;
//    m_tuplesPerChunk = 1;
//#else
//    m_tuplesPerChunk = m_tableAllocationTargetSize / m_tupleLength;
//    if (m_tuplesPerChunk < 1) {
//        m_tuplesPerChunk = 1;
//        m_tableAllocationSize = m_tupleLength;
//    } else {
//        m_tableAllocationSize = m_tableAllocationTargetSize;
//    }
//#endif

    // NOTE: we embed m_data pointer immediately after the
    // boundary of TableTuple, so that there is no extra Pool
    // that stores non-inlined tuple data.
    size_t tupleSize = schema->tupleLength() + TUPLE_HEADER_SIZE;
    m_dataStorage.reset(new Alloc{tupleSize});
    m_allowNulls.resize(m_columnCount);
    for (int i = m_columnCount - 1; i >= 0; --i) {
        TupleSchema::ColumnInfo const* columnInfo = m_schema->getColumnInfo(i);
        m_allowNulls[i] = columnInfo->allowNull;
    }

#ifdef MEMCHECK
    vassert(false); // see todo above.
#else
    m_tableAllocationSize = m_dataStorage->chunkSize();
    m_tuplesPerChunk = m_tableAllocationSize / m_tupleLength;
#endif
}

PersistentTable::~PersistentTable() {
    VOLT_DEBUG("Deleting TABLE %s as %s", m_name.c_str(), m_isReplicated?"REPLICATED":"PARTITIONED");

    // delete all tuples to free strings
    storage::for_each<PersistentTable::txn_iterator>(allocator(),[this](void* p) {
        void *tupleAddress = const_cast<void*>(reinterpret_cast<void const *>(p));
        TableTuple tuple(this->schema());
        tuple.move(tupleAddress);
        tuple.freeObjectColumns();
        tuple.setActiveFalse();
    });

    // note this class has ownership of the views, even if they
    // were allocated by VoltDBEngine
    BOOST_FOREACH (auto view, m_views) {
        delete view;
    }

    // clean up indexes
    BOOST_FOREACH (auto index, m_indexes) {
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
void PersistentTable::drLogTruncate(ExecutorContext* ec, bool fallible) {
    AbstractDRTupleStream* drStream = getDRTupleStream(ec);
    if (doDRActions(drStream)) {
        int64_t currentSpHandle = ec->currentSpHandle();
        int64_t currentUniqueId = ec->currentUniqueId();
        size_t drMark = drStream->truncateTable(m_signature, m_name, m_partitionColumn,
                currentSpHandle, currentUniqueId);

        UndoQuantum* uq = ec->getCurrentUndoQuantum();
        if (uq && fallible) {
            uq->registerUndoAction(
                    new (*uq) DRTupleStreamUndoAction(drStream, drMark, rowCostForDRRecord(DR_RECORD_TRUNCATE_TABLE)));
        }
    }
}

void PersistentTable::deleteAllTuples(bool, bool fallible) {
    // Instead of recording each tuple deletion, log it as a table truncation DR.
    ExecutorContext* ec = ExecutorContext::getExecutorContext();
    drLogTruncate(ec, fallible);

    // Temporarily disable DR binary logging so that it doesn't record the
    // individual deletions below.
    DRTupleStreamDisableGuard drGuard(ec, false);

    storage::for_each<PersistentTable::txn_iterator>(allocator(),
                  [this,fallible](void* p) {
        void *tupleAddress = const_cast<void*>(reinterpret_cast<void const *>(p));
        TableTuple inputTuple(this->schema());
        inputTuple.move(tupleAddress);
        if (!inputTuple.isPendingDeleteOnUndoRelease()) {
            this->deleteTuple(inputTuple, fallible);
        }
    });
}

bool PersistentTable::doDRActions(AbstractDRTupleStream* drStream) {
    return m_drEnabled && drStream && drStream->drStreamStarted();
}

void PersistentTable::truncateTableUndo(TableCatalogDelegate* tcd,
        PersistentTable* originalTable, bool replicatedTableAction) {
    VOLT_DEBUG("**** Truncate table undo *****\n");

    if (originalTable->m_tableStreamer != NULL) {
        // Elastic Index may complete when undo Truncate
        unsetTableForStreamIndexing();
    }

    VoltDBEngine* engine = ExecutorContext::getEngine();
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
    decrementRefcount();

    // reset base table pointer
    tcd->setTable(originalTable);

    engine->rebuildTableCollections(replicatedTableAction, false);
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

void PersistentTable::truncateTable(VoltDBEngine* engine, bool replicatedTable, bool fallible) {
    if (isPersistentTableEmpty()) {
        // Always log the the truncate if dr is enabled, see ENG-14528.
        drLogTruncate(ExecutorContext::getExecutorContext(), fallible);
        return;
    }

    // For a materialized view don't optimize truncate,
    // this needs more work - ENG-10323.
    // A catalog may not be present for tests, go through this path
    if (m_isMaterialized || engine->getCatalog() == nullptr) {
        /* // enable to debug
        std::cout << "DEBUG: truncating view table (retail) "
                  << activeTupleCount()
                  << " tuples in " << name() << std::endl;
        // */
        deleteAllTuples(true, fallible);
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
        deleteAllTuples(true, fallible);
        return;
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
            newHandler->catchUpWithExistingData(fallible);
        }
    }

    // If there is a purge fragment on the old table, pass it on to the new one
    if (hasPurgeFragment()) {
        vassert(! emptyTable->hasPurgeFragment());
        boost::shared_ptr<ExecutorVector> evPtr = getPurgeExecutorVector();
        emptyTable->swapPurgeExecutorVector(evPtr);
    }

    engine->rebuildTableCollections(replicatedTable, false);

    ExecutorContext* ec = ExecutorContext::getExecutorContext();
    drLogTruncate(ec, fallible);

    UndoQuantum* uq = ec->getCurrentUndoQuantum();
    if (uq) {
        if (!fallible) {
            throwFatalException("Attempted to truncate table %s when there was an "
                                "active undo quantum, and presumably an active transaction that should be there",
                                m_name.c_str());
        }
        emptyTable->m_tuplesPinnedByUndo = emptyTable->activeTupleCount();
        emptyTable->m_invisibleTuplesPendingDeleteCount = emptyTable->activeTupleCount();
        // Create and register an undo action.
        UndoReleaseAction* undoAction = new (*uq) PersistentTableUndoTruncateTableAction(tcd, this, emptyTable, replicatedTable);
        SynchronizedThreadLock::addTruncateUndoAction(isReplicatedTable(), uq, undoAction, this);
    }
    else {
        if (fallible) {
            throwFatalException("Attempted to truncate table %s when there was no "
                                "active undo quantum even though one was expected", m_name.c_str());
        }

        //Skip the undo log and "commit" immediately by asking the new emptyTable to perform
        //the truncate table release work rather then having it invoked by PersistentTableUndoTruncateTableAction
        emptyTable->truncateTableRelease(this);
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

#ifdef NDEBUG
static bool hasNameIntegrity(std::string const& tableName, std::vector<std::string> const& indexNames) {
    return true;
}
#else
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

void PersistentTable::insertTupleIntoDeltaTable(TableTuple const& source, bool fallible) {
    // If the current table does not have a delta table, return.
    // If the current table has a delta table, but it is used by
    // a single table view during snapshot restore process, return.
    if (! m_deltaTable || m_mvTrigger) {
        return;
    }

    // If the delta table has data in it, delete the data first.
    if (!m_deltaTable->isPersistentTableEmpty()) {
       storage::for_each<PersistentTable::txn_iterator>(dynamic_cast<PersistentTable*>(m_deltaTable)->allocator(),
                      [this, fallible](void* p) {
             void *tupleAddress = const_cast<void*>(reinterpret_cast<void const *>(p));
             TableTuple inputTuple(this->schema());
             inputTuple.move(tupleAddress);
             m_deltaTable->deleteTuple(inputTuple, fallible);
        });
    }

    TableTuple targetForDelta = m_deltaTable->createTuple(source);
    try {
        m_deltaTable->insertTupleCommon(source, targetForDelta, fallible);
    } catch (ConstraintFailureException const& e) {
        m_deltaTable->deleteTailTupleStorage(targetForDelta);
        throw;
    } catch (TupleStreamException const& e) {
        m_deltaTable->deleteTailTupleStorage(targetForDelta);
        throw;

    }
    // TODO: we do not catch other types of exceptions, such as
    // SQLException, etc. The assumption we held that no other
    // exceptions should be thrown in the try-block is pretty
    // daring and likely not correct.
}

TableTuple PersistentTable::createTuple(TableTuple const &source){
    TableTuple target(m_schema);
    void *address = const_cast<void*>(reinterpret_cast<void const *> (allocator().allocate()));
    target.move(address);
    target.copyForPersistentInsert(source);
    return target;
}

void PersistentTable::finalizeDelete() {

     TableTuple target(m_schema);
     BOOST_FOREACH (auto toDelete, m_releaseBatch) {
         target.move(toDelete);
         if (m_tableStreamer != NULL) {
              m_tableStreamer->notifyTupleDelete(target);
         }
         if (m_schema->getUninlinedObjectColumnCount() != 0) {
              decreaseStringMemCount(target.getNonInlinedMemorySizeForPersistentTable());
              target.freeObjectColumns();
         }
     }

    m_invisibleTuplesPendingDeleteCount -= m_releaseBatch.size();

    allocator().remove(m_releaseBatch, [this, &target](map<void*, void*> const& tuples) {
        TableTuple origin(m_schema);
        for(auto const& p : tuples) {
            target.move(p.first);
            origin.move(p.second);
            swapTuples(origin, target);
         }
    });
    m_releaseBatch.clear();
}

/*
 * Regular tuple insertion that does an allocation and copy for
 * uninlined strings and creates and registers an UndoAction.
 */
bool PersistentTable::insertTuple(TableTuple const& source) {
    insertPersistentTuple(source, true);
    return true;
}

void PersistentTable::insertPersistentTuple(TableTuple const& source, bool fallible, bool ignoreTupleLimit) {
    if (!ignoreTupleLimit && fallible && visibleTupleCount() >= m_tupleLimit) {
        std::ostringstream str;
        str << "Table " << m_name << " exceeds table maximum row count " << m_tupleLimit;
        throw ConstraintFailureException(this, source, str.str());
    }

    TableTuple target = createTuple(source);
    try {
        insertTupleCommon(source, target, fallible);
    } catch (TupleStreamException const& e) {
        deleteTailTupleStorage(target); // also frees object columns
        throw;
    } catch (ConstraintFailureException const& e) {
        deleteTailTupleStorage(target); // also frees object columns
        throw;
    }
}

void PersistentTable::doInsertTupleCommon(TableTuple const& source, TableTuple& target,
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

    ExecutorContext* ec = ExecutorContext::getExecutorContext();
    AbstractDRTupleStream* drStream = getDRTupleStream(ec);
    if (doDRActions(drStream) && shouldDRStream) {
        ExecutorContext* ec = ExecutorContext::getExecutorContext();
        int64_t currentSpHandle = ec->currentSpHandle();
        int64_t currentUniqueId = ec->currentUniqueId();
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

    TableTuple conflict(m_schema);
    try {
        tryInsertOnAllIndexes(&target, &conflict);    // Also evaluates if the index update might throw
    } catch (std::exception const& e) {
        deleteTailTupleStorage(target); // also frees object columns
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

    // add it to migrating index when loading tuple from a recover or rejoin snapshot (only)
     if (isTableWithMigrate(m_tableType)) {
         vassert(m_shadowStream != nullptr);
         NValue txnId = target.getHiddenNValue(getMigrateColumnIndex());
         if(!txnId.isNull()){
            migratingAdd(ValuePeeker::peekBigInt(txnId), target);
         }
     }

    // this is skipped for inserts that are never expected to fail,
    // like some (initially, all) cases of tuple migration on schema change
    if (fallible) {
        /*
         * Create and register an undo action.
         */
        UndoQuantum *uq = ExecutorContext::currentUndoQuantum();
        if (uq) {
            UndoReleaseAction* undoAction = createInstanceFromPool<PersistentTableUndoInsertAction>(
                  *uq->getPool(), target.address(), &m_surgeon);
            SynchronizedThreadLock::addUndoAction(isReplicatedTable(), uq, undoAction);
            if (isTableWithExportInserts(m_tableType)) {
                vassert(m_shadowStream != nullptr);

                // insert to partitioned table or partition id 0 for replicated
                if (!isReplicatedTable() || ec->getPartitionId() == 0) {
                     m_shadowStream->streamTuple(target, ExportTupleStream::STREAM_ROW_TYPE::INSERT);
                }
            }
        }
    }

    // Insert the tuple into the delta table first.
    //
    // (Note: we may hit a NOT NULL constraint violation, or any
    // types of constraint violation. In which case, we want to
    // clean up by calling deleteTailTupleStorage, below)
    insertTupleIntoDeltaTable(source, fallible);
}

void PersistentTable::insertTupleCommon(TableTuple const& source, TableTuple& target,
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
        // It's probably safer NOT to do too much cleanup -- such as trying to call deleteTailTupleStorage --
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
    ExecutorContext* ec = ExecutorContext::getExecutorContext();

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
           if (!isReplicatedTable() || ec->getPartitionId() == 0) {
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
       NValue txnId = sourceTupleWithNewValues.getHiddenNValue(migrateColumnIndex);
       if (txnId.isNull()) {
           if (fromMigrate) {
               int64_t spHandle = ec->currentSpHandle();
               sourceTupleWithNewValues.setHiddenNValue(migrateColumnIndex, ValueFactory::getBigIntValue(spHandle));
           }
       } else {
           sourceTupleWithNewValues.setHiddenNValue(migrateColumnIndex, NValue::getNullValue(ValueType::tBIGINT));
           migratingRemove(ValuePeeker::peekBigInt(txnId), targetTupleToUpdate);
       }
    }

    AbstractDRTupleStream* drStream = getDRTupleStream(ec);
    if (!fromMigrate && doDRActions(drStream)) {
        int64_t currentSpHandle = ec->currentSpHandle();
        int64_t currentUniqueId = ec->currentUniqueId();
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
    allocator().update(targetTupleToUpdate.address());
    targetTupleToUpdate.copyForPersistentUpdate(sourceTupleWithNewValues, oldObjects, newObjects);

    if (fromMigrate) {
        vassert(isTableWithMigrate(m_tableType) && m_shadowStream != nullptr);
        migratingAdd(ec->currentSpHandle(), targetTupleToUpdate);
        // add to shadow stream if the table is partitioned or partition 0 for replicated table
        if (!isReplicatedTable() || ec->getPartitionId() == 0) {
            m_shadowStream->streamTuple(sourceTupleWithNewValues, ExportTupleStream::MIGRATE, doDRActions(drStream) ? drStream : NULL);
        }
    }

    if (uq) {
        /*
         * Create and register an undo action with copies of the "before" and "after" tuple storage
         * and the "before" and "after" object pointers for non-inlined columns that changed.
         */
        UndoReleaseAction* undoAction = createInstanceFromPool<PersistentTableUndoUpdateAction>(
              *uq->getPool(), oldTupleData, targetTupleToUpdate.address(), oldObjects, newObjects, &m_surgeon, someIndexGotUpdated, fromMigrate);
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

    TableTuple targetTupleToUpdate(tupleWithUnwantedValues, m_schema);
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
        migratingRemove(ExecutorContext::getExecutorContext()->currentSpHandle(), targetTupleToUpdate);
    } else if (isTableWithMigrate(m_tableType)) {
        NValue const txnId = targetTupleToUpdate.getHiddenNValue(getMigrateColumnIndex());
        if(!txnId.isNull()){
            migratingAdd(ValuePeeker::peekBigInt(txnId), targetTupleToUpdate);
        }
    }
    //TO DO: undo update
    allocator().update(targetTupleToUpdate.address());
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
    ExecutorContext* ec = ExecutorContext::getExecutorContext();
    AbstractDRTupleStream* drStream = getDRTupleStream(ec);
    if (doDRActions(drStream)) {
        int64_t currentSpHandle = ec->currentSpHandle();
        int64_t currentUniqueId = ec->currentUniqueId();
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
            migratingRemove(ValuePeeker::peekBigInt(txnId), target);
        }
    }
    if (createUndoAction) {
        target.setPendingDeleteOnUndoReleaseTrue();
        ++m_tuplesPinnedByUndo;
        ++m_invisibleTuplesPendingDeleteCount;
        UndoReleaseAction* undoAction = createInstanceFromPool<PersistentTableUndoDeleteAction>(
              *uq->getPool(), target.address(), this);
        SynchronizedThreadLock::addUndoAction(isReplicatedTable(), uq, undoAction, this);
        if (isTableWithExportDeletes(m_tableType)) {
            vassert(m_shadowStream != nullptr);
            if (!isReplicatedTable() || ec->getPartitionId() == 0) {
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
    // For replicated table
    // delete the tuple directly but preserve the deleted tuples to tempTable for cowIterator
    // the same way as Update

    // A snapshot (background scan) in progress can still cause a hold-up.
    // notifyTupleDelete() defaults to returning true for all context types
    // other than CopyOnWriteContext.
    if (m_tableStreamer != NULL) {
         m_tableStreamer->notifyTupleDelete(target);
    }

    // No snapshot in progress cares, just whack it.
    deleteTupleStorage(target); // also frees object columns

    MigratingBatch batch{};
    batch.insert(target.address());
    allocator().remove(batch,[this](map<void*, void*> const& tuples) {
        TableTuple target(m_schema);
        TableTuple origin(m_schema);
        for(auto const& p : tuples) {
            target.move(p.first);
            origin.move(p.second);
            swapTuples(origin, target);
        }
   });
}


/**
 * This entry point is triggered by the successful release of an UndoDeleteAction.
 */
void PersistentTable::deleteTupleRelease(char* tuple) {
    TableTuple target(m_schema);
    target.move(tuple);
    target.setPendingDeleteOnUndoReleaseFalse();
    if (m_tableStreamer != NULL && ! m_tableStreamer->notifyTupleDelete(target)) {
        // Mark it pending delete and let the snapshot land the finishing blow.
        if (!target.isPendingDelete()) {
            ++m_invisibleTuplesPendingDeleteCount;
            target.setPendingDeleteTrue();
        }
    }
    m_releaseBatch.insert(tuple);
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
    TableTuple target(tupleData, m_schema);
    if (target.isNullTuple()) {
        throwFatalException("Failed to delete tuple from table %s: tuple does not exist\n%s\n", m_name.c_str(),
           target.debugNoHeader().c_str());
    }
    // Make sure that they are not trying to delete the same tuple twice
    vassert(target.isActive());
    deleteFromAllIndexes(&target);

    // The inserted tuple could have been migrated from stream snapshot/rejoin, undo the migrating indexes
    if (isTableWithMigrate(m_tableType)) {
        NValue txnId = target.getHiddenNValue(getMigrateColumnIndex());
        if(!txnId.isNull()){
            migratingRemove(ValuePeeker::peekBigInt(txnId), target);
        }
    }

    // A snapshot (background scan) in progress can still cause a hold-up.
    // notifyTupleDelete() defaults to returning true for all context types
    // other than CopyOnWriteContext.
    if (m_tableStreamer != NULL) {
         m_tableStreamer->notifyTupleDelete(target);
    }

    deleteTailTupleStorage(target); // also frees object columns
}

TableTuple PersistentTable::lookupTuple(TableTuple tuple, LookupType lookupType) {
    if (m_pkeyIndex) {
        return m_pkeyIndex->uniqueMatchingTuple(tuple);
    }
    size_t tuple_length;
    if (lookupType == LOOKUP_BY_VALUES && this->schema()->hiddenColumnCount() > 0) {
        // Looking up a tuple by values should not include any internal
        // hidden column values, which are appended to the end of the
        // tuple.
        tuple_length = this->schema()->offsetOfHiddenColumns();
     } else {
        tuple_length = this->schema()->tupleLength();
     }

    TableTuple matchedTuple(m_schema);
    storage::until<PersistentTable::txn_iterator>(allocator(), [&tuple, &lookupType, this, tuple_length, &matchedTuple](void* p) {
        void *tupleAddress = const_cast<void*>(reinterpret_cast<void const *>(p));
        TableTuple tableTuple(this->schema());
        tableTuple.move(tupleAddress);
        if (!tableTuple.isPendingDeleteOnUndoRelease()) {
            bool matched = false;
            if (lookupType == LOOKUP_FOR_DR && this->schema()->hiddenColumnCount()) {
                // Force column compare for DR so we can easily use the filter
                HiddenColumnFilter filter = HiddenColumnFilter::create(HiddenColumnFilter::EXCLUDE_MIGRATE, this->schema());
                matched = (tableTuple.equalsNoSchemaCheck(tuple, &filter));
            } else if (lookupType != LOOKUP_FOR_UNDO && this->schema()->getUninlinedObjectColumnCount() != 0) {
                matched = (tableTuple.equalsNoSchemaCheck(tuple));
            } else {
                // Do an inline tuple byte comparison
                // to avoid matching duplicate tuples with different pointers to Object storage
                // -- which would cause erroneous releases of the wrong Object storage copy.
                char* tableTupleData = tableTuple.address() + TUPLE_HEADER_SIZE;
                char* tupleData = tuple.address() + TUPLE_HEADER_SIZE;
                matched = (::memcmp(tableTupleData, tupleData, tuple_length) == 0);
            }
            if (matched) {
                matchedTuple.move(tableTuple.address());
                return true;
            }
        }
        return false;
     });
     return matchedTuple;
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

/*
 * claim ownership of a view. table is responsible for this view*
 */
void PersistentTable::addMaterializedView(MaterializedViewTriggerForWrite* view) {
    m_views.push_back(view);
}

/*
 * drop a view. the table is no longer feeding it.
 * The destination table will go away when the view metadata is deleted (or later?) as its refcount goes to 0.
 */
void PersistentTable::dropMaterializedView(MaterializedViewTriggerForWrite* targetView) {
    vassert( ! m_views.empty());
    MaterializedViewTriggerForWrite* lastView = m_views.back();
    if (targetView != lastView) {
        // iterator to vector element:
        std::vector<MaterializedViewTriggerForWrite*>::iterator toView = find(m_views.begin(), m_views.end(), targetView);
        vassert(toView != m_views.end());
        // Use the last view to patch the potential hole.
        *toView = lastView;
    }
    // The last element is now excess.
    m_views.pop_back();
    delete targetView;
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
    std::string infoSpacer = spacer + "  |";

    buffer << infoSpacer << tableType() << "(" << name() << "):\n";
    buffer << infoSpacer << "\tNumber of Columns: " << columnCount() << "\n";
    buffer << infoSpacer << "===========================================================\n";
    buffer << infoSpacer << "\tCOLUMNS\n";
    buffer << infoSpacer << m_schema->debug();
        //buffer << infoSpacer << " - TupleSchema needs a \"debug\" method. Add one for output here.\n";
    if (m_shadowStream != nullptr) {
        buffer << infoSpacer << "\tSHADOW STREAM: " << m_shadowStream->debug("") << "\n";
    }
#ifdef VOLT_TRACE_ENABLED
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
    TableTuple tuple(schema());
    TableIterator it = const_cast<PersistentTable *>(this)->iterator();
    while (it.next(tuple)) {
        buffer << tuple.debug("");
        buffer << "\n";
    }

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
        message << debug("") << std::endl;
        message << "The following columns are given:" << std::endl;
        for (int i = 0; i < colcount; i++) {
            message << "column " << i << ": " << names[i]
                    << ", type = " << getTypeName(types[i]) << std::endl;
        }
        throw SerializableEEException(message.str().c_str());
    }

    int tupleCount = serialInput.readInt();
    vassert(tupleCount >= 0);

    //Reserve space for a length prefix for rows that violate unique constraints
    //If there is no output supplied it will just throw
    size_t lengthPosition = 0;
    int32_t serializedTupleCount = 0;
    size_t tupleCountPosition = 0;
    if (uniqueViolationOutput != NULL) {
        lengthPosition = uniqueViolationOutput->reserveBytes(4);
    }

    for (int i = 0; i < tupleCount; ++i) {
        try {
           m_tempTuple.deserializeFrom(serialInput, stringPool, caller);
        } catch (SQLException &e) {
            throw;
        }

        TableTuple target = createTuple(m_tempTuple);
        // TODO: we do not catch other types of exceptions, such as
        // SQLException, etc. The assumption we held that no other
        // exceptions should be thrown in the try-block is pretty
        // daring and likely not correct.
        processLoadedTuple(target, uniqueViolationOutput, serializedTupleCount, tupleCountPosition,
                           caller.shouldDrStream(), caller.ignoreTupleLimit());
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
                                         bool shouldDRStreamRows,
                                         bool ignoreTupleLimit) {
    try {
        if (!ignoreTupleLimit && visibleTupleCount() >= m_tupleLimit) {
            std::ostringstream str;
            str << "Table " << m_name << " exceeds table maximum row count " << m_tupleLimit;
            throw ConstraintFailureException(this, tuple, str.str(), (! uniqueViolationOutput) ? &m_surgeon : NULL);
        }
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
        deleteTailTupleStorage(tuple);
    } catch (TupleStreamException& e) {
        deleteTailTupleStorage(tuple);
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

    storage::for_each<PersistentTable::txn_iterator>(allocator(),
                        [this, &pkeyIndex](void* p) {
         void *tupleAddress = const_cast<void*>(reinterpret_cast<void const *>(p));
         TableTuple inputTuple(this->schema());
         inputTuple.move(tupleAddress);
         pkeyIndex->addEntry(&inputTuple, NULL);
    });

    IndexCursor indexCursor(pkeyIndex->getTupleSchema());
    pkeyIndex->moveToEnd(true, indexCursor);

    size_t hashCode = 0;
    while (true) {
         TableTuple tuple = pkeyIndex->nextValue(indexCursor);
         if (tuple.isNullTuple()) {
             break;
         }
         tuple.hashCode(hashCode);
    }
    return hashCode;
}

void PersistentTable::swapTuples(TableTuple& originalTuple,
                                 TableTuple& destinationTuple) {
    vassert(!originalTuple.isPendingDeleteOnUndoRelease());

    BOOST_FOREACH (auto index, m_indexes) {
        index->replaceEntryNoKeyChange(destinationTuple, originalTuple);
    }
    if (isTableWithMigrate(m_tableType)) {
        uint16_t migrateColumnIndex = getMigrateColumnIndex();
        NValue txnId = originalTuple.getHiddenNValue(migrateColumnIndex);
        if (!txnId.isNull()) {
            migratingSwap(ValuePeeker::peekBigInt(txnId), originalTuple, destinationTuple);
       }
    }
    if (m_tableStreamer != NULL) {
        m_tableStreamer->notifyTupleMovement(originalTuple, destinationTuple);
    }
}

int64_t PersistentTable::validatePartitioning(TheHashinator* hashinator, int32_t partitionId) {
    int64_t mispartitionedRows = 0;
    storage::for_each<PersistentTable::txn_iterator>(allocator(),
                           [this, &hashinator, &mispartitionedRows, partitionId](void* p) {
       void *tupleAddress = const_cast<void*>(reinterpret_cast<void const *>(p));
       TableTuple tuple(this->schema());
       tuple.move(tupleAddress);
       int32_t newPartitionId = hashinator->hashinate(tuple.getNValue(m_partitionColumn));
       if (newPartitionId != partitionId) {
            std::ostringstream buffer;
            buffer << "@ValidPartitioning found a mispartitioned row (hash: "
                   << m_surgeon.generateTupleHash(tuple)
                   << " should in "<< partitionId
                   << ", but in " << newPartitionId << "):\n"
                   << tuple.debug(name())
                   << std::endl;
            LogManager::getThreadLogger(LOGGERID_HOST)->log(LOGLEVEL_WARN, buffer.str().c_str());
            mispartitionedRows++;
       }
    });

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

void PersistentTable::activateSnapshot(TableStreamType streamType) {
   if (streamType == TABLE_STREAM_SNAPSHOT) {
       vassert(m_snapIt.get() == nullptr);
       m_snapIt = allocator().template freeze<storage::truth>();
   } else if (streamType == TABLE_STREAM_ELASTIC_INDEX) {
       m_elasticIt = std::make_shared<ElasticIndexIterator>(allocator());
   }
}

bool PersistentTable::nextSnapshotTuple(TableTuple& tuple, TableStreamType streamType) {
    if (streamType == TABLE_STREAM_SNAPSHOT) {
       if (m_snapIt->drained()) {
          allocator().thaw();
          m_snapIt.reset();
          return false;
       }
       auto *p = **m_snapIt;
       tuple.move(const_cast<void*>(reinterpret_cast<const void*>(p)));
       ++*m_snapIt;
    } else if (streamType == TABLE_STREAM_ELASTIC_INDEX) {
       if (m_elasticIt->drained()) {
         m_elasticIt.reset();
         return false;
       }
       auto *p = **m_elasticIt;
       tuple.move(const_cast<void*>(reinterpret_cast<const void*>(p)));
       ++*m_elasticIt;
    }
    return true;
}

std::pair<TableIndex const*, uint32_t> PersistentTable::getUniqueIndexForDR() {
    // In active-active we always send full tuple instead of just index tuple.
    bool isActiveActive = ExecutorContext::getExecutorContext()->getEngine()->getIsActiveActiveDREnabled();
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
    throwFatalException("getBlockAddresses not applicable for PersistentTable");
}

#ifdef NDEBUG
static bool isExistingTableIndex(std::vector<TableIndex*>&, TableIndex*) {
    return false;
}
#else
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
    vassert(!isExistingTableIndex(m_indexes, index));

    storage::for_each<PersistentTable::txn_iterator>(allocator(),
                         [this, &index](void* p) {
          void *tupleAddress = const_cast<void*>(reinterpret_cast<void const *>(p));
          TableTuple inputTuple(this->schema());
          inputTuple.move(tupleAddress);
          index->addEntry(&inputTuple, NULL);
     });

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
    auto const success = it->second.insert(addr);
    vassert(success.second);
};

bool PersistentTable::migratingRemove(int64_t txnId, TableTuple& tuple) {
    vassert(isTableWithMigrate(m_tableType) && m_shadowStream != nullptr);
    MigratingRows::iterator it = m_migratingRows.find(txnId);
    if (it == m_migratingRows.end()) {
        vassert(false);
        return false;
    }

    size_t found = it->second.erase(tuple.address());
    if (it->second.empty()) {
        m_migratingRows.erase(it);
    }
    vassert(found == 1);
    return found == 1;
}

bool PersistentTable::migratingSwap(int64_t txnId, TableTuple& origtuple, TableTuple& desttuple) {
    vassert(isTableWithMigrate(m_tableType) && m_shadowStream != nullptr);
    MigratingRows::iterator it = m_migratingRows.find(txnId);
    if (it == m_migratingRows.end()) {
        vassert(false);
        return false;
    }

    size_t found = it->second.erase(origtuple.address());
    auto const success = it->second.insert(desttuple.address());
    vassert(found == 1 && success.second);
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
      vassert(ValuePeeker::peekBigInt(targetTuple.getHiddenNValue(getMigrateColumnIndex())) == currIt->first);
      deleteTuple(targetTuple, true, false);
   }
   currIt = m_migratingRows.erase(currIt);
   if (currIt == m_migratingRows.end() || currIt->first > deletableTxnId) {
       return false;
   }
   return true;
}

void PersistentTable::serializeTo(SerializeOutput &serialOutput) {
    // a placeholder for the total table size
    std::size_t pos = serialOutput.position();
    serialOutput.writeInt(-1);

    serializeColumnHeaderTo(serialOutput);

    // active tuple counts
    serialOutput.writeInt(static_cast<int32_t>(activeTupleCount()));
    int64_t written_count = 0;
    TableTuple tuple(m_schema);
    storage::for_each<PersistentTable::txn_iterator>(allocator(),
                    [this, &serialOutput, &written_count, &tuple](void* p) {
          void *tupleAddress = const_cast<void*>(reinterpret_cast<void const *>(p));
          tuple.move(tupleAddress);
          tuple.serializeTo(serialOutput);
          ++written_count;
      });
    vassert(written_count == activeTupleCount());

    // length prefix is non-inclusive
    int32_t sz = static_cast<int32_t>(serialOutput.position() - pos - sizeof(int32_t));
    vassert(sz > 0);
    serialOutput.writeIntAt(pos, sz);
}

void PersistentTable::serializeToWithoutTotalSize(SerializeOutput &serialOutput) {
    serializeColumnHeaderTo(serialOutput);

    // active tuple counts
    serialOutput.writeInt(static_cast<int32_t>(activeTupleCount()));
    int64_t written_count = 0;
    TableTuple tuple(m_schema);
    storage::for_each<PersistentTable::txn_iterator>(allocator(),
                        [this, &serialOutput, &written_count, &tuple](void* p) {
         void *tupleAddress = const_cast<void*>(reinterpret_cast<void const *>(p));
         tuple.move(tupleAddress);
         tuple.serializeTo(serialOutput);
         ++written_count;
    });
    vassert(written_count == activeTupleCount());
}

size_t PersistentTable::getAccurateSizeToSerialize() {
    // column header size
    size_t bytes = getColumnHeaderSizeToSerialize();

    // tuples
    bytes += sizeof(int32_t);  // tuple count
    int64_t written_count = 0;
    TableTuple tuple(m_schema);
    storage::for_each<PersistentTable::txn_iterator>(allocator(),
                            [this, &written_count, &tuple, &bytes](void* p) {
       void *tupleAddress = const_cast<void*>(reinterpret_cast<void const *>(p));
       tuple.move(tupleAddress);
       bytes += tuple.serializationSize();  // tuple size
       ++written_count;
    });

    vassert(written_count == activeTupleCount());

    return bytes;
}

void PersistentTable::loadTuplesFromNoHeader(SerializeInputBE &serialInput,
                                   Pool *stringPool) {
    int tupleCount = serialInput.readInt();
    vassert(tupleCount >= 0);

    int32_t serializedTupleCount = 0;
    size_t tupleCountPosition = 0;
    TableTuple target(m_schema);
    for (int i = 0; i < tupleCount; ++i) {
        void *address = const_cast<void*>(reinterpret_cast<void const *> (allocator().allocate()));
        target.move(address);
        target.deserializeFrom(serialInput, stringPool, LoadTableCaller::get(LoadTableCaller::INTERNAL));
        processLoadedTuple(target, NULL, serializedTupleCount, tupleCountPosition);
    }
    vassert(tupleCount == activeTupleCount());
}

bool PersistentTable::equals(voltdb::Table *other) {
    if (columnCount() != other->columnCount()) {
        return false;
    }

    if (activeTupleCount() != other->activeTupleCount()) {
        return false;
    }

    if (databaseId() != other->databaseId()) {
        return false;
    }

    if (name() != other->name()) {
        return false;
    }

    if (tableType() != other->tableType()) {
        return false;
    }

    const voltdb::TupleSchema *otherSchema = other->schema();
    if ( ! m_schema->equals(otherSchema)) {
        return false;
    }

    voltdb::TableIterator firstTI = iterator();
    voltdb::TableIterator secondTI = other->iterator();
    voltdb::TableTuple firstTuple(m_schema);
    voltdb::TableTuple secondTuple(otherSchema);
    while (firstTI.next(firstTuple)) {
        if ( ! secondTI.next(secondTuple)) {
            return false;
        }

        if ( ! firstTuple.equals(secondTuple)) {
            return false;
        }
    }
    return true;
}

} // namespace voltdb
