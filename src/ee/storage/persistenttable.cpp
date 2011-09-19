/* This file is part of VoltDB.
 * Copyright (C) 2008-2011 VoltDB Inc.
 *
 * This file contains original code and/or modifications of original code.
 * Any modifications made by VoltDB Inc. are licensed under the following
 * terms and conditions:
 *
 * VoltDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * VoltDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
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

#include <sstream>
#include <cassert>
#include <cstdio>

#include "boost/scoped_ptr.hpp"
#include "storage/persistenttable.h"

#include "common/debuglog.h"
#include "common/serializeio.h"
#include "common/FailureInjection.h"
#include "common/tabletuple.h"
#include "common/UndoQuantum.h"
#include "common/executorcontext.hpp"
#include "common/FatalException.hpp"
#include "common/types.h"
#include "common/RecoveryProtoMessage.h"
#include "indexes/tableindex.h"
#include "indexes/tableindexfactory.h"
#include "storage/table.h"
#include "storage/tableiterator.h"
#include "storage/TupleStreamWrapper.h"
#include "storage/TableStats.h"
#include "storage/PersistentTableStats.h"
#include "storage/PersistentTableUndoInsertAction.h"
#include "storage/PersistentTableUndoDeleteAction.h"
#include "storage/PersistentTableUndoUpdateAction.h"
#include "storage/ConstraintFailureException.h"
#include "storage/MaterializedViewMetadata.h"
#include "storage/CopyOnWriteContext.h"
#include "storage/tableiterator.h"

using namespace voltdb;

void* keyTupleStorage = NULL;
TableTuple keyTuple;

#define TABLE_BLOCKSIZE 2097152

PersistentTable::PersistentTable(ExecutorContext *ctx, bool exportEnabled) :
    Table(TABLE_BLOCKSIZE),
    m_iter(this, m_data.begin()),
    m_executorContext(ctx),
    m_uniqueIndexes(NULL), m_uniqueIndexCount(0), m_allowNulls(NULL),
    m_indexes(NULL), m_indexCount(0), m_pkeyIndex(NULL),
    stats_(this),
    m_COWContext(NULL),
    m_failedCompactionCount(0)
{
    for (int ii = 0; ii < TUPLE_BLOCK_NUM_BUCKETS; ii++) {
        m_blocksNotPendingSnapshotLoad.push_back(TBBucketPtr(new TBBucket()));
        m_blocksPendingSnapshotLoad.push_back(TBBucketPtr(new TBBucket()));
    }
}

PersistentTable::~PersistentTable() {

    for (int ii = 0; ii < TUPLE_BLOCK_NUM_BUCKETS; ii++) {
        m_blocksNotPendingSnapshotLoad[ii]->clear();
        m_blocksPendingSnapshotLoad[ii]->clear();
    }

    // delete all tuples to free strings
    TableIterator ti(this, m_data.begin());
    TableTuple tuple(m_schema);

    while (ti.next(tuple)) {
        // indexes aren't released as they don't have ownership of strings
        tuple.freeObjectColumns();
        tuple.setActiveFalse();
    }

    for (int i = 0; i < m_indexCount; ++i) {
        TableIndex *index = m_indexes[i];
        if (index != m_pkeyIndex) {
            delete index;
        }
    }
    if (m_pkeyIndex) delete m_pkeyIndex;
    if (m_uniqueIndexes) delete[] m_uniqueIndexes;
    if (m_allowNulls) delete[] m_allowNulls;
    if (m_indexes) delete[] m_indexes;

    // note this class has ownership of the views, even if they
    // were allocated by VoltDBEngine
    for (int i = 0; i < m_views.size(); i++) {
        delete m_views[i];
    }

}

// ------------------------------------------------------------------
// OPERATIONS
// ------------------------------------------------------------------
void PersistentTable::nextFreeTuple(TableTuple *tuple) {
    // First check whether we have any in our list
    // In the memcheck it uses the heap instead of a free list to help Valgrind.
    if (!m_blocksWithSpace.empty()) {
        VOLT_TRACE("GRABBED FREE TUPLE!\n");
        stx::btree_set<TBPtr >::iterator begin = m_blocksWithSpace.begin();
        TBPtr block = (*begin);
        std::pair<char*, int> retval = block->nextFreeTuple();

        /**
         * Check to see if the block needs to move to a new bucket
         */
        if (retval.second != -1) {
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

        tuple->move(retval.first);
        if (!block->hasFreeTuples()) {
            m_blocksWithSpace.erase(block);
        }
        assert (m_columnCount == tuple->sizeInValues());
        return;
    }

    // if there are no tuples free, we need to grab another chunk of memory
    // Allocate a new set of tuples
    TBPtr block = allocateNextBlock();

    // get free tuple
    assert (m_columnCount == tuple->sizeInValues());

    std::pair<char*, int> retval = block->nextFreeTuple();

    /**
     * Check to see if the block needs to move to a new bucket
     */
    if (retval.second != -1) {
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

    tuple->move(retval.first);
    //cout << "table::nextFreeTuple(" << reinterpret_cast<const void *>(this) << ") m_usedTuples == " << m_usedTuples << endl;

    if (block->hasFreeTuples()) {
        m_blocksWithSpace.insert(block);
    }
}

void PersistentTable::deleteAllTuples(bool freeAllocatedStrings) {
    // nothing interesting
    TableIterator ti(this, m_data.begin());
    TableTuple tuple(m_schema);
    while (ti.next(tuple)) {
        deleteTuple(tuple, true);
    }
}

void setSearchKeyFromTuple(TableTuple &source) {
    keyTuple.setNValue(0, source.getNValue(1));
    keyTuple.setNValue(1, source.getNValue(2));
}

/*
 * Regular tuple insertion that does an allocation and copy for
 * uninlined strings and creates and registers an UndoAction.
 */
bool PersistentTable::insertTuple(TableTuple &source) {

    // not null checks at first
    FAIL_IF(!checkNulls(source)) {
        throw ConstraintFailureException(this, source, TableTuple(),
                                         CONSTRAINT_TYPE_NOT_NULL);
    }

    //
    // First get the next free tuple
    // This will either give us one from the free slot list, or
    // grab a tuple at the end of our chunk of memory
    //
    nextFreeTuple(&m_tmpTarget1);
    m_tupleCount++;
    m_usedTupleCount++;

    //
    // Then copy the source into the target
    //
    m_tmpTarget1.copyForPersistentInsert(source); // tuple in freelist must be already cleared
    m_tmpTarget1.setActiveTrue();
    m_tmpTarget1.setPendingDeleteFalse();
    m_tmpTarget1.setPendingDeleteOnUndoReleaseFalse();

    /**
     * Inserts never "dirty" a tuple since the tuple is new, but...  The
     * COWIterator may still be scanning and if the tuple came from the free
     * list then it may need to be marked as dirty so it will be skipped. If COW
     * is on have it decide. COW should always set the dirty to false unless the
     * tuple is in a to be scanned area.
     */
    if (m_COWContext.get() != NULL) {
        m_COWContext->markTupleDirty(m_tmpTarget1, true);
    } else {
        m_tmpTarget1.setDirtyFalse();
    }
    m_tmpTarget1.isDirty();

    if (!tryInsertOnAllIndexes(&m_tmpTarget1)) {
        // Careful to delete allocated objects
        m_tmpTarget1.freeObjectColumns();
        deleteTupleStorage(m_tmpTarget1);
        throw ConstraintFailureException(this, source, TableTuple(),
                                         CONSTRAINT_TYPE_UNIQUE);
    }

    if (m_schema->getUninlinedObjectColumnCount() != 0)
    {
        increaseStringMemCount(m_tmpTarget1.getNonInlinedMemorySize());
    }
    /*
     * Create and register an undo action.
     */
    UndoQuantum *undoQuantum = m_executorContext->getCurrentUndoQuantum();
    assert(undoQuantum);
    Pool *pool = undoQuantum->getDataPool();
    assert(pool);
    PersistentTableUndoInsertAction *ptuia =
      new (pool->allocate(sizeof(PersistentTableUndoInsertAction)))
      PersistentTableUndoInsertAction(m_tmpTarget1, this, pool);
    undoQuantum->registerUndoAction(ptuia);

    // handle any materialized views
    for (int i = 0; i < m_views.size(); i++) {
        m_views[i]->processTupleInsert(source);
    }

    return true;
}

/*
 * Insert a tuple but don't allocate a new copy of the uninlineable
 * strings or create an UndoAction or update a materialized view.
 */
void PersistentTable::insertTupleForUndo(char *tuple) {

    m_tmpTarget1.move(tuple);
    m_tmpTarget1.setPendingDeleteOnUndoReleaseFalse();
    m_tuplesPinnedByUndo--;
    m_usedTupleCount++;

    /*
     * The only thing to do is reinsert the tuple into the indexes. It was never moved,
     * just marked as deleted.
     */
    if (!tryInsertOnAllIndexes(&m_tmpTarget1)) {
        deleteTupleStorage(m_tmpTarget1);
        throwFatalException("Failed to insert tuple into table %s for undo:"
                            " unique constraint violation\n%s\n", m_name.c_str(),
                            m_tmpTarget1.debugNoHeader().c_str());
    }
}

/*
 * Regular tuple update function that does a copy and allocation for
 * updated strings and creates an UndoAction.
 */
bool PersistentTable::updateTuple(TableTuple &source, TableTuple &target, bool updatesIndexes) {

    /*
     * Create and register an undo action and then use the copy of
     * the target (old value with no updates)
     */
     UndoQuantum *undoQuantum = m_executorContext->getCurrentUndoQuantum();
     assert(undoQuantum);
     Pool *pool = undoQuantum->getDataPool();
     assert(pool);
     PersistentTableUndoUpdateAction *ptuua =
       new (pool->allocate(sizeof(PersistentTableUndoUpdateAction)))
       PersistentTableUndoUpdateAction(target, this, pool);

     if (m_COWContext.get() != NULL) {
         m_COWContext->markTupleDirty(target, false);
     }

    if (m_schema->getUninlinedObjectColumnCount() != 0)
    {
        decreaseStringMemCount(target.getNonInlinedMemorySize());
        increaseStringMemCount(source.getNonInlinedMemorySize());
    }

     source.setActiveTrue();
     //Copy the dirty status that was set by markTupleDirty.
     if (target.isDirty()) {
         source.setDirtyTrue();
     } else {
         source.setDirtyFalse();
     }
     target.copyForPersistentUpdate(source);

     ptuua->setNewTuple(target, pool);

     if (!undoQuantum->isDummy()) {
         //DummyUndoQuantum calls destructor upon register.
         undoQuantum->registerUndoAction(ptuua);
     }

    // the planner should determine if this update can affect indexes.
    // if so, update the indexes here
    if (updatesIndexes) {
        if (!tryUpdateOnAllIndexes(ptuua->getOldTuple(), target)) {
            throw ConstraintFailureException(this, ptuua->getOldTuple(),
                                             target,
                                             CONSTRAINT_TYPE_UNIQUE);
        }

        //If the CFE is thrown the Undo action should not attempt to revert the
        //indexes.
        ptuua->needToRevertIndexes();
        updateFromAllIndexes(ptuua->getOldTuple(), target);
    }

    // handle any materialized views
    for (int i = 0; i < m_views.size(); i++) {
        m_views[i]->processTupleUpdate(ptuua->getOldTuple(), target);
    }

    /**
     * Check for nulls after the update has been performed because the source tuple may have garbage in
     * some columns
     */
    FAIL_IF(!checkNulls(target)) {
        throw ConstraintFailureException(this, ptuua->getOldTuple(),
                                         target,
                                         CONSTRAINT_TYPE_NOT_NULL);
    }

    if (undoQuantum->isDummy()) {
        //DummyUndoQuantum calls destructor upon register so it can't be called
        //earlier
        undoQuantum->registerUndoAction(ptuua);
    }

    return true;
}

/*
 * Source contains the tuple before the update and target is a
 * reference to the updated tuple including the actual table
 * storage. First backup the target to a temp tuple so it will be
 * available for updating indexes. Then revert the tuple to the
 * original preupdate values by copying the source to the target. Then
 * update the indexes to use the new key value (if the key has
 * changed). The backup is necessary because the indexes expect the
 * data ptr that will be used as the value in the index.
 */
void PersistentTable::updateTupleForUndo(TableTuple &source, TableTuple &target,
                                         bool revertIndexes) {
    if (m_schema->getUninlinedObjectColumnCount() != 0)
    {
        decreaseStringMemCount(target.getNonInlinedMemorySize());
        increaseStringMemCount(source.getNonInlinedMemorySize());
    }

    //Need to back up the updated version of the tuple to provide to
    //the indexes when updating The indexes expect source's data Ptr
    //to point into the table so it is necessary to copy source to
    //target. Without this backup the target would be lost and it
    //there would be nothing to provide to the index to lookup. In
    //regular updateTuple this storage is provided by the undo
    //quantum.
    TableTuple targetBackup = tempTuple();
    targetBackup.copy(target);

    bool dirty = target.isDirty();
    // this is the actual in-place revert to the old version
    target.copy(source);
    if (dirty) {
        target.setDirtyTrue();
    } else {
        target.setDirtyFalse();
    }
    target.isDirty();

    //If the indexes were never updated there is no need to revert them.
    if (revertIndexes) {
        if (!tryUpdateOnAllIndexes(targetBackup, target)) {
            // TODO: this might be too strict. see insertTuple()
            throwFatalException("Failed to update tuple in table %s for undo:"
                                " unique constraint violation\n%s\n%s\n", m_name.c_str(),
                                targetBackup.debugNoHeader().c_str(),
                                target.debugNoHeader().c_str());
        }
        updateFromAllIndexes(targetBackup, target);
    }
}

bool PersistentTable::deleteTuple(TableTuple &target, bool deleteAllocatedStrings) {
    // May not delete an already deleted tuple.
    assert(target.isActive());

    // The tempTuple is forever!
    assert(&target != &m_tempTuple);

    // Just like insert, we want to remove this tuple from all of our indexes
    deleteFromAllIndexes(&target);

    target.setPendingDeleteOnUndoReleaseTrue();
    m_tuplesPinnedByUndo++;
    m_usedTupleCount--;

    /*
     * Create and register an undo action.
     */
    UndoQuantum *undoQuantum = m_executorContext->getCurrentUndoQuantum();
    assert(undoQuantum);
    Pool *pool = undoQuantum->getDataPool();
    assert(pool);
    PersistentTableUndoDeleteAction *ptuda =
            new (pool->allocate(sizeof(PersistentTableUndoDeleteAction))) PersistentTableUndoDeleteAction( target.address(), this);

    // handle any materialized views
    for (int i = 0; i < m_views.size(); i++) {
        m_views[i]->processTupleDelete(target);
    }

    undoQuantum->registerUndoAction(ptuda, this);
    return true;
}

/*
 * Delete a tuple by looking it up via table scan or a primary key
 * index lookup. An undo initiated delete like deleteTupleForUndo
 * is in response to the insertion of a new tuple by insertTuple
 * and that by definition is a tuple that is of no interest to
 * the COWContext. The COWContext set the tuple to have the
 * correct dirty setting when the tuple was originally inserted.
 * TODO remove duplication with regular delete. Also no view updates.
 */
void PersistentTable::deleteTupleForUndo(TableTuple &tupleCopy) {
    TableTuple target = lookupTuple(tupleCopy);
    if (target.isNullTuple()) {
        throwFatalException("Failed to delete tuple from table %s:"
                            " tuple does not exist\n%s\n", m_name.c_str(),
                            tupleCopy.debugNoHeader().c_str());
    }
    else {
        // Make sure that they are not trying to delete the same tuple twice
        assert(target.isActive());

        // Also make sure they are not trying to delete our m_tempTuple
        assert(&target != &m_tempTuple);

        // Just like insert, we want to remove this tuple from all of our indexes
        deleteFromAllIndexes(&target);

        if (m_schema->getUninlinedObjectColumnCount() != 0)
        {
            decreaseStringMemCount(tupleCopy.getNonInlinedMemorySize());
        }

        // Delete the strings/objects
        target.freeObjectColumns();
        deleteTupleStorage(target);
        m_usedTupleCount--;
    }
}

TableTuple PersistentTable::lookupTuple(TableTuple tuple) {
    TableTuple nullTuple(m_schema);

    TableIndex *pkeyIndex = primaryKeyIndex();
    if (pkeyIndex == NULL) {
        /*
         * Do a table scan.
         */
        TableTuple tableTuple(m_schema);
        TableIterator ti(this, m_data.begin());
        while (ti.hasNext()) {
            ti.next(tableTuple);
            if (tableTuple.equalsNoSchemaCheck(tuple)) {
                return tableTuple;
            }
        }
        return nullTuple;
    }

    bool foundTuple = pkeyIndex->moveToTuple(&tuple);
    if (!foundTuple) {
        return nullTuple;
    }

    return pkeyIndex->nextValueAtKey();
}

void PersistentTable::insertIntoAllIndexes(TableTuple *tuple) {
    for (int i = m_indexCount - 1; i >= 0;--i) {
        if (!m_indexes[i]->addEntry(tuple)) {
            throwFatalException(
                    "Failed to insert tuple in Table: %s Index %s", m_name.c_str(), m_indexes[i]->getName().c_str());
        }
    }
}

void PersistentTable::deleteFromAllIndexes(TableTuple *tuple) {
    for (int i = m_indexCount - 1; i >= 0;--i) {
        if (!m_indexes[i]->deleteEntry(tuple)) {
            throwFatalException(
                    "Failed to delete tuple in Table: %s Index %s", m_name.c_str(), m_indexes[i]->getName().c_str());
        }
    }
}

void PersistentTable::updateFromAllIndexes(TableTuple &targetTuple, const TableTuple &sourceTuple) {
    for (int i = m_indexCount - 1; i >= 0;--i) {
        if (!m_indexes[i]->replaceEntry(&targetTuple, &sourceTuple)) {
            throwFatalException(
                    "Failed to update tuple in Table: %s Index %s", m_name.c_str(), m_indexes[i]->getName().c_str());
        }
    }
}

void PersistentTable::updateWithSameKeyFromAllIndexes(TableTuple &targetTuple, const TableTuple &sourceTuple) {
    for (int i = m_indexCount - 1; i >= 0;--i) {
        if (!m_indexes[i]->replaceEntryNoKeyChange(&targetTuple, &sourceTuple)) {
            throwFatalException(
                    "Failed to update tuple in Table: %s Index %s", m_name.c_str(), m_indexes[i]->getName().c_str());
        }
    }
}

bool PersistentTable::tryInsertOnAllIndexes(TableTuple *tuple) {
    for (int i = m_indexCount - 1; i >= 0; --i) {
        FAIL_IF(!m_indexes[i]->addEntry(tuple)) {
            VOLT_DEBUG("Failed to insert into index %s,%s",
                       m_indexes[i]->getTypeName().c_str(),
                       m_indexes[i]->getName().c_str());
            for (int j = i + 1; j < m_indexCount; ++j) {
                m_indexes[j]->deleteEntry(tuple);
            }
            return false;
        }
    }
    return true;
}

bool PersistentTable::tryUpdateOnAllIndexes(TableTuple &targetTuple, const TableTuple &sourceTuple) {
    for (int i = m_uniqueIndexCount - 1; i >= 0;--i) {
        if (m_uniqueIndexes[i]->checkForIndexChange(&targetTuple, &sourceTuple) == false)
            continue; // no update is needed for this index

        // if there is a change, the new_key has to be checked
        FAIL_IF (m_uniqueIndexes[i]->exists(&sourceTuple)) {
            VOLT_WARN("Unique Index '%s' complained to the update",
                      m_uniqueIndexes[i]->debug().c_str());
            return false; // cannot insert the new value
        }
    }
    return true;
}

bool PersistentTable::checkNulls(TableTuple &tuple) const {
    assert (m_columnCount == tuple.sizeInValues());
    for (int i = m_columnCount - 1; i >= 0; --i) {
        if (tuple.isNull(i) && !m_allowNulls[i]) {
            VOLT_TRACE ("%d th attribute was NULL. It is non-nillable attribute.", i);
            return false;
        }
    }
    return true;
}

/*
 * claim ownership of a view. table is responsible for this view*
 */
void PersistentTable::addMaterializedView(MaterializedViewMetadata *view) {
    m_views.push_back(view);
}

// ------------------------------------------------------------------
// UTILITY
// ------------------------------------------------------------------
std::string PersistentTable::tableType() const {
    return "PersistentTable";
}

std::string PersistentTable::debug() {
    std::ostringstream buffer;
    buffer << Table::debug();
    buffer << "\tINDEXES: " << m_indexCount << "\n";

    // Indexes
    buffer << "===========================================================\n";
    for (int index_ctr = 0; index_ctr < m_indexCount; ++index_ctr) {
        if (m_indexes[index_ctr]) {
            buffer << "\t[" << index_ctr << "] " << m_indexes[index_ctr]->debug();
            //
            // Primary Key
            //
            if (m_pkeyIndex != NULL && m_pkeyIndex->getName().compare(m_indexes[index_ctr]->getName()) == 0) {
                buffer << " [PRIMARY KEY]";
            }
            buffer << "\n";
        }
    }

    return buffer.str();
}

// ------------------------------------------------------------------
// Accessors
// ------------------------------------------------------------------
// Index
TableIndex *PersistentTable::index(std::string name) {
    for (int i = 0; i < m_indexCount; ++i) {
        TableIndex *index = m_indexes[i];
        if (index->getName().compare(name) == 0) {
            return index;
        }
    }
    std::stringstream errorString;
    errorString << "Could not find Index with name " << name << std::endl;
    for (int i = 0; i < m_indexCount; ++i) {
        TableIndex *index = m_indexes[i];
        errorString << index->getName() << std::endl;
    }
    throwFatalException( "%s", errorString.str().c_str());
}

std::vector<TableIndex*> PersistentTable::allIndexes() const {
    std::vector<TableIndex*> retval;
    for (int i = 0; i < m_indexCount; i++)
        retval.push_back(m_indexes[i]);

    return retval;
}

void PersistentTable::onSetColumns() {
    if (m_allowNulls != NULL) delete[] m_allowNulls;
    m_allowNulls = new bool[m_columnCount];
    for (int i = m_columnCount - 1; i >= 0; --i) {
        m_allowNulls[i] = m_schema->columnAllowNull(i);
    }

    // Also clear some used block state. this structure doesn't have
    // an block ownership semantics - it's just a cache. I think.
    m_blocksWithSpace.clear();

    // note that any allocated memory in m_data is left alone
    // as is m_allocatedTuples
    m_data.clear();
}

/*
 * Implemented by persistent table and called by Table::loadTuplesFrom
 * to do additional processing for views and Export and non-inline
 * memory tracking
 */
void PersistentTable::processLoadedTuple(TableTuple &tuple) {

    // not null checks at first
    FAIL_IF(!checkNulls(tuple)) {
        throw ConstraintFailureException(this, tuple, TableTuple(),
                                         CONSTRAINT_TYPE_NOT_NULL);
    }

    if (!tryInsertOnAllIndexes(&tuple)) {
        throw ConstraintFailureException(this, tuple, TableTuple(),
                                         CONSTRAINT_TYPE_UNIQUE);
    }

    // handle any materialized views
    for (int i = 0; i < m_views.size(); i++) {
        m_views[i]->processTupleInsert(m_tmpTarget1);
    }

    // Account for non-inlined memory allocated via bulk load or recovery
    if (m_schema->getUninlinedObjectColumnCount() != 0)
    {
        increaseStringMemCount(tuple.getNonInlinedMemorySize());
    }
}

TableStats* PersistentTable::getTableStats() {
    return &stats_;
}

/**
 * Switch the table to copy on write mode. Returns true if the table was already in copy on write mode.
 */
bool PersistentTable::activateCopyOnWrite(TupleSerializer *serializer, int32_t partitionId) {
    if (m_COWContext != NULL) {
        return true;
    }
    if (m_tupleCount == 0) {
        return false;
    }

    //All blocks are now pending snapshot
    m_blocksPendingSnapshot.swap(m_blocksNotPendingSnapshot);
    m_blocksPendingSnapshotLoad.swap(m_blocksNotPendingSnapshotLoad);
    assert(m_blocksNotPendingSnapshot.empty());
    for (int ii = 0; ii < m_blocksNotPendingSnapshotLoad.size(); ii++) {
        assert(m_blocksNotPendingSnapshotLoad[ii]->empty());
    }

    m_COWContext.reset(new CopyOnWriteContext( this, serializer, partitionId));
    return false;
}

/**
 * Attempt to serialize more tuples from the table to the provided output stream.
 * Returns true if there are more tuples and false if there are no more tuples waiting to be
 * serialized.
 */
bool PersistentTable::serializeMore(ReferenceSerializeOutput *out) {
    if (m_COWContext == NULL) {
        return false;
    }

    const bool hasMore = m_COWContext->serializeMore(out);
    if (!hasMore) {
        m_COWContext.reset(NULL);
    }

    return hasMore;
}

/**
 * Create a recovery stream for this table. Returns true if the table already has an active recovery stream
 */
bool PersistentTable::activateRecoveryStream(int32_t tableId) {
    if (m_recoveryContext != NULL) {
        return true;
    }
    m_recoveryContext.reset(new RecoveryContext( this, tableId ));
    return false;
}

/**
 * Serialize the next message in the stream of recovery messages. Returns true if there are
 * more messages and false otherwise.
 */
void PersistentTable::nextRecoveryMessage(ReferenceSerializeOutput *out) {
    if (m_recoveryContext == NULL) {
        return;
    }

    const bool hasMore = m_recoveryContext->nextMessage(out);
    if (!hasMore) {
        m_recoveryContext.reset(NULL);
    }
}

/**
 * Process the updates from a recovery message
 */
void PersistentTable::processRecoveryMessage(RecoveryProtoMsg* message, Pool *pool) {
    switch (message->msgType()) {
    case RECOVERY_MSG_TYPE_SCAN_TUPLES: {
        if (activeTupleCount() == 0) {
            uint32_t tupleCount = message->totalTupleCount();
            for (int i = 0; i < m_indexCount; i++) {
                m_indexes[i]->ensureCapacity(tupleCount);
            }
        }
        loadTuplesFromNoHeader(*message->stream(), pool);
        break;
    }
    default:
        throwFatalException("Attempted to process a recovery message of unknown type %d", message->msgType());
    }
}

/**
 * Create a tree index on the primary key and then iterate it and hash
 * the tuple data.
 */
size_t PersistentTable::hashCode() {
    TableIndexScheme sourceScheme = m_pkeyIndex->getScheme();
    sourceScheme.setTree();
    boost::scoped_ptr<TableIndex> pkeyIndex(TableIndexFactory::getInstance(sourceScheme));
    TableIterator iter(this, m_data.begin());
    TableTuple tuple(schema());
    while (iter.next(tuple)) {
        pkeyIndex->addEntry(&tuple);
    }

    pkeyIndex->moveToEnd(true);

    size_t hashCode = 0;
    while (true) {
         tuple = pkeyIndex->nextValue();
         if (tuple.isNullTuple()) {
             break;
         }
         tuple.hashCode(hashCode);
    }
    return hashCode;
}

void PersistentTable::notifyBlockWasCompactedAway(TBPtr block) {
    if (m_blocksNotPendingSnapshot.find(block) != m_blocksNotPendingSnapshot.end()) {
        assert(m_blocksPendingSnapshot.find(block) == m_blocksPendingSnapshot.end());
    } else {
        assert(m_COWContext != NULL);
        assert(m_blocksPendingSnapshot.find(block) != m_blocksPendingSnapshot.end());
        m_COWContext->notifyBlockWasCompactedAway(block);
    }

}

void PersistentTable::swapTuples(TableTuple sourceTuple, TableTuple destinationTuple) {
    ::memcpy(destinationTuple.address(), sourceTuple.address(), m_tupleLength);
    sourceTuple.setActiveFalse();
    assert(!sourceTuple.isPendingDeleteOnUndoRelease());
    if (!sourceTuple.isPendingDelete()) {
        updateWithSameKeyFromAllIndexes(sourceTuple, destinationTuple);
    }
}

bool PersistentTable::doCompactionWithinSubset(TBBucketMap *bucketMap) {
    /**
     * First find the two best candidate blocks
     */
    TBPtr fullest;
    TBBucketI fullestIterator;
    bool foundFullest = false;
    for (int ii = (TUPLE_BLOCK_NUM_BUCKETS - 1); ii >= 0; ii--) {
        fullestIterator = (*bucketMap)[ii]->begin();
        if (fullestIterator != (*bucketMap)[ii]->end()) {
            foundFullest = true;
            fullest = *fullestIterator;
            break;
        }
    }
    if (!foundFullest) {
        //std::cout << "Could not find a fullest block for compaction" << std::endl;
        return false;
    }

    int fullestBucketChange = -1;
    while (fullest->hasFreeTuples()) {
        TBPtr lightest;
        TBBucketI lightestIterator;
        bool foundLightest = false;

        for (int ii = 0; ii < TUPLE_BLOCK_NUM_BUCKETS; ii++) {
            lightestIterator = (*bucketMap)[ii]->begin();
            if (lightestIterator != (*bucketMap)[ii]->end()) {
                lightest = *lightestIterator;
                if (lightest != fullest) {
                    foundLightest = true;
                    break;
                } else {
                    lightestIterator++;
                    if (lightestIterator != (*bucketMap)[ii]->end()) {
                        lightest = *lightestIterator;
                        foundLightest = true;
                        break;
                    }
                }
            }
        }
        if (!foundLightest) {
//            TBMapI iter = m_data.begin();
//            while (iter != m_data.end()) {
//                std::cout << "Block " << static_cast<void*>(iter.data().get()) << " has " <<
//                        iter.data()->activeTuples() << " active tuples and " << iter.data()->lastCompactionOffset()
//                        << " last compaction offset and is in bucket " <<
//                        static_cast<void*>(iter.data()->currentBucket().get()) <<
//                        std::endl;
//                iter++;
//            }
//
//            for (int ii = 0; ii < TUPLE_BLOCK_NUM_BUCKETS; ii++) {
//                std::cout << "Bucket " << ii << "(" << static_cast<void*>((*bucketMap)[ii].get()) << ") has size " << (*bucketMap)[ii]->size() << std::endl;
//                if (!(*bucketMap)[ii]->empty()) {
//                    TBBucketI bucketIter = (*bucketMap)[ii]->begin();
//                    while (bucketIter != (*bucketMap)[ii]->end()) {
//                        std::cout << "\t" << static_cast<void*>(bucketIter->get()) << std::endl;
//                        bucketIter++;
//                    }
//                }
//            }
//
//            std::cout << "Could not find a lightest block for compaction" << std::endl;
            return false;
        }

        std::pair<int, int> bucketChanges = fullest->merge(this, lightest);
        int tempFullestBucketChange = bucketChanges.first;
        if (tempFullestBucketChange != -1) {
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
            if (lightestBucketChange != -1) {
                lightest->swapToBucket((*bucketMap)[lightestBucketChange]);
            }
        }
    }

    if (fullestBucketChange != -1) {
        fullest->swapToBucket((*bucketMap)[fullestBucketChange]);
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

void PersistentTable::doForcedCompaction() {
    if (m_recoveryContext != NULL)
    {
        std::cout << "Deferring compaction until recovery is complete..." << std::endl;
        return;
    }
    bool hadWork1 = true;
    bool hadWork2 = true;
    std::cout << "Doing forced compaction with allocated tuple count " << allocatedTupleCount() << std::endl;
    int failedCompactionCountBefore = m_failedCompactionCount;
    while (compactionPredicate()) {
        assert(hadWork1 || hadWork2);
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
                std::cerr << "Compaction predicate said there should be " <<
                             "blocks to compact but no blocks were found " <<
                             "to be eligible for compaction. This has " <<
                             "occured " << m_failedCompactionCount <<
                             " times." << std::endl;
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
        }
        if (!m_blocksPendingSnapshot.empty() && hadWork2) {
            //std::cout << "Compacting blocks pending snapshot " << m_blocksPendingSnapshot.size() << std::endl;
            hadWork2 = doCompactionWithinSubset(&m_blocksPendingSnapshotLoad);
        }
    }
    //If compactions have been failing lately, but it didn't fail this time
    //then compaction progressed until the predicate was satisfied
    if (failedCompactionCountBefore > 0 && failedCompactionCountBefore == m_failedCompactionCount) {
        std::cerr << "Recovered from a failed compaction scenario and compacted to the point that the compaction predicate was satisfied after " << failedCompactionCountBefore << " failed attempts" << std::endl;
        m_failedCompactionCount = 0;
    }
    assert(!compactionPredicate());
    std::cout << "Finished forced compaction with allocated tuple count " << allocatedTupleCount() << std::endl;
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
        std::cout << static_cast<void*>((*blocksNotPendingSnapshot)->address()) << ",";
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
            std::cout << "\t" << static_cast<void*>((*bucketIter)->address()) << std::endl;
            bucketIter++;
        }
    }

    boost::unordered_set<TBPtr>::iterator blocksPendingSnapshot = m_blocksPendingSnapshot.begin();
    std::cout << "Blocks pending snapshot: ";
    while (blocksPendingSnapshot != m_blocksPendingSnapshot.end()) {
        std::cout << static_cast<void*>((*blocksPendingSnapshot)->address()) << ",";
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
            std::cout << "\t" << static_cast<void*>((*bucketIter)->address()) << std::endl;
            bucketIter++;
        }
    }
    std::cout << std::endl;
}
