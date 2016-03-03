/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
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

#include <sstream>
#include <cassert>
#include <cstdio>
#include <algorithm>    // std::find
#include <boost/foreach.hpp>
#include <boost/scoped_ptr.hpp>

#include "streamedtable.h"
#include "persistenttable.h"
#include "StreamedTableUndoAction.hpp"
#include "ExportTupleStream.h"
#include "common/executorcontext.hpp"
#include "tableiterator.h"
#include "ExportMaterializedViewMetadata.h"

using namespace voltdb;

StreamedTable::StreamedTable(bool exportEnabled, int partitionColumn)
    : Table(1), stats_(this), m_executorContext(ExecutorContext::getExecutorContext()), m_wrapper(NULL),
      m_sequenceNo(0), m_partitionColumn(partitionColumn)
{
    // In StreamedTable, a non-null m_wrapper implies export enabled.
    if (exportEnabled) {
        enableStream();
    }
}

StreamedTable::StreamedTable(bool exportEnabled, ExportTupleStream* wrapper)
    : Table(1), stats_(this), m_executorContext(ExecutorContext::getExecutorContext()), m_wrapper(wrapper),
    m_sequenceNo(0), m_partitionColumn(-1)
{
    // In StreamedTable, a non-null m_wrapper implies export enabled.
    if (exportEnabled) {
        enableStream();
    }
}

StreamedTable *
StreamedTable::createForTest(size_t wrapperBufSize, ExecutorContext *ctx) {
    StreamedTable * st = new StreamedTable(true);
    st->m_wrapper->setDefaultCapacity(wrapperBufSize);
    return st;
}

//This returns true if a stream was created thus caller can setSignatureAndGeneration to push.
bool StreamedTable::enableStream() {
    if (!m_wrapper) {
        m_wrapper = new ExportTupleStream(m_executorContext->m_partitionId,
                                           m_executorContext->m_siteId);
        return true;
    }
    return false;
}

void
StreamedTable::updateMaterializedViewTargetTable(PersistentTable* target, catalog::MaterializedViewInfo* targetMvInfo)
{
    if (target == NULL) {
        return;
    }
    std::string targetName = target->name();

    // find the materialized view that uses the table or its precursor (by the same name).
    BOOST_FOREACH(ExportMaterializedViewMetadata* currView, m_views) {
        PersistentTable* currTarget = currView->targetTable();

        // found: target is alreafy set
        if (currTarget == target) {
            currView->setIndexForMinMax(targetMvInfo->indexForMinMax());
            return;
        }

        // found: this is the table to set the
        std::string currName = currTarget->name();
        if (currName == targetName) {
            // A match on name only indicates that the target table has been re-defined since
            // the view was initialized, so re-initialize the view.
            currView->setTargetTable(target);
            currView->setIndexForMinMax(targetMvInfo->indexForMinMax());
            return;
        }
    }

    // The connection needs to be made using a new MaterializedViewMetadata
    // This is not a leak -- the materialized view is self-installing into srcTable.
    new ExportMaterializedViewMetadata(this, target, targetMvInfo);
}

/*
 * claim ownership of a view. table is responsible for this view*
 */
void StreamedTable::addMaterializedView(ExportMaterializedViewMetadata *view)
{
    m_views.push_back(view);
}

void
StreamedTable::segregateMaterializedViews(std::map<std::string, catalog::MaterializedViewInfo*>::const_iterator const & start,
                                            std::map<std::string, catalog::MaterializedViewInfo*>::const_iterator const & end,
                                            std::vector< catalog::MaterializedViewInfo*> &survivingInfosOut,
                                            std::vector<ExportMaterializedViewMetadata*> &survivingViewsOut,
                                            std::vector<ExportMaterializedViewMetadata*> &obsoleteViewsOut)
{
    //////////////////////////////////////////////////////////
    // find all of the materialized views to remove or keep
    //////////////////////////////////////////////////////////

    // iterate through all of the existing views
    BOOST_FOREACH(ExportMaterializedViewMetadata* currView, m_views) {
        std::string currentViewId = currView->targetTable()->name();

        // iterate through all of the catalog views, looking for a match.
        std::map<std::string, catalog::MaterializedViewInfo*>::const_iterator viewIter;
        bool viewfound = false;
        for (viewIter = start; viewIter != end; ++viewIter) {
            catalog::MaterializedViewInfo* catalogViewInfo = viewIter->second;
            if (currentViewId == catalogViewInfo->name()) {
                viewfound = true;
                //TODO: This MIGHT be a good place to identify the need for view re-definition.
                survivingInfosOut.push_back(catalogViewInfo);
                survivingViewsOut.push_back(currView);
                break;
            }
        }


        // if the table has a view that the catalog doesn't, then prepare to remove (or fail to migrate) the view
        if (!viewfound) {
            obsoleteViewsOut.push_back(currView);
        }
    }
}

void StreamedTable::dropMaterializedView(ExportMaterializedViewMetadata *targetView) {
    assert( ! m_views.empty());
    ExportMaterializedViewMetadata *lastView = m_views.back();
    if (targetView != lastView) {
        // iterator to vector element:
        std::vector<ExportMaterializedViewMetadata*>::iterator toView = find(m_views.begin(), m_views.end(), targetView);
        assert(toView != m_views.end());
        // Use the last view to patch the potential hole.
        *toView = lastView;
    }
    // The last element is now excess.
    m_views.pop_back();
    delete targetView;
}

StreamedTable::~StreamedTable()
{
    // note this class has ownership of the views, even if they
    // were allocated by VoltDBEngine
    for (int i = 0; i < m_views.size(); i++) {
        delete m_views[i];
    }
    delete m_wrapper;
}

TableIterator& StreamedTable::iterator() {
    throw SerializableEEException(VOLT_EE_EXCEPTION_TYPE_EEEXCEPTION,
                                  "May not iterate a streamed table.");
}

TableIterator* StreamedTable::makeIterator() {
    throw SerializableEEException(VOLT_EE_EXCEPTION_TYPE_EEEXCEPTION,
                                  "May not iterate a streamed table.");
}

void StreamedTable::deleteAllTuples(bool freeAllocatedStrings)
{
    throw SerializableEEException(VOLT_EE_EXCEPTION_TYPE_EEEXCEPTION,
                                  "May not delete all tuples of a streamed"
                                  " table.");
}

TBPtr StreamedTable::allocateNextBlock() {
    throw SerializableEEException(VOLT_EE_EXCEPTION_TYPE_EEEXCEPTION,
                                  "May not use block alloc interface with "
                                  "streamed tables.");
}

void StreamedTable::nextFreeTuple(TableTuple *) {
    throw SerializableEEException(VOLT_EE_EXCEPTION_TYPE_EEEXCEPTION,
                                  "May not use nextFreeTuple with streamed tables.");
}

bool StreamedTable::insertTuple(TableTuple &source)
{
    size_t mark = 0;
    if (m_wrapper) {
        // handle any materialized views
        for (int i = 0; i < m_views.size(); i++) {
            m_views[i]->processTupleInsert(source, true);
        }
        mark = m_wrapper->appendTuple(m_executorContext->m_lastCommittedSpHandle,
                                      m_executorContext->currentSpHandle(),
                                      m_sequenceNo++,
                                      m_executorContext->currentUniqueId(),
                                      m_executorContext->currentTxnTimestamp(),
                                      source,
                                      ExportTupleStream::INSERT);
        m_tupleCount++;
        UndoQuantum *uq = m_executorContext->getCurrentUndoQuantum();
        if (!uq) {
            // With no active UndoLog, there is no undo support.
            return true;
        }
        uq->registerUndoAction(new (*uq) StreamedTableUndoAction(this, mark));
    } else {
        // handle any materialized views even though we dont have any connector.
        for (int i = 0; i < m_views.size(); i++) {
            m_views[i]->processTupleInsert(source, true);
        }
    }
    return true;
}

bool StreamedTable::updateTupleWithSpecificIndexes(TableTuple &, TableTuple &, std::vector<TableIndex*> const&, bool)
{
    throwFatalException("May not update a streamed table.");
    return true;
}

bool StreamedTable::deleteTuple(TableTuple &tuple, bool fallible)
{
    size_t mark = 0;
    if (m_wrapper) {
        for (int i = 0; i < m_views.size(); i++) {
            m_views[i]->processTupleDelete(tuple, fallible);
        }
        mark = m_wrapper->appendTuple(m_executorContext->m_lastCommittedSpHandle,
                                      m_executorContext->currentSpHandle(),
                                      m_sequenceNo++,
                                      m_executorContext->currentUniqueId(),
                                      m_executorContext->currentTxnTimestamp(),
                                      tuple,
                                      ExportTupleStream::DELETE);
        m_tupleCount++;
        // Infallible delete (schema change with tuple migration & views) is not supported for export tables
        assert(fallible);
        UndoQuantum *uq = m_executorContext->getCurrentUndoQuantum();
        if (!uq) {
            // With no active UndoLog, there is no undo support.
            return true;
        }
        uq->registerUndoAction(new (*uq) StreamedTableUndoAction(this, mark));
    }
    return true;
}

void StreamedTable::loadTuplesFrom(SerializeInputBE&, Pool*)
{
    throw SerializableEEException(VOLT_EE_EXCEPTION_TYPE_EEEXCEPTION,
                                  "May not update a streamed table.");
}

void StreamedTable::flushOldTuples(int64_t timeInMillis)
{
    if (m_wrapper) {
        m_wrapper->periodicFlush(timeInMillis,
                                 m_executorContext->m_lastCommittedSpHandle);
    }
}

/**
 * Inform the tuple stream wrapper of the table's delegate id
 */
void StreamedTable::setSignatureAndGeneration(std::string signature, int64_t generation) {
    if (m_wrapper) {
        m_wrapper->setSignatureAndGeneration(signature, generation);
    }
}

void StreamedTable::undo(size_t mark)
{
    if (m_wrapper) {
        m_wrapper->rollbackTo(mark, SIZE_MAX);
        //Decrementing the sequence number should make the stream of tuples
        //contiguous outside of actual system failures. Should be more useful
        //then having gaps.
        m_sequenceNo--;
    }
}

TableStats *StreamedTable::getTableStats() {
    return &stats_;
}

size_t StreamedTable::allocatedBlockCount() const {
    return 0;
}

int64_t StreamedTable::allocatedTupleMemory() const {
    if (m_wrapper) {
        return m_wrapper->allocatedByteCount();
    }
    return 0;
}

/**
 * Get the current offset in bytes of the export stream for this Table
 * since startup.
 */
void StreamedTable::getExportStreamPositions(int64_t &seqNo, size_t &streamBytesUsed) {
    seqNo = m_sequenceNo;
    streamBytesUsed = m_wrapper->bytesUsed();
}

/**
 * Set the current offset in bytes of the export stream for this Table
 * since startup (used for rejoin/recovery).
 */
void StreamedTable::setExportStreamPositions(int64_t seqNo, size_t streamBytesUsed) {
    // assume this only gets called from a fresh rejoined node
    assert(m_sequenceNo == 0);
    m_sequenceNo = seqNo;
    m_wrapper->setBytesUsed(streamBytesUsed);
}
