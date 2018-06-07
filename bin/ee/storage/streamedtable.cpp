/* This file is part of VoltDB.
 * Copyright (C) 2008-2018 VoltDB Inc.
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

#include "streamedtable.h"

#include "ExportTupleStream.h"
#include "MaterializedViewTriggerForInsert.h"
#include "StreamedTableUndoAction.hpp"
#include "tableiterator.h"

#include "catalog/materializedviewinfo.h"
#include "common/executorcontext.hpp"
#include "common/FailureInjection.h"
#include "ConstraintFailureException.h"

#include <boost/foreach.hpp>
#include <boost/scoped_ptr.hpp>

#include <algorithm>    // std::find
#include <cassert>
#include <cstdio>
#include <sstream>

using namespace voltdb;

StreamedTable::StreamedTable(int partitionColumn)
    : Table(1)
    , m_stats(this)
    , m_executorContext(ExecutorContext::getExecutorContext())
    , m_wrapper(NULL)
    , m_sequenceNo(0)
    , m_partitionColumn(partitionColumn)
{
}

StreamedTable::StreamedTable(ExportTupleStream *wrapper, int partitionColumn)
    : Table(1)
    , m_stats(this)
    , m_executorContext(ExecutorContext::getExecutorContext())
    , m_wrapper(wrapper)
    , m_sequenceNo(0)
    , m_partitionColumn(partitionColumn)
{
}

StreamedTable *
StreamedTable::createForTest(size_t wrapperBufSize, ExecutorContext *ctx,
    TupleSchema *schema, std::vector<std::string> & columnNames) {
    StreamedTable * st = new StreamedTable();
    st->m_wrapper = new ExportTupleStream(ctx->m_partitionId,
                                           ctx->m_siteId, 0, "sign");
    st->initializeWithColumns(schema, columnNames, false, wrapperBufSize);
    st->m_wrapper->setDefaultCapacityForTest(wrapperBufSize);
    return st;
}

/*
 * claim ownership of a view. table is responsible for this view*
 */
void StreamedTable::addMaterializedView(MaterializedViewTriggerForStreamInsert* view) {
    m_views.push_back(view);
}

void StreamedTable::dropMaterializedView(MaterializedViewTriggerForStreamInsert* targetView) {
    assert( ! m_views.empty());
    MaterializedViewTriggerForStreamInsert* lastView = m_views.back();
    if (targetView != lastView) {
        // iterator to vector element:
        std::vector<MaterializedViewTriggerForStreamInsert*>::iterator toView = find(m_views.begin(), m_views.end(), targetView);
        assert(toView != m_views.end());
        // Use the last view to patch the potential hole.
        *toView = lastView;
    }
    // The last element is now excess.
    m_views.pop_back();
    delete targetView;
}

StreamedTable::~StreamedTable() {
    // note this class has ownership of the views, even if they
    // were allocated by VoltDBEngine
    for (int i = 0; i < m_views.size(); i++) {
        delete m_views[i];
    }
    //When stream is dropped its wrapper is kept safe in pending list until tick or push pushes all buffers and deleted there after.
    if (m_wrapper) {
        delete m_wrapper;
    }
}

TableIterator  StreamedTable::iterator() {
    throw SerializableEEException(VOLT_EE_EXCEPTION_TYPE_EEEXCEPTION,
                                  "May not iterate a streamed table.");
}

TableIterator StreamedTable::iteratorDeletingAsWeGo() {
    throw SerializableEEException(VOLT_EE_EXCEPTION_TYPE_EEEXCEPTION,
                                  "May not iterate a streamed table.");
}

void StreamedTable::deleteAllTuples(bool freeAllocatedStrings, bool fallible)
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
    // not null checks at first
    FAIL_IF(!checkNulls(source)) {
        throw ConstraintFailureException(this, source, TableTuple(), CONSTRAINT_TYPE_NOT_NULL);
    }

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
                                      name(),
                                      source,
                                      getColumnNames(),
                                      partitionColumn(),
                                      ExportTupleStream::INSERT);
        m_tupleCount++;
        UndoQuantum *uq = m_executorContext->getCurrentUndoQuantum();
        if (!uq) {
            // With no active UndoLog, there is no undo support.
            return true;
        }
        uq->registerUndoAction(new (*uq) StreamedTableUndoAction(this, mark));
    }
    else {
        // handle any materialized views even though we dont have any connector.
        for (int i = 0; i < m_views.size(); i++) {
            m_views[i]->processTupleInsert(source, true);
        }
    }
    return true;
}

void StreamedTable::loadTuplesFrom(SerializeInputBE&, Pool*) {
    throw SerializableEEException(VOLT_EE_EXCEPTION_TYPE_EEEXCEPTION,
                                  "May not update a streamed table.");
}

void StreamedTable::flushOldTuples(int64_t timeInMillis) {
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

void StreamedTable::undo(size_t mark) {
    if (m_wrapper) {
        m_wrapper->rollbackTo(mark, SIZE_MAX);
        //Decrementing the sequence number should make the stream of tuples
        //contiguous outside of actual system failures. Should be more useful
        //then having gaps.
        m_sequenceNo--;
    }
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
    if (m_wrapper) {
        streamBytesUsed = m_wrapper->bytesUsed();
    }
}

/**
 * Set the current offset in bytes of the export stream for this Table
 * since startup (used for rejoin/recovery).
 */
void StreamedTable::setExportStreamPositions(int64_t seqNo, size_t streamBytesUsed) {
    // assume this only gets called from a fresh rejoined node
    assert(m_sequenceNo == 0);
    m_sequenceNo = seqNo;
    if (m_wrapper) {
        m_wrapper->setBytesUsed(streamBytesUsed);
    }
}
