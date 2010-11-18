/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB Inc.
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

#include "streamedtable.h"
#include "StreamedTableUndoAction.hpp"
#include "TupleStreamWrapper.h"
#include "common/executorcontext.hpp"
#include "tableiterator.h"

using namespace voltdb;

StreamedTable::StreamedTable(ExecutorContext *ctx, bool exportEnabled)
    : Table(100), stats_(this), m_executorContext(ctx), m_wrapper(NULL),
      m_sequenceNo(0)
{
    // In StreamedTable, a non-null m_wrapper implies export enabled.
    if (exportEnabled) {
        m_wrapper = new TupleStreamWrapper(m_executorContext->m_partitionId,
                                           m_executorContext->m_siteId,
                                           m_executorContext->m_lastTickTime);
    }
}

StreamedTable::StreamedTable(int tableAllocationTargetSize)
    : Table(tableAllocationTargetSize), stats_(this),
      m_executorContext(NULL), m_wrapper(NULL), m_sequenceNo(0)
{
    throwFatalException("Must provide executor context to streamed table constructor.");
}

StreamedTable *
StreamedTable::createForTest(size_t wrapperBufSize, ExecutorContext *ctx) {
    StreamedTable * st = new StreamedTable(ctx, true);
    st->m_wrapper->setDefaultCapacity(wrapperBufSize);
    return st;
}


StreamedTable::~StreamedTable()
{
    delete m_wrapper;
}

TableIterator StreamedTable::iterator() {
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

bool StreamedTable::insertTuple(TableTuple &source)
{
    size_t mark = 0;
    if (m_wrapper) {
        mark = m_wrapper->appendTuple(m_executorContext->m_lastCommittedTxnId,
                                      m_executorContext->currentTxnId(),
                                      m_sequenceNo++,
                                      m_executorContext->currentTxnTimestamp(),
                                      source,
                                      TupleStreamWrapper::INSERT);
        m_tupleCount++;

        UndoQuantum *uq = m_executorContext->getCurrentUndoQuantum();
        Pool *pool = uq->getDataPool();
        StreamedTableUndoAction *ua =
          new (pool->allocate(sizeof(StreamedTableUndoAction)))
          StreamedTableUndoAction(this, mark);
        uq->registerUndoAction(ua);
    }
    return true;
}

bool StreamedTable::updateTuple(TableTuple &source, TableTuple &target, bool updatesIndexes)
{
    throwFatalException("May not update a streamed table.");
}

bool StreamedTable::deleteTuple(TableTuple &tuple, bool deleteAllocatedStrings)
{
    size_t mark = 0;
    if (m_wrapper) {
        mark = m_wrapper->appendTuple(m_executorContext->m_lastCommittedTxnId,
                                      m_executorContext->currentTxnId(),
                                      m_sequenceNo++,
                                      m_executorContext->currentTxnTimestamp(),
                                      tuple,
                                      TupleStreamWrapper::DELETE);
        m_tupleCount++;

        UndoQuantum *uq = m_executorContext->getCurrentUndoQuantum();
        Pool *pool = uq->getDataPool();
        StreamedTableUndoAction *ua =
          new (pool->allocate(sizeof(StreamedTableUndoAction)))
          StreamedTableUndoAction(this, mark);
        uq->registerUndoAction(ua);
    }
    return true;
}

void StreamedTable::loadTuplesFrom(bool, SerializeInput&, Pool*)
{
    throw SerializableEEException(VOLT_EE_EXCEPTION_TYPE_EEEXCEPTION,
                                  "May not update a streamed table.");
}

void StreamedTable::flushOldTuples(int64_t timeInMillis)
{
    if (m_wrapper) {
        m_wrapper->periodicFlush(timeInMillis,
                                 m_executorContext->m_lastTickTime,
                                 m_executorContext->m_lastCommittedTxnId,
                                 m_executorContext->currentTxnId());
    }
}

StreamBlock*
StreamedTable::getCommittedExportBytes()
{
    if (m_wrapper)
    {
        return m_wrapper->getCommittedExportBytes();
    }
    return NULL;
}

bool
StreamedTable::releaseExportBytes(int64_t releaseOffset)
{
    if (m_wrapper)
    {
        return m_wrapper->releaseExportBytes(releaseOffset);
    }
    return false;
}

void
StreamedTable::resetPollMarker()
{
    if (m_wrapper)
    {
        m_wrapper->resetPollMarker();
    }
}

void StreamedTable::undo(size_t mark)
{
    if (m_wrapper) {
        m_wrapper->rollbackTo(mark);
    }
}

voltdb::TableStats *StreamedTable::getTableStats() {
    return &stats_;
}
