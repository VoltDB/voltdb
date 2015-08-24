/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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
#include "StreamedTableUndoAction.hpp"
#include "ExportTupleStream.h"
#include "common/executorcontext.hpp"
#include "tableiterator.h"

using namespace voltdb;

StreamedTable::StreamedTable(bool exportEnabled)
    : Table(1), stats_(this), m_executorContext(ExecutorContext::getExecutorContext()), m_wrapper(NULL),
      m_sequenceNo(0)
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

StreamedTable::~StreamedTable()
{
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
        m_wrapper->rollbackTo(mark, 0);
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
