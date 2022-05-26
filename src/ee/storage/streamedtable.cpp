/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
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
#include <common/debuglog.h>
#include <cstdio>
#include <sstream>

using namespace voltdb;

StreamedTable::StreamedTable(int partitionColumn, bool isReplicated)
    : ViewableAndReplicableTable(1, partitionColumn, isReplicated)
    , m_stats(this)
    , m_wrapper(NULL)
    , m_sequenceNo(0)
{
}

StreamedTable::StreamedTable(ExportTupleStream *wrapper, int partitionColumn, bool isReplicated)
    : ViewableAndReplicableTable(1, partitionColumn, isReplicated)
    , m_stats(this)
    , m_wrapper(wrapper)
    , m_sequenceNo(0)
{
}

StreamedTable *
StreamedTable::createForTest(size_t wrapperBufSize, ExecutorContext *ctx,
    TupleSchema *schema, std::string tableName, std::vector<std::string> & columnNames) {
    StreamedTable * st = new StreamedTable(-1, true);
    st->m_name = tableName;
    st->m_wrapper = new ExportTupleStream(ctx->m_partitionId, ctx->m_siteId, 0, st->m_name);
    st->initializeWithColumns(schema, columnNames, false, wrapperBufSize);
    st->m_wrapper->setDefaultCapacityForTest(wrapperBufSize);
    return st;
}

StreamedTable::~StreamedTable() {
    //When stream is dropped its wrapper is kept safe in pending list until tick or push pushes all buffers and deleted there after.
    if (m_wrapper) {
        delete m_wrapper;
    }
}

// Stream writes were done so commit all the writes
void StreamedTable::notifyQuantumRelease() {
    if (m_wrapper) {
        if (m_migrateTxnSizeGuard.undoToken == getLastSeenUndoToken()) {
            m_migrateTxnSizeGuard.reset();
        }
        m_wrapper->commit(m_executorContext->getContextEngine(), getTableTxnId(), m_executorContext->currentUniqueId());
    }
}

TableIterator  StreamedTable::iterator() {
    throw SerializableEEException("May not iterate a streamed table.");
}

TableIterator StreamedTable::iteratorDeletingAsWeGo() {
    throw SerializableEEException("May not iterate a streamed table.");
}

void StreamedTable::deleteAllTuples()
{
    throw SerializableEEException("May not delete all tuples of a streamed table.");
}

TBPtr StreamedTable::allocateNextBlock() {
    throw SerializableEEException("May not use block alloc interface with streamed tables.");
}

void StreamedTable::nextFreeTuple(TableTuple *) {
    throw SerializableEEException("May not use nextFreeTuple with streamed tables.");
}

void StreamedTable::streamTuple(TableTuple &source, ExportTupleStream::STREAM_ROW_TYPE type, AbstractDRTupleStream *drStream) {
    if (m_executorContext->externalStreamsEnabled()) {
        int64_t currSequenceNo = ++m_sequenceNo;
        vassert(m_columnNames.size() == source.columnCount());
        size_t mark = m_wrapper->appendTuple(m_executorContext->getContextEngine(),
                                      getTableTxnId(),
                                      currSequenceNo,
                                      m_executorContext->currentUniqueId(),
                                      source,
                                      partitionColumn(),
                                      type);

        UndoQuantum *uq = m_executorContext->getCurrentUndoQuantum();
        if (!uq) {
            // With no active UndoLog, there is no undo support.
            return;
        }
        uq->registerUndoAction(new (*uq) StreamedTableUndoAction(this, mark, currSequenceNo), this);
        if (drStream != NULL) {
            if (m_migrateTxnSizeGuard.undoToken == 0L) {
                // The buffer size includes the row length and null array, as DR buffer also has those.
                int64_t rawExportBufSize =
                        m_wrapper->getUso() - mark - ExportTupleStream::getExportMetaHeaderSize();
                m_migrateTxnSizeGuard.undoToken = uq->getUndoToken();
                m_migrateTxnSizeGuard.estimatedDRLogSize += rawExportBufSize + DRTupleStream::getDRLogHeaderSize();
            } else {
                // The buffer size includes the row length and null array, as DR buffer also has those.
                int64_t rawExportBufSize =
                        m_wrapper->getUso() - m_migrateTxnSizeGuard.uso - ExportTupleStream::getExportMetaHeaderSize();
                m_migrateTxnSizeGuard.estimatedDRLogSize += rawExportBufSize + DRTupleStream::getDRLogHeaderSize();
            }
            m_migrateTxnSizeGuard.uso = m_wrapper->getUso();
            if (m_migrateTxnSizeGuard.estimatedDRLogSize >= voltdb::SECONDARY_BUFFER_SIZE) {
                throw SerializableEEException("Migrate transaction failed, exceeding 50MB DR buffer size limit.");
            }
        }
    }
}

bool StreamedTable::insertTuple(TableTuple &source)
{
    // not null checks at first
    FAIL_IF(!checkNulls(source)) {
        throw ConstraintFailureException(this, source, TableTuple(), CONSTRAINT_TYPE_NOT_NULL);
    }

    // handle any materialized views
    for (int i = 0; i < m_views.size(); i++) {
        m_views[i]->processTupleInsert(source, true);
    }

    if (m_wrapper) {
        streamTuple(source, ExportTupleStream::INSERT, NULL);
    }
    return true;
}

void StreamedTable::loadTuplesFrom(SerializeInputBE&, Pool*) {
    throw SerializableEEException("May not update a streamed table.");
}

void StreamedTable::flushOldTuples(int64_t timeInMillis) {
    if (m_wrapper) {
        m_wrapper->periodicFlush(timeInMillis,
                                 m_executorContext->m_lastCommittedSpHandle);
    }
}

void StreamedTable::undo(size_t mark, int64_t seqNo) {
    if (m_wrapper) {
        vassert(seqNo == m_sequenceNo);
        m_wrapper->rollbackExportTo(mark, seqNo);
        if (getLastSeenUndoToken() == m_migrateTxnSizeGuard.undoToken) {
            m_migrateTxnSizeGuard.estimatedDRLogSize -=
                    m_migrateTxnSizeGuard.uso - mark -
                    ExportTupleStream::getExportMetaHeaderSize() + DRTupleStream::getDRLogHeaderSize();
            vassert(m_migrateTxnSizeGuard.estimatedDRLogSize >= 0);
            m_migrateTxnSizeGuard.uso = mark;
            if (m_migrateTxnSizeGuard.estimatedDRLogSize == 0) {
                m_migrateTxnSizeGuard.reset();
            }
        }
        //Decrementing the sequence number should make the stream of tuples
        //contiguous outside of actual system failures. Should be more useful
        //than having gaps.
        m_sequenceNo--;
    }
}

size_t StreamedTable::allocatedBlockCount() const {
    return 0;
}

int64_t StreamedTable::allocatedTupleMemory() const {
    return 0;
}

/**
 * Get the current offset in bytes of the export stream for this Table
 * since startup.
 */
void StreamedTable::getExportStreamPositions(int64_t &seqNo, size_t &streamBytesUsed, int64_t &genIdCreated) {
    seqNo = m_sequenceNo;
    if (m_wrapper) {
        streamBytesUsed = m_wrapper->bytesUsed();
        genIdCreated = m_wrapper->getGenerationIdCreated();
    }
}

/**
 * Set the current offset in bytes of the export stream for this Table
 * since startup (used for rejoin/recovery).
 */
void StreamedTable::setExportStreamPositions(int64_t seqNo, size_t streamBytesUsed, int64_t generationIdCreated) {
    // assume this only gets called from a fresh rejoined node or after the reset of a wrapper
    vassert(m_sequenceNo == 0 || seqNo == 0);
    m_sequenceNo = seqNo;
    if (m_wrapper) {
        m_wrapper->setBytesUsed(seqNo, streamBytesUsed);
        m_wrapper->setGenerationIdCreated(generationIdCreated);
    }
}
