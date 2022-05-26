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

#include "storage/ExportTupleStream.h"
#include "execution/VoltDBEngine.h"
#include "common/TupleSchema.h"
#include "common/TxnEgo.h"
#include "common/UniqueId.hpp"

#include <cstdio>
#include <limits>
#include <ctime>
#include <utility>
#include <math.h>

using namespace std;
using namespace voltdb;

ExportTupleStream::ExportTupleStream(CatalogId partitionId,
                                     int64_t siteId,
                                     int64_t generation,
                                     const std::string &tableName)
    : TupleStreamBase(EL_BUFFER_SIZE, s_EXPORT_BUFFER_HEADER_SIZE),
      m_partitionId(partitionId),
      m_siteId(siteId),
      m_generationIdCreated(generation),
      m_tableName(tableName),
      m_nextSequenceNumber(1),
      m_committedSequenceNumber(0),
      m_flushPending(false),
      m_nextFlushStream(NULL),
      m_prevFlushStream(NULL)
{}

void ExportTupleStream::setGenerationIdCreated(int64_t generation) {
    // If stream is initialized first with the current generation ID, it may
    // move backward after restoring from snapshot digest. However it should
    // never go forward.
    vassert(generation <= m_generationIdCreated);
    m_generationIdCreated = generation;
}

/*
 * Serialize the supplied tuple in to the stream.
 * Return m_uso before this invocation - this marks the point
 * in the stream the caller can rollback to if this append
 * should be rolled back.
 */
size_t ExportTupleStream::appendTuple(
        VoltDBEngine* engine,
        int64_t txnId,
        int64_t seqNo,
        int64_t uniqueId,
        const TableTuple &tuple,
        int partitionColumn,
        ExportTupleStream::STREAM_ROW_TYPE type)
{
    size_t streamHeaderSz = 0;
    size_t tupleMaxLength = 0;

    // Transaction IDs for transactions applied to this tuple stream
    // should always be moving forward in time.
    if (txnId < m_openTxnId)
    {
        throwFatalException(
                "Active transactions moving backwards: openTxnId is %jd, while the append txnId is %jd",
                (intmax_t)m_openTxnId, (intmax_t)txnId);
    }
    m_openTxnId = txnId;
    m_openUniqueId = uniqueId;

    // Compute the upper bound on bytes required to serialize tuple.
    // exportxxx: can memoize this calculation.
    tupleMaxLength = computeOffsets(tuple, &streamHeaderSz);
    //First time always include schema.
    if (!m_currBlock) {
        extendBufferChain(m_defaultCapacity);
    }
    if ((m_currBlock->remaining() < tupleMaxLength)) {
        //If we can not fit the data get a new block with size that includes schemaSize as well.
        extendBufferChain(tupleMaxLength);
    }

    // initialize the full row header to 0. This also
    // has the effect of setting each column non-null.
    ::memset(m_currBlock->mutableDataPtr(), 0, streamHeaderSz);

    // the nullarray lives in rowheader after the 4 byte header length prefix + 4 bytes for column count +
    // 4 partition index
    uint8_t *nullArray =
      reinterpret_cast<uint8_t*>(m_currBlock->mutableDataPtr()
              + sizeof(int32_t)         // row length
              + sizeof(int32_t)         // partition index
              + sizeof(int32_t)         // column count
              );

    // position the serializer after the full rowheader
    ExportSerializeOutput io(m_currBlock->mutableDataPtr() + streamHeaderSz, m_currBlock->remaining() - streamHeaderSz);

    // write metadata columns - data we always write this.
    io.writeLong(txnId);
    io.writeLong(UniqueId::ts(uniqueId));
    io.writeLong(seqNo);
    // Report the TxnId partition ID
    io.writeLong(TxnEgo::getPartitionId(txnId));
    io.writeLong(m_siteId);
    // use 1 for INSERT EXPORT op
    io.writeByte(static_cast<int8_t>(type));
    // write the tuple's data
    tuple.serializeToExport(io, METADATA_COL_CNT, nullArray);

    // row size, generation, partition-index, column count and hasSchema flag (byte)
    ExportSerializeOutput hdr(m_currBlock->mutableDataPtr(), streamHeaderSz);
    // write the row size in to the row header rowlength does not include
    // the 4 byte row header but does include the null array.
    hdr.writeInt((int32_t)(io.position()) + (int32_t)streamHeaderSz - 4);
    hdr.writeInt(METADATA_COL_CNT + partitionColumn);           // partition index
    hdr.writeInt(METADATA_COL_CNT + tuple.columnCount());      // column count

    vassert(seqNo > 0 && m_nextSequenceNumber == seqNo);

    return recordTupleAppended(streamHeaderSz + io.position(), uniqueId);;
}

void ExportTupleStream::appendToList(ExportTupleStream** oldest, ExportTupleStream** newest)
{
    vassert(!m_prevFlushStream && !m_nextFlushStream);
    if (*oldest == NULL) {
        *oldest = this;
    }
    else {
        m_prevFlushStream = *newest;
        m_prevFlushStream->m_nextFlushStream = this;
    }
    *newest = this;
}

void ExportTupleStream::stitchToNextNode(ExportTupleStream* next)
{
    m_nextFlushStream = next;
    next->m_prevFlushStream = this;
}

void ExportTupleStream::removeFromFlushList(VoltDBEngine* engine, bool moveToTail)
{
    if (m_flushPending) {
        if (m_nextFlushStream) {
            // We are not at the tail so move this stream to the tail.
            if (m_prevFlushStream) {
                // Remove myself from the middle of the flush list
                vassert(m_prevFlushStream->m_nextFlushStream == this);
                m_prevFlushStream->m_nextFlushStream = m_nextFlushStream;
                vassert(m_nextFlushStream->m_prevFlushStream == this);
                m_nextFlushStream->m_prevFlushStream = m_prevFlushStream;
            }
            else {
                // Remove myself from the beginning of the flush list
                m_nextFlushStream->m_prevFlushStream = NULL;
                *engine->getOldestExportStreamWithPendingRowsForAssignment() = m_nextFlushStream;
            }
            if (moveToTail) {
                m_prevFlushStream = *engine->getNewestExportStreamWithPendingRowsForAssignment();
                vassert(m_prevFlushStream->m_nextFlushStream == NULL);
                m_prevFlushStream->m_nextFlushStream = this;
                *engine->getNewestExportStreamWithPendingRowsForAssignment() = this;
            }
            else {
                m_prevFlushStream = NULL;
                m_flushPending = false;
            }
            m_nextFlushStream = NULL;
        }
        else {
            // If this node is at the end of the list do nothing
            vassert(*engine->getNewestExportStreamWithPendingRowsForAssignment() == this);
            vassert(m_nextFlushStream == NULL);
            if (!moveToTail) {
                // Removing the end Node
                if (m_prevFlushStream) {
                    m_prevFlushStream->m_nextFlushStream = NULL;
                    *engine->getNewestExportStreamWithPendingRowsForAssignment() = m_prevFlushStream;
                    m_prevFlushStream = NULL;
                }
                else {
                    // End node is also the beginning node
                    *engine->getOldestExportStreamWithPendingRowsForAssignment() = NULL;
                    *engine->getNewestExportStreamWithPendingRowsForAssignment() = NULL;
                }
                m_flushPending = false;
            }
        }
    }
    else {
        if (moveToTail) {
            appendToList(engine->getOldestExportStreamWithPendingRowsForAssignment(),
                    engine->getNewestExportStreamWithPendingRowsForAssignment());
            m_flushPending = true;
        }
    }
}

/*
 * Handoff fully committed blocks to the top end.
 */
void ExportTupleStream::commit(VoltDBEngine* engine, int64_t currentTxnId, int64_t uniqueId)
{
    vassert(currentTxnId == m_openTxnId && uniqueId == m_openUniqueId);

    if (m_uso != m_committedUso) {
        m_committedUso = m_uso;
        m_committedUniqueId = m_openUniqueId;
        // Advance the tip to the new transaction.
        m_committedTxnId = m_openTxnId;
        if (m_currBlock->startSequenceNumber() > m_committedSequenceNumber) {
            // Started a new block so reset the flush timeout
            m_lastFlush = UniqueId::tsInMillis(m_committedUniqueId);
            removeFromFlushList(engine, true);
        }
        m_committedSequenceNumber = m_nextSequenceNumber-1;
        m_currBlock->setCommittedSequenceNumber(m_committedSequenceNumber);
        m_currBlock->recordLastCommittedSpHandle(m_committedTxnId);

        pushPendingBlocks();
    }
}

size_t
ExportTupleStream::computeOffsets(const TableTuple &tuple, size_t *streamHeaderSz) const {
    // round-up columncount to next multiple of 8 and divide by 8
    int columnCount = tuple.columnCount() + METADATA_COL_CNT;
    int nullMaskLength = ((columnCount + 7) & -8) >> 3;

    // tuple stream header
    *streamHeaderSz = sizeof (int32_t)      // row size
            + sizeof (int32_t)           // partition index
            + sizeof (int32_t)           // column count
            + nullMaskLength;           // null array


    size_t dataSz = 0;
    if (!tuple.areAllCollumnsVarAndNull() && (dataSz = tuple.maxExportSerializationSize()) == 0) {
        // if all columns are null value var-length type, serialization size would be 0
        // otherwise 0 size indicate corrupt tuple detected
        throwFatalException("Invalid tuple passed to computeTupleMaxLength. Crashing System.");
    }
    //Data size for metadata columns.
    dataSz += (5 * sizeof(int64_t)) + 1;

    return *streamHeaderSz              // row header
            + dataSz;                   // non-null tuple data
}

void ExportTupleStream::pushStreamBuffer(ExportStreamBlock *block) {
    ExecutorContext::getPhysicalTopend()->pushExportBuffer(
                    m_partitionId,
                    m_tableName,
                    block);
}

/*
 * Create a new buffer and flush all pending committed data.
 * Creating a new buffer will push all queued data into the
 * pending list for commit to operate against.
 */
bool ExportTupleStream::periodicFlush(int64_t timeInMillis,
                                      int64_t lastCommittedTxnId)
{
    // negative timeInMillis instructs a mandatory flush
    vassert(timeInMillis < 0 || (timeInMillis - m_lastFlush > s_exportFlushTimeout));
    if (!m_currBlock || m_currBlock->lastSequenceNumber() > m_committedSequenceNumber) {
        // There is no buffer or the (MP) transaction has not been committed to the buffer yet
        // so don't release the buffer yet
        if (timeInMillis < 0) {
            // Send a null buffer
            pushStreamBuffer(NULL);
        }
        return false;
    }

    if (m_currBlock) {
        // Note that if the block is empty the lastSequenceNumber will be startSequenceNumber-1;
        vassert(m_currBlock->lastSequenceNumber() == m_committedSequenceNumber);
        // Any blocks before the current block should have been sent already by the commit path
        vassert(m_pendingBlocks.empty());
        if (m_flushPending) {
            vassert(m_currBlock->getRowCount() > 0);
            // Most paths move a block to m_pendingBlocks and then use pushPendingBlocks (comment from there)
            // The block is handed off to the topend which is responsible for releasing the
            // memory associated with the block data.
            m_currBlock->writeOutHeader();
            pushStreamBuffer(m_currBlock);
            m_currBlock = NULL;
            extendBufferChain(0);
            m_prevFlushStream = NULL;
            m_nextFlushStream = NULL;
            m_flushPending = false;
        } else {
            if (timeInMillis < 0) {
                pushStreamBuffer(NULL);
            }
        }

        return true;
    }
    // periodicFlush should only be called if this ExportTupleStream was registered with the engine by commit()
    vassert(false);
    return false;
}

void ExportTupleStream::extendBufferChain(size_t minLength) {
    size_t blockSize = (minLength <= m_defaultCapacity) ? m_defaultCapacity : m_maxCapacity;
    TupleStreamBase::commonExtendBufferChain(blockSize, m_uso);

    m_currBlock->recordStartSequenceNumber(m_nextSequenceNumber);
}

size_t ExportTupleStream::getExportMetaHeaderSize() {
    return EXPORT_ROW_HEADER_SIZE + EXPORT_BUFFER_METADATA_HEADER_SIZE;
}

