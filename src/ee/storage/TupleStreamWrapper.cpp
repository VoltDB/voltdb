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

#include "storage/TupleStreamWrapper.h"

#include "common/TupleSchema.h"
#include "common/types.h"
#include "common/NValue.hpp"
#include "common/ValuePeeker.hpp"
#include "common/tabletuple.h"
#include "common/ExportSerializeIo.h"

#include <cstdio>
#include <iostream>
#include <cassert>
#include <ctime>
#include <utility>
#include <math.h>

using namespace std;
using namespace voltdb;

const int METADATA_COL_CNT = 6;
const int MAX_BUFFER_AGE = 4000;

TupleStreamWrapper::TupleStreamWrapper(CatalogId partitionId,
                                       CatalogId siteId,
                                       int64_t lastFlush)
    : m_partitionId(partitionId), m_siteId(siteId),
      m_lastFlush(lastFlush), m_defaultCapacity(EL_BUFFER_SIZE),
      m_uso(0), m_currBlock(NULL), m_fakeBlock(NULL),
      m_openTransactionId(0), m_openTransactionUso(0),
      m_committedTransactionId(0), m_committedUso(0), m_firstUnpolledUso(0)
{
    assert(lastFlush > -1);
    extendBufferChain(m_defaultCapacity);
}

void
TupleStreamWrapper::setDefaultCapacity(size_t capacity)
{
    assert (capacity > 0);
    if (m_uso != 0 || m_openTransactionId != 0 ||
        m_openTransactionUso != 0 || m_committedTransactionId != 0 ||
        m_committedUso != 0 || m_firstUnpolledUso != 0)
    {
        throwFatalException("setDefaultCapacity only callable before "
                            "TupleStreamWrapper is used");
    }
    cleanupManagedBuffers();
    m_defaultCapacity = capacity;
    extendBufferChain(m_defaultCapacity);
}



/*
 * Essentially, shutdown.
 */
void TupleStreamWrapper::cleanupManagedBuffers()
{
    StreamBlock *sb = NULL;

    delete m_currBlock;
    m_currBlock = NULL;

    delete m_fakeBlock;
    m_fakeBlock = NULL;

    while (m_pendingBlocks.empty() != true) {
        sb = m_pendingBlocks.front();
        m_pendingBlocks.pop_front();
        delete sb;
    }

    while (m_freeBlocks.empty() != true) {
        sb = m_freeBlocks.front();
        m_freeBlocks.pop_front();
        delete sb;
    }
}

/*
 * Handoff fully committed blocks to the top end.
 *
 * This is the only function that should modify m_openTransactionId,
 * m_openTransactionUso.
 */
void TupleStreamWrapper::commit(int64_t lastCommittedTxnId, int64_t currentTxnId)
{
    if (currentTxnId < m_openTransactionId)
    {
        throwFatalException("Transactions moving backwards");
    }

    // more data for an ongoing transaction with no new committed data
    if ((currentTxnId == m_openTransactionId) &&
        (lastCommittedTxnId == m_committedTransactionId))
    {
        return;
    }

    // If the current TXN ID has advanced, then we know that:
    // - The old open transaction has been committed
    // - The current transaction is now our open transaction
    if (m_openTransactionId < currentTxnId)
    {
        m_committedUso = m_uso;
        // Advance the tip to the new transaction.
        m_committedTransactionId = m_openTransactionId;
        m_openTransactionId = currentTxnId;
    }

    // now check to see if the lastCommittedTxn tells us that our open
    // transaction should really be committed.  If so, update the
    // committed state.
    if (m_openTransactionId <= lastCommittedTxnId)
    {
        m_committedUso = m_uso;
        m_committedTransactionId = m_openTransactionId;
    }
}


/*
 * Discard all data with a uso gte mark
 */
void TupleStreamWrapper::rollbackTo(size_t mark)
{
    if (mark > m_uso) {
        throwFatalException("Truncating the future.");
    }

    // back up the universal stream counter
    m_uso = mark;

    // working from newest to oldest block, throw
    // away blocks that are fully after mark; truncate
    // the block that contains mark.
    if (!(m_currBlock->uso() >= mark)) {
        m_currBlock->truncateTo(mark);
    }
    else {
        StreamBlock *sb = NULL;
        discardBlock(m_currBlock);
        m_currBlock = NULL;
        while (m_pendingBlocks.empty() != true) {
            sb = m_pendingBlocks.back();
            m_pendingBlocks.pop_back();
            if (sb->uso() >= mark) {
                discardBlock(sb);
            }
            else {
                sb->truncateTo(mark);
                m_currBlock = sb;
                break;
            }
        }
    }
}

/*
 * Correctly release and delete a managed buffer that won't
 * be handed off
 */
void TupleStreamWrapper::discardBlock(StreamBlock *sb) {
    delete sb;
}

/*
 * Allocate another buffer, preserving the current buffer's content in
 * the pending queue.
 */
void TupleStreamWrapper::extendBufferChain(size_t minLength)
{
    if (m_defaultCapacity < minLength) {
        // exportxxx: rollback instead?
        throwFatalException("Default capacity is less than required buffer size.");
    }

    if (m_currBlock) {
        if (m_currBlock->offset() > 0) {
            m_pendingBlocks.push_back(m_currBlock);
            m_currBlock = NULL;
        }
        // fully discard empty blocks. makes valgrind/testcase
        // conclusion easier.
        else {
            discardBlock(m_currBlock);
            m_currBlock = NULL;
        }
    }

    char *buffer = new char[m_defaultCapacity];
    if (!buffer) {
        throwFatalException("Failed to claim managed buffer for Export.");
    }

    m_currBlock = new StreamBlock(buffer, m_defaultCapacity, m_uso);
}

/*
 * Create a new buffer and flush all pending committed data.
 * Creating a new buffer will push all queued data into the
 * pending list for commit to operate against.
 */
void
TupleStreamWrapper::periodicFlush(int64_t timeInMillis,
                                  int64_t lastTickTime,
                                  int64_t lastCommittedTxnId,
                                  int64_t currentTxnId)
{
    // negative timeInMillis instructs a mandatory flush
    if (timeInMillis < 0 || (timeInMillis - m_lastFlush > MAX_BUFFER_AGE)) {
        if (timeInMillis > 0) {
            m_lastFlush = timeInMillis;
        }

        extendBufferChain(0);
        commit(lastCommittedTxnId, currentTxnId);
    }
}

/*
 * If txnId represents a new transaction, commit previous data.
 * Always serialize the supplied tuple in to the stream.
 * Return m_uso before this invocation - this marks the point
 * in the stream the caller can rollback to if this append
 * should be rolled back.
 */
size_t TupleStreamWrapper::appendTuple(int64_t lastCommittedTxnId,
                                       int64_t txnId,
                                       int64_t seqNo,
                                       int64_t timestamp,
                                       TableTuple &tuple,
                                       TupleStreamWrapper::Type type)
{
    size_t rowHeaderSz = 0;
    size_t tupleMaxLength = 0;

    assert(txnId >= m_openTransactionId);
    commit(lastCommittedTxnId, txnId);

    // Compute the upper bound on bytes required to serialize tuple.
    // exportxxx: can memoize this calculation.
    tupleMaxLength = computeOffsets(tuple, &rowHeaderSz);
    if (!m_currBlock) {
        extendBufferChain(m_defaultCapacity);
    }

    if ((m_currBlock->offset() + tupleMaxLength) > m_defaultCapacity) {
        extendBufferChain(tupleMaxLength);
    }

    // initialize the full row header to 0. This also
    // has the effect of setting each column non-null.
    ::memset(m_currBlock->mutableDataPtr(), 0, rowHeaderSz);

    // the nullarray lives in rowheader after the 4 byte header length prefix
    uint8_t *nullArray =
      reinterpret_cast<uint8_t*>(m_currBlock->mutableDataPtr() + sizeof (int32_t));

    // position the serializer after the full rowheader
    ExportSerializeOutput io(m_currBlock->mutableDataPtr() + rowHeaderSz,
                             m_currBlock->remaining() - rowHeaderSz);

    // write metadata columns
    io.writeLong(txnId);
    io.writeLong(timestamp);
    io.writeLong(seqNo);
    io.writeLong(m_partitionId);
    io.writeLong(m_siteId);

    // use 1 for INSERT EXPORT op, 0 for DELETE EXPORT op
    io.writeLong((type == INSERT) ? 1L : 0L);

    // write the tuple's data
    tuple.serializeToExport(io, METADATA_COL_CNT, nullArray);

    // write the row size in to the row header
    // rowlength does not include the 4 byte row header
    // but does include the null array.
    ExportSerializeOutput hdr(m_currBlock->mutableDataPtr(), 4);
    hdr.writeInt((int32_t)(io.position()) + (int32_t)rowHeaderSz - 4);

    // update m_offset
    m_currBlock->consumed(rowHeaderSz + io.position());

    // update uso.
    const size_t startingUso = m_uso;
    m_uso += (rowHeaderSz + io.position());
    return startingUso;
}

size_t
TupleStreamWrapper::computeOffsets(TableTuple &tuple,
                                   size_t *rowHeaderSz)
{
    // round-up columncount to next multiple of 8 and divide by 8
    int columnCount = tuple.sizeInValues() + METADATA_COL_CNT;
    int nullMaskLength = ((columnCount + 7) & -8) >> 3;

    // row header is 32-bit length of row plus null mask
    *rowHeaderSz = sizeof (int32_t) + nullMaskLength;

    // metadata column width: 5 int64_ts plus CHAR(1).
    size_t metadataSz = (sizeof (int64_t) * 5) + 1;

    // returns 0 if corrupt tuple detected
    size_t dataSz = tuple.maxExportSerializationSize();
    if (dataSz == 0) {
        throwFatalException("Invalid tuple passed to computeTupleMaxLength. Crashing System.");
    }

    return *rowHeaderSz + metadataSz + dataSz;
}

void
TupleStreamWrapper::resetPollMarker()
{
    if (m_pendingBlocks.empty() != true) {
        StreamBlock *oldest_block = m_pendingBlocks.front();
        if (oldest_block != NULL) {
            m_firstUnpolledUso = oldest_block->unreleasedUso();
        }
    }
}

StreamBlock*
TupleStreamWrapper::getCommittedExportBytes()
{
    StreamBlock* first_unpolled_block = NULL;

    deque<StreamBlock*>::iterator pending_iter = m_pendingBlocks.begin();
    while (pending_iter != m_pendingBlocks.end())
    {
        StreamBlock* block = *pending_iter;
        // find the first block that has unpolled data
        if (m_firstUnpolledUso < (block->uso() + block->offset()))
        {
            // check that the entire remainder is committed
            if (m_committedUso >= (block->uso() + block->offset()))
            {
                first_unpolled_block = block;
                // find the value to update m_firstUnpolledUso
                m_firstUnpolledUso = block->uso() + block->offset();
            }
            else
            {
                // if the unpolled block is not committed,
                // -- construct a fake StreamBlock that makes no progress
                // --- unreleased USO of this block but offset of 0
                // don't advance the first unpolled USO
                delete m_fakeBlock;
                m_fakeBlock = new StreamBlock(0, 0, block->unreleasedUso());
                first_unpolled_block = m_fakeBlock;
            }
            break;
        }
        ++pending_iter;
    }

    // The first unpolled block wasn't found in the pending.
    // It had better be m_currBlock or we've got troubles
    // Since we're here, m_currBlock is not fully committed, so
    // we just want to create a fake block based on its metadata
    if (first_unpolled_block == NULL)
    {
        delete m_fakeBlock;
        m_fakeBlock = new StreamBlock(0, 0, m_currBlock->unreleasedUso());
        first_unpolled_block = m_fakeBlock;
    }

    // return the appropriate pointah
    return first_unpolled_block;
}

bool
TupleStreamWrapper::releaseExportBytes(int64_t releaseOffset)
{
    // if released offset is in an already-released past, just return success
    if ((m_pendingBlocks.empty() && releaseOffset < m_currBlock->uso()) ||
        (!m_pendingBlocks.empty() && releaseOffset < m_pendingBlocks.front()->uso()))
    {
        return true;
    }

    // if released offset is in the uncommitted bytes, then set up
    // to release everything that is committed
    if (releaseOffset > m_committedUso)
    {
        releaseOffset = m_committedUso;
    }

    bool retval = false;

    if (releaseOffset >= m_currBlock->uso())
    {
        while (m_pendingBlocks.empty() != true) {
            StreamBlock* sb = m_pendingBlocks.back();
            m_pendingBlocks.pop_back();
            discardBlock(sb);
        }
        m_currBlock->releaseUso(releaseOffset);
        retval = true;
    }
    else
    {
        StreamBlock* sb = m_pendingBlocks.front();
        while (!m_pendingBlocks.empty() && !retval)
        {
            if (releaseOffset >= sb->uso() + sb->offset())
            {
                m_pendingBlocks.pop_front();
                discardBlock(sb);
                sb = m_pendingBlocks.front();
            }
            else
            {
                sb->releaseUso(releaseOffset);
                retval = true;
            }
        }
    }

    if (retval)
    {
        if (m_firstUnpolledUso < releaseOffset)
        {
            m_firstUnpolledUso = releaseOffset;
        }
    }

    return retval;
}
