/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB L.L.C.
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

#include "common/Topend.h"
#include "common/TupleSchema.h"
#include "common/types.h"
#include "common/NValue.hpp"
#include "common/ValuePeeker.hpp"
#include "common/tabletuple.h"

#include <cstdio>
#include <iostream>
#include <cassert>
#include <ctime>
#include <math.h>

namespace voltdb {
const int METADATA_COL_CNT = 6;

TupleStreamWrapper::TupleStreamWrapper(CatalogId partitionId,
                                       CatalogId siteId,
                                       CatalogId tableId,
                                       Topend *topend,
                                       int64_t lastFlush)
    : m_partitionId(partitionId), m_siteId(siteId), m_tableId(tableId),
      m_topend(topend), m_lastFlush(lastFlush), m_defaultCapacity(EL_BUFFER_SIZE),
      m_uso(0), m_openTransactionId(0), m_openTransactionUso(0), m_currBlock(NULL),
      m_cachedBlockHeader(NULL), m_currBlockTupleCount(0)
{
    assert(topend);
    assert(lastFlush > -1);
}

void TupleStreamWrapper::cacheBlockHeader(TupleSchema &schema) {

    // 12 byte signature
    //  4 byte header length
    //  2 byte version
    //  1 byte sync marker length (0)
    //  2 byte column count
    //  4 byte column width (per column)
    uint32_t totalColumns = schema.columnCount() + METADATA_COL_CNT;
    delete[] m_cachedBlockHeader;
    m_cachedBlockHeaderSize = 21 + 4 * totalColumns;
    m_cachedBlockHeader = new char[m_cachedBlockHeaderSize];

    // header
    m_cachedBlockHeader[0]  = 'V';
    m_cachedBlockHeader[1]  = 'B';
    m_cachedBlockHeader[2]  = 'I';
    m_cachedBlockHeader[3]  = 'N';
    m_cachedBlockHeader[4]  = 'A';
    m_cachedBlockHeader[5]  = 'R';
    m_cachedBlockHeader[6]  = 'Y';
    m_cachedBlockHeader[7]  = '\n';
    m_cachedBlockHeader[8]  = '\377';
    m_cachedBlockHeader[9]  = '\r';
    m_cachedBlockHeader[10] = '\n';
    m_cachedBlockHeader[11] = '\0';

    // length of version, sync marker, column count and column widths
    uint32_t varHeaderSize = 5 + 4 * totalColumns;
    *(reinterpret_cast<uint32_t*>(&m_cachedBlockHeader[12])) = htonl(varHeaderSize);

    // serialization version
    *(reinterpret_cast<uint16_t*>(&m_cachedBlockHeader[16])) = htons(1);

    // sync marker length. sync markers are not implemented.
    m_cachedBlockHeader[18] = 0;

    // column count
    *(reinterpret_cast<uint16_t*>(&m_cachedBlockHeader[19])) = htons(totalColumns);

    // metadata col. widths: txnid, timestamp, seqNo, partitionId, siteId, 'I/D'
    *(reinterpret_cast<uint32_t*>(&m_cachedBlockHeader[21])) = htonl(sizeof(int64_t));
    *(reinterpret_cast<uint32_t*>(&m_cachedBlockHeader[25])) = htonl(sizeof(int64_t));
    *(reinterpret_cast<uint32_t*>(&m_cachedBlockHeader[29])) = htonl(sizeof(int64_t));
    *(reinterpret_cast<uint32_t*>(&m_cachedBlockHeader[33])) = htonl(sizeof(int64_t));
    *(reinterpret_cast<uint32_t*>(&m_cachedBlockHeader[37])) = htonl(sizeof(int64_t));
    *(reinterpret_cast<uint32_t*>(&m_cachedBlockHeader[41])) = htonl(sizeof(char));

    // tuple schema column widths
    for (int i=0; i < schema.columnCount(); ++i) {
        if (schema.columnType(i) == VALUE_TYPE_VARCHAR ||
            schema.columnType(i) == VALUE_TYPE_DECIMAL)
        {
            // length preceded fields have -1 column width per specification
            *(reinterpret_cast<int32_t*>(&m_cachedBlockHeader[45 + i * 4]))
              = 0xFFFFFF;
        }
        else
        {
            // otherwise, all integer value are serialized as int64.
            *(reinterpret_cast<uint32_t*>(&m_cachedBlockHeader[45 + i * 4]))
              = htonl(sizeof(int64_t));
        }
    }
}


void TupleStreamWrapper::cleanupManagedBuffers(Topend *)
{
    StreamBlock *sb = NULL;

    if (m_currBlock && m_currBlock->dataPtr()) {
        m_topend->releaseManagedBuffer(m_currBlock->dataPtr());
    }
    delete m_currBlock;
    m_currBlock = 0;

    while (m_pendingBlocks.empty() != true) {
        sb = m_pendingBlocks.front();
        m_pendingBlocks.pop_front();
        m_topend->releaseManagedBuffer(sb->dataPtr());
        delete sb;
    }
}

/*
 * Handoff fully committed blocks to the top end.
 *
 * If txnId is greater than m_openTransactionId, then all data in the
 * stream is committed. Otherwise, only the data before
 * m_openTransactionUso is committed. This forumalation allows a
 * flush() call in the middle of an open transaction.
 */
void TupleStreamWrapper::commit(int64_t txnId)
{
    StreamBlock *sb = NULL;
    size_t committedUso = m_openTransactionUso;

    if (txnId > m_openTransactionId) {
        committedUso = m_uso;
    }

    // push all fully committed blocks to the topend
    while (m_pendingBlocks.empty() != true) {
        sb = m_pendingBlocks.front();
        if (!((sb->uso() + sb->remaining() - 1) < committedUso)) {
            break;
        }

        m_pendingBlocks.pop_front();
        m_topend->handoffReadyELBuffer(sb->dataPtr(),
                                       (int32_t)sb->remaining(),
                                       m_tableId);
        delete sb;
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
            if (sb->uso() >= mark) {
                m_pendingBlocks.pop_back();
                discardBlock(sb);
            }
            else {
                sb->truncateTo(mark);
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
    m_topend->releaseManagedBuffer(sb->dataPtr());
    delete sb;
}

/*
 * Write the cached block header into the stream block
 */
void TupleStreamWrapper::writeBlockHeader(StreamBlock &block) {
    if (m_cachedBlockHeader) {
        memcpy(block.dataPtr(), m_cachedBlockHeader, m_cachedBlockHeaderSize);
        block.consumed(m_cachedBlockHeaderSize);
        m_uso += m_cachedBlockHeaderSize;
    }
}

/*
 * Allocate another buffer, preserving the current buffer's content in
 * the pending queue.
 */
void TupleStreamWrapper::extendBufferChain(size_t minLength)
{
    if (m_defaultCapacity < minLength) {
        // eltxxx: rollback instead?
        throwFatalException("Default capacity is less than required buffer size.");
    }

    if (m_currBlock) {
        if (m_currBlock->remaining() > 0) {

//  FOR VERTICA TEMPORARILY
//            // write terminal row header
//            *(reinterpret_cast<int32_t*>
//              (m_currBlock->dataPtr() + m_currBlock->remaining())) = htonl(-1);
//            m_currBlock->consumed(sizeof(int32_t));

            m_pendingBlocks.push_back(m_currBlock);
            m_currBlock = NULL;
            m_currBlockTupleCount = 0;
        }
        // fully discard empty blocks. makes valgrind/testcase
        // conclusion easier.
        else {
            discardBlock(m_currBlock);
            m_currBlock = NULL;
        }
    }

    char *buffer = m_topend->claimManagedBuffer((int32_t)m_defaultCapacity);
    if (!buffer) {
        throwFatalException("Failed to claim managed buffer for ELT.");
    }

    m_currBlock = new StreamBlock(buffer, m_defaultCapacity, m_uso);
    writeBlockHeader(*m_currBlock);
}

/*
 * Create a new buffer and flush all pending committed data.
 * Creating a new buffer will push all queued data into the
 * pending list for commit to operate against.
 */
void TupleStreamWrapper::flushOldTuples(int64_t lastCommittedTxn,
                                        int64_t currentTime)
{
    // sanity checks
    assert(currentTime >= 0);
    m_lastFlush = currentTime;

    // update the commit markers
    if (lastCommittedTxn >= m_openTransactionId) {
        m_openTransactionId = lastCommittedTxn + 1; // or 0?
        m_openTransactionUso = m_uso;
    }

    extendBufferChain(0);
    commit(lastCommittedTxn);
}

/*
 * If txnId represents a new transaction, commit previous data.
 * Always serialize the supplied tuple in to the stream.
 * Return m_uso before this invocation - this marks the point
 * in the stream the caller can rollback to if this append
 * should be rolled back.
 */
size_t TupleStreamWrapper::appendTuple(int64_t txnId,
                                       int64_t seqNo,
                                       int64_t timestamp,
                                       TableTuple &tuple,
                                       TupleStreamWrapper::Type type)
{
    size_t rowHeaderSz = 0;
    size_t tupleMaxLength = 0;

    assert(txnId >= m_openTransactionId);

    // n.b., does not support multiple open, uncommitted transactions
    // -- data for txnId MUST NOT be in stream yet
    // -- data for all transactions preceding txnId must be committed
    //    or already rolled back.
    if (txnId > m_openTransactionId) {
        commit(txnId);
        m_openTransactionId = txnId;
        m_openTransactionUso = m_uso;
    }

    // Compute the upper bound on bytes required to serialize tuple.
    // eltxxx: can memoize this calculation.
    tupleMaxLength = computeOffsets(tuple, &rowHeaderSz);
    if (!m_currBlock) {
        extendBufferChain(m_defaultCapacity);
    }

//  FOR VERTICA TEMPORARILY
//  if ((m_currBlock->remaining() + tupleMaxLength + 4) > m_defaultCapacity) {
    if ((m_currBlock->remaining() + tupleMaxLength) > m_defaultCapacity) {
        extendBufferChain(tupleMaxLength);
    }
    ++m_currBlockTupleCount;
    char *basePtr = m_currBlock->dataPtr();
    size_t offset = m_currBlock->remaining();

    // initialize the full row header to 0. This also
    // has the effect of setting each column non-null.
    ::memset(basePtr + offset, 0, rowHeaderSz);

    char *startPtr  = basePtr + offset;
    uint8_t *nullArray = reinterpret_cast<uint8_t*>(basePtr + offset + sizeof (int32_t));
    char *dataPtr = basePtr + offset + rowHeaderSz;

    // write metadata columns
    *(reinterpret_cast<int64_t*>(dataPtr)) = htonll(txnId);
    dataPtr += sizeof (int64_t);  // 0

    *(reinterpret_cast<int64_t*>(dataPtr)) = htonll(timestamp);
    dataPtr += sizeof (int64_t);  // 1

    *(reinterpret_cast<int64_t*>(dataPtr)) = htonll(seqNo);
    dataPtr += sizeof (int64_t);  // 2

    *(reinterpret_cast<int64_t*>(dataPtr)) = htonll(m_partitionId);
    dataPtr += sizeof (int64_t);  // 3

    *(reinterpret_cast<int64_t*>(dataPtr)) = htonll(m_siteId);
    dataPtr += sizeof (int64_t);  // 4

    const char ins = (type == INSERT) ? 'I' : 'D';
    *(reinterpret_cast<char*>(dataPtr)) = ins;
    dataPtr +=  sizeof (char);    // 5

    // write the tuple's data
    dataPtr += tuple.serializeToELT(METADATA_COL_CNT, nullArray, dataPtr);

    // write the row size in to the row header
    // rowlength does not include the 4 byte row header
    *(reinterpret_cast<int32_t*>(startPtr)) = htonl(((int32_t)(dataPtr - (startPtr + 4))));

    // success: move m_offset.
    m_currBlock->consumed(dataPtr - startPtr);
    size_t startingOffset = m_uso;
    m_uso += (dataPtr - startPtr);
    return startingOffset;
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
    size_t dataSz = tuple.maxELTSerializationSize();
    if (dataSz == 0) {
        throwFatalException("Invalid tuple passed to computeTupleMaxLength. Crashing System.");
    }

    return *rowHeaderSz + metadataSz + dataSz;
}

} // namespace voltdb
