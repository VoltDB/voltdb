/* This file is part of VoltDB.
 * Copyright (C) 2008-2011 VoltDB Inc.
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

#ifndef TUPLESTREAMWRAPPER_H_
#define TUPLESTREAMWRAPPER_H_

#include "StreamBlock.h"

#include "common/ids.h"
#include "common/tabletuple.h"
#include "common/executorcontext.hpp"
#include "common/FatalException.hpp"
#include "common/Topend.h"
#include <deque>
#include <cassert>
namespace voltdb {

class Topend;
//If you change this constant here change it in Java in the StreamBlockQueue where
//it is used to calculate the number of bytes queued
const int EL_BUFFER_SIZE = /* 1024; */ 2 * 1024 * 1024;

class TupleStreamWrapper {
public:
    enum Type { INSERT, DELETE };

    TupleStreamWrapper(CatalogId partitionId, CatalogId siteId);

    ~TupleStreamWrapper() {
        cleanupManagedBuffers();
    }

    /**
     * Drop and release all claimed buffers. Intended for use at
     * shutdown to achieve full memory deallocation for valgrind.
     */
    void cleanupManagedBuffers();

    /**
     * Configure the buffer size requested from JNI pool.
     * This allows testcases to use significantly smaller buffers
     * to test buffer rollover.
     */
    void setDefaultCapacity(size_t capacity);

    void setSignatureAndGeneration(std::string signature, int64_t generation);

    /** Read the total bytes used over the life of the stream */
    size_t bytesUsed() {
        return m_uso;
    }

    /** Set the total number of bytes used (for rejoin/recover) */
    void setBytesUsed(size_t count) {
        assert(m_uso == 0);
        StreamBlock *sb = new StreamBlock(new char[1], 0, count);
        ExecutorContext::getExecutorContext()->getTopend()->pushExportBuffer(
                                m_generation, m_partitionId, m_signature, sb, false, false);
        delete [] sb->rawPtr();
        delete sb;
        m_uso = count;
    }

    int64_t allocatedByteCount() const {
        return (m_pendingBlocks.size() * m_defaultCapacity) +
                ExecutorContext::getExecutorContext()->getTopend()->getQueuedExportBytes( m_partitionId, m_signature);
    }

    /** truncate stream back to mark */
    void rollbackTo(size_t mark);

    /** age out committed data */
    void periodicFlush(int64_t timeInMillis,
                       int64_t lastComittedTxnId,
                       int64_t currentTxnId);

    /** write a tuple to the stream */
    size_t appendTuple(int64_t lastCommittedTxnId,
                       int64_t txnId,
                       int64_t seqNo,
                       int64_t timestamp,
                       TableTuple &tuple,
                       TupleStreamWrapper::Type type);

    size_t computeOffsets(TableTuple &tuple,size_t *rowHeaderSz);
    void extendBufferChain(size_t minLength);
    void discardBlock(StreamBlock *sb);

    /** Send committed data to the top end */
    void commit(int64_t lastCommittedTxnId, int64_t txnId, bool sync = false);

    // cached catalog values
    const CatalogId m_partitionId;
    const CatalogId m_siteId;

    /** timestamp of most recent flush() */
    int64_t m_lastFlush;

    /** size of buffer requested from the top-end */
    size_t m_defaultCapacity;

    /** Universal stream offset. Total bytes appended to this stream. */
    size_t m_uso;

    /** Current block */
    StreamBlock *m_currBlock;

    /** Blocks not yet committed and pushed to the top-end */
    std::deque<StreamBlock*> m_pendingBlocks;

    /** transaction id of the current (possibly uncommitted) transaction */
    int64_t m_openTransactionId;

    /** Universal stream offset when current transaction was opened */
    size_t m_openTransactionUso;

    /** last committed transaction id */
    int64_t m_committedTransactionId;

    /** current committed uso */
    size_t m_committedUso;

    std::string m_signature;
    int64_t m_generation;
};

}

#endif
