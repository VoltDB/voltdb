/* This file is part of VoltDB.
 * Copyright (C) 2008-2014 VoltDB Inc.
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

#ifndef TUPLESTREAMBASE_H_
#define TUPLESTREAMBASE_H_

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
const int EL_BUFFER_SIZE = /* 1024; */ (2 * 1024 * 1024) + MAGIC_HEADER_SPACE_FOR_JAVA;

class TupleStreamBase {
public:

    TupleStreamBase(CatalogId partitionId, int64_t siteId);

    virtual ~TupleStreamBase() {
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

    /** Read the total bytes used over the life of the stream */
    size_t bytesUsed() {
        return m_uso;
    }

    virtual void pushExportBuffer(StreamBlock *block, bool sync, bool endOfStream) = 0;

    virtual int64_t allocatedByteCount() const = 0;

    /** truncate stream back to mark */
    void rollbackTo(size_t mark);

    /** age out committed data */
    void periodicFlush(int64_t timeInMillis,
                       int64_t lastComittedSpHandle,
                       int64_t currentSpHandle);

    void extendBufferChain(size_t minLength);
    void discardBlock(StreamBlock *sb);

    /** Send committed data to the top end */
    void commit(int64_t lastCommittedSpHandle, int64_t spHandle, bool sync = false);

    // cached catalog values
    const CatalogId m_partitionId;
    const int64_t m_siteId;

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
    int64_t m_openSpHandle;

    /** Universal stream offset when current transaction was opened */
    size_t m_openTransactionUso;

    /** last committed transaction id */
    int64_t m_committedSpHandle;

    /** current committed uso */
    size_t m_committedUso;
};

}

#endif
