/* This file is part of VoltDB.
 * Copyright (C) 2008-2019 VoltDB Inc.
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

#include "common/ids.h"
#include "common/tabletuple.h"
#include "common/executorcontext.hpp"
#include "common/FatalException.hpp"
#include "common/StreamBlock.h"
#include "common/Topend.h"
#include <deque>
#include <cassert>
namespace voltdb {

class Topend;

class TupleStreamBase {
public:

    TupleStreamBase(size_t defaultBufferSize, size_t extraHeaderSpace, int maxBufferSize = -1);

    virtual ~TupleStreamBase()
    {
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
    void setDefaultCapacityForTest(size_t capacity);
    virtual void setSecondaryCapacity(size_t capacity) {}

    /** truncate stream back to mark */
    virtual void rollbackTo(size_t mark, size_t drRowCost, int64_t exportSeqNo);

    /** age out committed data */
    virtual void periodicFlush(int64_t timeInMillis,
                               int64_t lastComittedSpHandle);

    void extendBufferChain(size_t minLength);
    virtual void pushStreamBuffer(StreamBlock *block, bool sync) = 0;
    void pushPendingBlocks();
    void discardBlock(StreamBlock *sb);

    virtual bool checkOpenTransaction(StreamBlock *sb, size_t minLength, size_t& blockSize, size_t& uso) { return false; }

    virtual void handleOpenTransaction(StreamBlock *oldBlock) {}

    /** Send committed data to the top end. */
    void commit(int64_t lastCommittedSpHandle, int64_t spHandle, int64_t uniqueId, bool sync, bool flush);

    /** time interval between flushing partially filled buffers */
    int64_t m_flushInterval;

    /** timestamp of most recent flush() */
    int64_t m_lastFlush;

    /** size of buffer requested from the top-end */
    size_t m_defaultCapacity;

    /** max allowed buffer capacity */
    size_t m_maxCapacity;

    /**
     * Universal stream offset. Total bytes appended to this stream.
     *
     * PLEASE NOTE THAT this is only used in TABLE stats while rest
     * of the export system use sequence number to track rows.
     * */
    size_t m_uso;

    /** Current block */
    StreamBlock *m_currBlock;

    /** Blocks not yet committed and pushed to the top-end */
    std::deque<StreamBlock*> m_pendingBlocks;

    /** transaction id of the current (possibly uncommitted) transaction */
    int64_t m_openSpHandle;

    int64_t m_openUniqueId;

    /** Universal stream offset when current transaction was opened */
    size_t m_openTransactionUso;

    /** last committed transaction id */
    int64_t m_committedSpHandle;

    /** current committed uso */
    size_t m_committedUso;

    int64_t m_committedUniqueId;

    size_t m_headerSpace;

    /**
     * Count of exported tuples not yet counted in the row count header of the Export Stream Block for the current transaction;
     * this count will be added to the row count header (and this count reset to 0) on the following conditions:
     * 1) current transaction appears "committed" when we start handling the _next_ transaction
     * 2) when the current transaction needs a new Stream Block
     * 3) current transaction is rolled back (with the rollback affecting one or more Stream blocks)
     */
    int64_t m_stashedTupleCount;

    int64_t m_exportSequenceNumber;
};

}

#endif
