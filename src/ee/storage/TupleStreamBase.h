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

#ifndef TUPLESTREAMBASE_H_
#define TUPLESTREAMBASE_H_

#include "common/ids.h"
#include "common/tabletuple.h"
#include "common/executorcontext.hpp"
#include "common/FatalException.hpp"
#include "common/Topend.h"
#include "common/TupleSchema.h"
#include "common/types.h"
#include "common/NValue.hpp"
#include "common/ValuePeeker.hpp"
#include "common/tabletuple.h"
#include "common/ExportSerializeIo.h"
#include "common/StreamBlock.h"
#include "storage/TupleStreamException.h"

#include <cstdio>
#include <limits>
#include <iostream>
#include <ctime>
#include <utility>
#include <math.h>
#include <deque>
#include <common/debuglog.h>
namespace voltdb {

class Topend;
const int MAX_BUFFER_AGE = 4000;

template <class SB>
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

    inline int64_t getUso() const { return m_uso; }
    inline int64_t getCommittedUso() const { return m_committedUso; }
    inline size_t getDefaultCapacity() const { return m_defaultCapacity; }

    inline int64_t getFlushInterval() const {
        return m_flushInterval;
    }
    inline void setFlushInterval(int64_t flushInterval) {
        m_flushInterval = flushInterval;
    }

    /** truncate stream back to mark */
    void rollbackBlockTo(size_t mark);

    /** age out committed data; returns true if m_currBlock is now empty */
    virtual bool periodicFlush(int64_t timeInMillis,
                               int64_t lastComittedSpHandle) = 0;

    virtual void extendBufferChain(size_t minLength) = 0;
    virtual void pushStreamBuffer(SB *block) = 0;
    void pushPendingBlocks();
    void discardBlock(SB *sb);

    const SB* getCurrBlock() const {
        return m_currBlock;
    }

protected:
    void commonExtendBufferChain(size_t blockSize, size_t startUso);

    virtual SB* allocateBlock(char* buffer, size_t length, int64_t uso) const {
        return new SB(buffer, m_headerSpace, length, uso);
    }

    /** time interval between flushing partially filled buffers */
    int64_t m_flushInterval;

    /** timestamp of most recent flush() for DR last buffer create time for Export */
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
    SB *m_currBlock;

    /** Blocks not yet committed and pushed to the top-end */
    std::deque<SB*> m_pendingBlocks;

    /** transaction id of the current (possibly uncommitted) transaction. Could be either TxnId or SpHandle */
    int64_t m_openTxnId;

    int64_t m_openUniqueId;

    /** Universal stream offset when current transaction was opened */
    size_t m_openTransactionUso;

    /** last committed transaction id. Could be either TxnId or SpHandle*/
    int64_t m_committedTxnId;

    /** current committed uso */
    size_t m_committedUso;

    int64_t m_committedUniqueId;

    size_t m_headerSpace;
};

template <class SB>
TupleStreamBase<SB>::TupleStreamBase(size_t defaultBufferSize,
                                     size_t extraHeaderSpace /*= 0*/,
                                     int maxBufferSize /*= -1*/)
    : m_flushInterval(MAX_BUFFER_AGE),
      m_lastFlush(0),
      m_defaultCapacity(defaultBufferSize),
      m_maxCapacity( (maxBufferSize < defaultBufferSize) ? defaultBufferSize : maxBufferSize),
      m_uso(0),
      m_currBlock(NULL),
      // snapshot restores will call load table which in turn
      // calls appendTupple with LONG_MIN transaction ids
      // this allows initial ticks to succeed after rejoins
      m_openTxnId(0),
      m_openUniqueId(0),
      m_openTransactionUso(0),
      m_committedTxnId(0),
      m_committedUso(0),
      m_committedUniqueId(0),
      m_headerSpace(MAGIC_HEADER_SPACE_FOR_JAVA + extraHeaderSpace)
{}

template <class SB>
void TupleStreamBase<SB>::setDefaultCapacityForTest(size_t capacity)
{
    vassert(capacity > 0);
    if (m_uso != 0 || m_openTxnId != 0 ||
        m_openTransactionUso != 0 || m_committedTxnId != 0)
    {
        throwFatalException("setDefaultCapacity only callable before "
                            "TupleStreamBase is used");
    }
    cleanupManagedBuffers();
    if (m_maxCapacity < capacity || m_maxCapacity == m_defaultCapacity) {
        m_maxCapacity = capacity;
    }
    m_defaultCapacity = capacity;
    extendBufferChain(m_defaultCapacity);
}



/*
 * Essentially, shutdown.
 */
template <class SB>
void TupleStreamBase<SB>::cleanupManagedBuffers()
{
    SB *sb = NULL;

    discardBlock(m_currBlock);
    m_currBlock = NULL;

    while (m_pendingBlocks.empty() != true) {
        sb = m_pendingBlocks.front();
        m_pendingBlocks.pop_front();
        discardBlock(sb);
    }
}

template <class SB>
void TupleStreamBase<SB>::pushPendingBlocks()
{
    while (!m_pendingBlocks.empty()) {
        SB* block = m_pendingBlocks.front();
        //std::cout << "m_committedUso(" << m_committedUso << "), block->uso() + block->offset() == "
        //<< (block->uso() + block->offset()) << std::endl;

        // check that the entire remainder is committed
        if (m_committedUso >= (block->uso() + block->offset()))
        {
            //The block is handed off to the topend which is responsible for releasing the
            //memory associated with the block data.
            block->writeOutHeader();
            pushStreamBuffer(block);
            m_pendingBlocks.pop_front();
        }
        else
        {
            break;
        }
    }
}

/*
 * Discard all data with a uso gte mark
 */
template <class SB>
void TupleStreamBase<SB>::rollbackBlockTo(size_t mark)
{
    if (mark > m_uso) {
        throwFatalException("Truncating the future: mark %jd, current USO %jd.",
                            (intmax_t)mark, (intmax_t)m_uso);
    } else if (mark < m_committedUso) {
        throwFatalException("Truncating committed tuple data: mark %jd, committed USO %jd, current USO %jd, open spHandle %jd, committed spHandle %jd.",
                            (intmax_t)mark, (intmax_t)m_committedUso, (intmax_t)m_uso, (intmax_t)m_openTxnId, (intmax_t)m_committedTxnId);
    }

    // back up the universal stream counter
    m_uso = mark;

    // working from newest to oldest block, throw
    // away blocks that are fully after mark; truncate
    // the block that contains mark.
    if (m_currBlock != NULL) {
        if (m_currBlock->uso() >= mark) {
            SB *sb = NULL;
            discardBlock(m_currBlock);
            m_currBlock = NULL;
            while (m_pendingBlocks.empty() != true) {
                sb = m_pendingBlocks.back();
                m_pendingBlocks.pop_back();
                if (sb->uso() >= mark) {
                    discardBlock(sb);
                }
                else {
                    m_currBlock = sb;
                    break;
                }
            }
            if (m_currBlock == NULL) {
                extendBufferChain(m_defaultCapacity);
            }
        }
        if (m_uso == m_committedUso) {
            m_openTxnId = m_committedTxnId;
            m_openUniqueId = m_committedUniqueId;
        }
    }
}

/*
 * Correctly release and delete a managed buffer that won't
 * be handed off
 */
template <class SB>
void TupleStreamBase<SB>::discardBlock(SB *sb)
{
    if (sb != NULL) {
        delete [] sb->rawPtr();
        delete sb;
    }
}

/*
 * Allocate another buffer, preserving the current buffer's content in
 * the pending queue.
 */
template <class SB>
void TupleStreamBase<SB>::commonExtendBufferChain(size_t blockSize, size_t startUso)
{
    if (m_maxCapacity < blockSize) {
        // exportxxx: rollback instead?
        throwFatalException("Default capacity is less than required buffer size.");
    }

    if (m_currBlock) {
        if (!m_currBlock->empty()) {
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

    if (blockSize == 0) {
        throw TupleStreamException(SQLException::volt_output_buffer_overflow, "Transaction is bigger than DR Buffer size");
    }

    char *buffer = new char[blockSize];
    if (!buffer) {
        throwFatalException("Failed to claim managed buffer for Export.");
    }
    m_currBlock = allocateBlock(buffer, blockSize, startUso);
    if (blockSize > m_defaultCapacity) {
        m_currBlock->setType(LARGE_STREAM_BLOCK);
    }
}

}

#endif
