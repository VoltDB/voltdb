/* This file is part of VoltDB.
 * Copyright (C) 2008-2018 VoltDB Inc.
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

#include "storage/TupleStreamBase.h"

#include "common/TupleSchema.h"
#include "common/types.h"
#include "common/NValue.hpp"
#include "common/ValuePeeker.hpp"
#include "common/tabletuple.h"
#include "common/ExportSerializeIo.h"
#include "common/executorcontext.hpp"
#include "storage/TupleStreamException.h"

#include <cstdio>
#include <limits>
#include <iostream>
#include <cassert>
#include <ctime>
#include <utility>
#include <math.h>

using namespace std;
using namespace voltdb;

const int MAX_BUFFER_AGE = 4000;

TupleStreamBase::TupleStreamBase(size_t defaultBufferSize, size_t extraHeaderSpace /*= 0*/, int maxBufferSize /*= -1*/)
    : m_flushInterval(MAX_BUFFER_AGE),
      m_lastFlush(0), m_defaultCapacity(defaultBufferSize),
      m_maxCapacity( (maxBufferSize < defaultBufferSize) ? defaultBufferSize : maxBufferSize),
      m_uso(0), m_currBlock(NULL),
      // snapshot restores will call load table which in turn
      // calls appendTupple with LONG_MIN transaction ids
      // this allows initial ticks to succeed after rejoins
      m_openSpHandle(0),
      m_openUniqueId(0),
      m_openTransactionUso(0),
      m_committedSpHandle(0), m_committedUso(0),
      m_committedUniqueId(0),
      m_headerSpace(MAGIC_HEADER_SPACE_FOR_JAVA + extraHeaderSpace)
{
    extendBufferChain(m_defaultCapacity);
}

void
TupleStreamBase::setDefaultCapacityForTest(size_t capacity)
{
    assert (capacity > 0);
    if (m_uso != 0 || m_openSpHandle != 0 ||
        m_openTransactionUso != 0 || m_committedSpHandle != 0)
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
void TupleStreamBase::cleanupManagedBuffers()
{
    StreamBlock *sb = NULL;

    discardBlock(m_currBlock);
    m_currBlock = NULL;

    while (m_pendingBlocks.empty() != true) {
        sb = m_pendingBlocks.front();
        m_pendingBlocks.pop_front();
        discardBlock(sb);
    }
}

/*
 * Handoff fully committed blocks to the top end.
 *
 * This is the only function that should modify m_openSpHandle,
 * m_openTransactionUso.
 */
void TupleStreamBase::commit(int64_t lastCommittedSpHandle, int64_t currentSpHandle, int64_t uniqueId,
        bool sync, bool flush)
{
    if (currentSpHandle < m_openSpHandle) {
        throwFatalException(
                "Active transactions moving backwards: openSpHandle is %jd, while the current spHandle is %jd",
                (intmax_t)m_openSpHandle, (intmax_t)currentSpHandle
                );
    } else if (currentSpHandle == m_openSpHandle && uniqueId >= 0 && uniqueId != m_openUniqueId) {
        throwFatalException(
                "Received a new transaction, but with the same spHandle (%jd): old uniqueId is %jd, new uniqueId is %jd",
                (intmax_t)m_openSpHandle, (intmax_t)m_openUniqueId, (intmax_t)uniqueId);
    }

    // more data for an ongoing transaction with no new committed data
    if ((currentSpHandle == m_openSpHandle) &&
        (lastCommittedSpHandle == m_committedSpHandle)) {
        //std::cout << "Current spHandle(" << currentSpHandle << ") == m_openSpHandle(" << m_openSpHandle <<
        //") && lastCommittedSpHandle(" << lastCommittedSpHandle << ") m_committedSpHandle(" <<
        //m_committedSpHandle << ")" << std::endl;
        if (sync) {
            pushStreamBuffer(NULL, true);
        }

        if (flush) {
            extendBufferChain(0);
        }

        return;
    }

    // If the current TXN ID has advanced, then we know that:
    // - The old open transaction has been committed
    // - The current transaction is now our open transaction
    // If last committed and current sphandle are the same
    // this isn't a new transaction, the block below handles it better
    // by ending the current open transaction and not starting a new one
    if (m_openSpHandle < currentSpHandle && currentSpHandle != lastCommittedSpHandle) {
        if (uniqueId < 0) {
            throwFatalException(
                    "Received invalid uniqueId (%jd) with open spHandle %jd, currentSpHandle %jd, lastCommittedSpHandle %jd.",
                    (intmax_t)uniqueId, (intmax_t)m_openSpHandle, (intmax_t)currentSpHandle, (intmax_t)lastCommittedSpHandle);
        }
        //std::cout << "m_openSpHandle(" << m_openSpHandle << ") < currentSpHandle("
        //<< currentSpHandle << ")" << std::endl;
        m_committedUso = m_uso;
        m_committedUniqueId = m_openUniqueId;
        // Advance the tip to the new transaction.
        m_committedSpHandle = m_openSpHandle;
        m_openSpHandle = currentSpHandle;
        m_openUniqueId = uniqueId;

        if (flush) {
            extendBufferChain(0);
        }
    }

    // now check to see if the lastCommittedSpHandle tells us that our open
    // transaction should really be committed.  If so, update the
    // committed state.
    if (m_openSpHandle <= lastCommittedSpHandle) {
        //std::cout << "m_openSpHandle(" << m_openSpHandle << ") <= lastCommittedSpHandle(" <<
        //lastCommittedSpHandle << ")" << std::endl;
        m_committedUso = m_uso;
        m_committedSpHandle = m_openSpHandle;
        m_committedUniqueId = m_openUniqueId;

        if (flush) {
            extendBufferChain(0);
        }
    }

    pushPendingBlocks();

    if (sync) {
        pushStreamBuffer(NULL, true);
    }
}

void TupleStreamBase::pushPendingBlocks()
{
    while (!m_pendingBlocks.empty()) {
        StreamBlock* block = m_pendingBlocks.front();
        //std::cout << "m_committedUso(" << m_committedUso << "), block->uso() + block->offset() == "
        //<< (block->uso() + block->offset()) << std::endl;

        // check that the entire remainder is committed
        if (m_committedUso >= (block->uso() + block->offset()))
        {
            //The block is handed off to the topend which is responsible for releasing the
            //memory associated with the block data. The metadata is deleted here.
            pushStreamBuffer(block, false);
            delete block;
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
void TupleStreamBase::rollbackTo(size_t mark, size_t)
{
    if (mark > m_uso) {
        throwFatalException("Truncating the future: mark %jd, current USO %jd.",
                            (intmax_t)mark, (intmax_t)m_uso);
    } else if (mark < m_committedUso) {
        throwFatalException("Truncating committed tuple data: mark %jd, committed USO %jd, current USO %jd, open spHandle %jd, committed spHandle %jd.",
                            (intmax_t)mark, (intmax_t)m_committedUso, (intmax_t)m_uso, (intmax_t)m_openSpHandle, (intmax_t)m_committedSpHandle);
    }

    // back up the universal stream counter
    m_uso = mark;

    // working from newest to oldest block, throw
    // away blocks that are fully after mark; truncate
    // the block that contains mark.
    if (m_currBlock != NULL && !(m_currBlock->uso() >= mark)) {
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
        if (m_currBlock == NULL) {
            extendBufferChain(m_defaultCapacity);
        }
    }
    if (m_uso == m_committedUso) {
        m_openSpHandle = m_committedSpHandle;
        m_openUniqueId = m_committedUniqueId;
    }
}

/*
 * Correctly release and delete a managed buffer that won't
 * be handed off
 */
void TupleStreamBase::discardBlock(StreamBlock *sb)
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
void TupleStreamBase::extendBufferChain(size_t minLength)
{
    if (m_maxCapacity < minLength) {
        // exportxxx: rollback instead?
        throwFatalException("Default capacity is less than required buffer size.");
    }
    StreamBlock *oldBlock = NULL;
    size_t uso = m_uso;

    if (m_currBlock) {
        if (m_currBlock->offset() > 0) {
            m_pendingBlocks.push_back(m_currBlock);
            oldBlock = m_currBlock;
            m_currBlock = NULL;
        }
        // fully discard empty blocks. makes valgrind/testcase
        // conclusion easier.
        else {
            discardBlock(m_currBlock);
            m_currBlock = NULL;
        }
    }
    size_t blockSize = (minLength <= m_defaultCapacity) ? m_defaultCapacity : m_maxCapacity;
    bool openTransaction = checkOpenTransaction(oldBlock, minLength, blockSize, uso);

    if (blockSize == 0) {
        throw TupleStreamException(SQLException::volt_output_buffer_overflow, "Transaction is bigger than DR Buffer size");
    }

    char *buffer = new char[blockSize];
    if (!buffer) {
        throwFatalException("Failed to claim managed buffer for Export.");
    }
    m_currBlock = new StreamBlock(buffer, m_headerSpace, blockSize, uso);
    if (blockSize > m_defaultCapacity) {
        m_currBlock->setType(LARGE_STREAM_BLOCK);
    }

    if (openTransaction) {
        handleOpenTransaction(oldBlock);
    }

    pushPendingBlocks();
}

/*
 * Create a new buffer and flush all pending committed data.
 * Creating a new buffer will push all queued data into the
 * pending list for commit to operate against.
 */
void
TupleStreamBase::periodicFlush(int64_t timeInMillis,
                               int64_t lastCommittedSpHandle)
{
    // negative timeInMillis instructs a mandatory flush
    if (timeInMillis < 0 || (s_exportFlushTimeout > 0 && timeInMillis - m_lastFlush > s_exportFlushTimeout)) {
        int64_t maxSpHandle = std::max(m_openSpHandle, lastCommittedSpHandle);
        if (timeInMillis > 0) {
            m_lastFlush = timeInMillis;
        }

        /*
         * handle cases when the currentSpHandle was set by rejoin
         * and snapshot restore load table statements that send in
         * Long.MIN_VALUE as their tx id. ee's tick which results
         * in calls to this procedure may be called right after
         * these.
         */
        commit(lastCommittedSpHandle, maxSpHandle, std::numeric_limits<int64_t>::min(), timeInMillis < 0 ? true : false, true);
    }
}
