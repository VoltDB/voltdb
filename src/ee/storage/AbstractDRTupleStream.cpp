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

#include "AbstractDRTupleStream.h"
#include "stdarg.h"
#include <cassert>

using namespace std;
using namespace voltdb;

AbstractDRTupleStream::AbstractDRTupleStream(int partitionId, size_t defaultBufferSize, uint8_t drProtocolVersion)
        : TupleStreamBase(defaultBufferSize, MAGIC_DR_TRANSACTION_PADDING, SECONDARY_BUFFER_SIZE),
          m_enabled(true),
          m_guarded(false),
          m_openSequenceNumber(-1),
          m_committedSequenceNumber(-1),
          m_partitionId(partitionId),
          m_drProtocolVersion(drProtocolVersion),
          m_secondaryCapacity(SECONDARY_BUFFER_SIZE),
          m_rowTarget(-1),
          m_opened(false),
          m_txnRowCount(0)
{}

// for test purpose
void AbstractDRTupleStream::setSecondaryCapacity(size_t capacity)
{
    assert (capacity > 0);
    if (m_uso != 0 || m_openSpHandle != 0 ||
        m_openTransactionUso != 0 || m_committedSpHandle != 0)
    {
        throwFatalException("setSecondaryCapacity only callable before "
                            "TupleStreamBase is used");
    }
    m_secondaryCapacity = capacity;
}

void AbstractDRTupleStream::pushStreamBuffer(StreamBlock *block, bool sync)
{
    if (sync) return;
    int64_t rowTarget = ExecutorContext::getPhysicalTopend()->pushDRBuffer(m_partitionId, block);
    if (rowTarget >= 0) {
        m_rowTarget = rowTarget;
    }
}

// Set m_opened = false first otherwise checkOpenTransaction() may
// consider the transaction being rolled back as open.
void AbstractDRTupleStream::rollbackTo(size_t mark, size_t drRowCost)
{
    if (mark == INVALID_DR_MARK) {
        m_openSpHandle = m_committedSpHandle;
        m_openUniqueId = m_committedUniqueId;
        m_openSequenceNumber = m_committedSequenceNumber;
        m_opened = false;
        return;
    }
    if (drRowCost <= m_txnRowCount) {
        m_txnRowCount -= drRowCost;
    } else {
        // convenience to let us just throw away everything at once
        assert(drRowCost == SIZE_MAX);
        m_txnRowCount = 0;
    }
    if (mark == m_committedUso) {
        assert(m_txnRowCount == 0);
        m_openSequenceNumber = m_committedSequenceNumber;
        m_opened = false;
    }
    TupleStreamBase::rollbackTo(mark, drRowCost);
}

void AbstractDRTupleStream::periodicFlush(int64_t timeInMillis,
                                     int64_t lastCommittedSpHandle)
{
    // negative timeInMillis instructs a mandatory flush
    if (timeInMillis < 0 || (m_flushInterval > 0 && timeInMillis - m_lastFlush > m_flushInterval)) {
        int64_t currentSpHandle = std::max(m_openSpHandle, lastCommittedSpHandle);
        if (timeInMillis > 0) {
            m_lastFlush = timeInMillis;
        }

        if (currentSpHandle < m_openSpHandle) {
            fatalDRErrorWithPoisonPill(m_openSpHandle, m_openUniqueId,
                    "Active transactions moving backwards: openSpHandle is %jd, while the current spHandle is %jd",
                    (intmax_t)m_openSpHandle, (intmax_t)currentSpHandle);
            return;
        }

        // more data for an ongoing transaction with no new committed data
        if ((currentSpHandle == m_openSpHandle) &&
                (lastCommittedSpHandle == m_committedSpHandle)) {
            extendBufferChain(0);
            return;
        }

        // the open transaction should be committed
        if (m_openSpHandle <= lastCommittedSpHandle) {
            extendBufferChain(0);
        }

        pushPendingBlocks();
    }
}

void AbstractDRTupleStream::setLastCommittedSequenceNumber(int64_t sequenceNumber)
{
    assert(m_committedSequenceNumber <= m_openSequenceNumber);
    m_openSequenceNumber = sequenceNumber;
    m_committedSequenceNumber = sequenceNumber;
}

void AbstractDRTupleStream::handleOpenTransaction(StreamBlock *oldBlock)
{
    size_t uso = m_currBlock->uso();
    size_t partialTxnLength = oldBlock->offset() - oldBlock->lastDRBeginTxnOffset();
    ::memcpy(m_currBlock->mutableDataPtr(), oldBlock->mutableLastBeginTxnDataPtr(), partialTxnLength);
    m_currBlock->startDRSequenceNumber(m_openSequenceNumber);
    m_currBlock->recordLastBeginTxnOffset();
    m_currBlock->consumed(partialTxnLength);
    ::memset(oldBlock->mutableLastBeginTxnDataPtr(), 0, partialTxnLength);
    oldBlock->truncateTo(uso);
    oldBlock->clearLastBeginTxnOffset();
    // If the whole previous block has been moved to new block, discards the empty one.
    if (oldBlock->offset() == 0) {
        m_pendingBlocks.pop_back();
        discardBlock(oldBlock);
    }
}

void AbstractDRTupleStream::fatalDRErrorWithPoisonPill(int64_t spHandle, int64_t uniqueId, const char *format, ...)
{
    char reallysuperbig_failure_message[8192];
    va_list arg;
    va_start(arg, format);
    vsnprintf(reallysuperbig_failure_message, 8192, format, arg);
    va_end(arg);
    std::string failureMessageForVoltLogger = reallysuperbig_failure_message;
    ExecutorContext::getPhysicalTopend()->pushPoisonPill(m_partitionId, failureMessageForVoltLogger, m_currBlock);
    m_currBlock = NULL;
    if (m_opened) {
        commitTransactionCommon();
        ++m_openSequenceNumber;
        if (m_enabled) {
            beginTransaction(m_openSequenceNumber, spHandle, uniqueId);
        }
        else {
            openTransactionCommon(spHandle, uniqueId);
        }
    }
    else {
        commitTransactionCommon();
    }
}

void AbstractDRTupleStream::openTransactionCommon(int64_t spHandle, int64_t uniqueId)
{
    m_openSpHandle = spHandle;
    m_openUniqueId = uniqueId;

    m_opened = true;
}

void AbstractDRTupleStream::commitTransactionCommon()
{
    m_committedSpHandle = m_openSpHandle;
    m_committedUniqueId = m_openUniqueId;
    m_committedSequenceNumber = m_openSequenceNumber;

    m_opened = false;
}
