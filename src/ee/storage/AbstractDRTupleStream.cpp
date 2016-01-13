/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
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
#include <cassert>

using namespace std;
using namespace voltdb;

AbstractDRTupleStream::AbstractDRTupleStream()
        : TupleStreamBase(MAGIC_DR_TRANSACTION_PADDING),
          m_enabled(true),
          m_secondaryCapacity(SECONDARY_BUFFER_SIZE),
          m_rowTarget(-1),
          m_opened(false),
          m_txnRowCount(0) {

}

// for test purpose
void AbstractDRTupleStream::setSecondaryCapacity(size_t capacity) {
    assert (capacity > 0);
    if (m_uso != 0 || m_openSpHandle != 0 ||
        m_openTransactionUso != 0 || m_committedSpHandle != 0)
    {
        throwFatalException("setSecondaryCapacity only callable before "
                            "TupleStreamBase is used");
    }
    m_secondaryCapacity = capacity;
}

void AbstractDRTupleStream::pushExportBuffer(StreamBlock *block, bool sync, bool endOfStream) {
    if (sync) return;
    int64_t rowTarget = ExecutorContext::getExecutorContext()->getTopend()->pushDRBuffer(m_partitionId, block);
    if (rowTarget >= 0) {
        m_rowTarget = rowTarget;
    }
}

// Set m_opened = false first otherwise checkOpenTransaction() may
// consider the transaction being rolled back as open.
void AbstractDRTupleStream::rollbackTo(size_t mark, size_t drRowCost) {
    if (mark == INVALID_DR_MARK) {
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
        m_opened = false;
    }
    TupleStreamBase::rollbackTo(mark, drRowCost);
}

void AbstractDRTupleStream::setLastCommittedSequenceNumber(int64_t sequenceNumber) {
    assert(m_committedSequenceNumber == -1);
    m_openSequenceNumber = sequenceNumber;
    m_committedSequenceNumber = sequenceNumber;
}
