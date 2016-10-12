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

#ifndef DRTUPLESTREAMBASE_H_
#define DRTUPLESTREAMBASE_H_

#include "common/ids.h"
#include "common/tabletuple.h"
#include "common/FatalException.hpp"
#include "storage/TupleStreamBase.h"
#include <deque>

namespace voltdb {

// Extra space to write a StoredProcedureInvocation wrapper in Java without copying
// this magic number is tied to the serialization size of an InvocationBuffer
const int MAGIC_DR_TRANSACTION_PADDING = 78;
const int SECONDARY_BUFFER_SIZE = (45 * 1024 * 1024) + 4096;
// Use this to indicate uninitialized DR mark
const size_t INVALID_DR_MARK = SIZE_MAX;

struct DRCommittedInfo{
    int64_t seqNum;
    int64_t spUniqueId;
    int64_t mpUniqueId;

    DRCommittedInfo(int64_t seq, int64_t spUID, int64_t mpUID) : seqNum(seq), spUniqueId(spUID), mpUniqueId(mpUID) {}
};

class AbstractDRTupleStream : public TupleStreamBase {
    friend class ExecutorContext;

public:
    AbstractDRTupleStream(int partitionId, int defaultBufferSize);

    virtual ~AbstractDRTupleStream() {}

    virtual void pushExportBuffer(StreamBlock *block, bool sync, bool endOfStream);
    /** truncate stream back to mark */
    virtual void rollbackTo(size_t mark, size_t drRowCost);

    virtual void periodicFlush(int64_t timeInMillis,
                       int64_t lastComittedSpHandle);

    virtual void setSecondaryCapacity(size_t capacity);

    void setLastCommittedSequenceNumber(int64_t sequenceNumber);

    /**
     * write an insert or delete record to the stream
     * for active-active conflict detection purpose, write full row image for delete records.
     * */
    virtual size_t appendTuple(int64_t lastCommittedSpHandle,
                       char *tableHandle,
                       int partitionColumn,
                       int64_t spHandle,
                       int64_t uniqueId,
                       TableTuple &tuple,
                       DRRecordType type) = 0;

    /**
     * write an update record to the stream
     * for active-active conflict detection purpose, write full before image for update records.
     * */
    virtual size_t appendUpdateRecord(int64_t lastCommittedSpHandle,
                       char *tableHandle,
                       int partitionColumn,
                       int64_t spHandle,
                       int64_t uniqueId,
                       TableTuple &oldTuple,
                       TableTuple &newTuple) = 0;

    virtual size_t truncateTable(int64_t lastCommittedSpHandle,
                       char *tableHandle,
                       std::string tableName,
                       int partitionColumn,
                       int64_t spHandle,
                       int64_t uniqueId) = 0;

    virtual void beginTransaction(int64_t sequenceNumber, int64_t spHandle, int64_t uniqueId) = 0;
    // If a transaction didn't generate any binary log data, calling this
    // would be a no-op because it was never begun.
    virtual void endTransaction(int64_t uniqueId) = 0;

    virtual bool checkOpenTransaction(StreamBlock *sb, size_t minLength, size_t& blockSize, size_t& uso) = 0;

    void handleOpenTransaction(StreamBlock *oldBlock);

    virtual DRCommittedInfo getLastCommittedSequenceNumberAndUniqueIds() = 0;

    virtual void generateDREvent(DREventType type, int64_t lastCommittedSpHandle, int64_t spHandle,
                                 int64_t uniqueId, ByteArray payloads) = 0;

    bool m_enabled;
    bool m_guarded; // strongest guard, reject all actions for DRTupleStream

    int64_t m_openSequenceNumber;
    int64_t m_committedSequenceNumber;
protected:
    virtual void openTransactionCommon(int64_t spHandle, int64_t uniqueId);

    virtual void commitTransactionCommon();

    CatalogId m_partitionId;
    size_t m_secondaryCapacity;
    int64_t m_rowTarget;
    bool m_opened;
    size_t m_txnRowCount;

private:
    // return true if stream state was switched from close to open
    virtual bool transactionChecks(int64_t lastCommittedSpHandle, int64_t spHandle, int64_t uniqueId) = 0;
};

class DRTupleStreamDisableGuard {
public:
    DRTupleStreamDisableGuard(ExecutorContext *ec, bool ignore) :
            m_drStream(ec->drStream()), m_drReplicatedStream(ec->drReplicatedStream()), m_drStreamOldValue(ec->drStream()->m_guarded),
            m_drReplicatedStreamOldValue(m_drReplicatedStream?m_drReplicatedStream->m_guarded:true)
    {
        if (!ignore) {
            setGuard();
        }
    }
    DRTupleStreamDisableGuard(ExecutorContext *ec) :
            m_drStream(ec->drStream()), m_drReplicatedStream(ec->drReplicatedStream()), m_drStreamOldValue(ec->drStream()->m_guarded),
            m_drReplicatedStreamOldValue(m_drReplicatedStream?m_drReplicatedStream->m_guarded:true)
    {
        setGuard();
    }
    ~DRTupleStreamDisableGuard() {
        m_drStream->m_guarded = m_drStreamOldValue;
        if (m_drReplicatedStream) {
            m_drReplicatedStream->m_guarded = m_drReplicatedStreamOldValue;
        }
    }

private:
    inline void setGuard()
    {
        m_drStream->m_guarded = true;
        if (m_drReplicatedStream) {
            m_drReplicatedStream->m_guarded = true;
        }
    }

    AbstractDRTupleStream *m_drStream;
    AbstractDRTupleStream *m_drReplicatedStream;
    const bool m_drStreamOldValue;
    const bool m_drReplicatedStreamOldValue;
};

}

#endif
