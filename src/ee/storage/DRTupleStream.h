/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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

#ifndef DRTUPLESTREAM_H_
#define DRTUPLESTREAM_H_

#include "common/ids.h"
#include "common/tabletuple.h"
#include "common/FatalException.hpp"
#include "storage/TupleStreamBase.h"
#include <deque>
#include <cassert>

namespace voltdb {
class StreamBlock;
class TableIndex;

// Use this to indicate uninitialized DR mark
const size_t INVALID_DR_MARK = SIZE_MAX;

// Extra space to write a StoredProcedureInvocation wrapper in Java without copying
const int MAGIC_DR_TRANSACTION_PADDING = 78;
const int SECONDARY_BUFFER_SIZE = (45 * 1024 * 1024) + 4096;

struct DRCommittedInfo{
    int64_t seqNum;
    int64_t spUniqueId;
    int64_t mpUniqueId;
    uint8_t drVersion;

    DRCommittedInfo(int64_t seq, int64_t spUID, int64_t mpUID, uint8_t ver) : seqNum(seq), spUniqueId(spUID), mpUniqueId(mpUID), drVersion(ver) {}
};

class DRTupleStream : public voltdb::TupleStreamBase {
public:
    //Version(1), type(1), drId(8), uniqueId(8), checksum(4)
    static const size_t BEGIN_RECORD_SIZE = 1 + 1 + 8 + 8 + 4;
    //Version(1), type(1), drId(8), checksum(4)
    static const size_t END_RECORD_SIZE = 1 + 1 + 8 + 4;
    //Version(1), type(1), table signature(8), checksum(4)
    static const size_t TXN_RECORD_HEADER_SIZE = 1 + 1 + 4 + 8;
    static const uint8_t MINIMUM_COMPATIBLE_DR_PROTOCOL_VERSION = 3;

    DRTupleStream();

    virtual ~DRTupleStream() {
    }

    void configure(CatalogId partitionId) {
        m_partitionId = partitionId;
    }

    // for test purpose
    virtual void setSecondaryCapacity(size_t capacity);

    virtual void rollbackTo(size_t mark, size_t drRowCost);

    virtual void pushExportBuffer(StreamBlock *block, bool sync, bool endOfStream);

    /**
     * write an insert or delete record to the stream
     * for active-active conflict detection purpose, write full row image for delete records.
     * */
    virtual size_t appendTuple(int64_t lastCommittedSpHandle,
                       char *tableHandle,
                       int64_t txnId,
                       int64_t spHandle,
                       int64_t uniqueId,
                       TableTuple &tuple,
                       DRRecordType type,
                       const std::pair<const TableIndex*, uint32_t>& indexPair);

    /**
     * write an update record to the stream
     * for active-active conflict detection purpose, write full before image for update records.
     * */
    virtual size_t appendUpdateRecord(int64_t lastCommittedSpHandle,
                       char *tableHandle,
                       int64_t txnId,
                       int64_t spHandle,
                       int64_t uniqueId,
                       TableTuple &oldTuple,
                       TableTuple &newTuple,
                       const std::pair<const TableIndex*, uint32_t>& indexPair);

    virtual size_t truncateTable(int64_t lastCommittedSpHandle,
                       char *tableHandle,
                       std::string tableName,
                       int64_t txnId,
                       int64_t spHandle,
                       int64_t uniqueId);

    void beginTransaction(int64_t sequenceNumber, int64_t uniqueId);
    // If a transaction didn't generate any binary log data, calling this
    // would be a no-op because it was never begun.
    void endTransaction(int64_t uniqueId);

    bool checkOpenTransaction(StreamBlock *sb, size_t minLength, size_t& blockSize, size_t& uso);

    DRCommittedInfo getLastCommittedSequenceNumberAndUniqueIds() {
        return DRCommittedInfo(m_committedSequenceNumber, m_lastCommittedSpUniqueId, m_lastCommittedMpUniqueId, m_drVersion);
    }
    void setLastCommittedSequenceNumber(int64_t sequenceNumber);

    void setDRProtocolVersion(uint8_t drVersion);

    bool m_enabled;

    static int32_t getTestDRBuffer(char *out);
private:
    void transactionChecks(int64_t lastCommittedSpHandle, int64_t txnId, int64_t spHandle, int64_t uniqueId);

    void writeRowTuple(TableTuple& tuple,
            size_t rowHeaderSz,
            size_t rowMetadataSz,
            const std::vector<int> *interestingColumns,
            const std::pair<const TableIndex*, uint32_t> &indexPair,
            ExportSerializeOutput &io);

    size_t computeOffsets(DRRecordType &type,
            const std::pair<const TableIndex*, uint32_t> &indexPair,
            TableTuple &tuple,
            size_t &rowHeaderSz,
            size_t &rowMetadataSz,
            const std::vector<int> *&interestingColumns);

    CatalogId m_partitionId;
    size_t m_secondaryCapacity;
    bool m_opened;
    int64_t m_rowTarget;
    size_t m_txnRowCount;
    int64_t m_lastCommittedSpUniqueId;
    int64_t m_lastCommittedMpUniqueId;
    uint8_t m_drVersion;
};

class MockDRTupleStream : public DRTupleStream {
public:
    MockDRTupleStream() : DRTupleStream() {}
    size_t appendTuple(int64_t lastCommittedSpHandle,
                           char *tableHandle,
                           int64_t txnId,
                           int64_t spHandle,
                           int64_t uniqueId,
                           TableTuple &tuple,
                           DRRecordType type,
                           const std::pair<const TableIndex*, uint32_t>& indexPair) {
        return 0;
    }

    void pushExportBuffer(StreamBlock *block, bool sync, bool endOfStream) {}

    void rollbackTo(size_t mark, size_t drRowCost) {}

    size_t truncateTable(int64_t lastCommittedSpHandle,
                       char *tableHandle,
                       std::string tableName,
                       int64_t txnId,
                       int64_t spHandle,
                       int64_t uniqueId) {
        return 0;
    }
};

class DRTupleStreamDisableGuard {
public:
    DRTupleStreamDisableGuard(DRTupleStream *drStream, DRTupleStream *drReplicatedStream, bool ignore) :
            m_drStream(drStream), m_drReplicatedStream(drReplicatedStream), m_drStreamOldValue(drStream->m_enabled),
            m_drReplicatedStreamOldValue(m_drReplicatedStream?m_drReplicatedStream->m_enabled:false)
    {
        if (!ignore) {
            setGuard();
        }
    }
    DRTupleStreamDisableGuard(DRTupleStream *drStream, DRTupleStream *drReplicatedStream) :
            m_drStream(drStream), m_drReplicatedStream(drReplicatedStream), m_drStreamOldValue(drStream->m_enabled),
            m_drReplicatedStreamOldValue(m_drReplicatedStream?m_drReplicatedStream->m_enabled:false)
    {
        setGuard();
    }
    ~DRTupleStreamDisableGuard() {
        m_drStream->m_enabled = m_drStreamOldValue;
        if (m_drReplicatedStream) {
            m_drReplicatedStream->m_enabled = m_drReplicatedStreamOldValue;
        }
    }

private:
    inline void setGuard() {
        m_drStream->m_enabled = false;
        if (m_drReplicatedStream) {
            m_drReplicatedStream->m_enabled = false;
        }
    }

    DRTupleStream *m_drStream;
    DRTupleStream *m_drReplicatedStream;
    const bool m_drStreamOldValue;
    const bool m_drReplicatedStreamOldValue;
};

}

#endif
