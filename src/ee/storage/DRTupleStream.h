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

// Extra space to write a StoredProcedureInvocation wrapper in Java without copying
const int MAGIC_DR_TRANSACTION_PADDING = 69;
const int SECONDARY_BUFFER_SIZE = (45 * 1024 * 1024) + 4096;

class DRTupleStream : public voltdb::TupleStreamBase {
public:
    //Version(1), type(1), drId(8), uniqueId(8), checksum(4)
    static const size_t BEGIN_RECORD_SIZE = 1 + 1 + 8 + 8 + 4;
    //Version(1), type(1), drId(8), checksum(4)
    static const size_t END_RECORD_SIZE = 1 + 1 + 8 + 4;
    //Version(1), type(1), table signature(8), checksum(4)
    static const size_t TXN_RECORD_HEADER_SIZE = 1 + 1 + 4 + 8;
    static const uint8_t DR_VERSION = 1;

    DRTupleStream();

    virtual ~DRTupleStream() {
    }

    void configure(CatalogId partitionId) {
        m_partitionId = partitionId;
    }

    // for test purpose
    virtual void setSecondaryCapacity(size_t capacity);

    virtual void rollbackTo(size_t mark);

    virtual void pushExportBuffer(StreamBlock *block, bool sync, bool endOfStream);

    /** write a tuple to the stream */
    virtual size_t appendTuple(int64_t lastCommittedSpHandle,
                       char *tableHandle,
                       int64_t txnId,
                       int64_t spHandle,
                       int64_t uniqueId,
                       TableTuple &tuple,
                       DRRecordType type);

    virtual size_t truncateTable(int64_t lastCommittedSpHandle,
                       char *tableHandle,
                       std::string tableName,
                       int64_t txnId,
                       int64_t spHandle,
                       int64_t uniqueId);

    size_t computeOffsets(TableTuple &tuple, size_t &rowHeaderSz, size_t &rowMetadataSz);

    void beginTransaction(int64_t sequenceNumber, int64_t uniqueId);
    // If a transaction didn't generate any binary log data, calling this
    // would be a no-op because it was never begun.
    void endTransaction();

    bool checkOpenTransaction(StreamBlock *sb, size_t minLength, size_t& blockSize, size_t& uso);

    std::pair<int64_t, int64_t> getLastCommittedSequenceNumberAndUniqueId() { return std::pair<int64_t, int64_t>(m_committedSequenceNumber, m_committedUniqueId); }
    void setLastCommittedSequenceNumber(int64_t sequenceNumber);

    bool m_enabled;

    static int32_t getTestDRBuffer(char *out);
private:
    CatalogId m_partitionId;
    size_t m_secondaryCapacity;
    bool m_opened;
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
                           DRRecordType type) {
        return 0;
    }

    void pushExportBuffer(StreamBlock *block, bool sync, bool endOfStream) {}

    void rollbackTo(size_t mark) {}

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
    DRTupleStreamDisableGuard(DRTupleStream *stream) : m_stream(stream), m_oldValue(stream->m_enabled) {
        stream->m_enabled = false;
    }
    ~DRTupleStreamDisableGuard() {
        m_stream->m_enabled = m_oldValue;
    }
private:
    DRTupleStream *m_stream;
    const bool m_oldValue;
};

}

#endif
