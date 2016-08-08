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

#ifndef COMPATIBLEDRTUPLESTREAM_H_
#define COMPATIBLEDRTUPLESTREAM_H_

#include "storage/AbstractDRTupleStream.h"

// IMPORTANT: DON'T CHANGE THIS FILE, THIS IS A FIXED VERSION OF DR STREAM ONLY FOR COMPATIBILITY MODE.

namespace voltdb {
class StreamBlock;

class CompatibleDRTupleStream : public AbstractDRTupleStream {
public:
    //Version(1), type(1), drId(8), uniqueId(8), checksum(4)
    static const size_t BEGIN_RECORD_SIZE = 1 + 1 + 8 + 8 + 4;
    //Version(1), type(1), drId(8), checksum(4)
    static const size_t END_RECORD_SIZE = 1 + 1 + 8 + 4;
    //Version(1), type(1), table signature(8), checksum(4)
    static const size_t TXN_RECORD_HEADER_SIZE = 1 + 1 + 4 + 8;

    static const uint8_t COMPATIBLE_PROTOCOL_VERSION = 3;

    CompatibleDRTupleStream(int partitionId, int defaultBufferSize);

    virtual ~CompatibleDRTupleStream() {}

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
                       DRRecordType type);

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
                       TableTuple &newTuple);

    virtual size_t truncateTable(int64_t lastCommittedSpHandle,
                       char *tableHandle,
                       std::string tableName,
                       int partitionColumn,
                       int64_t spHandle,
                       int64_t uniqueId);

    virtual void beginTransaction(int64_t sequenceNumber, int64_t spHandle, int64_t uniqueId);
    // If a transaction didn't generate any binary log data, calling this
    // would be a no-op because it was never begun.
    virtual void endTransaction(int64_t uniqueId);

    virtual bool checkOpenTransaction(StreamBlock *sb, size_t minLength, size_t& blockSize, size_t& uso);

    virtual DRCommittedInfo getLastCommittedSequenceNumberAndUniqueIds()
    {
        return DRCommittedInfo(m_committedSequenceNumber, m_lastCommittedSpUniqueId, m_lastCommittedMpUniqueId);
    }

    virtual void generateDREvent(DREventType type, int64_t lastCommittedSpHandle, int64_t spHandle,
                                 int64_t uniqueId, ByteArray payloads)
    {
        // This method is not supported in compatible stream
    }

    static int32_t getTestDRBuffer(int32_t partitionId,
                                   std::vector<int32_t> partitionKeyValueList,
                                   std::vector<int32_t> flagList,
                                   long startSequenceNumber,
                                   char *out);
private:
    bool transactionChecks(int64_t lastCommittedSpHandle, int64_t spHandle, int64_t uniqueId);

    void writeRowTuple(TableTuple& tuple,
            size_t rowHeaderSz,
            size_t rowMetadataSz,
            ExportSerializeOutput &io);

    size_t computeOffsets(DRRecordType &type,
            TableTuple &tuple,
            size_t &rowHeaderSz,
            size_t &rowMetadataSz);

    int64_t m_lastCommittedSpUniqueId;
    int64_t m_lastCommittedMpUniqueId;
};

}
#endif
