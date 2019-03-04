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

#ifndef EXPORTTUPLESTREAM_H_
#define EXPORTTUPLESTREAM_H_
#include "common/ids.h"
#include "common/tabletuple.h"
#include "common/StreamBlock.h"
#include "common/FatalException.hpp"
#include "storage/TupleStreamBase.h"
#include <deque>
#include <cassert>
namespace voltdb {

//If you change this constant here change it in Java in the StreamBlockQueue where
//it is used to calculate the number of bytes queued
//I am not sure if the statements on the previous 2 lines are correct. I didn't see anything in SBQ that would care
//It just reports the size of used bytes and not the size of the allocation
//Add a 4k page at the end for bytes beyond the 2 meg row limit due to null mask and length prefix and so on
//Necessary for very large rows
const int EL_BUFFER_SIZE = /* 1024; */ (2 * 1024 * 1024) + MAGIC_HEADER_SPACE_FOR_JAVA + (4096 - MAGIC_HEADER_SPACE_FOR_JAVA);

class VoltDBEngine;

class ExportTupleStream : public voltdb::TupleStreamBase<ExportStreamBlock> {
    friend class ExportStreamBlock;
    friend class StreamBlock;

public:
    enum Type { INSERT, DELETE };

    ExportTupleStream(CatalogId partitionId, int64_t siteId, int64_t generation,
                      std::string signature, const std::string &tableName,
                      const std::vector<std::string> &columnNames);

    ExportTupleStream(const ExportTupleStream &otherStream,
                      const std::vector<std::string> &columnNames);

    virtual ~ExportTupleStream() {
    }

    void setSignatureAndGeneration(std::string signature, int64_t generation);

    /** Read the total bytes used over the life of the stream */
    size_t bytesUsed() {
        return m_uso;
    }

    int64_t getSequenceNumber() {
        return m_nextSequenceNumber;
    }

    /** Set the total number of bytes used and starting sequence number for new buffer (for rejoin/recover) */
    void setBytesUsed(int64_t seqNo, size_t count) {
        assert(m_uso == 0);
        m_uso = count;
        // this is for start sequence number of stream block
        m_nextSequenceNumber = seqNo + 1;
        //Extend the buffer chain to replace any existing stream blocks with a new one
        //with the correct sequence number
        extendBufferChain(0);
    }

    inline size_t getTextStringSerializedSize(const std::string &value) const {
        return value.length() + sizeof(int32_t);
    }

    int64_t testAllocatedBytesInEE() const {
        DummyTopend* te = static_cast<DummyTopend*>(ExecutorContext::getPhysicalTopend());
        int64_t flushedBytes = te->getFlushedExportBytes(m_partitionId, m_signature);
        return (m_pendingBlocks.size() * (m_defaultCapacity - m_headerSpace)) + flushedBytes;
    }

    void pushStreamBuffer(ExportStreamBlock *block, bool sync);
    void pushEndOfStream();

    /** write a tuple to the stream */
    virtual size_t appendTuple(
            VoltDBEngine* engine,
            int64_t spHandle,
            int64_t seqNo,
            int64_t uniqueId,
            const TableTuple &tuple,
            int partitionColumn,
            ExportTupleStream::Type type);

    /** Close Txn and send full buffers with committed data to the top end. */
    void commit(VoltDBEngine* engine, int64_t spHandle, int64_t uniqueId);
    inline void rollbackExportTo(size_t mark, int64_t seqNo) {
        // make the stream of tuples contiguous outside of actual system failures
        assert(seqNo > m_committedSequenceNumber && m_nextSequenceNumber > m_committedSequenceNumber);
        m_nextSequenceNumber = seqNo;
        TupleStreamBase::rollbackBlockTo(mark);
        m_currBlock->truncateExportTo(mark, seqNo);
    }

    size_t computeOffsets(const TableTuple &tuple, size_t *rowHeaderSz) const;
    size_t computeSchemaSize(const std::string &tableName, const std::vector<std::string> &columnNames);
    void writeSchema(ExportSerializeOutput &hdr, const TableTuple &tuple);

    inline void resetFlushLinkages() {
        m_nextFlushStream = NULL;
        m_prevFlushStream = NULL;
    }
    void appendToList(ExportTupleStream** oldest, ExportTupleStream** newest);
    void stitchToNextNode(ExportTupleStream* next);
    void removeFromFlushList(VoltDBEngine* engine, bool moveToTail);

    /** age out committed data */
    inline bool flushTimerExpired(int64_t timeInMillis) {
        return (timeInMillis < 0 || (timeInMillis - m_lastFlush > s_exportFlushTimeout));
    }
    virtual bool periodicFlush(int64_t timeInMillis,
                               int64_t lastComittedSpHandle);
    virtual void extendBufferChain(size_t minLength);

    virtual int partitionId() { return m_partitionId; }

    inline ExportTupleStream* getNextFlushStream() const {
        return m_nextFlushStream;
    }

    inline bool testFlushPending() {
        return m_flushPending;
    }

    inline int64_t testFlushBuffCreateTime() {
        return m_lastFlush;
    }


public:
    // Size of Fixed buffer header (rowCount + uniqueId)
    static const size_t s_EXPORT_BUFFER_HEADER_SIZE;

private:
    // cached catalog values
    const CatalogId m_partitionId;
    const int64_t m_siteId;

    std::string m_signature;
    int64_t m_generation;
    const std::string &m_tableName;
    const std::vector<std::string> &m_columnNames;

    int64_t m_nextSequenceNumber;
    int64_t m_committedSequenceNumber;

    // Used to track what streams have partial blocks that could be flushed
    bool m_flushPending;
    ExportTupleStream* m_nextFlushStream;
    ExportTupleStream* m_prevFlushStream;

    // Buffer version (used for proper decoding of buffers by standalone processors)
    static const uint8_t s_EXPORT_BUFFER_VERSION;
    // meta-data column count
    static const int METADATA_COL_CNT = 6;

    // column names of meta data columns
    static const std::string VOLT_TRANSACTION_ID;
    static const std::string VOLT_EXPORT_TIMESTAMP;
    static const std::string VOLT_EXPORT_SEQUENCE_NUMBER;
    static const std::string VOLT_PARTITION_ID;
    static const std::string VOLT_SITE_ID;
    static const std::string VOLT_EXPORT_OPERATION;
};



}

#endif
