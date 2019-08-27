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

#ifndef EXPORTTUPLESTREAM_H_
#define EXPORTTUPLESTREAM_H_
#include "common/ids.h"
#include "common/tabletuple.h"
#include "common/FatalException.hpp"
#include "storage/TupleStreamBase.h"
#include <deque>
#include <cassert>
namespace voltdb {

class StreamBlock;

//If you change this constant here change it in Java in the StreamBlockQueue where
//it is used to calculate the number of bytes queued
//I am not sure if the statements on the previous 2 lines are correct. I didn't see anything in SBQ that would care
//It just reports the size of used bytes and not the size of the allocation
//Add a 4k page at the end for bytes beyond the 2 meg row limit due to null mask and length prefix and so on
//Necessary for very large rows
const int EL_BUFFER_SIZE = /* 1024; */ (2 * 1024 * 1024) + MAGIC_HEADER_SPACE_FOR_JAVA + (4096 - MAGIC_HEADER_SPACE_FOR_JAVA);

class ExportTupleStream : public voltdb::TupleStreamBase {
public:
    enum Type { INSERT, DELETE };

    ExportTupleStream(CatalogId partitionId, int64_t siteId, int64_t generation, const std::string &tableName, const std::vector<std::string> &columnNames);

    virtual ~ExportTupleStream() {
    }

    void setGeneration(int64_t generation);

    /** Read the total bytes used over the life of the stream */
    size_t bytesUsed() {
        return m_uso;
    }

    int64_t getSequenceNumber() {
        return m_exportSequenceNumber;
    }

    /** Set the total number of bytes used and starting sequence number for new buffer (for rejoin/recover) */
    void setBytesUsed(int64_t seqNo, size_t count) {
        assert(m_uso == 0);
        m_uso = count;
        // this is for start sequence number of stream block
        m_exportSequenceNumber = seqNo;
        //Extend the buffer chain to replace any existing stream blocks with a new one
        //with the correct sequence number
        extendBufferChain(0);
    }

    inline size_t getTextStringSerializedSize(const std::string &value) const {
        return value.length() + sizeof(int32_t);
    }

    // compute # of bytes needed to serialize the meta data column names
    inline size_t getMDColumnNamesSerializedSize() const { return s_mdSchemaSize; }

    int64_t debugAllocatedBytesInEE() const {
        DummyTopend* te = static_cast<DummyTopend*>(ExecutorContext::getPhysicalTopend());
        int64_t flushedBytes = te->getFlushedExportBytes(m_partitionId);
        return (m_pendingBlocks.size() * (m_defaultCapacity - m_headerSpace)) + flushedBytes;
    }

    void pushStreamBuffer(StreamBlock *block, bool sync);
    void pushEndOfStream();

    /** write a tuple to the stream */
    virtual size_t appendTuple(int64_t lastCommittedSpHandle,
            int64_t spHandle,
            int64_t seqNo,
            int64_t uniqueId,
            int64_t timestamp,
            const TableTuple &tuple,
            int partitionColumn,
            ExportTupleStream::Type type);

    size_t computeOffsets(const TableTuple &tuple, size_t *rowHeaderSz) const;
    size_t computeSchemaSize(const std::string &tableName, const std::vector<std::string> &columnNames);
    void writeSchema(ExportSerializeOutput &hdr, const TableTuple &tuple);

    virtual int partitionId() { return m_partitionId; }
    void setNew() { m_new = true; }

public:
    // Computed size for metadata columns
    static const size_t s_mdSchemaSize;
    // Size of Fixed header (not including schema)
    static const size_t s_FIXED_BUFFER_HEADER_SIZE;
    // Size of Fixed buffer header (rowCount + uniqueId)
    static const size_t s_EXPORT_BUFFER_HEADER_SIZE;

private:
    // cached catalog values
    const CatalogId m_partitionId;
    const int64_t m_siteId;

    // This indicates that stream is new or has been marked as new after UAC so that we include schema in next export stream write.
    bool m_new;
    int64_t m_generation;
    const std::string &m_tableName;
    const std::vector<std::string> &m_columnNames;
    const int32_t m_ddlSchemaSize;

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
