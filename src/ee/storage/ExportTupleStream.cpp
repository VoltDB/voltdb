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

#include "storage/ExportTupleStream.h"

#include "common/TupleSchema.h"

#include <cstdio>
#include <limits>
#include <ctime>
#include <utility>
#include <math.h>

using namespace std;
using namespace voltdb;

const std::string ExportTupleStream::VOLT_TRANSACTION_ID = "VOLT_TRANSACTION_ID"; // 19 + sizeof(int32_t)
const std::string ExportTupleStream::VOLT_EXPORT_TIMESTAMP = "VOLT_EXPORT_TIMESTAMP"; // 21 + sizeof(int32_t)
const std::string ExportTupleStream::VOLT_EXPORT_SEQUENCE_NUMBER = "VOLT_EXPORT_SEQUENCE_NUMBER"; // 27 + sizeof(int32_t)
const std::string ExportTupleStream::VOLT_PARTITION_ID = "VOLT_PARTITION_ID"; // 17 + sizeof(int32_t)
const std::string ExportTupleStream::VOLT_SITE_ID = "VOLT_SITE_ID"; // 12 + sizeof(int32_t);
const std::string ExportTupleStream::VOLT_EXPORT_OPERATION = "VOLT_EXPORT_OPERATION"; // 21 + sizeof(int32_t)
//Change this constant if anything changes with metadata column names number etc. (171)
const size_t ExportTupleStream::s_mdSchemaSize = (19 + 21 + 27 + 17 + 12 + 21 //Size of string column names
                                                                + ExportTupleStream::METADATA_COL_CNT // Volt Type byte
                                                                + (ExportTupleStream::METADATA_COL_CNT * sizeof(int32_t)) // Int for column names string size
                                                                + (ExportTupleStream::METADATA_COL_CNT * sizeof(int32_t))); // column length colInfo->length
const size_t ExportTupleStream::s_EXPORT_BUFFER_HEADER_SIZE = 12; // row count(4) + uniqueId(8)
const size_t ExportTupleStream::s_FIXED_BUFFER_HEADER_SIZE = 13; // Size of header before schema: Version(1) + GenerationId(8) + SchemaLen(4)
const uint8_t ExportTupleStream::s_EXPORT_BUFFER_VERSION = 1;

ExportTupleStream::ExportTupleStream(CatalogId partitionId, int64_t siteId, int64_t generation,
                                     const std::string &tableName, const std::vector<std::string> &columnNames)
    : TupleStreamBase(EL_BUFFER_SIZE, computeSchemaSize(tableName, columnNames) + s_FIXED_BUFFER_HEADER_SIZE + s_EXPORT_BUFFER_HEADER_SIZE),
      m_partitionId(partitionId),
      m_siteId(siteId),
      m_generation(generation),
      m_tableName(tableName),
      m_columnNames(columnNames),
      m_ddlSchemaSize(m_headerSpace - MAGIC_HEADER_SPACE_FOR_JAVA - s_FIXED_BUFFER_HEADER_SIZE - s_EXPORT_BUFFER_HEADER_SIZE)
{
    m_new = true;
}

void ExportTupleStream::setGeneration(int64_t generation) {
    assert(generation > m_generation);
    m_generation = generation;
}

/*
 * If SpHandle represents a new transaction, commit previous data.
 * Always serialize the supplied tuple in to the stream.
 * Return m_uso before this invocation - this marks the point
 * in the stream the caller can rollback to if this append
 * should be rolled back.
 */
size_t ExportTupleStream::appendTuple(int64_t lastCommittedSpHandle,
        int64_t spHandle,
        int64_t seqNo,
        int64_t uniqueId,
        int64_t timestamp,
        const TableTuple &tuple,
        int partitionColumn,
        ExportTupleStream::Type type)
{
    assert(m_columnNames.size() == tuple.columnCount());
    size_t streamHeaderSz = 0;
    size_t tupleMaxLength = 0;
    // update sequence number
    m_exportSequenceNumber = seqNo;

    // Transaction IDs for transactions applied to this tuple stream
    // should always be moving forward in time.
    if (spHandle < m_openSpHandle)
    {
        throwFatalException(
                "Active transactions moving backwards: openSpHandle is %jd, while the append spHandle is %jd",
                (intmax_t)m_openSpHandle, (intmax_t)spHandle
                );
    }

    //Most of the transaction id info and unique id info supplied to commit
    //is nonsense since it isn't currently supplied with a transaction id
    //but it is fine since export isn't currently using the info
    commit(lastCommittedSpHandle, spHandle, uniqueId, false, m_new);

    // Compute the upper bound on bytes required to serialize tuple.
    // exportxxx: can memoize this calculation.
    tupleMaxLength = computeOffsets(tuple, &streamHeaderSz);
    //First time always include schema.
    if (!m_currBlock) {
        extendBufferChain(m_defaultCapacity);
    }
    if ((m_currBlock->remaining() < tupleMaxLength) ) {
        //If we can not fit the data get a new block with size that includes schemaSize as well.
        extendBufferChain(tupleMaxLength);
    }
    // Mark the start sequence number of current block, starts from 1.
    if (m_currBlock->startExportSequenceNumber() == 0) {
        m_currBlock->recordStartExportSequenceNumber(m_exportSequenceNumber);
    }
    bool includeSchema = m_currBlock->needsSchema();
    if (includeSchema) {
        ExportSerializeOutput blkhdr(m_currBlock->headerDataPtr()+s_EXPORT_BUFFER_HEADER_SIZE,
                          m_currBlock->headerSize() - (MAGIC_HEADER_SPACE_FOR_JAVA+s_EXPORT_BUFFER_HEADER_SIZE));
        // FIXED_BUFFER_HEADER
        // version and generation Id for the buffer
        blkhdr.writeByte(s_EXPORT_BUFFER_VERSION);
        blkhdr.writeLong(m_generation);
        // length of schema header
        blkhdr.writeInt(m_ddlSchemaSize);
        // Schema
        writeSchema(blkhdr, tuple);
        m_currBlock->noSchema();
    }

    // initialize the full row header to 0. This also
    // has the effect of setting each column non-null.
    ::memset(m_currBlock->mutableDataPtr(), 0, streamHeaderSz);

    // the nullarray lives in rowheader after the 4 byte header length prefix + 4 bytes for column count +
    // 4 partition index
    uint8_t *nullArray =
      reinterpret_cast<uint8_t*>(m_currBlock->mutableDataPtr()
              + sizeof(int32_t)         // row length
              + sizeof(int32_t)         // partition index
              + sizeof(int32_t)         // column count
              );

    // position the serializer after the full rowheader
    ExportSerializeOutput io(m_currBlock->mutableDataPtr() + streamHeaderSz, m_currBlock->remaining() - streamHeaderSz);

    // write metadata columns - data we always write this.
    io.writeLong(spHandle);
    io.writeLong(timestamp);
    io.writeLong(seqNo);
    io.writeLong(m_partitionId);
    io.writeLong(m_siteId);
    // use 1 for INSERT EXPORT op, 0 for DELETE EXPORT op
    io.writeByte(static_cast<int8_t>((type == INSERT) ? 1L : 0L));
    // write the tuple's data
    tuple.serializeToExport(io, METADATA_COL_CNT, nullArray);

    m_stashedTupleCount++;

    // row size, generation, partition-index, column count and hasSchema flag (byte)
    ExportSerializeOutput hdr(m_currBlock->mutableDataPtr(), streamHeaderSz);
    // write the row size in to the row header rowlength does not include
    // the 4 byte row header but does include the null array.
    hdr.writeInt((int32_t)(io.position()) + (int32_t)streamHeaderSz - 4);
    hdr.writeInt(METADATA_COL_CNT + partitionColumn);           // partition index
    hdr.writeInt(METADATA_COL_CNT + tuple.columnCount());      // column count

    // update m_offset
    m_currBlock->consumed(streamHeaderSz + io.position());

    // update uso.
    const size_t startingUso = m_uso;
    m_uso += (streamHeaderSz + io.position());
    m_currBlock->recordCompletedSpTxn(uniqueId);
    cout << "Appending row " << streamHeaderSz + io.position() << " to uso " << m_currBlock->uso()
            << " sequence number " << seqNo
            << " offset " << m_currBlock->offset() << std::endl;
    //Not new anymore as we have new transaction after UAC
    m_new = false;
    return startingUso;
}

//Computes full schema size includes metadata columns.
size_t
ExportTupleStream::computeSchemaSize(const std::string &tableName, const std::vector<std::string> &columnNames) {
    // column names size for metadata columns
    size_t schemaSz = s_mdSchemaSize;
    // table name size
    schemaSz += getTextStringSerializedSize(tableName);
    // Column name sizes for table columns + Column length field.
    for (int i = 0; i < columnNames.size(); i++) {
        schemaSz += getTextStringSerializedSize(columnNames[i]);
        schemaSz += sizeof(int32_t);
    }
    // Add type byte for every column
    schemaSz += columnNames.size();
    return schemaSz;
}

void
ExportTupleStream::writeSchema(ExportSerializeOutput &hdr, const TableTuple &tuple) {
    // table name
    hdr.writeTextString(m_tableName);

    // encode name, type, column length
    hdr.writeTextString(VOLT_TRANSACTION_ID);
    hdr.writeEnumInSingleByte(VALUE_TYPE_BIGINT);
    hdr.writeInt(sizeof(int64_t));

    hdr.writeTextString(VOLT_EXPORT_TIMESTAMP);
    hdr.writeEnumInSingleByte(VALUE_TYPE_BIGINT);
    hdr.writeInt(sizeof(int64_t));

    hdr.writeTextString(VOLT_EXPORT_SEQUENCE_NUMBER);
    hdr.writeEnumInSingleByte(VALUE_TYPE_BIGINT);
    hdr.writeInt(sizeof(int64_t));

    hdr.writeTextString(VOLT_PARTITION_ID);
    hdr.writeEnumInSingleByte(VALUE_TYPE_BIGINT);
    hdr.writeInt(sizeof(int64_t));

    hdr.writeTextString(VOLT_SITE_ID);
    hdr.writeEnumInSingleByte(VALUE_TYPE_BIGINT);
    hdr.writeInt(sizeof(int64_t));

    hdr.writeTextString(VOLT_EXPORT_OPERATION);
    hdr.writeEnumInSingleByte(VALUE_TYPE_TINYINT);
    hdr.writeInt(sizeof(int8_t));

    const TupleSchema::ColumnInfo *columnInfo;
    // encode table columns name, type, length
    for (int i = 0; i < m_columnNames.size(); i++) {
        hdr.writeTextString(m_columnNames[i]);
        columnInfo = tuple.getSchema()->getColumnInfo(i);
        assert (columnInfo != NULL);
        hdr.writeEnumInSingleByte(columnInfo->getVoltType());
        hdr.writeInt(columnInfo->length);
    }
}

size_t
ExportTupleStream::computeOffsets(const TableTuple &tuple, size_t *streamHeaderSz) const {
    // round-up columncount to next multiple of 8 and divide by 8
    int columnCount = tuple.columnCount() + METADATA_COL_CNT;
    int nullMaskLength = ((columnCount + 7) & -8) >> 3;

    // tuple stream header
    *streamHeaderSz = sizeof (int32_t)      // row size
            + sizeof (int32_t)           // partition index
            + sizeof (int32_t)           // column count
            + nullMaskLength;           // null array

    // returns 0 if corrupt tuple detected
    size_t dataSz = tuple.maxExportSerializationSize();
    if (dataSz == 0) {
        throwFatalException("Invalid tuple passed to computeTupleMaxLength. Crashing System.");
    }
    //Data size for metadata columns.
    dataSz += (5 * sizeof(int64_t)) + 1;

    return *streamHeaderSz              // row header
            + dataSz;                   // non-null tuple data
}

void ExportTupleStream::pushStreamBuffer(StreamBlock *block, bool sync) {
    ExecutorContext::getPhysicalTopend()->pushExportBuffer(
                    m_partitionId,
                    m_tableName,
                    block,
                    sync);
}

void ExportTupleStream::pushEndOfStream() {
    ExecutorContext::getPhysicalTopend()->pushEndOfStream(
                    m_partitionId,
                    m_tableName);
}
