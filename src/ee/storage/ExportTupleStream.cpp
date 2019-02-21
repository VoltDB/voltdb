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

#include "storage/ExportTupleStream.h"
#include "execution/VoltDBEngine.h"
#include "common/TupleSchema.h"
#include "common/UniqueId.hpp"

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
                                     std::string signature, const std::string &tableName,
                                     const std::vector<std::string> &columnNames)
    : TupleStreamBase(EL_BUFFER_SIZE, computeSchemaSize(tableName, columnNames) + s_FIXED_BUFFER_HEADER_SIZE + s_EXPORT_BUFFER_HEADER_SIZE),
      m_partitionId(partitionId),
      m_siteId(siteId),
      m_signature(signature),
      m_generation(generation),
      m_tableName(tableName),
      m_columnNames(columnNames),
      m_ddlSchemaSize(m_headerSpace - MAGIC_HEADER_SPACE_FOR_JAVA - s_FIXED_BUFFER_HEADER_SIZE - s_EXPORT_BUFFER_HEADER_SIZE),
      m_nextSequenceNumber(1),
      m_committedSequenceNumber(0),
      m_flushPending(false),
      m_nextFlushStream(NULL),
      m_prevFlushStream(NULL)

{
    extendBufferChain(m_defaultCapacity);
    m_new = true;
}

void ExportTupleStream::setSignatureAndGeneration(std::string signature, int64_t generation) {
    assert(generation > m_generation);
    assert(signature == m_signature || m_signature == string(""));

    m_signature = signature;
    m_generation = generation;
}

/*
 * If SpHandle represents a new transaction, commit previous data.
 * Always serialize the supplied tuple in to the stream.
 * Return m_uso before this invocation - this marks the point
 * in the stream the caller can rollback to if this append
 * should be rolled back.
 */
size_t ExportTupleStream::appendTuple(
        VoltDBEngine* engine,
        int64_t spHandle,
        int64_t seqNo,
        int64_t uniqueId,
        const TableTuple &tuple,
        int partitionColumn,
        ExportTupleStream::Type type)
{
    assert(m_columnNames.size() == tuple.columnCount());
    size_t streamHeaderSz = 0;
    size_t tupleMaxLength = 0;

    // Transaction IDs for transactions applied to this tuple stream
    // should always be moving forward in time.
    if (spHandle < m_openSpHandle)
    {
        throwFatalException(
                "Active transactions moving backwards: openSpHandle is %jd, while the append spHandle is %jd",
                (intmax_t)m_openSpHandle, (intmax_t)spHandle
                );
    }
    m_openSpHandle = spHandle;
    m_openUniqueId = uniqueId;

    // Compute the upper bound on bytes required to serialize tuple.
    // exportxxx: can memoize this calculation.
    tupleMaxLength = computeOffsets(tuple, &streamHeaderSz);
    //First time always include schema.
    if (!m_currBlock) {
        extendBufferChain(m_defaultCapacity);
    }
    if ((m_currBlock->remaining() < tupleMaxLength)) {
        //If we can not fit the data get a new block with size that includes schemaSize as well.
        extendBufferChain(tupleMaxLength);
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
    io.writeLong(UniqueId::ts(uniqueId));
    io.writeLong(seqNo);
    io.writeLong(m_partitionId);
    io.writeLong(m_siteId);
    // use 1 for INSERT EXPORT op, 0 for DELETE EXPORT op
    io.writeByte(static_cast<int8_t>((type == INSERT) ? 1L : 0L));
    // write the tuple's data
    tuple.serializeToExport(io, METADATA_COL_CNT, nullArray);

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
    assert(seqNo > 0 && m_nextSequenceNumber == seqNo);
    m_nextSequenceNumber++;
    m_currBlock->recordCompletedSpTxn(uniqueId);
//    cout << "Appending row " << streamHeaderSz + io.position() << " to uso " << m_currBlock->uso()
//            << " sequence number " << seqNo
//            << " offset " << m_currBlock->offset() << std::endl;
    //Not new anymore as we have new transaction after UAC
    m_new = false;
    return startingUso;
}

void ExportTupleStream::appendToList(ExportTupleStream** oldest, ExportTupleStream** newest)
{
    assert(!m_prevFlushStream && !m_nextFlushStream);
    if (*oldest == NULL) {
        *oldest = this;
    }
    else {
        m_prevFlushStream = *newest;
        m_prevFlushStream->m_nextFlushStream = this;
    }
    *newest = this;
}

void ExportTupleStream::stitchToNextNode(ExportTupleStream* next)
{
    m_nextFlushStream = next;
    next->m_prevFlushStream = this;
}

void ExportTupleStream::removeFromFlushList(VoltDBEngine* engine, bool moveToTail)
{
    if (m_flushPending) {
        if (m_nextFlushStream) {
            // We are not at the tail so move this stream to the tail.
            if (m_prevFlushStream) {
                // Remove myself from the middle of the flush list
                assert(m_prevFlushStream->m_nextFlushStream == this);
                m_prevFlushStream->m_nextFlushStream = m_nextFlushStream;
                assert(m_nextFlushStream->m_prevFlushStream == this);
                m_nextFlushStream->m_prevFlushStream = m_prevFlushStream;
            }
            else {
                // Remove myself from the beginning of the flush list
                m_nextFlushStream->m_prevFlushStream = NULL;
                *engine->getOldestExportStreamWithPendingRowsForAssignment() = m_nextFlushStream;
            }
            if (moveToTail) {
                m_prevFlushStream = *engine->getNewestExportStreamWithPendingRowsForAssignment();
                assert(m_prevFlushStream->m_nextFlushStream == NULL);
                m_prevFlushStream->m_nextFlushStream = this;
                *engine->getNewestExportStreamWithPendingRowsForAssignment() = this;
            }
            else {
                m_prevFlushStream = NULL;
                m_flushPending = false;
            }
            m_nextFlushStream = NULL;
        }
        else {
            // If this node is at the end of the list do nothing
            assert(*engine->getNewestExportStreamWithPendingRowsForAssignment() == this);
            assert(m_nextFlushStream == NULL);
            if (!moveToTail) {
                // Removing the end Node
                if (m_prevFlushStream) {
                    m_prevFlushStream->m_nextFlushStream = NULL;
                    *engine->getNewestExportStreamWithPendingRowsForAssignment() = m_prevFlushStream;
                    m_prevFlushStream = NULL;
                }
                else {
                    // End node is also the beginning node
                    *engine->getOldestExportStreamWithPendingRowsForAssignment() = NULL;
                    *engine->getNewestExportStreamWithPendingRowsForAssignment() = NULL;
                }
                m_flushPending = false;
            }
        }
    }
    else {
        if (moveToTail) {
            appendToList(engine->getOldestExportStreamWithPendingRowsForAssignment(),
                    engine->getNewestExportStreamWithPendingRowsForAssignment());
            m_flushPending = true;
        }
    }
}

/*
 * Handoff fully committed blocks to the top end.
 *
 * This is the only function that should modify m_openSpHandle,
 * m_openTransactionUso.
 */
void ExportTupleStream::commit(VoltDBEngine* engine, int64_t currentSpHandle, int64_t uniqueId)
{
    assert(currentSpHandle == m_openSpHandle && uniqueId == m_openUniqueId);

    if (m_uso != m_committedUso) {
        m_committedUso = m_uso;
        m_committedUniqueId = m_openUniqueId;
        // Advance the tip to the new transaction.
        m_committedSpHandle = m_openSpHandle;
        if (m_currBlock->startSequenceNumber() > m_committedSequenceNumber) {
            // Started a new block so reset the flush timeout
            m_lastFlush = UniqueId::ts(m_committedUniqueId);
            removeFromFlushList(engine, true);
        }
        m_committedSequenceNumber = m_nextSequenceNumber-1;

        pushPendingBlocks();
    }
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

void ExportTupleStream::pushStreamBuffer(ExportStreamBlock *block, bool sync) {
    ExecutorContext::getPhysicalTopend()->pushExportBuffer(
                    m_partitionId,
                    m_signature,
                    block,
                    sync);
}

void ExportTupleStream::pushEndOfStream() {
    ExecutorContext::getPhysicalTopend()->pushEndOfStream(
                    m_partitionId,
                    m_signature);
}

/*
 * Create a new buffer and flush all pending committed data.
 * Creating a new buffer will push all queued data into the
 * pending list for commit to operate against.
 */
bool ExportTupleStream::periodicFlush(int64_t timeInMillis,
                                      int64_t lastCommittedSpHandle)
{
    // negative timeInMillis instructs a mandatory flush
    assert(timeInMillis < 0 || (timeInMillis - m_lastFlush > s_exportFlushTimeout));
    if (!m_currBlock || m_currBlock->lastSequenceNumber() > m_committedSequenceNumber) {
        // There is no buffer or the (MP) transaction has not been committed to the buffer yet
        // so don't release the buffer yet
        if (timeInMillis < 0) {
            // Send a null buffer with the sync flag
            pushStreamBuffer(NULL, true);
        }
        return false;
    }

    if (m_currBlock) {
        // Note that if the block is empty the lastSequenceNumber will be startSequenceNumber-1;
        assert(m_currBlock->lastSequenceNumber() == m_committedSequenceNumber);
        // Any blocks before the current block should have been sent already by the commit path
        assert(m_pendingBlocks.empty());
        if (m_flushPending) {
            assert(m_currBlock->getRowCount() > 0);
            // Most paths move a block to m_pendingBlocks and then use pushPendingBlocks (comment from there)
            // The block is handed off to the topend which is responsible for releasing the
            // memory associated with the block data. The metadata is deleted here.
            pushStreamBuffer(m_currBlock, timeInMillis < 0);
            delete m_currBlock;
            m_currBlock = NULL;
            extendBufferChain(0);
            m_prevFlushStream = NULL;
            m_nextFlushStream = NULL;
            m_flushPending = false;
        } else {
            if (timeInMillis < 0) {
                pushStreamBuffer(NULL, true);
            }
        }

        return true;
    }
    // periodicFlush should only be called if this ExportTupleStream was registered with the engine by commit()
    assert(false);
    return false;
}

void ExportTupleStream::extendBufferChain(size_t minLength) {
    size_t blockSize = (minLength <= m_defaultCapacity) ? m_defaultCapacity : m_maxCapacity;
    TupleStreamBase::commonExtendBufferChain(blockSize, m_uso);

    m_currBlock->recordStartSequenceNumber(m_nextSequenceNumber);
}


