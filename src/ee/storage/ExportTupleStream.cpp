/* This file is part of VoltDB.
 * Copyright (C) 2008-2017 VoltDB Inc.
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
#include "common/types.h"
#include "common/NValue.hpp"
#include "common/ValuePeeker.hpp"
#include "common/tabletuple.h"
#include "common/ExportSerializeIo.h"
#include "common/executorcontext.hpp"

#include <cstdio>
#include <limits>
#include <iostream>
#include <cassert>
#include <ctime>
#include <utility>
#include <math.h>

using namespace std;
using namespace voltdb;

const std::string ExportTupleStream::VOLT_TRANSACTION_ID = "VOLT_TRANSACTION_ID";
const std::string ExportTupleStream::VOLT_EXPORT_TIMESTAMP = "VOLT_EXPORT_TIMESTAMP";
const std::string ExportTupleStream::VOLT_EXPORT_SEQUENCE_NUMBER = "VOLT_EXPORT_SEQUENCE_NUMBER";
const std::string ExportTupleStream::VOLT_PARTITION_ID = "VOLT_PARTITION_ID";
const std::string ExportTupleStream::VOLT_SITE_ID = "VOLT_SITE_ID";
const std::string ExportTupleStream::VOLT_EXPORT_OPERATION = "VOLT_EXPORT_OPERATION";

ExportTupleStream::ExportTupleStream(CatalogId partitionId,
                                       int64_t siteId)
    : TupleStreamBase(EL_BUFFER_SIZE),
      m_partitionId(partitionId),
      m_siteId(siteId),
      m_signature(""),
      m_generation(0),
      m_mdColumnNamesSerializedSize(getTextStringSerializedSize(VOLT_TRANSACTION_ID)
              + getTextStringSerializedSize(VOLT_EXPORT_TIMESTAMP)
              + getTextStringSerializedSize(VOLT_EXPORT_SEQUENCE_NUMBER)
              + getTextStringSerializedSize(VOLT_PARTITION_ID)
              + getTextStringSerializedSize(VOLT_SITE_ID)
              + getTextStringSerializedSize(VOLT_EXPORT_OPERATION))
{}

void ExportTupleStream::setSignatureAndGeneration(std::string signature, int64_t generation) {
    assert(generation > m_generation);
    assert(signature == m_signature || m_signature == string(""));

    //The first time through this is catalog load and m_generation will be 0
    //Don't send the end of stream notice.
    if (generation != m_generation && m_generation > 0) {
        //Notify that no more data is coming from this generation.
        ExecutorContext::getExecutorContext()->getTopend()->pushExportBuffer(
                m_generation,
                m_partitionId,
                m_signature,
                NULL,
                false,
                true);
        /*
         * With the new generational code the USO is reset to 0 for each
         * generation. The sequence number stored on the table outside the wrapper
         * is not reset and remains constant. USO is really just for transport purposes.
         */
        m_uso = 0;
        m_openSpHandle = 0;
        m_openTransactionUso = 0;
        m_committedSpHandle = 0;
        m_committedUso = 0;
        //Reconstruct the next block so it has a USO of 0.
        assert(m_currBlock->offset() == 0);
        extendBufferChain(m_defaultCapacity);
    }
    m_signature = signature;
    m_generation = generation;
}

#include <sstream>

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
        const std::vector<std::string> &columnNames,
        int partitionColumn,
        ExportTupleStream::Type type)
{
    assert(columnNames.size() == tuple.sizeInValues());
    size_t rowHeaderSz = 0;
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

    //Most of the transaction id info and unique id info supplied to commit
    //is nonsense since it isn't currently supplied with a transaction id
    //but it is fine since export isn't currently using the info
    commit(lastCommittedSpHandle, spHandle, uniqueId, false, false);

    // Compute the upper bound on bytes required to serialize tuple.
    // exportxxx: can memoize this calculation.
    tupleMaxLength = computeOffsets(tuple, &rowHeaderSz);
    if (!m_currBlock) {
        extendBufferChain(m_defaultCapacity);
    }
    //Compute column names size
    size_t colNamesLength = m_mdColumnNamesSerializedSize;
    for (int i = 0; i < columnNames.size(); i++) {
        colNamesLength += getTextStringSerializedSize(columnNames[i]);
    }

    if (m_currBlock->remaining() < (tupleMaxLength + colNamesLength)) {
        extendBufferChain(tupleMaxLength + colNamesLength);
    }

    // initialize the full row header to 0. This also
    // has the effect of setting each column non-null.
    ::memset(m_currBlock->mutableDataPtr(), 0, rowHeaderSz);

    // the nullarray lives in rowheader after the 4 byte header length prefix + 4 bytes for column count
    uint8_t *nullArray =
      reinterpret_cast<uint8_t*>(m_currBlock->mutableDataPtr() + sizeof (int32_t) + sizeof (int32_t));

    // position the serializer after the full rowheader
    ExportSerializeOutput io(m_currBlock->mutableDataPtr() + rowHeaderSz,
                             m_currBlock->remaining() - rowHeaderSz);

    //Write partition column index
    io.writeInt(METADATA_COL_CNT + partitionColumn);
    // write metadata column names
    io.writeTextString(VOLT_TRANSACTION_ID);
    io.writeTextString(VOLT_EXPORT_TIMESTAMP);
    io.writeTextString(VOLT_EXPORT_SEQUENCE_NUMBER);
    io.writeTextString(VOLT_PARTITION_ID);
    io.writeTextString(VOLT_SITE_ID);
    io.writeTextString(VOLT_EXPORT_OPERATION);
    // write table column names
    for (int i = 0; i < columnNames.size(); i++) {
        io.writeTextString(columnNames[i]);
    }

    // write metadata columns
    io.writeEnumInSingleByte(VALUE_TYPE_BIGINT);
    io.writeLong(spHandle);
    io.writeEnumInSingleByte(VALUE_TYPE_BIGINT);
    io.writeLong(timestamp);
    io.writeEnumInSingleByte(VALUE_TYPE_BIGINT);
    io.writeLong(seqNo);
    io.writeEnumInSingleByte(VALUE_TYPE_BIGINT);
    io.writeLong(m_partitionId);
    io.writeEnumInSingleByte(VALUE_TYPE_BIGINT);
    io.writeLong(m_siteId);

    // use 1 for INSERT EXPORT op, 0 for DELETE EXPORT op
    io.writeEnumInSingleByte(VALUE_TYPE_TINYINT);
    io.writeByte(static_cast<int8_t>((type == INSERT) ? 1L : 0L));
    // write the tuple's data
    tuple.serializeToExport(io, METADATA_COL_CNT, nullArray, true);

    // write column count - after row size and before null array
    ExportSerializeOutput columnCount(m_currBlock->mutableDataPtr() + 4, sizeof(int32_t));
    columnCount.writeInt(METADATA_COL_CNT + tuple.sizeInValues());

    // write the row size in to the row header
    // rowlength does not include the 4 byte row header
    // but does include the null array.
    ExportSerializeOutput hdr(m_currBlock->mutableDataPtr(), 4);
    hdr.writeInt((int32_t)(io.position()) + (int32_t)rowHeaderSz - 4);

    // update m_offset
    m_currBlock->consumed(rowHeaderSz + io.position());

    // update uso.
    const size_t startingUso = m_uso;
    m_uso += (rowHeaderSz + io.position());
//    cout << "Appending row " << rowHeaderSz + io.position() << " to uso " << m_currBlock->uso()
//            << " offset " << m_currBlock->offset() << std::endl;
    return startingUso;
}

size_t
ExportTupleStream::computeOffsets(const TableTuple &tuple, size_t *rowHeaderSz) const {
    // round-up columncount to next multiple of 8 and divide by 8
    int columnCount = tuple.sizeInValues() + METADATA_COL_CNT;
    int nullMaskLength = ((columnCount + 7) & -8) >> 3;

    // row header is 32-bit length of row plus null mask plus four bytes for storing column-count
    *rowHeaderSz = sizeof (int32_t) + nullMaskLength + sizeof(int32_t);

    // size needed for storing values (data + type) of metadata column: 5 int64_ts plus CHAR(1) + 6 bytes for type.
    size_t metadataSz = (sizeof (int64_t) * 5) + 1 + 6;

    // returns 0 if corrupt tuple detected
    size_t dataSz = tuple.maxExportSerializationSize();
    if (dataSz == 0) {
        throwFatalException("Invalid tuple passed to computeTupleMaxLength. Crashing System.");
    }

    // row header + metadata value size + tuple column count for types + max export serialization size
    return *rowHeaderSz + metadataSz + tuple.sizeInValues() + dataSz;
}

void ExportTupleStream::pushExportBuffer(StreamBlock *block, bool sync, bool endOfStream) {
    ExecutorContext::getExecutorContext()->getTopend()->pushExportBuffer(
                    m_generation,
                    m_partitionId,
                    m_signature,
                    block,
                    sync,
                    endOfStream);
}
