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

const int METADATA_COL_CNT = 6;

ExportTupleStream::ExportTupleStream(CatalogId partitionId,
                                       int64_t siteId)
    : TupleStreamBase(EL_BUFFER_SIZE),
      m_partitionId(partitionId), m_siteId(siteId),
      m_signature(""), m_generation(0)
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
                                       TableTuple &tuple,
                                       std::vector<std::string> const& columnNames,
                                       int partitionColumn,
                                       ExportTupleStream::Type type)
{
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
//    std::cout << "Hdr: " << rowHeaderSz << " TupleMax: " << tupleMaxLength << "\n";
//    std::cout.flush();
    if (!m_currBlock) {
        extendBufferChain(m_defaultCapacity);
    }
    //Compute column names size
    size_t colNamesLength = 0;
    colNamesLength += getTextStringSerializedSize("VOLT_TRANSACTION_ID");
    colNamesLength += getTextStringSerializedSize("VOLT_EXPORT_TIMESTAMP");
    colNamesLength += getTextStringSerializedSize("VOLT_EXPORT_SEQUENCE_NUMBER");
    colNamesLength += getTextStringSerializedSize("VOLT_PARTITION_ID");
    colNamesLength += getTextStringSerializedSize("VOLT_SITE_ID");
    colNamesLength += getTextStringSerializedSize("VOLT_EXPORT_OPERATION");
    //Treat column count as hash and only allow ADD/DROP column? What about width of columns?
    for (int i = 0; i < columnNames.size(); i++) {
        colNamesLength += getTextStringSerializedSize(columnNames[i]);
    }
    // include type byte
    colNamesLength += METADATA_COL_CNT + columnNames.size();
    if (m_currBlock->remaining() < (tupleMaxLength + colNamesLength)) {
        extendBufferChain(tupleMaxLength + colNamesLength);
    }

    // initialize the full row header to 0. This also
    // has the effect of setting each column non-null.
    ::memset(m_currBlock->mutableDataPtr(), 0, rowHeaderSz);

    // the nullarray lives in rowheader after the 4 byte header length prefix
    uint8_t *nullArray =
      reinterpret_cast<uint8_t*>(m_currBlock->mutableDataPtr() + sizeof (int32_t));

    // position the serializer after the full rowheader
    ExportSerializeOutput io(m_currBlock->mutableDataPtr() + rowHeaderSz,
                             m_currBlock->remaining() - rowHeaderSz);

    //Write column Count
    io.writeInt(METADATA_COL_CNT + columnNames.size());
    //Write partition column index
    io.writeInt(METADATA_COL_CNT + partitionColumn);
    //Write column Names
    io.writeTextString("VOLT_TRANSACTION_ID");
    io.writeTextString("VOLT_EXPORT_TIMESTAMP");
    io.writeTextString("VOLT_EXPORT_SEQUENCE_NUMBER");
    io.writeTextString("VOLT_PARTITION_ID");
    io.writeTextString("VOLT_SITE_ID");
    io.writeTextString("VOLT_EXPORT_OPERATION");
    //Treat column count as hash and only allow ADD/DROP column? What about width of columns?
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

    nullArray[0] = (uint8_t) (METADATA_COL_CNT + tuple.sizeInValues());
    //Write partition column index
    // write the tuple's data
    tuple.serializeToExport(io, METADATA_COL_CNT, &nullArray[1], true);

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
//    std::cout << "Appending row " << rowHeaderSz + io.position() << " to uso " << m_currBlock->uso() << " offset " << m_currBlock->offset() << std::endl;
    return startingUso;
}

size_t
ExportTupleStream::computeOffsets(TableTuple &tuple,
                                   size_t *rowHeaderSz)
{
    // round-up columncount to next multiple of 8 and divide by 8
    int columnCount = tuple.sizeInValues() + METADATA_COL_CNT;
    int nullMaskLength = ((columnCount + 7) & -8) >> 3;

    // row header is 32-bit length of row plus null mask
    *rowHeaderSz = sizeof (int32_t) + nullMaskLength + sizeof(uint8_t);

    // metadata column width: 5 int64_ts plus CHAR(1) + 6 bytes for type.
    size_t metadataSz = (sizeof (int64_t) * 5) + 6 + 1;

    // returns 0 if corrupt tuple detected
    size_t dataSz = tuple.maxExportSerializationSize();
    if (dataSz == 0) {
        throwFatalException("Invalid tuple passed to computeTupleMaxLength. Crashing System.");
    }

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
