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

#include "storage/DRTupleStream.h"

#include "common/TupleSchema.h"
#include "common/types.h"
#include "common/NValue.hpp"
#include "common/ValuePeeker.hpp"
#include "common/tabletuple.h"
#include "common/ExportSerializeIo.h"
#include "common/executorcontext.hpp"
#include "common/UniqueId.hpp"
#include "crc/crc32c.h"

#include <cstdio>
#include <limits>
#include <iostream>
#include <cassert>
#include <ctime>
#include <utility>
#include <math.h>

using namespace std;
using namespace voltdb;

DRTupleStream::DRTupleStream()
    : TupleStreamBase(),
      m_enabled(true),
      m_secondaryCapacity(SECONDARY_BUFFER_SIZE)
{}

void DRTupleStream::setSecondaryCapacity(size_t capacity) {
    assert (capacity > 0);
    if (m_uso != 0 || m_openSpHandle != 0 ||
        m_openTransactionUso != 0 || m_committedSpHandle != 0)
    {
        throwFatalException("setSecondaryCapacity only callable before "
                            "TupleStreamBase is used");
    }
    m_secondaryCapacity = capacity;
}

size_t DRTupleStream::truncateTable(int64_t lastCommittedSpHandle,
                                    char *tableHandle,
                                    std::string tableName,
                                    int64_t txnId,
                                    int64_t spHandle,
                                    int64_t uniqueId) {
    //Drop the row, don't move the USO
    if (!m_enabled) return m_uso;

    // Transaction IDs for transactions applied to this tuple stream
    // should always be moving forward in time.
    if (spHandle < m_openSpHandle) {
        throwFatalException(
                "Active transactions moving backwards: openSpHandle is %jd, while the append spHandle is %jd",
                (intmax_t)m_openSpHandle, (intmax_t)spHandle
                );
    }

    size_t startingUso = commit(lastCommittedSpHandle, spHandle, txnId, uniqueId, false, false);

    if (!m_currBlock) {
        extendBufferChain(m_defaultCapacity);
    }

    const size_t tupleMaxLength = 1 + 1 + 8 + 4 + tableName.size() + 4;//version, type, table handle, name length prefix, table name, checksum
    if (m_currBlock->remaining() < tupleMaxLength) {
        extendBufferChain(tupleMaxLength);
    }


    ExportSerializeOutput io(m_currBlock->mutableDataPtr(),
                             m_currBlock->remaining());

    io.writeByte(DR_VERSION);
    io.writeByte(static_cast<int8_t>(DR_RECORD_TRUNCATE_TABLE));
    io.writeLong(*reinterpret_cast<int64_t*>(tableHandle));
    io.writeInt(static_cast<int32_t>(tableName.size()));
    io.writeBytes(tableName.c_str(), tableName.size());

    uint32_t crc = vdbcrc::crc32cInit();
    crc = vdbcrc::crc32c( crc, m_currBlock->mutableDataPtr(), io.position());
    crc = vdbcrc::crc32cFinish(crc);
    io.writeInt(crc);

    // update m_offset
    m_currBlock->consumed(io.position());

    // No BEGIN TXN entry was written, use the current USO
    if (startingUso == 0) {
        startingUso = m_uso;
    }
    // update uso.
    m_uso += io.position();

    return startingUso;
}

/*
 * If SpHandle represents a new transaction, commit previous data.
 * Always serialize the supplied tuple in to the stream.
 * Return m_uso before this invocation - this marks the point
 * in the stream the caller can rollback to if this append
 * should be rolled back.
 */
size_t DRTupleStream::appendTuple(int64_t lastCommittedSpHandle,
                                  char *tableHandle,
                                  int64_t txnId,
                                  int64_t spHandle,
                                  int64_t uniqueId,
                                  TableTuple &tuple,
                                  DRRecordType type)
{
    //Drop the row, don't move the USO
    if (!m_enabled) return m_uso;

    size_t rowHeaderSz = 0;
    size_t tupleMaxLength = 0;

    // Transaction IDs for transactions applied to this tuple stream
    // should always be moving forward in time.
    if (spHandle < m_openSpHandle)
    {
        throwFatalException(
                "Active transactions moving backwards: openSpHandle is %jd, while the truncate spHandle is %jd",
                (intmax_t)m_openSpHandle, (intmax_t)spHandle
                );
    }

    size_t startingUso = commit(lastCommittedSpHandle, spHandle, txnId, uniqueId, false, false);

    // Compute the upper bound on bytes required to serialize tuple.
    // exportxxx: can memoize this calculation.
    tupleMaxLength = computeOffsets(tuple, &rowHeaderSz) + TXN_RECORD_HEADER_SIZE;

    if (!m_currBlock) {
        extendBufferChain(m_defaultCapacity);
    }

    if (m_currBlock->remaining() < tupleMaxLength) {
        extendBufferChain(tupleMaxLength);
    }

    ExportSerializeOutput io(m_currBlock->mutableDataPtr(),
                             m_currBlock->remaining());
    io.writeByte(DR_VERSION);
    io.writeByte(static_cast<int8_t>(type));
    io.writeLong(*reinterpret_cast<int64_t*>(tableHandle));

    // initialize the full row header to 0. This also
    // has the effect of setting each column non-null.
    ::memset(m_currBlock->mutableDataPtr() + io.position(), 0, rowHeaderSz);

    // the nullarray lives in rowheader after the 4 byte header length prefix
    uint8_t *nullArray =
        reinterpret_cast<uint8_t*>(m_currBlock->mutableDataPtr() + io.position() + sizeof(int32_t));

    // Reserve the row header by moving the position beyond the row header.
    // The row header includes the 4 byte length prefix and the null array.
    const size_t lengthPrefixPosition = io.reserveBytes(rowHeaderSz);

    // write the tuple's data
    tuple.serializeToExport(io, 0, nullArray);

    // write the row size in to the row header
    // rowlength does not include the 4 byte length prefix or record header
    // but does include the null array.
    ExportSerializeOutput hdr(m_currBlock->mutableDataPtr() + lengthPrefixPosition, 4);
    //The TXN_RECORD_HEADER_SIZE is 4 bytes longer because it includes the checksum at the end
    //so there is no need to subtract and additional 4 bytes to make the length prefix not inclusive
    hdr.writeInt((int32_t)(io.position() - TXN_RECORD_HEADER_SIZE));

    uint32_t crc = vdbcrc::crc32cInit();
    crc = vdbcrc::crc32c( crc, m_currBlock->mutableDataPtr(), io.position());
    crc = vdbcrc::crc32cFinish(crc);
    io.writeInt(crc);

    // update m_offset
    m_currBlock->consumed(io.position());

    // No BEGIN TXN entry was written, use the current USO
    if (startingUso == 0) {
        startingUso = m_uso;
    }
    // update uso.
    m_uso += io.position();

//    std::cout << "Appending row " << io.position() << " at " << m_currBlock->offset() << std::endl;
    return startingUso;
}

size_t
DRTupleStream::computeOffsets(TableTuple &tuple,
                                   size_t *rowHeaderSz)
{
    // round-up columncount to next multiple of 8 and divide by 8
    const int columnCount = tuple.sizeInValues();
    int nullMaskLength = ((columnCount + 7) & -8) >> 3;

    // row header is 32-bit length of row plus null mask
    *rowHeaderSz = sizeof(int32_t) + nullMaskLength;

    //Can return 0 for a single column varchar with null
    size_t dataSz = tuple.maxExportSerializationSize();

    return *rowHeaderSz + dataSz;
}

void DRTupleStream::pushExportBuffer(StreamBlock *block, bool sync, bool endOfStream) {
    if (sync) return;
    ExecutorContext::getExecutorContext()->getTopend()->pushDRBuffer(m_partitionId, block);
}

size_t DRTupleStream::beginTransaction(int64_t sequenceNumber, int64_t uniqueId) {
    if (!m_currBlock) {
         extendBufferChain(m_defaultCapacity);
     }

     m_currBlock->recordLastBeginTxnOffset();

     if (m_currBlock->remaining() < BEGIN_RECORD_SIZE) {
         extendBufferChain(BEGIN_RECORD_SIZE);
     }

     if (m_currBlock->lastDRSequenceNumber() != std::numeric_limits<int64_t>::max() &&
         m_currBlock->lastDRSequenceNumber() != (sequenceNumber - 1)) {
         throwFatalException(
             "Appending begin transaction message to a DR buffer without closing the previous transaction."
             " Last closed DR sequence number (%jd), last closed unique ID (%jd)."
             " Current DR sequence number (%jd), current unique ID (%jd)",
             (intmax_t)m_currBlock->lastDRSequenceNumber(), (intmax_t)m_currBlock->lastUniqueId(),
             (intmax_t)sequenceNumber, (intmax_t)uniqueId);
     }

     m_currBlock->startDRSequenceNumber(sequenceNumber);

     ExportSerializeOutput io(m_currBlock->mutableDataPtr(),
                              m_currBlock->remaining());
     io.writeByte(DR_VERSION);
     io.writeByte(static_cast<int8_t>(DR_RECORD_BEGIN_TXN));
     io.writeLong(uniqueId);
     io.writeLong(sequenceNumber);
     uint32_t crc = vdbcrc::crc32cInit();
     crc = vdbcrc::crc32c( crc, m_currBlock->mutableDataPtr(), BEGIN_RECORD_SIZE - 4);
     crc = vdbcrc::crc32cFinish(crc);
     io.writeInt(crc);
     m_currBlock->consumed(io.position());

     const size_t startingUso = m_uso;
     m_uso += io.position();

     return startingUso;
}

size_t DRTupleStream::endTransaction(int64_t sequenceNumber, int64_t uniqueId) {
    if (!m_currBlock) {
         extendBufferChain(m_defaultCapacity);
     }

     if (m_currBlock->remaining() < END_RECORD_SIZE) {
         extendBufferChain(END_RECORD_SIZE);
     }

     if (m_currBlock->startDRSequenceNumber() == std::numeric_limits<int64_t>::max()) {
         throwFatalException(
             "Appending end transaction message to a DR buffer with no matching begin transaction message."
             " DR sequence number (%jd), unique ID (%jd)",
             (intmax_t)sequenceNumber, (intmax_t)uniqueId);
     }
     if (m_currBlock->lastDRSequenceNumber() != std::numeric_limits<int64_t>::max() &&
         m_currBlock->lastDRSequenceNumber() > sequenceNumber) {
         throwFatalException(
             "Appending end transaction message to a DR buffer with a greater DR sequence number."
             " Buffer end DR sequence number (%jd), buffer end unique ID (%jd)."
             " Current DR sequence number (%jd), current unique ID (%jd)",
             (intmax_t)m_currBlock->lastDRSequenceNumber(), (intmax_t)m_currBlock->lastUniqueId(),
             (intmax_t)sequenceNumber, (intmax_t)uniqueId);
     }

     m_currBlock->recordCompletedTxnForDR(sequenceNumber, uniqueId);

     ExportSerializeOutput io(m_currBlock->mutableDataPtr(),
                              m_currBlock->remaining());
     io.writeByte(DR_VERSION);
     io.writeByte(static_cast<int8_t>(DR_RECORD_END_TXN));
     io.writeLong(sequenceNumber);
     uint32_t crc = vdbcrc::crc32cInit();
     crc = vdbcrc::crc32c( crc, m_currBlock->mutableDataPtr(), END_RECORD_SIZE - 4);
     crc = vdbcrc::crc32cFinish(crc);
     io.writeInt(crc);
     m_currBlock->consumed(io.position());

     const size_t startingUso = m_uso;
     m_uso += io.position();

     return startingUso;
}

// If partial transaction is going to span multiple buffer, first time move it to
// the next buffer, the next time move it to a 45 megabytes buffer, then after throw
// an exception and rollback.
bool DRTupleStream::checkOpenTransaction(StreamBlock* sb, size_t minLength, size_t& blockSize, size_t& uso, bool continueTxn) {
    if (sb && continueTxn           /* this is not a flush, or there's still a transaction ongoing */
           && sb->hasDRBeginTxn()   /* this block contains a DR begin txn */
           && sb->lastDRBeginTxnOffset() != sb->offset() /* current txn is not a DR begin txn */) {
        size_t partialTxnLength = sb->offset() - sb->lastDRBeginTxnOffset();
        if (partialTxnLength + minLength >= (m_defaultCapacity - MAGIC_HEADER_SPACE_FOR_JAVA)) {
            switch (sb->type()) {
                case voltdb::NORMAL_STREAM_BLOCK:
                {
                    blockSize = m_secondaryCapacity;
                    break;
                }
                case voltdb::LARGE_STREAM_BLOCK:
                {
                    blockSize = 0;
                    break;
                }
            }
        }
        if (blockSize != 0) {
            uso -= partialTxnLength;
        }
        return true;
    }
    return false;
}

void DRTupleStream::setLastCommittedSequenceNumber(int64_t sequenceNumber) {
    assert(m_committedSequenceNumber == 0);
    m_openSequenceNumber = sequenceNumber;
    m_committedSequenceNumber = sequenceNumber;
}

int32_t DRTupleStream::getTestDRBuffer(char *outBytes) {
    DRTupleStream stream;
    stream.configure(42);

    char tableHandle[] = { 'f', 'f', 'f', 'f', 'f', 'f', 'f', 'f', 'f', 'f', 'f',
                           'f', 'f', 'f', 'f', 'f', 'f', 'f', 'f', 'f', 'f', 'f' };

    // set up the schema used to fill the new buffer
    std::vector<ValueType> columnTypes;
    std::vector<int32_t> columnLengths;
    std::vector<bool> columnAllowNull;
    for (int i = 0; i < 2; i++) {
        columnTypes.push_back(VALUE_TYPE_INTEGER);
        columnLengths.push_back(NValue::getTupleStorageSize(VALUE_TYPE_INTEGER));
        columnAllowNull.push_back(false);
    }
    TupleSchema *schema = TupleSchema::createTupleSchemaForTest(columnTypes,
                                                                columnLengths,
                                                                columnAllowNull);
    char tupleMemory[(2 + 1) * 8];
    TableTuple tuple(tupleMemory, schema);

    for (int ii = 0; ii < 100;) {
        int64_t lastUID = UniqueId::makeIdFromComponents(ii - 5, 0, 42);
        int64_t uid = UniqueId::makeIdFromComponents(ii, 0, 42);
        for (int zz = 0; zz < 5; zz++) {
            stream.appendTuple(lastUID, tableHandle, uid, uid, uid, tuple, DR_RECORD_INSERT );
        }
        ii += 5;
    }

    TupleSchema::freeTupleSchema(schema);

    int64_t lastUID = UniqueId::makeIdFromComponents(99, 0, 42);
    int64_t uid = UniqueId::makeIdFromComponents(100, 0, 42);
    stream.truncateTable(lastUID, tableHandle, "foobar", uid, uid, uid);

    int64_t committedUID = UniqueId::makeIdFromComponents(100, 0, 42);
    stream.commit(committedUID, committedUID, committedUID, committedUID, false, false);

    const int32_t adjustedLength = stream.m_currBlock->rawLength() - MAGIC_HEADER_SPACE_FOR_JAVA;
    ::memcpy(outBytes, stream.m_currBlock->rawPtr() + MAGIC_HEADER_SPACE_FOR_JAVA, adjustedLength);
    return adjustedLength;

}
