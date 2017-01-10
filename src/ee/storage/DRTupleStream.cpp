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

#include "storage/DRTupleStream.h"

#include "common/TupleSchema.h"
#include "common/types.h"
#include "common/NValue.hpp"
#include "common/ValuePeeker.hpp"
#include "common/ValueFactory.hpp"
#include "common/tabletuple.h"
#include "common/ExportSerializeIo.h"
#include "common/executorcontext.hpp"
#include "common/UniqueId.hpp"
#include "crc/crc32c.h"
#include "indexes/tableindex.h"
#include "storage/TupleStreamException.h"

#include <vector>
#include <cstdio>
#include <limits>
#include <iostream>
#include <cassert>
#include <ctime>
#include <utility>
#include <math.h>

using namespace std;
using namespace voltdb;

DRTupleStream::DRTupleStream(int partitionId, int defaultBufferSize)
    : AbstractDRTupleStream(partitionId, defaultBufferSize),
      m_initialHashFlag(partitionId == 16383 ? TXN_PAR_HASH_REPLICATED : TXN_PAR_HASH_PLACEHOLDER),
      m_hashFlag(m_initialHashFlag),
      m_firstParHash(LONG_MAX),
      m_lastParHash(LONG_MAX),
      m_beginTxnUso(0),
      m_lastCommittedSpUniqueId(0),
      m_lastCommittedMpUniqueId(0)
{}

size_t DRTupleStream::truncateTable(int64_t lastCommittedSpHandle,
                                    char *tableHandle,
                                    std::string tableName,
                                    int partitionColumn,
                                    int64_t spHandle,
                                    int64_t uniqueId)
{
    if (m_guarded) return INVALID_DR_MARK;

    size_t startingUso = m_uso;

    transactionChecks(lastCommittedSpHandle, spHandle, uniqueId);

    //Drop the row, don't move the USO
    if (!m_enabled) return INVALID_DR_MARK;

    bool requireHashDelimiter = updateParHash(partitionColumn == -1, LONG_MAX);

    if (!m_currBlock) {
        try {
            extendBufferChain(m_defaultCapacity);
        } catch (TupleStreamException &e) {
            e.appendContextToMessage(" DR truncate table " + tableName);
            throw;
        }
    }

    size_t tupleMaxLength = TXN_RECORD_HEADER_SIZE + 4 + tableName.size(); // table name length and table name
    if (requireHashDelimiter) {
        tupleMaxLength += HASH_DELIMITER_SIZE;
    }
    if (m_currBlock->remaining() < tupleMaxLength) {
        try {
            extendBufferChain(tupleMaxLength);
        } catch (TupleStreamException &e) {
            e.appendContextToMessage(" DR truncate table " + tableName);
            throw;
        }
    }

    ExportSerializeOutput io(m_currBlock->mutableDataPtr(),
                             m_currBlock->remaining());

    if (requireHashDelimiter) {
        io.writeByte(static_cast<int8_t>(DR_RECORD_HASH_DELIMITER));
        io.writeInt(-1);  // hash delimiter for TRUNCATE_TABLE records is always -1
    }
    io.writeByte(static_cast<int8_t>(DR_RECORD_TRUNCATE_TABLE));
    io.writeLong(*reinterpret_cast<int64_t*>(tableHandle));
    io.writeInt(static_cast<int32_t>(tableName.size()));
    io.writeBytes(tableName.c_str(), tableName.size());

    // update m_offset
    m_currBlock->consumed(io.position());

    // update uso.
    m_uso += io.position();

    // update row count
    m_txnRowCount += rowCostForDRRecord(DR_RECORD_TRUNCATE_TABLE);

    return startingUso;
}

int64_t DRTupleStream::getParHashForTuple(TableTuple& tuple, int partitionColumn)
{
    if (partitionColumn != -1) {
        return static_cast<int64_t>(tuple.getNValue(partitionColumn).murmurHash3());
    } else {
        return LONG_MAX;
    }
}

bool DRTupleStream::updateParHash(bool isReplicatedTable, int64_t parHash)
{
    if (isReplicatedTable) {
        // For replicated table changes, the hash flag should stay the same as
        // the initial value, which is TXN_PAR_HASH_REPLICATED
        assert(m_hashFlag == m_initialHashFlag);
        return false;
    }

    if (m_hashFlag == TXN_PAR_HASH_PLACEHOLDER) {  // initial status, first record
        m_lastParHash = parHash;
        m_firstParHash = parHash; // save first hash
        // if the first record is TRUNCATE_TABLE, set to SPECIAL, otherwise SINGLE
        m_hashFlag = (parHash == LONG_MAX) ? TXN_PAR_HASH_SPECIAL : TXN_PAR_HASH_SINGLE;
        // no delimiter needed for first record
        return false;
    }
    else if (parHash != m_lastParHash) {
        m_lastParHash = parHash;
        // set to SPECIAL whenever we see a TRUNCATE_TABLE record
        if (parHash == LONG_MAX) {
            m_hashFlag = TXN_PAR_HASH_SPECIAL;
        }
        // set to MULTI if it was SINGLE
        else if (m_hashFlag == TXN_PAR_HASH_SINGLE) {
            m_hashFlag = TXN_PAR_HASH_MULTI;
        }
        // delimiter needed before the pending record
        return true;
    }
    // no delimiter needed for contiguous identical hashes
    return false;
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
                                  int partitionColumn,
                                  int64_t spHandle,
                                  int64_t uniqueId,
                                  TableTuple &tuple,
                                  DRRecordType type)
{
    if (m_guarded) return INVALID_DR_MARK;

    size_t startingUso = m_uso;

    size_t rowHeaderSz = 0;
    size_t rowMetadataSz = 0;
    size_t tupleMaxLength = 0;

    transactionChecks(lastCommittedSpHandle, spHandle, uniqueId);

    //Drop the row, don't move the USO
    if (!m_enabled) return INVALID_DR_MARK;

    bool requireHashDelimiter = updateParHash(partitionColumn == -1, getParHashForTuple(tuple, partitionColumn));

    // Compute the upper bound on bytes required to serialize tuple.
    // exportxxx: can memoize this calculation.
    tupleMaxLength = computeOffsets(type, tuple, rowHeaderSz, rowMetadataSz) + TXN_RECORD_HEADER_SIZE;
    if (requireHashDelimiter) {
        tupleMaxLength += HASH_DELIMITER_SIZE;
    }

    if (!m_currBlock) {
        try {
            extendBufferChain(m_defaultCapacity);
        } catch (TupleStreamException &e) {
            char msg[64];
            snprintf(msg, 64, " DR record type %d", type);
            e.appendContextToMessage(msg);
            throw;
        }
    }

    if (m_currBlock->remaining() < tupleMaxLength) {
        try {
            extendBufferChain(tupleMaxLength);
        } catch (TupleStreamException &e) {
            char msg[64];
            snprintf(msg, 64, " DR record type %d", type);
            e.appendContextToMessage(msg);
            throw;
        }
    }

    ExportSerializeOutput io(m_currBlock->mutableDataPtr(),
                             m_currBlock->remaining());

    if (requireHashDelimiter) {
        io.writeByte(static_cast<int8_t>(DR_RECORD_HASH_DELIMITER));
        io.writeInt(static_cast<int32_t>(m_lastParHash));
    }

    io.writeByte(static_cast<int8_t>(type));
    io.writeLong(*reinterpret_cast<int64_t*>(tableHandle));

    writeRowTuple(tuple, rowHeaderSz, rowMetadataSz, io);

    // update m_offset
    m_currBlock->consumed(io.position());

    // update uso.
    m_uso += io.position();

    // update row count
    m_txnRowCount += rowCostForDRRecord(type);

    // std::cout << "Appending row " << io.position() << " at " << m_currBlock->offset() << std::endl;
    return startingUso;
}

size_t DRTupleStream::appendUpdateRecord(int64_t lastCommittedSpHandle,
                                         char *tableHandle,
                                         int partitionColumn,
                                         int64_t spHandle,
                                         int64_t uniqueId,
                                         TableTuple &oldTuple,
                                         TableTuple &newTuple)
{
    if (m_guarded) return INVALID_DR_MARK;

    size_t startingUso = m_uso;

    size_t oldRowHeaderSz = 0;
    size_t oldRowMetadataSz = 0;
    size_t newRowHeaderSz = 0;
    size_t newRowMetadataSz = 0;
    size_t maxLength = TXN_RECORD_HEADER_SIZE;

    transactionChecks(lastCommittedSpHandle, spHandle, uniqueId);

    //Drop the row, don't move the USO
    if (!m_enabled) return INVALID_DR_MARK;

    bool requireHashDelimiter = updateParHash(partitionColumn == -1, getParHashForTuple(oldTuple, partitionColumn));

    DRRecordType type = DR_RECORD_UPDATE;
    maxLength += computeOffsets(type, oldTuple, oldRowHeaderSz, oldRowMetadataSz);
    // No danger of replacing the second tuple by an index key, since if the type is going to change
    // it has already done so in the above computeOffsets() call
    maxLength += computeOffsets(type, newTuple, newRowHeaderSz, newRowMetadataSz);
    if (requireHashDelimiter) {
        maxLength += HASH_DELIMITER_SIZE;
    }

    if (!m_currBlock) {
        try {
            extendBufferChain(m_defaultCapacity);
        } catch (TupleStreamException &e) {
            e.appendContextToMessage(" DR update tuple");
            throw;
        }
    }

    if (m_currBlock->remaining() < maxLength) {
        try {
            extendBufferChain(maxLength);
        } catch (TupleStreamException &e) {
            e.appendContextToMessage(" DR update tuple");
            throw;
        }
    }

    ExportSerializeOutput io(m_currBlock->mutableDataPtr(),
                                 m_currBlock->remaining());

    if (requireHashDelimiter) {
        io.writeByte(static_cast<int8_t>(DR_RECORD_HASH_DELIMITER));
        io.writeInt(static_cast<int32_t>(m_lastParHash));
    }

    io.writeByte(static_cast<int8_t>(type));
    io.writeLong(*reinterpret_cast<int64_t*>(tableHandle));

    writeRowTuple(oldTuple, oldRowHeaderSz, oldRowMetadataSz, io);
    writeRowTuple(newTuple, newRowHeaderSz, newRowMetadataSz, io);

    // update m_offset
    m_currBlock->consumed(io.position());

    // update uso.
    m_uso += io.position();

    // update row count
    m_txnRowCount += rowCostForDRRecord(type);

    // std::cout << "Appending row " << io.position() << " at " << m_currBlock->offset() << std::endl;
    return startingUso;
}

bool DRTupleStream::transactionChecks(int64_t lastCommittedSpHandle, int64_t spHandle, int64_t uniqueId)
{
    // Transaction IDs for transactions applied to this tuple stream
    // should always be moving forward in time.
    if (spHandle < m_openSpHandle) {
        throwFatalException(
                "Active transactions moving backwards: openSpHandle is %jd, while the truncate spHandle is %jd",
                (intmax_t)m_openSpHandle, (intmax_t)spHandle
                );
    }

    bool switchedToOpen = false;
    if (!m_opened) {
        ++m_openSequenceNumber;

        if (m_enabled) {
            beginTransaction(m_openSequenceNumber, spHandle, uniqueId);
        }
        else {
            openTransactionCommon(spHandle, uniqueId);
        }
        switchedToOpen = true;
    }
    assert(m_opened);
    return switchedToOpen;
}

void DRTupleStream::writeRowTuple(TableTuple& tuple,
        size_t rowHeaderSz,
        size_t rowMetadataSz,
        ExportSerializeOutput &io)
{
    size_t startPos = io.position();
    // initialize the full row header to 0. This also
    // has the effect of setting each column non-null.
    ::memset(m_currBlock->mutableDataPtr() + io.position(), 0, rowHeaderSz);
    // the nullarray lives in rowheader after the 4 byte header length prefix
    uint8_t *nullArray =
        reinterpret_cast<uint8_t*>(m_currBlock->mutableDataPtr() + io.position() + rowMetadataSz);

    // Reserve the row header by moving the position beyond the row header.
    // The row header includes the 4 byte length prefix and the null array.
    const size_t lengthPrefixPosition = io.reserveBytes(rowHeaderSz);

    tuple.serializeToDR(io, 0, nullArray);

    ExportSerializeOutput hdr(m_currBlock->mutableDataPtr() + lengthPrefixPosition, rowMetadataSz);
    // add the row length to the header
    hdr.writeInt((int32_t)(io.position() - startPos - sizeof(int32_t)));
}

size_t DRTupleStream::computeOffsets(DRRecordType &type,
        TableTuple &tuple,
        size_t &rowHeaderSz,
        size_t &rowMetadataSz)
{
    rowMetadataSz = sizeof(int32_t);
    int columnCount;
    switch (type) {
    case DR_RECORD_DELETE:
    case DR_RECORD_UPDATE:
        columnCount = tuple.sizeInValues();
        break;
    default:
        columnCount = tuple.sizeInValues();
        break;
    }
    int nullMaskLength = ((columnCount + 7) & -8) >> 3;
    rowHeaderSz = rowMetadataSz + nullMaskLength;
    return rowHeaderSz + tuple.maxDRSerializationSize();
}

void DRTupleStream::beginTransaction(int64_t sequenceNumber, int64_t spHandle, int64_t uniqueId)
{
    assert(!m_opened);

    if (!m_currBlock) {
         extendBufferChain(m_defaultCapacity);
     }

     if (m_currBlock->remaining() < BEGIN_RECORD_SIZE) {
         extendBufferChain(BEGIN_RECORD_SIZE);
     }

     m_currBlock->recordLastBeginTxnOffset();

     if (m_currBlock->lastDRSequenceNumber() != std::numeric_limits<int64_t>::max() &&
         m_currBlock->lastDRSequenceNumber() != (sequenceNumber - 1)) {
         throwFatalException(
             "Appending begin transaction message to a DR buffer without closing the previous transaction (open=%s)"
             " Block state: last closed sequence number (%jd), last closed uniqueIds (%jd, %jd)."
             " Transaction parameters: sequence number (%jd), uniqueId (%jd)."
             " Stream state: open sequence number (%jd), committed sequence number (%jd), open uniqueId (%jd), open spHandle (%jd), committed spHandle (%jd)",
             (m_opened ? "true" : "false"),
             (intmax_t)m_currBlock->lastDRSequenceNumber(), (intmax_t)m_currBlock->lastSpUniqueId(), (intmax_t)m_currBlock->lastMpUniqueId(),
             (intmax_t)sequenceNumber, (intmax_t)uniqueId,
             (intmax_t)m_openSequenceNumber, (intmax_t)m_committedSequenceNumber, (intmax_t)m_openUniqueId, (intmax_t)m_openSpHandle, (intmax_t)m_committedSpHandle);
     }

     m_currBlock->startDRSequenceNumber(sequenceNumber);

     ExportSerializeOutput io(m_currBlock->mutableDataPtr(),
                              m_currBlock->remaining());
     io.writeByte(static_cast<uint8_t>(PROTOCOL_VERSION));
     io.writeByte(static_cast<int8_t>(DR_RECORD_BEGIN_TXN));
     io.writeLong(uniqueId);
     io.writeLong(sequenceNumber);
     io.writeByte(0); // placeholder for hash flag
     io.writeInt(0); // placeholder for txn length
     io.writeInt(0);  // placeholder for first partition hash

     m_currBlock->consumed(io.position());

     m_beginTxnUso = m_uso;
     m_uso += io.position();

     m_hashFlag = m_initialHashFlag;
     m_firstParHash = LONG_MAX;
     m_lastParHash = LONG_MAX;

     openTransactionCommon(spHandle, uniqueId);
}

void DRTupleStream::endTransaction(int64_t uniqueId)
{
    if (!m_opened) {
        return;
    }

    if (!m_enabled) {
        if (m_openUniqueId != uniqueId) {
            throwFatalException(
                "Stream UniqueId (%jd) does not match the Context's UniqueId (%jd)."
                " DR sequence number is out of sync with UniqueId",
                (intmax_t)m_openUniqueId, (intmax_t)uniqueId);
        }

        if (UniqueId::isMpUniqueId(uniqueId)) {
            m_lastCommittedMpUniqueId = uniqueId;
        } else {
            m_lastCommittedSpUniqueId = uniqueId;
        }

        commitTransactionCommon();
        return;
    }

    if (!m_currBlock) {
        extendBufferChain(m_defaultCapacity);
    }

    if (m_currBlock->remaining() < END_RECORD_SIZE) {
        extendBufferChain(END_RECORD_SIZE);
    }

    if (m_currBlock->startDRSequenceNumber() == std::numeric_limits<int64_t>::max()) {
        throwFatalException(
            "Appending end transaction message to a DR buffer with no matching begin transaction message."
            "Stream state: open sequence number (%jd), committed sequence number (%jd), open uniqueId (%jd), open spHandle (%jd), committed spHandle (%jd)",
            (intmax_t)m_openSequenceNumber, (intmax_t)m_committedSequenceNumber, (intmax_t)m_openUniqueId, (intmax_t)m_openSpHandle, (intmax_t)m_committedSpHandle);
    }
    if (m_currBlock->lastDRSequenceNumber() != std::numeric_limits<int64_t>::max() &&
            m_currBlock->lastDRSequenceNumber() > m_openSequenceNumber) {
        throwFatalException(
            "Appending end transaction message to a DR buffer with a greater DR sequence number."
            " Buffer end DR sequence number (%jd), buffer end UniqueIds (%jd, %jd)."
            " Current DR sequence number (%jd), current UniqueId (%jd)",
            (intmax_t)m_currBlock->lastDRSequenceNumber(), (intmax_t)m_currBlock->lastSpUniqueId(),
            (intmax_t)m_currBlock->lastMpUniqueId(), (intmax_t)m_openSequenceNumber, (intmax_t)m_openUniqueId);
    }

    if (m_openUniqueId != uniqueId) {
        throwFatalException(
            "Stream UniqueId (%jd) does not match the Context's UniqueId (%jd)."
            " DR sequence number is out of sync with UniqueId",
            (intmax_t)m_openUniqueId, (intmax_t)uniqueId);
    }

    if (UniqueId::isMpUniqueId(uniqueId)) {
        m_lastCommittedMpUniqueId = uniqueId;
        m_currBlock->recordCompletedMpTxnForDR(uniqueId);
    }
    else {
        m_lastCommittedSpUniqueId = uniqueId;
        m_currBlock->recordCompletedSpTxnForDR(uniqueId);
    }
    m_currBlock->recordCompletedSequenceNumForDR(m_openSequenceNumber);

    ExportSerializeOutput io(m_currBlock->mutableDataPtr(),
                             m_currBlock->remaining());
    io.writeByte(static_cast<int8_t>(DR_RECORD_END_TXN));
    io.writeLong(m_openSequenceNumber);
    io.writeInt(0); // placeholder for checksum of the entire txn

    m_currBlock->consumed(io.position());

    m_uso += io.position();

    int32_t txnLength = m_uso - m_beginTxnUso;
    ExportSerializeOutput extraio(m_currBlock->mutableDataPtr() - txnLength,
                                  txnLength);
    extraio.position(BEGIN_RECORD_HEADER_SIZE);
    extraio.writeByte(static_cast<int8_t>(m_hashFlag));
    extraio.writeInt(txnLength);
    // if it is the replicated stream or first record is TRUNCATE_TABLE
    // m_firstParHash will be LONG_MAX, and will be written as -1 after casting
    extraio.writeInt(static_cast<int32_t>(m_firstParHash));

    uint32_t crc = vdbcrc::crc32cInit();
    crc = vdbcrc::crc32c(crc, m_currBlock->mutableDataPtr() - txnLength, txnLength - 4);
    crc = vdbcrc::crc32cFinish(crc);
    extraio.position(txnLength - 4);
    extraio.writeInt(crc);

    m_committedUso = m_uso;
    commitTransactionCommon();

    size_t bufferRowCount = m_currBlock->updateRowCountForDR(m_txnRowCount);
    if (m_rowTarget >= 0 && bufferRowCount >= m_rowTarget) {
        extendBufferChain(0);
    }
    m_txnRowCount = 0;
}

// If partial transaction is going to span multiple buffer, first time move it to
// the next buffer, the next time move it to a 45 megabytes buffer, then after throw
// an exception and rollback.
bool DRTupleStream::checkOpenTransaction(StreamBlock* sb, size_t minLength, size_t& blockSize, size_t& uso)
{
    if (sb && sb->hasDRBeginTxn()   /* this block contains a DR begin txn */
           && m_opened) {
        size_t partialTxnLength = sb->offset() - sb->lastDRBeginTxnOffset();
        size_t spaceNeeded = m_headerSpace + partialTxnLength + minLength;
        if (spaceNeeded > m_secondaryCapacity) {
            // txn larger than the max buffer size, set blockSize to 0 so that caller will abort
            blockSize = 0;

            char msg[256];
            snprintf(msg, 256, "Transaction requiring %jd bytes exceeds max DR Buffer size of %jd bytes",
                     spaceNeeded, m_secondaryCapacity);
            throw TupleStreamException(SQLException::volt_output_buffer_overflow, msg);
        } else if (spaceNeeded > m_defaultCapacity) {
            blockSize = m_secondaryCapacity;
        }
        if (blockSize != 0) {
            uso -= partialTxnLength;
        }
        return true;
    }
    assert(!m_opened);
    return false;
}

void DRTupleStream::generateDREvent(DREventType type, int64_t lastCommittedSpHandle, int64_t spHandle,
        int64_t uniqueId, ByteArray payloads)
{
    assert(!m_opened);

    ++m_openSequenceNumber;

    if (!m_enabled) {
        if (UniqueId::isMpUniqueId(uniqueId)) {
            m_lastCommittedMpUniqueId = uniqueId;
        } else {
            m_lastCommittedSpUniqueId = uniqueId;
        }

        openTransactionCommon(spHandle, uniqueId);
        commitTransactionCommon();
        return;
    }

    switch (type) {
    case CATALOG_UPDATE:
    case DR_STREAM_START: {
        // Make sure current block is empty
        extendBufferChain(0);
        ExportSerializeOutput io(m_currBlock->mutableDataPtr(), m_currBlock->remaining());
        io.writeBinaryString(payloads.data(), payloads.length());
        m_currBlock->consumed(io.position());
        m_uso += io.position();

        m_currBlock->startDRSequenceNumber(m_openSequenceNumber);
        m_currBlock->recordCompletedSequenceNumForDR(m_openSequenceNumber);
        if (UniqueId::isMpUniqueId(uniqueId)) {
            m_lastCommittedMpUniqueId = uniqueId;
            m_currBlock->recordCompletedMpTxnForDR(uniqueId);
        } else {
            m_lastCommittedSpUniqueId = uniqueId;
            m_currBlock->recordCompletedSpTxnForDR(uniqueId);
        }
        m_currBlock->markAsEventBuffer(type);

        m_committedUso = m_uso;
        openTransactionCommon(spHandle, uniqueId);
        commitTransactionCommon();

        extendBufferChain(0);

        pushPendingBlocks();
        break;
    }
    default:
        assert(false);
    }
}

int32_t DRTupleStream::getTestDRBuffer(int32_t partitionId,
    std::vector<int32_t> partitionKeyValueList,
    std::vector<int32_t> flagList,
    long startSequenceNumber,
    char *outBytes)
{
    DRTupleStream stream(partitionId, 2 * 1024 * 1024 + MAGIC_HEADER_SPACE_FOR_JAVA + MAGIC_DR_TRANSACTION_PADDING); // 2MB

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

    int64_t lastUID = UniqueId::makeIdFromComponents(-5, 0, partitionId);
    // Override start sequence number
    stream.m_openSequenceNumber = startSequenceNumber - 1;
    for (int ii = 0; ii < flagList.size(); ii++) {
        bool isMp = (flagList[ii] == TXN_PAR_HASH_MULTI && partitionKeyValueList[ii] != -1) ||
                    flagList[ii] == TXN_PAR_HASH_SPECIAL ||
                    (flagList[ii] == TXN_PAR_HASH_SINGLE && partitionKeyValueList[ii] == -1);
        int64_t uid = UniqueId::makeIdFromComponents(ii * 5, 0, isMp ? 16383 : partitionId);
        tuple.setNValue(0, ValueFactory::getIntegerValue(partitionKeyValueList[ii]));

        if (flagList[ii] == TXN_PAR_HASH_SPECIAL) {
            stream.truncateTable(lastUID, tableHandle, "foobar", partitionId == 16383 ? -1 : 0, uid, uid);
        }

        for (int zz = 0; zz < 5; zz++) {
            stream.appendTuple(lastUID, tableHandle, partitionId == 16383 ? -1 : 0, uid, uid, tuple, DR_RECORD_INSERT);
        }

        if (flagList[ii] == TXN_PAR_HASH_MULTI) {
            tuple.setNValue(0, ValueFactory::getIntegerValue(partitionKeyValueList[ii] + 1));
            for (int zz = 0; zz < 5; zz++) {
                stream.appendTuple(lastUID, tableHandle,  partitionId == 16383 ? -1 : 0, uid, uid, tuple, DR_RECORD_INSERT);
            }
        }
        else if (flagList[ii] == TXN_PAR_HASH_SPECIAL) {
            stream.truncateTable(lastUID, tableHandle, "foobar", partitionId == 16383 ? -1 : 0, uid, uid);
            stream.truncateTable(lastUID, tableHandle, "foobar", partitionId == 16383 ? -1 : 0, uid, uid);
        }

        stream.endTransaction(uid);
        lastUID = uid;
    }

    TupleSchema::freeTupleSchema(schema);

    size_t headerSize = MAGIC_HEADER_SPACE_FOR_JAVA + MAGIC_DR_TRANSACTION_PADDING;
    const int32_t adjustedLength = static_cast<int32_t>(stream.m_currBlock->rawLength() - headerSize);
    ::memcpy(outBytes, stream.m_currBlock->rawPtr() + headerSize, adjustedLength);
    return adjustedLength;
}
