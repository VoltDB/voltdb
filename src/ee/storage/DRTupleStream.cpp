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

DRTupleStream::DRTupleStream()
    : AbstractDRTupleStream(),
      m_lastCommittedSpUniqueId(0),
      m_lastCommittedMpUniqueId(0)
{}

size_t DRTupleStream::truncateTable(int64_t lastCommittedSpHandle,
                                    char *tableHandle,
                                    std::string tableName,
                                    int64_t txnId,
                                    int64_t spHandle,
                                    int64_t uniqueId) {
    size_t startingUso = m_uso;

    //Drop the row, don't move the USO
    if (!m_enabled) return INVALID_DR_MARK;

    transactionChecks(lastCommittedSpHandle, txnId, spHandle, uniqueId);
    bool requireHashDelimiter = (m_hashFlag == TXN_PAR_HASH_REPLICATED) ? false : updateParHash(LONG_MAX);

    if (!m_currBlock) {
        extendBufferChain(m_defaultCapacity);
    }

    size_t tupleMaxLength = TXN_RECORD_HEADER_SIZE + 4 + tableName.size(); // table name length and table name
    if (requireHashDelimiter) {
        tupleMaxLength += HASH_DELIMITER_SIZE;
    }
    if (m_currBlock->remaining() < tupleMaxLength) {
        extendBufferChain(tupleMaxLength);
    }

    ExportSerializeOutput io(m_currBlock->mutableDataPtr(),
                             m_currBlock->remaining());

    if (requireHashDelimiter) {
        io.writeByte(static_cast<int8_t>(DR_RECORD_HASH_DELIMITER));
        io.writeInt(0);  // hash delimiter for TRUNCATE_TABLE records is always 0
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

int64_t DRTupleStream::getParHashForTuple(TableTuple& tuple, int partitionColumn) {
    if (partitionColumn == -1) {
        return LONG_MAX;
    }
    return static_cast<int64_t>(tuple.getNValue(partitionColumn).murmurHash3());
}

bool DRTupleStream::updateParHash(int64_t parHash) {
    if (m_hashFlag == TXN_PAR_HASH_PLACEHOLDER) {  // initial status, first record
        m_lastParHash = parHash;
        m_firstParHash = (parHash == LONG_MAX) ? 0 : parHash; // save first hash
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
                                  int64_t txnId,
                                  int64_t spHandle,
                                  int64_t uniqueId,
                                  TableTuple &tuple,
                                  DRRecordType type,
                                  const std::pair<const TableIndex*, uint32_t>& indexPair)
{
    size_t startingUso = m_uso;

    //Drop the row, don't move the USO
    if (!m_enabled) return INVALID_DR_MARK;

    size_t rowHeaderSz = 0;
    size_t rowMetadataSz = 0;
    size_t tupleMaxLength = 0;
    const std::vector<int>* interestingColumns;

    transactionChecks(lastCommittedSpHandle, txnId, spHandle, uniqueId);
    bool requireHashDelimiter = (m_hashFlag == TXN_PAR_HASH_REPLICATED) ? false : updateParHash(getParHashForTuple(tuple, partitionColumn));

    // Compute the upper bound on bytes required to serialize tuple.
    // exportxxx: can memoize this calculation.
    tupleMaxLength = computeOffsets(type, indexPair, tuple, rowHeaderSz, rowMetadataSz, interestingColumns) + TXN_RECORD_HEADER_SIZE;
    if (requireHashDelimiter) {
        tupleMaxLength += HASH_DELIMITER_SIZE;
    }

    if (!m_currBlock) {
        extendBufferChain(m_defaultCapacity);
    }

    if (m_currBlock->remaining() < tupleMaxLength) {
        extendBufferChain(tupleMaxLength);
    }

    ExportSerializeOutput io(m_currBlock->mutableDataPtr(),
                             m_currBlock->remaining());

    if (requireHashDelimiter) {
        io.writeByte(static_cast<int8_t>(DR_RECORD_HASH_DELIMITER));
        io.writeInt(static_cast<int32_t>(m_lastParHash));
    }

    io.writeByte(static_cast<int8_t>(type));
    io.writeLong(*reinterpret_cast<int64_t*>(tableHandle));

    writeRowTuple(tuple, rowHeaderSz, rowMetadataSz, interestingColumns, indexPair, io);

    // update m_offset
    m_currBlock->consumed(io.position());

    // update uso.
    m_uso += io.position();

    // update row count
    m_txnRowCount += rowCostForDRRecord(type);

//    std::cout << "Appending row " << io.position() << " at " << m_currBlock->offset() << std::endl;
    return startingUso;
}

size_t DRTupleStream::appendUpdateRecord(int64_t lastCommittedSpHandle,
                                         char *tableHandle,
                                         int partitionColumn,
                                         int64_t txnId,
                                         int64_t spHandle,
                                         int64_t uniqueId,
                                         TableTuple &oldTuple,
                                         TableTuple &newTuple,
                                         const std::pair<const TableIndex*, uint32_t>& indexPair) {
    size_t startingUso = m_uso;

    //Drop the row, don't move the USO
    if (!m_enabled) return INVALID_DR_MARK;

    size_t oldRowHeaderSz = 0;
    size_t oldRowMetadataSz = 0;
    size_t newRowHeaderSz = 0;
    size_t newRowMetadataSz = 0;
    size_t maxLength = TXN_RECORD_HEADER_SIZE;
    const std::vector<int>* oldRowInterestingColumns;
    const std::vector<int>* dummyInterestingColumns;

    transactionChecks(lastCommittedSpHandle, txnId, spHandle, uniqueId);
    bool requireHashDelimiter = (m_hashFlag == TXN_PAR_HASH_REPLICATED) ? false : updateParHash(getParHashForTuple(oldTuple, partitionColumn));

    DRRecordType type = DR_RECORD_UPDATE;
    maxLength += computeOffsets(type, indexPair, oldTuple, oldRowHeaderSz, oldRowMetadataSz, oldRowInterestingColumns);
    // No danger of replacing the second tuple by an index key, since if the type is going to change
    // it has already done so in the above computeOffsets() call
    maxLength += computeOffsets(type, indexPair, newTuple, newRowHeaderSz, newRowMetadataSz, dummyInterestingColumns);
    assert(!dummyInterestingColumns);
    if (requireHashDelimiter) {
        maxLength += HASH_DELIMITER_SIZE;
    }

    if (!m_currBlock) {
        extendBufferChain(m_defaultCapacity);
    }

    if (m_currBlock->remaining() < maxLength) {
        extendBufferChain(maxLength);
    }

    ExportSerializeOutput io(m_currBlock->mutableDataPtr(),
                                 m_currBlock->remaining());

    if (requireHashDelimiter) {
        io.writeByte(static_cast<int8_t>(DR_RECORD_HASH_DELIMITER));
        io.writeInt(static_cast<int32_t>(m_lastParHash));
    }

    io.writeByte(static_cast<int8_t>(type));
    io.writeLong(*reinterpret_cast<int64_t*>(tableHandle));

    writeRowTuple(oldTuple, oldRowHeaderSz, oldRowMetadataSz, oldRowInterestingColumns, indexPair, io);
    writeRowTuple(newTuple, newRowHeaderSz, newRowMetadataSz, NULL, indexPair, io);

    // update m_offset
    m_currBlock->consumed(io.position());

    // update uso.
    m_uso += io.position();

    // update row count
    m_txnRowCount += rowCostForDRRecord(type);

//    std::cout << "Appending row " << io.position() << " at " << m_currBlock->offset() << std::endl;
    return startingUso;
}

void DRTupleStream::transactionChecks(int64_t lastCommittedSpHandle, int64_t txnId, int64_t spHandle, int64_t uniqueId) {
    // Transaction IDs for transactions applied to this tuple stream
    // should always be moving forward in time.
    if (spHandle < m_openSpHandle) {
        throwFatalException(
                "Active transactions moving backwards: openSpHandle is %jd, while the truncate spHandle is %jd",
                (intmax_t)m_openSpHandle, (intmax_t)spHandle
                );
    }

    commit(lastCommittedSpHandle, spHandle, txnId, uniqueId, false, false);
    if (!m_opened) {
        beginTransaction(m_openSequenceNumber, uniqueId);
    }
    assert(m_opened);
}

void DRTupleStream::writeRowTuple(TableTuple& tuple,
        size_t rowHeaderSz,
        size_t rowMetadataSz,
        const std::vector<int> *interestingColumns,
        const std::pair<const TableIndex*, uint32_t> &indexPair,
        ExportSerializeOutput &io) {
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

    tuple.serializeToDR(io, 0, nullArray, interestingColumns);

    ExportSerializeOutput hdr(m_currBlock->mutableDataPtr() + lengthPrefixPosition, rowMetadataSz);
    if (interestingColumns) {
        // add the row length and a crc of the index used to the header
        hdr.writeInt((int32_t)(io.position() - startPos - 2 * sizeof(int32_t)));
        hdr.writeInt(indexPair.second);
    } else {
        // add the row length to the header
        hdr.writeInt((int32_t)(io.position() - startPos - sizeof(int32_t)));
    }
}

size_t DRTupleStream::computeOffsets(DRRecordType &type,
        const std::pair<const TableIndex*, uint32_t> &indexPair,
        TableTuple &tuple,
        size_t &rowHeaderSz,
        size_t &rowMetadataSz,
        const std::vector<int> *&interestingColumns) {
    interestingColumns = NULL;
    rowMetadataSz = sizeof(int32_t);
    int columnCount;
    switch (type) {
    case DR_RECORD_DELETE:
    case DR_RECORD_UPDATE:
        if (indexPair.first) {
            // The index-optimized versions of these types have values exactly
            // 5 larger than the unoptimized versions (asserted in test)
            // DR_RECORD_DELETE => DR_RECORD_DELETE_BY_INDEX
            // DR_RECORD_UPDATE => DR_RECORD_UPDATE_BY_INDEX
            type = static_cast<DRRecordType>((int)type + 5);
            interestingColumns = &(indexPair.first->getColumnIndices());
            rowMetadataSz += sizeof(int32_t);
            columnCount = static_cast<int>(interestingColumns->size());
        } else {
            columnCount = tuple.sizeInValues();
        }
        break;
    default:
        columnCount = tuple.sizeInValues();
        break;
    }
    int nullMaskLength = ((columnCount + 7) & -8) >> 3;
    rowHeaderSz = rowMetadataSz + nullMaskLength;
    return rowHeaderSz + tuple.maxDRSerializationSize(interestingColumns);
}

void DRTupleStream::beginTransaction(int64_t sequenceNumber, int64_t uniqueId) {
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

     m_opened = true;
}

void DRTupleStream::endTransaction(int64_t uniqueId) {
    if (!m_opened) {
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

    size_t txnLength = m_uso - m_beginTxnUso;
    ExportSerializeOutput extraio(m_currBlock->mutableDataPtr() - txnLength,
                                  txnLength);
    extraio.position(BEGIN_RECORD_HEADER_SIZE);
    extraio.writeByte(static_cast<int8_t>(m_hashFlag));
    extraio.writeInt(static_cast<uint32_t>(txnLength));
    extraio.writeInt(static_cast<int32_t>(m_firstParHash));

    uint32_t crc = vdbcrc::crc32cInit();
    crc = vdbcrc::crc32c(crc, m_currBlock->mutableDataPtr() - txnLength, txnLength - 4);
    crc = vdbcrc::crc32cFinish(crc);
    extraio.position(txnLength - 4);
    extraio.writeInt(crc);

    m_opened = false;
    if (m_hashFlag != TXN_PAR_HASH_REPLICATED) {
        m_hashFlag = TXN_PAR_HASH_PLACEHOLDER;
    }

    size_t bufferRowCount = m_currBlock->updateRowCountForDR(m_txnRowCount);
    if (m_rowTarget >= 0 && bufferRowCount >= m_rowTarget) {
        extendBufferChain(0);
    }
    m_txnRowCount = 0;
}

// If partial transaction is going to span multiple buffer, first time move it to
// the next buffer, the next time move it to a 45 megabytes buffer, then after throw
// an exception and rollback.
bool DRTupleStream::checkOpenTransaction(StreamBlock* sb, size_t minLength, size_t& blockSize, size_t& uso) {
    if (sb && sb->hasDRBeginTxn()   /* this block contains a DR begin txn */
           && m_opened) {
        size_t partialTxnLength = sb->offset() - sb->lastDRBeginTxnOffset();
        size_t spaceNeeded = m_headerSpace + partialTxnLength + minLength;
        if (spaceNeeded > m_secondaryCapacity) {
            // txn larger than the max buffer size, set blockSize to 0 so that caller will abort
            blockSize = 0;
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

int32_t DRTupleStream::getTestDRBuffer(int32_t partitionKeyValue, int32_t partitionId, int32_t flag, char *outBytes) {
    if (flag == TXN_PAR_HASH_REPLICATED) {
        partitionId = 16383;
    }
    DRTupleStream stream;
    stream.configure(partitionId);

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
    // set the partition key
    tuple.setNValue(0, ValueFactory::getIntegerValue(partitionKeyValue));

    const TableIndex* index = NULL;
    std::pair<const TableIndex*, uint32_t> uniqueIndex = std::make_pair(index, -1);
    for (int ii = 0; ii < 100;) {
        int64_t lastUID = UniqueId::makeIdFromComponents(ii - 5, 0, partitionId);
        int64_t uid = UniqueId::makeIdFromComponents(ii, 0, partitionId);

        for (int zz = 0; zz < 5; zz++) {
            stream.appendTuple(lastUID, tableHandle, 0, uid, uid, uid, tuple, DR_RECORD_INSERT, uniqueIndex);
        }

        if (flag == TXN_PAR_HASH_REPLICATED || flag == TXN_PAR_HASH_SPECIAL) {
            stream.truncateTable(lastUID, tableHandle, "foobar", uid, uid, uid);
            stream.truncateTable(lastUID, tableHandle, "foobar", uid, uid, uid);
        }

        if (flag != TXN_PAR_HASH_SINGLE) {
            tuple.setNValue(0, ValueFactory::getIntegerValue(partitionKeyValue + 1));
            for (int zz = 0; zz < 5; zz++) {
                stream.appendTuple(lastUID, tableHandle, 0, uid, uid, uid, tuple, DR_RECORD_INSERT, uniqueIndex);
            }
            tuple.setNValue(0, ValueFactory::getIntegerValue(partitionKeyValue));
        }

        stream.endTransaction(uid);
        ii += 5;
    }

    TupleSchema::freeTupleSchema(schema);

    int64_t committedUID = UniqueId::makeIdFromComponents(95, 0, partitionId);
    stream.commit(committedUID, committedUID, committedUID, committedUID, false, false);

    size_t headerSize = MAGIC_HEADER_SPACE_FOR_JAVA + MAGIC_DR_TRANSACTION_PADDING;
    const int32_t adjustedLength = static_cast<int32_t>(stream.m_currBlock->rawLength() - headerSize);
    ::memcpy(outBytes, stream.m_currBlock->rawPtr() + headerSize, adjustedLength);
    return adjustedLength;

}
